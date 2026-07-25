from kafka import KafkaProducer, KafkaConsumer
import time
import threading
from typing import Callable, Optional
from source_command import SourceCommand
import logging
import subprocess
from settings import general_settings
import storage
import json

logger = logging.getLogger(__name__)


class HealthService:
    def __init__(
        self,
        run_source: Callable[[SourceCommand], list[bool]],
    ):
        self.run_source = run_source

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _handle_retry(self):
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                timeout=10,
                text=True,
            )
            if result.returncode != 0:
                logging.error(
                    f"Error while getting list of running adapters: {result.stderr}"
                )
                return

            containers = result.stdout.strip().split("\n")
            running_adapter_ids = set(
                [
                    c.removeprefix(general_settings.container_name_prefix)
                    for c in containers
                    if c.startswith(general_settings.container_name_prefix)
                ]
            )
            rtsp_ids = set(storage.list_ids())

            shutdown_adapters = rtsp_ids - running_adapter_ids
            if len(untracked := running_adapter_ids - rtsp_ids):

                logging.critical(
                    f"There are untracked adapters running:"
                    f"{[general_settings.container_name_prefix + u for u in untracked]}"
                )

            sources_to_retry = {
                rtsp_id: storage.get(rtsp_id) for rtsp_id in shutdown_adapters
            }

            for rtsp_id, command in sources_to_retry.items():
                if command is None:
                    logging.warning(
                        f"No persisted command found for source '{rtsp_id}'"
                    )
                    continue

                success, retry = self.run_source(command)
                if success or retry:
                    if success:
                        logging.info(
                            f"Successfully added source {rtsp_id} in retry. Active sources: {list(rtsp_ids)}"
                        )
                    else:
                        logging.info(
                            f"Unable to add the new source {rtsp_id}. Will retry again later."
                        )
                else:
                    logging.error(
                        f"Retry to start adapter for {command.rtsp_url} failed again"
                    )
                    return

        except FileNotFoundError:
            logging.error("Docker is not installed or not in PATH")
        except subprocess.TimeoutExpired:
            logging.error(
                "Docker command timed out while retrying to run failed adapters"
            )
        except Exception as e:
            logging.exception(
                f"Unexpected error while retrying to run failed adapters: {e}"
            )

    def _watch_sources(self):
        while True:
            time.sleep(general_settings.retry_seconds)
            self._handle_retry()

    def start(self):
        if self._thread is not None:
            raise RuntimeError("HealthService already started")

        self._stop_event.clear()

        self._thread = threading.Thread(
            target=self._watch_sources, name="watch_sources", daemon=True
        )
        self._thread.start()
        logger.info("HealthService started")

    def stop(self, timeout=3):
        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

        logger.info("HealthService stopped")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def __enter__(self) -> "HealthService":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False  # don't suppress exceptions
