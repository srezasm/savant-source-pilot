import time
import logging
import threading
import subprocess
from typing import Callable, Optional
from storage import SourceStore
from source_command import SourceCommand

logger = logging.getLogger(__name__)


class HealthService:
    def __init__(
        self,
        retry_seconds: int,
        container_name_prefix: str,
        storage: SourceStore,
        run_source: Callable[[SourceCommand], list[bool]],
    ):
        self.retry_seconds = retry_seconds
        self.container_name_prefix = container_name_prefix
        self.storage = storage
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
                    c.removeprefix(self.container_name_prefix)
                    for c in containers
                    if c.startswith(self.container_name_prefix)
                ]
            )
            active_source_ids = set(self.storage.list_ids())

            sources_needing_retry = active_source_ids - running_adapter_ids
            if len(untracked := running_adapter_ids - active_source_ids):

                logging.critical(
                    f"There are untracked adapters running:"
                    f"{[self.container_name_prefix + u for u in untracked]}"
                )

            sources_to_retry = {
                src_id: self.storage.get(src_id) for src_id in sources_needing_retry
            }

            for src_id, command in sources_to_retry.items():
                if command is None:
                    logging.warning(f"No persisted command found for source '{src_id}'")
                    continue

                success, retry = self.run_source(command)
                if success or retry:
                    if success:
                        logging.info(
                            f"Successfully added source {src_id} in retry. Active sources: {list(active_source_ids)}"
                        )
                    else:
                        logging.info(
                            f"Unable to add the new source {src_id}. Will retry again later."
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
            time.sleep(self.retry_seconds)
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
