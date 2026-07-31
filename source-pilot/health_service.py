import time
import logging
import threading
import subprocess
from typing import Callable, Optional
from storage import SourceStore
from settings import kafka_settings
from kafka_service import KafkaService
from source_command import SourceCommand
from utils import gen_stat_msg, OperationResult

logger = logging.getLogger(__name__)


class HealthService:
    def __init__(
        self,
        retry_seconds: int,
        container_name_prefix: str,
        storage: SourceStore,
        kafka_service: KafkaService,
        run_source: Callable[[SourceCommand], OperationResult],
        remove_source: Callable[[str], OperationResult],
    ):
        self.retry_seconds = retry_seconds
        self.container_name_prefix = container_name_prefix
        self.storage = storage
        self.run_source = run_source
        self.remove_source = remove_source
        self.kafka_service = kafka_service

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
                logger.error(
                    f"Error while getting list of running adapters: {result.stderr}"
                )
                return

            containers = result.stdout.strip().split("\n")
            running_adapter_ids = {
                c.removeprefix(self.container_name_prefix)
                for c in containers
                if c.startswith(self.container_name_prefix)
            }
            active_source_ids = set(self.storage.list_ids())

            # Remove untracked containers that are running but not in storage
            untracked_source_ids = running_adapter_ids - active_source_ids
            for src_id in untracked_source_ids:
                result = self.remove_source(src_id)
                if result.success:
                    logger.info(f"Successfully removed source {src_id}")
                    self.kafka_service.produce(
                        kafka_settings.status_topic, gen_stat_msg("terminated"), src_id
                    )
                elif result.retry:
                    logger.info(
                        f"Unable to remove the source {src_id}. Will retry later."
                    )
                    self.kafka_service.produce(
                        kafka_settings.status_topic, gen_stat_msg("draining"), src_id
                    )
                else:
                    logger.error(f"Failed to stop adapter for source with id={src_id}")

            # Retry starting adapters that are in storage but not running
            retry_source_ids = active_source_ids - running_adapter_ids
            sources_to_retry = {
                src_id: self.storage.get(src_id) for src_id in retry_source_ids
            }
            for src_id, command in sources_to_retry.items():
                result = self.run_source(command)
                if result.success:
                    logger.info(
                        f"Successfully added source {src_id} in retry"
                    )
                    self.kafka_service.produce(
                        kafka_settings.status_topic,
                        gen_stat_msg("recovered"),
                        command.source_id,
                    )
                elif result.retry:
                    self.kafka_service.produce(
                        kafka_settings.status_topic,
                        gen_stat_msg("stalled"),
                        command.source_id,
                    )
                    logger.info(
                        f"Retry to start adapter for {src_id} failed, but will retry again later"
                    )
                else:
                    self.kafka_service.produce(
                        kafka_settings.status_topic,
                        gen_stat_msg("aborted"),
                        command.source_id,
                    )
                    logger.error(
                        f"Retry to start adapter for {src_id} failed, and wont retry again later"
                    )

        except FileNotFoundError:
            logger.error("Docker is not installed or not in PATH")
        except subprocess.TimeoutExpired:
            logger.error(
                "Docker command timed out while retrying to run failed adapters"
            )
        except Exception as e:
            logger.exception(
                f"Unexpected error while retrying to run failed adapters: {e}"
            )

    def _watch_sources(self):
        while not self._stop_event.wait(self.retry_seconds):
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
