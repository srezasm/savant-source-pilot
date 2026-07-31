import json
import signal
import logging
import threading
from functools import partial
from contextlib import ExitStack
from utils import *
from settings import *
from storage import SourceStore
from kafka_service import KafkaService
from health_service import HealthService
from source_command import SourceCommand
from adapter import run_adapter, stop_adapter
from source_manager import add_sources, remove_sources

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def on_message(record, service: KafkaService, storage: SourceStore):
    try:
        command = SourceCommand.model_validate_json(record.value)

        if command.type == "add":
            add_sources(command, storage, service)
        elif command.type == "remove":
            remove_sources(command, storage, service)
        else:
            logger.warning(f"Unknown key type: {record.key}")

    except json.JSONDecodeError as e:
        logger.error(f"Malformed JSON in message: {record.offset}: {e}")
    except ValueError as e:
        logger.error(f"Invalid message at offset {record.offset}: {e}")
    except Exception as e:
        logger.exception(f"Error processing message: {record.offset}")


def main():
    shutdown_event = threading.Event()

    source_store = SourceStore(redis_settings.host, redis_settings.port)
    kafka_service = KafkaService(
        consume_topics=[kafka_settings.commands_topic],
        produce_topics=[kafka_settings.status_topic],
        bootstrap_servers=kafka_settings.bootstrap_server,
        group_id=kafka_settings.group_id,
        on_message=partial(on_message, storage=source_store),
    )
    health_service = HealthService(
        retry_seconds=general_settings.retry_seconds,
        container_name_prefix=general_settings.container_name_prefix,
        storage=source_store,
        kafka_service=kafka_service,
        run_source=run_adapter,
        remove_source=stop_adapter,
    )

    def handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    with ExitStack() as stack:
        stack.enter_context(source_store)
        stack.enter_context(kafka_service)
        stack.enter_context(health_service)

        logger.info("Services started. Press Ctrl+C to stop.")

        while not shutdown_event.is_set():
            if not kafka_service.is_running() or not health_service.is_running():
                logger.error("A service died unexpectedly, shutting down...")
                break

            shutdown_event.wait(timeout=0.5)

    logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
