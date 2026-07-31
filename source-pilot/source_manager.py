import logging
from storage import SourceStore
from settings import kafka_settings
from kafka_service import KafkaService
from source_command import SourceCommand
from utils import redact_url, gen_stat_msg
from adapter import run_adapter, stop_adapter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def add_sources(
    command: SourceCommand, storage: SourceStore, kafka_service: KafkaService
):
    if storage.exists(command.source_id):
        logger.warning(f"RTSP id '{command.source_id}' already exists")
        return

    logger.info(f"Adding source: {command.source_id} -> {redact_url(command.rtsp_url)}")

    result = run_adapter(command)
    if result.success or result.retry:
        storage.add(command)

        if result.success:
            logger.info(
                f"Successfully added source {command.source_id}"
            )
            kafka_service.produce(
                kafka_settings.status_topic, gen_stat_msg("active"), command.source_id
            )
        else:
            logger.info(
                f"Unable to add the new source {command.source_id}. Will retry later."
            )
            kafka_service.produce(
                kafka_settings.status_topic, gen_stat_msg("faulted"), command.source_id
            )
    else:
        logger.error(f"Failed to start adapter for {redact_url(command.rtsp_url)}")
        kafka_service.produce(
            kafka_settings.status_topic, gen_stat_msg("aborted"), command.source_id
        )
        return


def remove_sources(
    command: SourceCommand, storage: SourceStore, kafka_service: KafkaService
):
    if not storage.exists(command.source_id):
        logger.warning(f"RTSP id '{command.source_id}' does not exist")
        return

    logger.info(f"Removing source: {command.source_id}")

    result = stop_adapter(command.source_id)
    if result.success or result.retry:
        storage.delete(command.source_id)

        if result.success:
            logger.info(
                f"Successfully removed source {command.source_id}"
            )
            kafka_service.produce(
                kafka_settings.status_topic,
                gen_stat_msg("terminated"),
                command.source_id,
            )
        else:
            logger.info(
                f"Unable to remove the source {command.source_id}. Will retry later."
            )
            kafka_service.produce(
                kafka_settings.status_topic, gen_stat_msg("draining"), command.source_id
            )
    else:
        # * This case should not happen, because all of the edge cases should have been
        # * resolved when the stream was getting started.
        logger.error(f"Failed to stop adapter for source with id={command.source_id}")
        return
