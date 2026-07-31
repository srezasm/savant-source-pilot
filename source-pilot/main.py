import json
import signal
import logging
import threading
import subprocess
from functools import partial
from contextlib import ExitStack
from utils import *
from settings import *
from utils import gen_stat_msg
from storage import SourceStore
from kafka_service import KafkaService
from health_service import HealthService
from source_command import SourceCommand

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_rtsp_connection(rtsp_url: str) -> OperationResult:
    logger.info(f"Testing RTSP connection to: {redact_url(rtsp_url)}")
    try:
        # Use ffprobe (from ffmpeg) to test the stream
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "quiet",
                "-rtsp_transport",
                "tcp",
                "-i",
                rtsp_url,
            ],
            capture_output=True,
            timeout=10,
        )

        if result.returncode == 0:
            logger.info(f"RTSP connection test successful: {rtsp_url}")
            return OperationResult(
                success=True, retry=False, reason="Valid and reachable"
            )
        else:
            return OperationResult(
                success=False,
                retry=True,
                reason="Cannot connect to stream (ffprobe failed)",
            )

    except FileNotFoundError:
        logger.warning("ffprobe not found. Skipping connection test.")
        return OperationResult(
            success=True,
            retry=False,
            reason="Format looks valid (ffprobe not available for testing)",
        )
    except subprocess.TimeoutExpired:
        return OperationResult(
            success=False, retry=True, reason="Connection test timed out"
        )
    except Exception as e:
        return OperationResult(
            success=False, retry=True, reason=f"Connection test error: {str(e)}"
        )


def add_sources(
    command: SourceCommand, storage: SourceStore, kafka_service: KafkaService
):
    if storage.exists(command.source_id):
        logger.warning(f"RTSP id '{command.source_id}' already exists")
        return

    logger.info(
        f"Adding source: {command.source_id} -> {redact_url(command.rtsp_url)}"
    )

    result = run_adapter(command)
    if result.success or result.retry:
        storage.add(command)

        if result.success:
            logger.info(
                f"Successfully added source {command.source_id}. Active sources: {storage.list_ids()}"
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
                f"Successfully removed source {command.source_id}. Active sources: {storage.list_ids()}"
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


def run_adapter(command: SourceCommand) -> OperationResult:
    if not command.source_id or not command.rtsp_url:
        logger.error("run_adapter: Missing 'source_id' or 'rtsp_url' in command")
        return OperationResult(success=False, retry=False, reason="")

    # RTSP check
    result = check_rtsp_connection(command.rtsp_url)
    if not result.success:
        logger.error(
            f"RTSP check {redact_url(command.rtsp_url)} failed: {result.reason}"
        )
        return OperationResult(success=False, retry=result.retry, reason="")

    adapter_name = general_settings.container_name_prefix + command.source_id
    logger.info(f"Starting adapter {adapter_name} for {redact_url(command.rtsp_url)}")

    try:
        # Verify docker exists
        subprocess.run(
            ["docker", "--version"], check=True, capture_output=True, timeout=5
        )

        # Remove container in case of previous failure
        subprocess.run(
            ["docker", "rm", "-f", adapter_name],
            check=False,
            capture_output=True,
            timeout=5,
        )

        adapter = command.adapter
        env_vars = {
            "ZMQ_ENDPOINT": adapter.zmq_endpoint,
            "SOURCE_ID": command.source_id,
            "RTSP_URI": command.rtsp_url,
            **adapter.extra_env,
        }

        # Construct docker command
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            adapter_name,
            "--network",
            adapter.network,
            "--entrypoint",
            adapter.entrypoint,
        ]
        # Add environment variables
        for key, value in env_vars.items():
            docker_cmd += ["-e", f"{key}={value}"]
        # Add volumes
        for volume in adapter.volumes:
            docker_cmd += ["-v", volume]
        # Add possible extra arguments
        docker_cmd += adapter.extra_args
        # Add adapter image
        docker_cmd.append(adapter.image)

        # Run docker command and capture the result
        result = subprocess.run(
            docker_cmd,
            capture_output=True,
            timeout=30,
            text=True,
        )

        if result.returncode == 0:
            logger.info(f"Successfully started adapter: {adapter_name}")
            return OperationResult(success=True, retry=False, reason="")
        elif "permission denied" in result.stderr:
            logger.error(
                "Current user doesn't have access to Docker. Run this script in sudo mode or give your user access to Docker."
            )
            return OperationResult(success=False, retry=False, reason="")
        else:
            logger.error(
                f"Failed to start adapter {adapter_name}. Stderr: {result.stderr.strip()}"
            )
            return OperationResult(success=False, retry=True, reason="")

    except FileNotFoundError:
        logger.error("Docker is not installed or not in PATH")
        return OperationResult(success=False, retry=False, reason="")
    except subprocess.TimeoutExpired:
        logger.error("Docker command timed out while starting adapter")
        return OperationResult(success=False, retry=True, reason="")
    except Exception as e:
        logger.exception(f"Unexpected error starting adapter {adapter_name}: {e}")
        return OperationResult(success=False, retry=True, reason="")


def stop_adapter(source_id: str) -> OperationResult:
    if not source_id:
        logger.error("stop_adapter: Missing 'source_id' in command")
        return OperationResult(success=False, retry=False, reason="")

    adapter_name = general_settings.container_name_prefix + source_id
    logger.info(f"Stopping adapter {adapter_name}")

    try:
        result = subprocess.run(
            [
                "docker",
                "stop",
                adapter_name,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            logger.info(f"Successfully stopped adapter: {adapter_name}")
            return OperationResult(success=True, retry=False, reason="")
        else:
            if "No such container" in result.stderr:
                logger.warning(f"Container {adapter_name} was not running")
                return OperationResult(success=True, retry=False, reason="")
            else:
                logger.error(
                    f"Failed to stop adapter {adapter_name}: {result.stderr.strip()}"
                )
                return OperationResult(success=False, retry=True, reason="")

    except FileNotFoundError:
        logger.warning("Docker is not available")
        return OperationResult(success=False, retry=False, reason="")
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout while stopping adapter {adapter_name}")
        return OperationResult(success=False, retry=True, reason="")
    except Exception as e:
        logger.exception(f"Unexpected error stopping adapter {adapter_name}: {e}")
        return OperationResult(success=False, retry=True, reason="")


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
