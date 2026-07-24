import json
import time
import logging
import threading
import subprocess
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError, NoBrokersAvailable
from source_command import SourceCommand
from settings import general_settings, kafka_settings
import storage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def check_rtsp_connection(rtsp_url: str) -> tuple[bool, bool, str]:
    logging.info(f"Testing RTSP connection to: {rtsp_url}")
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
            logging.info(f"RTSP connection test successful: {rtsp_url}")
            return True, False, "Valid and reachable"
        else:
            return False, True, f"Cannot connect to stream (ffprobe failed)"

    except FileNotFoundError:
        logging.warning("ffprobe not found. Skipping connection test.")
        return True, False, "Format looks valid (ffprobe not available for testing)"
    except subprocess.TimeoutExpired:
        return False, True, "Connection test timed out"
    except Exception as e:
        return False, True, f"Connection test error: {str(e)}"


def add_sources(command: SourceCommand):
    if storage.exists(command.source_id):
        logging.warning(f"RTSP id '{command.source_id}' already exists")
        return

    logging.info(f"Adding source: {command.source_id} -> {command.rtsp_url}")

    success, retry = run_adapter(command)
    if success or retry:
        storage.add(command)

        if success:
            logging.info(
                f"Successfully added source {command.source_id}. Active sources: {storage.list_ids()}"
            )
        else:
            logging.info(f"Unable to add the new source {command.source_id}. Will retry later.")
    else:
        logging.error(f"Failed to start adapter for {command.rtsp_url}")
        return


def remove_sources(command: SourceCommand):
    if not storage.exists(command.source_id):
        logging.warning(f"RTSP id '{command.source_id}' does not exist")
        return

    logging.info(f"Removing source: {command.source_id}")

    success, retry = stop_adapter(command)
    if success or retry:
        storage.delete(command.source_id)

        if success:
            logging.info(
                f"Successfully removed source {command.source_id}. Active sources: {storage.list_ids()}"
            )
        else:
            logging.info(f"Unable to remove the source {command.source_id}. Will retry later.")
    else:
        logging.error(f"Failed to stop adapter for source with id={command.source_id}")
        return


def run_adapter(command: SourceCommand) -> tuple[bool, bool]:
    if not command.source_id or not command.rtsp_url:
        logging.error("run_adapter: Missing 'source_id' or 'rtsp_url' in command")
        return False, False

    # RTSP check
    is_valid, retry, msg = check_rtsp_connection(command.rtsp_url)
    if not is_valid:
        logging.error(f"RTSP check {command.rtsp_url} failed: {msg}")
        return False, retry

    adapter_name = general_settings.container_name_prefix + command.source_id
    logging.info(f"Starting adapter {adapter_name} for {command.rtsp_url}")

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
            logging.info(f"Successfully started adapter: {adapter_name}")
            return True, False
        elif "permission denied" in result.stderr:
            logging.error(
                f"Current user doesn't have access to Docker. Run this script in sudo mode or give your user access to Docker."
            )
            return False, False
        else:
            logging.error(
                f"Failed to start adapter {adapter_name}. Stderr: {result.stderr.strip()}"
            )
            return False, True

    except FileNotFoundError:
        logging.error("Docker is not installed or not in PATH")
        return False, False
    except subprocess.TimeoutExpired:
        logging.error("Docker command timed out while starting adapter")
        return False, True
    except Exception as e:
        logging.exception(f"Unexpected error starting adapter {adapter_name}: {e}")
        return False, True


def stop_adapter(command: SourceCommand):
    rtsp_id = command.source_id
    if not rtsp_id:
        logging.error("stop_adapter: Missing 'source_id' in command")
        return False, False

    adapter_name = general_settings.container_name_prefix + rtsp_id
    logging.info(f"Stopping adapter {adapter_name}")

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
            logging.info(f"Successfully stopped adapter: {adapter_name}")
            return True
        else:
            if "No such container" in result.stderr:
                logging.warning(f"Container {adapter_name} was not running")
                return True, False
            else:
                logging.error(
                    f"Failed to stop adapter {adapter_name}: {result.stderr.strip()}"
                )
                return False, True

    except FileNotFoundError:
        logging.warning("Docker is not available")
        return False, False
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout while stopping adapter {adapter_name}")
        return False, True
    except Exception as e:
        logging.exception(f"Unexpected error stopping adapter {adapter_name}: {e}")
        return False, True


def watch_kafka():
    consumer = None
    retry_delay = 5  # seconds

    while True:
        try:
            consumer = KafkaConsumer(
                kafka_settings.commands_topic,
                bootstrap_servers=[kafka_settings.bootstrap_server],
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                group_id="source-management",
                session_timeout_ms=30000,  # max time between heartbeats
                request_timeout_ms=40000,  # max waiting time for response from broker
                max_poll_interval_ms=60000,  # max time between two polls(processing messages)
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8"),
            )

            # Check if topic exists
            topics = consumer.topics()
            if kafka_settings.commands_topic not in topics:
                logging.warning(
                    f"Topic '{kafka_settings.commands_topic}' does not exist yet. Waiting..."
                )
                time.sleep(10)
                consumer.close()
                continue  # Try again
            logging.info(
                f"Topic '{kafka_settings.commands_topic}' found. Available topics: {sorted(topics)}"
            )

            for message in consumer:
                try:
                    key = message.key
                    value = message.value
                    command = SourceCommand.model_validate(value)

                    if command.type == "add":
                        add_sources(command)
                    elif command.type == "remove":
                        remove_sources(command)
                    else:
                        logging.warning(f"Unknown key type: {key}")

                    consumer.commit()

                except json.JSONDecodeError as e:
                    logging.error(f"Malformed JSON in message: {message.offset}: {e}")
                    consumer.commit()
                except ValueError as e:
                    logging.error(f"Invalid message at offset {message.offset}: {e}")
                    consumer.commit()  # Skip poison message
                except Exception as e:
                    logging.exception(f"Error processing message: {message.offset}")
                    consumer.commit()  # To avoid infinite loop

        except (KafkaConnectionError, NoBrokersAvailable) as e:
            logging.error(
                f"Kafka connection error: {e}. Reconnecting in {retry_delay}s..."
            )
            time.sleep(retry_delay)
        except Exception as e:
            logging.exception(f"Unexpected error in Kafka consumer: {e}")
            time.sleep(retry_delay)
        finally:
            if consumer:
                consumer.close()
            consumer = None


def handle_retry():
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
                logging.warning(f"No persisted command found for source '{rtsp_id}'")
                continue

            success, retry = run_adapter(command)
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
        logging.error("Docker command timed out while retrying to run failed adapters")
    except Exception as e:
        logging.exception(
            f"Unexpected error while retrying to run failed adapters: {e}"
        )


def watch_sources():
    while True:
        time.sleep(general_settings.retry_seconds)
        handle_retry()


if __name__ == "__main__":
    watch_kafka_thread = threading.Thread(
        target=watch_kafka, name="watch_kafka", daemon=True
    )
    watch_sources_thread = threading.Thread(
        target=watch_sources, name="watch_sources", daemon=True
    )

    watch_kafka_thread.start()
    watch_sources_thread.start()

    try:
        while True:
            watch_kafka_thread.join(timeout=1.0)
            watch_sources_thread.join(timeout=1.0)

            if not watch_kafka_thread.is_alive() or not watch_sources_thread.is_alive():
                logging.error("One of the watcher threads died. Shutting down.")
                break

    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        storage.close()
