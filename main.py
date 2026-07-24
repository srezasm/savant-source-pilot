import json
import time
import logging
import subprocess
import threading
import logging
from urllib.parse import urlparse
import subprocess
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError, NoBrokersAvailable

KAFKA_COMMANDS_TOPIC = "rtsp-source-commands"
KAFKA_BOOTSTRAP_SERVER = "localhost:29092"

STATE_FILE = "active_sources.json"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

change_event = threading.Event()
dict_lock = threading.Lock()
active_sources = {}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            with dict_lock:
                json.dump(active_sources, f, indent=4)
    except Exception as e:
        logging.exception(
            f"Unexpected exception while trying to save final state into {STATE_FILE}: {e}"
        )


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            with dict_lock:
                active_sources = json.load(f)
            logging.info(f"Loaded {len(active_sources)} from {STATE_FILE}")
    except FileNotFoundError:
        logging.info(f"Couldn't locate last state file {STATE_FILE}")
    except Exception as e:
        logging.exception(f"Failed to load last state from file {STATE_FILE}: {e}")


def validate_rtsp_url(
    rtsp_url: str, test_connection: bool = True
) -> tuple[bool, bool, str]:
    """
    Validates RTSP URL. Can also test actual connection if test_connection=True.
    """
    if not rtsp_url or not isinstance(rtsp_url, str):
        return False, False, "RTSP URL is empty or invalid type"

    rtsp_url = rtsp_url.strip()

    if not rtsp_url.startswith("rtsp://"):
        return False, False, "Must start with 'rtsp://'"

    parsed = urlparse(rtsp_url)
    if not parsed.netloc:
        return False, False, "Missing host in URL"

    if test_connection:
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

    return True, "Format looks valid"


def add_sources(message):
    rtsp_id = message.get("id")
    rtsp_url = message.get("rtsp")

    if rtsp_id is None or rtsp_url is None:
        logging.error("Add message must contain 'id' and 'rtsp' keys")
        return

    with dict_lock:
        if rtsp_id in active_sources:
            logging.warning(f"RTSP id '{rtsp_id}' already exists")
            return

    # Validate RTSP
    is_valid, retry, msg = validate_rtsp_url(rtsp_url, test_connection=True)
    if not is_valid:
        logging.error(f"Invalid RTSP URL for {rtsp_id}: {msg}")
        if not retry:
            return

    logging.info(f"Adding source: {rtsp_id} -> {rtsp_url}")

    success, retry = run_adapter(message)
    if success or retry:
        with dict_lock:
            active_sources[rtsp_id] = rtsp_url
        change_event.set()
        save_state()  # Persist active_sources

        if success:
            logging.info(
                f"Successfully added source {rtsp_id}. Active sources: {list(active_sources.keys())}"
            )
        else:
            logging.info(f"Unable to add the new source {rtsp_id}. Will retry later.")
    else:
        logging.error(f"Failed to start adapter for {rtsp_url}")
        return


def remove_sources(message):
    rtsp_id = message.get("id")

    if rtsp_id is None:
        logging.error("Remove message must contain 'id' key")
        return

    with dict_lock:
        if rtsp_id not in active_sources:
            logging.warning(f"RTSP id '{rtsp_id}' does not exist")
            return

    logging.info(f"Removing source: {rtsp_id}")

    success, retry = stop_adapter(message)
    if success or retry:
        with dict_lock:
            active_sources.pop(rtsp_id, None)
        change_event.set()
        save_state()  # Persist active_sources

        if success:
            logging.info(
                f"Successfully removed source {rtsp_id}. Active sources: {list(active_sources.keys())}"
            )
        else:
            logging.info(f"Unable to remove the source {rtsp_id}. Will retry later.")
    else:
        logging.error(f"Failed to stop adapter for rtsp with id={rtsp_id}")
        return


def run_adapter(message: dict) -> tuple[bool, bool]:
    rtsp_id = message.get("id")
    rtsp_url = message.get("rtsp")

    if not rtsp_id or not rtsp_url:
        logging.error("run_adapter: Missing 'id' or 'rtsp' in message")
        return False, False

    adapter_name = f"source-rtsp-{rtsp_id}"
    logging.info(f"Starting adapter for {rtsp_url} with ID: {rtsp_id}")

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

        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                adapter_name,
                "--network",
                "host",
                "--entrypoint",
                "/opt/savant/adapters/gst/sources/rtsp.sh",
                "-e",
                "ZMQ_ENDPOINT=dealer+connect:ipc:///tmp/zmq-sockets/input-video.ipc",
                "-e",
                "SOURCE_ID=test",
                "-e",
                f"RTSP_URI={rtsp_url}",
                "-v",
                "/tmp/zmq-sockets:/tmp/zmq-sockets",
                "ghcr.io/insight-platform/savant-adapters-gstreamer:0.6.0",
            ],
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
        logging.exception(f"Unexpected error starting adapter {adapter_name}")
        return False, True


def stop_adapter(message: dict):
    rtsp_id = message.get("id")
    if not rtsp_id:
        logging.error("stop_adapter: Missing 'id' in message")
        return False, False

    adapter_name = f"source-rtsp-{rtsp_id}"
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
                KAFKA_COMMANDS_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
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
            if KAFKA_COMMANDS_TOPIC not in topics:
                logging.warning(f"Topic '{KAFKA_COMMANDS_TOPIC}' does not exist yet. Waiting...")
                time.sleep(10)
                consumer.close()
                continue  # Try again
            logging.info(f"Topic '{KAFKA_COMMANDS_TOPIC}' found. Available topics: {sorted(topics)}")

            for message in consumer:
                try:
                    key = message.key
                    value = message.value

                    if key == "add":
                        add_sources(value)
                    elif key == "remove":
                        remove_sources(value)
                    else:
                        logging.warning(f"Unknown key type: {key}")

                    consumer.commit()

                except json.JSONDecodeError as e:
                    logging.error(f"Malformed JSON in message: {message.offset}: {e}")
                    consumer.commit()
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
            [c.split("-")[-1] for c in containers if c.startswith("source-rtsp-")]
        )
        with dict_lock:
            rtsp_ids = set(active_sources.keys())

        shutdown_adapters = rtsp_ids - running_adapter_ids
        if len(untracked := running_adapter_ids - rtsp_ids):
            logging.critical(
                f"There are untracked adapters running:"
                f"{['source-rtsp-' + u for u in untracked]}"
            )

        for rtsp_id in shutdown_adapters:
            with dict_lock:
                rtsp_url = active_sources.get(rtsp_id)

            success, retry = run_adapter({"id": rtsp_id, "rtsp": rtsp_url})
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
                logging.error(f"Retry to start adapter for {rtsp_url} failed again")
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
        handle_retry()


if __name__ == "__main__":
    # Load active_sources from last persistent state
    load_state()

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
