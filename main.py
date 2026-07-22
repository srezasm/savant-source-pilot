import json
import time
import logging
import subprocess
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaConnectionError, NoBrokersAvailable

READ_KAFKA_TOPIC_FROM_BEGINNING = True

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

change_event = threading.Event()
dict_lock = threading.Lock()
active_sources = {}


def add_sources(message):
    global active_sources

    print(f"Adding source: {message}")
    rtsp_id = message.get("id")
    rtsp_url = message.get("rtsp")

    if rtsp_id is None or rtsp_url is None:
        print("Add message must contain id and rtsp keys")
        return
    with dict_lock:
        if rtsp_id in active_sources:
            print("RTSP id already exists")
            return

    with dict_lock:
        active_sources[rtsp_id] = rtsp_url
    change_event.set()
    run_adapter(message)

    print(f"Current active sources:\n{active_sources}")


def remove_sources(message):
    global active_sources

    print(f"Removing source: {message}")
    rtsp_id = message.get("id")

    if rtsp_id is None:
        print("Remove message must contain id key")
        return
    with dict_lock:
        if rtsp_id not in active_sources:
            print("id does not exist in active sources")
            return

    with dict_lock:
        active_sources.pop(rtsp_id)
    change_event.set()
    stop_adapter(message)

    print(f"Current active sources:\n{active_sources}")


# checks:
# - docker available
# - image exists
# - rtsp is valid
# - rtsp is running
# - ...
def run_adapter(message: dict):
    rtsp_id = message.get("id")
    rtsp_url = message.get("rtsp")
    adapter_name = f"source-rtsp-{rtsp_id}"
    print("running docker")

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            adapter_name,
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
        ]
    )


def stop_adapter(message: dict):
    rtsp_id = message.get("id")
    adapter_name = f"source-rtsp-{rtsp_id}"
    print("stopping docker")

    subprocess.run(
        [
            "docker",
            "stop",
            adapter_name,
        ]
    )


def watch_kafka():
    consumer = None
    retry_delay = 5  # seconds

    while True:
        try:
            consumer = KafkaConsumer(
                "sources",
                bootstrap_servers=["localhost:29092"],
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
            if "sources" not in topics:
                logging.warning(f"Topic 'sources' does not exist yet. Waiting...")
                time.sleep(10)
                consumer.close()
                continue  # Try again
            logging.info(f"Topic 'sources' found. Available topics: {sorted(topics)}")

            # Assign partition to consumer
            consumer.poll(timeout_ms=1000)
            if READ_KAFKA_TOPIC_FROM_BEGINNING:
                consumer.seek_to_beginning()
                logging.info("Reset to beginning of topic")

            logging.info("Kafka consumer connected successfully")

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


def watch_sources():
    global active_sources

    while True:
        change_event.wait()

        with dict_lock:
            pass

        change_event.clear()


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
