import json
import logging
import threading
from typing import Callable, Optional
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient

logger = logging.getLogger(__name__)


class TopicsNotFoundError(Exception):
    """Raised at startup when one or more required topics don't exist on the broker."""


def _value_serializer(v):
    if isinstance(v, bytes):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v).encode("utf-8")
    return str(v).encode("utf-8")


def _key_serializer(k):
    return k if k is None or isinstance(k, bytes) else str(k).encode("utf-8")


class KafkaService:
    def __init__(
        self,
        consume_topics: list[str],
        produce_topics: list[str],
        bootstrap_servers: list[str] | str,
        group_id: str,
        on_message: Callable[[object, "KafkaService"], None],
        verify_topics: bool = True,
    ):
        self.consume_topics = consume_topics
        self.produce_topics = produce_topics
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.on_message = on_message
        self.verify_topics = verify_topics

        self._producer: Optional[KafkaProducer] = None
        self._consumer: Optional[KafkaConsumer] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _build_producer(self) -> KafkaProducer:
        logger.info(
            f"Creating KafkaProducer (bootstrap_servers={self.bootstrap_servers})"
        )
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            retries=5,
            linger_ms=10,
            value_serializer=_value_serializer,
            key_serializer=_key_serializer,
        )

    def _build_consumer(self) -> KafkaConsumer:
        logger.info(
            "Creating KafkaConsumer (bootstrap_servers=%s, topics=%s, group_id=%s)",
            self.bootstrap_servers,
            self.consume_topics,
            self.group_id,
        )
        return KafkaConsumer(
            *self.consume_topics,
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            group_id=self.group_id,
            consumer_timeout_ms=1000,
            session_timeout_ms=30000,  # max time between heartbeats
            request_timeout_ms=40000,  # max waiting time for response from broker
            max_poll_interval_ms=60000,  # max time between two polls(processing messages)
            value_deserializer=lambda v: (v.decode("utf-8") if v is not None else None),
            key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        )

    def _verify_topics(self):
        """
        Check that every required topic (input + output) exists on the
        broker before starting anything. Raises TopicsNotFoundError if any
        are missing — fail fast at startup instead of silently consuming
        nothing (missing input topic) or silently failing every produce
        call later (missing output topic).
        """
        required = set(self.consume_topics) | set(self.produce_topics)
        if not required:
            return

        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            existing = set(admin.list_topics())
        finally:
            admin.close()

        missing = required - existing
        if missing:
            raise TopicsNotFoundError(
                f"Required Kafka topic(s) not found on broker: {sorted(missing)}. "
                f"Create them first, or pass verify_topics=False to skip this check "
                f"(e.g. if your broker auto-creates topics on first use)."
            )

        logger.info("Verified topics exist: %s", sorted(required))

    def produce(self, topic: str, value, key=None, headers: Optional[list] = None):
        """
        Async send with delivery callbacks attached.
        Call this after .start()
        """
        if self._producer is None:
            raise RuntimeError("KafkaService.start() must be called before produce()")

        future = self._producer.send(topic, value, key, headers)
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)
        return future

    @staticmethod
    def _on_send_success(record_metadata):
        logger.debug(
            "Delivered to %s[%d]@%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    @staticmethod
    def _on_send_error(excp):
        logger.error("Failed to deliver message", exc_info=excp)

    def _watch_kafka(self):
        while not self._stop_event.is_set():
            try:
                for record in self._consumer:
                    if self._stop_event.is_set():
                        break

                    logger.debug(
                        "Consumed %s[%d]@%d key=%s",
                        record.topic,
                        record.partition,
                        record.offset,
                        record.key,
                    )

                    try:
                        self.on_message(record, self)
                    except Exception:
                        logger.exception(
                            "on_message callback raised for key=%s", record.key
                        )

            except Exception:
                logger.exception("Error in watch_kafka loop, continuing")

        logger.info("watch_kafka stopping (stop_event set)")

    def start(self):
        if self._thread is not None:
            raise RuntimeError("KafkaService already started")

        if self.verify_topics:
            self._verify_topics()

        self._producer = self._build_producer()
        self._consumer = self._build_consumer()
        self._stop_event.clear()

        self._thread = threading.Thread(
            target=self._watch_kafka, name="watch_kafka", daemon=True
        )
        self._thread.start()
        logger.info("KafkaService started")

    def stop(self, timeout=3):
        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

        if self._producer is not None:
            logger.info("Flushing producer...")
            self._producer.close(timeout=timeout)
            self._producer = None

        if self._consumer is not None:
            logger.info("Closing consumer")
            self._consumer.close()
            self._consumer = None

        logger.info("KafkaService stopped")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def __enter__(self) -> "KafkaService":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False  # don't suppress exceptions
