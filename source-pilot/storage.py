import logging
from typing import Optional
from redis import Redis, RedisError
from source_command import SourceCommand

logger = logging.getLogger(__name__)

ACTIVE_KEY_PREFIX = "sources:active:"


class SourceStore:
    def __init__(self, host: str, port: int):
        self.host: str = host
        self.port: int = port
        self._client: Optional[Redis] = None

    def connect(self):
        if self._client is not None:
            return

        self._client = Redis(
            host=self.host,
            port=self.port,
            decode_responses=True,  # Automatically decode bytes to str
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            max_connections=10,
        )

        logger.info(
            "Connected to Redis (host=%s, port=%s)",
            self.host,
            self.port,
        )

    def close(self):
        if self._client:
            try:
                self._client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis: {e}")
            finally:
                self._client = None

    def __enter__(self) -> "SourceStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False  # don't suppress exceptions

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("SourceStore.connect() must be called before use")
        return self._client

    def add(self, command: SourceCommand) -> None:
        try:
            self._client.set(
                f"{ACTIVE_KEY_PREFIX}{command.source_id}", command.model_dump_json()
            )
        except RedisError as e:
            logger.exception(f"Failed to add source {command.source_id}: {e}")

    def delete(self, source_id: str) -> None:
        try:
            self._client.delete(f"{ACTIVE_KEY_PREFIX}{source_id}")
        except RedisError as e:
            logger.exception(f"Failed to remove source {source_id}: {e}")

    def get(self, source_id: str) -> SourceCommand | None:
        try:
            command_json = self._client.get(f"{ACTIVE_KEY_PREFIX}{source_id}")
        except RedisError as e:
            logger.exception(f"Failed to get source {source_id}: {e}")

        if not command_json:
            return None
        return SourceCommand.model_validate_json(command_json)

    def exists(self, source_id: str) -> bool:
        try:
            return self._client.exists(f"{ACTIVE_KEY_PREFIX}{source_id}") == 1
        except RedisError as e:
            logger.exception(f"Failed to check existence of source {source_id}: {e}")

    def list_ids(self) -> list[str]:
        try:
            return [
                key.removeprefix(ACTIVE_KEY_PREFIX)
                for key in self._client.scan_iter(ACTIVE_KEY_PREFIX + "*")
            ]
        except RedisError as e:
            logger.exception(f"Failed to list source ids: {e}")
