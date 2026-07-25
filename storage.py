import redis
from settings import redis_settings
from source_command import SourceCommand
import logging

logger = logging.getLogger(__name__)

client = redis.Redis(
    host=redis_settings.host,
    port=redis_settings.port,
    decode_responses=True,  # Automatically decode bytes to str
    socket_timeout=5,
    socket_connect_timeout=5,
    retry_on_timeout=True,
    max_connections=10,
)


def add(command: SourceCommand):
    client.set(f"sources:active:{command.source_id}", command.model_dump_json())


def delete(source_id: str):
    client.delete(f"sources:active:{source_id}")


def exists(source_id: str) -> bool:
    return client.exists(f"sources:active:{source_id}") == 1


def list_ids() -> list[str]:
    keys = client.keys("sources:active:*")
    return [key.removeprefix("sources:active:") for key in keys]


def get(source_id: str) -> SourceCommand | None:
    command_json = client.get(f"sources:active:{source_id}")
    if not command_json:
        return None
    return SourceCommand.model_validate_json(command_json)


def close():
    if client:
        try:
            client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {e}")
