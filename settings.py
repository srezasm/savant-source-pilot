"""
Default settings, loaded from a .env file (or real
environment variables, which take precedence over .env).
"""

from pydantic import Field
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class AdapterSettings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="ADAPTER_",
        extra="ignore",
    )

    image: str = "ghcr.io/insight-platform/savant-adapters-gstreamer:0.6.0"
    entrypoint: str = "/opt/savant/adapters/gst/sources/rtsp.sh"
    network: str = "host"
    zmq_endpoint: str = "dealer+connect:ipc:///tmp/zmq-sockets/input-video.ipc"
    # comma-separated in .env, e.g. ADAPTER_VOLUMES=/tmp/zmq-sockets:/tmp/zmq-sockets,/etc/foo:/etc/foo
    # Kept as a raw string (not List[str]) so pydantic-settings doesn't try
    # to JSON-decode it; split into a list via the `volumes` property below.
    volumes_raw: str = Field(
        default="/tmp/zmq-sockets:/tmp/zmq-sockets", alias="ADAPTER_VOLUMES"
    )

    @property
    def volumes(self) -> List[str]:
        return [item.strip() for item in self.volumes_raw.split(",") if item.strip()]


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="KAFKA_", extra="ignore"
    )

    commands_topic: str = "rtsp-source-commands"
    bootstrap_server: str = "localhost:29092"
    group_id: str = "source-management"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="REDIS_", extra="ignore"
    )

    host: str = "localhost"
    port: int = 6379


class GeneralSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    state_file: str = "active_sources.json"
    retry_seconds: int = 5
    container_name_prefix: str = "source-rtsp-"


adapter_settings = AdapterSettings()
kafka_settings = KafkaSettings()
redis_settings = RedisSettings()
general_settings = GeneralSettings()
