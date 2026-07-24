from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal, Optional, Dict, Any, List
from datetime import datetime
import re
from urllib.parse import urlparse


class AdapterSettings(BaseSettings):
    """
    Default adapter/docker settings, loaded from a .env file (or real
    environment variables, which take precedence over .env).

    These are the *defaults* used when a SourceCommand doesn't override
    them via its `adapter` field.
    """

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


# Loaded once at import time; reused as the default source for AdapterConfig.
adapter_settings = AdapterSettings()


class AdapterConfig(BaseModel):
    """Configuration for the adapter's docker container itself."""

    model_config = ConfigDict(extra="forbid")

    image: str = Field(
        default_factory=lambda: adapter_settings.image,
        description="Docker image to run",
    )
    entrypoint: str = Field(
        default_factory=lambda: adapter_settings.entrypoint,
        description="Container entrypoint",
    )
    network: str = Field(
        default_factory=lambda: adapter_settings.network,
        description="Docker network mode",
    )
    zmq_endpoint: str = Field(
        default_factory=lambda: adapter_settings.zmq_endpoint,
        description="ZMQ_ENDPOINT env var value",
    )
    volumes: List[str] = Field(
        default_factory=lambda: list(adapter_settings.volumes),
        description="Bind mounts, each as 'host_path:container_path[:mode]'",
    )
    extra_env: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional environment variables to pass to the container "
        "(merged with ZMQ_ENDPOINT, SOURCE_ID, RTSP_URI)",
    )
    extra_args: List[str] = Field(
        default_factory=list,
        description="Additional raw arguments appended to the docker run command",
    )

    @field_validator("volumes")
    @classmethod
    def validate_volumes(cls, v: List[str]) -> List[str]:
        for vol in v:
            parts = vol.split(":")
            if len(parts) < 2 or not all(parts[:2]):
                raise ValueError(
                    f"Invalid volume spec '{vol}', expected 'host_path:container_path[:mode]'"
                )
        return v

    @field_validator("extra_env")
    @classmethod
    def validate_extra_env(cls, v: Dict[str, str]) -> Dict[str, str]:
        reserved = {"ZMQ_ENDPOINT", "SOURCE_ID", "RTSP_URI"}
        conflicts = reserved & v.keys()
        if conflicts:
            raise ValueError(
                f"extra_env cannot override reserved variables: {sorted(conflicts)}"
            )
        for key in v:
            # Should not start with a number
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                raise ValueError(f"Invalid environment variable name: '{key}'")
        return v


class SourceCommand(BaseModel):
    """Pydantic model for messages in the source command topic"""

    model_config = ConfigDict(extra="allow")

    type: Literal["add", "remove", "update"] = Field(
        ..., description="Type of operation"
    )

    source_id: str = Field(
        ...,
        description="Unique identifier for the source",
    )

    rtsp_url: Optional[str] = Field(
        None, description="RTSP URL (required for add and update)"
    )

    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When this command was created",
    )

    adapter: Optional[AdapterConfig] = Field(
        default_factory=AdapterConfig,
        description="Docker/adapter-level configuration (image, network, "
        "volumes, extra env vars, etc.). Only relevant for 'add'/'update'; "
        "defaults to values from .env if not overridden.",
    )

    config: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Optional additional configuration"
    )

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
            raise ValueError(
                "source_id can only contain letters, numbers, underscores, dots, and hyphens"
            )
        return v.strip()

    @model_validator(mode="after")
    def validate_rtsp_url_for_type(self):
        if self.type in ("add", "update") and not self.rtsp_url:
            raise ValueError(f"rtsp_url is required when type is '{self.type}'")

        if self.type == "remove":
            if self.rtsp_url:
                self.rtsp_url = None
            if self.adapter is not None:
                raise ValueError(
                    "adapter config is not applicable when type is 'remove'"
                )

        return self

    @field_validator("rtsp_url")
    @classmethod
    def validate_rtsp_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None

        v = v.strip()
        if not v.startswith("rtsp://"):
            raise ValueError("RTSP URL must start with 'rtsp://'")

        parsed = urlparse(v)
        if not parsed.netloc:
            raise ValueError("Missing host in RTSP URL")

        return v
