from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Literal, Optional, Dict, Any
from datetime import datetime
import re
from urllib.parse import urlparse


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

    config: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Optional additional configuration"
    )

    # --- Validations ---

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

        if self.type == "remove" and self.rtsp_url:
            self.rtsp_url = None

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
