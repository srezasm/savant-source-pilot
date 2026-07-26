from typing import Literal
from datetime import datetime
from urllib.parse import urlparse


def gen_stat_msg(
    status: Literal[
        "active", "faulted", "recovered", "stalled", "terminated", "aborted", "draining"
    ],
) -> dict[str, str]:
    """Generates a standardized status message payload for Kafka propagation.

    The status captures whether a stream successfully boots, encounters
    dropouts and recovers, or experiences terminal failures and administrative deletions.

    * active: The stream successfully initialized on the first attempt.
    * faulted: The initial connection attempt failed, but will be retried.
    * recovered: A previously dropped stream was successfully brought back online by the retry loop.
    * stalled: Failed to restore the dropped stream in the retrying attempt.
    * aborted: The stream failed to start due to a unrecoverable issue and will not be retried.
    * terminated: The RTSP source was deliberately deleted.
    * draining: The deliberate removal of the stream failed, but will be retried.

    Args:
        status: The current lifecycle state of the RTSP stream.

    Returns:
        A dictionary containing the status string and the current timestamp.
    """
    return {"status": status, "timestamp": str(datetime.now())}


def redact_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        return url.replace(parsed.netloc, f"***@{parsed.hostname}")
    return url
