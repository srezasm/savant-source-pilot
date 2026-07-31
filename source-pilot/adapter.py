import logging
import subprocess
from settings import general_settings
from source_command import SourceCommand
from utils import redact_url, OperationResult

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_rtsp_connection(rtsp_url: str) -> OperationResult:
    logger.info(f"Testing RTSP connection to: {redact_url(rtsp_url)}")
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
            logger.info(f"RTSP connection test successful: {rtsp_url}")
            return OperationResult(
                success=True, retry=False, reason="Valid and reachable"
            )
        else:
            return OperationResult(
                success=False,
                retry=True,
                reason="Cannot connect to stream (ffprobe failed)",
            )

    except FileNotFoundError:
        logger.warning("ffprobe not found. Skipping connection test.")
        return OperationResult(
            success=True,
            retry=False,
            reason="Format looks valid (ffprobe not available for testing)",
        )
    except subprocess.TimeoutExpired:
        return OperationResult(
            success=False, retry=True, reason="Connection test timed out"
        )
    except Exception as e:
        return OperationResult(
            success=False, retry=True, reason=f"Connection test error: {str(e)}"
        )


def run_adapter(command: SourceCommand) -> OperationResult:
    if not command.source_id or not command.rtsp_url:
        logger.error("run_adapter: Missing 'source_id' or 'rtsp_url' in command")
        return OperationResult(success=False, retry=False, reason="")

    # RTSP check
    result = check_rtsp_connection(command.rtsp_url)
    if not result.success:
        logger.error(
            f"RTSP check {redact_url(command.rtsp_url)} failed: {result.reason}"
        )
        return OperationResult(success=False, retry=result.retry, reason="")

    adapter_name = general_settings.container_name_prefix + command.source_id
    logger.info(f"Starting adapter {adapter_name} for {redact_url(command.rtsp_url)}")

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
            logger.info(f"Successfully started adapter: {adapter_name}")
            return OperationResult(success=True, retry=False, reason="")
        elif "permission denied" in result.stderr:
            logger.error(
                "Current user doesn't have access to Docker. Run this script in sudo mode or give your user access to Docker."
            )
            return OperationResult(success=False, retry=False, reason="")
        else:
            logger.error(
                f"Failed to start adapter {adapter_name}. Stderr: {result.stderr.strip()}"
            )
            return OperationResult(success=False, retry=True, reason="")

    except FileNotFoundError:
        logger.error("Docker is not installed or not in PATH")
        return OperationResult(success=False, retry=False, reason="")
    except subprocess.TimeoutExpired:
        logger.error("Docker command timed out while starting adapter")
        return OperationResult(success=False, retry=True, reason="")
    except Exception as e:
        logger.exception(f"Unexpected error starting adapter {adapter_name}: {e}")
        return OperationResult(success=False, retry=True, reason="")


def stop_adapter(source_id: str) -> OperationResult:
    if not source_id:
        logger.error("stop_adapter: Missing 'source_id' in command")
        return OperationResult(success=False, retry=False, reason="")

    adapter_name = general_settings.container_name_prefix + source_id
    logger.info(f"Stopping adapter {adapter_name}")

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
            logger.info(f"Successfully stopped adapter: {adapter_name}")
            return OperationResult(success=True, retry=False, reason="")
        else:
            if "No such container" in result.stderr:
                logger.warning(f"Container {adapter_name} was not running")
                return OperationResult(success=True, retry=False, reason="")
            else:
                logger.error(
                    f"Failed to stop adapter {adapter_name}: {result.stderr.strip()}"
                )
                return OperationResult(success=False, retry=True, reason="")

    except FileNotFoundError:
        logger.warning("Docker is not available")
        return OperationResult(success=False, retry=False, reason="")
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout while stopping adapter {adapter_name}")
        return OperationResult(success=False, retry=True, reason="")
    except Exception as e:
        logger.exception(f"Unexpected error stopping adapter {adapter_name}: {e}")
        return OperationResult(success=False, retry=True, reason="")
