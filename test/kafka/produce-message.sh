#!/usr/bin/env bash
#
# produce-rtsp-command.sh
#
# Interactively (or via flags) build a JSON message and produce it to the
# "rtsp-source-commands" Kafka topic, running inside a Docker container
# named "kafka".
#
# Message format:
# {
#   "type": "add" | "remove" | "update",
#   "source_id": "cam-front",
#   "rtsp_url": "rtsp://...",
#   "timestamp": "2026-01-01T00:00:00Z",
#   "config": { ... }   // optional, left empty for now
# }

set -euo pipefail

CONTAINER_NAME="kafka"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC="rtsp-source-commands"

TYPE=""
SOURCE_ID=""
RTSP_URL=""
TIMESTAMP=""
CONFIG=""
RANDOM_MODE=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  -t, --type <add|remove|update>   Message type
  -s, --source-id <id>             Source ID (e.g. cam-front)
  -u, --rtsp-url <url>             RTSP URL (e.g. rtsp://host:554/stream)
      --timestamp <ISO8601>        Timestamp (defaults to "now" if omitted)
  -c, --config <json>              Optional config object as a raw JSON string, e.g. '{"fps":15}'
  -r, --random                     Fill unspecified fields with random values
  -h, --help                       Show this help message

Any field not provided via flags (and not filled by --random) will be
prompted for interactively.

Examples:
  $(basename "$0") -t add -s cam-front -u rtsp://192.168.1.10:554/stream1
  $(basename "$0") --random
  $(basename "$0") -t update -s cam-back -r
EOF
}

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--type)       TYPE="$2"; shift 2 ;;
    -s|--source-id)  SOURCE_ID="$2"; shift 2 ;;
    -u|--rtsp-url)   RTSP_URL="$2"; shift 2 ;;
    --timestamp)     TIMESTAMP="$2"; shift 2 ;;
    -c|--config)     CONFIG="$2"; shift 2 ;;
    -r|--random)     RANDOM_MODE=true; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

# ---------- helpers ----------

random_choice() {
  # $1: space-separated list of choices
  local arr=($1)
  echo "${arr[$((RANDOM % ${#arr[@]}))]}"
}

random_source_id() {
  local names=("cam-front" "cam-back" "cam-side" "cam-gate" "cam-lobby" "cam-yard")
  local base
  base=$(random_choice "${names[*]}")
  echo "${base}-$((RANDOM % 900 + 100))"
}

random_rtsp_url() {
  local ip="192.168.$((RANDOM % 255)).$((RANDOM % 255))"
  local port=554
  local stream="stream$((RANDOM % 4 + 1))"
  echo "rtsp://${ip}:${port}/${stream}"
}

random_timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

now_timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

prompt() {
  # $1: prompt text  $2: default (optional)
  local text="$1" default="${2:-}" reply
  if [[ -n "$default" ]]; then
    read -r -p "$text [$default]: " reply
    echo "${reply:-$default}"
  else
    read -r -p "$text: " reply
    echo "$reply"
  fi
}

# ---------- fill fields ----------

if [[ "$RANDOM_MODE" == true ]]; then
  [[ -z "$TYPE" ]]      && TYPE=$(random_choice "add remove update")
  [[ -z "$SOURCE_ID" ]] && SOURCE_ID=$(random_source_id)
  [[ -z "$RTSP_URL" ]]  && RTSP_URL=$(random_rtsp_url)
  [[ -z "$TIMESTAMP" ]] && TIMESTAMP=$(random_timestamp)
else
  [[ -z "$TYPE" ]]      && TYPE=$(prompt "Type (add/remove/update)" "add")
  [[ -z "$SOURCE_ID" ]] && SOURCE_ID=$(prompt "Source ID" "cam-front")
  [[ -z "$RTSP_URL" ]]  && RTSP_URL=$(prompt "RTSP URL" "rtsp://192.168.1.10:554/stream1")
  [[ -z "$TIMESTAMP" ]] && TIMESTAMP=$(prompt "Timestamp (ISO8601)" "$(now_timestamp)")
fi

# validate type
case "$TYPE" in
  add|remove|update) ;;
  *)
    echo "Error: --type must be one of: add, remove, update (got: '$TYPE')" >&2
    exit 1
    ;;
esac

# ---------- build JSON ----------

if command -v jq >/dev/null 2>&1; then
  if [[ -n "$CONFIG" ]]; then
    # validate config is valid JSON
    if ! echo "$CONFIG" | jq empty >/dev/null 2>&1; then
      echo "Error: --config is not valid JSON" >&2
      exit 1
    fi
    MESSAGE=$(jq -nc \
      --arg type "$TYPE" \
      --arg source_id "$SOURCE_ID" \
      --arg rtsp_url "$RTSP_URL" \
      --arg timestamp "$TIMESTAMP" \
      --argjson config "$CONFIG" \
      '{type: $type, source_id: $source_id, rtsp_url: $rtsp_url, timestamp: $timestamp, config: $config}')
  else
    MESSAGE=$(jq -nc \
      --arg type "$TYPE" \
      --arg source_id "$SOURCE_ID" \
      --arg rtsp_url "$RTSP_URL" \
      --arg timestamp "$TIMESTAMP" \
      '{type: $type, source_id: $source_id, rtsp_url: $rtsp_url, timestamp: $timestamp, config: {}}')
  fi
else
  # fallback: manual JSON construction (no jq available)
  CONFIG_JSON="${CONFIG:-{\}}"
  MESSAGE=$(cat <<EOF
{"type":"${TYPE}","source_id":"${SOURCE_ID}","rtsp_url":"${RTSP_URL}","timestamp":"${TIMESTAMP}","config":${CONFIG_JSON}}
EOF
)
fi

echo "----------------------------------------"
echo "Producing to topic '${TOPIC}' via container '${CONTAINER_NAME}':"
echo "$MESSAGE"
echo "----------------------------------------"

# ---------- produce ----------
#
# rtsp-source-commands is a compacted topic, so every record needs a
# non-null key (compaction dedupes by key). We use "type" as the key,
# separated from the value by a tab, and tell the producer to parse it.

printf '%s\t%s\n' "$TYPE" "$MESSAGE" | docker exec -i "$CONTAINER_NAME" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC" \
  --property "parse.key=true" \
  --property "key.separator=$(printf '\t')"

echo "Message sent (key: ${TYPE})."