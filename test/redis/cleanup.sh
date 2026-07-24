#!/usr/bin/env bash
#
# redis-key-cleaner.sh
#
# Interactively (or via flags) delete keys from a Redis instance running
# in a Docker container (as defined in your docker-compose.yml).
#
# Usage:
#   ./redis-key-cleaner.sh                     # interactive mode: list keys, pick which to delete
#   ./redis-key-cleaner.sh -k session:123       # delete a specific key
#   ./redis-key-cleaner.sh -k a -k b -k c       # delete multiple specific keys
#   ./redis-key-cleaner.sh -p "cache:*"         # interactive mode, filtered by glob pattern
#   ./redis-key-cleaner.sh -a                   # delete ALL keys (FLUSHDB), asks for confirmation
#   ./redis-key-cleaner.sh -a -y                # delete ALL keys, no confirmation prompt
#   ./redis-key-cleaner.sh -c my-redis-ctr -k x # use a different container name
#
# Env vars (optional overrides):
#   REDIS_CONTAINER   Container name (default: redis)
#   REDIS_DB          DB index to select with -n (default: 0)
#   REDIS_AUTH        Password, if your redis requires auth (adds -a "$REDIS_AUTH" --no-auth-warning)

set -euo pipefail

CONTAINER="${REDIS_CONTAINER:-redis}"
DB="${REDIS_DB:-0}"
PATTERN="*"
ALL=false
YES=false
declare -a KEYS_TO_DELETE=()

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
    grep '^#' "$0" | sed -e 's/^#//' -e '1d'
    exit 0
}

# --- arg parsing -------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--key)
            KEYS_TO_DELETE+=("$2")
            shift 2
            ;;
        -a|--all)
            ALL=true
            shift
            ;;
        -p|--pattern)
            PATTERN="$2"
            shift 2
            ;;
        -c|--container)
            CONTAINER="$2"
            shift 2
            ;;
        -y|--yes)
            YES=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown argument: $1${NC}" >&2
            usage
            ;;
    esac
done

# --- redis-cli wrapper ---------------------------------------------------
rcli() {
    if [[ -n "${REDIS_AUTH:-}" ]]; then
        docker exec -i "$CONTAINER" redis-cli -n "$DB" -a "$REDIS_AUTH" --no-auth-warning "$@"
    else
        docker exec -i "$CONTAINER" redis-cli -n "$DB" "$@"
    fi
}

# --- sanity checks ---------------------------------------------------
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo -e "${RED}Container '$CONTAINER' is not running.${NC}" >&2
    exit 1
fi

if ! rcli ping >/dev/null 2>&1; then
    echo -e "${RED}Could not reach redis-cli inside container '$CONTAINER'.${NC}" >&2
    exit 1
fi

confirm() {
    local prompt="$1"
    $YES && return 0
    read -r -p "$(echo -e "${YELLOW}${prompt} [y/N]: ${NC}")" ans
    [[ "$ans" =~ ^[Yy]$ ]]
}

# --- mode: delete all ---------------------------------------------------
if $ALL; then
    count=$(rcli dbsize | tr -d '\r')
    echo -e "${RED}${BOLD}This will delete ALL $count key(s) from DB $DB on container '$CONTAINER'.${NC}"
    if confirm "Are you sure you want to FLUSHDB?"; then
        rcli flushdb
        echo -e "${GREEN}All keys removed.${NC}"
    else
        echo "Aborted."
    fi
    exit 0
fi

# --- mode: specific keys via -k ---------------------------------------------------
if [[ ${#KEYS_TO_DELETE[@]} -gt 0 ]]; then
    echo -e "${BOLD}Keys to delete:${NC}"
    printf '  - %s\n' "${KEYS_TO_DELETE[@]}"
    if confirm "Delete ${#KEYS_TO_DELETE[@]} key(s)?"; then
        rcli del "${KEYS_TO_DELETE[@]}"
        echo -e "${GREEN}Done.${NC}"
    else
        echo "Aborted."
    fi
    exit 0
fi

# --- mode: interactive ---------------------------------------------------
mapfile -t ALL_KEYS < <(rcli --scan --pattern "$PATTERN" | tr -d '\r' | sort)

if [[ ${#ALL_KEYS[@]} -eq 0 ]]; then
    echo -e "${YELLOW}No keys found matching pattern '$PATTERN' in DB $DB.${NC}"
    exit 0
fi

echo -e "${BOLD}Keys in DB $DB (pattern: $PATTERN):${NC}"
for i in "${!ALL_KEYS[@]}"; do
    key="${ALL_KEYS[$i]}"
    type=$(rcli type "$key" | tr -d '\r')
    ttl=$(rcli ttl "$key" | tr -d '\r')
    [[ "$ttl" == "-1" ]] && ttl="no expiry"
    printf "  ${BLUE}%3d${NC}) %-40s [%s, ttl:%s]\n" "$((i+1))" "$key" "$type" "$ttl"
done

echo
echo "Enter numbers to delete (space/comma separated), 'a' for all listed, or 'q' to quit:"
read -r -p "> " selection

[[ "$selection" == "q" || -z "$selection" ]] && { echo "Aborted."; exit 0; }

if [[ "$selection" == "a" ]]; then
    SELECTED=("${ALL_KEYS[@]}")
else
    SELECTED=()
    selection="${selection//,/ }"
    for num in $selection; do
        if ! [[ "$num" =~ ^[0-9]+$ ]] || (( num < 1 || num > ${#ALL_KEYS[@]} )); then
            echo -e "${RED}Invalid selection: $num${NC}" >&2
            exit 1
        fi
        SELECTED+=("${ALL_KEYS[$((num-1))]}")
    done
fi

if [[ ${#SELECTED[@]} -eq 0 ]]; then
    echo "Nothing selected."
    exit 0
fi

echo -e "${BOLD}About to delete:${NC}"
printf '  - %s\n' "${SELECTED[@]}"

if confirm "Delete ${#SELECTED[@]} key(s)?"; then
    rcli del "${SELECTED[@]}"
    echo -e "${GREEN}Done.${NC}"
else
    echo "Aborted."
fi