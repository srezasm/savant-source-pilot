#!/usr/bin/env bash
#
# stream.sh — stream one or more video files to a MediaMTX RTSP server via ffmpeg.
#
# Usage (flags):
#   ./stream.sh -f movie.mp4
#   ./stream.sh -f movie1.mp4:cam1 -f movie2.mp4:cam2
#   ./stream.sh -f movie.mp4 -p mystream -a rtsp://192.168.1.50:8554 -l -t
#   ./stream.sh                 # interactive mode
#
# Flags:
#   -f FILE[:PATH]   Video file to stream. Optionally give ":PATH" to set the
#                     RTSP path name. Repeatable for multiple videos.
#   -p PATH          Path name for the *next single* -f that has no ":PATH"
#                     (only meaningful with exactly one -f). Ignored if the
#                     file already specifies its own path with ":PATH".
#   -a ADDR          RTSP server base address. Default: rtsp://localhost:8554
#   -l               Loop each video forever (-stream_loop -1).
#   -t               Transcode to H.264/AAC instead of stream-copy.
#   -s               Auto-start mediamtx in the background if not running.
#   -i               Force interactive mode even if flags are given.
#   -h               Show this help.
#
# Examples:
#   ./stream.sh -f big_buck_bunny.mp4                 -> rtsp://localhost:8554/big_buck_bunny
#   ./stream.sh -f a.mp4:cam1 -f b.mp4:cam2 -l -s      -> two concurrent streams, looped, auto-starts mediamtx
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MEDIAMTX_BIN="$SCRIPT_DIR/mediamtx"
MEDIAMTX_CFG="$SCRIPT_DIR/mediamtx.yml"

DEFAULT_ADDR="rtsp://localhost:8554"
ADDR="$DEFAULT_ADDR"
LOOP=false
TRANSCODE=false
AUTOSTART=false
FORCE_INTERACTIVE=false
SINGLE_PATH_OVERRIDE=""

declare -a FILES=()
declare -a PATHS=()
declare -a PIDS=()

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

usage() {
    sed -n '2,29p' "$0" | sed 's/^# \{0,1\}//'
}

err()  { echo "Error: $*" >&2; }
info() { echo "[stream.sh] $*"; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || { err "'$1' is not installed or not in PATH."; exit 1; }
}

path_from_filename() {
    local base
    base="$(basename -- "$1")"
    base="${base%.*}"
    # sanitize: keep alnum, dash, underscore
    base="$(echo "$base" | tr -c 'A-Za-z0-9_-' '_')"
    echo "$base"
}

port_open() {
    # $1 = host, $2 = port
    (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && exec 3>&- 3<&-
}

ensure_mediamtx_running() {
    local host="localhost" port="8554"
    if port_open "$host" "$port"; then
        info "MediaMTX RTSP port $port already open — assuming server is running."
        return 0
    fi

    if [ "$AUTOSTART" = true ]; then
        [ -x "$MEDIAMTX_BIN" ] || { err "mediamtx binary not found/executable at $MEDIAMTX_BIN"; exit 1; }
        info "Starting mediamtx in the background..."
        nohup "$MEDIAMTX_BIN" "$MEDIAMTX_CFG" >"$SCRIPT_DIR/mediamtx.log" 2>&1 &
        disown
        for _ in $(seq 1 20); do
            port_open "$host" "$port" && { info "mediamtx is up."; return 0; }
            sleep 0.5
        done
        err "mediamtx did not come up in time — check $SCRIPT_DIR/mediamtx.log"
        exit 1
    else
        err "MediaMTX doesn't seem to be running on $host:$port."
        err "Start it manually (./mediamtx mediamtx.yml) or re-run with -s to auto-start it."
        exit 1
    fi
}

cleanup() {
    if [ "${#PIDS[@]}" -gt 0 ]; then
        info "Stopping ffmpeg stream(s)..."
        kill "${PIDS[@]}" 2>/dev/null
        wait "${PIDS[@]}" 2>/dev/null
    fi
}
trap cleanup INT TERM

# ---------------------------------------------------------------------------
# interactive prompt
# ---------------------------------------------------------------------------

run_interactive() {
    echo "== MediaMTX RTSP stream setup =="
    read -rp "RTSP base address [$DEFAULT_ADDR]: " a
    ADDR="${a:-$DEFAULT_ADDR}"

    read -rp "How many video files do you want to stream? [1]: " n
    n="${n:-1}"

    for ((i = 1; i <= n; i++)); do
        local f p
        while true; do
            read -rp "  File #$i path: " f
            [ -f "$f" ] && break
            err "File not found: $f"
        done
        read -rp "  Stream/path name for file #$i [$(path_from_filename "$f")]: " p
        p="${p:-$(path_from_filename "$f")}"
        FILES+=("$f")
        PATHS+=("$p")
    done

    read -rp "Loop videos forever? [y/N]: " l
    [[ "$l" =~ ^[Yy]$ ]] && LOOP=true

    read -rp "Transcode to H.264/AAC instead of stream-copy? [y/N]: " t
    [[ "$t" =~ ^[Yy]$ ]] && TRANSCODE=true

    read -rp "Auto-start mediamtx if not already running? [Y/n]: " s
    [[ "$s" =~ ^[Nn]$ ]] || AUTOSTART=true
}

# ---------------------------------------------------------------------------
# arg parsing
# ---------------------------------------------------------------------------

while getopts ":f:p:a:ltsih" opt; do
    case "$opt" in
        f)
            file="${OPTARG%%:*}"
            if [[ "$OPTARG" == *:* ]]; then
                p="${OPTARG#*:}"
            else
                p=""
            fi
            FILES+=("$file")
            PATHS+=("$p")
            ;;
        p) SINGLE_PATH_OVERRIDE="$OPTARG" ;;
        a) ADDR="$OPTARG" ;;
        l) LOOP=true ;;
        t) TRANSCODE=true ;;
        s) AUTOSTART=true ;;
        i) FORCE_INTERACTIVE=true ;;
        h) usage; exit 0 ;;
        \?) err "Unknown option: -$OPTARG"; usage; exit 1 ;;
        :)  err "Option -$OPTARG requires an argument."; usage; exit 1 ;;
    esac
done

require_cmd ffmpeg

if [ "${#FILES[@]}" -eq 0 ] || [ "$FORCE_INTERACTIVE" = true ]; then
    run_interactive
fi

# apply -p override only when there's exactly one file and it had no inline path
if [ -n "$SINGLE_PATH_OVERRIDE" ] && [ "${#FILES[@]}" -eq 1 ] && [ -z "${PATHS[0]}" ]; then
    PATHS[0]="$SINGLE_PATH_OVERRIDE"
fi

# fill in any missing path names from the filename
for i in "${!FILES[@]}"; do
    if [ -z "${PATHS[$i]}" ]; then
        PATHS[$i]="$(path_from_filename "${FILES[$i]}")"
    fi
    [ -f "${FILES[$i]}" ] || { err "File not found: ${FILES[$i]}"; exit 1; }
done

ADDR="${ADDR%/}"

ensure_mediamtx_running

# ---------------------------------------------------------------------------
# launch ffmpeg streams
# ---------------------------------------------------------------------------

info "Base RTSP address: $ADDR"

for i in "${!FILES[@]}"; do
    f="${FILES[$i]}"
    p="${PATHS[$i]}"
    url="$ADDR/$p"

    args=(-re)
    [ "$LOOP" = true ] && args+=(-stream_loop -1)
    args+=(-i "$f")

    if [ "$TRANSCODE" = true ]; then
        args+=(-c:v libx264 -preset veryfast -tune zerolatency -c:a aac)
    else
        args+=(-c copy)
    fi
    args+=(-f rtsp -rtsp_transport tcp "$url")

    info "Streaming '$f' -> $url $([ "$LOOP" = true ] && echo "(looping)")"
    ffmpeg -hide_banner -loglevel warning "${args[@]}" &
    PIDS+=("$!")
done

echo
info "Streaming ${#FILES[@]} file(s). Press Ctrl+C to stop."
for i in "${!PATHS[@]}"; do
    echo "  - ${ADDR}/${PATHS[$i]}   (from ${FILES[$i]})"
done

wait "${PIDS[@]}"