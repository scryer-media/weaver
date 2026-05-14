#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$SCRIPT_DIR/runtime-select.sh"

apply_umask() {
    if [ -n "${UMASK:-}" ]; then
        umask "$UMASK" || {
            printf 'invalid UMASK: %s\n' "$UMASK" >&2
            exit 1
        }
    fi
}

print_modern_cpu_error() {
    arch=$1
    baseline=$2

    cat >&2 <<EOF
weaver modern image requires the ${baseline} CPU baseline for ${arch}, but this host does not satisfy it.
Use ghcr.io/scryer-media/weaver:latest-slim or ghcr.io/scryer-media/weaver:latest on this host instead.
EOF
    exit 1
}

ARCH=$(detect_container_arch)
CPUINFO_PATH=${WEAVER_CPUINFO_PATH:-/proc/cpuinfo}
PAYLOAD_ROOT=${WEAVER_PAYLOAD_ROOT:-/opt/weaver}
RUNTIME_MODE=${WEAVER_RUNTIME_MODE:-default}
SELECTED_LANE=$(select_linux_lane_for_arch "$ARCH" "$CPUINFO_PATH")
SELECTED_PAYLOAD=$(selected_payload_name "$ARCH" "$CPUINFO_PATH")
PORTABLE_PAYLOAD=$(portable_payload_name)
SELECTED_BINARY="$PAYLOAD_ROOT/$SELECTED_PAYLOAD"
PORTABLE_BINARY="$PAYLOAD_ROOT/$PORTABLE_PAYLOAD"

case "$RUNTIME_MODE" in
    modern)
        OPTIMIZED_PAYLOAD=$(optimized_payload_name "$ARCH")
        case "$ARCH" in
            amd64) OPTIMIZED_BASELINE=haswell ;;
            arm64) OPTIMIZED_BASELINE=cortex-a76 ;;
            *)
                printf 'unsupported architecture for weaver modern image: %s\n' "$ARCH" >&2
                exit 1
                ;;
        esac

        [ "$SELECTED_LANE" = "portable" ] && print_modern_cpu_error "$ARCH" "$OPTIMIZED_BASELINE"

        RUNTIME_BIN="$PAYLOAD_ROOT/$OPTIMIZED_PAYLOAD"
        [ -x "$RUNTIME_BIN" ] || {
            printf 'missing optimized weaver payload for modern image: %s\n' "$RUNTIME_BIN" >&2
            exit 1
        }
        ;;
    *)
        RUNTIME_BIN="$SELECTED_BINARY"
        ;;
esac

if [ "$RUNTIME_MODE" != "modern" ] && [ ! -x "$RUNTIME_BIN" ]; then
    if [ "$SELECTED_PAYLOAD" = "$PORTABLE_PAYLOAD" ] || [ ! -x "$PORTABLE_BINARY" ]; then
        exit 1
    fi
    RUNTIME_BIN="$PORTABLE_BINARY"
fi

apply_umask

if [ "$(id -u)" -ne 0 ]; then
    exec "$RUNTIME_BIN" "$@"
fi

PUID=${PUID:-1000}
PGID=${PGID:-1000}

mkdir -p /config
chown -R "$PUID":"$PGID" /config

echo "
───────────────────────────────────
  weaver
  User UID:  $PUID
  User GID:  $PGID
  Config:    /config
───────────────────────────────────
"

exec su-exec "$PUID":"$PGID" "$RUNTIME_BIN" "$@"
