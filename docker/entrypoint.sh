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

ARCH=$(detect_container_arch)
CPUINFO_PATH=${WEAVER_CPUINFO_PATH:-/proc/cpuinfo}
PAYLOAD_ROOT=${WEAVER_PAYLOAD_ROOT:-/opt/weaver}
SELECTED_PAYLOAD=$(selected_payload_name "$ARCH" "$CPUINFO_PATH")
PORTABLE_PAYLOAD=$(portable_payload_name)
SELECTED_BINARY="$PAYLOAD_ROOT/$SELECTED_PAYLOAD"
PORTABLE_BINARY="$PAYLOAD_ROOT/$PORTABLE_PAYLOAD"
RUNTIME_BIN="$SELECTED_BINARY"

apply_umask

if [ ! -x "$RUNTIME_BIN" ]; then
    if [ "$SELECTED_PAYLOAD" = "$PORTABLE_PAYLOAD" ] || [ ! -x "$PORTABLE_BINARY" ]; then
        exit 1
    fi
    RUNTIME_BIN="$PORTABLE_BINARY"
fi

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
