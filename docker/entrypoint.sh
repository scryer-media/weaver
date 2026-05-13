#!/bin/sh
set -e

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$SCRIPT_DIR/runtime-select.sh"

ARCH=$(detect_container_arch)
PAYLOAD_ROOT=/opt/weaver
SELECTED_PAYLOAD=$(selected_payload_name "$ARCH" /proc/cpuinfo)
PORTABLE_PAYLOAD=$(portable_payload_name)
SELECTED_BINARY="$PAYLOAD_ROOT/$SELECTED_PAYLOAD"
PORTABLE_BINARY="$PAYLOAD_ROOT/$PORTABLE_PAYLOAD"
RUNTIME_BIN="$SELECTED_BINARY"

if [ ! -x "$RUNTIME_BIN" ]; then
    if [ "$SELECTED_PAYLOAD" = "$PORTABLE_PAYLOAD" ] || [ ! -x "$PORTABLE_BINARY" ]; then
        exit 1
    fi
    RUNTIME_BIN="$PORTABLE_BINARY"
fi

# If not running as root (e.g. --user flag), skip privilege setup
# and just exec the selected binary directly.
if [ "$(id -u)" -ne 0 ]; then
    exec "$RUNTIME_BIN" "$@"
fi

PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Ensure directories are owned by the requested user
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
