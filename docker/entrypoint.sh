#!/bin/sh
set -eu

RUNTIME_BIN=/opt/weaver/weaver

apply_umask() {
    if [ -n "${UMASK:-}" ]; then
        umask "$UMASK" || {
            printf 'invalid UMASK: %s\n' "$UMASK" >&2
            exit 1
        }
    fi
}

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

exec setpriv --reuid "$PUID" --regid "$PGID" --clear-groups "$RUNTIME_BIN" "$@"
