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

# Retaining NET_RAW is opt-in even when the container runtime grants it by default.
retain_net_raw=false
if [ "${WEAVER_RETAIN_NET_RAW:-false}" = "true" ] && [ -r /proc/self/status ]; then
    while read -r capability_key capability_value capability_rest; do
        if [ "$capability_key" = "CapPrm:" ] && [ "$((0x$capability_value & 8192))" -ne 0 ]; then
            retain_net_raw=true
            break
        fi
    done < /proc/self/status
fi
if [ "$retain_net_raw" = "true" ]; then
    exec setpriv --reuid "$PUID" --regid "$PGID" --clear-groups \
        --inh-caps=-all,+net_raw --ambient-caps=-all,+net_raw "$RUNTIME_BIN" "$@" </dev/null
fi
exec setpriv --reuid "$PUID" --regid "$PGID" --clear-groups "$RUNTIME_BIN" "$@" </dev/null
