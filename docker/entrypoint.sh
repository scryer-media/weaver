#!/bin/sh
set -e

# If not running as root (e.g. --user flag), skip privilege setup
# and just exec the binary directly.
if [ "$(id -u)" -ne 0 ]; then
    exec /usr/local/bin/weaver "$@"
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

exec su-exec "$PUID":"$PGID" /usr/local/bin/weaver "$@"
