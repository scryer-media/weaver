#!/bin/sh
set -e

# If not running as root (e.g. --user flag), skip privilege setup
# and just exec the binary directly.
if [ "$(id -u)" -ne 0 ]; then
    exec /usr/local/bin/weaver "$@"
fi

PUID=${PUID:-1000}
PGID=${PGID:-1000}

# ── Migrate from /data to /config ────────────────────────────────────────────
# Previous images stored the database in /data. If the old database exists but
# the new location doesn't, move it automatically so existing installs upgrade
# seamlessly.
if [ -f /data/weaver.db ] && [ ! -f /config/weaver.db ]; then
    echo "  Migrating database from /data/weaver.db to /config/weaver.db"
    mkdir -p /config
    cp /data/weaver.db /config/weaver.db
    [ -f /data/weaver.db-wal ] && cp /data/weaver.db-wal /config/weaver.db-wal
    [ -f /data/weaver.db-shm ] && cp /data/weaver.db-shm /config/weaver.db-shm
    [ -f /data/weaver.toml ] && [ ! -f /config/weaver.toml ] && cp /data/weaver.toml /config/weaver.toml
    [ -f /data/encryption.key ] && [ ! -f /config/encryption.key ] && cp /data/encryption.key /config/encryption.key
    echo "  Migration complete. You can remove /data/weaver.db after verifying."
fi

# Ensure directories are owned by the requested user
mkdir -p /config
chown -R "$PUID":"$PGID" /config
[ -d /data ] && chown -R "$PUID":"$PGID" /data

echo "
───────────────────────────────────
  weaver
  User UID:  $PUID
  User GID:  $PGID
  Config:    /config
───────────────────────────────────
"

exec su-exec "$PUID":"$PGID" /usr/local/bin/weaver "$@"
