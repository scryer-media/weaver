#!/bin/bash
# Deploy weaver: kill existing, clean logs, release build, start with logging.
#
# Usage: deploy.sh [level:components]
#   No args:                RUST_LOG=info,weaver::pipeline=debug
#   deploy.sh debug:nntp    RUST_LOG=info,weaver::pipeline=debug,weaver_nntp=debug
#   deploy.sh debug:nntp,scheduler  RUST_LOG=info,weaver::pipeline=debug,weaver_nntp=debug,weaver_scheduler=debug
#   deploy.sh trace:pipeline        RUST_LOG=info,weaver::pipeline=trace
#   deploy.sh debug:all     RUST_LOG=debug
set -e

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="/tmp/weaver.log"
BINARY="$PROJ_DIR/target/release/weaver"

# Build RUST_LOG from arguments.
RUST_LOG="info,weaver::pipeline=debug"
if [ $# -gt 0 ]; then
    level="${1%%:*}"
    targets="${1#*:}"
    if [ "$targets" = "all" ]; then
        RUST_LOG="$level"
    else
        IFS=',' read -ra parts <<< "$targets"
        for target in "${parts[@]}"; do
            # Bare name = crate (weaver_foo), qualified = module path (weaver::pipeline)
            if [[ "$target" == *::* || "$target" == *.* ]]; then
                RUST_LOG="${RUST_LOG},${target}=${level}"
            else
                RUST_LOG="${RUST_LOG},weaver_${target//-/_}=${level}"
            fi
        done
    fi
fi

CONFIG_FILE="$PROJ_DIR/weaver.toml"
BACKUP_DIR="$PROJ_DIR/tmp/config-backups"

# Back up weaver.toml before deploy (keeps last 5).
if [ -f "$CONFIG_FILE" ]; then
    mkdir -p "$BACKUP_DIR"
    cp "$CONFIG_FILE" "$BACKUP_DIR/weaver.toml.$(date +%Y%m%d-%H%M%S)"
    # Prune old backups, keep last 5.
    ls -t "$BACKUP_DIR"/weaver.toml.* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
fi

echo "==> Stopping existing weaver..."
# Kill by process name patterns (covers both path forms and bare command).
pkill -f 'weaver serve' 2>/dev/null || true
pkill -f 'target/release/weaver' 2>/dev/null || true
pkill -f 'target/debug/weaver' 2>/dev/null || true
# Also kill anything holding port 9090 as a fallback.
lsof -ti:9090 | xargs kill 2>/dev/null || true
sleep 1
# Verify port is free — if something survived, force-kill it.
if lsof -ti:9090 >/dev/null 2>&1; then
    echo "    Port 9090 still in use, sending SIGKILL..."
    lsof -ti:9090 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

echo "==> Cleaning old logs..."
> "$LOG_FILE"

echo "==> Building frontend..."
cd "$PROJ_DIR/apps/weaver-web"
npm run build 2>&1 | tail -10

echo "==> Building release..."
cd "$PROJ_DIR"
cargo build --release -p weaver 2>&1 | tail -5

echo "==> Starting weaver (RUST_LOG=$RUST_LOG, logging to $LOG_FILE)..."
RUST_LOG="$RUST_LOG" "$BINARY" serve >>"$LOG_FILE" 2>&1 &
PID=$!
sleep 2

if kill -0 "$PID" 2>/dev/null; then
    echo "==> Weaver running (PID=$PID, port=9090)"
    echo "==> Log: tail -f $LOG_FILE"
    head -10 "$LOG_FILE"
else
    echo "==> FAILED to start! Check $LOG_FILE"
    tail -20 "$LOG_FILE"
    exit 1
fi
