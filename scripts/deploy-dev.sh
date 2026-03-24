#!/bin/bash
# Quick dev deploy: builds frontend then cargo run (no separate build step).
#
# Usage: deploy-dev.sh [level:components]
#   Same log-level syntax as deploy.sh (default: info,weaver::pipeline=debug)
set -e

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="/tmp/weaver.log"

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
            if [[ "$target" == *::* || "$target" == *.* ]]; then
                RUST_LOG="${RUST_LOG},${target}=${level}"
            else
                RUST_LOG="${RUST_LOG},weaver_${target//-/_}=${level}"
            fi
        done
    fi
fi

echo "==> Stopping existing weaver..."
pkill -f 'cargo run -p weaver' 2>/dev/null || true
pkill -f 'weaver serve' 2>/dev/null || true
pkill -f 'target/release/weaver' 2>/dev/null || true
pkill -f 'target/debug/weaver' 2>/dev/null || true
lsof -ti:9090 | xargs kill 2>/dev/null || true
sleep 1
if lsof -ti:9090 >/dev/null 2>&1; then
    echo "    Port 9090 still in use, sending SIGKILL..."
    lsof -ti:9090 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

echo "==> Cleaning old logs..."
> "$LOG_FILE"

echo "==> Building frontend..."
cd "$PROJ_DIR/apps/weaver-web"
npm run build 2>&1 | tail -5

echo "==> Starting weaver via cargo run (RUST_LOG=$RUST_LOG, logging to $LOG_FILE)..."
cd "$PROJ_DIR"
RUST_LOG="$RUST_LOG" cargo run -p weaver -- serve >>"$LOG_FILE" 2>&1 &
PID=$!

# Wait for compile + startup
echo "    Compiling and starting (PID=$PID)..."
for i in $(seq 1 60); do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "==> FAILED to start! Check $LOG_FILE"
        tail -20 "$LOG_FILE"
        exit 1
    fi
    if lsof -ti:9090 >/dev/null 2>&1; then
        echo "==> Weaver running (PID=$PID, port=9090)"
        echo "==> Log: tail -f $LOG_FILE"
        head -10 "$LOG_FILE"
        exit 0
    fi
    sleep 2
done

echo "==> Timed out waiting for port 9090. Check $LOG_FILE"
tail -20 "$LOG_FILE"
exit 1
