#!/bin/bash
# Local UI dev harness: run Weaver via cargo run and host the Vite frontend with HMR.
#
# Usage:
#   scripts/deploy-dev.sh
#   scripts/deploy-dev.sh debug:nntp,scheduler
#
# Environment overrides:
#   WEAVER_DEV_BACKEND_PORT   Backend port for Weaver (default: 6789)
#   WEAVER_DEV_FRONTEND_PORT  Frontend port for Vite (default: 5173)
#   WEAVER_DEV_BACKEND_LOG    Backend log file (default: /tmp/weaver-dev-backend.log)
#   WEAVER_DEV_STATE_DIR      Persistent dev state dir for weaver.db + encryption.key
#   WEAVER_DEV_DATA_DIR       Ephemeral working data dir for downloads/intermediate files
#   WEAVER_DEV_KEEP_DATA      Set to 1 to keep the ephemeral data dir after exit
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WEB_DIR="$PROJ_DIR/apps/weaver-web"
BACKEND_PORT="${WEAVER_DEV_BACKEND_PORT:-6789}"
FRONTEND_PORT="${WEAVER_DEV_FRONTEND_PORT:-5173}"
BACKEND_LOG="${WEAVER_DEV_BACKEND_LOG:-/tmp/weaver-dev-backend.log}"
STATE_DIR="${WEAVER_DEV_STATE_DIR:-$PROJ_DIR/tmp/dev-instance}"
STATE_KEY_FILE="$STATE_DIR/encryption.key"
DATA_DIR="${WEAVER_DEV_DATA_DIR:-}"

if [ -z "$DATA_DIR" ]; then
    mkdir -p "$PROJ_DIR/tmp"
    find "$PROJ_DIR/tmp" -maxdepth 1 -mindepth 1 -type d -name 'dev-data.*' -exec rm -rf {} + 2>/dev/null || true
    DATA_DIR="$(mktemp -d "$PROJ_DIR/tmp/dev-data.XXXXXX")"
fi

build_rust_log() {
    local rust_log="info,weaver::pipeline=debug"
    if [ $# -eq 0 ] || [ -z "${1:-}" ]; then
        printf '%s\n' "$rust_log"
        return
    fi

    local level="${1%%:*}"
    local targets="${1#*:}"
    if [ "$targets" = "all" ]; then
        printf '%s\n' "$level"
        return
    fi

    local -a parts=()
    IFS=',' read -ra parts <<< "$targets"
    for target in "${parts[@]}"; do
        if [[ "$target" == *::* || "$target" == *.* ]]; then
            rust_log="${rust_log},${target}=${level}"
        else
            rust_log="${rust_log},weaver_${target//-/_}=${level}"
        fi
    done
    printf '%s\n' "$rust_log"
}

stop_port() {
    local port="$1"
    local pids
    pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -z "$pids" ]; then
        return
    fi

    echo "==> Stopping listener(s) on port $port..."
    kill $pids 2>/dev/null || true
    sleep 1

    pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -n "$pids" ]; then
        echo "    Port $port still busy, sending SIGKILL..."
        kill -9 $pids 2>/dev/null || true
        sleep 1
    fi
}

wait_for_backend() {
    local pid="$1"
    local url="http://127.0.0.1:${BACKEND_PORT}/"

    for _ in $(seq 1 60); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "==> Weaver failed to start. Tail of $BACKEND_LOG:"
            tail -50 "$BACKEND_LOG" || true
            exit 1
        fi

        if curl -fsS "$url" >/dev/null 2>&1; then
            return
        fi

        sleep 1
    done

    echo "==> Timed out waiting for Weaver on $url. Tail of $BACKEND_LOG:"
    tail -50 "$BACKEND_LOG" || true
    exit 1
}

sql_escape() {
    printf "%s" "${1//\'/\'\'}"
}

seed_runtime_dirs() {
    local db_path="$STATE_DIR/weaver.db"
    local data_sql
    local intermediate_sql
    local complete_sql

    data_sql="$(sql_escape "$DATA_DIR")"
    intermediate_sql="$(sql_escape "$DATA_DIR/intermediate")"
    complete_sql="$(sql_escape "$DATA_DIR/complete")"

    mkdir -p "$DATA_DIR/intermediate" "$DATA_DIR/complete"

    sqlite3 "$db_path" <<SQL
INSERT INTO settings(key, value) VALUES ('data_dir', '$data_sql')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
INSERT INTO settings(key, value) VALUES ('intermediate_dir', '$intermediate_sql')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
INSERT INTO settings(key, value) VALUES ('complete_dir', '$complete_sql')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
SQL
}

load_state_encryption_key() {
    if [ -f "$STATE_KEY_FILE" ]; then
        WEAVER_ENCRYPTION_KEY="$(tr -d '\r\n' < "$STATE_KEY_FILE")"
        export WEAVER_ENCRYPTION_KEY
    fi
}

bootstrap_state_dir() {
    if [ -f "$STATE_DIR/weaver.db" ]; then
        return
    fi

    echo "==> Bootstrapping dev state in $STATE_DIR"
    (
        cd "$PROJ_DIR"
        RUST_LOG=warn cargo run -p weaver -- --config "$STATE_DIR" serve --port "$BACKEND_PORT" >>"$BACKEND_LOG" 2>&1
    ) &
    local bootstrap_pid=$!

    wait_for_backend "$bootstrap_pid"
    kill "$bootstrap_pid" 2>/dev/null || true
    wait "$bootstrap_pid" 2>/dev/null || true
}

cleanup() {
    if [ -n "${BACKEND_PID:-}" ] && kill -0 "$BACKEND_PID" 2>/dev/null; then
        kill "$BACKEND_PID" 2>/dev/null || true
        wait "$BACKEND_PID" 2>/dev/null || true
    fi

    if [ "${WEAVER_DEV_KEEP_DATA:-0}" != "1" ] && [ -n "${DATA_DIR:-}" ] && [ -d "$DATA_DIR" ]; then
        rm -rf "$DATA_DIR"
    fi
}

trap cleanup EXIT INT TERM

RUST_LOG="$(build_rust_log "${1:-}")"

stop_port "$BACKEND_PORT"
stop_port "$FRONTEND_PORT"

mkdir -p "$(dirname "$BACKEND_LOG")"
: > "$BACKEND_LOG"
mkdir -p "$STATE_DIR"

load_state_encryption_key
bootstrap_state_dir
load_state_encryption_key
seed_runtime_dirs

echo "==> Starting Weaver backend with cargo run on :$BACKEND_PORT"
(
    cd "$PROJ_DIR"
    RUST_LOG="$RUST_LOG" cargo run -p weaver -- --config "$STATE_DIR" serve --port "$BACKEND_PORT" >>"$BACKEND_LOG" 2>&1
) &
BACKEND_PID=$!

wait_for_backend "$BACKEND_PID"

echo "==> Weaver backend ready"
echo "    Backend:  http://127.0.0.1:$BACKEND_PORT"
echo "    Frontend: http://127.0.0.1:$FRONTEND_PORT"
echo "    State:    $STATE_DIR"
echo "    Data:     $DATA_DIR"
echo "    Log:      tail -f $BACKEND_LOG"
echo
echo "==> Starting Vite dev server with live updates..."
cd "$WEB_DIR"
npm run dev -- --host 0.0.0.0 --port "$FRONTEND_PORT"
