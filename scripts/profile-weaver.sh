#!/bin/bash
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_ROOT="${PROJ_DIR}/tmp/profiles"
DURATION="${1:-30}"
INTERVAL_MS="${2:-5}"

if ! [[ "$DURATION" =~ ^[0-9]+$ ]]; then
  echo "duration must be an integer number of seconds" >&2
  exit 1
fi

if ! [[ "$INTERVAL_MS" =~ ^[0-9]+$ ]]; then
  echo "interval must be an integer number of milliseconds" >&2
  exit 1
fi

PID="$(lsof -nP -iTCP:9090 -sTCP:LISTEN -t | head -n1 || true)"
if [ -z "$PID" ]; then
  echo "no process found listening on port 9090" >&2
  exit 1
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_ROOT}/${STAMP}"
mkdir -p "$OUT_DIR"

SAMPLE_OUT="${OUT_DIR}/weaver.sample.txt"
SPINDUMP_OUT="${OUT_DIR}/weaver.spindump.txt"
META_OUT="${OUT_DIR}/meta.txt"

{
  echo "pid=${PID}"
  echo "duration_seconds=${DURATION}"
  echo "sample_interval_ms=${INTERVAL_MS}"
  echo "started_at=$(date -Iseconds)"
  ps -p "$PID" -o pid= -o command=
} >"$META_OUT"

echo "Profiling PID $PID for ${DURATION}s (${INTERVAL_MS}ms sample interval)"
echo "Output dir: $OUT_DIR"

sample "$PID" "$DURATION" "$INTERVAL_MS" -mayDie -fullPaths -file "$SAMPLE_OUT" >/dev/null 2>&1 &
SAMPLE_PID=$!

spindump "$PID" "$DURATION" 10 -o "$SPINDUMP_OUT" >/dev/null 2>&1 &
SPINDUMP_PID=$!

wait "$SAMPLE_PID"
SPINDUMP_STATUS=0
if ! wait "$SPINDUMP_PID"; then
  SPINDUMP_STATUS=$?
fi

if [ ! -f "$SPINDUMP_OUT" ]; then
  {
    echo "spindump_status=${SPINDUMP_STATUS}"
    echo "spindump_available=0"
  } >>"$META_OUT"
else
  {
    echo "spindump_status=${SPINDUMP_STATUS}"
    echo "spindump_available=1"
  } >>"$META_OUT"
fi

echo "Done."
echo "sample:   $SAMPLE_OUT"
if [ -f "$SPINDUMP_OUT" ]; then
  echo "spindump: $SPINDUMP_OUT"
else
  echo "spindump: unavailable"
fi
echo "meta:     $META_OUT"
