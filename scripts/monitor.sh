#!/bin/bash
# Monitor weaver: logs + memory pressure
# Usage: ./scripts/monitor.sh

PID=$(pgrep -f 'target/debug/weaver' | head -1)
if [ -z "$PID" ]; then
  echo "weaver not running"
  exit 1
fi

echo "Monitoring weaver PID=$PID"
echo "---"

while true; do
  # Memory info
  RSS_KB=$(ps -o rss= -p "$PID" 2>/dev/null)
  if [ -z "$RSS_KB" ]; then
    echo "[$(date +%H:%M:%S)] weaver process gone"
    exit 1
  fi
  RSS_MB=$((RSS_KB / 1024))

  # System memory pressure (macOS)
  MEM_PRESSURE=$(memory_pressure -Q 2>/dev/null | grep "System-wide" | head -1)

  # Last meaningful log line (skip idle ticks)
  LAST_LOG=$(ps -o command= -p "$PID" 2>/dev/null)

  echo "[$(date +%H:%M:%S)] RSS=${RSS_MB}MB | $MEM_PRESSURE"

  # Alert if over 1GB
  if [ "$RSS_MB" -gt 1024 ]; then
    echo "⚠️  MEMORY WARNING: ${RSS_MB}MB"
  fi

  sleep 5
done
