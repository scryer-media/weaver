#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."
exec cargo xtask perf par2-x86 "$@"
