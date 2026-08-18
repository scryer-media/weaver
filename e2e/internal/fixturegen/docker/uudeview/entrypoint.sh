#!/bin/sh
# Selects one of the UUDeview suite's two binaries. The remaining arguments are
# exec'd as the tool's own argument vector: nothing here is re-parsed, expanded
# or passed through a shell.
set -eu

if [ "$#" -lt 1 ]; then
    echo "usage: <uuenview|uudeview> [arguments...]" >&2
    exit 2
fi

tool="$1"
shift

case "$tool" in
    uuenview|uudeview)
        exec "/usr/local/bin/$tool" "$@"
        ;;
    *)
        echo "unknown tool: $tool (expected uuenview or uudeview)" >&2
        exit 2
        ;;
esac
