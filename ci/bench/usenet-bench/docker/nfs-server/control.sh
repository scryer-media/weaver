#!/bin/sh
# The controller's only handle on the NFS server. It exists so the harness can
# take an exclusive lease, create and destroy one run's export subtree, and read
# the server's own export options back — without ever giving a measured product
# a way to touch its own storage.
set -eu

EXPORT_ROOT=/export
LEASE_DIR=/run/nntpbench-nfs-lease
LEASE_FILE="$LEASE_DIR/id"
REPORT_PATH=/run/nntpbench-storage-shaper.json

usage() {
    cat >&2 <<'EOF_USAGE'
usage: nntpbench-nfs-control <command> [argument]

  lease-acquire <id>   take the exclusive run lease, failing if one is held
  lease-release <id>   release the lease held under this id
  lease-id             print the current lease holder
  export-create <dir>  create one run's empty export subtree
  export-remove <dir>  remove one run's export subtree
  export-report        print the server's own view of its export options
  health               succeed only when the server and its shaper are up
EOF_USAGE
    exit 64
}

fail() {
    printf 'nfs-control: %s\n' "$1" >&2
    exit 65
}

require_lease_id() {
    printf '%s' "$1" | grep -Eq '^[0-9a-f]{64}$' || fail "lease id must be 64 hexadecimal characters"
}

# A run directory is a single lower-case segment under the pseudo-root. The
# check is a whitelist, not an escape filter: nothing with a separator, a dot
# segment or whitespace can reach a path expression.
require_export_dir() {
    printf '%s' "$1" | grep -Eq '^/[a-z0-9][a-z0-9-]{0,63}$' || fail "invalid export directory: $1"
}

[ $# -ge 1 ] || usage
command="$1"
shift

case "$command" in
    lease-acquire)
        [ $# -eq 1 ] || usage
        require_lease_id "$1"
        # mkdir is the atomic primitive here: two controllers racing for the
        # same server cannot both believe they own it.
        mkdir "$LEASE_DIR" 2>/dev/null || fail "the NFS execution lease is already held"
        printf '%s' "$1" > "$LEASE_FILE"
        ;;
    lease-release)
        [ $# -eq 1 ] || usage
        require_lease_id "$1"
        [ -f "$LEASE_FILE" ] || fail "no NFS execution lease is held"
        [ "$(cat "$LEASE_FILE")" = "$1" ] || fail "the NFS execution lease is held by another run"
        rm -rf "$LEASE_DIR"
        ;;
    lease-id)
        [ $# -eq 0 ] || usage
        [ -f "$LEASE_FILE" ] || fail "no NFS execution lease is held"
        cat "$LEASE_FILE"
        ;;
    export-create)
        [ $# -eq 1 ] || usage
        require_export_dir "$1"
        if [ -e "$EXPORT_ROOT$1" ]; then
            fail "export directory $1 already exists"
        fi
        mkdir -p "$EXPORT_ROOT$1/complete" "$EXPORT_ROOT$1/incomplete"
        chmod 0777 "$EXPORT_ROOT$1" "$EXPORT_ROOT$1/complete" "$EXPORT_ROOT$1/incomplete"
        ;;
    export-remove)
        [ $# -eq 1 ] || usage
        require_export_dir "$1"
        rm -rf "${EXPORT_ROOT:?}$1"
        ;;
    export-report)
        [ $# -eq 0 ] || usage
        exportfs -v
        ;;
    health)
        [ $# -eq 0 ] || usage
        [ -f "$REPORT_PATH" ] || fail "the shaper report has not been written"
        grep -q '+4.1' /proc/fs/nfsd/versions || fail "NFSv4.1 is not enabled"
        [ "$(cat /proc/fs/nfsd/threads)" -gt 0 ] || fail "no nfsd threads are running"
        ;;
    *)
        usage
        ;;
esac
