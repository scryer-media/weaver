#!/bin/sh
# Bring up an NFSv4-only kernel server whose link is shaped to the rate and
# fixed delay the plan declared, then publish a machine-readable report of what
# was actually configured. The controller re-derives every value in that report
# from live `tc` output before it accepts a run, so this file is evidence, not
# a promise.
set -eu

REPORT_PATH=/run/nntpbench-storage-shaper.json
REPORT_SCHEMA=1

: "${NFS_LINK_BITS_PER_SECOND:?set NFS_LINK_BITS_PER_SECOND from nntpbench storage-env}"
: "${NFS_LINK_BURST_BYTES:?set NFS_LINK_BURST_BYTES from nntpbench storage-env}"
: "${NFS_RTT_MICROS:?set NFS_RTT_MICROS from nntpbench storage-env}"
: "${NFS_EXPORT_OPTIONS:?set NFS_EXPORT_OPTIONS from nntpbench storage-env}"

INTERFACE="${NFS_INTERFACE:-eth0}"
INGRESS_DEVICE="${NFS_INGRESS_DEVICE:-ifb-nfs}"
THREADS="${NFS_THREADS:-8}"
EXPORT_ROOT=/export

require_number() {
    if ! printf '%s' "$2" | grep -Eq '^[0-9]+$'; then
        printf 'nfs-server: %s must be a decimal number, got %s\n' "$1" "$2" >&2
        exit 64
    fi
    if [ "$2" -le 0 ]; then
        printf 'nfs-server: %s must be greater than zero\n' "$1" >&2
        exit 64
    fi
}

require_number NFS_LINK_BITS_PER_SECOND "$NFS_LINK_BITS_PER_SECOND"
require_number NFS_LINK_BURST_BYTES "$NFS_LINK_BURST_BYTES"
require_number NFS_RTT_MICROS "$NFS_RTT_MICROS"

# The plan declares a round trip; each direction carries half of it as a fixed
# delay with zero jitter, so the observed round trip is the declared one and a
# repeated run cannot drift.
ONE_WAY_MICROS=$((NFS_RTT_MICROS / 2))
if [ "$ONE_WAY_MICROS" -le 0 ]; then
    printf 'nfs-server: NFS_RTT_MICROS=%s is too small to split across two directions\n' "$NFS_RTT_MICROS" >&2
    exit 64
fi

# ---------------------------------------------------------------- NFS server

mkdir -p "$EXPORT_ROOT" /var/lib/nfs/v4recovery
chmod 0777 "$EXPORT_ROOT"

mountpoint -q /proc/fs/nfsd || mount -t nfsd nfsd /proc/fs/nfsd

# The kernel server cannot export the container's own overlay filesystem, and
# `exportfs` says so only obliquely. Fail here with the actual remedy: the
# export has to be a Docker volume backed by a real filesystem.
EXPORT_FSTYPE="$(stat -f -c %T "$EXPORT_ROOT" 2>/dev/null || echo unknown)"
if [ "$EXPORT_FSTYPE" = "overlayfs" ] || [ "$EXPORT_FSTYPE" = "overlay" ]; then
    printf 'nfs-server: %s is the container overlay, which no kernel NFS server can export; mount a Docker volume at %s\n' \
        "$EXPORT_ROOT" "$EXPORT_ROOT" >&2
    exit 64
fi

# A fresh server would otherwise hold a 90 s v4 grace period, which would land
# inside the first measured run as unexplained latency. Shortening it moves that
# cost into container startup, before any run opens a session.
if [ -w /proc/fs/nfsd/nfsv4leasetime ]; then
    echo 10 > /proc/fs/nfsd/nfsv4leasetime
fi
if [ -w /proc/fs/nfsd/nfsv4gracetime ]; then
    echo 10 > /proc/fs/nfsd/nfsv4gracetime
fi

# NFSv4 only: v2 and v3 would need rpcbind, a separate mountd port and, for
# locking, statd — three more unshaped conversations. The client pins 4.1, whose
# back channel shares the same TCP connection, so one shaped port carries
# everything.
exportfs -o "$NFS_EXPORT_OPTIONS" "*:$EXPORT_ROOT"
rpc.mountd -N 2 -N 3 2>/dev/null || rpc.mountd -N 3
# Current nfs-utils builds have NFSv2 compiled out and reject `-N 2` outright,
# while older ones still need it to disable v2. Ask for both and fall back, so
# the image works either way and never silently leaves v2 listening.
rpc.nfsd -N 2 -N 3 "$THREADS" 2>/dev/null || rpc.nfsd -N 3 "$THREADS"

NFS_VERSIONS="$(cat /proc/fs/nfsd/versions 2>/dev/null || echo unknown)"
KERNEL_RELEASE="$(uname -r)"

# ------------------------------------------------------------------- shaping

tc qdisc del dev "$INTERFACE" root 2>/dev/null || true
tc qdisc del dev "$INTERFACE" ingress 2>/dev/null || true

# Server to client: a token bucket for the rate and a netem child for the fixed
# one-way delay. tbf is the root qdisc so `tc -s qdisc show` attributes every
# sent byte on this interface to the shaped path.
tc qdisc add dev "$INTERFACE" root handle 1: tbf \
    rate "${NFS_LINK_BITS_PER_SECOND}bit" burst "$NFS_LINK_BURST_BYTES" latency 200ms
tc qdisc add dev "$INTERFACE" parent 1:1 handle 10: netem \
    delay "${ONE_WAY_MICROS}us" 0us

# Client to server: Linux cannot shape ingress directly, so mirror it to an
# intermediate functional block device and shape that device's egress with the
# same tbf + netem pair. This is the preferred mechanism because it delays and
# queues rather than dropping.
setup_ifb() {
    ip link add "$INGRESS_DEVICE" type ifb || return 1
    ip link set dev "$INGRESS_DEVICE" up || return 1
    tc qdisc add dev "$INTERFACE" handle ffff: ingress || return 1
    tc filter add dev "$INTERFACE" parent ffff: protocol all prio 1 u32 \
        match u32 0 0 action mirred egress redirect dev "$INGRESS_DEVICE" || return 1
    tc qdisc add dev "$INGRESS_DEVICE" root handle 1: tbf \
        rate "${NFS_LINK_BITS_PER_SECOND}bit" burst "$NFS_LINK_BURST_BYTES" latency 200ms || return 1
    tc qdisc add dev "$INGRESS_DEVICE" parent 1:1 handle 10: netem \
        delay "${ONE_WAY_MICROS}us" 0us || return 1
    return 0
}

# The fallback polices instead of queueing: it enforces the same rate but drops
# the excess and cannot add delay. Which mechanism ran is recorded rather than
# assumed, and the controller asserts a zero client-to-server delay when this
# path is taken, so a policed run can never be read as a delayed one.
setup_police() {
    ip link del "$INGRESS_DEVICE" 2>/dev/null || true
    tc qdisc del dev "$INTERFACE" ingress 2>/dev/null || true
    tc qdisc add dev "$INTERFACE" handle ffff: ingress
    tc filter add dev "$INTERFACE" parent ffff: protocol all prio 1 u32 \
        match u32 0 0 police rate "${NFS_LINK_BITS_PER_SECOND}bit" \
        burst "$NFS_LINK_BURST_BYTES" conform-exceed drop flowid :1
}

if setup_ifb; then
    INGRESS_MECHANISM="ifb-tbf+netem"
    REPORTED_INGRESS_DEVICE="$INGRESS_DEVICE"
else
    printf 'nfs-server: ifb redirect unavailable, falling back to ingress policing\n' >&2
    setup_police
    INGRESS_MECHANISM="ingress-police"
    REPORTED_INGRESS_DEVICE=""
fi

# -------------------------------------------------------------------- report

cat > "$REPORT_PATH" <<EOF_REPORT
{
  "schema_version": $REPORT_SCHEMA,
  "interface": "$INTERFACE",
  "ingress_device": "$REPORTED_INGRESS_DEVICE",
  "egress_mechanism": "tbf+netem",
  "ingress_mechanism": "$INGRESS_MECHANISM",
  "link_bits_per_second": $NFS_LINK_BITS_PER_SECOND,
  "link_burst_bytes": $NFS_LINK_BURST_BYTES,
  "rtt_micros": $NFS_RTT_MICROS,
  "one_way_delay_micros": $ONE_WAY_MICROS,
  "export_options": "$NFS_EXPORT_OPTIONS",
  "nfs_versions": "$(printf '%s' "$NFS_VERSIONS" | tr -d '"')",
  "kernel_release": "$(printf '%s' "$KERNEL_RELEASE" | tr -d '"')"
}
EOF_REPORT

printf 'nfs-server: exporting %s at %s bit/s, %sus one-way delay, ingress via %s\n' \
    "$EXPORT_ROOT" "$NFS_LINK_BITS_PER_SECOND" "$ONE_WAY_MICROS" "$INGRESS_MECHANISM" >&2

exec sleep infinity
