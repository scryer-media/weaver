#!/bin/sh
# Render the plan's fixed client<->server round trip with tc netem on the
# shaper's benchmark-facing interface, publish a machine-readable report of
# what was configured, then hand over to the proxy. The proxy refuses to start
# if the report does not describe the round trip it was told to expect, and it
# reads the qdiscs back with tc for every attestation snapshot, so this file is
# evidence, not a promise. With NNTP_RTT_MICROS=0 no qdisc is touched and the
# proxy starts exactly as it did before round trips existed.
set -eu

REPORT_PATH="${NNTP_LINK_REPORT_PATH:-/run/nntpshaper-link.json}"
REPORT_SCHEMA=1
RTT_MICROS="${NNTP_RTT_MICROS:-0}"
RATE_BITS="${NNTP_EGRESS_BITS_PER_SECOND:-0}"
INGRESS_DEVICE="${NNTP_INGRESS_DEVICE:-ifb-nntp}"

require_number() {
    if ! printf '%s' "$2" | grep -Eq '^[0-9]+$'; then
        printf 'nntp-shaper: %s must be a decimal number, got %s\n' "$1" "$2" >&2
        exit 64
    fi
}
require_number NNTP_RTT_MICROS "$RTT_MICROS"
require_number NNTP_EGRESS_BITS_PER_SECOND "$RATE_BITS"

if [ "$RTT_MICROS" -eq 0 ]; then
    rm -f "$REPORT_PATH"
    exec /nntpshaper "$@"
fi

# ------------------------------------------------------------ interfaces
# The container sits on two networks: the internal one to the upstream server
# and the benchmark one the clients use. Only the benchmark side is delayed.
# The upstream side is found by routing to the upstream host; the benchmark
# side must then be the one remaining non-loopback interface.
UPSTREAM_HOST="${UPSTREAM_ADDR:-nntp-upstream:119}"
UPSTREAM_HOST="${UPSTREAM_HOST%:*}"
UPSTREAM_IP="$(getent ahostsv4 "$UPSTREAM_HOST" | awk 'NR==1 {print $1}')"
if [ -z "$UPSTREAM_IP" ]; then
    printf 'nntp-shaper: cannot resolve upstream host %s\n' "$UPSTREAM_HOST" >&2
    exit 64
fi
UPSTREAM_INTERFACE="$(ip -o route get "$UPSTREAM_IP" | sed -n 's/.* dev \([^ ]*\).*/\1/p' | head -n 1)"
if [ -z "$UPSTREAM_INTERFACE" ]; then
    printf 'nntp-shaper: no route to upstream %s\n' "$UPSTREAM_IP" >&2
    exit 64
fi
INTERFACE="${NNTP_SHAPED_INTERFACE:-}"
if [ -z "$INTERFACE" ]; then
    CANDIDATES="$(ip -o link show | awk -F': ' '{print $2}' | sed 's/@.*//' \
        | grep -v -e '^lo$' -e "^${UPSTREAM_INTERFACE}\$" -e '^ifb' || true)"
    if [ "$(printf '%s\n' "$CANDIDATES" | grep -c .)" -ne 1 ]; then
        printf 'nntp-shaper: expected exactly one benchmark-facing interface besides %s, found: %s\n' \
            "$UPSTREAM_INTERFACE" "$(printf '%s' "$CANDIDATES" | tr '\n' ' ')" >&2
        exit 64
    fi
    INTERFACE="$CANDIDATES"
fi

# Each direction carries half the round trip as a fixed delay with zero
# jitter, so the observed round trip is the declared one and a repeated run
# cannot drift. An odd microsecond, if any, lands on the egress side.
INGRESS_DELAY_MICROS=$((RTT_MICROS / 2))
EGRESS_DELAY_MICROS=$((RTT_MICROS - INGRESS_DELAY_MICROS))

# netem holds every in-flight packet for the delay. Its default limit (1000)
# would drop packets long before a gigabit link's bandwidth-delay product,
# so size it from the declared rate and round trip with a four-fold margin,
# and generously when the rate is unlimited.
if [ "$RATE_BITS" -gt 0 ]; then
    LIMIT_PACKETS=$((RATE_BITS / 8 / 1000 * RTT_MICROS / 1000000 * 4))
    if [ "$LIMIT_PACKETS" -lt 20000 ]; then
        LIMIT_PACKETS=20000
    fi
else
    LIMIT_PACKETS=1000000
fi

# ---------------------------------------------------------------- shaping
tc qdisc del dev "$INTERFACE" root 2>/dev/null || true
tc qdisc del dev "$INTERFACE" ingress 2>/dev/null || true
ip link del "$INGRESS_DEVICE" 2>/dev/null || true

# Server to client: netem as the root qdisc on the benchmark interface.
tc qdisc add dev "$INTERFACE" root handle 1: netem \
    delay "${EGRESS_DELAY_MICROS}us" 0us limit "$LIMIT_PACKETS"

# Client to server: Linux cannot delay ingress directly, so mirror it to an
# intermediate functional block device and delay that device's egress.
setup_ifb() {
    ip link add "$INGRESS_DEVICE" type ifb || return 1
    ip link set dev "$INGRESS_DEVICE" up || return 1
    tc qdisc add dev "$INTERFACE" handle ffff: ingress || return 1
    tc filter add dev "$INTERFACE" parent ffff: protocol all prio 1 u32 \
        match u32 0 0 action mirred egress redirect dev "$INGRESS_DEVICE" || return 1
    tc qdisc add dev "$INGRESS_DEVICE" root handle 1: netem \
        delay "${INGRESS_DELAY_MICROS}us" 0us limit "$LIMIT_PACKETS" || return 1
    return 0
}

# Without an ifb module the whole round trip is carried by the egress side:
# the sender still sees the declared round trip, but the client's commands
# and handshakes arrive undelayed. Which layout ran is recorded, never
# assumed, and the attestation names it.
if setup_ifb; then
    INGRESS_MECHANISM="ifb-netem"
    REPORTED_INGRESS_DEVICE="$INGRESS_DEVICE"
else
    printf 'nntp-shaper: ifb redirect unavailable (modprobe ifb on the host); carrying the whole round trip on egress\n' >&2
    ip link del "$INGRESS_DEVICE" 2>/dev/null || true
    tc qdisc del dev "$INTERFACE" ingress 2>/dev/null || true
    INGRESS_MECHANISM="none"
    REPORTED_INGRESS_DEVICE=""
    INGRESS_DELAY_MICROS=0
    EGRESS_DELAY_MICROS="$RTT_MICROS"
    tc qdisc replace dev "$INTERFACE" root handle 1: netem \
        delay "${EGRESS_DELAY_MICROS}us" 0us limit "$LIMIT_PACKETS"
fi

# ----------------------------------------------------------------- report
json_string() {
    printf '%s' "$1" | tr -d '"\\' | tr '\n\t' '  '
}
cat > "$REPORT_PATH" <<EOF_REPORT
{
  "schema_version": $REPORT_SCHEMA,
  "interface": "$INTERFACE",
  "ingress_device": "$REPORTED_INGRESS_DEVICE",
  "egress_mechanism": "netem",
  "ingress_mechanism": "$INGRESS_MECHANISM",
  "rtt_micros": $RTT_MICROS,
  "egress_delay_micros": $EGRESS_DELAY_MICROS,
  "ingress_delay_micros": $INGRESS_DELAY_MICROS,
  "netem_limit_packets": $LIMIT_PACKETS,
  "tcp_wmem": "$(json_string "$(cat /proc/sys/net/ipv4/tcp_wmem 2>/dev/null || echo unknown)")",
  "tcp_rmem": "$(json_string "$(cat /proc/sys/net/ipv4/tcp_rmem 2>/dev/null || echo unknown)")",
  "kernel_release": "$(json_string "$(uname -r)")",
  "live_egress_delay_micros": 0,
  "live_ingress_delay_micros": 0
}
EOF_REPORT

printf 'nntp-shaper: %sus round trip on %s (egress %sus, ingress %sus via %s, netem limit %s packets)\n' \
    "$RTT_MICROS" "$INTERFACE" "$EGRESS_DELAY_MICROS" "$INGRESS_DELAY_MICROS" "$INGRESS_MECHANISM" "$LIMIT_PACKETS" >&2

exec /nntpshaper "$@"
