#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$SCRIPT_DIR/runtime-select.sh"

assert_eq() {
    expected=$1
    actual=$2
    message=$3

    if [ "$expected" != "$actual" ]; then
        printf 'assertion failed: %s\nexpected: %s\nactual: %s\n' "$message" "$expected" "$actual" >&2
        exit 1
    fi
}

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT INT TERM

cat <<'EOF' > "$tmpdir/amd64-haswell.cpuinfo"
processor : 0
vendor_id : GenuineIntel
flags     : fpu sse3 ssse3 sse4_1 sse4_2 avx avx2 bmi1 bmi2 f16c fma lzcnt movbe pclmulqdq popcnt rdrand xsave xsaveopt
EOF

cat <<'EOF' > "$tmpdir/amd64-haswell-abm.cpuinfo"
processor : 0
vendor_id : GenuineIntel
flags     : fpu sse3 ssse3 sse4_1 sse4_2 avx avx2 bmi1 bmi2 f16c fma abm movbe pclmulqdq popcnt rdrand xsave xsaveopt
EOF

cat <<'EOF' > "$tmpdir/amd64-portable.cpuinfo"
processor : 0
vendor_id : GenuineIntel
flags     : fpu sse3 ssse3 sse4_1 sse4_2 avx bmi1 bmi2 f16c fma lzcnt movbe pclmulqdq popcnt rdrand xsave xsaveopt
EOF

cat <<'EOF' > "$tmpdir/arm64-cortex-a76.cpuinfo"
processor : 0
Features  : fp asimd aes sha2 crc32 atomics fphp asimdrdm asimddp
EOF

cat <<'EOF' > "$tmpdir/arm64-portable.cpuinfo"
processor : 0
Features  : fp asimd aes sha2 crc32 atomics fphp asimdrdm
EOF

assert_eq "haswell" "$(select_linux_lane_for_arch amd64 "$tmpdir/amd64-haswell.cpuinfo")" "amd64 selects haswell when all required features are present"
assert_eq "haswell" "$(select_linux_lane_for_arch amd64 "$tmpdir/amd64-haswell-abm.cpuinfo")" "amd64 treats abm as lzcnt for haswell selection"
assert_eq "portable" "$(select_linux_lane_for_arch amd64 "$tmpdir/amd64-portable.cpuinfo")" "amd64 falls back to portable when a required feature is missing"
assert_eq "cortex-a76" "$(select_linux_lane_for_arch arm64 "$tmpdir/arm64-cortex-a76.cpuinfo")" "arm64 selects cortex-a76 when all required features are present"
assert_eq "portable" "$(select_linux_lane_for_arch arm64 "$tmpdir/arm64-portable.cpuinfo")" "arm64 falls back to portable when a required feature is missing"
assert_eq "portable" "$(select_linux_lane_for_arch amd64 "$tmpdir/missing.cpuinfo")" "missing cpuinfo falls back to portable"

assert_eq "weaver-haswell" "$(selected_payload_name amd64 "$tmpdir/amd64-haswell.cpuinfo")" "amd64 haswell payload name"
assert_eq "weaver-cortex-a76" "$(selected_payload_name arm64 "$tmpdir/arm64-cortex-a76.cpuinfo")" "arm64 optimized payload name"
assert_eq "weaver-portable" "$(selected_payload_name arm64 "$tmpdir/missing.cpuinfo")" "portable payload name on unreadable cpuinfo"

printf 'docker runtime selection tests passed\n'
