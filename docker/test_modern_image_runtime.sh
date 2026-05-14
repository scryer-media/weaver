#!/bin/sh
set -eu

IMAGE_TAG=${1:?image tag is required}
PLATFORM=${2:?platform is required}
ARCH=${3:?arch is required}

assert_contains() {
    haystack=$1
    needle=$2
    message=$3

    case "$haystack" in
        *"$needle"*) ;;
        *)
            printf 'assertion failed: %s\nmissing: %s\n' "$message" "$needle" >&2
            exit 1
            ;;
    esac
}

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT INT TERM

case "$ARCH" in
    amd64)
        cat >"$tmpdir/supported.cpuinfo" <<'EOF'
flags		: avx avx2 bmi1 bmi2 f16c fma abm movbe pclmulqdq popcnt rdrand sse3 sse4_1 sse4_2 ssse3 xsave xsaveopt
EOF
        cat >"$tmpdir/unsupported.cpuinfo" <<'EOF'
flags		: popcnt sse3 sse4_1 sse4_2 ssse3
EOF
        ;;
    arm64)
        cat >"$tmpdir/supported.cpuinfo" <<'EOF'
Features	: fp asimd evtstrm aes sha2 crc32 atomics fphp asimdrdm asimddp
EOF
        cat >"$tmpdir/unsupported.cpuinfo" <<'EOF'
Features	: fp asimd evtstrm aes sha2
EOF
        ;;
    *)
        printf 'unsupported arch for modern image smoke test: %s\n' "$ARCH" >&2
        exit 1
        ;;
esac

output=$(
    docker run --rm --platform "$PLATFORM" \
        -e WEAVER_CONTAINER_ARCH="$ARCH" \
        -e WEAVER_CPUINFO_PATH=/fixtures/supported.cpuinfo \
        -v "$tmpdir:/fixtures:ro" \
        "$IMAGE_TAG" \
        --help
)
assert_contains "$output" "Usenet binary downloader" "modern image should start on a supported host"

set +e
docker run --rm --platform "$PLATFORM" \
    -e WEAVER_CONTAINER_ARCH="$ARCH" \
    -e WEAVER_CPUINFO_PATH=/fixtures/unsupported.cpuinfo \
    -v "$tmpdir:/fixtures:ro" \
    "$IMAGE_TAG" \
    --help >"$tmpdir/unsupported.stdout" 2>"$tmpdir/unsupported.stderr"
status=$?
set -e

[ "$status" -ne 0 ] || {
    printf 'assertion failed: modern image should fail on an unsupported host\n' >&2
    exit 1
}

stderr_output=$(cat "$tmpdir/unsupported.stderr")
assert_contains "$stderr_output" "does not satisfy it" "modern image should explain the unsupported CPU error"
assert_contains "$stderr_output" "latest-slim" "modern image should point users at the slim tag"
assert_contains "$stderr_output" "ghcr.io/scryer-media/weaver:latest" "modern image should point users at the base tag"

printf 'docker modern image smoke tests passed for %s\n' "$PLATFORM"
