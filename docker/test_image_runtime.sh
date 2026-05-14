#!/bin/sh
set -eu

IMAGE_TAG=${1:?image tag is required}
PLATFORM=${2:?platform is required}

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

owner_of() {
    if stat -c '%u:%g' "$1" >/dev/null 2>&1; then
        stat -c '%u:%g' "$1"
    else
        stat -f '%u:%g' "$1"
    fi
}

run_help() {
    docker run --rm --platform "$PLATFORM" "$IMAGE_TAG" --help
}

output=$(run_help)
assert_contains "$output" "Usenet binary downloader" "image should proxy --help output"

output=$(docker run --rm --platform "$PLATFORM" --read-only "$IMAGE_TAG" --help)
assert_contains "$output" "Usenet binary downloader" "read-only rootfs should still start"

output=$(
    docker run --rm --platform "$PLATFORM" \
        --read-only \
        --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
        "$IMAGE_TAG" \
        --help
)
assert_contains "$output" "Usenet binary downloader" "noexec tmpfs should still start"

current_uid=$(id -u)
current_gid=$(id -g)
tmpdir=$(mktemp -d)
trap 'sudo rm -rf "$tmpdir"' EXIT INT TERM

sudo chown 65534:65534 "$tmpdir"
docker run --rm --platform "$PLATFORM" \
    -e PUID="$current_uid" \
    -e PGID="$current_gid" \
    -v "$tmpdir:/config" \
    "$IMAGE_TAG" \
    --help >/dev/null

owner=$(owner_of "$tmpdir")
[ "$owner" = "$current_uid:$current_gid" ] || {
    printf 'assertion failed: root entrypoint path should chown /config\nexpected: %s\nactual: %s\n' \
        "$current_uid:$current_gid" "$owner" >&2
    exit 1
}

sudo chown 65534:65534 "$tmpdir"
docker run --rm --platform "$PLATFORM" \
    --user "$current_uid:$current_gid" \
    -e PUID=12345 \
    -e PGID=12345 \
    -v "$tmpdir:/config" \
    "$IMAGE_TAG" \
    --help >/dev/null

owner=$(owner_of "$tmpdir")
[ "$owner" = "65534:65534" ] || {
    printf 'assertion failed: non-root entrypoint path should skip chown\nexpected: 65534:65534\nactual: %s\n' "$owner" >&2
    exit 1
}

printf 'docker launcher image smoke tests passed for %s\n' "$PLATFORM"
