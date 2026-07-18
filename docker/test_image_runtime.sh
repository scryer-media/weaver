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

mode_of() {
    if stat -c '%a' "$1" >/dev/null 2>&1; then
        stat -c '%a' "$1"
    else
        stat -f '%Lp' "$1"
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
payload_dir=$(mktemp -d)
key_container="weaver-key-persistence-$$"
cleanup() {
    docker rm -f "$key_container" >/dev/null 2>&1 || true
    sudo rm -rf "$tmpdir" "$payload_dir"
}
trap cleanup EXIT INT TERM
chmod 0755 "$payload_dir"

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

start_key_test_container() {
    docker run -d --name "$key_container" --platform "$PLATFORM" \
        -e PUID="$current_uid" \
        -e PGID="$current_gid" \
        -v "$tmpdir:/config" \
        "$IMAGE_TAG" >/dev/null
}

wait_for_key_file() {
    attempts=0
    while [ ! -s "$tmpdir/encryption.key" ]; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 100 ]; then
            docker logs "$key_container" >&2 || true
            printf 'assertion failed: container did not create /config/encryption.key\n' >&2
            exit 1
        fi
        sleep 0.1
    done
}

wait_for_container_log() {
    expected_log=$1
    attempts=0
    while :; do
        container_logs=$(docker logs "$key_container" 2>&1 || true)
        case "$container_logs" in
            *"$expected_log"*) return ;;
        esac
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 100 ]; then
            printf 'assertion failed: container log did not contain: %s\n%s\n' \
                "$expected_log" "$container_logs" >&2
            exit 1
        fi
        sleep 0.1
    done
}

start_key_test_container
wait_for_key_file
wait_for_container_log "persisted encryption master key in key file"
first_key=$(tr -d '\r\n' <"$tmpdir/encryption.key")
[ "$(mode_of "$tmpdir/encryption.key")" = "600" ] || {
    printf 'assertion failed: /config/encryption.key should have mode 600\n' >&2
    exit 1
}
docker rm -f "$key_container" >/dev/null

start_key_test_container
wait_for_key_file
wait_for_container_log "using encryption master key from key file"
second_key=$(tr -d '\r\n' <"$tmpdir/encryption.key")
[ "$second_key" = "$first_key" ] || {
    printf 'assertion failed: container restart should reuse /config/encryption.key\n' >&2
    exit 1
}
docker rm -f "$key_container" >/dev/null

cat >"$payload_dir/weaver-portable" <<'EOF'
#!/bin/sh
id -G
EOF
chmod +x "$payload_dir/weaver-portable"

groups_output=$(
    docker run --rm --platform "$PLATFORM" \
        -e PUID=12345 \
        -e PGID=12345 \
        -e WEAVER_PAYLOAD_ROOT=/payload \
        -e WEAVER_CPUINFO_PATH=/missing-cpuinfo \
        -v "$payload_dir:/payload:ro" \
        "$IMAGE_TAG"
)
groups=$(printf '%s\n' "$groups_output" | tail -n 1 | tr -d '\r')
[ "$groups" = "12345" ] || {
    printf 'assertion failed: root entrypoint path should clear supplementary groups\nexpected groups: 12345\nactual groups: %s\nfull output:\n%s\n' \
        "$groups" "$groups_output" >&2
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
