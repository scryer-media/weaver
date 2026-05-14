#!/bin/sh
set -eu

ARTIFACTS_DIR=${1:?artifacts directory is required}
CONTEXT_DIR=${2:?context directory is required}

rm -rf "$CONTEXT_DIR" docker-build

mkdir -p \
    "$CONTEXT_DIR/amd64/payloads" \
    "$CONTEXT_DIR/arm64/payloads" \
    docker-build/amd64-portable \
    docker-build/amd64-haswell \
    docker-build/arm64-portable \
    docker-build/arm64-cortex-a76

tar -xzf "$ARTIFACTS_DIR/weaver-linux-x86_64-portable.tar.gz" -C docker-build/amd64-portable
tar -xzf "$ARTIFACTS_DIR/weaver-linux-x86_64-haswell.tar.gz" -C docker-build/amd64-haswell
tar -xzf "$ARTIFACTS_DIR/weaver-linux-arm64-portable.tar.gz" -C docker-build/arm64-portable
tar -xzf "$ARTIFACTS_DIR/weaver-linux-arm64-cortex-a76.tar.gz" -C docker-build/arm64-cortex-a76

zstd -19 -T0 -f docker-build/amd64-portable/weaver -o "$CONTEXT_DIR/amd64/payloads/weaver-portable.zst"
zstd -19 -T0 -f docker-build/amd64-haswell/weaver -o "$CONTEXT_DIR/amd64/payloads/weaver-haswell.zst"
zstd -19 -T0 -f docker-build/arm64-portable/weaver -o "$CONTEXT_DIR/arm64/payloads/weaver-portable.zst"
zstd -19 -T0 -f docker-build/arm64-cortex-a76/weaver -o "$CONTEXT_DIR/arm64/payloads/weaver-cortex-a76.zst"

install -m 0755 "$ARTIFACTS_DIR/weaver-docker-launcher-linux-amd64" "$CONTEXT_DIR/amd64/weaver-docker-launcher"
install -m 0755 "$ARTIFACTS_DIR/weaver-docker-launcher-linux-arm64" "$CONTEXT_DIR/arm64/weaver-docker-launcher"

test -x "$CONTEXT_DIR/amd64/weaver-docker-launcher"
test -x "$CONTEXT_DIR/arm64/weaver-docker-launcher"
test -f "$CONTEXT_DIR/amd64/payloads/weaver-portable.zst"
test -f "$CONTEXT_DIR/amd64/payloads/weaver-haswell.zst"
test -f "$CONTEXT_DIR/arm64/payloads/weaver-portable.zst"
test -f "$CONTEXT_DIR/arm64/payloads/weaver-cortex-a76.zst"

cp docker/test_image_runtime.sh "$CONTEXT_DIR/"
