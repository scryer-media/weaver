#!/bin/sh
set -eu

ARTIFACTS_DIR=${1:?artifacts directory is required}
CONTEXT_DIR=${2:?context directory is required}

rm -rf "$CONTEXT_DIR" docker-build

mkdir -p \
    "$CONTEXT_DIR/amd64" \
    "$CONTEXT_DIR/arm64" \
    docker-build/amd64-portable \
    docker-build/amd64-haswell \
    docker-build/arm64-portable \
    docker-build/arm64-cortex-a76

tar -xzf "$ARTIFACTS_DIR/weaver-linux-x86_64-portable.tar.gz" -C docker-build/amd64-portable
tar -xzf "$ARTIFACTS_DIR/weaver-linux-x86_64-haswell.tar.gz" -C docker-build/amd64-haswell
tar -xzf "$ARTIFACTS_DIR/weaver-linux-arm64-portable.tar.gz" -C docker-build/arm64-portable
tar -xzf "$ARTIFACTS_DIR/weaver-linux-arm64-cortex-a76.tar.gz" -C docker-build/arm64-cortex-a76

install -m 0755 docker-build/amd64-portable/weaver "$CONTEXT_DIR/amd64/weaver-portable"
install -m 0755 docker-build/amd64-haswell/weaver "$CONTEXT_DIR/amd64/weaver-haswell"
install -m 0755 docker-build/arm64-portable/weaver "$CONTEXT_DIR/arm64/weaver-portable"
install -m 0755 docker-build/arm64-cortex-a76/weaver "$CONTEXT_DIR/arm64/weaver-cortex-a76"

test -x "$CONTEXT_DIR/amd64/weaver-portable"
test -x "$CONTEXT_DIR/amd64/weaver-haswell"
test -x "$CONTEXT_DIR/arm64/weaver-portable"
test -x "$CONTEXT_DIR/arm64/weaver-cortex-a76"

cp docker/entrypoint.sh docker/runtime-select.sh "$CONTEXT_DIR/"
