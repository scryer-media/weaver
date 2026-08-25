#!/bin/sh
set -eu

ARTIFACTS_DIR=${1:?artifacts directory is required}
CONTEXT_DIR=${2:?context directory is required}

rm -rf "$CONTEXT_DIR" docker-build

mkdir -p \
    "$CONTEXT_DIR/amd64" \
    "$CONTEXT_DIR/arm64" \
    docker-build/amd64 \
    docker-build/arm64

tar -xzf "$ARTIFACTS_DIR/weaver-linux-x86_64-portable.tar.gz" -C docker-build/amd64
tar -xzf "$ARTIFACTS_DIR/weaver-linux-arm64-portable.tar.gz" -C docker-build/arm64

install -m 0755 docker-build/amd64/weaver "$CONTEXT_DIR/amd64/weaver"
install -m 0755 docker-build/arm64/weaver "$CONTEXT_DIR/arm64/weaver"

test -x "$CONTEXT_DIR/amd64/weaver"
test -x "$CONTEXT_DIR/arm64/weaver"

cp docker/entrypoint.sh "$CONTEXT_DIR/"
