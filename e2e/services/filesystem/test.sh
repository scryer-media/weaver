#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
set -eu

# Standalone bounded filesystem lane; no existing Compose project is used.
# Run: sh e2e/services/filesystem/test.sh
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/weaver-fs-fixture.XXXXXXXX")
run_id=$(basename "$scratch")
image="weaver-fs-fixture:$run_id"
engine=${CONTAINER_ENGINE:-docker}
label=org.scryer-media.weaver.fs-fixture-run

cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ -s "$scratch/container-id" ]; then
        id=$(cat "$scratch/container-id")
        owner=$("$engine" inspect --format "{{ index .Config.Labels \"$label\" }}" "$id")
        if [ "$owner" != "$run_id" ]; then
            printf 'refusing to remove a container without this run ownership label: %s\n' "$id" >&2
            exit 1
        fi
        "$engine" rm -f "$id"
    fi
    "$engine" image rm "$image" >/dev/null 2>&1 || true
    rm -rf -- "$scratch"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

toolchain=$(python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1],"rb"))["toolchain"]["channel"])' "$root/rust-toolchain.toml")
"$engine" build --load --build-arg "WEAVER_IMAGE=rust:$toolchain-bookworm" \
    -t "$image" "$root/e2e/services/filesystem"
"$engine" create --cidfile "$scratch/container-id" --label "$label=$run_id" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined --security-opt label=disable \
    --mount "type=bind,source=$root,target=/workspace,readonly" \
    --entrypoint python3 "$image" /fs-fixture/regressions.py
id=$(cat "$scratch/container-id")
"$engine" start "$id"
"$engine" logs --follow "$id"
running=$("$engine" inspect --format '{{.State.Running}}' "$id")
if [ "$running" != "false" ]; then
    printf 'log stream ended before the filesystem test container exited\n' >&2
    exit 1
fi
code=$("$engine" inspect --format '{{.State.ExitCode}}' "$id")
exit "$code"
