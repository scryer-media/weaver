#!/usr/bin/env bash
# Build the pinned e2e oracle images a corpus-generation job needs, and no
# others. `ONLY` is a space-separated list of toolchain ids (from
# `fixturegen --list-json`); a pin whose id is not in the list is skipped, the
# way rarpar's `bench toolchains build --only-images-for` skips. Run from the
# e2e directory.
#
# Every image verifies the reviewed SHA-256 of its download before installing
# anything, and the generator re-verifies the same digest in Go before it
# calls docker at all.
#
# The runner is amd64, which is what every image pins, so nothing is emulated:
# the RARLAB 3.93/4.20/5.00 releases are 32-bit x86 binaries and the kernel's
# IA-32 support runs them directly. A developer regenerating on an arm64 host
# needs QEMU binfmt handlers for amd64 and 386 instead — see
# e2e/docs/generators.md.
set -euo pipefail

if [ -z "${ONLY:-}" ]; then
  echo "::error::ONLY must name the toolchain ids this job builds" >&2
  exit 1
fi

lock=test-corpus/toolchains.json
wanted=" $ONLY "

jq -c '[.rar_writers[], .archivers[], .par2_generator] | .[] | select(.dockerfile != null)' "$lock" \
| while read -r pin; do
    id=$(echo "$pin" | jq -r .id)
    case "$wanted" in
      *" $id "*) ;;
      *) echo "skipping $id (not needed by this job)"; continue ;;
    esac
    dockerfile=$(echo "$pin" | jq -r .dockerfile)
    case "$dockerfile" in
      */rarlab/*)
        args=(--build-arg "RAR_URL=$(echo "$pin" | jq -r .url)"
              --build-arg "RAR_SHA256=$(echo "$pin" | jq -r .sha256)"
              --build-arg "RAR_BINARY=$(echo "$pin" | jq -r .binary)") ;;
      */par2/*)
        args=(--build-arg "PAR2_URL=$(echo "$pin" | jq -r .url)"
              --build-arg "PAR2_SHA256=$(echo "$pin" | jq -r .sha256)") ;;
      */sevenzip/*)
        args=(--build-arg "SEVENZIP_URL=$(echo "$pin" | jq -r .url)"
              --build-arg "SEVENZIP_SHA256=$(echo "$pin" | jq -r .sha256)") ;;
      *)
        echo "::error::toolchain $id names an unknown Dockerfile $dockerfile"
        exit 1 ;;
    esac
    echo "::group::$id"
    docker buildx build \
      --platform "$(echo "$pin" | jq -r .platform)" \
      --tag "$(echo "$pin" | jq -r .image)" \
      --file "$dockerfile" \
      "${args[@]}" \
      --cache-from "type=gha,scope=corpus-$id" \
      --cache-to "type=gha,mode=max,scope=corpus-$id" \
      --load "$(dirname "$dockerfile")"
    echo "::endgroup::"
  done

encoder_id=$(jq -r .video_encoder.id "$lock")
case "$wanted" in
  *" $encoder_id "*)
    docker pull --platform "$(jq -r .video_encoder.platform "$lock")" "$(jq -r .video_encoder.image "$lock")"
    ;;
  *)
    echo "skipping $encoder_id (not needed by this job)"
    ;;
esac
