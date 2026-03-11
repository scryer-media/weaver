#!/bin/bash
# Regenerate the checked-in split 7z fixtures used by
# tests/sevenz_material_split.rs.
#
# Requirements:
# - ffmpeg on PATH
# - 7z on PATH
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_PASSWORD="TestPass123"
WORK="$(mktemp -d)"
cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$FIXTURE_DIR/originals"
rm -f "$FIXTURE_DIR"/generated_split_*.7z.*

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i testsrc2=size=960x540:rate=24 \
  -f lavfi -i sine=frequency=880:sample_rate=48000 \
  -t 4 \
  -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p \
  -c:a aac -b:a 160k -shortest \
  "$WORK/generated_split_clip.mkv"

cp "$WORK/generated_split_clip.mkv" "$FIXTURE_DIR/originals/generated_split_clip.mkv"

make_split() {
    local base="$1"
    shift
    7z a -bd -y "$@" "$WORK/${base}.7z" "$WORK/generated_split_clip.mkv" >/dev/null
    find "$WORK" -maxdepth 1 -type f -name "${base}.7z.*" -exec cp {} "$FIXTURE_DIR/" \;
    find "$WORK" -maxdepth 1 -type f -name "${base}.7z.*" -delete
}

make_split generated_split_store_plain -t7z -mx0 -v160k
make_split generated_split_lzma2_plain -t7z -mx9 -v160k
make_split generated_split_store_enc -t7z -mx0 -v160k -p"$TEST_PASSWORD" -mhe=on
make_split generated_split_lzma2_enc -t7z -mx9 -v160k -p"$TEST_PASSWORD" -mhe=on

echo "Generated split 7z fixtures."
