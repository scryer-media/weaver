#!/bin/bash
# Generate the rar5_heavy_damage fixture: ~80MB RAR with 30 recovery blocks
# at 64KB slices. Designed to push PAR2 repair to its limits.
#
# The fixture is stored in Git LFS.
#
# Requirements:
# - ffmpeg on PATH
# - par2 on PATH
# - docker image: e2e-archive-tools:latest (contains rar binary)
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTDIR="$FIXTURE_DIR/rar5_heavy_damage"
WORK="$(mktemp -d)"
cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

echo "==> Generating ~80MB source video..."
# 50s of 1080p test pattern at CRF 14 — produces ~73MB after RAR compression.
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i testsrc2=size=1920x1080:rate=30 \
  -f lavfi -i sine=frequency=440:sample_rate=48000 \
  -t 50 \
  -c:v libx264 -preset veryfast -crf 14 -pix_fmt yuv420p \
  -c:a aac -b:a 192k -shortest \
  "$WORK/heavy_damage_clip.mkv"

SRC_SIZE=$(stat -f%z "$WORK/heavy_damage_clip.mkv" 2>/dev/null || stat -c%s "$WORK/heavy_damage_clip.mkv")
echo "    source: $(( SRC_SIZE / 1024 / 1024 ))MB"

echo "==> Creating RAR5 archive (single volume, LZ compression)..."
docker run --rm --platform linux/amd64 \
  -v "$WORK:/work" -w /work \
  e2e-archive-tools:latest \
  rar a -idq -m5 -ep1 fixture_rar5_heavy_damage.rar heavy_damage_clip.mkv

RAR_SIZE=$(stat -f%z "$WORK/fixture_rar5_heavy_damage.rar" 2>/dev/null || stat -c%s "$WORK/fixture_rar5_heavy_damage.rar")
echo "    archive: $(( RAR_SIZE / 1024 / 1024 ))MB"

# 64KB slices, 500 recovery blocks.
# ~73MB / 64KB = ~1163 slices. 500 recovery blocks = can repair up to 500 damaged slices.
echo "==> Creating PAR2 recovery set (64KB slices, 500 recovery blocks)..."
cd "$WORK"
par2 create -q -s65536 -c500 -n10 \
  fixture_rar5_heavy_damage_repair.par2 \
  fixture_rar5_heavy_damage.rar

PAR2_TOTAL=0
for f in fixture_rar5_heavy_damage_repair*.par2; do
    sz=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f")
    PAR2_TOTAL=$(( PAR2_TOTAL + sz ))
done
echo "    recovery: $(( PAR2_TOTAL / 1024 / 1024 ))MB across $(ls fixture_rar5_heavy_damage_repair*.par2 | wc -l | tr -d ' ') files"

TOTAL_SLICES=$(( (RAR_SIZE + 65535) / 65536 ))
echo "    slices: $TOTAL_SLICES (30 recoverable)"

echo "==> Copying to fixture directory..."
rm -f "$OUTDIR"/*
cp fixture_rar5_heavy_damage.rar "$OUTDIR/"
cp fixture_rar5_heavy_damage_repair*.par2 "$OUTDIR/"

echo "==> Done."
echo ""
ls -lh "$OUTDIR/"
echo ""
echo "Fixture ready. Add to LFS with:"
echo "  cd $(dirname "$FIXTURE_DIR") && git lfs track 'tests/fixtures/rar5_heavy_damage/*.rar' 'tests/fixtures/rar5_heavy_damage/*.par2'"
