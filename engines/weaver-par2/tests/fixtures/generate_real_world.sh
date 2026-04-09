#!/bin/bash
# Regenerate the checked-in PAR2 repair fixtures used by
# tests/real_world_generated.rs.
#
# Requirements:
# - ffmpeg on PATH
# - par2 on PATH
# - docker on PATH
# - local images: rar:latest, rar:4
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$FIXTURE_DIR/source" "$FIXTURE_DIR/rar5_lz_plain" "$FIXTURE_DIR/rar4_store_enc"

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i testsrc2=size=960x540:rate=24 \
  -f lavfi -i sine=frequency=880:sample_rate=48000 \
  -t 4 \
  -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p \
  -c:a aac -b:a 160k -shortest \
  "$WORK/generated_sample_clip.mkv"

cp "$WORK/generated_sample_clip.mkv" "$FIXTURE_DIR/source/generated_sample_clip.mkv"

make_set() {
    local image="$1"
    local maflag="$2"
    local mode_flag="$3"
    local encflag="$4"
    local stem="$5"
    local outdir="$6"

    rm -f "$outdir"/*
    cp "$WORK/generated_sample_clip.mkv" "$WORK/input_clip.mkv"

    if [ -n "$maflag" ]; then
        if [ -n "$encflag" ]; then
            docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                a -idq "$maflag" "$mode_flag" -ep1 -v192k "$encflag" \
                "$stem.rar" input_clip.mkv >/dev/null
        else
            docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                a -idq "$maflag" "$mode_flag" -ep1 -v192k \
                "$stem.rar" input_clip.mkv >/dev/null
        fi
    else
        if [ -n "$encflag" ]; then
            docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                a -idq "$mode_flag" -ep1 -v192k "$encflag" \
                "$stem.rar" input_clip.mkv >/dev/null
        else
            docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                a -idq "$mode_flag" -ep1 -v192k \
                "$stem.rar" input_clip.mkv >/dev/null
        fi
    fi

    par2 create -q -s65536 -c12 -n6 "${stem}_repair.par2" "${stem}"*.rar >/dev/null

    find "$WORK" -maxdepth 1 -type f \( -name "${stem}*.rar" -o -name "${stem}_repair*.par2" \) \
        -exec cp {} "$outdir/" \;
    find "$WORK" -maxdepth 1 -type f \( -name "${stem}*.rar" -o -name "${stem}_repair*.par2" \) \
        -delete
    rm -f "$WORK/input_clip.mkv"
}

make_set "rar:latest" "" "-m5" "" "fixture_rar5_lz_plain" "$FIXTURE_DIR/rar5_lz_plain"
make_set "rar:4" "-ma4" "-m0" "-ptestpass123" "fixture_rar4_store_enc" "$FIXTURE_DIR/rar4_store_enc"

echo "Generated PAR2 real-world fixtures."
echo ""
echo "NOTE: rar5_heavy_damage is generated separately via generate_heavy_damage.sh"
