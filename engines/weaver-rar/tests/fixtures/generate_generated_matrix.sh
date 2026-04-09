#!/bin/bash
# Regenerate the checked-in generated matrix fixtures used by
# tests/generated_multivolume_matrix.rs.
#
# Requirements:
# - ffmpeg on PATH
# - docker on PATH
# - local images: rar:latest, rar:4
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT

ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i testsrc2=size=960x540:rate=24 \
  -f lavfi -i sine=frequency=880:sample_rate=48000 \
  -t 4 \
  -c:v libx264 -preset veryfast -crf 18 -pix_fmt yuv420p \
  -c:a aac -b:a 160k -shortest \
  "$WORK/generated_matrix_clip.mkv"

cp "$WORK/generated_matrix_clip.mkv" "$FIXTURE_DIR/originals/generated_matrix_clip.mkv"

for flavor in rar5 rar4; do
    if [ "$flavor" = "rar5" ]; then
        image="rar:latest"
        maflag=""
    else
        image="rar:4"
        maflag="-ma4"
    fi

    for mode in store lz; do
        if [ "$mode" = "store" ]; then
            mode_flag="-m0"
        else
            mode_flag="-m5"
        fi

        for enc in plain enc; do
            base="generated_matrix_${flavor}_${mode}_${enc}"
            rm -f "$FIXTURE_DIR/$flavor/${base}"*.rar
            if [ -n "$maflag" ]; then
                if [ "$enc" = "enc" ]; then
                    docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                        a -idq "$maflag" "$mode_flag" -ep1 -v160k -ptestpass123 \
                        "$base.rar" generated_matrix_clip.mkv >/dev/null
                else
                    docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                        a -idq "$maflag" "$mode_flag" -ep1 -v160k \
                        "$base.rar" generated_matrix_clip.mkv >/dev/null
                fi
            else
                if [ "$enc" = "enc" ]; then
                    docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                        a -idq "$mode_flag" -ep1 -v160k -ptestpass123 \
                        "$base.rar" generated_matrix_clip.mkv >/dev/null
                else
                    docker run --rm --platform linux/amd64 -v "$WORK:/work" -w /work "$image" \
                        a -idq "$mode_flag" -ep1 -v160k \
                        "$base.rar" generated_matrix_clip.mkv >/dev/null
                fi
            fi

            find "$WORK" -maxdepth 1 -type f -name "${base}*.rar" -exec cp {} "$FIXTURE_DIR/$flavor/" \;
            find "$WORK" -maxdepth 1 -type f -name "${base}*.rar" -delete
        done
    done
done

echo "Generated multivolume matrix fixtures."
