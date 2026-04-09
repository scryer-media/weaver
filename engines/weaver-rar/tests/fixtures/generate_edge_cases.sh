#!/bin/bash
# Generate edge-case RAR test fixtures using Docker RAR images.
#
# Requirements: docker images `rar:latest` (RAR5/7.20) and `rar:4` (RAR4/6.24)
# Both images have `rar` as their Docker ENTRYPOINT, so we pass args directly.
#
# Usage: cd engines/weaver-rar/tests/fixtures && bash generate_edge_cases.sh
set -euo pipefail

FIXTURE_DIR="$(cd "$(dirname "$0")" && pwd)"
RAR5_DIR="$FIXTURE_DIR/rar5"
RAR4_DIR="$FIXTURE_DIR/rar4"

mkdir -p "$RAR5_DIR" "$RAR4_DIR"

# Helper: run RAR5 (7.20) inside Docker (entrypoint = rar)
rar5() {
    docker run --rm --platform linux/amd64 -v "$FIXTURE_DIR:/work" -w /work rar:latest "$@"
}

# Helper: run RAR4 (6.24) inside Docker (entrypoint = rar)
rar4() {
    docker run --rm --platform linux/amd64 -v "$FIXTURE_DIR:/work" -w /work rar:4 "$@"
}

# Create source files for edge cases inside a temp dir that Docker can see
SRCDIR="$FIXTURE_DIR/_edge_src"
rm -rf "$SRCDIR"
mkdir -p "$SRCDIR/subdir/nested"

# --- Source files ---
echo "Hello, world!" > "$SRCDIR/hello.txt"
echo "Second file content here" > "$SRCDIR/second.txt"
printf '' > "$SRCDIR/empty.txt"
# Unicode filenames
echo "日本語テスト" > "$SRCDIR/日本語ファイル.txt"
echo "Emoji content 🎉" > "$SRCDIR/café-résumé.txt"
# Nested directory files
echo "Nested file A" > "$SRCDIR/subdir/a.txt"
echo "Deeply nested B" > "$SRCDIR/subdir/nested/b.txt"
# Compressible data for solid testing
dd if=/dev/zero bs=1024 count=64 2>/dev/null > "$SRCDIR/zeros_64k.bin"
# Small binary
head -c 512 /dev/urandom > "$SRCDIR/random_512.bin"
# 4K binary for tiny-volume tests
head -c 4096 /dev/urandom > "$SRCDIR/random_4k.bin"
# File with very long name (200 chars)
LONGNAME="$(printf 'a%.0s' {1..200}).txt"
echo "long filename test" > "$SRCDIR/$LONGNAME"
# Comment file
echo "This is a test comment for the archive" > "$SRCDIR/_comment.txt"
# Symlink
ln -sf hello.txt "$SRCDIR/link_to_hello.txt"

echo "=== Generating RAR5 edge-case fixtures ==="

echo "  [1/12] multi-file stored"
rar5 a -m0 -ep1 rar5/rar5_multifile_store.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/random_512.bin

echo "  [2/12] multi-file compressed"
rar5 a -m3 -ep1 rar5/rar5_multifile_lz.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/zeros_64k.bin

echo "  [3/12] directory structure"
rar5 a -m0 -r rar5/rar5_dirs.rar _edge_src/subdir/

echo "  [4/12] empty file member"
rar5 a -m0 -ep1 rar5/rar5_empty_member.rar _edge_src/empty.txt _edge_src/hello.txt

echo "  [5/12] unicode filenames (RAR5 only — RAR4 Docker locale breaks unicode)"
rar5 a -m0 -ep1 rar5/rar5_unicode.rar "_edge_src/日本語ファイル.txt" "_edge_src/café-résumé.txt"

echo "  [6/12] solid archive"
rar5 a -m3 -s -ep1 rar5/rar5_solid.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/zeros_64k.bin _edge_src/random_512.bin

echo "  [7/12] recovery record"
rar5 a -m0 -rr5p -ep1 rar5/rar5_recovery.rar _edge_src/hello.txt _edge_src/second.txt

echo "  [8/12] comment archive"
rar5 a -m0 -ep1 -z_edge_src/_comment.txt rar5/rar5_comment.rar _edge_src/hello.txt

echo "  [9/12] many small volumes (4KB file → 5 × 1KB volumes)"
rar5 a -m0 -v1k -ep1 rar5/rar5_tiny_volumes.rar _edge_src/random_4k.bin

echo "  [10/12] compression level best"
rar5 a -m5 -ep1 rar5/rar5_best.rar _edge_src/zeros_64k.bin

echo "  [11/12] long filename (200 chars)"
rar5 a -m0 -ep1 rar5/rar5_longname.rar "_edge_src/$LONGNAME"

echo "  [12/12] symlink"
rar5 a -m0 -ol -ep1 rar5/rar5_symlink.rar _edge_src/link_to_hello.txt _edge_src/hello.txt

echo ""
echo "=== Generating RAR4 edge-case fixtures ==="

echo "  [1/8] multi-file stored"
rar4 a -ma4 -m0 -ep1 rar4/rar4_multifile_store.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/random_512.bin

echo "  [2/8] multi-file compressed"
rar4 a -ma4 -m3 -ep1 rar4/rar4_multifile_lz.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/zeros_64k.bin

echo "  [3/8] directory structure"
rar4 a -ma4 -m0 -r rar4/rar4_dirs.rar _edge_src/subdir/

echo "  [4/8] empty file member"
rar4 a -ma4 -m0 -ep1 rar4/rar4_empty_member.rar _edge_src/empty.txt _edge_src/hello.txt

echo "  [5/8] solid archive"
rar4 a -ma4 -m3 -s -ep1 rar4/rar4_solid.rar _edge_src/hello.txt _edge_src/second.txt _edge_src/zeros_64k.bin _edge_src/random_512.bin

echo "  [6/8] recovery record"
rar4 a -ma4 -m0 -rr5p -ep1 rar4/rar4_recovery.rar _edge_src/hello.txt _edge_src/second.txt

echo "  [7/8] comment archive"
rar4 a -ma4 -m0 -ep1 -z_edge_src/_comment.txt rar4/rar4_comment.rar _edge_src/hello.txt

echo "  [8/8] long filename (200 chars)"
rar4 a -ma4 -m0 -ep1 rar4/rar4_longname.rar "_edge_src/$LONGNAME"

echo ""
echo "=== Saving originals and cleaning up ==="
cp _edge_src/random_4k.bin originals/
cp _edge_src/hello.txt originals/
cp _edge_src/second.txt originals/
cp _edge_src/zeros_64k.bin originals/
cp _edge_src/random_512.bin originals/
cp _edge_src/empty.txt originals/
cp "_edge_src/日本語ファイル.txt" originals/
cp "_edge_src/café-résumé.txt" originals/
cp _edge_src/subdir/a.txt originals/subdir_a.txt
cp _edge_src/subdir/nested/b.txt originals/subdir_nested_b.txt
cp "_edge_src/$LONGNAME" "originals/$LONGNAME"
rm -rf _edge_src

echo "=== Done! ==="
