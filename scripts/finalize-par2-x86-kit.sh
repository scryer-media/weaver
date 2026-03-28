#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Inject the pipeline-built Linux x86_64 Weaver binary into an existing PAR2 x86 kit,
then optionally repack the tarball.

Expected Weaver artifact shape:
  - CI artifact tarball named like `weaver-linux-x86_64.tar.gz`
  - tarball contains a top-level `weaver` binary

Usage:
  scripts/finalize-par2-x86-kit.sh \
    --kit-dir /path/to/weaver-par2-x86-kit-20260327 \
    --weaver-artifact /path/to/weaver-linux-x86_64.tar.gz \
    [--output-tar /path/to/weaver-par2-x86-kit-20260327.tar.gz]
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

KIT_DIR=""
WEAVER_ARTIFACT=""
OUTPUT_TAR=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --kit-dir)
      KIT_DIR="$2"
      shift 2
      ;;
    --weaver-artifact)
      WEAVER_ARTIFACT="$2"
      shift 2
      ;;
    --output-tar)
      OUTPUT_TAR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[ -n "$KIT_DIR" ] || die "--kit-dir is required"
[ -n "$WEAVER_ARTIFACT" ] || die "--weaver-artifact is required"
[ -d "$KIT_DIR" ] || die "kit directory not found: $KIT_DIR"
[ -f "$WEAVER_ARTIFACT" ] || die "weaver artifact not found: $WEAVER_ARTIFACT"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

tar -xzf "$WEAVER_ARTIFACT" -C "$TMP_DIR"
[ -f "$TMP_DIR/weaver" ] || die "artifact did not contain a top-level 'weaver' binary"

mkdir -p "$KIT_DIR/bin/weaver-linux-x86_64"
cp "$TMP_DIR/weaver" "$KIT_DIR/bin/weaver-linux-x86_64/weaver"
chmod +x "$KIT_DIR/bin/weaver-linux-x86_64/weaver"

ARTIFACT_SHA=""
WEAVER_SHA=""
if command -v shasum >/dev/null 2>&1; then
  ARTIFACT_SHA="$(shasum -a 256 "$WEAVER_ARTIFACT" | awk '{print $1}')"
  WEAVER_SHA="$(shasum -a 256 "$KIT_DIR/bin/weaver-linux-x86_64/weaver" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  ARTIFACT_SHA="$(sha256sum "$WEAVER_ARTIFACT" | awk '{print $1}')"
  WEAVER_SHA="$(sha256sum "$KIT_DIR/bin/weaver-linux-x86_64/weaver" | awk '{print $1}')"
fi

cat >"$KIT_DIR/meta/weaver-artifact.env" <<EOF
artifact_path=$(cd "$(dirname "$WEAVER_ARTIFACT")" && pwd)/$(basename "$WEAVER_ARTIFACT")
artifact_sha256=${ARTIFACT_SHA}
binary_path=$(cd "$KIT_DIR/bin/weaver-linux-x86_64" && pwd)/weaver
binary_sha256=${WEAVER_SHA}
EOF

if [ -f "$KIT_DIR/README.md" ]; then
  python3 - "$KIT_DIR/README.md" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
marker = "- `bin/weaver-linux-x86_64/weaver`\n"
if marker not in text:
    insert_after = "- `bin/par2cmdline-turbo-1.4.0-linux-amd64.zip`\n  - Original release asset from GitHub.\n"
    replacement = insert_after + "- `bin/weaver-linux-x86_64/weaver`\n  - Pipeline-built Linux x86_64 Weaver binary, injected after CI completed.\n"
    if insert_after in text:
        text = text.replace(insert_after, replacement)

quick_use_old = "3. Run:\n"
quick_use_new = "3. Run:\n"
path.write_text(text)
PY
fi

if [ -n "$OUTPUT_TAR" ]; then
  rm -f "$OUTPUT_TAR"
  (
    cd "$(dirname "$KIT_DIR")"
    tar -czf "$OUTPUT_TAR" "$(basename "$KIT_DIR")"
  )
fi

echo "kit_dir=$KIT_DIR"
echo "weaver_binary=$KIT_DIR/bin/weaver-linux-x86_64/weaver"
if [ -n "$OUTPUT_TAR" ]; then
  echo "output_tar=$OUTPUT_TAR"
fi
