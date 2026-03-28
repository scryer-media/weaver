#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Populate a PAR2 x86 kit with the latest public x86_64 Linux binaries for:
  - Weaver
  - par2cmdline-turbo

Usage:
  scripts/finalize-par2-x86-kit.sh \
    --kit-dir /path/to/weaver-par2-x86-kit-20260327 \
    [--output-tar /path/to/weaver-par2-x86-kit-self-contained.tar.gz]
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

download_release_asset() {
  local repo="$1"
  local selector="$2"
  local out_file="$3"
  local tmp_dir="$4"
  local release_json="$tmp_dir/release.json"

  curl -fsSL "https://api.github.com/repos/${repo}/releases/latest" -o "$release_json"
  python3 - "$release_json" "$selector" "$tmp_dir" <<'PY'
import json
import re
import sys
from pathlib import Path

release_path = Path(sys.argv[1])
selector = sys.argv[2]
tmp_dir = Path(sys.argv[3])
release = json.loads(release_path.read_text())
assets = release.get("assets", [])

asset = None
if selector.startswith("regex:"):
    pattern = re.compile(selector[len("regex:"):])
    asset = next((a for a in assets if pattern.fullmatch(a.get("name") or "")), None)
else:
    asset = next((a for a in assets if a.get("name") == selector), None)

if asset is None:
    print(f"missing asset for selector {selector!r}", file=sys.stderr)
    raise SystemExit(1)

(tmp_dir / "tag.txt").write_text((release.get("tag_name") or "") + "\n")
(tmp_dir / "url.txt").write_text((asset.get("browser_download_url") or "") + "\n")
(tmp_dir / "name.txt").write_text((asset.get("name") or "") + "\n")
(tmp_dir / "digest.txt").write_text((asset.get("digest") or "") + "\n")
PY

  local asset_url
  asset_url="$(tr -d '\n' < "$tmp_dir/url.txt")"
  [ -n "$asset_url" ] || die "failed to resolve asset URL for $repo"
  curl -fL "$asset_url" -o "$out_file"
}

sha256_file() {
  local file="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  else
    printf '\n'
  fi
}

KIT_DIR=""
OUTPUT_TAR=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --kit-dir)
      KIT_DIR="$2"
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
[ -d "$KIT_DIR" ] || die "kit directory not found: $KIT_DIR"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

WEAVER_TMP="$WORK_DIR/weaver"
TURBO_TMP="$WORK_DIR/turbo"
mkdir -p "$WEAVER_TMP" "$TURBO_TMP"

WEAVER_ARCHIVE="$WEAVER_TMP/weaver-linux-x86_64.tar.gz"
TURBO_ARCHIVE="$TURBO_TMP/par2cmdline-turbo-linux-amd64.zip"

download_release_asset "scryer-media/weaver" "weaver-linux-x86_64.tar.gz" "$WEAVER_ARCHIVE" "$WEAVER_TMP"
download_release_asset "animetosho/par2cmdline-turbo" "regex:par2cmdline-turbo-.*-linux-amd64\\.zip" "$TURBO_ARCHIVE" "$TURBO_TMP"

mkdir -p "$KIT_DIR/bin/weaver-linux-x86_64" "$KIT_DIR/bin/par2cmdline-turbo-linux-amd64"
rm -f "$KIT_DIR/bin/weaver-linux-x86_64/weaver"
rm -f "$KIT_DIR/bin/par2cmdline-turbo-linux-amd64/par2"

tar -xzf "$WEAVER_ARCHIVE" -C "$WEAVER_TMP"
[ -f "$WEAVER_TMP/weaver" ] || die "weaver release asset did not contain a top-level 'weaver' binary"
cp "$WEAVER_TMP/weaver" "$KIT_DIR/bin/weaver-linux-x86_64/weaver"
chmod +x "$KIT_DIR/bin/weaver-linux-x86_64/weaver"

unzip -q -o "$TURBO_ARCHIVE" -d "$TURBO_TMP/unpacked"
[ -f "$TURBO_TMP/unpacked/par2" ] || die "turbo release asset did not contain a top-level 'par2' binary"
cp "$TURBO_TMP/unpacked/par2" "$KIT_DIR/bin/par2cmdline-turbo-linux-amd64/par2"
chmod +x "$KIT_DIR/bin/par2cmdline-turbo-linux-amd64/par2"

cat >"$KIT_DIR/meta/tool-downloads.env" <<EOF
weaver_release_repo=scryer-media/weaver
weaver_release_tag=$(tr -d '\n' < "$WEAVER_TMP/tag.txt")
weaver_asset_name=$(tr -d '\n' < "$WEAVER_TMP/name.txt")
weaver_asset_digest=$(tr -d '\n' < "$WEAVER_TMP/digest.txt")
weaver_asset_sha256=$(sha256_file "$WEAVER_ARCHIVE")
weaver_binary_sha256=$(sha256_file "$KIT_DIR/bin/weaver-linux-x86_64/weaver")
turbo_release_repo=animetosho/par2cmdline-turbo
turbo_release_tag=$(tr -d '\n' < "$TURBO_TMP/tag.txt")
turbo_asset_name=$(tr -d '\n' < "$TURBO_TMP/name.txt")
turbo_asset_digest=$(tr -d '\n' < "$TURBO_TMP/digest.txt")
turbo_asset_sha256=$(sha256_file "$TURBO_ARCHIVE")
turbo_binary_sha256=$(sha256_file "$KIT_DIR/bin/par2cmdline-turbo-linux-amd64/par2")
EOF

if [ -n "$OUTPUT_TAR" ]; then
  rm -f "$OUTPUT_TAR"
  (
    cd "$(dirname "$KIT_DIR")"
    tar -czf "$OUTPUT_TAR" "$(basename "$KIT_DIR")"
  )
fi

echo "kit_dir=$KIT_DIR"
echo "weaver_binary=$KIT_DIR/bin/weaver-linux-x86_64/weaver"
echo "turbo_binary=$KIT_DIR/bin/par2cmdline-turbo-linux-amd64/par2"
if [ -n "$OUTPUT_TAR" ]; then
  echo "output_tar=$OUTPUT_TAR"
fi
