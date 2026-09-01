#!/usr/bin/env bash
# Assemble Weaver.app around already-built binaries.
#
# The bundle is a wrapper, not a second build: the executables it contains are
# the same `weaver` and `weaver-tray` the portable tarball ships. Nothing here
# signs anything — the release workflow signs the finished bundle, and doing it
# twice would just invalidate the first signature.
#
# Only tools that ship with macOS are used, so this runs on any Mac.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: build-app.sh --version X.Y.Z --weaver <path> --tray <path> --output <dir>

  --version  Version string written into CFBundleShortVersionString/CFBundleVersion.
  --weaver   Path to the built `weaver` server binary.
  --tray     Path to the built `weaver-tray` desktop wrapper binary.
  --output   Directory the bundle is created in; Weaver.app is placed inside it.
USAGE
  exit 2
}

version=""
weaver_binary=""
tray_binary=""
output_dir=""

while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="${2:-}"; shift 2 ;;
    --weaver) weaver_binary="${2:-}"; shift 2 ;;
    --tray) tray_binary="${2:-}"; shift 2 ;;
    --output) output_dir="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[ -n "$version" ] || usage
[ -n "$weaver_binary" ] || usage
[ -n "$tray_binary" ] || usage
[ -n "$output_dir" ] || usage

# The version lands in Info.plist through an unquoted sed substitution, and
# Launch Services rejects bundles whose version strings are not plain
# dotted numbers — so reject anything else before it is baked into a bundle.
case "$version" in
  *[!0-9.]*|.*|*.|*..*|"")
    echo "version must be release-derived major.minor.patch, got: $version" >&2
    exit 1
    ;;
esac

for binary in "$weaver_binary" "$tray_binary"; do
  if [ ! -f "$binary" ]; then
    echo "not a file: $binary" >&2
    exit 1
  fi
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
icon_source="$script_dir/assets/weaver.icns"

if [ ! -f "$icon_source" ]; then
  echo "missing app icon at $icon_source" >&2
  exit 1
fi

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"
bundle="$output_dir/Weaver.app"
rm -rf "$bundle"
mkdir -p "$bundle/Contents/MacOS" "$bundle/Contents/Resources"

# Launch Services reads CFBundleExecutable, so the wrapper has to keep its own
# name inside the bundle; the server sits beside it because that is where the
# wrapper looks for it.
install -m 0755 "$tray_binary" "$bundle/Contents/MacOS/weaver-tray"
install -m 0755 "$weaver_binary" "$bundle/Contents/MacOS/weaver"

# The icon is committed rather than rendered here: it is squircle-masked to
# Apple's icon grid, which the raw web icon is not, and reviewed artwork should
# not be re-rasterized by every build. assets/generate-assets.sh reproduces it.
install -m 0644 "$icon_source" "$bundle/Contents/Resources/weaver.icns"

sed -e "s/@VERSION@/$version/g" "$script_dir/Info.plist" > "$bundle/Contents/Info.plist"
plutil -lint "$bundle/Contents/Info.plist"

echo "built $bundle"
