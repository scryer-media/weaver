#!/usr/bin/env bash
# Regenerate the committed macOS brand assets.
#
# This is NOT run in CI. weaver.icns and dmg-background.tiff are committed
# because they are reviewed artwork, and because ImageMagick's rasterization is
# not stable enough across versions to make a release depend on it. The script
# exists so the artwork can be reproduced and adjusted rather than being an
# opaque binary.
#
# Requires `brew install imagemagick`; everything else ships with macOS.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: generate-assets.sh [--output <dir>]

  --output  Directory to write weaver.icns and dmg-background.tiff into.
            Defaults to this script's directory, i.e. it overwrites the
            committed assets in place.
USAGE
  exit 2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
output_dir="$script_dir"

while [ $# -gt 0 ]; do
  case "$1" in
    --output) output_dir="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[ -n "$output_dir" ] || usage
mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd)"

if ! command -v magick >/dev/null 2>&1; then
  echo "magick not found; brew install imagemagick" >&2
  exit 1
fi

icon_source="$repo_root/apps/weaver-web/public/app-icon-dark-512.png"
if [ ! -f "$icon_source" ]; then
  echo "missing icon source at $icon_source" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# --- weaver.icns -------------------------------------------------------------
#
# Apple's icon grid puts the artwork in an 824px squircle inside the 1024px
# canvas, with roughly a 22.5% corner radius. Rendering the tile at 824 and
# padding to 1024 is what keeps Weaver's icon the same visual size as the
# system's in the Dock.
magick "$icon_source" -resize 824x824 "$work/tile.png"
magick -size 824x824 xc:black -fill white \
  -draw "roundrectangle 0,0,823,823,185,185" "$work/mask.png"
magick "$work/tile.png" "$work/mask.png" \
  -alpha off -compose CopyOpacity -composite "$work/masked.png"
magick -size 1024x1024 xc:none "$work/masked.png" \
  -gravity center -compose over -composite "$work/master-1024.png"

iconset="$work/weaver.iconset"
mkdir -p "$iconset"
for size in 16 32 128 256 512; do
  sips -z "$size" "$size" "$work/master-1024.png" \
    --out "$iconset/icon_${size}x${size}.png" >/dev/null
  retina=$((size * 2))
  sips -z "$retina" "$retina" "$work/master-1024.png" \
    --out "$iconset/icon_${size}x${size}@2x.png" >/dev/null
done
iconutil --convert icns "$iconset" --output "$output_dir/weaver.icns"

# --- dmg-background.tiff -----------------------------------------------------
#
# The arrow has to land between the two icon positions in dmg-settings.py
# (Weaver.app at 140,210 and Applications at 460,210 in 1x window points), so
# these coordinates and that file move together. The art is light-themed
# because Finder paints filename labels black over any custom background
# picture — dark art makes the labels unreadable.
# Named by path rather than family: ImageMagick on macOS has no font cache of
# its own, so `-font Arial-Bold` resolves only on some installs.
wordmark_font="/System/Library/Fonts/Supplemental/Arial Bold.ttf"
if [ ! -f "$wordmark_font" ]; then
  echo "missing wordmark font at $wordmark_font" >&2
  exit 1
fi

draw_background() {
  local scale="$1" out="$2"
  local width=$((600 * scale)) height=$((400 * scale))
  local wordmark=$((27 * scale)) offset=$((40 * scale))
  local ax=$((240 * scale)) ay=$((210 * scale))
  local bx=$((330 * scale)) tipx=$((362 * scale))
  local shaft=$((5 * scale)) head=$((16 * scale))

  # The arrow is a single polygon drawn opaque on its own layer and faded once
  # on composite — a stroked shaft plus a filled head would double-composite
  # their translucent alphas where they overlap and leave a seam.
  magick -size "${width}x${height}" xc:none -fill '#0f172a' \
    -draw "polygon $ax,$((ay - shaft)) $bx,$((ay - shaft)) $bx,$((ay - head)) $tipx,$ay $bx,$((ay + head)) $bx,$((ay + shaft)) $ax,$((ay + shaft))" \
    -channel A -evaluate multiply 0.30 +channel "$work/arrow${scale}x.png"

  magick -size "${width}x${height}" gradient:'#fbfcfe-#eef1f6' \
    "$work/arrow${scale}x.png" -compose over -composite \
    -font "$wordmark_font" -pointsize "$wordmark" -fill 'rgba(15,23,42,0.45)' \
    -gravity north -annotate "+0+$offset" 'Weaver' \
    "$out"
}

draw_background 1 "$work/bg1x.png"
draw_background 2 "$work/bg2x.png"

# A single TIFF carrying both representations is how Finder is told which one
# is the Retina image; two separate files would leave the 1x art upscaled.
tiffutil -cathidpicheck "$work/bg1x.png" "$work/bg2x.png" \
  -out "$output_dir/dmg-background.tiff"

echo "wrote $output_dir/weaver.icns"
echo "wrote $output_dir/dmg-background.tiff"
