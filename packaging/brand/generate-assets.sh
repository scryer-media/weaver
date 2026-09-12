#!/usr/bin/env bash
# Regenerate every committed raster brand asset from the vector sources in this
# directory.
#
# This is NOT run in CI. The rasters are committed because they are reviewed
# artwork and because a release must not depend on a rasterizer version. The
# script exists so the artwork can be reproduced and adjusted rather than being
# an opaque set of binaries.
#
# Requires `brew install librsvg imagemagick`. librsvg is not optional:
# ImageMagick's own SVG renderer ignores the `fill: url(#linear-gradient)` that
# these sources declare through a CSS class and silently emits a black
# silhouette instead of the brand gradient.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: generate-assets.sh [--check]

  --check  Write to a scratch directory and compare against the committed
           assets instead of overwriting them. Exits non-zero if they differ.
USAGE
  exit 2
}

check_only=false
while [ $# -gt 0 ]; do
  case "$1" in
    --check) check_only=true; shift ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

for tool in rsvg-convert magick; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "$tool not found; brew install librsvg imagemagick" >&2
    exit 1
  }
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Where a generated file lands. --check diverts the whole tree so the committed
# assets can be compared against a fresh run.
destination="$repo_root"
if [ "$check_only" = true ]; then
  destination="$work/tree"
fi

MARK_COLOR="$script_dir/weaver-mark-color.svg"
MARK_DARK="$script_dir/weaver-mark-dark.svg"
MARK_WHITE="$script_dir/weaver-mark-white.svg"

# A copy of a source whose viewBox is its own ink box, cached per source.
#
# The sources centre their artwork inside a canvas with slack around it, and
# every destination here wants the ink at a given size rather than the canvas.
# With the slack removed, `rsvg-convert -w N` draws the artwork at exactly N
# pixels wide and the rasterizer does its own antialiasing at that size.
# Rendering large and downsampling instead is what made the 16px frames
# unreadable: the thin lines that weave the mark's strokes together closed up
# into one solid block.
#
# The ink box has to be measured, because nothing in the file declares it.
tight_svg() {
  local source="$1"
  local out="$work/tight-$(basename "$source")"
  if [ ! -f "$out" ]; then
    local view canvas_width origin_x origin_y box
    view="$(sed -n 's/.*viewBox="\([^"]*\)".*/\1/p' "$source" | head -1)"
    origin_x="$(printf '%s\n' "$view" | awk '{print $1}')"
    origin_y="$(printf '%s\n' "$view" | awk '{print $2}')"
    canvas_width="$(printf '%s\n' "$view" | awk '{print $3}')"
    # Four pixels per user unit, so the measured box resolves to a quarter unit.
    rsvg-convert -w "$(awk -v w="$canvas_width" 'BEGIN{printf "%d", w * 4}')" \
      "$source" -o "$work/probe.png"
    box="$(magick "$work/probe.png" -format '%@' info:)"
    sed "s|viewBox=\"$view\"|viewBox=\"$(
      printf '%s\n' "$box" \
        | tr 'x+' '   ' \
        | awk -v ox="$origin_x" -v oy="$origin_y" \
            '{printf "%.3f %.3f %.3f %.3f", ox + $3/4, oy + $4/4, $1/4, $2/4}'
    )\"|" "$source" > "$out"
  fi
  printf '%s\n' "$out"
}

# emit_to <source.svg> <canvas-px> <percent> <background> <absolute output>
#
# The ink is drawn at <percent> of the canvas and centred on <background>
# ("none" for transparency); a menu-bar glyph fills its box, an app icon sits
# inside the margin Apple's icon grid expects. The canvas stays square because
# the macOS wrapper declares its status-item image at one size and would
# otherwise stretch the artwork to it.
emit_to() {
  local source="$1" canvas="$2" percent="$3" background="$4" out="$5"
  local ink=$((canvas * percent / 100))
  local tight
  tight="$(tight_svg "$source")"
  mkdir -p "$(dirname "$out")"
  rsvg-convert -w "$ink" "$tight" -o "$work/render.png"
  magick "$work/render.png" \
    -background "$background" -gravity center -extent "${canvas}x${canvas}" \
    -strip -define png:exclude-chunk=date,time \
    "$out"
}

# The same, for a committed asset named by its <repo-relative output>.
emit() {
  emit_to "$1" "$2" "$3" "$4" "$destination/$5"
}

# emit_icon <source.svg> <repo-relative output> <size>...
#
# One frame per size rather than letting the shell scale one, which is what
# keeps the smallest frame legible. TrueColorAlpha is what keeps it legible
# too: an .ico frame defaults to a one-bit transparency mask, which throws away
# every antialiased edge pixel and leaves the small sizes looking chewed.
emit_icon() {
  local source="$1" out="$destination/$2"
  shift 2
  local frames=()
  local size
  for size in "$@"; do
    emit_to "$source" "$size" 100 none "$work/frame-$size.png"
    frames+=("$work/frame-$size.png")
  done
  mkdir -p "$(dirname "$out")"
  magick "${frames[@]}" -type TrueColorAlpha -depth 8 -strip "$out"
}

web="apps/weaver-web/public"
macos="server/app/weaver/resources/macos"
windows="server/app/weaver/resources/windows"

# --- the web app ------------------------------------------------------------
#
# The two 32px favicons are one drawing: the mark carries its own colour, so it
# needs no light and dark variant. They stay two files because the document
# head selects between them by colour scheme, and a scheme-specific mark may be
# wanted later.
emit "$MARK_COLOR" 32 100 none "$web/favicon-light-32.png"
emit "$MARK_COLOR" 32 100 none "$web/favicon-dark-32.png"
emit_icon "$MARK_COLOR" "$web/favicon.ico" 16 32 48 64 128 256

# The installed-app icons sit on a plate because a home screen or app launcher
# composites them over wallpaper, where a transparent mark would disappear.
emit "$MARK_COLOR" 180 80 "#323232" "$web/apple-touch-icon.png"
emit "$MARK_COLOR" 192 80 "#323232" "$web/app-icon-dark-192.png"
emit "$MARK_COLOR" 512 80 "#323232" "$web/app-icon-dark-512.png"
emit "$MARK_COLOR" 192 80 "#b9b9b9" "$web/app-icon-light-192.png"
emit "$MARK_COLOR" 512 80 "#b9b9b9" "$web/app-icon-light-512.png"
cp "$destination/$web/app-icon-light-192.png" "$destination/$web/icon-192.png"
cp "$destination/$web/app-icon-light-512.png" "$destination/$web/icon-512.png"

# The unplated master, kept for anywhere the mark is placed by hand.
emit_to "$MARK_COLOR" 1024 100 none "$work/master.png"
magick "$work/master.png" -trim +repage -strip "$destination/$web/favicon.webp"

# --- the macOS menu bar -----------------------------------------------------
#
# Named for the appearance each one serves, not for its own colour: the
# dark-named file is the light-coloured drawing, because that is the one a dark
# menu bar needs. The tray wrapper picks between them itself, so these ship as
# finished artwork rather than as a template mask for AppKit to tint.
emit "$MARK_DARK" 18 94 none "$macos/menubar-light.png"
emit "$MARK_DARK" 36 94 none "$macos/menubar-light@2x.png"
emit "$MARK_WHITE" 18 94 none "$macos/menubar-dark.png"
emit "$MARK_WHITE" 36 94 none "$macos/menubar-dark@2x.png"

# --- Windows ----------------------------------------------------------------
#
# One icon resource serves both the executable and the notification area, so it
# is the colour mark: the shell does not tint a notification icon the way the
# macOS menu bar does, and this same artwork is what Explorer and the taskbar
# show.
emit_icon "$MARK_COLOR" "$windows/weaver.ico" 16 20 24 32 40 48 64 96 128 256

# --- documentation ----------------------------------------------------------
emit_to "$MARK_COLOR" 400 80 "#323232" "$work/hero.png"
mkdir -p "$destination/docs/img"
magick "$work/hero.png" -strip "$destination/docs/img/weaver-hero.webp"

if [ "$check_only" = true ]; then
  status=0
  while IFS= read -r -d '' generated; do
    relative="${generated#"$destination"/}"
    if ! cmp -s "$generated" "$repo_root/$relative"; then
      echo "differs: $relative" >&2
      status=1
    fi
  done < <(find "$destination" -type f -print0)
  [ "$status" -eq 0 ] && echo "the committed brand assets match a fresh run"
  exit "$status"
fi

echo "regenerated the brand rasters under $web, $macos, $windows and docs/img."
echo "weaver.icns is derived from $web/app-icon-dark-512.png:"
echo "  packaging/macos/assets/generate-assets.sh"
