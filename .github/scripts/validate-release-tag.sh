#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_REF_NAME:?GITHUB_REF_NAME must be set}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
workspace_manifest="${WEAVER_WORKSPACE_MANIFEST:-$script_dir/../../Cargo.toml}"

release_tag="$GITHUB_REF_NAME"
if [[ "$release_tag" == *$'\n'* || "$release_tag" == *$'\r'* ]]; then
  echo "release tag must be a single line" >&2
  exit 1
fi

stable_tag_pattern='^weaver-v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
if [[ ! "$release_tag" =~ $stable_tag_pattern ]]; then
  echo "release tag must be canonical stable form weaver-vMAJOR.MINOR.PATCH" >&2
  exit 1
fi

version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"

# The tag names the release everywhere downstream (Docker, Homebrew, WinGet,
# the GitHub release) while the binaries and the macOS bundle report the
# workspace version. The two must agree, or one release ships under two names.
if [[ ! -f "$workspace_manifest" ]]; then
  echo "workspace manifest not found at $workspace_manifest" >&2
  exit 1
fi
workspace_version="$(awk '
  /^\[/ { in_workspace_package = ($0 == "[workspace.package]"); next }
  in_workspace_package && /^version[[:space:]]*=/ {
    sub(/^version[[:space:]]*=[[:space:]]*"/, "")
    sub(/".*$/, "")
    print
    exit
  }
' "$workspace_manifest")"
if [[ -z "$workspace_version" ]]; then
  echo "could not read [workspace.package] version from $workspace_manifest" >&2
  exit 1
fi
if [[ "$workspace_version" != "$version" ]]; then
  echo "release tag $release_tag names version $version but $workspace_manifest declares workspace version $workspace_version" >&2
  exit 1
fi

{
  echo "release_tag=$release_tag"
  echo "version=$version"
} >> "$GITHUB_OUTPUT"
