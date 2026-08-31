#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_REF_NAME:?GITHUB_REF_NAME must be set}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"

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
{
  echo "release_tag=$release_tag"
  echo "version=$version"
} >> "$GITHUB_OUTPUT"
