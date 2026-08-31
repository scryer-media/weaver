#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
validator="$script_dir/validate-release-tag.sh"

assert_valid() {
  local release_tag="$1"
  local expected_version="$2"
  local output
  output="$(mktemp)"
  trap 'rm -f "$output"' RETURN

  GITHUB_REF_NAME="$release_tag" GITHUB_OUTPUT="$output" bash "$validator"
  diff -u \
    <(printf 'release_tag=%s\nversion=%s\n' "$release_tag" "$expected_version") \
    "$output"
}

assert_invalid() {
  local release_tag="$1"
  local output
  output="$(mktemp)"
  trap 'rm -f "$output"' RETURN

  if GITHUB_REF_NAME="$release_tag" GITHUB_OUTPUT="$output" bash "$validator" 2>/dev/null; then
    echo "accepted invalid release tag: $(printf %q "$release_tag")" >&2
    exit 1
  fi
}

# Canonical stable-tag fixtures.
assert_valid 'weaver-v0.9.7' '0.9.7'
assert_valid 'weaver-v10.20.30' '10.20.30'

# Invalid fixtures cover malformed, non-stable, and script-shaped values.
invalid_tags=(
  'weaver-v'
  'weaver-v1'
  'weaver-v1.2'
  'weaver-v01.2.3'
  'weaver-v1.02.3'
  'weaver-v1.2.03'
  'weaver-v1.2.3-rc.1'
  'weaver-v1.2.3+build.1'
  "weaver-v1.2.3\$(id)"
  "weaver-v1.2.3\`id\`"
  'weaver-v1.2.3"'
  "weaver-v1.2.3'"
  'weaver-v1.2.3;id'
  'weaver-v1.2.3 release'
  'weaver-v1.2.3/release'
  $'weaver-v1.2.3\nrelease'
)

for release_tag in "${invalid_tags[@]}"; do
  assert_invalid "$release_tag"
done
