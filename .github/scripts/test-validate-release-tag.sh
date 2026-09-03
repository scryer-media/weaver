#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
validator="$script_dir/validate-release-tag.sh"

manifest_dir="$(mktemp -d)"
trap 'rm -rf "$manifest_dir"' EXIT

# A workspace manifest declaring `version`, in the shape Cargo writes it.
manifest_with_version() {
  local version="$1"
  local path="$manifest_dir/Cargo-$version.toml"
  printf '[workspace]\nmembers = ["xtask"]\n\n[workspace.package]\nversion = "%s"\nedition = "2024"\n\n[workspace.dependencies]\nversion = "not-the-workspace-version"\n' \
    "$version" > "$path"
  echo "$path"
}

assert_valid() {
  local release_tag="$1"
  local expected_version="$2"
  local output
  output="$(mktemp)"
  trap 'rm -f "$output"' RETURN

  GITHUB_REF_NAME="$release_tag" GITHUB_OUTPUT="$output" \
    WEAVER_WORKSPACE_MANIFEST="$(manifest_with_version "$expected_version")" \
    bash "$validator"
  diff -u \
    <(printf 'release_tag=%s\nversion=%s\n' "$release_tag" "$expected_version") \
    "$output"
}

assert_invalid() {
  local release_tag="$1"
  local manifest="$2"
  local output
  output="$(mktemp)"
  trap 'rm -f "$output"' RETURN

  if GITHUB_REF_NAME="$release_tag" GITHUB_OUTPUT="$output" \
    WEAVER_WORKSPACE_MANIFEST="$manifest" bash "$validator" 2>/dev/null; then
    echo "accepted invalid release tag: $(printf %q "$release_tag") against $manifest" >&2
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

matching_manifest="$(manifest_with_version '1.2.3')"
for release_tag in "${invalid_tags[@]}"; do
  assert_invalid "$release_tag" "$matching_manifest"
done

# A well-formed tag that disagrees with the workspace version is refused, and
# so is a tag with no manifest to agree with.
assert_invalid 'weaver-v0.9.7' "$(manifest_with_version '0.9.6')"
assert_invalid 'weaver-v0.9.7' "$manifest_dir/missing/Cargo.toml"
printf '[workspace.package]\nedition = "2024"\n' > "$manifest_dir/no-version.toml"
assert_invalid 'weaver-v0.9.7' "$manifest_dir/no-version.toml"

echo "validate-release-tag fixtures passed"
