#!/bin/sh
set -eu

README_PATH=${1:-README.md}
README_CONTENT=$(cat "$README_PATH")

assert_contains() {
    haystack=$1
    needle=$2
    message=$3

    case "$haystack" in
        *"$needle"*) ;;
        *)
            printf 'assertion failed: %s\nmissing: %s\n' "$message" "$needle" >&2
            exit 1
            ;;
    esac
}

assert_contains "$README_CONTENT" "## Docker" "README should include a Docker section"
assert_contains "$README_CONTENT" "ghcr.io/scryer-media/weaver:latest" "README should document the published image"
assert_contains "$README_CONTENT" "ghcr.io/scryer-media/weaver:latest-slim" "README should document the slim image"
assert_contains "$README_CONTENT" "ghcr.io/scryer-media/weaver:latest-modern" "README should document the modern image"
assert_contains "$README_CONTENT" "Sigstore Cosign" "README should mention image signing"
assert_contains "$README_CONTENT" "/config" "README should document the /config volume contract"
assert_contains "$README_CONTENT" "PUID=1000" "README should document PUID"
assert_contains "$README_CONTENT" "PGID=1000" "README should document PGID"
assert_contains "$README_CONTENT" "TZ=Etc/UTC" "README should document the default timezone"
assert_contains "$README_CONTENT" "UMASK=022" "README should document UMASK"
assert_contains "$README_CONTENT" "--user=1000:1000" "README should document non-root usage"
assert_contains "$README_CONTENT" "--read-only=true" "README should document read-only support"
assert_contains "$README_CONTENT" "/path/to/weaver/config:/config" "README should include a /config mount example"
assert_contains "$README_CONTENT" "zstd-compressed OCI image layers" "README should explain the slim and modern layer compression"
assert_contains "$README_CONTENT" 'Use `latest-modern` only on hosts that satisfy the optimized CPU baseline.' "README should explain modern CPU requirements"

printf 'docker docs contract check passed for %s\n' "$README_PATH"
