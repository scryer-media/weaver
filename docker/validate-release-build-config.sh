#!/bin/sh
set -eu

workflow_file=${1:-.github/workflows/deploy.yml}

if [ ! -f "$workflow_file" ]; then
    printf 'Release workflow not found: %s\n' "$workflow_file" >&2
    exit 1
fi

release_job=$(awk '
    /^  docker-publish:$/ { in_job = 1 }
    in_job && /^  [[:alnum:]_-]+:$/ && $0 != "  docker-publish:" { exit }
    in_job { print }
' "$workflow_file")

if [ -z "$release_job" ]; then
    printf 'docker-publish job was not found in %s\n' "$workflow_file" >&2
    exit 1
fi

workflow_value() {
    key=$1
    value=$(printf '%s\n' "$release_job" | awk -v key="$key" '
        $0 ~ "^[[:space:]]*" key ":[[:space:]]*" {
            sub("^[[:space:]]*" key ":[[:space:]]*", "")
            print
        }
    ')
    count=$(printf '%s\n' "$value" | awk 'NF { count += 1 } END { print count + 0 }')
    if [ "$count" -ne 1 ]; then
        printf 'Expected exactly one docker-publish %s setting; found %s\n' "$key" "$count" >&2
        exit 1
    fi
    printf '%s\n' "$value"
}

context=$(workflow_value context)
dockerfile=$(workflow_value file)
platforms=$(workflow_value platforms)
push=$(workflow_value push)
provenance=$(workflow_value provenance)
sbom=$(workflow_value sbom)

[ "$context" = "docker-ctx" ] || {
    printf 'docker-publish context must be docker-ctx; found %s\n' "$context" >&2
    exit 1
}
[ "$dockerfile" = "docker/weaver.Dockerfile" ] || {
    printf 'docker-publish file must be docker/weaver.Dockerfile; found %s\n' "$dockerfile" >&2
    exit 1
}
[ "$platforms" = "linux/amd64,linux/arm64" ] || {
    printf 'docker-publish platforms must cover linux/amd64 and linux/arm64; found %s\n' "$platforms" >&2
    exit 1
}
[ "$push" = "true" ] || {
    printf 'docker-publish must push the release image; found %s\n' "$push" >&2
    exit 1
}
[ "$sbom" = "true" ] || {
    printf 'docker-publish must emit an SBOM; found %s\n' "$sbom" >&2
    exit 1
}

check_dir=$(mktemp -d)
cleanup() {
    rm -rf -- "$check_dir"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$check_dir/amd64" "$check_dir/arm64"
touch \
    "$check_dir/amd64/weaver-portable" \
    "$check_dir/amd64/weaver-haswell" \
    "$check_dir/arm64/weaver-portable" \
    "$check_dir/arm64/weaver-cortex-a76"
cp "$dockerfile" "$check_dir/Dockerfile"
cp docker/entrypoint.sh docker/runtime-select.sh "$check_dir/"

docker buildx build \
    --check \
    --platform "$platforms" \
    --provenance "$provenance" \
    --sbom="$sbom" \
    --file "$check_dir/Dockerfile" \
    "$check_dir"
