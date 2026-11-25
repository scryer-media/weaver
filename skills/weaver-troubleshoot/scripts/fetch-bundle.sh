#!/bin/sh
# Download the Weaver diagnostics bundle to one file the operator names.
#
#   fetch-bundle.sh <archive-file.tar.zst>
#
# The file must not exist yet; nothing else is written, replaced or removed.
# Weaver takes at least 10 seconds to build the bundle. The bundle's contents
# are never printed: this prints only the file name and its size.
#
# Needs WEAVER_URL and WEAVER_API_KEY (an ADMIN key) in the environment, plus
# curl and od. The key is passed to curl on stdin, never on the command line,
# and is never printed.
#
# Exit codes: 0 ok, 2 usage or environment, 3 refused by Weaver, 4 bad
# download or transport failure.
set -eu

usage() {
  echo "usage: fetch-bundle.sh <archive-file.tar.zst>" >&2
  exit 2
}

[ $# -eq 1 ] || usage
out=$1
[ -n "$out" ] || usage

[ -n "${WEAVER_URL:-}" ] || { echo "WEAVER_URL is not set; complete weaver-connect first" >&2; exit 2; }
[ -n "${WEAVER_API_KEY:-}" ] || { echo "WEAVER_API_KEY is not set; complete weaver-connect first" >&2; exit 2; }
for tool in curl od; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 2; }
done

if [ -e "$out" ] || [ -L "$out" ]; then
  echo "refusing: $out already exists; name a new file" >&2
  exit 2
fi
out_dir=$(dirname -- "$out")
[ -d "$out_dir" ] && [ -w "$out_dir" ] || { echo "cannot write in $out_dir" >&2; exit 2; }

# Download beside the target, then move it into place only when it is a
# zstd archive, so a refusal never leaves a file under the operator's name.
part=$(mktemp "$out.part.XXXXXX")
trap 'rm -f "$part"' EXIT HUP INT TERM

url=${WEAVER_URL%/}/api/system/diagnostics

set +e
status=$(printf 'header = "Authorization: Bearer %s"\n' "$WEAVER_API_KEY" \
  | curl --silent --show-error --config - \
      --output "$part" \
      --write-out '%{http_code}' \
      "$url")
curl_status=$?
set -e

if [ "$curl_status" -ne 0 ]; then
  echo "request to $url failed before Weaver answered (curl exit $curl_status)" >&2
  exit 4
fi

case $status in
  200) ;;
  401)
    echo "refused: HTTP 401: the key was not accepted; redo weaver-connect" >&2
    exit 3 ;;
  403)
    echo "refused: HTTP 403: the bundle needs an ADMIN key" >&2
    exit 3 ;;
  421)
    echo "refused: HTTP 421: the host name in WEAVER_URL is not allowed" >&2
    exit 3 ;;
  *)
    echo "refused: HTTP $status: Weaver could not build the bundle; see ServiceLogs" >&2
    exit 3 ;;
esac

magic=$(od -An -tx1 -N4 "$part" | tr -d ' \n')
if [ "$magic" != "28b52ffd" ]; then
  echo "download is not a zstd archive (got $magic); check WEAVER_URL and any reverse proxy" >&2
  exit 4
fi

# ln never replaces an existing file; the trap then drops the part name.
ln -- "$part" "$out" || { echo "refusing: $out appeared meanwhile; name a new file" >&2; exit 2; }
bytes=$(wc -c < "$out" | tr -d ' ')
echo "saved: $out ($bytes bytes)"
