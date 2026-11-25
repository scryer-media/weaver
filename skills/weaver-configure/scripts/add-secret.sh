#!/bin/sh
# Set a news server's username or password in a variables file
# ({"input": {...}}) without the value passing through the command line or
# stdout.
#
# Usage:
#   add-secret.sh <vars-in.json> <username|password> <vars-out.json> --file <path>
#   add-secret.sh <vars-in.json> <username|password> <vars-out.json> --env <VARIABLE>
#
#   --file reads the value from a file the operator wrote (one trailing
#          newline is removed).
#   --env  reads it from an environment variable the operator exported.
#
# <vars-out.json> is written with mode 600, replacing any existing file and
# whatever mode it had. It contains the value: keep it in a private directory
# and tell the operator where it is. Prints only the field name and the
# output path.
set -eu

in=${1:?usage: add-secret.sh <vars-in.json> <username|password> <vars-out.json> --file <path> | --env <VARIABLE>}
field=${2:?missing field}
out=${3:?missing output path}
mode=${4:?missing --file or --env}
src=${5:?missing secret source}

case "$field" in
  username | password) ;;
  *)
    echo "field must be username or password" >&2
    exit 2
    ;;
esac

umask 077
# Written to a new private file and renamed into place, so an existing output
# file never keeps a wider mode and is never left half written.
tmp=$(mktemp "$out.XXXXXX")
trap 'rm -f "$tmp"' EXIT
case "$mode" in
  --file)
    [ -s "$src" ] || { echo "secret file is missing or empty" >&2; exit 2; }
    jq --arg f "$field" --rawfile s "$src" \
      'if (.input | type) != "object" then error("no input object") else . end
       | .input[$f] = ($s | sub("\r?\n$"; ""))' \
      "$in" > "$tmp"
    ;;
  --env)
    printenv "$src" >/dev/null 2>&1 || { echo "environment variable $src is not set" >&2; exit 2; }
    jq --arg f "$field" --arg n "$src" \
      'if ($ENV[$n] // "") == "" then error("environment variable is empty") else . end
       | if (.input | type) != "object" then error("no input object") else . end
       | .input[$f] = $ENV[$n]' \
      "$in" > "$tmp"
    ;;
  *)
    echo "fourth argument must be --file or --env" >&2
    exit 2
    ;;
esac
chmod 600 "$tmp"
mv -f "$tmp" "$out"
trap - EXIT

echo "$field written to $out"
