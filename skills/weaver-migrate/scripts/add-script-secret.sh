#!/bin/sh
# Write the variables for CreateSecret ({"name", "value"}) or, with --id,
# UpdateSecret ({"id", "name", "value"}) without the value passing through
# the command line or stdout.
#
# Usage:
#   add-script-secret.sh <secret-name> <vars-out.json> --file <path> [--id <secret-id>]
#   add-script-secret.sh <secret-name> <vars-out.json> --env <VARIABLE> [--id <secret-id>]
#   add-script-secret.sh <secret-name> <vars-out.json> --ini <config-file> <key> [--id <secret-id>]
#
#   --file reads the value from a file the operator wrote (one trailing
#          newline is removed).
#   --env  reads it from an environment variable the operator exported.
#   --ini  reads it from the old client's config file: the one line whose
#          name, before the first "=", is exactly <key> once surrounding
#          spaces are trimmed. The value is the rest of that line, spaces
#          trimmed. Refused when the key is on no line or on more than one
#          (a key repeated in several sections): use --file then.
#
# <vars-out.json> is written with mode 600, replacing any existing file and
# whatever mode it had. It contains the value: keep it in a private directory
# and tell the operator where it is. Prints only the secret name and the
# output path.
set -eu

usage='usage: add-script-secret.sh <secret-name> <vars-out.json> --file <path> | --env <VARIABLE> | --ini <config-file> <key> [--id <secret-id>]'
name=${1:?$usage}
out=${2:?$usage}
mode=${3:?$usage}
src=${4:?$usage}
shift 4
key=
if [ "$mode" = "--ini" ]; then
  key=${1:?missing key for --ini}
  shift
fi
id=
if [ "$#" -gt 0 ]; then
  [ "$1" = "--id" ] || { echo "$usage" >&2; exit 2; }
  id=${2:?missing secret id after --id}
  shift 2
fi
[ "$#" -eq 0 ] || { echo "$usage" >&2; exit 2; }

umask 077
# Written to a new private file and renamed into place, so an existing output
# file never keeps a wider mode and is never left half written.
tmp=$(mktemp "$out.XXXXXX")
val=
trap 'rm -f "$tmp" ${val:+"$val"}' EXIT

case "$mode" in
  --file)
    [ -s "$src" ] || { echo "secret file is missing or empty" >&2; exit 2; }
    jq -n --arg n "$name" --arg id "$id" --rawfile s "$src" \
      '{name: $n, value: ($s | sub("\r?\n$"; ""))}
       | if $id != "" then .id = $id else . end
       | if .value == "" then error("secret is empty") else . end' > "$tmp"
    ;;
  --env)
    printenv "$src" >/dev/null 2>&1 || { echo "environment variable $src is not set" >&2; exit 2; }
    jq -n --arg n "$name" --arg id "$id" --arg v "$src" \
      'if ($ENV[$v] // "") == "" then error("environment variable is empty") else . end
       | {name: $n, value: $ENV[$v]}
       | if $id != "" then .id = $id else . end' > "$tmp"
    ;;
  --ini)
    [ -f "$src" ] || { echo "config file not found" >&2; exit 2; }
    val=$(mktemp "$out.value.XXXXXX")
    # The value goes from awk straight to a private file; awk's exit status
    # carries only the number of matching lines.
    set +e
    awk -v k="$key" -v dest="$val" '
      {
        sub(/\r$/, "")
        eq = index($0, "=")
        if (eq == 0) next
        left = substr($0, 1, eq - 1)
        gsub(/^[ \t]+|[ \t]+$/, "", left)
        if (left != k) next
        hits++
        right = substr($0, eq + 1)
        gsub(/^[ \t]+|[ \t]+$/, "", right)
        if (hits == 1) printf "%s", right > dest
      }
      END { exit (hits > 9 ? 9 : hits) }
    ' "$src"
    hits=$?
    set -e
    case "$hits" in
      1) ;;
      0) echo "key not found in the config file: give the value with --file" >&2; exit 2 ;;
      *) echo "key is on more than one line: give the value with --file" >&2; exit 2 ;;
    esac
    [ -s "$val" ] || { echo "the key has an empty value" >&2; exit 2; }
    jq -n --arg n "$name" --arg id "$id" --rawfile s "$val" \
      '{name: $n, value: $s} | if $id != "" then .id = $id else . end' > "$tmp"
    ;;
  *)
    echo "third argument must be --file, --env or --ini" >&2
    exit 2
    ;;
esac
chmod 600 "$tmp"
mv -f "$tmp" "$out"

echo "secret $name written to $out"
