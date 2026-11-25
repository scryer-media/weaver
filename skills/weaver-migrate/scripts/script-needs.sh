#!/bin/sh
# List what an old client's script reads, by name only, so each need can be
# mapped to what Weaver hands a script. Never prints script content: every
# output line is a name built from a fixed pattern, then a count.
#
# Usage:
#   script-needs.sh <script-file>
#   script-needs.sh --command <file>
#
#   <script-file>  a local copy of the script. Reports:
#     - variables of the families SAB_, NZBPP_, NZBOP_, NZBPO_, NZBNA_,
#       NZBPR_, NZBNP_, NZBFP_, NZBSP_ and WEAVER_, wherever they appear
#     - positional arguments, as argv[N]: $1..$9 and ${1}..${9} (shell),
#       sys.argv[N] (Python), %1..%9 (batch), $args[N] (PowerShell, +1)
#     - other variables read by name: os.environ["X"], os.environ.get("X"),
#       os.getenv("X"), os.Getenv("X"), $env:X, as env:X
#     - os.environ used whole (copied or looped over), as env:(all)
#   --command <file>  a file holding a qBittorrent "run external program"
#     command line. Reports its %N %L %G %F %R %D %C %Z %T %I %J %K
#     placeholders.
#
# Output: "<name> <count>", one per line, sorted. Nothing found prints
# nothing and exits 0. A hit is a hint: confirm each with the operator.
set -eu

usage='usage: script-needs.sh <script-file> | script-needs.sh --command <file>'
command_mode=0
if [ "${1:-}" = "--command" ]; then
  command_mode=1
  shift
fi
file=${1:?$usage}
[ -f "$file" ] || { echo "not a file: $file" >&2; exit 2; }

count() {
  LC_ALL=C sort | LC_ALL=C uniq -c | awk '{ print $2 " " $1 }'
}

if [ "$command_mode" -eq 1 ]; then
  { grep -oE '%[NLGFRDCZTIJK]' "$file" || true; } | count
  exit 0
fi

{
  # Compatibility and Weaver families. A longer token that only contains one
  # (MY_SAB_X) is dropped.
  { grep -oE '[A-Za-z0-9_]*(SAB|NZBPP|NZBOP|NZBPO|NZBNA|NZBPR|NZBNP|NZBFP|NZBSP|WEAVER)_[A-Za-z0-9_]+' "$file" || true; } |
    { grep -E '^(SAB|NZBPP|NZBOP|NZBPO|NZBNA|NZBPR|NZBNP|NZBFP|NZBSP|WEAVER)_' || true; }

  # Positional arguments.
  { grep -oE '\$\{?[1-9]\}?' "$file" || true; } | tr -cd '0-9\n' | awk 'NF { print "argv[" $0 "]" }'
  { grep -oE 'sys\.argv\[[1-9][0-9]*\]' "$file" || true; } | tr -cd '0-9\n' | awk 'NF { print "argv[" $0 "]" }'
  { grep -oE '%[1-9]' "$file" || true; } | tr -cd '0-9\n' | awk 'NF { print "argv[" $0 "]" }'
  # shellcheck disable=SC2016 # a literal PowerShell token, not a shell expansion
  { grep -oE '\$args\[[0-9]+\]' "$file" || true; } | tr -cd '0-9\n' | awk 'NF { print "argv[" ($0 + 1) "]" }'

  # Other variables read by name.
  { grep -oE 'os\.environ(\.get)?[[(][[:space:]]*["'"'"'][A-Za-z_][A-Za-z0-9_]*["'"'"']' "$file" || true; } |
    sed -E 's/.*["'"'"']([A-Za-z_][A-Za-z0-9_]*)["'"'"']$/env:\1/'
  { grep -oE 'os\.(getenv|Getenv)\([[:space:]]*["'"'"'][A-Za-z_][A-Za-z0-9_]*["'"'"']' "$file" || true; } |
    sed -E 's/.*["'"'"']([A-Za-z_][A-Za-z0-9_]*)["'"'"']$/env:\1/'
  # shellcheck disable=SC2016 # a literal PowerShell token, not a shell expansion
  { grep -oiE '\$env:[A-Za-z_][A-Za-z0-9_]*' "$file" || true; } | sed -E 's/^\$[Ee][Nn][Vv]:/env:/'

  # The environment used whole.
  { grep -oE 'os\.environ([^.[A-Za-z0-9_]|$)|os\.environ\.(copy|items|keys|values)\(' "$file" || true; } |
    awk '{ print "env:(all)" }'
} | awk '
  # A family variable is also reported as env:NAME when read by name; keep
  # the family line only.
  /^env:(SAB|NZBPP|NZBOP|NZBPO|NZBNA|NZBPR|NZBNP|NZBFP|NZBSP|WEAVER)_/ { next }
  { print }
' | count
