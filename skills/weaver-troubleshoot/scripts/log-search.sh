#!/bin/sh
# Search saved Weaver service log records and print a capped, compact result.
#
# Usage:
#   log-search.sh <log-file> --summary
#       Record counts by level, then the 15 most frequent sources of WARN and
#       ERROR records.
#   log-search.sh <log-file> <regex> [level] [max-lines]
#       Records whose message or target matches <regex> (case-insensitive).
#       level filters to TRACE|DEBUG|INFO|WARN|ERROR. max-lines defaults to 40.
#
# <log-file> is either a saved ServiceLogs response
# (gql.sh operations/ServiceLogs.graphql vars.json > logs.json) or the
# bundle's service-log.txt. Reads the file only; writes nothing.
#
# Exit codes: 0 ok, 64 usage or environment.
set -eu

usage() {
  echo "usage: log-search.sh <logs.json|service-log.txt> --summary | <regex> [level] [max-lines]" >&2
  exit 64
}

[ $# -ge 2 ] && [ $# -le 4 ] || usage
file=$1
mode=$2
level=${3:-}
max=${4:-40}

[ -r "$file" ] || { echo "log-search.sh: cannot read $file" >&2; exit 64; }
command -v jq >/dev/null 2>&1 || { echo "log-search.sh: jq is required" >&2; exit 64; }
case $max in '' | *[!0-9]*) usage ;; esac
case $level in '' | TRACE | DEBUG | INFO | WARN | ERROR) ;; *) usage ;; esac

# One compact object per record on stdout: {t, l, g, m}.
records() {
  if jq -e '.data.serviceLogs.records | type == "array"' "$file" >/dev/null 2>&1; then
    jq -c '
      .data.serviceLogs.records[]
      | (fromjson? // {fields: {message: .}})
      | {t: (.timestamp // ""), l: (.level // ""), g: (.target // ""),
         m: ((.fields.message // "") | tostring)}
    ' "$file"
  else
    # service-log.txt: "<timestamp> <LEVEL> <spans> <target>: <message>".
    jq -R -c '
      (capture("^(?<t>\\S+)\\s+(?<l>[A-Z]+)\\s+(?<m>.*)$") // {t: "", l: "", m: .})
      | . + {g: ""}
    ' "$file"
  fi
}

if [ "$mode" = "--summary" ]; then
  records | jq -r '.l' | sort | uniq -c | sort -rn
  echo "top WARN/ERROR sources:"
  records | jq -r '
    select(.l == "WARN" or .l == "ERROR")
    | "\(.l) \(if .g != "" then .g else .m[0:60] end)"
  ' | sort | uniq -c | sort -rn | head -n 15
  exit 0
fi

records | jq -r --arg re "$mode" --arg lvl "$level" '
  select($lvl == "" or .l == $lvl)
  | select((.m + " " + .g) | test($re; "i"))
  | "\(.t) \(.l) \(.g) \(.m[0:400])"
' | head -n "$max"
