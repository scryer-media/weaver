#!/bin/sh
# Print only what a write would change, for the operator to approve.
#
# Usage: show-change.sh <before.json> <after.json>
#   <before.json>  the current object: one server from ServerSettings
#                  (.data.server) or one category from ListCategories.
#                  For an add, a file holding {}.
#   <after.json>   the variables file for the write ({"input": {...}}), or
#                  the bare input object.
#
# Output lines: "path: before -> after" ("path: value" for an add). Fields
# the input leaves out are shown at what Weaver will store: the default or
# cleared value, or nothing when Weaver keeps the stored value (password,
# maxDownloadSpeed, downloadQuota, the route). The username and password are
# never printed: a value being sent shows as <new secret>, and an update that
# sends no username shows "username: <cleared if one is stored>". The adopted
# certificate shows as its length only.
set -eu

before=${1:?usage: show-change.sh <before.json> <after.json>}
after=${2:?usage: show-change.sh <before.json> <after.json>}

jq -n -r --slurpfile b "$before" --slurpfile a "$after" '
  ($a[0] | (.input // .)) as $A
  | ($b[0] // {}) as $B
  | (if $A | has("host") then "server" else "category" end) as $kind
  | ($B | length == 0) as $adding
  | (if $kind == "server" then
       ["host", "port", "tls", "connections", "active", "priority", "backfill",
        "retentionDays", "maxDownloadSpeed", "downloadQuota", "tlsCaCert",
        "tlsNameMismatchCertificateDerBase64"]
     else ["name", "destDir", "aliases"] end) as $fields
  | (if $kind == "server" then
       {active: true, priority: 0, backfill: false, retentionDays: 0,
        tlsCaCert: null, tlsNameMismatchCertificateDerBase64: null}
     else {destDir: null, aliases: ""} end) as $defaults
  | ["maxDownloadSpeed", "downloadQuota"] as $kept
  | def quota: if type == "object"
      then {enabled, limitBytes, period, resetTimeMinutesLocal,
            weeklyResetWeekday, monthlyResetDay}
      else . end;
    def show($k; $v):
      if $k == "tlsNameMismatchCertificateDerBase64" and ($v | type) == "string"
      then "<certificate, \($v | length) chars>"
      else ($v | tojson) end;
    def leaves($k; $v):
      if $k == "downloadQuota" and ($v | type) == "object"
      then ($v | quota | to_entries[] | ["\($k).\(.key)", .value])
      else [$k, $v] end;
  [ $fields[] as $k
    | if ($A | has($k)) then [$k, ($A[$k] | if $k == "downloadQuota" then quota else . end)]
      elif ($kept | index($k)) then empty
      elif ($defaults | has($k)) then [$k, $defaults[$k]]
      else empty end ] as $after_fields
  | if $adding then
      ($after_fields[] | . as [$k, $v] | leaves($k; $v) | "\(.[0]): \(show(.[0]; .[1]))"),
      (["username", "password"][] as $f | select($A | has($f)) | "\($f): <new secret>")
    else
      ([ $after_fields[] | . as [$k, $v]
         | leaves($k; $v) as [$p, $new]
         | ($p | split(".")) as $path
         | ($B | getpath($path)) as $old
         | select($old != $new)
         | "\($p): \(show($k; $old)) -> \(show($k; $new))" ]
       + [ ["username", "password"][] as $f | select($A | has($f)) | "\($f): <new secret>" ]
       + (if $kind == "server" and ($A | has("username") | not)
          then ["username: <cleared if one is stored>"] else [] end)) as $lines
      | if ($lines | length) == 0 then "no changes" else $lines[] end
    end
'
