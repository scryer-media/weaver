# From SABnzbd

## What carries over

| Item | How |
|---|---|
| News servers | `weaver-configure` (`references/servers.md` there) |
| Categories | `weaver-configure` (`references/categories.md` there) |
| Post-processing scripts | this skill, as `POST_PROCESSING` jobs |
| Queue, history, downloads in progress | not imported: finish them in SABnzbd or re-add them in Weaver |
| Everything else (RSS, schedules, sorting, notifications) | not covered here; the Weaver web UI |

Weaver has no import path for SABnzbd: not `sabnzbd.ini`, not its queue or
history.

## Scripts

Weaver runs a script without an NZBGet header under the SABnzbd contract, as
post-processing only: same 8 arguments in the same order, `SAB_*` variables,
exit `0` for success. Most scripts run unchanged.

A SABnzbd pre-queue or notification script has no Weaver equivalent. Leave it
in SABnzbd and tell the operator, or rewrite it for a `SCAN` or `QUEUE` job
against `references/scripts.md`.

## Inputs

`script-needs.sh` output → Weaver. A name not in this table, or a row marked
"no equivalent", needs a changed copy.

| Script reads | Weaver |
|---|---|
| `argv[1]` final folder | same |
| `argv[2]` NZB file name | same |
| `argv[3]` job name | same |
| `argv[4]` | always empty |
| `argv[5]` category | same |
| `argv[6]` group | same |
| `argv[7]` status | same (`0` ok, `1` verify/repair, `2` unpack/move, `-1` download) |
| `argv[8]` | always empty |
| `SAB_NZO_ID` | Weaver job id |
| `SAB_FINAL_NAME` `SAB_FILENAME` `SAB_CAT` `SAB_GROUP` `SAB_COMPLETE_DIR` | same meaning |
| `SAB_PP_STATUS` `SAB_FAIL_MSG` `SAB_URL` `SAB_PASSWORD` | same meaning |
| `SAB_BYTES` `SAB_BYTES_DOWNLOADED` | same meaning |
| `SAB_BYTES_TRIED` | equals `SAB_BYTES_DOWNLOADED` |
| `SAB_REPAIR` `SAB_UNPACK` | `1` when `NZBPP_PARSTATUS` / `NZBPP_UNPACKSTATUS` is not `0` |
| `SAB_STATUS` | always `Running` |
| `SAB_VERSION` | Weaver's version: a version check written for SABnzbd may misread it |
| `SAB_SCRIPT` | the script's name |
| `SAB_PROGRAM_DIR` | Weaver's program folder, when known |
| `SAB_FAILURE_URL` | always empty: no equivalent |
| `SAB_CORRECT_PASSWORD` `SAB_DUPLICATE` `SAB_DUPLICATE_KEY` `SAB_ENCRYPTED` `SAB_OVERSIZED` `SAB_PP` `SAB_PRIORITY` `SAB_UNWANTED_EXT` | always empty: no equivalent |
| any other `SAB_*` | not set: no equivalent |
| `env:NAME` (non-SAB) | not passed through: make `NAME` an input and read `SAB_OPTION_NAME` |
| `env:(all)` | only the listed host variables and Weaver's: check what it relies on |
| calls to SABnzbd's API | Weaver has no SABnzbd API: no equivalent. Rewrite to call back as in `references/scripts.md`, "Calling Weaver back" |

"No equivalent" with a script that only logs the value: wire unchanged and
tell the operator the value is empty. With a script that decides on it:
changed copy, or leave the script in SABnzbd.

## Example change

SABnzbd passed the variable `PUSHOVER_TOKEN` from its own environment:

```diff
-token = os.getenv("PUSHOVER_TOKEN")
+token = os.getenv("SAB_OPTION_PUSHOVER_TOKEN")
```

Then an input named `PUSHOVER_TOKEN`, linked to a secret.

## Old config file

`sabnzbd.ini` holds passwords and API keys. Never print it. A secret the
script needs moves file to file:
`add-script-secret.sh <name> vars.json --ini sabnzbd.ini <key>`, the key named
by the operator. A key that appears in several sections (one per server or
category) is refused: the operator writes that value to a file instead.

## Pointing an Arr app at Weaver

Weaver has no SABnzbd API. In the Arr app, add a **NZBGet** download client
for Weaver:

| Field | Value |
|---|---|
| Host, port, TLS | from `WEAVER_URL` |
| URL base | Weaver's base path from `WEAVER_URL`, if any (the API sits at `<base>/jsonrpc`) |
| Username | anything (ignored) |
| Password | a Weaver API key with **Control the queue** scope, made by the operator |
| Category | a Weaver category name or alias |

A category the client sends is matched to a Weaver category by name
(case-insensitive) or alias. An unmatched name becomes a folder of that name
under Weaver's complete folder. The client is shown the name it sent.

Keep both clients until the operator cuts over. The cut-over (switching or
removing the SABnzbd client in the Arr app) is the operator's.
