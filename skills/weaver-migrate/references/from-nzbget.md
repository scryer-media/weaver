# From NZBGet

## What carries over

| Item | How |
|---|---|
| News servers | `weaver-configure` (`references/servers.md` there) |
| Categories | `weaver-configure` (`references/categories.md` there) |
| Extension scripts (post-processing, queue, scan, scheduler, feed) | this skill, one job per trigger |
| Queue, history, downloads in progress | not imported: finish them in NZBGet or re-add them in Weaver |
| Everything else (RSS, schedules, dupe settings) | not covered here; the Weaver web UI |

Weaver has no import path for NZBGet: not `nzbget.conf`, not its queue or
history.

## Scripts

Weaver reads NZBGet's script header (`### NZBGET ... SCRIPT`) and v2
`manifest.json`, sets the `NZBPP_` / `NZBNA_` / `NZBNP_` / `NZBSP_` /
`NZBFP_` / `NZBPO_` / `NZBPR_` families, and honours exit codes `93`, `95`
and `[NZB]` commands. Most scripts run unchanged. A script with a header:
`SetUpScriptFromHeader`.

Option values set in NZBGet (`<Script>:<Option>=` lines in `nzbget.conf`) do
not carry over by themselves: each becomes an input on the job.

## Inputs

`script-needs.sh` output → Weaver. A name not in this table, or a row marked
"no equivalent", needs a changed copy.

| Script reads | Weaver |
|---|---|
| `NZBPP_NZBID` `NZBPP_NZBNAME` `NZBPP_DIRECTORY` `NZBPP_NZBFILENAME` `NZBPP_URL` `NZBPP_CATEGORY` `NZBPP_TAGS` | same meaning |
| `NZBPP_QUEUEDFILE` | equals `NZBPP_NZBFILENAME` |
| `NZBPP_FINALDIR` | the final folder a previous script set, else `NZBPP_DIRECTORY` |
| `NZBPP_STATUS` `NZBPP_TOTALSTATUS` `NZBPP_SCRIPTSTATUS` `NZBPP_PARSTATUS` `NZBPP_UNPACKSTATUS` `NZBPP_HEALTH` `NZBPP_CRITICALHEALTH` | same meaning; Weaver's repair and unpack set them |
| any other `NZBPP_*` | not set: no equivalent |
| `NZBNA_*` (queue) | the `NZBNA_` list in `references/scripts.md`; `NZBNA_QUEUEDFILE` always empty |
| `NZBNP_*` (scan) | the `NZBNP_` list in `references/scripts.md` |
| `NZBSP_TASKID` | always set; `0` in a test run |
| `NZBFP_FEEDID` `NZBFP_FILENAME` | same meaning |
| `NZBPO_<Option>` | the job input of that name |
| `NZBPR_<name>` | the download's parameters |
| `NZBOP_Version` | `16.0-weaver`: check any minimum-version test in the script |
| `NZBOP_ControlIP` `ControlPort` `ControlUsername` `ControlPassword` | Weaver's NZBGet API, with a token for this run |
| `NZBOP_AppDir` `MainDir` `InterDir` `DestDir` `TempDir` | Weaver's folders, when known |
| any other `NZBOP_*` (NZBGet's own settings) | not set: no equivalent. Make it an input and read `NZBPO_<Name>` |
| `env:NAME` (other) | not passed through: make `NAME` an input |
| `env:(all)` | only the listed host variables and Weaver's: check what it relies on |

Exit codes: `93` success, `95` skipped, `92` success (no PAR check runs),
`94` and anything else failure.

Calling back over JSON-RPC or XML-RPC with the `NZBOP_Control*` values works
for queue methods: `version status listgroups listfiles history config
loadconfig log loadlog postqueue urlqueue servervolumes append appendurl
editqueue pausedownload resumedownload pausedownload2 resumedownload2
pausepost resumepost pausescan resumescan scheduleresume rate writelog`. Any
other method (feeds, `loadextensions`) is refused: no equivalent.

## Example change

The script read NZBGet's own `Unpack` setting:

```diff
-if [ "$NZBOP_UNPACK" != "yes" ]; then exit 95; fi
+if [ "$NZBPO_UNPACK" != "yes" ]; then exit 95; fi
```

Then an input `Unpack` with the value the operator states.

## Old config file

`nzbget.conf` holds server passwords and the control password. Never print
it. To list a script's option names without values:

```sh
grep -E '^<Script>:' nzbget.conf | cut -d= -f1
```

A non-secret value: the operator names the key; read that one line with
`grep -m1 -E '^<Script>:<Option>=' nzbget.conf | cut -d= -f2-`. A secret
value moves file to file:
`add-script-secret.sh <name> vars.json --ini nzbget.conf '<Script>:<Option>'`.

## Pointing an Arr app at Weaver

Keep the Arr app's **NZBGet** download client type; change it to point at
Weaver:

| Field | Value |
|---|---|
| Host, port, TLS | from `WEAVER_URL` |
| URL base | Weaver's base path from `WEAVER_URL`, if any (the API sits at `<base>/jsonrpc`) |
| Username | anything (ignored) |
| Password | a Weaver API key with **Control the queue** scope, made by the operator |
| Category | a Weaver category name or alias |

NZBGet's control username and password do not carry over. A category the
client sends is matched to a Weaver category by name (case-insensitive) or
alias; an unmatched name becomes a folder of that name under Weaver's
complete folder. The client is shown the name it sent.

Keep both clients until the operator cuts over. The cut-over is the
operator's.
