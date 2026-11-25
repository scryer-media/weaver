# Weaver's script contract

What Weaver hands a script and what it takes back. A script lives in the
scripts directory; a **script job** wires it to one trigger with its inputs.

## Scripts directory

- `ScriptSettings` → `scriptDirectory`. The path is as Weaver sees it (in a
  container, the path inside it). First start: `WEAVER_SCRIPTS_DIR`, else
  `<data dir>/scripts`; after that the saved value wins.
- Weaver never uploads, edits or deletes a script file. The operator copies
  scripts in.
- Changing the directory turns **every** script job off. Web UI only:
  **Settings → Scripts → Configuration**, **Scripts directory**.
- A script file directly in the directory is named by its file name
  (`notify.py`). An NZBGet package folder with `manifest.json` is named by its
  manifest `name`. `DiscoveredScripts` lists the names, and `problems` lists
  files it could not read.

## Execution switch

- `executionEnabled` is off on a new install. Off: scripts are listed, never
  run, and test runs are refused (`post-processing script execution is
  disabled`). Web label: **Run scripts**.
- `strictSecurityRefusesExecution` true (`WEAVER_STRICT_SECURITY=1` on the
  host): nothing runs and turning execution on is refused. Host setting; stop.

## Triggers

| `trigger` | Runs when | Arguments |
|---|---|---|
| `POST_PROCESSING` | an NZB download finishes Weaver's own repair and unpack | 8 (SABnzbd order) |
| `QUEUE` + `queueEvent` | the event below | none |
| `SCAN` | an NZB is added, before it is queued | none |
| `SCHEDULER` | at its schedule's times | none |
| `FEED` | an RSS feed is fetched, before Weaver reads it | none |

Torrents never run `POST_PROCESSING`. A finished torrent raises
`TORRENT_DOWNLOADED` and `JOB_DOWNLOADED`; nothing waits for a torrent's
scripts.

Queue events: `NZB_ADDED` `NZB_DOWNLOADED` `NZB_DELETED` `NZB_MARKED`
`FILE_DOWNLOADED` `URL_COMPLETED`; `TORRENT_ADDED` `TORRENT_DOWNLOADED`
`TORRENT_DELETED` `TORRENT_MARKED`; `JOB_ADDED` `JOB_DOWNLOADED` `JOB_DELETED`
(every job, beside the NZB or torrent event); `SEEDING_FINISHED`
`SEEDING_STOPPED`; `QUOTA_NORMAL` `QUOTA_TRICKLE` `QUOTA_STOPPED`;
`ROUTE_DOWN` `ROUTE_UP`. `NZB_NAMED` is accepted but never raised.

One job per trigger. A header that declares several triggers or queue events
becomes several jobs.

## Job settings

- `categories`: empty runs for every category. Only `POST_PROCESSING` and
  `QUEUE` jobs can be narrowed.
- `globalScriptsRun`: `ALWAYS` runs every-category jobs, then the category's
  own. `ONLY_WITHOUT_CATEGORY_SCRIPTS`: a category with its own jobs runs only
  those.
- `blocking` true: runs in line; the next job and the download wait. False:
  started and left; its result changes nothing.
- Timeout: `timeoutSeconds`, else `POST_PROCESSING` 24 hours, others
  `eventScriptTimeoutSeconds` (default 300). Past it: stopped, after
  `terminationGraceSeconds` (default 10) killed.

## How a script is started

| Extension | Started as |
|---|---|
| `.py` | `pythonInterpreter`, else `python3` |
| `.ps1` | `pwsh -NoProfile -NonInteractive -File` |
| `.bat` `.cmd` | `batchInterpreter`, else `COMSPEC`, else `cmd.exe`, with `/D /S /C` |
| `.go` | `go run`: one file, standard library only, no module download |
| other | executed directly; on Unix a non-executable file with a `#!` line runs through it |

- Working directory: the download's final folder (`POST_PROCESSING`), else
  the event's folder.
- Host environment: only `PATH HOME USERPROFILE SYSTEMROOT WINDIR COMSPEC
  PATHEXT TEMP TMP TMPDIR LANG LC_ALL TZ`. Anything else a script read from
  the old client's environment is gone: make it an input.

## Variables, every trigger

| Variable | Value |
|---|---|
| `WEAVER_RUN_ID` `WEAVER_INSTANCE_ID` `WEAVER_INSTANCE_NAME` `WEAVER_TRIGGER` | this run and job |
| `WEAVER_VERSION` | Weaver's version |
| `WEAVER_DATA_DIR` `WEAVER_COMPLETE_DIR` | Weaver's folders, when known |
| `WEAVER_API_URL` `WEAVER_RUN_TOKEN` | GraphQL URL and a token for this run only (below) |
| `WEAVER_INPUT_<NAME>` `NZBPO_<Name>` `SAB_OPTION_<NAME>` | each job input |
| `NZBOP_Version` | `16.0-weaver` |
| `NZBOP_ControlIP` `ControlPort` `ControlUsername` `ControlPassword` | the NZBGet facade; username `weaver`, password the run token |
| `NZBOP_AppDir` `MainDir` `InterDir` `DestDir` `TempDir` | Weaver's folders, when known |
| `NZBPR_<name>` | the download's parameters |

`<NAME>`: upper case, every character outside `A-Z 0-9` → `_`. `NZBPO_`,
`NZBOP_` and `NZBPR_` are set both as written and in that form
(`NZBOP_ControlIP` and `NZBOP_CONTROLIP`).

## Variables, `POST_PROCESSING`

| Weaver | SABnzbd form | NZBGet form |
|---|---|---|
| `WEAVER_JOB_ID` | `SAB_NZO_ID` | `NZBPP_NZBID` |
| `WEAVER_JOB_NAME` | `SAB_FINAL_NAME` | `NZBPP_NZBNAME` |
| `WEAVER_CATEGORY` | `SAB_CAT` | `NZBPP_CATEGORY` |
| `WEAVER_DIRECTORY` | `SAB_COMPLETE_DIR` | `NZBPP_DIRECTORY` |
| `WEAVER_FINAL_DIRECTORY` | | `NZBPP_FINALDIR` |
| `WEAVER_STATUS` | | `NZBPP_TOTALSTATUS` |
| | `SAB_FILENAME` | `NZBPP_NZBFILENAME` `NZBPP_QUEUEDFILE` |
| | `SAB_URL` | `NZBPP_URL` |
| | `SAB_PP_STATUS` | `NZBPP_STATUS` |

Also set: `SAB_VERSION` (Weaver's), `SAB_GROUP`, `SAB_STATUS` (`Running`),
`SAB_FAIL_MSG`, `SAB_FAILURE_URL` (empty), `SAB_BYTES`,
`SAB_BYTES_DOWNLOADED`, `SAB_BYTES_TRIED` (= downloaded), `SAB_PASSWORD`,
`SAB_REPAIR`, `SAB_UNPACK`, `SAB_SCRIPT`, `SAB_PROGRAM_DIR` (when known);
`NZBPP_TAGS`, `NZBPP_SCRIPTSTATUS`, `NZBPP_PARSTATUS`, `NZBPP_UNPACKSTATUS`,
`NZBPP_HEALTH`, `NZBPP_CRITICALHEALTH` (per mille).

Set but always empty: `SAB_CORRECT_PASSWORD` `SAB_DUPLICATE`
`SAB_DUPLICATE_KEY` `SAB_ENCRYPTED` `SAB_OVERSIZED` `SAB_PP` `SAB_PRIORITY`
`SAB_UNWANTED_EXT`.

Status values:
- `SAB_PP_STATUS`: `0` ok, `1` verify or repair failed, `2` unpack or move
  failed, `-1` download failed.
- `NZBPP_STATUS`: `SUCCESS/ALL`, `SUCCESS/HEALTH`, `FAILURE/BAD`,
  `FAILURE/HEALTH`, `FAILURE/PAR`, `FAILURE/UNPACK`, `FAILURE/MOVE`.
  `NZBPP_TOTALSTATUS` is the part before `/`.

Arguments (SABnzbd order): `1` final folder, `2` NZB file name, `3` job name,
`4` empty, `5` category, `6` group, `7` `SAB_PP_STATUS`, `8` empty.

## Variables, other triggers

- `QUEUE`: `WEAVER_JOB_ID` `WEAVER_JOB_NAME` `WEAVER_CATEGORY`
  `WEAVER_DIRECTORY`; `NZBNA_EVENT NZBID LASTID NZBNAME FILENAME QUEUEDFILE
  DIRECTORY URL CATEGORY PRIORITY DELETESTATUS MARKSTATUS URLSTATUS DUPEKEY
  DUPESCORE DUPEMODE TAGS` (each `NZBNA_`-prefixed). Torrent events add
  `WEAVER_PROTOCOL` (`torrent`), `WEAVER_INFOHASH_V1`, `WEAVER_INFOHASH_V2`,
  `WEAVER_TRACKER`. `SEEDING_FINISHED`: `WEAVER_SEEDING_ACTION`.
  `SEEDING_STOPPED`: `WEAVER_SEEDING_STOPPED_REASON`. Quota and route events:
  `WEAVER_EGRESS_ID`, `WEAVER_QUOTA_DIRECTION`, `WEAVER_QUOTA_STATE`,
  `WEAVER_ROUTE_KIND`, `WEAVER_ROUTE_NAME`, `WEAVER_ROUTE_REASON`.
- `SCAN`: `NZBNP_DIRECTORY FILENAME NZBNAME URL CATEGORY PRIORITY TOP PAUSED
  DUPEKEY DUPESCORE DUPEMODE` (each `NZBNP_`-prefixed).
- `SCHEDULER`: `NZBSP_TASKID`.
- `FEED`: `NZBFP_FEEDID`, `NZBFP_FILENAME`.

No `SAB_*` job variables and no `NZBPP_*` outside `POST_PROCESSING`.

## Exit codes

| Code | Result |
|---|---|
| `0` `93` | succeeded |
| `92` | succeeded; the PAR-check request is ignored (Weaver's repair already ran) |
| `95` | skipped |
| anything else, or no exit (killed, timed out) | failed |

So `94` is a failure. A later job sees earlier results in
`NZBPP_SCRIPTSTATUS`.

## Printed lines

- Log levels: a line starting `[INFO] `, `[WARNING] `, `[ERROR] `,
  `[DETAIL] ` or `[DEBUG] `.
- Commands: `[NZB] KEY=value`. Applied as printed; a test run applies none.

| Command | Post-processing | Queue | Scan |
|---|---|---|---|
| `NZBPR_<name>=v` | yes | yes | yes |
| `MARK=BAD` | yes | yes | no |
| `DIRECTORY=path` | yes | downloaded events only | no |
| `FINALDIR=path` | yes | no | no |
| `NZBNAME` `CATEGORY` `PRIORITY` `TOP` `PAUSED` `DUPEKEY` `DUPESCORE` `DUPEMODE` | no | no | yes |

Scheduler and feed scripts: no commands. A command not allowed logs
`Command X is not allowed for <event>`; a malformed one logs
`Invalid command`.

## Calling Weaver back

- NZBGet style: JSON-RPC or XML-RPC to `NZBOP_ControlIP:ControlPort` with
  `ControlUsername`/`ControlPassword`. Queue methods only; no feeds.
- Weaver style: `POST $WEAVER_API_URL` with
  `Authorization: Bearer $WEAVER_RUN_TOKEN`. The token reads the queue, history
  and its own run, and changes its run only through `scriptRun` (the same
  commands as `[NZB]`). Anything else: `NOT_ALLOWED_FOR_SCRIPT_RUN`.
- The token dies when the run ends. The `ScriptRun` scope is that token's; it
  is never an API key.

## Header

- Bare script, no header: SABnzbd adapter. Header in the first comment block
  `### NZBGET <KINDS> SCRIPT` (kinds `POST-PROCESSING`, `QUEUE`, `SCAN`,
  `SCHEDULER`, `FEED`, joined by `/`): NZBGet adapter. Go scripts write it in
  `//` comments.
- `### QUEUE EVENTS: A, B` narrows queue events (none listed = all).
  `### TASK TIME: 03:00;*:15` sets scheduler times (`*` = at start-up).
- `### OPTIONS ###` section: `#Name=default` lines become inputs. A name with
  a whole word `key`, `apikey`, `token`, `password`, `passwd`, `passphrase`,
  `secret`, or ending in `pass`, becomes a secret input with no default.
- NZBGet package: `manifest.json` (NZBGet v2 manifest) beside `main`. Options
  marked `"secret": true` are secret inputs.
- `setUpScriptFromHeader` reads the header once. Later header edits change no
  job; `headerDrift` true says the inputs no longer match. Report it; this
  skill does not reapply a header.

## Inputs and secrets

- Plain input: `{"name", "value"}`. Read back as written.
- Named secret: `CreateSecret`, then `{"name", "secretId"}`. Write-only;
  shared by every job that links it; `UpdateSecret` changes all of them.
- Sealed input: `{"name", "value", "secret": true}`, kept with the job only.
  On update `{"name", "secret": true}` keeps it. This skill links named
  secrets instead, so values come only from `add-script-secret.sh`.
- Secret values are redacted from captured output.

## Test run

`TestScriptJob` runs the job's script once with the same variables and
arguments a real run gets, built from a made-up download
(`Weaver.Test.Download`, job id 2000000000, one file `weaver-test.txt`) in a
scratch folder under Weaver's data folder, removed afterwards. Inputs and
secrets are real. Commands are listed in `commands`, never applied. Held to
the job's timeout. A disabled job can be tested. Refused while execution is
off.

`ScriptTestResult`: `running` false → done. `status`: `SUCCEEDED`, `SKIPPED`,
`WARNING`, `FAILED`, `TIMED_OUT`, `CANCELLED`, `INTERRUPTED`.
