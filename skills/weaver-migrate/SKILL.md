---
name: weaver-migrate
description: Moves an operator from SABnzbd, NZBGet or qBittorrent to Weaver - maps each old post-processing, extension or "run external program" script to Weaver's script contract, wires it as a script job with its inputs and secrets, test-runs it on a yes, and lists what carries over, what does not, and how to point an Arr app at Weaver. Use when an operator wants to bring scripts or settings over from SABnzbd, NZBGet or qBittorrent to their own Weaver server.
---

# Migrate to Weaver

Script migration first. Servers and categories go through `weaver-configure`.
Reads are safe. Every write and every test run is shown to the operator first
and waits for a yes.

## Prerequisites

- Complete `weaver-connect` first (sets `WEAVER_URL`, `WEAVER_API_KEY`;
  provides `../weaver-connect/scripts/gql.sh`, below
  `G="$S"/../weaver-connect/scripts/gql.sh`).
- `S` = this skill's directory (absolute). Work in a separate directory the
  operator agrees to. Files that hold a secret go in a private subdirectory
  of it.
- Scope: `ADMIN` (**Full administration**).
- Local tools: `jq`, `grep`, `awk`.

## Operations

| File | Root field | Scope |
|---|---|---|
| `ScriptSettings` | `postProcessingSettings` | ADMIN |
| `SetScriptSettings` | `setPostProcessingSettings` | ADMIN |
| `DiscoveredScripts` | `discoveredScripts` | ADMIN |
| `ListScriptJobs` | `scriptInstances` | ADMIN |
| `ScriptJob` | `scriptInstance` | ADMIN |
| `CreateScriptJob` | `createScriptInstance` | ADMIN |
| `UpdateScriptJob` | `updateScriptInstance` | ADMIN |
| `SetUpScriptFromHeader` | `setUpScriptFromHeader` | ADMIN |
| `ListSecrets` | `secrets` | ADMIN |
| `CreateSecret` | `createSecret` | ADMIN |
| `UpdateSecret` | `updateSecret` | ADMIN |
| `TestScriptJob` | `testScriptInstance` | ADMIN |
| `ScriptTestResult` | `scriptTestRun` | ADMIN |
| `CancelScriptTest` | `cancelScriptTest` | ADMIN |
| `ScriptJobResults` | `postProcessingResults` | READ |

Each file's leading comment gives its purpose and a variables example.

## References

- `references/scripts.md`: Weaver's script contract. Read it first.
- `references/from-sabnzbd.md`, `references/from-nzbget.md`,
  `references/from-qbittorrent.md`: load only the one for the old client.

## Rules

- **Old config files hold secrets.** `sabnzbd.ini`, `nzbget.conf` and
  `qBittorrent.conf` are never printed, `cat`, opened whole or pasted from.
  A non-secret value comes only from a narrow grep for a key the operator
  names. A secret moves file to file with `scripts/add-script-secret.sh`;
  failing that, the operator types it into a file and gives the path.
- **Never contact, stop, uninstall or change the old client.** Both run side
  by side; the cut-over is the operator's.
- **Never edit a file in Weaver's scripts directory.** A changed script is a
  copy in the work directory; the operator installs it.
- **Never delete** a script job or a secret. Give the web UI area:
  Settings → Scripts.
- **Global script settings** change only after a yes that names each setting.
  `SetScriptSettings` clears an interpreter left out: copy all four from
  `ScriptSettings`. Never change the scripts directory: changing it turns
  every job off; give the web UI field (Settings → Scripts → Configuration,
  "Scripts directory").
- **Never select a secret back.** Script output and test logs: quote only the
  lines that matter.
- **No queue, history or in-flight migration.** Say once: "Weaver imports no
  queue or history: finish those in the old client or re-add them in Weaver."
- Every response to a file; read with jq.

## Procedure

1. **Read the settings**:
   `sh "$G" "$S"/operations/ScriptSettings.graphql > settings.json`. Note
   `scriptDirectory`, `executionEnabled` and the interpreters.

2. **Copy the script in.** The operator copies the script (and any files it
   needs) into `scriptDirectory` on the Weaver host, and a local copy into the
   work directory. Confirm with `DiscoveredScripts`: the script is listed
   under `scripts` (with what its header declares), or its reason is under
   `problems`.

3. **List what it reads**:

   ```sh
   sh "$S"/scripts/script-needs.sh ./notify.py
   sh "$S"/scripts/script-needs.sh --command ./qbit-command.txt
   ```

   The second form is for a qBittorrent command line. Output is names and
   counts only.

4. **Map each need** with the old client's table. Report every "no
   equivalent" row to the operator in one list.

5. **Unchanged or changed copy.**
   - Every need met by a compatibility variable: wire the script unchanged.
   - Otherwise write a changed copy in the work directory, show
     `diff -u <original> <copy>`, and the operator installs it in the
     scripts directory. qBittorrent always needs a wrapper
     (`references/from-qbittorrent.md`).

6. **Create the job**, after a yes on a summary: script, trigger, queue
   event, categories, blocking, enabled, inputs by name (secret inputs as
   "linked to `<secret name>`").
   - Script with a header (`references/scripts.md`, "Header"):
     `SetUpScriptFromHeader` with `{"script": "<name>"}`. The summary says
     the jobs arrive **enabled**. When `executionEnabled` is true, follow
     straight away with `UpdateScriptJob` setting `enabled: false`, unless the
     operator wants them live.
   - Otherwise: `CreateScriptJob` with `"enabled": false`.

7. **Inputs and secrets.**
   - Plain input: in the job's `inputs` as `{"name", "value"}`.
   - Secret: check `ListSecrets` for an existing name. New one:

     ```sh
     sh "$S"/scripts/add-script-secret.sh pushover private/vars.json --file private/value.txt
     sh "$G" "$S"/operations/CreateSecret.graphql private/vars.json > created.json
     ```

     Then link it: `{"name": "PUSHOVER_TOKEN", "secretId": "<id>"}` through
     `UpdateScriptJob`, built from `ScriptJob` (it replaces the job; sealed
     inputs go back as `{"name", "secret": true}`).
   - From an old config file: `--ini <file> <key>` instead of `--file`.

8. **Test run**, only after a yes on: "This executes `<script>` on the Weaver
   host, against a made-up download in a scratch folder, with the job's real
   inputs and secrets." If `executionEnabled` is false, the test is refused:
   turning it on needs its own yes naming `executionEnabled`, and makes every
   enabled job live. Then `TestScriptJob` with `{"id": "<job id>"}`.

9. **Report**: read `ScriptTestResult` with `{"id": "<test run id>"}` once,
   and again only when the operator asks. When `running` is false, report
   `status`, `exitCode`, `durationMs`, the size of `log`
   (`jq -r '.data.scriptTestRun.log | length'`), `logTruncated`, and quote only
   the log lines that matter. `CancelScriptTest` on request.

10. **Enable or leave disabled**, as the operator says: `UpdateScriptJob`
    with `enabled: true`, after a yes.

11. **Arr apps and the rest**: give the old client's "Pointing an Arr app at
    Weaver" table and its "What carries over" list. The operator makes the
    API key and changes the Arr app.

12. **Hand back secret files.** Name the local files that hold a secret so the
    operator can dispose of them. Do not remove them yourself.

## Stop and ask

- Before every write, every test run and every global setting change.
- When a need has no equivalent and the script decides on it: changed copy,
  or leave it in the old client.
- When the scripts directory or the work directory is unclear.
- When the request needs something not covered: queue or history import,
  removals, the scripts directory, RSS, schedules, torrents settings. Give the
  web UI area instead.
