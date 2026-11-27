---
name: weaver-troubleshoot
description: Diagnoses problems on an operator's own Weaver download server from read-only API queries and the diagnostics bundle. Use when an operator says a Weaver download is stuck, a job failed or ended Partial, a news server is unreachable, a disk is full or slow, or a torrent does not connect.
---

# Troubleshoot Weaver

Find the cause reading as little as possible. Diagnosis is read-only: this
skill runs no mutation of any kind. Bulk data to files; read only counts, jq
extracts and capped searches.

## Prerequisites

- Complete `weaver-connect` first (sets `WEAVER_URL`, `WEAVER_API_KEY`;
  provides `../weaver-connect/scripts/gql.sh`).
- `S` = this skill's directory (absolute). Work in a separate directory the
  operator agrees to, so output never lands inside the skill.
- Scope: `READ` covers every operation but `ServiceLogs`. `ServiceLogs`, the
  bundle and the two MCP diagnostics tools need `ADMIN`. Without it, skip those steps and say so.
- Local tools: `curl`, `jq`; `zstd` and `tar` to unpack a bundle.

## Operations

| File | Root fields | Scope |
|---|---|---|
| `QueueState` | `globalQueueState`, `queueSummary` | READ |
| `QueuePage` | `queuePage` | READ |
| `JobDetail` | `jobDetailSnapshot` | READ |
| `JobEvents` | `jobEvents` | READ |
| `HistoryPage` | `historyPage` | READ |
| `ServerHealth` | `serverHealth` | READ |
| `TorrentsPage` | `torrents` | READ |
| `TorrentDetail` | `torrent` | READ |
| `TorrentNetwork` | `torrentPresence`, `torrentRoutes` | READ |
| `DiskState` | `diskUsage`, `systemInfo` | READ |
| `SystemFacts` | `systemInfo`, `updateStatus`, `applicationUpgradeStatus` | READ |
| `ServiceLogs` | `serviceLogs` | ADMIN |

Each file's leading comment gives its purpose and a variables example.

## Rules

- No schema or source reads. The operations are complete for this skill.
  Never add a field that holds a secret: news server passwords, tracker URLs,
  API keys.
- Never print, log or paste the API key. Never `cat` a whole response, log or
  bundle member.
- Branch on codes and enum values (`codes.md`), never on error or log
  sentences.
- No mutation: no pause, resume, retry, reprocess, remove, setting change or
  connection test. Describe the action; the operator does it, or approves it
  in another skill.
- Never delete files, downloads or Weaver data.
- State times in the operator's local time zone. API `EpochMs` values and
  bundle timestamps are UTC; convert them.

## Procedure

1. **Classify the symptom** into one class: job stuck / job Failed or
   Partial / server unreachable / disk full or slow / torrent not
   connecting. Ask one short question only if unclear. Ask for the job or
   torrent name when the problem is about one.

2. **Check facts and global holds:**

   ```sh
   G="$S"/../weaver-connect/scripts/gql.sh
   sh "$G" "$S"/operations/SystemFacts.graphql > facts.json
   jq '{v: .data.systemInfo.version, up: .data.systemInfo.uptimeSeconds,
     os: .data.systemInfo.operatingSystem, deploy: .data.systemInfo.deployment,
     update: .data.updateStatus.updateAvailable}' facts.json
   sh "$G" "$S"/operations/QueueState.graphql > state.json
   jq '.data.globalQueueState | {isPaused, pausedClasses, block: .downloadBlock.kind}' state.json
   ```

3. **Walk the decision tree.** Load `references/decision-trees.md`; follow
   only this class's section. Load `references/codes.md` when a value needs
   explaining.

4. **Page, don't list.** Queue and history: `pageSize` 25 first, filter by
   `states`, `status` or `search`. Torrents: `first` 25; the next page's
   `after` is `nextCursor`, null on the last page:

   ```sh
   after=$(jq -r '.data.torrents.nextCursor // empty' torrents.json)
   jq -n --arg a "$after" '{first: 25, after: $a}' > vars.json
   ```

5. **Search logs narrowly** (`ADMIN`) when a step asks:

   ```sh
   jq -n '{limit: 500}' > vars.json
   sh "$G" "$S"/operations/ServiceLogs.graphql vars.json > logs.json
   sh "$S"/scripts/log-search.sh logs.json --summary
   sh "$S"/scripts/log-search.sh logs.json '<regex>' WARN 40
   ```

   Weaver keeps about the last 1000 records in memory. `ServiceLogs` records
   are not sanitized: they can name jobs, servers and folders. Keep
   `logs.json` in the work directory; quote only the lines that matter.

   With an MCP client, prefer `search_service_logs` (sanitized):
   `{"pattern": "<regex>", "level": "WARN", "max_lines": 40}`. Lines are
   sanitized before matching: write the pattern against stand-in names, not
   real ones. Newest first; page with `before` set to `next_cursor`. A call
   can take many seconds.

6. **Bundle, when live data is not enough** (`ADMIN`; at least 10 s):

   ```sh
   sh "$S"/scripts/fetch-bundle.sh ./weaver-diag.tar.zst
   ```

   Writes only that file and prints its size. Exit 3: refused (401 key, 403
   not `ADMIN`). Then unpack and read as `references/bundle.md` says. The
   operator can also download it from **System info → Download
   diagnostics**. Names in the bundle are stand-ins.

   With an MCP client, first read one section with `read_diagnostics_section`
   (`{"section": "server_health"}`; sections in `references/bundle.md`).
   Fetch the whole bundle only when sections are not enough.

7. **Report**: likely cause, evidence (codes and counts, not raw dumps), fix.
   For each fix say who does it: an operator action in the web UI, a
   host-side action, or a change another skill can make after a yes.

## Stop and ask

- Before any change at all: this skill makes none. Name the change and
  stop.
- Before testing a server connection: it contacts the provider; it is a
  mutation; not in this skill.
- Evidence contradicts Weaver's own rules (a Partial job that delivered only
  archive volumes, split parts, PAR2/PAR3, `.rev` or SFV files; a Failed job
  that delivered a whole usable file; a crash; a code path contradicting its
  own status): stop diagnosing; report it as a possible Weaver defect with
  job id, codes and the bundle file name.
- Cause outside Weaver (provider account, retention, tracker, disk hardware,
  network, reverse proxy): say what to check and stop.

## Token budget

Typical run: about 10k to 25k tokens of tool output.

- facts and global holds: 0.5k
- 2 to 4 live queries, filtered: 2k to 6k
- one job's detail and 30 events: 2k to 4k
- log summary plus 1 to 3 searches of 40 lines: 3k to 8k
- one reference file: 2k to 4k

A single read over about 5k tokens: filter further with jq, `head` or a
narrower regex instead.
