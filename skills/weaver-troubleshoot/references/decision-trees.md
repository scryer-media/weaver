# Decision trees

Follow only the symptom's section. `S` = this skill's directory,
`O="$S/operations"`, `G="$S/../weaver-connect/scripts/gql.sh"`. Every
response to a file; read with jq. Value meanings: `codes.md`. Ids below
(`42`) are examples; take real ids from a list query.

Shared first step for every tree:

```sh
sh "$G" "$O"/QueueState.graphql > state.json
jq '.data.globalQueueState | {isPaused, pausedClasses, block: .downloadBlock.kind,
  hold: .downloadBlock.scheduleHoldReason, slow: [.downloadBlock.diskVolumes[] | select(.slow) | .label]}' state.json
```

`block` other than `NONE`, or a class in `pausedClasses`, explains a stop on
its own. Report it and stop.

## Job stuck

1. Find the job:

   ```sh
   jq -n '{input: {pageIndex: 0, pageSize: 25, search: "<words from the name>"}}' > vars.json
   sh "$G" "$O"/QueuePage.graphql vars.json > queue.json
   jq -r '.data.queuePage.items[] | [.id, .state, .downloadState, .postState,
     (.waitReason // "-"), (.downloadWaitReason // "-"), (.attention.code // "-"), .name[0:60]] | @tsv' queue.json
   ```

2. Branch on what holds it:
   - `state` `PAUSED` → held by the operator. Report.
   - `downloadWaitReason`:
     - `propagation_delay`, `nzb_slot` → normal wait. Report when it
       resumes (`downloadRetryAtEpochMs` when set).
     - `disk_space`, `disk_too_small`, or `waitReason` `DISK_*` → go to
       **Disk full**.
     - `login_refused:...` or `NNTP authentication blocked` → go to
       **Server unreachable**, for the servers named.
     - `Restoring saved job` → restart recovery; wait, read again once.
   - `waitReason` `*_CAPACITY` → another job holds that step. Find it in
     `stateCounts` (`REPAIRING`, `EXTRACTING`, `POST_PROCESSING`).
   - `attention.code` `WAITING_FOR_VOLUMES` or `AWAITING_REPAIR` → unpack
     waits on volumes or repair. Read **Job detail** below.
   - `diskWrite.slow` or `diskWrite.draining` true → the disk is the limit.
     Go to **Disk full**, step 3.
   - `DOWNLOADING` with no progress over two reads a minute apart → read
     **Server unreachable**.
3. **Job detail**:

   ```sh
   jq -n --argjson id 42 '{jobId: $id}' > vars.json
   sh "$G" "$O"/JobDetail.graphql vars.json > job.json
   jq '.data.jobDetailSnapshot | {events: .jobEventCount, layout: .downloadSummary.layout,
     post: .downloadSummary.post, recovery: [.downloadSummary.recovery[] | {kind, outcome}],
     givenUp: [.downloadSummary.nodes[] | select(.givenUp) | {kind, reason}]}' job.json
   ```

4. Last events:

   ```sh
   jq -n --argjson id 42 '{jobId: $id, limit: 30}' > vars.json
   sh "$G" "$O"/JobEvents.graphql vars.json > events.json
   jq -r '.data.jobEvents[] | "\(.kind) \(.message[0:160])"' events.json
   ```

## Job Failed vs Partial

1. Read it from history:

   ```sh
   jq -n '{input: {pageIndex: 0, pageSize: 25, status: "FAILURE"}}' > vars.json
   sh "$G" "$O"/HistoryPage.graphql vars.json > history.json
   jq -r '.data.historyPage.items[] | [.id, (.outcome // "-"), .health, .failedBytes,
     .failedForMemory, (.attention.code // "-"), .name[0:60]] | @tsv' history.json
   ```

2. Decide on `outcome`, never `state` (Partial shows `state` `FAILED`):
   - `PARTIAL` → at least one usable file was delivered; something else is
     missing. Name what with step 3.
   - `FAILED` with `attention.code` `CANCELLED` → cancelled. Report.
   - `FAILED` and `failedForMemory` true → a decode window was over the
     memory profile's ceiling. The operator may reprocess it without the
     memory limit (a `CONTROL` action; not in this skill).
   - `FAILED` otherwise → step 3.
3. `JobDetail` and `JobEvents` (as above), then:
   - `post.missingArticles` > 0 and recovery `outcome` `impossible` or no
     recovery → articles gone from every server, too few recovery blocks.
     Cause outside Weaver (retention, takedown). Check **Server
     unreachable** only if `ServerHealth` shows a server down.
   - A node `givenUp` with `passwordNeeded` → needs a password.
   - `givenUp` with `reason` `encrypted` or `compressed` → Weaver cannot read
     that archive.
   - `EXTRACTION_FAILED` / `EXTRACTION_MEMBER_FAILED` events → unpack
     failed; quote the event message.
   - `SCRIPT_ERROR` events → a post-processing script failed.
4. A job marked Partial that delivered only volumes, split parts, PAR2/PAR3,
   `.rev` or SFV files, or a Failed job that delivered a whole usable file,
   contradicts Weaver's rule. Stop and report it as a possible defect with
   the job id, `outcome` and the evidence.

## Server unreachable

1. Live state:

   ```sh
   sh "$G" "$O"/ServerHealth.graphql > servers.json
   jq -r '.data.serverHealth[] | [.serverId, .label, .tier, .state, .activity, .down,
     "\(.connectionsOpen)/\(.connectionsMax)", .consecutiveFailures, .prematureDeaths,
     .articlesNotFound] | @tsv' servers.json
   ```

2. Branch:
   - `activity` `over_limit` or `capacityPenaltyUntilEpochMs` set → the
     provider limits connections. Not down. Suggest the operator lowers
     that server's connection count.
   - `state` `disabled` → switched off. Report.
   - `down` true or `cooling_down` → faulting. Step 3.
   - High `consecutiveFailures` with zero `successCount` → it never
     connected. Step 3.
   - `prematureDeaths` rising → connections die young (network, proxy, or
     too many connections).
   - Healthy but `articlesNotFound` high → retention or takedown, not a
     connection fault.
3. Logs (needs `ADMIN`):

   ```sh
   jq -n '{limit: 500}' > vars.json
   sh "$G" "$O"/ServiceLogs.graphql vars.json > logs.json
   sh "$S"/scripts/log-search.sh logs.json --summary
   sh "$S"/scripts/log-search.sh logs.json 'login|auth|refused|tls|timed out|connect' WARN 40
   ```

   Login refusals → the operator checks the account. TLS or connect errors →
   host, port, TLS setting, network route. Never test a connection without
   a yes: it contacts the provider.

## Disk full

1. Space:

   ```sh
   sh "$G" "$O"/DiskState.graphql > disk.json
   jq -r '.data.diskUsage[] | "\(.label) free=\(.freeBytes) total=\(.totalBytes)"' disk.json
   jq '.data.systemInfo.configuredStorage[] | select(.error) | {labels, error}' disk.json
   ```

2. For the waiting job, compare `diskNeedBytes` with `diskFreeBytes`
   (`QueuePage`):
   - `disk_too_small` → that disk can never hold the job. The operator
     frees space or points the folder at a larger disk. Weaver never fails
     the job for it.
   - `disk_space` → waits for room; resumes by itself when room is made.
   - A `configuredStorage` `error` → the folder could not be read
     (unmounted drive, permissions).
   - Torrent `stateReason` `DISK_FULL` → a write found no room; it keeps
     seeding.
3. Slow disk: `jq '.data.systemInfo.volumes[]' disk.json`. `storageKind`
   `platter` or `network` with `bufferedBytes` near `bufferLimitBytes` → the
   disk limits speed. Report; no setting fixes the hardware.

Never delete files to make room. Describe what could go; the operator
decides.

## Torrent not connecting

1. Client and routes:

   ```sh
   sh "$G" "$O"/TorrentNetwork.graphql > tnet.json
   jq '.data.torrentPresence' tnet.json
   jq -r '.data.torrentRoutes[] | [.key, .open, (.reason // "-"), (.exposure.inbound | tostring),
     (.detail // "")[0:120]] | @tsv' tnet.json
   ```

   - `available` false → the server was started with `WEAVER_TORRENTS`
     turned off. Report; the operator decides.
   - `enabled` false → torrents are off in settings.
   - A route `open` false → its `reason` (`codes.md`) is the cause.
2. The torrent:

   ```sh
   jq -n '{first: 25}' > vars.json
   sh "$G" "$O"/TorrentsPage.graphql vars.json > torrents.json
   jq -r '.data.torrents.items[] | [.id, .state, (.stateReason // "-"), (.routeReason // "-"), .private,
     .connectedPeers, (.swarmSeeds // "-"), .name[0:50]] | @tsv' torrents.json
   jq -n --argjson id 42 '{id: $id}' > vars.json
   sh "$G" "$O"/TorrentDetail.graphql vars.json > torrent.json
   jq -r '.data.torrent.trackers[] | [.tier, .status, .fails, (.message // "")[0:120]] | @tsv' torrent.json
   ```

3. Branch:
   - `routeReason` set → the route is the cause (step 1).
   - `NEEDS_CERTIFICATE` → an SSL torrent with no certificate; the operator
     adds one in its tracker rule.
   - `FILES_MISSING` → its files are not where they were (drive not
     mounted).
   - `FETCHING_METADATA` with `stateReason` `FETCH_WAITING` → no peer named that
     may serve the metadata.
   - Every tracker `NOT_WORKING`, `UNREACHABLE` or `TRACKER_ERROR` → quote
     the tracker `message`. A private torrent has no other peer source.
   - Trackers `WORKING` but `swarmSeeds` 0 → no seeder exists. Not a Weaver
     fault.
   - Trackers `WORKING`, seeders exist, `connectedPeers` 0, route `exposure.inbound`
     false → peers cannot connect in; report the route.
