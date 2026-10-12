# Weaver 0.15.0 release notes

Everything below is new since 0.14.7. This is a pre-release.

## Highlights

- Advanced networking. Each Usenet server and each RSS feed now has its own
  route: a ladder of legs that can be the system egress, a chosen interface
  or source address, a SOCKS5, HTTP or SSH proxy, a userspace WireGuard hop,
  a chain of proxies, or a pool. Legs carry weights and health; a leg that
  is unknown or down takes no traffic. The Networking screen shows the live
  flow from every consumer through its legs to the network.
- Download quotas per egress. Each egress, the System egress included, can
  carry a download quota with the same periods and reset settings a server
  quota has. A download that would go past it is refused at that egress,
  the leg goes down with "Quota reached" and its share moves to the other
  legs of the route; a job with no leg left parks as blocked by the egress
  quota until the window resets. Raising the limit lifts the block at once.
  The egress editor, the egress table, the Bandwidth panel, Monitoring and
  the jobs list show usage against the quota. This replaces the
  instance-wide ISP bandwidth cap.
- Scripts are wired as jobs. A script job is one script on one trigger
  (post-processing, a queue event, scan, schedule or feed) with its own
  inputs, categories, blocking flag and time limit. The script header is a
  preset that fills the form; after that the job is the truth. One Scripts
  table lists every job, grouped by trigger, with a Test dialog. A running
  script can call back into weaver with a run-scoped token. The Runs table
  opens each run in place and shows its full output in the log viewer.
- Secrets are their own records. A token or password is typed once under a
  name on the new Secrets screen, kept encrypted, and linked from any script
  job input that needs it; the same secret can serve many jobs, and a
  secret stays linked when the job is pointed at another script. A secret's
  value is never read back out. Deleting one is refused while a job links
  it. A job input can also hold its own encrypted secret without creating a
  shared named secret; these sealed values are never returned by the API.
- Schedules control download and post-processing pauses, watch-folder and
  RSS pauses, speed limits, hardware profiles, quota metering, history
  pruning, and server activation. Script jobs carry their own schedules.
- Archives that posters obfuscate, misnumber, repost or post short now
  finish where 0.14 failed them. Hex-named RAR volumes are grouped by the
  member chain their headers describe, obfuscated 7z parts are named by
  their PAR2 descriptions and stay in direct store, a volume the post never
  carried is rebuilt from the recovery set, a one-volume 7z repairs itself
  from the PAR3 it carries, and an archive found inside a directly stored
  7z is extracted instead of published as is. See "Archives and recovery".
- Archive passwords have a home. Settings → General keeps a list of
  passwords to try, encrypted, and a password file to read more from. An
  encrypted archive is tried with the job's own password first, then the
  NZB's password, then the filename convention, then the list and the file.
- The legacy web UI is gone. The Next UI is the only interface.

## Kill switch

Choosing "Kill switch" when creating a server or feed starts it with nothing
allowed out: no proxy leg and no direct fallback, so it cannot reach the
network until you give it a route under Networking and enable it. That is a
starting state, not a lasting guarantee. A later edit of the route can add a
direct leg or a direct fallback, and weaver will follow the route as saved.
If a server must never leave except through a VPN or proxy, keep its route
free of direct legs and fallbacks, and check it after any change. An
enforced never-direct setting is planned for a later release.

## Archives and recovery

- RAR volumes with no number in their names are placed by the member chain
  their headers describe. A RAR5 volume that states no number is volume 0.
  A set with no first volume waits for volumes instead of failing to open,
  and two obfuscated sets a job cannot tell apart are never merged.
- Split 7z parts that arrive under obfuscated names are held until the PAR2
  descriptions name them, then laid out as a direct set. An obfuscated
  single-file 7z is named by the PAR2 description whose hash matches its
  first 16 KiB. A 7z set whose end header cannot arrive is demoted as soon
  as that is known instead of holding every byte until the set settles.
- A one-volume 7z that carries PAR3 recovery packets after its end header is
  accepted, and a lost member article is repaired from that embedded set.
  PAR3 embedded in a split 7z recovers across the parts, and a verified
  PAR3 tail on the final part is accepted.
- A volume the NZB never listed but the recovery set describes is taken on
  by its set and rebuilt in place, the same as a volume lost after posting.
  A hole in a run of numbered parts asks the recovery set for the part
  instead of failing on it. A 7z set posted short of its part count waits
  for the described tail.
- The missing-part check waits for downloads to drain and for the restore
  from recovery volumes (`.rev`) to have its turn, and it no longer counts a
  part an eager extraction already consumed as missing. When an opened RAR
  set still cannot finish, the failure names each awaited volume number and
  the last volume seen before it.
- Parts posted under one another's names, or under duplicate names, are
  read by what PAR2 says they are. Every PAR2-identified archive input lands
  on its described name before cleanup, so a completed job no longer shows
  alias or duplicate names.
- Malformed, truncated or wrongly signed RAR headers go to PAR2 repair
  instead of ending the job. Encrypted-header RAR sets repair their
  decryptable edges independently. A restored direct image is read before
  it is trusted as verified.
- A job with several archive sets fails as a whole when one set cannot be
  recovered, and publishes nothing. A healthy direct set waits for a
  demoted neighbour's conventional extraction before it finalizes.
- A byte-identical repost never displaces a set that is already extracting.
  A late decode write after delivery has started is rejected, and extracted
  members alone get their permissions normalized.
- After a restart, a file the previous process started conventionally is
  never renamed onto a live direct set's volume, and PAR3 restored prefixes
  are published for verification. A split 7z part rebuilt and named after
  the topology was built is added to it instead of waiting forever.
- A part the recovery set wrote is never chased by direct unpack; the set
  is already whole on disk and is joined conventionally.
- The hardware profile decides how a 7z chase widens once complete runs
  pile up behind the decoding thread: Efficient adds a thread per waiting
  run and looks every 200 ms, Balanced every 100 ms, Performance widens to
  its ceiling on the first waiting run and looks every 50 ms. A profile put
  in force reaches the next chase; a running chase keeps what it started
  with.
- uuencoded posts whose last line declares more bytes than it carries no
  longer gain a zero byte at every article boundary.
- RAR3 recovery volumes are restored with a derived decode matrix applied
  to whole regions instead of one byte column at a time.

## What changed

- A script run's token may read queue items, history items and its own run.
  Other guarded queries and every mutation except `scriptRun` refuse it.
- Changing the scripts directory disables every script job until you
  explicitly enable the jobs you want to run from the new directory.
- Script statuses reported to Sonarr, Radarr and nzb360 through the NZBGet
  compatibility API follow NZBGet's own rule: only blocking post-processing
  runs count; a missing or failed script reports FAILURE, a skipped script
  NONE.
- Global scripts cascade: a download's category scripts run after the global
  ones instead of replacing them. A new setting, "global scripts run", chooses
  between always and only when the category has no scripts of its own. The
  upgrade picks the second when category lists existed.
- The completed folder is named after the release as it was posted, not the
  display name. The display name is unchanged. A collision still gets the
  job-id suffix.
- Commands that open the database prepare a pre-migration backup when
  automatic backups are enabled with a key.
  Automatic backups default to off, so the 0.14.x upgrade does not take one.
  Configure encrypted automatic backups and their
  schedule in settings. `--require-upgrade-backup` or
  `WEAVER_REQUIRE_UPGRADE_BACKUP=true` requires a successful backup before
  migration when a backup is attempted; these switches do not enable
  disabled backups. `--skip-upgrade-backup` explicitly skips preparation.
  With that skip flag, `--reset-automatic-backup-settings` disables
  automatic backups and discards their stored password for recovery.
  Upgrade backups have no fixed execution timeout. Every command that
  opens the database records the version that started, so a one-shot
  command that migrated does not cause a second pre-migration backup on
  the next start.
- Script output is kept by count, not by size. Each run keeps its last
  32 KB of output, stdout and stderr together in the order they arrived,
  compressed hard. A run that printed more shows a "Truncated" chip on the
  Runs table and in the Test dialog. Each download keeps its newest runs
  (default 32) plus its newest failed runs past that (default 8, new
  setting "Retain failed runs"); runs that belong to no download are kept
  the same way per scan, schedule or feed. Lowering either count deletes
  the older runs in the background as soon as you save. The "captured
  output per run", "compressed output budget" and "compressed output cap
  per run" settings are gone. Deleting a script job keeps its runs.
- The NZB analyzer caps its input while reading instead of after, and
  refuses a file above the 256 MiB NZB limit before reading it. The server
  refuses an inline NZB above that limit before decoding it.
- A RAR chase counts the articles after a short first one.
- On Windows, a route leg cannot bind to an interface by name; choose a
  source address instead, and Windows sends through the adapter that owns
  it. That adapter needs its own default gateway, as Windows picks the way
  out by the source address alone. Proxies, WireGuard, chains, pools and
  the kill switch work the same on every platform.
- An idle daemon does almost nothing. With no download moving, the job
  snapshot is published only when something changes, gauges are sampled
  once a second instead of ten times, finished jobs are shared between
  publishes instead of rebuilt, completion checks run only for jobs that
  could have changed, egress health is recomputed only when an interface
  or a quota moved, and quota usage is written only when bytes moved.
  Feeds, schedules and script jobs are no longer reloaded in full
  just to decide whether anything is due. Metrics history still writes a
  sample every ten seconds; script-output retention sweeps run hourly.
- Executables are refused by default. The unwanted extension list, which
  shipped empty, now starts as exe, bat, cmd, com, scr, msi, vbs, ps1, lnk
  and js. A job that holds one fails with the reason "unwanted extension
  '.exe' in '<file>'", at RAR open and before publication, so Scryer sees
  a failed grab and moves on instead of parking an import. The delivery
  rename never names a job after a file the list refuses, nor after an
  executable even when the list is empty. Clearing the list turns the
  check off.
- A "Set speed limit" schedule names the limit for every target at once:
  Global, each egress and each provider, each with its own value. A blank
  leaves that target as it is and 0 removes its limit. The lowest limit
  that applies to a download is the one in force; the two waits are no
  longer added together. A speed rule takes one time of day.
- Schedules can pause and resume RSS. While paused, no feed is polled.
- A script job that runs on a schedule carries its own run times, days and
  "also run at startup" on the job itself. The Schedules table no longer
  lists scripts.
- "Quota metering" schedules target every egress or one egress; one egress's
  rule wins over the rule for every egress. Active schedule tracks are
  evaluated again at startup. An egress quota's usage can be reset from
  its editor without clearing lifetime usage.
- NNTP rate limits allow 50 ms of burst credit instead of one second.
  The sustained limit is unchanged, including at low rates.
- Interface and source-address bindings constrain connections, but system
  DNS lookup traffic is not bound to that egress and may use the default
  route. Use a routed resolver when DNS must follow a tunnel.
- Containers drop `NET_RAW` unless `WEAVER_RETAIN_NET_RAW` explicitly opts
  into retaining it, including when `PUID` is 0.
- The settings search box searches every settings panel, not only the open
  one. Matches show under a heading per panel; the open panel stays
  editable and another panel's result opens that panel with the query
  kept. Every word of the query must match, in any order.
- A script job set to run at startup gets `WEAVER_API_URL` and
  `WEAVER_RUN_TOKEN` on that first run.
- A job waiting on its queue scripts is listed with the active downloads.
- A support report for a job that has just finished is built from its
  history entry.
- The route editor offers only targets the route can use: an RSS route
  lists only proxies and pools with DNS servers, a proxy already used
  elsewhere in the route is not offered again, and a disabled egress is
  not offered for a new leg. A route that repeats a proxy or uses a
  disabled egress cannot be saved.
- A proxy pool holds 2 to 16 members.
- A quota allowance that is not a number is treated as missing, so the
  quota cannot be switched on with it.
- A job whose route keeps its only connections on a leg past its egress
  quota parks as blocked by the quota instead of retrying. A retry that
  exhausts its egress parks as well.
- A `.go` file in the scripts directory runs as a script. It is built
  offline into a cache under the data directory, so it may import the
  standard library only. The `go` command sits beside the other
  interpreters in the scripts settings.
- A script cut off by a restart is recorded as interrupted, a status of its
  own in the API, the NZBGet compatibility API and the Runs table. After a
  restart, a failed job's unstarted scripts resume with the same outcome
  they would have been told; a started script is never rerun.
- A script that exits 95 leaves the previous script's status unchanged, so
  the next script does not see SUCCESS after a skip.
- A job that has queue-event script jobs but no post-processing job still
  waits for its queue events before it leaves the queue.
- A download submitted by URL passes that URL to its post-processing
  scripts. Script job events keep their own event kinds in the API.
- A script destination that does not exist, or a delivery worker that
  fails, is reported as an operational failure instead of a security
  refusal.
- A one-shot schedule, such as a history prune, missed while weaver was
  stopped runs once on the first tick after start. A schedule that names an
  unknown weekday is refused instead of firing on the wrong days. A speed
  limit set from the queue becomes the configured limit, so the queue
  state reports it. A watch-folder resume that fails no longer leaves
  scanning paused in memory.
- A leg reads down while every rung of its ladder is cooling, so a revoked
  route shows blocked instead of idle. A chain names the hop that could
  not be reached rather than the hop that answered, and the flow reports
  the first failing hop on each rung with its reason.
- A WireGuard hop stacked on another WireGuard hop is sized to carry it: the
  lower session runs at the MTU the carried tunnel needs, and a chain the
  cap cannot carry at full size reports a warning on its leg. Saving refuses
  a WireGuard proxy that reaches one egress by two different paths, since a
  WireGuard server accepts one connection per key.
- Leg throughput counts bytes once their rate wait is over, so a leg's
  sample no longer reads above the egress limit.
- A new feed is polled only once its route is installed. Items a feed
  returned before it had any rule are not marked seen, so the first accept
  rule still gets them.
- The folder picker can pick a file. The archive password file field uses
  it. GraphQL: `browseDirectories` takes `includeFiles` and returns `files`.
- A job archived to History with a password is marked as having had one,
  never with the secret. The password shows on the job page.
- Prometheus: script run counts, durations and last-run gauges, slot
  occupancy and queue-event backlog; schedule evaluations, actions, replays
  and per-rule fire state; route leg, egress, pool, proxy-hop, DNS and
  ladder families. The Grafana dashboard has a row for each.
- Backup restore accepts an older bundle whose table set differs from the
  current catalog. Newer-version backups are kept and failed ones pruned.

## Removed

- GraphQL: `setScriptLists`, `setScriptOptions`, `testScript`,
  `ScriptOption.value` and `Schedule.script` are gone. Script wiring is the
  `scriptInstances` query and the `createScriptInstance`,
  `updateScriptInstance`, `deleteScriptInstance` and `testScriptInstance`
  mutations. Scheduled script jobs store their run times on the job.
- GraphQL: `scriptOutputCeilingBytes`, `scriptOutputRingBytes` and
  `scriptOutputRunCapBytes` are gone from the post-processing settings and
  their input; `scriptOutputFailedRunsPerJob` is new.
- Implicit schedules derived from a script's `### TASK TIME:` header are
  gone. "Set up from header" saves the run times on the script job instead.
  After a host sleep or long stall, scheduled script jobs run the latest
  missed occurrence once.
- The NZBGet `<Script>:=no` opt-out and `<Script>:=yes` opt-in are not
  supported per download.
- The schedule actions "scan watch folder", "fetch RSS", "run script" and
  "use configured speed limit" are gone. GraphQL: `Schedule.instanceId`,
  `Schedule.runAtStartup` and `Schedule.feedId` are removed;
  `Schedule.speedLimits` and `Schedule.quotaEgressId` are new, with
  `ScheduleSpeedLimitInput` on the input; `Schedule.speedLimitBytes` stays,
  deprecated, mirroring the Global value. `ScriptInstance.schedule` and
  `ScriptInstanceScheduleInput` carry a job's run times.
- The ISP bandwidth cap is gone, replaced by the System egress's download
  quota. GraphQL: `GeneralSettings.ispBandwidthCap`,
  `GeneralSettingsInput.ispBandwidthCap`, the `IspBandwidthCapSettings`,
  `IspBandwidthCapSettingsInput`, `IspBandwidthCapPeriod` and `QuotaWeekday`
  types, `DownloadBlockKind.ISP_CAP` and `DownloadBlock.capEnabled`,
  `period` and `reservedBytes` are removed. New: `EgressInterface.downloadQuota`
  and `downloadQuotaUsage`, `EgressInterfaceInput.downloadQuota`, the
  `DownloadQuotaUsage` type, `DownloadBlockKind.EGRESS_QUOTA` and
  `DownloadBlock.egressId` and `egressName`. Prometheus: the
  `weaver_bandwidth_cap_*` gauges and the ISP cap alerts are replaced by
  `weaver_egress_download_quota_*{egress_id}`,
  `weaver_download_quota_block_window_end_seconds` and the
  `WeaverDownloadsGatedByEgressQuota` and `WeaverEgressQuotaNearlyExhausted`
  alerts.

## Upgrade notes

- An install whose unwanted extension list was empty gets the default list
  once, on this upgrade, because an empty list could not be told apart from
  the old default. A list cleared after the upgrade stays cleared, and a
  list that already named extensions is kept.
- The database moves from schema 50 to 59 in one step. The 0.14.7
  post-processing script lists, options and category assignments become
  post-processing script jobs. Other triggers in a script's header are
  not enabled by the upgrade. A pause or resume
  rule that used to fall back to the configured speed limit becomes a
  Global entry pinned to the limit saved at upgrade time. Each secret option a script had saved
  becomes one named secret, "<script> <option>", linked by every job of that
  script. The upgrade stops with a message if the
  saved scripts directory exists but cannot be read; make it readable and
  start again. A script named in a list but missing from the directory is
  carried over turned off, with a warning in the log. Schemas 57 and 58
  record which post-processing scripts of a job have started and how its
  download ended, so a pass interrupted by a restart resumes with the
  scripts it had not started and tells them the same outcome. A saved
  script concurrency below 32 is raised to 32, the new default, because
  the setting now bounds every script weaver runs, not only the ones a
  download waits for. Schema 59 keeps a script job's runs when the job is
  deleted.
- Category script lists that were empty, or had every script turned off, no
  longer keep scripts from running for that category. The log names each
  such category on upgrade.
- Routes default to the direct route through the system egress for every
  existing server and feed.
- An enabled ISP bandwidth cap becomes the System egress's download quota,
  and the bytes already counted in its current window carry over, so the
  quota resumes where the cap left off. A cap that was turned off is dropped.
