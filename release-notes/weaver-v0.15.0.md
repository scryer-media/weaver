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
  it.
- Schedules can run a script job, set a speed limit, prune history or turn
  a server on or off, from one Schedules table.
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

## What changed

- A script run's token may read whatever the API answers and may change
  nothing except through `scriptRun`. Every other mutation refuses it.
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
- Every command that opens the database takes the pre-migration backup.
- Script output is kept by count, not by size. Each run keeps its last
  32 KB of output, stdout and stderr together in the order they arrived,
  compressed hard. A run that printed more shows a "Truncated" chip on the
  Runs table and in the Test dialog. Each download keeps its newest runs
  (default 32) plus its newest failed runs past that (default 8, new
  setting "Retain failed runs"); runs that belong to no download are kept
  the same way per scan, schedule or feed. Lowering either count deletes
  the older runs in the background as soon as you save. The "captured
  output per run", "compressed output budget" and "compressed output cap
  per run" settings are gone.
- The NZB analyzer caps its input while reading instead of after.
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
  Feeds, schedules and script instances are no longer reloaded in full
  just to decide whether anything is due.
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
  rule wins over the rule for every egress.
- The settings search box searches every settings panel, not only the open
  one. Matches show under a heading per panel; the open panel stays
  editable and another panel's result opens that panel with the query
  kept. Every word of the query must match, in any order.

## Removed

- GraphQL: `setScriptLists`, `setScriptOptions`, `testScript`,
  `ScriptOption.value` and `Schedule.script` are gone. Script wiring is the
  `scriptInstances` query and the `createScriptInstance`,
  `updateScriptInstance`, `deleteScriptInstance` and `testScriptInstance`
  mutations. Schedules that ran a script now name a script job.
- GraphQL: `scriptOutputCeilingBytes`, `scriptOutputRingBytes` and
  `scriptOutputRunCapBytes` are gone from the post-processing settings and
  their input; `scriptOutputFailedRunsPerJob` is new.
- Implicit schedules derived from a script's `### TASK TIME:` header are
  gone. "Set up from header" creates real schedule rows instead.
- The NZBGet `<Script>:=no` per-download opt-out is not supported.
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
- The database moves from schema 50 to 57 in one step. Scripts, their
  options, category lists, feed scripts and script schedules are carried
  over as script jobs automatically. A schedule that ran a script becomes
  that job's run times; its days are joined with the job's. Schedules for
  the removed actions are dropped, each named in the log. A pause or resume
  rule that used to fall back to the configured speed limit becomes a
  Global entry pinned to the limit saved at upgrade time. Each secret option a script had saved
  becomes one named secret, "<script> <option>", linked by every job of that
  script. The upgrade stops with a message if the
  saved scripts directory exists but cannot be read; make it readable and
  start again. A script named in a list but missing from the directory is
  carried over turned off, with a warning in the log.
- Category script lists that were empty, or had every script turned off, no
  longer keep scripts from running for that category. The log names each
  such category on upgrade.
- Routes default to the direct route through the system egress for every
  existing server and feed.
- An enabled ISP bandwidth cap becomes the System egress's download quota,
  and the bytes already counted in its current window carry over, so the
  quota resumes where the cap left off. A cap that was turned off is dropped.
