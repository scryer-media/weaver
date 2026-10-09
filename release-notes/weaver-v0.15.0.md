# Weaver 0.15.0 release notes

Everything below is new since 0.14.7. This is a pre-release.

## Highlights

- Advanced networking. Each Usenet server and each RSS feed now has its own
  route: a ladder of legs that can be the system egress, a chosen interface
  or source address, a SOCKS5, HTTP or SSH proxy, a userspace WireGuard hop,
  a chain of proxies, or a pool. Legs carry weights and health; a leg that
  is unknown or down takes no traffic. Each egress can cap the traffic that
  leaves through it. The Networking screen shows the live flow from every
  consumer through its legs to the network.
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

## Upgrade notes

- The database moves from schema 50 to 55 in one step. Scripts, their
  options, category lists, feed scripts and script schedules are carried
  over as script jobs automatically. Each secret option a script had saved
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
