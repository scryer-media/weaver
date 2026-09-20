# Weaver 0.13.1 release notes

## Highlights

- **A job no longer sits at "Downloading" at 0 B/s forever when no server can
  fetch an article.** Several paths put an article back in the queue with
  every server excluded from fetching it. No connection would ever take it
  and nothing marked it as failed, so the job never finished. The most common
  case was a single-server setup where an article failed to decode. Every
  re-queue now checks whether another server remains. If none does, the
  article is booked as missing or damaged at once, and health checks and PAR2
  repair decide what happens to the file, as they do for any other bad
  article.
- **The providers rail says what a server is doing, not just how many
  connections it has.** A bare "1 / 100" looked like a broken server even
  when the pool knew exactly why: the provider was refusing further
  connections, cooling down after errors, or reading PAR2 repair data. The
  rail now leads with a state word and the reason for it, and draws the
  connected and active sockets as two layers on one bar.

## What changed

### Downloading

- **An article that fails to decode moves to another server only while one
  remains.** When none does, it is marked as failed to decode, and repair
  takes over.
- **Whole-file CRC recovery no longer re-queues a segment that no other
  server can fetch.** It leaves the file to the repair decision instead.
- **A recovery segment that runs out of decode retries no longer fails the
  whole job.** The recovery is abandoned and the segment is booked as damage.
- **A job that retention rules exclude from every server is booked missing.**
  Only the old download path used to do this; the scheduler now does it too.
- **An abandoned whole-file CRC recovery no longer lets the corrupt segment
  through.** The doubted segment stayed marked as received, so the file read
  as complete again and the known-bad bytes went on to extraction. The
  segment is now withdrawn first and booked as damage, and repair decides the
  file's fate.
- **Servers that need a newsgroup selected before an article fetch work
  again.** 0.13.0's download lanes stopped passing a job's newsgroups to the
  connection, so a server that answers a message-id fetch only after a
  `GROUP` command got no group, and a new connection had none to probe.

### Extraction and repair

- **A RAR job whose index article never arrived is extracted before it is
  finished.** When every volume landed and a recovery volume supplied the
  set, a shortcut for "only an undeliverable index is missing" skipped the
  complete volumes without extracting them, and the job reported success with
  nothing in its output.
- **Extracting an `.xz` file no longer waits for memory it is already
  holding.** The extraction reserved the decoder's full allowance and then the
  decoder asked for its own share on top, so the inner request could wait on
  memory only the outer one would release. The decoder's footprint is now
  reserved once.
- **A PAR3 recovery set embedded after an archive's own data is no longer
  reported as damaged.** The scan counted every byte before the embedded
  packets as a hole.
- **PAR3 repair accounts for all the memory its recovery index holds.** Copies
  kept for planning were not counted against the repair's memory budget.

### Web UI

- **Providers show an activity state.** A provider refusing connections and
  one paused after errors both read as cooling, with the reason underneath.
  Preparing says it is fetching repair data.
  The counts beneath read active, open and maximum.
- **"Preparing" appears only while a job is waiting on the connections.**
  The pool keeps idle connections open for minutes after a download
  finishes, and a paused queue holds them open too, so open-but-idle
  connections alone no longer read as preparing. Nor does a backup server
  idling while the primary carries the download.
- **A server taken out after a login failure or repeated errors says so,**
  and counts down to its retry the same way a cooling one does.
- **The open and busy meters update live** instead of following only the
  active count.
- **A held-back provider now shows in the attention strip on Downloads,** not
  only on the diagnostic screens.
- **Degraded and disabled providers are drawn in orange,** the error colour,
  because in both states the server itself is the problem. A provider-side
  holdoff stays amber.
- **The reason line wraps instead of being cut off** in a narrow column.
- **The Next UI's System Info page can install upgrades.** It shares its
  state and install action with the classic update card.
- **A release found after the page opened shows its install action** without
  reloading the page.

### Upgrades and restarts

- **A failed upgrade keeps the phase and download progress it reached,**
  rather than reporting the state it started from.
- **A first request to restart is no longer overridden by a later one.** A
  plain restart used to lose to an exit or relaunch requested afterwards.
- **The macOS menu bar app no longer treats an update relaunch as a crash.**
  If the server exited to relaunch the updated app while the supervisor was
  starting it or waiting for it to become ready, the exit was counted as an
  ordinary restart. It now reopens the app to finish the update.
- **A Windows upgrade that needs a reboot finishes after the reboot.** Weaver
  could not tell when Windows had last booted, so the upgrade stayed
  "running" forever and blocked every later one.

### Logging and diagnostics

- **A Windows crash dump no longer risks reading freed memory.** If the wait
  for the dump ended early, the dump thread went on reading its request from
  a stack frame that was already gone. The request now lives for as long as
  the dump thread does.
- **PAR3 phase times start when the phase does.** Each phase inherited the
  start time of the one before, so the current phase read as older than it
  was, and older the longer the job ran.

### History

- **Pages of history no longer load the whole table while jobs are
  running.** Excluding live jobs from the list now happens in the database
  query.

### Metrics and API

- `ServerHealth` gains `activity`, `activityUntilEpochMs`, `connectionsOpen`
  and `connectionsBusy`. These are additive; `connectionsActive` keeps its
  meaning. `activityUntilEpochMs` also carries a quarantined server's retry
  time.
- `ProviderConnections` gains additive open and busy counts, read the same
  way as `ServerHealth`'s.

### Dependencies

- `unrar-rs` 0.10.8: the ARM BLAKE2sp hash no longer relies on the alignment
  of its input, and x86-64 machines with AVX2 get a slightly faster RAR5
  decode loop, chosen at runtime.
- `sevenz-turbo` 0.24.0 and `lzma-turbo` 0.4.0, which move together:
  sevenz-turbo now takes its BCJ and delta filters from lzma-turbo.
