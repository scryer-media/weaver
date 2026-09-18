# Weaver 0.12.5 release notes

## Highlights

- **Weaver can upgrade itself.** It checks for a new release, downloads it,
  verifies its signature, and replaces itself: a portable install in place,
  `Weaver.app` in place, and Windows through a helper that takes over once the
  server exits. A journal kept on disk finishes or undoes an upgrade that was
  interrupted, the next time Weaver starts. Releases now ship signed manifests,
  per-architecture app-bundle archives and a Windows portable archive.
- **One scheduler decides what every connection downloads.** Each server asks
  a single scheduler for its next articles. The scheduler serves the first
  runnable job in priority and submission order, and moves past it only when
  that job has nothing the asking server can fetch. A job finishes instead of
  creeping along beside every other queued job. A connection sits empty only
  for a reason that applies to the whole link: a pause, a byte-pressure or cap
  limit, the rate limiter, or a connection pool being handed over.
- **Memory returns to its idle level after a download.** Articles are allocated
  on one thread and freed on another, and the allocator kept that memory
  resident, so Weaver's footprint grew with its peak article rate and climbed
  from job to job. The allocator is now configured at startup to give it back.
  Throughput is unchanged. Peak memory at the 90th percentile is 36–63% lower,
  and a single 1 GB job settles at about 148 MiB after it completes instead of
  about 299 MiB. Any of the three settings can still be overridden with its
  `MIMALLOC_*` environment variable.

## What changed

### Downloading

- **The owned download lanes are the only download engine.** STARTTLS servers
  now upgrade the lane's connection in band, so they get an owned lane like
  every other server.
- **Downloads use every connection they are allowed.** A dispatch pass counted
  each lease twice, so it stopped at roughly half capacity, at a point that
  depended on thread scheduling. It now counts each server's seats once per
  pass.
- **A dispatch pass still sends work when the health lock is busy.** The pass
  reuses the server order it last ranked. A pass that has not ranked yet asks
  again, briefly, before falling back to idle connections.
- **Backfill servers fetch only what the fill servers have given up on.** A
  dispatch goes to a server only while that server can take the work: it has
  an idle cached connection or a free permit.
- **Idle lanes survive the end of a job.** A lane waiting between jobs used to
  be torn down and redialled when the job it last served was removed. Only
  lanes with articles still outstanding are closed with the job.
- **Existence checks are answered on busy connections.** When every
  connection to a server is busy downloading, a check for whether an article
  exists is answered on one of those connections after its outstanding
  articles have been read. It no longer needs a new connection or a free
  permit.
- **Other callers no longer queue behind busy download lanes.** When every
  connection to a server is busy downloading, a request from outside the
  download lanes fails at once. It no longer waits out its whole timeout
  first.

### Servers

- **Adding a high-latency server no longer turns off pipelining.** The server
  test made a separate connection to inspect the certificate, sent three
  latency pings, then made a second connection to check cipher order. At
  500 ms per round trip that came to about 22 round trips, which is past the
  10-second limit, so the server was set up without pipelining. The test now
  gets all of it from the one real connection:
  - the hostname is checked during that connection's TLS handshake;
  - latency is read from the CAPABILITIES round trip that setup already makes;
  - cipher-order preference is read from the cipher suite the server chose.

### Health checks and repair

- **One failed batch no longer throws away a whole health check.** A batch
  could fail to get an answer when no connection freed up in time, and that
  used to end the whole check. A check samples three to eleven batches, so it
  often reported nothing. A failed batch now only reduces what the check
  covers. The verdict is read from the batches that did answer, and the log
  line says how many segments it rests on.
- **PAR2 recovery files are looked for several at a time.** Up to four
  recovery volumes are probed together. After three volumes of one posting
  come back missing, the rest of that posting is skipped, unless any of it has
  been confirmed to exist.
- **Large uuencoded posts download.** Weaver could hold only 16 out-of-order
  parts of a uuencoded file while it waited for the next one in sequence, so
  every part beyond that was fetched again on each pass and was eventually
  counted as a permanent failure. A 307-part uuencoded post failed this way
  every time. Weaver can now hold every part of the file while it waits, and
  those extra fetches are no longer mistaken for a stuck loop.

### History

- **A job still in the queue no longer appears in history.** The history
  list, the history page, the list returned after deleting from history, and
  the history count all leave out jobs the scheduler still owns. The page's
  totals and group counts agree with its items.

### Logging and diagnostics

- **Long jobs no longer flood the log.**
  - A job being deferred by health checks logs once at info and reports the
    total when the deferral ends.
  - Download warnings are limited per job instead of per worker, and the next
    line that gets through says how many were suppressed.
  - A job waiting for local connection capacity has its own message and no
    longer uses the "check server health and credentials" warning.
  - Jobs that are paused, complete or failed are noted at debug level.
  - Duplicate NZB segments are counted per file and reported in one line.
- **A heartbeat line every minute.** It records uptime and the process's
  current and peak memory, so a gap in the log shows when the process
  stopped. The peak is also logged at shutdown.
- **Windows crashes leave evidence.**
  - An unhandled exception writes a minidump next to the log files and logs
    the exception code and where the dump went. The dump is written from a
    separate thread, so it works after a stack overflow too.
  - The desktop app copies the server's error output to `weaver.stderr.log`
    and records the exit code when the server exits. The diagnostics bundle
    includes that file.

### Upgrades

- **Upgrade verification data is refreshed in the background at startup.**
  An upgrade then checks the release against current trust data, not a copy
  built into the binary. Neither startup nor an upgrade waits for the refresh.
- **Upgrades work under the desktop app.** Every upgrade started from the
  desktop app used to report that it was not allowed.
- **An upgrade interrupted before anything was replaced is marked failed on
  the next start.** Before, it stayed "running" and blocked the next attempt.
- **A second upgrade cannot start while an earlier one is unfinished.** This
  includes one that is waiting for a reboot.
- **A failed macOS upgrade removes the staging directory it created.**
- **`WEAVER_UPDATE_CHECK=0` also stops the background trust-data refresh.**

### Metrics and API

- New Prometheus counters for the scheduler:
  `weaver_pipeline_download_scheduler_handouts_total`, and
  `weaver_pipeline_download_scheduler_idle_with_servable_total`, which counts
  times a connection sat idle while work was available and should stay at
  zero.
- **Breaking:** the hot-dispatch and spillover metrics are gone, along with
  the dispatch model they described. This removes the
  `weaver_pipeline_hot_dispatch_*` Prometheus series, the matching `hotDispatch*`
  GraphQL and API fields, and the hot and spillover lane-park counters.

### Dependencies

- New: `application-updater` 0.1.0 and `artifact-trust` 0.1.2.
- `libmimalloc-sys` is pinned to an exact version, because the allocator
  settings are addressed by that version's option numbers.
