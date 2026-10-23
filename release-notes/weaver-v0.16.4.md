# Weaver 0.16.4 release notes

## Highlights

- **Bulk actions over large selections.** Cancel, pause, resume, and edit jobs
  in bounded groups instead of rejecting the whole selection as too complex.
  Every group is accounted for: successful jobs leave the selection, refused
  jobs stay selected, and the result reports partial success.
- **Reliable cancellation.** A cancelled job leaves the queue only after its
  history record commits. If that write fails, the job stays live and its
  repair and completion work can resume instead of disappearing until a
  restart. GraphQL adds `cancelJobs` with a result for each job.
- **Restore archive write ordering.** Revert the delayed disk-write behavior
  introduced in 0.16.3 so archive readers see decoded bytes only after their
  write completes. This restores reliable direct unpack; slow disk writes can
  again briefly delay pipeline and queue updates.

## Downloads and post-processing

- **Recover unexplained download stalls.** After two minutes with queued work
  but no active connections and no known reason to wait, reset the affected
  jobs' dispatch holds and try again. If a job makes no byte progress for ten
  minutes after that reset, treat its remaining queued articles as missing
  and let normal repair and completion checks decide the outcome. Other jobs'
  retry deadlines and legitimate waits remain protected.
- **Accurate download status.** A job that has started remains `Downloading`
  between retries or connection assignments. NZBGet-compatible clients no
  longer see it revert to queued or incorrectly report server standby.
- **Keep server rotation flexible.** A preference to retry an article on
  another server no longer blocks the original server when no alternative
  can serve it.
- **Wait for queue scripts before moving files.** Completion waits for all
  outstanding queue-event scripts, including `FILE_DOWNLOADED` runs when no
  `NZB_DOWNLOADED` subscriber is configured.
- **Fail lost handback writes.** A failed disk write while falling back from
  direct extraction fails the job instead of leaving an unreported hole.

## History and operations

- **Responsive bulk cleanup.** History deletion and cancellation use batches,
  and bandwidth usage saves run through the database writer queue. History
  delete batches are limited to 50 jobs to contain slow scripts and errors.
- **Keep history views synchronized.** Announce every deleted history row,
  even when another item is refused or filesystem cleanup fails afterward.
  Cleanup failures still appear in the operation's result.
- **Stall recovery diagnostics.** Count resets and releases in
  `weaver_pipeline_download_dispatch_stall_recoveries_total`. Set
  `WEAVER_DOWNLOAD_STALL_RECOVERY=0`, `false`, or `off` to disable recovery.
- **Build and test improvements.** Optimize additional backup cryptography,
  SQLite, and compression crates in test builds. Intel macOS packages use
  ThinLTO.
