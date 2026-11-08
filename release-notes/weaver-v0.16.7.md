# Weaver 0.16.7 release notes

Everything below is new since 0.16.6.

## Highlights

- **Malformed posts no longer crash Weaver or cut files short.** Incorrect
  yEnc file sizes combined with missing articles could abort the daemon on
  every restart, or leave a truncated file marked Complete. Missing ranges
  are now checked safely, and complete files keep every byte their parts
  wrote. An inconsistent job fails without stopping other downloads.
- **Less memory for large queues.** Weaver keeps full runtime state for a
  window of the next jobs to run, plus jobs already doing work. Jobs farther
  down the queue stay in the database with lightweight queue rows until
  needed. Restarts load the same window, while the web interface and APIs
  can still list and inspect the entire queue.
- **Progress and time left reflect the download's actual work.** Recovery
  held back until needed no longer inflates remaining bytes. Clean posts
  reach 100% with zero remaining, and time-left estimates follow pauses and
  the speed limit currently in force, including scheduled unlimited windows.

## Upgrade note

Standard output and log files now default to one JSON record per event.
If a log collector expects text, set `WEAVER_LOG_FORMAT=text` or pass
`--log-format text`. The web log viewer continues to show readable text
lines with either setting.

## Downloads and recovery

- **Keep downloaded bytes across a pause.** Bytes held in memory while
  Weaver identifies a file are written to scratch before the pause
  checkpoint, so resuming or restarting a paused job does not fetch those
  articles again.
- **Pauses do not spend the extraction budget.** The extraction clock stops
  during individual, global, scheduled and speed-test pauses, including
  jobs restored while paused. A long pause no longer causes extraction to
  fail immediately after resume.
- **Handle misleading recovery names.** A file named like PAR2 or PAR3
  recovery is initially held out of the download's wanted bytes. If its
  first article shows ordinary data, it counts as data immediately instead
  of making progress jump backward near completion.
- **Count recovery and phase work correctly.** Live reports and history
  include the recovery bytes actually downloaded. Remaining recovery-file
  counts include PAR2 indexes and PAR3 volumes and exclude exhausted missing
  articles. Verification, repair and extraction have phase rows and stage
  timings, including after a restart.
- **Missing tails stay missing.** A missing run at or beyond an incorrect
  declared file size is still reported as loss. It can be repaired when
  recovery covers it, rather than disappearing from the job's accounting
  and allowing a cut-short file to complete.

## Extracted files and restart safety

- **Share directories without colliding with files.** Archives that name the
  same directory share it. A file that wants a directory's path gets a
  distinct name, with moves ordered so extraction does not fail over a
  file still occupying a parent path.
- **Keep payloads out of internal paths.** Archive members that use Weaver's
  staging or job-marker names are renamed, so completion does not omit or
  remove them as internal files.
- **Clean up changed archive layouts.** When repair retracts a directory,
  remove it as a directory after its contents leave. Archive fallback and
  renames also remove abandoned empty parent directories so later files can
  use those paths.
- **Restore only checkpointed files.** Restart cleanup descends into
  directories named by the checkpoint and removes uncommitted contents,
  comparing names consistently on filesystems that fold case.
- **Directories are not usable output.** Empty directory entries cannot
  make a job Complete or Partial. Recovery files remain excluded from usable
  output even when unpacked from a nested archive.

## Queue, history, and integrations

- **Arrival scripts run at arrival.** The job-added script runs once per
  add, including jobs outside the runtime window, rather than waiting for
  them to enter it.
- **Honor Arr history removal.** An Arr client's non-final NZBGet
  `HistoryDelete` now hides the finished job from Weaver's History page and
  GraphQL history listings as well as the NZBGet history view. Hidden rows
  retain duplicate detection and remain accessible or deletable by id;
  clearing history removes them too. Counts and pagination exclude hidden
  rows consistently.
- **More accurate NZBGet reporting.** Held-back recovery files appear paused
  in `listfiles`, and group remaining bytes distinguish that paused recovery
  from work still to fetch. `Health` and `CriticalHealth` use NZBGet's
  calculations, and active post-processing stages report elapsed time.
- **Consistent API and metric totals.** Queue items expose `remainingBytes`
  and `wantedBytes`; queue totals, time-left calculations and live job byte
  gauges use the same basis as download progress. Download status exposes
  `speedLimitInForce` and `speedLimitBySchedule`.
- **Archive fallback in job history.** An `ARCHIVE_DEMOTED` event records the
  member, archive type and reason when Weaver gives up treating an archive
  as an archive. It reports the archive once per reason, without labelling
  a successful handoff to another engine as a fallback.

## Web interface and diagnostics

- **Steadier speed and time-left displays.** Paused queues and held jobs
  show no download speed. Resume and speed-limit changes start fresh ETA
  samples, while changes to queue totals no longer reset the estimate.
  Scheduled limits, including unlimited windows, also set the speed button's
  label correctly.
- **Health colors account for repair capacity.** Job health uses graduated
  colors and turns red only below the job's critical health. The misleading
  saved-bandwidth block has been removed from the side rail.
- **Inspect structured logs.** Select a log line to see its JSON record.
  Following pauses while the record is open and resumes on close if it was
  following before. GraphQL adds `serviceLogs.records` and
  `serviceLogRecords`; the existing text fields remain available.
- **Safer support packages.** Name and directory replacement also matches
  JSON-escaped and quoted spellings in logs.
- **Quieter heartbeat logging.** Steady memory heartbeats appear at INFO
  once an hour, with intervening beats at DEBUG. The first beat and resident
  memory jumps still appear at INFO.
- **Speed tests behind TLS proxies.** `WEAVER_SPEED_TEST_EXTRA_ROOTS` accepts
  a PEM file of additional trusted root certificates for the speed-test
  client. Invalid or unreadable files fail the run with an explicit error.
  End-to-end speed-test fixtures now serve their own HTTPS bandwidth endpoint
  instead of depending on a public service's rate limit.
