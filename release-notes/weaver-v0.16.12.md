# Weaver 0.16.12 release notes

Everything below is new since 0.16.11.

## Highlights

- **A stalled console can no longer freeze Weaver.** Console logging used to
  write to stdout on whatever thread logged. If nothing was reading stdout,
  for example a container log driver that had stopped draining, that thread
  blocked, and if it was a runtime worker or held a lock, downloads and the
  API stopped with it. Console output now goes through the same bounded
  delivery as the log file: one writer thread, and events dropped and
  counted when its queue is full.
- **Restoring a backup works on Windows again.** The WAL checkpoint task
  added in 0.16.11 kept its own database connection open after the database
  closed, so Windows refused to replace the file during a restore
  (`os error 32`). Closing the database now stops the task and waits for
  its connection to close.

## Logging

- **Console drops are counted.** Events dropped because stdout fell behind
  show up in `weaver_log_dropped_events_total{sink="stdout"}`, next to the
  file sink's count.
- **Stall warnings only for real stalls.** Dispatch stall warnings used to
  count recovery articles still waiting for a repair round, and jobs queued
  behind a slot held by a job that was verifying, repairing or extracting.
  A job doing post-processing could log "dispatch stall" warnings for its
  whole repair. The warnings now count only articles dispatch could
  actually hand out, from jobs that hold a slot. The log field is now named
  `dispatchable_queue_depth`, and the per-job recovery count is
  `parked_recovery_len`.

## Restarts and delivery

- **A resumed move leaves what it was told to leave.** When a consumed
  input can't be removed, for example a `.par2` a virus scanner holds open,
  0.16.11 left it in the working directory. If Weaver restarted part way
  through that move, the resumed move forgot this and carried the file
  into the completed folder. The list of files to leave is now saved with
  the move, so a resumed move leaves the same files.

## Regression coverage

- **The WAL checkpoint test counts runs before it wakes writers.** A write
  released by a checkpoint could look at the run count before the run that
  released it was counted.
