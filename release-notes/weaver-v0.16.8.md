# Weaver 0.16.8 release notes

Everything below is new since 0.16.7.

## Highlights

- **Restarts finish delivery instead of downloading again.** Weaver records
  the final destination before moving files. A restart during delivery
  resumes that move, replaces interrupted copies from the working directory,
  and keeps the job's original Complete or Partial result.
- **A crashing job stays paused after restart.** If a panic can be traced
  to a job, Weaver records that job before the daemon exits. On the next
  start it leaves the job paused with the crash reason, keeping other jobs
  available. The job stays parked across further restarts until resumed.
- **Less overhead during downloads and extraction.** Queue processing no
  longer waits for database work on the pipeline thread. Encrypted RAR and
  7z data is decrypted in larger runs, ZIP extraction emits larger pieces,
  and eligible RAR members spanning volumes can decode in parallel.

## Delivery and restart safety

- **Save the move before starting it.** The Moving state and reserved
  destination must commit before files leave the working directory. A
  database failure stops delivery with an operational error.
- **Resume the work already reached.** Jobs that reached final delivery or
  post-processing resume there instead of restoring a fresh download.
  A completed move is recognized when the working directory is gone and
  the recorded destination exists.
- **Replace incomplete copies.** During a resumed move, files still in the
  working directory replace their interrupted destination copies. Partly
  copied directories are merged entry by entry; files already moved whole
  are retained.
- **Keep the delivery verdict.** Partial results, shortfall reasons,
  recovery verification and extraction results survive a restart during
  the final move and carry through to history.
- **Move files on more filesystems.** When a filesystem does not support
  exclusive rename, Weaver falls back to linking and unlinking, then to a
  destination check and rename where links are unavailable. A retry also
  finishes a move interrupted between linking and unlinking. This addresses
  rename failures on mounts such as NFS, SMB, FUSE and shared VM storage.
- **Keep restored article placement checks.** A restored job rebuilds the
  placement records for intact articles, so newly arriving parts are checked
  against the bytes already placed before the restart.

## Downloads and recovery

- **Handle exchanged yEnc begin and end fields.** When the decoded body
  proves the two offsets were swapped exactly, Weaver places the part at
  the corrected offset and updates its checksum record. Other invalid end
  values continue to use the declared begin offset.
- **Count out-of-order PAR2 recovery fully.** Recovery data arriving beyond
  the in-memory hold limit is read back from disk when the parser reaches
  it. Available recovery blocks no longer disappear from the count just
  because their articles arrived far ahead of earlier data.
- **Retry RAR4 passwords after repair.** A correct password can appear
  wrong when posted encrypted data is corrupt. After repair rewrites that
  data, Weaver tries the password candidates again instead of remaining
  stuck asking for a password it already has.
- **Do not parse missing RAR data as headers.** Missing ranges remain
  unread even if stale bytes occupy their scratch space. Damaged ranges
  that actually arrived can still be inspected once the volume closes.
- **Recognize obfuscated SFV listings.** A small file with valid SFV entries
  can be identified by its contents when the post has no named listing or
  recovery files, allowing checksum verification despite an unhelpful name.

## Performance and diagnostics

- **Keep queue processing responsive during database work.** Slot changes,
  restore batches and completion-script admission finish asynchronously.
  Writes remain ordered per job, and work that depends on a committed
  write waits for its result without holding up the pipeline loop.
- **Avoid scanning long queues for each failed batch.** Download results
  find their job through the queue index rather than walking queue rows.
- **Judge server depth from supplied work.** Adaptive download depth no
  longer treats a lane waiting for work as evidence of a slow server.
- **Reduce encrypted extraction bookkeeping.** Consecutive cipher blocks
  are staged together, and eligible aligned arrivals go directly to a
  bulk cipher call. Existing checkpoint data remains readable.
- **Batch ZIP output and parallel RAR work.** ZIP output is gathered into
  256 KiB pieces. RAR members can share decoder threads once all volumes
  they span are available and the memory budget permits it.
- **Tune file-backed SQLite caching.** File databases use a 64 MiB page
  cache, checkpoint the WAL at about 16 MiB, and limit the retained journal
  to 64 MiB. In-memory databases keep their existing cache setting.
- **Report malformed NZB segments once per document.** The warning includes
  a count and the first malformed segment, instead of flooding the log
  with one warning per segment.
- **Identify the job behind a panic.** Panic logs and saved crash markers
  include the job when known, together with the message, location, build
  and time, making repeated failures easier to diagnose.

## Regression coverage

The ordinary engine suite adds cases for ZIP64, uuencoding, SFV completion,
RAR encryption, missing PAR2 indexes, truncated compressed streams and
restarts during delivery. Additional fixtures exercise misleading yEnc
headers. The end-to-end harness also reuses resolved images during restarts
and records where release-gate time is spent.
