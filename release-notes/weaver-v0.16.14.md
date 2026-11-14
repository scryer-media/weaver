# Weaver 0.16.14 release notes

Everything below is new since 0.16.13.

## Highlights

- **Large queues cost far less memory and time.** A queued article now
  costs one 32-byte entry instead of a full work item with its own message
  id and newsgroup list. The ids are read from the job's stored article
  list when the article is sent. A restart builds waiting jobs from a small
  per-file outline stored when the job is added, so it parses no NZB before
  the web interface is up.
- **The yEnc decoder and encoder are faster on every CPU.** On real
  article bodies, every decode tier, from plain scalar code to AVX-512
  VBMI2 and NEON, is faster than rapidyenc and faster than 0.16.13 on
  Zen 4, Sapphire Rapids and Apple silicon. The encoder gains kernels for
  SSE2, SSSE3, AVX, AVX2, AVX-512 VBMI2 and NEON, chosen once for the CPU,
  each at least as fast as rapidyenc's.
- **Memory goes back to the system when Weaver is idle.** When the
  pipeline runs out of work, or settles with jobs waiting on a server
  that is not set up, the allocator returns its freed pages. Download
  connections waiting for work free their read buffers. On macOS, SQLite
  now allocates through the same allocator: on a 300-job queue at idle,
  its heap holds about 2 MiB instead of 58 to 65 MiB.
- **History search finds what you mean.** The search matches the full
  release name, the display title and the short name, ignoring case,
  accents and separators. `ecole` finds `École.2024`, and `show s01e01`
  finds `Show.S01E01.1080p`. On Postgres it also matches close spellings
  when the `pg_trgm` extension can be installed. Sorting by name uses the
  full release name too.
- **NZBs that SABnzbd and NZBGet accept are accepted.** A file with no
  poster, subject or date is no longer refused. Text that is not UTF-8 is
  read as Latin-1, and an upload is recognised by its content, including
  bzip2-compressed and misnamed files. The 2,000,000-article and 128 MB
  limits are unchanged.

## Downloads

- **A download connection reads 128 KiB per socket read.** Each TLS
  connection now holds 384 KiB of read buffers and each plain one 256 KiB,
  where both held 768 KiB. Reads from a real provider at 8 and 20
  connections had a median of 6 to 16 KiB, and on loopback the smaller
  read cost no extra CPU. Every host with 2 GiB or more keeps these sizes
  up to 100 connections, and with more connections the buffers shrink to
  stay inside the budget.
- **A plain connection reads straight into its read buffer,** with no
  staging buffer and copy in between.
- **A TLS connection's read buffer keeps its size.** It could double from
  512 KiB to 1 MiB under some reads.
- **A set's volumes are fetched in order.** Once a volume's number in its
  set is known, the queue ranks it by that number, so a shuffled post is
  fetched as if it were posted in order and holds back no more bytes.
- **Priority edits apply to the jobs next up at once.** The jobs read
  ahead for the next slots now see a priority edit before the pipeline
  applies it, as admission does. A job resumed while it waits for its slot
  is next up again straight away.

## Archives and repair

- **Non-solid 7z archives decode their folders side by side,** on as many
  workers as the job's thread and memory budget allows.
- **PAR2 repair spreads across every granted repair thread** on every
  hardware profile, as PAR3 already did.
- **A PAR2 volume named by its block range is read correctly.** ParPar
  names volumes `vol<start>-<end>`. Weaver read those as naming no blocks,
  so a damaged post could not choose volumes by name, and the NZB analyzer
  added the end numbers up as counts.
- **PAR2 and SFV on the same files no longer undo each other.** A post
  with both could hand files back and forth to the repair worker forever,
  writing a checkpoint each time. An SFV now stands aside while PAR2 or
  PAR3 describes the same files.
- **sevenz-turbo 0.30.1 and bzip2-turbo 0.2.1** bring bzip2 decode fixes.

## Delivery

- **A job stages on the drive its category delivers to.** When a
  category has its own folder on another drive, the job's files are
  written there as they are extracted, so the final move is a rename. A
  job on the same drive as its destination stages nothing. The choice is
  stored with the job, so a restart or a category edit finds it again.
- **Staging never follows a symbolic link.** Staging folders are made,
  checked and removed without following links, and one is removed with
  its contents only when it carries the job's marker. A staging folder
  swapped for a link holds nothing of the job's, and the job delivers from
  its working folder.

## History

- **Every History page is paged, counted and filtered in the database.**
  Search, every sort and the category filter no longer read every history
  row into memory.
- **Counts, totals and category facets run only when the page shows
  them.** The history badge asks only for its count.
- **A finished job's detail page refreshes after a history edit,** such
  as a rename or a category change through NZBGet's API.
- **The job detail page updates only when its own job changes,** not on
  every other job's progress, and pauses while its tab is hidden.

## API

- **`historyItems` returns at most 1,000 rows** unless asked for fewer.
  Pages are no longer cut short by live jobs.
- **`queueItems` and `jobs` cap a requested size at 10,000 rows.** With
  no size given they still return every row.
- **`jobEvents` takes `limit` and `offset`,** returning the newest 2,000
  events by default and at most 2,000 per call.
- **A post-processing script's token can read its own job,** its details,
  timeline and events, and no other job's.
- **NZBGet's `log` call returns at most 1,000 entries.**

## Start-up and upgrades

- **The upgrade page answers during the pre-upgrade backup.** The backup
  takes as long as the database is large, and nothing answered while it
  ran, so a container health check could restart Weaver mid-upgrade. The
  page now comes up, answers `/healthz` and says it is backing up.
- **Stored NZBs move to tables of their own.** Migrations 84 and 85 move
  each job's NZB and outline out of the queue table, in committed batches
  of 32 on SQLite, so an upgrade that is cut short carries on where it
  stopped. A finished job keeps its NZB in the same table, so archiving it
  copies nothing.
- **A job from an older version whose NZB cannot be read fails** with
  `Cannot recover persisted NZB` and moves to history, instead of holding
  its slot.
- Migrations 86 to 88 add the History indexes, the stored full release
  name and the stored search key, and fill them for existing rows.

## Database

- **The WAL checkpoint is asked for once per threshold.** Once the WAL
  was past its threshold and a read held it, every commit asked for
  another pass that could copy nothing. New `weaver_db_wal_*` metrics
  report passes, busy passes, restarts, duration, frames and file size,
  and a pass of a second or more is logged.

## Diagnostics

- **The diagnostics package is written even when Weaver is stuck.** If
  the pipeline does not answer, `pipeline-internals.json` says which phase
  its current turn is in and how long it has run. `database.json` reports
  the database, WAL and shm sizes. A database read that misses its
  deadline is recorded instead of holding up the package.
- **The package includes an NZB report for each job being worked on,**
  up to 25, post-processing jobs first.

## Web

- **Background tabs stop polling.** Server health, history deletes and
  the backups list poll only while the page is visible, and refresh once
  when it is shown again.

## Removed

- **The buffer pool is gone.** Nothing used its buffers. Its settings are
  deleted on load, and the Prometheus series
  `weaver_runtime_buffer_pool_in_use_bytes`,
  `weaver_runtime_buffer_pool_total_bytes` and
  `weaver_runtime_buffer_pool_waits_total` are removed.

## Regression coverage

- **Fuzz targets cover every yEnc encode tier,** encode-then-decode round
  trips, and every decode tier against the scalar decoder and rapidyenc.
- **Guard-page tests** hold every yEnc kernel inside its input and output
  bounds.
- **The queue, dispatch order and article list changes are each checked
  against the code they replace** over seeded queues, step by step.
- **The e2e restart rewinds follow the migration manifest** through
  migration 88.
