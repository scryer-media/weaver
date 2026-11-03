# Weaver 0.16.6 release notes

## Highlights

- **One engine for every job.** Weaver now processes each job as a tree:
  the posted files feed the archives, streams and recovery sets they make
  up, and each of those feeds whatever it holds, as deep as the archives
  nest. RAR, 7z, zip, tar, split files and compressed streams (gz, bz2, xz,
  zstd, lzma, deflate, brotli) are read as their bytes arrive. PAR2, PAR3,
  RAR recovery volumes (`.rev`) and the recovery record inside a RAR volume
  repair the members beside them at any depth, and a member a repair
  rebuilds is offered again to whatever refused it.
- **Large queues stay fast.** A queue of thousands of jobs no longer costs
  work per job on every pass: jobs are found by id through an index,
  queued jobs hold no article list until they start, a paused queue uses no
  CPU, and the web queue reads one server-side page at a time.
- **A limit on concurrent NZBs.** A new **Maximum concurrent NZBs** setting
  (default 10) bounds how many Usenet jobs are in progress at once, from
  their start through repair, extraction and final completion, shared
  across all providers.
- **Partial means something usable.** A job ends Partial only when at least
  one whole file of a type people open (video, audio, images, documents and
  the like) was delivered. Whole archive volumes, split parts, recovery
  files and bare `.bin` files do not count; with nothing usable the job
  fails.

## Upgrade note

This release adds database migrations 0066 through 0079 for both SQLite and
PostgreSQL. Each job now keeps its restart state in a new checkpoint, and
the tables that held the previous engine's restart state are dropped.
Jobs that were downloading when you upgrade have no checkpoint in the new
form, so they start their download again from the beginning. Queued jobs
that had not started, and job history, are kept as they were. Jobs that
had already started are admitted under the new concurrent-NZB limit
without waiting for a slot.

## Downloads and recovery

- **Resume where the job stood.** A job's checkpoint records what is on
  disk at every level of its tree, including bytes held aside for a later
  read. A graceful stop writes held bytes to the job's scratch file so a
  restart reads them back instead of fetching them again, and parts that
  arrived ahead of their place are kept across a pause or restart.
- **Dead posts are judged once.** The first-article sample, the article
  availability probe and the post's critical health line are weighed
  together, counting only articles the servers said are missing. A dead
  post fails early with its reason; once a post is dead, files that already
  lost an article are abandoned while the rest keep coming. The verdict is
  kept across a restart.
- **Recovery that holds up.** A recovery set does not rule while its own
  articles are still to come. A PAR2 set whose index is damaged is founded
  from its recovery volumes, a set without slice checksums is verified by its whole-file
  hash, another PAR2 block is read when the first choice does not rebuild,
  and a member posted short is rebuilt to its described length. A RAR
  verdict stays Damaged once a volume was refused.
- **Repeated postings are used.** When an NZB lists one part more than
  once, each message-id is kept and tried in order before the article is
  counted lost. A file posted again under its own name folds into the file
  it repeats.
- **Damaged streams fail cleanly.** A damaged compressed stream or streamed
  tar is not repairable and fails the job, while its pieces are still
  delivered as they stand. A tar with one whole entry ends Partial.
- **Passwords and encryption.** A RAR4 or 7z password that fails its
  checksum on verified bytes is ruled wrong for those bytes only, and the
  affected data is fetched again and opened with the next candidate. A
  password-protected RAR volume changed after it was written fails its
  keyed whole-file check and is treated as damaged.
- **Provider connections.** Only a provider's too-many-connections reply
  starts the over-limit backoff; a connection the server accepts and then
  drops waits the ordinary reconnect delay. A server holding connections
  back no longer has work handed to it that it cannot take. An unreachable
  address family is skipped while the other works. The backfill tier opens
  only for articles every main server lacks, and the provider toggle
  formerly labelled "Block account" is now **Backfill server**.
- **Faster article handling for large sets.** Per-article work that grew
  with the size of a RAR or split set is gone: volumes find their parts
  among their own pieces, split stores track where each piece starts, and
  pending work is built once per article instead of three times.

## Queue, API, and integrations

- **Queue at large sizes.** The queue finds a job by id through a position
  index, re-walks its rows only when something changed (a new request, a
  job coming or going, or a gate moving), writes queue order with spaced
  position keys only for rows whose stored position no longer sorts where
  they stand, and stores each job's wait reason so a dispatch pass reports
  only jobs that changed. A paused or idle queue disarms its periodic work
  and uses no CPU.
- **Faster restarts with long queues.** A restart reads each queued job's
  NZB once, in batches of 32 with slot holders first, keeps only its file
  list, lets downloads proceed between batches, and writes a restored row
  back only when the restore changed it.
- **Server-side queue page.** The new `queuePage` query returns one page of
  the queue with search, state, priority and category filters, sorting by
  name, state, priority, category, progress or size, counts per state and
  category, remaining bytes, and the figures for time left. The web queue
  page now uses it and keeps the current page usable until the next one
  arrives. Release-name caches are bounded.
- **Batch queue changes.** `pauseQueueItems`, `resumeQueueItems`,
  `cancelQueueItems`, `setQueueItemsCategory` and `setQueueItemsPriority`
  take up to 1,000 ids or a server-side selection, apply the whole batch as
  one queue change, and list each job they did not change. A selection with
  no filter must say `all: true`. A burst of many item changes publishes one
  global change notice instead of one per item.
- **Metrics and NZBGet without queue copies.** The Prometheus scrape takes
  its job counts from the queue in place and copies only jobs that get
  their own series; Partial jobs are now counted under failed. The NZBGet
  `status`, `listgroups`, `listfiles`, `postqueue` and `history` calls read
  the queue in place, and `listgroups` streams its groups in chunks of 256.
  `editqueue` applies every id it finds.
- **Coded GraphQL auth errors.** A rejected GraphQL request now returns a
  JSON body with a stable code: `INVALID_API_KEY`,
  `AUTHENTICATION_REQUIRED`, or `BROWSER_VERIFICATION_REQUIRED`. Status codes
  and who is accepted are unchanged.
- **Finished jobs never vanish.** A finishing job stays in the queue until
  its history row commits, so it is never missing from both.
- **NZBs from web tools.** HTML named entities in NZB text decode to their
  characters, and an unknown entity is dropped instead of rejecting the NZB.

## Startup and files

- **Quieter startup.** The disk speed probe runs off the startup path,
  reuses a result under seven days old, and stops on a failed read instead
  of reporting a falsely fast disk. Database maintenance and free-space
  sampling wait for it, and download connection threads start on first use
  and are released after five idle minutes. A disk measured under 100 MB/s
  extracts one job at a time.
- **Startup survives a bad row.** A job row that cannot be archived at
  startup is logged and left for the next start instead of stopping Weaver.
- **Names on disk.** Names that differ only in Unicode composition or case
  are treated as one path, a directory and a file of the same name never
  share it, and Windows drive colons in part names become underscores.

## Web interface

- **Copy buttons work over plain HTTP.** Where the browser offers no
  asynchronous clipboard, copy buttons fall back to the selection command
  and report a refusal instead of failing silently.

## Security

- Refuse zip archives whose local headers name different files, sizes or
  methods than their central directory (GHSA-m665-v7pm-7grj), in jobs and in
  watch-folder intake.
- Bound zip central-directory size and entry count, and cap RAR decoder
  dictionary and PPMd memory to the job's memory grant.
- Diagnostics packages replace job names, metadata titles, member names and
  directory names, read from active jobs and the 2,000 most recent history
  jobs, with stand-ins in every file of the package. The manifest states
  how many history jobs the replacement covered.
- Update `unrar-rs` to 0.15.0 and `par2-rs` to 0.11.0.
