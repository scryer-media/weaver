# Weaver 0.16.16 release notes

Everything below is new since 0.16.15.

## Highlights

- **A full disk holds a job; it no longer fails it.** A write that finds
  no room, a quota refusal, or a disk below the reserve parks the job. It
  keeps its place, shows that it waits for disk space, and goes on by
  itself when its disk shows room, or when you resume it.
- **Each disk has its own writer.** File writes left the pipeline's main
  loop. One writer thread per volume takes every write, close and rename
  for the files on it, with a hard limit on the bytes waiting for that
  volume. A slow disk no longer slows downloads to a fast one.
- **Archive a history entry.** You can remove a finished job's files and
  keep its history entry, by hand or by a retention setting.
- **Old RAR sets deliver.** Sets written by RAR 3.0 to 3.3 are read as
  RAR wrote them, their volumes past `.r99` are found, and their recovery
  volumes are matched to them.
- **More hardening against crafted posts.** A second round of fixes
  bounds memory, stack and repeated work in the archive, repair, naming
  and ingest paths.

## Disk space

- **A job short of room waits.** This covers a download write, an unpack,
  a decode window, a repair's staged output, a recovery-volume restore and
  the final move to the destination. The wait is logged once as it begins,
  with the folder, what the job owes, what is free and the reserve.
- **A held job goes on from its last checkpoint** and fetches or writes
  again only what that checkpoint does not hold. A held final move goes on
  from the files it had left.
- **Room promised to one job is not promised to the next.** Jobs held on
  one disk are let go only as far as the room that is really there.
- **A history write the database's disk has no room for is kept,** shown
  waiting for disk space, and written when its folder shows room.
- **A file that never arrived is rebuilt by PAR3 only where the disk has
  room for it.**
- **One owner asks the disk for free space and holds the answers.** A
  path that is gone is read at the nearest folder above it, never at a
  file on the way. A configured folder inside another root is a volume of
  its own.

## Disk writers

- **Finished files are renamed into place by the writer of the volume
  they land on.** A file only a copy can place is copied by the mover on
  its own thread, with progress shown as the bytes land.
- **Repairs, checks and extraction reads wait for queued writes on their
  own threads,** not on the main loop.
- **Each volume has its own buffer limit,** sized by the profile, memory,
  connections and the volume's measured write rate. The limit is a budget
  nothing goes over. A download lane held for a full buffer is asked again
  only when the volume has room.
- **Files are sized up front where the filesystem makes holes.** On
  Windows a sparse output is sized to its declared length when made.
- **On Windows a volume is told by its GUID path,** or by its share root
  where it has none, not by its serial number.
- **The system page and the API list each volume** with its write rate,
  throughput, buffered bytes and buffer limit. The metrics page and the
  Grafana dashboard have a Volumes row. The loop-side disk write timings
  are removed, since writes no longer run there.
- **The disk speed probe never touches a downloaded file.** It writes a
  file of its own, made only where no file has its name, and removes
  nothing else.

## History

- **Archive removes a job's files and keeps the entry.** The entry is
  stamped, keeps a record of up to 5,000 removed files, and shows a
  download summary on the job page.
- **History retention, default 90 days; 0 keeps forever.** An install
  that already has history starts at 0, so nothing is removed until you
  choose a value.
- **An archive never takes another job's delivery.** Output folders carry
  the job's marker, which is removed last, so a failed removal can be
  retried.
- **The archive password can no longer be read through the API.**

## Archives

- **RAR 3.1 to 3.3 end records are read as written,** through unrar-rs
  0.16.2. A set whose end records state no volume number delivers in name
  order.
- **Old-style volume names run through `.z99`,** a hundred to a letter. By
  name alone only `.rNN` and `.sNN` are RAR volumes; `.tNN` through `.zNN`
  are one only beside a RAR volume of the same set.
- **RAR 3.0 recovery volumes,** named by their set's data, recovery and
  position counts, are matched to their set.
- **A RAR volume held by the wrong store is given back, taken by the
  store that wants it, and fetched again if its bytes are gone.** A job
  could stay at Verifying.
- **A stored set that is given up reads its volumes back one at a
  time.** The whole set was read back in one step, so on a large set
  downloads stopped and a cancel or a stop was not heard until it ended.
- **A stray file under a RAR volume's name no longer keeps the real
  volume out.** A volume's place comes from its headers.
- **A split set whose first piece is numbered zero counts from zero.**
- **Missing RAR volumes are reported as runs,** with no cap on the last
  volume's number.
- **A member whose name the filesystem refuses is skipped,** and the job
  goes on.
- **A refused member's lost ranges are not written out as zeros.**
- **Nested archives with members too large to hold in memory unpack.** A
  hand-over to the inner archive skips delivered bytes only for the
  volume that got them.

## Repair

- **Recovery is asked for as soon as a loss is known.**
- **A PAR2 repair works in its whole memory grant** and reads recovery
  blocks from the volumes where they lie, holding none. A recovery block
  that does not read is dropped and the repair is planned again with the
  blocks left.
- **Repair memory grants are lower:** Efficient 128 MiB (was 256 MiB),
  Balanced 512 MiB (was 1 GiB), Performance 1 GiB (was 2 GiB).
- **PAR3 recovery volumes are read by location, a piece at a time.** PAR3
  length matching, binding and cover each take one pass.
- **A RAR recovery set that covers no data volume rules on nothing.**
- **A PAR2 set waits for recovery volumes whose names give no block
  count,** such as `name.vol-01.par2`. Such a volume counted as bringing
  no blocks, so a job that its queued recovery could repair was given up
  as unrepairable.
- **A lost run in a uuencoded volume starts where the article before it
  ended.** Bytes that arrived were told lost, the repair read them as
  zeros, and a 7z set with PAR2 that should complete failed.

## Servers and downloads

- **A server that refuses the login is waited for an hour,** as through
  an outage, before the articles only it could bring are booked missing.
  The job's failure then names that server, and its next duplicate is
  tried.
- **A server with no login that answers a download with 480 counts as
  refusing the login.** It was asked again without end, so the jobs
  waiting on it kept their slots and the rest of the queue did not start.
- **A held server recovers.** Its recovery probe carries real work and is
  never left unanswered, so jobs no longer stall after body timeouts or a
  refused login.
- **Articles held in memory are fetched again after a restart.** A
  checkpoint no longer counts them as on disk.
- **A pipelining trial runs on one connection,** and only a fault after a
  pipelined answer counts against it.
- **A 412 that selecting a group cannot answer spends the article's
  retry.**
- **Less work per article.** The phase read, the health re-check and the
  server sample no longer walk the job or take a lock on every article. An
  idle ask of the download scan walks no files.
- **A server list past 50 is trimmed once, by a migration.** A backup
  holding more than 50 servers restores.
- **A server's card shows what it found and did not find.** A server that
  was never timed shows a dash for its round trip.

## Queue and jobs

- **A cancelled job is not restarted while a move of its working folder
  is still out.** A retry waits for the move and the removal first.
- **Housekeeping removes only folders that carry the job's marker,** and
  sweeps a failed job's files once. It looks up only the jobs those
  folders name, not the whole queue and history.
- **Database writes no longer queue behind every checkpoint pass.** Slow
  statements gained indexes.
- **Long chains of files that want one name are seated without rescans.**
  Several quadratic scans in naming and removal are gone, and a long
  chain runs in a loop, not on the stack.

## Stopping

- **Every wait for the server to stop is 60 seconds.** The tray waits
  that long for a stop you asked for, and the Windows installer now waits
  before it kills. A stop at session end keeps the short wait.
- **Shutdown waits for every database write.** A step that writes nothing
  is left behind after 10 seconds, and a slow step is named in the log.
  Shutdown no longer waits forever on a change held behind a queued
  write.

## Security

- **The setup code is eight characters.** Attempts are limited per
  client, an IPv6 client is counted as its /64, and a full failure table
  refuses new clients instead of forgetting old ones.
- **A key link asking for an Admin key fills the form and waits for a
  click.** The created-key dialog names the scope.
- **A diagnostics bundle replaces a WireGuard key and its peer key
  prefix wherever spelled.** The bundle also covers every recovery kind,
  and waiting jobs show their figures.
- **The unused per-hop proxy code is gone,** so nothing can dial a proxy
  around its route's egress. A proxy pool with every member blocked fails
  its leg.
- **The `Forwarded` header is read as a chain.**
- **A Go post-processing script builds away from the download,** and the
  tray opens only web links.
- **Crafted posts:** stream runs hold only the window reserved for them,
  folded copies of an entry count toward the entry limit, output another
  archive reads is under the extraction budget, the name ledger's memory
  is charged to the budget, and a retired job's key derivation stops.

## Install defaults

- **macOS:** a fresh standalone install uses the application data folder
  and the macOS download defaults.
- **Linux:** a fresh standalone install uses the XDG data directory.

## API changes

This release changes the GraphQL API in ways that can break a client.

- **`validatedArchivePassword` is removed.**
- **The count of articles the download lanes hold is named articles in
  flight** in the API, the Prometheus gauge, the diagnostics bundle and
  the web history. The old name is gone.
- **New:** `archiveHistory` and its types, `HistoryItem.archivedAt`, and
  one row per volume in the system query.
- **`jobOutputFiles` and `jobEvents` page at 100.** A `jobOutputFiles`
  page of a folder on disk starts after the last file of the page before.
  A cursor from an earlier version is refused there; list again from the
  start.

## Dependencies

- unrar-rs 0.16.3.
- par2-rs 0.12.0 and par3-rs 0.8.2, from the registry.

## Regression coverage

- **Adversarial end-to-end cases** for NZB text, article-level lies, RAR
  volumes, split sets, recovery sets, passwords, containers, the
  extraction budget and output names. They run by name only.
- **A pipeline on real writer threads** is tested with a volume held,
  paused, cancelled, failed, over its write budget and stopped.
- **Rows past the size limits** that the small test rows never cross.
- **End-to-end waits follow state,** not fixed sleeps, and a missing
  metric series fails a spec instead of reading 0.
- **A second end-to-end full run refuses to start** while one is in
  process on the same machine.
