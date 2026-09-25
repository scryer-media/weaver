# Weaver 0.13.4 release notes

Everything below is new since 0.13.3.

## Highlights

- **Dead posts fail in seconds, not minutes.** The first article of each of
  a job's leading files is now fetched ahead of the payload as its own
  dispatch class. When the sample comes back and four fifths of at least ten
  files answer "no such article" on every server, the job is refused with
  what it read, naming the missing and total counts. Anything short of that
  is left to the health arithmetic, which now credits a recovery volume only
  while one of its articles has arrived or can still arrive, so a posting
  whose recovery volumes are gone can no longer buy itself a deferral on the
  strength of their filenames.
- **Stored 7z containers route through the direct store.** A split 7z whose
  members use the Copy method is placed straight into its output files as
  the articles land, the way RAR sets have been since 0.12. The container's
  geometry follows from volume zero alone, so no volume has to be heard from
  before bytes are placed, and every disagreement between the map and what
  arrives demotes the set to the conventional extractor instead of failing
  the job. Header-encrypted containers are opened against the job's whole
  password harvest, and a volume PAR3 rebuilds in place is routed like any
  other.
- **A restart no longer starves the rest of a download.** A job restored
  mid-file kept its write cursor at zero, so every part that arrived after
  the restart waited on bytes that were never coming, the durable floor
  never advanced, and the restart guard handed out one article at a time for
  the rest of the job. The cursor now resumes from the restored prefix.
- **Demoted sets hand volumes back as they finish.** A demoted set's
  reconstruction sweep used to hold every volume out of dispatch until the
  last one landed, leaving a job idle for the whole sweep on a slow working
  directory. Each rebuilt volume now returns to dispatch the moment it is
  finished (#76).
- **Many connections no longer spread a link across jobs.** Each lane is
  held to its share of the hot job's unfetched articles, and a lane that is
  full of one job does not open the next until its pipe has drained. A
  saturated lane that faulted mid-lease could previously deadlock against
  the dispatcher; its tail is now returned before its refill.
- **The direct store carries decoded bytes to disk without a copy.** Decoded
  articles are written straight from the decoder's buffers, an encrypted run
  is resolved before it is materialized and decrypted in place, short holds
  are copied out of pool slots so a few retained bytes no longer pin a whole
  buffer, and a run waiting on a missing cipher block is skipped instead of
  re-attempted on every arrival.
- **A destination that refuses a write fails the job instead of refetching
  it.** When the direct store's destination refuses a routed write, a
  member, or an empty entry, the job fails the way any other disk write
  failure does. Causes are: no space or quota, no permission, a read-only
  filesystem, or a file where a directory belongs. It used to demote the
  set, which refetched every volume only for conventional extraction to
  meet the same refusal.
- **Finished direct sets survive a restart.** A finalized set leaves an
  installation marker, and on restart a set whose members are still in
  place at their recorded lengths is restored as finished, so none of its
  volumes is fetched again.

## What changed

### Download and dispatch

- The first-article sample leads the ordinary queue as its own class, ahead
  of the payload and behind completion-critical repair work. Recovery volumes
  stay out of the sample.
- Health aborts attributed to the first-article sample report the missing
  and total counts, and the verdict is decided as soon as the missing share
  crosses the gate.
- The first-article gate stands down when the files it rules missing, taken
  as wholly lost, fit inside the recovery the job can still obtain. Losses
  in a parsed set are counted in that set's own PAR2 slices rather than as
  encoded bytes. Recovery volumes no longer count against the sample's file
  budget, so a post that lists its PAR2 volumes first still samples its
  payload.
- The below-critical deferral requires recovery the pipeline has actually
  observed and caps tolerated damage at that ceiling. PAR2 metadata
  discovery closes when a candidate article was retired during the ordinary
  download pass. A job whose own lanes starve its health probe stands down
  for one handout so the probe gets a lane.
- A lane's holdings of a job are capped at that job's unfetched articles
  divided over the link's connections, never less than a full pipe plus one.
  A lane at its share is reported busy rather than idle, and a lane opens a
  job it holds nothing of only once its whole pipe is below a full pipe plus
  one.
- A parking lane returns its pending tail before draining its refill.
- After a restart the per-file write reorder buffer starts past the resumed
  prefix, adopting the offset of the first part that follows it.

### Direct store and direct unpack

- 7z containers whose members are stored route direct. Geometry is derived
  from volume zero's declared part size and the start header's coordinates;
  a volume whose declared or decoded length disagrees, a part count that is
  not the set's, or a map the total cannot divide demotes the set.
- A 7z set whose volume can never state its length, because every article
  of it is terminally unavailable, is demoted under its own refusal instead
  of waiting forever.
- A 7z volume that states its length, or finishes decoding, before volume
  zero's front arrives is checked once the geometry is known. A truncated
  last volume is refused on its length whatever order the volumes land in,
  instead of holding the set until every article is in and then demoting
  it as an unreadable map.
- A 7z container of only directories and empty files is declined up front
  as `7z_nothing_to_route` and extracted conventionally.
- A destination refusal while routing, committing a member, or creating an
  empty file or directory fails the job. Other write failures still demote,
  as does a member whose partial is gone, since that is lost data and the
  refetch is how it comes back. Once a job has failed, finalization stops
  committing its other sets.
- A finalized set records an installation marker in its coverage row. On
  restart, a marker whose members are still in place at their recorded
  lengths restores the set as finalized and extracted. A marker whose
  members are gone is deleted and the set downloads fresh. Barriers skip
  finalized sets, and a source retry drops the retried sets' coverage rows.
- A demotion handback that leaves verified conventional bytes with no floor
  or completed-file row retires the set's coverage row at once, so a
  restart cannot resume the set over them.
- Header-encrypted containers are opened against the job's whole password
  harvest in harvest order, after one attempt with no key.
- A restart-seeded volume length goes through the router when the volume
  completes, a zero-length member is dataless whether or not it claims a
  stream, and a segment that lands twice after its file is committed no
  longer wedges the reorder buffer.
- A volume PAR3 rebuilds for an installed 7z set is written at the volume's
  own name so the set can route it, and is removed at finalization.
- Encrypted members keep the plaintext of cipher blocks that straddle
  article boundaries, so a read across a gap is served from the edge block
  instead of refused.
- Direct-unpack chases pay widening memory one thread at a time through a
  non-blocking reservation instead of reserving the whole widening room up
  front. One blocked waiter unwinds at most one parked chase, instead of
  every chase in the process.
- A demoted volume whose owner can no longer finish is settled as damaged
  once the job's download pipeline is idle, and a handed-back file's write
  buffer is drained to completion.
- Decoded payloads reach disk without a user-space copy; the encrypted path
  keeps one contiguous materialization for decryption and adopts its output.
- A held view is copied out of its decoder buffer when it is 64 KiB or
  shorter or covers less than half of the buffer. Every view is copied
  when its article's pool tier has a quarter or fewer of its slots free,
  or when the article is not in a pool slot. A whole held article keeps
  the zero-copy path. Holds pin at most twice what the budget charges, and
  copied bytes are reported as `direct_store.holds.copied_out_bytes`.
- An RAR5 key is derived once per tuple through the shared KDF cache.

### Extraction and repair

- 7z block decode errors (corrupted, checksum mismatch, invalid data,
  unexpected EOF) are recoverable extraction errors. A failed 7z set with
  every volume present reopens the PAR2 verdict instead of leaving the job
  in Extracting.
- A finalized direct set's source volumes no longer count as archives
  awaiting a topology, so a 7z-only job whose set had already installed its
  members no longer re-arms its completion check forever.
- PAR3 rebuilds wholly missing RAR and 7z volumes in place.
- A 7z decode whose memory need cannot be measured within the ceiling
  reserves what fits beside every job's retained state, instead of waiting
  for the whole ceiling behind any other queued job.
- lzma-turbo 0.6.0 and sevenz-turbo 0.26.0: the parallel LZMA2 reader sizes
  its read-ahead from the stream, spends a memory limit on decoding before
  reading ahead, and decodes incompressible runs narrow.

### Persistence

- Each job's NZB password candidates are derived once and kept in job
  state. Classifying a completed archive file no longer reloads the NZB from
  the database and re-parses it, on either datastore.
- On PostgreSQL, ordered writes run concurrently across per-job lanes
  instead of one at a time. Writes for one job still apply in submission
  order, a job's archive runs after every earlier write for that job, flush
  and shutdown drain wait for everything queued before them, and the lanes
  are bounded so a slow database applies backpressure. A lane is forgotten
  once its last write finishes, so the lane table no longer grows by one
  entry per job. SQLite keeps its single writer.

### Logging

- The RAR completion checkpoint logs at INFO only when its content changes
  and summarizes the suspect list as ranges. The PAR2 read-back failure
  warns once per file. A demoted set blocking PAR2 settlement is announced
  at WARN with the file and the reason, once per state change.

### Windows

- Windows MSI installs are recognized by the in-app updater. Program Files
  roots are canonicalized before comparison with the executable path, MSI
  elevation and managed-install restrictions are preserved, and the generic
  ineligibility message is corrected in every locale.

## Upgrade notes

- No schema migration or configuration change is required.
- The direct-store checkpoint plan digest appends a family tag for non-RAR
  sets only; every RAR checkpoint written by earlier releases still
  validates.
- Jobs that were mid-download when 0.13.3 or earlier stopped resume with a
  correct write cursor on first start; no manual intervention is needed.
