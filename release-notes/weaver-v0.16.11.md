# Weaver 0.16.11 release notes

Everything below is new since 0.16.10.

## Highlights

- **No more "database is locked" on network storage.** Every SQLite write
  now goes through the writer gate. Opening a connection no longer writes
  to the database, and WAL checkpoints run on their own task instead of
  inside a commit.
- **Less CPU for large store-RAR posts.** A RAR volume's header walk picks
  up where it stopped instead of starting again at the signature. On a
  125-volume Blu-ray store-RAR post this cut instructions by 29%, and the
  follow-up walk changes cut a further 14%. Every delivered file is
  byte-identical.
- **Encrypted archives no longer stall on restart.** A coverage check read
  every run from the start of the file, once per 16-byte block. A job
  reactivating an encrypted RAR with many gaps could spend minutes on one
  thread before it resumed. The check now starts at the run it needs.
- **Pause the queue for a set time.** Pause all is now a split button with
  30-minute, 1-hour, 2-hour and 8-hour options. The queue resumes by itself
  when the time is up.
- **Expired sessions log in again in place.** When a server expires a
  session in the middle of a batch, Weaver logs in again on the same
  connection. It used to drop the connection and dial a new one, and an
  article could run out of retries.

## Usenet connections

- **Logging in again on a 480.** A server may answer any command with 480
  once a session expires. Weaver now keeps reading the replies already
  queued on the connection and sends nothing new. Once they're read, it
  logs in again on the same socket. If the login fails, or the connection
  has no credentials, the connection is dropped as before.
- **A 480 never costs the article.** An article that got a 480 in place of
  its reply, or was left unanswered behind a failed batch, is retried like
  any connection fault. It no longer counts toward the run of transport
  failures. Before, a server answering half its requests with 480 could use
  up a segment's retries and mark it exhausted.
- **Retries are held only for a quarantined server.** A segment whose
  transport keeps failing has its retry held back only while its server is
  quarantined, so the server's recovery probe tries different work. A
  server that drops some connections but keeps serving others now gets its
  retries promptly.

## Database

- **One way to write.** 96 writes that ran outside the writer gate now go
  through it, from API keys and history to settings and maintenance. Any of
  them could meet a transaction holding SQLite's write lock, wait out the
  busy timeout, and then fail.
- **Opening a connection is a read.** `auto_vacuum` is set only when the
  database is created. Before, every new pool connection wrote the database
  header, so behind a long commit an open could fail with "database is
  locked".
- **A fixed set of connections.** The file pool opens all its connections at
  startup and keeps them for the life of the process. Before, it dropped
  idle connections and recycled every connection after 30 minutes, and each
  reopen read the schema again over network storage.
- **Checkpoints run on their own task.** A commit no longer copies the WAL
  into the database file while every other write waits. A background task
  runs a passive checkpoint once the WAL passes 4,000 frames, and a final
  one at shutdown.

## Downloads page

- **Timed pause.** While a timed pause is on, the resume button reads
  "Resuming in HH:MM", turns warning yellow with the update pulse, and
  changes to "Resume now" on hover.
- **Better search.** Search matches words in any order across `.`, `_` and
  `-`, so "show s01" finds `Show.S01E02`. It also accepts SQL LIKE
  wildcards (`%` and `_`).
- **Speed limit shown.** The speed-limit button gets a yellow outline while
  a limit is in force.
- **Steady speed line.** A rate-limited download's speed line keeps its last
  rate between articles instead of flickering. This holds in every active
  state, including repair, extraction and moving.
- **Inspector buttons wrap.** Buttons in the download inspector stack and
  wrap long labels instead of overflowing.

## Settings

- **Less text.** Helper text that restated its field, and most panel
  subtitles, are gone. The Servers panel keeps its note, and the TLS
  setting is labeled "SSL/TLS".
- **A clearer upgrade failure.** When GitHub returns a server error during
  an in-app upgrade, the message says GitHub is temporarily unavailable and
  asks you to try again later.

## Encrypted ZIP (ZipCrypto)

- **A repaired byte keeps its password.** A posted-corrupt article could make
  the right password fail its CRC, so the member ended Encrypted even after
  recovery repaired the byte. The password now stays open until the repair
  lands.
- **A restore proves its password first.** Keys are never derived from
  plaintext another candidate produced. When no candidate proves a restored
  member, the member starts over, its ciphertext is fetched again, and you
  are asked for a password.
- **Repairs replay only from the repaired byte.** A repair once replayed the
  whole member, so several repairs in a 50 GB member meant several 50 GB
  read-backs. The replay now runs from the repair to the member's end.
- **Replays use bounded memory.** A replayed range is read back and staged
  on disk 1 MiB at a time. A replay of a whole 50 GB member no longer needs
  about 100 GB of RAM.
- **Uncovering a range replays the bytes after it.** An article sent back
  for another copy, or a recheck that uncovers a range, now also replays
  the plaintext after it. This keeps working across a restart.
- **Fewer rebuilds and copies.** A repair near the end of the archive no
  longer rebuilds the directory map. Redeliveries are tried without copying
  the member's keys, and a posting shares one set of keys per member.
- **Archives that read as damaged now open:**
  - entries whose data descriptor uses zip64 widths because the local header
    has a zip64 field;
  - encrypted directory entries, which close without asking for a password;
  - entries whose spans overlap.

## RAR and the graph

- **Held bytes keep their evidence.** When volumes arrive out of order, held
  bytes now carry the checksums they arrived with, so each layer below no
  longer hashes them again. A member served backwards now hashes as many
  bytes as one served in order.
- **Fewer lookups for held bytes.** The release pass asks a store where held
  bytes go only after the store's map has changed.
- **Checkpoints are snapshotted only when they are written.** A graph step
  adds to the dirty count instead of snapshotting the job each time. The
  snapshot is taken when the write is due: after 256 MiB, after 5 seconds,
  or when a move waits on a commit.
- **Cheaper lookups:**
  - held-range lookups on replay read only the holds that overlap;
  - a seek that nothing has changed since it was last answered is not
    mapped again;
  - kept wants and deliveries look up only the pieces in their range;
  - a header walk images only the runs past its cursor.
- **Volume facts reuse.** RAR volume facts are cached per volume and dropped
  when a repair changes a header. Checkpoints written by 0.16.10 still load.
- **Sparse replays skip lost ranges.** A replay sends down only runs that
  arrived, even where a sparse file covers a lost range.

## Restarts and delivery

- **A file that can't be removed no longer fails the job.** If a consumed
  input can't be removed, for example a `.par2` a virus scanner holds open
  or a file an SMB share refuses to delete, Weaver logs a warning and
  finishes the delivery. The file is left in the working directory.
- **Nested complete directories resume correctly.** When the complete
  directory, or a category's destination, is inside the intermediate
  directory, a restart during the move no longer splits the job across two
  folders.

## Extraction

- **Extraction uses every allowed CPU.** On Linux, extraction workers were
  inheriting the download threads' CPU pinning. They now get the full CPU
  set the process or container allows.
- **Multithreaded BZip2 in 7z.** sevenz-turbo 0.29.5 hands the extraction
  thread budget to the BZip2 decoder, which ran on one thread before. It
  also carries that release's security fixes.

## Submission

- **Protection flags aren't passwords.** Indexers can send values such as
  `-1` or `protected` in `X-DNZB-Password` to flag a protected post. These
  are no longer tried as archive passwords; any real password in the header
  is still used.

## Regression coverage

- **The crash oracle matches coalesced checkpoints.** The extended archive
  campaigns now expect a crash to cost the articles delivered since the
  last checkpoint, and nothing that checkpoint names.
- **The timed-pause test steps the paused clock.** The timed resume test no
  longer depends on when the resume timer first runs.
- **Clippy enforces the writer gate.** `clippy.toml` bans sqlx's write,
  transaction and connection entry points, so a new SQLite write has to go
  through the writer gate or carry a reviewed exception. A test checks the
  ban list and every exception.
- **The restart test reads the schema before renaming a column.** With
  every pool connection kept open, a column rename could run on a
  connection whose cached schema predated an earlier rename.
- **End-to-end specs locate by test id.** The jobs, networking and settings
  pages, and the values in the RSS, backup and speed-test reports, carry
  `data-testid` attributes defined in one module per area. The Playwright
  specs find elements by those ids, so a wording change no longer fails
  them. An audit rejects locators that use visible text, unless the text is
  what the test checks. There are no visible changes.
- **More reliable end-to-end runs.** Fixed timeouts are replaced with waits
  on the state under test. The RSS error flow no longer races the first
  poll, and a `compose up` that the container engine's API dropped is
  retried.
