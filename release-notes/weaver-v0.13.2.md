# Weaver 0.13.2 release notes

## Highlights

- **Articles that other clients decode no longer fail in Weaver.** A yEnc
  header with a missing, unreadable or reversed part range, a part header
  Weaver did not expect, stray lines around the header, a missing `=yend`
  trailer, bytes after the trailer, or a terminator that arrived in a separate
  network read — each of these used to fail the article, and a file whose
  articles disagreed with the NZB's part numbering could be abandoned
  altogether. Weaver now places the bytes it actually decoded, records the
  defect, and lets the article CRC, the whole-file CRC and PAR2 decide whether
  those bytes are kept. Damaged bytes are retained for repair without the file
  claiming to be complete.
- **Memory no longer scales with the connection count.** An idle
  100-connection installation held 542 MB; a parked download lane now returns
  its own idle pages, decoded articles ride a pooled buffer all the way to the
  writer instead of being copied into a fresh allocation per article, and every
  platform but Windows now allocates through jemalloc, whose steady page decay
  suits a pipeline that cycles the same few buffer shapes thousands of times a
  job. Peak per job on the 100 ms round-trip benchmark fell from 410-430 MiB to
  330-350 for the same wall time.
- **Submitting a batch of NZBs is no longer expensive all at once.** Every
  queued job that had received its opening articles armed a direct-unpack
  chase — a worker, a staging tree, a coverage map and an extraction budget
  apiece — for a download that was not moving. A chase now waits until the job
  reaches the front of the dispatch order.
- **A Windows installation can move between a released build and one built
  from source.** Both now compute the same migration checksums, so a database
  no longer has to be refused with a checksum mismatch.

## What changed

### Downloading and decoding

- **A part range Weaver cannot use no longer fails the article.** Only the
  start offset decides where bytes land, and the length that decoded decides
  how many. A missing, unreadable or reversed `end`, or one past the declared
  file size, is recorded as a defect and the article is decoded.
- **A part header after a single-part `=ybegin`, and stray lines before it, are
  tolerated** instead of being read as a malformed article.
- **A missing `=yend` trailer, and bytes after it, no longer fail the
  article.** Neither does an NNTP terminator whose three bytes arrived split
  across two reads.
- **A CRC mismatch keeps the decoded bytes.** They are not verified data, but
  they remain available for a replacement from another server and for repair,
  rather than being discarded on the spot.
- **A file is no longer abandoned because its articles' part numbers disagree
  with the NZB.** Weaver stopped fetching a file once twelve articles agreed
  on a layout other than the declared one. Poster part and total numbers can be
  stale while the bytes themselves are fine, so they no longer gate placement;
  the offset is still bounded by the NZB's own envelope, and the checksums
  above still decide what is kept.
- **A part header that cannot say where it starts no longer fails the
  article.** A missing, unreadable or zero `begin` is recorded as a defect and
  the part is laid immediately after the part before it; if that part is not
  placed yet, the article is asked for again, with a bound on how often.
- **An article the NZB cannot bound is accepted.** An NZB that skips a segment
  number, or understates a segment's byte count, made every later article of
  the file look out of place, so every server's copy was refused and the rest
  of the file abandoned. A placement now has to fit the NZB or the article's
  own declared file size, under fixed per-article and per-file ceilings, and a
  declared segment larger than the per-article ceiling raises it.
- **An article that looks cut short is fetched again from another server.** A
  missing trailer, or a length the headers disagree about, was accepted when
  there was no checksum to fail it on. Those bytes are now written but not
  counted as coverage until a verified copy, or one whose own length adds up,
  settles it.
- **A whole-file CRC32 no longer ends a job.** Posters that write a running
  checksum, or zeros, on every part but the last made the parts disagree, and
  that disagreement failed the job; the file now drops its whole-file
  expectation instead. When every part verified and the parts cover the file
  exactly, a mismatching whole-file value is treated as the wrong one.
  Otherwise the file is marked incomplete and left to verification and repair
  rather than failing the job.
- **Damaged bytes are written without the file reading as complete.** The file
  is marked as needing verification, and repair decides its fate, instead of
  known-bad bytes passing as finished output.

### Memory

- **A parked download lane releases its idle pages.** Each connection is one
  long-lived thread, and the article buffers it allocates are freed by the
  decode and writer threads, so the pages returned to that lane's own heap and
  stayed there once it parked. Idle footprint was 151 MB at 5 connections,
  329 MB at 40 and 542 MB at 100.
- **A decoded article rides its pool buffer to the writer.** The copy into a
  fresh allocation per article, allocated on one thread and freed on another,
  is gone; the slot returns to the pool when the write batch is done with it.
  An empty pool tier also no longer parks decode work.
- **Article pages are recycled inside a bounded arena** rather than each
  article-sized buffer becoming a fresh mapping whose pages are faulted in
  again. Memory is returned at the boundaries that know the pages are
  unwanted: a parked lane, and the end of a job.
- **Every platform but Windows allocates through jemalloc,** including
  glibc Linux, which previously used the system allocator. Windows keeps its
  tuned mimalloc. The portable arm64 Linux build is built for 64 KiB pages and
  runs correctly on 4 KiB kernels as well.
- **A direct-unpack chase is armed for the job being downloaded,** not for
  every queued job that happens to have received its head wave. The refusal is
  counted and not latched, so a set arms as soon as its job reaches the front.

### Database

- **Migration checksums no longer depend on line endings.** The released
  Windows build embedded migration SQL with CRLF line endings and recorded
  CRLF checksums, so a Windows build made from an ordinary checkout refused
  that database with a migration checksum mismatch, and the reverse held too.
  Checksums are now taken over an LF-canonical body, and a database carrying
  either of the old forms is accepted and rewritten to the canonical value on
  open. An unrelated or corrupt checksum still fails as before.

### Upgrades

- **Settings > General has a Check for updates button.** A new admin-only
  `checkForUpdates` mutation asks the release checker to look now rather than
  waiting up to six hours for the next scheduled check, and reports what it
  found beneath the button. It shares a lock with the scheduled check, so a
  click during a running check waits for that check instead of fetching twice,
  respects the release API's rate-limit backoff, and does nothing when checks
  are turned off. A backoff the release API asks of a manual check also holds
  back the scheduled one, and the result under the button keeps following the
  checker afterwards instead of freezing on the answer to the click.
- **The "Update available" notice opens the in-app installer.** Both the rail
  block and the classic sidebar notice used to open the GitHub release page,
  sending an installation that can upgrade itself off to download the release
  by hand. They now open System Info, where the Install button is. The GitHub
  link stays for installations something else manages: a container, Homebrew,
  winget or a service.

### Web UI

- **Filtering Completed by a category no longer hides the others.** The rail
  was built from the rows the page had just been handed, so a page filtered to
  one category contained nothing else and the only way back was to already
  know what existed. It now lists every category in the archive.
- **Each category shows how many jobs it holds,** on the queue and the
  archive. The counts follow the search, since that changes what is on offer,
  but not the category or status filters, so a number does not move as you
  select.
- **A recovery set being fetched is no longer flagged in yellow.** A few KB of
  PAR2 index against a several-hundred-megabyte download is the normal case.
  Skipping the set entirely still reads as notable, and a job with no set
  posted stays muted.
- **The Next job detail page can save a job's NZB and its individual output
  files again,** the NZB beside the job's other header actions and the
  download as the first action on each file's row. Downloads are a plain
  navigation, so the bytes stream straight to disk instead of through a blob
  the tab has to hold. Weaver's own directory marker files no longer appear in
  the list.
- **Provider rows show how many BODY fetches the lanes keep in flight,** as
  `x2`, `x8` and so on, or `seq` where pipelining proved not to pay, with the
  latency band it was derived from in the hover text.

### Access and browser security

- **A session bound to one origin is treated the same way on reads as on
  writes.** Same-origin navigations often omit the `Origin` header, so a
  session created on one port could appear authenticated to a page served from
  another port on the same host, and then be refused as soon as it tried to do
  anything. The page's own origin is now used where `Origin` is absent, and
  the interface treats a refusal like a missing login, so it offers the
  sign-in page instead of failing silently.
- **The NZBGet RPC endpoints accept browser extension origins.** Chrome,
  Firefox and Safari extensions can call `jsonrpc` and `xmlrpc` with an
  explicit API key. Cookie-bearing requests stay confined to the web origins
  configured for the installation.

### Metrics and API

- **The process says where its memory is.** `/metrics` and the diagnostics
  bundle now carry the process resident set on every platform, buffer pool
  in-use and total bytes per tier, the bytes lanes hold in flight, and how
  many jobs are eligible versus actually downloading.
- `historyPage` gains `categoryCounts`: every category in the archive with its
  row count.
- A new admin-only `checkForUpdates` mutation runs a release check on demand.

### Build and packaging

- **The Intel macOS artifact is built on an Intel runner.** It was the one
  cross-architecture build in the release, which matters more now that the
  binary carries an allocator whose page size is fixed when it is compiled.
- **The aarch64 musl build links again.** Ubuntu's aarch64 compiler enables
  outline atomics by default, which pulls in an object that needs a glibc-only
  symbol, and the allocator's probes read that as a platform without atomics.

### Dependencies

- `par2-rs` 0.10.6 and `par3-rs` 0.4.3, with `reedsolomon-rs` 0.4.7 beneath
  them.
- `lzma-turbo` 0.5.0 and `sevenz-turbo` 0.25.0, which move together:
  sevenz-turbo now takes its LZMA encoder from lzma-turbo.
