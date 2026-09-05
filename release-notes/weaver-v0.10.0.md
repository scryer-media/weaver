# Weaver 0.10.0 release notes

> These notes describe the net change from `weaver-v0.9.6` to `weaver-v0.10.0`.
> They describe the completed release rather than the order individual
> release-branch changes landed. No notes were published for 0.9.1 through
> 0.9.6; changes from those tags are not repeated here.

## Highlights

Weaver 0.10.0 is a download-path performance, extraction, and packaging
release. The NNTP client now pipelines session setup, orders TLS ciphers by
what the CPU accelerates, and decodes without the reallocations that a
many-article download used to pay per part. CRC composition across the whole
pipeline moved to one native carry-less-multiply combine, and the disk writer
groups contiguous articles into vectored writes and closes handles off its
owner thread. On the bench corpus these land as a measurable per-job CPU
reduction on top of the 0.9.0 baseline.

Direct-store RAR handling now repairs encrypted sets in place instead of
demoting them, and 7z direct unpack ships as an opt-in preview: a coverage-gated
chase that begins extracting a split 7z set while it downloads, parks on the
first damaged block, and resumes after PAR2 repair instead of starting over.

Weaver also gains a native desktop wrapper for macOS and Windows, a signed DMG
and MSI, save-time TLS diagnostics per server, SFV verification for jobs with no
PAR2 set, and a set of security hardening changes around delivery naming,
extraction resources, RSS egress, and destructive history operations.

## What changed

### NNTP client and download path

- **Correct message-id framing on the wire.** The owned and async lanes passed
  bare message ids to `BODY`; RFC 3977 reads an unbracketed argument as an
  article number, and real providers answered 430 for every article. The
  command encoder now brackets bare ids and a test pins the stored form.
- **Pipelined session setup.** On a server known to pipeline, `MODE READER`
  and the lane's first `GROUP` leave in one write and are answered in order.
  `AUTHINFO USER` and `AUTHINFO PASS` run serially first, because RFC 4643
  forbids pipelining them and some providers enforce it. Servers of unknown or
  negative capability keep the serial exchange.
- **A 430 keeps its lane.** A 430 is a complete, bodyless answer, not a
  transport fault. It no longer quits the TLS session, drops the rest of the
  leased batch, or blocks the server's pipelining proof. Only outcomes that
  damaged the socket dirty a batch now.
- **CPU-ordered TLS ciphers.** The ClientHello offers AES-GCM first where AES
  acceleration exists and ChaCha20 first otherwise, resolved once per process.
  rustls orders its suites accordingly; s2n keeps its AES-first policy and a
  ChaCha-first preference routes to rustls.
- **Backfill unlock on auth-disabled fill servers.** An auth-disabled fill
  server counts as exhausted for the backfill gate, so an article the other
  fill servers 430'd spills to backfill instead of requeueing against a
  deadline only the operator can resolve. Cooldowns and failure-ratio
  disables still wait because they heal on their own.
- **Per-server drain and owned-lane reclaim.** A connection error or failed
  stale ping drains only the failing server's idle sockets. Async-only leases
  (PAR2 recovery) reclaim exactly one idle owned permit on a server, and only
  when that server has none available, instead of resetting the whole
  owned-lane fleet per batch.
- **Decode without reallocation.** The fused decoder trims input to the
  reserved output batch and decodes a full-sized batch's tail through a small
  scratch buffer, so a truthful yEnc header never grows or shrinks its batch.
  An `output_grow_events` stat counts the exceptions.
- **One clock read per read turn.** The BODY budget is a deadline shared with
  the read timeout; only an actual throttle wait refreshes it.
- **Cheaper leases and queues.** Newsgroups are shared per file as one
  `Arc<[String]>`; leases borrow message ids by reference; the orchestrator
  ingests up to sixteen released results per turn before running dispatch; the
  RAR unlock plan lives in the download queue so a retry requeue no longer
  rebuilds both heaps; the ISP cap window is recomputed only when the clock
  leaves it.
- **Per-article events on their own channel.** `ServerAttempt`,
  `ArticleDownloaded`, `SegmentDecoded`, and `SegmentCommitted` travel on a
  dedicated segment-event channel that builds an event only when a listener
  exists. The GraphQL raw event streams and queue snapshot triggers merge both
  channels, so their output is unchanged. Order is kept within each channel,
  not across the job-level and per-article channels; no consumer sequences one
  against the other.

### CRC composition

- **One native CRC32 combine everywhere.** `crc32_combine` is now the
  zlib-style polynomial identity with a reusable per-length operator, and the
  polynomial multiply runs on PCLMULQDQ (x86-64) or PMULL (aarch64) with a
  Barrett reduction where the CPU has it. It is bit-identical to `crc-fast`
  for every length and to the par2-rs operator on well-formed input.
- The in-stream PAR2 grid collector, the completed-file checksum fold,
  direct-store run composition and member verification, repair read-back, and
  the settle-time PAR2 slice fold all use that one implementation instead of
  building a 32x32 GF(2) matrix per call. A combine that cost 9-75 µs now
  costs tens of nanoseconds, and the per-segment matrix build that showed at
  up to nine percent of on-CPU time in a many-article download is gone.

### Disk writer

- **Vectored writes per contiguous run.** Each run of adjacent articles in a
  batch is one seek and one `writev` loop, for decoded segment batches and
  direct-store raw fragments alike. A short or interrupted write reports the
  fully written prefix and retries the cut segment whole.
- **Handle close off the owner thread.** Handles leaving the cache go to a
  FIFO closer thread, so the flush macOS performs on last close no longer
  stalls every other file hashing to that owner. The completion path releases a
  handle only after the archive probe has read the file, so the probe never
  waits on the flush.
- Idle write handles are swept once per interval rather than after every
  batch.

### Direct-store RAR: encrypted repair in place

- **Encrypted sets repair without demoting.** The router now exposes its held
  cipher blocks and the virtual volume serves them as posted bytes, so PAR2
  verify and repair read past a hole in an encrypted member instead of reading
  nothing at exactly the offsets the repair needs. A decrypted run files its
  own CBC predecessor as a checkpoint so a run that begins after a hole
  re-encrypts from its own seed.
- **The repair scratch carries a partial article.** A covered run that stops
  inside an article, which an encrypted member's placed frontier always does
  before a hole, is verified up to its last article boundary and carried
  through unverified for the remainder. Previously that run was refused and
  every encrypted set needing repair demoted at the step meant to repair it.
- **End-to-end coverage.** Store-method scenarios now cover RAR4 `-p`, RAR5
  `-p`, and RAR5 `-hp`, each with a PAR2 repair variant that deletes the tail
  articles of an interior volume. The expected direct-set count in the corpus
  rises from 8 to 12.
- Applying PAR2 canonical identity no longer deletes non-RAR archive
  topologies. Retirement refuses non-RAR topologies outright and every
  topology removal is logged.

### 7z direct unpack (opt-in preview)

- **A coverage-gated chase extracts a split 7z set while it downloads.** A
  gated reader serves each part behind the set's coverage watermark, the chase
  is armed from the archive probe, consumed at extraction, and tainted by every
  repair rewrite. All codec chains are decoded in process; a 26-scenario
  oracle-generated codec matrix runs in e2e with the gate on.
- **Gate on first damage, resume after repair.** The first `Damaged` verdict
  anywhere in the set gates it: each part then serves only its contiguous
  grid-vouched intact prefix, so unreached damage parks the chase instead of
  racing the readahead. Clean sets never read the gate. A gated chase forces
  the authoritative PAR2 pass, a clean verdict lifts the gate, and the reader
  reopens a part whose file the repairer replaced by rename.
- **Right-sized memory and its own pool.** A chase reserves per pass from a
  chase-only pool, header-sized while listing and decoder-sized while decoding,
  with the need computed from the archive's own coder chains. Chases run on
  their own rayon pool with their own process-memory allowance, admission
  counted against actual worker occupancy, start/exit logging, a zombie
  warning, and a deadline on the consumption await that extracts conventionally
  instead of wedging the job.
- **Honest settlement and demotion.** The end-of-download settle takes part
  lengths from the assembly, not the file on disk; a strict pass at the
  completion check ends chases whose parts never arrived and requires the
  download drain to be current. Each demotion is counted once under its own
  reason (`repair_rewrote`, `repair_failed`, `gated_stall`, and others), and a
  part with no grid claim can be vouched through a repair by the file-level
  verdict.
- **Off by default.** Enable with `[direct_unpack] enabled = true` in the
  config or `WEAVER_DIRECT_UNPACK=on`; the environment wins over config, the
  same precedence as direct-store. Turning the default on is a later release
  decision.

### Servers: save-time TLS diagnostics

- The save-time connectivity probe handshakes twice on TLS servers, once with
  Weaver's CPU-preferred cipher family first and once with the opposite family
  first, and records whether the server follows the client's order.
- The outcome is stored per server in a new `server_tls_diagnostics` table
  (migration 0044, cascades on server delete, included in backups), exposed on
  `Server`, `ServerDetails`, and `TestConnectionResult` as `tlsCipherSuite` and
  `tlsHonorsClientCipherOrder`, and shown in the TLS column of the Servers page
  and the test-connection result line. A plaintext probe clears the row; saving
  an inactive server leaves the stored facts untouched.

### Verification and delivery naming

- **SFV verification for jobs without PAR2.** A job that carries no recovery
  data, which for uuencode means no per-article checksum at all, is now judged
  by its `.sfv` listing when one is posted. A mismatch is terminal because there
  is nothing to repair from; a file absent from every listing is left
  unverified rather than treated as suspect. Obfuscated SFV listings are
  detected.
- **Delivery naming controls.** The srrdb lookup used to rename obfuscated
  members, previously config-file only and off by default, is now exposed as
  `enableSrrdbLookup` on General settings and in the API. Member checksums sent
  to srrdb are keyed by path, responses are bounded, and long queue names are
  truncated in the UI.
- **Unacceptable extensions.** Post-processing settings gain
  `unacceptableExtensions`; members with a listed extension are refused during
  extraction and RAR scheduling. Omitting the field on update preserves the
  existing list, an empty list disables filtering, and `null` is refused.

### Security and resource hardening

- **Admin scope for destructive history operations.** History mutations that
  also remove completed output (`deleteFiles: true`) require an administrator;
  history-only removal remains available to control callers.
- **RSS egress controls.** Feeds may use local, private, link-local, or
  container-network addresses by default; set
  `WEAVER_RSS_ALLOW_PRIVATE_NETWORK=false` to limit fetching to public egress.
  Feed Basic Auth credentials are sent only to requests whose scheme, host, and
  effective port exactly match the configured feed URL. Decompressed feed bodies
  are capped at 16 MiB; the NZB response limit remains configurable with
  `WEAVER_NZB_DECOMPRESSED_LIMIT_BYTES`.
- **Extraction resource limits honour cgroups.** The system probe reads the
  process's cgroup memory limit through nested v2 and v1 hierarchies, not just
  the namespace root, so a systemd or container limit constrains extraction
  memory budgets.
- **Windows reparse points.** Output-path safety checks now read the raw
  reparse attribute, so junctions and volume mount points are rejected like
  symlinks.
- **uuencode spool bounds.** Disk-backed uu segments waiting for their missing
  prefix are capped by bytes, segment count, and free disk, and admission is
  capped under download pressure. Released results are now accounted in decode
  pressure. New metrics: `weaver_pipeline_write_pending_bytes`,
  `weaver_pipeline_uu_spooled_bytes`, `weaver_pipeline_uu_spooled_segments`.

### Native desktop wrapper and packaging

- **Weaver is a desktop app on macOS and Windows.** `weaver-tray` becomes a
  WKWebView (macOS) or WebView2 (Windows) app window while the server stays a
  plain web server. macOS gets a menu-bar status item with a queue popover,
  open-panel support for NZB uploads, and a real main menu (Cmd+W closes the
  window, Cmd+Q prompts). Windows gets a DPI-scaled window with themed chrome,
  a tray icon with a hover flyout of the top queue items on Windows 10, and
  Ctrl+W to hide.
- **Readiness probe requires Weaver's own page.** The wrapper previously
  accepted any HTTP 200 on its port as a running server and would load a
  stranger's page. It now requires the entry document every server page
  carries and reports a foreign listener as such instead of timing out.
- **New artifacts.** `weaver-darwin-<arch>.dmg` is a signed drag-to-Applications
  image built with a hash-pinned dmgbuild and committed icon and background art
  with a reproducing generator script. `weaver-windows-<arch>.msi` continues.
  Portable `.tar.gz` and `.zip` archives remain for headless and service
  installs. Release disk images are attested and ship with bundle SBOMs.
- `LICENSE` and `THIRD_PARTY_NOTICES` gain the WebView2Loader static-link
  exception.

### CI and release validation

- A release tag that disagrees with the workspace version in `Cargo.toml` is
  refused before any artifact is built, with fixtures for a mismatch, a missing
  manifest, and a manifest without a version.
- A deploy-workflow contract check guards the job graph, and the e2e release
  gate is enforced from the harness.
- The e2e corpus is content-addressed: artifacts are cached by content and
  scenarios are stamped, ending stale-artifact reuse; fixtures whose writers
  draw a per-archive salt carry salted ledger entries; pre-seeded NNTP images are
  pinned only after capture. `task functional` runs the Functional SQLite phase
  alone.

### Dependencies

- `unrar-rs` 0.7.0: `RarVolumeFacts::volume_number` is a contract, and the
  extractor no longer reserves a member's declared expanded size before
  extraction establishes how much output is available.
- `sevenz-rust2` 0.22 gains the `lz4` feature for the direct-unpack codec
  matrix.

## Upgrade notes

- **Database migration 0044** adds `server_tls_diagnostics` on both SQLite and
  PostgreSQL. It runs automatically and is included in backups. No manual
  conversion is required.
- **7z direct unpack is off.** Nothing changes for 7z sets unless
  `[direct_unpack] enabled = true` or `WEAVER_DIRECT_UNPACK=on` is set. Direct
  RAR storage remains on by default as in 0.9.0.
- **History deletion with `deleteFiles: true` now requires admin scope.**
  Integrations that delete completed output through the API with a control-scope
  key must move to an admin key or drop `deleteFiles`.
- **RSS feeds on private networks keep working by default.** Operators who want
  public-only egress should set `WEAVER_RSS_ALLOW_PRIVATE_NETWORK=false`.
- **Bracketed message ids.** Providers that previously answered 430 for every
  article on the owned and async lanes will now serve them. No configuration
  change is needed.
- **macOS desktop build.** Open the DMG and drag Weaver to Applications. Images
  are signed but not every release is notarized, so the first launch may need a
  one-time right-click → Open.

## GraphQL and integration compatibility

The schema change is additive. Strongly typed clients should regenerate and
review the following.

### Additions

- `Server.tlsCipherSuite: String` and `Server.tlsHonorsClientCipherOrder:
  Boolean`, also on `ServerDetails` and `TestConnectionResult`.
- `GeneralSettings.enableSrrdbLookup: Boolean!` and the matching optional
  input field.
- `PostProcessingSettingsGql.unacceptableExtensions: [String!]!` and the matching
  optional input field. Omission preserves the current list; an empty list
  disables filtering; `null` is refused.

### Changed semantics

- History deletion mutations with `deleteFiles: true` require admin scope.
  History-only deletion is unchanged.
- The raw pipeline event streams are unchanged in content but are now merged
  from two channels; ordering is guaranteed within the job-level and
  per-article channels, not between them.

## Reliability fixes included in this release

- Prevented a foreign listener on the desktop wrapper's port from being
  treated as a running Weaver.
- Prevented every article from failing with 430 on providers that enforce
  RFC 3977 message-id framing.
- Prevented a provider that enforces RFC 4643 from failing the pipelined
  session setup with no fallback.
- Prevented a 430 from quitting the TLS session and dropping the rest of a
  leased batch.
- Prevented encrypted direct-store sets from demoting at the step meant to
  repair them.
- Prevented a chased 7z part from resuming over a moved-aside damaged file
  after repair installed the repaired one.
- Prevented a chase gated on PAR2 damage from finalizing through the clean
  strong-decode skip without ever running the authoritative verdict.
- Prevented a single chase from reserving the entire decoder-memory ceiling and
  single-filing every other chase behind it.
- Prevented PAR2 canonical identity from silently deleting 7z and other non-RAR
  topologies.
- Prevented a strict settle from ending a chase mid-download on a stale drain
  stamp.
- Prevented a watermark past a declared length from tripping a debug assert
  that killed the pipeline task.
- Prevented the disk owner's last-close flush from stalling other files and the
  orchestrator's archive probe.
- Prevented the desktop wrapper's port probe, the release tag validator, and
  the post-processing script-start test from depending on host state.
- Corrected srrdb member checksum keying, bounded srrdb responses, and queue
  name truncation.
- Corrected decode-pressure accounting for released results.
- Corrected Windows reparse-point detection for junctions and mount points.
