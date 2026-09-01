# Weaver 0.9.0 release notes

> Draft — these notes describe the net change from `weaver-v0.8.3` to
> `weaver-v0.9.0`. They intentionally describe the completed release rather
> than the order individual release-branch changes landed.

## Highlights

Weaver 0.9.0 is a substantial download-pipeline, repair, and operations release.
Direct RAR storage is now enabled by default, clean downloads can be verified
from CRC evidence gathered while decoding, and PAR2 repair is aware of multiple
independent recovery sets. The repair tail reuses verified state and rereads
only rewritten slices where possible instead of routinely walking complete
archives again.

The release also makes queue dispatch hot-first, adds uuencode and XZ support,
streams large NZB documents through parsing and persistence, introduces a safer
first-run setup experience, and expands system, pipeline, NNTP, and database
observability. A new signed end-to-end corpus exercises the same RAR, PAR2,
network-chaos, restart, SQLite, and PostgreSQL behavior used to stabilize this
release.

## What changed

### Faster direct RAR downloads with safer fallback

- **Direct-store routing is enabled by default.** Eligible store-method RAR
  members are written directly to their final output instead of first
  materializing every source volume. Set `WEAVER_RAR_DIRECT_STORE=off` to use
  the conventional path.
- Obfuscated and multi-volume RAR sets can be admitted from observed archive
  identity and PAR2 metadata rather than filename shape alone. Probe work is
  ordered early enough to establish volume identity before ordinary payload
  dispatch fans out.
- The direct-store hold scratch ceiling now defaults to 1 GiB. The existing
  `WEAVER_RAR_DIRECT_STORE_SCRATCH_CEILING_BYTES` environment override remains
  available for operators who need a different ceiling.
- Scratch holds are compacted before ceiling-driven demotion. When direct
  routing does demote, Weaver materializes bytes it can still prove locally,
  hands the triggering decoded article to the conventional writer exactly
  once, and refetches only data it cannot verify from routed or held coverage.
- Demotion settlement now keeps PAR2 behind conventional durability. Repair
  cannot inspect a half-reset volume while decoded data is still moving from
  direct routing into the conventional assembly path.
- Direct-store recovery preserves identity, placement, accounting, and restart
  invariants across malformed chains, swapped volumes, sparse-file failures,
  checksum failures, and scratch-ceiling fallback.

### Multi-set PAR2 verification and selective repair

- **Jobs may now contain multiple independent PAR2 recovery sets.** Discovery,
  file binding, promotion, verification, repair, and completion settlement are
  tracked per set, so one clean or repaired set cannot accidentally settle an
  unrelated archive.
- The in-stream PAR2 grid combines block CRC evidence gathered during article
  decoding across sets with different slice boundaries. A clean set can settle
  without a second whole-file read when the collected evidence covers its
  complete description.
- Quick-digest, strong-decode, grid, and authoritative verification now converge
  through one typed settlement gate. Multi-set jobs therefore retain the
  verification source that actually vouched for each set.
- Repair carries scan state through the one-shot repairer flow and seeds known
  grid evidence into verification. The post-repair pass selectively rereads
  rewritten slices and keeps strict whole-file verification for changes that
  actually populate canonical paths.
- PAR2 placement normalization handles renamed, swapped, and renumbered RAR
  volumes without treating a clean placement-only result as damage. Repaired
  canonical volumes become authoritative before RAR topology is rebuilt, so a
  stale `.duplicateN` alias cannot displace repaired bytes during extraction.
- Opaque files with valid PAR2 magic can bootstrap metadata and bind obfuscated
  payloads. Incomplete optional recovery carriers now exhaust bounded prefix
  discovery instead of holding an otherwise complete job open indefinitely.
- Damage analysis waits for the job's downloads to drain, while promoted
  completion-critical recovery remains globally more important than ordinary
  High-priority download work.

### Hot-first queue scheduling and clearer activity

- **New NNTP lanes are concentrated on the best-ranked job.** Weaver fills the
  hot job first and uses a bounded fallback window only when that job cannot
  lease more work, avoiding the previous spread of tiny transfers across a
  large same-priority queue.
- Completion-critical PAR2 recovery outranks all user priorities. Among
  ordinary jobs, configured priority and FIFO order select the hot job, and a
  live priority change takes effect on the next available lane without
  cancelling valid in-flight batches.
- Per-job queues remain critical-first, so returned lanes naturally serve the
  hot job's recovery articles before ordinary payload work.
- Queue presentation now distinguishes lifecycle state from current pipeline
  ownership. Jobs with no live download, decode, or buffered-write activity are
  shown as queued rather than retaining a stale `Downloading` badge and rate.
- The default queue order ranks actively transferring jobs ahead of queued jobs
  and uses live transfer rate within that presentation. Virtualized queue rows
  and progress views are more stable during rapid updates.

### Streaming NZB ingestion and broader decoding support

- **NZB XML is parsed incrementally from a bounded reader.** Upload staging and
  persisted-NZB ingestion parse, hash, and zstd-compress decoded XML in one pass
  instead of retaining an extra complete decompressed document.
- The obsolete 100,000-segments-per-file limit is removed. The aggregate limits
  remain in force: 128 MiB of XML, 100,000 files, 2,000,000 accepted segments,
  16 TiB of declared data, and a 32 MiB sanity ceiling for one segment.
- Native uuencode article decoding is supported alongside yEnc, including
  mixed posts, preamble/tail variation, missing-segment handling, restart, and
  identity behavior.
- XZ input and nested XZ members are supported. Selective nested extraction now
  unpacks compressed siblings such as `.nfo.xz` without dropping ordinary
  members delivered beside them in the same RAR set.
- RAR recovery volumes, SFV verification for PAR2-less jobs, and safer nested
  archive handling broaden recovery without weakening extraction containment
  or output-budget enforcement.
- Delivered obfuscated members can be renamed from verified archive and PAR2
  identity, including a fallback metadata lookup when the local evidence is not
  sufficient.

### Setup, server security, and upgrade behavior

- **Fresh native installs bind to `127.0.0.1` by default.** The first-run wizard
  establishes access policy and allows the bind address to be widened from the
  local UI without editing a service file by hand.
- Existing pre-0.9 installations retain their historical wide bind when they
  never stored an explicit value. This compatibility shim avoids silently
  making an existing remote installation unreachable during upgrade.
- The setup flow is container-aware, can coordinate a self-restart when the
  installation supports it, and explains externally managed configuration when
  the browser cannot safely change it.
- NNTP server host input accepts common `https://`, `nntp://`, and `nntps://`
  prefixes and stores the normalized host. Connection testing can offer explicit
  adoption of a presented certificate when TLS is valid except for a hostname
  mismatch; the adopted certificate is pinned to that server configuration.
- Browser access to the metrics endpoint follows the configured browser auth
  policy, and TLS, credential encryption, egress, and startup checks received
  additional hardening.
- Homebrew upgrades preserve the existing data directory, service environment,
  encryption-key source, and pre-0.9 bind compatibility behavior.

### Post-processing is smaller and script-centered

- Post-processing now uses one operator-selected script directory, a global
  ordered script list, optional per-category lists, and manifest-validated
  script options.
- Execution concurrency, termination grace, and Python, PowerShell, and batch
  interpreter selection remain configurable. Completed jobs can rerun their
  configured scripts, and active post-processing can be cancelled.
- The former extension discovery, revision approval, profile, plan, artifact,
  attempt, queue-management, diagnostic, and webhook-oriented GraphQL surfaces
  are removed. This is an intentional API simplification; integrations using
  those experimental surfaces must migrate to the script-list API described
  below.
- Post-processing execution and log persistence use fewer polling and database
  round trips, and run lookup indexes are added automatically during migration.

### Observability and database performance

- A new System Information page reports platform, runtime, decoder tier, and
  relevant resource information for diagnostics.
- Metrics are descriptor-driven and include per-server transfer, job lifecycle,
  pipeline, decode, PAR2 grid, repair read-split, database wait/execution, and
  process telemetry. A Grafana overview dashboard and Prometheus alert rules are
  included in `contrib/`.
- Pipeline timelines and phase progress expose active verification, repair,
  extraction, and finalization more accurately, while high-frequency snapshot
  and unverified-CRC logging is debounced.
- SQLite pure reads are moved off the single writer lane. PostgreSQL combines
  more hot-path persistence work into fewer round trips and uses
  `synchronous_commit=off` for Weaver sessions to match the application's
  existing SQLite durability posture.
- Failed-byte and terminal job accounting is derived from authoritative segment
  and claim state, reducing stale or double-counted progress after retries,
  foreign-post detection, cancellation, and completion races.

### Packaging, CI, and release validation

- Release packaging is standardized on portable artifacts for Linux, macOS,
  and Windows across x86-64 and ARM64, with platform-specific validation and
  signed release metadata.
- macOS releases add a branded `weaver-darwin-<arch>.dmg` containing a
  Weaver.app desktop bundle, alongside the existing portable tarball. Each
  image is mounted and checked in CI and ships with its own SBOM. Images are
  signed; when a release is not notarized, the first launch needs a one-time
  right-click → **Open** to satisfy Gatekeeper.
- Windows startup, notification-area detection, resource embedding, package
  validation, GraphQL version checks, and native toolchain handling are
  hardened.
- Release-candidate tags publish through the normal release workflow. Tag builds
  use the shared cache configuration without introducing cloud benchmark hosts
  into CI.
- The repository now includes a signed, reproducible E2E corpus and harness
  covering SQLite and PostgreSQL functional runs, NNTP/TCP chaos, container and
  datastore restart, browser flows, archive oracles, multi-server fallback, and
  the PAR2/direct-store cases observed during 0.9 stabilization.

## Upgrade notes

- **Direct-store now defaults on.** To retain the conventional source-volume
  path, set `WEAVER_RAR_DIRECT_STORE=off` before starting 0.9.0. The default
  scratch ceiling is 1 GiB and can be overridden with
  `WEAVER_RAR_DIRECT_STORE_SCRATCH_CEILING_BYTES`.
- **Fresh installs are loopback-only by default.** Existing databases from
  0.8.3 and earlier preserve the former implicit `0.0.0.0` bind unless an
  explicit bind was already stored. Review the Security page after upgrade if
  you want to narrow an existing installation.
- Database migrations are automatic for completed-file hash provenance,
  post-processing lookup indexes and script settings, and adopted NNTP
  hostname-mismatch certificates. No manual database conversion is required.
- The old post-processing extension/profile/revision API is not compatible with
  0.9.0. Recreate post-processing policy as a script directory plus global or
  per-category script lists.
- Strongly typed GraphQL clients must regenerate against the 0.9.0 schema and
  review the breaking and dangerous changes enumerated below.
- Release artifacts are portable packages. Operators using Docker or Homebrew
  should continue upgrading through those channels; existing Homebrew service
  state is preserved.

## GraphQL and integration compatibility

### Removed types

- `E2EpostProcessingSeed`
- `JSON`
- `PostProcessingAdapterInput`
- `PostProcessingArtifact`
- `PostProcessingAttempt`
- `PostProcessingDiagnosticInput`
- `PostProcessingDiagnosticOutputLine`
- `PostProcessingDiagnosticResult`
- `PostProcessingExtensionRevision`
- `PostProcessingJobPlan`
- `PostProcessingLogChunkGql`
- `PostProcessingLogPageGql`
- `PostProcessingOnFailureInput`
- `PostProcessingOptionInput`
- `PostProcessingOptionValueKind`
- `PostProcessingOutcomeImpactInput`
- `PostProcessingProfile`
- `PostProcessingProfileInput`
- `PostProcessingProfileStepInput`
- `PostProcessingRerunInput`
- `PostProcessingRerunModeInput`
- `PostProcessingRun`
- `PostProcessingRunWhenInput`
- `PostProcessingSelectionInput`
- `PostProcessingSelectionModeInput`

### Removed or changed fields and arguments

- `Metrics.hotDispatchWarmupComplete` was removed.
- `Metrics.hotDispatchSpilloverBlockedWarmupTotal` was removed.
- `Metrics.hotDispatchSpilloverBlockedRecentExpansionHelpedTotal` was removed.
- `MutationRoot.updatePostProcessingSettings` was removed; use
  `setPostProcessingSettings`.
- `MutationRoot.discoverPostProcessingExtensions` was removed.
- `MutationRoot.runPostProcessingDiagnostic` was removed.
- `MutationRoot.approvePostProcessingRevision` was removed.
- `MutationRoot.disablePostProcessingRevision` was removed.
- `MutationRoot.revokePostProcessingRevision` was removed.
- `MutationRoot.savePostProcessingProfile` was removed.
- `MutationRoot.deletePostProcessingProfile` was removed.
- `MutationRoot.assignGlobalPostProcessingProfile` was removed.
- `MutationRoot.assignCategoryPostProcessingProfile` was removed.
- `MutationRoot.setJobPostProcessingSelection` was removed.
- `MutationRoot.seedE2EPostProcessingRuns` was removed.
- `MutationRoot.pausePostProcessingQueue` was removed.
- `MutationRoot.resumePostProcessingQueue` was removed.
- `MutationRoot.reorderPostProcessingQueue` was removed.
- `MutationRoot.rerunPostProcessing(input:)` was removed. The replacement is
  `rerunPostProcessing(jobId: Int!): Boolean!`.
- `PostProcessingSettingsGql.discoveryEnabled` was removed.
- `PostProcessingSettingsGql.webhooksEnabled` was removed.
- `PostProcessingSettingsGql.allowedRoots` was removed.
- `PostProcessingSettingsInput.discoveryEnabled` was removed.
- `PostProcessingSettingsInput.webhooksEnabled` was removed.
- `PostProcessingSettingsInput.allowedRoots` was removed.
- `QueryRoot.postProcessingRevisions` was removed.
- `QueryRoot.postProcessingProfiles` was removed.
- `QueryRoot.postProcessingJobPlan` was removed.
- `QueryRoot.postProcessingRuns` was removed.
- `QueryRoot.postProcessingQueue` was removed.
- `QueryRoot.postProcessingRun` was removed.
- `QueryRoot.postProcessingAttempts` was removed.
- `QueryRoot.postProcessingArtifacts` was removed.
- `QueryRoot.postProcessingLogs` was removed.
- `SubmitNzbInput.postProcessing` was removed.
- `SubmitStagedNzbsInput.postProcessing` was removed.

### Additions requiring client review

- `QueueItemState.FETCHING_REPAIR_DATA` was added.
- `QueueItemState.FINALIZING_DOWNLOAD` was added.
- Optional `ServerInput.tlsNameMismatchCertificateDerBase64` was added.

## Reliability fixes included in this release

- Prevented optional incomplete PAR2 carriers from wedging otherwise complete
  jobs with no work left to dispatch.
- Prevented PAR2 from observing direct-store demotion between assembly reset and
  conventional persistence.
- Prevented repaired canonical RAR volumes from being replaced in topology by a
  damaged `.duplicateN` alias.
- Rebuilt repaired RAR member topology before extraction and retired stale alias
  sets that could otherwise fail a clean job.
- Preserved ordinary siblings while selectively unpacking nested compressed
  members.
- Prevented placement-only PAR2 normalization from being reported as damage or
  from triggering unnecessary repair.
- Prevented redundant whole-set post-repair verification when only a damaged
  alias was quarantined and canonical bytes were unchanged.
- Preserved segment ownership, byte accounting, grid invalidation, and retry
  lineage when direct routing hands an article to conventional materialization.
- Corrected queue preemption after live priority changes and ensured recovery
  work can interrupt ordinary jobs without cancelling valid in-flight batches.
- Corrected stale active/download-rate presentation after a job loses all live
  pipeline ownership.
- Corrected RAR refresh gaps, partial quick-verification carry, repaired-file
  identity, terminal claim settlement, and failed-byte accounting across
  retries and restarts.
- Hardened release, Windows, Homebrew, benchmark, Docker, GraphQL, and E2E gates
  so public artifacts are reproducible and release validation fails closed.
