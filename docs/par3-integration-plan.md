# Weaver PAR3 integration plan

Implementation started on 2026-09-08 in `feature/par3-integration`, based on
local `release-0.12.0` at `6b079b40acea50e64df2cfce956dd0412963d906`.
This is an implementation record, not a claim that the entire pipeline is ready.

Implemented: the native operation contract, PAR3 carrier roles, completed-carrier
discovery on blocking workers, retained authenticated packet locations, and a
source publication adapter separating availability revisions from content
generations. PAR3-specific state is lazy. Engine allocations share a process-wide
256 MiB budget and 128 handles; each job uses one codec worker and the default
64 MiB retained ceiling. Source publication tables bound sources and ranges.
Official-fixture tests cover split headers/payloads, interior holes, duplicate
arrivals, evidence reuse, selective output staging, and typed cancellation.

Carrier scanning now uses detached tickets: one live carrier worker, bounded
job/candidate queues, retained ownership returned with the result, and stale
ticket rejection after a job is forgotten or recreated. Cancelled workers keep
their capacity until they return. Completion waits for outstanding discovery.
The coordinator is allocated only on PAR3 admission. Both engines return native
outcomes through the existing repair completion channel, with one format dispatch
per finished operation. PAR3 does not add another actor receive branch; only
PAR3 results box their larger retained state.

Authenticated sets now retain native repair sessions. Completed conventional
sources publish committed decoded placements, including files completed before
the first carrier; apparent filesystem length never fills an assembly hole.
Workers bind exact authenticated names and retain native evidence across
recovery-only merges. Bounded actor views preserve file-coordinate damage,
verified prefixes, matrix identities and per-cohort recovery requirements.
Queued publications and their retained ranges share a 16 MiB host budget with
these views. Pending source changes and worker errors hide old actor answers.
Dispatch rotates between jobs instead of draining one job's entire backlog.
Mutable Windows disk source snapshot costs still require tuning before the
read-free reassessment contract can be claimed on that platform.

Conventional writes and identity rebindings now withdraw prior coverage without
I/O. Reader snapshots and worker epochs fence old evidence, including an already
finished worker awaiting actor handback. Retired results keep their worker slot
until handback and preserve unrelated native evidence. Once writers drain, dirty
sources are republished through the same bounded queue; carriers also use only
committed assembly ranges. Source/error/dirty bookkeeping retains its own host
budget lease, and failed scans retain leases for any publication they installed.

Still pending: incremental decode-to-publication wiring,
repair/download gating, direct-volume adapters, positioned verification, embedded
archives, persistence, and product surfaces. Completed carriers are discovered
in production code, but PAR3 does not yet decide job completion or execute repairs.

Dependency baseline: PAR2 0.10.2, UnRAR 0.10.3, and registry Reed-Solomon 0.4.4.
The approved development pointer pins PAR3 and its separate arithmetic instance
to rarpar `54e51e83226767b2ff93ec56e2d3d59e1bdcc8ee`. Metadata comparison before
and after resolution confirms every existing dependency node is unchanged
except server core's new PAR3 edge: no PAR2 features, versions, or transitive
edges changed. This isolation is deliberate during the performance comparison.

Performance baselines and matched ARM64/x86-64 acceptance measurements below
remain required. Source/dependency identity and correctness tests alone do not
establish the 2% non-regression requirement.

First-slice validation on macOS ARM64: formatting and all-target/all-feature
workspace Clippy pass; the full locked Nextest sweep reports 3,768 passed and
13 existing skips; all three doctests pass. The fresh worktree required an
`npm ci` and web asset build for the embedded frontend, without manifest or
lockfile changes. Rustdoc must match the compiler build, including its
distribution, rather than only its displayed version number.

Discovery-slice validation: formatting and all-target/all-feature Clippy pass;
the locked workspace Nextest sweep reports 3,776 passed and 13 existing skips;
all three doctests pass. All 652 resolved dependency nodes are identical to the
first slice after normalizing the approved PAR3 Git revision change.

Worker-slice validation: all-target/all-feature Clippy passes; six targeted
worker/pipeline tests pass. The full locked workspace sweep passes all 3,782
tests with eight test workers and 13 existing skips. An earlier full-concurrency
run reported one leak in the existing disabled-script runner test; it passed
in isolation and in the full rerun. The strict 500 ms leak-failure policy remains
unchanged. This is not evidence of a PAR2 throughput result.

Shared-channel validation: 50 targeted PAR2/PAR3 regressions, the inline queue
payload-size regression, all-target/all-feature Clippy, and all three doctests
pass. The final isolated full sweep reports 3,783 passed and 13 existing skips.
A preceding sweep overlapped Cargo work and reported a leak in a pure NNTP
sniffer test; the final sweep ran without concurrent Cargo work and no leak
checks were relaxed. The cause of the intermittent leak reports is unconfirmed.

Retained-assessment validation: all 3,788 workspace tests pass with 13 existing
skips; formatting, all-target/all-feature Clippy and all three doctests pass.
New regressions cover late metadata after conventional sources, file-coordinate
damage, recovery-only evidence reuse, stale views, carrier replay after a new
generation, host budget rejection and round-robin dispatch. PAR2 performance
acceptance remains unmeasured.

Generation-fence validation: all 3,791 workspace tests pass with 13 existing
skips; formatting, all-target/all-feature Clippy and all three doctests pass.
The 24 targeted PAR3/source tests include open-reader withdrawal, a finished
worker rejected after a write, retirement of queued stale publications, and
pipeline re-verification after the existing write-invalidation hook.

## Decisions

- Integrate standalone PAR3 verification and repair, selective recovery
  downloads, direct extraction, and embedded ZIP/ZIP64/7z protection.
- Weaver never creates protection sets. Regeneration needed during embedded
  self-repair is allowed; creation features are excluded.
- Prefer PAR2 when both formats protect the same files. Use PAR3 when PAR2
  cannot finish; independent PAR3 sets proceed normally.
- Verified extraction may complete with a warning when only embedded
  protection packets remain incomplete. Do not regenerate parity solely for
  protection completeness.
- Require no reproducible PAR2 slowdown above 2% per core workload.
- The operator explicitly permits a pinned GitHub dependency during development.
  Use the signed rarpar review-fix revision above; do not use moving branches or
  external filesystem patches. Return to a published engine containing these
  APIs and fixes before integration is complete. Publication, deployment, and
  production operations are outside this plan.

## Architecture

Extend Weaver's existing `pipeline/repair` domain with a shared coordinator and
concrete format backends. Do not add a framework crate or invoke the rarpar CLI.

Introduce `RepairFormat`, a format-tagged `RepairSetKey`, and a private
`RepairBackend` contract for assessment, requirements, invalidation, execution,
and retained-memory reporting. Keep associated native evidence and analysis
types; dispatch through a concrete PAR2/PAR3 enum once per scheduled operation.
Do not box block operations or reduce native evidence to generic checksum flags.

The pipeline owns sessions and transfers one into a blocking worker for each
operation. Return the session with a generation-tagged result; stale results
cannot update newer state. Expose a narrow common assessment view containing
typed status, file-coordinate damage, verified coverage, planned output changes,
and format-specific recovery requirements. PAR3 requirements retain matrix,
cohort, and recovery-index identity; they cannot be collapsed into one deficit.

Distinguish missing data, incomplete metadata, insufficient recovery,
unsupported features, resource limits, source changes, cancellation, and I/O
failures. Preserve underlying error chains. Extraction consumes a borrowed
coverage/mutation view while existing PAR2 calculations stay unchanged beneath it.

Retain PAR2's `FileAccess` and cached encrypted-volume readers. Add a separate
PAR3 `SourceAccess` adapter over disk, committed assembly ranges, and
`HybridVolumeProvider`. Share resource ownership and coverage bookkeeping where
useful without adding another virtual call to PAR2 reads.

## Implementation slices

### 1. Baseline and coordinator

Record baseline source/dependency revisions, binary, fixtures, and measurements,
including current ZIP/direct-unpack behavior. Wrap PAR2 before adding PAR3 and
measure abstraction overhead separately. Preserve decoder checkpoint plans,
CRC/MD5 substitution, retained sessions, scan carry, pending repair assessment,
promotion, hole handling, sequential reads, encrypted reader reuse, the 64 MiB
repair-buffer default, existing settings, and completion behavior.

### 2. Discovery and acquisition

Add standalone classification and authenticated content recognition. Embedded
protection supplements the archive role rather than replacing it. Scan committed
carrier ranges incrementally with bounded split-packet frontiers and hole
resumption. Route late metadata and authenticated packets by set identity;
count unique recovery only by set, matrix, and index.

Extend existing acquisition to request metadata, map known packet ranges to NZB
articles using established offsets, and probe unknown carriers in bounded steps.
Names and sizes are discovery hints, never proof. Satisfy deficits per cohort,
deduplicate article requests across sets, and exhaust a finite probe/candidate
frontier before reporting unavailable recovery.

PAR2 owns the initial repair of overlapping protection. PAR3 becomes eligible
when available PAR2 metadata/recovery cannot finish, or PAR2 reports unsupported
or resource-limited execution. Cancellation and I/O retain distinct handling.
Serialize repairs touching overlapping sources. Never translate proofs between
formats.

### 3. Positioned verification

Allocate PAR3 state only after an authenticated binding. PAR2-only sources gain
no BLAKE3 hashing, payload copies, reads, or worker messages. After existing
consumers finish, move decoded buffers into bounded blocking verification work;
do not globally convert decoded storage to reference counting. Feed positioned
bytes after conventional write commitment or the direct-store publication
barrier. Keep hashing off the pipeline actor.

On queue or out-of-order budget exhaustion, leave affected extents unknown for
later `SourceAccess` verification without blocking unrelated downloads. Late
metadata triggers verification of already committed unknown extents once, not
speculative retention of every article. Admit sealed evidence only for the
matching layout, source, generation, and published coverage. yEnc CRC32 and PAR2
proofs cannot replace PAR3 fingerprints.

Separate availability revision from content generation. Filling a hole can keep
generation; rewriting bytes, withdrawing coverage, truncating, rebinding, or
changed materialization invalidates it. Duplicate rewrites invalidate unless
byte identity is established.

### 4. Repair, extraction, and embedded archives

Keep clean sources virtual and stage only files requiring reconstruction under
existing scratch/output policy. Wait for writes and demotion barriers. Reuse
extraction invalidation, cancellation, staging, and durable installation. Keep a
chase only when consumed bytes remain verified and repair cannot change them;
otherwise discard staged extraction and restart. Reconcile every installed file,
handle partial installation explicitly, and invalidate changed bindings. Do not
unconditionally reread or rewrite clean sources.

Locate embedded protection through existing archive boundary reads and bounded
format-derived probes, not full scans of every clean archive. Damaged/ambiguous
candidates may use a bounded fallback scan. Authenticate the protected layout
before recognizing an unprotected packet gap. Use `SelfRepairPlan`; exact
restoration requires a complete captured manifest. Otherwise explicitly replace
the carrier while preserving available authenticated packets, without requesting
extra parity solely for completeness. Validate protected bytes and container
structure, preserve offsets/member bytes/compression/ZIP footer duplication, and
track protected-data completeness separately from protection completeness.
Unsupported/ambiguous layouts fail explicitly; never silently strip protection.

### 5. Resources, restart, and product surfaces

Keep PAR2 limits unchanged. PAR3 lazily shares one process-wide 256 MiB engine
budget, a 64 MiB retained ceiling per session, and a 16 MiB aggregate input queue.
Include sessions in existing aggregate retained-session eviction accounting.
Account for queued buffer ownership without double-counting physical memory;
share handle leases and admit workers through existing post-processing capacity.
Do not multiply unrestricted pools. Keep the default Cauchy loss limit.

Persist bindings, validated carrier/acquisition progress, warnings, and versioned
evidence checkpoints with a trusted digest stored separately in job metadata.
Retain the durability/generation anchors needed to establish source continuity.
Do not serialize sessions/solver state. Replay only when the same bytes can be
established; otherwise reverify without unnecessary refetching. Support SQLite
and PostgreSQL without rewriting existing PAR2 records.

Use existing progress/history surfaces with format-aware additions. Keep PAR2
API fields and metrics compatible; never record PAR3 work under PAR2 names.
Update Weaver architecture documentation and operator guidance.

## Acceptance

Assert unchanged PAR2 hashing, reads, checkpoint behavior, and clean-output
writes, with zero PAR3 verification allocations/queued work for PAR2-only sources.
For PAR3 require zero protected-source reads on unchanged reassessment and
recovery-only merges, no rereads for admitted strong evidence, no clean-source
materialization, and enforced memory/queue/worker/handle/scratch bounds.

Cover conventional/virtual sources, damage/holes, late/out-of-order/duplicate
arrivals, stale workers, cancellation, eviction, restart, renames, multiple and
overlapping sets, both Cauchy fields, FFT/uneven cohorts, deduplication, Data-only
repair, packed tails, large block counts, selective articles, partial install,
and embedded ZIP/ZIP64/7z damage/protection/boundary/invalidation scenarios.
PAR3 fixtures retain official provenance; never hand-build packets.

Extend `cargo xtask perf` with baseline/candidate clean-download, verification,
selective-repair, repeated-assessment, direct/encrypted-volume, small-file,
large-block, multi-set, ZIP/RAR extraction, mixed-format starvation, and PAR3
workloads. Match optimized builds, worker limits, fixtures, and isolated outputs
on ARM64/x86-64. Use two warm-ups and ten alternating pairs, increasing to thirty
if inconclusive. Each core PAR2 elapsed-time ratio needs an upper 95% confidence
bound at most 1.02; averages cannot hide individual regressions. Record CPU, peak
memory, I/O, recovery downloads, and output hashes. Do not change existing services.

Run formatting, all-target Clippy, consumer regressions, full Nextest with
`--locked --no-fail-fast`, doctests, and API compatibility checks. Check PAR2
invariants after each shared-orchestration slice. Select minimal published
dependencies satisfying the contract, preserving PAR2 and crypto features and
auditing shared arithmetic/feature unification. Completion requires working
Weaver pipelines and measured non-regression evidence, not compilation alone.
