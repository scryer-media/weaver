# Weaver PAR3 integration plan

Implementation started on 2026-09-08 in `feature/par3-integration`, based on
local `release-0.12.0` at `6b079b40acea50e64df2cfce956dd0412963d906`.
This is an implementation record, not a claim that the entire pipeline is ready.

## Functional MVP completion

The operator requested complete case coverage before further optimization on
2026-09-09. The MVP must handle the agreed standalone and embedded formats,
including explicit safe failure for insufficient recovery, unsupported native
features, changed sources and exhausted resources. It must not silently narrow
coverage to make the positive scenarios pass.

Remaining correctness gates:

- [x] Large interleaved sets pass clean and damaged native cases under a documented,
  enforced shared memory budget; retain the enabled 65,538-block regressions.
- [ ] Embedded ZIP, ZIP64 and 7z discovery, protected-data repair and extraction
  preserve container structure, member bytes and authenticated packet gaps.
- [ ] Renamed/obfuscated sources and overlapping source aliases bind consistently;
  conflicting authenticated descriptions fail without delivering corrupt output.
- [ ] Concurrent jobs, eviction, cancellation during native work and partial
  installations enforce shared limits and preserve verified output safely.
- [ ] SQLite and PostgreSQL restart scenarios preserve required bindings and
  acquisition state, using fresh verification wherever continuity is unproven.
- [ ] API/history/progress/metrics distinguish native PAR3 work accurately while
  preserving existing PAR2 contracts.
- [ ] The full native matrix and Rust/consumer sweeps pass on the supported
  platforms, with explicit records for any unavailable validation environment.

Conservative rereads, fresh restart verification and extraction restart remain
acceptable interim implementations when they preserve correctness and resource
bounds. Positioned hashing, evidence replay and faster repair are optimization
work; they cannot substitute for the gates above. Preserve the existing PAR2
non-regression requirement and measured baseline throughout this work.

## Implementation record

Implemented: the native operation contract, PAR3 carrier roles, completed-carrier
discovery on blocking workers, retained authenticated packet locations, and a
source publication adapter separating availability revisions from content
generations. PAR3-specific state is lazy. Engine allocations share a process-wide
256 MiB budget and 128 handles; each job uses one codec worker and each native
set has the approved 128 MiB retained ceiling. Source publication tables bound
sources and ranges.
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
Windows publications establish a strong generation once, then check file identity
and change time through budgeted handles. They retain no sharing lock that could
block a later article write. Native Windows runtime validation remains required.

Conventional writes and identity rebindings now withdraw prior coverage without
I/O. Reader snapshots and worker epochs fence old evidence, including an already
finished worker awaiting actor handback. Retired results keep their worker slot
until handback and preserve unrelated native evidence. Once writers drain, dirty
sources are republished through the same bounded queue; carriers also use only
committed assembly ranges. Source/error/dirty bookkeeping retains its own host
budget lease, and failed scans retain leases for any publication they installed.
Repair requests reserve output-path memory before dispatch and retain that lease
through successful or partial-error handback and database reconciliation. Repair
requests and source publications use the same aggregate pending-work ceiling.

Conventional PAR3 jobs now enter a completion gate and execute native staged
repair on the shared completion transport. The gate requests one recovery article
at a time and reassesses native cohort requirements without trusting filename
capacity hints. Verified installations reconcile assembly and persistent file
completion; partial installation errors remain failures. No PAR3 digest is stored
as PAR2 MD5. The article-hole pipeline regression restores the missing bytes and
checks that a clean neighboring file was not rewritten.

Carrier probes retain a budgeted article frontier. Known decoded placements guide
missing-offset requests; unknown positions use finite ordinal probes. Scanning
can authenticate packets beyond a hole and revisit the unfinished packet after
arrival. The native missing-article scenario repairs using only the first article
of a two-article recovery volume; its remaining protection is not downloaded.

PAR3 can now read direct-store volume images, including encrypted members,
without materializing clean archives. One bounded reader per job retains the
cipher frontier; backing snapshots fence shared partial files before writes.
Metadata, held-file pins, temporary buffers and open readers carry resource
leases. Direct finalization waits for PAR3 verification. Live direct sets now
receive verified repair outputs through bounded readback tickets, including
multiple damaged volumes and encrypted members. Archive checksum failures that
demote before native repair can run still cross the reconstruction barrier.

Repair read-back now has a codec-independent file-range boundary that preserves
I/O errors and can return bounded stripes with their CBC edge bytes. The PAR2
wrapper still supplies all of a volume's rewritten ranges to its existing
router call. Whole-volume confirmation now checks actual `(start, end)` coverage;
an overlapping range cannot inflate coverage over a hole or a missing tail.
This fix uses the existing unreleased workspace version, 0.11.3.
Validation: formatting, all-target/all-feature workspace Clippy, all 3,807
workspace Nextest tests (13 existing skips), and all three doctests pass.

The router now accepts replacement batches for one volume and defers integrity
gates until the last batch. Pending batches block finalization and checkpoint
recreation; foreign-volume, empty closing and premature whole-volume calls are
refused. Regressions cover plain/encrypted parts and a wholly missing final
volume whose headers span several arrivals. PAR2 keeps its one-call repair
wrapper. This batch-primitive slice preceded the PAR3 read-back scheduler,
installation reconciliation and memory accounting described below.
Batch-slice validation: all 390 direct-store tests, the full 3,813-test workspace
sweep (13 existing skips), all three doctests, formatting and workspace Clippy
pass. Failed closing drains also keep checkpoints fenced until router retirement.

Direct placement now separates writing/coverage admission from demotion policy.
The conventional wrapper still demotes on sparse or destination-write failure;
the repair-facing boundary returns the original I/O error so its caller can
preserve verified materialized output. Failure regressions keep both that output
and clean virtual coverage intact, without claiming failed writes or launching
reconstruction. PAR3 readback now uses this checked placement boundary.
Placement-slice validation: all 3,815 workspace Nextest tests (13 existing skips),
all three doctests, formatting and all-target/all-feature workspace Clippy pass.
It continues to use the existing unreleased 0.11.3 workspace version.

PAR3 installation now captures authenticated output lengths and backing snapshots
on its native worker, then routes live plain direct volumes in 256 KiB stripes.
Each stripe carries a host-memory lease through placement and uses the shared
handle budget. The existing PAR3 worker tickets yield between stripes and keep
assessments/finalization fenced until installation settles. Checkpoints retire
before replacement; whole-volume CRC composition is replaced only after all
stripes are placed, followed by cached layout facts and a coverage barrier.
Native verified output survives routing or placement failure. Repaired direct
files republish virtual images and do not acquire conventional completed-file
rows. Their decoded lengths come from committed coverage, never assembly's
encoded progress count. This continues the unreleased 0.11.3 workspace version.
Readback validation: all 3,820 workspace Nextest tests pass (13 existing skips),
with all 21 native PAR3 boundary tests rerun after the final error-preservation
adjustment. All 22 native PAR3 E2E cases and 54 existing PAR2 archive E2E cases
pass. The missing plain RAR test now requires direct finalization without demotion.
Formatting, all-target/all-feature workspace Clippy and three doctests pass.

Encrypted readback captures all cross-volume CBC edges before the first stripe
can rewrite a shared partial. Verified installed neighbours take precedence over
old virtual sources. At most 4,096 edge requests share a 1 MiB host reservation;
the router refuses excess requests before building the list. Same-volume CBC
edges accompany each stripe under its existing memory/handle lease. An edge
hole or changed source refuses before placement, and a failed preflight cannot
be resumed as though its drained requests succeeded. The native encrypted
missing-article scenario now requires direct finalization without demotion.
Encrypted-readback validation: all 3,824 workspace Nextest tests pass (13 existing
skips), including all 401 direct-store/readback regressions. Formatting,
all-target/all-feature workspace Clippy, all three doctests, the 22 existing
native PAR3 scenarios and 54 PAR2 archive scenarios pass. This remains within
the existing unreleased 0.11.3 version.

Terminal delivery accounting now accepts current, authenticated PAR3 verdicts
for bound protected source identities. It excludes unrelated files and recovery
carriers, and withdraws those verdicts while a source is invalidated or work is
pending. This fixes a completed conventional repair retaining recovered article
failures in history. Successful native repairs also emit the shared repair
completion event and lifecycle metric. All 22 native PAR3 E2E scenarios now check
history health and failed bytes; successful delivery requires 1000/1000 health
and zero failed bytes. The terminal settlement and stale-evidence regressions
pass. Formatting, workspace Clippy, three doctests and all 54 PAR2 archive E2E
cases pass. The initial full sweep reported a process-handle leak after the
disabled-script test passed its assertions. All 13 post-processing tests passed
in isolation, and the full confirmation sweep passed all 3,825 tests with 13
existing skips and unchanged leak detection. No assertion or timeout was weakened.
This continues the existing unreleased 0.11.3 version.

Multi-volume direct repair now uses ordered set-wide replacement transactions.
Checksum checks and durable coverage stay fenced between stripes and volumes;
failed or incomplete replacement cannot reopen them. CBC preflight uses known
archive part geometry, so an adjacent repaired volume supplies cipher edges that
never reached direct storage. The native two-missing-volume encrypted RAR case
first reproduced the old demotion and then an omitted CBC edge, and now passes
without demotion. That slice passed all 3,830 workspace Nextest tests (13 existing
skips), with formatting, all-target/all-feature Clippy, three doctests, 23 PAR3
and 54 PAR2 native E2E cases passing. This continues unreleased 0.11.3.

Completed conventional sources restored without article-placement records now
publish their actual disk extent as a candidate. They receive fresh native
verification; the completion marker does not confer checksum evidence. Partial
sources and virtual holes retain explicit availability. A real-process SQLite
restart test stops while recovery is blocked, changes another downloaded source,
and resumes successfully without refetching either completed payload. Its first
run exposed empty source publications that incorrectly exhausted recovery.

Declared PAR3-only archive jobs now defer early part/member checksum failures
until native verification settles. A complete native verdict then reopens the
archive checks; it cannot override a failing archive checksum or close an active
replacement transaction. Mixed jobs retain the existing PAR2 policy. Plain and
encrypted single-byte corruption now pass the stronger native E2E requirement
of direct finalization without demotion. Four focused router regressions pass.
Authenticated discovery now also updates active and subsequently admitted direct
sets when the carrier was posted under an unrelated filename. Two native cases
hold the corrupted RAR volume until an official recovery carrier named `.bin`
authenticates; both first reproduced demotion and now remain direct. Deferral
cannot retroactively undo a demotion that preceded every PAR3 hint.
The archive-deferral slice passed all 3,835 workspace Nextest tests (13 existing
skips), full workspace Clippy and 93 of 95 native E2E cases. Only the two large
interleaving cases described below failed.

Checked stale-gap rereads now run through the same PAR3 worker ticket and shared
memory/handle budgets as output readback. Each handback covers at most 256 KiB,
with a 64 KiB read buffer and one bounded path; the router selects one run without
allocating the full plan or a part-boundary list. It checks source snapshots
around the read, rejects changed worker epochs and layout on handback, and keeps
replacement/checkpoint gates fenced until every gap has a checksum. Reread errors
fail the job while preserving verified native images. Focused bounded-I/O,
cancellation and worker-fence tests pass, as do all 78 conventional/archive/restart
PAR3 and PAR2 archive E2E cases. All 3,839 workspace Nextest tests pass (13
existing skips), along with workspace Clippy, formatting and all three doctests.

Identical disk publications now preserve native evidence only when the backing
path and generation, filename binding, logical publication generation and exact
committed ranges still match. A write fence or rebinding forces fresh verification
even if the underlying bytes happen to be unchanged. A focused regression asserts
zero additional authoritative source verifications on unchanged publication.
This does not yet preserve generations across actual hole-filling writes.
That slice passed all 3,840 workspace Nextest tests (13 existing skips), full
Clippy, formatting, three doctests and 96 of 98 native E2E cases. Only the two
large-interleaving cases failed.

A publication that only extends committed visibility over an unchanged disk
backing now preserves its logical generation and native evidence. The registry
rechecks both physical and logical generations when admitting the extension;
withdrawal or changed backing rejects the continuity claim. The focused test
closes an interior hole without repeating authoritative verification of clean
extents. This slice passed all 3,841 workspace Nextest tests (13 existing skips),
full Clippy and 98 of 100 native E2E cases. The same two large-interleaving cases
remain blocked by the retained-layout ceiling.

Disk carrier replay now validates the path and logical publication generation
as well as the backing snapshot. Withdrawal or rebinding forces authentication
again, while identical ranges incur no scan work. Visibility extensions retain
the scanner; a missing suffix keeps its pending packet hash unless scanning
explicitly jumps past a hole to discover later packets. Focused regressions
measure exactly the newly visible bytes read inside an incomplete packet and
verify replay, withdrawal and rebinding against official carrier fixtures.
Actual carrier writes still require a new generation and fresh scanning.
All 3,843 workspace Nextest tests pass (13 existing skips), alongside formatting,
full Clippy and all three doctests.
Both the debug and optimized native binaries pass 98 of 100 E2E scenarios; only
the two known large-interleaving resource-limit failures remain.

Direct restart tests exposed an ordering gap between native verification and
router settlement. Restored encrypted members can mark every participating volume
as suspect, even when native repair needs only one volume. Direct finalization now
waits for the native verdict to clear that deferred state and re-run the archive
checks; completion re-enters finalization before conventional extraction can run.
Plain and NZB-password encrypted restart cases pass without demotion or completed
source refetch, including changed member partials. A separate eight-block encrypted
case confirms that insufficient recovery fails without delivering output; the
successful changed-CBC-chain case uses 24 official recovery blocks.
Initial API password overrides were lost at admission. With explicit operator
approval, initial overrides, later updates and parked duplicate-candidate
passwords now use the existing AES-256-GCM encryption key and stored envelope.
Restore authenticates decryption and preserves the distinction between no override
and an explicit empty password. Existing plaintext overrides migrate through the
normal startup encryption pass; archive credentials participate in missing-key
and wrong-key validation. The enabled API-password encrypted-RAR restart scenario
now passes, asserting encrypted storage before restart, byte-exact repair and
preserved direct extraction. Validation passed formatting, all-target/all-feature
Clippy, all 3,854 workspace Nextest cases (13 existing skips), and three doctests.
The full native matrix passes 130 leaves; only the two 65,538-block retained-layout
limit cases fail. PostgreSQL runtime validation remains unavailable locally.
This uses the existing unreleased 0.11.3 workspace version and adds no dependencies.
The 64 MiB large-geometry limit remains separately unresolved.

Repeated direct restart now preserves persisted RAR facts owned by an accepted,
non-demoted direct checkpoint. Conventional discovery previously discarded those
facts because virtual source volumes had no complete disk image, leaving a later
checkpoint impossible to restore. The regression repeats a PAR2 direct restart
and checks identical persisted facts, retained download floors and final bytes.
Four native PAR3 crash cases interrupt repaired coverage before persistence or
after publication for plain and encrypted RAR; all pass with byte-exact installed
archive images before the interruption. Published checkpoints resume without
completed-source refetch. Two cancellation cases also pass, checking cancelled
history, active-row retirement, no delivered output and no further downloads.

The intermittent process-handle leak also reproduced outside Weaver in a scratch
Rust crate with no dependencies and four tests that only sleep (two for 40 ms,
two for 900 ms). Under the same 500 ms leak-failure timeout, two of 60 concurrent
stress iterations failed. The Weaver post-processing group passed all 20 serial
stress iterations, and its disabled-script case passed all 30 isolated iterations.
This isolates the intermittent failure from Weaver application code; the precise
runner/OS cause remains unconfirmed. No leak timeout, test assertion or repository
concurrency setting was weakened.

The advanced native matrix now covers GF16, FFT, uneven cohorts, aligned/sliding
deduplication, Data-only repair and packed tails. Three additional passing cases
put independent Cauchy and FFT sets in one job: clean, both damaged, and only
Cauchy damaged. The last case prohibits recovery downloads for the clean FFT set.
The matrix also verifies that surplus recovery in other cohorts cannot satisfy
one cohort's deficit. Official creation and verbose listing establish each geometry.
The 65,538-block cases initially exceeded the engine's default 64 MiB retained
ceiling. With explicit operator approval on 2026-09-09, Weaver now allows 128 MiB
retained per session within the unchanged shared 256 MiB engine pool. Both clean
and damaged cases pass, and the complete native matrix now passes all 142 leaves.
The shared-session regression observes another session's retained allocations and
confirms that dropping the owner releases its charge. Formatting, full Clippy,
all 3,856 workspace Nextest cases (13 existing skips), and three doctests pass.
This establishes case correctness, not performance acceptance. Newly added
embedded-archive coverage remains a separate incomplete gate. The existing
unreleased 0.11.3 version covers this change; dependencies and PAR2 limits did not change.

Embedded ZIP, ZIP64 and 7z discovery now reads bounded archive framing on blocking
workers, reuses the CRC-validated 7z start header, and hashes candidate packets
through the retained scanner. The framing probe does not initialize Windows
whole-file source hashing. Probe records are charged to the existing host budget
and retired with the job. Completion probes also cover incomplete and restored
archives; native publication still exposes only committed article ranges.

Embedded sources remain archives while also carrying packets. Protected damage
uses the native `SelfRepairPlan` and a separate staging directory; protected bytes
and container framing are verified before installation. Available authenticated
packets are preserved in an explicitly requested replacement carrier, with no
additional parity requested solely for completeness. Replacement emits an
operator warning rather than claiming byte-for-byte restoration. A hole confined
to the unprotected packet gap takes the same verified replacement path before
assembly completion. Ordinary intact archives are not rewritten. Nested embedded
destinations are currently refused explicitly; renamed placement, embedded
restart/cancellation and durable warning surfaces remain part of the open MVP
gates.

The new official-reference insertion harness covers each of the three containers
with clean bytes, corrupt body, corrupt header, a recoverable interior article
hole, missing protection-only bytes and insufficient recovery. Successful cases
require native PAR3 authentication and byte-exact extracted members; unrecoverable
cases must fail without publishing extracted output. Ordinary ZIP payloads and
comments containing the PAR3 signature do not admit a carrier. Fixture provenance
records the reference executable and input/output hashes. The existing prospective
0.11.3 version covers the integration, with no dependency or PAR2 limit changes.
All 18 embedded cases pass. The combined native matrix passes all 160 leaves,
and three repeated runs pass all 12 selected mixed ZIP/ZIP64 cases after the
native-versus-deferred PAR2 settlement fix described below. Final formatting,
all-target/all-feature workspace Clippy, all 3,859 workspace Nextest cases
(13 existing skips), and all three doctests pass. The clean-set guard also
checks that authenticated matrix identity survives a clean assessment with
no recovery requirements. This remains correctness evidence rather than
throughput acceptance or completion of the remaining MVP gates.

The embedded boundary follow-up uses late authenticated layout metadata to
rewind to an earlier packet gap when damaged framing initially skips the start of
a large recovery packet. The carrier retains that floor per source generation,
so unchanged replay performs no additional packet reads or scanning work; holes
retain their retry position. ZIP64 locators are read by absolute position even
when a 65,535-byte comment puts them outside the bounded tail window. Source
writes and identity changes withdraw negative discovery results before engine
admission, without allocating an engine for ordinary jobs. A repaired disk image
retains its explicit complete availability across later probes; old article-hole
ranges cannot replace that publication and trigger repeated repairs. Later
source writes and identity changes withdraw this availability fact as well as
native evidence. Repeated discovery tests require unchanged verification counts.

Fourteen added native cases pass on 16 MiB stored ZIP, ZIP64 and 7z archives,
covering clean inputs, damaged headers, interior missing articles, missing
protection, and damaged duplicated ZIP footers. A small checked-in official
insertion fixture has a deterministic regeneration recipe and provenance for
scanner replay tests. No third-party dependency or PAR2 budget changed; these
fixes remain covered by prospective workspace version 0.11.3.
The pinned reference rejected insertion into regenerated ZIP/ZIP64 sources with
65,535-byte comments, and a smaller standalone probe also rejected a 1,024-byte
comment (a 16-byte comment succeeded). No embedded long-comment fixture or
compatibility claim is fabricated; the ordinary ZIP64 maximum-comment regression
still validates discovery without a false carrier admission.

Final boundary-slice validation passes all 174 native scenarios, all 3,862
workspace Nextest tests (13 existing skips), all three doctests, formatting and
all-target/all-feature workspace Clippy. The fixture regeneration is byte-exact.
Native archive harnesses now cancel their own unfinished job after a failed
scenario, preserving its failure and artifacts without starving subsequent cases.

PAR3 sets sharing an output path now compare authenticated length, comparable
whole protected-data fingerprints, and fingerprints or inline bytes of equal
complete extents before assessment. Different codecs and block sizes remain
compatible; whole fingerprints over different unprotected gaps are not compared,
and fragment digests never stand in for complete hashes. Temporary indexing is
charged to the existing host budget, with cancellation between comparisons.
Contradictory descriptions fail before repair can overwrite the shared path.
Four official-reference native cases cover clean and damaged shared inputs under
compatible Cauchy/FFT sets and contradictory sets. A deterministic standard-library
fixture generator supplies the small index-only Rust regression, which confirms
repeated consistency checks and conflict rejection require no source reads. This
uses the existing prospective 0.11.3 version, with no dependency changes. Renamed
source placement and remaining lifecycle/product gates are still open.
Validation passes all 178 native scenarios, 3,863 workspace Nextest tests
(13 existing skips), three doctests, formatting, and all-target/all-feature
workspace Clippy.

Successful embedded replacements now carry their native repaired-block count and
an explicit original-carrier limitation into the persisted repair-completion
history entry. The existing GraphQL event enum and job-detail rendering remain
compatible. Clean archives, insufficient recovery, stale handbacks and failed
reconciliation cannot emit a successful replacement warning. All 32 official
ZIP/ZIP64/7z scenarios check warning presence or absence through persisted
GraphQL history; an isolated SQLite regression checks the warning after reopening.
This uses prospective 0.11.3 without dependency changes. Source placement,
remaining lifecycle scenarios and native verification metrics remain open.
Validation passes all 178 native scenarios, 3,864 workspace Nextest tests
(13 existing skips), three doctests, formatting and workspace Clippy. The runtime
GraphQL schema remains byte-for-byte identical to the checked-in schema.

Settled PAR3 assessments now record native verification outcomes and history
messages without widening the GraphQL event enum. Missing source bindings report
`missing`; incomplete bound inputs report `damaged`; complete protected data
reports `intact`. The terminal fallback consumes any pending native verdict before
considering `unverifiable`. Verification receipts use actual full/incremental
source work and set membership, excluding recovery arrivals, metadata-only
rebuilds and cache reuse. Source changes fence receipts until fresh assessment.
Verification timing includes native assessment/arrival calls that read source
bytes, excluding queue waits and packet scans. This leaves PAR2 timing and its
hot paths intact; PAR2-only jobs allocate no PAR3 receipt state. Official-fixture
regressions cover replay, changed sources and false terminal fallback attribution.
This uses prospective 0.11.3 without dependency changes. Live verification progress,
source placement and remaining lifecycle gates still require completion.
Validation passes all 178 native scenarios, all 3,865 workspace Nextest tests
(13 existing skips), the added incremental-arrival assertions, three doctests,
formatting and all-target/all-feature Clippy. The runtime GraphQL schema remains
byte-for-byte unchanged.

Source-name rebinding now withdraws the previous native binding as well as the
host lookup. The published native API has no unbind operation, so an unavailable
reserved identity retires an old name without discarding unrelated evidence.
Source and carrier publication reject that identity; host assessments expose it
as missing, never as a selectable source. Standalone and embedded regressions
confirm the old path cannot remain complete after a rebind, unchanged siblings
are not reread, packet scanning does not restart, and a new explicit binding
restores verification. The original regression reproduced a false `Complete`
assessment before the fix. Automatic content placement is still a separate gate.
Validation passed all 178 native scenarios, 3,867 Rust tests (13 existing skips),
three doctests, formatting, and all-target/all-feature Clippy. This uses
prospective 0.11.3 without dependency changes.

Standalone mixed-format coordination now gives PAR2 its first repair attempt and
excludes PAR3 volumes from its size-based recovery predictions. PAR2 writes fence
only affected PAR3 source evidence, then republish installed bytes for native
verification. After PAR2 exhausts recovery, PAR3 may repair shared sources; affected
PAR2 sets reopen and perform their own verification. Current, conflicting native
verdicts refuse further writes and delivery. A `strong_decode` settlement defers
integrity to archive extraction and is tracked separately from a PAR2 hash pass.
Current, bound PAR3 damage reopens only overlapping deferred claims and forces
the selected PAR2 set through its own authoritative verification. This fixes a
mixed ZIP/ZIP64 timing failure without weakening native conflict checks. Reopening
claims performs no source reads and preserves unrelated settled sets.
Ten real-process scenarios cover
independent clean/repair jobs, preservation of an unrelated PAR2 failure,
shared clean/preferred/fallback/insufficient jobs, and conflicting official
descriptions in both directions and after fallback. Fallback eligibility is a
typed native fact (insufficient recovery or resource limitation); error strings,
I/O and cancellation cannot grant it. The focused handoff regression measures
exactly one new native PAR3 source verification after a one-file PAR2 repair,
with zero verification rereads for its clean siblings.
Cross-format temporary file lists share the existing 16 MiB host budget, and
PAR2-only jobs allocate no handoff state. Ten additional native scenarios cover
PAR2-first and PAR3-fallback repair for ZIP, ZIP64, split payloads, stored RAR and
encrypted multi-volume RAR. All ten pass, with byte-exact members, no RAR direct
store demotion, no PAR3 recovery download when PAR2 suffices, and only the first
PAR3 recovery packet downloaded for the fallback damage. Direct admission keeps
both native verdicts outstanding; an exhausted native PAR2 verdict can hand off
without a disk-only reanalysis. Repaired virtual source lengths use committed
decoded coverage, since completion progress is in NZB encoded units. The harness
also waits for HTTP startup before inserting its fixture API key, avoiding a
write racing SQLite initialization. That slice passed formatting,
all-target/all-feature Clippy, all 3,849 workspace Nextest cases (13 existing
skips), and three doctests. Its native matrix passed 129 leaves; the subsequent
encrypted-password persistence fix above resolved the API-password restart failure.
The two 65,538-block memory-limit cases remain open.

Six additional native cases cover two distinct PAR2 sets sharing one PAR3 source:
clean, PAR2-first repair, PAR3 fallback, and conflicts in both directions and after
fallback. They exposed a handoff that used a globally ambiguous file binding when
the active PAR2 set was known. Fencing and refresh now resolve that exact set;
conflict detection checks every settled set. All six pass, and fallback requires
fresh authoritative verification from both distinct PAR2 set IDs. The Rust
regression also covers overlapping sets and still measures exactly one source
verification after repair, with no clean sibling rereads. A second regression
reproduced the same ambiguity in absent-set detection: an empty shared source
could incorrectly skip both native sets. That check now uses the selected set's
binding too; a known source remains that set's responsibility even without a disk
image.

Unavailable PAR2 metadata can no longer veto delivery after current authenticated
PAR3 evidence verifies every payload. This exception never clears a parsed PAR2
set's failure or turns PAR3 hashes into PAR2 checksums. Payloads outside the PAR3
set and evidence invalidated by a write remain ineligible. Native clean/repair
cases reproduce the previous failure and now pass; negative cases and a Rust
regression cover unrelated verified sources, stale evidence and zero policy I/O.
These changes continue the existing unreleased 0.11.3 version without new dependencies.
Formatting, all-target/all-feature Clippy and all three doctests pass. The initial
workspace sweep passed 3,854 tests and reported the previously reproduced
exited-successfully process-handle leak in the disabled-script test. That test
passed in isolation; the unchanged four-worker confirmation sweep passed all
3,855 tests with 13 existing skips. Leak detection and assertions remain unchanged.
The rebuilt native binary passes 140 of 142 E2E leaves, including all twenty mixed
cases and the encrypted API-password restart. Only the two 65,538-block retained
layout budget cases fail. Both remain enabled; the 64 MiB ceiling is unchanged.

Still pending: incremental decode-to-publication wiring, positioned verification,
embedded archives, evidence persistence and product surfaces. Rebuilt files currently
receive a verification read when republished; clean native evidence is retained.
PAR3 repairs conservatively retire extraction chases until a PAR3 mutation view
can positively vouch for their consumed bytes. Full E2E and performance acceptance
remain in progress.

Dependency baseline: PAR2 0.10.2, UnRAR 0.10.3, and registry Reed-Solomon 0.4.4.
The original development pointer used rarpar
`54e51e83226767b2ff93ec56e2d3d59e1bdcc8ee`. With explicit operator approval,
Weaver now uses published PAR3 0.3.0 and shared registry Reed-Solomon 0.4.5.
The published source trees match the reviewed pointer. Dependency metadata shows
only the approved PAR3 origin and arithmetic unification changes: PAR2 remains
0.10.2 with native-crypto, and UnRAR remains 0.10.3 with crypto-aws-lc.

The first [ARM64 CLI comparison](par2-performance-arm64.md) passes all nine
elapsed-time gates across streaming, large-block and many-small-file fixtures,
with 256 successful trials including warm-ups. The largest upper ratio bound
is 1.0150. Full pipeline and x86-64 measurements below remain required; this is
not full performance acceptance or an integration-readiness claim.

The first performance harness is now `cargo xtask perf par2-compare`, documented
in [paired PAR2 measurements](par2-performance.md). It compares clean verification,
damaged verification and repair through the native CLI, using private fixture
copies, matching output hashes, alternating pairs, CPU/RSS/filesystem operation
counters and separate confidence gates. It adds no dependencies. Its 36 xtask
tests pass. An identical-binary smoke run completed 152 trials with valid output
hashes; both verification workloads remained statistically inconclusive after
30 pairs, while repair passed. This validates the harness behavior, not candidate
performance. End-to-end download/session/extraction measurements remain required.
Full formatting and Clippy pass. The initial full Rust sweep reported 3,844 passes
and one exited-successfully NNTP HTTP3 test with leaked output handles. It passed
in isolation; the unchanged four-worker confirmation sweep passed all 3,845 tests
with 13 existing skips. No assertion, leak timeout or repository concurrency policy
was changed. The existing unreleased 0.11.3 version covers the application changes;
the new private xtask helper has no additional publishable-crate impact.

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

Conventional-repair validation: all 3,792 workspace tests pass with 13 existing
skips; all-target/all-feature workspace Clippy passes. The 25 targeted PAR3/source
tests include a repair through the pipeline gate with an interior article hole,
native output verification, completion reconciliation and no clean-file rewrite.

Acquisition validation: all 3,793 workspace tests pass with 13 existing skips;
all-target/all-feature Clippy passes. Seven native-process scenarios exercise
the authenticated API, real yEnc/NNTP downloads, repair and final move: clean,
corrupt with valid yEnc CRCs, missing article, missing/omitted/late index, and
insufficient recovery. Clean jobs request no recovery carrier and repairs stop
before the final surplus carrier. These tests found and fixed premature health
abort, missing partial-source publication, and counting an unavailable index as
missing payload after alternate carriers verified the set. See `e2e/docs/par3.md`.
Local Windows cross-checking is blocked by missing Windows SDK headers in AWS-LC;
native Windows validation remains outstanding.

Article-selection validation: all 3,794 workspace tests pass with 13 existing
skips; formatting and all-target/all-feature Clippy pass. All seven native-process
scenarios pass with the stricter partial-carrier request assertion. An official
two-packet carrier regression authenticates the packet beyond an interior hole,
then counts both packets exactly once after the missing range arrives.

Virtual-source and archive validation: 22 native PAR3 scenarios pass, including
ZIP, ZIP64, split files, stored RAR and encrypted multi-volume RAR. Clean RAR
jobs finalize without materializing source volumes; a single-byte corruption
needs only the first recovery packet. All 54 existing PAR2 archive E2E cases
also pass. These runs found and fixed an idle extraction phase blocking repair,
lost reconstructed and buffered article placements during demotion, and stale
extraction failures refetching over repaired output. Known archive damage now
waits for PAR3 publication and verification, and demotion fences virtual reads
until its materialization ticket returns. Native Windows and performance
acceptance remain outstanding.

Formatting, all-target/all-feature Clippy, all three doctests and the final
locked workspace sweep pass: 3,804 tests with 13 existing skips. Two earlier
eight-worker sweeps reported leak timeouts in unrelated restore-history and yEnc
tests. Both passed in isolation; a serial sweep and the final four-worker sweep
passed completely. The strict leak threshold is unchanged; the intermittent
reports' cause remains unconfirmed.

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
budget, the approved 128 MiB retained ceiling per session, and a 16 MiB aggregate input queue.
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
