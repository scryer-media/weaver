# Native PAR3 pipeline scenarios

`TestPar3E2E` owns a new Weaver process, loopback NNTP fixture and SQLite database.
It submits NZBs through the authenticated GraphQL API and checks final output
bytes, terminal status, history health, recovery article requests and standalone
carrier cleanup. Successful cases require zero failed bytes and 1000/1000 health.
No existing application instance, database or service is used.

From the repository root:

```sh
cargo build --locked -p weaver
cd e2e
WEAVER_PAR3_E2E_BIN="$(cd .. && pwd)/target/debug/weaver" \
  go test ./internal/weaver -run '^TestPar3E2E$' -count=1 -v -timeout 8m
```

The checked-in carriers under `internal/weaver/testdata/par3-native` come from
the pinned official reference. Their provenance, command transcript and SHA-256
manifest accompany them. The protected input is regenerated in the test; only
input bytes or article availability are damaged. No packet is assembled or edited.

The conventional-file matrix covers:

- clean data, with no recovery download;
- corruption whose article and whole-file yEnc CRCs both match the damaged bytes;
- a missing article exceeding the legacy health-abort threshold;
- a missing index, an omitted index and alternate-carrier metadata;
- late metadata held until payload bytes are on disk;
- insufficient recovery, which must fail rather than deliver corrupt bytes.

Successful repairs must reproduce the expected BLAKE3 and byte length, stop before
requesting the final surplus carrier, and leave no spent `.par3` files in the
delivered directory. The test retains its directory and writes per-case evidence
with request counts and terminal status beside the Weaver log and database.
The missing-article case also requires exactly one article from its two-article
recovery volume, proving acquisition stops before the carrier is complete.

`TestPar3ArchiveE2E` adds clean, corrupt and missing-article cases for ZIP,
ZIP64, split files, stored RAR and encrypted multi-volume RAR. Direct-store
routing is enabled in its isolated process. Supply `WEAVER_PAR3_REFERENCE_BIN`
as an absolute path to the pinned official `par3cmdline` reference to generate
protection in the preserved run directory:

```sh
WEAVER_PAR3_E2E_BIN=/absolute/path/to/weaver \
WEAVER_PAR3_REFERENCE_BIN=/absolute/path/to/official/par3 \
  go test ./internal/weaver -run '^TestPar3ArchiveE2E$' -count=1 -v -timeout 15m
```

The fixture helper records the reference binary hash, exact arguments, protected
input hashes, carrier hashes and creation transcript. It uses the existing RAR
fixtures and an installed `7zz` to read their expected members, plus installed
Info-ZIP for forced ZIP64. It installs no tools and changes only protected inputs
or article availability for damage cases. The pinned reference revision is
`2971702e501f1350b1c7b9d11369af9157d6ed56`; platform adaptations need their own
provenance alongside the binary hash.

All RAR cases, including corruption and missing articles, require direct
finalization without demotion. Encrypted RAR also loses an article in each of two adjacent volumes, requiring both verified
repairs to enter the shared direct output. Every single-byte
corruption case must complete using only the first recovery packet; downloading
extra parity to compensate for lost source-coverage metadata fails the test.
The conventional/archive matrix passes all 25 PAR3 scenarios, alongside 54
existing PAR2 archive regression cases. The two disguised-carrier cases post an
official recovery volume as `.bin` and hold the damaged RAR until that carrier
authenticates; both must keep direct extraction. This is correctness evidence, not a throughput
or full integration readiness claim.

`TestPar3GeometryE2E` uses the same binary variables and records official creation
and verbose listing output for GF16, FFT, uneven cohorts, more than 65,536 blocks,
aligned/sliding deduplication, Data-only repair and packed tails. It also puts
independent Cauchy and FFT sets in one job, requiring both repairs when damaged
and no recovery downloads for the FFT set when only Cauchy input is damaged.
Clean cases must avoid recovery downloads. A cohort-deficit case must refuse
delivery even when other cohorts have surplus recovery. The two large-block cases currently
exceed the published engine's 64 MiB retained-state limit and remain enabled;
this matrix is not yet fully passing.

`TestPar3RestartE2E` owns two successive processes over its isolated SQLite state.
It stops while recovery is blocked, changes a completed source on disk, resumes,
and requires byte-exact repair with no completed-source refetch. This tests fresh
verification after restart, not durable evidence replay.

`TestPar3DirectRestartE2E` restarts after a durable direct coverage checkpoint
exists and before recovery arrives. Plain RAR and encrypted RAR with an NZB
password must resume, repair and finalize without demotion, volume materialization
or completed-source refetch. Additional cases change a member partial while the
first process is stopped. Changed encrypted plaintext affects the reconstructed
CBC chain: the positive case has 24 official recovery blocks, while eight blocks
must produce an explicit insufficient-recovery failure without publishing output.
Cancellation cases stop while recovery is blocked and require the existing
`FAILED`/`CANCELLED` history contract, no delivered output, no further requests,
and removal of active job and coverage rows. Crash cases interrupt the repaired
coverage barrier before persistence and after publication, then start a third
process. Both require byte-exact repaired archive installation before the crash;
a published checkpoint must resume without refetching completed source volumes.
The pre-persistence interruption may refetch because no checkpoint survived.

The API-password case remains enabled and currently fails because initial job
admission does not persist its password override. Its persistence fix awaits
operator approval; successful NZB-password cases do not cover that gap.

Run these additional suites with the same binary variables:

```sh
go test -mod=readonly ./internal/weaver -run '^TestPar3(Geometry|DirectRestart|Restart)E2E$' -count=1 -v -timeout 20m
```

Embedded protection, durable evidence replay, both datastores and performance
acceptance remain tracked in
`docs/par3-integration-plan.md` at the repository root.
