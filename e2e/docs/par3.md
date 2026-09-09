# Native PAR3 pipeline scenarios

`TestPar3E2E` owns a new Weaver process, loopback NNTP fixture and SQLite database.
It submits NZBs through the authenticated GraphQL API and checks final output
bytes, terminal status, recovery article requests and standalone carrier cleanup.
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

Clean RAR cases, the wholly missing plain RAR volume and the encrypted
missing-article case require direct finalization without demotion. Every single-byte
corruption case must complete using only the first recovery packet; downloading
extra parity to compensate for lost source-coverage metadata fails the test.
The native matrix currently passes all 22 PAR3 scenarios, alongside 54 existing
PAR2 archive regression cases. This is correctness evidence, not a throughput
or full integration readiness claim.

These matrices do not yet cover embedded protection, restart evidence, both
datastores, multi-damage selective direct repair or performance acceptance.
Those remain tracked in `docs/par3-integration-plan.md` at the repository root.
