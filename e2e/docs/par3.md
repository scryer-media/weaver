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

This matrix does not yet cover virtual/encrypted sources, archive extraction,
embedded protection, restart evidence, both datastores or performance acceptance.
Those remain tracked in `docs/par3-integration-plan.md` at the repository root.
