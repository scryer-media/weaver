# Direct-unpack archive matrix

`TestDirectUnpackE2E` runs a native Weaver binary against its own loopback NNTP
fixture and fresh SQLite database. It generates deterministic 4 MiB payloads,
submits real NZBs through an authenticated GraphQL API, and drives the normal
download, verification, repair, extraction and final-move scheduler.

From the repository root, with the locked frontend assets already built:

```sh
cargo build --locked -p weaver
cd e2e
WEAVER_DIRECT_UNPACK_E2E_BIN="$(cd .. && pwd)/target/debug/weaver" \
  go test ./internal/weaver -run '^TestDirectUnpackE2E$' -count=1 -v -timeout 10m
```

Requirements: the repository's Rust and Go toolchains, `par2`, Info-ZIP `zip`,
and `xz`. The ZIP64 cases use Info-ZIP's forced ZIP64 and streamed-input modes.
XZ fixtures use the system liblzma writer. No published corpus or Docker stack
is needed. Without the binary environment variable the test reports a skip;
the Rust CI job sets it and runs the matrix explicitly.

## Scenarios and assertions

The matrix covers TAR, tar.gz, tgz, tar.bz2, tar.xz, gzip, bzip2, XZ, zst,
zstd, Brotli, raw DEFLATE, plain numbered parts, ZIP, forced ZIP64 and streamed
ZIP64. Every representation runs clean, missing-article and corrupt-article
cases. Numbered parts also exercise PAR2 protecting the joined file and a
whole first, interior or last part absent from the NZB, plus shorter and longer
posted first parts: 54 scenarios in total.

Each scenario holds an archive BODY response behind an explicit gate. It
requires nonempty member output in that job's direct-unpack staging directory
while that same job has held responses and remains nonterminal. A fixed sleep
or an admission log alone cannot satisfy this assertion. The gate leaves the
ZIP directory and one available numbered part downloadable so bootstrap does
not depend on releasing the gate.

The wrong-length cases require staged output beyond the posted first part's
boundary before release. Article and whole-file CRCs match the posted bytes;
PAR2 describes the original lengths. Final length and BLAKE3 assertions prove
repair invalidates the old joined output and restores the complete payload.

After release, every job must complete and match the expected length and
BLAKE3. Clean jobs must install their staged chase output. Damaged jobs must
also show that PAR2 wrote repaired outputs; invalidated chases may fall back
to extraction of the repaired source. Corruption preserves valid article
CRCs while contradicting the whole-file CRC, exercising the actual repair
acceptance gate rather than only missing-article retries.

The test prints its retained temporary directory. Each successful scenario
has `fixtures/<scenario>/evidence.json` with the job ID, held response count,
bytes staged before release, terminal status and output digest. The directory
also contains the Weaver log, generated archives, parity, NZBs and database.
CI uploads the log and evidence files. Processes and listeners started by
the test are stopped on exit; unrelated instances and prior artifacts are
never discovered or changed.

This small-payload matrix proves overlap and repair behavior. It does not
replace the large ZIP64 (>4 GiB), archive parser, resource-limit, restart,
multi-set PAR2 or full corpus suites.

See the [2026-09-08 audit](direct-unpack-audit.md) for reproduced defects,
fixes and validation limits.
