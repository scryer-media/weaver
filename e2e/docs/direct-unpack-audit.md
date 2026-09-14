# Direct-unpack audit — 2026-09-08

This audit revisited the sequential archive implementation in `70622fa6` and
its tests in `8c78e738`, including the shared ZIP/ZIP64 chase infrastructure.
The earlier helper-level tests did not establish that real download completion
would admit repair. The new [native e2e matrix](direct-unpack.md) exercises
that scheduler and proves extraction overlaps downloading.

The later [repair grounding follow-up](direct-unpack-repair-audit.md) found and
fixed two additional repair/read and split-boundary defects. Its validation
extends the matrix to 54 scenarios; the results below describe the earlier run.

## Reproduced defects and fixes

| Finding | Reproduction | Resolution |
| --- | --- | --- |
| A physical short file could masquerade as archive EOF | Advertise 4096 committed bytes, truncate the disk image to 1024, read through both gated reader modes | Zero bytes inside committed coverage now returns `UnexpectedEof`; legitimate archive EOF still comes from settled coverage |
| Cancelling a chase did not cancel its decoder-memory wait | One half-downloaded gzip holds the process memory allowance; pause a second chase waiting for it | Retain the chase budget with its owner and cancel it alongside coverage, including extraction handoff timeout; wait for the blocking worker before removing its staging directory |
| Whole-file CRC rejection prevented usable PAR2 recovery | Valid per-article CRCs describe damaged posted bytes, the full CRC describes the original, and matching PAR2 is available | Defer only when bound PAR2 length and complete slice-CRC metadata agree with the expected full CRC; retain the actual damaged checksum and expected CRC, invalidate durable completion, and require verification/repair |
| A repair could add a numbered part without fixing extraction's part list | Remove the first, interior or last part from the NZB while retaining PAR2 over every original part | Adopt verified parts into matching topology, invalidate running or completed chases built from the smaller list, and share the topology-based path collector with conventional extraction |

The CRC fix needed an additional acceptance guard: once taint removed the
chase, the existing strong-decode shortcut could otherwise ignore the recorded
whole-file mismatch. A known mismatch now requires an authoritative verdict
even after the worker exits. Conflicting CRC metadata, missing slice checksums
and absent PAR2 still fail; this is not a CRC waiver.

During development, the matrix caught incomplete fixes too: merely aborting
the running chase left completed output eligible, and the old simple-archive
path collector omitted adopted parts. Both could install a three-MiB join
instead of the expected four-MiB file. Final e2e assertions check length and
BLAKE3, so admission, a repair log or terminal success alone cannot pass.

## Other boundaries reviewed

- Decoder selection, admission before topology exists, committed-range reads,
  final EOF, memory reservation, pause/abort, completion ownership, taint and
  repair handback were traced through production callers.
- New negative tests reject truncated gzip, bzip2, XZ, Brotli, Zstandard,
  DEFLATE and compressed TAR streams without repair data. Existing corrupt
  trailer and concatenated-stream tests continue to pass.
- Extraction still uses the shared `ExtractionRoot`, budgeted readers and
  writers, member/path validation and resource limits. The full sweep retains
  existing path traversal, symlink, malformed-header, cancellation and
  multi-set recovery coverage.
- Standalone `.xz`, direct `.xz` and `.tar.xz` use liblzma through
  `ingest/xz.rs`. Completed single-stream, multi-block XZ retains the bounded
  liblzma parallel decoder; streaming input uses its bounded concatenated
  decoder. The `lzma_rust2` imports there are test writers under `cfg(test)`.
  The upstream 7z decoder remains the stated exception.
- No dependencies, features, public APIs, schemas, configuration keys or
  fixture expectations changed. All 1,019 source files are below 4,000 lines;
  the largest is 3,777 lines.

## Validation

The 52 native SQLite scenarios passed normally and with Go's race detector.
They cover 16 archive representations, missing articles, corrupt articles,
PAR2 over joined data, and whole absent numbered parts. Every case produced
staged bytes while its own archive responses were held, completed, and matched
the expected length and BLAKE3. Clean cases consumed their chase; damaged
cases required actual repair output. CI now runs this matrix in the Rust job
and retains its log and per-scenario evidence.

Formatting, host Clippy, the repository Linux Clippy check, `actionlint`,
`git diff --check`, and `go test ./...` passed. The initial locked full
Nextest sweep passed 3,726 tests with 13 ignored tests. It reported one
process-leak warning in the existing post-processing persistence test;
20 isolated repetitions passed without reproducing that warning. A subsequent
full run received SIGTERM after 3,723 passes; its three interrupted tests are
not counted as passing that run. The final full confirmation passed all 3,726
tests in 48.54 seconds, with 13 ignored tests and no leak warnings.

The two normally ignored ZIP64 tests also passed: forced/streamed 150 MiB
archives and real five-GiB members with later members at offsets beyond 4 GiB
from both Info-ZIP and 7-Zip. The latter asserts output appears before its
middle input range becomes readable and verifies final digests. These are
gated-reader integration tests; PAR2 behavior is covered by the native matrix.

Local evidence retained for this audit:

- `weaver-audit-nextest-final.log`: final full Rust sweep.
- `weaver-audit-zip64-large.log`: both normally ignored ZIP64 tests.
- `weaver-audit-e2e-matrix-7.log` and `weaver-audit-e2e-race.log`: native matrix.
- `weaver-audit-clippy-final-host.log`, `weaver-audit-clippy-final-linux.log`,
  `weaver-audit-go-full.log`, and `weaver-audit-actionlint.log`: hygiene checks.

These logs are in the audit host's temporary directory. The native matrix log
prints its separate preserved artifact root, containing 52 evidence records.

## Limits

The native e2e matrix uses SQLite and four-MiB payloads. This audit does not
claim a new Windows/macOS/Linux runtime matrix, a PostgreSQL e2e run, the full
published functional/chaos/Arr corpus, or a process crash at every repair
handoff. Rust tests verify the CRC-rejected source has no durable completed
hash, but that is not a new crash-and-restart e2e scenario. No production or
existing local application instance was operated on. Prior artifacts remain
untouched.
