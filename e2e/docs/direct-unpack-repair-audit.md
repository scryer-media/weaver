# Direct-unpack repair follow-up audit

The grounding review of Weaver `ec9d0f3e` exposed two gaps not exercised by
the original 52-scenario matrix. This follow-up fixes those gaps on
`bugfix/direct-unpack-repair-guards`, based on the audited Weaver revision.

## Fixes and invariants

**Repair racing an admitted disk read.** Previously, coverage resolution and
consumption publication surrounded an unlocked disk read. Repair could vouch
the old consumed prefix, replace the file, and then receive a late consumption
update for damaged bytes read through the old handle.

The controller now freezes consumption before taking that snapshot. Disk reads
revalidate their generation and current readable range when publishing their
consumption under the coverage mutex. A rejected read retries from its original
position; no rejected bytes reach the decoder. Disk I/O stays outside the mutex,
and ordinary successful reads retain two coverage lock acquisitions. Repair
resumes consumption after all part generations and lengths are reconciled.
Abort also reaches cached EOF and memory waits. New chases cannot join a job
after its repair snapshot until handback clears the admission barrier.

**Repair changing an already-used split boundary.** Previously, the reader
cached part lengths while repair could change those lengths independently.
A shortened first part could be read through successfully, then grow during
repair without putting the missing bytes into the already-joined output.

Coverage now records boundaries used to map the stream and aborts if repair
changes one. The controller also compares settled source lengths with PAR2's
described lengths before retaining either a running or completed chase. A
byte-prefix vouch cannot justify a different layout. Changes before a boundary
is used remain supported when they preserve the consumed prefix; otherwise
normal extraction reads the repaired sources afresh.

CRC checks, extraction budgets, dependencies, and codec selection are unchanged.
Standalone XZ and tar.xz continue using liblzma; the upstream 7z decoder remains
the existing exception. All changed source files remain below 4,000 lines.

## Regression coverage

Ten new Rust tests cover reads stopped after disk I/O but before consumption,
repair replacement, a newly lowered damage cap, a repair pause without a prior
damage cap, multipart release, abort during the read, cancellation at cached
EOF, changed boundaries, safe changes before boundary use, completed outcomes,
and admission during repair. The interleaving tests use per-reader channels,
not sleeps or a global hook. Controls verify that safely vouched bytes remain
reusable. Controller fixtures use real block-aligned PAR2 evidence, including
a length mismatch whose entire posted prefix is intact.

The [native matrix](direct-unpack.md) now has 54 SQLite scenarios. The new
`split/short-first` and `split/long-first` cases post a first part 64 KiB shorter
or longer than PAR2 describes. Both article and whole-file CRCs match the posted
bytes, so a CRC rejection cannot accidentally invalidate the chase for the test.
Each requires output beyond the wrong boundary while archive responses remain
held, then actual PAR2 repair and the exact final length and BLAKE3.

The normal run staged 1,310,720 bytes for the shortened first part and 1,572,864
bytes for the longer first part before release. Both delivered the original
4 MiB payload. Evidence records, generated sources, logs and SQLite state remain
under the run's printed artifact directory; the original supplied artifacts
and existing application instances were untouched.

## Validation

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` and `git diff --check` | Passed |
| Locked workspace host Clippy, all repository-prescribed targets | Passed |
| `cargo xtask ci clippy --linux-only` | Passed |
| `cargo nextest run --workspace --locked --no-fail-fast` | 3,736 passed; 13 ignored tests skipped |
| Explicit ignored ZIP64 tests | Both passed: forced/streamed 150 MiB and real 5 GiB members with offsets beyond 4 GiB from both writers |
| Native SQLite matrix | All 54 scenarios passed, normally and with Go's race detector |
| `go test ./...` in the e2e harness | Passed after restoring existing generated fixture prerequisites in the new worktree |

Nextest reported two process-leak warnings during the initial affected-test
sweep and two in different existing tests during the full sweep. The four tests
exercise argument parsing, empty CRC input, coverage completion, and dictionary
size parsing. Each passed 20 isolated repetitions without a leak warning (80
executions). Their warning cause remains unproven; no leak timeout or test
expectation was changed. The full sweep's two slow tests also completed normally.

The initial controller geometry fixture did not vouch its partial final PAR2
block as intended. The corrected fixture redistributes that tail into the next
part, retaining a valid archive while aligning the tested boundary to complete
PAR2 blocks. The assertion that the entire posted prefix is vouched remains.

Local logs use the `/private/tmp/weaver-repair-guards-` prefix:
`nextest-full.log`, `leak-followup.log`, `zip64-large.log`,
`clippy-host.log`, `clippy-linux.log`, `e2e-full.log`, `e2e-race.log`, and
`go-full-final.log`. The normal native run's 54 evidence records are under
`/var/folders/8x/tq_8ldy17r734s057p4l4xkm0000gn/T/weaver-direct-unpack-e2e-302527601`.

## Scope

This fixes the two correctness findings. The grounding review's ZIP scheduling,
recursive nested-chase, and source-disk avoidance differences remain separate
performance work. This validation does not claim new PostgreSQL, Windows or
Linux runtime e2e coverage, a crash at every repair handoff, or a rerun of the
published full functional/chaos corpus.
