# ARM64 PAR2 CLI comparison

All nine measured workloads passed the elapsed-time non-regression gate. This
covers native CLI verification and repair; it does not establish full Weaver
download, retained-session, direct-extraction or mixed-format performance.

## Setup

- Host: Apple M5 Max, 18 cores; four Rayon workers permitted for both binaries.
- Compiler: Rust 1.97.1, LLVM 22.1.6, aarch64-apple-darwin.
- Build: optimized release profile, native CPU flags, fat LTO, one codegen unit,
  panic abort and stripped binaries. Separate Cargo target directories.
- Baseline: `6b079b40acea50e64df2cfce956dd0412963d906`, Reed-Solomon 0.4.4.
- Candidate: the PAR3 integration tree at `3367dbe8d657fa5262c251042fc77dbb193e7007` plus the
  recorded dirty patch; PAR3 0.3.0 and shared Reed-Solomon 0.4.5.
- PAR2 0.10.2 and UnRAR 0.10.3 versions and crypto features remain unchanged.
- Two warm-up pairs, ten alternating measured pairs, extended to thirty when
  inconclusive. Two-sided 95% Student-t intervals on paired log elapsed ratios.
- A workload passes only when its upper ratio bound is at most 1.02.

Fixture creation uses the installed PAR2 reference tool. Only protected input
bytes are damaged. Every timed command runs on a fresh copy hashed before entry,
with output hashes checked afterward. Binaries and original fixture trees also
match their initial hashes at completion. All 256 trials, including warm-ups,
passed their expected exit-status and output-hash checks.

## Elapsed-time results

Each entry gives candidate/baseline ratio, 95% interval, and measured pair count.
The streaming fixture is 512 MiB plus a tail with 256 KiB blocks. The large-block
fixture is 1 GiB plus a tail with 8 MiB blocks. The small-file fixture contains
2,048 files of about 16 KiB with 8 KiB blocks.

### Streaming

- clean-verify: **0.9998**, [0.9976, 1.0020], 10 pairs.
- damaged-verify: **0.9991**, [0.9955, 1.0028], 10 pairs.
- repair: **1.0019**, [0.9979, 1.0059], 10 pairs.

### Large blocks

- clean-verify: **1.0015**, [0.9992, 1.0037], 10 pairs.
- damaged-verify: **1.0006**, [0.9979, 1.0034], 10 pairs.
- repair: **0.9986**, [0.9966, 1.0006], 10 pairs.

### Many small files

- clean-verify: **0.9984**, [0.9887, 1.0082], 30 pairs.
- damaged-verify: **0.9971**, [0.9795, 1.0150], 10 pairs.
- repair: **0.9966**, [0.9812, 1.0122], 10 pairs.

## Resource observations and limits

Per-trial CPU seconds, peak RSS and OS filesystem operation counts are preserved.
Peak RSS below is baseline → candidate, in MiB; these maxima are observations,
not a separate statistical memory non-regression gate.

- streaming / clean-verify: 13.17 → 13.23 MiB.
- streaming / damaged-verify: 13.98 → 14.02 MiB.
- streaming / repair: 24.95 → 25.33 MiB.
- large-block / clean-verify: 13.00 → 13.05 MiB.
- large-block / damaged-verify: 13.05 → 13.11 MiB.
- large-block / repair: 71.27 → 71.31 MiB.
- many-small / clean-verify: 18.92 → 18.22 MiB.
- many-small / damaged-verify: 17.38 → 19.02 MiB.
- many-small / repair: 29.52 → 29.58 MiB.

The OS reported zero block-operation counters even for repair writes. These
values cannot establish byte-I/O behavior. The CLI measurements also include
placement scanning and the CLI’s verification passes, so they do not measure
the server’s retained-session or selective-download path.

The streaming fixture used the development build of the runner; the other two
used an optimized build of the same source to shorten untimed fixture hashing.
Each comparison uses identical runner behavior for both sides. Native Weaver
binaries, worker limits and the copy/hash cache policy are unchanged.

A shared-target candidate build was discarded after Cargo reused a stale baseline
model artifact. The successful candidate came from a new isolated target directory.
No source workaround was made for that cache failure.

## Reproduction

Use [the comparison runner](par2-performance.md). The artifact bundle contains
`builds.json`, the tracked candidate patch and untracked-source snapshot, fixture
recipes and reference creation logs, per-trial logs/results, each fixture’s
`summary.json` and `completion.json`, and the aggregate JSON/CSV.

Binary SHA-256 identities:

- Baseline: `47de47e2541c9a0733fde9eefa7e7b27bd8be79611130aff27974087cd75d6bb`.
- Candidate: `074df625b1accd435376534809cf2577a7dda2ecd4350d53dec3467763aa0c52`.

The remaining integration and performance gates are tracked in
[the integration plan](par3-integration-plan.md).
