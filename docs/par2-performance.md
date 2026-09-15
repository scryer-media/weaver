# Paired PAR2 CLI measurements

Use `cargo xtask perf par2-compare` for native PAR2 verification and repair
comparisons on macOS or Linux. Build both Weaver binaries with the same optimized
profile, compiler, CPU flags and feature selection, using the baseline revision
and candidate tree. Give each source tree its own `CARGO_TARGET_DIR`; reusing a
target directory across exported trees can retain stale local-crate artifacts.
Preserve the build commands and source revisions beside the results. The runner records binary SHA-256 hashes, fixture hashes, platform,
worker limit, timing and resource counters. It does not infer optimization flags
from an executable or establish download/extraction pipeline performance.

Prepare two directories: a complete clean PAR2 set and a damaged copy. Keep their
index and recovery carriers identical; damage only protected inputs. Both must
contain the index passed through `--seed`. Input trees are read-only to the runner.
It refuses symlinks, special files and an existing result directory.

```sh
cargo run --locked -p xtask -- perf par2-compare \
  --baseline /absolute/path/to/baseline/weaver \
  --candidate /absolute/path/to/candidate/weaver \
  --fixture /absolute/path/to/damaged \
  --expected /absolute/path/to/clean \
  --seed set.par2 \
  --output /absolute/path/to/new-results \
  --threads 4
```

The runner measures clean verification, damaged verification and repair separately.
Each trial copies and hashes its input before timing, giving both sides the same
warm-cache policy. It sets `RAYON_NUM_THREADS` equally and alternates which binary
runs first. Copying and output hashing are excluded from elapsed time; native
process startup is included. No running application or service is used.

Each workload receives two warm-up pairs and ten measured pairs. Its geometric
mean candidate/baseline elapsed ratio and two-sided 95% Student-t interval are
computed on paired log ratios. An upper bound at most 1.02 passes. An inconclusive
interval gets twenty additional pairs; an established regression or a still
inconclusive result fails the command. Workload results are independent: a fast
repair cannot hide slower verification. Avoid concurrent builds, tests and other
heavy work during acceptance runs. Short runs may remain inconclusive even when
both binary paths identify the same executable.

`metadata.json` identifies the inputs. Every trial retains stdout, stderr and
`result.json`; `summary.json` records each completed workload and its paired ratios.
`completion.json` appears only after the final input/binary identity checks. Require
its `accepted` field as well as a successful command exit before accepting a run;
partial summaries alone are not acceptance evidence.
`/usr/bin/time` supplies user/system CPU seconds, peak RSS normalized to bytes,
and filesystem block-operation counts. These operation counts are not byte-I/O
counters; an OS-reported zero does not establish zero bytes read or written. Native exit status and expected output hashes must match on every trial.
Private input copies are removed after their trial; original inputs remain intact.

Repeat with representative large-block and many-small-file fixtures. These CLI
results cover only part of the integration acceptance matrix: retained-session
reassessment, selective downloads, direct/encrypted extraction, mixed-format
scheduling and end-to-end pipeline measurements still need their own evidence.
The helper uses existing xtask dependencies and adds no application dependency.
