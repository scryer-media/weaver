# Weaver 0.16.2 release notes

## Release validation fixes

- **Deterministic worker-capacity test.** Remove a scheduling race in the
  direct-unpack regression test that caused Windows CI to fail after a worker
  exited earlier than the test expected. A sequential gzip fixture keeps the
  worker behind the pool gate, preserving the capacity and draining-worker
  assertions.

## Build pipeline

- **Optimized test kernels.** Build the hot yEnc, checksum, RAR/7z codec,
  password-derivation, and PAR recovery crates at O3 in the test profile while
  retaining debug assertions and overflow checks. Ordinary test harnesses keep
  their fast debug builds.
- **Compiler caching across the Rust pipeline.** Enable sccache for every Rust
  compilation job, including Windows tests and Clippy, audit, ARM/SIMD checks,
  packaging, and upgrade-manifest generation.
- **R2 cache reuse for verified releases.** Use the existing Cloudflare R2 cache
  and compiler/target namespaces for verified release tags. Pull requests use
  local compiler caching. Disable incremental compilation so it cannot bypass
  sccache.
