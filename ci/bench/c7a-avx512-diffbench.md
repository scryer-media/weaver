# c7a AVX-512 differential correctness + performance bench (weaver-yenc)

Validate weaver's SIMD yEnc decode kernels — **especially the AVX-512 VBMI2
512-bit kernel** — for correctness and performance on **real** AMD Zen 4 silicon,
against the `rapidyenc` reference library.

> Authoring note: every command below is grounded in a real repo path + line.
> Anything not verifiable from the repo is marked `TODO(verify on box)`.

---

## 1. Why c7a.xlarge

`c7a` = AWS instances on **AMD EPYC 4th gen (Zen 4, "Genoa")**. On real silicon a
Zen 4 core exposes, all at once:

- `avx512f`, `avx512bw`, `avx512vl` (AVX-512 foundation + byte/word + vector-length)
- **`avx512vbmi`, `avx512vbmi2`** (the byte-permute / compress-store ISA the weaver
  512-bit decode kernel is built on)
- **`gfni`** (Galois-field affine — the PAR2 / Reed-Solomon GF16 accelerator)
- **`vpclmulqdq`**, `vaes`

None of our existing hardware has this combination on real silicon:

| Box | uarch | AVX-512 | VBMI2 | GFNI | VPCLMULQDQ |
|-----|-------|---------|-------|------|------------|
| SYLIX | Ryzen 5 3600 (Zen 2) | no | no | no | no |
| x86 bench box | Core Ultra 9 285H (Arrow Lake-H) | **no** | no | yes | yes |
| Mac | Apple Silicon | n/a (ARM/NEON) | n/a | n/a | n/a |
| **c7a.xlarge** | **EPYC Zen 4** | **yes** | **yes** | **yes** | **yes** |

Consequence: weaver's **AVX-512 VBMI2 decode kernel has only ever executed under
Intel SDE emulation**, never on physical silicon. The CI lane that covers it is
`rust-test-sde` / matrix leg `spr-gfni-avx512`, which runs every kernel test under
`sde64 -spr`
(`.github/workflows/deploy.yml:206-302`, features at `:218`, wrapper at `:255`).
SDE is a functional emulator: it can hide real-silicon issues (micro-arch corner
cases, real masking/permute behavior, alignment/store faults) and tells us nothing
about performance. **c7a replaces the `sde64 -spr` wrapper with a real Zen 4 core.**

This run also gives first-real-silicon coverage of the Zen 4 **GFNI + AVX-512 GF16
tier** in the rarpar workspace (stretch section §7) — the 285H has GFNI but no
AVX-512, so weaver's `gfni,avx512bw,avx512vl` GF16 path
(`/Users/jeremy/dev/scryer-media/rarpar/crates/weaver-reed-solomon/src/gf_simd.rs`)
has likewise never run natively.

---

## 2. What natural dispatch selects on c7a (and why no tier-forcing env exists)

`decode_kernel` (`engines/weaver-yenc/src/simd.rs:267-305`) picks the VBMI2 kernel
first when the CPU reports the full feature set:

```
engines/weaver-yenc/src/simd.rs:277-293
    if is_x86_feature_detected!("avx512vbmi2")
        && is_x86_feature_detected!("avx512vl")
        && is_x86_feature_detected!("avx512bw")
        && is_x86_feature_detected!("avx512f")
        && is_x86_feature_detected!("avx2")
    { return ... decode_kernel_avx512_vbmi2(...) }
```

On c7a all five predicates are true, so **natural runtime dispatch selects the
VBMI2 512-bit kernel** (`decode_kernel_avx512_vbmi2`, defined at
`engines/weaver-yenc/src/simd.rs:1006`+). No `RUSTFLAGS` target-feature pinning is
needed: the per-tier kernels are `#[target_feature(enable = "...")]` functions
(e.g. `engines/weaver-yenc/src/simd.rs:1006`) that are **always compiled** on
`x86_64` and chosen at runtime — this is exactly why the SDE lane can build once and
run every tier.

There is **no `WEAVER_YENC_FORCE_TIER` (or similar) runtime env var** — confirmed by
search; forcing is done at the test layer, not via env. Two mechanisms give
tier-explicit coverage regardless of what dispatch picks:

- **`forced_tier_kernels_match_scalar_with_line_hints`**
  (`engines/weaver-yenc/src/simd.rs:6129-6219`) calls **every** tier kernel directly
  — `ssse3`, `sse4.1`, `avx`, `avx2`, and **`avx512-vbmi2`** (the VBMI2 leg is at
  `:6167-6174`) — against the scalar oracle. Tiers whose features are absent
  self-skip (`available` flag). On c7a **all legs are available, so the VBMI2 leg
  runs on real silicon.**
- The per-block unit tests `avx512_vbmi2_block_*`
  (referenced in the SDE lane at `.github/workflows/deploy.yml:282-284`) run the
  VBMI2 block path directly.

So on c7a you get **both** natural dispatch (integration/differential tests, bench)
**and** explicit VBMI2 exercise (forced-tier + block unit tests) in one `cargo test`.

---

## 3. Two independent rapidyenc reference paths (do not conflate)

weaver checks itself against rapidyenc **two different ways**, each with its **own
discovery env var**:

### 3a. Differential correctness — compiles rapidyenc **from source**
`engines/weaver-yenc/tests/rapidyenc_decode_diff.rs`

- Discovery env: **`RAPIDYENC_ROOT`** (`tests/rapidyenc_decode_diff.rs:350`), default
  hardcoded mac path `/Users/jeremy/dev/supporting-codebases/rapidyenc`
  (`:352`) — **must be overridden on the box.**
- It compiles a tiny oracle with **`$CXX`** (default `c++`,
  `tests/rapidyenc_decode_diff.rs:146`) linking
  `rapidyenc.cc` + `src/decoder.cc` from `RAPIDYENC_ROOT`
  (`:154-158`); it requires those two files to exist (`:353`).
- The oracle **stubs out every ISA hook** (`decoder_set_vbmi2_funcs`, etc → no-ops;
  `cpu_supports_isa()==0`) at `tests/rapidyenc_decode_diff.rs:21-34`, so the
  reference is rapidyenc's **scalar** decoder. This is the correctness oracle:
  **weaver VBMI2 (dispatched) vs rapidyenc scalar**, byte-for-byte, across fixed +
  random + chunk-boundary cases.
- Tests: `rapidyenc_decode_ex_matches_local_oracle` (`:234`),
  `rapidyenc_incremental_matches_local_oracle` (`:264`),
  `rapidyenc_chunk_boundaries_match_local_oracle` (`:292`).
- If `RAPIDYENC_ROOT` is unset/invalid these tests **silently pass by skipping**
  (`Oracle::new()` returns `Ok(None)`, `:236-238`). **Setting `RAPIDYENC_ROOT` is
  what makes them actually run** — otherwise the c7a run proves nothing new here.
- No `build.rs` in the crate (`engines/weaver-yenc/` has none); nothing is linked at
  crate build time — the oracle is compiled at test runtime.

### 3b. Performance parity — dlopens the rapidyenc **shared library**
`engines/weaver-yenc/benches/rapidyenc_parity.rs`

- Discovery env: **`WEAVER_RAPIDYENC_LIB`** (`benches/rapidyenc_parity.rs:34`),
  pointing at `librapidyenc.so` (a cmake build).
- `libloading::Library::new` dlopens it (`:35`) and resolves
  `rapidyenc_decode_init`, `rapidyenc_crc_init`, `rapidyenc_decode`, `rapidyenc_crc`,
  plus optional `rapidyenc_decode_kernel` / `rapidyenc_crc_kernel`
  (`:44-59`). Because it's the **full** cmake lib (ISA hooks live), rapidyenc runs
  its **own AVX-512/VBMI2** decode on c7a → a true VBMI2-vs-VBMI2 A/B.
- Fixtures (`:190-196`): `realshape`, `clean`, `crlf_only`, `esc_only`, `dots_body`.
- Per-fixture it first **asserts byte parity** (`:205-210`) before timing, then emits
  Criterion benches `parity_weaver_decode_<fixture>` and
  `parity_rapidyenc_decode_<fixture>` (`:217-226`), plus CRC benches
  `parity_crc32fast_decoded` / `parity_rapidyenc_crc_decoded` (`:234-245`).
- If `WEAVER_RAPIDYENC_LIB` is unset the bench registers nothing and prints a skip
  (`:178-184`).
- **Must be the GNU target, not musl.** Static musl binaries can't `dlopen` a shared
  library; `WEAVER_RAPIDYENC_LIB` loading only works on
  `x86_64-unknown-linux-gnu`. (The release musl lanes at
  `.github/workflows/deploy.yml:344-434` are for shipping, not for this bench.)

Both env vars point into the same rapidyenc checkout, but **3a needs the source tree
and 3b needs the built `.so`** — the bootstrap produces both.

---

## 4. rapidyenc oracle build (cmake)

Reference: `https://github.com/animetosho/rapidyenc`.
`CMakeLists.txt` builds a shared lib by default (`option(DISABLE_SHARED ... OFF)` at
`/Users/jeremy/dev/supporting-codebases/rapidyenc/CMakeLists.txt:11`) via
`add_library(rapidyenc_shared SHARED ...)` (`:269`) with `OUTPUT_NAME rapidyenc`
(`:271`) → on Linux the artifact is **`build/librapidyenc.so`** (the dev mac produced
`build/librapidyenc.dylib`, confirmed present).

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
# => $RAPIDYENC_ROOT/build/librapidyenc.so
```

The checkout vendors `crcutil-1.0/`; the bootstrap runs
`git submodule update --init --recursive` after clone to be safe.
`TODO(verify on box)`: confirm `build/librapidyenc.so` (not a versioned soname like
`librapidyenc.so.1`) is the emitted file; if it's versioned, point
`WEAVER_RAPIDYENC_LIB` at the real file (the run script globs for it).
`TODO(verify on box)`: rapidyenc revision is left at the clone default (upstream
master). The dev-mac checkout is at an unknown local commit; the source-compiled
oracle (§3a) depends only on the stable `rapidyenc_decode_ex` /
`rapidyenc_decode_incremental` / `RapidYencDecoderState` API, so master should match,
but pin a specific rev here if a divergence shows up.

---

## 5. Toolchain / build recipe (mirrors CI, drops SDE)

Grounded in `.github/workflows/deploy.yml` + `rust-toolchain.toml`:

- Rust toolchain **1.96**, pinned in `rust-toolchain.toml:2` (and
  `.github/workflows/deploy.yml:20`). rustup auto-installs it on first `cargo`
  invocation inside `$WEAVER_DIR` because of the pin.
- **`nasm` is required** — `aws-lc-sys` (the TLS crypto backend pulled into the
  workspace build graph) needs it. CI installs it in every native build lane, e.g.
  `.github/workflows/deploy.yml:130-133` (rust-test-build) and `:229-232` (SDE lane).
- Build/test target: **`x86_64-unknown-linux-gnu`** (SDE lane builds with
  `--target x86_64-unknown-linux-gnu`, `.github/workflows/deploy.yml:248`). This is
  also the native host target on the instance.
- `--locked` everywhere (matches CI).
- SDE lane sets `CARGO_TARGET_..._RUSTFLAGS` to force target-features *only so older
  default CPUs can build the intrinsics* (`.github/workflows/deploy.yml:218,244-245`).
  **On c7a we omit those** — the `#[target_feature]` kernels compile on the plain
  target and dispatch at runtime (§2). The perf bench optionally uses
  `-C target-cpu=native` for a fair A/B vs cmake-`Release` rapidyenc (see run
  script `BENCH_RUSTFLAGS`).

Assume **Ubuntu 24.04**. System packages installed by the bootstrap:
`build-essential` (g++ for the §3a oracle + cc), `cmake`, `nasm`, `pkg-config`,
`git`, `curl`, `ca-certificates`.

---

## 6. How to run

```sh
# 0. one-time: clone weaver to ~/weaver (default WEAVER_DIR) before bootstrap
git clone <weaver-remote> ~/weaver

# 1. bootstrap: system deps + rustup(1.96) + rapidyenc source & .so + feature gate
cd ~/weaver
./ci/bench/c7a-bootstrap.sh

# 2. run: feature assert + full weaver-yenc test suite + parity bench, teed + summarized
./ci/bench/c7a-run.sh
```

Env overrides (both scripts, all have defaults):

| Var | Default | Meaning |
|-----|---------|---------|
| `WEAVER_DIR` | `~/weaver` | weaver checkout (already cloned) |
| `RAPIDYENC_ROOT` | `~/rapidyenc` | rapidyenc source tree (§3a) + cmake build dir |
| `RAPIDYENC_GIT` | `https://github.com/animetosho/rapidyenc.git` | clone URL |
| `WEAVER_RAPIDYENC_LIB` | `$RAPIDYENC_ROOT/build/librapidyenc.so` | dlopen target (§3b) |
| `TARGET` | `x86_64-unknown-linux-gnu` | cargo target |
| `BENCH_RUSTFLAGS` | `-C target-cpu=native` | RUSTFLAGS for the perf bench only |
| `RESULTS_DIR` | `$WEAVER_DIR/ci/bench/results/<UTC-timestamp>` | output dir (run script) |

---

## 7. (Stretch / optional) PAR2 GFNI + CRC on Zen 4 — rarpar workspace

The rarpar workspace (`/Users/jeremy/dev/scryer-media/rarpar`) carries GF16 /
Reed-Solomon tiers that gate on **GFNI + AVX-512**:

- `crates/weaver-reed-solomon/src/gf_simd.rs` — `#[target_feature(enable =
  "gfni,avx2")]` and `#[target_feature(enable = "gfni,avx512bw,avx512vl")]` GF16
  multiply-accumulate kernels, dispatched via `is_x86_feature_detected!` on
  `gfni` + `avx512bw` + `avx512vl`.
- `crates/weaver-par2/src/repair.rs` — GFNI/AVX2 repair path.

The `gfni,avx512bw,avx512vl` leg is the one **no real box has ever run** (285H = GFNI
but no AVX-512). c7a validates it for the first time.

Grounded, runnable **internal** Criterion benches (self-contained, no external tool):

```sh
# PAR2 repair (weaver-par2), bench name grounded at
#   rarpar/crates/weaver-par2/Cargo.toml:54-57 ("par2_repair", harness=false)
#   scenario filter env: WEAVER_PAR2_BENCH_SCENARIOS
#   (rarpar/crates/weaver-par2/benches/par2_repair.rs top-of-file)
cargo bench -p weaver-par2 --bench par2_repair --target x86_64-unknown-linux-gnu

# unrar hotspots (weaver-unrar), bench name grounded at
#   rarpar/crates/weaver-unrar/Cargo.toml:100-102 ("archive_hotspots", harness=false)
cargo bench -p weaver-unrar --bench archive_hotspots --target x86_64-unknown-linux-gnu
```

(Run these from the rarpar checkout root; it is a separate repo, not part of
`$WEAVER_DIR`.)

**External A/B (par2cmdline-turbo, unrar 7.x)** — the head-to-head comparisons
referenced in `scryer-docs/plans/124-weaver-yenc-x86-decode-inline-plan.md` and
`scryer-docs/weaver_comp_.md` are **not driven by a repo bench target I could ground
to an exact command**. `TODO(verify on box)`: locate the external-A/B harness (the
par2cmdline-turbo / unrar-7.x oracles live under
`/Users/jeremy/dev/supporting-codebases/par2cmdline-turbo` and `.../unrar`) and add
its concrete invocation here before running — do not invent flags. The internal
Criterion benches above are the grounded, safe-to-run stretch coverage.

The c7a-run.sh script does **not** run §7 automatically (rarpar is a separate repo);
it's documented here as a follow-up to run manually once the rarpar checkout is on
the box.

---

## 8. What a PASS looks like

1. **Feature gate green.** `c7a-run.sh` (and bootstrap) assert `/proc/cpuinfo`
   exposes: `avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes`.
   Missing any ⇒ **wrong instance family** (not a real c7a / Zen 4) ⇒ hard abort with
   a clear message. This is the precondition that makes the whole run meaningful.
2. **Full weaver-yenc suite green on real silicon**, with the VBMI2 tier actually
   exercised:
   - `forced_tier_kernels_match_scalar_with_line_hints` passes with the
     `avx512-vbmi2` leg **available and run** (§2).
   - `avx512_vbmi2_block_*` unit tests pass.
   - `dispatch_kernel_matches_scalar_*` pass (dispatch = VBMI2 here).
   - The three **`rapidyenc_*_matches_local_oracle`** differential tests **actually
     run** (not skip) because `RAPIDYENC_ROOT` is set, and report non-zero checked
     counts (`eprintln!` "…differential cases: N", e.g.
     `tests/rapidyenc_decode_diff.rs:259,287,344`).
3. **Parity bench**: every fixture's pre-timing byte-parity assert passes
   (`benches/rapidyenc_parity.rs:205-210`) and Criterion emits times for all
   fixtures. Fill in:

   | fixture | weaver decode | rapidyenc decode | ratio (w/r) |
   |---------|---------------|------------------|-------------|
   | realshape | _fill_ | _fill_ | _fill_ |
   | clean | _fill_ | _fill_ | _fill_ |
   | crlf_only | _fill_ | _fill_ | _fill_ |
   | esc_only | _fill_ | _fill_ | _fill_ |
   | dots_body | _fill_ | _fill_ | _fill_ |
   | crc (realshape) | _fill_ (crc32fast) | _fill_ (rapidyenc_crc) | _fill_ |

   The bench also prints `rapidyenc kernels: decode=<n> crc=<n>`
   (`benches/rapidyenc_parity.rs:185-188`) — on c7a the decode kernel id should be
   rapidyenc's AVX-512/VBMI2 tier, confirming the A/B is VBMI2-vs-VBMI2. Record it.

---

## 9. Triage: a differential FAILS on real silicon but passed under SDE

That is the headline risk this run exists to catch: **a real VBMI2 bug that Intel SDE
did not model.** If any `forced_tier_* (avx512-vbmi2 leg)`, `avx512_vbmi2_block_*`,
`dispatch_kernel_*`, or `rapidyenc_*_matches_local_oracle` test fails on c7a while the
SDE lane is green, capture the first divergent byte:

- The crate's byte-level divergence diagnostic is
  **`dump_avx2_divergence`** (`engines/weaver-yenc/src/simd.rs:6088`, `#[ignore]`).
  Despite the `avx2` in its name it compares the **dispatch-selected** kernel
  (`run_kernel_whole(..., scalar=false, ...)` → `decode_kernel`,
  `engines/weaver-yenc/src/simd.rs:5813-5821`) against the scalar oracle — so **on
  c7a it dumps VBMI2-vs-scalar divergence**, which is exactly what you want. There is
  no separate `dump_avx512_divergence`; this one is it on Zen 4.

  Run it:
  ```sh
  cargo test -p weaver-yenc --locked --target x86_64-unknown-linux-gnu \
    -- --ignored --nocapture dump_avx2_divergence
  ```
  It prints `DIVERGE case=… first_diff_out=… simd_out[..]=… ref_out[..]=…`
  (`engines/weaver-yenc/src/simd.rs:6111-6120`) — the first byte offset where VBMI2
  and scalar disagree, with surrounding context and the kernel state.

- For the rapidyenc differential failures, the assert messages already print the
  offending hex input and raw/state (`tests/rapidyenc_decode_diff.rs:248-256,
  307-313`) — reproduce that single case directly against the scalar oracle.

- Cross-check the same input under SDE to confirm SDE-vs-silicon divergence (proving
  it's a real-silicon-only bug, not a logic bug): rebuild the test binary and run the
  failing test name under `sde64 -spr -- <bin> <test>` exactly as
  `.github/workflows/deploy.yml:251-298` does. Same failure under SDE ⇒ ordinary
  logic bug; **passes under SDE, fails on c7a ⇒ genuine VBMI2 real-silicon bug** —
  file it with the `DIVERGE` dump.
