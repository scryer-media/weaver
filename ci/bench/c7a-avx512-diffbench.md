# c7a AVX-512 differential correctness + performance bench (weaver-yenc + rarpar)

Validate weaver's SIMD yEnc decode kernels — **especially the AVX-512 VBMI2
512-bit kernel** — plus the CRC port gate and rarpar's GFNI+AVX-512 GF16 tier,
for correctness and performance on **real** AMD Zen 4 silicon, against the
`rapidyenc` reference library.

> Authoring note: every command below is grounded in a real repo path + line,
> verified against the working tree on 2026-08-11. Anything not verifiable from
> the repo is marked `TODO(verify on box)`.

**No time or cost estimates appear in this runbook — by standing rule.** The run
*measures* its own wall time per phase and records it in
`metadata.json.phase_timings_seconds`, and `summary.txt` prints the same table.
Use those numbers; do not predict them. The bootstrap arms a dead-man
`shutdown -h +120` as a safety net, not as a schedule — it only terminates if
the instance's shutdown-behavior is `terminate` (§11).

---

## 1. Why c7a.xlarge

> ### Instance type is LOCKED to `c7a.xlarge`
>
> **Changing the instance size requires the project owner's explicit permission.**
> Not the
> family, not the size, not "just this once for a faster build" — the build is
> not on the box any more anyway (§6). Both scripts assume it and the CPU gate
> aborts on anything that is not a real Zen 4.
>
> Two supporting facts, both checked:
>
> - **Account vCPU quota**: the Standard on-demand limit is **16 vCPU**.
>   `c7a.xlarge` is **4 vCPU**, so a single instance fits with plenty of
>   headroom — but it also means a careless jump to, say, `c7a.8xlarge`
>   (32 vCPU) would be *refused by the quota*, not merely expensive.
> - **No performance argument for a bigger box**: the bench lanes here are
>   single-threaded, so extra vCPUs would add noise, not throughput.


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
| SYLIX (Windows) | Ryzen 5 3600 (Zen 2) | no | no | no | no |
| codex-x86 | Core i7-1240P (Alder Lake) | no | no | no | yes |
| codex-x86-2 | Core Ultra 9 285H (Arrow Lake-H) | **no** | no | yes | yes |
| Mac | Apple Silicon (M5 Max) | n/a (ARM/NEON) | n/a | n/a | n/a |
| **c7a.xlarge** | **EPYC Zen 4** | **yes** | **yes** | **yes** | **yes** |

Three consequences, and they are the whole reason for the run:

1. weaver's **AVX-512 VBMI2 decode kernel has only ever executed under Intel SDE
   emulation**, never on physical silicon. The CI lane that covers it is
   `rust-test-sde` / matrix leg `spr-gfni-avx512`, which runs a hand-listed set of
   kernel tests under `sde64 -spr` (`.github/workflows/deploy.yml:318-414`; matrix
   leg at `:331-333`, SDE wrapper at `:365-373`). SDE is a functional emulator: it
   can hide real-silicon issues (micro-arch corner cases, real masking/permute
   behavior, alignment/store faults) and tells us nothing about performance.
   **c7a replaces the `sde64 -spr` wrapper with a real Zen 4 core.**
2. weaver's **VPCLMUL CRC port gate now excludes `avx512vl`** (§4). Every box we
   own is on the *enabled* side of that gate or lacks VPCLMULQDQ entirely — none
   can prove the *disabled* side. c7a is the first machine that can.
3. rarpar's **`gfni,avx512bw,avx512vl` GF16 tier** has likewise never run
   natively (285H has GFNI but no AVX-512). §8 makes that a mandatory phase, not
   a stretch goal.

---

## 2. What natural dispatch selects on c7a (and how tiers are forced)

The x86 tier decision is memoized once per process in `dispatch_x86_decode_kernel`
(`engines/weaver-yenc/src/simd/mod.rs:357-393`), called from `decode_kernel`
(`:277-341`, dispatch call at `:302-314`):

```
engines/weaver-yenc/src/simd/mod.rs:362-368
        if is_x86_feature_detected!("avx512vbmi2")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx2")
        {
            decode_kernel_avx512_vbmi2 as DecodeKernelFn
```

On c7a all five predicates are true, so **natural runtime dispatch selects the
VBMI2 512-bit kernel** (`decode_kernel_avx512_vbmi2`,
`engines/weaver-yenc/src/simd/x86_avx512.rs:4-5`). No `RUSTFLAGS` target-feature
pinning is needed: the per-tier kernels are `#[target_feature(enable = "...")]`
functions that are **always compiled** on `x86_64` and chosen at runtime — this is
exactly why the SDE lane can build once and run every tier.

> **Layout note.** The SIMD code is no longer one `simd.rs`. It is a module
> directory: `engines/weaver-yenc/src/simd/{mod,scalar,x86_common,x86_sse,
> x86_avx2,x86_avx512,neon,tests}.rs` (declarations at `mod.rs:500-515`). Any
> older note citing `engines/weaver-yenc/src/simd.rs:<line>` is stale.

There is **no `WEAVER_YENC_FORCE_TIER` (or similar) runtime env var** — confirmed
by search; forcing is done at the test layer. Three mechanisms give tier-explicit
coverage regardless of what dispatch picks:

- **`forced_tier_kernels_match_scalar_with_line_hints`**
  (`engines/weaver-yenc/src/simd/tests.rs:1618-1707`) calls `ssse3`, `sse4.1`,
  `avx`, `avx2` and **`avx512-vbmi2`** (leg at `:1655-1663`) against the scalar
  oracle, hint-less and hinted. It only ever drives
  `(dot=true, preserve=false, search_end=false)`.
- **`forced_tier_kernels_match_scalar_in_production_shape`**
  (`engines/weaver-yenc/src/simd/tests.rs:2113-2190`) — **new since this doc was
  first written, and the important one.** It drives the full tier list from
  `forced_tier_kernels()` (`:2051-2102`, which adds an `sse2` leg the other test
  lacks; VBMI2 leg at `:2083-2091`) in the *production* decode shape:
  `(dot=true, preserve=true, search_end=true)` plus the
  `(dot=true, preserve=true, search_end=false)` control leg (`:2119`), over the
  `production_shape_bodies()` corpus (`:1975-2036`: window-edge sweeps of
  `\r\n=y` / `\r\n.\r\n` / `\r\n.=y`, mid-line `=y`, dot-stuffed line starts,
  escape runs). It also checks the written-back `DecodeState` (`:2166-2185`), not
  just the bytes.

  On c7a **its `avx512-vbmi2` leg is the first execution of the 512-bit
  `searchEnd` probe on real silicon.** That probe lives at
  `engines/weaver-yenc/src/simd/x86_avx512.rs:243-306` (with-stuffed-dot arm
  `:243-288`, without-dot arm `:290-307`).
- The per-block unit tests `avx512_vbmi2_block_*`
  (`engines/weaver-yenc/src/simd/tests.rs:1016,1042,1064`; invoked in the SDE
  lane at `.github/workflows/deploy.yml:393-395`) run the VBMI2 block path
  directly.

### 2a. Why the test pass must run **debug and release**

Inside the VBMI2 `searchEnd` probe there is a bit-identity assertion:

```
engines/weaver-yenc/src/simd/x86_avx512.rs:272
                            debug_assert_eq!(m34eqy & !(1u64 << 63), m3eqy >> 1);
```

It proves the direct `+3`/`+4` mask construction agrees with the oracle's
per-parity `match34EqY` derivation (rationale comment at `:253-269`). Being a
`debug_assert_eq!`, it is **compiled out in release**. So:

- **debug pass** — exercises the bit-identity assertion on real ZMM masks.
- **release pass** — exercises the codegen shape production actually ships, with
  the assertion gone.

Neither pass subsumes the other. `c7a-run.sh` runs both.

> Note the SDE lane does **not** invoke
> `forced_tier_kernels_match_scalar_in_production_shape` (its hand-listed test
> names stop at `forced_tier_kernels_match_scalar_with_line_hints`,
> `.github/workflows/deploy.yml:397`), and the ordinary
> `rust-test-regular` runner has no AVX-512, so that test's VBMI2 leg self-skips
> there. c7a is therefore the **only** place it has ever run, emulated or not.
> (CI gap, not fixed here — `.github` is out of scope for this change.)

---

## 3. Two independent rapidyenc reference paths (do not conflate)

weaver checks itself against rapidyenc **two different ways**, each with its **own
discovery env var**. A third env var exists for an in-process static link.

### 3a. Differential correctness — compiles rapidyenc **from source**
`engines/weaver-yenc/tests/rapidyenc_decode_diff.rs`

- Discovery env: **`RAPIDYENC_ROOT`** (`tests/rapidyenc_decode_diff.rs:431`).
  It must point at a rapidyenc checkout on the box; the two required files are
  checked at `:435`.
- It compiles a tiny oracle with **`$CXX`** (default `c++`,
  `tests/rapidyenc_decode_diff.rs:146`) linking `rapidyenc.cc` + `src/decoder.cc`
  from `RAPIDYENC_ROOT` (`:155-156`).
- The oracle **stubs out every ISA hook** (`decoder_set_vbmi2_funcs`, etc → no-ops;
  `cpu_supports_isa()==0`) at `tests/rapidyenc_decode_diff.rs:21-34`, so the
  reference is rapidyenc's **scalar** decoder. This is the correctness oracle:
  **weaver VBMI2 (dispatched) vs rapidyenc scalar**, byte-for-byte.
- **Four** test families (was three when this doc was first written):

  | test | line | cases on a green run |
  |------|------|----------------------|
  | `rapidyenc_decode_ex_matches_local_oracle` | `:235` | **5 978** |
  | `rapidyenc_incremental_matches_local_oracle` | `:269` | **2 989** |
  | `rapidyenc_chunk_boundaries_match_local_oracle` | `:301` | **3 997** |
  | `rapidyenc_simd_chunk_boundaries_match_local_oracle` | `:367` | **41 986** |
  | | | **54 950 total (~55k)** |

- **The corpus now reaches SIMD.** It used to top out at 95 bytes — under the
  128-byte flat-kernel gate — so the C oracle never validated a weaver SIMD
  window at all. It now carries `simd_fixed_cases()` (`:678-731`, 39 bodies past
  the gate, sweeping each terminator form across window-edge offsets 254..=258),
  `simd_random_cases()` (`:737-765`, lengths 129..=4096 biased to ±4 of a 64-byte
  boundary), a ~600-byte `simd_chunk_sweep_case()` (`:664-668`) split
  exhaustively at every offset, and `sparse_split_offsets()` (`:771-781`, every
  61st and 64th offset plus the first/last eight) for the long cases.
  **On c7a dispatch selects the VBMI2 kernel, so all ~55k cases validate the
  512-bit kernel against a scalar C oracle.**
- If `RAPIDYENC_ROOT` is unset/invalid these tests **silently pass by skipping**
  (`Oracle::new()` returns `Ok(None)`, `:132-134`; skip notes at `:432` and
  `:438-441`). **Setting `RAPIDYENC_ROOT` is what makes them actually run** —
  otherwise the c7a run proves nothing new here. No CI lane sets it.
- Each test prints its case count with `eprintln!` (`:263,295,352,425`) and
  asserts `checked > 0` (`:264,296,353,426`). **`eprintln!` from a passing test is
  captured by libtest** — the run script therefore passes `-- --nocapture`, or
  those lines never reach the log.

### 3b. Performance parity — dlopens the rapidyenc **shared library**
`engines/weaver-yenc/benches/rapidyenc_parity.rs`

- Discovery env: **`WEAVER_RAPIDYENC_LIB`** (`benches/rapidyenc_parity.rs:34`),
  pointing at `librapidyenc.so` (a cmake build).
- `libloading::Library::new` dlopens it (`:35`) and resolves
  `rapidyenc_decode_init`, `rapidyenc_crc_init`, `rapidyenc_decode`, `rapidyenc_crc`,
  plus optional `rapidyenc_decode_kernel` / `rapidyenc_crc_kernel` (`:44-59`).
  Because it's the **full** cmake lib (ISA hooks live), rapidyenc runs its **own
  AVX-512/VBMI2** decode on c7a → a true VBMI2-vs-VBMI2 A/B.
- Fixtures (`:190-196`): `realshape`, `clean`, `crlf_only`, `esc_only`, `dots_body`.
- Per fixture it asserts, **before** timing:
  - decoded **length** parity (`:205-208`) and **byte** parity (`:209-213`);
  - **decoded-CRC parity** (`:217-223`) — `weaver_yenc::crc::Crc32` over weaver's
    output vs `rapidyenc_crc` over rapidyenc's output, `assert_eq!` at `:223`.
    **This is new.** The previous revision of this doc claimed CRC parity was
    checked; it was not — the two CRC benches only *timed* the implementations and
    never compared them. The equality claim now exists in code, and each fixture
    prints `parity ok [<name>]: N encoded -> M decoded, crc=0x…` (`:224-228`).
- Then it emits Criterion benches `parity_weaver_decode_<fixture>` /
  `parity_rapidyenc_decode_<fixture>` (`:230-239`) and the CRC pair
  `parity_crc_fast_decoded` / `parity_rapidyenc_crc_decoded` (`:247-258`).
- If `WEAVER_RAPIDYENC_LIB` is unset the bench registers nothing and prints a skip
  (`:178-184`).
- **Must be the GNU target, not musl.** Static musl binaries can't `dlopen` a
  shared library. (The x86_64 musl release lanes at
  `.github/workflows/deploy.yml:454-546` are for shipping, not for this bench.)
- **No host tuning on either side — by design.** Earlier revisions of this
  runbook built the weaver bench with `-C target-cpu=native` (`BENCH_RUSTFLAGS`)
  and argued it made for a fairer A/B against a cmake-`Release` rapidyenc. **That
  is gone in v2 and should not come back.** Both sides are now prebuilt with
  plain flags: weaver from the bundle (`rustflags: ""`), rapidyenc as a generic
  cmake `Release` `.so`. Tuning only weaver would bias the comparison, and it
  would buy nothing where it matters anyway — the AVX-512 kernels are
  `#[target_feature]`-compiled and runtime-dispatched regardless of `target-cpu`
  (§2, §6). There is also no cargo on the box to rebuild with. If
  `BENCH_RUSTFLAGS` / `RARPAR_BENCH_RUSTFLAGS` / `RUSTFLAGS` are set in the
  environment the run script **warns and ignores them** rather than pretending
  they took effect.

The bench also prints `rapidyenc kernels: decode=<n> crc=<n>`
(`benches/rapidyenc_parity.rs:185-188`) as **decimal** `RYKERN_*` ids
(`rapidyenc/rapidyenc.h:24-42`). Decode table for grading:

| id (dec) | id (hex) | `RYKERN_*` |
|---|---|---|
| 0 | 0x0 | `GENERIC` |
| 256 | 0x100 | `SSE2` |
| 512 | 0x200 | `SSSE3` |
| 897 | 0x381 | `AVX` |
| 1027 | 0x403 | `AVX2` |
| **1539** | **0x603** | **`VBMI2`** ← expected on c7a |
| 832 | 0x340 | `PCLMUL` (crc) |
| **1088** | **0x440** | **`VPCLMUL`** (crc) ← expected on c7a |

`decode=1539 crc=1088` is the confirmation that the A/B is VBMI2-vs-VBMI2 and
that rapidyenc's CRC is on its VPCLMUL tier. Anything else ⇒ the `.so` was built
without the AVX-512 groups; stop and rebuild.

### 3c. In-process static link (not used by this run, but the crate *does* have a build.rs)

**Correction:** the previous revision of this doc asserted "No `build.rs` in the
crate … nothing is linked at crate build time". That is now false.
`engines/weaver-yenc/build.rs` exists (81 lines). When **`WEAVER_RAPIDYENC_SRC`**
points at a rapidyenc checkout (`build.rs:12,15`) it compiles rapidyenc's decode
sources + `rapidyenc_shim.cc` via `cc` and emits `cfg(rapidyenc_linked)`
(`:11,40,79`), including a VBMI2 group built with
`-mavx512vbmi2 -mavx512vl -mavx512bw` (`:59-63`). When the env var is unset it is
a **complete no-op** (`:15-17`) — no rapidyenc dependency, no behavior change.

This run leaves `WEAVER_RAPIDYENC_SRC` **unset**: the dlopen A/B (§3b) is the
sanctioned parity measurement, and a static link would change weaver's own
codegen. Do not set it. It is documented here only so nobody re-derives the stale
"no build.rs" claim.

Summary of the three env vars — all can point at the *same* checkout, but they do
different things:

| var | consumer | needs |
|-----|----------|-------|
| `RAPIDYENC_ROOT` | §3a diff tests | the **source tree** (`rapidyenc.cc`, `src/decoder.cc`) |
| `WEAVER_RAPIDYENC_LIB` | §3b parity bench | the **built `.so`** |
| `WEAVER_RAPIDYENC_SRC` | `build.rs` static link | source tree — **leave unset here** |

The bootstrap produces the first two.

---

## 4. C1 proof: the VPCLMUL CRC port gate on Zen 4 (NEW — read before running)

weaver carries its own 2x256-bit VPCLMULQDQ CRC folding port
(`engines/weaver-yenc/src/crc.rs:101-308`). Its availability gate is
`x86_vpclmul::available()` (`:108-125`), and it now ends with an **exclusion**:

```
engines/weaver-yenc/src/crc.rs:123
                && !is_x86_feature_detected!("avx512vl")
```

Rationale, verbatim in the code at `:115-122`: `crc-fast` only enables its own
VPCLMULQDQ kernel when `avx512vl` is also present, and that kernel is a
**4x512-bit ZMM fold (256 B/iter, ternary-logic XOR3)** which beats weaver's
2x256 port. Standing aside on those parts (Zen 4/5, AVX-512 Intel server) leaves
the faster kernel in place. weaver's port exists *solely* for
VPCLMULQDQ-without-AVX512VL CPUs (Alder Lake → Arrow Lake), where `crc-fast`
drops all the way to its 128-bit SSE tier.

**c7a is the first machine that can prove the disabled side of that gate.**
Expected behavior on Zen 4, and exactly what to grep for:

1. `crc32_forced_vpclmul_matches_crc_fast` (`crc.rs:432-462`) calls
   `x86_vpclmul::test_update_forced` (`:305-307`), which is
   `available().then(...)`. On c7a `available()` is **false** (avx512vl present),
   so it returns `None` and the test prints, then returns:

   ```
   skipping crc32_forced_vpclmul_matches_crc_fast: VPCLMUL port unavailable on this CPU
   ```

   (`crc.rs:450-452`.) **That skip line IS the proof the gate works — record it
   verbatim in the results.** Without the line the test reports `ok` while
   executing nothing, which is indistinguishable from real coverage in a log;
   the line was added for precisely this run. `c7a-run.sh` grep-asserts it.

   Because the line comes from a **passing** test, it only reaches the log under
   `-- --nocapture`. The run script always passes that.

2. `crc32_mixed_size_interleaved_updates` (`crc.rs:382-428`) drives six
   interleaved update sequences whose sizes straddle `VPCLMUL_MIN_UPDATE = 256`
   (`:29`) in both directions, checking every prefix against
   `crc_fast::crc32_iso_hdlc`. With `use_vpclmul == false` on c7a, **every**
   update — including the 4096-byte ones — goes straight to
   `self.hasher.update(data)` (`:70`), i.e. **crc-fast's 4x512 ZMM tier**. That
   test is the functional confirmation that handing the large chunks to crc-fast
   on this part is correct.

**What this run does *not* prove:** with the gate disabled, weaver's `folded:
Option<u32>` streak-batching path (`crc.rs:44-71`) is inert on c7a. Its
hand-off behavior was validated on codex-x86 (ADL) where `available()` is true.
c7a proves the *gate*, ADL proves the *path*. Record both facts together or the
CRC story reads as half-tested.

**Precondition — the box must run a rev that contains all of this.** The C1 gate,
the skip `eprintln!`, and the §3b CRC parity assert landed in **`a3e3f68d`**
("yenc updates, CI fixes"); they were uncommitted while this runbook was being
written, so any checkout older than that cannot prove a thing here and will still
report a cheerful green. `c7a-run.sh` grep-checks the *source on the box* for all
of these markers before it runs anything and aborts if the tree is stale — keep
that gate even though the markers are now committed, because the box is populated
from the corpus image (§7a) and an image built before an increment landed would
otherwise sail through green. Cross-check against `REVISION.json` in
`revisions.txt`.

---

## 5. rapidyenc oracle build (cmake)

Reference: `https://github.com/animetosho/rapidyenc`.
`CMakeLists.txt` builds a shared lib by default
(`option(DISABLE_SHARED "Don't build shared library" OFF)`, `:11`) via
`add_library(rapidyenc_shared SHARED ...)` (`:269`) with `OUTPUT_NAME rapidyenc`
(`:271`) → on Linux the artifact is **`build/librapidyenc.so`** (the dev mac
produced an unversioned `build/librapidyenc.dylib`, confirmed present).

**This build no longer happens on the c7a box.** `librapidyenc.so` is produced on
the builder and ships at `/corpus/prebuilt/lib/librapidyenc.so` (§6a); the
bootstrap only checks that it exists and that `ldd` resolves it cleanly. The
recipe is recorded here because it is what the *builder* runs:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
# => build/librapidyenc.so
```

The checkout vendors `crcutil-1.0/` in-tree (no `.gitmodules`), so no submodule
initialization is needed — which matters, because `.git` does not ship.

Revision: **there is no clone step.** rapidyenc arrives in the corpus image
(§7a) as a clean tree pinned at **`27f435a`** ("Add build options to skip
building tool/shared components"), i.e. rapidyenc v1.1.1-10-g27f435a — the rev
every prior weaver-vs-rapidyenc number was taken against. Pinning it in the
image is what keeps the c7a numbers comparable to the M5/ADL/Zen2 baselines in
§9 instead of drifting with upstream master.

`crcutil-1.0/` is vendored in-tree upstream (no `.gitmodules`), so nothing needs
initializing either — which matters, because `.git` does not ship. The bootstrap
prints the rev it found from `REVISION.json` and warns if `crcutil-1.0/` is
absent; confirm it reads `27f435a` before trusting any A/B number.

---

## 6. Build model — prebuilt binaries, no toolchain on the box

**Nothing is compiled from Rust on the c7a instance.** No rustup, no cargo, no
`rust-toolchain.toml` resolution, no `aws-lc-sys`, no rapidyenc cmake build. All
executables are produced ahead of time on the local Linux builder and ship in
the corpus image as `/corpus/prebuilt/`.

Why this is safe — and why it is *better* than building on the box:

- The builder (**codex-x86-2**, Ubuntu 24.04) matches the AMI's glibc, so the
  binaries load unmodified. The bootstrap compares `BUILDINFO.json`'s `glibc`
  against the host's and warns if that assumption ever breaks.
- The build uses the plain `x86_64-unknown-linux-gnu` target with
  **`rustflags: ""` — no `target-cpu`, no `target-feature`**. Every kernel tier
  is therefore compiled in and selected by *runtime dispatch*
  (`simd/mod.rs:357-393`), which is the entire point of the run: pinning would
  have decided at build time what this run exists to measure on real silicon.
  The builder has no AVX-512 of its own, and that does not matter for the same
  reason the SDE lane can build once and run every tier (§2).
- It removes a whole failure surface. Building weaver's release test binary
  on-box meant `lto = "fat"` + `codegen-units = 1`
  (`weaver/Cargo.toml:100-104`); that step is simply gone.

### 6a. Bundle layout

```
/corpus/prebuilt/
  bin/<id>            one executable per manifest entry
  lib/librapidyenc.so the parity bench's dlopen target (§3b)
  manifest.json       [{id, kind, repo, crate, profile, orig_name, needs_env}, …]
  BUILDINFO.json      {rustc, toolchains, builder, glibc, weaver_rev,
                       rarpar_rev, rapidyenc_rev, rustflags:"", built_at_utc,
                       builder_rarpar_root}
```

The ten binary ids:

| id | kind | repo | notes |
|---|---|---|---|
| `weaver-yenc-lib-debug` / `-release` | test | weaver | the `simd::tests` + `crc::tests` suites |
| `weaver-yenc-diff-debug` / `-release` | test | weaver | the four §3a differential families |
| `weaver-yenc-bench-parity` | bench | weaver | §3b, 12 lanes |
| `weaver-yenc-bench-decode-simd` | bench | weaver | §9e, 11 lanes |
| `rarpar-reedsolomon-test` | test | rarpar | GF16 tiers (§8) |
| `rarpar-par2-test` | test | rarpar | |
| *(~10 more par2 integration test binaries)* | test | rarpar | ids follow `orig_name`; **not** enumerated — see below |
| `rarpar-bench-par2-repair` | bench | rarpar | |
| `rarpar-bench-archive-hotspots` | bench | rarpar | |

Both debug **and** release weaver binaries ship because they are not
interchangeable — see §2a: the VBMI2 `searchEnd` probe's `debug_assert_eq!`
(`simd/x86_avx512.rs:272`) exists only in the debug one.

**The rarpar test binaries are not a fixed pair.** A `-p`-scoped build emits one
binary per test target, so par2 alone contributes about ten integration binaries
beyond the two headline ids, and the bundle carries **12 rarpar test binaries**
in total. The run script therefore *iterates every manifest entry with
`kind == "test"` and `repo == "rarpar"`* rather than naming ids — a bundle that
gains or loses a test target needs no script edit. Four of those binaries
legitimately report **0 tests** under default features (they are slow-tests
gated); a 0-test binary that exits 0 is logged as **gated-empty** and counted,
never treated as a failure. The summary prints the gated-empty count precisely so
a wall of zeros can never be mistaken for "everything ran".

**Two copies of the rapidyenc oracle source ship.** The canonical one is the
corpus tree at `$RAPIDYENC_ROOT` (`~/rapidyenc`) — that is what `RAPIDYENC_ROOT`
points at and what the §3a diff binaries actually compile against at runtime. The
bundle contains a second copy purely as a builder-side sanity artifact. Do not
point `RAPIDYENC_ROOT` at the bundle copy; the run must exercise the same tree
whose `REVISION.json` is recorded in `metadata.json`.

**Prebuilt binaries cannot run doctests.** `cargo test` would also compile and run
the crate's documentation examples; a bare libtest binary has no such stage. Test
totals here are therefore *expected* to differ from a full `cargo test` on a dev
box — that is the model working as intended, not missing coverage. Expected for
the weaver lib suite: **156 passed / 0 failed / 2 ignored per profile** (the 2
ignored are the `#[ignore]` diagnostics, §10).

### 6b. What the box still needs a compiler for

`g++`. Not for Rust — the §3a differential test binaries compile their C oracle
at **runtime** via `$CXX` (`tests/rapidyenc_decode_diff.rs:146-159`). That is why
`build-essential` remains in the bootstrap's package list, and why the run script
exports `CXX=g++`. The full on-box package list is now just
`build-essential jq tar curl ca-certificates` (plus `docker.io` + `awscli` for
the corpus pull). Gone: `cmake`, `nasm`, `pkg-config`, `git`, rustup.

### 6c. Staleness is a hard failure

The prebuilt model's one real hazard is a bundle built from a different revision
than the source shipped beside it — every downstream gate would then be
measuring something other than what the operator believes, and would report
green. So the bootstrap **hard-fails** when `BUILDINFO.json`'s `weaver_rev` /
`rarpar_rev` / `rapidyenc_rev` do not each equal the corresponding tree's
`REVISION.json` `rev`. The run script re-checks the same thing, because it can
be invoked on its own. Do not work around this — rebuild the bundle.

### 6d. Caveat the bundle build must satisfy — baked fixture paths

rarpar's test and bench sources resolve fixtures through
`env!("CARGO_MANIFEST_DIR")`, which is a **compile-time** constant:
`crates/weaver-unrar/benches/archive_hotspots.rs:4`,
`crates/weaver-par2/tests/support/benchmark_support.rs:217`, and ~15 more across
those two crates. A binary built elsewhere therefore looks for its fixtures at
the *builder's* absolute path.

weaver-yenc is unaffected — it has no `env!("CARGO_MANIFEST_DIR")` anywhere in
`src/`, `tests/` or `benches/`, so its binaries are genuinely relocatable.

Cleanest fix is at build time: build rarpar at the same absolute path the corpus
extracts to (`$CORPUS_DEST/rarpar`, i.e. `~/rarpar`). Failing that, the bootstrap
recovers automatically by symlinking the builder's root to the extracted tree —
one symlink repairs every lookup at once — resolving that root in two steps:

1. **`BUILDINFO.json`'s `builder_rarpar_root`**, which the bundle build now
   records. This is authoritative and is used whenever present.
2. **Fallback string scan**, only if that field is absent. This is filtered
   hard, because the binaries are full of paths that merely *look* like
   candidates: a weaver bench binary alone carries ~123 dependency panic-path
   literals under the builder's Cargo registry. The scan therefore considers
   only binaries whose manifest `repo` is `rarpar`, **rejects any path
   containing `/.cargo/registry/`**, and accepts a candidate only when it sits
   under `…/rarpar/crates/` and the derived root ends in `/rarpar`. On a
   representative binary that reduces 125 raw regex matches to the 1 correct
   root.

If it cannot create the path it aborts, rather than letting the rarpar phase fail
obscurely on "malformed archive".


---

## 7. How to run

### 7a. Get the sources onto the box — **pre-pushed ECR corpus image**

> **AWS runs only.** This section describes source delivery for the c7a run.
> Local-box runs (SYLIX, codex-x86) keep their own rsync recipes and are
> completely unaffected by anything here.

Source delivery is a single pre-built image:

```
651588424025.dkr.ecr.us-east-1.amazonaws.com/weaver-bench-corpus:latest
```

It carries `/corpus/weaver`, `/corpus/rarpar` and `/corpus/rapidyenc` — each with
its own `REVISION.json` (`{repo, rev, dirty_files, staged_at_utc}`) — plus
`/corpus/prebuilt`, the executable bundle described in §6a.

**Why an image and not a clone** — the same two reasons that used to make rsync
mandatory, now handled once at image-build time instead of on every run:

- weaver must be tested as **working-tree state**. Its C1 gate / skip line / CRC
  parity assert only exist from `a3e3f68d` onward (§4), and any increment in
  flight is by definition not yet pushed. The image stages the working tree,
  uncommitted work included — hence `dirty_files` in `REVISION.json`, which is
  recorded rather than assumed to be zero.
- rarpar's `archive_hotspots` fixtures are **git-LFS**
  (`rarpar/.gitattributes`: `crates/weaver-unrar/tests/fixtures/**/*.rar filter=lfs …`).
  A `git clone` on the box yields LFS *pointer* files and the bench dies on a
  malformed archive. The image build hydrates them.

rapidyenc ships **pre-pinned at `27f435a`** (§5), so there is no clone step, no
network fetch, and no revision drift between runs.

**`.git` does not ship.** Provenance therefore comes from `REVISION.json`, never
from `git rev-parse` — which is what `metadata.json` and `revisions.txt` read
(§9g).

#### Provisioning prerequisite — IAM (do this at launch, not here)

The instance needs an IAM role (or environment credentials) granting, on that
repository:

| action | why |
|---|---|
| `ecr:GetAuthorizationToken` | `aws ecr get-login-password` |
| `ecr:BatchGetImage` | manifest read |
| `ecr:GetDownloadUrlForLayer` | layer download |

**Neither script creates or modifies IAM.** Attach the role when you launch the
instance; the bootstrap fails with this list if login is refused.

#### Pull and extract

```sh
CORPUS_IMAGE="651588424025.dkr.ecr.us-east-1.amazonaws.com/weaver-bench-corpus:latest"
CORPUS_REGISTRY="${CORPUS_IMAGE%%/*}"

sudo apt-get update -y && sudo apt-get install -y docker.io awscli

aws ecr get-login-password --region us-east-1 \
  | sudo docker login --username AWS --password-stdin "$CORPUS_REGISTRY"

sudo docker pull "$CORPUS_IMAGE"

# Extract WITHOUT running anything from the image.
cid="$(sudo docker create "$CORPUS_IMAGE")"
sudo docker cp "$cid:/corpus/." ~/
sudo docker rm -v "$cid"
```

That leaves `~/weaver`, `~/rarpar`, `~/rapidyenc` and `~/prebuilt` — the defaults
`WEAVER_DIR` / `RARPAR_DIR` / `RAPIDYENC_ROOT` / `PREBUILT_DIR` already expect.

**No container is ever started.** `docker create` materializes a container's
filesystem without running it, `docker cp` reads bytes out, `docker rm` discards
it — no process from the image executes on the box. Keep it that way; the image
is a delivery vehicle, not a runtime.

`c7a-bootstrap.sh` performs this same pull-and-extract itself when any tree is
missing, so in practice you only need enough of it by hand to get the weaver
tree (which contains the scripts). It is a no-op once all three trees carry a
`REVISION.json`; pass `CORPUS_FORCE=1` to re-pull deliberately.

#### SSH: multiplex, with a SHORT control path

A fresh box has been observed to time out during the SSH banner exchange on the
first connection, and this runbook opens many short-lived sessions. Use
`ControlMaster` so they share one connection — but **the socket path must be
short**: a UNIX domain socket is capped at ~104 bytes, and a scratchpad-style
path blows straight past it. Keep it in `/tmp`:

```sh
ssh -o ControlMaster=auto \
    -o ControlPath=/tmp/cm-%r@%h \
    -o ControlPersist=10m \
    -o ConnectTimeout=15 \
    -o ServerAliveInterval=30 \
    "$BOX"
```

`/tmp/cm-%r@%h` expands to something like `/tmp/cm-ubuntu@ec2-…`, comfortably
inside the limit. If the first connection still times out on a just-booted
instance, retry — sshd is up before the banner is.

### 7b. Bootstrap, then run

```sh
# 1. bootstrap: dead-man shutdown + CPU gate + system deps (NO rust) + corpus
#    image (pull/extract, no-op if already extracted) + prebuilt-bundle gate
cd ~/weaver
./ci/bench/c7a-bootstrap.sh

# 2. run: everything, teed and summarized, weaver-yenc then rarpar
./ci/bench/c7a-run.sh
```

The bootstrap re-runs the §7a pull-and-extract for any tree that is missing, so
by hand you only need enough of §7a to land `~/weaver` (which is where these
scripts live); the bootstrap fetches the rest.

`c7a-run.sh` sequence:

1. CPU feature gate (abort if not a real Zen 4).
2. Prebuilt-bundle check — manifest parses, `BUILDINFO.json` revs match all
   three trees (§6c), then source-precondition check (§4): the box's tree must
   carry the C1 gate, the skip line, the CRC parity assert and the
   production-shape forced-tier test.
3. **Tests first**, straight from `$PREBUILT_DIR/bin`: weaver-yenc **debug**
   (`weaver-yenc-lib-debug`, `weaver-yenc-diff-debug`), then **release**, all
   with `RAPIDYENC_ROOT` + `CXX` set and `--nocapture` so the §3a case counts and
   the §4 skip line reach the log.
4. Grep-asserts: the C1 skip line is present; all four differential case counts
   are present and non-zero.
5. Steady-state wait (1-min loadavg below `LOAD_THRESHOLD`, or `STEADY_TIMEOUT`).
6. **Discarded warm pass** of each bench binary (`rapidyenc_parity`,
   `decode_simd`), then the **recorded pass twice**, then a **drift check**
   flagging any lane that moved more than `DRIFT_PCT` between the two.
7. **rarpar phase** (§8): tests, then the same warm/2x/drift bench protocol.
8. Summary + **teardown checklist**.

Env overrides (both scripts, all have defaults):

| Var | Default | Meaning |
|-----|---------|---------|
| `CORPUS_IMAGE` | `651588424025.dkr.ecr.us-east-1.amazonaws.com/weaver-bench-corpus:latest` | ECR corpus image (§7a); recorded in `metadata.json` |
| `CORPUS_DEST` | `~` | where `/corpus/.` is extracted; the three tree defaults derive from it |
| `CORPUS_REGION` | `us-east-1` | region for `aws ecr get-login-password` (bootstrap) |
| `CORPUS_FORCE` | `0` | `1` re-pulls and re-extracts even when the trees are present (bootstrap) |
| `WEAVER_DIR` | `$CORPUS_DEST/weaver` | weaver tree from the corpus image |
| `RARPAR_DIR` | `$CORPUS_DEST/rarpar` | rarpar tree — **run aborts if absent** |
| `RAPIDYENC_ROOT` | `$CORPUS_DEST/rapidyenc` | rapidyenc tree, pinned at `27f435a` (§3a) + cmake build dir |
| `WEAVER_RAPIDYENC_LIB` | `$RAPIDYENC_ROOT/build/librapidyenc.so` | dlopen target (§3b) |
| `TARGET` | `x86_64-unknown-linux-gnu` | cargo target |
| `PREBUILT_DIR` | `$CORPUS_DEST/prebuilt` | the executable bundle (§6a) — manifest, BUILDINFO, `bin/`, `lib/` |
| `CXX` | `g++` | compiler the §3a diff binaries invoke at **runtime** to build their C oracle (`rapidyenc_decode_diff.rs:146`) |
| `RESULTS_DIR` | `$WEAVER_DIR/ci/bench/results/<UTC-timestamp>` | output dir (run script) |
| `LOAD_THRESHOLD` | `0.2` | 1-min loadavg the box must fall below before timing |
| `STEADY_TIMEOUT` | `300` | seconds to wait for that before proceeding anyway |
| `DRIFT_PCT` | `2.0` | inter-pass drift percentage that triggers a warning |
| `DEADMAN_MINUTES` | `120` | bootstrap's `shutdown -h +N`; set `0` to skip |
| `WEAVER_PAR2_BENCH_SCENARIOS` | **force-unset by the run script** | scenario filter for the rarpar par2 bench (`crates/weaver-par2/benches/par2_repair.rs:20`). `c7a-run.sh` clears it before benching (warning if it was set) so an inherited value cannot narrow the recorded suite — see §9g |
| `METADATA_INSTANCE_TYPE` | *(unset ⇒ IMDS)* | fallback instance type for `metadata.json` when IMDS is unreachable (§9g) |

Neither script makes an AWS API call. Provisioning and teardown are the
operator's, by hand.

---

## 8. rarpar phases

### 8a. HEADLINE — the FULL 31-case corpus, generated on the box

`config/corpus.json` defines **31 cases** — the `rar-*` evidence family. That
corpus is **not** pre-shipped; it is generated here, which is how the Windows
evidence was produced, and digest-keying keeps the two comparable.

**Toolchain images actually required**, enumerated from the config rather than
assumed (`[.cases[].writer] | group_by(.)` over `config/corpus.json`):

| writer | cases |
|---|---|
| `rarlab-3.93` | 5 |
| `rarlab-4.20` | 8 |
| `rarlab-5.00` | 10 |
| `rarlab-6.24` | 1 |
| `rarlab-7.23` | 7 |
| **total** | **31** |

…plus the **PAR2 generator**, because 5 cases carry `par2: true` and
`ToolchainIDs` (`internal/bench/toolchain.go:118-125`) adds the par2 generator
for exactly those. So all five writers *and* par2 are genuinely needed — the
enumeration justifies the build rather than rubber-stamping it. Note also that
`toolchains build` has **no subsetting flag**: `BuildToolchains`
(`toolchain.go:67-91`) loops every writer then par2, and `main.go:83-84` passes
no filter. Building all six is correct here, not lazy.

#### Tarball integrity is the harness's own — no script-side check added

Verified, so the run script deliberately adds no redundant hashing:

| control | source |
|---|---|
| every writer/par2 `sha256` must be 64 hex chars, platform `linux/amd64` | `toolchain.go:40`, `:52` (`ToolchainLock.Validate`) |
| the pin is passed into the build as `RAR_SHA256` / `PAR2_SHA256` | `toolchain.go:75`, `:86` |
| **the download is verified against it** | `docker/rarlab/Dockerfile:12` and `docker/par2/Dockerfile:12`, both `… \| sha256sum -c -` under `set -eux` |
| base image pinned by digest | `toolchain.go:94-106` (`verifyDockerfiles`) |

A mismatch fails the image build, which fails the phase.

#### Run-time external dependency

This phase is the **only** part of the run that needs the public internet. The
image builds `curl` from:

- **`www.rarlab.com`** — the five `rarlinux-*.tar.gz` writer tarballs;
- **`github.com`** — `par2cmdline-turbo` v1.4.0.

**Failure mode:** if either host is unreachable, or a pin no longer matches what
is served, the toolchain build fails and this phase stops. Per the containment
rule (§8d) that is *contained* — the perf-corpus suite and the criterion phases
still run, and anything already generated stays on disk.

#### Phase shape

`toolchains validate` → `toolchains build` → `corpus generate --out DIR`
(`main.go:109-121`) → `corpus verify` → `plan create` → `run` → `report` →
`render`. The generated corpus lands in the extracted tree's `.cache/corpora`
alongside the shipped perf digest; the harness picks the digest, so the script
**discovers** it afterwards rather than predicting it, and records digest +
case count in `rarpar-bench-full/run-parameters.txt`. Evidence uses the same
`<machine-label>/rar-<rev8>-c7a` convention.

### 8b. The pre-cached 4-case perf corpus


The centrepiece of the c7a run: the Go harness at `rarpar/bench/rarpar-bench`
driving the **rarpar CLI against the reference unrar** over the digest-cached
corpus. Everything else in this document is supporting evidence around it.

Everything below is grounded in a read of the harness sources, not inferred:

| fact | source |
|---|---|
| subcommands `toolchains \| corpus \| plan \| preflight \| run \| report \| render` | `cmd/rarpar-bench/main.go:25-45` |
| `plan create --corpus --out [--seed] [--lane] [--family] [--par2-placement] [--warmups] [--repeats]` | `main.go:53`, `:127-151` |
| `run --corpus --plan --candidate --out [--reference-rar --reference-par2] [--candidate-label --reference-label] [--machine] [--perf]` | `main.go:55`, `:168-201` |
| `corpus verify --root DIR` | `main.go:104-108` |
| `report --input raw.json --out FILE`; `render --input report.json --out DIR` | `main.go:56-57`, `:204-246` |
| `run` reads `<corpus>/corpus.json` for the digest, and `LoadPlan` validates the plan against it | `main.go:189-199`, `internal/bench/plan.go:90` |
| `Run` writes `raw.json` into `--out` | `internal/bench/run.go:123` |
| lanes are `cpu`, `metal`, `docker-cpu` | `internal/bench/plan.go:13-14` |

**Corpus layout** (verified against the local cache): the harness reads a
digest-addressed tree at
`<rarpar>/bench/rarpar-bench/.cache/corpora/<digest>/corpus/`, containing
`corpus.json` (`{digest, case_count, schema_version}`) plus one directory per
case. The cache ships inside the image and merges into the extracted rarpar
tree; the bootstrap hard-fails if it is missing or empty.

**Evidence naming** follows the convention already in the repo's
`.cache/evidence`: `<machine-label>/<family>-<rev8>-<variant>` — observed
examples `windows-ryzen5-3600/rar-6cc9c523-current-v2` and
`linux-i5-1240p/rar5-perf-6cc9c523`. This run writes
`<machine-label>/rar-<rev8>-c7a`, with `<rev8>` taken from rarpar's
`REVISION.json` and `machine-label` defaulting to `linux-c7a-xlarge-zen4`.

The phase runs `corpus verify` → `plan create` → `run` → `report` → `render`,
and the evidence directory lands under `$RESULTS_DIR` in the same shape as the
repo's existing evidence entries (`plan.json`, `raw.json`, `report.json`,
`charts/`). **That directory is the SVG source data** and is archived to
`rarpar-bench-evidence.tar.gz` (§9g).

#### Docker is NOT required — verified

This was the explicit blocker check, and the answer is clean. Docker appears in
exactly three places in the harness:

| site | path | reached here? |
|---|---|---|
| corpus **generation** | `internal/bench/corpus.go:113-237`, `:444-481` | **no** — the corpus ships pre-generated |
| **toolchain image builds** | `internal/bench/toolchain.go:67-92` | **no** — never invoked |
| `docker version` probe | `internal/bench/host.go:32` | yes, but harmless |

The third goes through `commandLine()` (`host.go:77-83`), which **swallows the
error and returns `""`** — a Docker-less box simply records an empty
`docker_version`. `corpus verify` (`corpus.go:546+`) is pure filesystem and
digest checking.

Two things must stay true for that to hold, and the run script enforces both:

- **Never call `rarpar-bench preflight`.** It hard-requires Docker *and* Go
  (`host.go:85-96`, *"Docker is required for corpus generation"*) and would fail
  on this box for no useful reason.
- **Lane stays `cpu`.** `docker-cpu` is a legal lane string (`plan.go:13-14`)
  and is the one way to drag Docker back in.

#### Also deliberately not passed: `--source-manifest` / `--source-target`

That path shells out to `cargo run -p xtask -- feature-audit` and requires a Git
checkout — `run.go:859-875` errors with *"source benchmark must run from a Git
checkout"*. The corpus image ships **no `.git` and no cargo**, so passing it
would hard-fail. Provenance comes from `REVISION.json` instead (§9g). The
`source_target` field in `config/hosts.example.json` is therefore *not* wired.

#### Findings from reading the harness

1. **Both references are mandatory — not just unrar.** `run.go:59-64` hard
   errors with *"a comparative corpus run requires both reference RAR and
   reference PAR2 binaries"* the moment one is supplied without the other. The
   par2 arm is timed at `run.go:248-258` and labelled `par2cmdline-turbo` in
   `report.go:83-88`. So every `run` invocation here passes **both**
   `--reference-rar oracle-unrar-723` and `--reference-par2
   oracle-par2-turbo-140`, unconditionally, and the bundle gate treats both as
   required. `oracle-unrar-624` is the only optional spare.
2. **The harness binary does not read a hosts config.** There is no `--hosts`
   flag and no Go code that loads it: `grep` for `LoadHosts|HostConfig|hosts.json`
   across the harness returns nothing, and `internal/bench/host.go` is machine
   *metadata* collection (`CollectMachine`) plus `Preflight` — not config
   parsing. `config/hosts.example.json` is an input for an **external SSH
   driver** (`docs/benchmarking.md` walks an operator through copying it to
   `hosts.local.json`), and that doc still refers to the harness by its old
   `bench/ln` path. So writing a c7a `hosts.json` would be inert. This run
   therefore passes `--candidate` / `--reference-rar` / `--corpus` / `--plan` /
   `--out` directly, which is what the harness actually consumes. The
   `reference_rar` *concept* from that schema maps to the `--reference-rar`
   flag, and that is the only part of it that matters here.
2. **Two corpora, and only one of them ships.** `config/corpus.json` defines
   **31** cases (the `rar-*` evidence family); `config/perf-corpus.json` defines
   **4** (`rar5-perf-normal-binary`, `rar5-perf-normal-text`,
   `rar5-perf-solid-binary`, `rar5-perf-solid-text`). The pre-shipped cache is
   the 4-case perf corpus. The 31-case corpus is therefore **generated on the
   box** (§8a) — precedent: the Windows evidence was produced the same way, and
   digest-keying keeps the runs comparable. Both suites run; each records its
   digest and case count in its own `run-parameters.txt`.

### 8c. rarpar criterion micro-benches (secondary)


> Promoted from "stretch / optional" to a required phase. rarpar is a **separate
> repo**, delivered as `/corpus/rarpar` in the same ECR image as weaver and
> extracted to `$RARPAR_DIR` (§7a). `c7a-run.sh` **hard-fails** if `$RARPAR_DIR`
> is missing.

rarpar carries GF16 / Reed-Solomon tiers that gate on **GFNI + AVX-512**:

- `crates/weaver-reed-solomon/src/gf_simd.rs` —
  `#[target_feature(enable = "gfni,avx512bw,avx512vl")]` GF16 multiply-accumulate
  kernels at `:1008` and `:1303`, plus a plain `avx512bw,avx512vl` kernel at
  `:1378`, dispatched by `is_x86_feature_detected!` triples at `:232-234`,
  `:436-438`, `:521-523`, `:1138-1140` (each falling back to `gfni,avx2` at
  `:240`, `:443`, `:1145`).
- `crates/weaver-par2/src/repair.rs` — GFNI selection at `:677`, `:2074`,
  `:3055`.

The `gfni,avx512bw,avx512vl` leg is the one **no real box has ever run**
(285H = GFNI but no AVX-512; 3600/1240P = neither). c7a validates it for the
first time — which is why the **tests run before the benches** in this phase.

> **Package names are not the directory names.** The crates were renamed on
> publish and the directories deliberately kept the old `weaver-*` names (see the
> note at `weaver/Cargo.toml:106-114`). `cargo` needs the *package* names:
>
> | directory | package (`-p`) |
> |---|---|
> | `crates/weaver-reed-solomon` | **`reedsolomon-rs`** |
> | `crates/weaver-par2` | **`par2-rs`** |
> | `crates/weaver-unrar` | **`unrar-rs`** |
>
> Any older note saying `cargo bench -p weaver-par2` is wrong and fails with
> "package not found". Verified via `cargo metadata --no-deps`.

```sh
# (a) correctness first — first real-silicon run of the gfni+avx512 GF16 arms.
#     The run script executes EVERY prebuilt test binary whose manifest entry
#     has kind=test and repo=rarpar (12 of them), not a hardcoded pair.
#     For reference, the equivalent cargo invocation on a dev box would be:
#       cargo test --locked -p reedsolomon-rs -p par2-rs --target x86_64-unknown-linux-gnu

# (b) benches, same warm/2x/drift protocol as the weaver ones.
#     par2_repair  : rarpar/crates/weaver-par2/Cargo.toml:72-75  (harness = false)
#                    scenario filter env WEAVER_PAR2_BENCH_SCENARIOS
#                    (rarpar/crates/weaver-par2/benches/par2_repair.rs:20)
#     archive_hotspots : rarpar/crates/weaver-unrar/Cargo.toml:111-113 (harness = false)
cargo bench --locked -p par2-rs   --bench par2_repair       --target x86_64-unknown-linux-gnu
cargo bench --locked -p unrar-rs  --bench archive_hotspots  --target x86_64-unknown-linux-gnu
```

Those two bench targets are the only ones in scope, and both are grounded to a
`[[bench]]` stanza above. Do not substitute others —
`unrar-rs` also declares `ppmd_compare` (`crates/weaver-unrar/Cargo.toml:115-117`)
and `reedsolomon-rs` declares `gf16_gpu_vs_cpu`
(`crates/weaver-reed-solomon/Cargo.toml:40`), neither of which is part of this
run's scope (the latter wants a GPU that a c7a does not have).

Prerequisites, all already satisfied by §6/§7:

- **`nasm`** — `unrar-rs` builds `aws-lc-sys` (feature `crypto-aws-lc`); the
  bootstrap package list includes `nasm` and `cmake`. Verified.
- **Toolchain** — rarpar pins the same 1.97.1, so nothing extra to install.
- **Fixtures** — `crates/weaver-unrar/tests/fixtures` (~797 MB) and
  `crates/weaver-par2/tests/fixtures` (~114 MB) must be the real LFS-hydrated
  bytes; the corpus image build hydrates them (§7a). `archive_hotspots` reads
  them directly via `CARGO_MANIFEST_DIR` (`benches/archive_hotspots.rs:3-8`);
  `par2_repair` stages a scenario into a tempdir per run
  (`crates/weaver-par2/tests/support/benchmark_support.rs:125-135`) and asserts
  loudly if the par2 files are missing (`:130-135`) — that assertion firing means
  unhydrated LFS, not a code bug.

**External A/B (par2cmdline-turbo, unrar 7.x)** — the head-to-head comparisons
quoted in our comparison write-ups are **not driven by a repo bench target I
could ground to an exact command**. `TODO(verify on box)`: locate the external-A/B
harness (the par2cmdline-turbo / unrar-7.x oracles live in local supporting
checkouts) and add its concrete invocation here before running — do not invent
flags. The internal Criterion benches above are the grounded, in-scope coverage.

---

### 8d. Failure containment — save partials, resume don't restart

Standing order, and it shapes the whole run script:

- **Every suite phase is independently failable.** `run_phase` never propagates
  a non-zero status; it records the outcome, then returns 0 so the run
  continues. If full-corpus generation or its `run` fails, the perf-corpus
  suite and both criterion phases still execute. `summary.txt` prints a
  per-phase outcome table, and any failure is still counted as a gate failure
  so the run's exit status is non-zero.
- **Outputs land incrementally.** Evidence directories, harness logs, criterion
  trees, lane files and `run-parameters.txt` are written directly under
  `$RESULTS_DIR` as they are produced. Nothing is staged elsewhere and copied at
  the end — a run killed mid-flight leaves everything it had finished. The
  end-of-run tarballs are convenience archives of data already in place, and a
  failed suite still gets whatever it produced archived.
- **Resume, don't restart.** Each successful phase writes
  `$RESULTS_DIR/.phase-<name>.ok`. Re-invoking with `RESUME=1` and the *same*
  `RESULTS_DIR` skips completed phases and retries only what failed:

  ```sh
  RESUME=1 RESULTS_DIR=<previous-results-dir> ./ci/bench/c7a-run.sh
  ```

  Phase timings append rather than truncate under `RESUME=1`.

## 9. What a PASS looks like

### 9a. Gates

1. **Feature gate green.** Both scripts assert `/proc/cpuinfo` exposes:
   `avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes`.
   Missing any ⇒ **wrong instance family** (not a real c7a / Zen 4) ⇒ hard abort.
   This is the precondition that makes the whole run meaningful.
2. **Source-precondition green** (§4) — the box's tree carries the C1 gate, the
   skip line, the CRC parity assert and
   `forced_tier_kernels_match_scalar_in_production_shape`.
3. **weaver-yenc suite green on real silicon, in debug *and* release** (§2a), with
   the VBMI2 tier actually exercised:
   - `forced_tier_kernels_match_scalar_in_production_shape` passes with the
     `avx512-vbmi2` leg **available and run** — first real-silicon execution of the
     512-bit `searchEnd` probe; the debug leg additionally clears the
     `debug_assert_eq!` at `x86_avx512.rs:272`.
   - `forced_tier_kernels_match_scalar_with_line_hints` passes with the same leg live.
   - `avx512_vbmi2_block_*` unit tests pass.
   - `dispatch_kernel_matches_scalar_*` pass (dispatch = VBMI2 here).
4. **All four `rapidyenc_*` differential tests actually run** (not skip), with
   these counts (§3a): `5978`, `2989`, `3997`, `41986` — **54 950 total**. Any
   `0`, or a missing line, means `RAPIDYENC_ROOT` was not honored and the run
   proved nothing new.
5. **C1 proof recorded** (§4): the exact line
   `skipping crc32_forced_vpclmul_matches_crc_fast: VPCLMUL port unavailable on this CPU`
   appears in the test log, and `crc32_mixed_size_interleaved_updates` passes.
6. **Parity bench pre-timing asserts pass** for all five fixtures — length, bytes
   **and CRC** — with a `parity ok [<fixture>] … crc=0x…` line each.
7. **FULL-corpus suite green** (§8a, the headline): toolchain images build
   (sha256-verified downloads), `corpus generate` emits a 31-case corpus that
   passes `corpus verify`, then `plan create` / `run` / `report` / `render` all
   exit 0 with `raw.json` + `report.json` present. Needs the public internet.
8. **Perf-corpus suite green** (§8b): same chain over the pre-shipped 4-case
   cache; needs no internet and no Docker.
   A failure in either suite is contained (§8d) — the run continues and exits
   non-zero at the end.
9. **rarpar tests green** — all 12 prebuilt test binaries run; the
   slow-tests-gated ones reporting `running 0 tests` and exiting 0 are
   **gated-empty**, which is a pass. The run logs the gated-empty count; expect
   **4**. Then both rarpar benches complete.
10. **Inter-pass drift** below `DRIFT_PCT` on every lane. A lane over threshold
   means the box was noisy; re-run that bench rather than recording the number.

### 9b. Record: rapidyenc kernel ids

```
rapidyenc kernels: decode=____ crc=____
```
Expected `decode=1539` (0x603 VBMI2) and `crc=1088` (0x440 VPCLMUL) — see the
table in §3b. Record verbatim.

### 9c. Record: decode parity (§3b), weaver VBMI2 vs rapidyenc VBMI2

| fixture | weaver decode | rapidyenc decode | ratio (w/r) |
|---------|---------------|------------------|-------------|
| realshape | _fill_ | _fill_ | _fill_ |
| clean | _fill_ | _fill_ | _fill_ |
| crlf_only | _fill_ | _fill_ | _fill_ |
| esc_only | _fill_ | _fill_ | _fill_ |
| dots_body | _fill_ | _fill_ | _fill_ |

Cross-arch context for the same fixtures (ratios >1 = weaver behind):

| box | realshape | clean | dots_body | note |
|-----|-----------|-------|-----------|------|
| M5 Max (NEON) | 0.95 (weaver +5.4%) | ~1.06 | ~1.06 | `crlf_only` weaver +11.1%, `esc_only` parity |
| SYLIX Zen 2 (in-process static, MSVC) | 0.98 | 1.11 | 1.17 | weaver ahead on realshape only |
| codex-x86 ADL | 1.13–1.17 | 0.86 | 0.88 | **inverted vs Zen 2** |
| **c7a Zen 4 (VBMI2 both sides)** | _fill_ | _fill_ | _fill_ | first 512b-vs-256b A/B |

This lane answers the open A13 question: weaver's VBMI2 kernel is a **true
512-bit** kernel while rapidyenc deliberately caps its decode at 256-bit. Whether
that is a win on Zen 4 (downclocking, 512-bit port pressure) is unmeasured until
this run. Do not claim it in either direction beforehand.

### 9d. Record: CRC lanes (§3b + §4)

| lane | c7a time | implied tier |
|------|----------|--------------|
| `parity_crc_fast_decoded` | _fill_ | crc-fast **4x512 ZMM** (weaver's port stands aside — §4) |
| `parity_rapidyenc_crc_decoded` | _fill_ | rapidyenc `RYKERN_VPCLMUL` 0x440 (2x256-class) |
| ratio (weaver/rapidyenc) | _fill_ | headline number |

Cross-arch context:

| box | weaver CRC | rapidyenc CRC | ratio | which weaver tier |
|-----|-----------|---------------|-------|-------------------|
| M5 Max | 102 GiB/s | 11.1 GiB/s | **9.2x weaver** | crc-fast ARM (oracle uses a single serial `crc32d` chain) |
| codex-x86 ADL | ~47 GiB/s (15.3 µs) | _n/a_ | — | weaver's own **VPCLMUL port active** (`available()==true`) |
| SYLIX Zen 2 | ~37 µs | _n/a_ | — | crc-fast **SSE** tier (no vpclmulqdq on Zen 2) |
| **c7a Zen 4** | _fill_ | _fill_ | _fill_ | crc-fast **ZMM** tier, weaver port gated OFF |

The c7a row is the one that justifies the C1 change: if crc-fast's ZMM tier does
**not** beat the ADL-measured ~47 GiB/s of weaver's own port by a clear margin,
the `!avx512vl` exclusion needs re-examining.

### 9e. Record: production-shape lanes (`decode_simd` bench)

The end-detecting production entry point costs more than raw decode. That "family
gap" is the acceptance metric the whole Y1 searchEnd port was judged on, and c7a
is the first VBMI2 measurement of it.

| lane | bench id (`decode_simd`) | source | c7a |
|------|--------------------------|--------|-----|
| decode-only, realshape | `yenc_decode_only_realshape_128col` | `benches/decode_simd.rs:485` (helper `:114-122`) | _fill_ |
| decode-only, bigbang | `yenc_decode_only_bigbang_like` | `:486` | _fill_ |
| until-control, bigbang, 3 chunks | `yenc_decode_bigbang_like_until_control_3_chunks` | `:487` (helper `:166-187`) | _fill_ |
| **family gap** = until_control / decode_only_bigbang | — | — | **_fill_** |

Cross-arch baselines for the family gap (pre-Y1 → post-Y1):

| box | tier | until_control pre → post | decode_only | family gap pre → post |
|-----|------|--------------------------|-------------|-----------------------|
| M5 Max | NEON | 79.4 → 66.5 µs (−16.2%) | ~55.9 µs | **1.44 → 1.19** |
| codex-x86 ADL 1240P (P-core pinned) | AVX2 | 89.0 → 70.5 µs (−20.7%) | 49.8 µs (−1.4%, in band) | **1.79 → 1.44** |
| SYLIX Zen 2 3600 | AVX2 | 144.6 → 120.4 µs (−16.8%) | 77.8 → 63.9 µs (−17.9%, unattributed) | **1.86 → 1.88** |
| **c7a Zen 4** | **VBMI2** | _fill_ | _fill_ | **_fill_** |

Reading the c7a number: the post-Y1 band across owned boxes is **1.19–1.88**, and
the two AVX2 x86 boxes disagree because Zen 2's `decode_only` control lane itself
moved (its ratio is not a clean measurement). A c7a gap at or below ADL's 1.44
says the 512-bit `searchEnd` probe carries its weight; materially above 1.88 says
the 512-bit probe is *more* expensive than the 256-bit one and A13 needs revisiting.

Also record the whole-body lanes for completeness — `yenc_decode_realshape_128col`,
`yenc_decode_bigbang_like_body`, `yenc_decode_article_realshape_128col`,
`yenc_decode_chunked_awkward_splits` (`benches/decode_simd.rs:440-489`) — since
the CRC change (§4) moves the `body(+CRC)` lanes on this part specifically.

### 9f. Record: rarpar (§8)

| lane | c7a | note |
|------|-----|------|
| `reedsolomon-rs` + `par2-rs` test suites | pass/fail | **first real-silicon `gfni,avx512bw,avx512vl` GF16 execution** |
| `par2_repair` scenarios | _fill_ | vs the AVX2+GFNI tier on 285H if a comparable run exists |
| `archive_hotspots` | _fill_ | mostly non-GF16; a control lane for box speed |

### 9g. Preserved data for SVG generation

The tables above are the human summary. The **machine-readable record is the
deliverable** — charts get generated from it later, long after the instance is
gone, so it has to leave the box complete on the first try.

**Full suites, no filters.** Every `cargo bench` in `c7a-run.sh` is invoked with a
bench target and nothing else — no trailing Criterion filter argument anywhere —
so each binary runs its complete lane set:

| bench target | lanes recorded | note |
|---|---|---|
| `rapidyenc_parity` | **12** | 5 fixtures × 2 engines + 2 CRC lanes |
| `decode_simd` | **11** | every lane at `benches/decode_simd.rs:440-488` |
| `par2_repair` | all scenarios | `WEAVER_PAR2_BENCH_SCENARIOS` is **force-unset** by the run script |
| `archive_hotspots` | all fixtures | fixture-driven, no filter |

`par2_repair` is the one suite whose filter is an *environment* variable rather
than an argument, so trusting the caller's environment is not good enough — the
script clears it (warning if it was set) before benching. The run script asserts
the lane count per binary: **zero lanes is a hard gate failure**, fewer than the
expected count above is a warning (a corpus change is legitimate; a silent filter
is not).

**Expected skip — not a failure.** `decode_simd` also defines three lanes gated on
`WEAVER_YENC_REAL_ARTICLE` (`benches/decode_simd.rs:352-400`):
`yenc_decode_real_article_body`, `yenc_decode_real_article_macro_chunks`,
`yenc_decode_real_article_macro_input_360k`. That gate wants a path to a real
Usenet article file, which this run does not supply, so `bench_real_article_if_configured`
returns immediately (`:353-355`) and those three lanes are **absent by design**.
11 `decode_simd` lanes is the correct full-suite number here, not 14.

**Where criterion writes.** With no cargo on the box, criterion resolves its
output directory from `CARGO_TARGET_DIR`, which the run script sets per repo to
`$RESULTS_DIR/criterion/{weaver,rarpar}` — so the data lands inside the results
area (and inside the scp) instead of in a source tree. Standalone criterion keeps
the same `base`/`new` rotation as a cargo-driven run.

**`base/` and `new/` are both recorded passes.** Criterion rotates the previous
`new/` into `base/` on every run. The per-binary sequence is
warm → pass 1 → pass 2, so the discarded warm pass has already been rotated out by
the time the tree ships and what survives is:

| criterion dir | contents |
|---|---|
| `<lane>/base/` | **recorded pass 1** |
| `<lane>/new/` | **recorded pass 2** |
| `<lane>/change/` | pass-2-vs-pass-1 ratios — *not* timings, and skipped by `summary.json` |

Both passes therefore ship with `estimates.json` (mean / median / std_dev /
median_abs_dev / slope point estimates + confidence intervals) and `sample.json`
(`iters` + `times`, 100 samples) intact. That is the raw material: error bars,
distributions, and the drift check are all reconstructable offline.

**`$RESULTS_DIR` layout:**

```
<UTC-timestamp>/
  rarpar-bench-full-evidence.tar.gz  # HEADLINE: 31-case corpus evidence (§8a)
  rarpar-bench-evidence.tar.gz       # perf corpus evidence (§8b)
  rarpar-bench-full/           # live: harness.log, plan.json, evidence/,
                               #   toolchains-required.txt, toolchains-build.txt,
                               #   run-parameters.txt (digest, writers, lane,
                               #   seed, warmups/repeats, candidate, both refs)
  rarpar-bench/                # same shape for the perf suite
  .phase-<name>.ok             # per-phase completion markers (RESUME=1)
  phase-timings.tsv            # MEASURED wall time per phase
  criterion-weaver.tar.gz      # complete weaver criterion tree
  criterion-rarpar.tar.gz      # complete $RARPAR_DIR/target/criterion tree
  metadata.json                # provenance for the whole run (below)
  summary.json                 # flat per-lane estimates, both passes, both repos
  summary.txt                  # human summary
  revisions.txt                # REVISION.json provenance per tree
  proof-gates.txt              # C1 proof + differential case counts
  lane-to-bench.tsv            # lane -> bench-target map used to build summary.json
  cpu-features.log
  weaver-yenc-tests-debug.log
  weaver-yenc-tests-release.log
  criterion/weaver/criterion/   live criterion tree (CARGO_TARGET_DIR)
  criterion/rarpar/criterion/   live criterion tree (CARGO_TARGET_DIR)
  weaver/   <label>-{warm-DISCARDED,pass1,pass2}.log
            <label>-{pass1,pass2}.lanes   <label>-drift.txt
  rarpar/   rarpar-tests.log  README-phase.txt  (+ the same per-label files)
```

`metadata.json` carries instance type (IMDSv2, falling back to IMDSv1 then
`METADATA_INSTANCE_TYPE`), CPU model / core count / full flags line, kernel,
target, and a **`provenance`** object recording that the binaries are prebuilt —
builder, `built_at_utc`, toolchains, the empty build `rustflags`, and build-vs-host
glibc — all read from `BUILDINFO.json`, since there is no `rustc` on the box to
ask. Plus `rev` + `dirty_files` for
weaver / rarpar / rapidyenc — read from each tree's `REVISION.json`, **not** from
git, because the corpus image ships no `.git`; weaver deliberately ships with
uncommitted increments, so `dirty_files` is recorded rather than assumed zero —
plus `staged_at_utc`, the `corpus_image` reference, the UTC run stamp,
**`phase_timings_seconds`** (measured, never estimated — one entry per phase),
and
the decoded `RYKERN_*` decode/crc kernel ids parsed out of the parity bench
output — e.g. `{"decode_id": 1539, "decode_name": "VBMI2", "crc_id": 1088,
"crc_name": "VPCLMUL"}`.

`summary.json` is one flat array, built by walking both criterion trees with `jq`:

```json
[ { "repo": "weaver", "bench": "rapidyenc-parity",
    "lane": "parity_weaver_decode_realshape", "pass": "base",
    "mean_ns": 546290.1, "median_ns": 545880.3,
    "std_dev_ns": 1811.4, "sample_count": 100 } ]
```

`repo` is `weaver` | `rarpar`; `pass` is `base` | `new` per the table above;
`bench` comes from the lane→target map the run script records, not from guessing
at lane-name prefixes. Sanity check after copying it back: row count should be
`2 × (total lanes)` — for weaver alone that is `2 × 23 = 46`.

---

## 10. Triage: a differential FAILS on real silicon but passed under SDE

That is the headline risk this run exists to catch: **a real VBMI2 bug that Intel
SDE did not model.** If any `forced_tier_*` (`avx512-vbmi2` leg),
`avx512_vbmi2_block_*`, `dispatch_kernel_*`, or `rapidyenc_*_matches_local_oracle`
test fails on c7a while the SDE lane is green, capture the first divergent byte:

- The crate's byte-level divergence diagnostic is **`dump_avx2_divergence`**
  (`engines/weaver-yenc/src/simd/tests.rs:1523`, `#[ignore]` at `:1522`).
  Despite the `avx2` in its name it compares the **dispatch-selected** kernel
  (`run_kernel_whole(..., scalar=false, ...)` → `decode_kernel`,
  `engines/weaver-yenc/src/simd/tests.rs:1230-1238`) against the scalar oracle —
  so **on c7a it dumps VBMI2-vs-scalar divergence**, which is exactly what you
  want. There is no separate `dump_avx512_divergence`; this one is it on Zen 4.
  It sweeps `(dot, preserve, search)` including the `search_end=true` legs
  (`:1531-1536`), so it covers the probe.

  Run it:
  ```sh
  cargo test -p weaver-yenc --locked --target x86_64-unknown-linux-gnu \
    -- --ignored --nocapture dump_avx2_divergence
  ```
  It prints `DIVERGE case=… first_diff_out=… simd_out[..]=… ref_out[..]=…`
  (`engines/weaver-yenc/src/simd/tests.rs:1548-1562`) — the first byte offset
  where VBMI2 and scalar disagree, with surrounding context and kernel state.

- If the failure is in the **debug-only** `debug_assert_eq!`
  (`x86_avx512.rs:272`), the release pass will pass while debug fails. That is
  *not* a false alarm: it means the direct `+3`/`+4` mask construction disagrees
  with the oracle's parity derivation on real silicon, and the release binary is
  then silently wrong. Treat a debug-only failure as a hard stop.

- For the rapidyenc differential failures, the assert messages already print the
  offending hex input and raw/state (`tests/rapidyenc_decode_diff.rs:252-257,
  285-290, 315-320, 388-393`) — reproduce that single case directly against the
  scalar oracle.

- Cross-check the same input under SDE to confirm SDE-vs-silicon divergence:
  rebuild the test binary and run the failing test name under
  `sde64 -spr -- <bin> <test>` exactly as `.github/workflows/deploy.yml:365-402`
  does. Same failure under SDE ⇒ ordinary logic bug; **passes under SDE, fails on
  c7a ⇒ genuine VBMI2 real-silicon bug** — file it with the `DIVERGE` dump.

- `--exact` gotcha: `cargo test -- --exact <bare_name>` filters to **0 tests** and
  reads as green. Always use the full module path
  (`simd::tests::forced_tier_kernels_match_scalar_in_production_shape`).

---

## 11. Teardown

`c7a-run.sh` prints this checklist at the end; it is reproduced here because the
script's copy is the one that gets skipped when someone Ctrl-Cs.

1. **`scp` the results directory off-box and open it locally** before touching the
   instance. `RESULTS_DIR` defaults to
   `$WEAVER_DIR/ci/bench/results/<UTC-timestamp>`.
2. **Confirm these six arrived and are non-empty.** They are the raw material
   for SVG generation (§9g) and cannot be reconstructed once the instance is gone:
   - `rarpar-bench-full-evidence.tar.gz` ← headline, 31-case corpus (§8a)
   - `rarpar-bench-evidence.tar.gz` ← perf corpus (§8b)
   - `criterion-weaver.tar.gz`
   - `criterion-rarpar.tar.gz`
   - `metadata.json`
   - `summary.json`

   Spot-check locally before going further:
   ```sh
   tar -tzf rarpar-bench-full-evidence.tar.gz | head
   tar -tzf rarpar-bench-evidence.tar.gz | head
   tar -tzf criterion-weaver.tar.gz | head
   jq '.instance_type, .phase_timings_seconds' metadata.json
   jq 'length' summary.json      # expect 2 x total lanes
   ```
3. Confirm `summary.txt`, `proof-gates.txt`, both recorded bench passes and the
   drift reports are present and non-empty locally.
4. **Terminate** the instance (root volume is `DeleteOnTermination`).
5. Delete the **session security group**.
6. Delete the **ephemeral keypair**.
7. Confirm in the console that no c7a instance, SG or keypair from this session
   remains.

The bootstrap's dead-man `shutdown -h +120` only helps if the instance's
`--instance-initiated-shutdown-behavior` is `terminate` (as
`ci/bench/avx2-aws-run.sh:94` sets for the AVX2 box). Verify that at launch;
otherwise the timer stops the instance and EBS keeps billing.
