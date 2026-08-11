# c7a AVX-512 differential correctness + performance bench (weaver-yenc + rarpar)

Validate weaver's SIMD yEnc decode kernels — **especially the AVX-512 VBMI2
512-bit kernel** — plus the CRC port gate and rarpar's GFNI+AVX-512 GF16 tier,
for correctness and performance on **real** AMD Zen 4 silicon, against the
`rapidyenc` reference library.

> Authoring note: every command below is grounded in a real repo path + line,
> verified against the working tree on 2026-08-11. Anything not verifiable from
> the repo is marked `TODO(verify on box)`.

Session estimate: **~2.5–3 h** on one `c7a.xlarge` on-demand (~$0.21/hr), i.e.
~$0.55–0.65 all-in. The bootstrap arms a dead-man `shutdown -h +240`, so a
forgotten session caps at 4 h (~$0.85) provided the instance's
shutdown-behavior is `terminate`.

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
by rsync (§7a) and an rsync can be run from the wrong directory.

---

## 5. rapidyenc oracle build (cmake)

Reference: `https://github.com/animetosho/rapidyenc`.
`CMakeLists.txt` builds a shared lib by default
(`option(DISABLE_SHARED "Don't build shared library" OFF)`, `:11`) via
`add_library(rapidyenc_shared SHARED ...)` (`:269`) with `OUTPUT_NAME rapidyenc`
(`:271`) → on Linux the artifact is **`build/librapidyenc.so`** (the dev mac
produced an unversioned `build/librapidyenc.dylib`, confirmed present).

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
# => $RAPIDYENC_ROOT/build/librapidyenc.so
```

The checkout vendors `crcutil-1.0/`; the bootstrap runs
`git submodule update --init --recursive` after clone to be safe, and globs for
the emitted lib in case the name is versioned (`librapidyenc.so.1`).

Revision: the bootstrap clones upstream master. The **dev-machine reference
checkout** — call it `$RAPIDYENC_LOCAL`, defaulting to a sibling of your weaver
checkout (`$(git rev-parse --show-toplevel)/../rapidyenc`); set it explicitly if
your local rapidyenc lives elsewhere — is at **`27f435a`** ("Add build options to
skip building tool/shared components"), i.e. rapidyenc v1.1.1-10-g27f435a. That
is the rev all prior weaver-vs-rapidyenc numbers were taken against. Confirm it
before running:

```sh
RAPIDYENC_LOCAL="${RAPIDYENC_LOCAL:-$(git rev-parse --show-toplevel)/../rapidyenc}"
git -C "$RAPIDYENC_LOCAL" describe --tags --always   # expect v1.1.1-10-g27f435a
```

If upstream master has moved, check out `27f435a` on the box manually before
running the bootstrap (the bootstrap leaves an existing checkout's revision
alone), so the c7a numbers stay comparable to the M5/ADL/Zen2 baselines in §9.
The bootstrap prints whatever revision it ended up with.

---

## 6. Toolchain / build recipe (mirrors CI, drops SDE)

Grounded in `rust-toolchain.toml` + `.github/workflows/deploy.yml`:

- Rust toolchain **1.97.1**, pinned in `rust-toolchain.toml:2`. rustup
  auto-installs it on first `cargo` invocation inside `$WEAVER_DIR` because of the
  pin. rarpar pins the same **1.97.1** (`rarpar/rust-toolchain.toml:2`,
  `rust-version = "1.97.1"` at `rarpar/Cargo.toml:14`), so one toolchain serves
  both phases.
  CI agrees as of `491dff03` ("toolchain fix"):
  `.github/workflows/deploy.yml:20` now sets `RUST_TOOLCHAIN: "1.97.1"`. It read
  `"1.96"` until that commit; ignore any older note claiming a divergence.
- **`nasm` is required** — `aws-lc-sys` (v0.42.0, in both workspaces' build
  graphs) needs it. CI installs it in every native build lane, e.g.
  `.github/workflows/deploy.yml:183` (clippy), `:215` (rust-test-build) and
  `:346` (SDE lane). The bootstrap's package list includes it (§7).
- Build/test target: **`x86_64-unknown-linux-gnu`** (the SDE lane builds with
  `--target x86_64-unknown-linux-gnu`, `.github/workflows/deploy.yml:362`). This
  is also the native host target on the instance.
- `--locked` everywhere (matches CI).
- The SDE lane sets `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS` to force
  target-features *only so older default CPUs can build/emulate the intrinsics*
  (`.github/workflows/deploy.yml:332,359`). **On c7a we omit those** — the
  `#[target_feature]` kernels compile on the plain target and dispatch at runtime
  (§2). The weaver perf bench uses `-C target-cpu=native` for a fair A/B vs
  cmake-`Release` rapidyenc (`BENCH_RUSTFLAGS`); the correctness passes use no
  extra rustflags.
- **Expect two full builds of weaver-yenc's dep graph.** The correctness passes
  (no RUSTFLAGS) and the bench passes (`-C target-cpu=native`) have different
  fingerprints, so cargo rebuilds between them. On top of that the release test
  pass inherits `[profile.release] lto = "fat"`, `codegen-units = 1`
  (`weaver/Cargo.toml:100-104`) and is the single slowest step of the run. Budget
  accordingly; this is why the session estimate is ~2.5–3 h and not 1 h.
  (`panic = "abort"` at `:104` is ignored by cargo for test targets — the release
  test pass builds fine; cargo prints a warning about it.)

Assume **Ubuntu 24.04**. System packages installed by the bootstrap:
`build-essential` (g++ for the §3a oracle + cc), `cmake`, `nasm`, `pkg-config`,
`git`, `curl`, `ca-certificates`.

---

## 7. How to run

### 7a. Get both repos onto the box — **rsync, not clone**

Both phases must test the *working tree*, not a remote branch:

- weaver's C1 gate / skip line / CRC parity assert only exist from `a3e3f68d`
  onward (§4), and any local work in flight is by definition not yet pushed;
- rarpar's `archive_hotspots` fixtures are **git-LFS** (`rarpar/.gitattributes`:
  `crates/weaver-unrar/tests/fixtures/**/*.rar filter=lfs …`). A plain `git clone`
  on the box yields LFS *pointer* files and the bench dies on a malformed archive.
  rsync carries the hydrated bytes.

Run this **from inside your weaver checkout** so the defaults resolve. `$BOX` is
`<user>@<public-dns>` for the instance.

```sh
WEAVER_LOCAL="${WEAVER_LOCAL:-$(git rev-parse --show-toplevel)}"
RARPAR_LOCAL="${RARPAR_LOCAL:-$WEAVER_LOCAL/../rarpar}"

rsync -az --info=progress2 \
  --exclude 'target/' --exclude '.git/' --exclude 'tmp/' \
  "$WEAVER_LOCAL/" "$BOX:~/weaver/"

rsync -az --info=progress2 \
  --exclude 'target/' --exclude '.git/' \
  "$RARPAR_LOCAL/" "$BOX:~/rarpar/"
```

`RARPAR_LOCAL` defaults to a **sibling checkout** of weaver; set it explicitly if
your rarpar checkout lives elsewhere. These two are *dev-machine* variables for
the sync step only — they are not read by either on-box script (whose own
variables are `WEAVER_DIR` / `RARPAR_DIR`, table in §7b).

Payloads with those excludes: weaver ≈ **0.7 GB**, rarpar ≈ **1.6 GB** (of which
~0.9 GB is the unrar/par2 LFS fixture corpus). Excluding `target/` is what keeps
this from being an 80 GB transfer; excluding `.git/` is what keeps LFS out of the
picture entirely.

### 7b. Bootstrap, then run

```sh
# 1. bootstrap: dead-man shutdown + CPU gate + system deps + rustup(1.97.1)
#    + rapidyenc source & .so
cd ~/weaver
./ci/bench/c7a-bootstrap.sh

# 2. run: everything, teed and summarized, weaver-yenc then rarpar
./ci/bench/c7a-run.sh
```

`c7a-run.sh` sequence:

1. CPU feature gate (abort if not a real Zen 4).
2. Source-precondition check (§4) — the box's tree must carry the C1 gate, the
   skip line, the CRC parity assert and the production-shape forced-tier test.
3. **Tests first**: weaver-yenc **debug**, then **release**, both with
   `RAPIDYENC_ROOT` set and `-- --nocapture` so the §3a case counts and the §4
   skip line reach the log.
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
| `WEAVER_DIR` | `~/weaver` | weaver checkout (rsync'd, §7a) |
| `RARPAR_DIR` | `~/rarpar` | rarpar checkout (rsync'd) — **run aborts if absent** |
| `RAPIDYENC_ROOT` | `~/rapidyenc` | rapidyenc source tree (§3a) + cmake build dir |
| `RAPIDYENC_GIT` | `https://github.com/animetosho/rapidyenc.git` | clone URL (bootstrap) |
| `WEAVER_RAPIDYENC_LIB` | `$RAPIDYENC_ROOT/build/librapidyenc.so` | dlopen target (§3b) |
| `TARGET` | `x86_64-unknown-linux-gnu` | cargo target |
| `BENCH_RUSTFLAGS` | `-C target-cpu=native` | RUSTFLAGS for the **weaver** perf benches only |
| `RARPAR_BENCH_RUSTFLAGS` | *(empty)* | RUSTFLAGS for the rarpar benches; empty on purpose — rarpar's GF16 tiers are `#[target_feature]` + runtime dispatch (`crates/weaver-reed-solomon/src/gf_simd.rs:232-234,436-438`), so pinning would only obscure which tier ran |
| `RESULTS_DIR` | `$WEAVER_DIR/ci/bench/results/<UTC-timestamp>` | output dir (run script) |
| `LOAD_THRESHOLD` | `0.2` | 1-min loadavg the box must fall below before timing |
| `STEADY_TIMEOUT` | `300` | seconds to wait for that before proceeding anyway |
| `DRIFT_PCT` | `2.0` | inter-pass drift percentage that triggers a warning |
| `DEADMAN_MINUTES` | `240` | bootstrap's `shutdown -h +N`; set `0` to skip |
| `RUST_TOOLCHAIN_FALLBACK` | `1.97.1` | bootstrap only, used if `rust-toolchain.toml` is missing |
| `CXX` | `c++` | compiler for the §3a source oracle (`rapidyenc_decode_diff.rs:146`) |
| `WEAVER_PAR2_BENCH_SCENARIOS` | **force-unset by the run script** | scenario filter for the rarpar par2 bench (`crates/weaver-par2/benches/par2_repair.rs:20`). `c7a-run.sh` clears it before benching (warning if it was set) so an inherited value cannot narrow the recorded suite — see §9g |
| `METADATA_INSTANCE_TYPE` | *(unset ⇒ IMDS)* | fallback instance type for `metadata.json` when IMDS is unreachable (§9g) |

Neither script makes an AWS API call. Provisioning and teardown are the
operator's, by hand.

---

## 8. rarpar phase — PAR2 GFNI + AVX-512 GF16 on Zen 4 (mandatory)

> Promoted from "stretch / optional" to a required phase. rarpar is a **separate
> repo** (`$RARPAR_LOCAL`, by default a sibling checkout of weaver), rsync'd to
> `$RARPAR_DIR` on the box alongside weaver (§7a). `c7a-run.sh` **hard-fails** if
> `$RARPAR_DIR` is missing.

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
cd "$RARPAR_DIR"

# (a) correctness first — first real-silicon run of the gfni+avx512 GF16 arms
cargo test --locked -p reedsolomon-rs -p par2-rs --target x86_64-unknown-linux-gnu

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
  bytes; rsync from the dev mac guarantees that (§7a). `archive_hotspots` reads
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
7. **rarpar tests green**, then both rarpar benches complete.
8. **Inter-pass drift** below `DRIFT_PCT` on every lane. A lane over threshold
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
  criterion-weaver.tar.gz      # complete $WEAVER_DIR/target/criterion tree
  criterion-rarpar.tar.gz      # complete $RARPAR_DIR/target/criterion tree
  metadata.json                # provenance for the whole run (below)
  summary.json                 # flat per-lane estimates, both passes, both repos
  summary.txt                  # human summary
  revisions.txt                # rev + dirty count per repo
  proof-gates.txt              # C1 proof + differential case counts
  lane-to-bench.tsv            # lane -> bench-target map used to build summary.json
  cpu-features.log
  weaver-yenc-tests-debug.log
  weaver-yenc-tests-release.log
  weaver/   <label>-{warm-DISCARDED,pass1,pass2}.log
            <label>-{pass1,pass2}.lanes   <label>-drift.txt
  rarpar/   rarpar-tests.log  README-phase.txt  (+ the same per-label files)
```

`metadata.json` carries instance type (IMDSv2, falling back to IMDSv1 then
`METADATA_INSTANCE_TYPE`), CPU model / core count / full flags line, kernel,
`rustc -V`, target, both RUSTFLAGS settings, `rev` + `dirty_files` for
weaver / rarpar / rapidyenc (trees arrive by rsync and may legitimately be dirty,
so the dirty count is recorded rather than assumed zero), the UTC run stamp, and
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
2. **Confirm these four arrived and are non-empty.** They are the raw material
   for SVG generation (§9g) and cannot be reconstructed once the instance is gone:
   - `criterion-weaver.tar.gz`
   - `criterion-rarpar.tar.gz`
   - `metadata.json`
   - `summary.json`

   Spot-check locally before going further:
   ```sh
   tar -tzf criterion-weaver.tar.gz | head
   jq '.instance_type, .rapidyenc_kernels' metadata.json
   jq 'length' summary.json      # expect 2 x total lanes
   ```
3. Confirm `summary.txt`, `proof-gates.txt`, both recorded bench passes and the
   drift reports are present and non-empty locally.
4. **Terminate** the instance (root volume is `DeleteOnTermination`).
5. Delete the **session security group**.
6. Delete the **ephemeral keypair**.
7. Confirm in the console that no c7a instance, SG or keypair from this session
   remains.

The bootstrap's dead-man `shutdown -h +240` only helps if the instance's
`--instance-initiated-shutdown-behavior` is `terminate` (as
`ci/bench/avx2-aws-run.sh:94` sets for the AVX2 box). Verify that at launch;
otherwise the timer stops the instance and EBS keeps billing.
