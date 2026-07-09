#!/usr/bin/env bash
# avx512-profile.sh — validate + benchmark weaver's NEW flat 512-bit VBMI2 yEnc
# decode kernel (decode_kernel_avx512_raw) against rapidyenc's own VBMI2 tier, on
# a real AVX-512 VBMI2 Linux box, weaver and rapidyenc same-run in one process.
#
# Target box: Intel Ice Lake / Sapphire Rapids (AWS c6i / c7i) or AMD Zen4 (c7a)
# — all expose avx512vbmi2, so weaver naturally dispatches decode_kernel_avx512_vbmi2
# -> decode_kernel_avx512_raw (the 512-bit port), and rapidyenc dispatches its
# own do_decode_avx2<...,ISA_LEVEL_VBMI2>. This is the tier the 512-bit rewrite is
# meant to WIN on (one 512-bit window halves the µops/byte vs the 256-bit AVX2 tier).
#
# Two deliverables:
#   1. CORRECTNESS on real AVX-512 silicon — `cargo test -p weaver-yenc` exercises
#      the forced-tier differential + dispatch/adversarial suites with the VBMI2
#      tier live (first real-silicon run of the flat 512-bit kernel; SDE-only until
#      now). No PMU needed.
#   2. PERF — same-run decode_timing ratio weaver-VBMI2 / rapidyenc-VBMI2. No PMU
#      needed, so a cheap virtualized box suffices. perf topdown is best-effort
#      (metal-only; skipped/partial on virtualized instances).
#
# Reuses the same-run rapidyenc harness (engines/weaver-yenc/build.rs cc-links
# rapidyenc when WEAVER_RAPIDYENC_SRC is set — it already compiles decoder_vbmi2.cc
# with -mavx512vbmi2 -mavx512vl -mavx512bw; examples/decode_timing.rs prints the
# weaver/rapidyenc ratio; examples/profile_kernel.rs hammers one kernel for perf).
set -euo pipefail

# Robust to being launched from a context that doesn't export HOME (systemd,
# SSM RunShellScript, detached setsid, …): derive it from the passwd db.
: "${HOME:=$(getent passwd "$(id -u)" 2>/dev/null | cut -d: -f6)}"
: "${HOME:=/home/$(id -un 2>/dev/null || echo ubuntu)}"
export HOME

WEAVER_DIR="${WEAVER_DIR:-$HOME/weaver}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$HOME/rapidyenc}"
RAPIDYENC_GIT="${RAPIDYENC_GIT:-https://github.com/animetosho/rapidyenc.git}"
FIXTURE="${FIXTURE:-realshape}"
PROFILE_ITERS="${PROFILE_ITERS:-400000}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS="${RESULTS:-$WEAVER_DIR/ci/bench/results/avx512-$STAMP}"
EXAMPLE_DIR="$WEAVER_DIR/target/release/examples"

log(){ printf '\033[1;34m[avx512-prof]\033[0m %s\n' "$*"; }
warn(){ printf '\033[1;33m[avx512-prof:warn]\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[1;31m[avx512-prof:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# shellcheck disable=SC1090,SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"; export PATH="$HOME/.cargo/bin:$PATH"

# ── 0. gate: must be an AVX-512 VBMI2 box (so the VBMI2 tier dispatches) ───────
assert_vbmi2_box() {
  [ -r /proc/cpuinfo ] || die "no /proc/cpuinfo"
  local flags; flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2-)"
  local f
  for f in avx2 avx512f avx512vl avx512bw; do
    case " $flags " in
      *" $f "*) : ;;
      *) die "CPU missing '$f' — not an AVX-512 box." ;;
    esac
  done
  # Linux /proc/cpuinfo spells VBMI2 as 'avx512_vbmi2' (underscore) on modern
  # kernels (Ice Lake / Sapphire Rapids / Zen4); older kernels used 'avx512vbmi2'.
  # (Rust's is_x86_feature_detected!("avx512vbmi2") uses CPUID, not this string.)
  case " $flags " in
    *" avx512_vbmi2 "*|*" avx512vbmi2 "*) : ;;
    *) die "CPU missing avx512_vbmi2 (VBMI2) — weaver would NOT dispatch the VBMI2 tier.
     Use Intel Ice Lake+/SPR (AWS c6i/c7i) or AMD Zen4 (c7a)." ;;
  esac
  log "VBMI2 gate OK ($(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'))"
  log "  avx512 features: $(echo "$flags" | tr ' ' '\n' | grep -E '^avx512|^vpclmul|^gfni|^vaes' | tr '\n' ' ')"
}

need() { command -v "$1" >/dev/null 2>&1 || die "missing tool '$1' ($2)"; }

install_deps() {
  [ "${INSTALL_DEPS:-1}" = "1" ] || { log "INSTALL_DEPS=0 — skipping apt/rustup"; return; }
  command -v apt-get >/dev/null 2>&1 || { warn "no apt-get; install deps manually"; return; }
  local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
  log "installing system deps (apt)…"
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y \
      build-essential cmake nasm git curl ca-certificates pkg-config \
      "linux-tools-$(uname -r)" linux-tools-generic linux-tools-common linux-tools-aws \
    || $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y \
      build-essential cmake nasm git curl ca-certificates pkg-config linux-tools-generic
  if ! command -v cargo >/dev/null 2>&1; then
    log "installing rustup toolchain…"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"; export PATH="$HOME/.cargo/bin:$PATH"
  fi
}

setup() {
  install_deps
  # shellcheck disable=SC1090,SC1091
  [ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"; export PATH="$HOME/.cargo/bin:$PATH"
  need cargo "install rust toolchain first (rustup)"
  need cmake "apt-get install -y cmake"
  need nasm  "apt-get install -y nasm (aws-lc-sys)"
  local par; par="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)"
  if [ "$par" -gt 1 ] 2>/dev/null; then
    warn "perf_event_paranoid=$par; lowering to 1 (needs sudo)"
    sudo sh -c 'echo 1 > /proc/sys/kernel/perf_event_paranoid' || warn "could not lower paranoid; perf may be limited (fine — the same-run ratio needs no PMU)"
  fi
  if [ ! -d "$RAPIDYENC_ROOT/.git" ]; then
    log "cloning rapidyenc -> $RAPIDYENC_ROOT"
    git clone "$RAPIDYENC_GIT" "$RAPIDYENC_ROOT"
    ( cd "$RAPIDYENC_ROOT" && git submodule update --init --recursive || true )
  fi
  [ -f "$RAPIDYENC_ROOT/src/decoder.h" ] || die "bad rapidyenc checkout at $RAPIDYENC_ROOT"
  mkdir -p "$RESULTS"
  log "results -> $RESULTS"
}

# ── 1. CORRECTNESS on real AVX-512 silicon: the full weaver-yenc suite with the
#      VBMI2 tier live (forced-tier differential + adversarial/chunk-split). ────
cargo_test() {
  cd "$WEAVER_DIR"
  export WEAVER_RAPIDYENC_SRC="$RAPIDYENC_ROOT"
  export RUSTFLAGS="-C target-cpu=native"
  log "=== cargo test -p weaver-yenc (REAL AVX-512 VBMI2 tier live) ==="
  cargo test --release -p weaver-yenc 2>&1 | tee "$RESULTS/cargo-test.txt" | grep -E 'test result|FAILED|error\[' || true
  if grep -q 'test result: ok' "$RESULTS/cargo-test.txt" && ! grep -q 'FAILED' "$RESULTS/cargo-test.txt"; then
    log "  CORRECTNESS: PASS on real AVX-512 silicon"
  else
    warn "  CORRECTNESS: check cargo-test.txt — not a clean pass"
  fi
}

build() {
  cd "$WEAVER_DIR"
  export WEAVER_RAPIDYENC_SRC="$RAPIDYENC_ROOT"
  export RUSTFLAGS="-C target-cpu=native -C debuginfo=2"
  log "building decode_timing + profile_kernel (rapidyenc-linked, target-cpu=native)…"
  cargo build --release --example decode_timing  -p weaver-yenc
  cargo build --release --example profile_kernel -p weaver-yenc
  [ -x "$EXAMPLE_DIR/decode_timing" ]  || die "decode_timing build missing"
  [ -x "$EXAMPLE_DIR/profile_kernel" ] || die "profile_kernel build missing"
}

# ── 2. the number that matters: same-run weaver-VBMI2 / rapidyenc-VBMI2 ratio ──
same_run_ratio() {
  log "=== same-run weaver(VBMI2) vs rapidyenc(VBMI2) — <1.0 means weaver WINS ==="
  "$EXAMPLE_DIR/decode_timing" 2>&1 | tee "$RESULTS/decode_timing.txt"
}

# ── 3. perf stat (best-effort; metal-only, partial/empty on virtualized) ──────
PERF_EVENTS="cycles,instructions,stalled-cycles-frontend,stalled-cycles-backend,cache-references,cache-misses,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses"
perf_stat_one() {
  local tag="$1"; shift
  local out="$RESULTS/perf-stat-$tag.txt"
  log "perf stat [$tag] ($FIXTURE x $PROFILE_ITERS)… (best-effort)"
  { echo "### $tag  fixture=$FIXTURE iters=$PROFILE_ITERS"
    env "$@" perf stat -e "$PERF_EVENTS" -- \
      "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 || true
    echo; echo "### $tag  topdown"
    env "$@" perf stat -M TopdownL1 -- \
      "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 \
      || env "$@" perf stat --topdown -- \
        "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 || true
  } | tee "$out"
}
perf_stat_both() {
  command -v perf >/dev/null 2>&1 || { warn "no perf — skipping topdown (same-run ratio is the deliverable)"; return; }
  log "=== perf stat: backend/frontend/cache split (best-effort) ==="
  perf_stat_one weaver
  perf_stat_one rapidyenc WEAVER_PROFILE_RAPIDYENC=1
}

summary() {
  {
    echo "=========================================================="
    echo " AVX-512 VBMI2 profile summary  ($STAMP)"
    echo " CPU: $(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
    echo "=========================================================="
    echo "-- CORRECTNESS (real AVX-512 silicon) --"
    grep -E 'test result' "$RESULTS/cargo-test.txt" 2>/dev/null | tail -3 || echo "  (no cargo-test.txt)"
    echo
    echo "-- PERF: same-run weaver(VBMI2) vs rapidyenc(VBMI2)  [<1.0 = weaver wins, target the win] --"
    grep -E 'weaver/rapidyenc|realshape|clean|crlf|esc|dots|rapidyenc ' "$RESULTS/decode_timing.txt" 2>/dev/null || true
    echo
    echo "-- perf-stat (if PMU available) --"
    for m in instructions cycles stalled-cycles-backend stalled-cycles-frontend; do
      printf '  %-26s weaver=%s  rapidyenc=%s\n' "$m" \
        "$(grep -m1 "$m" "$RESULTS/perf-stat-weaver.txt"   2>/dev/null | awk '{print $1}')" \
        "$(grep -m1 "$m" "$RESULTS/perf-stat-rapidyenc.txt" 2>/dev/null | awk '{print $1}')"
    done
    echo
    echo "artifacts in: $RESULTS"
    echo "=========================================================="
  } | tee "$RESULTS/summary.txt"
}

main() {
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found — clone weaver there"
  assert_vbmi2_box
  setup
  cargo_test
  build
  same_run_ratio
  perf_stat_both
  summary
  log "DONE. tar the results dir and send it, or paste summary.txt + decode_timing.txt."
}
main "$@"
