#!/usr/bin/env bash
# avx2-profile.sh — find WHY weaver's AVX2 yEnc decode is slower than rapidyenc,
# on a real AVX2 Linux box, using perf (topdown + per-instruction), with weaver
# and rapidyenc profiled SIDE BY SIDE in the same process.
#
# Target box: an AMD Zen 2/Zen 3 instance (AWS c5a / c6a) — AVX2-native, NO
# AVX-512, so weaver naturally dispatches decode_kernel_avx2 (the tier we must
# get within 5% of rapidyenc). Zen 2 == the SYLIX Windows target's uarch, so
# numbers transfer directly. (Intel would dispatch VBMI2 — wrong tier here.)
#
# Why this exists: the Windows uProf "time-based hotspots" view is misleading on
# an out-of-order core — it attributes samples to where retirement STALLS (the
# 512KB-LUT compaction gather), not to the throughput bottleneck (deleting the
# whole compaction is wall-clock ZERO). perf's topdown + stall-cycle split tells
# us the actual bound: memory-bound (LUT/L3 bandwidth) vs backend-core-bound (a
# saturated execution port) vs frontend. That is the thing we need to fix.
#
# Reuses the same-run rapidyenc harness (engines/weaver-yenc/build.rs cc-links
# rapidyenc when WEAVER_RAPIDYENC_SRC is set; examples/decode_timing.rs prints
# the weaver/rapidyenc ratio; examples/profile_kernel.rs hammers one kernel for
# perf, weaver by default or rapidyenc when WEAVER_PROFILE_RAPIDYENC=1).
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
PROFILE_ITERS="${PROFILE_ITERS:-400000}"     # ~30-40s of kernel at ~90us/iter
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS="${RESULTS:-$WEAVER_DIR/ci/bench/results/avx2-$STAMP}"
EXAMPLE_DIR="$WEAVER_DIR/target/release/examples"

log(){ printf '\033[1;34m[avx2-prof]\033[0m %s\n' "$*"; }
warn(){ printf '\033[1;33m[avx2-prof:warn]\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[1;31m[avx2-prof:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# shellcheck disable=SC1090,SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"; export PATH="$HOME/.cargo/bin:$PATH"

# ── 0. gate: must be an AVX2 box that is NOT AVX-512 (so AVX2 tier dispatches) ─
assert_avx2_box() {
  [ -r /proc/cpuinfo ] || die "no /proc/cpuinfo"
  local flags; flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2-)"
  case " $flags " in *" avx2 "*) : ;; *) die "CPU has no AVX2 — wrong box" ;; esac
  case " $flags " in
    *" avx512f "*)
      die "CPU has AVX-512 — weaver would dispatch VBMI2, not the AVX2 tier.
     Use an AMD Zen 2/Zen 3 box (AWS c5a/c6a): AVX2-native, no AVX-512." ;;
  esac
  log "AVX2 gate OK ($(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'))"
  case " $flags " in *" bmi2 "*) : ;; *) warn "no bmi2 — unusual for an AVX2 box" ;; esac
}

need() { command -v "$1" >/dev/null 2>&1 || die "missing tool '$1' ($2)"; }

# Idempotent dep install (Ubuntu). Skipped if INSTALL_DEPS=0.
install_deps() {
  [ "${INSTALL_DEPS:-1}" = "1" ] || { log "INSTALL_DEPS=0 — skipping apt/rustup"; return; }
  command -v apt-get >/dev/null 2>&1 || { warn "no apt-get; install deps manually"; return; }
  local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
  log "installing system deps (apt)…"
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
  # linux-tools-<kernel> gives `perf`; on AWS Ubuntu the kernel flavour is -aws.
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
  need perf   "apt-get install -y linux-tools-\$(uname -r) linux-tools-generic (or set INSTALL_DEPS=1)"
  need cmake  "apt-get install -y cmake"
  need nasm   "apt-get install -y nasm   (aws-lc-sys)"
  # perf needs relaxed paranoia to sample in user mode + read kernel maps.
  local par; par="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)"
  if [ "$par" -gt 1 ] 2>/dev/null; then
    warn "perf_event_paranoid=$par; lowering to 1 (needs sudo)"
    sudo sh -c 'echo 1 > /proc/sys/kernel/perf_event_paranoid' || warn "could not lower paranoid; perf may be limited"
  fi

  if [ ! -d "$RAPIDYENC_ROOT/.git" ]; then
    log "cloning rapidyenc -> $RAPIDYENC_ROOT"
    git clone "$RAPIDYENC_GIT" "$RAPIDYENC_ROOT"
    ( cd "$RAPIDYENC_ROOT" && git submodule update --init --recursive || true )
  fi
  # cmake build gives the .so for the Criterion parity bench (optional);
  # the cc-harness (WEAVER_RAPIDYENC_SRC) compiles the sources directly.
  [ -f "$RAPIDYENC_ROOT/src/decoder.h" ] || die "bad rapidyenc checkout at $RAPIDYENC_ROOT"

  mkdir -p "$RESULTS"
  log "results -> $RESULTS"
}

build() {
  cd "$WEAVER_DIR"
  # Fair fight: weaver's non-intrinsic driver code gets Zen tuning like
  # cmake-Release rapidyenc; debuginfo so perf annotate has symbols+source;
  # WEAVER_RAPIDYENC_SRC cc-links the real rapidyenc into the harness.
  export WEAVER_RAPIDYENC_SRC="$RAPIDYENC_ROOT"
  export RUSTFLAGS="-C target-cpu=native -C debuginfo=2"
  log "building decode_timing + profile_kernel (rapidyenc-linked, debuginfo, target-cpu=native)…"
  cargo build --release --example decode_timing  -p weaver-yenc
  cargo build --release --example profile_kernel -p weaver-yenc
  [ -x "$EXAMPLE_DIR/decode_timing" ]  || die "decode_timing build missing"
  [ -x "$EXAMPLE_DIR/profile_kernel" ] || die "profile_kernel build missing"
}

# ── 1. the number that matters: same-run weaver/rapidyenc ratio (target <=1.05) ─
same_run_ratio() {
  log "=== same-run weaver vs rapidyenc (target ratio <= 1.05) ==="
  "$EXAMPLE_DIR/decode_timing" 2>&1 | tee "$RESULTS/decode_timing.txt"
}

# ── 2. perf stat: topdown + stall split + cache, for weaver AND rapidyenc ─────
PERF_EVENTS="cycles,instructions,stalled-cycles-frontend,stalled-cycles-backend,cache-references,cache-misses,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses"
perf_stat_one() {
  local tag="$1"; shift            # env for rapidyenc vs weaver
  local out="$RESULTS/perf-stat-$tag.txt"
  log "perf stat [$tag] ($FIXTURE x $PROFILE_ITERS)…"
  { echo "### $tag  fixture=$FIXTURE iters=$PROFILE_ITERS"
    # portable event set (works Intel + AMD)
    env "$@" perf stat -e "$PERF_EVENTS" -- \
      "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 || true
    echo; echo "### $tag  topdown metric group (best-effort)"
    # Intel: TopdownL1; recent AMD perf: PipelineL1/branch/etc. Try both, ignore misses.
    env "$@" perf stat -M TopdownL1 -- \
      "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 \
      || env "$@" perf stat --topdown -- \
        "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" 2>&1 || true
  } | tee "$out"
}
perf_stat_both() {
  log "=== perf stat: backend/frontend/cache split ==="
  perf_stat_one weaver
  perf_stat_one rapidyenc WEAVER_PROFILE_RAPIDYENC=1
}

# ── 3. perf record + annotate: per-instruction (with the OOO caveat) ──────────
perf_annotate_one() {
  local tag="$1"; shift
  local data="$RESULTS/perf-$tag.data"
  log "perf record [$tag]…"
  env "$@" perf record -F 4000 -g --call-graph=fp -o "$data" -- \
    "$EXAMPLE_DIR/profile_kernel" "$FIXTURE" "$PROFILE_ITERS" >/dev/null 2>&1 || { warn "perf record failed for $tag"; return; }
  perf report -i "$data" --stdio --percent-limit 1 2>/dev/null | head -40 > "$RESULTS/perf-report-$tag.txt" || true
  # per-instruction annotation of the hottest symbols (decode_kernel_avx2 / rapidyenc)
  perf annotate -i "$data" --stdio -l 2>/dev/null | sed -n '1,220p' > "$RESULTS/perf-annotate-$tag.txt" || true
  log "  wrote perf-report-$tag.txt + perf-annotate-$tag.txt"
}
perf_annotate_both() {
  log "=== perf annotate: per-instruction (latency-sink caveat applies) ==="
  perf_annotate_one weaver
  perf_annotate_one rapidyenc WEAVER_PROFILE_RAPIDYENC=1
}

summary() {
  {
    echo "=========================================================="
    echo " AVX2 profile summary  ($STAMP)"
    echo " CPU: $(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
    echo "=========================================================="
    echo "-- same-run ratio (target <= 1.05) --"
    grep -E 'weaver/rapidyenc|realshape|rapidyenc ' "$RESULTS/decode_timing.txt" 2>/dev/null || true
    echo
    echo "-- key perf-stat deltas (weaver vs rapidyenc) --"
    for m in instructions cycles stalled-cycles-backend stalled-cycles-frontend cache-misses LLC-load-misses; do
      printf '  %-26s weaver=%s  rapidyenc=%s\n' "$m" \
        "$(grep -m1 "$m" "$RESULTS/perf-stat-weaver.txt"   2>/dev/null | awk '{print $1}')" \
        "$(grep -m1 "$m" "$RESULTS/perf-stat-rapidyenc.txt" 2>/dev/null | awk '{print $1}')"
    done
    echo
    echo "artifacts in: $RESULTS"
    echo "  decode_timing.txt   perf-stat-{weaver,rapidyenc}.txt"
    echo "  perf-report-*.txt   perf-annotate-*.txt"
    echo "SEND ME: perf-stat-weaver.txt + perf-stat-rapidyenc.txt (the topdown/stall split is the key)."
    echo "=========================================================="
  } | tee "$RESULTS/summary.txt"
}

main() {
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found — clone weaver there"
  assert_avx2_box
  setup
  build
  same_run_ratio
  perf_stat_both
  perf_annotate_both
  summary
  log "DONE. tar the results dir and send it, or paste summary.txt + the two perf-stat files."
}
main "$@"
