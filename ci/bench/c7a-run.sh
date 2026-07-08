#!/usr/bin/env bash
# c7a-run.sh — run the weaver-yenc AVX-512/VBMI2 differential + parity bench on a
# real AMD Zen 4 (AWS c7a). Re-runnable. Tees everything to a timestamped results
# dir and prints a parsed summary (test pass/fail counts + per-fixture parity times).
#
# Sequence (grounded in ci/bench/c7a-avx512-diffbench.md):
#   (a) assert CPU features (avx512vbmi2 + gfni …) — abort if wrong instance
#   (b) full weaver-yenc test suite on x86_64-unknown-linux-gnu, RAPIDYENC_ROOT set
#       so the source-compiled rapidyenc differential tests RUN (not skip), and the
#       VBMI2 tier is naturally dispatched + forced-tier-exercised
#       (engines/weaver-yenc/src/simd.rs:277-293, 6129-6219)
#   (c) parity bench (Criterion) vs the dlopen'd librapidyenc.so, WEAVER_RAPIDYENC_LIB
#       set (engines/weaver-yenc/benches/rapidyenc_parity.rs:34)
#
# Must be the GNU target, not musl: the bench dlopens a shared lib.
set -euo pipefail

# ── Config (all overridable) ─────────────────────────────────────────────────
WEAVER_DIR="${WEAVER_DIR:-$HOME/weaver}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$HOME/rapidyenc}"
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$RAPIDYENC_ROOT/build/librapidyenc.so}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
# Perf bench only: let weaver's non-intrinsic driver code use Zen4 for a fair A/B
# vs cmake-Release rapidyenc. Correctness tests run WITHOUT extra rustflags.
BENCH_RUSTFLAGS="${BENCH_RUSTFLAGS:--C target-cpu=native}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="${RESULTS_DIR:-$WEAVER_DIR/ci/bench/results/$STAMP}"

log()  { printf '\033[1;34m[run]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[run:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# shellcheck disable=SC1090,SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
export PATH="$HOME/.cargo/bin:$PATH"

# ── (a) CPU feature assertion ────────────────────────────────────────────────
REQUIRED_FEATURES="avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes"
assert_cpu_features() {
  log "Asserting CPU features: ${REQUIRED_FEATURES}"
  [ -r /proc/cpuinfo ] || die "/proc/cpuinfo unreadable"
  local flags missing="" f
  flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2- || true)"
  [ -n "$flags" ] || die "could not read CPU flags"
  for f in $REQUIRED_FEATURES; do
    case " $flags " in *" $f "*) : ;; *) missing="$missing $f" ;; esac
  done
  [ -z "$missing" ] || die "missing CPU feature(s):${missing} — not a real Zen 4 c7a. Abort."
  log "CPU feature gate PASSED (real AVX-512 VBMI2 + GFNI)."
}

main() {
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found"
  # Prefer the env file the bootstrap wrote (resolves versioned .so names).
  if [ -f "$RAPIDYENC_ROOT/weaver-bench.env" ]; then
    # shellcheck disable=SC1091
    . "$RAPIDYENC_ROOT/weaver-bench.env"
  fi
  export RAPIDYENC_ROOT WEAVER_RAPIDYENC_LIB

  mkdir -p "$RESULTS_DIR"
  local test_log="$RESULTS_DIR/weaver-yenc-tests.log"
  local bench_log="$RESULTS_DIR/rapidyenc-parity-bench.log"
  local summary="$RESULTS_DIR/summary.txt"

  assert_cpu_features | tee "$RESULTS_DIR/cpu-features.log"
  {
    echo "CPU model: $(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
    echo "flags: $(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2-)"
  } >> "$RESULTS_DIR/cpu-features.log"

  # Warn (don't skip) if the rapidyenc reference is not wired up.
  [ -d "$RAPIDYENC_ROOT" ] || warn "RAPIDYENC_ROOT=$RAPIDYENC_ROOT missing — differential tests will SKIP"
  [ -e "$WEAVER_RAPIDYENC_LIB" ] || warn "WEAVER_RAPIDYENC_LIB=$WEAVER_RAPIDYENC_LIB missing — parity bench will SKIP"

  cd "$WEAVER_DIR"

  # ── (b) Full weaver-yenc test suite (natural VBMI2 dispatch + forced tiers) ─
  log "Running weaver-yenc test suite (target=$TARGET) -> $test_log"
  log "  RAPIDYENC_ROOT=$RAPIDYENC_ROOT (enables source-compiled differential tests)"
  set +e
  RAPIDYENC_ROOT="$RAPIDYENC_ROOT" \
    cargo test -p weaver-yenc --locked --no-fail-fast --target "$TARGET" \
    2>&1 | tee "$test_log"
  local test_rc=${PIPESTATUS[0]}
  set -e
  log "weaver-yenc tests exit code: $test_rc"

  # ── (c) rapidyenc parity bench (dlopen librapidyenc.so) ────────────────────
  log "Running rapidyenc parity bench (RUSTFLAGS='$BENCH_RUSTFLAGS') -> $bench_log"
  set +e
  RUSTFLAGS="$BENCH_RUSTFLAGS" \
  WEAVER_RAPIDYENC_LIB="$WEAVER_RAPIDYENC_LIB" \
    cargo bench -p weaver-yenc --bench rapidyenc_parity --target "$TARGET" \
    2>&1 | tee "$bench_log"
  local bench_rc=${PIPESTATUS[0]}
  set -e
  log "parity bench exit code: $bench_rc"

  # ── Summary ────────────────────────────────────────────────────────────────
  {
    echo "=========================================================="
    echo " c7a weaver-yenc diffbench summary  ($STAMP)"
    echo "=========================================================="
    echo "target            : $TARGET"
    echo "RAPIDYENC_ROOT    : $RAPIDYENC_ROOT"
    echo "WEAVER_RAPIDYENC_LIB: $WEAVER_RAPIDYENC_LIB"
    echo "bench RUSTFLAGS   : $BENCH_RUSTFLAGS"
    echo

    echo "----- test suite (exit $test_rc) -----"
    awk '
      /test result:/ {
        for (i=1;i<=NF;i++){
          if ($(i+1)=="passed;") p+=$i;
          if ($(i+1)=="failed;") f+=$i;
          if ($(i+1)=="ignored;") ig+=$i;
        }
      }
      END { printf "  totals: %d passed, %d failed, %d ignored\n", p, f, ig }
    ' "$test_log"
    # Confirm the differential tests actually RAN (not skipped): the crate prints
    # "…differential cases: N" (tests/rapidyenc_decode_diff.rs:259,287,344) or a
    # skip note when RAPIDYENC_ROOT is absent.
    echo "  rapidyenc differential status:"
    if grep -q "differential cases:" "$test_log"; then
      grep -E "differential cases:" "$test_log" | sed 's/^/    ran: /'
    elif grep -q "skipping rapidyenc differential" "$test_log"; then
      echo "    SKIPPED — RAPIDYENC_ROOT not pointing at a valid checkout"
    else
      echo "    (no differential marker found; check $test_log)"
    fi
    echo "  failing tests (if any):"
    grep -E "^test .* \.\.\. FAILED" "$test_log" | sed 's/^/    /' || echo "    none"
    echo

    echo "----- parity bench (exit $bench_rc) -----"
    if grep -q "rapidyenc kernels:" "$bench_log"; then
      grep "rapidyenc kernels:" "$bench_log" | sed 's/^/  /'
    fi
    if grep -q "skipping rapidyenc parity bench" "$bench_log"; then
      echo "  SKIPPED — WEAVER_RAPIDYENC_LIB not set/loadable"
    fi
    echo "  per-fixture point estimates (Criterion mid value):"
    # Criterion prints the bench id on one line, then "time: [lo mid hi]" next.
    awk '
      /^parity_/ && NF==1 { name=$1; next }
      /time:/ {
        # extract the three values inside [ ... ]; mid = tokens 3 & 4
        s=$0; sub(/.*\[/,"",s); sub(/\].*/,"",s);
        n=split(s, t, " ");
        if (name != "" && n>=4) printf "    %-34s %s %s\n", name, t[3], t[4];
        name="";
      }
    ' "$bench_log"
    echo
    echo "logs: $test_log"
    echo "      $bench_log"
    echo "=========================================================="
  } | tee "$summary"

  log "Results written to $RESULTS_DIR"
  # Non-zero overall exit if the correctness suite failed (bench perf is informational).
  [ "$test_rc" -eq 0 ] || die "weaver-yenc test suite FAILED (see $test_log) — see plan §9 triage (dump_avx2_divergence)"
  log "DONE."
}

main "$@"
