#!/usr/bin/env bash
# c7a-run.sh — run the weaver-yenc AVX-512/VBMI2 differential + parity bench AND
# the rarpar GFNI/AVX-512 GF16 phase on a real AMD Zen 4 (AWS c7a). Re-runnable.
# Tees everything to a timestamped results dir and prints a parsed summary.
#
# Makes NO AWS API calls. Teardown is the operator's, by hand — the checklist is
# printed at the end (and mirrored in ci/bench/c7a-avx512-diffbench.md §11).
#
# Sequence (grounded in ci/bench/c7a-avx512-diffbench.md):
#   (a) assert CPU features (avx512vbmi2 + gfni …) — abort if wrong instance
#   (b) assert the SOURCE ON THE BOX carries the markers this run exists to
#       prove (C1 gate, skip line, CRC parity assert, production-shape test) —
#       they were uncommitted on the dev mac, so a `git clone` box is stale
#   (c) weaver-yenc tests FIRST, DEBUG then RELEASE, with RAPIDYENC_ROOT set so
#       the source-compiled rapidyenc differential tests RUN (not skip), and
#       `-- --nocapture` so their case counts and the C1 skip line reach the log.
#       debug != release here: the VBMI2 searchEnd probe carries a
#       `debug_assert_eq!` (simd/x86_avx512.rs:272) compiled out in release.
#   (d) grep-assert the C1 proof line and non-zero differential case counts
#   (e) weaver benches: steady-state wait, DISCARDED warm pass, then the
#       recorded pass TWICE, then a drift check (>DRIFT_PCT warns)
#   (f) rarpar phase (MANDATORY): tests, then the same bench protocol
#   (g) preserve the raw data: criterion trees tarred, metadata.json,
#       summary.json (doc §9g) — the input for later SVG generation
#   (h) summary + teardown checklist
#
# FULL SUITES, NO FILTERS. Every `cargo bench` below is invoked with a bench
# target and nothing else — no trailing criterion filter argument anywhere, so
# each binary runs its complete lane set. The one suite with an *environment*
# filter (par2_repair / WEAVER_PAR2_BENCH_SCENARIOS) has it explicitly unset in
# rarpar_phase so an inherited value cannot silently narrow the run.
#
# Must be the GNU target, not musl: the parity bench dlopens a shared lib.
set -euo pipefail

# ── Config (all overridable; keep in sync with the doc §7b table) ────────────
# Source delivery is the ECR corpus image (doc §7a); it extracts /corpus/. into
# $CORPUS_DEST, giving $CORPUS_DEST/{weaver,rarpar,rapidyenc}. CORPUS_IMAGE is
# recorded in metadata.json so a chart can always be traced back to its input.
CORPUS_DEST="${CORPUS_DEST:-$HOME}"
CORPUS_IMAGE="${CORPUS_IMAGE:-651588424025.dkr.ecr.us-east-1.amazonaws.com/weaver-bench-corpus:latest}"
WEAVER_DIR="${WEAVER_DIR:-$CORPUS_DEST/weaver}"
RARPAR_DIR="${RARPAR_DIR:-$CORPUS_DEST/rarpar}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$CORPUS_DEST/rapidyenc}"
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$RAPIDYENC_ROOT/build/librapidyenc.so}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
# Perf bench only: let weaver's non-intrinsic driver code use Zen4 for a fair A/B
# vs cmake-Release rapidyenc. Correctness tests run WITHOUT extra rustflags.
BENCH_RUSTFLAGS="${BENCH_RUSTFLAGS:--C target-cpu=native}"
# Deliberately EMPTY: rarpar's GF16 tiers are #[target_feature] + runtime
# dispatch (crates/weaver-reed-solomon/src/gf_simd.rs:232-234,436-438), so
# pinning target-cpu would only obscure which tier actually ran.
RARPAR_BENCH_RUSTFLAGS="${RARPAR_BENCH_RUSTFLAGS:-}"
LOAD_THRESHOLD="${LOAD_THRESHOLD:-0.2}"
STEADY_TIMEOUT="${STEADY_TIMEOUT:-300}"
DRIFT_PCT="${DRIFT_PCT:-2.0}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="${RESULTS_DIR:-$WEAVER_DIR/ci/bench/results/$STAMP}"

log()  { printf '\033[1;34m[run]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[run:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

GATE_FAILURES=0
gate_fail() { printf '\033[1;31m[run:GATE]\033[0m %s\n' "$*" >&2; GATE_FAILURES=$((GATE_FAILURES + 1)); }

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

# ── (b) Source preconditions ─────────────────────────────────────────────────
# These markers are what the run is FOR. The corpus image ships weaver as
# working-tree state precisely so they are present, but a STALE image — built
# before an increment landed — would run a tree that cannot prove any of them
# and would still report a cheerful green. Fail loudly and early instead, and
# cross-check the result against REVISION.json in revisions.txt.
assert_source_preconditions() {
  log "Asserting the weaver tree on this box carries the markers under test…"
  local crc="$WEAVER_DIR/engines/weaver-yenc/src/crc.rs"
  local bench="$WEAVER_DIR/engines/weaver-yenc/benches/rapidyenc_parity.rs"
  local tests="$WEAVER_DIR/engines/weaver-yenc/src/simd/tests.rs"
  local avx512="$WEAVER_DIR/engines/weaver-yenc/src/simd/x86_avx512.rs"

  [ -f "$crc" ]    || die "missing $crc — is WEAVER_DIR right?"
  [ -f "$bench" ]  || die "missing $bench"
  [ -f "$tests" ]  || die "missing $tests (the simd/ module split is expected; a single simd.rs means a very old tree)"
  [ -f "$avx512" ] || die "missing $avx512"

  grep -q '!is_x86_feature_detected!("avx512vl")' "$crc" \
    || die "crc.rs has no '&& !avx512vl' exclusion in available() — stale tree, the C1 proof is impossible (doc §4)"
  grep -q 'VPCLMUL port unavailable on this CPU' "$crc" \
    || die "crc.rs has no visible-skip eprintln in crc32_forced_vpclmul_matches_crc_fast — stale tree, the C1 proof cannot be grepped (doc §4)"
  grep -q 'decoded CRC parity' "$bench" \
    || die "rapidyenc_parity.rs does not assert CRC parity — stale tree (doc §3b)"
  grep -q 'fn forced_tier_kernels_match_scalar_in_production_shape' "$tests" \
    || die "simd/tests.rs has no forced_tier_kernels_match_scalar_in_production_shape — stale tree (doc §2)"
  grep -q 'debug_assert_eq!(m34eqy' "$avx512" \
    || die "simd/x86_avx512.rs has no m34eqy debug_assert_eq! — stale tree (doc §2a)"

  log "Source preconditions PASSED (C1 gate + skip line + CRC parity assert + production-shape test + m34eqy assert)."
}

record_revisions() {
  local out="$RESULTS_DIR/revisions.txt"
  local d
  {
    echo "stamp: $STAMP"
    echo "corpus image: ${CORPUS_IMAGE:-unknown}"
    for d in "$WEAVER_DIR" "$RARPAR_DIR" "$RAPIDYENC_ROOT"; do
      echo "--- $d ---"
      if [ -f "$d/REVISION.json" ]; then
        echo "  repo:      $(revision_field "$d" repo "$(basename "$d")")"
        echo "  rev:       $(revision_field "$d" rev)"
        echo "  dirty:     $(revision_field "$d" dirty_files 0) file(s) at image-build time"
        echo "  staged_at: $(revision_field "$d" staged_at_utc)"
      else
        echo "  NO REVISION.json — this tree did not come from the corpus image (doc §7a)"
      fi
    done
    echo "--- toolchain ---"
    echo "  $(rustc --version 2>/dev/null || echo '?')"
    echo "  $(cargo --version 2>/dev/null || echo '?')"
  } | tee "$out"
}

# ── Steady state ─────────────────────────────────────────────────────────────
# Called ONCE per bench binary, immediately before its DISCARDED warm pass — not
# between the two recorded passes. warm -> pass1 -> pass2 run back-to-back on
# purpose: identical thermal/turbo/cache conditions are exactly what makes the
# drift check a measurement of noise rather than of cooldown.
wait_for_steady_state() {
  local deadline=$((SECONDS + STEADY_TIMEOUT)) load=""
  log "Waiting for steady state (1-min loadavg < $LOAD_THRESHOLD, timeout ${STEADY_TIMEOUT}s)…"
  while [ "$SECONDS" -lt "$deadline" ]; do
    load="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo 99)"
    if awk -v l="$load" -v t="$LOAD_THRESHOLD" 'BEGIN { exit !(l + 0 < t + 0) }'; then
      log "Steady state reached (loadavg1=$load)."
      return 0
    fi
    sleep 10
  done
  warn "loadavg1 still ${load:-?} after ${STEADY_TIMEOUT}s; proceeding anyway (timings may carry noise)"
}

# ── Criterion parsing ────────────────────────────────────────────────────────
# Emits "<bench id>\t<point estimate in ns>". Handles both Criterion layouts:
# id padded onto the same line as `time:`, and id on its own line above it.
CRIT_AWK='
function tons(v, u) {
  if (u == "ns") return v;
  if (u == "ms") return v * 1000000.0;
  if (u == "ps") return v / 1000.0;
  if (u == "s")  return v * 1000000000.0;
  if (u ~ /s$/)  return v * 1000.0;   # us / µs, however the locale encodes it
  return -1.0;
}
{
  line = $0;
  if (line ~ /time:[ \t]*\[/) {
    pre = line; sub(/time:.*/, "", pre);
    gsub(/^[ \t]+/, "", pre); gsub(/[ \t]+$/, "", pre);
    nm = (pre != "") ? pre : pending;
    s = line; sub(/.*\[/, "", s); sub(/\].*/, "", s);
    n = split(s, t, " ");
    if (nm != "" && n >= 4) {
      v = tons(t[3] + 0, t[4]);
      if (v > 0) printf "%s\t%.4f\n", nm, v;
    }
    pending = "";
    next;
  }
  l = line; gsub(/^[ \t]+/, "", l); gsub(/[ \t]+$/, "", l);
  if (l != "" && l !~ /[ \t]/ && l !~ /:/ \
      && l !~ /^(warning|error|Compiling|Finished|Running|Benchmarking|Gnuplot|thread)/) pending = l;
}
'

DRIFT_AWK='
BEGIN { FS = "\t" }
NR == FNR { a[$1] = $2; next }
{
  if (!($1 in a)) { printf "  %-6s %-48s (absent from pass 1)\n", "NEW", $1; miss++; next }
  b = $2 + 0; c = a[$1] + 0;
  if (c <= 0) next;
  d = (b - c) / c * 100.0;
  ad = (d < 0) ? -d : d;
  if (ad > thr) { flag = "DRIFT"; n++ } else { flag = "ok" }
  printf "  %-6s %-48s %14.2f -> %14.2f ns  %+7.2f%%\n", flag, $1, c, b, d;
}
END {
  if (n > 0) printf "  WARNING: %d lane(s) drifted more than %.2f%% between recorded passes\n", n, thr;
  else       printf "  all lanes within %.2f%% between recorded passes\n", thr;
  if (miss > 0) printf "  WARNING: %d lane(s) present only in pass 2\n", miss;
}
'

# ── Bench protocol: steady wait, discarded warm pass, recorded x2, drift ─────
# usage: run_bench_protocol <label> <outdir> <workdir> <rustflags> -- <argv…>
run_bench_protocol() {
  local label="$1" outdir="$2" workdir="$3" rustflags="$4"
  shift 4
  [ "${1:-}" = "--" ] && shift

  mkdir -p "$outdir"
  local warm="$outdir/${label}-warm-DISCARDED.log"
  local p1="$outdir/${label}-pass1.log"
  local p2="$outdir/${label}-pass2.log"
  local rc1 rc2

  wait_for_steady_state

  log "[$label] DISCARDED warm pass (output kept only for triage) -> $warm"
  if [ -n "$rustflags" ]; then
    ( cd "$workdir" && RUSTFLAGS="$rustflags" "$@" ) >"$warm" 2>&1 \
      || warn "[$label] warm pass exited non-zero (see $warm)"
  else
    ( cd "$workdir" && "$@" ) >"$warm" 2>&1 \
      || warn "[$label] warm pass exited non-zero (see $warm)"
  fi

  log "[$label] recorded pass 1 -> $p1"
  set +e
  if [ -n "$rustflags" ]; then
    ( cd "$workdir" && RUSTFLAGS="$rustflags" "$@" ) 2>&1 | tee "$p1"
  else
    ( cd "$workdir" && "$@" ) 2>&1 | tee "$p1"
  fi
  rc1=${PIPESTATUS[0]}

  log "[$label] recorded pass 2 (back-to-back, no cooldown) -> $p2"
  if [ -n "$rustflags" ]; then
    ( cd "$workdir" && RUSTFLAGS="$rustflags" "$@" ) 2>&1 | tee "$p2"
  else
    ( cd "$workdir" && "$@" ) 2>&1 | tee "$p2"
  fi
  rc2=${PIPESTATUS[0]}
  set -e
  log "[$label] recorded exit codes: pass1=$rc1 pass2=$rc2"

  awk "$CRIT_AWK" "$p1" | sort > "$outdir/${label}-pass1.lanes"
  awk "$CRIT_AWK" "$p2" | sort > "$outdir/${label}-pass2.lanes"
  {
    echo "----- drift check: $label (threshold ${DRIFT_PCT}%) -----"
    awk -v thr="$DRIFT_PCT" "$DRIFT_AWK" \
      "$outdir/${label}-pass1.lanes" "$outdir/${label}-pass2.lanes"
  } | tee "$outdir/${label}-drift.txt"

  # Full-suite check: a filter (criterion arg, or an inherited env filter) shows
  # up here as a short lane list. Zero lanes is a hard failure; fewer than the
  # documented expectation is a warning, because a corpus change is legitimate.
  local got expect
  got="$(wc -l < "$outdir/${label}-pass1.lanes" | tr -d ' ')"
  expect="$(expected_lane_count "$label")"
  if [ "$got" -eq 0 ]; then
    gate_fail "$label produced ZERO criterion lanes — bench skipped, or a filter narrowed it to nothing"
  elif [ "$expect" -gt 0 ] && [ "$got" -lt "$expect" ]; then
    warn "[$label] $got lane(s), expected >= $expect — a filter may have narrowed the suite (doc §9g)"
  else
    log "[$label] full suite: $got lane(s) recorded (expected >= ${expect})"
  fi

  BENCH_STATUS="${BENCH_STATUS}
  $label: pass1=$rc1 pass2=$rc2  lanes=$got/${expect}  ($(grep -c '^  DRIFT' "$outdir/${label}-drift.txt" || true) lane(s) over ${DRIFT_PCT}%)"
  [ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ] || gate_fail "$label bench exited non-zero (pass1=$rc1 pass2=$rc2)"
}

# Unconditional lane counts per bench target, verified against the sources:
#   rapidyenc-parity : 5 fixtures x 2 engines + 2 CRC lanes = 12
#                      (benches/rapidyenc_parity.rs:190-196, :230-239, :247-258)
#   decode-simd      : 11 (benches/decode_simd.rs:440-488). The three
#                      WEAVER_YENC_REAL_ARTICLE lanes (:352-400) are NOT counted
#                      — that gate is env-provided and absent by design here.
#   par2-repair / archive-hotspots : scenario- and fixture-driven, so no fixed
#                      expectation; 0 is still a hard failure above.
expected_lane_count() {
  case "$1" in
    rapidyenc-parity) printf '12' ;;
    decode-simd)      printf '11' ;;
    *)                printf '0'  ;;
  esac
}

# ── (d) Proof greps ──────────────────────────────────────────────────────────
# The C1 proof and the differential case counts are printed by eprintln! from
# PASSING tests, so they only exist in the log because every cargo test
# invocation below passes `-- --nocapture`. Without that flag libtest swallows
# them and this whole section silently reports "not found".
C1_SKIP_LINE='skipping crc32_forced_vpclmul_matches_crc_fast: VPCLMUL port unavailable on this CPU'

assert_c1_proof() {
  local logs=("$@")
  echo "----- C1 proof: VPCLMUL port gate excludes avx512vl (doc §4) -----"
  if grep -h -F "$C1_SKIP_LINE" "${logs[@]}" >/dev/null 2>&1; then
    echo "  PROVEN — the gate stood aside on this CPU. Recorded line:"
    { grep -h -F "$C1_SKIP_LINE" "${logs[@]}" 2>/dev/null | head -1 | sed 's/^/    /'; } || true
    echo "    => crc-fast's 4x512 ZMM tier carried every >=256B update instead"
    echo "       (crc.rs:47-58 never taken; crc.rs:70 always taken)"
  else
    echo "  NOT FOUND — expected line was:"
    echo "    $C1_SKIP_LINE"
    gate_fail "C1 proof line absent from the test logs (missing --nocapture, stale crc.rs, or available() unexpectedly true)"
  fi
}

# Expected counts on a green run (doc §3a). Non-zero is the gate; the expected
# value is printed alongside so a corpus change is visible rather than silent.
assert_differential_counts() {
  local logs=("$@")
  local -a labels=(
    "rapidyenc decode_ex differential cases:|5978"
    "rapidyenc incremental differential cases:|2989"
    "rapidyenc chunk-boundary differential cases:|3997"
    "rapidyenc SIMD chunk-boundary differential cases:|41986"
  )
  echo "----- rapidyenc differential corpus (doc §3a) -----"
  local entry label expect actual
  for entry in "${labels[@]}"; do
    label="${entry%%|*}"
    expect="${entry##*|}"
    actual="$(grep -h -F "$label" "${logs[@]}" 2>/dev/null | tail -1 | awk '{print $NF}' || true)"
    if [ -z "${actual:-}" ]; then
      printf '  %-52s MISSING (expected %s)\n' "$label" "$expect"
      gate_fail "differential marker absent: $label (RAPIDYENC_ROOT not honored, or --nocapture missing)"
    elif [ "$actual" -le 0 ] 2>/dev/null; then
      printf '  %-52s %s  <= 0  (expected %s)\n' "$label" "$actual" "$expect"
      gate_fail "differential case count is zero: $label"
    elif [ "$actual" != "$expect" ]; then
      printf '  %-52s %s  (expected %s — corpus changed, not a failure)\n' "$label" "$actual" "$expect"
    else
      printf '  %-52s %s\n' "$label" "$actual"
    fi
  done
  if grep -h -q 'skipping rapidyenc differential' "${logs[@]}" 2>/dev/null; then
    gate_fail "a rapidyenc differential test SKIPPED — RAPIDYENC_ROOT is not a valid checkout"
  fi
}

# ── Phases ───────────────────────────────────────────────────────────────────
BENCH_STATUS=""
TEST_RC_DEBUG=0
TEST_RC_RELEASE=0

weaver_tests() {
  local debug_log="$RESULTS_DIR/weaver-yenc-tests-debug.log"
  local release_log="$RESULTS_DIR/weaver-yenc-tests-release.log"

  # DEBUG: the only pass where the VBMI2 searchEnd probe's bit-identity
  # debug_assert_eq! (simd/x86_avx512.rs:272) is live.
  log "weaver-yenc tests — DEBUG (target=$TARGET) -> $debug_log"
  log "  RAPIDYENC_ROOT=$RAPIDYENC_ROOT (enables the source-compiled differential tests)"
  set +e
  ( cd "$WEAVER_DIR" && RAPIDYENC_ROOT="$RAPIDYENC_ROOT" \
      cargo test -p weaver-yenc --locked --no-fail-fast --target "$TARGET" \
      -- --nocapture ) 2>&1 | tee "$debug_log"
  TEST_RC_DEBUG=${PIPESTATUS[0]}
  set -e
  log "weaver-yenc DEBUG tests exit code: $TEST_RC_DEBUG"

  # RELEASE: the codegen shape production ships (fat LTO, codegen-units=1 —
  # weaver/Cargo.toml:100-104). Slowest build of the run by a wide margin.
  # `panic = "abort"` at :104 is ignored by cargo for test targets; the warning
  # it prints is expected.
  log "weaver-yenc tests — RELEASE (target=$TARGET) -> $release_log"
  set +e
  ( cd "$WEAVER_DIR" && RAPIDYENC_ROOT="$RAPIDYENC_ROOT" \
      cargo test -p weaver-yenc --release --locked --no-fail-fast --target "$TARGET" \
      -- --nocapture ) 2>&1 | tee "$release_log"
  TEST_RC_RELEASE=${PIPESTATUS[0]}
  set -e
  log "weaver-yenc RELEASE tests exit code: $TEST_RC_RELEASE"

  [ "$TEST_RC_DEBUG" -eq 0 ]   || gate_fail "weaver-yenc DEBUG test suite FAILED (see $debug_log) — doc §10 triage"
  [ "$TEST_RC_RELEASE" -eq 0 ] || gate_fail "weaver-yenc RELEASE test suite FAILED (see $release_log) — doc §10 triage"

  # Brace group with a plain redirect, NOT a pipeline: a `| tee` here would run
  # the asserts in a subshell and silently discard their gate_fail increments.
  {
    assert_c1_proof "$debug_log" "$release_log"
    echo
    assert_differential_counts "$debug_log" "$release_log"
  } > "$RESULTS_DIR/proof-gates.txt"
  cat "$RESULTS_DIR/proof-gates.txt"
}

weaver_benches() {
  local outdir="$RESULTS_DIR/weaver"
  # §3b — VBMI2-vs-VBMI2 A/B against the dlopen'd librapidyenc.so.
  # WEAVER_RAPIDYENC_LIB is already exported by main(); no per-call prefix.
  run_bench_protocol "rapidyenc-parity" "$outdir" "$WEAVER_DIR" "$BENCH_RUSTFLAGS" -- \
    cargo bench -p weaver-yenc --locked --bench rapidyenc_parity --target "$TARGET"

  # §9e — production-shape lanes (decode_only vs until_control family gap).
  run_bench_protocol "decode-simd" "$outdir" "$WEAVER_DIR" "$BENCH_RUSTFLAGS" -- \
    cargo bench -p weaver-yenc --locked --bench decode_simd --target "$TARGET"
}

# rarpar phase — MANDATORY (doc §8).
#
# PACKAGE NAMES ARE NOT DIRECTORY NAMES. The crates were renamed on publish and
# the directories deliberately kept their old weaver-* names (weaver/Cargo.toml
# :106-114). Verified with `cargo metadata --no-deps`:
#     crates/weaver-reed-solomon -> reedsolomon-rs
#     crates/weaver-par2         -> par2-rs
#     crates/weaver-unrar        -> unrar-rs
# `-p weaver-par2` fails with "package not found". Do not "fix" these back.
rarpar_phase() {
  [ -d "$RARPAR_DIR" ] || die "RARPAR_DIR '$RARPAR_DIR' not found.
     The rarpar phase is MANDATORY (doc §8). rarpar ships in the corpus image
     alongside weaver, so a missing tree means the pull or the extraction did
     not complete. Re-pull and re-extract (doc §7a):
       CORPUS_FORCE=1 ./ci/bench/c7a-bootstrap.sh
     If it is still absent, the image itself is missing /corpus/rarpar and
     needs rebuilding — do not fall back to a git clone, the unrar bench
     fixtures are git-LFS and would arrive as pointer files."

  local outdir="$RESULTS_DIR/rarpar"
  mkdir -p "$outdir"
  local test_log="$outdir/rarpar-tests.log"
  local rc

  {
    echo "=========================================================="
    echo " rarpar phase — GFNI + AVX-512 GF16 on real Zen 4 silicon"
    echo "=========================================================="
    echo "This is the FIRST real-silicon execution of the"
    echo "  #[target_feature(enable = \"gfni,avx512bw,avx512vl\")]"
    echo "GF16 multiply-accumulate kernels"
    echo "  (crates/weaver-reed-solomon/src/gf_simd.rs:1008, 1303;"
    echo "   dispatch gates at :232-234, :436-438, :521-523, :1138-1140)."
    echo "Every box we own has GFNI without AVX-512, or neither."
    echo "Correctness runs BEFORE timing for exactly that reason."
    echo "=========================================================="
  } | tee "$outdir/README-phase.txt"

  log "rarpar tests (reedsolomon-rs + par2-rs, target=$TARGET) -> $test_log"
  set +e
  ( cd "$RARPAR_DIR" && \
      cargo test --locked -p reedsolomon-rs -p par2-rs --target "$TARGET" \
      -- --nocapture ) 2>&1 | tee "$test_log"
  rc=${PIPESTATUS[0]}
  set -e
  log "rarpar tests exit code: $rc"
  [ "$rc" -eq 0 ] || gate_fail "rarpar test suite FAILED (see $test_log) — the gfni+avx512 GF16 arms are the prime suspect"

  # Same warm/2x/drift protocol as the weaver benches. These two targets are the
  # doc §8-grounded ones:
  #   par2_repair      : rarpar/crates/weaver-par2/Cargo.toml:72-75
  #   archive_hotspots : rarpar/crates/weaver-unrar/Cargo.toml:111-113
  # Do not substitute ppmd_compare or gf16_gpu_vs_cpu — out of scope, and the
  # latter wants a GPU this instance does not have.

  # FULL SCENARIO SET. par2_repair filters its scenarios from the environment
  # (crates/weaver-par2/benches/par2_repair.rs:20 -> select_scenarios, empty
  # filter == run everything). An inherited WEAVER_PAR2_BENCH_SCENARIOS would
  # narrow the run silently and the log would still read green, so clear it
  # here rather than trusting the caller's environment.
  if [ -n "${WEAVER_PAR2_BENCH_SCENARIOS:-}" ]; then
    warn "clearing inherited WEAVER_PAR2_BENCH_SCENARIOS='$WEAVER_PAR2_BENCH_SCENARIOS' — this run records the FULL scenario set"
  fi
  unset WEAVER_PAR2_BENCH_SCENARIOS

  run_bench_protocol "par2-repair" "$outdir" "$RARPAR_DIR" "$RARPAR_BENCH_RUSTFLAGS" -- \
    cargo bench --locked -p par2-rs --bench par2_repair --target "$TARGET"

  run_bench_protocol "archive-hotspots" "$outdir" "$RARPAR_DIR" "$RARPAR_BENCH_RUSTFLAGS" -- \
    cargo bench --locked -p unrar-rs --bench archive_hotspots --target "$TARGET"
}

# ── (g) Data preservation for later SVG generation (doc §9g) ────────────────
#
# Criterion's two-pass bookkeeping is what makes this worth keeping: on every
# run the previous `new/` is rotated into `base/`. Our per-binary sequence is
# warm -> pass1 -> pass2, so the shipped tree ends with
#     base/ = recorded pass 1,  new/ = recorded pass 2
# (the discarded warm pass has already been rotated out). Both recorded passes
# therefore survive, with estimates.json + sample.json intact per lane.

# Provenance comes from each tree's REVISION.json, NOT from git: the corpus
# image ships working-tree state without `.git`, so `git rev-parse` would fail
# on every tree. Schema: {repo, rev, dirty_files, staged_at_utc}. `dirty_files`
# is recorded rather than assumed zero — weaver deliberately ships with
# uncommitted increments staged into the image.
revision_field() {   # <tree-dir> <field> [default]
  local f="$1/REVISION.json" out=""
  if [ -f "$f" ] && command -v jq >/dev/null 2>&1; then
    out="$(jq -r --arg k "$2" '.[$k] // empty' "$f" 2>/dev/null || true)"
  fi
  printf '%s' "${out:-${3:-unknown}}"
}

# Link-local IMDS only — not an AWS API call, no credentials, 2s timeouts so
# this is a fast no-op anywhere that is not an EC2 instance.
detect_instance_type() {
  local token="" itype=""
  token="$(curl -s --max-time 2 -X PUT 'http://169.254.169.254/latest/api/token' \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  if [ -n "$token" ]; then
    itype="$(curl -s --max-time 2 -H "X-aws-ec2-metadata-token: $token" \
      'http://169.254.169.254/latest/meta-data/instance-type' 2>/dev/null || true)"
  fi
  [ -n "$itype" ] || itype="$(curl -s --max-time 2 \
    'http://169.254.169.254/latest/meta-data/instance-type' 2>/dev/null || true)"
  [ -n "$itype" ] || itype="${METADATA_INSTANCE_TYPE:-}"
  printf '%s' "${itype:-unknown}"
}

# RYKERN_* ids from rapidyenc/rapidyenc.h:24-42, as decimal.
rykern_name() {
  case "${1:-}" in
    0)    printf 'GENERIC' ;;
    256)  printf 'SSE2' ;;
    512)  printf 'SSSE3' ;;
    897)  printf 'AVX' ;;
    1027) printf 'AVX2' ;;
    1539) printf 'VBMI2' ;;
    832)  printf 'PCLMUL' ;;
    1088) printf 'VPCLMUL' ;;
    4096) printf 'NEON' ;;
    -1)   printf 'unavailable' ;;
    *)    printf 'unknown' ;;
  esac
}

archive_criterion_trees() {
  local repo root out
  for repo in weaver rarpar; do
    case "$repo" in
      weaver) root="$WEAVER_DIR/target/criterion" ;;
      rarpar) root="$RARPAR_DIR/target/criterion" ;;
      *)      continue ;;
    esac
    out="$RESULTS_DIR/criterion-$repo.tar.gz"
    if [ -d "$root" ]; then
      # -C the parent so the archive unpacks as ./criterion/…
      if tar -czf "$out" -C "$(dirname "$root")" criterion; then
        log "archived $root -> $out ($(du -h "$out" | cut -f1 | tr -d ' '))"
      else
        gate_fail "failed to archive $root"
      fi
    else
      gate_fail "criterion tree missing: $root — benches produced no persisted data"
    fi
  done
}

write_metadata_json() {
  local out="$RESULTS_DIR/metadata.json"
  command -v jq >/dev/null 2>&1 || { gate_fail "jq missing — cannot write metadata.json (recoverable later from the tarballs)"; return 0; }

  local kernels dec_id="" crc_id=""
  kernels="$(grep -h -m1 'rapidyenc kernels:' \
    "$RESULTS_DIR"/weaver/rapidyenc-parity-pass1.log 2>/dev/null || true)"
  if [ -n "$kernels" ]; then
    dec_id="$(printf '%s' "$kernels" | sed -n 's/.*decode=\(-\{0,1\}[0-9]\{1,\}\).*/\1/p')"
    crc_id="$(printf '%s' "$kernels" | sed -n 's/.*crc=\(-\{0,1\}[0-9]\{1,\}\).*/\1/p')"
  fi

  jq -n \
    --arg timestamp_utc  "$STAMP" \
    --arg instance_type  "$(detect_instance_type)" \
    --arg cpu_model      "$(grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || true)" \
    --arg cpu_flags      "$(grep -m1 '^flags' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || true)" \
    --arg cpu_cores      "$(nproc 2>/dev/null || echo 0)" \
    --arg kernel         "$(uname -sr 2>/dev/null || echo unknown)" \
    --arg rustc          "$(rustc -V 2>/dev/null || echo unknown)" \
    --arg target         "$TARGET" \
    --arg bench_rustflags        "$BENCH_RUSTFLAGS" \
    --arg rarpar_bench_rustflags "$RARPAR_BENCH_RUSTFLAGS" \
    --arg corpus_image   "${CORPUS_IMAGE:-unknown}" \
    --arg weaver_rev     "$(revision_field "$WEAVER_DIR" rev)" \
    --arg weaver_dirty   "$(revision_field "$WEAVER_DIR" dirty_files 0)" \
    --arg weaver_staged  "$(revision_field "$WEAVER_DIR" staged_at_utc)" \
    --arg rarpar_rev     "$(revision_field "$RARPAR_DIR" rev)" \
    --arg rarpar_dirty   "$(revision_field "$RARPAR_DIR" dirty_files 0)" \
    --arg rarpar_staged  "$(revision_field "$RARPAR_DIR" staged_at_utc)" \
    --arg rapidyenc_rev  "$(revision_field "$RAPIDYENC_ROOT" rev)" \
    --arg rapidyenc_dirty "$(revision_field "$RAPIDYENC_ROOT" dirty_files 0)" \
    --arg rapidyenc_staged "$(revision_field "$RAPIDYENC_ROOT" staged_at_utc)" \
    --arg dec_id "${dec_id:-}" --arg dec_name "$(rykern_name "${dec_id:-}")" \
    --arg crc_id "${crc_id:-}" --arg crc_name "$(rykern_name "${crc_id:-}")" \
    '{
      timestamp_utc: $timestamp_utc,
      instance_type: $instance_type,
      cpu: { model: $cpu_model, cores: ($cpu_cores | tonumber? // 0), flags: $cpu_flags },
      kernel: $kernel,
      rustc: $rustc,
      target: $target,
      rustflags: { weaver_bench: $bench_rustflags, rarpar_bench: $rarpar_bench_rustflags },
      corpus_image: $corpus_image,
      revision_source: "REVISION.json per tree (corpus image ships no .git)",
      revisions: {
        weaver:    { rev: $weaver_rev,    dirty_files: ($weaver_dirty    | tonumber? // 0), staged_at_utc: $weaver_staged },
        rarpar:    { rev: $rarpar_rev,    dirty_files: ($rarpar_dirty    | tonumber? // 0), staged_at_utc: $rarpar_staged },
        rapidyenc: { rev: $rapidyenc_rev, dirty_files: ($rapidyenc_dirty | tonumber? // 0), staged_at_utc: $rapidyenc_staged }
      },
      rapidyenc_kernels: {
        decode_id:   ($dec_id | tonumber? // null), decode_name: $dec_name,
        crc_id:      ($crc_id | tonumber? // null), crc_name:    $crc_name
      },
      criterion_pass_convention: {
        base: "recorded pass 1",
        new:  "recorded pass 2",
        note: "the discarded warm pass was rotated out before pass 1 landed in base/"
      }
    }' > "$out" \
    && log "wrote $out" \
    || gate_fail "could not write $out"
}

# Flat array of every criterion estimate, both passes, both repos.
# `change/` subdirs are skipped on purpose: their estimates.json is a
# ratio schema (mean/median only), not a timing schema.
write_summary_json() {
  local out="$RESULTS_DIR/summary.json"
  command -v jq >/dev/null 2>&1 || { gate_fail "jq missing — cannot write summary.json (recoverable later from the tarballs)"; return 0; }

  # lane -> bench-target map, taken from the lane lists the parser already
  # produced per binary, so nothing has to be guessed from lane-name prefixes.
  local map="$RESULTS_DIR/lane-to-bench.tsv"
  : > "$map"
  local f label
  for f in "$RESULTS_DIR"/weaver/*-pass1.lanes "$RESULTS_DIR"/rarpar/*-pass1.lanes; do
    [ -f "$f" ] || continue
    label="$(basename "$f" -pass1.lanes)"
    awk -F'\t' -v b="$label" 'NF { print $1 "\t" b }' "$f" >> "$map"
  done

  local ndjson="$RESULTS_DIR/summary.ndjson"
  : > "$ndjson"
  local repo root est pass lane bench sample
  for repo in weaver rarpar; do
    case "$repo" in
      weaver) root="$WEAVER_DIR/target/criterion" ;;
      rarpar) root="$RARPAR_DIR/target/criterion" ;;
      *)      continue ;;
    esac
    [ -d "$root" ] || continue
    while IFS= read -r est; do
      pass="$(basename "$(dirname "$est")")"
      case "$pass" in base|new) : ;; *) continue ;; esac
      lane="$(dirname "$(dirname "$est")")"
      lane="${lane#"$root"/}"
      bench="$(awk -F'\t' -v l="$lane" '$1 == l { print $2; exit }' "$map" || true)"
      [ -n "$bench" ] || bench="unknown"
      sample="$(dirname "$est")/sample.json"
      jq -c -n \
        --arg repo "$repo" --arg bench "$bench" --arg lane "$lane" --arg pass "$pass" \
        --argjson est "$(cat "$est" 2>/dev/null || echo '{}')" \
        --argjson smp "$(cat "$sample" 2>/dev/null || echo '{}')" \
        '{
          repo: $repo, bench: $bench, lane: $lane, pass: $pass,
          mean_ns:      ($est.mean.point_estimate?    // null),
          median_ns:    ($est.median.point_estimate?  // null),
          std_dev_ns:   ($est.std_dev.point_estimate? // null),
          sample_count: (($smp.times? // []) | length)
        }' >> "$ndjson" || warn "could not parse $est"
    done < <(find "$root" -type f -name estimates.json | sort)
  done

  if jq -s '.' "$ndjson" > "$out"; then
    log "wrote $out ($(jq 'length' "$out") estimate rows across both repos)"
    rm -f "$ndjson"
  else
    gate_fail "could not assemble $out"
  fi
}

preserve_data() {
  log "Preserving raw bench data for later SVG generation (doc §9g)…"
  archive_criterion_trees
  write_metadata_json
  write_summary_json
}

print_summary() {
  local summary="$RESULTS_DIR/summary.txt"
  local l lanes d
  {
    echo "=========================================================="
    echo " c7a weaver-yenc + rarpar diffbench summary  ($STAMP)"
    echo "=========================================================="
    echo "target               : $TARGET"
    echo "WEAVER_DIR           : $WEAVER_DIR"
    echo "RARPAR_DIR           : $RARPAR_DIR"
    echo "RAPIDYENC_ROOT       : $RAPIDYENC_ROOT"
    echo "WEAVER_RAPIDYENC_LIB : $WEAVER_RAPIDYENC_LIB"
    echo "weaver bench RUSTFLAGS: $BENCH_RUSTFLAGS"
    echo "rarpar bench RUSTFLAGS: ${RARPAR_BENCH_RUSTFLAGS:-<none>}"
    echo "steady state         : loadavg1 < $LOAD_THRESHOLD (timeout ${STEADY_TIMEOUT}s)"
    echo "drift threshold      : ${DRIFT_PCT}%"
    echo

    echo "----- weaver-yenc tests -----"
    echo "  debug   exit: $TEST_RC_DEBUG"
    echo "  release exit: $TEST_RC_RELEASE"
    for l in "$RESULTS_DIR/weaver-yenc-tests-debug.log" "$RESULTS_DIR/weaver-yenc-tests-release.log"; do
      [ -f "$l" ] || continue
      echo "  $(basename "$l"):"
      awk '
        /test result:/ {
          for (i = 1; i <= NF; i++) {
            if ($(i+1) == "passed;")  p  += $i;
            if ($(i+1) == "failed;")  f  += $i;
            if ($(i+1) == "ignored;") ig += $i;
          }
        }
        END { printf "    totals: %d passed, %d failed, %d ignored\n", p, f, ig }
      ' "$l"
      grep -E "^test .* \.\.\. FAILED" "$l" | sed 's/^/    FAILED: /' || true
    done
    echo

    if [ -f "$RESULTS_DIR/proof-gates.txt" ]; then
      cat "$RESULTS_DIR/proof-gates.txt"
      echo
    fi

    echo "----- rapidyenc kernel ids (doc §3b; expect decode=1539 crc=1088) -----"
    grep -h "rapidyenc kernels:" "$RESULTS_DIR"/weaver/rapidyenc-parity-pass*.log 2>/dev/null \
      | sort -u | sed 's/^/  /' || echo "  (not found — parity bench skipped?)"
    grep -h "^parity ok \[" "$RESULTS_DIR"/weaver/rapidyenc-parity-pass1.log 2>/dev/null \
      | sed 's/^/  /' || true
    if grep -hq "skipping rapidyenc parity bench" "$RESULTS_DIR"/weaver/rapidyenc-parity-pass*.log 2>/dev/null; then
      echo "  SKIPPED — WEAVER_RAPIDYENC_LIB not set/loadable"
    fi
    echo

    echo "----- bench passes (label: exit codes, drifting lanes) -----"
    printf '%s\n' "${BENCH_STATUS:-  (none run)}"
    echo

    echo "----- recorded point estimates, pass 1 (ns) -----"
    for lanes in "$RESULTS_DIR"/weaver/*-pass1.lanes "$RESULTS_DIR"/rarpar/*-pass1.lanes; do
      [ -f "$lanes" ] || continue
      echo "  $(basename "$lanes" -pass1.lanes):"
      sed 's/^/    /' "$lanes"
    done
    echo

    echo "----- drift reports -----"
    for d in "$RESULTS_DIR"/weaver/*-drift.txt "$RESULTS_DIR"/rarpar/*-drift.txt; do
      [ -f "$d" ] || continue
      cat "$d"
    done
    echo

    echo "----- preserved data (doc §9g) -----"
    for l in criterion-weaver.tar.gz criterion-rarpar.tar.gz metadata.json summary.json; do
      if [ -s "$RESULTS_DIR/$l" ]; then
        printf '  OK      %-26s %s\n' "$l" "$(du -h "$RESULTS_DIR/$l" | cut -f1 | tr -d ' ')"
      else
        printf '  MISSING %-26s\n' "$l"
      fi
    done
    if [ -s "$RESULTS_DIR/summary.json" ] && command -v jq >/dev/null 2>&1; then
      echo "  summary.json rows: $(jq 'length' "$RESULTS_DIR/summary.json" 2>/dev/null || echo '?')"
      echo "  rows per repo/pass:"
      jq -r 'group_by(.repo + "/" + .pass) | .[] | "    \(.[0].repo)/\(.[0].pass): \(length)"' \
        "$RESULTS_DIR/summary.json" 2>/dev/null || true
    fi
    echo

    echo "gate failures: $GATE_FAILURES"
    echo "results dir  : $RESULTS_DIR"
    echo "=========================================================="
  } | tee "$summary"
}

print_teardown_checklist() {
  printf '\n\033[1;36m'
  cat <<EOF
============================================================
  TEARDOWN CHECKLIST — do these in order, by hand
------------------------------------------------------------
  1. COPY RESULTS OFF-BOX FIRST, before touching the instance:
       scp -r <box>:$RESULTS_DIR ./
  2. VERIFY locally that these FOUR arrived and are non-empty. They are
     the raw material for later SVG generation and CANNOT be
     reconstructed once the instance is gone:
       (a) criterion-weaver.tar.gz   full weaver criterion tree
       (b) criterion-rarpar.tar.gz   full rarpar criterion tree
       (c) metadata.json             instance/cpu/kernel/rustc/revisions
       (d) summary.json              flat per-lane estimates, both passes
     Spot-check them locally before terminating:
       tar -tzf criterion-weaver.tar.gz | head
       jq '.instance_type, .rapidyenc_kernels' metadata.json
       jq 'length' summary.json
  3. VERIFY the rest of the run also arrived:
       summary.txt, revisions.txt, proof-gates.txt,
       weaver-yenc-tests-debug.log, weaver-yenc-tests-release.log,
       weaver/*-pass1.log, weaver/*-pass2.log, weaver/*-drift.txt,
       rarpar/rarpar-tests.log, rarpar/*-pass1.log, rarpar/*-pass2.log
     Fill in doc §9 (c7a-avx512-diffbench.md) from them NOW, while the
     box still exists — a missing number is cheap to re-measure today
     and expensive to re-measure next week.
  3. TERMINATE the instance (root volume is DeleteOnTermination).
  4. DELETE the session security group.
  5. DELETE the ephemeral keypair.
  6. CONFIRM in the console that no instance, SG or keypair from this
     session remains.

  The bootstrap armed a dead-man 'shutdown -h +240'. That only
  TERMINATES if the instance was launched with
  --instance-initiated-shutdown-behavior terminate
  (cf. ci/bench/avx2-aws-run.sh:94). Otherwise it merely stops the
  instance and the EBS root volume keeps billing. Do not rely on it.
============================================================
EOF
  printf '\033[0m\n'
}

main() {
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found"

  # Prefer the env file the bootstrap wrote (resolves versioned .so names).
  if [ -f "$RAPIDYENC_ROOT/weaver-bench.env" ]; then
    # shellcheck disable=SC1091
    . "$RAPIDYENC_ROOT/weaver-bench.env"
  fi
  export RAPIDYENC_ROOT WEAVER_RAPIDYENC_LIB
  # Deliberately NOT set: it would statically link rapidyenc into weaver via
  # build.rs:15 and change weaver's own codegen (doc §3c).
  unset WEAVER_RAPIDYENC_SRC 2>/dev/null || true

  mkdir -p "$RESULTS_DIR"

  assert_cpu_features | tee "$RESULTS_DIR/cpu-features.log"
  {
    echo "CPU model: $(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
    echo "cores    : $(nproc)"
    echo "flags: $(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2-)"
  } >> "$RESULTS_DIR/cpu-features.log"

  assert_source_preconditions
  record_revisions

  # Warn (don't skip) if the rapidyenc reference is not wired up.
  [ -d "$RAPIDYENC_ROOT" ]        || warn "RAPIDYENC_ROOT=$RAPIDYENC_ROOT missing — differential tests will SKIP"
  [ -e "$WEAVER_RAPIDYENC_LIB" ]  || warn "WEAVER_RAPIDYENC_LIB=$WEAVER_RAPIDYENC_LIB missing — parity bench will SKIP"
  [ -d "$RARPAR_DIR" ]            || warn "RARPAR_DIR=$RARPAR_DIR missing — the mandatory rarpar phase will ABORT"

  weaver_tests
  weaver_benches
  rarpar_phase
  preserve_data

  print_summary
  print_teardown_checklist

  if [ "$GATE_FAILURES" -ne 0 ]; then
    die "$GATE_FAILURES gate(s) FAILED — see $RESULTS_DIR/summary.txt and doc §10 triage"
  fi
  log "DONE — all gates green. Results in $RESULTS_DIR"
}

main "$@"
