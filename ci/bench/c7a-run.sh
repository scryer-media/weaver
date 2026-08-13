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
#   (c) weaver-yenc tests FIRST, DEBUG then RELEASE, with RAPIDYENC_ROOT + CXX
#       set so the runtime-compiled rapidyenc differential oracle RUNS (not
#       skips), and `--nocapture` so their case counts and the C1 skip line
#       reach the log. debug != release here: the VBMI2 searchEnd probe carries
#       a `debug_assert_eq!` (simd/x86_avx512.rs:272) compiled out in release.
#   (d) grep-assert the C1 proof line and non-zero differential case counts
#   (e) weaver benches: steady-state wait, DISCARDED warm pass, then the
#       recorded pass TWICE, then a drift check (>DRIFT_PCT warns)
#   (f) rarpar phase (MANDATORY): EVERY manifest test binary for repo=rarpar
#       (12 of them; slow-tests-gated ones report 0 tests and pass as
#       'gated-empty'), then the same bench protocol
#   (g) preserve the raw data: criterion trees tarred, metadata.json,
#       summary.json (doc §9g) — the input for later SVG generation
#   (h) summary + teardown checklist
#
# PREBUILT-BINARY MODEL (plan v2). There is NO cargo and NO Rust toolchain on
# this box. Every test and bench executable is resolved from the prebuilt
# bundle's manifest.json ($PREBUILT_DIR/bin/<id>) and invoked directly:
#   * libtest binaries take `--nocapture` as a direct argument (no `--`
#     separator, because there is no cargo in front to split against);
#   * criterion bench binaries are harness=false, so they need `--bench` to
#     enter benchmarking mode instead of libtest mode — that is exactly the
#     flag `cargo bench` used to pass for us.
# Criterion resolves its output directory from CARGO_TARGET_DIR, so each repo
# gets one under the results area and preserve_data archives those.
#
# FULL SUITES, NO FILTERS. No bench binary is given a criterion filter
# argument, so each runs its complete lane set. The one suite with an
# *environment* filter (par2_repair / WEAVER_PAR2_BENCH_SCENARIOS) has it
# explicitly unset in rarpar_phase so an inherited value cannot narrow the run.
#
# The binaries are plain x86_64-unknown-linux-gnu with NO target-cpu flags, so
# every kernel tier is compiled in and selected by runtime dispatch — which is
# the whole point: pinning would have decided the answer at build time.
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
PREBUILT_DIR="${PREBUILT_DIR:-$CORPUS_DEST/prebuilt}"
MANIFEST_JSON="$PREBUILT_DIR/manifest.json"
BUILDINFO_JSON="$PREBUILT_DIR/BUILDINFO.json"
# The parity bench dlopens the PREBUILT .so (benches/rapidyenc_parity.rs:34-35).
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$PREBUILT_DIR/lib/librapidyenc.so}"
# The differential test binaries compile their C oracle at RUNTIME
# (tests/rapidyenc_decode_diff.rs:146-159), so a C++ compiler is still needed.
CXX="${CXX:-g++}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"   # informational: what the bundle was built for
LOAD_THRESHOLD="${LOAD_THRESHOLD:-0.2}"
STEADY_TIMEOUT="${STEADY_TIMEOUT:-300}"
DRIFT_PCT="${DRIFT_PCT:-2.0}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="${RESULTS_DIR:-$WEAVER_DIR/ci/bench/results/$STAMP}"
# Criterion picks its output dir from CARGO_TARGET_DIR and writes <dir>/criterion.
# One per repo, under the results area, so the data lands where preserve_data
# archives it instead of inside a source tree.
WEAVER_CRIT_DIR="$RESULTS_DIR/criterion/weaver"
RARPAR_CRIT_DIR="$RESULTS_DIR/criterion/rarpar"

log()  { printf '\033[1;34m[run]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[run:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# True when one rev is a prefix of the other and the shorter side is ≥7 chars
# (REVISION.json carries full 40-char revs; BUILDINFO the short form).
revs_agree() {
  local a="$1" b="$2" short
  [ -n "$a" ] && [ -n "$b" ] || return 1
  if [ "${#a}" -le "${#b}" ]; then
    short="$a"; case "$b" in "$a"*) : ;; *) return 1 ;; esac
  else
    short="$b"; case "$a" in "$b"*) : ;; *) return 1 ;; esac
  fi
  [ "${#short}" -ge 7 ]
}

GATE_FAILURES=0
gate_fail() { printf '\033[1;31m[run:GATE]\033[0m %s\n' "$*" >&2; GATE_FAILURES=$((GATE_FAILURES + 1)); }

# ── Retired knobs ────────────────────────────────────────────────────────────
# BENCH_RUSTFLAGS used to default to `-C target-cpu=native` so weaver's driver
# code was tuned for the host in the parity A/B. v2 deletes the idea outright:
# both sides of that A/B are now prebuilt with plain flags (weaver via the
# bundle, rapidyenc via a generic cmake Release .so), and the AVX-512 kernels
# are `#[target_feature]`-compiled regardless of tuning — so native tuning would
# only bias one side. There is also no cargo here to rebuild with. Warn and
# ignore rather than silently implying it did something.
for _rf in BENCH_RUSTFLAGS RARPAR_BENCH_RUSTFLAGS RUSTFLAGS; do
  if [ -n "${!_rf:-}" ]; then
    warn "$_rf is set ('${!_rf}') but IGNORED — v2 runs prebuilt binaries; nothing is compiled on this box (doc §6)"
  fi
done
unset _rf

# ── Prebuilt bundle: manifest-driven binary resolution ───────────────────────
# Schema per entry: {id, kind: test|bench, repo, crate, profile, orig_name,
# needs_env}. Tolerates a bare top-level array or a {binaries|bins:[…]} wrapper.
manifest_ids() {
  jq -r '(if type=="array" then . elif type=="object" then (.binaries // .bins // []) else [] end) | .[].id' \
    "$MANIFEST_JSON" 2>/dev/null || true
}
manifest_field() {   # <id> <field>
  jq -r --arg id "$1" --arg k "$2" \
    '(if type=="array" then . elif type=="object" then (.binaries // .bins // []) else [] end)
     | map(select(.id == $id)) | .[0][$k] // empty' \
    "$MANIFEST_JSON" 2>/dev/null || true
}
manifest_ids_where() {   # <kind> <repo> -> newline-separated ids
  jq -r --arg k "$1" --arg r "$2" \
    '(if type=="array" then . elif type=="object" then (.binaries // .bins // []) else [] end)
     | map(select(.kind == $k and .repo == $r)) | .[].id' \
    "$MANIFEST_JSON" 2>/dev/null || true
}
manifest_needs_env() {   # <id> -> newline-separated env var names
  jq -r --arg id "$1" \
    '(if type=="array" then . elif type=="object" then (.binaries // .bins // []) else [] end)
     | map(select(.id == $id)) | .[0].needs_env // [] | .[]' \
    "$MANIFEST_JSON" 2>/dev/null || true
}
buildinfo_field() {  # <field> [default]
  local out=""
  if [ -f "$BUILDINFO_JSON" ]; then
    out="$(jq -r --arg k "$1" '.[$k] // empty' "$BUILDINFO_JSON" 2>/dev/null || true)"
  fi
  printf '%s' "${out:-${2:-unknown}}"
}

# Resolve <id> to an executable path, verifying the manifest's needs_env are
# actually set. An unset needs_env entry is exactly how a "green" run ends up
# proving nothing (RAPIDYENC_ROOT missing => the diff tests skip silently), so
# it is a gate, not a warning.
resolve_bin() {   # <id> -> prints path, non-zero on failure
  local id="$1" bin need missing=""
  bin="$PREBUILT_DIR/bin/$id"
  if [ ! -x "$bin" ]; then
    gate_fail "prebuilt binary '$id' missing or not executable at $bin"
    return 1
  fi
  while IFS= read -r need; do
    [ -n "$need" ] || continue
    if [ -z "${!need:-}" ]; then missing="$missing $need"; fi
  done <<EOF
$(manifest_needs_env "$id")
EOF
  if [ -n "$missing" ]; then
    gate_fail "binary '$id' declares needs_env:${missing} but they are unset — it would skip its real work and still exit 0"
    return 1
  fi
  printf '%s' "$bin"
}

assert_bundle_present() {
  command -v jq >/dev/null 2>&1 || die "jq is required to read the prebuilt manifest"
  [ -f "$MANIFEST_JSON" ]  || die "missing $MANIFEST_JSON — run ./ci/bench/c7a-bootstrap.sh first (doc §7b)"
  [ -f "$BUILDINFO_JSON" ] || die "missing $BUILDINFO_JSON"
  jq -e . "$MANIFEST_JSON" >/dev/null 2>&1 || die "$MANIFEST_JSON is not valid JSON"
  # Cheap re-check of the bootstrap's hard gate: this script can be run on its
  # own, and a re-extracted corpus without a re-bootstrap would otherwise slip
  # a stale bundle past every downstream gate while reporting green.
  local pair name field tree bi tr
  for pair in "weaver:weaver_rev:$WEAVER_DIR" "rarpar:rarpar_rev:$RARPAR_DIR" "rapidyenc:rapidyenc_rev:$RAPIDYENC_ROOT"; do
    name="${pair%%:*}"; tree="${pair##*:}"
    field="$(printf '%s' "$pair" | cut -d: -f2)"
    bi="$(buildinfo_field "$field")"; tr="$(revision_field "$tree" rev)"
    # Prefix-tolerant (short vs full rev forms), ≥7 chars — mirrors bootstrap.
    revs_agree "$bi" "$tr" || die "STALE BUNDLE: $name built from $bi but tree is $tr — re-run c7a-bootstrap.sh (doc §6)"
  done
  log "Prebuilt bundle present; BUILDINFO revs match all three trees."
}

# ── (a) CPU feature assertion ────────────────────────────────────────────────
# The BMI/POPCNT/LZCNT half is not decorative: the decoder's VBMI2 tier gates
# on all nine features (see vbmi2_tier_available()), so a box missing them
# silently benchmarks the AVX2 tier instead.
REQUIRED_FEATURES="avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes bmi1 bmi2 popcnt lzcnt"
assert_cpu_features() {
  log "Asserting CPU features: ${REQUIRED_FEATURES}"
  [ -r /proc/cpuinfo ] || die "/proc/cpuinfo unreadable"
  local flags missing="" f
  flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2- || true)"
  [ -n "$flags" ] || die "could not read CPU flags"
  # Linux spells VBMI2 as avx512_vbmi2 and LZCNT as abm in /proc/cpuinfo —
  # normalize (see c7a-bootstrap.sh assert_cpu_features).
  case " $flags " in *" avx512_vbmi2 "*) flags="$flags avx512vbmi2" ;; esac
  case " $flags " in *" abm "*) flags="$flags lzcnt" ;; esac
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
    echo "--- prebuilt bundle (no toolchain on this host) ---"
    echo "  builder:      $(buildinfo_field builder)"
    echo "  built_at_utc: $(buildinfo_field built_at_utc)"
    echo "  rustc:        $(buildinfo_field rustc_verbose "$(buildinfo_field rustc)")"
    echo "  rustflags:    '$(buildinfo_field rustflags '')'"
    echo "  glibc build:  $(buildinfo_field glibc)   host: $(ldd --version 2>/dev/null | head -1 | awk '{print $NF}' || echo '?')"
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
# usage: run_bench_protocol <label> <outdir> <workdir> <criterion-target-dir> -- <argv…>
#
# The 4th argument is CARGO_TARGET_DIR, which is how criterion (running
# standalone, with no cargo anywhere) decides where to keep its data: it writes
# <dir>/criterion/<lane>/{base,new,change}. Standalone mode maintains the same
# base/new rotation cargo-driven runs get, so warm -> pass1 -> pass2 still
# leaves base=pass1 and new=pass2.
run_bench_protocol() {
  local label="$1" outdir="$2" workdir="$3" critdir="$4"
  shift 4
  [ "${1:-}" = "--" ] && shift

  mkdir -p "$outdir" "$critdir"
  local warm="$outdir/${label}-warm-DISCARDED.log"
  local p1="$outdir/${label}-pass1.log"
  local p2="$outdir/${label}-pass2.log"
  local rc1 rc2

  wait_for_steady_state

  log "[$label] DISCARDED warm pass (output kept only for triage) -> $warm"
  ( cd "$workdir" && CARGO_TARGET_DIR="$critdir" "$@" ) >"$warm" 2>&1 \
    || warn "[$label] warm pass exited non-zero (see $warm)"

  log "[$label] recorded pass 1 -> $p1"
  set +e
  ( cd "$workdir" && CARGO_TARGET_DIR="$critdir" "$@" ) 2>&1 | tee "$p1"
  rc1=${PIPESTATUS[0]}

  log "[$label] recorded pass 2 (back-to-back, no cooldown) -> $p2"
  ( cd "$workdir" && CARGO_TARGET_DIR="$critdir" "$@" ) 2>&1 | tee "$p2"
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
# PASSING tests, so they only exist in the log because every test binary below
# is invoked with `--nocapture`. Without that flag libtest swallows them and
# this whole section silently reports "not found".
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

# ── Failure containment (standing order: save partials, resume-not-restart) ──
#
# Every suite phase is independently resumable and independently failable. A
# phase that dies must not take the rest of the run with it: if full-corpus
# generation or its run fails, the perf suite and the criterion phases still
# execute, and whatever the failed phase managed to produce stays on disk.
#
# Two mechanisms:
#   * `run_phase <name> <fn>` never propagates a non-zero status. It records the
#     outcome, writes a per-phase marker, and returns 0 so main() continues.
#   * With RESUME=1 and an explicit RESULTS_DIR pointing at a previous run, a
#     phase whose .ok marker exists is SKIPPED rather than redone.
#
# All phase outputs are written directly under $RESULTS_DIR as they are
# produced — evidence dirs, harness logs, criterion trees, lane files. Nothing
# is staged elsewhere and copied at the end; the end-of-run tarballs are
# convenience archives of data that is already in place, so a run killed
# mid-flight still leaves everything it had finished.
RESUME="${RESUME:-0}"
PHASE_RESULTS=""

run_phase() {   # <name> <function>
  local name="$1" fn="$2" marker="$RESULTS_DIR/.phase-${1}.ok" rc=0
  if [ "$RESUME" = "1" ] && [ -f "$marker" ]; then
    log "───── phase SKIPPED (already complete, RESUME=1): $name"
    PHASE_RESULTS="${PHASE_RESULTS}
  $name: skipped (resumed)"
    return 0
  fi
  phase_begin "$name"
  set +e
  "$fn"
  rc=$?
  set -e
  phase_end
  if [ "$rc" -eq 0 ]; then
    : > "$marker"
    PHASE_RESULTS="${PHASE_RESULTS}
  $name: ok"
  else
    rm -f "$marker"
    PHASE_RESULTS="${PHASE_RESULTS}
  $name: FAILED (rc=$rc) — run continued"
    warn "phase '$name' failed (rc=$rc); continuing with the remaining phases (partials kept under $RESULTS_DIR)"
  fi
  return 0
}

# ── Phase timing ─────────────────────────────────────────────────────────────
# No estimates anywhere in this runbook — the run MEASURES its own phases and
# records them in metadata.json. Wall time is an output, never a prediction.
PHASE_TSV="$RESULTS_DIR/phase-timings.tsv"
PHASE_NAME=""; PHASE_T0=0
phase_begin() {
  PHASE_NAME="$1"; PHASE_T0="$(date +%s)"
  log "───── phase start: $PHASE_NAME"
}
phase_end() {
  local t1 dt
  t1="$(date +%s)"; dt=$((t1 - PHASE_T0))
  printf '%s\t%s\t%s\n' "$PHASE_NAME" "$PHASE_T0" "$dt" >> "$PHASE_TSV"
  log "───── phase end:   $PHASE_NAME (${dt}s)"
}

# ── Phases ───────────────────────────────────────────────────────────────────
BENCH_STATUS=""
TEST_RC_DEBUG=0
TEST_RC_RELEASE=0

# Run one prebuilt libtest binary, appending to <log>. libtest takes
# `--nocapture` directly — there is no cargo in front to separate against.
run_test_bin() {   # <id> <log>  -> sets RUN_TEST_RC
  local id="$1" log="$2" bin
  RUN_TEST_RC=1
  bin="$(resolve_bin "$id")" || return 1
  log "  test binary $id  ($(manifest_field "$id" crate), profile $(manifest_field "$id" profile))"
  set +e
  ( cd "$WEAVER_DIR" && \
      RAPIDYENC_ROOT="$RAPIDYENC_ROOT" CXX="$CXX" \
      WEAVER_RAPIDYENC_LIB="$WEAVER_RAPIDYENC_LIB" \
      "$bin" --nocapture ) 2>&1 | tee -a "$log"
  RUN_TEST_RC=${PIPESTATUS[0]}
  set -e
  log "  $id exit code: $RUN_TEST_RC"
  return 0
}

weaver_tests() {
  local debug_log="$RESULTS_DIR/weaver-yenc-tests-debug.log"
  local release_log="$RESULTS_DIR/weaver-yenc-tests-release.log"
  : > "$debug_log"; : > "$release_log"

  # DEBUG: the only pass where the VBMI2 searchEnd probe's bit-identity
  # debug_assert_eq! (simd/x86_avx512.rs:272) is live.
  log "weaver-yenc tests — DEBUG -> $debug_log"
  log "  RAPIDYENC_ROOT=$RAPIDYENC_ROOT  CXX=$CXX (the diff oracle is compiled at runtime)"
  TEST_RC_DEBUG=0
  run_test_bin weaver-yenc-lib-debug  "$debug_log" || TEST_RC_DEBUG=1
  [ "${RUN_TEST_RC:-1}" -eq 0 ] || TEST_RC_DEBUG=1
  run_test_bin weaver-yenc-diff-debug "$debug_log" || TEST_RC_DEBUG=1
  [ "${RUN_TEST_RC:-1}" -eq 0 ] || TEST_RC_DEBUG=1
  log "weaver-yenc DEBUG aggregate exit: $TEST_RC_DEBUG"

  # RELEASE: the codegen shape production ships. With prebuilt binaries this is
  # no longer a build at all — it is just another exec.
  log "weaver-yenc tests — RELEASE -> $release_log"
  TEST_RC_RELEASE=0
  run_test_bin weaver-yenc-lib-release  "$release_log" || TEST_RC_RELEASE=1
  [ "${RUN_TEST_RC:-1}" -eq 0 ] || TEST_RC_RELEASE=1
  run_test_bin weaver-yenc-diff-release "$release_log" || TEST_RC_RELEASE=1
  [ "${RUN_TEST_RC:-1}" -eq 0 ] || TEST_RC_RELEASE=1
  log "weaver-yenc RELEASE aggregate exit: $TEST_RC_RELEASE"

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
  local outdir="$RESULTS_DIR/weaver" bin
  # `--bench` puts a harness=false criterion binary into benchmarking mode;
  # without it the binary runs libtest-style and measures nothing. No filter
  # argument follows it, so the full lane set runs.
  #
  # §3b — VBMI2-vs-VBMI2 A/B against the dlopen'd librapidyenc.so.
  # WEAVER_RAPIDYENC_LIB is exported by main(); resolve_bin gates on it via the
  # manifest's needs_env.
  if bin="$(resolve_bin weaver-yenc-bench-parity)"; then
    run_bench_protocol "rapidyenc-parity" "$outdir" "$WEAVER_DIR" "$WEAVER_CRIT_DIR" -- \
      "$bin" --bench
  fi

  # §9e — production-shape lanes (decode_only vs until_control family gap).
  if bin="$(resolve_bin weaver-yenc-bench-decode-simd)"; then
    run_bench_protocol "decode-simd" "$outdir" "$WEAVER_DIR" "$WEAVER_CRIT_DIR" -- \
      "$bin" --bench
  fi
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
rarpar_tests_only() {
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

  # EVERY rarpar test binary in the bundle, not a hardcoded pair. The `-p`
  # build produces one binary per test target, so par2 alone contributes ~10
  # integration binaries beyond the two headline ids; their manifest ids follow
  # orig_name and are not worth enumerating here. Iterate the manifest instead,
  # so a bundle that gains or loses a test target needs no edit to this script.
  local ids id bin one empty=0 ran=0
  ids="$(manifest_ids_where test rarpar)"
  [ -n "$ids" ] || { gate_fail "manifest lists no rarpar test binaries"; return 0; }
  log "rarpar tests: $(printf '%s\n' "$ids" | grep -c . ) prebuilt binaries -> $test_log"
  : > "$test_log"
  rc=0
  local per="$outdir/.per-bin.log"
  while IFS= read -r id; do
    [ -n "$id" ] || continue
    if ! bin="$(resolve_bin "$id")"; then rc=1; continue; fi
    log "  test binary $id  ($(manifest_field "$id" crate), profile $(manifest_field "$id" profile))"
    set +e
    ( cd "$RARPAR_DIR" && "$bin" --nocapture ) >"$per" 2>&1
    one=$?
    set -e
    {
      echo "===== $id ($(manifest_field "$id" orig_name)) ====="
      cat "$per"
      echo
    } >> "$test_log"
    ran=$((ran + 1))
    # A binary reporting 0 tests is EXPECTED for the slow-tests-gated targets:
    # under default features they compile to an empty suite. Exiting 0 with no
    # tests is a gated-empty pass, not a failure — but count and surface it so
    # "everything ran" can never be inferred from a wall of zeros.
    if [ "$one" -eq 0 ] && grep -q '^running 0 tests' "$per"; then
      empty=$((empty + 1))
      log "    gated-empty (0 tests under default features) — OK"
    else
      log "    exit $one  $(grep -m1 '^test result:' "$per" || true)"
    fi
    [ "$one" -eq 0 ] || rc=1
  done <<EOF
$ids
EOF
  rm -f "$per"
  log "rarpar tests: $ran binaries, $empty gated-empty, aggregate exit $rc"
  [ "$rc" -eq 0 ] || gate_fail "rarpar test suite FAILED (see $test_log) — the gfni+avx512 GF16 arms are the prime suspect"

}

# rarpar criterion micro-benches — SECONDARY to the rarpar-bench corpus suite
# (doc §8a), which is the headline. Kept because they isolate the GF16 and
# unrar hot paths that the corpus suite only exercises end-to-end.
rarpar_criterion() {
  local outdir="$RESULTS_DIR/rarpar" bin
  mkdir -p "$outdir"
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

  if bin="$(resolve_bin rarpar-bench-par2-repair)"; then
    run_bench_protocol "par2-repair" "$outdir" "$RARPAR_DIR" "$RARPAR_CRIT_DIR" -- \
      "$bin" --bench
  fi

  if bin="$(resolve_bin rarpar-bench-archive-hotspots)"; then
    run_bench_protocol "archive-hotspots" "$outdir" "$RARPAR_DIR" "$RARPAR_CRIT_DIR" -- \
      "$bin" --bench
  fi
}

# ── HEADLINE PHASE: the rarpar-bench corpus suite (doc §8a) ──────────────────
#
# Everything below is grounded in a read of the harness, not in guesswork:
#   cmd/rarpar-bench/main.go:25-45      subcommands: toolchains|corpus|plan|
#                                       preflight|run|report|render
#   main.go:53                          plan create --corpus --out [--seed]
#                                       [--lane] [--family] [--par2-placement]
#                                       [--warmups] [--repeats]
#   main.go:55,168-201                  run --corpus --plan --candidate --out
#                                       [--reference-rar --reference-par2]
#                                       [--candidate-label --reference-label]
#                                       [--machine] [--perf]
#   main.go:104-108                     corpus verify --root DIR
#   main.go:56-57                       report --input raw.json --out FILE
#                                       render --input report.json --out DIR
#   main.go:189-199                     run reads <corpus>/corpus.json for the
#                                       digest and LoadPlan validates the plan
#                                       against it — so plan and corpus cannot
#                                       drift apart silently
#   internal/bench/run.go:123           Run writes raw.json into --out
#
# WHY NO DOCKER IS NEEDED HERE (the explicit blocker check):
#   Docker appears in exactly three places in the harness —
#     internal/bench/corpus.go:113-237,444-481  corpus GENERATION
#     internal/bench/toolchain.go:67-92         toolchain image builds
#     internal/bench/host.go:32                 `docker version` probe
#   The first two are the `corpus generate` / `toolchains build` paths, which
#   this run never invokes because the corpus ships pre-generated and
#   digest-addressed. The third is inside CollectMachine and goes through
#   commandLine() (host.go:77-83), which SWALLOWS the error and returns "" — so
#   a Docker-less box merely records an empty docker_version.
#   `corpus verify` (corpus.go:546+) is pure filesystem + digest checking.
#   NOTE: `rarpar-bench preflight` DOES hard-require Docker *and* Go
#   (host.go:85-96, "Docker is required for corpus generation"). We never call
#   it — calling it would fail on this box for no useful reason.
#   Lane must stay `cpu`; `docker-cpu` is a valid lane string (plan.go:13-14)
#   and would be the one way to drag Docker back in.
#
# ALSO DELIBERATELY NOT PASSED: --source-manifest / --source-target. That path
# shells out to `cargo run -p xtask` and requires a Git checkout
# (run.go:859-875, "source benchmark must run from a Git checkout"). The corpus
# image ships no .git and no cargo, so passing it would hard-fail. Provenance
# comes from REVISION.json instead (§9g).
RARPAR_BENCH_DIR="${RARPAR_BENCH_DIR:-$RARPAR_DIR/bench/rarpar-bench}"
MACHINE_LABEL="${MACHINE_LABEL:-linux-c7a-xlarge-zen4}"
BENCH_LANE="${BENCH_LANE:-cpu}"
BENCH_SEED="${BENCH_SEED:-rarpar-benchmark-plan-v1}"
BENCH_PAR2_PLACEMENT="${BENCH_PAR2_PLACEMENT:-canonical}"
BENCH_WARMUPS="${BENCH_WARMUPS:-1}"
BENCH_REPEATS="${BENCH_REPEATS:-5}"

# ── FULL-CORPUS phase: generate the 31-case corpus on the box, then run it ───
#
# The perf corpus ships pre-generated; the FULL corpus (config/corpus.json, 31
# cases — the rar-* evidence family) is generated here. Precedent: the Windows
# evidence was produced the same way, and digest-keying keeps runs comparable.
#
# Writer images actually required, enumerated from config/corpus.json rather
# than assumed — `[.cases[].writer] | group_by(.)` gives:
#     rarlab-3.93  x5     rarlab-4.20  x8     rarlab-5.00 x10
#     rarlab-6.24  x1     rarlab-7.23  x7                      (= 31)
# plus the PAR2 generator, because 5 cases carry `par2: true`
# (ToolchainIDs, internal/bench/toolchain.go:118-125, adds the par2 generator
# for exactly those). So all five writers AND par2 are genuinely needed — and
# `toolchains build` has no subsetting flag anyway (toolchain.go:67-91 loops
# every writer then par2; main.go:83-84 passes no filter). Building all six is
# therefore correct here, not lazy.
#
# TARBALL INTEGRITY IS THE HARNESS'S OWN — no script-side check is added:
#   * ToolchainLock.Validate (toolchain.go:40,52) rejects any writer or par2
#     entry whose sha256 is not 64 hex chars, or whose platform is not
#     linux/amd64;
#   * BuildToolchains passes the pin in as a build-arg (toolchain.go:75,86);
#   * the Dockerfiles verify the DOWNLOAD against it —
#       docker/rarlab/Dockerfile:12  echo "$RAR_SHA256  /tmp/rar.tar.gz"  | sha256sum -c -
#       docker/par2/Dockerfile:12    echo "$PAR2_SHA256 /tmp/par2.tar.gz" | sha256sum -c -
#     under `set -eux`, so a mismatch fails the build;
#   * verifyDockerfiles (toolchain.go:94-106) additionally pins the base image
#     by digest.
#
# RUN-TIME EXTERNAL DEPENDENCY: the image builds curl from www.rarlab.com (the
# five rar tarballs) and github.com (par2cmdline-turbo v1.4.0). This phase is
# therefore the one part of the run that needs the public internet. If either
# host is unreachable, or a pin no longer matches what is served, this phase
# fails — and per the containment rule that is contained: the perf suite and
# the criterion phases still run, and whatever was generated stays on disk.
FULL_CORPUS_RC=0
full_corpus_suite() {
  local harness candidate reference_rar reference_par2 rc=0
  local outdir="$RESULTS_DIR/rarpar-bench-full"
  mkdir -p "$outdir"
  local log_file="$outdir/harness.log"; : > "$log_file"

  harness="$(resolve_bin rarpar-bench-harness)"         || { FULL_CORPUS_RC=1; return 1; }
  candidate="$(resolve_bin rarpar-cli)"                 || { FULL_CORPUS_RC=1; return 1; }
  reference_rar="$(resolve_bin oracle-unrar-723)"       || { FULL_CORPUS_RC=1; return 1; }
  reference_par2="$(resolve_bin oracle-par2-turbo-140)" || { FULL_CORPUS_RC=1; return 1; }

  _hf() {   # harness subcommand, tee'd into the full-corpus log
    local what="$1"; shift
    log "  harness $what"
    set +e
    ( cd "$RARPAR_BENCH_DIR" && "$harness" "$@" ) 2>&1 | tee -a "$log_file"
    local r=${PIPESTATUS[0]}
    set -e
    [ "$r" -eq 0 ] || warn "  harness $what exited $r"
    return "$r"
  }

  # (a) toolchain images. Docker is already on the box from the corpus pull.
  local writers
  writers="$(jq -r '[.cases[].writer]|unique|join(", ")' "$RARPAR_BENCH_DIR/config/corpus.json" 2>/dev/null || echo '?')"
  local par2_cases
  par2_cases="$(jq -r '[.cases[]|select(.par2)]|length' "$RARPAR_BENCH_DIR/config/corpus.json" 2>/dev/null || echo '?')"
  log "full corpus needs writers: $writers  (+ par2 generator; $par2_cases case(s) set par2:true)"
  {
    echo "writers_required: $writers"
    echo "par2_cases: $par2_cases"
  } > "$outdir/toolchains-required.txt"

  if _hf "toolchains validate" toolchains validate; then
    log "  toolchain lock valid (sha256 pins well-formed, base digest-pinned)"
  else
    rc=1
  fi
  if [ "$rc" -eq 0 ]; then
    log "  building toolchain images — downloads from www.rarlab.com + github.com"
    _hf "toolchains build" toolchains build || rc=1
    {
      echo "toolchains_build_rc: $rc"
      echo "note: sha256 of each tarball is verified inside the image build"
      echo "      (docker/rarlab/Dockerfile:12, docker/par2/Dockerfile:12)"
    } > "$outdir/toolchains-build.txt"
  fi
  [ "$rc" -eq 0 ] || { gate_fail "toolchain image build FAILED — full-corpus generation cannot proceed (network to rarlab.com/github.com, or a pin no longer matches). Remaining phases still run."; FULL_CORPUS_RC=1; return 1; }

  # (b) generate the full corpus INTO the shipped cache, alongside the perf
  #     digest. `corpus generate --out DIR` (main.go:109-121); the digest is
  #     chosen by the harness, so discover it afterwards rather than guessing.
  local gen_root="$RARPAR_BENCH_DIR/.cache/corpora/full-$STAMP"
  _hf "corpus generate" corpus generate --out "$gen_root" || rc=1
  [ "$rc" -eq 0 ] || { gate_fail "corpus generate FAILED — remaining phases still run"; FULL_CORPUS_RC=1; return 1; }

  local corpus_root digest cases
  corpus_root="$(find "$gen_root" -mindepth 0 -maxdepth 2 -type f -name corpus.json 2>/dev/null | head -1 || true)"
  corpus_root="${corpus_root%/corpus.json}"
  if [ -z "$corpus_root" ] || [ ! -f "$corpus_root/corpus.json" ]; then
    gate_fail "corpus generate produced no corpus.json under $gen_root"
    FULL_CORPUS_RC=1; return 1
  fi
  digest="$(jq -r '.digest // "unknown"' "$corpus_root/corpus.json" 2>/dev/null || echo unknown)"
  cases="$(jq -r '.case_count // 0' "$corpus_root/corpus.json" 2>/dev/null || echo 0)"
  log "generated full corpus: digest ${digest:0:16}… , $cases case(s)"

  _hf "corpus verify" corpus verify --root "$corpus_root" || rc=1
  [ "$rc" -eq 0 ] || { gate_fail "generated corpus failed verification"; FULL_CORPUS_RC=1; return 1; }

  # (c) plan -> run -> report -> render, same binaries and naming convention.
  local rev8 evidence_dir plan_out
  rev8="$(revision_field "$RARPAR_DIR" rev)"; rev8="${rev8:0:8}"
  evidence_dir="$RESULTS_DIR/rarpar-bench-full/evidence/$MACHINE_LABEL/rar-${rev8}-c7a"
  plan_out="$outdir/plan.json"
  mkdir -p "$(dirname "$evidence_dir")"
  rm -rf "$evidence_dir"

  _hf "plan create" plan create --corpus "$corpus_root" --out "$plan_out" \
    --seed "$BENCH_SEED" --lane "$BENCH_LANE" \
    --par2-placement "$BENCH_PAR2_PLACEMENT" \
    --warmups "$BENCH_WARMUPS" --repeats "$BENCH_REPEATS" || rc=1
  if [ "$rc" -eq 0 ]; then
    _hf "run" run --corpus "$corpus_root" --plan "$plan_out" \
      --candidate "$candidate" --candidate-label rarpar \
      --reference-rar "$reference_rar" --reference-par2 "$reference_par2" \
      --reference-label reference \
      --out "$evidence_dir" --machine "$MACHINE_LABEL" || rc=1
  fi
  if [ "$rc" -eq 0 ] && [ -f "$evidence_dir/raw.json" ]; then
    _hf "report" report --input "$evidence_dir/raw.json" --out "$evidence_dir/report.json" || rc=1
    [ "$rc" -eq 0 ] && [ -f "$evidence_dir/report.json" ] && \
      { _hf "render" render --input "$evidence_dir/report.json" --out "$evidence_dir/charts" || rc=1; }
  fi
  cp -p "$plan_out" "$evidence_dir/plan.json" 2>/dev/null || true

  {
    echo "suite: full-corpus (config/corpus.json)"
    echo "corpus_root: $corpus_root"
    echo "corpus_digest: $digest"
    echo "corpus_case_count: $cases"
    echo "writers_required: $writers"
    echo "par2_cases: $par2_cases"
    echo "machine_label: $MACHINE_LABEL"
    echo "lane: $BENCH_LANE   seed: $BENCH_SEED   placement: $BENCH_PAR2_PLACEMENT"
    echo "warmups: $BENCH_WARMUPS   repeats: $BENCH_REPEATS"
    echo "candidate: $candidate"
    echo "reference_rar: $reference_rar"
    echo "reference_par2: $reference_par2"
    echo "evidence_dir: $evidence_dir"
  } > "$outdir/run-parameters.txt"

  FULL_CORPUS_RC=$rc
  [ "$rc" -eq 0 ] || gate_fail "full-corpus suite FAILED (see $log_file) — partials kept, remaining phases still run"
  log "full-corpus suite exit: $rc  (evidence: $evidence_dir)"
  return "$rc"
}

RARPAR_BENCH_RC=0
rarpar_bench_suite() {
  local harness corpus_root plan_out evidence_root evidence_dir rev8 log_file rc

  harness="$(resolve_bin rarpar-bench-harness)" || { RARPAR_BENCH_RC=1; return 0; }
  local candidate reference_rar reference_par2
  candidate="$(resolve_bin rarpar-cli)"             || { RARPAR_BENCH_RC=1; return 1; }
  reference_rar="$(resolve_bin oracle-unrar-723)"   || { RARPAR_BENCH_RC=1; return 1; }
  # BOTH references or neither — run.go:59-64 hard errors otherwise. Never
  # conditional: the par2 arm is timed (run.go:248-258) and reported as
  # "par2cmdline-turbo" (report.go:83-88).
  reference_par2="$(resolve_bin oracle-par2-turbo-140)" || { RARPAR_BENCH_RC=1; return 1; }

  # Corpus root: .cache/corpora/<digest>/corpus (verified layout — that
  # directory holds corpus.json plus one directory per case).
  corpus_root="$(find "$RARPAR_BENCH_DIR/.cache/corpora" -mindepth 2 -maxdepth 2 -type d -name corpus 2>/dev/null | sort | head -1 || true)"
  if [ -z "$corpus_root" ] || [ ! -f "$corpus_root/corpus.json" ]; then
    gate_fail "no cached corpus under $RARPAR_BENCH_DIR/.cache/corpora — bootstrap's corpus gate should have caught this"
    RARPAR_BENCH_RC=1; return 0
  fi

  local digest cases
  digest="$(jq -r '.digest // "unknown"' "$corpus_root/corpus.json" 2>/dev/null || echo unknown)"
  cases="$(jq -r '.case_count // 0' "$corpus_root/corpus.json" 2>/dev/null || echo 0)"
  log "corpus root : $corpus_root"
  log "corpus      : digest ${digest:0:16}… , $cases case(s)"

  # Evidence naming follows the existing convention observed in the repo's
  # .cache/evidence: <machine-label>/<family>-<rev8>-<variant>
  #   e.g. windows-ryzen5-3600/rar-6cc9c523-current-v2
  #        linux-i5-1240p/rar5-perf-6cc9c523
  rev8="$(revision_field "$RARPAR_DIR" rev)"; rev8="${rev8:0:8}"
  evidence_root="$RESULTS_DIR/rarpar-bench/evidence/$MACHINE_LABEL"
  # rar5-perf- prefix per the existing convention (linux-i5-1240p/rar5-perf-*)
  # AND to keep this label DISTINCT from the full-corpus suite's rar-<rev8>-c7a:
  # identical labels caused a silent merge/overwrite during evidence
  # relocation on 2026-08-12 that destroyed the full-corpus raw evidence.
  evidence_dir="$evidence_root/rar5-perf-${rev8}-c7a"
  mkdir -p "$evidence_root"
  plan_out="$RESULTS_DIR/rarpar-bench/plan.json"
  log_file="$RESULTS_DIR/rarpar-bench/harness.log"
  mkdir -p "$RESULTS_DIR/rarpar-bench"
  : > "$log_file"

  # `run` wants a FRESH --out directory, so it is NOT pre-created here.
  rm -rf "$evidence_dir"

  _h() {   # run a harness subcommand, tee'd, returning its status
    local what="$1"; shift
    log "  harness $what"
    set +e
    ( cd "$RARPAR_BENCH_DIR" && "$harness" "$@" ) 2>&1 | tee -a "$log_file"
    local r=${PIPESTATUS[0]}
    set -e
    [ "$r" -eq 0 ] || warn "  harness $what exited $r"
    return "$r"
  }

  rc=0
  _h "corpus verify" corpus verify --root "$corpus_root" || rc=1
  if [ "$rc" -eq 0 ]; then
    _h "plan create" plan create \
      --corpus "$corpus_root" --out "$plan_out" \
      --seed "$BENCH_SEED" --lane "$BENCH_LANE" \
      --par2-placement "$BENCH_PAR2_PLACEMENT" \
      --warmups "$BENCH_WARMUPS" --repeats "$BENCH_REPEATS" || rc=1
  fi
  if [ "$rc" -eq 0 ]; then
    _h "run" run --corpus "$corpus_root" --plan "$plan_out" \
      --candidate "$candidate" --candidate-label rarpar \
      --reference-rar "$reference_rar" --reference-par2 "$reference_par2" \
      --reference-label reference \
      --out "$evidence_dir" --machine "$MACHINE_LABEL" || rc=1
  fi
  if [ "$rc" -eq 0 ] && [ -f "$evidence_dir/raw.json" ]; then
    _h "report" report --input "$evidence_dir/raw.json" --out "$evidence_dir/report.json" || rc=1
    if [ "$rc" -eq 0 ] && [ -f "$evidence_dir/report.json" ]; then
      _h "render" render --input "$evidence_dir/report.json" --out "$evidence_dir/charts" || rc=1
    fi
  elif [ "$rc" -eq 0 ]; then
    gate_fail "harness run produced no raw.json in $evidence_dir"
    rc=1
  fi

  # The evidence directory IS the SVG source data, in the same shape as the
  # repo's .cache/evidence entries (plan.json + report.json + charts).
  cp -p "$plan_out" "$evidence_dir/plan.json" 2>/dev/null || true
  {
    echo "corpus_root: $corpus_root"
    echo "corpus_digest: $digest"
    echo "corpus_case_count: $cases"
    echo "machine_label: $MACHINE_LABEL"
    echo "lane: $BENCH_LANE   seed: $BENCH_SEED   placement: $BENCH_PAR2_PLACEMENT"
    echo "warmups: $BENCH_WARMUPS   repeats: $BENCH_REPEATS"
    echo "candidate: $candidate"
    echo "reference_rar: $reference_rar"
    echo "reference_par2: $reference_par2"
    echo "evidence_dir: $evidence_dir"
  } > "$RESULTS_DIR/rarpar-bench/run-parameters.txt"

  RARPAR_BENCH_RC=$rc
  [ "$rc" -eq 0 ] || gate_fail "rarpar-bench perf suite FAILED (see $log_file) — partials kept, remaining phases still run"
  log "rarpar-bench perf suite exit: $rc  (evidence: $evidence_dir)"
  return "$rc"
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
      weaver) root="$WEAVER_CRIT_DIR/criterion" ;;
      rarpar) root="$RARPAR_CRIT_DIR/criterion" ;;
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
    --arg rustc          "$(buildinfo_field rustc_verbose "$(buildinfo_field rustc)")" \
    --argjson toolchains "$(jq -c '.toolchains // null' "$BUILDINFO_JSON" 2>/dev/null || echo null)" \
    --arg builder        "$(buildinfo_field builder)" \
    --arg build_glibc    "$(buildinfo_field glibc)" \
    --arg host_glibc     "$(ldd --version 2>/dev/null | head -1 | awk '{print $NF}' || true)" \
    --arg built_at_utc   "$(buildinfo_field built_at_utc)" \
    --arg build_rustflags "$(buildinfo_field rustflags '')" \
    --arg target         "$TARGET" \
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
    --argjson phase_timings "$(awk -F'\t' 'BEGIN{printf "{"} {printf "%s\"%s\":%s", (n++?",":""), $1, $3} END{printf "}"}' "$PHASE_TSV" 2>/dev/null || echo '{}')" \
    --arg dec_id "${dec_id:-}" --arg dec_name "$(rykern_name "${dec_id:-}")" \
    --arg crc_id "${crc_id:-}" --arg crc_name "$(rykern_name "${crc_id:-}")" \
    '{
      timestamp_utc: $timestamp_utc,
      instance_type: $instance_type,
      cpu: { model: $cpu_model, cores: ($cpu_cores | tonumber? // 0), flags: $cpu_flags },
      kernel: $kernel,
      rustc: $rustc,
      target: $target,
      corpus_image: $corpus_image,
      provenance: {
        binaries: "prebuilt",
        note: "no Rust toolchain on the bench host; every test and bench executable was built on the builder below and shipped in the corpus image at /corpus/prebuilt",
        builder: $builder,
        built_at_utc: $built_at_utc,
        toolchains: $toolchains,
        rustflags: $build_rustflags,
        rustflags_note: "empty on purpose: plain x86_64-unknown-linux-gnu with no target-cpu pinning, so every kernel tier is compiled in and chosen by runtime dispatch",
        glibc: { build: $build_glibc, host: $host_glibc }
      },
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
      phase_timings_seconds: $phase_timings,
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
      weaver) root="$WEAVER_CRIT_DIR/criterion" ;;
      rarpar) root="$RARPAR_CRIT_DIR/criterion" ;;
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

archive_rarpar_bench_evidence() {
  # Both corpus suites. A failed suite still gets whatever it produced archived
  # — partials are the point of the containment rule.
  local suite root out
  for suite in full perf; do
    case "$suite" in
      full) root="$RESULTS_DIR/rarpar-bench-full"; out="$RESULTS_DIR/rarpar-bench-full-evidence.tar.gz" ;;
      perf) root="$RESULTS_DIR/rarpar-bench";      out="$RESULTS_DIR/rarpar-bench-evidence.tar.gz" ;;
      *)    continue ;;
    esac
    if [ -d "$root/evidence" ]; then
      if tar -czf "$out" -C "$root" evidence; then
        log "archived $root/evidence -> $out ($(du -h "$out" | cut -f1 | tr -d ' '))"
      else
        gate_fail "failed to archive the $suite-corpus evidence"
      fi
    else
      warn "no $suite-corpus evidence at $root/evidence (that phase produced nothing)"
    fi
  done
}

preserve_data() {
  log "Preserving raw bench data for later SVG generation (doc §9g)…"
  archive_rarpar_bench_evidence
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
    echo "PREBUILT_DIR         : $PREBUILT_DIR"
    echo "binaries             : prebuilt (builder $(buildinfo_field builder), $(buildinfo_field built_at_utc))"
    echo "build rustflags      : '$(buildinfo_field rustflags '')' (no target-cpu pinning)"
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

    echo "----- phase outcomes (failures are contained, not fatal) -----"
    printf '%s\n' "${PHASE_RESULTS:-  (none)}"
    echo

    echo "----- FULL-corpus suite (HEADLINE, doc §8a) -----"
    echo "  exit: $FULL_CORPUS_RC"
    if [ -f "$RESULTS_DIR/rarpar-bench-full/run-parameters.txt" ]; then
      sed 's/^/  /' "$RESULTS_DIR/rarpar-bench-full/run-parameters.txt"
    else
      echo "  (no run-parameters.txt — suite did not start or failed before generation)"
    fi
    echo

    echo "----- perf-corpus suite (pre-cached, doc §8b) -----"
    echo "  exit: $RARPAR_BENCH_RC"
    if [ -f "$RESULTS_DIR/rarpar-bench/run-parameters.txt" ]; then
      sed 's/^/  /' "$RESULTS_DIR/rarpar-bench/run-parameters.txt"
    else
      echo "  (no run-parameters.txt — suite did not start)"
    fi
    echo

    echo "----- measured phase wall time (seconds) -----"
    if [ -s "$PHASE_TSV" ]; then
      awk -F'\t' '{ printf "  %-22s %6ds\n", $1, $3; total += $3 }
                   END { printf "  %-22s %6ds\n", "TOTAL", total }' "$PHASE_TSV"
    else
      echo "  (none recorded)"
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
    for l in rarpar-bench-full-evidence.tar.gz rarpar-bench-evidence.tar.gz criterion-weaver.tar.gz criterion-rarpar.tar.gz metadata.json summary.json; do
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
       (a) rarpar-bench-full-evidence.tar.gz  HEADLINE 31-case corpus (8a)
       (a2) rarpar-bench-evidence.tar.gz      perf corpus evidence (8b)
       (b) criterion-weaver.tar.gz   full weaver criterion tree
       (c) criterion-rarpar.tar.gz   full rarpar criterion tree
       (d) metadata.json             instance/cpu/kernel/provenance/timings
       (e) summary.json              flat per-lane estimates, both passes
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

  # Prefer the env file the bootstrap wrote.
  if [ -f "$PREBUILT_DIR/weaver-bench.env" ]; then
    # shellcheck disable=SC1091
    . "$PREBUILT_DIR/weaver-bench.env"
  fi
  # Exported so the prebuilt binaries see them; the manifest's needs_env is
  # checked against these in resolve_bin.
  export RAPIDYENC_ROOT WEAVER_RAPIDYENC_LIB CXX
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

  assert_bundle_present
  assert_source_preconditions
  record_revisions

  # Warn (don't skip) if the rapidyenc reference is not wired up.
  [ -d "$RAPIDYENC_ROOT" ]        || warn "RAPIDYENC_ROOT=$RAPIDYENC_ROOT missing — differential tests will SKIP"
  [ -e "$WEAVER_RAPIDYENC_LIB" ]  || warn "WEAVER_RAPIDYENC_LIB=$WEAVER_RAPIDYENC_LIB missing — parity bench will SKIP"
  [ -d "$RARPAR_DIR" ]            || warn "RARPAR_DIR=$RARPAR_DIR missing — the mandatory rarpar phase will ABORT"

  [ "$RESUME" = "1" ] || : > "$PHASE_TSV"
  # Every phase is independently failable and independently resumable
  # (run_phase never propagates a failure). Order: correctness, then the
  # generated FULL corpus, then the pre-cached perf corpus, then the secondary
  # criterion micro-benches.
  run_phase weaver-tests       weaver_tests
  run_phase rarpar-tests       rarpar_tests_only
  run_phase full-corpus-suite  full_corpus_suite
  run_phase perf-corpus-suite  rarpar_bench_suite
  run_phase weaver-criterion   weaver_benches
  run_phase rarpar-criterion   rarpar_criterion
  run_phase preserve-data      preserve_data

  print_summary
  print_teardown_checklist

  if [ "$GATE_FAILURES" -ne 0 ]; then
    die "$GATE_FAILURES gate(s) FAILED — see $RESULTS_DIR/summary.txt and doc §10 triage"
  fi
  log "DONE — all gates green. Results in $RESULTS_DIR"
}

main "$@"
