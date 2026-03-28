#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run Weaver PAR2 verify/repair hot-path profiling on an x86 Linux box.

This script is designed for a server that already has:
  - a prebuilt x86_64 Weaver binary
  - an optional par2cmdline-turbo binary
  - a damaged, repairable PAR2 fixture directory
  - perf and GNU /usr/bin/time installed

It never mutates the original fixture. Repair runs happen on temporary copies.

Usage:
  scripts/run-par2-x86-hotpaths.sh \
    --weaver /path/to/weaver \
    --fixture /path/to/fixture_dir \
    [--seed-par2 /path/to/main.par2] \
    [--turbo /path/to/par2] \
    [--output-dir /path/to/out] \
    [--memory-limit-mb 50] \
    [--threads 16] \
    [--taskset 0-15] \
    [--label skylake-repair] \
    [--perf-freq 999] \
    [--no-verify-perf] \
    [--no-repair-perf-stat] \
    [--no-repair-perf-record] \
    [--keep-workdirs]

Examples:
  scripts/run-par2-x86-hotpaths.sh \
    --weaver ./weaver \
    --fixture ./rar5_heavy_damage \
    --turbo ./par2 \
    --taskset 0-15

  scripts/run-par2-x86-hotpaths.sh \
    --weaver ./weaver \
    --fixture ./large-fixture \
    --memory-limit-mb 0 \
    --no-verify-perf
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

cpu_count() {
  if have_cmd nproc; then
    nproc
  else
    getconf _NPROCESSORS_ONLN
  fi
}

copy_tree() {
  local src="$1"
  local dst="$2"
  mkdir -p "$dst"
  if cp -a --reflink=auto --sparse=always "$src"/. "$dst"/ 2>/dev/null; then
    return
  fi
  cp -a "$src"/. "$dst"/
}

discover_seed_par2() {
  local dir="$1"
  local -a primary=()
  local -a all=()

  while IFS= read -r path; do
    all+=("$path")
    case "$(basename "$path")" in
      *.vol*+*.par2) ;;
      *) primary+=("$path") ;;
    esac
  done < <(find "$dir" -maxdepth 1 -type f -name '*.par2' | sort)

  if [ "${#primary[@]}" -eq 1 ]; then
    printf '%s\n' "${primary[0]}"
    return
  fi
  if [ "${#all[@]}" -eq 1 ]; then
    printf '%s\n' "${all[0]}"
    return
  fi

  die "could not uniquely determine the seed PAR2 file in $dir; pass --seed-par2"
}

time_field() {
  local file="$1"
  local key="$2"
  awk -F': ' -v key="$key" '$1 == key { print $2 }' "$file"
}

append_time_summary() {
  local label="$1"
  local time_file="$2"
  {
    echo "$label"
    echo "  wall: $(time_field "$time_file" 'Elapsed (wall clock) time (h:mm:ss or m:ss)')"
    echo "  user: $(time_field "$time_file" 'User time (seconds)')"
    echo "  sys:  $(time_field "$time_file" 'System time (seconds)')"
    echo "  rss:  $(time_field "$time_file" 'Maximum resident set size (kbytes)') kB"
  } >>"$SUMMARY_FILE"
}

append_perf_symbol_summary() {
  local label="$1"
  local report_file="$2"
  shift 2

  {
    echo "$label"
    local found=0
    local symbol
    for symbol in "$@"; do
      if grep -q "$symbol" "$report_file"; then
        echo "  found: $symbol"
        found=1
      fi
    done
    if [ "$found" -eq 0 ]; then
      echo "  found: none of the expected symbols"
    fi
  } >>"$SUMMARY_FILE"
}

run_expected() {
  local expected_status="$1"
  local time_file="$2"
  local log_file="$3"
  shift 3

  local status
  set +e
  /usr/bin/time -v -o "$time_file" "$@" >"$log_file" 2>&1
  status=$?
  set -e

  printf '%s\n' "$status" >"${time_file}.status"
  if [ "$status" -ne "$expected_status" ]; then
    die "command failed with exit $status (expected $expected_status); see $log_file"
  fi
}

run_perf_stat_expected() {
  local expected_status="$1"
  local stat_file="$2"
  local log_file="$3"
  shift 3

  local status
  set +e
  perf stat -d -o "$stat_file" -- "$@" >"$log_file" 2>&1
  status=$?
  set -e

  printf '%s\n' "$status" >"${stat_file}.status"
  if [ "$status" -ne "$expected_status" ]; then
    die "perf stat command failed with exit $status (expected $expected_status); see $log_file"
  fi
}

run_perf_record_expected() {
  local expected_status="$1"
  local perf_data="$2"
  local log_file="$3"
  shift 3

  local status
  set +e
  perf record -F "$PERF_FREQ" -g -o "$perf_data" -- "$@" >"$log_file" 2>&1
  status=$?
  set -e

  printf '%s\n' "$status" >"${perf_data}.status"
  if [ "$status" -ne "$expected_status" ]; then
    die "perf record command failed with exit $status (expected $expected_status); see $log_file"
  fi
}

make_workdir() {
  local prefix="$1"
  local dir
  dir="$(mktemp -d "${OUT_DIR}/${prefix}.XXXXXX")"
  TEMP_DIRS+=("$dir")
  copy_tree "$FIXTURE_DIR" "$dir"
  printf '%s\n' "$dir"
}

build_weaver_cmd() {
  local subcommand="$1"
  local seed_path="$2"
  local -a cmd=(env "RAYON_NUM_THREADS=${THREADS}")

  if [ -n "$TASKSET_CPUSET" ]; then
    cmd+=(taskset -c "$TASKSET_CPUSET")
  fi

  cmd+=("$WEAVER_BIN" par2 "$subcommand")

  if [ "$subcommand" = "repair" ]; then
    cmd+=(--working-dir "$(dirname "$seed_path")" --memory-limit-mb "$MEMORY_LIMIT_MB")
  fi

  cmd+=("$seed_path")
  printf '%s\0' "${cmd[@]}"
}

build_turbo_cmd() {
  local subcommand="$1"
  local seed_path="$2"
  local -a cmd=()

  if [ -n "$TASKSET_CPUSET" ]; then
    cmd+=(taskset -c "$TASKSET_CPUSET")
  fi

  cmd+=("$TURBO_BIN" "$subcommand")

  if [ "$subcommand" = "repair" ]; then
    cmd+=("-t${THREADS}")
  fi

  cmd+=("$seed_path")
  printf '%s\0' "${cmd[@]}"
}

write_metadata() {
  {
    echo "label=${LABEL}"
    echo "started_at=$(date -Iseconds)"
    echo "fixture_dir=${FIXTURE_DIR}"
    echo "seed_par2=${SEED_PAR2}"
    echo "weaver_bin=${WEAVER_BIN}"
    echo "turbo_bin=${TURBO_BIN:-}"
    echo "threads=${THREADS}"
    echo "taskset=${TASKSET_CPUSET:-}"
    echo "memory_limit_mb=${MEMORY_LIMIT_MB}"
    echo "perf_freq=${PERF_FREQ}"
    echo
    echo "uname:"
    uname -a
    echo
    if have_cmd lscpu; then
      echo "lscpu:"
      lscpu
      echo
    fi
    echo "perf:"
    perf --version
    echo
    echo "weaver file:"
    file "$WEAVER_BIN"
    if have_cmd sha256sum; then
      sha256sum "$WEAVER_BIN"
    elif have_cmd shasum; then
      shasum -a 256 "$WEAVER_BIN"
    fi
    echo
    if [ -n "$TURBO_BIN" ]; then
      echo "turbo file:"
      file "$TURBO_BIN"
      if have_cmd sha256sum; then
        sha256sum "$TURBO_BIN"
      elif have_cmd shasum; then
        shasum -a 256 "$TURBO_BIN"
      fi
      echo
    fi
  } >"$META_FILE"
}

WEAVER_BIN=""
TURBO_BIN=""
FIXTURE_DIR=""
SEED_PAR2=""
OUT_DIR=""
MEMORY_LIMIT_MB="50"
THREADS="$(cpu_count)"
TASKSET_CPUSET=""
LABEL="x86-par2"
PERF_FREQ="999"
RUN_VERIFY_PERF=1
RUN_REPAIR_PERF_STAT=1
RUN_REPAIR_PERF_RECORD=1
KEEP_WORKDIRS=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --weaver)
      WEAVER_BIN="$2"
      shift 2
      ;;
    --turbo)
      TURBO_BIN="$2"
      shift 2
      ;;
    --fixture)
      FIXTURE_DIR="$2"
      shift 2
      ;;
    --seed-par2)
      SEED_PAR2="$2"
      shift 2
      ;;
    --output-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --memory-limit-mb)
      MEMORY_LIMIT_MB="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    --taskset)
      TASKSET_CPUSET="$2"
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --perf-freq)
      PERF_FREQ="$2"
      shift 2
      ;;
    --no-verify-perf)
      RUN_VERIFY_PERF=0
      shift
      ;;
    --no-repair-perf-stat)
      RUN_REPAIR_PERF_STAT=0
      shift
      ;;
    --no-repair-perf-record)
      RUN_REPAIR_PERF_RECORD=0
      shift
      ;;
    --keep-workdirs)
      KEEP_WORKDIRS=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[ -n "$WEAVER_BIN" ] || die "--weaver is required"
[ -n "$FIXTURE_DIR" ] || die "--fixture is required"
[ -x "$WEAVER_BIN" ] || die "weaver binary is not executable: $WEAVER_BIN"
[ -d "$FIXTURE_DIR" ] || die "fixture directory not found: $FIXTURE_DIR"

if [ -n "$TURBO_BIN" ] && [ ! -x "$TURBO_BIN" ]; then
  die "turbo binary is not executable: $TURBO_BIN"
fi

if [ -z "$SEED_PAR2" ]; then
  SEED_PAR2="$(discover_seed_par2 "$FIXTURE_DIR")"
fi
[ -f "$SEED_PAR2" ] || die "seed PAR2 file not found: $SEED_PAR2"

if ! have_cmd perf; then
  die "perf is required"
fi
if [ ! -x /usr/bin/time ]; then
  die "/usr/bin/time is required"
fi
if [ -n "$TASKSET_CPUSET" ] && ! have_cmd taskset; then
  die "taskset requested but not installed"
fi

case "$(uname -m)" in
  x86_64|amd64) ;;
  *)
    die "this script is intended for x86_64 hosts"
    ;;
esac

if [ -z "$OUT_DIR" ]; then
  STAMP="$(date +%Y%m%d-%H%M%S)"
  OUT_DIR="$(pwd)/tmp/par2-x86/${STAMP}-${LABEL}"
fi
mkdir -p "$OUT_DIR"

META_FILE="${OUT_DIR}/meta.txt"
SUMMARY_FILE="${OUT_DIR}/summary.txt"
TEMP_DIRS=()

cleanup() {
  if [ "$KEEP_WORKDIRS" -eq 1 ]; then
    return
  fi
  local dir
  for dir in "${TEMP_DIRS[@]}"; do
    rm -rf "$dir"
  done
}
trap cleanup EXIT

write_metadata
: >"$SUMMARY_FILE"

SEED_BASENAME="$(basename "$SEED_PAR2")"
FIXTURE_SEED="${FIXTURE_DIR}/${SEED_BASENAME}"
[ -f "$FIXTURE_SEED" ] || die "fixture copy of seed PAR2 file not found: $FIXTURE_SEED"

log "Output dir: $OUT_DIR"
log "Fixture: $FIXTURE_DIR"
log "Seed PAR2: $FIXTURE_SEED"
log "Weaver binary: $WEAVER_BIN"
if [ -n "$TURBO_BIN" ]; then
  log "Turbo binary: $TURBO_BIN"
fi

log "Running Weaver verify on the original damaged fixture"
mapfile -d '' -t WEAVER_VERIFY_CMD < <(build_weaver_cmd verify "$FIXTURE_SEED")
run_expected 1 "${OUT_DIR}/weaver-verify.time" "${OUT_DIR}/weaver-verify.log" "${WEAVER_VERIFY_CMD[@]}"
append_time_summary "weaver verify" "${OUT_DIR}/weaver-verify.time"

if [ "$RUN_VERIFY_PERF" -eq 1 ]; then
  log "Recording Weaver verify hot paths with perf"
  run_perf_record_expected 1 \
    "${OUT_DIR}/weaver-verify.perf.data" \
    "${OUT_DIR}/weaver-verify.perf.log" \
    "${WEAVER_VERIFY_CMD[@]}"
  perf report --stdio -i "${OUT_DIR}/weaver-verify.perf.data" >"${OUT_DIR}/weaver-verify.perf.report.txt"
  append_perf_symbol_summary \
    "weaver verify perf symbols" \
    "${OUT_DIR}/weaver-verify.perf.report.txt" \
    md5_multi_x86 md5_asm
fi

WEAVER_REPAIR_DIR="$(make_workdir weaver-repair)"
WEAVER_REPAIR_SEED="${WEAVER_REPAIR_DIR}/${SEED_BASENAME}"
mapfile -d '' -t WEAVER_REPAIR_CMD < <(build_weaver_cmd repair "$WEAVER_REPAIR_SEED")

log "Running Weaver repair on a fresh copy"
run_expected 0 "${OUT_DIR}/weaver-repair.time" "${OUT_DIR}/weaver-repair.log" "${WEAVER_REPAIR_CMD[@]}"
append_time_summary "weaver repair" "${OUT_DIR}/weaver-repair.time"

mapfile -d '' -t WEAVER_REPAIRED_VERIFY_CMD < <(build_weaver_cmd verify "$WEAVER_REPAIR_SEED")
run_expected 0 "${OUT_DIR}/weaver-post-repair-verify.time" "${OUT_DIR}/weaver-post-repair-verify.log" "${WEAVER_REPAIRED_VERIFY_CMD[@]}"
append_time_summary "weaver post-repair verify" "${OUT_DIR}/weaver-post-repair-verify.time"

if [ "$RUN_REPAIR_PERF_STAT" -eq 1 ]; then
  WEAVER_REPAIR_STAT_DIR="$(make_workdir weaver-repair-perf-stat)"
  WEAVER_REPAIR_STAT_SEED="${WEAVER_REPAIR_STAT_DIR}/${SEED_BASENAME}"
  mapfile -d '' -t WEAVER_REPAIR_STAT_CMD < <(build_weaver_cmd repair "$WEAVER_REPAIR_STAT_SEED")
  log "Collecting Weaver repair perf stat counters"
  run_perf_stat_expected 0 \
    "${OUT_DIR}/weaver-repair.perf.stat.txt" \
    "${OUT_DIR}/weaver-repair.perf.stat.log" \
    "${WEAVER_REPAIR_STAT_CMD[@]}"
fi

if [ "$RUN_REPAIR_PERF_RECORD" -eq 1 ]; then
  WEAVER_REPAIR_RECORD_DIR="$(make_workdir weaver-repair-perf-record)"
  WEAVER_REPAIR_RECORD_SEED="${WEAVER_REPAIR_RECORD_DIR}/${SEED_BASENAME}"
  mapfile -d '' -t WEAVER_REPAIR_RECORD_CMD < <(build_weaver_cmd repair "$WEAVER_REPAIR_RECORD_SEED")
  log "Recording Weaver repair hot paths with perf"
  run_perf_record_expected 0 \
    "${OUT_DIR}/weaver-repair.perf.data" \
    "${OUT_DIR}/weaver-repair.perf.log" \
    "${WEAVER_REPAIR_RECORD_CMD[@]}"
  perf report --stdio -i "${OUT_DIR}/weaver-repair.perf.data" >"${OUT_DIR}/weaver-repair.perf.report.txt"
  append_perf_symbol_summary \
    "weaver repair perf symbols" \
    "${OUT_DIR}/weaver-repair.perf.report.txt" \
    mul_acc_multi_region_gfni_avx2 \
    mul_acc_region_gfni_avx512 \
    mul_acc_region_avx512 \
    mul_acc_region_avx2 \
    mul_acc_region_ssse3
fi

if [ -n "$TURBO_BIN" ]; then
  log "Running turbo verify on the original damaged fixture"
  mapfile -d '' -t TURBO_VERIFY_CMD < <(build_turbo_cmd verify "$FIXTURE_SEED")
  run_expected 1 "${OUT_DIR}/turbo-verify.time" "${OUT_DIR}/turbo-verify.log" "${TURBO_VERIFY_CMD[@]}"
  append_time_summary "turbo verify" "${OUT_DIR}/turbo-verify.time"

  TURBO_REPAIR_DIR="$(make_workdir turbo-repair)"
  TURBO_REPAIR_SEED="${TURBO_REPAIR_DIR}/${SEED_BASENAME}"
  mapfile -d '' -t TURBO_REPAIR_CMD < <(build_turbo_cmd repair "$TURBO_REPAIR_SEED")
  log "Running turbo repair on a fresh copy"
  run_expected 0 "${OUT_DIR}/turbo-repair.time" "${OUT_DIR}/turbo-repair.log" "${TURBO_REPAIR_CMD[@]}"
  append_time_summary "turbo repair" "${OUT_DIR}/turbo-repair.time"
fi

{
  echo
  echo "outputs"
  echo "  meta:    ${META_FILE}"
  echo "  summary: ${SUMMARY_FILE}"
  if [ "$RUN_VERIFY_PERF" -eq 1 ]; then
    echo "  verify perf report: ${OUT_DIR}/weaver-verify.perf.report.txt"
  fi
  if [ "$RUN_REPAIR_PERF_STAT" -eq 1 ]; then
    echo "  repair perf stat:   ${OUT_DIR}/weaver-repair.perf.stat.txt"
  fi
  if [ "$RUN_REPAIR_PERF_RECORD" -eq 1 ]; then
    echo "  repair perf report: ${OUT_DIR}/weaver-repair.perf.report.txt"
  fi
} >>"$SUMMARY_FILE"

log "Done."
cat "$SUMMARY_FILE"
