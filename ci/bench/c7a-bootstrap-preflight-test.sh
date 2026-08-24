#!/usr/bin/env bash
# Regression coverage for the bootstrap's non-mutating preflight boundary.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
bootstrap="$root/ci/bench/c7a-bootstrap.sh"
tmp="$(mktemp -d "${TMPDIR%/}/c7a-bootstrap-test.XXXXXX")"
stub_dir="$tmp/stubs"
log="$tmp/invocations"
post_arm_stopper="$tmp/post-arm-stopper.sh"
valid_image="registry.example.invalid/bench/corpus@sha256:0000000000000000000000000000000000000000000000000000000000000000"

cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
mkdir -p "$stub_dir" "$tmp/home"

for command in apt apt-get aws curl dnf docker ldd mkdir shutdown yum; do
  printf '%s\n' '#!/usr/bin/env bash' \
    'printf "%s %s\n" "$(basename "$0")" "$*" >> "${C7A_STUB_LOG:?}"' \
    'exit 0' > "$stub_dir/$command"
  chmod +x "$stub_dir/$command"
done
printf '%s\n' '#!/usr/bin/env bash' \
  'printf "sudo %s\n" "$*" >> "${C7A_STUB_LOG:?}"' \
  'exit 0' > "$stub_dir/sudo"
chmod +x "$stub_dir/sudo"

# Non-interactive bash evaluates BASH_ENV before the executable. This stops at
# the first main() log immediately after arm_deadman, before CPU/package/corpus
# work. It leaves the bootstrap itself unmodified and gives the test a precise
# post-arm boundary on every host OS.
printf '%s\n' \
  'printf() {' \
  '  case "${2:-}" in CORPUS_IMAGE=*) exit "${C7A_POST_ARM_STOP_STATUS:?}" ;; esac' \
  '  builtin printf "$@"' \
  '}' > "$post_arm_stopper"

run_bootstrap() {
  local destination="$1"
  shift
  local unset_corpus_image=0
  if [ "${1:-}" = "--without-corpus-image" ]; then
    unset_corpus_image=1
    shift
  fi
  if [ "$unset_corpus_image" -eq 1 ]; then
    env -u CORPUS_IMAGE \
      PATH="$stub_dir:$PATH" \
      C7A_STUB_LOG="$log" \
      HOME="$destination/home" \
      CORPUS_DEST="$destination/corpus" \
      "$@" \
      bash "$bootstrap"
    return
  fi
  env \
    PATH="$stub_dir:$PATH" \
    C7A_STUB_LOG="$log" \
    HOME="$destination/home" \
    CORPUS_DEST="$destination/corpus" \
    "$@" \
    bash "$bootstrap"
}

expect_rejected_without_host_action() {
  local label="$1"
  shift
  local unsafe_prebuilt=0
  if [ "${1:-}" = "--unsafe-prebuilt" ]; then
    unsafe_prebuilt=1
    shift
  fi
  local case_root="$tmp/rejected-$RANDOM"
  local corpus_dest="$case_root/corpus"
  local env_file="$corpus_dest/prebuilt/weaver-bench.env"
  local escaped_env_file="$case_root/weaver-bench.env"
  mkdir "$case_root"
  chmod 500 "$case_root"
  : > "$log"
  if [ "$unsafe_prebuilt" -eq 1 ]; then
    set -- "$@" "PREBUILT_DIR=$case_root"
  fi
  if run_bootstrap "$case_root" "$@"; then
    printf 'expected %s bootstrap to fail\n' "$label" >&2
    exit 1
  fi
  if [ -s "$log" ]; then
    printf '%s bootstrap invoked a host-side command:\n' "$label" >&2
    cat "$log" >&2
    exit 1
  fi
  if [ -e "$corpus_dest" ] || [ -e "$env_file" ] || [ -e "$escaped_env_file" ]; then
    printf '%s bootstrap wrote under its unwritable destination root\n' "$label" >&2
    exit 1
  fi
}

expect_rejected_without_host_action "missing CORPUS_IMAGE" --without-corpus-image
expect_rejected_without_host_action "floating CORPUS_IMAGE" \
  'CORPUS_IMAGE=registry.example.invalid/bench/corpus:latest'
expect_rejected_without_host_action "invalid DEADMAN_MINUTES" \
  "CORPUS_IMAGE=$valid_image" 'DEADMAN_MINUTES=-1'
expect_rejected_without_host_action "unsafe PREBUILT_DIR" \
  --unsafe-prebuilt "CORPUS_IMAGE=$valid_image"

expect_main_stops_after_arming() {
  local case_root="$tmp/valid-arm-$RANDOM"
  local corpus_dest="$case_root/corpus"
  local status
  mkdir "$case_root"
  chmod 500 "$case_root"
  : > "$log"
  if run_bootstrap "$case_root" \
    "CORPUS_IMAGE=$valid_image" \
    'DEADMAN_MINUTES=1' \
    "BASH_ENV=$post_arm_stopper" \
    'C7A_POST_ARM_STOP_STATUS=97'; then
    printf 'valid bootstrap unexpectedly continued after the arm stopper\n' >&2
    exit 1
  else
    status=$?
  fi
  if [ "$status" -ne 97 ]; then
    printf 'valid bootstrap stopped with %s, not the post-arm stopper:\n' "$status" >&2
    cat "$log" >&2
    exit 1
  fi
  if [ "$(grep -c '^sudo shutdown -c$' "$log")" -ne 1 ] || \
     [ "$(grep -c '^sudo shutdown -h +1 ' "$log")" -ne 1 ] || \
     [ "$(wc -l < "$log")" -ne 2 ]; then
    printf 'valid bootstrap did not reach exactly the safe arm boundary:\n' >&2
    cat "$log" >&2
    exit 1
  fi
  if [ -e "$corpus_dest" ] || [ -e "$corpus_dest/prebuilt/weaver-bench.env" ]; then
    printf 'valid bootstrap wrote before the post-arm stopper\n' >&2
    exit 1
  fi
}

expect_main_stops_after_disabled_arm() {
  local case_root="$tmp/valid-zero-$RANDOM"
  local corpus_dest="$case_root/corpus"
  local status
  mkdir "$case_root"
  chmod 500 "$case_root"
  : > "$log"
  if run_bootstrap "$case_root" \
    "CORPUS_IMAGE=$valid_image" \
    'DEADMAN_MINUTES=0' \
    "BASH_ENV=$post_arm_stopper" \
    'C7A_POST_ARM_STOP_STATUS=97'; then
    printf 'zero-deadman bootstrap unexpectedly continued past the safe stopper\n' >&2
    exit 1
  else
    status=$?
  fi
  if [ "$status" -ne 97 ]; then
    printf 'zero-deadman bootstrap stopped with %s, not the post-arm stopper:\n' "$status" >&2
    cat "$log" >&2
    exit 1
  fi
  if [ -s "$log" ]; then
    printf 'DEADMAN_MINUTES=0 invoked a host-side command:\n' >&2
    cat "$log" >&2
    exit 1
  fi
  if [ -e "$corpus_dest" ] || [ -e "$corpus_dest/prebuilt/weaver-bench.env" ]; then
    printf 'DEADMAN_MINUTES=0 wrote before the post-arm stopper\n' >&2
    exit 1
  fi
}

# The invalid cases above run main and fail on any sudo/aws/docker/shutdown log,
# so moving arm_deadman before preflight makes this test fail. The valid cases
# below prove the executable reaches the same boundary in both timer modes.
expect_main_stops_after_arming
expect_main_stops_after_disabled_arm

printf 'c7a bootstrap preflight tests passed\n'
