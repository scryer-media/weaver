#!/usr/bin/env bash
set -euo pipefail

workflow="${1:-.github/workflows/deploy.yml}"

if ! awk '
  function indentation(line) {
    match(line, /[^ ]/)
    return RSTART - 1
  }

  /^[[:space:]]*run:[[:space:]]*/ && $0 !~ /^[[:space:]]*run:[[:space:]]*[>|]/ && /\$\{\{[[:space:]]*github\.ref_name[[:space:]]*\}\}/ {
    printf "%s:%d: github.ref_name must be passed through env, not expanded in run\n", FILENAME, FNR > "/dev/stderr"
    invalid = 1
  }

  /^[[:space:]]*run:[[:space:]]*/ && $0 !~ /^[[:space:]]*run:[[:space:]]*[>|]/ && /\$\{\{[[:space:]]*needs\.verify-release-tag\.outputs\.(release_tag|version)[[:space:]]*\}\}/ {
    printf "%s:%d: validated release outputs must be passed through env, not expanded in run\n", FILENAME, FNR > "/dev/stderr"
    invalid = 1
  }

  /^[[:space:]]*run:[[:space:]]*[>|][-+]?[[:space:]]*$/ {
    in_run = 1
    run_indent = indentation($0)
    next
  }

  in_run && $0 !~ /^[[:space:]]*$/ && indentation($0) <= run_indent {
    in_run = 0
  }

  in_run && /\$\{\{[[:space:]]*github\.ref_name[[:space:]]*\}\}/ {
    printf "%s:%d: github.ref_name must be passed through env, not expanded in run\n", FILENAME, FNR > "/dev/stderr"
    invalid = 1
  }

  in_run && /\$\{\{[[:space:]]*needs\.verify-release-tag\.outputs\.(release_tag|version)[[:space:]]*\}\}/ {
    printf "%s:%d: validated release outputs must be passed through env, not expanded in run\n", FILENAME, FNR > "/dev/stderr"
    invalid = 1
  }

  END { exit invalid }
' "$workflow"; then
  exit 1
fi

require_secret_in_step() {
  local secret="$1"
  local expected_step="$2"

  awk -v secret="$secret" -v expected_step="$expected_step" '
    /^[[:space:]]*-[[:space:]]+name:[[:space:]]/ {
      step = $0
      sub(/^[[:space:]]*-[[:space:]]+name:[[:space:]]*/, "", step)
    }

    index($0, secret) {
      occurrences++
      if (step != expected_step) {
        printf "%s:%d: %s is outside %s\n", FILENAME, FNR, secret, expected_step > "/dev/stderr"
        invalid = 1
      }
    }

    END {
      if (occurrences != 1) {
        printf "%s: expected exactly one %s secret binding, found %d\n", FILENAME, secret, occurrences > "/dev/stderr"
        invalid = 1
      }
      exit invalid
    }
  ' "$workflow"
}

require_secret_in_step 'secrets.TAP_PUSH_TOKEN' 'Publish Homebrew tap update'
require_secret_in_step 'secrets.WEB_DISPATCH_TOKEN' 'Trigger marketing site rebuild'

# The matrix must not inherit test failures or an optional release-tag skip.
awk '
  function reject(message) {
    printf "%s: %s\n", FILENAME, message > "/dev/stderr"
    invalid = 1
  }

  /^  [[:alnum:]_-]+:$/ {
    job = $0
    sub(/^  /, "", job)
    sub(/:$/, "", job)
  }

  job == "direct-store-matrix-build" {
    if ($0 ~ /^    needs:/) build_needs = $0
    if ($0 ~ /^    if:/) build_if = $0
    if ($0 ~ /run: cargo nextest archive/) archives++
    if ($0 ~ /cargo nextest run/) build_runs_tests = 1
    if ($0 ~ /^          name: direct-store-matrix-linux-x86_64$/) uploads++
  }

  job == "rust-test" && /cargo nextest archive/ { archive_after_tests = 1 }

  job == "direct-store-matrix" {
    if ($0 ~ /^    needs:/) matrix_needs = $0
    if ($0 ~ /^    if:/) matrix_if = $0
    if ($0 ~ /^    runs-on:/) matrix_runner = $0
    if ($0 ~ /^      max-parallel:/) matrix_parallel = $NF
    if ($0 ~ /^          name: direct-store-matrix-linux-x86_64$/) downloads++
    if (index($0, "--partition count:${{ matrix.partition }}/32")) partition_command = 1
    if ($0 ~ /^        partition:/) {
      values = $0
      sub(/^[^[]*\[/, "", values)
      sub(/\].*$/, "", values)
      count = split(values, partitions, ",")
      if (count != 32) reject("direct-store matrix must define 32 partitions")
      for (i = 1; i <= count; i++) {
        if (partitions[i] + 0 != i) reject("direct-store matrix partitions must cover 1 through 32 exactly once")
      }
    }
  }

  END {
    if (build_needs != "    needs: [changes, verify-release-tag, web-build]")
      reject("matrix compilation must run independently of the test jobs")
    if (!index(build_if, "!cancelled()"))
      reject("matrix compilation must handle skipped optional ancestors explicitly")
    if (archives != 1 || uploads != 1 || build_runs_tests || archive_after_tests)
      reject("matrix artifact must be published by its dedicated build job before any test suite")
    if (matrix_needs != "    needs: [direct-store-matrix-build]")
      reject("matrix workers must depend only on their artifact build")
    if (!index(matrix_if, "!cancelled()") || !index(matrix_if, "needs.direct-store-matrix-build.result =="))
      reject("matrix workers need an explicit cancellation and build-result condition")
    if (matrix_runner != "    runs-on: ubuntu-24.04" || matrix_parallel != 32 || count != 32)
      reject("matrix must stay on 32 Linux x86 workers")
    if (downloads != 1 || !partition_command)
      reject("matrix workers must consume the compiled artifact and select their partition")
    exit invalid
  }
' "$workflow"
