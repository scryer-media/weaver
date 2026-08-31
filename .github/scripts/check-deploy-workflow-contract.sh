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
