#!/usr/bin/env bash
# c7a-bootstrap.sh — idempotent one-shot setup for the weaver-yenc AVX-512/VBMI2
# differential + parity bench (and the rarpar GFNI/AVX-512 GF16 phase) on an AWS
# c7a.xlarge (AMD Zen 4) Ubuntu 24.04 box.
#
# PREBUILT-BINARY MODEL. Nothing is compiled from Rust on this box:
# no rustup, no cargo, no toolchain, no rapidyenc cmake build. Every executable
# — test binaries, bench binaries and librapidyenc.so — is built ahead of time
# on the local Linux builder (Ubuntu 24.04, same glibc as the AMI, plain
# x86_64-unknown-linux-gnu with NO target-cpu flags so runtime dispatch stays
# intact) and ships inside the corpus image at /corpus/prebuilt/.
#
# What this box still needs a compiler for: the rapidyenc differential test
# binaries compile their C oracle at RUNTIME via $CXX
# (engines/weaver-yenc/tests/rapidyenc_decode_diff.rs:146-159). Hence g++.
#
# AWS runs use c7a.xlarge because the benchmark protocol requires its Zen 4
# feature set. Other targets use separate recipes.
#
# The only AWS calls made here are `ecr:GetAuthorizationToken` (via
# `aws ecr get-login-password`) and the layer reads `docker pull` performs.
# Instance provisioning and teardown remain the operator's, by hand
# (see ci/bench/c7a-avx512-diffbench.md §11). No IAM is created or modified —
# the pull permissions are a PROVISIONING PREREQUISITE, documented in §7a.
#
# Grounding (see ci/bench/c7a-avx512-diffbench.md):
#   - runtime C oracle needs $CXX : engines/weaver-yenc/tests/rapidyenc_decode_diff.rs:146
#   - RAPIDYENC_ROOT (src diff)   : engines/weaver-yenc/tests/rapidyenc_decode_diff.rs:431-435
#   - WEAVER_RAPIDYENC_LIB (dlopen): engines/weaver-yenc/benches/rapidyenc_parity.rs:34
#   - WEAVER_RAPIDYENC_SRC is deliberately NOT set: engines/weaver-yenc/build.rs:15
#     would statically link rapidyenc into weaver and change weaver's own codegen.
#
# Does NOT run any test/bench — that is c7a-run.sh's job.
set -euo pipefail

# ── Dead-man shutdown ────────────────────────────────────────────────────────
# Armed only after configuration preflight succeeds, so invalid input cannot
# alter host shutdown state before the script reports the real error.
#
# REQUIRES the instance to have been launched with
#   --instance-initiated-shutdown-behavior terminate
# (as ci/bench/avx2-aws-run.sh:94 does for the AVX2 box). Without it this only
# STOPS the instance and the EBS root volume keeps billing.
#
# This is a safety net, not a schedule. c7a-run.sh measures each phase and
# records the numbers in
# metadata.json. Raise DEADMAN_MINUTES if a run ever approaches it; set 0 to
# skip (e.g. re-running on an already-armed box).
DEADMAN_MINUTES="${DEADMAN_MINUTES:-120}"

arm_deadman() {
  if [ "$DEADMAN_MINUTES" = 0 ]; then
    printf '\033[1;33m[bootstrap:warn]\033[0m dead-man shutdown DISABLED (DEADMAN_MINUTES=%s)\n' \
      "$DEADMAN_MINUTES" >&2
    return 0
  fi
  if ! command -v shutdown >/dev/null 2>&1; then
    printf '\033[1;33m[bootstrap:warn]\033[0m no shutdown(8); dead-man timer NOT armed\n' >&2
    return 0
  fi
  sudo shutdown -c >/dev/null 2>&1 || true
  if sudo shutdown -h "+$DEADMAN_MINUTES" \
      "c7a bench dead-man: instance halts in ${DEADMAN_MINUTES}m" >/dev/null 2>&1; then
    printf '\033[1;34m[bootstrap]\033[0m dead-man shutdown armed: -h +%s (cancel: sudo shutdown -c)\n' \
      "$DEADMAN_MINUTES"
  else
    printf '\033[1;33m[bootstrap:warn]\033[0m could not arm dead-man shutdown (no sudo?)\n' >&2
  fi
}

# ── Config (all overridable) ─────────────────────────────────────────────────
# The corpus image extracts /corpus/. into $CORPUS_DEST, which yields
# $CORPUS_DEST/{weaver,rarpar,rapidyenc,prebuilt}.
CORPUS_DEST="${CORPUS_DEST:-$HOME}"
CORPUS_IMAGE="${CORPUS_IMAGE:-}"
CORPUS_REGION="${CORPUS_REGION:-us-east-1}"
CORPUS_REGISTRY="${CORPUS_IMAGE%%/*}"
CORPUS_FORCE="${CORPUS_FORCE:-0}"   # 1 = re-pull and re-extract even if present

WEAVER_DIR="${WEAVER_DIR:-$CORPUS_DEST/weaver}"
RARPAR_DIR="${RARPAR_DIR:-$CORPUS_DEST/rarpar}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$CORPUS_DEST/rapidyenc}"
PREBUILT_DIR="${PREBUILT_DIR:-$CORPUS_DEST/prebuilt}"
MANIFEST_JSON="$PREBUILT_DIR/manifest.json"
BUILDINFO_JSON="$PREBUILT_DIR/BUILDINFO.json"
# The parity bench dlopens the PREBUILT .so — there is no cmake build any more.
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$PREBUILT_DIR/lib/librapidyenc.so}"
DOCKER=""   # resolved by resolve_docker()
BOOTSTRAP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[bootstrap:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

assert_corpus_image_pinned() {
  [ -n "$CORPUS_IMAGE" ] \
    || die "set CORPUS_IMAGE to a digest-pinned ECR image"
  printf '%s' "$CORPUS_IMAGE" | grep -Eq '.+/.+@sha256:[0-9a-fA-F]{64}$' \
    || die "CORPUS_IMAGE must use an immutable @sha256 digest, not a mutable tag"
}

assert_safe_destination() {
  local name="$1" path="$2"
  case "$path" in
    ""|/) die "$name must be a non-root absolute path" ;;
    /*) ;;
    *) die "$name must be an absolute path" ;;
  esac
  case "$path" in
    *$'\n'*|*$'\r'*|*//*|*/./*|*/../*|*/.|*/..)
      die "$name must not contain empty, dot, or parent path components"
      ;;
  esac
}

assert_child_destination() {
  local name="$1" path="$2"
  case "$path" in
    "$CORPUS_DEST"/*) ;;
    *) die "$name must remain below CORPUS_DEST" ;;
  esac
}

assert_preflight_prerequisites() {
  local tool
  for tool in awk bash cut dirname find grep head id ldd sed sort; do
    command -v "$tool" >/dev/null 2>&1 \
      || die "required bootstrap prerequisite '$tool' is not available"
  done
  if [ "$(id -u)" -ne 0 ] && ! command -v sudo >/dev/null 2>&1; then
    die "sudo is required when bootstrap does not run as root"
  fi
  [ -r "$BOOTSTRAP_DIR/c7a-run.sh" ] \
    || die "missing script-local companion $BOOTSTRAP_DIR/c7a-run.sh"
}

preflight() {
  assert_preflight_prerequisites
  assert_corpus_image_pinned
  case "$DEADMAN_MINUTES" in
    0|[1-9]|[1-9][0-9]*) ;;
    *) die "DEADMAN_MINUTES must be zero or a positive whole number" ;;
  esac

  assert_safe_destination "CORPUS_DEST" "$CORPUS_DEST"
  assert_safe_destination "WEAVER_DIR" "$WEAVER_DIR"
  assert_safe_destination "RARPAR_DIR" "$RARPAR_DIR"
  assert_safe_destination "RAPIDYENC_ROOT" "$RAPIDYENC_ROOT"
  assert_safe_destination "PREBUILT_DIR" "$PREBUILT_DIR"
  assert_safe_destination "WEAVER_RAPIDYENC_LIB" "$WEAVER_RAPIDYENC_LIB"
  assert_child_destination "WEAVER_DIR" "$WEAVER_DIR"
  assert_child_destination "RARPAR_DIR" "$RARPAR_DIR"
  assert_child_destination "RAPIDYENC_ROOT" "$RAPIDYENC_ROOT"
  assert_child_destination "PREBUILT_DIR" "$PREBUILT_DIR"
  case "$WEAVER_RAPIDYENC_LIB" in
    "$PREBUILT_DIR"/*) ;;
    *) die "WEAVER_RAPIDYENC_LIB must remain below PREBUILT_DIR" ;;
  esac
  if [ "$WEAVER_DIR" = "$RARPAR_DIR" ] || [ "$WEAVER_DIR" = "$RAPIDYENC_ROOT" ] || \
     [ "$WEAVER_DIR" = "$PREBUILT_DIR" ] || [ "$RARPAR_DIR" = "$RAPIDYENC_ROOT" ] || \
     [ "$RARPAR_DIR" = "$PREBUILT_DIR" ] || [ "$RAPIDYENC_ROOT" = "$PREBUILT_DIR" ]; then
    die "WEAVER_DIR, RARPAR_DIR, RAPIDYENC_ROOT, and PREBUILT_DIR must be distinct"
  fi
}

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

# ── CPU feature assertion (the precondition that makes this run meaningful) ──
# avx512vl is load-bearing twice over: it selects the VBMI2 decode kernel
# (simd/mod.rs:362-368) AND it is the feature that DISABLES weaver's own VPCLMUL
# CRC port (crc.rs:123) — the CRC gate proof.
REQUIRED_FEATURES="avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes"
assert_cpu_features() {
  log "Asserting Zen 4 CPU features: ${REQUIRED_FEATURES}"
  [ -r /proc/cpuinfo ] || die "/proc/cpuinfo unreadable; cannot verify CPU (not Linux?)"
  local flags missing=""
  flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2- || true)"
  [ -n "$flags" ] || die "could not read CPU flags from /proc/cpuinfo"
  # Linux spells VBMI2 with an underscore in /proc/cpuinfo (avx512_vbmi2, vs
  # avx512vbmi for VBMI1). Normalize so the canonical token below matches on
  # real hardware; a CPU without VBMI2 reports neither spelling and still fails.
  case " $flags " in *" avx512_vbmi2 "*) flags="$flags avx512vbmi2" ;; esac
  local f
  for f in $REQUIRED_FEATURES; do
    case " $flags " in
      *" $f "*) : ;;
      *) missing="$missing $f" ;;
    esac
  done
  if [ -n "$missing" ]; then
    warn "CPU model: $(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
    die "missing CPU feature(s):${missing}
     This is NOT a real AVX-512/VBMI2/GFNI Zen 4 core.
     The benchmark protocol requires c7a.xlarge (doc §1); other instance
     families may not expose avx512vbmi2 + gfni on real silicon."
  fi
  log "CPU feature gate PASSED — real AVX-512 VBMI2 + GFNI present."
}

# ── System packages (Ubuntu 24.04) ──────────────────────────────────────────
# Deliberately minimal now that nothing Rust is compiled here:
#   build-essential -> g++ for the RUNTIME C-oracle compile in the diff tests
#   jq              -> manifest/BUILDINFO/REVISION parsing + summary.json
#   tar             -> criterion tree archiving (doc §9g)
#   curl/ca-certs   -> IMDS + general
# NOT installed any more: rustup/cargo (no Rust build), cmake + nasm (were for
# the rapidyenc cmake build and aws-lc-sys), pkg-config, git (no .git ships).
install_system_deps() {
  log "Installing system dependencies (apt)…"
  local pkgs="build-essential jq tar curl ca-certificates"
  if command -v apt-get >/dev/null 2>&1; then
    local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
    # shellcheck disable=SC2086
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $pkgs
  else
    warn "apt-get not found; ensure these are installed manually: $pkgs"
  fi
  local bin
  for bin in c++ jq tar curl; do
    command -v "$bin" >/dev/null 2>&1 || die "required tool '$bin' still missing after install"
  done
  log "System deps OK (c++, jq, tar, curl present). No Rust toolchain — by design."
}

# ── Corpus delivery: pre-pushed ECR image ────────────────────────────────────
#
# `.git` does NOT ship, so every tree carries its own REVISION.json
# ({repo, rev, dirty_files, staged_at_utc}) and provenance is read from that
# rather than from git. Every source tree must be clean; rarpar ships with its
# LFS fixtures already hydrated, and rapidyenc is pinned at 27f435a.
# /corpus/prebuilt/ carries executables built from exactly those revisions.
revision_field() {   # <tree-dir> <field> [default]
  local f="$1/REVISION.json" out=""
  if [ -f "$f" ] && command -v jq >/dev/null 2>&1; then
    out="$(jq -r --arg k "$2" '.[$k] // empty' "$f" 2>/dev/null || true)"
  fi
  printf '%s' "${out:-${3:-unknown}}"
}

buildinfo_field() {  # <field> [default]
  local out=""
  if [ -f "$BUILDINFO_JSON" ] && command -v jq >/dev/null 2>&1; then
    out="$(jq -r --arg k "$1" '.[$k] // empty' "$BUILDINFO_JSON" 2>/dev/null || true)"
  fi
  printf '%s' "${out:-${2:-unknown}}"
}

# Tolerates a bare top-level array or a {binaries|bins: [...]} wrapper.
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

resolve_docker() {
  if docker info >/dev/null 2>&1; then
    DOCKER="docker"
  elif sudo docker info >/dev/null 2>&1; then
    # Fresh docker.io installs leave the login shell outside the `docker` group
    # until it is re-established; sudo sidesteps that without a re-login.
    DOCKER="sudo docker"
  else
    die "docker daemon unreachable (tried 'docker' and 'sudo docker'). Is docker.io installed and running?"
  fi
  log "docker command: $DOCKER"
}

install_ecr_tooling() {
  local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
  # NOTE: `awscli` is NOT in the Ubuntu 24.04 (noble) archive, and listing it
  # in the same apt transaction as docker.io makes the WHOLE install fail
  # atomically — docker never gets installed either. docker.io comes from apt;
  # the AWS CLI comes from the official v2 bundle below (needs unzip).
  local want=""
  command -v docker >/dev/null 2>&1 || want="$want docker.io"
  command -v unzip  >/dev/null 2>&1 || want="$want unzip"
  if [ -n "$want" ]; then
    log "Installing corpus-delivery tooling (apt):$want"
    if command -v apt-get >/dev/null 2>&1; then
      $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
      # shellcheck disable=SC2086
      $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $want
    else
      warn "apt-get not found; install manually:$want"
    fi
  fi
  if ! command -v aws >/dev/null 2>&1; then
    log "Installing AWS CLI v2 (awscli has no installation candidate on noble)…"
    local tmpd; tmpd="$(mktemp -d)"
    ( curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
        -o "$tmpd/awscliv2.zip" \
      && cd "$tmpd" && unzip -q awscliv2.zip && $SUDO ./aws/install ) \
      || { rm -rf "$tmpd"; die "AWS CLI v2 install failed"; }
    rm -rf "$tmpd"
  fi
  command -v docker >/dev/null 2>&1 || die "docker still missing after install"
  command -v aws    >/dev/null 2>&1 || die "aws CLI still missing after install (needed for 'aws ecr get-login-password')"
  resolve_docker
}

corpus_is_extracted() {
  [ -f "$WEAVER_DIR/REVISION.json" ] &&
  [ -f "$RARPAR_DIR/REVISION.json" ] &&
  [ -f "$RAPIDYENC_ROOT/REVISION.json" ] &&
  [ -f "$MANIFEST_JSON" ] &&
  [ -f "$BUILDINFO_JSON" ]
}

# Pull + extract WITHOUT ever running the image: `docker create` materializes a
# container's filesystem but starts nothing, `docker cp` reads it out, `docker
# rm` discards it. No process from the image is ever executed on this box.
fetch_corpus() {
  if corpus_is_extracted && [ "$CORPUS_FORCE" != "1" ]; then
    log "corpus + prebuilt bundle already extracted under $CORPUS_DEST (CORPUS_FORCE=1 to re-pull)."
    return 0
  fi

  install_ecr_tooling

  log "Logging in to $CORPUS_REGISTRY (region $CORPUS_REGION)…"
  aws ecr get-login-password --region "$CORPUS_REGION" \
    | $DOCKER login --username AWS --password-stdin "$CORPUS_REGISTRY" \
    || die "ECR login failed.
     This is a PROVISIONING PREREQUISITE, not something this script fixes:
     the instance needs an IAM role (or env credentials) granting
       ecr:GetAuthorizationToken, ecr:BatchGetImage, ecr:GetDownloadUrlForLayer
     on $CORPUS_IMAGE. Attach the role and re-run. This script never
     creates or modifies IAM."

  log "Pulling $CORPUS_IMAGE …"
  $DOCKER pull "$CORPUS_IMAGE" || die "docker pull failed — check ecr:BatchGetImage / ecr:GetDownloadUrlForLayer"

  log "Extracting /corpus/. -> $CORPUS_DEST/ (container is created, never run)"
  local cid rc=0
  # The dummy argv is REQUIRED: the corpus image is FROM scratch with no CMD,
  # so a bare `docker create` fails with "no command specified". The container
  # is never started; /bin/true need not exist in the image.
  cid="$($DOCKER create "$CORPUS_IMAGE" /bin/true)" || die "docker create failed"
  $DOCKER cp "$cid:/corpus/." "$CORPUS_DEST/" || rc=$?
  $DOCKER rm -v "$cid" >/dev/null 2>&1 || warn "could not remove scratch container $cid"
  [ "$rc" -eq 0 ] || die "docker cp of /corpus/. failed (rc=$rc)"

  # docker cp preserves in-image ownership (root). Reclaim the trees or the run
  # user cannot write RESULTS_DIR, generate the corpus in-tree, or build
  # rapidyenc oracles into it.
  local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
  $SUDO chown -R "$(id -un):$(id -gn)" \
      "$WEAVER_DIR" "$RARPAR_DIR" "$RAPIDYENC_ROOT" "$PREBUILT_DIR" 2>/dev/null \
    || warn "chown of extracted trees failed — expect permission errors in the run"

  corpus_is_extracted || die "extraction finished but the expected layout is incomplete.
     wanted REVISION.json in each of: $WEAVER_DIR, $RARPAR_DIR, $RAPIDYENC_ROOT
     plus $MANIFEST_JSON and $BUILDINFO_JSON
     (is the image's /corpus layout what this runbook expects?)"

  local t
  for t in "$WEAVER_DIR" "$RARPAR_DIR" "$RAPIDYENC_ROOT"; do
    log "  $(revision_field "$t" repo "$(basename "$t")"): rev=$(revision_field "$t" rev) dirty_files=$(revision_field "$t" dirty_files 0) staged_at_utc=$(revision_field "$t" staged_at_utc)"
  done
  log "Corpus + prebuilt bundle extracted."
}

# ── Prebuilt bundle integrity ────────────────────────────────────────────────
# The single most dangerous failure mode of the prebuilt model is a STALE
# bundle: binaries built from an older revision than the source trees shipped
# beside them. Every gate downstream (the CRC proof, the differential counts,
# the forced-tier coverage) would then be measuring something other than the
# source the operator believes they are testing — and would report green.
# Rev mismatch is therefore a HARD FAIL, not a warning.
assert_prebuilt_bundle() {
  log "Verifying the prebuilt bundle at $PREBUILT_DIR …"
  [ -d "$PREBUILT_DIR" ]     || die "no prebuilt bundle at $PREBUILT_DIR — image predates the prebuilt layer (doc §6)"
  [ -f "$MANIFEST_JSON" ]    || die "missing $MANIFEST_JSON"
  [ -f "$BUILDINFO_JSON" ]   || die "missing $BUILDINFO_JSON"

  jq -e . "$MANIFEST_JSON"  >/dev/null 2>&1 || die "$MANIFEST_JSON is not valid JSON"
  jq -e . "$BUILDINFO_JSON" >/dev/null 2>&1 || die "$BUILDINFO_JSON is not valid JSON"

  local ids id bin kind n=0 bad=0
  ids="$(manifest_ids)"
  [ -n "$ids" ] || die "$MANIFEST_JSON lists no binaries (expected a top-level array, or {binaries:[…]})"

  while IFS= read -r id; do
    [ -n "$id" ] || continue
    n=$((n + 1))
    bin="$PREBUILT_DIR/bin/$id"
    kind="$(manifest_field "$id" kind)"
    case "$kind" in
      test|bench) : ;;
      *) warn "  $id: unexpected kind='${kind:-<empty>}' (want test|bench)" ;;
    esac
    if [ ! -f "$bin" ]; then
      warn "  MISSING   $id -> $bin"; bad=$((bad + 1))
    elif [ ! -x "$bin" ]; then
      warn "  NOT EXEC  $id -> $bin"; bad=$((bad + 1))
    else
      log "  ok  $id ($kind)"
    fi
  done <<EOF
$ids
EOF
  [ "$bad" -eq 0 ] || die "$bad of $n manifest binaries are missing or not executable under $PREBUILT_DIR/bin"
  log "  $n manifest binaries present and executable."

  # Ids the run script names explicitly. Everything else is discovered by
  # iterating the manifest, but these must exist or a whole phase silently
  # disappears — the rarpar-bench suite is the headline of the run (doc §8a).
  #   rarpar-cli            -> `run --candidate`
  #   rarpar-bench-harness  -> the Go harness itself
  #   oracle-unrar-723      -> `run --reference-rar`
  #   oracle-par2-turbo-140 -> `run --reference-par2`
  # BOTH references are mandatory, not one: internal/bench/run.go:59-64 hard
  # errors with "a comparative corpus run requires both reference RAR and
  # reference PAR2 binaries" the moment either is supplied without the other.
  # The par2 arm is timed at run.go:248-258 and labelled "par2cmdline-turbo" in
  # report.go:83-88. oracle-unrar-624 is the ONLY optional spare.
  local want missing_req=""
  for want in rarpar-cli rarpar-bench-harness oracle-unrar-723 oracle-par2-turbo-140; do
    if [ ! -x "$PREBUILT_DIR/bin/$want" ]; then missing_req="$missing_req $want"; fi
  done
  [ -z "$missing_req" ] || die "required binaries absent from the bundle:${missing_req}
     These are named directly by c7a-run.sh; without them the rarpar-bench
     phase cannot run. Rebuild the bundle with them included."
  log "  required ids present: rarpar-cli, rarpar-bench-harness, oracle-unrar-723, oracle-par2-turbo-140"
  # oracle-unrar-624 is the only optional spare (a second UnRAR major for
  # ad-hoc comparison; the harness takes exactly one --reference-rar).
  [ -x "$PREBUILT_DIR/bin/oracle-unrar-624" ] && log "  optional spare also present: oracle-unrar-624"

  # BUILDINFO revs must match the source trees shipped alongside them.
  local pair name binfo_rev tree_rev tree dirty
  for pair in "weaver:weaver_rev:$WEAVER_DIR" "rarpar:rarpar_rev:$RARPAR_DIR" "rapidyenc:rapidyenc_rev:$RAPIDYENC_ROOT"; do
    name="${pair%%:*}"
    tree="${pair##*:}"
    binfo_rev="$(buildinfo_field "$(printf '%s' "$pair" | cut -d: -f2)")"
    tree_rev="$(revision_field "$tree" rev)"
    dirty="$(revision_field "$tree" dirty_files 0)"
    [ "$dirty" = "0" ] \
      || die "$name source has $dirty dirty file(s); rebuild the corpus from a clean revision"
    if [ "$binfo_rev" = "unknown" ] || [ "$tree_rev" = "unknown" ]; then
      die "cannot compare $name revisions (BUILDINFO='$binfo_rev' REVISION.json='$tree_rev') — refusing to run a bundle of unknown provenance"
    fi
    # Prefix-tolerant compare: REVISION.json records full 40-char revs while
    # BUILDINFO records the short form. Either may prefix the other, but a
    # match needs at least 7 chars — strict equality here bricked run #2
    # (every tree "stale" despite identical commits).
    if ! revs_agree "$binfo_rev" "$tree_rev"; then
      die "STALE BUNDLE: $name binaries were built from $binfo_rev but the shipped tree is $tree_rev.
     The prebuilt executables do not correspond to the source in this image.
     Rebuild the bundle and re-push the corpus image; do not 'work around' this."
    fi
    log "  rev match $name: $binfo_rev"
  done

  # glibc: the builder is chosen to match the AMI. A mismatch means that
  # assumption broke and the binaries may not load at all.
  local built_glibc host_glibc
  built_glibc="$(buildinfo_field glibc)"
  host_glibc="$(ldd --version 2>/dev/null | head -1 | awk '{print $NF}' || true)"
  if [ "$built_glibc" != "unknown" ] && [ -n "$host_glibc" ] && [ "$built_glibc" != "$host_glibc" ]; then
    warn "glibc differs: bundle built against '$built_glibc', this box has '$host_glibc' — binaries may fail to load"
  else
    log "  glibc: bundle='$built_glibc' host='${host_glibc:-?}'"
  fi

  log "  builder: $(buildinfo_field builder)   built_at_utc: $(buildinfo_field built_at_utc)"
  log "  rustc:   $(buildinfo_field rustc_verbose "$(buildinfo_field rustc)")"
  log "  rustflags: '$(buildinfo_field rustflags '')' (empty on purpose — runtime dispatch, no target-cpu pinning)"
}

# ── rarpar-bench corpus cache ────────────────────────────────────────────────
# The Go harness reads a pre-generated, digest-addressed corpus from
#   <rarpar>/bench/rarpar-bench/.cache/corpora/<digest>/corpus/
# (verified layout: that directory holds corpus.json plus one directory per
# case). The cache ships inside the image and merges into the extracted rarpar
# tree. Its presence is what keeps Docker out of this run: corpus GENERATION is
# the only Docker-dependent path in the harness (internal/bench/corpus.go:113+,
# internal/bench/toolchain.go:67+), and we never call it.
RARPAR_BENCH_DIR="${RARPAR_BENCH_DIR:-$RARPAR_DIR/bench/rarpar-bench}"
assert_corpus_cache() {
  local cache="$RARPAR_BENCH_DIR/.cache/corpora"
  log "Verifying the rarpar-bench corpus cache…"
  [ -d "$cache" ] || die "corpus cache missing: $cache
     The rarpar-bench suite is the headline phase of this run and it cannot
     generate its own corpus here — generation needs Docker plus the pinned
     toolchain images (doc §8a). The cache must ship in the corpus image and
     merge into the extracted rarpar tree. Re-pull with CORPUS_FORCE=1; if it
     is still absent the image needs rebuilding with the .cache layer."
  local roots n=0 root
  roots="$(find "$cache" -mindepth 2 -maxdepth 2 -type d -name corpus 2>/dev/null | sort || true)"
  [ -n "$roots" ] || die "no <digest>/corpus directory under $cache — the cache is present but empty"
  while IFS= read -r root; do
    [ -n "$root" ] || continue
    n=$((n + 1))
    if [ -f "$root/corpus.json" ]; then
      log "  corpus digest $(basename "$(dirname "$root")") — $(jq -r '.case_count // "?"' "$root/corpus.json" 2>/dev/null) case(s)"
    else
      die "corpus root $root has no corpus.json — cache is malformed"
    fi
  done <<EOF
$roots
EOF
  log "  $n cached corpus root(s) present."
}

assert_prebuilt_rapidyenc_lib() {
  local lib="$WEAVER_RAPIDYENC_LIB"
  log "Verifying the prebuilt librapidyenc.so …"
  [ -e "$lib" ] || die "missing $lib — the parity bench (§3b) dlopens this and would silently skip without it"
  local unresolved
  unresolved="$(ldd "$lib" 2>&1 | grep 'not found' || true)"
  [ -z "$unresolved" ] || die "librapidyenc.so has unresolved shared-library deps:
$unresolved"
  log "  $lib resolves cleanly (ldd)."
}

# ── Compile-time path baking (rarpar only) ───────────────────────────────────
# rarpar's test and bench sources resolve their fixtures through
# env!("CARGO_MANIFEST_DIR"), which is baked in at COMPILE time — e.g.
#   crates/weaver-unrar/benches/archive_hotspots.rs:4
#   crates/weaver-par2/tests/support/benchmark_support.rs:217
# plus ~15 more across the two crates. A binary built elsewhere therefore looks
# for its fixtures at the BUILDER's absolute path, which does not exist here.
# One symlink per baked root repairs every one of those lookups at once.
#
# weaver-yenc is clean — it has no env!("CARGO_MANIFEST_DIR") anywhere in its
# src/, tests/ or benches/, so its binaries are genuinely relocatable.
link_baked_manifest_roots() {
  command -v grep >/dev/null 2>&1 || return 0
  local id bin baked root roots="" seen declared

  # Preferred source of truth: the bundle states where it was built.
  declared="$(buildinfo_field builder_rarpar_root)"
  if [ -n "$declared" ] && [ "$declared" != "unknown" ]; then
    roots=" $declared"
    log "Baked rarpar root declared by BUILDINFO.builder_rarpar_root."
  else
    # Fallback only: recover the root by scanning the binaries' string data.
    #
    # This must be filtered hard. A weaver bench binary carries ~123 dependency
    # panic-path literals under the builder's Cargo registry
    # (…/.cargo/registry/src/…/some-crate-1.2.3/src/lib.rs), and those are NOT
    # fixture roots — symlinking one would be nonsense at best. Accept a
    # candidate only when it is a real rarpar checkout root: the path contains
    # /rarpar/crates/ and the derived root ends in /rarpar.
    log "BUILDINFO has no builder_rarpar_root; scanning rarpar binaries for baked fixture paths…"
    while IFS= read -r id; do
      [ -n "$id" ] || continue
      [ "$(manifest_field "$id" repo)" = "rarpar" ] || continue
      bin="$PREBUILT_DIR/bin/$id"
      [ -x "$bin" ] || continue
      while IFS= read -r baked; do
        [ -n "$baked" ] || continue
        case "$baked" in
          */.cargo/registry/*) continue ;;   # dependency panic paths, not fixtures
          */rarpar/crates/*)   : ;;
          *)                   continue ;;
        esac
        root="${baked%/crates/*}"
        [ -n "$root" ] && [ "$root" != "$baked" ] || continue
        case "$root" in */rarpar) : ;; *) continue ;; esac
        case " $roots " in *" $root "*) : ;; *) roots="$roots $root" ;; esac
      done <<EOF
$(grep -aoE '/[A-Za-z0-9._@+-]+(/[A-Za-z0-9._@+-]+)*/crates/weaver-(par2|unrar|reed-solomon)' "$bin" 2>/dev/null | sort -u || true)
EOF
    done <<EOF
$(manifest_ids)
EOF
  fi

  if [ -z "${roots// /}" ]; then
    log "  no baked rarpar roots found (nothing to link)."
    return 0
  fi

  for seen in $roots; do
    if [ "$seen" = "$RARPAR_DIR" ]; then
      log "  baked root == RARPAR_DIR ($seen) — builder and box agree, nothing to do."
      continue
    fi
    if [ -e "$seen" ]; then
      log "  baked root already resolves: $seen"
      continue
    fi
    log "  linking baked root $seen -> $RARPAR_DIR"
    local parent; parent="$(dirname "$seen")"
    mkdir -p "$parent" 2>/dev/null || sudo mkdir -p "$parent" \
      || die "cannot create $parent to host the baked-path symlink"
    ln -s "$RARPAR_DIR" "$seen" 2>/dev/null || sudo ln -s "$RARPAR_DIR" "$seen" \
      || die "cannot symlink $seen -> $RARPAR_DIR
     Without it every rarpar fixture lookup fails, because the bench and test
     binaries carry the builder's absolute path (see the comment above this
     function). Either create that path by hand, or rebuild the bundle with the
     builder's rarpar checkout living at $RARPAR_DIR."
  done
}

# ── rarpar presence (mandatory phase; warn here, hard-fail in c7a-run.sh) ────
check_rarpar() {
  if [ -d "$RARPAR_DIR" ]; then
    log "rarpar tree present at $RARPAR_DIR (rev $(revision_field "$RARPAR_DIR" rev))"
    # The archive_hotspots fixtures are git-LFS upstream. The image build
    # hydrates them; an unhydrated fixture is a ~130-byte pointer file and the
    # bench dies on a malformed archive rather than on anything informative.
    local fx="$RARPAR_DIR/crates/weaver-unrar/tests/fixtures/rar5/rar5_lz.rar"
    if [ -f "$fx" ]; then
      if head -c 5 "$fx" | grep -q 'Rar!'; then
        log "  unrar fixtures hydrated (rar5_lz.rar has a real RAR signature)."
      else
        warn "  $fx is NOT a real RAR (git-LFS pointer?) — re-pull with CORPUS_FORCE=1; if it persists the image needs rebuilding"
      fi
    else
      warn "  $fx missing — the archive_hotspots bench will fail"
    fi
  else
    warn "rarpar tree NOT found at $RARPAR_DIR.
     The rarpar phase is MANDATORY and c7a-run.sh will ABORT without it.
     Re-pull and re-extract the corpus image (doc §7a):
       CORPUS_FORCE=1 ./ci/bench/c7a-bootstrap.sh"
  fi
}

write_env_file() {
  cat > "$PREBUILT_DIR/weaver-bench.env" <<EOF
# sourced by ci/bench/c7a-run.sh — discovery for the prebuilt bundle
export PREBUILT_DIR="$PREBUILT_DIR"
export CORPUS_IMAGE="$CORPUS_IMAGE"
export CORPUS_REGION="$CORPUS_REGION"
export RAPIDYENC_ROOT="$RAPIDYENC_ROOT"
export WEAVER_RAPIDYENC_LIB="$WEAVER_RAPIDYENC_LIB"
export RARPAR_BENCH_DIR="$RARPAR_BENCH_DIR"
EOF
  log "Wrote $PREBUILT_DIR/weaver-bench.env"
}

main() {
  preflight
  arm_deadman
  log "CORPUS_IMAGE=$CORPUS_IMAGE"
  log "CORPUS_DEST=$CORPUS_DEST"
  log "WEAVER_DIR=$WEAVER_DIR"
  log "RARPAR_DIR=$RARPAR_DIR"
  log "RAPIDYENC_ROOT=$RAPIDYENC_ROOT"
  log "PREBUILT_DIR=$PREBUILT_DIR"
  assert_cpu_features
  # jq first: everything below reads manifest/BUILDINFO/REVISION JSON.
  install_system_deps
  fetch_corpus
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found even after corpus extraction (doc §7a)."
  assert_prebuilt_bundle
  assert_prebuilt_rapidyenc_lib
  assert_corpus_cache
  link_baked_manifest_roots
  check_rarpar
  write_env_file

  printf '\n\033[1;32m'
  cat <<'BANNER'
============================================================
  c7a bootstrap READY  (prebuilt-binary model)
------------------------------------------------------------
  - dead-man shutdown armed (see DEADMAN_MINUTES)
  - CPU AVX-512 VBMI2 + GFNI feature gate: PASSED
  - system deps (g++/jq/tar/curl): installed
  - NO Rust toolchain installed - binaries ship prebuilt
  - corpus image: pulled + extracted (weaver/rarpar/rapidyenc/prebuilt)
  - prebuilt bundle: manifest + BUILDINFO verified, revs match
  - librapidyenc.so: present and ldd-resolved
  - rarpar-bench corpus cache: present (no Docker needed)
  - rarpar tree: see log above (MANDATORY for the run)
  Next:  ./ci/bench/c7a-run.sh
============================================================
BANNER
  printf '\033[0m\n'
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
