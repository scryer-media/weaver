#!/usr/bin/env bash
# c7a-bootstrap.sh — idempotent one-shot setup for the weaver-yenc AVX-512/VBMI2
# differential + parity bench (and the rarpar GFNI/AVX-512 GF16 phase) on an AWS
# c7a.xlarge (AMD Zen 4) Ubuntu 24.04 box.
#
# Installs: system build deps, the ECR corpus image (source delivery), rustup +
# the repo-pinned toolchain (1.97.1 via rust-toolchain.toml), then cmake-builds
# the rapidyenc reference, and asserts the CPU actually exposes the
# AVX-512/VBMI2/GFNI feature set. Prints a READY banner on success. Safe to
# re-run.
#
# AWS RUNS ONLY. Source delivery is the pre-pushed ECR corpus image (§7a).
# Local-box runs (SYLIX / codex-x86) keep their own rsync recipes and are not
# affected by anything in this script.
#
# The only AWS calls made here are `ecr:GetAuthorizationToken` (via
# `aws ecr get-login-password`) and the layer reads `docker pull` performs.
# Instance provisioning and teardown remain the operator's, by hand
# (see ci/bench/c7a-avx512-diffbench.md §11). No IAM is created or modified —
# the pull permissions are a PROVISIONING PREREQUISITE, documented in §7a.
#
# Grounding (see ci/bench/c7a-avx512-diffbench.md):
#   - toolchain 1.97.1          : rust-toolchain.toml:2  (rarpar pins the same)
#   - nasm required (aws-lc-sys): .github/workflows/deploy.yml:183, 215, 346
#   - rapidyenc shared lib      : rapidyenc/CMakeLists.txt:11, 269, 271
#   - RAPIDYENC_ROOT (src diff) : engines/weaver-yenc/tests/rapidyenc_decode_diff.rs:431-435
#   - WEAVER_RAPIDYENC_LIB (bench dlopen): engines/weaver-yenc/benches/rapidyenc_parity.rs:34
#   - WEAVER_RAPIDYENC_SRC is deliberately NOT set: engines/weaver-yenc/build.rs:15
#     would statically link rapidyenc into weaver and change weaver's own codegen.
#
# Does NOT run cargo test/bench — that is c7a-run.sh's job.
set -euo pipefail

# ── Dead-man shutdown ────────────────────────────────────────────────────────
# Armed FIRST, before anything can fail, so a session abandoned mid-bootstrap
# still self-destructs. Caps a forgotten session at 4h (~$0.85 on c7a.xlarge).
#
# REQUIRES the instance to have been launched with
#   --instance-initiated-shutdown-behavior terminate
# (as ci/bench/avx2-aws-run.sh:94 does for the AVX2 box). Without it this only
# STOPS the instance and the EBS root volume keeps billing.
#
# The 240-minute budget covers: rapidyenc cmake build, weaver-yenc debug tests,
# weaver-yenc release tests (fat LTO, codegen-units=1 — the slow one), two
# weaver bench binaries x (warm + 2 recorded passes), then the rarpar phase
# (aws-lc-sys build + reedsolomon/par2 tests + par2_repair + archive_hotspots).
# Set DEADMAN_MINUTES=0 to skip (e.g. when re-running on an already-armed box).
DEADMAN_MINUTES="${DEADMAN_MINUTES:-240}"

arm_deadman() {
  if [ "$DEADMAN_MINUTES" -le 0 ] 2>/dev/null; then
    printf '\033[1;33m[bootstrap:warn]\033[0m dead-man shutdown DISABLED (DEADMAN_MINUTES=%s)\n' \
      "$DEADMAN_MINUTES" >&2
    return 0
  fi
  if ! command -v shutdown >/dev/null 2>&1; then
    printf '\033[1;33m[bootstrap:warn]\033[0m no shutdown(8); dead-man timer NOT armed\n' >&2
    return 0
  fi
  # Cancel any timer from a previous run so re-running does not stack them.
  sudo shutdown -c >/dev/null 2>&1 || true
  if sudo shutdown -h "+$DEADMAN_MINUTES" \
      "c7a bench dead-man: instance halts in ${DEADMAN_MINUTES}m" >/dev/null 2>&1; then
    printf '\033[1;34m[bootstrap]\033[0m dead-man shutdown armed: -h +%s (cancel: sudo shutdown -c)\n' \
      "$DEADMAN_MINUTES"
  else
    printf '\033[1;33m[bootstrap:warn]\033[0m could not arm dead-man shutdown (no sudo?)\n' >&2
  fi
}
arm_deadman

# ── Config (all overridable) ─────────────────────────────────────────────────
# The corpus image extracts /corpus/. into $CORPUS_DEST, which yields
# $CORPUS_DEST/{weaver,rarpar,rapidyenc} — hence the defaults below.
CORPUS_DEST="${CORPUS_DEST:-$HOME}"
CORPUS_IMAGE="${CORPUS_IMAGE:-651588424025.dkr.ecr.us-east-1.amazonaws.com/weaver-bench-corpus:latest}"
CORPUS_REGION="${CORPUS_REGION:-us-east-1}"
CORPUS_REGISTRY="${CORPUS_IMAGE%%/*}"
CORPUS_FORCE="${CORPUS_FORCE:-0}"   # 1 = re-pull and re-extract even if the trees are present

WEAVER_DIR="${WEAVER_DIR:-$CORPUS_DEST/weaver}"
RARPAR_DIR="${RARPAR_DIR:-$CORPUS_DEST/rarpar}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$CORPUS_DEST/rapidyenc}"
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$RAPIDYENC_ROOT/build/librapidyenc.so}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
RUST_TOOLCHAIN_FALLBACK="${RUST_TOOLCHAIN_FALLBACK:-1.97.1}"  # only if rust-toolchain.toml is absent
DOCKER=""   # resolved by resolve_docker()

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[bootstrap:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# ── CPU feature assertion (the precondition that makes this run meaningful) ──
# Same list the run script asserts. avx512vl is load-bearing twice over: it
# selects the VBMI2 decode kernel (simd/mod.rs:362-368) AND it is the feature
# that DISABLES weaver's own VPCLMUL CRC port (crc.rs:123) — the C1 proof.
REQUIRED_FEATURES="avx512f avx512bw avx512vl avx512vbmi avx512vbmi2 gfni vpclmulqdq vaes"
assert_cpu_features() {
  log "Asserting Zen 4 CPU features: ${REQUIRED_FEATURES}"
  [ -r /proc/cpuinfo ] || die "/proc/cpuinfo unreadable; cannot verify CPU (not Linux?)"
  local flags missing=""
  flags="$(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2- || true)"
  [ -n "$flags" ] || die "could not read CPU flags from /proc/cpuinfo"
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
     Launch an AWS c7a.* (AMD EPYC 4th gen 'Genoa') instance and re-run.
     (c6a/c5a/t3/m6i etc. will NOT have avx512vbmi2 + gfni on real silicon.)"
  fi
  log "CPU feature gate PASSED — real AVX-512 VBMI2 + GFNI present."
}

# ── System packages (Ubuntu 24.04) ──────────────────────────────────────────
# nasm + cmake are required by aws-lc-sys, which is in BOTH workspaces' build
# graphs (weaver via the TLS stack, rarpar via unrar-rs feature crypto-aws-lc).
# g++/cc build the §3a source-compiled rapidyenc oracle and rapidyenc itself.
# jq builds $RESULTS_DIR/summary.json + metadata.json from the criterion trees
# (doc §9g) — the raw material for later SVG generation.
install_system_deps() {
  log "Installing system build dependencies (apt)…"
  local pkgs="build-essential cmake nasm pkg-config git curl ca-certificates jq"
  if command -v apt-get >/dev/null 2>&1; then
    local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
    # shellcheck disable=SC2086
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $pkgs
  else
    warn "apt-get not found; ensure these are installed manually: $pkgs"
  fi
  for bin in cc c++ cmake nasm git curl jq tar; do
    command -v "$bin" >/dev/null 2>&1 || die "required tool '$bin' still missing after install"
  done
  log "System deps OK (cc, c++, cmake, nasm, git, curl, jq, tar present)."
}

# ── Corpus delivery: pre-pushed ECR image ────────────────────────────────────
#
# `.git` does NOT ship in the image, so every tree carries its own REVISION.json
# ({repo, rev, dirty_files, staged_at_utc}) and provenance is read from that
# rather than from git. weaver ships as WORKING-TREE state (uncommitted
# increments included), rarpar with its LFS fixtures already hydrated, and
# rapidyenc as a clean tree pinned at 27f435a — which is exactly why the image
# exists: a `git clone` on the box would miss the in-flight weaver work and turn
# rarpar's LFS fixtures into pointer files.
revision_field() {   # <tree-dir> <field> [default]
  local f="$1/REVISION.json" out=""
  if [ -f "$f" ] && command -v jq >/dev/null 2>&1; then
    out="$(jq -r --arg k "$2" '.[$k] // empty' "$f" 2>/dev/null || true)"
  fi
  printf '%s' "${out:-${3:-unknown}}"
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
  local want=""
  command -v docker >/dev/null 2>&1 || want="$want docker.io"
  command -v aws    >/dev/null 2>&1 || want="$want awscli"
  if [ -n "$want" ]; then
    log "Installing corpus-delivery tooling (apt):$want"
    if command -v apt-get >/dev/null 2>&1; then
      local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
      $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
      # shellcheck disable=SC2086
      $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $want
    else
      warn "apt-get not found; install manually:$want"
    fi
  else
    log "docker + aws CLI already present."
  fi
  command -v docker >/dev/null 2>&1 || die "docker still missing after install"
  command -v aws    >/dev/null 2>&1 || die "aws CLI still missing after install (needed for 'aws ecr get-login-password')"
  resolve_docker
}

corpus_is_extracted() {
  [ -f "$WEAVER_DIR/REVISION.json" ] &&
  [ -f "$RARPAR_DIR/REVISION.json" ] &&
  [ -f "$RAPIDYENC_ROOT/REVISION.json" ]
}

# Pull + extract WITHOUT ever running the image: `docker create` materializes a
# container's filesystem but starts nothing, `docker cp` reads it out, `docker
# rm` discards it. No process from the image is ever executed on this box.
fetch_corpus() {
  if corpus_is_extracted && [ "$CORPUS_FORCE" != "1" ]; then
    log "corpus already extracted under $CORPUS_DEST (set CORPUS_FORCE=1 to re-pull)."
    return 0
  fi

  install_ecr_tooling

  log "Logging in to $CORPUS_REGISTRY (region $CORPUS_REGION)…"
  # Needs ecr:GetAuthorizationToken on the instance role / env credentials.
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

  corpus_is_extracted || die "extraction finished but REVISION.json is missing from one or more trees
     expected: $WEAVER_DIR, $RARPAR_DIR, $RAPIDYENC_ROOT
     (is the image's /corpus layout what this runbook expects?)"

  local t
  for t in "$WEAVER_DIR" "$RARPAR_DIR" "$RAPIDYENC_ROOT"; do
    log "  $(revision_field "$t" repo "$(basename "$t")"): rev=$(revision_field "$t" rev) dirty_files=$(revision_field "$t" dirty_files 0) staged_at_utc=$(revision_field "$t" staged_at_utc)"
  done
  log "Corpus extracted."
}

# ── Rust toolchain via rustup (pinned by rust-toolchain.toml:2 -> 1.97.1) ────
install_rust() {
  if ! command -v rustup >/dev/null 2>&1; then
    log "Installing rustup…"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none
  else
    log "rustup already present."
  fi
  # shellcheck disable=SC1090,SC1091
  [ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
  export PATH="$HOME/.cargo/bin:$PATH"

  local channel="$RUST_TOOLCHAIN_FALLBACK"
  if [ -f "$WEAVER_DIR/rust-toolchain.toml" ]; then
    channel="$(grep -E '^\s*channel' "$WEAVER_DIR/rust-toolchain.toml" \
      | head -1 | sed -E 's/.*"([^"]+)".*/\1/' || echo "$RUST_TOOLCHAIN_FALLBACK")"
    log "weaver rust-toolchain.toml pins channel: $channel"
  else
    warn "no rust-toolchain.toml in $WEAVER_DIR; using fallback $channel"
  fi

  # rarpar is a separate workspace with its own pin. They agree today (both
  # 1.97.1); if they ever diverge, installing only weaver's would make the
  # rarpar phase silently rustup-download a second toolchain mid-run.
  if [ -f "$RARPAR_DIR/rust-toolchain.toml" ]; then
    local rarpar_channel
    rarpar_channel="$(grep -E '^\s*channel' "$RARPAR_DIR/rust-toolchain.toml" \
      | head -1 | sed -E 's/.*"([^"]+)".*/\1/' || echo "")"
    if [ -n "$rarpar_channel" ] && [ "$rarpar_channel" != "$channel" ]; then
      warn "rarpar pins $rarpar_channel but weaver pins $channel — installing both"
      rustup toolchain install "$rarpar_channel" --profile minimal || true
      rustup target add --toolchain "$rarpar_channel" "$TARGET" || true
    else
      log "rarpar rust-toolchain.toml pins the same channel: ${rarpar_channel:-<unreadable>}"
    fi
  fi

  rustup toolchain install "$channel" --profile minimal --component rustfmt clippy || true
  # Host target on this box IS x86_64-unknown-linux-gnu; add defensively (idempotent).
  rustup target add --toolchain "$channel" "$TARGET" || true
  log "Rust toolchain ready: $(rustup run "$channel" rustc --version 2>/dev/null || echo '?')"
}

# ── rapidyenc reference: source (for §3a diff test) + shared lib (for §3b bench)
build_rapidyenc() {
  # No clone: rapidyenc arrives pre-pinned in the corpus image (§7a), so there
  # is no network fetch and no revision drift between runs. crcutil-1.0/ is
  # vendored in-tree upstream (no .gitmodules), so nothing needs initializing
  # either — which is just as well, since .git does not ship.
  [ -d "$RAPIDYENC_ROOT" ] || die "rapidyenc tree missing at $RAPIDYENC_ROOT — re-extract the corpus image (CORPUS_FORCE=1)"
  log "rapidyenc revision (REVISION.json): $(revision_field "$RAPIDYENC_ROOT" rev)"
  log "  expected 27f435a = v1.1.1-10-g27f435a — the rev every prior weaver-vs-rapidyenc number was taken against"
  [ -d "$RAPIDYENC_ROOT/crcutil-1.0" ] || warn "crcutil-1.0/ absent from $RAPIDYENC_ROOT — the cmake build may fail"

  # Sanity: the source-compiled differential oracle needs these two files present
  # (tests/rapidyenc_decode_diff.rs:435).
  [ -f "$RAPIDYENC_ROOT/rapidyenc.cc" ]     || die "missing $RAPIDYENC_ROOT/rapidyenc.cc (bad checkout)"
  [ -f "$RAPIDYENC_ROOT/src/decoder.cc" ]   || die "missing $RAPIDYENC_ROOT/src/decoder.cc (bad checkout)"
  [ -f "$RAPIDYENC_ROOT/rapidyenc.h" ]      || die "missing $RAPIDYENC_ROOT/rapidyenc.h (bad checkout)"

  log "cmake-building rapidyenc (Release) for the parity bench…"
  cmake -S "$RAPIDYENC_ROOT" -B "$RAPIDYENC_ROOT/build" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$RAPIDYENC_ROOT/build" -j "$(nproc)"

  # Resolve the emitted shared lib (may be versioned, e.g. librapidyenc.so.1).
  local lib="$WEAVER_RAPIDYENC_LIB"
  if [ ! -e "$lib" ]; then
    lib="$(find "$RAPIDYENC_ROOT/build" -maxdepth 2 -name 'librapidyenc.so*' -type f 2>/dev/null | head -1 || true)"
  fi
  [ -n "$lib" ] && [ -e "$lib" ] || die "librapidyenc.so not found under $RAPIDYENC_ROOT/build after cmake build"
  log "rapidyenc shared lib: $lib"

  # Emit an env file the run script (and the user) can source.
  cat > "$RAPIDYENC_ROOT/weaver-bench.env" <<EOF
# sourced by ci/bench/c7a-run.sh — rapidyenc discovery for weaver-yenc
export RAPIDYENC_ROOT="$RAPIDYENC_ROOT"
export WEAVER_RAPIDYENC_LIB="$lib"
EOF
  log "Wrote $RAPIDYENC_ROOT/weaver-bench.env (RAPIDYENC_ROOT, WEAVER_RAPIDYENC_LIB)."
}

# ── rarpar presence (mandatory phase; warn here, hard-fail in c7a-run.sh) ────
check_rarpar() {
  if [ -d "$RARPAR_DIR" ]; then
    log "rarpar tree present at $RARPAR_DIR (rev $(revision_field "$RARPAR_DIR" rev))"
    # The archive_hotspots fixtures are git-LFS upstream. The image build
    # hydrates them, so this is a cheap check that the image really did — an
    # unhydrated fixture is a ~130-byte pointer file and the bench dies on a
    # malformed archive rather than on anything informative.
    local fx="$RARPAR_DIR/crates/weaver-unrar/tests/fixtures/rar5/rar5_lz.rar"
    if [ -f "$fx" ]; then
      if head -c 5 "$fx" | grep -q 'Rar!'; then
        log "  unrar fixtures hydrated (rar5_lz.rar has a real RAR signature)."
      else
        warn "  $fx is NOT a real RAR (git-LFS pointer?) — the corpus image was built without hydrated LFS; re-pull with CORPUS_FORCE=1, and if it persists the image needs rebuilding"
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

main() {
  log "CORPUS_IMAGE=$CORPUS_IMAGE"
  log "CORPUS_DEST=$CORPUS_DEST"
  log "WEAVER_DIR=$WEAVER_DIR"
  log "RARPAR_DIR=$RARPAR_DIR"
  log "RAPIDYENC_ROOT=$RAPIDYENC_ROOT"
  assert_cpu_features
  # jq first: fetch_corpus reads REVISION.json, and every later step wants it.
  install_system_deps
  fetch_corpus
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found even after corpus extraction (doc §7a)."
  install_rust
  build_rapidyenc
  check_rarpar

  printf '\n\033[1;32m'
  cat <<'BANNER'
============================================================
  c7a bootstrap READY
------------------------------------------------------------
  - dead-man shutdown armed (see DEADMAN_MINUTES)
  - CPU AVX-512 VBMI2 + GFNI feature gate: PASSED
  - system deps (cc/c++/cmake/nasm/jq): installed
  - corpus image: pulled + extracted (weaver/rarpar/rapidyenc)
  - rust toolchain (rust-toolchain.toml pin): installed
  - rapidyenc: pre-pinned tree + librapidyenc.so built
  - rarpar tree: see log above (MANDATORY for the run)
  Next:  ./ci/bench/c7a-run.sh
============================================================
BANNER
  printf '\033[0m\n'
}

main "$@"
