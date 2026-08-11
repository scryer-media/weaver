#!/usr/bin/env bash
# c7a-bootstrap.sh — idempotent one-shot setup for the weaver-yenc AVX-512/VBMI2
# differential + parity bench (and the rarpar GFNI/AVX-512 GF16 phase) on an AWS
# c7a.xlarge (AMD Zen 4) Ubuntu 24.04 box.
#
# Installs: system build deps, rustup + the repo-pinned toolchain (1.97.1 via
# rust-toolchain.toml), then clones + cmake-builds the rapidyenc reference, and
# asserts the CPU actually exposes the AVX-512/VBMI2/GFNI feature set. Prints a
# READY banner on success. Safe to re-run.
#
# Makes NO AWS API calls. Provisioning and teardown are the operator's, by hand
# (see ci/bench/c7a-avx512-diffbench.md §11).
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
WEAVER_DIR="${WEAVER_DIR:-$HOME/weaver}"
RARPAR_DIR="${RARPAR_DIR:-$HOME/rarpar}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$HOME/rapidyenc}"
RAPIDYENC_GIT="${RAPIDYENC_GIT:-https://github.com/animetosho/rapidyenc.git}"
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$RAPIDYENC_ROOT/build/librapidyenc.so}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
RUST_TOOLCHAIN_FALLBACK="${RUST_TOOLCHAIN_FALLBACK:-1.97.1}"  # only if rust-toolchain.toml is absent

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
install_system_deps() {
  log "Installing system build dependencies (apt)…"
  local pkgs="build-essential cmake nasm pkg-config git curl ca-certificates"
  if command -v apt-get >/dev/null 2>&1; then
    local SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get update -y
    # shellcheck disable=SC2086
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $pkgs
  else
    warn "apt-get not found; ensure these are installed manually: $pkgs"
  fi
  for bin in cc c++ cmake nasm git curl; do
    command -v "$bin" >/dev/null 2>&1 || die "required tool '$bin' still missing after install"
  done
  log "System deps OK (cc, c++, cmake, nasm, git, curl present)."
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
  if [ -d "$RAPIDYENC_ROOT/.git" ]; then
    log "rapidyenc checkout already at $RAPIDYENC_ROOT (leaving revision as-is)."
  else
    log "Cloning rapidyenc -> $RAPIDYENC_ROOT"
    git clone "$RAPIDYENC_GIT" "$RAPIDYENC_ROOT"
  fi
  ( cd "$RAPIDYENC_ROOT" && git submodule update --init --recursive || \
      warn "git submodule update failed (crcutil may be vendored already; continuing)" )
  log "rapidyenc revision: $(git -C "$RAPIDYENC_ROOT" describe --tags --always 2>/dev/null || echo '?')"
  log "  (dev-mac reference numbers were taken against 27f435a = v1.1.1-10-g27f435a)"

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
    log "rarpar checkout present at $RARPAR_DIR"
    # The archive_hotspots fixtures are git-LFS. rsync from the dev mac carries
    # hydrated bytes; a plain `git clone` leaves ~130-byte pointer files and the
    # bench dies on a malformed archive. Cheap smoke test.
    local fx="$RARPAR_DIR/crates/weaver-unrar/tests/fixtures/rar5/rar5_lz.rar"
    if [ -f "$fx" ]; then
      if head -c 5 "$fx" | grep -q 'Rar!'; then
        log "  unrar fixtures look hydrated (rar5_lz.rar has a real RAR signature)."
      else
        warn "  $fx is NOT a real RAR (git-LFS pointer?) — rsync rarpar from the dev mac, do not git clone"
      fi
    else
      warn "  $fx missing — the archive_hotspots bench will fail"
    fi
  else
    warn "rarpar checkout NOT found at $RARPAR_DIR.
     The rarpar phase is MANDATORY and c7a-run.sh will ABORT without it.
     rsync it from the dev machine before running (doc §7a) — from inside your
     weaver checkout, with RARPAR_LOCAL defaulting to a sibling checkout:
       RARPAR_LOCAL=\"\${RARPAR_LOCAL:-\$(git rev-parse --show-toplevel)/../rarpar}\"
       rsync -az --exclude 'target/' --exclude '.git/' \\
         \"\$RARPAR_LOCAL/\" \"\$BOX:~/rarpar/\"
     rsync, not git clone: the unrar bench fixtures are git-LFS."
  fi
}

main() {
  log "WEAVER_DIR=$WEAVER_DIR"
  log "RARPAR_DIR=$RARPAR_DIR"
  log "RAPIDYENC_ROOT=$RAPIDYENC_ROOT"
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found — rsync weaver there first (doc §7a)."
  assert_cpu_features
  install_system_deps
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
  - system deps (cc/c++/cmake/nasm): installed
  - rust toolchain (rust-toolchain.toml pin): installed
  - rapidyenc: source tree + librapidyenc.so built
  - rarpar checkout: see log above (MANDATORY for the run)
  Next:  ./ci/bench/c7a-run.sh
============================================================
BANNER
  printf '\033[0m\n'
}

main "$@"
