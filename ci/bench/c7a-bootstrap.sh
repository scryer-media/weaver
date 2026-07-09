#!/usr/bin/env bash
# c7a-bootstrap.sh — idempotent one-shot setup for the weaver-yenc AVX-512/VBMI2
# differential + parity bench on an AWS c7a.xlarge (AMD Zen 4) Ubuntu 24.04 box.
#
# Installs: system build deps, rustup + the repo-pinned toolchain (1.96 via
# rust-toolchain.toml), then clones + cmake-builds the rapidyenc reference, and
# asserts the CPU actually exposes the AVX-512/VBMI2/GFNI feature set. Prints a
# READY banner on success. Safe to re-run.
#
# Grounding (see ci/bench/c7a-avx512-diffbench.md):
#   - toolchain 1.96            : rust-toolchain.toml:2
#   - nasm required (aws-lc-sys): .github/workflows/deploy.yml:130-133, 229-232
#   - rapidyenc shared lib      : supporting-codebases/rapidyenc/CMakeLists.txt:11,269,271
#   - RAPIDYENC_ROOT (src diff) : engines/weaver-yenc/tests/rapidyenc_decode_diff.rs:350-353
#   - WEAVER_RAPIDYENC_LIB (bench dlopen): engines/weaver-yenc/benches/rapidyenc_parity.rs:34
#
# Does NOT run cargo test/bench — that is c7a-run.sh's job.
set -euo pipefail

# ── Config (all overridable) ─────────────────────────────────────────────────
WEAVER_DIR="${WEAVER_DIR:-$HOME/weaver}"
RAPIDYENC_ROOT="${RAPIDYENC_ROOT:-$HOME/rapidyenc}"
RAPIDYENC_GIT="${RAPIDYENC_GIT:-https://github.com/animetosho/rapidyenc.git}"
WEAVER_RAPIDYENC_LIB="${WEAVER_RAPIDYENC_LIB:-$RAPIDYENC_ROOT/build/librapidyenc.so}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
RUST_TOOLCHAIN_FALLBACK="1.96"   # only used if rust-toolchain.toml is absent

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[bootstrap:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# ── CPU feature assertion (the precondition that makes this run meaningful) ──
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

# ── Rust toolchain via rustup (pinned by rust-toolchain.toml:2 -> 1.96) ──────
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
    log "rust-toolchain.toml pins channel: $channel"
  else
    warn "no rust-toolchain.toml in $WEAVER_DIR; using fallback $channel"
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

  # Sanity: the source-compiled differential oracle needs these two files present
  # (tests/rapidyenc_decode_diff.rs:353).
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

main() {
  log "WEAVER_DIR=$WEAVER_DIR"
  log "RAPIDYENC_ROOT=$RAPIDYENC_ROOT"
  [ -d "$WEAVER_DIR" ] || die "WEAVER_DIR '$WEAVER_DIR' not found — clone weaver there first."
  assert_cpu_features
  install_system_deps
  install_rust
  build_rapidyenc

  printf '\n\033[1;32m'
  cat <<'BANNER'
============================================================
  c7a bootstrap READY
------------------------------------------------------------
  - CPU AVX-512 VBMI2 + GFNI feature gate: PASSED
  - system deps (cc/c++/cmake/nasm): installed
  - rust toolchain (rust-toolchain.toml pin): installed
  - rapidyenc: source tree + librapidyenc.so built
  Next:  ./ci/bench/c7a-run.sh
============================================================
BANNER
  printf '\033[0m\n'
}

main "$@"
