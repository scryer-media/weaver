#!/usr/bin/env bash
#
# release.sh — pre-release validation and tagging script
#
# Validates: web lint/build and Rust fmt/audit/test/clippy in parallel
# Then:      bumps workspace version · signed tag · push
#
# Usage:
#   ./scripts/release.sh              # auto-increment patch (0.1.0 → 0.1.1)
#   ./scripts/release.sh --minor      # increment minor
#   ./scripts/release.sh --major      # increment major
#   ./scripts/release.sh 0.2.0        # explicit version
#   ./scripts/release.sh --dry-run    # validate only, no commit/tag/push
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$REPO_ROOT/apps/weaver-web"

# ── Colors ─────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'

step() { echo -e "\n${BLUE}${BOLD}▶  $*${RESET}"; }
ok()   { echo -e "   ${GREEN}✓  $*${RESET}"; }
warn() { echo -e "   ${YELLOW}⚠  $*${RESET}"; }
die()  { echo -e "\n${RED}${BOLD}✗  $*${RESET}" >&2; exit 1; }

# ── Argument parsing ───────────────────────────────────────────────────────────
BUMP="patch"
EXPLICIT_VERSION=""
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --major)   BUMP="major" ;;
        --minor)   BUMP="minor" ;;
        --patch)   BUMP="patch" ;;
        --dry-run) DRY_RUN=true ;;
        v[0-9]*.[0-9]*.[0-9]*) EXPLICIT_VERSION="${arg#v}" ;;
        [0-9]*.[0-9]*.[0-9]*)  EXPLICIT_VERSION="$arg" ;;
        *) die "Unknown argument: $arg" ;;
    esac
done

# ── Determine next version ─────────────────────────────────────────────────────
step "Determining next version"

cd "$REPO_ROOT"

# Look for weaver-v* tags first, fall back to bare semver tags (legacy).
LATEST_TAG="$(git tag --sort=-version:refname | grep '^weaver-v' | head -1 || true)"
if [[ -z "$LATEST_TAG" ]]; then
    LATEST_TAG="$(git tag --sort=-version:refname | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | head -1 || true)"
fi
CURRENT_VERSION="${LATEST_TAG#weaver-v}"
CURRENT_VERSION="${CURRENT_VERSION:-0.0.0}"

if [[ -n "$EXPLICIT_VERSION" ]]; then
    NEXT_VERSION="$EXPLICIT_VERSION"
else
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"
    case "$BUMP" in
        major) NEXT_VERSION="$((MAJOR + 1)).0.0" ;;
        minor) NEXT_VERSION="${MAJOR}.$((MINOR + 1)).0" ;;
        patch) NEXT_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
    esac
fi

TAG_NAME="weaver-v${NEXT_VERSION}"

echo "   Latest tag : ${LATEST_TAG:-none}"
echo "   Next tag   : ${TAG_NAME}"
$DRY_RUN && echo -e "   ${YELLOW}(dry run — no commits, tags, or pushes)${RESET}"

# ── Pre-flight checks ──────────────────────────────────────────────────────────
step "Pre-flight checks"

if git tag | grep -qx "$TAG_NAME"; then
    die "Tag $TAG_NAME already exists"
fi

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "   Branch : $BRANCH"

if [[ -n "$(git status --porcelain)" ]]; then
    warn "Working tree has uncommitted changes:"
    git status --short | sed 's/^/     /'
    echo ""
    read -r -p "   Continue anyway? [y/N] " REPLY
    [[ "$REPLY" =~ ^[Yy]$ ]] || die "Aborted"
fi

ok "Pre-flight OK"

# ── Validation functions ──────────────────────────────────────────────────────

run_web_validation() {
    step "Running npm audit fix"

    cd "$WEB_DIR"
    npm audit fix 2>&1
    ok "npm audit fix complete"

    step "Running TypeScript + ESLint"

    npm run lint 2>&1 || die "Web lint failed — fix before releasing"

    ok "Web lint passed"

    step "Running web build"

    npm run build 2>&1 || die "Web build failed — fix before releasing"

    ok "Web build passed"
}

run_rust_validation() {
    step "Running cargo fmt --all --check"

    cd "$REPO_ROOT"
    cargo fmt --all --check 2>&1 || die "Rust formatting drift detected — fix before releasing"

    ok "cargo fmt passed"

    step "Updating Cargo.lock (cargo update)"

    cargo update 2>&1
    ok "Cargo.lock updated"

    step "Running cargo audit"

    if ! command -v cargo-audit &>/dev/null; then
        warn "cargo-audit not installed — installing"
        cargo install --locked cargo-audit 2>&1 || die "failed to install cargo-audit"
    fi

    CARGO_AUDIT_IGNORES=()
    # Add advisory IDs here as needed, e.g.:
    # CARGO_AUDIT_IGNORES+=("RUSTSEC-2099-0001")

    if [[ ${#CARGO_AUDIT_IGNORES[@]} -gt 0 ]]; then
        warn "Ignoring advisories pending upstream fixes: ${CARGO_AUDIT_IGNORES[*]}"
    fi

    CARGO_AUDIT_ARGS=()
    for advisory in "${CARGO_AUDIT_IGNORES[@]+"${CARGO_AUDIT_IGNORES[@]}"}"; do
        CARGO_AUDIT_ARGS+=(--ignore "$advisory")
    done

    cargo audit ${CARGO_AUDIT_ARGS[@]+"${CARGO_AUDIT_ARGS[@]}"} 2>&1 || die "cargo audit found vulnerabilities — fix before releasing"
    ok "cargo audit passed"

    step "Running Rust tests (cargo nextest run --workspace --locked)"

    if ! command -v cargo-nextest &>/dev/null; then
        warn "cargo-nextest not installed — installing"
        cargo install --locked cargo-nextest 2>&1 || die "failed to install cargo-nextest"
    fi

    cargo nextest run --workspace --locked 2>&1 || die "Rust tests failed — fix before releasing"

    ok "Rust tests passed"

    step "Running cargo clippy (linux ci target)"

    "$REPO_ROOT/scripts/clippy-ci.sh" --linux-only 2>&1 || die "Clippy errors — fix before releasing"

    ok "Clippy passed"
}

# ── Run validation in parallel ────────────────────────────────────────────────
step "Running web and Rust validation in parallel"

(
    exec > >(sed 's/^/[web] /') 2>&1
    run_web_validation
) &
WEB_VALIDATION_PID=$!

(
    exec > >(sed 's/^/[rust] /') 2>&1
    run_rust_validation
) &
RUST_VALIDATION_PID=$!

VALIDATION_FAILED=false

if ! wait "$WEB_VALIDATION_PID"; then
    VALIDATION_FAILED=true
    warn "Web validation failed"
fi

if ! wait "$RUST_VALIDATION_PID"; then
    VALIDATION_FAILED=true
    warn "Rust validation failed"
fi

if [[ "$VALIDATION_FAILED" == true ]]; then
    die "Validation failed — fix before releasing"
fi

ok "Parallel validation passed"

# ── Bump workspace version ────────────────────────────────────────────────────
step "Updating workspace version to $NEXT_VERSION"

cd "$REPO_ROOT"

# Weaver uses workspace.package.version — one place to update.
sed -i '' 's/^version = "[^"]*"/version = "'"$NEXT_VERSION"'"/' "$REPO_ROOT/Cargo.toml"

WRITTEN_VERSION="$(grep -m1 '^version = ' "$REPO_ROOT/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')"
[[ "$WRITTEN_VERSION" == "$NEXT_VERSION" ]] \
    || die "Version write failed — Cargo.toml shows: $WRITTEN_VERSION"

ok "Workspace version updated to $NEXT_VERSION"

# ── Verify build after bump ────────────────────────────────────────────────────
step "Running cargo check after version bump"

cargo check 2>&1 || die "cargo check failed after version bump"

ok "cargo check passed"

# ── From here on nothing destructive happens in dry-run mode ──────────────────
if $DRY_RUN; then
    echo ""
    echo -e "${YELLOW}${BOLD}Dry run complete — stopping before commit/tag/push.${RESET}"
    echo -e "  Version $NEXT_VERSION validated OK."
    git checkout -- "$REPO_ROOT/Cargo.toml"
    git checkout -- "$REPO_ROOT/Cargo.lock" 2>/dev/null || true
    git checkout -- "$WEB_DIR/package-lock.json" 2>/dev/null || true
    exit 0
fi

# ── Commit version bump ────────────────────────────────────────────────────────
step "Committing version bump"

CHANGED_FILES=()
CARGO_TOML="$REPO_ROOT/Cargo.toml"
[[ -n "$(git diff --name-only "$CARGO_TOML")" ]] && CHANGED_FILES+=("$CARGO_TOML")
CARGO_LOCK="$REPO_ROOT/Cargo.lock"
[[ -n "$(git diff --name-only "$CARGO_LOCK")" ]] && CHANGED_FILES+=("$CARGO_LOCK")
NPM_LOCK="$WEB_DIR/package-lock.json"
[[ -n "$(git diff --name-only "$NPM_LOCK")" ]] && CHANGED_FILES+=("$NPM_LOCK")

if [[ ${#CHANGED_FILES[@]} -gt 0 ]]; then
    git add "${CHANGED_FILES[@]}"
    git commit -m "release: bump weaver to $NEXT_VERSION"
    ok "Committed: ${CHANGED_FILES[*]##*/}"
else
    ok "Nothing to commit"
fi

# ── Create signed tag ──────────────────────────────────────────────────────────
step "Creating signed tag $TAG_NAME"

git tag -s "$TAG_NAME" -m "Release $TAG_NAME"
ok "Tag $TAG_NAME created"

# ── Push ───────────────────────────────────────────────────────────────────────
step "Pushing to origin"

git push origin "$BRANCH"
git push origin "$TAG_NAME"
ok "Pushed $BRANCH and tag $TAG_NAME"

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Released $TAG_NAME${RESET}"
echo ""
echo -e "${YELLOW}${BOLD}Reminder:${RESET} Scryer depends on weaver-rar."
echo -e "   Update the weaver tag reference in Scryer's Cargo.toml to ${BOLD}${TAG_NAME}${RESET}"
