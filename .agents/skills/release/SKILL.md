---
name: release
description: Release a new weaver version. Runs the full validation suite, bumps the workspace version, prunes old GitHub releases, and creates/pushes a signed `weaver-v<version>` tag. Use when asked to "release", "tag", "cut a release", or "bump" weaver.
---

# Weaver Release

Releases are driven entirely by `cargo xtask release`. Never bump the workspace version, create tags, or push release commits by hand.

If you need code discovery while debugging a release issue, use the shared `claude-context` MCP bridge first (`http://127.0.0.1:8765/mcp`) and only use shell search afterward for exact follow-up confirmation.

## When to activate

- User asks to release, tag, bump, or cut a release for weaver
- User asks you to ship a fix/feature to weaver
- User says "weaver-v<x.y.z>" without further context

## Invocation

From the weaver repo root:

```bash
cargo xtask release              # patch bump (default)
cargo xtask release --minor
cargo xtask release --major
cargo xtask release 0.2.7        # explicit version
cargo xtask release --dry-run    # full release rehearsal without version bump/commit/tag/push
```

The `scripts/release.sh` wrapper simply `exec`s `cargo xtask release` — call the xtask command directly.

## Pre-flight the task enforces

1. Computes next version from the latest `weaver-v*` tag.
2. Aborts if the computed tag already exists.
3. Records the current branch and prompts if the working tree is dirty.
4. Requires `gh` in PATH (used for GitHub release pruning).

## What the task does

1. Runs the full mutating release-validation stack in parallel, including web `npm audit fix`/lint/build and Rust `cargo fmt --all`, `cargo update`, audit, tests, and clippy.
2. On `--dry-run`: does **not** bump `[workspace.package].version`, restores tracked files changed by release validation before exit, and writes a reusable marker to `tmp/xtask-release-dry-run.json`.
3. On a later real `cargo xtask release`, if the worktree is clean and the dry-run marker still matches the current commit, release args, and next-tag math, the task may skip re-running validation and go straight to version bump, commit, signed tag, push, and release pruning.
4. Without a valid matching dry-run marker: bumps `[workspace.package].version` in the root `Cargo.toml`, re-runs `cargo check`, then commits `release: bump weaver to <version>`, prunes old GitHub releases and GHCR images (keeps 4 most recent), creates a **signed** `weaver-v<version>` tag, and pushes the branch and tag to `origin`.

## Pre-release expectations you are responsible for

- Working tree is clean, or any dirty files are intentional and about to be committed.
- The release is a meaningful unit — don't release just to bump a number.

## Runtime expectations

- Release takes several minutes. Stream its output to a file or background process so your shell/timeout doesn't kill it partway through.
- Pass `--locked` to any Cargo invocations you run alongside release work; the release task already does this internally.
- Always run `cargo xtask release --dry-run` first. Only move on to the real release command after a successful dry run on the same clean commit.

## Failure handling

If validation fails, read the failure reason, fix it at its root cause, commit the fix, and rerun `cargo xtask release` with the same args. Cap this loop at ~3 attempts — if you can't get it green, stop and report what's failing.

If a real release says it is skipping dry-run cache reuse, trust that signal. The usual causes are a dirty worktree, a changed commit, different release args, or a newer release tag changing the computed next version/tag.

Do not:
- Skip validation steps or pass flags that weaken them.
- Amend or rewrite the version-bump commit.
- Manually create the tag or push if the task failed.
- **Bypass the 1Password SSH commit/tag signer.** `git tag -s` in the release task uses `op-ssh-sign`. If signing fails because 1Password is locked (symptoms: `agent refused operation`, `gpg failed to sign`, Touch ID prompt times out), STOP and ask the user to unlock 1Password. Do not pass `--no-gpg-sign`, `-c tag.gpgsign=false`, edit git config to disable signing, or substitute a different signer. An unsigned tag is a release bug.

## Post-release

1. Tag format: `weaver-v<version>` (e.g. `weaver-v0.2.7`).
2. The task prints a reminder that weaver consumers (notably scryer's `crates/scryer-application/Cargo.toml`) may need their `weaver-*` tag references updated. If the user intends scryer to pick up this weaver release, update those tag refs in a scryer branch and run `cargo update -p weaver-rar` (or the relevant crate) there.
3. Watch the GitHub Actions run triggered by the tag (`gh run list --limit 1`, `gh run watch <id>`). Report the CI status when it completes.
4. If CI fails after the push, do not delete the tag or force-push — investigate the root cause and report.
