# Contributing to Weaver

This file covers contributor setup notes that sit alongside [ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

- Rust (stable toolchain) + Cargo
- Any repo-specific external tools used by the `cargo xtask` command you are running

## Git Hooks

`gitleaks` is required for commits in this repo.

After cloning, run:

```bash
git config core.hooksPath .githooks
```

The versioned `pre-commit` hook will block commits when `gitleaks` reports staged secrets or when staged diffs contain machine-local usernames or home-directory paths.

## macOS Privacy & Security

If `cargo build`, `cargo xtask`, or other Rust commands stall around `build-script-build`, macOS is likely blocking newly compiled local binaries from your terminal app.

Enable your terminal under `System Settings -> Privacy & Security -> Developer Tools`, then fully quit and reopen it.

## Repo Automation

Use `cargo xtask` as the canonical interface for repo automation. See [ARCHITECTURE.md](ARCHITECTURE.md) for the architectural rules and command surface.
