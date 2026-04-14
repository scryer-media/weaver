# Contributing to Weaver

This file covers contributor setup notes that sit alongside [ARCHITECTURE.md](/Users/jeremy/dev/scryer-media/weaver/ARCHITECTURE.md).

## Prerequisites

- Rust (stable toolchain) + Cargo
- Any repo-specific external tools used by the `cargo xtask` command you are running

## macOS Privacy & Security

If `cargo build`, `cargo xtask`, or other Rust commands stall around `build-script-build`, macOS is likely blocking newly compiled local binaries from your terminal app.

Enable your terminal under `System Settings -> Privacy & Security -> Developer Tools`, then fully quit and reopen it.

`spctl developer-mode enable-terminal` only helps `Terminal.app`. If you use Ghostty, iTerm, WezTerm, or another terminal, you must allow that specific app in the Developer Tools list.

## Repo Automation

Use `cargo xtask` as the canonical interface for repo automation. See [ARCHITECTURE.md](/Users/jeremy/dev/scryer-media/weaver/ARCHITECTURE.md) for the architectural rules and command surface.
