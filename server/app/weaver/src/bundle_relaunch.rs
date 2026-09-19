//! The one signal the supervised server sends its desktop wrapper.
//!
//! A macOS `.app` upgrade replaces the whole bundle — the wrapper binary
//! included — so the running wrapper cannot become the new build by restarting
//! itself the way a portable install does. It has to hand off to a fresh
//! instance launched from the replaced bundle, and the only process that knows
//! the swap succeeded is the server the wrapper started.
//!
//! The channel is that server's exit status, and nothing else. It needs no
//! authentication because it cannot be forged: the wrapper reads the status of
//! a child it spawned itself. It adds no listening surface, no file anything
//! else can write, and no new privilege — deliberately, because an upgrade
//! trigger is the last thing that should gain a network endpoint.
//!
//! Both `weaver` and `weaver-tray` compile this file, so the two halves of the
//! protocol cannot drift apart.

#![allow(
    dead_code,
    reason = "the server writes this code and the wrapper reads it; neither binary uses both halves"
)]

/// The exit code the supervised server uses to ask its wrapper to relaunch the
/// application after an in-place bundle upgrade.
///
/// Outside the range an ordinary failure produces: not `0`, not the `1`/`2` a
/// startup error exits with, and not the `101` a Rust panic uses.
pub(crate) const BUNDLE_RELAUNCH_EXIT_CODE: i32 = 87;

/// Whether an exited child asked for the application to be relaunched.
///
/// A child killed by signal 87 reports no exit code at all, so a signal can
/// never be mistaken for the request.
pub(crate) fn is_bundle_relaunch_exit(status: std::process::ExitStatus) -> bool {
    status.code() == Some(BUNDLE_RELAUNCH_EXIT_CODE)
}
