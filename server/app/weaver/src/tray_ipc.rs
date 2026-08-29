//! The private window class and messages the Windows tray listens on.
//!
//! Compiled into both `weaver.exe` and `weaver-tray.exe`, which must agree on
//! every value here or the server cannot reach its own supervisor. Each binary
//! uses a different subset of the contract.
#![allow(
    dead_code,
    reason = "weaver.exe and weaver-tray.exe each use a subset of this shared contract"
)]

use windows_sys::Win32::UI::WindowsAndMessaging::WM_APP;

/// Registered by the tray, and the only handle another process has on it.
/// `FindWindowW` is session-local, so a lookup by this name finds the tray
/// belonging to the same signed-in user and no other.
pub(crate) const CLASS_NAME: &str = "ScryerMedia.Weaver.Desktop.v1.Tray";

/// Shell notification-icon callback.
pub(crate) const TRAY_CALLBACK_MESSAGE: u32 = WM_APP + 1;
/// A second tray invocation asking the owning instance to open the UI.
pub(crate) const OPEN_WINDOW_MESSAGE: u32 = WM_APP + 2;
/// `--shutdown` asking the owning instance to exit.
pub(crate) const SHUTDOWN_MESSAGE: u32 = WM_APP + 3;
/// Posted by the server once its own graceful teardown is complete. The tray
/// owns the relaunch because it owns the server process as a child; a server
/// that started its own replacement would leave the tray supervising a
/// process it did not start.
pub(crate) const RESTART_MESSAGE: u32 = WM_APP + 4;
