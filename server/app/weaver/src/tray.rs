//! `weaver-tray`: the desktop wrapper around the Weaver server.
//!
//! The wrapper is not a second implementation of Weaver. It starts and stops
//! the same `weaver` binary that ships beside it, and shows the same web UI a
//! browser would — the app window and the browser can be pointed at the same
//! running server at the same time. Everything portable about that lives in
//! [`shared`]; the platform modules own only the window, the menu, and the
//! system integration each platform requires.
#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
#[path = "tray_ipc.rs"]
mod tray_ipc;

#[path = "tray/shared.rs"]
mod shared;

#[cfg(windows)]
#[path = "tray/windows.rs"]
mod windows;

#[cfg(target_os = "macos")]
#[path = "tray/macos.rs"]
mod macos;

#[cfg(windows)]
fn main() {
    if let Err(error) = windows::run() {
        windows::show_error("Weaver", &error);
        std::process::exit(1);
    }
}

#[cfg(target_os = "macos")]
fn main() {
    if let Err(error) = macos::run() {
        macos::show_error("Weaver", &error);
        std::process::exit(1);
    }
}

// Linux and the BSDs run Weaver as a service, not as a desktop app, so the
// binary still builds there — the package ships one set of binaries — but it
// has nothing to do.
#[cfg(not(any(windows, target_os = "macos")))]
fn main() {
    eprintln!("weaver-tray is only supported on Windows and macOS");
    std::process::exit(1);
}
