//! Replacing the running Weaver process with a fresh one.
//!
//! Every mechanism here runs AFTER the serve loop's graceful teardown: the
//! Unix path replaces the process image, which runs no destructors, and both
//! Windows paths hand the listening port to a process that is about to bind
//! it. The capability rules themselves live in `weaver-server-core` so the
//! REST and GraphQL surfaces answer from the same function this does.

use std::path::Path;

use tracing::{error, info};
use weaver_server_core::runtime::restart::{current_restart_capability, resolvable_executable};

/// Restart this process, or explain why the restart was refused.
///
/// The capability is settled again here rather than trusted from whenever the
/// button was rendered: the deployment cannot change under a running process,
/// but the program file can — an upgrade may have replaced or removed it.
pub(crate) fn restart_now() -> Result<(), String> {
    let capability = current_restart_capability();
    if !capability.supported {
        let reason = capability
            .reason
            .unwrap_or_else(|| "this deployment cannot restart Weaver".to_string());
        error!(reason, "refusing to restart Weaver");
        return Err(reason);
    }
    let executable = resolvable_executable().ok_or_else(|| {
        "Weaver cannot find its own program file, so it cannot start itself again".to_string()
    })?;

    info!(executable = %executable.display(), "restarting Weaver");
    restart_process(&executable)
}

/// Re-exec in place. The PID is preserved, so a systemd unit or a launchd job
/// sees no exit and cannot race a second copy into existence — which is why
/// this is the right primitive for every native Unix install, service-managed
/// or not. Returns only on failure.
#[cfg(unix)]
fn restart_process(executable: &Path) -> Result<(), String> {
    use std::os::unix::process::CommandExt;

    let mut arguments = std::env::args_os();
    // argv[0] is passed through verbatim: a process that renamed itself, or was
    // started through a symlink, must come back looking the same.
    let program_name = arguments
        .next()
        .unwrap_or_else(|| executable.as_os_str().to_os_string());
    let error = std::process::Command::new(executable)
        .arg0(program_name)
        .args(arguments)
        .exec();

    error!(%error, executable = %executable.display(), "failed to re-execute Weaver");
    Err(format!(
        "failed to re-execute {}: {error}",
        executable.display()
    ))
}

/// Windows has no exec. Either the tray supervises this process and owns the
/// relaunch, or nothing does and the process starts its own replacement.
#[cfg(windows)]
fn restart_process(executable: &Path) -> Result<(), String> {
    if post_tray_restart() {
        info!("handed the restart to the Weaver tray");
        return Ok(());
    }
    spawn_replacement(executable)
}

/// Ask the tray to restart the server it owns. False when no tray is running
/// in this session, or when the message could not be delivered — either way
/// the caller falls back to starting the replacement itself.
#[cfg(windows)]
fn post_tray_restart() -> bool {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::WindowsAndMessaging::{FindWindowW, PostMessageW};

    let class_name: Vec<u16> = std::ffi::OsStr::new(crate::tray_ipc::CLASS_NAME)
        .encode_wide()
        .chain(Some(0))
        .collect();
    // SAFETY: The class name is a valid nul-terminated UTF-16 string.
    let window = unsafe { FindWindowW(class_name.as_ptr(), std::ptr::null()) };
    if window.is_null() {
        return false;
    }
    // SAFETY: The target is a same-session Weaver tray window identified by its private class.
    if unsafe { PostMessageW(window, crate::tray_ipc::RESTART_MESSAGE, 0, 0) } == 0 {
        error!(
            error = %std::io::Error::last_os_error(),
            "failed to ask the Weaver tray to restart the server"
        );
        return false;
    }
    true
}

/// Start a detached replacement that outlives this process. The teardown that
/// ran before this released the listening socket, so the replacement can bind
/// the same port while this process is still exiting.
#[cfg(windows)]
fn spawn_replacement(executable: &Path) -> Result<(), String> {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};

    let child = std::process::Command::new(executable)
        .args(std::env::args_os().skip(1))
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .map_err(|error| format!("failed to start {}: {error}", executable.display()))?;

    info!(pid = child.id(), "started the replacement Weaver process");
    Ok(())
}
