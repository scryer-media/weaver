//! Minidump capture for Windows structured exceptions.
//!
//! The panic hook covers Rust panics only. A structured exception — an access
//! violation, a stack overflow, a fault raised inside a native archive or
//! crypto library — never reaches it: the OS terminates the process straight
//! away, so the log ends mid-line with nothing to say why. Installing a
//! top-level exception filter buys one artefact from that moment: a minidump
//! with the faulting thread's stack and the modules loaded, which a debugger
//! can open later against the matching build.

use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::OnceLock;

use tracing::{error, warn};
use windows_sys::Win32::Foundation::{CloseHandle, GENERIC_WRITE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_ALWAYS, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ,
};
use windows_sys::Win32::System::Diagnostics::Debug::{
    EXCEPTION_CONTINUE_SEARCH, EXCEPTION_POINTERS, MINIDUMP_EXCEPTION_INFORMATION,
    MiniDumpWithDataSegs, MiniDumpWithIndirectlyReferencedMemory, MiniDumpWriteDump,
    SetUnhandledExceptionFilter,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentProcessId, GetCurrentThreadId,
};

/// The dump destination for this process, resolved once at install time.
///
/// Everything the filter needs is precomputed here: by the time it runs the
/// process is already faulting, possibly on a thread whose stack is exhausted
/// and with an allocator in an unknown state, so it must not format a path,
/// consult the clock, or encode a string. The wide form is what `CreateFileW`
/// takes; the display form is only for the log line.
static DUMP_TARGET: OnceLock<DumpTarget> = OnceLock::new();

struct DumpTarget {
    /// NUL-terminated UTF-16, ready to hand to `CreateFileW` as-is.
    wide_path: Vec<u16>,
    display_path: String,
}

/// Installs the top-level exception filter, writing dumps next to the log
/// files so a user collecting logs picks the dump up with them.
///
/// The name carries the process start time and id rather than the crash time:
/// the filter cannot safely format a timestamp, and one process can crash at
/// most once, so a per-run name is already unique.
pub(crate) fn install_unhandled_exception_filter() {
    let Some(directory) = dump_directory() else {
        warn!("no writable directory for crash dumps; unhandled exceptions will not be captured");
        return;
    };
    let path = directory.join(format!(
        "weaver-crash-{}-{}.dmp",
        chrono::Local::now().format("%Y%m%d-%H%M%S"),
        std::process::id()
    ));
    let target = DumpTarget {
        wide_path: std::ffi::OsStr::new(path.as_os_str())
            .encode_wide()
            .chain(std::iter::once(0))
            .collect(),
        display_path: path.display().to_string(),
    };
    if DUMP_TARGET.set(target).is_err() {
        // Already installed; a second filter would only overwrite the first.
        return;
    }
    // SAFETY: the filter is a plain `extern "system"` function with static
    // lifetime and no captured state.
    unsafe { SetUnhandledExceptionFilter(Some(write_minidump)) };
}

/// The directory the log files live in, falling back to the system temporary
/// directory when this process writes no log file.
fn dump_directory() -> Option<PathBuf> {
    let log_dir = weaver_server_core::runtime::log_buffer::log_file_path()
        .and_then(|path| path.parent())
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(std::path::Path::to_path_buf);
    log_dir.or_else(|| {
        let temp = std::env::temp_dir();
        (!temp.as_os_str().is_empty()).then_some(temp)
    })
}

/// The top-level exception filter itself.
unsafe extern "system" fn write_minidump(exception_info: *const EXCEPTION_POINTERS) -> i32 {
    if let Some(target) = DUMP_TARGET.get() {
        // Emitted before the dump, not after: writing the dump is the part
        // that can hang or fault, and the path is worth having either way.
        error!(
            dump_path = target.display_path.as_str(),
            "unhandled exception; writing minidump"
        );
        // SAFETY: `exception_info` is the pointer the OS handed this filter,
        // valid for the duration of the call, and the target was built once at
        // install time and is never mutated.
        unsafe { write_dump(target, exception_info) };
    }
    // Continue the search rather than handling the exception: returning
    // `EXCEPTION_EXECUTE_HANDLER` would swallow the fault and let the process
    // unwind to a normal exit, which changes the exit code the supervisor sees
    // and hides the crash from Windows Error Reporting. Continuing leaves the
    // process dying exactly as it does today; the dump is purely additive.
    EXCEPTION_CONTINUE_SEARCH
}

/// Writes the dump. Every failure is swallowed: the process is already dying
/// and there is nothing useful left to do about a failed diagnostic.
unsafe fn write_dump(target: &DumpTarget, exception_info: *const EXCEPTION_POINTERS) {
    // SAFETY: `wide_path` is NUL-terminated UTF-16 built at install time.
    let file = unsafe {
        CreateFileW(
            target.wide_path.as_ptr(),
            GENERIC_WRITE,
            FILE_SHARE_READ,
            std::ptr::null(),
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if file == INVALID_HANDLE_VALUE {
        return;
    }

    let exception = MINIDUMP_EXCEPTION_INFORMATION {
        // SAFETY: no preconditions; reports the faulting thread.
        ThreadId: unsafe { GetCurrentThreadId() },
        ExceptionPointers: exception_info.cast_mut(),
        // The pointers belong to this process, not to a client one.
        ClientPointers: 0,
    };
    // A deliberately modest dump: the faulting thread's stacks plus the memory
    // they point at and the loaded modules' data segments. Enough to read a
    // stack trace and the globals around it, while staying small enough that a
    // user can attach it — a full-memory dump of this process would be gigabytes.
    let dump_type = MiniDumpWithIndirectlyReferencedMemory | MiniDumpWithDataSegs;
    // SAFETY: the handle is open and writable, and the exception record lives
    // until this call returns.
    let _ = unsafe {
        MiniDumpWriteDump(
            GetCurrentProcess(),
            GetCurrentProcessId(),
            file,
            dump_type,
            &exception,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    // SAFETY: `file` came from `CreateFileW` above and is not used afterwards.
    unsafe { CloseHandle(file) };
}
