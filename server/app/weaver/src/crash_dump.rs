//! Minidump capture for Windows structured exceptions.
//!
//! The panic hook covers Rust panics only. A structured exception — an access
//! violation, a stack overflow, a fault raised inside a native archive or
//! crypto library — never reaches it: the OS terminates the process, and the
//! log ends mid-line with nothing to say why. Installing a top-level exception
//! filter buys two artefacts from that moment: a minidump with the faulting
//! thread's stack and the modules loaded, which a debugger can open later
//! against the matching build, and one line in the log naming the exception
//! and where the dump went.
//!
//! The filter is not a catch-all. A Rust panic under `panic = "abort"`, an
//! allocation failure and `std::process::abort` all end in a fast-fail, which
//! the kernel terminates without consulting user-mode exception filters. Those
//! paths write their last words to stderr first, so the desktop wrapper keeps
//! the server's stderr on disk for them. A stack overflow does arrive here:
//! the runtime's vectored handler reports it on stderr and passes it on, on a
//! thread with a few kilobytes of stack left. That is why the dump is written
//! from a fresh thread rather than the faulting one.
//!
//! Everything the filter touches is prepared at install time. By the time it
//! runs the process is faulting, possibly with the allocator's locks held by
//! the faulting thread, so the filter neither allocates nor takes the tracing
//! subscriber's log lock: the log line is assembled in a stack buffer and
//! appended through a second handle on the log file.

use std::ffi::c_void;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, Ordering};

use tracing::{info, warn};
use windows_sys::Win32::Foundation::{
    CloseHandle, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, SYSTEMTIME, WAIT_OBJECT_0,
};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_ALWAYS, CreateFileW, FILE_APPEND_DATA, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_ALWAYS, WriteFile,
};
use windows_sys::Win32::System::Diagnostics::Debug::{
    EXCEPTION_CONTINUE_SEARCH, EXCEPTION_POINTERS, MINIDUMP_EXCEPTION_INFORMATION,
    MiniDumpWithDataSegs, MiniDumpWithIndirectlyReferencedMemory, MiniDumpWriteDump,
    SetUnhandledExceptionFilter,
};
use windows_sys::Win32::System::SystemInformation::GetLocalTime;
use windows_sys::Win32::System::Threading::{
    CreateThread, GetCurrentProcess, GetCurrentProcessId, GetCurrentThreadId, INFINITE, Sleep,
    WaitForSingleObject,
};

/// How long the faulting thread waits for the dump thread before giving up
/// and letting the process die. A dump of this process takes seconds; a wait
/// this long only ends when the dump thread itself is stuck, and then the
/// process is better off terminated than hung.
const DUMP_WAIT_MS: u32 = 120_000;

/// The dump destination for this process, resolved once at install time.
static DUMP_TARGET: OnceLock<DumpTarget> = OnceLock::new();

/// Whether a thread is already inside the filter. Only the first faulting
/// thread dumps; any other that faults meanwhile must not return, because
/// returning ends the process under the first thread's dump.
static FILTER_ENTERED: AtomicBool = AtomicBool::new(false);

struct DumpTarget {
    /// NUL-terminated UTF-16, ready to hand to `CreateFileW` as-is.
    dump_wide_path: Vec<u16>,
    /// The same path as bytes, for the log line.
    dump_display_path: String,
    /// The log file, NUL-terminated UTF-16, when this process writes one.
    log_wide_path: Option<Vec<u16>>,
}

/// Installs the top-level exception filter, writing dumps next to the log
/// files so a user collecting logs picks the dump up with them.
///
/// The name carries the process start time and id rather than the crash time:
/// the filter cannot safely format a path, and one process can crash at most
/// once, so a per-run name is already unique.
pub(crate) fn install_unhandled_exception_filter() {
    let log_file = weaver_server_core::runtime::log_buffer::log_file_path();
    let Some(directory) = dump_directory(log_file) else {
        warn!("no writable directory for crash dumps; unhandled exceptions will not be captured");
        return;
    };
    let path = directory.join(format!(
        "weaver-crash-{}-{}.dmp",
        chrono::Local::now().format("%Y%m%d-%H%M%S"),
        std::process::id()
    ));
    let target = DumpTarget {
        dump_wide_path: wide_path(&path),
        dump_display_path: path.display().to_string(),
        log_wide_path: log_file.map(wide_path),
    };
    if DUMP_TARGET.set(target).is_err() {
        // Already installed; a second filter would only overwrite the first.
        return;
    }
    // SAFETY: the filter is a plain `extern "system"` function with static
    // lifetime and no captured state.
    unsafe { SetUnhandledExceptionFilter(Some(write_minidump)) };
    info!(
        dump_path = %path.display(),
        "an unhandled exception in this process will leave a minidump"
    );
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// The directory the log files live in, falling back to the system temporary
/// directory when this process writes no log file.
fn dump_directory(log_file: Option<&Path>) -> Option<PathBuf> {
    let log_dir = log_file
        .and_then(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf);
    log_dir.or_else(|| {
        let temp = std::env::temp_dir();
        (!temp.as_os_str().is_empty()).then_some(temp)
    })
}

/// What the dump thread needs from the faulting thread, and its answer.
///
/// Process-static rather than a local of the filter. The dump thread reads it
/// for as long as it runs, and a wait that ends early — a timeout, or a wait
/// that fails outright — leaves that thread running: a request on the faulting
/// thread's stack would then be read out of a frame that had already gone.
/// Static is also the allocation-free way to own it, which matters in a filter
/// that must not allocate. Only the first faulting thread ever reaches it, so
/// there is exactly one writer.
struct DumpRequest {
    exception_info: AtomicPtr<EXCEPTION_POINTERS>,
    faulting_thread_id: AtomicU32,
    written: AtomicBool,
}

static DUMP_REQUEST: DumpRequest = DumpRequest {
    exception_info: AtomicPtr::new(std::ptr::null_mut()),
    faulting_thread_id: AtomicU32::new(0),
    written: AtomicBool::new(false),
};

/// The top-level exception filter itself.
unsafe extern "system" fn write_minidump(exception_info: *const EXCEPTION_POINTERS) -> i32 {
    if FILTER_ENTERED.swap(true, Ordering::SeqCst) {
        loop {
            // SAFETY: no preconditions.
            unsafe { Sleep(INFINITE) };
        }
    }
    if let Some(target) = DUMP_TARGET.get() {
        let request = &DUMP_REQUEST;
        request
            .exception_info
            .store(exception_info.cast_mut(), Ordering::SeqCst);
        // SAFETY: no preconditions; identifies the faulting thread.
        request
            .faulting_thread_id
            .store(unsafe { GetCurrentThreadId() }, Ordering::SeqCst);
        // SAFETY: `exception_info` is the pointer the OS handed this filter,
        // valid for the duration of the call; the request is static, so the
        // dump thread can read it whatever this thread does next; the target
        // was built once at install time and is never mutated.
        unsafe {
            if !dump_on_fresh_thread(request) {
                // No thread could be created, so nothing else is reading the
                // request: dump on this stack and hope it is deep enough.
                // Better a truncated dump than none.
                dump_thread_main(std::ptr::from_ref(request).cast_mut().cast());
            }
            record_in_log(
                target,
                exception_info,
                request.written.load(Ordering::SeqCst),
            );
        }
    }
    // Continue the search rather than handling the exception: returning
    // `EXCEPTION_EXECUTE_HANDLER` would swallow the fault and let the process
    // unwind to a normal exit, which changes the exit code the supervisor sees
    // and hides the crash from Windows Error Reporting. Continuing leaves the
    // process dying exactly as it does today; the dump is purely additive.
    EXCEPTION_CONTINUE_SEARCH
}

/// Runs the dump on a new thread with a full stack and waits for it. Returns
/// `false` when no thread could be started — and only then, so the inline
/// fallback never runs beside a live dump thread.
///
/// The wait has three outcomes and each is taken for what it is. Signalled:
/// the dump is finished and its answer is in the request. Timed out, or the
/// wait itself failed: the dump thread may still be running and still reading
/// the request, so nothing may be freed on its account — which is why the
/// request is static — and the caller writes its log line from an answer that
/// is simply not there yet, then returns to let the process die as it was
/// going to. Either way the process is already terminating; this function's
/// job is only to make sure the worker never outlives what it reads.
unsafe fn dump_on_fresh_thread(request: &'static DumpRequest) -> bool {
    // SAFETY: the start routine has the required ABI, and the parameter is a
    // pointer to process-static state, valid for as long as the thread runs.
    let thread: HANDLE = unsafe {
        CreateThread(
            std::ptr::null(),
            0,
            Some(dump_thread_main),
            (request as *const DumpRequest).cast(),
            0,
            std::ptr::null_mut(),
        )
    };
    if thread.is_null() {
        return false;
    }
    // SAFETY: `thread` is a live handle from `CreateThread` above.
    let wait = unsafe { WaitForSingleObject(thread, DUMP_WAIT_MS) };
    if wait != WAIT_OBJECT_0 {
        // The dump thread is stuck, or the wait could not be made at all. The
        // request it reads is static and the handle below is only this
        // thread's reference to it, so letting go of both is safe; the dump
        // simply did not finish, and the log line says so.
        request.written.store(false, Ordering::SeqCst);
    }
    // SAFETY: `thread` came from `CreateThread` above and is not used again.
    unsafe { CloseHandle(thread) };
    true
}

/// The dump thread's body; also the inline fallback.
unsafe extern "system" fn dump_thread_main(parameter: *mut c_void) -> u32 {
    let Some(target) = DUMP_TARGET.get() else {
        return 0;
    };
    // SAFETY: the parameter is the process-static `DumpRequest`, which
    // outlives every thread that reads it.
    let request = unsafe { &*parameter.cast::<DumpRequest>() };
    // SAFETY: the request's exception pointers are the ones the OS handed the
    // filter, valid until the filter returns, which is after this thread.
    let written = unsafe { write_dump(target, request) };
    request.written.store(written, Ordering::SeqCst);
    0
}

/// Writes the dump and reports whether the write succeeded. Every failure is
/// otherwise swallowed: the process is already dying and there is nothing
/// useful left to do about a failed diagnostic.
unsafe fn write_dump(target: &DumpTarget, request: &DumpRequest) -> bool {
    // SAFETY: `dump_wide_path` is NUL-terminated UTF-16 built at install time.
    let file = unsafe {
        CreateFileW(
            target.dump_wide_path.as_ptr(),
            GENERIC_WRITE,
            FILE_SHARE_READ,
            std::ptr::null(),
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if file == INVALID_HANDLE_VALUE {
        return false;
    }

    let exception = MINIDUMP_EXCEPTION_INFORMATION {
        ThreadId: request.faulting_thread_id.load(Ordering::SeqCst),
        ExceptionPointers: request.exception_info.load(Ordering::SeqCst),
        // The pointers belong to this process, not to a client one.
        ClientPointers: 0,
    };
    // A deliberately modest dump: every thread's stack plus the memory those
    // stacks point at, and the loaded modules' data segments. Enough to read a
    // stack trace and the globals around it, while staying small enough that a
    // user can attach it — a full-memory dump of this process would be gigabytes.
    let dump_type = MiniDumpWithIndirectlyReferencedMemory | MiniDumpWithDataSegs;
    // SAFETY: the handle is open and writable, and the exception record lives
    // until this call returns.
    let written = unsafe {
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
    written != 0
}

/// Appends one ERROR line to the log file, in the logger's own line shape so
/// the wrapper's start-failure report finds it like any other error, without
/// allocating or touching the logger.
unsafe fn record_in_log(
    target: &DumpTarget,
    exception_info: *const EXCEPTION_POINTERS,
    written: bool,
) {
    let Some(log_path) = target.log_wide_path.as_ref() else {
        return;
    };
    let (code, address) = {
        // SAFETY: the OS hands the filter either a null pointer or a record
        // valid for the duration of the call.
        let record = unsafe { exception_info.as_ref().map(|info| info.ExceptionRecord) };
        match record.filter(|record| !record.is_null()) {
            // SAFETY: checked non-null just above, same validity as `exception_info`.
            Some(record) => unsafe {
                (
                    (*record).ExceptionCode as u32,
                    (*record).ExceptionAddress as usize,
                )
            },
            None => (0, 0),
        }
    };

    let mut line = LineBuffer::new();
    push_local_timestamp(&mut line);
    line.push(b" ERROR weaver::crash_dump: unhandled exception 0x");
    line.push_hex(u64::from(code), 8);
    line.push(b" (");
    line.push(describe_exception(code));
    line.push(b") at 0x");
    line.push_hex(address as u64, 16);
    line.push(if written {
        b"; minidump written to "
    } else {
        b"; minidump could not be written to "
    });
    line.push(target.dump_display_path.as_bytes());
    line.finish();

    // Sharing must admit the logger's own append handle, which is open with
    // read, write and delete sharing; asking for less would fail the open.
    // SAFETY: `log_path` is NUL-terminated UTF-16 built at install time.
    let file = unsafe {
        CreateFileW(
            log_path.as_ptr(),
            FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            std::ptr::null_mut(),
        )
    };
    if file == INVALID_HANDLE_VALUE {
        return;
    }
    let mut written_bytes = 0u32;
    // SAFETY: the buffer outlives the call and the length is its filled size.
    unsafe {
        WriteFile(
            file,
            line.as_bytes().as_ptr(),
            line.as_bytes().len() as u32,
            &mut written_bytes,
            std::ptr::null_mut(),
        );
        CloseHandle(file);
    }
}

/// A human name for the exception codes a Rust process is likely to die of.
fn describe_exception(code: u32) -> &'static [u8] {
    match code {
        0xC000_0005 => b"access violation",
        0xC000_0006 => b"in-page error",
        0xC000_001D => b"illegal instruction",
        0xC000_008C => b"array bounds exceeded",
        0xC000_0094 => b"integer divide by zero",
        0xC000_0095 => b"integer overflow",
        0xC000_00FD => b"stack overflow",
        0xC000_0374 => b"heap corruption",
        0xC000_0409 => b"fast-fail",
        0x8000_0003 => b"breakpoint",
        _ => b"unrecognised",
    }
}

/// Writes the local time to the millisecond, in the log timer's shape minus
/// its zone suffix: a fixed string of digits that still sorts with the lines
/// around it.
fn push_local_timestamp(line: &mut LineBuffer) {
    let mut now = SYSTEMTIME {
        wYear: 0,
        wMonth: 0,
        wDayOfWeek: 0,
        wDay: 0,
        wHour: 0,
        wMinute: 0,
        wSecond: 0,
        wMilliseconds: 0,
    };
    // SAFETY: `now` is a valid, writable `SYSTEMTIME`.
    unsafe { GetLocalTime(&mut now) };
    line.push_decimal(now.wYear, 4);
    line.push(b"-");
    line.push_decimal(now.wMonth, 2);
    line.push(b"-");
    line.push_decimal(now.wDay, 2);
    line.push(b"T");
    line.push_decimal(now.wHour, 2);
    line.push(b":");
    line.push_decimal(now.wMinute, 2);
    line.push(b":");
    line.push_decimal(now.wSecond, 2);
    line.push(b".");
    line.push_decimal(now.wMilliseconds, 3);
}

/// A fixed-size line assembled without allocating. Content past the capacity
/// is dropped; the terminating newline always fits.
struct LineBuffer {
    bytes: [u8; 2048],
    len: usize,
}

impl LineBuffer {
    const fn new() -> Self {
        Self {
            bytes: [0; 2048],
            len: 0,
        }
    }

    fn push(&mut self, text: &[u8]) {
        let room = self.bytes.len() - 1 - self.len;
        let take = text.len().min(room);
        self.bytes[self.len..self.len + take].copy_from_slice(&text[..take]);
        self.len += take;
    }

    fn push_hex(&mut self, value: u64, digits: u32) {
        for shift in (0..digits).rev() {
            let nibble = ((value >> (shift * 4)) & 0xF) as usize;
            self.push(&b"0123456789ABCDEF"[nibble..=nibble]);
        }
    }

    /// Zero-padded decimal, at most five digits.
    fn push_decimal(&mut self, value: u16, digits: usize) {
        let mut rendered = [b'0'; 5];
        let mut remaining = value;
        for slot in rendered.iter_mut().rev() {
            *slot = b'0' + (remaining % 10) as u8;
            remaining /= 10;
        }
        self.push(&rendered[rendered.len() - digits.min(rendered.len())..]);
    }

    fn finish(&mut self) {
        self.bytes[self.len] = b'\n';
        self.len += 1;
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_crash_line_is_shaped_like_a_logger_error_line() {
        let mut line = LineBuffer::new();
        line.push(b"2026-09-18T10:11:12.345");
        line.push(b" ERROR weaver::crash_dump: unhandled exception 0x");
        line.push_hex(0xC000_0005, 8);
        line.push(b" (");
        line.push(describe_exception(0xC000_0005));
        line.push(b") at 0x");
        line.push_hex(0x7FF6_1234_ABCD, 16);
        line.finish();
        assert_eq!(
            std::str::from_utf8(line.as_bytes()).unwrap(),
            "2026-09-18T10:11:12.345 ERROR weaver::crash_dump: unhandled exception 0xC0000005 (access violation) at 0x00007FF61234ABCD\n"
        );
    }

    #[test]
    fn decimal_fields_are_zero_padded() {
        let mut line = LineBuffer::new();
        line.push_decimal(7, 2);
        line.push(b"/");
        line.push_decimal(2026, 4);
        line.push(b"/");
        line.push_decimal(45, 3);
        assert_eq!(line.as_bytes(), b"07/2026/045");
    }

    #[test]
    fn an_overlong_line_is_truncated_but_still_terminated() {
        let mut line = LineBuffer::new();
        line.push(&[b'x'; 4096]);
        line.finish();
        assert_eq!(line.as_bytes().len(), 2048);
        assert_eq!(line.as_bytes().last(), Some(&b'\n'));
    }
}
