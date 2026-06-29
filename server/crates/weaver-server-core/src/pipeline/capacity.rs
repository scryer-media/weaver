use std::io;

pub(in crate::pipeline) const FD_CAPACITY_ERROR_MARKER: &str = "RAR source FD capacity pressure";

pub(in crate::pipeline) fn is_fd_capacity_io_error(error: &io::Error) -> bool {
    error
        .raw_os_error()
        .is_some_and(is_fd_capacity_raw_os_error)
}

pub(in crate::pipeline) fn is_fd_capacity_error_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    message.contains(FD_CAPACITY_ERROR_MARKER)
        || lower.contains("too many open files")
        || lower.contains("file table overflow")
}

pub(in crate::pipeline) fn format_fd_capacity_error(context: &str, error: &io::Error) -> String {
    if is_fd_capacity_io_error(error) {
        format!("{FD_CAPACITY_ERROR_MARKER}: {context}: {error}")
    } else {
        format!("{context}: {error}")
    }
}

#[cfg(unix)]
fn is_fd_capacity_raw_os_error(code: i32) -> bool {
    code == libc::EMFILE || code == libc::ENFILE
}

#[cfg(not(unix))]
fn is_fd_capacity_raw_os_error(code: i32) -> bool {
    // Windows commonly reports ERROR_TOO_MANY_OPEN_FILES (4). Keep EMFILE's
    // conventional value as a fallback for C runtime surfaced errors.
    code == 4 || code == 24
}
