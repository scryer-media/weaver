// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;

pub(in crate::pipeline) const FD_CAPACITY_ERROR_MARKER: &str = "RAR source FD capacity pressure";

pub(in crate::pipeline) fn is_fd_capacity_io_error(error: &io::Error) -> bool {
    error
        .raw_os_error()
        .is_some_and(is_fd_capacity_raw_os_error)
}

pub(in crate::pipeline) fn format_fd_capacity_error(context: &str, error: &io::Error) -> String {
    if is_fd_capacity_io_error(error) {
        format!("{FD_CAPACITY_ERROR_MARKER}: {context}: {error}")
    } else {
        format!("{context}: {error}")
    }
}

// A source file that would not open, kept typed inside the `io::Error` the
// reader returns so the extraction that reads through it still sees the OS
// error code. It renders as it always has: the capacity marker when the code
// is file-descriptor exhaustion, then the context and the OS error.
#[derive(Debug)]
pub(in crate::pipeline) struct SourceOpenError {
    context: String,
    cause: io::Error,
}

impl SourceOpenError {
    pub(in crate::pipeline) fn cause(&self) -> &io::Error {
        &self.cause
    }
}

impl std::fmt::Display for SourceOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&format_fd_capacity_error(&self.context, &self.cause))
    }
}

impl std::error::Error for SourceOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[cfg(unix)]
pub(in crate::pipeline) fn is_fd_capacity_raw_os_error(code: i32) -> bool {
    code == libc::EMFILE || code == libc::ENFILE
}

#[cfg(not(unix))]
pub(in crate::pipeline) fn is_fd_capacity_raw_os_error(code: i32) -> bool {
    // Windows commonly reports ERROR_TOO_MANY_OPEN_FILES (4). Keep EMFILE's
    // conventional value as a fallback for C runtime surfaced errors.
    code == 4 || code == 24
}
