//! s2n's synchronous callback boundary for a userspace route.
use crate::route_stream::BlockingSocket;
use std::io::{self, Read, Write};

fn result(operation: impl FnOnce() -> io::Result<usize>) -> libc::c_int {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
        .unwrap_or_else(|_| Err(io::Error::other("tunnel I/O panicked")));
    match result {
        Ok(length) => length as libc::c_int,
        Err(error) => {
            let code = match error.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => libc::EWOULDBLOCK,
                io::ErrorKind::Interrupted => libc::EINTR,
                io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset => {
                    libc::ECONNRESET
                }
                _ => libc::EIO,
            };
            // These functions return this thread's C errno, which s2n reads
            // immediately after the callback returns.
            unsafe {
                #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
                {
                    *libc::__error() = code;
                }
                #[cfg(any(target_os = "linux", target_os = "dragonfly"))]
                {
                    *libc::__errno_location() = code;
                }
                #[cfg(any(target_os = "android", target_os = "netbsd", target_os = "openbsd"))]
                {
                    *libc::__errno() = code;
                }
            }
            -1
        }
    }
}

pub(super) unsafe extern "C" fn recv(
    context: *mut libc::c_void,
    buffer: *mut u8,
    length: u32,
) -> libc::c_int {
    result(|| {
        if length == 0 {
            return Ok(0);
        }
        if context.is_null() || buffer.is_null() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // s2n supplies a writable buffer and our stable boxed socket context.
        // Length is bounded to the callback's signed return range.
        let stream = unsafe { &mut *context.cast::<BlockingSocket>() };
        let buffer =
            unsafe { std::slice::from_raw_parts_mut(buffer, length.min(i32::MAX as u32) as usize) };
        stream.read(buffer)
    })
}

pub(super) unsafe extern "C" fn send(
    context: *mut libc::c_void,
    buffer: *const u8,
    length: u32,
) -> libc::c_int {
    result(|| {
        if length == 0 {
            return Ok(0);
        }
        if context.is_null() || buffer.is_null() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // s2n supplies readable bytes for this synchronous call only.
        let stream = unsafe { &mut *context.cast::<BlockingSocket>() };
        let buffer =
            unsafe { std::slice::from_raw_parts(buffer, length.min(i32::MAX as u32) as usize) };
        stream.write(buffer)
    })
}
