// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io;

#[cfg(windows)]
const MESSAGE_SIZE: i32 = 10040;
#[cfg(not(windows))]
const MESSAGE_SIZE: i32 = libc::EMSGSIZE;

/// An OS message-size error for a datagram rejected before acceptance.
pub fn message_size_error() -> io::Error {
    io::Error::from_raw_os_error(MESSAGE_SIZE)
}

/// Whether a send failed because its datagram was too large.
pub fn is_message_size_error(error: &io::Error) -> bool {
    error.raw_os_error() == Some(MESSAGE_SIZE)
}

#[cfg(any(windows, test))]
fn windows_recoverable_receive_code(code: i32) -> bool {
    matches!(
        code,
        // Winsock: host unreachable, connection reset/refused/aborted,
        // network reset/unreachable, message too large.
        10065 | 10054 | 10061 | 10053 | 10052 | 10051 | 10040
        // Win32: more data, host/port unreachable, retry, network
        // unreachable, connection refused/aborted.
            | 234 | 1232 | 1234 | 1237 | 1231 | 1225 | 1236
    )
}

pub(crate) fn is_recoverable_receive_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::Interrupted
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
    ) {
        return true;
    }
    let Some(code) = error.raw_os_error() else {
        return false;
    };
    #[cfg(windows)]
    {
        windows_recoverable_receive_code(code)
    }
    #[cfg(not(windows))]
    {
        [
            libc::EHOSTUNREACH,
            libc::ECONNRESET,
            libc::ECONNREFUSED,
            libc::ECONNABORTED,
            libc::ENETRESET,
            libc::ENETUNREACH,
            libc::EMSGSIZE,
        ]
        .contains(&code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_size_constructor_and_classifier_use_the_native_socket_code() {
        let error = message_size_error();
        #[cfg(windows)]
        assert_eq!(error.raw_os_error(), Some(10040));
        #[cfg(not(windows))]
        assert_eq!(error.raw_os_error(), Some(libc::EMSGSIZE));
        assert!(is_message_size_error(&error));
        assert!(is_recoverable_receive_error(&error));
        assert!(!is_message_size_error(&io::Error::new(
            io::ErrorKind::BrokenPipe,
            "terminal"
        )));
        #[cfg(windows)]
        assert!(!is_message_size_error(&io::Error::from_raw_os_error(234)));
    }

    #[test]
    fn custom_transport_receive_kinds_are_narrowly_recoverable() {
        for kind in [
            io::ErrorKind::WouldBlock,
            io::ErrorKind::Interrupted,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionAborted,
        ] {
            assert!(
                is_recoverable_receive_error(&io::Error::new(kind, "datagram")),
                "{kind:?}"
            );
        }
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::NotConnected,
            io::ErrorKind::UnexpectedEof,
        ] {
            assert!(
                !is_recoverable_receive_error(&io::Error::new(kind, "terminal")),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn windows_socket_and_system_receive_codes_are_mapped_explicitly() {
        for code in [
            10065, 10054, 10061, 10053, 10052, 10051, 10040, 234, 1232, 1234, 1237, 1231, 1225,
            1236,
        ] {
            assert!(windows_recoverable_receive_code(code), "{code}");
        }
        for code in [0, 5, 6, 109, 995, 10014, 10038, 10050, 10057] {
            assert!(!windows_recoverable_receive_code(code), "{code}");
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_receive_codes_are_mapped_explicitly() {
        for code in [
            libc::EHOSTUNREACH,
            libc::ECONNRESET,
            libc::ECONNREFUSED,
            libc::ECONNABORTED,
            libc::ENETRESET,
            libc::ENETUNREACH,
            libc::EMSGSIZE,
            libc::EAGAIN,
            libc::EINTR,
        ] {
            assert!(
                is_recoverable_receive_error(&io::Error::from_raw_os_error(code)),
                "{code}"
            );
        }
        for code in [
            libc::EBADF,
            libc::EPIPE,
            libc::EACCES,
            libc::ENOTCONN,
            libc::EFAULT,
            libc::ECANCELED,
        ] {
            assert!(
                !is_recoverable_receive_error(&io::Error::from_raw_os_error(code)),
                "{code}"
            );
        }
    }
}
