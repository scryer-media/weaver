// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a kernel datagram socket is set to before uTP runs over it: its
//! datagrams are never fragmented, so a path-MTU probe larger than the path
//! is dropped rather than split, and its receive buffer is as large as the
//! host allows, so a burst across however many streams the socket carries
//! waits in the kernel instead of being dropped while the engine is busy.

use std::{io, net::SocketAddr};

use tracing::{debug, info, warn};

/// The most a socket's receive buffer can be asked for.
const LARGEST_ASK: usize = i32::MAX as usize;

/// Set `socket`, asked for at `requested`, for uTP. Neither setting is
/// needed for the socket to work: one the host refuses is logged, and the
/// socket is used as it is.
pub(crate) fn set_for_utp(socket: &tokio::net::UdpSocket, requested: SocketAddr) {
    if requested.is_ipv4() {
        if let Err(error) = set_dont_fragment(socket, Family::V4) {
            warn!(%requested, %error, "the socket's IPv4 datagrams may be fragmented");
        }
    } else if let Err(error) = set_dont_fragment(socket, Family::V6) {
        debug!(%requested, %error, "the socket's IPv6 datagrams may be fragmented");
    }

    let socket = socket2::SockRef::from(socket);
    let before = socket.recv_buffer_size().ok();
    match raise_receive_buffer(&socket) {
        Ok(now) => info!(?before, now, "the socket's receive buffer was raised"),
        Err(error) => warn!(?before, %error, "the socket's receive buffer was not raised"),
    }
}

/// Raise `socket`'s receive buffer as far as the host allows, and read back
/// what it holds. A host that clamps an ask grants its limit at once; one
/// that refuses an ask above its limit is asked for the largest size it
/// accepts.
pub(crate) fn raise_receive_buffer(socket: &socket2::SockRef<'_>) -> io::Result<usize> {
    if socket.set_recv_buffer_size(LARGEST_ASK).is_err() {
        // Accepted at `low`, refused above `high`.
        let (mut low, mut high) = (socket.recv_buffer_size()?, LARGEST_ASK);
        socket.set_recv_buffer_size(low)?;
        while high - low > 1 {
            let ask = low + (high - low) / 2;
            if socket.set_recv_buffer_size(ask).is_ok() {
                low = ask;
            } else {
                high = ask;
            }
        }
        socket.set_recv_buffer_size(low)?;
    }
    socket.recv_buffer_size()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Family {
    V4,
    V6,
}

/// Forbid fragmenting `socket`'s datagrams of `family`.
pub(crate) fn set_dont_fragment(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
    sys::set(socket, family)
}

/// Whether `socket`'s datagrams of `family` are never fragmented.
#[cfg(test)]
pub(crate) fn dont_fragment(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<bool> {
    sys::get(socket, family)
}

// Linux asks for path-MTU discovery that never fragments.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod sys {
    use std::{io, os::fd::AsRawFd};

    use nix::libc;

    use super::Family;

    fn option(family: Family) -> (libc::c_int, libc::c_int, libc::c_int) {
        match family {
            Family::V4 => (
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                libc::IP_PMTUDISC_DO,
            ),
            Family::V6 => (
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                libc::IPV6_PMTUDISC_DO,
            ),
        }
    }

    pub(super) fn set(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
        let (level, name, value) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, value)
    }

    #[cfg(test)]
    pub(super) fn get(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<bool> {
        let (level, name, value) = option(family);
        Ok(super::unix::get_int(socket.as_raw_fd(), level, name)? == value)
    }
}

// Apple's systems and FreeBSD carry a flag for it.
#[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
mod sys {
    use std::{io, os::fd::AsRawFd};

    use nix::libc;

    use super::Family;

    fn option(family: Family) -> (libc::c_int, libc::c_int) {
        match family {
            Family::V4 => (libc::IPPROTO_IP, libc::IP_DONTFRAG),
            Family::V6 => (libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG),
        }
    }

    pub(super) fn set(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
        let (level, name) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, 1)
    }

    #[cfg(test)]
    pub(super) fn get(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<bool> {
        let (level, name) = option(family);
        Ok(super::unix::get_int(socket.as_raw_fd(), level, name)? != 0)
    }
}

#[cfg(windows)]
mod sys {
    use std::{io, os::windows::io::AsRawSocket};

    use windows::Win32::Networking::WinSock::{
        IP_DONTFRAGMENT, IPPROTO_IP, IPPROTO_IPV6, IPV6_DONTFRAG, SOCKET, WSAGetLastError,
        getsockopt, setsockopt,
    };

    use super::Family;

    fn option(family: Family) -> (i32, i32) {
        match family {
            Family::V4 => (IPPROTO_IP.0, IP_DONTFRAGMENT),
            Family::V6 => (IPPROTO_IPV6.0, IPV6_DONTFRAG),
        }
    }

    fn last_error() -> io::Error {
        // SAFETY: reads the calling thread's last socket error.
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() }.0)
    }

    pub(super) fn set(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
        let (level, name) = option(family);
        let value = 1i32.to_ne_bytes();
        // SAFETY: the socket is open for as long as `socket` is borrowed, and
        // the option takes an int, which `value` holds.
        let done = unsafe {
            setsockopt(
                SOCKET(socket.as_raw_socket() as usize),
                level,
                name,
                Some(&value),
            )
        };
        if done == 0 { Ok(()) } else { Err(last_error()) }
    }

    #[cfg(test)]
    pub(super) fn get(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<bool> {
        let (level, name) = option(family);
        let mut value = [0u8; 4];
        let mut len = value.len() as i32;
        // SAFETY: as in `set`; `value` has room for the int the option is.
        let done = unsafe {
            getsockopt(
                SOCKET(socket.as_raw_socket() as usize),
                level,
                name,
                windows::core::PSTR(value.as_mut_ptr()),
                &mut len,
            )
        };
        if done == 0 {
            Ok(i32::from_ne_bytes(value) != 0)
        } else {
            Err(last_error())
        }
    }
}

// A host with no such option: the socket keeps the host's default.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd",
    windows
)))]
mod sys {
    use std::io;

    use super::Family;

    pub(super) fn set(_: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("this host has no option to forbid fragmenting {family:?} datagrams"),
        ))
    }

    #[cfg(test)]
    pub(super) fn get(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<bool> {
        set(socket, family).map(|()| false)
    }
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd"
))]
mod unix {
    use std::{io, mem, os::fd::RawFd};

    use nix::libc;

    pub(super) fn set_int(
        fd: RawFd,
        level: libc::c_int,
        name: libc::c_int,
        value: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: `fd` is an open socket for the call, and the option at
        // `level`/`name` takes an int, which `value` is.
        let done = unsafe {
            libc::setsockopt(
                fd,
                level,
                name,
                (&raw const value).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if done == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    pub(super) fn get_int(
        fd: RawFd,
        level: libc::c_int,
        name: libc::c_int,
    ) -> io::Result<libc::c_int> {
        let mut value: libc::c_int = 0;
        let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: as in `set_int`; `value` has room for the int the option is.
        let done =
            unsafe { libc::getsockopt(fd, level, name, (&raw mut value).cast(), &raw mut len) };
        if done == 0 {
            Ok(value)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::IoSlice,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    };

    use super::{Family, dont_fragment};
    use crate::network::DatagramSocket;

    // A fresh socket of `family`, at the host's default receive buffer.
    fn fresh(family: Family) -> socket2::Socket {
        let domain = match family {
            Family::V4 => socket2::Domain::IPV4,
            Family::V6 => socket2::Domain::IPV6,
        };
        socket2::Socket::new(domain, socket2::Type::DGRAM, None).unwrap()
    }

    #[tokio::test]
    async fn a_kernel_datagram_socket_is_set_for_utp() {
        for (addr, family) in [
            (SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), Family::V4),
            (SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), Family::V6),
        ] {
            let socket = crate::test_support::sockets()
                .bind_kernel_udp(addr)
                .unwrap();
            match dont_fragment(socket.socket(), family) {
                Ok(set) => assert!(set, "{family:?} datagrams may be fragmented"),
                // A host with no such option keeps its default.
                Err(error) => eprintln!("{family:?}: no option to forbid fragmenting: {error}"),
            }
            let buffer = socket2::SockRef::from(socket.socket())
                .recv_buffer_size()
                .unwrap();
            // At least the host's default, and nothing larger is granted.
            let other = fresh(family);
            assert!(buffer >= other.recv_buffer_size().unwrap(), "{family:?}");
            if other.set_recv_buffer_size(buffer * 2).is_ok() {
                assert!(other.recv_buffer_size().unwrap() <= buffer, "{family:?}");
            }
        }
    }

    #[tokio::test]
    async fn a_vectored_send_is_one_datagram() {
        let receiver = crate::test_support::loopback_kernel_udp();
        let sender = crate::test_support::loopback_udp().await;
        let to = receiver.bind_addr();
        let parts = [IoSlice::new(b"ab"), IoSlice::new(b"cde")];
        let sent = std::future::poll_fn(|cx| sender.poll_send_to_vectored(cx, &parts, to))
            .await
            .unwrap();
        assert_eq!(sent, 5);
        let mut buf = [0u8; 16];
        let (len, from) = receiver.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&b"abcde"[..], sender.bind_addr()));
    }
}
