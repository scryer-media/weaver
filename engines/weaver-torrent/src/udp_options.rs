// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a kernel datagram socket is set to before uTP runs over it: its
//! datagrams are never fragmented, so a path-MTU probe larger than the path
//! is dropped rather than split, and its receive buffer holds a full receive
//! window for every connection the session may hold, as far as the host
//! allows, so a burst across them waits in the kernel instead of being
//! dropped while the engine is busy.

use std::{io, net::SocketAddr};

use tracing::{debug, warn};

/// The receive window each uTP stream is given.
pub(crate) const STREAM_WINDOW: usize = 1024 * 1024;

/// The receive buffer a uTP socket asks for when the session may hold
/// `connections_limit` connections: a full window for each, up to the
/// largest size the option can carry.
pub(crate) fn receive_buffer_for(connections_limit: usize) -> usize {
    connections_limit
        .saturating_mul(STREAM_WINDOW)
        .min(i32::MAX as usize)
}

/// Set `socket`, asked for at `requested`, for uTP, with a receive buffer of
/// `receive_buffer` bytes. Neither setting is needed for the socket to work:
/// one the host refuses is logged, and the socket is used as it is.
pub(crate) fn set_for_utp(
    socket: &tokio::net::UdpSocket,
    requested: SocketAddr,
    receive_buffer: usize,
) {
    if requested.is_ipv4() {
        if let Err(error) = set_dont_fragment(socket, Family::V4) {
            warn!(%requested, %error, "the socket's IPv4 datagrams may be fragmented");
        }
    } else {
        if let Err(error) = set_dont_fragment(socket, Family::V6) {
            debug!(%requested, %error, "the socket's IPv6 datagrams may be fragmented");
        }
        // A socket at an IPv6 address that takes IPv4 as well sends its IPv4
        // datagrams under the IPv4 option. Some hosts refuse it on such a
        // socket; those datagrams then keep the host's default.
        if takes_ipv4(socket)
            && let Err(error) = set_dont_fragment(socket, Family::V4)
        {
            debug!(%requested, %error, "the socket's IPv4 datagrams may be fragmented");
        }
    }

    let socket = socket2::SockRef::from(socket);
    let before = socket.recv_buffer_size().ok();
    match raise_receive_buffer(&socket, receive_buffer) {
        Ok(now) if before != Some(now) => {
            debug!(
                ?before,
                now,
                asked = receive_buffer,
                "the socket's receive buffer was raised"
            );
        }
        Ok(now) => debug!(
            now,
            asked = receive_buffer,
            "the socket's receive buffer was kept"
        ),
        Err(error) => {
            debug!(?before, asked = receive_buffer, %error, "the socket's receive buffer was not raised");
        }
    }
}

/// Ask for `socket`'s receive buffer to hold `ask` bytes, and read back what
/// it holds. A buffer already that large is kept. A host that clamps the ask
/// grants its limit at once; one that refuses it is asked for the largest
/// size between the current one and `ask` that it accepts.
pub(crate) fn raise_receive_buffer(socket: &socket2::SockRef<'_>, ask: usize) -> io::Result<usize> {
    let before = socket.recv_buffer_size()?;
    if before >= ask {
        return Ok(before);
    }
    if socket.set_recv_buffer_size(ask).is_err() {
        // The socket holds `low`; `high` was refused.
        let (mut low, mut high) = (before, ask);
        while high - low > 1 {
            let mid = low + (high - low) / 2;
            if socket.set_recv_buffer_size(mid).is_ok() {
                low = mid;
            } else {
                high = mid;
            }
        }
    }
    socket.recv_buffer_size()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Family {
    V4,
    V6,
}

// Whether `socket`, an IPv6 one, takes IPv4 peers too.
fn takes_ipv4(socket: &tokio::net::UdpSocket) -> bool {
    socket2::SockRef::from(socket)
        .only_v6()
        .is_ok_and(|only_v6| !only_v6)
}

/// Forbid fragmenting `socket`'s datagrams of `family`.
pub(crate) fn set_dont_fragment(socket: &tokio::net::UdpSocket, family: Family) -> io::Result<()> {
    sys::set(socket, family)
}

/// Whether `socket`'s datagrams of `family` are never fragmented.
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
        let len = int_len()?;
        // SAFETY: `fd` is an open socket for the call, and the option at
        // `level`/`name` takes an int, which `value` is.
        let done = unsafe { libc::setsockopt(fd, level, name, (&raw const value).cast(), len) };
        if done == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    // The length of the int a socket option is.
    fn int_len() -> io::Result<libc::socklen_t> {
        libc::socklen_t::try_from(mem::size_of::<libc::c_int>()).map_err(io::Error::other)
    }

    pub(super) fn get_int(
        fd: RawFd,
        level: libc::c_int,
        name: libc::c_int,
    ) -> io::Result<libc::c_int> {
        let mut value: libc::c_int = 0;
        let mut len = int_len()?;
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

    #[cfg(unix)]
    #[tokio::test]
    async fn the_factory_reports_whether_its_datagrams_may_fragment() {
        use crate::network::SocketFactory;

        let factory = crate::test_support::sockets();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let utp = factory.bind_utp(addr, super::STREAM_WINDOW).await.unwrap();
        assert!(utp.dont_fragment());
        let udp = factory.bind_udp(addr).await.unwrap();
        assert!(!udp.dont_fragment());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_dualstack_socket_forbids_fragmenting_each_family_the_host_allows() {
        let socket = crate::test_support::sockets()
            .bind_kernel_utp((Ipv6Addr::UNSPECIFIED, 0).into(), super::STREAM_WINDOW)
            .unwrap();
        assert!(!socket2::SockRef::from(socket.socket()).only_v6().unwrap());
        assert!(dont_fragment(socket.socket(), Family::V6).unwrap());
        match dont_fragment(socket.socket(), Family::V4) {
            Ok(set) => assert!(set, "IPv4 datagrams may be fragmented"),
            // Apple's hosts refuse the IPv4 option on a socket of both
            // families; its IPv4 datagrams keep the host's default.
            Err(_) if cfg!(target_vendor = "apple") => {}
            Err(error) => panic!("{error}"),
        }
        assert_eq!(
            socket.dont_fragment(),
            dont_fragment(socket.socket(), Family::V4).is_ok_and(|set| set)
        );
    }

    #[tokio::test]
    async fn a_utp_socket_is_set_for_utp() {
        let ask = super::receive_buffer_for(crate::connection_slots::DEFAULT_CONNECTIONS_LIMIT);
        assert_eq!(ask, 200 * 1024 * 1024);
        for (addr, family) in [
            (SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), Family::V4),
            (SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), Family::V6),
        ] {
            let socket = crate::test_support::sockets()
                .bind_kernel_utp(addr, ask)
                .unwrap();
            match dont_fragment(socket.socket(), family) {
                Ok(set) => assert!(set, "{family:?} datagrams may be fragmented"),
                // A host with no such option keeps its default.
                Err(error) => eprintln!("{family:?}: no option to forbid fragmenting: {error}"),
            }
            let granted = socket2::SockRef::from(socket.socket())
                .recv_buffer_size()
                .unwrap();
            // What the host grants a socket asked for the window per
            // connection; a host that reads back twice what it holds may
            // report up to twice the ask.
            let other = fresh(family);
            let other_granted = super::raise_receive_buffer(&(&other).into(), ask).unwrap();
            assert_eq!(granted, other_granted, "{family:?}");
            assert!(granted <= 2 * ask, "{family:?}: {granted}");
            if granted < ask {
                // The host's bound: a larger ask is granted no more.
                let larger = fresh(family);
                let larger_granted =
                    super::raise_receive_buffer(&(&larger).into(), 2 * ask).unwrap();
                assert_eq!(larger_granted, granted, "{family:?}");
            }
        }
    }

    #[test]
    fn a_buffer_already_as_large_as_the_ask_is_kept() {
        let socket = fresh(Family::V4);
        let before = socket.recv_buffer_size().unwrap();
        assert_eq!(
            super::raise_receive_buffer(&(&socket).into(), 1).unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn the_dht_and_tracker_sockets_keep_the_kernels_defaults() {
        // The factory's plain datagram socket, which the DHT and the UDP
        // trackers bind.
        for (addr, family) in [
            (SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), Family::V4),
            (SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), Family::V6),
        ] {
            let socket = crate::test_support::sockets()
                .bind_kernel_udp(addr)
                .unwrap();
            if let Ok(set) = dont_fragment(socket.socket(), family) {
                assert!(!set, "{family:?} datagrams may not be fragmented");
            }
            let buffer = socket2::SockRef::from(socket.socket())
                .recv_buffer_size()
                .unwrap();
            assert_eq!(
                buffer,
                fresh(family).recv_buffer_size().unwrap(),
                "{family:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_vectored_send_is_one_datagram() {
        use std::sync::atomic::Ordering;

        let receiver = crate::test_support::loopback_kernel_udp();
        let sender = crate::test_support::loopback_kernel_udp();
        let to = receiver.bind_addr();
        let parts = [IoSlice::new(b"ab"), IoSlice::new(b"cde")];
        let before = crate::network::VECTORED_SENDS.load(Ordering::Relaxed);
        let sent = std::future::poll_fn(|cx| sender.poll_send_to_vectored(cx, &parts, to))
            .await
            .unwrap();
        assert_eq!(sent, 5);
        // Sent from the slices by the kernel socket itself.
        assert!(crate::network::VECTORED_SENDS.load(Ordering::Relaxed) > before);
        let mut buf = [0u8; 16];
        let (len, from) = receiver.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&b"abcde"[..], sender.bind_addr()));
    }
}
