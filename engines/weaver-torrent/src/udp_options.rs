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

/// A datagram socket the options are read and set on: one registered with
/// a runtime or one not yet.
#[cfg(unix)]
pub(crate) trait AsSocket: std::os::fd::AsFd + std::os::fd::AsRawFd {}
#[cfg(unix)]
impl<T: std::os::fd::AsFd + std::os::fd::AsRawFd> AsSocket for T {}
#[cfg(windows)]
pub(crate) trait AsSocket:
    std::os::windows::io::AsSocket + std::os::windows::io::AsRawSocket
{
}
#[cfg(windows)]
impl<T: std::os::windows::io::AsSocket + std::os::windows::io::AsRawSocket> AsSocket for T {}

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
pub(crate) fn set_for_utp(socket: &impl AsSocket, requested: SocketAddr, receive_buffer: usize) {
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
pub(crate) fn takes_ipv4(socket: &impl AsSocket) -> bool {
    socket2::SockRef::from(socket)
        .only_v6()
        .is_ok_and(|only_v6| !only_v6)
}

/// Forbid fragmenting `socket`'s datagrams of `family`.
pub(crate) fn set_dont_fragment(socket: &impl AsSocket, family: Family) -> io::Result<()> {
    sys::set(socket, family)
}

/// Whether `socket`'s datagrams of `family` are never fragmented.
pub(crate) fn dont_fragment(socket: &impl AsSocket, family: Family) -> io::Result<bool> {
    sys::get(socket, family)
}

/// Allow fragmenting `socket`'s datagrams of `family`.
pub(crate) fn clear_dont_fragment(socket: &impl AsSocket, family: Family) -> io::Result<()> {
    sys::clear(socket, family)
}

/// Run `send`, one send of a datagram of `family` on `socket`, allowing it
/// to be fragmented: a socket that forbids fragmenting allows it for this
/// send and forbids it again after, whatever the send's outcome, as
/// libtorrent switches the option around one send (udp_socket.cpp
/// v2.0.15:160-180, 346-350). The caller holds the socket's lock for sends
/// that test the path's size, so no probe goes out while it is allowed.
/// Where the option cannot be switched off the datagram goes as any other.
pub(crate) fn send_fragmenting(
    socket: &impl AsSocket,
    family: Family,
    send: impl FnOnce() -> io::Result<usize>,
) -> io::Result<usize> {
    if !dont_fragment(socket, family).unwrap_or(false) {
        return send();
    }
    if let Err(error) = clear_dont_fragment(socket, family) {
        debug!(?family, %error, "a datagram that may be fragmented goes without it");
        return send();
    }
    let sent = send();
    if let Err(error) = set_dont_fragment(socket, family) {
        warn!(?family, %error, "the socket's datagrams may be fragmented");
    }
    sent
}

/// The most datagrams one [`send_run`] carries, and the most bytes: Linux
/// takes 64 segments in a call (UDP_MAX_SEGMENTS, include/linux/udp.h) and
/// their sum must fit one UDP datagram's length.
pub(crate) const SEND_RUN_DATAGRAMS: usize = 64;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) const SEND_RUN_BYTES: usize = 65_000;

/// Whether one system call can send several datagrams on `socket`: Linux
/// with UDP segmentation (4.18 and later). No other host has a public call
/// for it.
pub(crate) fn sends_runs(socket: &impl AsSocket) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        runs::supported(socket.as_raw_fd())
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = socket;
        false
    }
}

/// Sends `bytes` to `target` in one system call as datagrams of `segment`
/// bytes each, the last one shorter when `bytes` does not divide. The
/// kernel takes all of them or none.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn send_run(
    socket: &impl AsSocket,
    bytes: &[u8],
    segment: u16,
    target: &socket2::SockAddr,
) -> io::Result<usize> {
    runs::send(socket.as_raw_fd(), bytes, segment, target)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod runs {
    use std::{io, mem, os::fd::RawFd};

    use nix::libc;

    pub(super) fn supported(fd: RawFd) -> bool {
        super::unix::get_int(fd, libc::SOL_UDP, libc::UDP_SEGMENT).is_ok()
    }

    pub(super) fn send(
        fd: RawFd,
        bytes: &[u8],
        segment: u16,
        target: &socket2::SockAddr,
    ) -> io::Result<usize> {
        // Room for one control message that holds the segment size, kept
        // at the alignment a control message header needs.
        let mut control = [0u64; 4];
        let mut iov = libc::iovec {
            iov_base: bytes.as_ptr().cast_mut().cast(),
            iov_len: bytes.len(),
        };
        // SAFETY: a message header is plain data for which all zeroes is a
        // valid value.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_name = target.as_ptr().cast_mut().cast();
        msg.msg_namelen = target.len();
        msg.msg_iov = &raw mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        // SAFETY: CMSG_SPACE and CMSG_LEN only compute sizes.
        let (space, len) = unsafe {
            (
                libc::CMSG_SPACE(mem::size_of::<u16>() as u32),
                libc::CMSG_LEN(mem::size_of::<u16>() as u32),
            )
        };
        debug_assert!(space as usize <= mem::size_of_val(&control));
        msg.msg_controllen = space as _;
        // SAFETY: `msg` names `control`, which is aligned for a control
        // message header and has room for the one message of `space`
        // bytes, so the first header and its data lie inside it. `bytes`,
        // `target` and `control` outlive the call, which only reads them,
        // and `fd` is an open socket for the call.
        let sent = unsafe {
            let header = libc::CMSG_FIRSTHDR(&raw const msg);
            (*header).cmsg_level = libc::SOL_UDP;
            (*header).cmsg_type = libc::UDP_SEGMENT;
            (*header).cmsg_len = len as _;
            libc::CMSG_DATA(header).cast::<u16>().write_unaligned(segment);
            libc::sendmsg(fd, &raw const msg, 0)
        };
        if sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(sent as usize)
        }
    }
}

// Linux asks for path-MTU discovery that never fragments.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod sys {
    use std::io;

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

    pub(super) fn set(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name, value) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, value)
    }

    // Never set don't-fragment, as libtorrent switches it off
    // (socket.hpp v2.0.15:248-249).
    pub(super) fn clear(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name, _) = option(family);
        let value = match family {
            Family::V4 => libc::IP_PMTUDISC_DONT,
            Family::V6 => libc::IPV6_PMTUDISC_DONT,
        };
        super::unix::set_int(socket.as_raw_fd(), level, name, value)
    }

    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
        let (level, name, value) = option(family);
        Ok(super::unix::get_int(socket.as_raw_fd(), level, name)? == value)
    }
}

// Apple's systems and FreeBSD carry a flag for it.
#[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
mod sys {
    use std::io;

    use nix::libc;

    use super::Family;

    fn option(family: Family) -> (libc::c_int, libc::c_int) {
        match family {
            Family::V4 => (libc::IPPROTO_IP, libc::IP_DONTFRAG),
            Family::V6 => (libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG),
        }
    }

    pub(super) fn set(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, 1)
    }

    pub(super) fn clear(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, 0)
    }

    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
        let (level, name) = option(family);
        Ok(super::unix::get_int(socket.as_raw_fd(), level, name)? != 0)
    }
}

#[cfg(windows)]
mod sys {
    use std::io;

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

    pub(super) fn set(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        set_to(socket, family, 1)
    }

    pub(super) fn clear(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        set_to(socket, family, 0)
    }

    fn set_to(socket: &impl super::AsSocket, family: Family, value: i32) -> io::Result<()> {
        let (level, name) = option(family);
        let value = value.to_ne_bytes();
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

    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
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

    pub(super) fn set(_: &impl super::AsSocket, family: Family) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("this host has no option to forbid fragmenting {family:?} datagrams"),
        ))
    }

    pub(super) fn clear(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        set(socket, family)
    }

    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
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

    // A send that tests the path's size, either kind, leaves the socket
    // forbidding fragmentation as before, for each family a socket of one
    // family or of both sends, and its datagram arrives.
    #[tokio::test]
    async fn a_size_send_leaves_the_socket_forbidding_fragmentation() {
        use crate::network::Fragmenting;

        let v4_receiver = crate::test_support::loopback_kernel_udp();
        let v6_receiver = crate::test_support::sockets()
            .bind_kernel_udp((Ipv6Addr::LOCALHOST, 0).into())
            .unwrap();
        for (bound, receiver, family) in [
            (Ipv4Addr::UNSPECIFIED.into(), &v4_receiver, Family::V4),
            (Ipv6Addr::UNSPECIFIED.into(), &v6_receiver, Family::V6),
            // A socket of both families sends to an IPv4 peer under the
            // IPv4 option.
            (Ipv6Addr::UNSPECIFIED.into(), &v4_receiver, Family::V4),
        ] {
            let bound: std::net::IpAddr = bound;
            let socket = crate::test_support::sockets()
                .bind_kernel_utp((bound, 0).into(), super::STREAM_WINDOW)
                .unwrap();
            let before = dont_fragment(socket.socket(), family).ok();
            let to = receiver.bind_addr();
            for fragmenting in [Fragmenting::Allowed, Fragmenting::Never] {
                let parts = [IoSlice::new(b"size"), IoSlice::new(b" test")];
                let sent = std::future::poll_fn(|cx| {
                    socket.poll_send_to_vectored_fragmenting(cx, &parts, to, fragmenting)
                })
                .await
                .unwrap();
                assert_eq!(sent, 9);
                let mut buf = [0u8; 16];
                let (len, _) = receiver.recv_from(&mut buf).await.unwrap();
                assert_eq!(
                    &buf[..len],
                    b"size test",
                    "{bound} {family:?} {fragmenting:?}"
                );
                assert_eq!(
                    dont_fragment(socket.socket(), family).ok(),
                    before,
                    "{bound} {family:?} {fragmenting:?}"
                );
            }
            // Where the host has the option, the socket forbids it. Apple's
            // hosts refuse the IPv4 option on a socket of both families.
            if !(cfg!(target_vendor = "apple") && bound.is_ipv6() && matches!(family, Family::V4)) {
                assert_eq!(before, Some(true), "{bound} {family:?}");
            }
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
