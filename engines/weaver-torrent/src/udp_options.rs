// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a kernel datagram socket is set to before uTP runs over it: its
//! receive buffer holds a full receive window for every connection the
//! session may hold, as far as the host allows, so a burst across them waits
//! in the kernel instead of being dropped while the engine is busy. Its
//! datagrams keep the host's default for fragmenting; only a path-MTU probe
//! to an IPv4 peer goes with don't-fragment, set around its one send, so a
//! probe larger than the path is dropped rather than split (libtorrent
//! utp_stream.cpp v2.0.15:1810-1812, udp_socket.cpp v2.0.15:159-180,
//! 346-348).

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
/// `receive_buffer` bytes. The setting is not needed for the socket to work:
/// a buffer the host refuses is logged, and the socket is used as it is. Its
/// fragmenting is left at the host's default, as libtorrent leaves it.
pub(crate) fn set_for_utp(socket: &impl AsSocket, requested: SocketAddr, receive_buffer: usize) {
    let socket = socket2::SockRef::from(socket);
    let before = socket.recv_buffer_size().ok();
    match raise_receive_buffer(&socket, receive_buffer) {
        Ok(now) if before != Some(now) => {
            debug!(
                %requested,
                ?before,
                now,
                asked = receive_buffer,
                "the socket's receive buffer was raised"
            );
        }
        Ok(now) => debug!(
            %requested,
            now,
            asked = receive_buffer,
            "the socket's receive buffer was kept"
        ),
        Err(error) => {
            debug!(%requested, ?before, asked = receive_buffer, %error, "the socket's receive buffer was not raised");
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

/// Whether `socket`'s datagrams of `family` are never fragmented.
#[cfg(test)]
pub(crate) fn dont_fragment(socket: &impl AsSocket, family: Family) -> io::Result<bool> {
    sys::get(socket, family)
}

/// Run `send`, one send of a size probe of `family` on `socket`, with
/// don't-fragment set for it alone: set before the send and cleared after,
/// whatever the send's outcome, as libtorrent sets it around one send
/// (udp_socket.cpp v2.0.15:159-180). Only a probe to an IPv4 peer carries
/// it (udp_socket.cpp v2.0.15:347-348); one to an IPv6 peer goes as any
/// other datagram. The caller holds the socket's lock for sends that test
/// the path's size, so a datagram given up on as too large never goes out
/// while it is set. Where the host refuses the option, the probe goes as
/// any other datagram, as libtorrent ignores that error.
pub(crate) fn send_probe(
    socket: &impl AsSocket,
    family: Family,
    send: impl FnOnce() -> io::Result<usize>,
) -> io::Result<usize> {
    if matches!(family, Family::V6) {
        return send();
    }
    if let Err(error) = sys::set(socket, family) {
        debug!(?family, %error, "a size probe goes without don't-fragment");
        return send();
    }
    let sent = send();
    if let Err(error) = sys::clear(socket, family) {
        warn!(?family, %error, "the socket's datagrams may not be fragmented");
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

// The casts here hand the host struct sizes, address families and counts
// bounded by a fixed array, each far inside the C type it goes to.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(clippy::cast_possible_truncation)]
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
            libc::CMSG_DATA(header)
                .cast::<u16>()
                .write_unaligned(segment);
            libc::sendmsg(fd, &raw const msg, 0)
        };
        if sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(sent as usize)
        }
    }
}

/// The most datagrams one [`recv_many`] returns and one [`send_many`]
/// carries.
pub(crate) const RECV_MANY_DATAGRAMS: usize = 64;
pub(crate) const SEND_MANY_DATAGRAMS: usize = 32;

/// Whether one system call receives several datagrams, and sends several
/// to several addresses, on this host: Linux (recvmmsg and sendmmsg).
pub(crate) const TAKES_MANY: bool = cfg!(any(target_os = "linux", target_os = "android"));

/// Receives the datagrams that wait on `socket` in one system call, at
/// most `into.len()` and [`RECV_MANY_DATAGRAMS`] of them, each into its
/// entry's buffer, with its length and sender. What of a datagram does not
/// fit its entry's buffer is read into `spill`, which the entries share:
/// at most `spill.len()` bytes of a datagram are read in all, and the last
/// long datagram of a call is whole. One before it in the call, and one
/// cut short, read as `spill.len()` bytes. How many came.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn recv_many(
    socket: &impl AsSocket,
    into: &mut [weaver_utp::Received],
    spill: &mut [u8],
) -> io::Result<usize> {
    many::recv(socket.as_raw_fd(), into, spill)
}

/// Sends datagrams that lie end to end in `bytes` in one system call:
/// `parts` gives each one's length and address, at most
/// [`SEND_MANY_DATAGRAMS`] of them are sent. How many went, counted from
/// the first; it fails when the first could not be sent.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn send_many(
    socket: &impl AsSocket,
    bytes: &[u8],
    parts: &[(usize, SocketAddr)],
) -> io::Result<usize> {
    many::send(socket.as_raw_fd(), bytes, parts)
}

// The casts here hand the host struct sizes, address families and counts
// bounded by a fixed array, each far inside the C type it goes to.
#[cfg(any(target_os = "linux", target_os = "android"))]
#[allow(clippy::cast_possible_truncation)]
mod many {
    use std::{
        io, mem,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
        os::fd::RawFd,
    };

    use nix::libc;

    use super::{RECV_MANY_DATAGRAMS, SEND_MANY_DATAGRAMS};

    // An address of either family fits the IPv6 one's room.
    type Name = libc::sockaddr_in6;

    // Writes `addr` as the host names it; its length.
    fn name(addr: SocketAddr, out: &mut Name) -> libc::socklen_t {
        match addr {
            SocketAddr::V4(v4) => {
                let named = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: `out` is valid for writes of an IPv6 address,
                // which is larger than an IPv4 one and aligned at least as
                // strictly.
                unsafe {
                    std::ptr::from_mut(out)
                        .cast::<libc::sockaddr_in>()
                        .write(named)
                };
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            SocketAddr::V6(v6) => {
                *out = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        }
    }

    // The address the host wrote, when it is an IP one.
    fn named(name: &Name) -> Option<SocketAddr> {
        match libc::c_int::from(name.sin6_family) {
            libc::AF_INET => {
                // SAFETY: the host wrote an IPv4 address, which is smaller
                // than `name` and aligned no more strictly; any bytes are a
                // valid one.
                let v4 = unsafe { std::ptr::from_ref(name).cast::<libc::sockaddr_in>().read() };
                Some(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(v4.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(v4.sin_port),
                )))
            }
            libc::AF_INET6 => Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(name.sin6_addr.s6_addr),
                u16::from_be(name.sin6_port),
                name.sin6_flowinfo,
                name.sin6_scope_id,
            ))),
            _ => None,
        }
    }

    pub(super) fn recv(
        fd: RawFd,
        into: &mut [weaver_utp::Received],
        spill: &mut [u8],
    ) -> io::Result<usize> {
        let most = into.len().min(RECV_MANY_DATAGRAMS);
        // SAFETY: message headers, buffer descriptions and addresses are
        // plain data for which all zeroes is a valid value.
        let mut names: [Name; RECV_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let mut bufs: [[libc::iovec; 2]; RECV_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let mut msgs: [libc::mmsghdr; RECV_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let spill_at = spill.as_mut_ptr();
        for (i, entry) in into[..most].iter_mut().enumerate() {
            bufs[i] = [
                libc::iovec {
                    iov_base: entry.head.as_mut_ptr().cast(),
                    iov_len: entry.head.len(),
                },
                // Every datagram's second buffer is the one `spill`: the
                // host fills the datagrams in order, so a long one's rest
                // stays there unless a later long one follows it.
                libc::iovec {
                    iov_base: spill_at.cast(),
                    iov_len: spill.len().saturating_sub(entry.head.len()),
                },
            ];
            let msg = &mut msgs[i].msg_hdr;
            msg.msg_name = (&raw mut names[i]).cast();
            msg.msg_namelen = mem::size_of::<Name>() as libc::socklen_t;
            msg.msg_iov = bufs[i].as_mut_ptr();
            msg.msg_iovlen = 2;
        }
        // SAFETY: the first `most` headers each name an address and two
        // buffers that are valid for writes of the lengths given and
        // outlive the call: the arrays above, the entries' buffers, which
        // `into` borrows exclusively, and `spill`, borrowed exclusively
        // and named by every header, which the host only writes bytes to.
        // `fd` is an open socket for the call, which does not wait.
        let got = unsafe {
            libc::recvmmsg(
                fd,
                msgs.as_mut_ptr(),
                most as libc::c_uint,
                libc::MSG_DONTWAIT as _,
                std::ptr::null_mut(),
            )
        };
        if got < 0 {
            return Err(io::Error::last_os_error());
        }
        let got = got as usize;
        // The rest of a long datagram is whole only for the last long one.
        let mut long_after = false;
        for (i, entry) in into[..got].iter_mut().enumerate().rev() {
            let cut = msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC != 0;
            let len = msgs[i].msg_len as usize;
            let long = len > entry.head.len();
            match named(&names[i]) {
                Some(from) => {
                    entry.len = if cut || (long && long_after) {
                        spill.len()
                    } else {
                        len
                    };
                    entry.from = from;
                }
                // A datagram from no IP address is read as an empty one,
                // which is no packet.
                None => entry.len = 0,
            }
            long_after |= long;
        }
        Ok(got)
    }

    pub(super) fn send(
        fd: RawFd,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> io::Result<usize> {
        let most = parts.len().min(SEND_MANY_DATAGRAMS);
        // SAFETY: message headers, buffer descriptions and addresses are
        // plain data for which all zeroes is a valid value.
        let mut names: [Name; SEND_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let mut bufs: [libc::iovec; SEND_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let mut msgs: [libc::mmsghdr; SEND_MANY_DATAGRAMS] = unsafe { mem::zeroed() };
        let mut at = 0;
        for (i, (len, target)) in parts[..most].iter().enumerate() {
            let datagram = bytes.get(at..at + len).ok_or(io::ErrorKind::InvalidInput)?;
            at += len;
            bufs[i] = libc::iovec {
                iov_base: datagram.as_ptr().cast_mut().cast(),
                iov_len: datagram.len(),
            };
            let msg = &mut msgs[i].msg_hdr;
            msg.msg_namelen = name(*target, &mut names[i]);
            msg.msg_name = (&raw mut names[i]).cast();
            msg.msg_iov = &raw mut bufs[i];
            msg.msg_iovlen = 1;
        }
        // SAFETY: the first `most` headers each name an address and one
        // buffer inside `bytes`, all of which outlive the call, which only
        // reads them. `fd` is an open socket for the call.
        let sent = unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), most as libc::c_uint, 0) };
        if sent < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(sent as usize)
        }
    }
}

// Linux has no flag for it: libtorrent sets path-MTU discovery to probe,
// which sets don't-fragment and sends past the path size the host holds,
// and clears it to never set don't-fragment (socket.hpp v2.0.15:246-249).
#[cfg(any(target_os = "linux", target_os = "android"))]
mod sys {
    use std::io;

    use nix::libc;

    use super::Family;

    // The option's level and name, and its values for set and cleared.
    fn option(family: Family) -> (libc::c_int, libc::c_int, libc::c_int, libc::c_int) {
        match family {
            Family::V4 => (
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                libc::IP_PMTUDISC_PROBE,
                libc::IP_PMTUDISC_DONT,
            ),
            Family::V6 => (
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                libc::IPV6_PMTUDISC_PROBE,
                libc::IPV6_PMTUDISC_DONT,
            ),
        }
    }

    pub(super) fn set(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name, set, _) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, set)
    }

    pub(super) fn clear(socket: &impl super::AsSocket, family: Family) -> io::Result<()> {
        let (level, name, _, cleared) = option(family);
        super::unix::set_int(socket.as_raw_fd(), level, name, cleared)
    }

    // Set when the host sets don't-fragment on every datagram: probing, or
    // discovering the path's size and never fragmenting.
    #[cfg(test)]
    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
        let (level, name, set, _) = option(family);
        let value = super::unix::get_int(socket.as_raw_fd(), level, name)?;
        let never = match family {
            Family::V4 => libc::IP_PMTUDISC_DO,
            Family::V6 => libc::IPV6_PMTUDISC_DO,
        };
        Ok(value == set || value == never)
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

    #[cfg(test)]
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
        setsockopt,
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

    // The socket's handle, which is pointer sized: a `SOCKET` is a `UINT_PTR`.
    fn handle(socket: &impl super::AsSocket) -> io::Result<SOCKET> {
        usize::try_from(socket.as_raw_socket())
            .map(SOCKET)
            .map_err(|_| io::Error::other("a socket handle wider than a pointer"))
    }

    fn set_to(socket: &impl super::AsSocket, family: Family, value: i32) -> io::Result<()> {
        let (level, name) = option(family);
        let value = value.to_ne_bytes();
        // SAFETY: the socket is open for as long as `socket` is borrowed, and
        // the option takes an int, which `value` holds.
        let done = unsafe { setsockopt(handle(socket)?, level, name, Some(&value)) };
        if done == 0 { Ok(()) } else { Err(last_error()) }
    }

    #[cfg(test)]
    pub(super) fn get(socket: &impl super::AsSocket, family: Family) -> io::Result<bool> {
        let (level, name) = option(family);
        let mut value = [0u8; 4];
        let mut len = 4_i32;
        // SAFETY: as in `set`; `value` has room for the int the option is.
        let done = unsafe {
            windows::Win32::Networking::WinSock::getsockopt(
                handle(socket)?,
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

// A host with no such option: a probe goes as any other datagram.
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

    #[cfg(test)]
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

    #[cfg(any(test, target_os = "linux", target_os = "android"))]
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

    // Whether `socket`'s datagrams of `family` go with don't-fragment, where
    // the host can say: None where it has no option for it, or refuses it
    // on a socket of both families, as Apple's hosts do for IPv4.
    fn dont_fragment_where_held(socket: &tokio::net::UdpSocket, family: Family) -> Option<bool> {
        dont_fragment(socket, family).ok()
    }

    // A socket of both families leaves each family's datagrams at the host's
    // default, which lets them fragment (libtorrent sets no option when it
    // opens one: udp_socket.cpp v2.0.15:159-180 sets it for a probe alone).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_dualstack_socket_lets_each_familys_datagrams_fragment() {
        let socket = crate::test_support::sockets()
            .bind_kernel_utp((Ipv6Addr::UNSPECIFIED, 0).into(), super::STREAM_WINDOW)
            .unwrap();
        assert!(!socket2::SockRef::from(socket.socket()).only_v6().unwrap());
        for family in [Family::V4, Family::V6] {
            assert_ne!(
                dont_fragment_where_held(socket.socket(), family),
                Some(true),
                "{family:?}"
            );
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
            // An ordinary datagram may be fragmented: the socket keeps the
            // host's default, as a fresh one does.
            let held = dont_fragment_where_held(socket.socket(), family);
            assert_ne!(held, Some(true), "{family:?}");
            assert_eq!(
                held,
                dont_fragment(&fresh(family), family).ok(),
                "{family:?}"
            );
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
    // letting its datagrams fragment as before, for each family a socket of
    // one family or of both sends, and its datagram arrives.
    #[tokio::test]
    async fn a_size_send_leaves_the_socket_letting_datagrams_fragment() {
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
            let before = dont_fragment_where_held(socket.socket(), family);
            assert_ne!(before, Some(true), "{bound} {family:?}");
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
                    dont_fragment_where_held(socket.socket(), family),
                    before,
                    "{bound} {family:?} {fragmenting:?}"
                );
            }
        }
    }

    // libtorrent sends a probe with don't-fragment set around its one send,
    // to an IPv4 peer alone (utp_stream.cpp v2.0.15:1810-1812, udp_socket.cpp
    // v2.0.15:159-180, 347-348). A probe to an IPv4 peer is sent forbidding
    // fragmentation, and the socket lets datagrams fragment again after it,
    // whatever the send's outcome; one to an IPv6 peer is sent as any other.
    #[tokio::test]
    async fn only_a_probe_to_an_ipv4_peer_is_sent_forbidding_fragmentation() {
        let v4_receiver = crate::test_support::loopback_kernel_udp();
        let v6_receiver = crate::test_support::sockets()
            .bind_kernel_udp((Ipv6Addr::LOCALHOST, 0).into())
            .unwrap();
        for (bound, receiver, family) in [
            (Ipv4Addr::UNSPECIFIED.into(), &v4_receiver, Family::V4),
            (Ipv6Addr::UNSPECIFIED.into(), &v6_receiver, Family::V6),
            (Ipv6Addr::UNSPECIFIED.into(), &v4_receiver, Family::V4),
        ] {
            let bound: std::net::IpAddr = bound;
            let socket = crate::test_support::sockets()
                .bind_kernel_utp((bound, 0).into(), super::STREAM_WINDOW)
                .unwrap();
            let to = socket2::SockAddr::from(match (bound, receiver.bind_addr()) {
                (std::net::IpAddr::V6(_), SocketAddr::V4(v4)) => {
                    SocketAddr::from((v4.ip().to_ipv6_mapped(), v4.port()))
                }
                (_, to) => to,
            });
            let before = dont_fragment_where_held(socket.socket(), family);
            for outcome in [Ok(()), Err(())] {
                let mut during = None;
                let sent = super::send_probe(socket.socket(), family, || {
                    during = Some(dont_fragment_where_held(socket.socket(), family));
                    let sent = socket2::SockRef::from(socket.socket()).send_to(b"probe", &to)?;
                    outcome
                        .map(|()| sent)
                        .map_err(|()| std::io::Error::other("refused"))
                });
                assert_eq!(sent.is_ok(), outcome.is_ok());
                let mut buf = [0u8; 16];
                let (len, _) = receiver.recv_from(&mut buf).await.unwrap();
                assert_eq!(&buf[..len], b"probe");
                let during = during.expect("the probe was sent");
                match family {
                    // Where the host has the option, set for the probe.
                    Family::V4 if before.is_some() => {
                        assert_eq!(during, Some(true), "{bound} {outcome:?}");
                    }
                    Family::V4 => assert_eq!(during, None, "{bound} {outcome:?}"),
                    Family::V6 => assert_eq!(during, before, "{bound} {outcome:?}"),
                }
                assert_ne!(
                    dont_fragment_where_held(socket.socket(), family),
                    Some(true),
                    "{bound} {family:?} {outcome:?}"
                );
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
