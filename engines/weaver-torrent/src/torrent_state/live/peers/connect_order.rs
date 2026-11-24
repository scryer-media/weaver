// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The order the peer list hands out connect candidates in (libtorrent
//! `peer_list::compare_peer`, peer_list.cpp:110-142 in 2.0.15), and the
//! two ranks it reads: where a peer came from (`source_rank`,
//! request_blocks.cpp:49-57) and BEP 40's peer priority (`peer_priority`,
//! torrent_peer.cpp:77-148).

use std::cmp::Reverse;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use tokio::time::Instant;

use super::holepunch::is_local;
use crate::type_aliases::{PeerSourceFlag, PeerSources};

/// Where a connect candidate stands; the lowest goes first. In turn: a
/// holepunched peer; the fewest failures in a row; one on a local network;
/// the lowest [`ConnectRanking::session_seconds`] of its last close, a peer
/// never connected to at 0; while we are finished, one no PEX message
/// flagged as a seed; the highest [`source_rank`]; the highest BEP 40
/// [`peer_priority`] between our address and the peer's.
pub(crate) type ConnectRank = (bool, u32, bool, u64, bool, Reverse<u8>, Reverse<u32>);

/// The endpoints peers see us at, for BEP 40 (libtorrent `external_ip`,
/// ip_voter.hpp:113-132, with `torrent_state::port`): one address for each
/// of a peer on a local network or not, IPv4 or IPv6, and our port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OurEndpoints {
    /// Indexed `[local][ipv6]`.
    pub(crate) addresses: [[IpAddr; 2]; 2],
    pub(crate) port: u16,
}

impl Default for OurEndpoints {
    /// No address known, and no port: libtorrent's `external_ip` before
    /// any is known, and `listen_port` with no listen socket.
    fn default() -> Self {
        let none = [Ipv4Addr::UNSPECIFIED.into(), Ipv6Addr::UNSPECIFIED.into()];
        Self {
            addresses: [none, none],
            port: 0,
        }
    }
}

impl OurEndpoints {
    /// Where `peer` sees us (libtorrent `external_ip::external_address`,
    /// ip_voter.cpp:188-193).
    pub(crate) fn seen_by(&self, peer: IpAddr) -> SocketAddr {
        let peer = peer.to_canonical();
        let ip = self.addresses[usize::from(is_local(peer))][usize::from(peer.is_ipv6())];
        SocketAddr::new(ip, self.port)
    }
}

/// Where the session reads the endpoints peers see us at.
type ReadOurEndpoints = Arc<dyn Fn() -> OurEndpoints + Send + Sync>;

/// What a peer list orders its connect candidates by beyond the peers
/// themselves: when the session started, which dates every close, and the
/// endpoints peers see us at.
#[derive(Clone)]
pub(crate) struct ConnectOrder {
    session_start: Instant,
    ours: ReadOurEndpoints,
}

/// A [`ConnectOrder`] as it stands for one choice of candidate: the
/// endpoints are read once for every candidate compared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConnectRanking {
    session_start: Instant,
    ours: OurEndpoints,
}

impl fmt::Debug for ConnectOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOrder")
            .field("session_start", &self.session_start)
            .finish_non_exhaustive()
    }
}

impl Default for ConnectOrder {
    /// A list no session orders: its own start dates closes, and peers see
    /// us at [`OurEndpoints::default`].
    fn default() -> Self {
        Self::new(Instant::now(), Arc::new(OurEndpoints::default))
    }
}

impl ConnectOrder {
    pub(crate) fn new(session_start: Instant, ours: ReadOurEndpoints) -> Self {
        Self {
            session_start,
            ours,
        }
    }

    /// The order as it stands now.
    pub(crate) fn now(&self) -> ConnectRanking {
        ConnectRanking {
            session_start: self.session_start,
            ours: (self.ours)(),
        }
    }
}

impl ConnectRanking {
    /// libtorrent `session_impl::session_time` (session_impl.hpp:1143-1151):
    /// the whole seconds from the session's start to `at`, plus one, so a
    /// close in the first second is told from none. None, and a time
    /// before the start, are 0, the value of a peer never connected to and
    /// of a close `peer_connection::fast_reconnect` (peer_connection.cpp:
    /// 967-981) dated back past the start.
    pub(crate) fn session_seconds(&self, at: Option<Instant>) -> u64 {
        match at {
            Some(at) if at >= self.session_start => {
                (at - self.session_start).as_secs().saturating_add(1)
            }
            _ => 0,
        }
    }

    /// BEP 40's priority between us, as `peer` sees us, and `peer`
    /// (libtorrent `torrent_peer::rank`, torrent_peer.cpp:190-199).
    pub(crate) fn peer_rank(&self, peer: SocketAddr) -> u32 {
        peer_priority(self.ours.seen_by(peer.ip()), peer)
    }
}

/// libtorrent `source_rank` (request_blocks.cpp:49-57): a tracker counts
/// most, then local discovery, the DHT, and PEX; resume data and an
/// incoming connection count nothing.
pub(crate) fn source_rank(sources: PeerSources) -> u8 {
    [
        (PeerSourceFlag::Tracker, 1 << 5),
        (PeerSourceFlag::Lsd, 1 << 4),
        (PeerSourceFlag::Dht, 1 << 3),
        (PeerSourceFlag::Pex, 1 << 2),
    ]
    .into_iter()
    .filter(|(source, _)| sources.contains(*source))
    .fold(0, |rank, (_, bit)| rank | bit)
}

/// BEP 40's canonical peer priority of two endpoints of one address family
/// (libtorrent `peer_priority`, torrent_peer.cpp:77-148): the CRC-32C of
/// their ports when the addresses are one, else of both addresses, lowest
/// first, masked by how much of them they share.
pub(crate) fn peer_priority(e1: SocketAddr, e2: SocketAddr) -> u32 {
    let canonical = |e: SocketAddr| SocketAddr::new(e.ip().to_canonical(), e.port());
    let (mut e1, mut e2) = (canonical(e1), canonical(e2));
    if e1.ip() == e2.ip() {
        let (low, high) = if e1.port() > e2.port() {
            (e2.port(), e1.port())
        } else {
            (e1.port(), e2.port())
        };
        let mut buf = [0; 4];
        buf[..2].copy_from_slice(&low.to_be_bytes());
        buf[2..].copy_from_slice(&high.to_be_bytes());
        return crc32c(&buf);
    }
    if e1 > e2 {
        std::mem::swap(&mut e1, &mut e2);
    }
    match (e1.ip(), e2.ip()) {
        (IpAddr::V6(a1), IpAddr::V6(a2)) => {
            let (mut b1, mut b2) = (a1.octets(), a2.octets());
            // The first six bytes are never masked; past the first byte
            // the two differ in, every byte after the next is.
            let mut offset = None;
            for i in 0..b1.len() {
                match offset {
                    None if b1[i] != b2[i] => offset = Some((i + 1).max(5)),
                    Some(at) if i > at => {
                        b1[i] &= 0x55;
                        b2[i] &= 0x55;
                    }
                    _ => {}
                }
            }
            let mut buf = [0; 32];
            buf[..16].copy_from_slice(&b1);
            buf[16..].copy_from_slice(&b2);
            crc32c(&buf)
        }
        (a1, a2) => {
            let v4 = |a: IpAddr| match a {
                IpAddr::V4(a) => a.octets(),
                IpAddr::V6(a) => a.to_ipv4_mapped().unwrap_or(Ipv4Addr::UNSPECIFIED).octets(),
            };
            let (b1, b2) = (v4(a1), v4(a2));
            let mask: [u8; 4] = if b1[..2] != b2[..2] {
                [0xff, 0xff, 0x55, 0x55]
            } else if b1[..3] != b2[..3] {
                [0xff, 0xff, 0xff, 0x55]
            } else {
                [0xff; 4]
            };
            let mut buf = [0; 8];
            for i in 0..4 {
                buf[i] = b1[i] & mask[i];
                buf[4 + i] = b2[i] & mask[i];
            }
            crc32c(&buf)
        }
    }
}

// CRC-32C (Castagnoli), reflected, as libtorrent's `crc32c` and
// `crc32c_32` compute it over the bytes in memory (crc32c.cpp).
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i: u32 = 0;
    while i < 256 {
        let mut crc = i;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
};

fn crc32c(bytes: &[u8]) -> u32 {
    !bytes.iter().fold(!0u32, |crc, &b| {
        CRC32C_TABLE[usize::from(crc.to_le_bytes()[0] ^ b)] ^ (crc >> 8)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(s: &str, port: u16) -> SocketAddr {
        SocketAddr::new(s.parse().unwrap(), port)
    }

    fn hash_hex(hex: &str) -> u32 {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        crc32c(&bytes)
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    // libtorrent test/test_peer_priority.cpp, `peer_priority`, every case.
    #[test]
    fn peer_priority_matches_libtorrents_vectors() {
        let cases = [
            (
                ep("230.12.123.3", 0x4d2),
                ep("230.12.123.3", 0x12c),
                "012c04d2",
            ),
            (
                ep("230.12.123.1", 0x4d2),
                ep("230.12.123.3", 0x12c),
                "e60c7b01e60c7b03",
            ),
            (
                ep("230.12.23.1", 0x4d2),
                ep("230.12.123.3", 0x12c),
                "e60c1701e60c7b01",
            ),
            (
                ep("230.120.23.1", 0x4d2),
                ep("230.12.123.3", 0x12c),
                "e60c5101e6781501",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
                "012c04d2",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                "012c04d2",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                ep("ffff:0fff:ffff:ffff:ffff:ffff:ffff:ffff", 0x12c),
                "ffff0fffffff55555555555555555555ffffffffffff55555555555555555555",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                ep("ffff:ffff:0fff:ffff:ffff:ffff:ffff:ffff", 0x12c),
                "ffffffff0fff55555555555555555555ffffffffffff55555555555555555555",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                ep("ffff:ffff:ff0f:ffff:ffff:ffff:ffff:ffff", 0x12c),
                "ffffffffff0fff555555555555555555ffffffffffffff555555555555555555",
            ),
            (
                ep("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0x4d2),
                ep("ffff:ffff:ffff:0fff:ffff:ffff:ffff:ffff", 0x12c),
                "ffffffffffff0fff5555555555555555ffffffffffffffff5555555555555555",
            ),
        ];
        for (e1, e2, hex) in cases {
            assert_eq!(peer_priority(e1, e2), hash_hex(hex), "{e1} {e2}");
        }
        // BEP 40's own vectors.
        assert_eq!(
            peer_priority(ep("123.213.32.10", 0), ep("98.76.54.32", 0)),
            0xec2d_7224
        );
        assert_eq!(
            peer_priority(ep("123.213.32.10", 0), ep("123.213.32.234", 0)),
            0x9956_8189
        );
    }

    // libtorrent `external_ip::external_address`: the address of the
    // peer's locality and family, the unspecified IPv6 one for an IPv6 peer
    // while none is known.
    #[test]
    fn a_peer_sees_us_at_the_address_of_its_locality_and_family() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let ours = OurEndpoints {
            addresses: [
                [ip("198.51.100.1"), ip("2001:db8::1")],
                [ip("192.168.1.2"), Ipv6Addr::UNSPECIFIED.into()],
            ],
            port: 6881,
        };
        assert_eq!(ours.seen_by(ip("203.0.113.9")), ep("198.51.100.1", 6881));
        assert_eq!(
            ours.seen_by(ip("::ffff:203.0.113.9")),
            ep("198.51.100.1", 6881)
        );
        assert_eq!(ours.seen_by(ip("2001:db8::9")), ep("2001:db8::1", 6881));
        assert_eq!(ours.seen_by(ip("192.168.1.9")), ep("192.168.1.2", 6881));
        assert_eq!(ours.seen_by(ip("fd00::9")), ep("::", 6881));
    }

    #[test]
    fn source_rank_weighs_a_tracker_over_lsd_over_the_dht_over_pex() {
        let rank = |sources: &[PeerSourceFlag]| source_rank(sources.iter().copied().collect());
        assert_eq!(rank(&[PeerSourceFlag::Tracker]), 32);
        assert_eq!(rank(&[PeerSourceFlag::Lsd]), 16);
        assert_eq!(rank(&[PeerSourceFlag::Dht]), 8);
        assert_eq!(rank(&[PeerSourceFlag::Pex]), 4);
        assert_eq!(rank(&[PeerSourceFlag::ResumeData]), 0);
        assert_eq!(rank(&[PeerSourceFlag::Incoming]), 0);
        assert_eq!(rank(&[PeerSourceFlag::Dht, PeerSourceFlag::Pex]), 12);
    }

    // libtorrent `session_impl::session_time` counts from 1 in the first
    // second, and `fast_reconnect` clamps a date-back past the start to 0.
    #[tokio::test(start_paused = true)]
    async fn session_seconds_count_from_one_and_clamp_before_the_start() {
        let start = Instant::now() + std::time::Duration::from_secs(1000);
        let order = ConnectOrder::new(start, Arc::new(OurEndpoints::default)).now();
        let secs = std::time::Duration::from_secs;
        assert_eq!(order.session_seconds(None), 0);
        assert_eq!(order.session_seconds(Some(start - secs(1))), 0);
        assert_eq!(order.session_seconds(Some(start)), 1);
        assert_eq!(
            order.session_seconds(Some(start + std::time::Duration::from_millis(999))),
            1
        );
        assert_eq!(order.session_seconds(Some(start + secs(180))), 181);
    }
}
