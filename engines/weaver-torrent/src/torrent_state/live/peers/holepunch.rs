// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The peer list's part in the holepunch extension (BEP 55): peers a
//! holepunch introduced, and the connected peer that can introduce us to
//! one we failed to reach.

use std::net::{IpAddr, SocketAddr};

use super::{Displace, PeerStates};
use crate::torrent_state::live::peer::{Peer, PeerTx};

// libtorrent `pex_holepunch`: the PEX flag of a peer that speaks the
// holepunch extension.
pub(crate) const PEX_HOLEPUNCH: u8 = 0x08;

impl PeerStates {
    /// A holepunch introduced the peer at `addr`: it joins the list as a
    /// newcomer would, or the known one is connected to next, unless we are
    /// connected or connecting to it already (libtorrent
    /// `bt_peer_connection::on_holepunch`, `connect`). A banned peer is
    /// turned away at admission.
    pub fn holepunch(&self, addr: SocketAddr) {
        self.admit(
            addr,
            Displace::NonCandidates,
            |peer| peer.holepunch(self),
            || Some(Peer::new_holepunched(addr)),
        );
        self.candidates_changed.notify_one();
    }

    /// A PEX message named the peer at `addr` with `flags`: one that speaks
    /// the holepunch extension is marked so (libtorrent
    /// `peer_list::add_peer`, `update_peer`).
    pub fn pex_flags(&self, addr: SocketAddr, flags: u8) {
        if flags & PEX_HOLEPUNCH != 0 {
            self.with_peer_mut(addr, "pex_flags", |peer| peer.supports_holepunch = true);
        }
    }

    /// The connection to `addr` failed, and the peer speaks the holepunch
    /// extension: a connected peer that speaks it too and whose PEX named
    /// `addr` can introduce us (libtorrent `torrent::find_introducer`).
    /// Returns its queue.
    pub fn introducer(&self, addr: SocketAddr) -> Option<PeerTx> {
        self.states.iter().find_map(|e| {
            let live = e.value().get_live()?;
            (live.supports_holepunch && live.was_introduced(addr)).then(|| live.tx.clone())
        })
    }
}

/// Whether `ip` is on a local network, where a holepunch neither bridges to
/// nor connects from the internet (libtorrent `aux::is_local`).
pub(crate) fn is_local(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(a) => {
            let [a0, a1, ..] = a.octets();
            a.is_private()
                || a.is_link_local()
                || a.is_loopback()
                || (a0 == 100 && (a1 & 0xc0) == 64)
        }
        IpAddr::V6(a) => {
            let s0 = a.segments()[0];
            a.is_loopback()
                || a.is_unicast_link_local()
                // Site-local, fec0::/10.
                || (s0 & 0xffc0) == 0xfec0
                // Link- and site-local multicast.
                || (a.is_multicast() && matches!(s0 & 0x000f, 0x2 | 0x5))
                || a.is_unique_local()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_local;

    // libtorrent `aux::is_local`.
    #[test]
    fn local_addresses_are_the_private_loopback_link_and_site_local_ranges() {
        for local in [
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.0.1",
            "127.0.0.1",
            "100.64.0.1",
            "100.127.255.255",
            "::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "ff05::1",
            "fc00::1",
            "fd12::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(is_local(local.parse().unwrap()), "{local}");
        }
        for global in [
            "8.8.8.8",
            "172.32.0.1",
            "100.128.0.1",
            "2001:db8::1",
            "ff0e::1",
        ] {
            assert!(!is_local(global.parse().unwrap()), "{global}");
        }
    }
}
