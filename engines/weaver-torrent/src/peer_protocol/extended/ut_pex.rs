// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};

use crate::buffers::{ByteBuf, ByteBufOwned, ByteBufT};
use crate::core::compact_ip::{
    CompactListInBuffer, CompactListInBufferOwned, CompactSerialize, CompactSerializeFixedLen,
};
use serde_derive::{Deserialize, Serialize};

// libtorrent `pex_flags.hpp`: what a PEX message says of a peer it adds.
/// The peer is a seed (libtorrent `pex_seed`).
pub const PEX_SEED: u8 = 0x02;
/// We are connected to the peer over uTP (libtorrent `pex_utp`).
pub const PEX_UTP: u8 = 0x04;
/// The peer speaks the holepunch extension (libtorrent `pex_holepunch`).
pub const PEX_HOLEPUNCH: u8 = 0x08;

#[derive(Clone, Copy)]
pub struct PexPeerInfo {
    pub flags: u8,
    pub addr: SocketAddr,
}

impl core::fmt::Debug for PexPeerInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)?;
        if self.flags != 0 {
            write!(f, ";flags={}", self.flags)?;
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq)]
struct Flags(u8);

impl CompactSerialize for Flags {
    type Slice = [u8; 1];

    fn expecting() -> &'static str {
        "1 byte"
    }

    fn as_slice(&self) -> [u8; 1] {
        [self.0]
    }

    fn from_slice(buf: &[u8]) -> Option<Self> {
        Some(Flags(*buf.first()?))
    }
}

impl CompactSerializeFixedLen for Flags {
    fn fixed_len() -> usize {
        1
    }
}

#[derive(Serialize, Default, Deserialize, Eq, PartialEq)]
pub struct UtPex<B: ByteBufT> {
    #[serde(skip_serializing_if = "Option::is_none")]
    added: Option<CompactListInBuffer<B, SocketAddrV4>>,
    #[serde(rename = "added.f")]
    #[serde(skip_serializing_if = "Option::is_none")]
    added_f: Option<CompactListInBuffer<B, Flags>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    added6: Option<CompactListInBuffer<B, SocketAddrV6>>,
    #[serde(rename = "added6.f")]
    #[serde(skip_serializing_if = "Option::is_none")]
    added6_f: Option<CompactListInBuffer<B, Flags>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped: Option<CompactListInBuffer<B, SocketAddrV4>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped6: Option<CompactListInBuffer<B, SocketAddrV6>>,
}

struct IterDebug<I>(I);
impl<I> core::fmt::Debug for IterDebug<I>
where
    I: Iterator<Item = PexPeerInfo> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.clone()).finish()
    }
}

impl<ByteBuf: ByteBufT> core::fmt::Debug for UtPex<ByteBuf> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtPex")
            .field("added", &IterDebug(self.as_borrowed().added_peers()))
            .field("dropped", &IterDebug(self.as_borrowed().dropped_peers()))
            .finish()
    }
}

impl<B: ByteBufT> UtPex<B> {
    pub fn as_borrowed(&self) -> UtPex<ByteBuf<'_>> {
        UtPex {
            added: self.added.as_ref().map(CompactListInBuffer::as_borrowed),
            added_f: self.added_f.as_ref().map(CompactListInBuffer::as_borrowed),
            added6: self.added6.as_ref().map(CompactListInBuffer::as_borrowed),
            added6_f: self.added6_f.as_ref().map(CompactListInBuffer::as_borrowed),
            dropped: self.dropped.as_ref().map(CompactListInBuffer::as_borrowed),
            dropped6: self.dropped6.as_ref().map(CompactListInBuffer::as_borrowed),
        }
    }
}

impl<'a> UtPex<ByteBuf<'a>> {
    fn added_peers_inner<T: CompactSerialize + CompactSerializeFixedLen + Into<SocketAddr>>(
        &self,
        buf: &Option<CompactListInBuffer<ByteBuf<'a>, T>>,
        flags: &Option<CompactListInBuffer<ByteBuf<'a>, Flags>>,
    ) -> impl Iterator<Item = PexPeerInfo> + Clone {
        buf.iter()
            .flat_map(|l| l.iter())
            .enumerate()
            .map(|(idx, ip)| PexPeerInfo {
                flags: flags
                    .as_ref()
                    .and_then(|f| f.get(idx).map(|f| f.0))
                    .unwrap_or(0),
                addr: ip.into(),
            })
    }

    pub fn added_peers(&self) -> impl Iterator<Item = PexPeerInfo> + Clone {
        self.added_peers_inner(&self.added, &self.added_f)
            .chain(self.added_peers_inner(&self.added6, &self.added6_f))
    }

    pub fn dropped_peers(&self) -> impl Iterator<Item = PexPeerInfo> + Clone {
        self.added_peers_inner(&self.dropped, &None)
            .chain(self.added_peers_inner(&self.dropped6, &None))
    }
}

impl UtPex<ByteBufOwned> {
    /// A PEX message adding `added`, each with its flags, and dropping
    /// `dropped` (BEP 11).
    pub fn from_peers(
        added: impl Iterator<Item = PexPeerInfo> + Clone,
        dropped: impl Iterator<Item = SocketAddr> + Clone,
    ) -> Self {
        fn list<T: CompactSerialize + CompactSerializeFixedLen>(
            it: impl Iterator<Item = T>,
        ) -> Option<CompactListInBufferOwned<T>> {
            let l = CompactListInBufferOwned::new_from_iter(it);
            if l.is_empty() { None } else { Some(l) }
        }
        let v4 = |p: &PexPeerInfo| match p.addr {
            SocketAddr::V4(a) => Some((a, Flags(p.flags))),
            SocketAddr::V6(_) => None,
        };
        let v6 = |p: &PexPeerInfo| match p.addr {
            SocketAddr::V6(a) => Some((a, Flags(p.flags))),
            SocketAddr::V4(_) => None,
        };
        let added4 = added.clone().filter_map(|p| v4(&p));
        let added6 = added.filter_map(|p| v6(&p));

        Self {
            added: list(added4.clone().map(|(a, _)| a)),
            added_f: list(added4.map(|(_, f)| f)),
            added6: list(added6.clone().map(|(a, _)| a)),
            added6_f: list(added6.map(|(_, f)| f)),
            dropped: list(dropped.clone().filter_map(|a| match a {
                SocketAddr::V4(a) => Some(a),
                SocketAddr::V6(_) => None,
            })),
            dropped6: list(dropped.filter_map(|a| match a {
                SocketAddr::V6(a) => Some(a),
                SocketAddr::V4(_) => None,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bencode::{bencode_serialize_to_writer, from_bytes};
    use crate::buffers::ByteBuf;

    use super::*;

    fn decode_hex(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2));
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_pex_deserialization() {
        let msg = "64353a616464656431323ab99f9d14b56797f969861090373a61646465642e66323a0c00363a616464656436303a383a6164646564362e66303a373a64726f70706564303a383a64726f7070656436303a65";
        let bytes = decode_hex(msg);
        let pex = from_bytes::<UtPex<ByteBuf>>(&bytes).unwrap();
        let addrs: Vec<_> = pex.added_peers().collect();
        assert_eq!(2, addrs.len());
        assert_eq!(
            "185.159.157.20:46439".parse::<SocketAddr>().unwrap(),
            addrs[0].addr
        );
        assert_eq!(12, addrs[0].flags);
        assert_eq!(
            "151.249.105.134:4240".parse::<SocketAddr>().unwrap(),
            addrs[1].addr
        );
        assert_eq!(0, addrs[1].flags);
    }

    #[test]
    fn test_pex_roundtrip() {
        let a1 = "185.159.157.20:46439".parse::<SocketAddr>().unwrap();
        let a2 = "151.249.105.134:4240".parse::<SocketAddr>().unwrap();
        //IPV6
        let aa1 = "[5be8:dde9:7f0b:d5a7:bd01:b3be:9c69:573b]:46439"
            .parse::<SocketAddr>()
            .unwrap();
        let aa2 = "[f16c:f7ec:cfa2:e1c5:9a3c:cb08:801f:36b8]:4240"
            .parse::<SocketAddr>()
            .unwrap();

        let addrs = [a1, aa1, a2, aa2];
        let added = addrs.iter().zip([PEX_SEED, PEX_UTP, 0, PEX_HOLEPUNCH]);
        let pex = UtPex::from_peers(
            added.map(|(&addr, flags)| PexPeerInfo { flags, addr }),
            addrs.iter().copied(),
        );
        let pex = pex.as_borrowed();
        let mut bytes = Vec::new();
        bencode_serialize_to_writer(&pex, &mut bytes).unwrap();
        let pex2 = from_bytes::<UtPex<ByteBuf>>(&bytes).unwrap();
        assert_eq!(4, pex2.added_peers().count());
        assert_eq!(pex.added_peers().count(), pex2.added_peers().count());
        let addrs2: Vec<_> = pex2.added_peers().map(|p| (p.addr, p.flags)).collect();
        assert_eq!(
            addrs2,
            [
                (a1, PEX_SEED),
                (a2, 0),
                (aa1, PEX_UTP),
                (aa2, PEX_HOLEPUNCH)
            ]
        );
        let addrs2: Vec<_> = pex2.dropped_peers().collect();
        assert_eq!(a1, addrs2[0].addr);
        assert_eq!(a2, addrs2[1].addr);
        assert_eq!(aa1, addrs2[2].addr);
        assert_eq!(aa2, addrs2[3].addr);
    }
}
