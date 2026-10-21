// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::clone_to_owned::CloneToOwned;
use bytes::Bytes;
use data_encoding::BASE32;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id<const N: usize>(pub [u8; N]);

impl<const N: usize> Id<N> {
    pub fn new(from: [u8; N]) -> Id<N> {
        Id(from)
    }

    pub fn as_string(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_bytes(b: &[u8]) -> anyhow::Result<Self> {
        let mut v = [0u8; N];
        if b.len() != N {
            anyhow::bail!("buffer length must be {}, but it's {}", N, b.len());
        }
        v.copy_from_slice(b);
        Ok(Id(v))
    }

    pub fn distance(&self, other: &Id<N>) -> Id<N> {
        let mut xor = [0u8; N];
        for (idx, (s, o)) in self
            .0
            .iter()
            .copied()
            .zip(other.0.iter().copied())
            .enumerate()
        {
            xor[idx] = s ^ o;
        }
        Id(xor)
    }
    pub fn get_bit(&self, bit: u8) -> bool {
        let n = self.0[(bit / 8) as usize];
        let mask = 1 << (7 - bit % 8);
        n & mask > 0
    }

    pub fn set_bit(&mut self, bit: u8, value: bool) {
        let n = &mut self.0[(bit / 8) as usize];
        if value {
            *n |= 1 << (7 - bit % 8)
        } else {
            let mask = !(1 << (7 - bit % 8));
            *n &= mask;
        }
    }
    pub fn set_bits_range(&mut self, r: std::ops::Range<u8>, value: bool) {
        for bit in r {
            self.set_bit(bit, value)
        }
    }
}

impl<const N: usize> CloneToOwned for Id<N> {
    type Target = Id<N>;

    fn clone_to_owned(&self, _within_buffer: Option<&Bytes>) -> Self::Target {
        *self
    }
}

impl<const N: usize> Default for Id<N> {
    fn default() -> Self {
        Id([0; N])
    }
}

impl<const N: usize> std::fmt::Debug for Id<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x?}")?;
        }
        Ok(())
    }
}

impl<const N: usize> FromStr for Id<N> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0u8; N];
        let base32_encoded_size = (N as f64 / 5f64).ceil() as usize * 8;
        if s.len() == N * 2 {
            hex::decode_to_slice(s, &mut out)?;
            Ok(Id(out))
            // try decode as base32
        } else if s.len() == base32_encoded_size {
            match BASE32.decode(s.as_bytes()) {
                Ok(decoded) => {
                    out.copy_from_slice(&decoded);
                    Ok(Id(out))
                }
                Err(err) => {
                    anyhow::bail!("fail to decode base32 string {}: {}", s, err)
                }
            }
        } else {
            anyhow::bail!(
                "expected a hex string of length {} or {}",
                N * 2,
                base32_encoded_size
            );
        }
    }
}

impl<const N: usize> Serialize for Id<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de, const N: usize> Deserialize<'de> for Id<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IdVisitor<const N: usize>;

        impl<'de, const N: usize> serde::de::Visitor<'de> for IdVisitor<N> {
            type Value = Id<N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter
                    .write_str("a byte array of length ")
                    .and_then(|_| formatter.write_fmt(format_args!("{N}")))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v.len() != N * 2 {
                    return Err(E::invalid_length(40, &self));
                }
                let mut out = [0u8; N];
                match hex::decode_to_slice(v, &mut out) {
                    Ok(_) => Ok(Id(out)),
                    Err(e) => Err(E::custom(e)),
                }
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_bytes(v)
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v.len() != N {
                    return Err(E::invalid_length(N, &self));
                }
                let mut buf = [0u8; N];
                buf.copy_from_slice(v);
                Ok(Id(buf))
            }
        }

        deserializer.deserialize_any(IdVisitor {})
    }
}

/// A 20-byte hash used throughout librqbit, for torrent info hashes, peer ids etc.
pub type Id20 = Id<20>;
/// A 32-byte hash used in Bittorrent V2, for torrent info hashes, piece hashing, etc.
pub type Id32 = Id<32>;

impl Id32 {
    /// Truncate to 20 bytes for DHT and tracker compatibility (BEP 52).
    ///
    /// DHT uses 20-byte node IDs, and BEP 3 handshakes use 20-byte info hashes.
    /// For v2-only torrents, the SHA-256 info hash is truncated to 20 bytes.
    pub fn truncate_for_dht(&self) -> Id20 {
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&self.0[..20]);
        Id20::new(bytes)
    }
}

/// A torrent's info hashes: SHA-1 of its info dict when it has a v1
/// listing, SHA-256 when it is v2 (libtorrent `info_hash_t`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct InfoHashes {
    pub v1: Option<Id20>,
    pub v2: Option<Id32>,
}

impl InfoHashes {
    /// The 20-byte hash Weaver knows the torrent by: the v1 hash, else the
    /// truncated v2 hash. libtorrent's `info_hash_t::get_best` prefers v2;
    /// Weaver keeps v1 so that a hybrid keeps the identity it had when only
    /// its v1 half was read.
    pub fn best(&self) -> Id20 {
        self.v1
            .or_else(|| self.v2.map(|v2| v2.truncate_for_dht()))
            .unwrap_or_default()
    }

    /// Every 20-byte hash the torrent answers to and announces under: v1
    /// and the truncated v2 (libtorrent `info_hash_t::for_each`).
    pub fn ids20(&self) -> impl Iterator<Item = Id20> + use<> {
        self.v1
            .into_iter()
            .chain(self.v2.map(|v2| v2.truncate_for_dht()))
    }

    /// Whether `id` is one of the 20-byte hashes the torrent answers to.
    pub fn answers_to(&self, id: Id20) -> bool {
        self.ids20().any(|own| own == id)
    }

    /// Whether the two name a torrent in common.
    pub fn overlaps(&self, other: &InfoHashes) -> bool {
        other.ids20().any(|id| self.answers_to(id))
    }

    /// Whether `id` is the truncated v2 hash, which a peer that speaks v2
    /// greets with (libtorrent `bt_peer_connection::on_receive_handshake`).
    pub fn is_v2(&self, id: Id20) -> bool {
        self.v2.is_some_and(|v2| v2.truncate_for_dht() == id)
    }

    /// The hash to greet a peer with: the truncated v2 hash for a peer that
    /// speaks v2, else v1, and the truncated v2 hash when there is no v1
    /// (libtorrent `peer_connection::associated_info_hash`).
    pub fn for_peer(&self, speaks_v2: bool) -> Id20 {
        match (self.v1, self.v2) {
            (Some(v1), _) if !speaks_v2 || self.v2.is_none() => v1,
            (_, Some(v2)) => v2.truncate_for_dht(),
            _ => Id20::default(),
        }
    }

    /// Whether a peer greeted with `sent` may answer with `got`: the same
    /// hash, or the truncated v2 hash, which moves a hybrid's connection to
    /// v2 (libtorrent `bt_peer_connection::on_receive_handshake`).
    pub fn accepts_answer(&self, sent: Id20, got: Id20) -> bool {
        got == sent || self.is_v2(got)
    }

    /// Whether `info_bytes` are this torrent's info dict: its SHA-1 is the
    /// v1 hash, or its SHA-256 truncated is (a v2 torrent found by a btih
    /// link), and its SHA-256 is the v2 hash (libtorrent
    /// `torrent::set_metadata`).
    pub fn matches_info(&self, info_bytes: &[u8]) -> bool {
        use crate::sha1w::{ISha1, ISha256, Sha1, Sha256};
        let sha256 = || {
            let mut h = Sha256::new();
            h.update(info_bytes);
            Id32::new(h.finish())
        };
        let v1 = self.v1.is_none_or(|v1| {
            let mut h = Sha1::new();
            h.update(info_bytes);
            Id20::new(h.finish()) == v1 || sha256().truncate_for_dht() == v1
        });
        v1 && self.v2.is_none_or(|v2| sha256() == v2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_set_bit_range() {
        let mut id = Id20::default();
        id.set_bits_range(9..17, true);
        assert_eq!(
            id,
            Id20::new([
                0, 127, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ])
        )
    }

    #[test]
    fn test_id32_from_str() {
        let str = "06f04cc728bef957a658876ef807f0514e4d715392969998efef584d2c3e435e";
        let _ih = Id32::from_str(str).unwrap();
    }

    #[test]
    fn test_id20_base32_encoded_from_str() {
        let str = "Z7QRDHYSJCA4U4HXGBXTFYUSDFGIRQMV";
        let ih1 = Id20::from_str(str).unwrap();
        let s2 = "cfe1119f124881ca70f7306f32e292194c88c195";
        let ih2 = Id20::from_str(s2).unwrap();
        assert_eq!(ih1, ih2);
    }

    #[test]
    fn test_id32_truncate_for_dht_uses_first_20_bytes() {
        let id32 = Id32::new([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x99, 0x88,
            0x77, 0x66, 0x55, 0x44,
        ]);
        let truncated = id32.truncate_for_dht();
        assert_eq!(
            truncated,
            Id20::new([
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
            ])
        );
    }

    // libtorrent test_torrents/v2_hybrid.torrent's info dict.
    fn hybrid_info() -> (Vec<u8>, InfoHashes) {
        let bytes = include_bytes!("../../resources/test/libtorrent/v2_hybrid.torrent");
        let meta = crate::core::torrent_metainfo::torrent_from_bytes(bytes).unwrap();
        (meta.info.raw_bytes.0.to_vec(), meta.info_hashes)
    }

    #[test]
    fn an_info_dict_matches_as_libtorrent_accepts_metadata() {
        // libtorrent torrent::set_metadata: SHA-1 against v1, or SHA-256
        // truncated against v1, and SHA-256 against v2.
        let (info, both) = hybrid_info();
        let v1 = both.v1.unwrap();
        let v2 = both.v2.unwrap();
        let only = |v1, v2| InfoHashes { v1, v2 };
        assert!(both.matches_info(&info));
        assert!(only(Some(v1), None).matches_info(&info));
        assert!(only(None, Some(v2)).matches_info(&info));
        assert!(only(Some(v2.truncate_for_dht()), None).matches_info(&info));
        let mut other = v2;
        other.0[0] ^= 1;
        assert!(!only(Some(v1), Some(other)).matches_info(&info));
        assert!(!only(Some(other.truncate_for_dht()), None).matches_info(&info));
        assert!(!both.matches_info(&info[1..]));
    }

    #[test]
    fn a_peer_is_greeted_with_the_hash_of_the_protocol_it_speaks() {
        // libtorrent peer_connection::associated_info_hash and
        // bt_peer_connection::on_receive_handshake.
        let (_, both) = hybrid_info();
        let v1 = both.v1.unwrap();
        let v2 = both.v2.unwrap().truncate_for_dht();
        assert_eq!(both.for_peer(false), v1);
        assert_eq!(both.for_peer(true), v2);
        let v2_only = InfoHashes {
            v1: None,
            v2: both.v2,
        };
        assert_eq!(v2_only.for_peer(false), v2);
        assert_eq!(v2_only.for_peer(true), v2);
        let v1_only = InfoHashes {
            v1: Some(v1),
            v2: None,
        };
        assert_eq!(v1_only.for_peer(true), v1);

        assert!(both.accepts_answer(v1, v1));
        assert!(both.accepts_answer(v1, v2));
        assert!(!both.accepts_answer(v2, v1));
        assert!(!v1_only.accepts_answer(v1, v2));
        assert!(both.is_v2(v2) && !both.is_v2(v1));
        assert_eq!(both.ids20().collect::<Vec<_>>(), [v1, v2]);
        assert!(v2_only.overlaps(&both) && v1_only.overlaps(&both));
        assert!(!v1_only.overlaps(&v2_only));
    }

    #[test]
    fn test_clone_to_owned_id20() {
        let id = Id20::new([1u8; 20]);
        let owned = id.clone_to_owned(None);
        assert_eq!(owned, id);
    }

    #[test]
    fn test_clone_to_owned_id32() {
        let id = Id32::new([2u8; 32]);
        let owned = id.clone_to_owned(None);
        assert_eq!(owned, id);
    }
}
