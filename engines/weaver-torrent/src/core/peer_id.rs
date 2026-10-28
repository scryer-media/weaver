// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::core::hash_id::Id20;
use rand::RngExt;

/// Return the version of the invoking crate as a tuple
#[macro_export]
macro_rules! crate_version {
    () => {
        (
            env!("CARGO_PKG_VERSION_MAJOR").parse::<u8>().unwrap_or(0),
            env!("CARGO_PKG_VERSION_MINOR").parse::<u8>().unwrap_or(0),
            env!("CARGO_PKG_VERSION_PATCH").parse::<u8>().unwrap_or(0),
            env!("CARGO_PKG_VERSION_PRE").parse::<u8>().unwrap_or(0),
        )
    };
}

#[derive(Debug)]
pub enum AzureusStyleKind {
    Deluge,
    LibTorrent,
    Transmission,
    QBittorrent,
    UTorrent,
    RQBit,
    Other([u8; 2]),
}

#[derive(Debug)]
pub struct AzureusStyle {
    pub kind: AzureusStyleKind,
    pub version: [u8; 4],
}

impl AzureusStyleKind {
    pub const fn from_bytes(b1: u8, b2: u8) -> Self {
        match &[b1, b2] {
            b"DE" => AzureusStyleKind::Deluge,
            b"lt" | b"LT" => AzureusStyleKind::LibTorrent,
            b"TR" => AzureusStyleKind::Transmission,
            b"qB" => AzureusStyleKind::QBittorrent,
            b"UT" => AzureusStyleKind::UTorrent,
            b"rQ" => AzureusStyleKind::RQBit,
            other => AzureusStyleKind::Other(*other),
        }
    }
}

fn try_decode_azureus_style(p: &Id20) -> Option<AzureusStyle> {
    let p = p.0;
    if !(p[0] == b'-' && p[7] == b'-') {
        return None;
    }
    let mut version = [b'0'; 4];
    for (i, c) in p[3..7].iter().copied().enumerate() {
        version[i] = version_digit_from_id(c)?;
    }
    let kind = AzureusStyleKind::from_bytes(p[1], p[2]);
    Some(AzureusStyle { kind, version })
}

#[derive(Debug)]
pub enum PeerId {
    AzureusStyle(AzureusStyle),
}

pub fn try_decode_peer_id(p: Id20) -> Option<PeerId> {
    Some(PeerId::AzureusStyle(try_decode_azureus_style(&p)?))
}

/// Clients by the peer id code their own sources set (Deluge `core.py`,
/// libtorrent `settings_pack.cpp`, Transmission `CMakeLists.txt`, Weaver
/// `identity.rs`, rTorrent's libtorrent `client_list.cc`, qBittorrent
/// `sessionimpl.cpp`, rqbit `session.rs`), in byte order of code.
const CLIENTS: &[(&[u8], &str)] = &[
    (b"DE", "Deluge"),
    (b"LT", "libtorrent"),
    (b"TR", "Transmission"),
    (b"WV", "Weaver"),
    (b"lt", "rTorrent"),
    (b"qB", "qBittorrent"),
    (b"rQ", "rqbit"),
];

/// The client a peer id names by one of BEP 20's two encodings, for a peer
/// that does not name itself in its extended handshake: `-XX1234-`, a two
/// character code and four version characters between dashes, or
/// `X12345`, a one character code and up to five version characters,
/// dash padded. A code we do not know reads as itself. `None` when the id
/// follows neither.
pub fn identify_client(peer_id: Id20) -> Option<String> {
    let id = &peer_id.0;
    let (code, version) = if id[0] == b'-' && id[7] == b'-' {
        let code = &id[1..3];
        let mut version = versions(&id[3..7])?;
        // The fourth number is a build or tag, left out when it is 0.
        if version.last() == Some(&0) {
            version.pop();
        }
        (code, version)
    } else if id[0].is_ascii_alphanumeric() {
        let width = id[1..6].iter().position(|&c| c == b'-')?;
        if width == 0 || id[1 + width..6].iter().any(|&c| c != b'-') {
            return None;
        }
        (&id[..1], versions(&id[1..1 + width])?)
    } else {
        return None;
    };
    if !code.iter().all(|c| c.is_ascii_graphic()) {
        return None;
    }
    let name = CLIENTS
        .iter()
        .find(|(known, _)| *known == code)
        .map_or_else(
            || String::from_utf8_lossy(code).into_owned(),
            |(_, name)| (*name).to_owned(),
        );
    let version: Vec<String> = version.iter().map(u8::to_string).collect();
    Some(format!("{name} {}", version.join(".")))
}

// Each version character as its number, `0`-`9` then `A`-`Z`, `a`-`z`,
// `.` (BEP 20); a dash is no version character.
fn versions(chars: &[u8]) -> Option<Vec<u8>> {
    chars
        .iter()
        .map(|&c| (c != b'-').then(|| version_digit_from_id(c)).flatten())
        .collect()
}

/// Returns `None` for bytes greater than 64
fn version_digit_to_id(d: u8) -> Option<u8> {
    let version_map = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";
    version_map.get(d as usize).copied()
}

/// Returns `None` for bytes that aren't alphanumeric, `.` or `-`.
fn version_digit_from_id(d: u8) -> Option<u8> {
    match d {
        b'0'..=b'9' => Some(d - b'0'),
        b'A'..=b'Z' => Some(d - b'A' + 10),
        b'a'..=b'z' => Some(d - b'a' + 36),
        b'.' => Some(62),
        b'-' => Some(63),
        _ => None,
    }
}

/// Generate a client fingerprint in the Azereus format, where `b"-xx1234-"` corresponds to version `1.2.3.4`` of the torrent client abbreviated by `xx`
pub fn generate_azereus_style(client: [u8; 2], version: (u8, u8, u8, u8)) -> Id20 {
    let mut fingerprint = [b'-'; 8];

    fingerprint[1..3].copy_from_slice(&client);
    fingerprint[3] = version_digit_to_id(version.0).unwrap();
    fingerprint[4] = version_digit_to_id(version.1).unwrap();
    fingerprint[5] = version_digit_to_id(version.2).unwrap();
    fingerprint[6] = version_digit_to_id(version.3).unwrap();
    generate_peer_id(&fingerprint)
}

/// Panics if the `fingerprint` slice isn't eight bytes long
pub fn generate_peer_id(fingerprint: &[u8]) -> Id20 {
    let mut peer_id = [0u8; 20];

    peer_id[..8].copy_from_slice(fingerprint);
    url_random(&mut peer_id[8..]);

    Id20::new(peer_id)
}

/// Fills `dest` with characters a URL carries unescaped, ' left out as some
/// trackers fail on it (libtorrent `url_random`, the rest of its peer ids).
pub fn url_random(dest: &mut [u8]) {
    const PRINTABLE: &[u8] =
        b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_.!~*()";
    let mut rng = rand::rng();
    for byte in dest {
        *byte = PRINTABLE[rng.random_range(0..PRINTABLE.len())];
    }
}

#[cfg(test)]
mod tests {
    use crate::core::peer_id::generate_azereus_style;

    #[test]
    fn test_azereus_peer_id_generation() {
        for (client, version, correct_fingerprint) in [
            (*b"xx", (1, 2, 3, 4), *b"-xx1234-"),
            (*b"00", (10, 0, 0, 0), *b"-00A000-"),
            (*b"\xFF\xFF", (36, 37, 62, 63), *b"-\xFF\xFFab.--"),
        ] {
            let id1 = generate_azereus_style(client, version);
            let id2 = generate_azereus_style(client, version);
            assert_ne!(id1, id2);
            assert_eq!(id1.0[..8], id2.0[..8]);
            assert_eq!(id1.0[..8], correct_fingerprint);
        }
    }

    // BEP 20's two encodings: a known code by its client's name, an
    // unknown one as itself, and any other id names no client.
    #[test]
    fn a_peer_id_names_its_client() {
        use super::identify_client;
        use crate::core::hash_id::Id20;
        fn id(head: &[u8]) -> Id20 {
            let mut id = [b'x'; 20];
            id[..head.len()].copy_from_slice(head);
            Id20::new(id)
        }
        for (head, client) in [
            (&b"-qB4650-"[..], Some("qBittorrent 4.6.5")),
            (b"-LT2010-", Some("libtorrent 2.0.1")),
            (b"-DE13F0-", Some("Deluge 1.3.15")),
            (b"-TR4000-", Some("Transmission 4.0.0")),
            (b"-lt0D80-", Some("rTorrent 0.13.8")),
            (b"-ZZ1234-", Some("ZZ 1.2.3.4")),
            (b"T03I--", Some("T 0.3.18")),
            (b"S58B-----", Some("S 5.8.11")),
            (b"-qB46!0-", None),
            (b"T-3I--", None),
            (b"T03-I-", None),
            (b"exbc", None),
        ] {
            assert_eq!(identify_client(id(head)).as_deref(), client, "{head:?}");
        }
        assert_eq!(identify_client(Id20::new([0; 20])), None);
    }

    // Each version digit decodes to the value it was encoded from.
    #[test]
    fn test_azureus_version_digits_round_trip() {
        use super::{version_digit_from_id, version_digit_to_id};
        for d in 0..64u8 {
            let c = version_digit_to_id(d).unwrap();
            assert_eq!(
                version_digit_from_id(c),
                Some(d),
                "digit {d} as {:?}",
                c as char
            );
        }
    }
}
