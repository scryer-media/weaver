// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! qBittorrent 5.3.0, on libtorrent 2.0.15, as a torrent may announce as
//! it: the names it goes by and the shape of its announces.

use crate::tracker::shape::{AnnounceShape, Escape, Field, Header, KeyForm, OwnQuery};

/// libtorrent's fingerprint of `qB` 5.3.0.0 (qBittorrent
/// `src/base/version.h.in` 31-34, `src/base/bittorrent/sessionimpl.cpp`
/// 1807).
pub const PEER_ID_PREFIX: [u8; 8] = *b"-qB5300-";

/// The User-Agent of its HTTP announces (qBittorrent
/// `src/base/bittorrent/sessionimpl.cpp` 134, 1811).
pub const USER_AGENT: &str = "qBittorrent/5.3.0";

/// The `v` of its extended handshake: libtorrent sends the user agent.
pub const VERSION: &str = USER_AGENT;

/// Its announces, as libtorrent writes them.
pub const SHAPE: AnnounceShape = AnnounceShape {
    // libtorrent `src/http_tracker_connection.cpp` 122-170 and 197-214:
    // its order, the key as eight hex digits, compact and no peer id always
    // asked for, encryption only ever said to be supported, the redundant
    // bytes after it, then the tracker id and a private torrent's own
    // addresses.
    fields: &[
        Field::InfoHash,
        Field::PeerId,
        Field::Port,
        Field::Uploaded,
        Field::Downloaded,
        Field::Left,
        Field::Corrupt,
        Field::Key(KeyForm::Hex8),
        Field::Event,
        Field::Numwant,
        Field::Compact { always: true },
        Field::NoPeerId { always: true },
        Field::Crypto { require: false },
        Field::Redundant,
        Field::TrackerId,
        Field::OwnAddrs,
    ],
    // libtorrent `escape_string`, as `src/http_tracker_connection.cpp`
    // 123 and 143 call it.
    escape: Escape::LowerHex,
    // libtorrent `src/http_tracker_connection.cpp` 105-120: the URL's own
    // query, then `&` and the announce's.
    own_query: OwnQuery::First,
    // libtorrent `src/http_connection.cpp` 213-228: Host, User-Agent, gzip
    // as the only encoding, and the connection closed after, each name in
    // title case.
    headers: &[
        Header::Host,
        Header::UserAgent,
        Header::AcceptGzip,
        Header::ConnectionClose,
    ],
    title_case: true,
    // libtorrent `src/session_impl.cpp` 1294-1295 and 1327
    // (`make_announce_port`): a listen port of 0 is announced as 1.
    no_listener_port: 1,
    // libtorrent `src/torrent.cpp` 3115-3116 sets `paused` before the
    // `started` fallback at 3323-3326, which only an announce with no event
    // reaches.
    paused_first: true,
};

/// The started announce of a qBittorrent 5.3.0 seed, as a tracker
/// recorded it.
#[cfg(test)]
pub(crate) const CAPTURED_ANNOUNCE: &str = "info_hash=F%b6%7by!%3eC%9a%97%8b%bce%a3uu%a5H%07%8b%a2&peer_id=-qB5300-CDwwoycKIZ6y&port=19681&uploaded=34340864&downloaded=0&left=0&corrupt=0&key=AEB9ABBF&event=started&numwant=200&compact=1&no_peer_id=1&supportcrypto=1&redundant=0";

/// The info-hash of [`CAPTURED_ANNOUNCE`].
#[cfg(test)]
pub(crate) const CAPTURED_HASH: [u8; 20] = [
    b'F', 0xb6, 0x7b, b'y', b'!', 0x3e, b'C', 0x9a, 0x97, 0x8b, 0xbc, b'e', 0xa3, b'u', b'u', 0xa5,
    b'H', 0x07, 0x8b, 0xa2,
];

/// The request of [`CAPTURED_ANNOUNCE`], under `encryption`.
#[cfg(test)]
pub(crate) fn captured_request<'a>(
    info_hash: &'a crate::core::hash_id::Id20,
    peer_id: &'a crate::core::hash_id::Id20,
    encryption: crate::EncryptionPolicy,
) -> crate::tracker::tracker_comms_http::TrackerRequest<'a> {
    crate::tracker::tracker_comms_http::TrackerRequest {
        info_hash,
        peer_id,
        port: 19681,
        uploaded: 34340864,
        downloaded: 0,
        left: 0,
        corrupt: 0,
        redundant: 0,
        compact: true,
        no_peer_id: false,
        event: Some(crate::tracker::tracker_comms_http::TrackerRequestEvent::Started),
        numwant: 200,
        key: Some(0xAEB9ABBF),
        trackerid: None,
        encryption,
        own_addrs: &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EncryptionPolicy;
    use crate::core::hash_id::Id20;
    use crate::tracker::tracker_comms_http::TrackerRequest;
    use std::net::IpAddr;

    // The announce is the one qBittorrent sends, byte for byte. Requiring
    // encryption says only that it is supported, turning it off says
    // nothing, and a tracker id follows the redundant bytes, escaped as
    // libtorrent escapes.
    #[test]
    fn an_announce_is_the_captured_one() {
        let info_hash = Id20::new(CAPTURED_HASH);
        let peer_id = Id20::new(*b"-qB5300-CDwwoycKIZ6y");
        for encryption in [EncryptionPolicy::Prefer, EncryptionPolicy::Require] {
            assert_eq!(
                SHAPE.query(&captured_request(&info_hash, &peer_id, encryption)),
                CAPTURED_ANNOUNCE
            );
        }
        let plain = captured_request(&info_hash, &peer_id, EncryptionPolicy::Disabled);
        assert_eq!(
            SHAPE.query(&plain),
            CAPTURED_ANNOUNCE.replace("&supportcrypto=1", "")
        );
        let request = TrackerRequest {
            event: None,
            trackerid: Some(b"a b&!"),
            ..plain
        };
        let query = SHAPE.query(&request);
        assert!(
            query.ends_with(
                "&key=AEB9ABBF&numwant=200&compact=1&no_peer_id=1&redundant=0&trackerid=a%20b%26!"
            ),
            "{query}"
        );
    }

    // A private torrent's own addresses follow the tracker id, IPv4 then
    // IPv6, each escaped (libtorrent `src/http_tracker_connection.cpp`
    // 197-214).
    #[test]
    fn own_addresses_follow_the_tracker_id() {
        let info_hash = Id20::new(CAPTURED_HASH);
        let own: [IpAddr; 2] = ["2001:db8::1".parse().unwrap(), [198, 51, 100, 1].into()];
        let peer_id = Id20::new(*b"-qB5300-CDwwoycKIZ6y");
        let request = TrackerRequest {
            trackerid: Some(b"t"),
            own_addrs: &own,
            ..captured_request(&info_hash, &peer_id, EncryptionPolicy::Disabled)
        };
        let query = SHAPE.query(&request);
        assert!(
            query.ends_with("&trackerid=t&ipv4=198.51.100.1&ipv6=2001%3adb8%3a%3a1"),
            "{query}"
        );
    }

    // Alphanumerics and `-_!.~*()` go as they are and every other byte in
    // lowercase hex; a scrape escapes the hash the same way.
    #[test]
    fn a_scrape_escapes_all_but_libtorrents_unreserved_bytes() {
        let all: Vec<u8> = (0..=255).collect();
        for chunk in all.chunks(20) {
            let mut hash = [0u8; 20];
            hash[..chunk.len()].copy_from_slice(chunk);
            let escaped: String = hash
                .iter()
                .map(|&b| match b {
                    b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' => char::from(b).to_string(),
                    b'-' | b'_' | b'!' | b'.' | b'~' | b'*' | b'(' | b')' => {
                        char::from(b).to_string()
                    }
                    _ => format!("%{b:02x}"),
                })
                .collect();
            assert_eq!(
                SHAPE.scrape_query(&Id20::new(hash)),
                format!("info_hash={escaped}")
            );
        }
    }
}
