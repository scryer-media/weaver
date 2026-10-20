// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::buffers::ByteBuf;
use either::Either;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use serde::Deserializer;
use serde_derive::Deserialize;
use std::{
    marker::PhantomData,
    net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

use crate::core::{
    compact_ip::{CompactListInBuffer, CompactSerialize, CompactSerializeFixedLen},
    hash_id::Id20,
};

/// The `event` of an announce (BEP 3). A regular re-announce carries none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackerRequestEvent {
    Started,
    Stopped,
    Completed,
}

pub struct TrackerRequest<'a> {
    pub info_hash: &'a Id20,
    pub peer_id: &'a Id20,
    pub event: Option<TrackerRequestEvent>,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: bool,
    pub no_peer_id: bool,

    pub ip: Option<IpAddr>,
    pub numwant: Option<usize>,
    pub key: Option<u32>,
    pub trackerid: Option<&'a str>,
}

#[derive(Deserialize, Debug)]
pub struct TrackerError<'a> {
    #[serde(rename = "failure reason", borrow)]
    pub failure_reason: ByteBuf<'a>,
}

pub enum Peers<'a, AddrType> {
    DictPeers(Vec<SocketAddr>),
    Compact(CompactListInBuffer<ByteBuf<'a>, AddrType>),
}

impl<'a, AddrType> std::fmt::Debug for Peers<'a, AddrType>
where
    AddrType:
        std::fmt::Debug + CompactSerialize + CompactSerializeFixedLen + Copy + Into<SocketAddr>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a, AddrType> Default for Peers<'a, AddrType> {
    fn default() -> Self {
        Self::DictPeers(Default::default())
    }
}

impl<'a, AddrType> Peers<'a, AddrType>
where
    AddrType: CompactSerialize + CompactSerializeFixedLen + Copy + Into<SocketAddr>,
{
    fn iter(&self) -> impl Iterator<Item = SocketAddr> {
        match self {
            Peers::DictPeers(a) => Either::Left(a.iter().copied()),
            Peers::Compact(l) => Either::Right(l.iter().map(Into::into)),
        }
    }
}

impl<'a, 'de, AddrType> serde::de::Deserialize<'de> for Peers<'a, AddrType>
where
    AddrType: CompactSerialize + CompactSerializeFixedLen + Into<SocketAddr> + 'static,
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct DictPeer {
            #[serde(deserialize_with = "ip_from_str")]
            ip: IpAddr,
            port: u16,
        }

        // The address comes as a string and parses as `IpAddr` text.
        fn ip_from_str<'de, D: Deserializer<'de>>(deserializer: D) -> Result<IpAddr, D::Error> {
            struct IpVisitor;
            impl serde::de::Visitor<'_> for IpVisitor {
                type Value = IpAddr;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a string")
                }

                fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<IpAddr, E> {
                    v.parse().map_err(E::custom)
                }
            }
            deserializer.deserialize_str(IpVisitor)
        }

        struct Visitor<'a, 'de, AddrType> {
            phantom: std::marker::PhantomData<&'de &'a AddrType>,
        }
        impl<'a, 'de, AddrType> serde::de::Visitor<'de> for Visitor<'a, 'de, AddrType>
        where
            AddrType: CompactSerialize + CompactSerializeFixedLen + Into<SocketAddr>,
        {
            type Value = Peers<'de, AddrType>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a list of peers in dict or compact format")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut addrs = Vec::new();
                while let Some(peer) = seq.next_element::<DictPeer>()? {
                    addrs.push(SocketAddr::from((peer.ip, peer.port)))
                }
                Ok(Peers::DictPeers(addrs))
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Peers::Compact(CompactListInBuffer::new_from_buf(v.into())))
            }
        }
        deserializer.deserialize_any(Visitor {
            phantom: PhantomData,
        })
    }
}

#[derive(Deserialize, Debug)]
pub struct TrackerResponse<'a> {
    #[allow(dead_code)]
    #[serde(rename = "warning message", borrow)]
    pub warning_message: Option<ByteBuf<'a>>,
    pub complete: Option<u64>,
    pub interval: u64,
    #[serde(rename = "min interval")]
    pub min_interval: Option<u64>,
    #[allow(dead_code)]
    pub tracker_id: Option<ByteBuf<'a>>,
    pub incomplete: Option<u64>,
    #[serde(borrow)]
    pub peers: Peers<'a, SocketAddrV4>,
    #[serde(default, borrow)]
    pub peers6: Peers<'a, SocketAddrV6>,
}

impl TrackerResponse<'_> {
    pub fn iter_peers(&self) -> impl Iterator<Item = SocketAddr> {
        self.peers.iter().chain(self.peers6.iter())
    }
}

// Every byte but RFC 3986's unreserved characters is escaped.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

impl TrackerRequest<'_> {
    pub fn as_querystring(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        s.push_str("info_hash=");
        s.extend(percent_encode(&self.info_hash.0, QUERY_VALUE));
        s.push_str("&peer_id=");
        s.extend(percent_encode(&self.peer_id.0, QUERY_VALUE));
        if let Some(event) = self.event {
            write!(
                s,
                "&event={}",
                match event {
                    TrackerRequestEvent::Started => "started",
                    TrackerRequestEvent::Stopped => "stopped",
                    TrackerRequestEvent::Completed => "completed",
                }
            )
            .unwrap();
        }
        write!(s, "&port={}", self.port).unwrap();
        write!(s, "&uploaded={}", self.uploaded).unwrap();
        write!(s, "&downloaded={}", self.downloaded).unwrap();
        write!(s, "&left={}", self.left).unwrap();
        write!(s, "&compact={}", if self.compact { 1 } else { 0 }).unwrap();
        write!(s, "&no_peer_id={}", if self.no_peer_id { 1 } else { 0 }).unwrap();
        if let Some(ip) = &self.ip {
            write!(s, "&ip={ip}").unwrap();
        }
        if let Some(numwant) = &self.numwant {
            write!(s, "&numwant={numwant}").unwrap();
        }
        if let Some(key) = &self.key {
            write!(s, "&key={key}").unwrap();
        }
        if let Some(trackerid) = &self.trackerid {
            write!(s, "&trackerid={trackerid}").unwrap();
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_serialize() {
        let info_hash = Id20::new([
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        let peer_id = info_hash;
        let request = TrackerRequest {
            info_hash: &info_hash,
            peer_id: &peer_id,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1024 * 1024,
            compact: true,
            no_peer_id: false,
            event: Some(TrackerRequestEvent::Started),
            ip: Some("127.0.0.1".parse().unwrap()),
            numwant: None,
            key: None,
            trackerid: None,
        };
        dbg!(request.as_querystring());
    }

    #[test]
    fn query_values_escape_all_but_unreserved_bytes() {
        let all: Vec<u8> = (0..=255).collect();
        for chunk in all.chunks(20) {
            let mut id = [0u8; 20];
            id[..chunk.len()].copy_from_slice(chunk);
            let id = Id20::new(id);
            let request = TrackerRequest {
                info_hash: &id,
                peer_id: &id,
                port: 1,
                uploaded: 0,
                downloaded: 0,
                left: 0,
                compact: true,
                no_peer_id: false,
                event: None,
                ip: None,
                numwant: None,
                key: None,
                trackerid: None,
            };
            let e: String =
                id.0.iter()
                    .map(|&b| match b {
                        b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'.' | b'_' | b'~' => {
                            char::from(b).to_string()
                        }
                        _ => format!("%{b:02X}"),
                    })
                    .collect();
            assert_eq!(
                request.as_querystring(),
                format!(
                    "info_hash={e}&peer_id={e}&port=1&uploaded=0&downloaded=0&left=0&compact=1&no_peer_id=0"
                )
            );
        }
    }

    #[test]
    fn test_parse_tracker_response_compact() {
        let data = b"d8:intervali1800e5:peers6:iiiipp6:peers618:iiiiiiiiiiiiiiiippe";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(
            response.iter_peers().collect::<Vec<_>>(),
            vec![
                "105.105.105.105:28784".parse().unwrap(),
                "[6969:6969:6969:6969:6969:6969:6969:6969]:28784"
                    .parse()
                    .unwrap()
            ]
        );
        dbg!(response);
    }

    #[test]
    fn test_parse_tracker_response_counts() {
        let data = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.complete, Some(3));
        assert_eq!(response.incomplete, Some(5));

        // Both counts are optional.
        let data = b"d8:intervali1800e5:peers0:e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.complete, None);
        assert_eq!(response.incomplete, None);
    }

    #[test]
    fn parse_peers_dict() {
        let buf = b"ld2:ip9:127.0.0.14:porti100eed2:ip39:6969:6969:6969:6969:6969:6969:6969:69694:porti101eee";
        dbg!(crate::bencode::dyn_from_bytes::<ByteBuf>(buf).unwrap());
        let peers = crate::bencode::from_bytes::<Peers<SocketAddrV4>>(buf).unwrap();
        assert_eq!(
            peers.iter().collect::<Vec<_>>(),
            vec![
                "127.0.0.1:100".parse().unwrap(),
                "[6969:6969:6969:6969:6969:6969:6969:6969]:101"
                    .parse()
                    .unwrap()
            ]
        );
    }
}
