// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::bencode::BencodeValue;
use crate::buffers::ByteBuf;
use crate::core::net::Resolver;
use crate::tracker::ScrapeOk;
use anyhow::Context;
use either::Either;
use futures::StreamExt;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use serde::Deserializer;
use serde_derive::Deserialize;
use std::{
    marker::PhantomData,
    net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6},
    time::Duration,
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
    /// How many peers to ask for.
    pub numwant: u32,
    pub key: Option<u32>,
    /// The `tracker id` this tracker gave in an earlier answer.
    pub trackerid: Option<&'a [u8]>,
    /// Our own addresses, each sent as `ipv4=` or `ipv6=` (BEP 7).
    pub ips: &'a [IpAddr],
}

#[derive(Deserialize, Debug)]
pub struct TrackerError<'a> {
    #[serde(rename = "failure reason", borrow)]
    pub failure_reason: ByteBuf<'a>,
    /// How long until the tracker may be asked again, when it says.
    pub interval: Option<u64>,
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
    /// To be sent back as `trackerid` on every later announce to this tracker.
    #[serde(rename = "tracker id", borrow)]
    pub tracker_id: Option<ByteBuf<'a>>,
    pub incomplete: Option<u64>,
    /// IPv4 peers when compact (BEP 23), or peers of either family as
    /// dictionaries. Absent when the tracker lists only IPv6 peers.
    #[serde(default, borrow)]
    pub peers: Peers<'a, SocketAddrV4>,
    /// IPv6 peers (BEP 7).
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
        write!(s, "&numwant={}", self.numwant).unwrap();
        if let Some(key) = &self.key {
            write!(s, "&key={key}").unwrap();
        }
        if let Some(trackerid) = self.trackerid {
            s.push_str("&trackerid=");
            s.extend(percent_encode(trackerid, QUERY_VALUE));
        }
        // Every IPv4 address, then every IPv6 one (libtorrent
        // `http_tracker_connection::start`).
        let v4 = self
            .ips
            .iter()
            .filter(|ip| ip.is_ipv4())
            .map(|ip| ("ipv4", ip));
        let v6 = self
            .ips
            .iter()
            .filter(|ip| ip.is_ipv6())
            .map(|ip| ("ipv6", ip));
        for (key, ip) in v4.chain(v6) {
            write!(s, "&{key}=").unwrap();
            s.extend(percent_encode(ip.to_string().as_bytes(), QUERY_VALUE));
        }
        s
    }
}

/// How long a tracker may go without sending a byte (libtorrent
/// `settings_pack::tracker_receive_timeout`).
pub(crate) const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// The most bytes a tracker's answer may hold, counted after decompression
/// (libtorrent `settings_pack::tracker_maximum_response_length`).
pub(crate) const MAX_RESPONSE_LENGTH: usize = 1024 * 1024;

/// What a GET to a tracker asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ask {
    Announce,
    Scrape,
}

/// The arguments an announce carries. A tracker URL whose own query names
/// one already is refused (libtorrent `has_tracker_query_string`, the names
/// compared as given, ignoring case).
const TRACKER_ARGS: [&str; 9] = [
    "info_hash",
    "event",
    "port",
    "left",
    "key",
    "uploaded",
    "downloaded",
    "corrupt",
    "peer_id",
];

fn has_tracker_query(query: &str) -> bool {
    query.split('&').any(|arg| {
        let name = arg.split_once('=').map_or(arg, |(name, _)| name);
        TRACKER_ARGS.iter().any(|a| a.eq_ignore_ascii_case(name))
    })
}

/// Why a GET of `url` for `ask`, whose host resolved to `addrs`, may not be
/// sent (libtorrent `ssrf_mitigation`, `http_tracker_connection::on_filter`):
/// a tracker at a loopback address is asked only what a standard BitTorrent
/// tracker answers, at a path starting `/announce`, or `/scrape` for a
/// scrape. The client picks the address it connects to, so every address
/// must pass.
fn ssrf_refusal(url: &url::Url, ask: Ask, addrs: &[IpAddr]) -> Option<&'static str> {
    let path = match ask {
        Ask::Announce => "/announce",
        Ask::Scrape => "/scrape",
    };
    let loopback = addrs.iter().any(|a| a.to_canonical().is_loopback());
    (loopback && !url.path().starts_with(path))
        .then_some("a loopback tracker asked at a path no tracker answers")
}

/// The addresses of `url`'s host: the address it names, or what
/// `resolver`, else the host's resolver, finds for its name.
async fn addresses(url: &url::Url, resolver: Option<&dyn Resolver>) -> anyhow::Result<Vec<IpAddr>> {
    let port = url.port_or_known_default().unwrap_or(0);
    let found = match url.host() {
        Some(url::Host::Ipv4(ip)) => return Ok(vec![IpAddr::V4(ip)]),
        Some(url::Host::Ipv6(ip)) => return Ok(vec![IpAddr::V6(ip)]),
        Some(url::Host::Domain(name)) => match resolver {
            Some(resolver) => resolver.resolve(name, port).await,
            None => tokio::net::lookup_host((name, port))
                .await
                .map(Iterator::collect),
        }
        .with_context(|| format!("error looking up hostname {name}"))?,
        None => anyhow::bail!("the tracker URL has no host"),
    };
    anyhow::ensure!(!found.is_empty(), "the tracker's name has no address");
    Ok(found.iter().map(SocketAddr::ip).collect())
}

/// Refuses a GET of `url` for `ask` that [`ssrf_refusal`] holds back.
async fn check(url: &url::Url, ask: Ask, resolver: Option<&dyn Resolver>) -> anyhow::Result<()> {
    let addrs = addresses(url, resolver).await?;
    match ssrf_refusal(url, ask, &addrs) {
        Some(why) => anyhow::bail!("SSRF mitigation: {why}"),
        None => Ok(()),
    }
}

/// The body of a tracker's answer to a GET for `ask` of `tracker` with
/// `query` added ahead of the URL's own. The GET is not sent when the
/// URL's own query already names an announce argument, or when the host,
/// as `resolver` resolves it, is one the request may not go to (libtorrent
/// `http_tracker_connection::start`, `on_filter`); a redirect the client
/// followed is held to the same before its answer is used. The answer fails
/// when the tracker sends nothing for [`RECEIVE_TIMEOUT`], the status is not
/// a success, or the body passes [`MAX_RESPONSE_LENGTH`]. The bounds are the
/// engine's own, so they hold whatever client is used.
pub(crate) async fn get(
    client: &reqwest::Client,
    resolver: Option<&dyn Resolver>,
    tracker: &url::Url,
    ask: Ask,
    query: &str,
    user_agent: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut url = tracker.clone();
    match tracker.query() {
        Some(own) if has_tracker_query(own) => {
            anyhow::bail!("SSRF mitigation: the tracker URL's query names an announce argument")
        }
        Some(own) => url.set_query(Some(&format!("{query}&{own}"))),
        None => url.set_query(Some(query)),
    }
    check(&url, ask, resolver).await?;
    let response = tokio::time::timeout(
        RECEIVE_TIMEOUT,
        client
            .get(url.clone())
            .header(reqwest::header::USER_AGENT, user_agent)
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the tracker sent nothing for {RECEIVE_TIMEOUT:?}"))??;
    if response.url() != &url {
        check(response.url(), ask, resolver).await?;
    }
    if !response.status().is_success() {
        anyhow::bail!("tracker responded with {:?}", response.status());
    }
    let chunks = futures::stream::try_unfold(response, |mut response| async move {
        Ok(response.chunk().await?.map(|chunk| (chunk, response)))
    });
    read_body(chunks).await
}

/// Collects a body from `chunks` within [`RECEIVE_TIMEOUT`] per chunk and
/// [`MAX_RESPONSE_LENGTH`] in all (libtorrent `timeout_handler`'s read
/// timeout, `http_connection::on_read`'s size limit).
async fn read_body(
    chunks: impl futures::Stream<Item = anyhow::Result<bytes::Bytes>>,
) -> anyhow::Result<Vec<u8>> {
    let mut chunks = std::pin::pin!(chunks);
    let mut body = Vec::new();
    loop {
        let chunk = tokio::time::timeout(RECEIVE_TIMEOUT, chunks.next())
            .await
            .map_err(|_| anyhow::anyhow!("the tracker sent nothing for {RECEIVE_TIMEOUT:?}"))?;
        let Some(chunk) = chunk.transpose()? else {
            return Ok(body);
        };
        if body.len() + chunk.len() > MAX_RESPONSE_LENGTH {
            anyhow::bail!("the tracker's answer is longer than {MAX_RESPONSE_LENGTH} bytes");
        }
        body.extend_from_slice(&chunk);
    }
}

/// The scrape URL of an announce URL (BEP 48): the last path component's
/// leading `announce` becomes `scrape`. None when it does not start with
/// `announce`, as the tracker then offers no scrape.
pub(crate) fn scrape_url(announce: &url::Url) -> Option<url::Url> {
    let path = announce.path();
    let (dir, last) = path.rsplit_once('/')?;
    let rest = last.strip_prefix("announce")?;
    let mut url = announce.clone();
    url.set_path(&format!("{dir}/scrape{rest}"));
    Some(url)
}

/// The query of a scrape for `info_hash`, escaped as an announce's is.
pub(crate) fn scrape_querystring(info_hash: &Id20) -> String {
    let mut s = String::from("info_hash=");
    s.extend(percent_encode(&info_hash.0, QUERY_VALUE));
    s
}

/// The counts a scrape reply gives for `info_hash`: the `files` entry keyed
/// by the raw hash (libtorrent `parse_tracker_response` with
/// `scrape_request`). A count the entry leaves out, or gives below zero, is
/// None.
pub(crate) fn parse_scrape(body: &[u8], info_hash: &Id20) -> anyhow::Result<ScrapeOk> {
    if let Ok((error, _)) = crate::bencode::from_bytes_with_rest::<TrackerError>(body) {
        anyhow::bail!(
            "tracker returned failure. Failure reason: {}",
            error.failure_reason
        )
    }
    let reply = crate::bencode::dyn_from_bytes::<ByteBuf>(body).map_err(|e| e.into_kind())?;
    let BencodeValue::Dict(reply) = reply else {
        anyhow::bail!("the scrape reply is not a dictionary")
    };
    let Some(BencodeValue::Dict(files)) = reply.get(&b"files"[..]) else {
        anyhow::bail!("the scrape reply has no files dictionary")
    };
    let Some(BencodeValue::Dict(entry)) = files.get(&info_hash.0[..]) else {
        anyhow::bail!("the scrape reply lists no counts for the torrent")
    };
    let count = |key: &[u8]| match entry.get(key) {
        Some(BencodeValue::Integer(n)) => u32::try_from(*n).ok(),
        _ => None,
    };
    Ok(ScrapeOk {
        complete: count(b"complete"),
        incomplete: count(b"incomplete"),
        downloaded: count(b"downloaded"),
    })
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
            numwant: 50,
            key: None,
            trackerid: None,
            ips: &[],
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
                numwant: 50,
                key: None,
                trackerid: None,
                ips: &[],
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
                    "info_hash={e}&peer_id={e}&port=1&uploaded=0&downloaded=0&left=0&compact=1&no_peer_id=0&numwant=50"
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
    fn every_listed_peer_of_either_family_is_read() {
        let v4: SocketAddr = "105.105.105.105:28784".parse().unwrap();
        let v6: SocketAddr = "[6969:6969:6969:6969:6969:6969:6969:6969]:28784"
            .parse()
            .unwrap();

        // Only IPv6 peers: no `peers` key at all.
        let data = b"d8:intervali1800e6:peers618:iiiiiiiiiiiiiiiippe";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.iter_peers().collect::<Vec<_>>(), vec![v6]);

        // Both lists in dictionary form.
        let data = b"d8:intervali1800e5:peersld2:ip15:105.105.105.1054:porti28784eee6:peers6ld2:ip39:6969:6969:6969:6969:6969:6969:6969:69694:porti28784eeee";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.iter_peers().collect::<Vec<_>>(), vec![v4, v6]);
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
    fn tracker_id_is_read_and_sent_back_escaped() {
        let data = b"d8:intervali1800e5:peers0:10:tracker id4:a b&e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        let id = response.tracker_id.as_ref().map(|id| id.as_ref());
        assert_eq!(id, Some(&b"a b&"[..]));

        let hash = Id20::new([0x61; 20]);
        let request = TrackerRequest {
            info_hash: &hash,
            peer_id: &hash,
            port: 1,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            compact: true,
            no_peer_id: false,
            event: None,
            ip: None,
            numwant: 200,
            key: None,
            trackerid: id,
            ips: &[],
        };
        assert!(
            request
                .as_querystring()
                .ends_with("&numwant=200&trackerid=a%20b%26"),
            "{}",
            request.as_querystring()
        );
    }

    #[test]
    fn our_addresses_go_out_as_ipv4_then_ipv6() {
        // BEP 7, as libtorrent http_tracker_connection::start sends them.
        let hash = Id20::new([0x61; 20]);
        let ips: Vec<IpAddr> = ["2001:db8::1", "198.51.100.7", "2001:db8::2"]
            .iter()
            .map(|ip| ip.parse().unwrap())
            .collect();
        let request = TrackerRequest {
            info_hash: &hash,
            peer_id: &hash,
            port: 1,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            compact: true,
            no_peer_id: false,
            event: None,
            ip: None,
            numwant: 200,
            key: None,
            trackerid: None,
            ips: &ips,
        };
        assert!(
            request.as_querystring().ends_with(
                "&numwant=200&ipv4=198.51.100.7&ipv6=2001%3Adb8%3A%3A1&ipv6=2001%3Adb8%3A%3A2"
            ),
            "{}",
            request.as_querystring()
        );
    }

    #[test]
    fn scrape_url_replaces_the_announce_that_starts_the_last_component() {
        let scrape = |s: &str| scrape_url(&url::Url::parse(s).unwrap()).map(|u| u.to_string());
        assert_eq!(
            scrape("http://t.invalid/announce").as_deref(),
            Some("http://t.invalid/scrape")
        );
        assert_eq!(
            scrape("https://t.invalid/x/announce.php?passkey=k").as_deref(),
            Some("https://t.invalid/x/scrape.php?passkey=k")
        );
        // Not the last component, or not at its start: no scrape.
        assert_eq!(scrape("http://t.invalid/announce/x"), None);
        assert_eq!(scrape("http://t.invalid/x_announce"), None);
        assert_eq!(scrape("http://announce.invalid/a"), None);
        assert_eq!(scrape("http://t.invalid/"), None);
    }

    #[test]
    fn scrape_reply_gives_the_torrents_counts() {
        let hash = Id20::new([1; 20]);
        let reply = |entry: &str| {
            let mut b = b"d5:filesd20:".to_vec();
            b.extend_from_slice(&hash.0);
            b.extend_from_slice(entry.as_bytes());
            b.extend_from_slice(b"ee");
            b
        };
        let counts = parse_scrape(
            &reply("d8:completei4e10:downloadedi9e10:incompletei6ee"),
            &hash,
        )
        .unwrap();
        assert_eq!(
            counts,
            ScrapeOk {
                complete: Some(4),
                incomplete: Some(6),
                downloaded: Some(9),
            }
        );

        // A count left out, or below zero, is not given.
        let counts = parse_scrape(&reply("d8:completei-1e10:incompletei2ee"), &hash).unwrap();
        assert_eq!(
            counts,
            ScrapeOk {
                complete: None,
                incomplete: Some(2),
                downloaded: None,
            }
        );

        // Another torrent's counts, no files, or a failure are no answer.
        assert!(parse_scrape(&reply("de"), &Id20::new([2; 20])).is_err());
        assert!(parse_scrape(b"d8:intervali1800ee", &hash).is_err());
        let e = parse_scrape(b"d14:failure reason4:nopee", &hash).unwrap_err();
        assert!(format!("{e:#}").contains("nope"), "{e:#}");
    }

    #[test]
    fn a_query_naming_an_announce_argument_is_one_a_tracker_url_may_not_carry() {
        // libtorrent has_tracker_query_string: the names, ignoring case.
        for q in [
            "info_hash=x",
            "a=1&Peer_ID=2",
            "port",
            "left=&b",
            "key=k",
            "uploaded=0",
            "downloaded=0",
            "corrupt=0",
            "event=started",
        ] {
            assert!(has_tracker_query(q), "{q}");
        }
        for q in ["passkey=info_hash", "", "info_hashes=1", "numwant=1&ip=1"] {
            assert!(!has_tracker_query(q), "{q}");
        }
    }

    #[test]
    fn a_loopback_tracker_is_asked_only_at_a_tracker_path() {
        // libtorrent http_tracker_connection::on_filter.
        let url = |s: &str| url::Url::parse(s).unwrap();
        let loopback: [IpAddr; 2] = ["127.0.0.2".parse().unwrap(), "::1".parse().unwrap()];
        let lan: IpAddr = "192.168.1.2".parse().unwrap();
        let global: IpAddr = "93.184.216.34".parse().unwrap();
        for a in loopback {
            for (u, ask) in [
                ("http://h/announce", Ask::Announce),
                ("http://h/announce.php?x=1", Ask::Announce),
                ("http://h/scrape.php", Ask::Scrape),
            ] {
                assert_eq!(ssrf_refusal(&url(u), ask, &[a]), None, "{u} {a}");
            }
            for (u, ask) in [
                ("http://h/x/announce", Ask::Announce),
                ("http://h/", Ask::Announce),
                ("http://h/scrape", Ask::Announce),
                ("http://h/announce", Ask::Scrape),
            ] {
                assert!(ssrf_refusal(&url(u), ask, &[a]).is_some(), "{u} {a}");
                // One loopback address among others is enough.
                assert!(
                    ssrf_refusal(&url(u), ask, &[global, a]).is_some(),
                    "{u} {a}"
                );
            }
        }
        // A v4 loopback written as v6 is loopback.
        let mapped: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(ssrf_refusal(&url("http://h/x"), Ask::Announce, &[mapped]).is_some());
        // Elsewhere any path is asked.
        for a in [lan, global] {
            assert_eq!(ssrf_refusal(&url("http://h/x"), Ask::Announce, &[a]), None);
        }
    }

    /// A body sent as `chunks`, each `gap` after the one before.
    fn trickle(
        chunks: Vec<Vec<u8>>,
        gap: Duration,
    ) -> impl futures::Stream<Item = anyhow::Result<bytes::Bytes>> {
        futures::stream::iter(chunks).then(move |chunk| async move {
            tokio::time::sleep(gap).await;
            Ok(bytes::Bytes::from(chunk))
        })
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_is_read_up_to_the_length_cap_and_refused_past_it() {
        let half = MAX_RESPONSE_LENGTH / 2;
        let body = read_body(trickle(
            vec![vec![b'a'; half], vec![b'b'; half]],
            Duration::ZERO,
        ))
        .await
        .unwrap();
        assert_eq!(body.len(), MAX_RESPONSE_LENGTH);

        let e = read_body(trickle(
            vec![vec![b'a'; half], vec![b'b'; half], vec![b'c']],
            Duration::ZERO,
        ))
        .await
        .unwrap_err();
        assert!(format!("{e:#}").contains("longer than"), "{e:#}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracker_may_pause_between_bytes_up_to_the_receive_timeout() {
        let gap = RECEIVE_TIMEOUT - Duration::from_secs(1);
        let body = read_body(trickle(vec![b"d".to_vec(), b"e".to_vec()], gap))
            .await
            .unwrap();
        assert_eq!(body, b"de");

        // Silent for the whole receive timeout: refused, whatever came before.
        let silent = futures::stream::iter([Ok(bytes::Bytes::from_static(b"d"))])
            .chain(futures::stream::pending());
        let e = read_body(silent).await.unwrap_err();
        assert!(format!("{e:#}").contains("sent nothing"), "{e:#}");
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
