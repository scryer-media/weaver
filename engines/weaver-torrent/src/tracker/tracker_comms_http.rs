// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::bencode::BencodeValue;
use crate::buffers::ByteBuf;
use crate::core::net::{HttpClients, HttpTarget, Resolved, Resolver};
use crate::tracker::ScrapeOk;
use crate::tracker::tracker_comms::{Reachable, only_reachable};
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
    /// Bytes that failed their hash check.
    pub corrupt: u64,
    /// Bytes received and not used.
    pub redundant: u64,
    pub compact: bool,
    pub no_peer_id: bool,
    /// How many peers to ask for.
    pub numwant: u32,
    pub key: Option<u32>,
    /// The `tracker id` this tracker gave in an earlier answer.
    pub trackerid: Option<&'a [u8]>,
}

#[derive(Deserialize, Debug)]
pub struct TrackerError<'a> {
    #[serde(rename = "failure reason", borrow)]
    pub failure_reason: ByteBuf<'a>,
    /// How long until the tracker may be asked again, when it says.
    #[serde(default, deserialize_with = "optional_interval")]
    pub interval: Option<Duration>,
}

/// The longest interval a tracker may ask for. libtorrent holds a
/// tracker's intervals as `seconds32` (`tracker_response`,
/// `parse_tracker_response`), so none is longer than this; a longer one is
/// taken as this, and no announce time reckoned from it can overflow.
pub(crate) const MAX_INTERVAL: Duration = Duration::from_secs(i32::MAX as u64);

/// A tracker's interval, in seconds, at most [`MAX_INTERVAL`].
fn interval<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
    let secs: u64 = serde::Deserialize::deserialize(de)?;
    Ok(Duration::from_secs(secs).min(MAX_INTERVAL))
}

/// The `interval` of an answer that names none (libtorrent
/// `parse_tracker_response`).
pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(1800);

fn default_interval() -> Duration {
    DEFAULT_INTERVAL
}

/// The `min interval` of an answer that names none (libtorrent
/// `parse_tracker_response`).
const DEFAULT_MIN_INTERVAL: Duration = Duration::from_secs(30);

fn default_min_interval() -> Duration {
    DEFAULT_MIN_INTERVAL
}

/// As [`interval`], for one a refusal may leave out.
fn optional_interval<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Duration>, D::Error> {
    let secs: Option<u64> = serde::Deserialize::deserialize(de)?;
    Ok(secs.map(|secs| Duration::from_secs(secs).min(MAX_INTERVAL)))
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
    /// Shown beside the tracker, the announce accepted (libtorrent
    /// `parse_tracker_response` reads it into `announce_entry::message`).
    #[serde(rename = "warning message", borrow)]
    pub warning_message: Option<ByteBuf<'a>>,
    pub complete: Option<u64>,
    /// Downloads the tracker has seen finish (libtorrent
    /// `parse_tracker_response` reads `downloaded`).
    pub downloaded: Option<u64>,
    #[serde(default = "default_interval", deserialize_with = "interval")]
    pub interval: Duration,
    #[serde(
        rename = "min interval",
        default = "default_min_interval",
        deserialize_with = "interval"
    )]
    pub min_interval: Duration,
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
    /// Our address as the tracker saw it (BEP 24), compact: 4 bytes for
    /// IPv4, 16 for IPv6 (libtorrent `parse_tracker_response`).
    #[serde(rename = "external ip", default, borrow)]
    pub external_ip: Option<ByteBuf<'a>>,
}

impl TrackerResponse<'_> {
    pub fn iter_peers(&self) -> impl Iterator<Item = SocketAddr> {
        self.peers.iter().chain(self.peers6.iter())
    }

    /// The `external ip` the answer gave, None when it gave none or one of
    /// neither length.
    pub fn external_ip(&self) -> Option<IpAddr> {
        let bytes: &[u8] = self.external_ip.as_ref()?.as_ref();
        match bytes.len() {
            4 => <[u8; 4]>::try_from(bytes).ok().map(IpAddr::from),
            16 => <[u8; 16]>::try_from(bytes).ok().map(IpAddr::from),
            _ => None,
        }
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
        write!(s, "&corrupt={}", self.corrupt).unwrap();
        write!(s, "&redundant={}", self.redundant).unwrap();
        write!(s, "&compact={}", if self.compact { 1 } else { 0 }).unwrap();
        write!(s, "&no_peer_id={}", if self.no_peer_id { 1 } else { 0 }).unwrap();
        write!(s, "&numwant={}", self.numwant).unwrap();
        if let Some(key) = &self.key {
            write!(s, "&key={key}").unwrap();
        }
        if let Some(trackerid) = self.trackerid {
            s.push_str("&trackerid=");
            s.extend(percent_encode(trackerid, QUERY_VALUE));
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

/// `url`'s host as a client is told it, and its addresses: the address it
/// names, or what `resolver` finds for its name. None when the session's
/// network resolves the name where it exits.
async fn addresses(
    url: &url::Url,
    resolver: &dyn Resolver,
) -> anyhow::Result<(String, Vec<IpAddr>)> {
    let port = url.port_or_known_default().unwrap_or(0);
    let name = match url.host() {
        Some(url::Host::Ipv4(ip)) => return Ok((ip.to_string(), vec![IpAddr::V4(ip)])),
        Some(url::Host::Ipv6(ip)) => return Ok((ip.to_string(), vec![IpAddr::V6(ip)])),
        Some(url::Host::Domain(name)) => name,
        None => anyhow::bail!("the tracker URL has no host"),
    };
    let found = match resolver
        .resolve(name, port)
        .await
        .with_context(|| format!("error looking up hostname {name}"))?
    {
        Resolved::Addresses(found) => found,
        Resolved::AtExit => return Ok((name.to_owned(), Vec::new())),
    };
    anyhow::ensure!(!found.is_empty(), "the tracker's name has no address");
    Ok((name.to_owned(), found.iter().map(SocketAddr::ip).collect()))
}

/// A client from `clients` for a GET of `url` for `ask`: it reaches the
/// URL's host at the addresses found for it now that the session may reach
/// (see [`only_reachable`]), in the order found, and nowhere else, and
/// follows no redirect. Refused when none is left, or when [`ssrf_refusal`]
/// holds the GET back from those left. Each address is filtered on its own,
/// so the order of the checks changes only which refusal is named;
/// [`ssrf_refusal`] refuses the GET for any loopback address the client may
/// pick, so it judges the addresses the client is handed, after the filter.
/// A name the session's network resolves where it exits has no address
/// here: nothing is checked, and the client hands the name on.
async fn reach(
    clients: &dyn HttpClients,
    url: &url::Url,
    ask: Ask,
    resolver: &dyn Resolver,
    reachable: &Reachable,
) -> anyhow::Result<reqwest::Client> {
    let (host, found) = addresses(url, resolver).await?;
    let addrs = only_reachable(found, |ip| *ip, reachable)?;
    if let Some(why) = ssrf_refusal(url, ask, &addrs) {
        anyhow::bail!("SSRF mitigation: {why}");
    }
    clients.client(Some(HttpTarget {
        host: &host,
        port: url.port_or_known_default().unwrap_or(0),
        addrs: &addrs,
    }))
}

/// The most redirects one GET follows (libtorrent
/// `http_tracker_connection::start` hands `http_connection::get` 5).
const MAX_REDIRECTS: usize = 5;

/// The body of a tracker's answer to a GET for `ask` of `tracker` with
/// `query` added ahead of the URL's own. The GET is not sent when the
/// URL's own query already names an announce argument (libtorrent
/// `http_tracker_connection::start`), and goes only where [`reach`] lets it.
/// A redirect is a GET of where it points, reached the same way before it
/// is sent, at most [`MAX_REDIRECTS`] of them; past that the redirect is the
/// answer (libtorrent `http_connection::on_read` restarts `get` at the new
/// URL with one redirect fewer). The answer fails when the tracker sends
/// nothing for [`RECEIVE_TIMEOUT`], the status is not a success, or the body
/// passes [`MAX_RESPONSE_LENGTH`]. The bounds are the engine's own, so they
/// hold whatever client is used.
pub(crate) async fn get(
    clients: &dyn HttpClients,
    resolver: &dyn Resolver,
    reachable: &Reachable,
    tracker: &url::Url,
    ask: Ask,
    query: &str,
    user_agent: &str,
) -> anyhow::Result<Answer> {
    let mut url = tracker.clone();
    match tracker.query() {
        Some(own) if has_tracker_query(own) => {
            anyhow::bail!("SSRF mitigation: the tracker URL's query names an announce argument")
        }
        Some(own) => url.set_query(Some(&format!("{query}&{own}"))),
        None => url.set_query(Some(query)),
    }
    let mut redirects = 0;
    let response = loop {
        let client = reach(clients, &url, ask, resolver, reachable).await?;
        let response = tokio::time::timeout(
            RECEIVE_TIMEOUT,
            client
                .get(url.clone())
                .header(reqwest::header::USER_AGENT, user_agent)
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the tracker sent nothing for {RECEIVE_TIMEOUT:?}"))??;
        if !response.status().is_redirection() || redirects == MAX_REDIRECTS {
            break response;
        }
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|l| l.to_str().ok())
            .context("the tracker redirected without a location")?;
        url = url
            .join(location)
            .context("the tracker redirected to no URL")?;
        redirects += 1;
    };
    if !response.status().is_success() {
        anyhow::bail!("tracker responded with {:?}", response.status());
    }
    let from = response.remote_addr().map(|addr| addr.ip());
    let chunks = futures::stream::try_unfold(response, |mut response| async move {
        Ok(response.chunk().await?.map(|chunk| (chunk, response)))
    });
    Ok(Answer {
        body: read_body(chunks).await?,
        from,
    })
}

/// A tracker's answer to a GET.
pub(crate) struct Answer {
    pub(crate) body: Vec<u8>,
    /// The address the answer came from, when the client tells it.
    pub(crate) from: Option<IpAddr>,
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

    // A resolver that answers every name with `answer`, counting the asks.
    struct Answering {
        answer: Resolved,
        asked: std::sync::atomic::AtomicUsize,
    }

    impl Resolver for Answering {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<Resolved>> {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move { Ok(self.answer.clone()) })
        }
    }

    // The targets clients were built for.
    #[derive(Default)]
    struct Targets(std::sync::Mutex<Vec<(String, u16, Vec<IpAddr>)>>);

    impl HttpClients for Targets {
        fn builder(
            &self,
            target: Option<HttpTarget<'_>>,
        ) -> anyhow::Result<reqwest::ClientBuilder> {
            let target = target.expect("a tracker is asked through a target");
            self.0.lock().unwrap().push((
                target.host.to_owned(),
                target.port,
                target.addrs.to_vec(),
            ));
            Ok(reqwest::Client::builder().no_proxy())
        }
    }

    // A tracker's client is built for exactly the addresses its name
    // resolved to that the session may reach; a name the network resolves
    // at its exit is handed on with no address and nothing checked.
    #[tokio::test]
    async fn a_trackers_client_carries_what_its_name_resolved_to() {
        let url = url::Url::parse("http://tracker.test:6969/announce").unwrap();
        let blocked = IpAddr::from([192, 0, 2, 2]);
        let reachable: Reachable = std::sync::Arc::new(move |ip| {
            if ip == blocked {
                Err(crate::tracker::tracker_comms::Unreachable::Filtered)
            } else {
                Ok(())
            }
        });
        for (answer, checked) in [
            (
                Resolved::Addresses(vec![
                    SocketAddr::from(([192, 0, 2, 1], 6969)),
                    SocketAddr::new(blocked, 6969),
                ]),
                vec![IpAddr::from([192, 0, 2, 1])],
            ),
            (Resolved::AtExit, Vec::new()),
        ] {
            let resolver = Answering {
                answer,
                asked: Default::default(),
            };
            let clients = Targets::default();
            reach(&clients, &url, Ask::Announce, &resolver, &reachable)
                .await
                .unwrap();
            assert_eq!(resolver.asked.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                *clients.0.lock().unwrap(),
                [("tracker.test".to_owned(), 6969, checked)]
            );
        }
    }
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
            corrupt: 0,
            redundant: 0,
            compact: true,
            no_peer_id: false,
            event: Some(TrackerRequestEvent::Started),
            numwant: 50,
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
                corrupt: 0,
                redundant: 0,
                compact: true,
                no_peer_id: false,
                event: None,
                numwant: 50,
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
                    "info_hash={e}&peer_id={e}&port=1&uploaded=0&downloaded=0&left=0&corrupt=0&redundant=0&compact=1&no_peer_id=0&numwant=50"
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
    fn the_external_ip_a_tracker_answer_gives_is_read_in_either_family() {
        let data = b"d11:external ip4:\xcb\x00\x71\x058:intervali1800e5:peers0:e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.external_ip(), Some("203.0.113.5".parse().unwrap()));

        let mut data = b"d11:external ip16:".to_vec();
        data.extend_from_slice(
            &"2001:db8::5"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .octets(),
        );
        data.extend_from_slice(b"8:intervali1800e5:peers0:e");
        let response = crate::bencode::from_bytes::<TrackerResponse>(&data).unwrap();
        assert_eq!(response.external_ip(), Some("2001:db8::5".parse().unwrap()));

        // Of neither length, or left out: none.
        let data = b"d11:external ip3:abc8:intervali1800e5:peers0:e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.external_ip(), None);
        let data = b"d8:intervali1800e5:peers0:e";
        let response = crate::bencode::from_bytes::<TrackerResponse>(data).unwrap();
        assert_eq!(response.external_ip(), None);
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
            corrupt: 0,
            redundant: 0,
            compact: true,
            no_peer_id: false,
            event: None,
            numwant: 200,
            key: None,
            trackerid: id,
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
