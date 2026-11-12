// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::EncryptionPolicy;
use crate::bencode::BencodeValue;
use crate::buffers::ByteBuf;
use crate::core::net::{
    ClientShape, ClientUse, HttpClients, HttpTarget, NamedBy, Resolved, Resolver, is_public,
};
use crate::tracker::ScrapeOk;
use crate::tracker::shape::AnnounceShape;
use crate::tracker::tracker_comms::{Reachable, only_reachable};
use anyhow::Context;
use either::Either;
use futures::StreamExt;
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
    /// A regular announce of a torrent that has every piece it wants and
    /// is not a seed (BEP 21; libtorrent `torrent::announce_with_tracker`).
    Paused,
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
    /// The encryption our connections are made under: a policy that
    /// encrypts says `supportcrypto`, and one that only encrypts says
    /// `requirecrypto` too.
    pub encryption: EncryptionPolicy,
    /// The public addresses this session announces from, which libtorrent
    /// lists for a private torrent as `ipv4` and `ipv6`
    /// (`torrent::announce_with_tracker`). Only a shape whose fields name
    /// them says them.
    pub own_addrs: &'a [IpAddr],
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

impl TrackerRequestEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
            Self::Paused => "paused",
        }
    }
}

impl TrackerRequest<'_> {
    /// The query of this announce, in `shape`.
    pub fn as_querystring(&self, shape: &AnnounceShape) -> String {
        shape.query(self)
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

/// An address family, which a GET may be held to: libtorrent announces once
/// from each listen socket, and each reaches only the tracker's addresses
/// of its own family (`http_tracker_connection::on_filter`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    V4,
    V6,
}

impl Family {
    pub(crate) fn of(ip: IpAddr) -> Self {
        match ip.to_canonical() {
            IpAddr::V4(_) => Self::V4,
            IpAddr::V6(_) => Self::V6,
        }
    }
}

/// The arguments an announce carries. A tracker URL whose own query names
/// one already is refused, the names compared as given, ignoring case.
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
/// sent: a tracker at a loopback address is asked only what a tracker
/// answers, at a path with a segment starting `announce`, or `scrape` for a
/// scrape, as `/announce.php`, `/<passkey>/announce` and
/// `/announce/<passkey>` have. The client picks the address it connects
/// to, so every address must pass. Only a tracker the operator named
/// reaches it: one that came with the torrent is never asked at loopback
/// (see [`NamedBy::may_ask`]). An e2e build asks any path.
fn ssrf_refusal(url: &url::Url, ask: Ask, addrs: &[IpAddr]) -> Option<&'static str> {
    if cfg!(feature = "e2e-local-trackers") {
        return None;
    }
    let word = match ask {
        Ask::Announce => "announce",
        Ask::Scrape => "scrape",
    };
    let loopback = addrs.iter().any(|a| a.to_canonical().is_loopback());
    let tracker_path = url.path().split('/').any(|seg| seg.starts_with(word));
    (loopback && !tracker_path).then_some("a loopback tracker asked at a path no tracker answers")
}

/// `url`'s host as a client is told it, and its addresses: the address it
/// names, or what `resolver` finds for its name. None when the session's
/// network resolves the name where it exits.
pub(crate) async fn addresses(
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

/// A client from `clients` for a GET of `url`, named by `named_by`, for
/// `ask`, and the addresses it is held to: it reaches the URL's host at the
/// addresses found for it now that the tracker may be asked at and the
/// session may reach (see [`only_reachable`]), in the order found, and
/// nowhere else, and follows no redirect. Refused when none is left, or
/// when [`ssrf_refusal`] holds the GET back from those left. Each address is
/// filtered on its own, so the order of the checks changes only which
/// refusal is named; [`ssrf_refusal`] refuses the GET for any loopback
/// address the client may pick, so it judges the addresses the client is
/// handed, after the filter. A name the session's network resolves where it
/// exits has no address here: nothing is checked, and the client hands the
/// name on. Its header names are written in `shape`'s case.
#[allow(clippy::too_many_arguments)]
async fn reach(
    clients: &dyn HttpClients,
    url: &url::Url,
    named_by: NamedBy,
    ask: Ask,
    family: Option<Family>,
    resolver: &dyn Resolver,
    reachable: &Reachable,
    shape: &AnnounceShape,
) -> anyhow::Result<(reqwest::Client, Vec<IpAddr>)> {
    let (host, mut found) = addresses(url, resolver).await?;
    if let Some(family) = family
        && !found.is_empty()
    {
        found.retain(|ip| Family::of(*ip) == family);
        anyhow::ensure!(!found.is_empty(), "the tracker has no {family:?} address");
    }
    let addrs = only_reachable(found, |ip| *ip, reachable, named_by)?;
    if let Some(why) = ssrf_refusal(url, ask, &addrs) {
        anyhow::bail!("SSRF mitigation: {why}");
    }
    let target = HttpTarget {
        host: &host,
        port: url.port_or_known_default().unwrap_or(0),
        addrs: &addrs,
    };
    let client = clients.client(
        Some(target),
        &ClientShape::of(ClientUse::Tracker {
            title_case: shape.title_case,
        }),
    )?;
    Ok((client, addrs))
}

/// The most redirects one GET follows.
const MAX_REDIRECTS: usize = 5;

/// Whether a redirect came from a tracker at a public address, reached at
/// one of `addrs` and answering from `endpoint`: the address it connected
/// to, when it is one of those, decides; otherwise every address must be
/// public, as the client may have picked any. With no address the name was
/// resolved where the session's network exits, and nothing is known.
fn moved_by_a_public_tracker(endpoint: Option<IpAddr>, addrs: &[IpAddr]) -> bool {
    let reached = endpoint.filter(|ip| addrs.iter().any(|a| a.to_canonical() == ip.to_canonical()));
    match reached {
        Some(ip) => is_public(ip),
        None => !addrs.is_empty() && addrs.iter().all(|a| is_public(*a)),
    }
}

/// The body of a tracker's answer to a GET for `ask` of `tracker`, named by
/// `named_by`, with `query` added ahead of the URL's own. The GET is not
/// sent when the URL's own query already names an announce argument, and
/// goes only where [`reach`] lets it. A redirect is a GET of where it
/// points, reached the same way before it is sent, at most
/// [`MAX_REDIRECTS`] of them; past that the redirect is the answer. A hop
/// answered at a public address (see [`moved_by_a_public_tracker`]) moves
/// the GET to public addresses only,
/// as a tracker that came with the torrent is held, whoever named the
/// first. The answer fails when the tracker sends nothing for
/// [`RECEIVE_TIMEOUT`], the status is not a success, or the body passes
/// [`MAX_RESPONSE_LENGTH`]. The bounds are the engine's own, so they hold
/// whatever client is used. `shape` places the URL's own query and names
/// the headers set.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn get(
    clients: &dyn HttpClients,
    resolver: &dyn Resolver,
    reachable: &Reachable,
    tracker: &url::Url,
    mut named_by: NamedBy,
    ask: Ask,
    family: Option<Family>,
    query: &str,
    user_agent: &str,
    shape: &AnnounceShape,
) -> anyhow::Result<Answer> {
    let mut url = tracker.clone();
    if tracker.query().is_some_and(has_tracker_query) {
        anyhow::bail!("SSRF mitigation: the tracker URL's query names an announce argument")
    }
    shape.place_query(&mut url, tracker.query(), query);
    let mut redirects = 0;
    let response = loop {
        let (client, addrs) = reach(
            clients, &url, named_by, ask, family, resolver, reachable, shape,
        )
        .await?;
        let request = client
            .get(url.clone())
            .headers(shape.headers(&url, user_agent)?);
        let response = tokio::time::timeout(RECEIVE_TIMEOUT, request.send())
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
        let endpoint = response.remote_addr().map(|a| a.ip());
        if moved_by_a_public_tracker(endpoint, &addrs) {
            named_by = NamedBy::Torrent;
        }
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
        fn client(
            &self,
            target: Option<HttpTarget<'_>>,
            shape: &ClientShape,
        ) -> anyhow::Result<reqwest::Client> {
            let target = target.expect("a tracker is asked through a target");
            self.0.lock().unwrap().push((
                target.host.to_owned(),
                target.port,
                target.addrs.to_vec(),
            ));
            crate::test_support::client(reqwest::Client::builder().no_proxy(), None, shape)
        }
    }

    // A tracker's client is built for exactly the addresses its name
    // resolved to that the session may reach; a name the network resolves
    // at its exit is handed on with no address and nothing checked.
    #[tokio::test]
    async fn a_trackers_client_carries_what_its_name_resolved_to() {
        // The documentation addresses are not public: the operator named
        // this tracker.
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
            reach(
                &clients,
                &url,
                NamedBy::Operator,
                Ask::Announce,
                None,
                &resolver,
                &reachable,
                &AnnounceShape::WEAVER,
            )
            .await
            .unwrap();
            assert_eq!(resolver.asked.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(
                *clients.0.lock().unwrap(),
                [("tracker.test".to_owned(), 6969, checked)]
            );
        }
    }
    // A tracker that came with the torrent is refused at a loopback,
    // private, link-local or unique-local address, at a tracker's path or
    // any other, and no client is built; one the operator named is asked at
    // loopback at a tracker's path. Under a name the network resolves at
    // its exit nothing is refused by address, whoever named it.
    // The e2e carveout asks a torrent's tracker at these local addresses.
    #[cfg(not(feature = "e2e-local-trackers"))]
    #[tokio::test]
    async fn a_tracker_the_torrent_named_is_asked_at_public_addresses_only() {
        let reachable: Reachable = std::sync::Arc::new(|_| Ok(()));
        let at = |ip: &str| Answering {
            answer: Resolved::Addresses(vec![SocketAddr::new(ip.parse().unwrap(), 80)]),
            asked: Default::default(),
        };
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "fd00::1",
        ] {
            for (path, ask) in [
                ("/announce", Ask::Announce),
                ("/scrape", Ask::Scrape),
                ("/admin", Ask::Announce),
            ] {
                let url = url::Url::parse(&format!("http://tracker.test{path}")).unwrap();
                let clients = Targets::default();
                let refused = reach(
                    &clients,
                    &url,
                    NamedBy::Torrent,
                    ask,
                    None,
                    &at(ip),
                    &reachable,
                    &AnnounceShape::WEAVER,
                )
                .await
                .err()
                .unwrap_or_else(|| panic!("{ip} {path} was asked"));
                assert!(
                    refused
                        .downcast_ref::<crate::tracker::tracker_comms::NotPublic>()
                        .is_some(),
                    "{ip} {path}: {refused:#}"
                );
                assert!(clients.0.lock().unwrap().is_empty(), "{ip} {path}");
            }
        }

        let url = url::Url::parse("http://tracker.test/announce").unwrap();
        let clients = Targets::default();
        let (_, addrs) = reach(
            &clients,
            &url,
            NamedBy::Operator,
            Ask::Announce,
            None,
            &at("127.0.0.1"),
            &reachable,
            &AnnounceShape::WEAVER,
        )
        .await
        .unwrap();
        assert_eq!(addrs, [IpAddr::from([127, 0, 0, 1])]);

        for named_by in [NamedBy::Torrent, NamedBy::Operator] {
            let clients = Targets::default();
            let at_exit = Answering {
                answer: Resolved::AtExit,
                asked: Default::default(),
            };
            reach(
                &clients,
                &url,
                named_by,
                Ask::Announce,
                None,
                &at_exit,
                &reachable,
                &AnnounceShape::WEAVER,
            )
            .await
            .unwrap();
            assert_eq!(
                *clients.0.lock().unwrap(),
                [("tracker.test".to_owned(), 80, Vec::new())]
            );
        }
    }

    // In an e2e build a tracker that came with the torrent is asked at any
    // address, at any path.
    #[cfg(feature = "e2e-local-trackers")]
    #[tokio::test]
    async fn an_e2e_build_asks_a_tracker_the_torrent_named_at_local_addresses() {
        let reachable: Reachable = std::sync::Arc::new(|_| Ok(()));
        let at = |ip: &str| Answering {
            answer: Resolved::Addresses(vec![SocketAddr::new(ip.parse().unwrap(), 80)]),
            asked: Default::default(),
        };
        let ask_at = |ip: &'static str, path: &str, ask: Ask| {
            let url = url::Url::parse(&format!("http://tracker.test{path}")).unwrap();
            let at = at(ip);
            let reachable = reachable.clone();
            async move {
                let clients = Targets::default();
                let asked = reach(
                    &clients,
                    &url,
                    NamedBy::Torrent,
                    ask,
                    None,
                    &at,
                    &reachable,
                    &AnnounceShape::WEAVER,
                )
                .await;
                let built = !clients.0.lock().unwrap().is_empty();
                (asked, built)
            }
        };

        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "fd00::1",
        ] {
            for (path, ask) in [
                ("/announce", Ask::Announce),
                ("/scrape", Ask::Scrape),
                ("/0123abcd/announce", Ask::Announce),
                ("/admin", Ask::Announce),
            ] {
                let (asked, built) = ask_at(ip, path, ask).await;
                let (_, addrs) = asked.unwrap_or_else(|e| panic!("{ip} {path}: {e:#}"));
                assert_eq!(addrs, [ip.parse::<IpAddr>().unwrap()], "{ip} {path}");
                assert!(built, "{ip} {path}");
            }
        }
    }

    use crate::tracker::qbittorrent::{self, CAPTURED_ANNOUNCE, CAPTURED_HASH, captured_request};

    // `paused` goes where any event goes, in either shape.
    #[test]
    fn a_paused_announce_says_so() {
        let info_hash = Id20::new(CAPTURED_HASH);
        let peer_id = Id20::new(*b"-WV0170-CDwwoycKIZ6y");
        for shape in [&AnnounceShape::WEAVER, &qbittorrent::SHAPE] {
            let request = TrackerRequest {
                event: Some(TrackerRequestEvent::Paused),
                ..captured_request(&info_hash, &peer_id, EncryptionPolicy::Disabled)
            };
            let query = request.as_querystring(shape);
            assert!(query.contains("&event=paused&"), "{query}");
        }
    }

    // Weaver's own announces list none of the session's addresses.
    #[test]
    fn a_weaver_announce_lists_no_own_address() {
        let info_hash = Id20::new(CAPTURED_HASH);
        let own: [IpAddr; 2] = ["2001:db8::1".parse().unwrap(), [198, 51, 100, 1].into()];
        let peer_id = Id20::new(*b"-WV0170-CDwwoycKIZ6y");
        let request = TrackerRequest {
            own_addrs: &own,
            ..captured_request(&info_hash, &peer_id, EncryptionPolicy::Disabled)
        };
        let query = request.as_querystring(&AnnounceShape::WEAVER);
        assert!(
            !query.contains("ipv4") && !query.contains("ipv6"),
            "{query}"
        );
    }

    // Weaver's query written out field by field: RFC 3986 escaping, the
    // request's own compact and no peer id, both crypto flags under a
    // policy that only encrypts, the key in decimal.
    fn weaver_by_hand(r: &TrackerRequest<'_>) -> String {
        use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
        const SET: &AsciiSet = &NON_ALPHANUMERIC
            .remove(b'-')
            .remove(b'.')
            .remove(b'_')
            .remove(b'~');
        let set = SET;
        let mut s = format!(
            "info_hash={}&peer_id={}",
            percent_encode(&r.info_hash.0, set),
            percent_encode(&r.peer_id.0, set)
        );
        if let Some(event) = r.event {
            s += &format!("&event={}", event.as_str());
        }
        s += &format!(
            "&port={}&uploaded={}&downloaded={}&left={}&corrupt={}&redundant={}&compact={}&no_peer_id={}&numwant={}",
            r.port,
            r.uploaded,
            r.downloaded,
            r.left,
            r.corrupt,
            r.redundant,
            u8::from(r.compact),
            u8::from(r.no_peer_id),
            r.numwant
        );
        s += match r.encryption {
            EncryptionPolicy::Disabled => "",
            EncryptionPolicy::Prefer => "&supportcrypto=1",
            EncryptionPolicy::Require => "&supportcrypto=1&requirecrypto=1",
        };
        if let Some(key) = r.key {
            s += &format!("&key={key}");
        }
        if let Some(id) = r.trackerid {
            s += &format!("&trackerid={}", percent_encode(id, set));
        }
        s
    }

    // libtorrent's query written out field by field, as
    // `http_tracker_connection::start` writes it.
    fn libtorrent_by_hand(r: &TrackerRequest<'_>) -> String {
        let esc = |bytes: &[u8]| -> String {
            bytes
                .iter()
                .map(|&b| {
                    if b.is_ascii_alphanumeric() || b"-_!.~*()".contains(&b) {
                        char::from(b).to_string()
                    } else {
                        format!("%{b:02x}")
                    }
                })
                .collect()
        };
        let mut s = format!(
            "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&corrupt={}",
            esc(&r.info_hash.0),
            esc(&r.peer_id.0),
            r.port,
            r.uploaded,
            r.downloaded,
            r.left,
            r.corrupt
        );
        if let Some(key) = r.key {
            s += &format!("&key={key:08X}");
        }
        if let Some(event) = r.event {
            s += &format!("&event={}", event.as_str());
        }
        s += &format!("&numwant={}&compact=1&no_peer_id=1", r.numwant);
        if r.encryption != EncryptionPolicy::Disabled {
            s += "&supportcrypto=1";
        }
        s += &format!("&redundant={}", r.redundant);
        if let Some(id) = r.trackerid {
            s += &format!("&trackerid={}", esc(id));
        }
        for ip in r
            .own_addrs
            .iter()
            .filter(|ip| Family::of(**ip) == Family::V4)
        {
            s += &format!("&ipv4={}", esc(ip.to_string().as_bytes()));
        }
        for ip in r
            .own_addrs
            .iter()
            .filter(|ip| Family::of(**ip) == Family::V6)
        {
            s += &format!("&ipv6={}", esc(ip.to_string().as_bytes()));
        }
        s
    }

    // Each row writes exactly the query its fields name, over every event,
    // policy, flag, key, tracker id and own address list, and a hash and
    // peer id that take in every byte.
    #[test]
    fn each_row_writes_its_query_field_by_field() {
        let own: [IpAddr; 3] = [
            "2001:db8::1".parse().unwrap(),
            [198, 51, 100, 1].into(),
            [203, 0, 113, 9].into(),
        ];
        let all: Vec<u8> = (0..=255).collect();
        let mut checked = 0;
        for chunk in all.chunks(20) {
            let mut bytes = [0u8; 20];
            bytes[..chunk.len()].copy_from_slice(chunk);
            let id = Id20::new(bytes);
            for event in [
                None,
                Some(TrackerRequestEvent::Started),
                Some(TrackerRequestEvent::Stopped),
                Some(TrackerRequestEvent::Completed),
                Some(TrackerRequestEvent::Paused),
            ] {
                for encryption in [
                    EncryptionPolicy::Disabled,
                    EncryptionPolicy::Prefer,
                    EncryptionPolicy::Require,
                ] {
                    for (compact, no_peer_id) in [(true, false), (false, true)] {
                        for key in [None, Some(0), Some(0xAEB9ABBF)] {
                            for trackerid in [None, Some(&b"a b&!*~"[..])] {
                                for own_addrs in [&own[..0], &own[..]] {
                                    let request = TrackerRequest {
                                        info_hash: &id,
                                        peer_id: &id,
                                        event,
                                        port: 6881,
                                        uploaded: 1 << 40,
                                        downloaded: 7,
                                        left: u64::MAX,
                                        corrupt: 3,
                                        redundant: 5,
                                        compact,
                                        no_peer_id,
                                        numwant: 200,
                                        key,
                                        trackerid,
                                        encryption,
                                        own_addrs,
                                    };
                                    assert_eq!(
                                        request.as_querystring(&AnnounceShape::WEAVER),
                                        weaver_by_hand(&request)
                                    );
                                    assert_eq!(
                                        request.as_querystring(&qbittorrent::SHAPE),
                                        libtorrent_by_hand(&request)
                                    );
                                    checked += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(checked, 13 * 5 * 3 * 2 * 3 * 2 * 2);
    }

    // A scrape escapes the hash as the row's announces do.
    #[test]
    fn a_scrape_escapes_as_its_rows_announces() {
        let id = Id20::new(CAPTURED_HASH);
        let request = captured_request(&id, &id, EncryptionPolicy::Disabled);
        for shape in [&AnnounceShape::WEAVER, &qbittorrent::SHAPE] {
            let announce = request.as_querystring(shape);
            let scrape = shape.scrape_query(&id);
            assert!(announce.starts_with(&format!("{scrape}&")), "{scrape}");
        }
    }

    // The request a tracker reads off the wire for a GET of `tracker` in
    // `shape`, the tracker answering an empty announce, and the port it
    // listened on, which `tracker` names as `PORT`.
    async fn sent(
        tracker: &str,
        shape: &AnnounceShape,
        query: &str,
        user_agent: &str,
    ) -> (String, u16) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let tracker = url::Url::parse(&tracker.replace("PORT", &port.to_string())).unwrap();
        let read = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            let body = b"d8:intervali1800e5:peers0:e";
            let answer = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(answer.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
            String::from_utf8(head).unwrap()
        });
        let reachable: Reachable = std::sync::Arc::new(|_| Ok(()));
        get(
            &Targets::default(),
            &crate::core::net::NoNames,
            &reachable,
            &tracker,
            NamedBy::Operator,
            Ask::Announce,
            None,
            query,
            user_agent,
            shape,
        )
        .await
        .unwrap();
        (read.await.unwrap(), port)
    }

    // In libtorrent's shape the tracker URL's own query comes first and the
    // headers are libtorrent's, in its order and case. reqwest adds an
    // `Accept: */*` libtorrent does not send, last.
    #[tokio::test]
    async fn a_libtorrent_get_carries_libtorrents_headers() {
        let (head, port) = sent(
            "http://127.0.0.1:PORT/announce?passkey=abc",
            &qbittorrent::SHAPE,
            CAPTURED_ANNOUNCE,
            "qBittorrent/5.3.0",
        )
        .await;
        assert_eq!(
            head,
            format!(
                "GET /announce?passkey=abc&{CAPTURED_ANNOUNCE} HTTP/1.1\r\n\
                 Host: 127.0.0.1:{port}\r\n\
                 User-Agent: qBittorrent/5.3.0\r\n\
                 Accept-Encoding: gzip\r\n\
                 Connection: close\r\n\
                 Accept: */*\r\n\r\n"
            )
        );
    }

    // Weaver's own GET puts its query ahead of the URL's and sets only its
    // user agent; reqwest adds the rest after it.
    #[tokio::test]
    async fn a_weaver_get_keeps_its_own_shape() {
        let (head, port) = sent(
            "http://127.0.0.1:PORT/announce?passkey=abc",
            &AnnounceShape::WEAVER,
            "info_hash=x",
            "Weaver/0.17.0",
        )
        .await;
        assert_eq!(
            head,
            format!(
                "GET /announce?info_hash=x&passkey=abc HTTP/1.1\r\n\
                 user-agent: Weaver/0.17.0\r\n\
                 accept: */*\r\n\
                 accept-encoding: zstd,gzip,deflate,br\r\n\
                 host: 127.0.0.1:{port}\r\n\r\n"
            )
        );
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
            encryption: EncryptionPolicy::Disabled,
            own_addrs: &[],
        };
        dbg!(request.as_querystring(&AnnounceShape::WEAVER));
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
                encryption: EncryptionPolicy::Disabled,
                own_addrs: &[],
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
                request.as_querystring(&AnnounceShape::WEAVER),
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

    // A compact answer naming many peers in `peers` and `peers6` gives
    // every one, in the order named.
    #[test]
    fn a_compact_answer_gives_every_peer_of_both_lists() {
        let v4: Vec<SocketAddr> = (1..=50u8)
            .map(|i| SocketAddr::from(([10, 0, 0, i], 6000 + u16::from(i))))
            .collect();
        let v6: Vec<SocketAddr> = (1..=20u16)
            .map(|i| SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, i], 7000 + i)))
            .collect();
        let compact = |peers: &[SocketAddr]| -> Vec<u8> {
            peers
                .iter()
                .flat_map(|peer| {
                    let ip = match peer.ip() {
                        IpAddr::V4(ip) => ip.octets().to_vec(),
                        IpAddr::V6(ip) => ip.octets().to_vec(),
                    };
                    [ip, peer.port().to_be_bytes().to_vec()].concat()
                })
                .collect()
        };
        let (v4_bytes, v6_bytes) = (compact(&v4), compact(&v6));
        let mut data = b"d8:completei1151e10:incompletei25e8:intervali1800e".to_vec();
        data.extend(format!("5:peers{}:", v4_bytes.len()).bytes());
        data.extend(&v4_bytes);
        data.extend(format!("6:peers6{}:", v6_bytes.len()).bytes());
        data.extend(&v6_bytes);
        data.push(b'e');

        let response = crate::bencode::from_bytes::<TrackerResponse>(&data).unwrap();
        let expected: Vec<SocketAddr> = v4.iter().chain(&v6).copied().collect();
        assert_eq!(response.iter_peers().collect::<Vec<_>>(), expected);
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
            encryption: EncryptionPolicy::Disabled,
            own_addrs: &[],
        };
        assert!(
            request
                .as_querystring(&AnnounceShape::WEAVER)
                .ends_with("&numwant=200&trackerid=a%20b%26"),
            "{}",
            request.as_querystring(&AnnounceShape::WEAVER)
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

    #[cfg(not(feature = "e2e-local-trackers"))]
    #[test]
    fn a_loopback_tracker_is_asked_only_at_a_tracker_path() {
        // Only a tracker the operator named reaches this rule.
        let url = |s: &str| url::Url::parse(s).unwrap();
        let loopback: [IpAddr; 2] = ["127.0.0.2".parse().unwrap(), "::1".parse().unwrap()];
        let lan: IpAddr = "192.168.1.2".parse().unwrap();
        let global: IpAddr = "93.184.216.34".parse().unwrap();
        for a in loopback {
            for (u, ask) in [
                ("http://h/announce", Ask::Announce),
                ("http://h/announce.php?x=1", Ask::Announce),
                ("http://h/scrape.php", Ask::Scrape),
                // Ocelot's and UNIT3D's passkey paths.
                ("http://h/0123abcd/announce", Ask::Announce),
                ("http://h/0123abcd/scrape", Ask::Scrape),
                ("http://h/announce/0123abcd", Ask::Announce),
            ] {
                assert_eq!(ssrf_refusal(&url(u), ask, &[a]), None, "{u} {a}");
            }
            for (u, ask) in [
                ("http://h/admin", Ask::Announce),
                ("http://h/x/announce", Ask::Scrape),
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

    #[test]
    fn a_redirect_is_trusted_as_the_address_that_answered_it() {
        let public: IpAddr = "93.184.216.34".parse().unwrap();
        let lan: IpAddr = "192.168.1.2".parse().unwrap();
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let proxy: IpAddr = "10.9.9.9".parse().unwrap();
        // A name found at one public and one local address: the one that
        // answered decides, whichever was found first.
        for local in [lan, loopback] {
            for found in [[public, local], [local, public]] {
                assert!(moved_by_a_public_tracker(Some(public), &found), "{found:?}");
                assert!(!moved_by_a_public_tracker(Some(local), &found), "{found:?}");
                // Written as v6, the same address.
                let mapped = match public {
                    IpAddr::V4(v4) => IpAddr::V6(v4.to_ipv6_mapped()),
                    v6 => v6,
                };
                assert!(moved_by_a_public_tracker(Some(mapped), &found));
                // Unknown, or an address it was not reached at (a proxy's):
                // any of them may have answered, so not every one is public.
                for endpoint in [None, Some(proxy)] {
                    assert!(!moved_by_a_public_tracker(endpoint, &found), "{endpoint:?}");
                }
            }
        }
        // Every address public: public whatever the endpoint says.
        let both = [public, "8.8.8.8".parse().unwrap()];
        for endpoint in [None, Some(public), Some(loopback), Some(proxy)] {
            assert!(moved_by_a_public_tracker(endpoint, &both), "{endpoint:?}");
        }
        // Every address local, and none at all: never.
        for endpoint in [None, Some(lan), Some(public)] {
            assert!(!moved_by_a_public_tracker(endpoint, &[lan, loopback]));
            assert!(!moved_by_a_public_tracker(endpoint, &[]));
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
