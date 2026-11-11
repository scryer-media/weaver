// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{HashMap, hash_map::Entry},
    ffi::CStr,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::core::{
    hash_id::Id20,
    net::{DatagramSocket, NoNames, Resolver},
    spawn_utils::spawn_with_cancel,
};
use anyhow::{Context, bail};
use parking_lot::RwLock;
use rand::RngExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, trace, warn};

use crate::tracker::tracker_comms_http::TrackerRequestEvent;

pub(crate) const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

pub const EVENT_NONE: u32 = 0;
pub const EVENT_COMPLETED: u32 = 1;
pub const EVENT_STARTED: u32 = 2;
pub const EVENT_STOPPED: u32 = 3;
/// Past BEP 15's events: libtorrent writes its `event_t::paused` as is
/// (`udp_tracker_connection::send_udp_announce`).
pub const EVENT_PAUSED: u32 = 4;

/// The `event` field of a UDP announce (BEP 15).
pub(crate) fn udp_event(event: Option<TrackerRequestEvent>) -> u32 {
    match event {
        None => EVENT_NONE,
        Some(TrackerRequestEvent::Completed) => EVENT_COMPLETED,
        Some(TrackerRequestEvent::Started) => EVENT_STARTED,
        Some(TrackerRequestEvent::Stopped) => EVENT_STOPPED,
        Some(TrackerRequestEvent::Paused) => EVENT_PAUSED,
    }
}

pub type ConnectionId = u64;
pub(crate) const CONNECTION_ID_MAGIC: ConnectionId = 0x41727101980;

pub type TransactionId = u32;

pub fn new_transaction_id() -> TransactionId {
    rand::rng().random()
}

#[derive(Debug)]
pub struct AnnounceFields {
    pub info_hash: Id20,
    pub peer_id: Id20,
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
    pub event: u32,
    pub key: u32,
    /// How many peers to ask for.
    pub num_want: u32,
    pub port: u16,
    /// The tracker URL's path and query, sent as the URL data option
    /// (BEP 41); none when empty.
    pub url_data: String,
}

/// The URL data option's type (BEP 41).
const OPTION_URL_DATA: u8 = 2;

/// The most bytes of URL data one announce carries: one option's length is
/// a byte, and libtorrent sends no more than this, in one option
/// (`udp_tracker_connection::send_udp_announce`).
const MAX_URL_DATA: usize = 255;

/// The URL data of an announce to `url`: its path and query, as libtorrent
/// takes them from the URL (`udp_tracker_connection::send_udp_announce`).
pub(crate) fn url_data(url: &url::Url) -> String {
    let mut data = url.path().to_owned();
    if let Some(query) = url.query() {
        data.push('?');
        data.push_str(query);
    }
    data
}

#[derive(Debug)]
pub enum Request {
    Connect,
    Announce(ConnectionId, AnnounceFields),
    /// One torrent's counts (BEP 15 scrape, action 2).
    Scrape(ConnectionId, Id20),
}

impl Request {
    pub fn serialize(
        &self,
        transaction_id: TransactionId,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        struct W<'a> {
            buf: &'a mut [u8],
            offset: usize,
        }
        impl W<'_> {
            fn extend_from_slice(&mut self, s: &[u8]) -> anyhow::Result<()> {
                if self.buf.len() < self.offset + s.len() {
                    bail!("not enough space in buffer")
                }
                self.buf[self.offset..self.offset + s.len()].copy_from_slice(s);
                self.offset += s.len();
                Ok(())
            }
        }

        let mut w = W { buf, offset: 0 };

        match self {
            Request::Connect => {
                w.extend_from_slice(&CONNECTION_ID_MAGIC.to_be_bytes())?;
                w.extend_from_slice(&ACTION_CONNECT.to_be_bytes())?;
                w.extend_from_slice(&transaction_id.to_be_bytes())?;
            }
            Request::Announce(connection_id, fields) => {
                w.extend_from_slice(&connection_id.to_be_bytes())?;
                w.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes())?;
                w.extend_from_slice(&transaction_id.to_be_bytes())?;
                w.extend_from_slice(&fields.info_hash.0)?;
                w.extend_from_slice(&fields.peer_id.0)?;
                w.extend_from_slice(&fields.downloaded.to_be_bytes())?;
                w.extend_from_slice(&fields.left.to_be_bytes())?;
                w.extend_from_slice(&fields.uploaded.to_be_bytes())?;
                w.extend_from_slice(&fields.event.to_be_bytes())?;
                w.extend_from_slice(&0u32.to_be_bytes())?; // ip address 0
                w.extend_from_slice(&fields.key.to_be_bytes())?;
                w.extend_from_slice(&fields.num_want.to_be_bytes())?;
                w.extend_from_slice(&fields.port.to_be_bytes())?;
                let data = fields.url_data.as_bytes();
                let data = &data[..data.len().min(MAX_URL_DATA)];
                if !data.is_empty() {
                    w.extend_from_slice(&[OPTION_URL_DATA, data.len() as u8])?;
                    w.extend_from_slice(data)?;
                }
            }
            Request::Scrape(connection_id, info_hash) => {
                w.extend_from_slice(&connection_id.to_be_bytes())?;
                w.extend_from_slice(&ACTION_SCRAPE.to_be_bytes())?;
                w.extend_from_slice(&transaction_id.to_be_bytes())?;
                w.extend_from_slice(&info_hash.0)?;
            }
        }
        Ok(w.offset)
    }
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub addrs: Vec<SocketAddr>,
}

/// A torrent's counts, as a scrape reply gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeResponse {
    pub complete: u32,
    pub downloaded: u32,
    pub incomplete: u32,
}

#[derive(Debug)]
pub enum Response {
    Connect(ConnectionId),
    Announce(AnnounceResponse),
    Scrape(ScrapeResponse),
    #[allow(dead_code)]
    Error(String),
    Unknown,
}

fn split_slice(s: &[u8], first_len: usize) -> Option<(&[u8], &[u8])> {
    if s.len() < first_len {
        return None;
    }
    Some(s.split_at(first_len))
}

fn s_to_arr<const T: usize>(buf: &[u8]) -> [u8; T] {
    let mut arr = [0u8; T];
    arr.copy_from_slice(buf);
    arr
}

trait ParseNum: Sized {
    fn parse_num(buf: &[u8]) -> anyhow::Result<(Self, &[u8])>;
}

macro_rules! parse_impl {
    ($ty:tt, $size:expr) => {
        impl ParseNum for $ty {
            fn parse_num(buf: &[u8]) -> anyhow::Result<($ty, &[u8])> {
                let (bytes, rest) =
                    split_slice(buf, $size).with_context(|| format!("expected {} bytes", $size))?;
                let num = $ty::from_be_bytes(s_to_arr(bytes));
                Ok((num, rest))
            }
        }
    };
}

parse_impl!(u32, 4);
parse_impl!(u64, 8);
parse_impl!(u128, 16);
parse_impl!(u16, 2);
parse_impl!(i32, 4);
parse_impl!(i64, 8);
parse_impl!(i16, 2);

/// The transaction id every reply carries in its second word.
fn reply_transaction_id(buf: &[u8]) -> anyhow::Result<TransactionId> {
    let (_action, buf) = u32::parse_num(buf).context("can't parse action")?;
    let (tid, _) = u32::parse_num(buf).context("can't parse transaction id")?;
    Ok(tid)
}

/// Whether an announce reply from `tracker` lists IPv6 peers (18 bytes each)
/// rather than IPv4 ones (6 bytes each). BEP 15 ties the format to the
/// family of the tracker's address, so it is read from the address the
/// request went to, an IPv4-mapped IPv6 address counting as IPv4; never from
/// the address the reply arrived from, which a dual-stack socket reports
/// mapped.
fn lists_ipv6_peers(tracker: SocketAddr) -> bool {
    tracker.ip().to_canonical().is_ipv6()
}

impl Response {
    pub fn parse(buf: &[u8], is_ipv6: bool) -> anyhow::Result<(TransactionId, Self)> {
        let (action, buf) = u32::parse_num(buf).context("can't parse action")?;
        let (tid, buf) = u32::parse_num(buf).context("can't parse transaction id")?;

        let response = match Self::parse_response(action, is_ipv6, buf) {
            Ok(r) => r,
            Err(e) => {
                debug!("error parsing: {e:#}");
                Response::Unknown
            }
        };

        Ok((tid, response))
    }

    fn parse_response(action: u32, is_ipv6: bool, mut buf: &[u8]) -> anyhow::Result<Self> {
        let response = match action {
            ACTION_CONNECT => {
                let (connection_id, b) =
                    u64::parse_num(buf).context("can't parse connection id")?;
                buf = b;
                Response::Connect(connection_id)
            }
            ACTION_ANNOUNCE => {
                let (interval, b) = u32::parse_num(buf).context("can't parse interval")?;
                let (leechers, b) = u32::parse_num(b).context("can't parse leechers")?;
                let (seeders, mut b) = u32::parse_num(b).context("can't parse seeders")?;
                let mut addrs = Vec::new();
                while !b.is_empty() {
                    let (addr, b2) = if is_ipv6 {
                        let (ip, b2) = u128::parse_num(b)?;
                        (IpAddr::V6(Ipv6Addr::from(ip)), b2)
                    } else {
                        let (ip, b2) = u32::parse_num(b)?;
                        (IpAddr::V4(Ipv4Addr::from(ip)), b2)
                    };
                    b = b2;

                    let (port, b2) = u16::parse_num(b)?;
                    b = b2;
                    addrs.push(SocketAddr::new(addr, port));
                }
                buf = b;
                Response::Announce(AnnounceResponse {
                    interval,
                    leechers,
                    seeders,
                    addrs,
                })
            }
            ACTION_SCRAPE => {
                // The first torrent's counts; a tracker that adds more past
                // them is not refused (libtorrent `on_scrape_response`).
                let (complete, b) = u32::parse_num(buf).context("can't parse complete")?;
                let (downloaded, b) = u32::parse_num(b).context("can't parse downloaded")?;
                let (incomplete, _) = u32::parse_num(b).context("can't parse incomplete")?;
                return Ok(Response::Scrape(ScrapeResponse {
                    complete,
                    downloaded,
                    incomplete,
                }));
            }
            ACTION_ERROR => {
                let msg = CStr::from_bytes_with_nul(buf)
                    .ok()
                    .and_then(|s| s.to_str().ok())
                    .or_else(|| std::str::from_utf8(buf).ok())
                    .unwrap_or("<invalid UTF-8>")
                    .to_owned();
                return Ok(Response::Error(msg));
            }
            _ => bail!("unsupported action {action}"),
        };

        if !buf.is_empty() {
            bail!(
                "parsed {response:?} so far, but got {} remaining bytes",
                buf.len()
            );
        }

        Ok(response)
    }
}

struct ConnectionIdMeta {
    id: ConnectionId,
    created: Instant,
}

#[derive(Default)]
struct ClientLocked {
    connections: HashMap<SocketAddr, ConnectionIdMeta>,
    // Each request's raw reply goes back to it to parse, as only the request
    // knows the tracker address that fixes the reply's peer format.
    transactions: HashMap<TransactionId, tokio::sync::oneshot::Sender<Vec<u8>>>,
}

struct ClientShared {
    sock: Arc<dyn DatagramSocket>,
    resolver: Arc<dyn Resolver>,
    locked: RwLock<ClientLocked>,
}

#[derive(Clone)]
pub struct UdpTrackerClient {
    state: Arc<ClientShared>,
}

struct TransactionIdGuard<'a> {
    tid: TransactionId,
    state: &'a ClientShared,
}

impl Drop for TransactionIdGuard<'_> {
    fn drop(&mut self) {
        let mut g = self.state.locked.write();
        g.transactions.remove(&self.tid);
    }
}

impl UdpTrackerClient {
    /// Create a client that sends and receives tracker traffic through
    /// `socket`.
    ///
    /// Tracker hostnames are looked up through `resolver`; with none, only
    /// trackers named by their address are asked.
    pub fn new_with_socket(
        cancel_token: CancellationToken,
        socket: Arc<dyn DatagramSocket>,
        resolver: Option<Arc<dyn Resolver>>,
    ) -> Self {
        let resolver = resolver.unwrap_or_else(|| Arc::new(NoNames));
        Self::spawn(cancel_token, socket, resolver)
    }

    fn spawn(
        cancel_token: CancellationToken,
        sock: Arc<dyn DatagramSocket>,
        resolver: Arc<dyn Resolver>,
    ) -> Self {
        let client = Self {
            state: Arc::new(ClientShared {
                sock,
                resolver,
                locked: RwLock::new(Default::default()),
            }),
        };

        spawn_with_cancel(debug_span!("udp_tracker"), "udp_tracker", cancel_token, {
            let client = client.clone();
            async move { client.run().await }
        });

        client
    }

    /// The resolver for tracker hostnames.
    pub(crate) fn resolver(&self) -> &dyn Resolver {
        &*self.state.resolver
    }

    async fn run(self) -> anyhow::Result<()> {
        let mut buf = [0u8; 16384];
        loop {
            let (len, addr) = match self.state.sock.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("error in UdpSocket::recv_from: {e:#}");
                    continue;
                }
            };

            let tid = match reply_transaction_id(&buf[..len]) {
                Ok(tid) => tid,
                Err(e) => {
                    debug!(?addr, "error parsing UDP response: {e:#}");
                    continue;
                }
            };

            trace!(?tid, len, ?addr, "received");

            let t = self.state.locked.write().transactions.remove(&tid);
            match t {
                Some(tx) => match tx.send(buf[..len].to_vec()) {
                    Ok(_) => {}
                    Err(_) => {
                        debug!(tid, "reader dead");
                    }
                },
                None => {
                    debug!(tid, "nowhere to send response");
                }
            };
        }
    }

    pub(crate) async fn get_connection_id(&self, addr: SocketAddr) -> anyhow::Result<ConnectionId> {
        if let Some(m) = self.state.locked.read().connections.get(&addr)
            && m.created.elapsed() < Duration::from_secs(60)
        {
            return Ok(m.id);
        }

        let response = self.request(addr, Request::Connect).await?;
        match response {
            Response::Connect(connection_id) => {
                self.state.locked.write().connections.insert(
                    addr,
                    ConnectionIdMeta {
                        id: connection_id,
                        created: Instant::now(),
                    },
                );
                Ok(connection_id)
            }
            _ => anyhow::bail!("expected connect response"),
        }
    }

    async fn request(&self, addr: SocketAddr, request: Request) -> anyhow::Result<Response> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tid_g = self.reserve_transaction_id(tx)?;

        let mut write_buf = [0u8; 1024];
        let len = request.serialize(tid_g.tid, &mut write_buf)?;
        self.state
            .sock
            .send_to(&write_buf[..len], addr)
            .await
            .with_context(|| format!("error sending to {addr:?}"))?;

        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .context("timeout connecting")?
            .context("sender dead")?;
        let (_, response) = Response::parse(&reply, lists_ipv6_peers(addr))?;
        trace!(tid = tid_g.tid, ?response, ?addr, "parsed");
        match &response {
            Response::Error(e) => {
                anyhow::bail!("remote errored: {e}")
            }
            Response::Unknown => {
                anyhow::bail!("remote replied with something we could not parse")
            }
            _ => {}
        }
        Ok(response)
    }

    fn reserve_transaction_id(
        &self,
        tx: tokio::sync::oneshot::Sender<Vec<u8>>,
    ) -> anyhow::Result<TransactionIdGuard<'_>> {
        let mut g = self.state.locked.write();
        for _ in 0..10 {
            let t = new_transaction_id();
            match g.transactions.entry(t) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(vac) => {
                    vac.insert(tx);
                    return Ok(TransactionIdGuard {
                        tid: t,
                        state: &self.state,
                    });
                }
            }
        }
        bail!("cant generate transaction id")
    }

    pub async fn announce(
        &self,
        tracker: SocketAddr,
        fields: AnnounceFields,
    ) -> anyhow::Result<AnnounceResponse> {
        let connection_id = self.get_connection_id(tracker).await?;
        let request = Request::Announce(connection_id, fields);
        let response = self.request(tracker, request).await?;
        match response {
            Response::Announce(r) => Ok(r),
            other => bail!("unexpected response {other:?}, expected announce"),
        }
    }

    /// The tracker's counts for `info_hash` (BEP 15 scrape).
    pub async fn scrape(
        &self,
        tracker: SocketAddr,
        info_hash: Id20,
    ) -> anyhow::Result<ScrapeResponse> {
        let connection_id = self.get_connection_id(tracker).await?;
        let response = self
            .request(tracker, Request::Scrape(connection_id, info_hash))
            .await?;
        match response {
            Response::Scrape(r) => Ok(r),
            other => bail!("unexpected response {other:?}, expected scrape"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use crate::core::{hash_id::Id20, net::DatagramSocket};
    use tokio_util::sync::CancellationToken;

    use crate::tracker::tracker_comms_udp::{
        ACTION_CONNECT, AnnounceFields, CONNECTION_ID_MAGIC, EVENT_NONE, Request, Response,
        ScrapeResponse, UdpTrackerClient,
    };

    #[tokio::test]
    async fn test_connect_through_custom_socket() {
        const CONNECTION_ID: u64 = 0x1122_3344_5566_7788;

        let tracker = crate::test_support::loopback_udp().await;
        let tracker_addr = tracker.bind_addr();

        let client_sock = crate::test_support::loopback_udp().await;
        let client_addr = client_sock.bind_addr();

        let token = CancellationToken::new();
        let client = UdpTrackerClient::new_with_socket(token.clone(), client_sock, None);

        let fake_tracker = async {
            let mut buf = [0u8; 1024];
            let (len, from): (usize, SocketAddr) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(from, client_addr);
            assert_eq!(len, 16);
            assert_eq!(buf[..8], CONNECTION_ID_MAGIC.to_be_bytes());
            assert_eq!(buf[8..12], ACTION_CONNECT.to_be_bytes());
            let tid = &buf[12..16];

            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            reply.extend_from_slice(tid);
            reply.extend_from_slice(&CONNECTION_ID.to_be_bytes());
            tracker.send_to(&reply, from).await.unwrap();
        };

        let (connection_id, ()) =
            tokio::join!(client.get_connection_id(tracker_addr), fake_tracker);
        assert_eq!(connection_id.unwrap(), CONNECTION_ID);
        token.cancel();
    }

    #[test]
    fn test_announce_request_carries_the_event() {
        use crate::tracker::tracker_comms_http::TrackerRequestEvent as E;
        for (event, wire) in [
            (None, 0u32),
            (Some(E::Completed), 1),
            (Some(E::Started), 2),
            (Some(E::Stopped), 3),
            (Some(E::Paused), 4),
        ] {
            let request = Request::Announce(
                1,
                AnnounceFields {
                    info_hash: Id20::new([1; 20]),
                    peer_id: Id20::new([2; 20]),
                    downloaded: 0,
                    left: 0,
                    uploaded: 0,
                    event: super::udp_event(event),
                    key: 0,
                    num_want: 7,
                    port: 6881,
                    url_data: String::new(),
                },
            );
            let mut buf = [0u8; 128];
            let len = request.serialize(0, &mut buf).unwrap();
            assert_eq!(len, 98);
            // connection id, action, transaction id, info hash, peer id,
            // downloaded, left, uploaded, then the event (BEP 15).
            assert_eq!(buf[80..84], wire.to_be_bytes());
            // Then ip, key, num_want and port.
            assert_eq!(buf[92..96], 7u32.to_be_bytes());
        }
    }

    fn announce_with(url_data: &str) -> (usize, [u8; 512]) {
        let request = Request::Announce(
            1,
            AnnounceFields {
                info_hash: Id20::new([1; 20]),
                peer_id: Id20::new([2; 20]),
                downloaded: 0,
                left: 0,
                uploaded: 0,
                event: 0,
                key: 0,
                num_want: 7,
                port: 6881,
                url_data: url_data.to_owned(),
            },
        );
        let mut buf = [0u8; 512];
        let len = request.serialize(0, &mut buf).unwrap();
        (len, buf)
    }

    // The URL data option follows the port: type 2, its length in a byte,
    // then the path and query as the URL gives them (BEP 41; libtorrent
    // `udp_tracker_connection::send_udp_announce`). None goes with none.
    #[test]
    fn an_announce_carries_the_trackers_path_and_query() {
        let url = url::Url::parse("udp://tracker.test:6969/announce/k3y?passkey=a%20b").unwrap();
        let data = super::url_data(&url);
        assert_eq!(data, "/announce/k3y?passkey=a%20b");
        let (len, buf) = announce_with(&data);
        assert_eq!(len, 98 + 2 + data.len());
        assert_eq!(buf[98], 2);
        assert_eq!(usize::from(buf[99]), data.len());
        assert_eq!(&buf[100..len], data.as_bytes());

        let bare = url::Url::parse("udp://tracker.test:6969").unwrap();
        assert_eq!(super::url_data(&bare), "");
        assert_eq!(announce_with("").0, 98);
    }

    // URL data past 255 bytes is cut to 255, in one option.
    #[test]
    fn url_data_is_cut_to_255_bytes() {
        let long = format!("/{}", "a".repeat(300));
        let (len, buf) = announce_with(&long);
        assert_eq!(len, 98 + 2 + 255);
        assert_eq!(buf[98..100], [2, 255]);
        assert_eq!(&buf[100..len], &long.as_bytes()[..255]);
        let (len, buf) = announce_with(&long[..255]);
        assert_eq!(len, 98 + 2 + 255);
        assert_eq!(buf[99], 255);
    }

    #[test]
    fn scrape_request_and_reply_follow_bep_15() {
        let mut buf = [0u8; 64];
        let len = Request::Scrape(0x0102_0304_0506_0708, Id20::new([9; 20]))
            .serialize(0x1112_1314, &mut buf)
            .unwrap();
        // connection id, action 2, transaction id, then the info hash.
        assert_eq!(len, 36);
        assert_eq!(buf[..8], 0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(buf[8..12], 2u32.to_be_bytes());
        assert_eq!(buf[12..16], 0x1112_1314u32.to_be_bytes());
        assert_eq!(buf[16..36], [9; 20]);

        // complete, downloaded, incomplete; a second torrent's counts past
        // them are not refused (libtorrent `on_scrape_response`).
        let reply = scrape_reply(&5u32.to_be_bytes(), [4, 9, 6]);
        for reply in [reply.clone(), [reply, vec![0; 12]].concat()] {
            let (tid, response) = Response::parse(&reply, false).unwrap();
            assert_eq!(tid, 5);
            match response {
                Response::Scrape(r) => assert_eq!(
                    r,
                    ScrapeResponse {
                        complete: 4,
                        downloaded: 9,
                        incomplete: 6,
                    }
                ),
                other => panic!("unexpected response {other:?}"),
            }
        }

        // Fewer than three counts is no answer.
        let short = &scrape_reply(&5u32.to_be_bytes(), [4, 9, 6])[..16];
        assert!(matches!(
            Response::parse(short, false).unwrap().1,
            Response::Unknown
        ));
    }

    fn scrape_reply(tid: &[u8], [complete, downloaded, incomplete]: [u32; 3]) -> Vec<u8> {
        let mut b = super::ACTION_SCRAPE.to_be_bytes().to_vec();
        b.extend_from_slice(tid);
        for n in [complete, downloaded, incomplete] {
            b.extend_from_slice(&n.to_be_bytes());
        }
        b
    }

    #[tokio::test]
    async fn scrape_connects_then_asks_for_the_torrents_counts() {
        let tracker_addr: SocketAddr = "192.0.2.1:6969".parse().unwrap();
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel();
        let token = CancellationToken::new();
        let client = UdpTrackerClient::new_with_socket(
            token.clone(),
            Arc::new(FakeSocket {
                sent: sent_tx,
                replies: parking_lot::Mutex::new(reply_rx),
            }),
            None,
        );

        let tracker = async move {
            let mut actions = Vec::new();
            while let Some((request, to)) = sent_rx.recv().await {
                assert_eq!(to, tracker_addr);
                let tid = &request[12..16];
                let action = u32::from_be_bytes(request[8..12].try_into().unwrap());
                actions.push(action);
                let reply = if action == ACTION_CONNECT {
                    let mut b = ACTION_CONNECT.to_be_bytes().to_vec();
                    b.extend_from_slice(tid);
                    b.extend_from_slice(&7u64.to_be_bytes());
                    b
                } else {
                    assert_eq!(request[..8], 7u64.to_be_bytes());
                    assert_eq!(request[16..36], [1; 20]);
                    scrape_reply(tid, [3, 8, 2])
                };
                reply_tx.send((reply, to)).unwrap();
            }
            actions
        };
        let tracker = tokio::spawn(tracker);

        let r = client
            .scrape(tracker_addr, Id20::new([1; 20]))
            .await
            .unwrap();
        assert_eq!(
            r,
            ScrapeResponse {
                complete: 3,
                downloaded: 8,
                incomplete: 2,
            }
        );
        token.cancel();
        drop(client);
        assert_eq!(tracker.await.unwrap(), vec![ACTION_CONNECT, 2]);
    }

    #[test]
    fn test_parse_announce() {
        let b = include_bytes!("../../resources/test/udp-tracker-announce-response.bin");
        let (tid, response) = Response::parse(b, false).unwrap();
        dbg!(tid, response);
    }

    #[test]
    fn test_parse_announce_counts() {
        let mut b = Vec::new();
        b.extend_from_slice(&super::ACTION_ANNOUNCE.to_be_bytes());
        b.extend_from_slice(&7u32.to_be_bytes()); // transaction id
        b.extend_from_slice(&1800u32.to_be_bytes()); // interval
        b.extend_from_slice(&5u32.to_be_bytes()); // leechers
        b.extend_from_slice(&3u32.to_be_bytes()); // seeders
        b.extend_from_slice(&[127, 0, 0, 1, 0x1a, 0xe1]);
        let (tid, response) = Response::parse(&b, false).unwrap();
        assert_eq!(tid, 7);
        match response {
            Response::Announce(r) => {
                assert_eq!(r.interval, 1800);
                assert_eq!(r.leechers, 5);
                assert_eq!(r.seeders, 3);
                assert_eq!(r.addrs, vec!["127.0.0.1:6881".parse().unwrap()]);
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    fn announce_reply(tid: &[u8], peers: &[SocketAddr]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&super::ACTION_ANNOUNCE.to_be_bytes());
        b.extend_from_slice(tid);
        b.extend_from_slice(&1800u32.to_be_bytes()); // interval
        b.extend_from_slice(&0u32.to_be_bytes()); // leechers
        b.extend_from_slice(&1u32.to_be_bytes()); // seeders
        for peer in peers {
            match peer {
                SocketAddr::V4(a) => b.extend_from_slice(&a.ip().octets()),
                SocketAddr::V6(a) => b.extend_from_slice(&a.ip().octets()),
            }
            b.extend_from_slice(&peer.port().to_be_bytes());
        }
        b
    }

    #[test]
    fn test_parse_ipv6_announce() {
        let peers: Vec<SocketAddr> = vec![
            "[2001:db8::2]:6881".parse().unwrap(),
            "[2001:db8::3]:51413".parse().unwrap(),
        ];
        let b = announce_reply(&9u32.to_be_bytes(), &peers);
        assert_eq!(b.len(), 20 + 2 * 18);
        let (tid, response) = Response::parse(&b, true).unwrap();
        assert_eq!(tid, 9);
        match response {
            Response::Announce(r) => assert_eq!(r.addrs, peers),
            other => panic!("unexpected response {other:?}"),
        }
    }

    /// An in-process socket: what the client sends comes out of `sent`, and
    /// what is put into `replies` is what it receives.
    struct FakeSocket {
        sent: tokio::sync::mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
        replies: parking_lot::Mutex<tokio::sync::mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>>,
    }

    impl DatagramSocket for FakeSocket {
        fn bind_addr(&self) -> SocketAddr {
            (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
        }

        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(async move {
                let (reply, from) = std::future::poll_fn(|cx| self.replies.lock().poll_recv(cx))
                    .await
                    .ok_or_else(|| std::io::Error::other("no more replies"))?;
                buf[..reply.len()].copy_from_slice(&reply);
                Ok((reply.len(), from))
            })
        }

        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            target: SocketAddr,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<usize>> {
            let sent = self.sent.send((buf.to_vec(), target));
            Box::pin(async move {
                sent.map_err(|_| std::io::Error::other("the tracker is gone"))?;
                Ok(buf.len())
            })
        }

        fn poll_send_to(
            &self,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
            _target: SocketAddr,
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
        }
    }

    #[tokio::test]
    async fn test_announce_reply_format_follows_the_tracker_address() {
        let v4_tracker: SocketAddr = "192.0.2.1:6969".parse().unwrap();
        let v6_tracker: SocketAddr = "[2001:db8::1]:6969".parse().unwrap();
        let v4_peer: SocketAddr = "192.0.2.7:6881".parse().unwrap();
        let v6_peer: SocketAddr = "[2001:db8::7]:6881".parse().unwrap();

        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel();
        let token = CancellationToken::new();
        let client = UdpTrackerClient::new_with_socket(
            token.clone(),
            Arc::new(FakeSocket {
                sent: sent_tx,
                replies: parking_lot::Mutex::new(reply_rx),
            }),
            None,
        );

        // Each tracker lists one peer of its own family. A dual-stack socket
        // reports the IPv4 tracker's replies as from its IPv4-mapped address.
        let tracker = async move {
            while let Some((request, to)) = sent_rx.recv().await {
                let tid = &request[12..16];
                let action = u32::from_be_bytes(request[8..12].try_into().unwrap());
                let reply = if action == ACTION_CONNECT {
                    let mut b = ACTION_CONNECT.to_be_bytes().to_vec();
                    b.extend_from_slice(tid);
                    b.extend_from_slice(&7u64.to_be_bytes());
                    b
                } else if to == v4_tracker {
                    announce_reply(tid, &[v4_peer])
                } else {
                    announce_reply(tid, &[v6_peer])
                };
                let from = match to {
                    SocketAddr::V4(a) => SocketAddr::new(a.ip().to_ipv6_mapped().into(), a.port()),
                    v6 => v6,
                };
                reply_tx.send((reply, from)).unwrap();
            }
        };
        let tracker = tokio::spawn(tracker);

        let fields = || AnnounceFields {
            info_hash: Id20::new([1; 20]),
            peer_id: Id20::new([2; 20]),
            downloaded: 0,
            left: 0,
            uploaded: 0,
            event: EVENT_NONE,
            key: 0,
            num_want: 200,
            port: 6881,
            url_data: String::new(),
        };
        let r = client.announce(v4_tracker, fields()).await.unwrap();
        assert_eq!(r.addrs, vec![v4_peer]);
        let r = client.announce(v6_tracker, fields()).await.unwrap();
        assert_eq!(r.addrs, vec![v6_peer]);

        token.cancel();
        drop(client);
        tracker.abort();
    }
}
