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
    net::{DatagramSocket, Resolver},
    spawn_utils::spawn_with_cancel,
};
use anyhow::{Context, bail};
use librqbit_dualstack_sockets::{BindDevice, UdpSocket};
use parking_lot::RwLock;
use rand::RngExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, trace, warn};

use crate::tracker::tracker_comms_http::TrackerRequestEvent;

pub(crate) const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
// const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

pub const EVENT_NONE: u32 = 0;
pub const EVENT_COMPLETED: u32 = 1;
pub const EVENT_STARTED: u32 = 2;
pub const EVENT_STOPPED: u32 = 3;

/// The `event` field of a UDP announce (BEP 15).
pub(crate) fn udp_event(event: Option<TrackerRequestEvent>) -> u32 {
    match event {
        None => EVENT_NONE,
        Some(TrackerRequestEvent::Completed) => EVENT_COMPLETED,
        Some(TrackerRequestEvent::Started) => EVENT_STARTED,
        Some(TrackerRequestEvent::Stopped) => EVENT_STOPPED,
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
}

#[derive(Debug)]
pub enum Request {
    Connect,
    Announce(ConnectionId, AnnounceFields),
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

#[derive(Debug)]
pub enum Response {
    Connect(ConnectionId),
    Announce(AnnounceResponse),
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
    transactions: HashMap<TransactionId, tokio::sync::oneshot::Sender<Response>>,
}

/// The socket the UDP tracker client talks through.
///
/// The kernel socket is called directly; a custom transport goes through the
/// [`DatagramSocket`] trait object.
enum UdpTrackerSocket {
    Udp(UdpSocket),
    Custom(Arc<dyn DatagramSocket>),
}

impl UdpTrackerSocket {
    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        match self {
            Self::Udp(sock) => sock.recv_from(buf).await,
            Self::Custom(sock) => sock.recv_from(buf).await,
        }
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        match self {
            Self::Udp(sock) => sock.send_to(buf, target).await,
            Self::Custom(sock) => sock.send_to(buf, target).await,
        }
    }
}

struct ClientShared {
    sock: UdpTrackerSocket,
    resolver: Option<Arc<dyn Resolver>>,
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
    pub async fn new(
        cancel_token: CancellationToken,
        bind_device: Option<&BindDevice>,
    ) -> anyhow::Result<Self> {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let sock = UdpSocket::bind_udp(
            addr,
            librqbit_dualstack_sockets::BindOpts {
                device: bind_device,
                ..Default::default()
            },
        )
        .with_context(|| format!("error creating UDP socket at {addr}"))?;

        Ok(Self::spawn(cancel_token, UdpTrackerSocket::Udp(sock), None))
    }

    /// Create a client that sends and receives tracker traffic through `socket`
    /// instead of binding a kernel UDP socket.
    ///
    /// When `resolver` is set, UDP tracker hostnames are looked up through it
    /// instead of the host's resolver.
    pub fn new_with_socket(
        cancel_token: CancellationToken,
        socket: Arc<dyn DatagramSocket>,
        resolver: Option<Arc<dyn Resolver>>,
    ) -> Self {
        Self::spawn(cancel_token, UdpTrackerSocket::Custom(socket), resolver)
    }

    fn spawn(
        cancel_token: CancellationToken,
        sock: UdpTrackerSocket,
        resolver: Option<Arc<dyn Resolver>>,
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

    /// The resolver for tracker hostnames, if one was injected.
    pub(crate) fn resolver(&self) -> Option<&dyn Resolver> {
        self.state.resolver.as_deref()
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

            let (tid, response) = match Response::parse(&buf[..len], addr.is_ipv6()) {
                Ok(r) => r,
                Err(e) => {
                    debug!(?addr, "error parsing UDP response: {e:#}");
                    continue;
                }
            };

            trace!(?tid, ?response, ?addr, "received");

            let t = self.state.locked.write().transactions.remove(&tid);
            match t {
                Some(tx) => match tx.send(response) {
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

        let response = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .context("timeout connecting")?
            .context("sender dead")?;
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
        tx: tokio::sync::oneshot::Sender<Response>,
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
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        net::{Ipv4Addr, SocketAddr},
        str::FromStr,
        sync::Arc,
    };

    use crate::core::{hash_id::Id20, net::DatagramSocket, peer_id::generate_peer_id};
    use tokio_util::sync::CancellationToken;

    use crate::tracker::tracker_comms_udp::{
        ACTION_CONNECT, AnnounceFields, CONNECTION_ID_MAGIC, EVENT_NONE, Request, Response,
        UdpTrackerClient, new_transaction_id,
    };

    #[tokio::test]
    async fn test_connect_through_custom_socket() {
        const CONNECTION_ID: u64 = 0x1122_3344_5566_7788;

        let tracker = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tracker_addr = tracker.local_addr().unwrap();

        let client_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();
        let client_sock: Arc<dyn DatagramSocket> = Arc::new(client_sock);

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

    #[ignore]
    #[tokio::test]
    async fn test_announce() {
        let sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
        sock.connect("opentor.net:6969").await.unwrap();

        let tid = new_transaction_id();
        let mut write_buf = [0u8; 16384];
        let mut read_buf = vec![0u8; 4096];

        let len = Request::Connect.serialize(tid, &mut write_buf).unwrap();

        sock.send(&write_buf[..len]).await.unwrap();

        let size = sock.recv(&mut read_buf).await.unwrap();

        let (rtid, response) = Response::parse(&read_buf[..size], false).unwrap();
        assert_eq!(tid, rtid);
        let connection_id = match response {
            Response::Connect(connection_id) => {
                dbg!(connection_id)
            }
            other => panic!("unexpected response {:?}", other),
        };

        let hash = Id20::from_str("775459190aa65566591634203f8d9f17d341f969").unwrap();

        let tid = new_transaction_id();
        let request = Request::Announce(
            connection_id,
            AnnounceFields {
                info_hash: hash,
                peer_id: generate_peer_id(b"-xx1234-"),
                downloaded: 0,
                left: 0,
                uploaded: 0,
                event: EVENT_NONE,
                key: 0, // whatever that is?
                num_want: 50,
                port: 24563,
            },
        );
        let size = request.serialize(tid, &mut write_buf).unwrap();

        sock.send(&write_buf[..size]).await.unwrap();
        let size = sock.recv(&mut read_buf).await.unwrap();

        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open("/tmp/proto.bin")
                .unwrap();
            f.write_all(&read_buf[..size]).unwrap();
        }

        dbg!(&read_buf[..size]);
        let (rtid, response) = Response::parse(&read_buf[..size], false).unwrap();
        assert_eq!(tid, rtid);
        match response {
            Response::Announce(r) => {
                dbg!(r);
            }
            other => panic!("unexpected response {:?}", other),
        }
    }
}
