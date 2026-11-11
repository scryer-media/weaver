// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    str::from_utf8,
    sync::Arc,
    time::Duration,
};

use crate::core::{Id20, spawn_utils::spawn_with_cancel};
use anyhow::Context;
use futures::{Stream, future::BoxFuture};
use parking_lot::Mutex;
use tokio::{
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, debug_span, trace};
use weaver_tunnel::egress::{MulticastSocket, SocketEgress};

const LSD_PORT: u16 = 6771;
const LSD_IPV4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(239, 192, 152, 143), LSD_PORT);
const LSD_IPV6: SocketAddrV6 = SocketAddrV6::new(
    Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f),
    LSD_PORT,
    0,
    0,
);

/// Every announcing torrent announces once in this long (libtorrent
/// `settings_pack::local_service_announce_interval`, 5 minutes).
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// While torrents wait for their first announce, announces go at least this
/// often (libtorrent `session_impl::update_lsd_announce_interval`).
const FIRST_ANNOUNCE_SPACING: Duration = Duration::from_secs(4);

/// Each announce goes out this many times, the n-th resend `n` times this
/// after the send before it (libtorrent `lsd::announce_impl`: at once, then
/// 2 s and 4 s later).
const SENDS_PER_ANNOUNCE: u32 = 3;
const RESEND_STEP: Duration = Duration::from_secs(2);

/// What the LSD sends on and receives from: the multicast socket, or a
/// test's.
pub(crate) trait Socket: Send + Sync + 'static {
    /// The next datagram. An error of kind [`io::ErrorKind::NotConnected`]
    /// means the socket is closed and nothing more arrives; any other is
    /// about one datagram, and the socket goes on receiving.
    fn recv_from<'a>(&'a self, buf: &'a mut [u8])
    -> BoxFuture<'a, io::Result<(usize, SocketAddr)>>;

    /// Sends `msg(is_v6)` to the LSD group on every interface.
    fn send_everywhere<'a>(&'a self, msg: &'a (dyn Fn(bool) -> String + Sync))
    -> BoxFuture<'a, ()>;

    /// Whether `source` is this host or on the network of one of its
    /// interfaces.
    fn is_local(&self, source: SocketAddr) -> bool;
}

impl Socket for MulticastSocket {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(MulticastSocket::recv_from(self, buf))
    }

    fn send_everywhere<'a>(
        &'a self,
        msg: &'a (dyn Fn(bool) -> String + Sync),
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.send_everywhere(|group| msg(group.is_ipv6()).into_bytes()))
    }

    fn is_local(&self, source: SocketAddr) -> bool {
        MulticastSocket::is_local(self, source)
    }
}

/// The port an announce says the session listens on, read as each
/// announce goes out, so a port a mapper gets on the gateway after a
/// torrent starts is the one its later announces give (libtorrent
/// `torrent::lsd_announce` asks `session_impl::listen_port` each time).
pub type AnnouncedPort = Arc<dyn Fn() -> Option<u16> + Send + Sync>;

struct Announce {
    tx: UnboundedSender<SocketAddr>,
    port: Option<AnnouncedPort>,
    /// Which stream registered it; see [`Registry::unregister`].
    stream: u64,
}

/// An announce with sends still to go (libtorrent `lsd::resend_announce`).
struct Resend {
    info_hash: Id20,
    /// The port the first send gave: a resend repeats that message.
    port: u16,
    sent: u32,
    at: Instant,
}

/// Every hash whose peer source listens, and the session's one announce
/// schedule over those that announce (libtorrent `session_impl`'s
/// `m_lsd_torrents`, its round robin over `m_torrents` and
/// `m_lsd_announce_timer`). A hash is announced, first or in turn, only
/// while it is registered here, which is while its stream lives.
struct Registry {
    receivers: HashMap<Id20, Announce>,
    /// Announcing hashes yet to make their first announce, oldest first
    /// (libtorrent `m_lsd_torrents`, filled by `prioritize_lsd`).
    first: VecDeque<Id20>,
    /// Announcing hashes past their first announce, next first (libtorrent
    /// `on_lsd_announce`'s round robin, `m_next_lsd_torrent`).
    rotation: VecDeque<Id20>,
    resends: Vec<Resend>,
    /// When the next hash takes its turn.
    next_at: Instant,
    /// The number the next registering stream goes by.
    next_stream: u64,
}

impl Registry {
    fn new() -> Self {
        Self {
            receivers: HashMap::new(),
            first: VecDeque::new(),
            rotation: VecDeque::new(),
            resends: Vec::new(),
            next_at: Instant::now() + ANNOUNCE_INTERVAL,
            next_stream: 0,
        }
    }

    /// Registers `info_hash`. One that announces goes in line for its first
    /// announce, at once when none waits before it (libtorrent
    /// `session_impl::prioritize_lsd`, which `torrent::start_announcing`
    /// calls). Returns whether the schedule moved earlier.
    fn register(&mut self, info_hash: Id20, announce: Announce, now: Instant) -> bool {
        self.forget(info_hash);
        let announces = announce.port.is_some();
        self.receivers.insert(info_hash, announce);
        if !announces {
            return false;
        }
        self.first.push_back(info_hash);
        if self.first.len() == 1 {
            self.next_at = now;
            return true;
        }
        false
    }

    /// The number a registering stream goes by.
    fn stream(&mut self) -> u64 {
        self.next_stream += 1;
        self.next_stream
    }

    /// `stream` ends: what it registered for `info_hash` goes, but not a
    /// later stream's registration for the hash, which replaced it.
    fn unregister(&mut self, info_hash: Id20, stream: u64) {
        if self
            .receivers
            .get(&info_hash)
            .is_some_and(|announce| announce.stream == stream)
        {
            self.forget(info_hash);
        }
    }

    /// Takes away everything `info_hash` would still announce.
    fn forget(&mut self, info_hash: Id20) {
        self.receivers.remove(&info_hash);
        self.first.retain(|h| *h != info_hash);
        self.rotation.retain(|h| *h != info_hash);
        self.resends.retain(|r| r.info_hash != info_hash);
    }

    fn port(&self, info_hash: Id20) -> Option<u16> {
        self.receivers.get(&info_hash)?.port.as_ref()?()
    }

    fn wake_at(&self) -> Instant {
        self.resends
            .iter()
            .map(|r| r.at)
            .fold(self.next_at, Instant::min)
    }

    /// The sends due at `now`: the resends that are, then the hash whose
    /// turn it is. A turn goes to the oldest hash waiting for its first
    /// announce, else to the next in rotation, and the next turn comes an
    /// interval over the number of announcing hashes later, so that each
    /// announces once an interval, or sooner while hashes wait for their
    /// first (libtorrent `session_impl::on_lsd_announce`,
    /// `update_lsd_announce_interval`).
    fn due(&mut self, now: Instant) -> Vec<(Id20, u16)> {
        let mut due = Vec::new();
        let receivers = &self.receivers;
        self.resends.retain_mut(|r| {
            if r.at > now {
                return true;
            }
            if receivers.contains_key(&r.info_hash) {
                due.push((r.info_hash, r.port));
            }
            r.sent += 1;
            r.at = now + RESEND_STEP * r.sent;
            r.sent < SENDS_PER_ANNOUNCE
        });

        if self.next_at > now {
            return due;
        }
        let turn = self.first.pop_front().or_else(|| self.rotation.pop_front());
        self.rotation.extend(turn);
        let announcing = (self.first.len() + self.rotation.len()).max(1);
        let mut delay = (ANNOUNCE_INTERVAL / u32::try_from(announcing).unwrap_or(u32::MAX))
            .max(Duration::from_millis(1));
        if !self.first.is_empty() {
            delay = delay.min(FIRST_ANNOUNCE_SPACING);
        }
        self.next_at = now + delay;

        if let Some(info_hash) = turn
            && let Some(port) = self.port(info_hash)
        {
            due.push((info_hash, port));
            self.resends.push(Resend {
                info_hash,
                port,
                sent: 1,
                at: now + RESEND_STEP,
            });
        }
        due
    }
}

struct LocalServiceDiscoveryInner {
    socket: Box<dyn Socket>,
    cookie: u32,
    registry: Mutex<Registry>,
    /// Wakes the schedule when it moves earlier.
    reschedule: Notify,
}

#[derive(Clone)]
pub struct LocalServiceDiscovery {
    inner: Arc<LocalServiceDiscoveryInner>,
}

#[derive(Default)]
pub struct LocalServiceDiscoveryOptions {
    pub cancel_token: CancellationToken,
    pub cookie: Option<u32>,
    /// The interface the LSD joins its groups on, or every one.
    pub egress: SocketEgress,
    /// Track the LSD's tasks in this tracker. They own the socket's use, so
    /// once the tracker is empty after cancellation, nothing is sent or
    /// received on it again.
    pub tasks: Option<TaskTracker>,
    /// A test's socket in place of the multicast one.
    #[cfg(test)]
    pub(crate) socket: Option<Box<dyn Socket>>,
}

impl LocalServiceDiscovery {
    pub async fn new(opts: LocalServiceDiscoveryOptions) -> anyhow::Result<Self> {
        let tasks = opts.tasks.unwrap_or_default();
        #[cfg(test)]
        if let Some(socket) = opts.socket {
            return Ok(Self::with_socket(
                socket,
                opts.cookie.unwrap_or_else(rand::random),
                opts.cancel_token,
                tasks,
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "the LSD's groups are on the local network alone, never a route"
        )]
        let socket = opts
            .egress
            .join_multicast(LSD_IPV4, LSD_IPV6)
            .context("error binding LSD socket")?;
        Ok(Self::with_socket(
            Box::new(socket),
            opts.cookie.unwrap_or_else(rand::random),
            opts.cancel_token,
            tasks,
        ))
    }

    fn with_socket(
        socket: Box<dyn Socket>,
        cookie: u32,
        cancel_token: CancellationToken,
        tasks: TaskTracker,
    ) -> Self {
        let lsd = Self {
            inner: Arc::new(LocalServiceDiscoveryInner {
                socket,
                cookie,
                registry: Mutex::new(Registry::new()),
                reschedule: Notify::new(),
            }),
        };

        spawn_with_cancel(
            debug_span!("lsd"),
            "lsd",
            cancel_token.clone(),
            tasks.track_future(lsd.clone().task_monitor_recv()),
        );
        spawn_with_cancel(
            debug_span!("lsd-announce"),
            "lsd-announce",
            cancel_token,
            tasks.track_future(lsd.clone().task_announce()),
        );

        lsd
    }

    fn gen_announce_msg(&self, info_hash: Id20, port: u16, is_v6: bool) -> String {
        let host: SocketAddr = if is_v6 {
            LSD_IPV6.into()
        } else {
            LSD_IPV4.into()
        };
        let cookie = self.inner.cookie;
        let info_hash = info_hash.as_string();
        format!(
            "BT-SEARCH * HTTP/1.1\r
Host: {host}\r
Port: {port}\r
Infohash: {info_hash}\r
cookie: {cookie}\r
\r
\r
"
        )
    }

    /// Takes one packet: an announce from the local network names a peer of
    /// each torrent it announces, and is never answered (libtorrent
    /// `lsd::on_announce`, lsd.cpp:215-330, v2.0.15). Fails only once the
    /// socket is closed.
    async fn recv_and_process_one(&self, buf: &mut [u8]) -> io::Result<()> {
        let mut headers = [httparse::EMPTY_HEADER; 16];

        let (sz, addr) = match self.inner.socket.recv_from(buf).await {
            Ok(received) => received,
            Err(error) if error.kind() == io::ErrorKind::NotConnected => return Err(error),
            Err(error) => {
                debug!(%error, "LSD receive error");
                return Ok(());
            }
        };
        if !self.inner.socket.is_local(addr) {
            trace!(?addr, "ignoring a packet from outside the local network");
            return Ok(());
        }
        let buf = bstr::BStr::new(&buf[..sz]);

        let bts = match try_parse_bt_search(buf, &mut headers) {
            Ok(bts) => bts,
            Err(e) => {
                trace!(?buf, ?addr, "error parsing message: {e:#}");
                return Ok(());
            }
        };

        trace!(?addr, ?bts, "received");

        if bts.our_cookie == Some(self.inner.cookie) {
            trace!(?bts, "ignoring our own message");
            return Ok(());
        }

        let mut addr = addr;
        addr.set_port(bts.port);
        let registry = self.inner.registry.lock();
        for hash in bts.hashes {
            if let Some(announce) = registry.receivers.get(&hash) {
                let _ = announce.tx.send(addr);
            }
        }
        Ok(())
    }

    /// Receives until the socket is closed. A receive error ends nothing
    /// else: the error is about one datagram.
    async fn task_monitor_recv(self) -> anyhow::Result<()> {
        let mut buf = [0u8; 4096];

        loop {
            self.recv_and_process_one(&mut buf)
                .await
                .context("the LSD socket is closed")?;
        }
    }

    /// Sends what the schedule says is due, whenever it is due.
    async fn task_announce(self) -> anyhow::Result<()> {
        loop {
            let wake_at = self.inner.registry.lock().wake_at();
            tokio::select! {
                biased;
                _ = self.inner.reschedule.notified() => {}
                _ = tokio::time::sleep_until(wake_at) => {}
            }
            let due = self.inner.registry.lock().due(Instant::now());
            for (info_hash, port) in due {
                trace!(?info_hash, port, "announcing");
                self.inner
                    .socket
                    .send_everywhere(&|is_v6| self.gen_announce_msg(info_hash, port, is_v6))
                    .await;
            }
        }
    }

    /// Whether a stream for `info_hash` lives.
    #[cfg(test)]
    pub(crate) fn registered(&self, info_hash: Id20) -> bool {
        self.inner
            .registry
            .lock()
            .receivers
            .contains_key(&info_hash)
    }

    /// Streams the peers announcing `info_hash`, and announces it on the
    /// schedule as listening on the port `announce_port` gives at each
    /// announce, if given, for as long as the stream lives.
    pub fn announce(
        &self,
        info_hash: Id20,
        announce_port: Option<AnnouncedPort>,
    ) -> impl Stream<Item = SocketAddr> + Send + Sync + 'static + use<> {
        let (tx, rx) = unbounded_channel::<SocketAddr>();

        struct AddrStream {
            info_hash: Id20,
            stream: u64,
            rx: UnboundedReceiver<SocketAddr>,
            lsd: LocalServiceDiscovery,
        }

        impl Stream for AddrStream {
            type Item = SocketAddr;

            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                self.rx.poll_recv(cx)
            }
        }

        impl Drop for AddrStream {
            fn drop(&mut self) {
                self.lsd
                    .inner
                    .registry
                    .lock()
                    .unregister(self.info_hash, self.stream);
            }
        }

        let (stream, moved_earlier) = {
            let mut registry = self.inner.registry.lock();
            let stream = registry.stream();
            let moved_earlier = registry.register(
                info_hash,
                Announce {
                    tx,
                    port: announce_port,
                    stream,
                },
                Instant::now(),
            );
            (stream, moved_earlier)
        };
        if moved_earlier {
            self.inner.reschedule.notify_one();
        }

        AddrStream {
            info_hash,
            stream,
            rx,
            lsd: self.clone(),
        }
    }
}

/// A socket that receives nothing and hands on each announce it sends.
#[cfg(test)]
pub(crate) struct RecordingSocket(pub(crate) UnboundedSender<String>);

#[cfg(test)]
impl Socket for RecordingSocket {
    fn recv_from<'a>(&'a self, _: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(std::future::pending())
    }

    fn send_everywhere<'a>(
        &'a self,
        msg: &'a (dyn Fn(bool) -> String + Sync),
    ) -> BoxFuture<'a, ()> {
        let _ = self.0.send(msg(false));
        Box::pin(async {})
    }

    fn is_local(&self, _: SocketAddr) -> bool {
        false
    }
}

/// A [`RecordingSocket`] that counts in `listening` the receives waiting on
/// it: one while the LSD listens, none once it has stopped.
#[cfg(test)]
pub(crate) struct ListeningSocket {
    pub(crate) sent: UnboundedSender<String>,
    pub(crate) listening: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl Socket for ListeningSocket {
    fn recv_from<'a>(&'a self, _: &'a mut [u8]) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        use std::sync::atomic::Ordering;

        struct Waiting(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Waiting {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        self.listening.fetch_add(1, Ordering::SeqCst);
        let waiting = Waiting(self.listening.clone());
        Box::pin(async move {
            let _waiting = waiting;
            std::future::pending().await
        })
    }

    fn send_everywhere<'a>(
        &'a self,
        msg: &'a (dyn Fn(bool) -> String + Sync),
    ) -> BoxFuture<'a, ()> {
        let _ = self.sent.send(msg(false));
        Box::pin(async {})
    }

    fn is_local(&self, _: SocketAddr) -> bool {
        false
    }
}

#[derive(Debug)]
struct BtSearchAnnounceMessage {
    /// Every torrent the packet announces, in its order.
    hashes: Vec<Id20>,
    our_cookie: Option<u32>,
    port: u16,
}

/// Reads a BT-SEARCH as libtorrent does (lsd.cpp:248-330, v2.0.15): it
/// needs a port in 1..=65534; every `Infohash` header names a torrent, one
/// that is not 40 hex digits or is all zeros is passed over; the `Host`
/// header is not read.
fn try_parse_bt_search<'a: 'h, 'h>(
    buf: &'a [u8],
    headers: &'a mut [httparse::Header<'h>],
) -> anyhow::Result<BtSearchAnnounceMessage> {
    let mut req = httparse::Request::new(headers);
    req.parse(buf).context("error parsing request")?;

    if !req
        .method
        .is_some_and(|method| method.eq_ignore_ascii_case("BT-SEARCH"))
    {
        anyhow::bail!("expecting BT-SEARCH");
    }
    let mut port = None;
    let mut hashes = Vec::new();
    let mut our_cookie = None;

    for header in req.headers.iter() {
        if header.name.eq_ignore_ascii_case("port") {
            port = Some(atoi::atoi::<u16>(header.value).context("port is not a number")?)
        } else if header.name.eq_ignore_ascii_case("infohash") {
            let hash = (header.value.len() == 40)
                .then(|| from_utf8(header.value).ok()?.parse::<Id20>().ok())
                .flatten();
            match hash {
                Some(hash) if hash != Id20::default() => hashes.push(hash),
                Some(_) => {}
                None => trace!(value = ?bstr::BStr::new(header.value), "invalid infohash header"),
            }
        } else if header.name.eq_ignore_ascii_case("cookie") {
            our_cookie = atoi::atoi::<u32>(header.value);
        }
    }

    match port {
        Some(port) if port > 0 && port < u16::MAX => Ok(BtSearchAnnounceMessage {
            hashes,
            our_cookie,
            port,
        }),
        Some(port) => anyhow::bail!("invalid port {port}"),
        None => anyhow::bail!("no port"),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use futures::{StreamExt, future::BoxFuture};
    use network_interface::NetworkInterface;
    use tokio::{
        sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        time::Instant,
    };
    use tokio_util::sync::CancellationToken;

    use super::{AnnouncedPort, LocalServiceDiscovery, Socket, try_parse_bt_search};
    use crate::core::Id20;

    const PORT: u16 = 6881;

    fn fixed(port: u16) -> AnnouncedPort {
        std::sync::Arc::new(move || Some(port))
    }

    /// A packet, or a receive error of this kind.
    type Incoming = Result<(Vec<u8>, SocketAddr), std::io::ErrorKind>;

    /// A socket that records what the LSD sends and hands it the packets and
    /// errors a test injects.
    struct TestSocket {
        incoming: tokio::sync::Mutex<UnboundedReceiver<Incoming>>,
        sent: UnboundedSender<(Instant, Id20, u16)>,
        nics: Vec<NetworkInterface>,
    }

    impl TestSocket {
        fn record(&self, payload: &str) {
            let mut headers = [httparse::EMPTY_HEADER; 16];
            let bts = try_parse_bt_search(payload.as_bytes(), &mut headers).unwrap();
            let _ = self.sent.send((Instant::now(), bts.hashes[0], bts.port));
        }
    }

    impl Socket for TestSocket {
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(async move {
                let Some(incoming) = self.incoming.lock().await.recv().await else {
                    return std::future::pending().await;
                };
                let (packet, from) = incoming.map_err(std::io::Error::from)?;
                buf[..packet.len()].copy_from_slice(&packet);
                Ok((packet.len(), from))
            })
        }

        fn send_everywhere<'a>(
            &'a self,
            msg: &'a (dyn Fn(bool) -> String + Sync),
        ) -> BoxFuture<'a, ()> {
            self.record(&msg(false));
            Box::pin(async {})
        }

        fn is_local(&self, source: SocketAddr) -> bool {
            crate::upnp::get_local_ip_relative_to(source, &self.nics).is_ok()
        }
    }

    struct Harness {
        lsd: LocalServiceDiscovery,
        sent: UnboundedReceiver<(Instant, Id20, u16)>,
        incoming: UnboundedSender<Incoming>,
        tasks: tokio_util::task::TaskTracker,
        start: Instant,
    }

    impl Harness {
        fn new() -> Self {
            let (incoming, incoming_rx) = unbounded_channel();
            let (sent_tx, sent) = unbounded_channel();
            // The host is on 10.0.0.0/24 alone.
            let socket = TestSocket {
                incoming: tokio::sync::Mutex::new(incoming_rx),
                sent: sent_tx,
                nics: vec![NetworkInterface::new_afinet(
                    "test0",
                    Ipv4Addr::new(10, 0, 0, 1),
                    Some(Ipv4Addr::new(255, 255, 255, 0)),
                    None,
                    1,
                    false,
                )],
            };
            let tasks = tokio_util::task::TaskTracker::new();
            Self {
                lsd: LocalServiceDiscovery::with_socket(
                    Box::new(socket),
                    1,
                    CancellationToken::new(),
                    tasks.clone(),
                ),
                sent,
                incoming,
                tasks,
                start: Instant::now(),
            }
        }

        /// The next send, on [`PORT`]: whose, and how long after the start,
        /// in seconds.
        async fn next(&mut self) -> (Id20, u64) {
            let (hash, secs, port) = self.next_with_port().await;
            assert_eq!(port, PORT);
            (hash, secs)
        }

        /// The next send: whose, how long after the start in seconds, and
        /// the port it gave.
        async fn next_with_port(&mut self) -> (Id20, u64, u16) {
            let (at, hash, port) = self.sent.recv().await.unwrap();
            (hash, (at - self.start).as_secs(), port)
        }

        /// A peer at `from` announces `info_hash` on `port`.
        fn receive(&self, from: &str, info_hash: Id20, port: u16) {
            self.receive_headers(
                from,
                &format!("Port: {port}\r\nInfohash: {}\r\n", info_hash.as_string()),
            );
        }

        /// A peer at `from` sends a BT-SEARCH with these header lines.
        fn receive_headers(&self, from: &str, lines: &str) {
            let msg = format!(
                "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\n{lines}cookie: 2\r\n\r\n\r\n"
            );
            self.incoming
                .send(Ok((msg.into_bytes(), from.parse().unwrap())))
                .unwrap();
        }

        /// The next receive fails with `kind`.
        fn fail(&self, kind: std::io::ErrorKind) {
            self.incoming.send(Err(kind)).unwrap();
        }

        /// The sends up to and including the first at or after `secs`.
        async fn until(&mut self, secs: u64) -> Vec<(Id20, u64)> {
            let mut sends = Vec::new();
            loop {
                let send = self.next().await;
                sends.push(send);
                if send.1 >= secs {
                    return sends;
                }
            }
        }
    }

    fn hash(b: u8) -> Id20 {
        Id20::new([b; 20])
    }

    // libtorrent `lsd::announce_impl`: three sends, 2 s then 4 s apart; the
    // next announce an interval later.
    #[tokio::test(start_paused = true)]
    async fn each_announce_goes_out_three_times() {
        let mut h = Harness::new();
        let a = hash(1);
        let _stream = h.lsd.announce(a, Some(fixed(PORT)));
        assert_eq!(
            h.until(300).await,
            [(a, 0), (a, 2), (a, 6), (a, 300)],
            "sends at 0, 2 and 6 s, the next announce at 300 s",
        );
    }

    // The announce belongs to the stream: once dropped, not even the
    // pending resends go out. `b`, registered after the drop, shows a
    // full interval passing.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_stream_stops_its_announces() {
        let mut h = Harness::new();
        let (a, b) = (hash(1), hash(2));
        let stream = h.lsd.announce(a, Some(fixed(PORT)));
        assert_eq!(h.next().await, (a, 0));
        drop(stream);
        let _b = h.lsd.announce(b, Some(fixed(PORT)));
        assert_eq!(
            h.until(300).await,
            [(b, 0), (b, 2), (b, 6), (b, 300)],
            "nothing more for the dropped stream",
        );
    }

    // A run's stream that ends after the next run registered the hash, as
    // a cancelled peer task's may, takes nothing of the next run's away.
    #[tokio::test(start_paused = true)]
    async fn a_late_drop_of_an_old_stream_keeps_the_new_registration() {
        let h = Harness::new();
        let a = hash(1);
        let old = h.lsd.announce(a, Some(fixed(PORT)));
        let new = h.lsd.announce(a, Some(fixed(PORT)));
        drop(old);
        assert!(h.lsd.registered(a));
        drop(new);
        assert!(!h.lsd.registered(a));
    }

    // A pause drops the stream and a resume makes a new one: the torrent
    // announces at once, then once an interval, as if new.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_torrent_announces_once_per_interval() {
        let mut h = Harness::new();
        let a = hash(1);
        let stream = h.lsd.announce(a, Some(fixed(PORT)));
        assert_eq!(h.next().await, (a, 0));
        drop(stream);
        let _resumed = h.lsd.announce(a, Some(fixed(PORT)));
        assert_eq!(
            h.until(600).await,
            [
                (a, 0),
                (a, 2),
                (a, 6),
                (a, 300),
                (a, 302),
                (a, 306),
                (a, 600)
            ],
        );
    }

    // libtorrent `session_impl::on_lsd_announce`: one torrent a turn, the
    // first announces at most 4 s apart, then turns an interval over the
    // number of torrents apart.
    #[tokio::test(start_paused = true)]
    async fn turns_spread_over_the_interval() {
        let mut h = Harness::new();
        let (a, b) = (hash(1), hash(2));
        let _a = h.lsd.announce(a, Some(fixed(PORT)));
        let _b = h.lsd.announce(b, Some(fixed(PORT)));
        assert_eq!(
            h.until(304).await,
            [
                (a, 0),
                (a, 2),
                (b, 4),
                (a, 6),
                (b, 6),
                (b, 10),
                (a, 154),
                (a, 156),
                (a, 160),
                (b, 304)
            ],
        );
    }

    // libtorrent `torrent::lsd_announce` reads the session's port at each
    // announce: a port that changes after the torrent starts goes out with
    // the next announce, and the resends repeat the message they resend.
    #[tokio::test(start_paused = true)]
    async fn each_announce_reads_the_port_as_it_goes_out() {
        let mut h = Harness::new();
        let a = hash(1);
        let port = std::sync::Arc::new(std::sync::atomic::AtomicU16::new(PORT));
        let read = port.clone();
        let _stream = h.lsd.announce(
            a,
            Some(std::sync::Arc::new(move || {
                Some(read.load(std::sync::atomic::Ordering::SeqCst))
            })),
        );
        assert_eq!(h.next_with_port().await, (a, 0, PORT));
        port.store(50000, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(h.next_with_port().await, (a, 2, PORT));
        assert_eq!(h.next_with_port().await, (a, 6, PORT));
        assert_eq!(h.next_with_port().await, (a, 300, 50000));
    }

    // A stream without a port listens and never announces.
    #[tokio::test(start_paused = true)]
    async fn a_listening_stream_does_not_announce() {
        let mut h = Harness::new();
        let (a, b) = (hash(1), hash(2));
        let _a = h.lsd.announce(a, None);
        let _b = h.lsd.announce(b, Some(fixed(PORT)));
        assert_eq!(h.until(300).await, [(b, 0), (b, 2), (b, 6), (b, 300)]);
    }

    // libtorrent `lsd::process_packet`: a source outside the netmask of the
    // interface is dropped. The peer on the host's network comes through
    // first, so the one outside it was dropped.
    #[tokio::test(start_paused = true)]
    async fn a_packet_from_outside_the_local_network_is_ignored() {
        let h = Harness::new();
        let a = hash(1);
        let mut peers = h.lsd.announce(a, None);
        h.receive("192.168.1.5:6771", a, 7000);
        h.receive("10.0.0.7:6771", a, 7001);
        assert_eq!(peers.next().await, Some("10.0.0.7:7001".parse().unwrap()));
    }

    // libtorrent `lsd::on_announce` answers nothing: the scheduled sends
    // are the only traffic. An answer would go out at 0 s, before the
    // resend at 2 s.
    #[tokio::test(start_paused = true)]
    async fn a_received_announce_is_not_answered() {
        let mut h = Harness::new();
        let a = hash(1);
        let mut peers = h.lsd.announce(a, Some(fixed(PORT)));
        h.receive("10.0.0.7:6771", a, 7001);
        assert_eq!(peers.next().await, Some("10.0.0.7:7001".parse().unwrap()));
        assert_eq!(h.until(2).await, [(a, 0), (a, 2)]);
    }

    // A receive error is about one datagram: the LSD goes on receiving.
    // Only a closed socket ends the receive task.
    #[tokio::test(start_paused = true)]
    async fn a_receive_error_ends_nothing_but_a_closed_socket_ends_the_receives() {
        let h = Harness::new();
        let a = hash(1);
        let mut peers = h.lsd.announce(a, None);
        h.fail(std::io::ErrorKind::ConnectionReset);
        h.fail(std::io::ErrorKind::Other);
        h.receive("10.0.0.7:6771", a, 7001);
        assert_eq!(peers.next().await, Some("10.0.0.7:7001".parse().unwrap()));

        h.fail(std::io::ErrorKind::NotConnected);
        h.receive("10.0.0.8:6771", a, 7002);
        // Of the two tasks the announcer alone is left.
        h.tasks.close();
        let announcer_only = async {
            while h.tasks.len() > 1 {
                tokio::task::yield_now().await;
            }
        };
        announcer_only.await;
        assert!(h.lsd.registered(a));
    }

    // libtorrent `lsd::on_announce` (lsd.cpp:303-330): every Infohash header
    // names a peer of that torrent; one that is malformed or all zeros is
    // passed over, and a packet without a usable port names nobody.
    #[tokio::test(start_paused = true)]
    async fn every_infohash_in_a_packet_is_read() {
        let h = Harness::new();
        let (a, b, zero) = (hash(1), hash(2), Id20::default());
        let mut peers_a = h.lsd.announce(a, None);
        let mut peers_b = h.lsd.announce(b, None);
        let _zero = h.lsd.announce(zero, None);
        h.receive_headers("10.0.0.9:6771", &format!("Port: 0\r\nInfohash: {}\r\n", a.as_string()));
        h.receive_headers(
            "10.0.0.7:6771",
            &format!(
                "Port: 7001\r\nInfohash: {}\r\nInfohash: nothex\r\nInfohash: {}\r\nInfohash: {}\r\n",
                a.as_string(),
                zero.as_string(),
                b.as_string()
            ),
        );
        let peer: SocketAddr = "10.0.0.7:7001".parse().unwrap();
        assert_eq!(peers_a.next().await, Some(peer));
        assert_eq!(peers_b.next().await, Some(peer));

        let mut headers = [httparse::EMPTY_HEADER; 16];
        let packet = format!(
            "BT-SEARCH * HTTP/1.1\r\nPort: 7001\r\nInfohash: {}\r\nInfohash: {}\r\n\r\n",
            zero.as_string(),
            b.as_string()
        );
        let bts = try_parse_bt_search(packet.as_bytes(), &mut headers).unwrap();
        assert_eq!(bts.hashes, [b], "no Host header needed, the zero hash passed over");
        let packet = "BT-SEARCH * HTTP/1.1\r\nPort: 65535\r\n\r\n";
        assert!(try_parse_bt_search(packet.as_bytes(), &mut [httparse::EMPTY_HEADER; 16]).is_err());
    }
}
