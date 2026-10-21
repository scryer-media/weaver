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
    str::{FromStr, from_utf8},
    sync::Arc,
    time::Duration,
};

use crate::{
    core::{Id20, spawn_utils::spawn_with_cancel},
    upnp::get_local_ip_relative_to,
};
use anyhow::Context;
use futures::{Stream, future::BoxFuture};
use librqbit_dualstack_sockets::{BindDevice, MulticastUdpSocket};
use network_interface::NetworkInterface;
use parking_lot::Mutex;
use tokio::{
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug_span, trace};

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
trait Socket: Send + Sync + 'static {
    fn recv_from<'a>(&'a self, buf: &'a mut [u8])
    -> BoxFuture<'a, io::Result<(usize, SocketAddr)>>;

    /// Sends `msg(is_v6)` to the LSD group on every interface.
    fn send_everywhere<'a>(&'a self, msg: &'a (dyn Fn(bool) -> String + Sync))
    -> BoxFuture<'a, ()>;

    /// The host's interfaces, whose networks are the local ones.
    fn nics(&self) -> &[NetworkInterface];
}

impl Socket for MulticastUdpSocket {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(MulticastUdpSocket::recv_from(self, buf))
    }

    fn send_everywhere<'a>(
        &'a self,
        msg: &'a (dyn Fn(bool) -> String + Sync),
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.try_send_mcast_everywhere(&|mopts| Some(msg(mopts.mcast_addr().is_ipv6())))
                .await
        })
    }

    fn nics(&self) -> &[NetworkInterface] {
        MulticastUdpSocket::nics(self)
    }
}

struct Announce {
    tx: UnboundedSender<SocketAddr>,
    port: Option<u16>,
}

/// An announce with sends still to go (libtorrent `lsd::resend_announce`).
struct Resend {
    info_hash: Id20,
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
}

impl Registry {
    fn new() -> Self {
        Self {
            receivers: HashMap::new(),
            first: VecDeque::new(),
            rotation: VecDeque::new(),
            resends: Vec::new(),
            next_at: Instant::now() + ANNOUNCE_INTERVAL,
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

    /// Takes away everything `info_hash` would still announce.
    fn forget(&mut self, info_hash: Id20) {
        self.receivers.remove(&info_hash);
        self.first.retain(|h| *h != info_hash);
        self.rotation.retain(|h| *h != info_hash);
        self.resends.retain(|r| r.info_hash != info_hash);
    }

    fn port(&self, info_hash: Id20) -> Option<u16> {
        self.receivers.get(&info_hash)?.port
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
            due.extend(
                receivers
                    .get(&r.info_hash)
                    .and_then(|a| a.port)
                    .map(|p| (r.info_hash, p)),
            );
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
        let mut delay = (ANNOUNCE_INTERVAL / announcing as u32).max(Duration::from_millis(1));
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
pub struct LocalServiceDiscoveryOptions<'a> {
    pub cancel_token: CancellationToken,
    pub cookie: Option<u32>,
    pub bind_device: Option<&'a BindDevice>,
}

impl LocalServiceDiscovery {
    pub async fn new(opts: LocalServiceDiscoveryOptions<'_>) -> anyhow::Result<Self> {
        let socket = MulticastUdpSocket::new(
            (Ipv6Addr::UNSPECIFIED, LSD_PORT).into(),
            LSD_IPV4,
            LSD_IPV6,
            None,
            opts.bind_device,
        )
        .await
        .context("error binding LSD socket")?;
        Ok(Self::with_socket(
            Box::new(socket),
            opts.cookie.unwrap_or_else(rand::random),
            opts.cancel_token,
        ))
    }

    fn with_socket(socket: Box<dyn Socket>, cookie: u32, cancel_token: CancellationToken) -> Self {
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
            lsd.clone().task_monitor_recv(),
        );
        spawn_with_cancel(
            debug_span!("lsd-announce"),
            "lsd-announce",
            cancel_token,
            lsd.clone().task_announce(),
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
    /// the torrent it announces, and is never answered (libtorrent
    /// `lsd::process_packet`).
    async fn recv_and_process_one(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let mut headers = [httparse::EMPTY_HEADER; 16];

        let (sz, addr) = self.inner.socket.recv_from(buf).await?;
        if get_local_ip_relative_to(addr, self.inner.socket.nics()).is_err() {
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
        if let Some(announce) = self.inner.registry.lock().receivers.get(&bts.hash) {
            let _ = announce.tx.send(addr);
        }
        Ok(())
    }

    async fn task_monitor_recv(self) -> anyhow::Result<()> {
        let mut buf = [0u8; 4096];

        loop {
            self.recv_and_process_one(&mut buf).await?;
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

    /// Streams the peers announcing `info_hash`, and announces it on the
    /// schedule as listening on `announce_port`, if given, for as long as
    /// the stream lives.
    pub fn announce(
        &self,
        info_hash: Id20,
        announce_port: Option<u16>,
    ) -> impl Stream<Item = SocketAddr> + Send + Sync + 'static {
        let (tx, rx) = unbounded_channel::<SocketAddr>();

        struct AddrStream {
            info_hash: Id20,
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
                self.lsd.inner.registry.lock().forget(self.info_hash);
            }
        }

        let moved_earlier = self.inner.registry.lock().register(
            info_hash,
            Announce {
                tx,
                port: announce_port,
            },
            Instant::now(),
        );
        if moved_earlier {
            self.inner.reschedule.notify_one();
        }

        AddrStream {
            info_hash,
            rx,
            lsd: self.clone(),
        }
    }
}

#[derive(Debug)]
struct BtSearchAnnounceMessage {
    hash: Id20,
    our_cookie: Option<u32>,
    #[allow(unused)]
    host: SocketAddr,
    port: u16,
}

fn try_parse_bt_search<'a: 'h, 'h>(
    buf: &'a [u8],
    headers: &'a mut [httparse::Header<'h>],
) -> anyhow::Result<BtSearchAnnounceMessage> {
    let mut req = httparse::Request::new(headers);
    req.parse(buf).context("error parsing request")?;

    match req.method {
        Some("BT-SEARCH") => {
            let mut host = None;
            let mut port = None;
            let mut infohash = None;
            let mut our_cookie = None;

            for header in req.headers.iter() {
                if header.name.eq_ignore_ascii_case("host") {
                    host = Some(
                        from_utf8(header.value)
                            .context("invalid utf-8 in host header")?
                            .parse()
                            .context("invalid IP in host header")?,
                    );
                } else if header.name.eq_ignore_ascii_case("port") {
                    port = Some(atoi::atoi::<u16>(header.value).context("port is not a number")?)
                } else if header.name.eq_ignore_ascii_case("infohash") {
                    infohash = Some(
                        Id20::from_str(from_utf8(header.value).context("infohash isn't utf-8")?)
                            .context("invalid infohash header")?,
                    );
                } else if header.name.eq_ignore_ascii_case("cookie") {
                    our_cookie = atoi::atoi::<u32>(header.value);
                }
            }

            match (host, port, infohash) {
                (Some(host), Some(port), Some(hash)) => Ok(BtSearchAnnounceMessage {
                    hash,
                    our_cookie,
                    host,
                    port,
                }),
                _ => anyhow::bail!("not all of host, man and st are set"),
            }
        }
        _ => anyhow::bail!("expecting BT-SEARCH"),
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

    use super::{LocalServiceDiscovery, Socket, try_parse_bt_search};
    use crate::core::Id20;

    const PORT: u16 = 6881;

    /// A socket that records what the LSD sends and hands it the packets a
    /// test injects.
    struct TestSocket {
        incoming: tokio::sync::Mutex<UnboundedReceiver<(Vec<u8>, SocketAddr)>>,
        sent: UnboundedSender<(Instant, Id20)>,
        nics: Vec<NetworkInterface>,
    }

    impl TestSocket {
        fn record(&self, payload: &str) {
            let mut headers = [httparse::EMPTY_HEADER; 16];
            let bts = try_parse_bt_search(payload.as_bytes(), &mut headers).unwrap();
            assert_eq!(bts.port, PORT);
            let _ = self.sent.send((Instant::now(), bts.hash));
        }
    }

    impl Socket for TestSocket {
        fn recv_from<'a>(
            &'a self,
            buf: &'a mut [u8],
        ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(async move {
                let Some((packet, from)) = self.incoming.lock().await.recv().await else {
                    return std::future::pending().await;
                };
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

        fn nics(&self) -> &[NetworkInterface] {
            &self.nics
        }
    }

    struct Harness {
        lsd: LocalServiceDiscovery,
        sent: UnboundedReceiver<(Instant, Id20)>,
        incoming: UnboundedSender<(Vec<u8>, SocketAddr)>,
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
            Self {
                lsd: LocalServiceDiscovery::with_socket(
                    Box::new(socket),
                    1,
                    CancellationToken::new(),
                ),
                sent,
                incoming,
                start: Instant::now(),
            }
        }

        /// The next send: whose, and how long after the start, in seconds.
        async fn next(&mut self) -> (Id20, u64) {
            let (at, hash) = self.sent.recv().await.unwrap();
            (hash, (at - self.start).as_secs())
        }

        /// A peer at `from` announces `info_hash` on `port`.
        fn receive(&self, from: &str, info_hash: Id20, port: u16) {
            let msg = format!(
                "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: {port}\r\n\
                 Infohash: {}\r\ncookie: 2\r\n\r\n\r\n",
                info_hash.as_string()
            );
            self.incoming
                .send((msg.into_bytes(), from.parse().unwrap()))
                .unwrap();
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
        let _stream = h.lsd.announce(a, Some(PORT));
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
        let stream = h.lsd.announce(a, Some(PORT));
        assert_eq!(h.next().await, (a, 0));
        drop(stream);
        let _b = h.lsd.announce(b, Some(PORT));
        assert_eq!(
            h.until(300).await,
            [(b, 0), (b, 2), (b, 6), (b, 300)],
            "nothing more for the dropped stream",
        );
    }

    // A pause drops the stream and a resume makes a new one: the torrent
    // announces at once, then once an interval, as if new.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_torrent_announces_once_per_interval() {
        let mut h = Harness::new();
        let a = hash(1);
        let stream = h.lsd.announce(a, Some(PORT));
        assert_eq!(h.next().await, (a, 0));
        drop(stream);
        let _resumed = h.lsd.announce(a, Some(PORT));
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
        let _a = h.lsd.announce(a, Some(PORT));
        let _b = h.lsd.announce(b, Some(PORT));
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

    // A stream without a port listens and never announces.
    #[tokio::test(start_paused = true)]
    async fn a_listening_stream_does_not_announce() {
        let mut h = Harness::new();
        let (a, b) = (hash(1), hash(2));
        let _a = h.lsd.announce(a, None);
        let _b = h.lsd.announce(b, Some(PORT));
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
        let mut peers = h.lsd.announce(a, Some(PORT));
        h.receive("10.0.0.7:6771", a, 7001);
        assert_eq!(peers.next().await, Some("10.0.0.7:7001".parse().unwrap()));
        assert_eq!(h.until(2).await, [(a, 0), (a, 2)]);
    }
}
