// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

// Two sockets joined by an in-memory link, run on tokio's paused clock: a
// connect, an accept and a counter echoed both ways, clean and with loss.

mod bottleneck;

use std::{
    future::poll_fn,
    io::IoSlice,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use tokio::time::Instant;

use anyhow::Context as _;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    try_join,
};
use tracing::{Instrument, error_span};

use crate::{
    PollSendToVectored, SocketOpts, Transport, UtpSocket, test_util::setup_test_logging,
    traits::DefaultUtpEnvironment,
};

type Datagram = (Instant, SocketAddr, Vec<u8>);

struct ForcedTimeout {
    dropped_seq_nr: Option<crate::seq_nr::SeqNr>,
    retransmitted: oneshot::Sender<()>,
}

// One end of a two-ended in-memory link. With `drop_every` set, every
// n-th datagram this end sends is lost, past the first few so the
// handshake goes through.
pub(crate) struct LinkEnd {
    addr: SocketAddr,
    tx: UnboundedSender<Datagram>,
    rx: Mutex<UnboundedReceiver<Datagram>>,
    drop_every: u64,
    one_way: Duration,
    sent: AtomicU64,
    pub(crate) lost: AtomicU64,
    // Pure ACKs and data packets this end sent.
    pub(crate) acks: AtomicU64,
    pub(crate) data: AtomicU64,
    forced_timeout: Mutex<Option<ForcedTimeout>>,
}

// With `one_way` set, each direction holds every datagram that long on the
// paused clock, in order.
pub(crate) fn link(
    a: SocketAddr,
    b: SocketAddr,
    drop_every: u64,
    one_way: Duration,
) -> (LinkEnd, LinkEnd) {
    let (a_tx, a_rx) = delayed(one_way);
    let (b_tx, b_rx) = delayed(one_way);
    let end = |addr, tx, rx| LinkEnd {
        addr,
        tx,
        rx: Mutex::new(rx),
        drop_every,
        one_way,
        sent: AtomicU64::new(0),
        lost: AtomicU64::new(0),
        acks: AtomicU64::new(0),
        data: AtomicU64::new(0),
        forced_timeout: Mutex::new(None),
    };
    (end(a, b_tx, a_rx), end(b, a_tx, b_rx))
}

fn delayed(one_way: Duration) -> (UnboundedSender<Datagram>, UnboundedReceiver<Datagram>) {
    let (tx, rx) = unbounded_channel();
    if one_way.is_zero() {
        return (tx, rx);
    }
    let (held_tx, mut held_rx) = unbounded_channel::<Datagram>();
    tokio::spawn(async move {
        while let Some((at, from, data)) = held_rx.recv().await {
            tokio::time::sleep_until(at).await;
            if tx.send((at, from, data)).is_err() {
                return;
            }
        }
    });
    (held_tx, rx)
}

impl LinkEnd {
    // Lose the next data segment until its retransmission, then reopen the link.
    fn force_timeout(&self) -> oneshot::Receiver<()> {
        let (retransmitted, observed) = oneshot::channel();
        let previous = self.forced_timeout.lock().replace(ForcedTimeout {
            dropped_seq_nr: None,
            retransmitted,
        });
        assert!(previous.is_none());
        observed
    }

    fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.sent.fetch_add(1, Ordering::Relaxed) + 1;
        match buf.first().map(|b| b >> 4) {
            Some(t) if t == crate::raw::Type::State as u8 => {
                self.acks.fetch_add(1, Ordering::Relaxed);
            }
            Some(t) if t == crate::raw::Type::Data as u8 => {
                self.data.fetch_add(1, Ordering::Relaxed);
                let mut forced = self.forced_timeout.lock();
                if let Some(timeout) = forced.as_mut() {
                    let (header, _) = crate::raw::UtpHeader::deserialize(buf).unwrap();
                    match timeout.dropped_seq_nr {
                        None => {
                            timeout.dropped_seq_nr = Some(header.seq_nr);
                            self.lost.fetch_add(1, Ordering::Relaxed);
                            return Ok(buf.len());
                        }
                        Some(seq_nr) => {
                            assert_eq!(header.seq_nr, seq_nr);
                            forced.take().unwrap().retransmitted.send(()).unwrap();
                        }
                    }
                }
            }
            _ => {}
        }
        if self.drop_every > 0 && n > 3 && n.is_multiple_of(self.drop_every) {
            self.lost.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        self.tx
            .send((Instant::now() + self.one_way, self.addr, buf.to_vec()))
            .map_err(|_| std::io::Error::other("link closed"))?;
        Ok(buf.len())
    }
}

impl Transport for LinkEnd {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let (_at, from, data) = poll_fn(|cx| self.rx.lock().poll_recv(cx))
            .await
            .ok_or_else(|| std::io::Error::other("link closed"))?;
        buf[..data.len()].copy_from_slice(&data);
        Ok((data.len(), from))
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], _target: SocketAddr) -> std::io::Result<usize> {
        self.send(buf)
    }

    fn poll_send_to(
        &self,
        _cx: &mut Context<'_>,
        buf: &[u8],
        _target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(self.send(buf))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.addr
    }

    fn dont_fragment(&self) -> bool {
        true
    }
}

impl PollSendToVectored for LinkEnd {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        _target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let buf: Vec<u8> = bufs.iter().flat_map(|b| b.iter().copied()).collect();
        Poll::Ready(self.send(&buf))
    }
}

pub(crate) type LinkSocket = Arc<UtpSocket<LinkEnd, DefaultUtpEnvironment>>;

fn link_transport(socket: &LinkSocket) -> Arc<LinkEnd> {
    socket.state.runtime.upgrade().unwrap().transport().unwrap()
}

pub(crate) fn sockets(
    a: SocketAddr,
    b: SocketAddr,
    drop_every: u64,
    one_way: Duration,
    opts: SocketOpts,
) -> (LinkSocket, LinkSocket) {
    let (end_a, end_b) = link(a, b, drop_every, one_way);
    let socket =
        |end| UtpSocket::new_with_opts(end, DefaultUtpEnvironment {}, opts.clone()).unwrap();
    (socket(end_a), socket(end_b))
}

async fn echo(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    const MAX_COUNTER: u64 = 10_000;

    let reader = async move {
        for expected in 0..=MAX_COUNTER {
            let current = reader.read_u64().await.context("error reading")?;
            if current != expected {
                anyhow::bail!("expected {expected}, got {current}");
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    let writer = async move {
        for counter in 0..=MAX_COUNTER {
            writer.write_u64(counter).await.context("error writing")?;
        }
        Ok::<_, anyhow::Error>(())
    };

    try_join!(reader, writer)?;
    Ok(())
}

async fn test_one_echo(server_addr: SocketAddr, client_addr: SocketAddr, drop_every: u64) {
    setup_test_logging();

    let opts = SocketOpts::default();
    let (server, client) = sockets(server_addr, client_addr, drop_every, Duration::ZERO, opts);

    let serve = async {
        let (r, w) = server.accept().await.unwrap().split();
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?server_addr));

    let dial = async {
        let (r, w) = client.connect(server_addr).await.unwrap().split();
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?client_addr));

    tokio::join!(serve, dial);

    assert_eq!(server.stats().accepts.load(Ordering::Relaxed), 1);
    assert_eq!(
        client.stats().connection_successes.load(Ordering::Relaxed),
        1
    );
    if drop_every > 0 {
        assert!(link_transport(&server).lost.load(Ordering::Relaxed) > 0);
        assert!(
            server.stats().data_retransmissions.load(Ordering::Relaxed)
                + client.stats().data_retransmissions.load(Ordering::Relaxed)
                > 0
        );
    }
}

fn ipv4(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

fn ipv6(port: u16) -> SocketAddr {
    (Ipv6Addr::LOCALHOST, port).into()
}

// No cap on live streams beyond the owner's connection limit: one socket
// holds 200 at once.
#[tokio::test(start_paused = true)]
async fn one_socket_holds_more_than_128_streams() {
    setup_test_logging();
    const STREAMS: usize = 200;
    let (server, client) = sockets(ipv4(1), ipv4(2), 0, Duration::ZERO, SocketOpts::default());
    let accept = async {
        let mut held = Vec::with_capacity(STREAMS);
        for _ in 0..STREAMS {
            held.push(server.accept().await.unwrap());
        }
        held
    };
    let dial = async {
        let mut held = Vec::with_capacity(STREAMS);
        for _ in 0..STREAMS {
            held.push(client.connect(ipv4(1)).await.unwrap());
        }
        held
    };
    let (accepted, dialled) = tokio::join!(accept, dial);
    assert_eq!(accepted.len(), STREAMS);
    assert_eq!(dialled.len(), STREAMS);
    assert_eq!(
        client.stats().connection_successes.load(Ordering::Relaxed),
        STREAMS as u64
    );
}

#[tokio::test(start_paused = true)]
async fn delivery_time_is_stamped_before_the_forwarder_runs() {
    let one_way = Duration::from_millis(10);
    let (sender, receiver) = link(ipv4(1), ipv4(2), 0, one_way);
    let sent_at = Instant::now();
    sender.send(b"packet").unwrap();
    tokio::time::advance(Duration::from_millis(50)).await;

    let (delivery_at, from, payload) = poll_fn(|cx| receiver.rx.lock().poll_recv(cx))
        .await
        .unwrap();
    assert_eq!(delivery_at, sent_at + one_way);
    assert_eq!(from, sender.addr);
    assert_eq!(payload, b"packet");
}

async fn delayed_transfer(drop_every: u64) -> crate::UtpStatsSnapshot {
    const TOTAL: usize = 2 << 20;
    let token = tokio_util::sync::CancellationToken::new();
    let (server, client) = sockets(
        ipv4(1),
        ipv4(2),
        drop_every,
        Duration::from_millis(10),
        SocketOpts {
            cancellation_token: token.clone(),
            ..Default::default()
        },
    );
    let serve = async {
        let (mut reader, writer) = server.accept().await.unwrap().split();
        let mut buf = vec![0u8; 1 << 16];
        let mut received = 0;
        while received < TOTAL {
            let n = reader.read(&mut buf).await.unwrap();
            assert!(n > 0);
            assert!(buf[..n].iter().all(|byte| *byte == 7));
            received += n;
        }
        assert_eq!(received, TOTAL);
        (reader, writer)
    };
    let dial = async {
        let (reader, mut writer) = client.connect(ipv4(1)).await.unwrap().split();
        assert_eq!(
            client
                .stats()
                .max_congestion_window_bytes
                .load(Ordering::Relaxed),
            1500
        );
        let chunk = vec![7u8; 1 << 16];
        for _ in 0..TOTAL / chunk.len() {
            writer.write_all(&chunk).await.unwrap();
        }
        writer.flush().await.unwrap();
        (reader, writer)
    };
    let (_server_halves, _client_halves) = tokio::join!(serve, dial);
    let stats = client.stats().snapshot();
    token.cancel();
    stats
}

#[tokio::test(start_paused = true)]
async fn the_sender_window_grows_on_a_delayed_link() {
    assert!(delayed_transfer(0).await.max_congestion_window_bytes > 1500);
}

#[tokio::test(start_paused = true)]
async fn a_lossy_transfer_reports_packet_loss() {
    let stats = delayed_transfer(20).await;
    assert!(stats.packets_lost > 0);
    assert!(stats.delay_samples_below_target > 0);
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_ipv4() {
    test_one_echo(ipv4(1), ipv4(2), 0).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_ipv6() {
    test_one_echo(ipv6(1), ipv6(2), 0).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_ipv6_to_ipv4_mapped_peer() {
    let mapped_peer = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 1));
    test_one_echo(mapped_peer, ipv6(2), 0).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_loss_5_pct() {
    test_one_echo(ipv4(1), ipv4(2), 20).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_loss_20_pct() {
    test_one_echo(ipv4(1), ipv4(2), 5).await;
}

#[derive(Clone, Copy)]
enum AckBenchScenario {
    Warm,
    ColdStart,
    AfterTimeout,
}

impl AckBenchScenario {
    fn label(self) -> &'static str {
        match self {
            Self::Warm => "warm",
            Self::ColdStart => "cold_start",
            Self::AfterTimeout => "after_timeout",
        }
    }

    fn total(self) -> usize {
        match self {
            Self::Warm => 256 << 20,
            Self::ColdStart | Self::AfterTimeout => 256 << 10,
        }
    }
}

struct AckBenchCounters {
    acks: u64,
    data: u64,
    immediate: u64,
    delayed: u64,
}

impl AckBenchCounters {
    fn read(server: &LinkSocket, client: &LinkSocket) -> Self {
        Self {
            acks: link_transport(server).acks.load(Ordering::Relaxed),
            data: link_transport(client).data.load(Ordering::Relaxed),
            immediate: server.stats().immediate_acks.load(Ordering::Relaxed),
            delayed: server.stats().delayed_acks.load(Ordering::Relaxed),
        }
    }
}

async fn write_bench_payload(writer: &mut (impl AsyncWrite + Unpin), mut remaining: usize) {
    let chunk = vec![7u8; 1 << 16];
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        writer.write_all(&chunk[..n]).await.unwrap();
        remaining -= n;
    }
}

async fn measure_ack_bench(scenario: AckBenchScenario, ack_per_drain: bool) {
    let token = tokio_util::sync::CancellationToken::new();
    let (server, client) = sockets(
        ipv4(1),
        ipv4(2),
        0,
        Duration::from_millis(10),
        SocketOpts {
            benchmark_delayed_acks: !ack_per_drain,
            cancellation_token: token.clone(),
            ..Default::default()
        },
    );
    let total = scenario.total();
    let initial_started = Instant::now();
    let initial_counters = AckBenchCounters::read(&server, &client);
    let serve = async {
        let (mut reader, writer) = server.accept().await.unwrap().split();
        let mut buf = vec![0u8; 1 << 16];
        let mut got = 0;
        while got < total {
            let n = reader.read(&mut buf).await.unwrap();
            assert!(n > 0);
            assert!(buf[..n].iter().all(|byte| *byte == 7));
            got += n;
        }
        assert_eq!(got, total);
        (reader, writer)
    };
    let dial = async {
        let (reader, mut writer) = client.connect(ipv4(1)).await.unwrap().split();
        let (started, counters, remaining) = if matches!(scenario, AckBenchScenario::AfterTimeout) {
            let prefix = total / 2;
            write_bench_payload(&mut writer, prefix).await;
            writer.flush().await.unwrap();
            assert_eq!(client.stats().rto_timeouts_count.load(Ordering::Relaxed), 0);

            // No other segment is outstanding or queued: only the timer can
            // retransmit this byte. Start measuring while its ACK is in flight.
            let retransmitted = link_transport(&client).force_timeout();
            writer.write_all(&[7]).await.unwrap();
            retransmitted.await.unwrap();
            assert_eq!(client.stats().rto_timeouts_count.load(Ordering::Relaxed), 1);
            assert_eq!(client.stats().snapshot().timeouts, 1);
            assert_eq!(link_transport(&client).lost.load(Ordering::Relaxed), 1);
            (
                Instant::now(),
                AckBenchCounters::read(&server, &client),
                total - prefix - 1,
            )
        } else {
            (initial_started, initial_counters, total)
        };
        write_bench_payload(&mut writer, remaining).await;
        writer.flush().await.unwrap();
        ((reader, writer), started, counters)
    };
    let (_server_halves, (_client_halves, started, before)) = tokio::join!(serve, dial);
    let duration = Instant::now().duration_since(started);
    let after = AckBenchCounters::read(&server, &client);
    let timeouts = client.stats().rto_timeouts_count.load(Ordering::Relaxed);
    assert_eq!(
        timeouts,
        u64::from(matches!(scenario, AckBenchScenario::AfterTimeout))
    );
    let mode = if ack_per_drain { "drain" } else { "current" };
    println!(
        "BENCH utp {} {mode}: {} pure ACKs for {} data packets ({} immediate, {} delayed), {duration:?} on the paused clock, {timeouts} RTOs",
        scenario.label(),
        after.acks - before.acks,
        after.data - before.data,
        after.immediate - before.immediate,
        after.delayed - before.delayed,
    );
    token.cancel();
}

async fn compare_ack_bench(scenario: AckBenchScenario) {
    measure_ack_bench(scenario, false).await;
    measure_ack_bench(scenario, true).await;
}

#[tokio::test(start_paused = true)]
#[ignore = "bench: moves 256 MiB for each ACK policy"]
async fn ack_bench() {
    compare_ack_bench(AckBenchScenario::Warm).await;
}

#[tokio::test(start_paused = true)]
#[ignore = "bench: moves 256 KiB for each ACK policy"]
async fn ack_bench_cold_start() {
    compare_ack_bench(AckBenchScenario::ColdStart).await;
}

#[tokio::test(start_paused = true)]
#[ignore = "bench: forces one data timeout for each ACK policy"]
async fn ack_bench_after_timeout() {
    compare_ack_bench(AckBenchScenario::AfterTimeout).await;
}
