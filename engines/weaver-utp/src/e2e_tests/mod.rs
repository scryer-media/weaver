// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

// Two sockets joined by an in-memory link, run on tokio's paused clock: a
// connect, an accept and a counter echoed both ways, clean and with loss.

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
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    try_join,
};
use tracing::{Instrument, error_span};

use crate::{
    PollSendToVectored, SocketOpts, Transport, UtpSocket, test_util::setup_test_logging,
    traits::DefaultUtpEnvironment,
};

type Datagram = (SocketAddr, Vec<u8>);

// One end of a two-ended in-memory link. With `drop_every` set, every
// n-th datagram this end sends is lost, past the first few so the
// handshake goes through.
pub(crate) struct LinkEnd {
    addr: SocketAddr,
    tx: UnboundedSender<Datagram>,
    rx: Mutex<UnboundedReceiver<Datagram>>,
    drop_every: u64,
    sent: AtomicU64,
    pub(crate) lost: AtomicU64,
    // Pure ACKs and data packets this end sent.
    pub(crate) acks: AtomicU64,
    pub(crate) data: AtomicU64,
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
        sent: AtomicU64::new(0),
        lost: AtomicU64::new(0),
        acks: AtomicU64::new(0),
        data: AtomicU64::new(0),
    };
    (end(a, b_tx, a_rx), end(b, a_tx, b_rx))
}

fn delayed(one_way: Duration) -> (UnboundedSender<Datagram>, UnboundedReceiver<Datagram>) {
    let (tx, rx) = unbounded_channel();
    if one_way.is_zero() {
        return (tx, rx);
    }
    let (held_tx, mut held_rx) = unbounded_channel::<(Instant, Datagram)>();
    let (in_tx, in_rx) = unbounded_channel::<Datagram>();
    // Stamped as it is sent: this task runs before the paused clock can move.
    tokio::spawn(async move {
        let mut in_rx = in_rx;
        while let Some(datagram) = in_rx.recv().await {
            if held_tx.send((Instant::now() + one_way, datagram)).is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        while let Some((at, datagram)) = held_rx.recv().await {
            tokio::time::sleep_until(at).await;
            if tx.send(datagram).is_err() {
                return;
            }
        }
    });
    (in_tx, rx)
}

impl LinkEnd {
    fn send(&self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.sent.fetch_add(1, Ordering::Relaxed) + 1;
        match buf.first().map(|b| b >> 4) {
            Some(t) if t == crate::raw::Type::ST_STATE as u8 => {
                self.acks.fetch_add(1, Ordering::Relaxed);
            }
            Some(t) if t == crate::raw::Type::ST_DATA as u8 => {
                self.data.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        if self.drop_every > 0 && n > 3 && n.is_multiple_of(self.drop_every) {
            self.lost.fetch_add(1, Ordering::Relaxed);
            return Ok(buf.len());
        }
        self.tx
            .send((self.addr, buf.to_vec()))
            .map_err(|_| std::io::Error::other("link closed"))?;
        Ok(buf.len())
    }
}

impl Transport for LinkEnd {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let (from, data) = poll_fn(|cx| self.rx.lock().poll_recv(cx))
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
        assert!(server.transport.lost.load(Ordering::Relaxed) > 0);
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

#[tokio::test(start_paused = true)]
async fn e2e_echo_ipv4() {
    test_one_echo(ipv4(1), ipv4(2), 0).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_ipv6() {
    test_one_echo(ipv6(1), ipv6(2), 0).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_loss_5_pct() {
    test_one_echo(ipv4(1), ipv4(2), 20).await;
}

#[tokio::test(start_paused = true)]
async fn e2e_echo_loss_20_pct() {
    test_one_echo(ipv4(1), ipv4(2), 5).await;
}

// 256 MiB one way over a clean link of 10 ms each way: the pure ACKs the
// receiver sends and the paused clock's time the transfer takes.
#[tokio::test(start_paused = true)]
#[ignore = "bench: moves 256 MiB"]
async fn ack_bench() {
    const TOTAL: usize = 256 << 20;
    let (server, client) = sockets(
        ipv4(1),
        ipv4(2),
        0,
        Duration::from_millis(10),
        SocketOpts::default(),
    );
    let started = tokio::time::Instant::now();
    let serve = async {
        let (mut r, _w) = server.accept().await.unwrap().split();
        let mut buf = vec![0u8; 1 << 16];
        let mut got = 0;
        while got < TOTAL {
            let n = r.read(&mut buf).await.unwrap();
            assert!(n > 0);
            got += n;
        }
    };
    let dial = async {
        let (_r, mut w) = client.connect(ipv4(1)).await.unwrap().split();
        let chunk = vec![7u8; 1 << 16];
        for _ in 0..TOTAL / chunk.len() {
            w.write_all(&chunk).await.unwrap();
        }
        w.flush().await.unwrap();
    };
    tokio::join!(serve, dial);
    let elapsed = started.elapsed();
    let data = client.transport.data.load(Ordering::Relaxed);
    println!(
        "BENCH utp acks: {} pure ACKs for {data} data packets ({} immediate, {} delayed), {elapsed:?} on the paused clock",
        server.transport.acks.load(Ordering::Relaxed),
        server.stats().immediate_acks.load(Ordering::Relaxed),
        server.stats().delayed_acks.load(Ordering::Relaxed),
    );
}
