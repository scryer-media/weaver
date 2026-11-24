// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use anyhow::Context as _;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, DhtSessionConfig, ListenerMode, ListenerOptions, Session,
    SessionOptions,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
        StreamListener,
    },
    storage::StorageFactory,
    tests::{
        e2e_tracker_announces::{param, read_announce_query, respond, tracker_session_opts},
        embedder_util::{MemoryStorageFactory, TestTorrent},
    },
};

/// A loopback datagram socket that says when it is dropped.
struct Watched {
    inner: Arc<dyn DatagramSocket>,
    dropped: Arc<AtomicBool>,
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl DatagramSocket for Watched {
    fn bind_addr(&self) -> SocketAddr {
        self.inner.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        self.inner.recv_from(buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        self.inner.send_to(buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_send_to(cx, buf, target)
    }
}

/// A loopback stream listener that says when it is dropped.
struct WatchedListener {
    inner: tokio::net::TcpListener,
    dropped: Arc<AtomicBool>,
}

impl Drop for WatchedListener {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl StreamListener for WatchedListener {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr().unwrap()
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept().await?;
            let (r, w) = stream.into_split();
            Ok((
                addr,
                Box::new(r) as BoxAsyncReadVectored,
                Box::new(w) as BoxAsyncWrite,
            ))
        })
    }
}

/// Binds every datagram socket as a [`Watched`] one and every stream
/// listener as a [`WatchedListener`], all on loopback, and dials nothing.
#[derive(Default)]
pub(crate) struct WatchedUdp {
    bound: Mutex<Vec<(SocketAddr, Arc<AtomicBool>)>>,
}

impl WatchedUdp {
    /// Whether the socket or listener bound at `addr` is gone.
    pub(crate) fn dropped(&self, addr: SocketAddr) -> bool {
        let bound = self.bound.lock().unwrap();
        let (_, dropped) = bound.iter().find(|(a, _)| *a == addr).unwrap();
        dropped.load(Ordering::SeqCst)
    }
}

impl SocketFactory for WatchedUdp {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move {
            let inner = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
            let dropped = Arc::new(AtomicBool::new(false));
            self.bound
                .lock()
                .unwrap()
                .push((inner.local_addr()?, dropped.clone()));
            Ok(Box::new(WatchedListener { inner, dropped }) as Box<dyn StreamListener>)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let inner = crate::test_support::loopback_udp().await;
            let dropped = Arc::new(AtomicBool::new(false));
            self.bound
                .lock()
                .unwrap()
                .push((inner.bind_addr(), dropped.clone()));
            Ok(Arc::new(Watched { inner, dropped }) as Arc<dyn DatagramSocket>)
        })
    }
}

#[tokio::test]
async fn test_stop_returns_once_the_dht_socket_is_gone() -> anyhow::Result<()> {
    // The DHT's one bootstrap node: a loopback socket nobody answers on.
    let bootstrap = crate::test_support::loopback_udp().await;
    let dir = TempDir::with_prefix("test_session_stop")?;
    let factory = Arc::new(WatchedUdp::default());

    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
                port: None,
                persistence: None,
            }),
            persistence: None,
            disable_local_service_discovery: true,
            socket_factory: factory.clone(),
            ..crate::test_support::session_options()
        },
    )
    .await?;
    let dht_addr = session
        .get_dht()
        .expect("the session runs a DHT")
        .listen_addr();

    // The DHT is up and talking through its socket.
    let mut buf = [0u8; 2048];
    loop {
        let (_, from) = bootstrap.recv_from(&mut buf).await?;
        if from == dht_addr {
            break;
        }
    }
    assert!(!factory.dropped(dht_addr));

    session.stop().await;
    assert!(
        factory.dropped(dht_addr),
        "stop() returned with the DHT socket still held"
    );
    Ok(())
}

async fn utp_session() -> anyhow::Result<(TempDir, Arc<Session>)> {
    let dir = TempDir::new()?;
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                mode: ListenerMode::UtpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ssl_listen_port: Some(0),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await?;
    Ok((dir, session))
}

#[tokio::test]
async fn a_stopped_session_releases_plain_and_ssl_utp_ports_with_stats_handles_alive()
-> anyhow::Result<()> {
    let (_dir, session) = utp_session().await?;
    let addresses = [
        session
            .listen_addr()
            .context("plain uTP listener is missing")?,
        session
            .ssl_listen_addr()
            .context("SSL uTP listener is missing")?,
    ];
    let sockets: Vec<_> = session
        .utp_sockets
        .iter()
        .map(|socket| socket.0.clone())
        .collect();
    assert_eq!(sockets.len(), 2);
    session.stop().await;

    let mut rebound = Vec::new();
    let mut held = Vec::new();
    for address in addresses {
        #[expect(
            clippy::disallowed_methods,
            reason = "exclusive loopback rebind proves release; the kernel factory enables address reuse"
        )]
        let binding = tokio::net::UdpSocket::bind(address).await;
        match binding {
            Ok(socket) => rebound.push(socket),
            Err(error) => held.push((address, error)),
        }
    }
    for (socket, address) in sockets.iter().zip(addresses) {
        let _snapshot = socket.stats().snapshot();
        assert_eq!(socket.bind_addr(), address);
    }
    assert!(
        held.is_empty(),
        "stop returned with uTP ports still held: {held:?}"
    );
    assert_eq!(rebound.len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_session_stop_completes_pending_utp_accept_and_connect() -> anyhow::Result<()> {
    use futures::FutureExt;

    let (_dir, session) = utp_session().await?;
    let socket = session
        .utp_sockets
        .first()
        .context("uTP socket is missing")?
        .0
        .clone();
    let unanswered = crate::test_support::loopback_udp().await;
    let accept = socket.accept();
    let connect = socket.connect(unanswered.bind_addr());
    tokio::pin!(accept, connect);
    assert!(accept.as_mut().now_or_never().is_none());
    assert!(connect.as_mut().now_or_never().is_none());

    session.stop().await;

    let closed = |result| {
        matches!(
            result,
            Some(Err(
                weaver_utp::Error::DispatcherDead | weaver_utp::Error::TaskCancelled
            ))
        )
    };
    assert!(
        closed(accept.as_mut().now_or_never()),
        "accept remained pending after stop"
    );
    assert!(
        closed(connect.as_mut().now_or_never()),
        "connect remained pending after stop"
    );
    Ok(())
}

/// A session with a loopback listener and DHT, both watched, running one
/// torrent that has announced `started` to a loopback tracker, and that
/// tracker.
async fn announced(
    name: &str,
) -> anyhow::Result<(
    TempDir,
    Arc<WatchedUdp>,
    Arc<Session>,
    tokio::net::TcpListener,
)> {
    let bootstrap = crate::test_support::loopback_udp().await;
    let t = TestTorrent::new(name, 2048, 1024).await?;
    let tracker = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let dir = TempDir::with_prefix(name)?;
    let factory = Arc::new(WatchedUdp::default());
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
                port: None,
                persistence: None,
            }),
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            socket_factory: factory.clone(),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    // Only a tracker the run heard take `started` is due `stopped`.
    handle
        .tracker_status()
        .context("a live run with trackers")?
        .subscribe()
        .wait_for(|announces| {
            announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_ok())
        })
        .await?;
    Ok((dir, factory, session, tracker))
}

#[tokio::test]
async fn stop_closes_the_listener_and_dht_before_the_tracker_hears_stopped() -> anyhow::Result<()> {
    let (_dir, factory, session, tracker) = announced("stop_swarm_first").await?;
    let listen = session.listen_addr().expect("the session listens");
    let dht = session
        .get_dht()
        .expect("the session runs a DHT")
        .listen_addr();
    assert!(!factory.dropped(listen) && !factory.dropped(dht));

    let tracker_side = async {
        let (conn, query) = read_announce_query(&tracker).await?;
        assert_eq!(param(&query, "event"), Some("stopped"));
        // Nothing reaches the session on its address by the time its
        // tracker hears it stopped.
        assert!(factory.dropped(listen), "the listener outlived stopped");
        assert!(factory.dropped(dht), "the DHT socket outlived stopped");
        respond(conn, "d8:intervali1800e5:peers0:e").await
    };
    let (_, heard) = tokio::join!(session.stop(), tracker_side);
    heard
}

#[tokio::test]
async fn a_session_that_withholds_stopped_sends_its_tracker_none() -> anyhow::Result<()> {
    let (_dir, factory, session, tracker) = announced("stop_withheld").await?;
    let listen = session.listen_addr().expect("the session listens");

    session.withhold_stopped();
    session.stop().await;
    // Every tracker task has ended and no stopped announce is running.
    session.stopped_announces().wait_idle().await;
    assert!(factory.dropped(listen));
    let tracker = tracker.into_std()?;
    tracker.set_nonblocking(true)?;
    let asked = tracker.accept();
    assert!(
        matches!(&asked, Err(e) if e.kind() == io::ErrorKind::WouldBlock),
        "the tracker was asked again: {asked:?}"
    );
    Ok(())
}
