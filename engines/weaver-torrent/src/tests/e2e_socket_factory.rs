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
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, CreateTorrentOptions, ListenerMode,
    ListenerOptions, Session, SessionOptions, create_torrent,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
        StreamListener,
    },
    spawn_utils::BlockingSpawner,
    stream_connect::{ConnectionKind, StreamConnector, StreamConnectorArgs, Transports},
    tests::test_util::{
        TestPeerMetadata, create_default_random_dir_with_torrents, setup_test_logging,
    },
};

/// A `SocketFactory` over plain tokio sockets that counts every call.
#[derive(Default)]
pub(super) struct CountingFactory {
    connect_tcp: AtomicUsize,
    listen_tcp: AtomicUsize,
    accept_tcp: Arc<AtomicUsize>,
    bind_udp: AtomicUsize,
    // Hands out datagram sockets that, as a tunnel's, keep their uTP
    // streams beside their I/O.
    tunnel: bool,
}

/// A datagram socket that leaves `spreads_streams` at its default, as a
/// tunnel's socket does.
struct AsTunnel(Arc<dyn DatagramSocket>);

impl DatagramSocket for AsTunnel {
    fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        self.0.recv_from(buf)
    }
    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        self.0.send_to(buf, target)
    }
    fn poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        self.0.poll_send_to(cx, buf, target)
    }
    fn poll_send_to_vectored(
        &self,
        cx: &mut std::task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
        target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        self.0.poll_send_to_vectored(cx, bufs, target)
    }
    fn register_io(&self) -> io::Result<()> {
        self.0.register_io()
    }
    fn link_mtu(&self) -> Option<u16> {
        self.0.link_mtu()
    }
    fn shutdown(&self) -> BoxFuture<'_, io::Result<()>> {
        self.0.shutdown()
    }
}

impl CountingFactory {
    fn count(c: &AtomicUsize) -> usize {
        c.load(Ordering::SeqCst)
    }
}

struct CountingListener {
    inner: tokio::net::TcpListener,
    accepts: Arc<AtomicUsize>,
}

impl StreamListener for CountingListener {
    fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr().unwrap()
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept().await?;
            self.accepts.fetch_add(1, Ordering::SeqCst);
            let (r, w) = stream.into_split();
            Ok((
                addr,
                Box::new(r) as BoxAsyncReadVectored,
                Box::new(w) as BoxAsyncWrite,
            ))
        })
    }
}

impl SocketFactory for CountingFactory {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            self.connect_tcp.fetch_add(1, Ordering::SeqCst);
            crate::test_support::tcp(addr).await
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move {
            self.listen_tcp.fetch_add(1, Ordering::SeqCst);
            let inner = tokio::net::TcpListener::bind(addr).await?;
            Ok(Box::new(CountingListener {
                inner,
                accepts: self.accept_tcp.clone(),
            }) as Box<dyn StreamListener>)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            self.bind_udp.fetch_add(1, Ordering::SeqCst);
            let socket = crate::test_support::udp_at(addr).await?;
            Ok(if self.tunnel {
                Arc::new(AsTunnel(socket))
            } else {
                socket
            })
        })
    }
}

fn session_opts(
    factory: &Arc<CountingFactory>,
    listen: Option<ListenerOptions>,
    connect: Option<ConnectionOptions>,
) -> SessionOptions {
    SessionOptions {
        dht: None,
        persistence: None,
        identity: Some(crate::Identity {
            peer_id: TestPeerMetadata::good().as_peer_id(),
            ..Default::default()
        }),
        disable_local_service_discovery: true,
        listen,
        connect,
        socket_factory: factory.clone(),
        ..crate::test_support::session_options()
    }
}

/// Seeds a torrent from a session whose sockets all come from `server_factory`,
/// then downloads it into a second session using `client_factory`. Returns
/// the connections the two sessions ran on their uTP threads, as
/// [`Session::on_utp_threads`] counts them.
async fn transfer_through_factories(
    name: &str,
    server_listen: ListenerOptions,
    client_listen: Option<ListenerOptions>,
    client_connect: Option<ConnectionOptions>,
    server_factory: &Arc<CountingFactory>,
    client_factory: &Arc<CountingFactory>,
) -> anyhow::Result<[usize; 3]> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, 8192, Some(name));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let orig_content = std::fs::read(files.path().join("0.data"))?;

    let server_session = Session::new_with_opts(
        files.path().into(),
        session_opts(server_factory, Some(server_listen), None),
    )
    .await
    .context("error creating server session")?;

    server_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                paused: false,
                output_folder: Some(files.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .unwrap()
        .wait_until_completed()
        .await
        .context("error seeding torrent")?;

    let peer = server_session
        .listen_addr()
        .context("expected listen_addr to be set")?;
    assert_ne!(peer.port(), 0, "listen_addr must report the factory's bind");

    let client_dir = TempDir::with_prefix(name)?;
    let client_session = Session::new_with_opts(
        client_dir.path().into(),
        session_opts(client_factory, client_listen, client_connect),
    )
    .await
    .context("error creating client session")?;

    let client_handle = client_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                paused: false,
                initial_peers: Some(vec![peer]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .unwrap();
    client_handle.wait_until_completed().await?;

    let downloaded = std::fs::read(client_dir.path().join("0.data"))?;
    assert!(downloaded == orig_content, "contents differ");
    let [server, client] = [&server_session, &client_session].map(|s| s.on_utp_threads());
    Ok(std::array::from_fn(|i| server[i] + client[i]))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_socket_factory_tcp() -> anyhow::Result<()> {
    let server = Arc::new(CountingFactory::default());
    let client = Arc::new(CountingFactory::default());
    timeout(
        Duration::from_secs(10),
        transfer_through_factories(
            "test_e2e_socket_factory_tcp",
            ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            },
            None,
            None,
            &server,
            &client,
        ),
    )
    .await??;

    assert_eq!(CountingFactory::count(&server.listen_tcp), 1);
    assert!(CountingFactory::count(&server.accept_tcp) >= 1);
    assert!(CountingFactory::count(&client.connect_tcp) >= 1);
    // The UDP tracker client binds one datagram socket per session, even with
    // TCP-only peer connections.
    assert_eq!(CountingFactory::count(&server.bind_udp), 1);
    assert_eq!(CountingFactory::count(&client.bind_udp), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_socket_factory_utp() -> anyhow::Result<()> {
    let server = Arc::new(CountingFactory::default());
    let client = Arc::new(CountingFactory::default());
    let utp_only = || ListenerOptions {
        mode: ListenerMode::UtpOnly,
        listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
        ..Default::default()
    };
    timeout(
        Duration::from_secs(10),
        transfer_through_factories(
            "test_e2e_socket_factory_utp",
            utp_only(),
            Some(utp_only()),
            Some(ConnectionOptions {
                enable_tcp: false,
                ..Default::default()
            }),
            &server,
            &client,
        ),
    )
    .await??;

    // One datagram socket for uTP plus one for the UDP tracker client.
    assert_eq!(CountingFactory::count(&server.bind_udp), 2);
    assert_eq!(CountingFactory::count(&client.bind_udp), 2);
    assert_eq!(CountingFactory::count(&server.listen_tcp), 0);
    assert_eq!(CountingFactory::count(&client.connect_tcp), 0);
    Ok(())
}

// A peer over a socket that does not spread its streams, as a tunnel's,
// runs where its stream does: on the session's first uTP thread, the one
// that reads and sends on the socket. On a host with one core the session
// has that thread alone, and the test shows only that the peers ran on it.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_over_a_tunnel_socket_runs_on_the_thread_that_reads_and_sends() -> anyhow::Result<()>
{
    let tunnel = || {
        Arc::new(CountingFactory {
            tunnel: true,
            ..Default::default()
        })
    };
    let (server, client) = (tunnel(), tunnel());
    let utp_only = || ListenerOptions {
        mode: ListenerMode::UtpOnly,
        listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
        ..Default::default()
    };
    let [incoming, outgoing, on_io] = transfer_through_factories(
        "a_peer_over_a_tunnel_socket",
        utp_only(),
        Some(utp_only()),
        Some(ConnectionOptions {
            enable_tcp: false,
            ..Default::default()
        }),
        &server,
        &client,
    )
    .await?;
    assert!(incoming > 0, "no incoming connection ran on a uTP thread");
    assert!(outgoing > 0, "no outgoing connection ran on a uTP thread");
    assert_eq!(
        on_io,
        incoming + outgoing,
        "a connection ran off the thread that reads and sends"
    );
    Ok(())
}

#[tokio::test]
async fn test_stream_connector_dials_through_factory() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let factory = Arc::new(CountingFactory::default());

    let connector = StreamConnector::new(StreamConnectorArgs {
        enable_tcp: true,
        ..StreamConnectorArgs::over(factory.clone())
    })
    .await?;

    let (accepted, connected) =
        tokio::join!(listener.accept(), connector.connect(addr, Transports::Any));
    let (kind, _r, _w) = connected?;
    let (_stream, peer_addr) = accepted?;

    assert!(matches!(kind, ConnectionKind::Tcp));
    assert_eq!(peer_addr.ip(), Ipv4Addr::LOCALHOST);
    assert_eq!(CountingFactory::count(&factory.connect_tcp), 1);
    Ok(())
}

// A route whose TCP room is always crowded out by others: every look leaves
// a want, counted while it is held. Everything else goes to the kernel
// sockets of a `CountingFactory`.
struct CrowdedTcp {
    inner: CountingFactory,
    wants: Arc<HeldWants>,
}

#[derive(Default)]
struct HeldWants {
    made: AtomicUsize,
    held: AtomicUsize,
    gone: tokio::sync::Notify,
}

struct HeldWant(Arc<HeldWants>);

impl Drop for HeldWant {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::SeqCst);
        self.0.gone.notify_one();
    }
}

impl SocketFactory for CrowdedTcp {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        self.inner.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.inner.listen_tcp(addr)
    }

    fn tcp_room(&self) -> crate::network::TcpRoom {
        self.wants.made.fetch_add(1, Ordering::SeqCst);
        self.wants.held.fetch_add(1, Ordering::SeqCst);
        crate::network::TcpRoom::Crowded(crate::network::TcpWant::new(HeldWant(Arc::clone(
            &self.wants,
        ))))
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.inner.bind_udp(addr)
    }
}

// A torrent in a thin swarm dials its last candidate. Its look finds TCP
// crowded out, so it wants TCP room and the dial goes over uTP, where the
// peer comes live. With that dial ended and no one left to dial, no TCP dial
// is possible: the connect loop looks again and lets the want go. Neither
// side has the payload, so the run never completes, and nothing but the dial
// ending can wake the loop. Real loopback uTP runs on real time; the test
// waits on the want's release, then on the peer coming live.
#[tokio::test(flavor = "multi_thread")]
async fn the_want_goes_once_the_last_dial_comes_live_over_utp() -> anyhow::Result<()> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, 8192, Some("last_dial_utp"));
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let utp_only = || ListenerOptions {
        mode: ListenerMode::UtpOnly,
        listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
        ..Default::default()
    };
    let add = |dir: &TempDir, initial_peers| {
        Some(AddTorrentOptions {
            paused: false,
            output_folder: Some(dir.path().to_str().unwrap().to_owned()),
            initial_peers,
            ..Default::default()
        })
    };

    let server_dir = TempDir::with_prefix("last_dial_utp_server")?;
    let server = Arc::new(CountingFactory::default());
    let server_session = Session::new_with_opts(
        server_dir.path().into(),
        session_opts(&server, Some(utp_only()), None),
    )
    .await?;
    let _served = server_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            add(&server_dir, None),
        )
        .await?;
    let peer = server_session.listen_addr().context("a uTP listener")?;

    let wants = Arc::new(HeldWants::default());
    let client = Arc::new(CrowdedTcp {
        inner: CountingFactory::default(),
        wants: Arc::clone(&wants),
    });
    let client_dir = TempDir::with_prefix("last_dial_utp_client")?;
    let client_session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            identity: Some(crate::Identity {
                peer_id: TestPeerMetadata::good().as_peer_id(),
                ..Default::default()
            }),
            disable_local_service_discovery: true,
            listen: Some(utp_only()),
            connect: None,
            socket_factory: client.clone(),
            ..crate::test_support::session_options()
        },
    )
    .await?;
    let handle = client_session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            add(&client_dir, Some(vec![peer])),
        )
        .await?
        .into_handle()
        .unwrap();

    // The look before the dial left a want; it goes once the loop finds
    // that no TCP dial is possible.
    loop {
        let gone = wants.gone.notified();
        if wants.made.load(Ordering::SeqCst) > 0 && wants.held.load(Ordering::SeqCst) == 0 {
            break;
        }
        gone.await;
    }
    // The dial ends before the peer's handshake makes it live.
    let live = handle.live().context("the torrent runs")?;
    let peers = live.peer_states();
    loop {
        let went_live = peers.live_changed.notified();
        tokio::pin!(went_live);
        went_live.as_mut().enable();
        if peers.stats().live_utp > 0 {
            break;
        }
        went_live.await;
    }
    assert_eq!(peers.stats().live_utp, 1, "the dial came live over uTP");
    assert_eq!(peers.dialling(), 0);
    assert_eq!(CountingFactory::count(&client.inner.connect_tcp), 0);
    Ok(())
}
