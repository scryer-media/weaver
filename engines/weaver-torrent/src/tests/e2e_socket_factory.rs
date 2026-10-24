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
            let (r, w) = tokio::net::TcpStream::connect(addr).await?.into_split();
            Ok((
                Box::new(r) as BoxAsyncReadVectored,
                Box::new(w) as BoxAsyncWrite,
            ))
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
            let sock = tokio::net::UdpSocket::bind(addr).await?;
            Ok(Arc::new(sock) as Arc<dyn DatagramSocket>)
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
        socket_factory: Some(factory.clone()),
        ..Default::default()
    }
}

/// Seeds a torrent from a session whose sockets all come from `server_factory`,
/// then downloads it into a second session using `client_factory`.
async fn transfer_through_factories(
    name: &str,
    server_listen: ListenerOptions,
    client_listen: Option<ListenerOptions>,
    client_connect: Option<ConnectionOptions>,
    server_factory: &Arc<CountingFactory>,
    client_factory: &Arc<CountingFactory>,
) -> anyhow::Result<()> {
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
    Ok(())
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

#[tokio::test]
async fn test_stream_connector_dials_through_factory() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let factory = Arc::new(CountingFactory::default());

    let connector = StreamConnector::new(StreamConnectorArgs {
        enable_tcp: true,
        socket_factory: Some(factory.clone()),
        ..Default::default()
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
