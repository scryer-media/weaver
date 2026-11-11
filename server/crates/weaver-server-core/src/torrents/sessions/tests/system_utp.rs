// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use anyhow::Context;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, SocketFactory, StreamListener,
    create_torrent,
    network::{BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, KernelSockets},
    spawn_utils::BlockingSpawner,
};

use super::{FakeRoutes, env, two_free_ports};
use crate::{
    persistence::Database,
    proxies::TorrentScope,
    settings::{TorrentConfig, TorrentPeerProtocol},
    torrents::{
        identity::IDENTITY,
        net::LegKind,
        sessions::{Routes, Sessions},
    },
};

// The host's own sockets, with the port of every datagram socket bound and
// every TCP dial counted.
#[derive(Default)]
struct Noted {
    datagram_ports: Mutex<Vec<u16>>,
    tcp_dials: Mutex<usize>,
}

struct NotingKernel {
    kernel: KernelSockets,
    noted: Arc<Noted>,
}

impl NotingKernel {
    fn new() -> (Arc<Self>, Arc<Noted>) {
        let noted = Arc::new(Noted::default());
        let factory = Arc::new(Self {
            kernel: KernelSockets::new(None).unwrap(),
            noted: Arc::clone(&noted),
        });
        (factory, noted)
    }

    fn note(&self, socket: &Arc<dyn DatagramSocket>) {
        self.noted
            .datagram_ports
            .lock()
            .unwrap()
            .push(socket.bind_addr().port());
    }
}

impl SocketFactory for NotingKernel {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        *self.noted.tcp_dials.lock().unwrap() += 1;
        self.kernel.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.kernel.listen_tcp(addr)
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let socket = self.kernel.bind_udp(addr).await?;
            self.note(&socket);
            Ok(socket)
        })
    }

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let socket = self.kernel.bind_utp(addr, receive_buffer).await?;
            self.note(&socket);
            Ok(socket)
        })
    }
}

// A route's sessions on a System leg over `factory`, under `torrent`.
fn system_route(factory: Arc<dyn SocketFactory>) -> Arc<FakeRoutes> {
    let routes = Arc::new(FakeRoutes::default());
    routes
        .no_local_discovery
        .store(true, std::sync::atomic::Ordering::SeqCst);
    routes.set_leg(LegKind::System, factory);
    routes
}

// Settings that keep a session to loopback: no gateway mapping, no DHT
// bootstrap node.
fn on_port(base_port: u16, peer_protocol: TorrentPeerProtocol) -> TorrentConfig {
    TorrentConfig {
        base_port,
        peer_protocol,
        upnp: false,
        dht_bootstrap_nodes: String::new(),
        ..TorrentConfig::default()
    }
}

// A session the server builds on a System leg, listening on a set port with
// TCP and uTP, comes up with its DHT on that port over the one datagram
// socket uTP listens on; a second such session, uTP alone, downloads a
// torrent from it over uTP.
#[tokio::test(flavor = "multi_thread")]
async fn a_system_leg_session_with_both_transports_builds_and_moves_data_over_utp()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let seed_directory = directory.path().join("seed");
    let download_directory = directory.path().join("download");
    tokio::fs::create_dir_all(&seed_directory).await?;
    let payload: Vec<_> = (0..512 * 1024 + 137)
        .map(|offset| u8::try_from(offset % 251).unwrap())
        .collect();
    tokio::fs::write(seed_directory.join("payload.bin"), &payload).await?;
    let metadata = create_torrent(
        &seed_directory,
        CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let (seed_port, download_port) = two_free_ports();

    let (seed_factory, seed_noted) = NotingKernel::new();
    let seed_routes = system_route(seed_factory);
    let seeds = Sessions::new(
        seed_routes.clone(),
        env(&directory.path().join("seed-state")),
        Database::open_in_memory()?,
    );
    seeds.set_config(on_port(seed_port, TorrentPeerProtocol::Both));
    let seed_plan = seed_routes.plan(TorrentScope::Default);
    let seed = seeds
        .session(&seed_plan, &IDENTITY.identity())
        .await
        .map_err(|error| anyhow::anyhow!("the seed's session did not build: {error}"))?;
    let view = seeds.view(&seed_plan.key).await.context("no route view")?;
    assert!(view.utp && view.held.is_none(), "{view:?}");
    assert_eq!(view.port, Some(seed_port));
    assert_eq!(
        seed.get_dht()
            .context("the seed runs no DHT")?
            .listen_addr()
            .port(),
        seed_port
    );
    // One datagram socket on the peer port, which uTP and the DHT share.
    let bound = seed_noted
        .datagram_ports
        .lock()
        .unwrap()
        .iter()
        .filter(|port| **port == seed_port)
        .count();
    assert_eq!(bound, 1);

    let seeded = seed
        .add_torrent(
            AddTorrent::from_bytes(metadata.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(seed_directory.to_str().context("UTF-8")?.into()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("missing seed handle")?;
    seeded.wait_until_completed().await?;

    let (download_factory, download_noted) = NotingKernel::new();
    let download_routes = system_route(download_factory);
    let downloads = Sessions::new(
        download_routes.clone(),
        env(&directory.path().join("download-state")),
        Database::open_in_memory()?,
    );
    downloads.set_config(on_port(download_port, TorrentPeerProtocol::Utp));
    let download_plan = download_routes.plan(TorrentScope::Default);
    let client = downloads
        .session(&download_plan, &IDENTITY.identity())
        .await
        .map_err(|error| anyhow::anyhow!("the downloader's session did not build: {error}"))?;
    let download = client
        .add_torrent(
            AddTorrent::from_bytes(metadata),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![SocketAddr::from((Ipv4Addr::LOCALHOST, seed_port))]),
                output_folder: Some(download_directory.to_str().context("UTF-8")?.into()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("missing download handle")?;
    download.wait_until_completed().await?;
    assert_eq!(
        tokio::fs::read(download_directory.join("payload.bin")).await?,
        payload
    );
    assert_eq!(*download_noted.tcp_dials.lock().unwrap(), 0);
    assert!(client.stats_snapshot().utp.max_congestion_window_bytes > 0);

    downloads.stop_all().await;
    seeds.stop_all().await;
    Ok(())
}
