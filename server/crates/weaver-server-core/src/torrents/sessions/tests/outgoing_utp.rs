// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, IpFilter, ListenerMode, ListenerOptions,
    Session, SessionOptions, SocketFactory, StreamListener, create_torrent,
    network::{BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket},
    spawn_utils::BlockingSpawner,
};
use weaver_tunnel::{
    TunnelError,
    endpoint::{DatagramSocket as TunnelSocket, DatagramTransport},
    pipe::{DialError, Dialed, Dialer, Resolution, Target},
};

use crate::{
    settings::{TorrentConfig, TorrentPeerProtocol},
    torrents::{
        identity::IDENTITY,
        net::{LegKind, LegSockets},
    },
};

#[derive(Default)]
struct Carrier {
    sent: AtomicU64,
    received: AtomicU64,
    tcp: AtomicUsize,
}

struct CarrierSocket {
    inner: Arc<dyn DatagramSocket>,
    carrier: Arc<Carrier>,
}

#[async_trait::async_trait]
impl TunnelSocket for CarrierSocket {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
        let bytes = self.inner.send_to(data, target).await?;
        self.carrier
            .sent
            .fetch_add(u64::try_from(bytes).unwrap(), Ordering::Relaxed);
        Ok(())
    }
    async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let received = self.inner.recv_from(buffer).await?;
        self.carrier
            .received
            .fetch_add(u64::try_from(received.0).unwrap(), Ordering::Relaxed);
        Ok(received)
    }
    fn local_addr(&self) -> Option<SocketAddr> {
        Some(self.inner.bind_addr())
    }
    fn link_mtu(&self) -> Option<u16> {
        Some(1280)
    }
    fn is_closed(&self) -> bool {
        false
    }
    async fn closed(&self) {
        std::future::pending().await
    }
}

struct CarriedRoute(Arc<Carrier>);

#[async_trait::async_trait]
impl DatagramTransport for CarriedRoute {
    async fn resolve(&self, _: &str) -> Result<Vec<IpAddr>, TunnelError> {
        Ok(Vec::new())
    }
    async fn bind(&self, _: SocketAddr) -> Result<Arc<dyn TunnelSocket>, TunnelError> {
        let sockets = weaver_torrent::network::KernelSockets::new(None)
            .map_err(|error| TunnelError::Engine(error.to_string()))?;
        let inner = sockets
            .bind_udp((Ipv4Addr::LOCALHOST, 0).into())
            .await
            .map_err(|error| TunnelError::Engine(error.to_string()))?;
        Ok(Arc::new(CarrierSocket {
            inner,
            carrier: self.0.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl Dialer for CarriedRoute {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        self.0.tcp.fetch_add(1, Ordering::Relaxed);
        Err(DialError::Skipped("datagram-only route".into()))
    }
    async fn resolve(&self, _: &str) -> Result<Resolution, DialError> {
        Ok(Resolution::Addresses(Vec::new()))
    }
    fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
        Some(self)
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "datagram-only route".into()
    }
}

struct RoutedFactory {
    sockets: LegSockets,
    utp: AtomicUsize,
    tcp: AtomicUsize,
    listeners: AtomicUsize,
    bindings: Mutex<Vec<Weak<dyn DatagramSocket>>>,
}

impl SocketFactory for RoutedFactory {
    fn connect_tcp<'a>(
        &'a self,
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        self.tcp.fetch_add(1, Ordering::Relaxed);
        self.sockets.connect_tcp(target)
    }
    fn listen_tcp<'a>(
        &'a self,
        address: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listeners.fetch_add(1, Ordering::Relaxed);
        self.sockets.listen_tcp(address)
    }
    fn bind_udp<'a>(
        &'a self,
        address: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.sockets.bind_udp(address)
    }
    fn bind_utp<'a>(
        &'a self,
        address: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.utp.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            let socket = self.sockets.bind_utp(address, receive_buffer).await?;
            self.bindings.lock().unwrap().push(Arc::downgrade(&socket));
            Ok(socket)
        })
    }
}

#[tokio::test]
async fn a_non_listening_datagram_route_completes_a_utp_only_torrent_transfer() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let seed_directory = directory.path().join("seed");
    let download_directory = directory.path().join("download");
    tokio::fs::create_dir_all(&seed_directory).await?;
    let payload: Vec<_> = (0..2 * 1024 * 1024 + 137)
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
    let seed = Session::new_with_opts(
        seed_directory.clone(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                mode: ListenerMode::UtpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..SessionOptions::new(Arc::new(weaver_torrent::network::KernelSockets::new(None)?))
        },
    )
    .await?;
    let seeded = seed
        .add_torrent(
            AddTorrent::from_bytes(metadata.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(
                    seed_directory
                        .to_str()
                        .context("seed path is not UTF-8")?
                        .into(),
                ),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("missing seed handle")?;
    seeded.wait_until_completed().await?;
    let peer = seed.listen_addr().context("seed is not listening")?;

    let carrier = Arc::new(Carrier::default());
    let dialer: Arc<dyn Dialer> = Arc::new(CarriedRoute(carrier.clone()));
    let kind = LegKind::Ladder;
    let shape = kind.shape(Some(&dialer));
    assert!(shape.utp && shape.datagrams && !shape.listen);
    let factory = Arc::new(RoutedFactory {
        sockets: LegSockets::new(dialer, &kind),
        utp: AtomicUsize::new(0),
        tcp: AtomicUsize::new(0),
        listeners: AtomicUsize::new(0),
        bindings: Mutex::new(Vec::new()),
    });
    let mut leg = super::leg(kind);
    leg.shape = shape;
    leg.factory = factory.clone();
    let torrent = TorrentConfig {
        peer_protocol: TorrentPeerProtocol::Utp,
        ..Default::default()
    };
    let mut options = super::session_options(
        &leg,
        0,
        directory.path(),
        4,
        &torrent,
        IpFilter::default(),
        &IDENTITY.identity(),
    );
    assert!(options.listen.is_none());
    let connect = options
        .connect
        .as_ref()
        .context("missing outgoing policy")?;
    assert!(!connect.enable_tcp);
    assert_eq!(connect.enable_utp, Some(true));
    options.dht = None;
    options.disable_trackers = true;
    options.disable_local_service_discovery = true;
    options.ipv4_only = true;
    let client = Session::new_with_opts(download_directory.clone(), options).await?;
    let download = client
        .add_torrent(
            AddTorrent::from_bytes(metadata),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![peer]),
                output_folder: Some(
                    download_directory
                        .to_str()
                        .context("download path is not UTF-8")?
                        .into(),
                ),
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
    assert!(
        carrier.sent.load(Ordering::Relaxed) > 0 && carrier.received.load(Ordering::Relaxed) > 0
    );
    assert_eq!(carrier.tcp.load(Ordering::Relaxed), 0);
    assert_eq!(factory.utp.load(Ordering::Relaxed), 1);
    assert_eq!(factory.tcp.load(Ordering::Relaxed), 0);
    assert_eq!(factory.listeners.load(Ordering::Relaxed), 0);
    assert_eq!(client.listen_addr(), None);
    assert_eq!(client.ssl_listen_addr(), None);
    assert_eq!(client.announce_port(), None);
    assert!(!client.listens_utp());
    assert!(client.uses_utp());
    assert!(crate::torrents::net::Exposure::of(&client, shape, None, &[]).utp);
    assert!(client.stats_snapshot().utp.max_congestion_window_bytes > 0);
    client.stop().await;
    assert!(
        factory
            .bindings
            .lock()
            .unwrap()
            .iter()
            .all(|socket| socket.upgrade().is_none())
    );
    seed.stop().await;
    Ok(())
}
