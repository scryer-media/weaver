// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use anyhow::Context;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, CreateTorrentOptions, Session,
    SessionOptions, SocketFactory, StreamListener, create_torrent,
    network::{
        AsyncReadVectoredIntoCompat, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture,
        DatagramSocket, TcpRoom, TcpWant,
    },
    spawn_utils::BlockingSpawner,
};
use weaver_tunnel::pipe::{
    SessionSockets, SocketBudget, SocketClass, SocketPermit, overhead_bytes, stream_bytes,
};

// A route whose TCP room is a real tunnel ledger: a look is the tunnel's
// wait for room, and a dial takes a socket from it. Each dial says whether
// it was seated; a seated dial keeps its socket and its far end.
struct TunnelRoute {
    tunnel: Arc<SessionSockets>,
    dials: tokio::sync::mpsc::UnboundedSender<bool>,
    seated: Mutex<Vec<(SocketPermit, tokio::io::DuplexStream)>>,
    // The UDP trackers' socket the session binds, though it asks no tracker,
    // on loopback.
    kernel: weaver_torrent::network::KernelSockets,
}

impl SocketFactory for TunnelRoute {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            match self.tunnel.take(SocketClass::Torrent, stream_bytes(None)) {
                Ok(permit) => {
                    let (ours, theirs) = tokio::io::duplex(64 * 1024);
                    self.seated
                        .lock()
                        .unwrap()
                        .push((permit.expect("a tunnel with a budget"), theirs));
                    let _ = self.dials.send(true);
                    let (read, write) = tokio::io::split(ours);
                    Ok((
                        Box::new(read.into_vectored_compat()) as BoxAsyncReadVectored,
                        Box::new(write) as BoxAsyncWrite,
                    ))
                }
                Err(refused) => {
                    drop(refused);
                    let _ = self.dials.send(false);
                    Err(io::Error::other("the tunnel is full"))
                }
            }
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async { Err(io::Error::from(io::ErrorKind::Unsupported)) })
    }

    fn tcp_room(&self) -> TcpRoom {
        match self
            .tunnel
            .wait_room(SocketClass::Torrent, stream_bytes(None))
        {
            Ok(()) => TcpRoom::Free,
            Err(refused) => match refused.demand {
                Some(demand) => TcpRoom::Crowded(TcpWant::new(demand)),
                None => TcpRoom::Full,
            },
        }
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.kernel
            .bind_udp(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
    }
}

// The reviewed case, through the torrent's own connect loop. Usenet holds
// every socket of a 50/50 tunnel but one torrent peer. The torrent's one
// candidate is dialled: its look finds the tunnel crowded out, its dial is
// refused, and the loop waits out the candidate's reconnect time. The
// torrent still wants the room through that wait, so the press stands when
// usenet's give-back runs on its own turn after it; usenet closes what it
// owes, and the torrent's next look is free and its dial seats.
#[tokio::test]
async fn a_torrent_waiting_out_a_reconnect_time_keeps_its_press_until_it_seats()
-> anyhow::Result<()> {
    let peer = stream_bytes(None);
    let tunnel = Arc::new(SessionSockets::new(
        SocketBudget {
            bytes: overhead_bytes(1420) + 4 * peer + 100 * peer,
            reserve: 4 * peer,
            torrent_kept: 0,
            mtu: 1420,
        },
        Arc::new(crate::proxies::SplitDivider::Fixed(None)),
    ));
    let mut usenet = Vec::new();
    while tunnel.has_room(SocketClass::Nntp, peer) {
        usenet.push(
            tunnel
                .take(SocketClass::Nntp, peer)
                .ok()
                .flatten()
                .context("the room usenet had")?,
        );
    }
    let _torrent = tunnel
        .take(SocketClass::Torrent, peer)
        .ok()
        .flatten()
        .context("the idle side's probe")?;
    assert_eq!(usenet.len(), 99);
    assert_eq!(tunnel.woken(), [false, false]);

    let directory = tempfile::tempdir()?;
    let seed_directory = directory.path().join("seed");
    let download_directory = directory.path().join("download");
    tokio::fs::create_dir_all(&seed_directory).await?;
    tokio::fs::write(seed_directory.join("payload.bin"), vec![7u8; 64 * 1024]).await?;
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
    let (dials, mut dialled) = tokio::sync::mpsc::unbounded_channel();
    let route = Arc::new(TunnelRoute {
        tunnel: Arc::clone(&tunnel),
        dials,
        seated: Mutex::new(Vec::new()),
        kernel: weaver_torrent::network::KernelSockets::new(None)?,
    });
    let session = Session::new_with_opts(
        download_directory.clone(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: None,
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: Some(false),
                peer_opts: None,
            }),
            ipv4_only: true,
            ..SessionOptions::new(route.clone())
        },
    )
    .await?;
    tokio::time::pause();
    let _torrent_handle = session
        .add_torrent(
            AddTorrent::from_bytes(metadata),
            Some(AddTorrentOptions {
                initial_peers: Some(vec![SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 6881))]),
                output_folder: Some(
                    download_directory
                        .to_str()
                        .context("download path is not UTF-8")?
                        .into(),
                ),
                ..Default::default()
            }),
        )
        .await?;

    assert_eq!(
        dialled.recv().await,
        Some(false),
        "the first dial is crowded out"
    );
    // Usenet's give-back, on its own turn after the dial: the torrent waits
    // out its candidate's reconnect time and still presses.
    assert!(tunnel.pressing(SocketClass::Torrent), "the want lapsed");
    assert_eq!(tunnel.woken(), [true, false], "usenet is woken");
    let owed = tunnel.owed(SocketClass::Nntp);
    assert_eq!(owed, (99 - 50) * peer, "usenet owes its bytes past half");
    for _ in 0..owed.div_ceil(peer) {
        drop(usenet.pop());
    }
    assert!(!tunnel.owes(SocketClass::Nntp));

    assert_eq!(dialled.recv().await, Some(true), "the next dial seats");
    assert!(
        !tunnel.pressing(SocketClass::Torrent),
        "the free look let the want go"
    );
    assert_eq!(route.seated.lock().unwrap().len(), 1);
    session.stop().await;
    Ok(())
}
