// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use tokio::sync::mpsc;

use crate::{
    AddTorrent, AddTorrentOptions, DhtSessionConfig, ListenerOptions, Session, SessionOptions,
    bencode::ByteBufOwned,
    dht::{
        DHT_ANNOUNCE_INTERVAL, Id20,
        bprotocol::{self, MessageKind, Response},
    },
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, KernelSockets,
        SocketFactory, StreamListener,
    },
    storage::StorageFactory,
    tests::embedder_util::{MemoryStorageFactory, TestTorrent},
};

struct Loopback;

impl SocketFactory for Loopback {
    fn connect_tcp<'a>(
        &'a self,
        _: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move {
            KernelSockets::new(None)
                .map_err(io::Error::other)?
                .listen_tcp(addr)
                .await
        })
    }

    fn bind_udp<'a>(&'a self, _: SocketAddr) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async {
            KernelSockets::new(None)
                .map_err(io::Error::other)?
                .bind_udp((Ipv4Addr::LOCALHOST, 0).into())
                .await
        })
    }
}

// The blocking receive keeps the paused clock still until the test advances
// it. Every query is answered; searches and announces are reported by hash.
struct Node {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    heard: mpsc::UnboundedReceiver<(Id20, bool)>,
}

impl Node {
    fn new() -> anyhow::Result<Self> {
        // Served from the blocking pool, which holds the paused clock still
        // while the node waits for a datagram.
        let socket = crate::test_support::loopback_udp_now();
        let addr = socket.bind_addr();
        let runtime = tokio::runtime::Handle::current();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let (tx, heard) = mpsc::unbounded_channel();
        tokio::task::spawn_blocking(move || {
            let mut buf = [0u8; 4096];
            loop {
                let (len, from) = runtime.block_on(socket.recv_from(&mut buf)).unwrap();
                if stopped.load(Ordering::SeqCst) {
                    break;
                }
                let msg = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
                match &msg.kind {
                    MessageKind::GetPeersRequest(q) => {
                        let _ = tx.send((q.info_hash, false));
                    }
                    MessageKind::AnnouncePeer(q) => {
                        let _ = tx.send((q.info_hash, true));
                    }
                    MessageKind::PingRequest(_) | MessageKind::FindNodeRequest(_) => {}
                    _ => continue,
                }
                let mut reply = Vec::new();
                bprotocol::serialize_message(
                    &mut reply,
                    msg.transaction_id,
                    None,
                    None,
                    MessageKind::Response(Response {
                        id: Id20::new([0x42; 20]),
                        token: Some(ByteBufOwned::from(b"token".as_slice())),
                        ..Default::default()
                    }),
                )
                .unwrap();
                runtime.block_on(socket.send_to(&reply, from)).unwrap();
            }
        });
        Ok(Self { addr, stop, heard })
    }

    async fn announced(&mut self, hash: Id20, forbidden: Option<Id20>) -> anyhow::Result<()> {
        loop {
            let (asked, announce) = self.heard.recv().await.context("the node stopped")?;
            assert_ne!(Some(asked), forbidden, "the paused hash asked the DHT");
            if asked == hash && announce {
                return Ok(());
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        use futures::FutureExt;
        let socket = crate::test_support::loopback_udp_now();
        let _ = socket.send_to(&[], self.addr).now_or_never();
    }
}

#[tokio::test(start_paused = true)]
async fn a_paused_hash_stops_searching_and_announcing_until_resumed() -> anyhow::Result<()> {
    let mut node = Node::new()?;
    let dir = tempfile::tempdir()?;
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(vec![node.addr.to_string()]),
                port: None,
                persistence: None,
            }),
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ..Default::default()
            }),
            socket_factory: Arc::new(Loopback),
            disable_local_service_discovery: true,
            persistence: None,
            ..crate::test_support::session_options()
        },
    )
    .await?;
    let add = |torrent: &TestTorrent| {
        session.add_torrent(
            AddTorrent::from_bytes(torrent.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                ..Default::default()
            }),
        )
    };
    let torrent = TestTorrent::new("dht_pause", 4096, 1024).await?;
    let paused = add(&torrent).await?.into_handle().context("no torrent")?;
    let hash = paused.shared().info_hash;
    node.announced(hash, None).await?;
    session.pause(&paused).await?;

    let torrent = TestTorrent::new("dht_control", 4096, 1024).await?;
    let control = add(&torrent).await?.into_handle().context("no control")?;
    let control_hash = control.shared().info_hash;
    node.announced(control_hash, Some(hash)).await?;
    for _ in 0..2 {
        tokio::time::advance(DHT_ANNOUNCE_INTERVAL).await;
        node.announced(control_hash, Some(hash)).await?;
    }
    session.unpause(&paused).await?;
    node.announced(hash, None).await?;
    session.stop().await;
    Ok(())
}
