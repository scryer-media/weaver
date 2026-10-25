// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A paused torrent goes quiet: it says `stopped` to its trackers, lets its
//! peers go, and asks nothing of either again while it rests.

use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, Session, SessionOptions,
    peer_info_reader::tests::{Dialed, MockPeer},
    storage::StorageFactory,
    tests::{
        e2e_tracker_announces::tracker_session_opts,
        embedder_util::{MemoryStorageFactory, TestTorrent},
    },
};

/// A request the tracker heard: its path and its `event`.
type Heard = (String, Option<String>);

/// An HTTP tracker on loopback that hands out `peer`, if any, and hears
/// every request.
/// Its accept loop runs on the blocking pool, which holds the paused clock
/// still: time moves only when the test moves it.
struct Tracker {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
}

impl Tracker {
    fn start(peer: Option<SocketAddr>) -> (Self, mpsc::UnboundedReceiver<Heard>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let stop = stop.clone();
            tokio::task::spawn_blocking(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(conn) = conn else { continue };
                    let tx = tx.clone();
                    std::thread::spawn(move || serve(conn, peer, &tx));
                }
            });
        }
        (Self { addr, stop }, rx)
    }

    fn url(&self) -> String {
        format!("http://{}/announce", self.addr)
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

fn serve(mut conn: TcpStream, peer: Option<SocketAddr>, heard: &mpsc::UnboundedSender<Heard>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        match conn.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let request = String::from_utf8_lossy(&buf).into_owned();
    let Some(target) = request.split(' ').nth(1) else {
        return;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let event = super::e2e_tracker_announces::param(query, "event").map(str::to_owned);
    let mut body = b"d8:intervali1800e5:peers".to_vec();
    match peer {
        Some(SocketAddr::V4(peer)) => {
            body.extend_from_slice(b"6:");
            body.extend_from_slice(&peer.ip().octets());
            body.extend_from_slice(&peer.port().to_be_bytes());
        }
        _ => body.extend_from_slice(b"0:"),
    }
    body.push(b'e');
    let _ = heard.send((path.to_owned(), event));
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = conn.write_all(head.as_bytes());
    let _ = conn.write_all(&body);
}

/// The next request `heard` hears.
async fn next(heard: &mut mpsc::UnboundedReceiver<Heard>) -> anyhow::Result<Heard> {
    heard.recv().await.context("the tracker stopped")
}

fn announce(event: Option<&str>) -> Heard {
    ("/announce".into(), event.map(str::to_owned))
}

/// Waits until the announce `torrent`'s tracker heard is answered and
/// recorded, its next one scheduled.
async fn answered(torrent: &crate::ManagedTorrent) -> anyhow::Result<()> {
    let status = torrent
        .tracker_status()
        .context("the torrent has no trackers")?;
    status.subscribe().wait_for(|a| !a[0].in_flight).await?;
    Ok(())
}

// A paused torrent says `stopped` to its tracker and lets its one peer go.
// Through two more announce intervals of a torrent beside it that keeps
// running, which mark how far time has gone, the paused one asks its tracker
// nothing (no announce, no scrape) and dials no peer.
#[tokio::test(start_paused = true)]
async fn test_a_paused_torrent_asks_nothing_of_its_tracker_or_peers() -> anyhow::Result<()> {
    let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let (tracker, mut heard) = Tracker::start(Some(peer_addr));
    let (control_tracker, mut control_heard) = Tracker::start(None);
    let (tx, mut dials) = mpsc::unbounded_channel();
    let out = TempDir::with_prefix("paused_quiet_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            socket_factory: Some(Arc::new(Dialed(tx))),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                peer_opts: None,
            }),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let add = |torrent: &TestTorrent, tracker: &Tracker| {
        session.add_torrent(
            AddTorrent::from_bytes(torrent.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![tracker.url()]),
                ..Default::default()
            }),
        )
    };

    let paused = TestTorrent::new("paused_quiet", 4096, 1024).await?;
    let paused = add(&paused, &tracker)
        .await?
        .into_handle()
        .context("expected a handle")?;
    assert_eq!(next(&mut heard).await?, announce(Some("started")));
    let (dialed, stream) = dials.recv().await.context("no dial")?;
    assert_eq!(dialed, peer_addr);
    let mut peer = MockPeer::greet(stream, paused.shared().info_hashes).await;
    let run = paused.live().context("the torrent is not live")?;
    while run.stats_snapshot().peer_stats.live != 1 {
        tokio::task::yield_now().await;
    }

    let control = TestTorrent::new("paused_quiet_control", 4096, 1024).await?;
    let control = add(&control, &control_tracker)
        .await?
        .into_handle()
        .context("expected a handle")?;
    assert_eq!(next(&mut control_heard).await?, announce(Some("started")));
    answered(&control).await?;

    session.pause(&paused).await?;
    assert_eq!(next(&mut heard).await?, announce(Some("stopped")));
    // The peer is let go: its connection ends, and the torrent counts no
    // peer.
    assert_eq!(peer.next_request().await, None);
    assert_eq!(run.stats_snapshot().peer_stats.live, 0);

    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(next(&mut control_heard).await?, announce(None));
        answered(&control).await?;
    }
    assert!(
        matches!(heard.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "the paused torrent asked its tracker"
    );
    assert!(
        matches!(dials.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "the paused torrent dialed a peer"
    );
    assert!(paused.is_paused());
    drop((tracker, control_tracker));
    session.stop().await;
    Ok(())
}
