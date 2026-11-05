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
    peer_info_reader::tests::{Dialed, MockPeer, WatchedDials},
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
    // The query of every request heard, in order, each recorded before it
    // is heard.
    queries: Arc<parking_lot::Mutex<Vec<String>>>,
}

impl Tracker {
    fn start(peer: Option<SocketAddr>) -> (Self, mpsc::UnboundedReceiver<Heard>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let queries = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let stop = stop.clone();
            let queries = queries.clone();
            tokio::task::spawn_blocking(move || {
                for conn in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(conn) = conn else { continue };
                    let tx = tx.clone();
                    let queries = queries.clone();
                    std::thread::spawn(move || serve(conn, peer, &tx, &queries));
                }
            });
        }
        (
            Self {
                addr,
                stop,
                queries,
            },
            rx,
        )
    }

    fn url(&self) -> String {
        format!("http://{}/announce", self.addr)
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        crate::test_support::knock(self.addr);
    }
}

fn serve(
    mut conn: TcpStream,
    peer: Option<SocketAddr>,
    heard: &mpsc::UnboundedSender<Heard>,
    queries: &parking_lot::Mutex<Vec<String>>,
) {
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
    queries.lock().push(query.to_owned());
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
            socket_factory: Arc::new(Dialed(tx)),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
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
    // The peer is let go: its connection ends, and neither the torrent nor
    // the session counts it live, nor does the torrent's view.
    assert_eq!(peer.next_request().await, None);
    assert_eq!(run.stats_snapshot().peer_stats.live, 0);
    assert_eq!(session.stats_snapshot().peers.live, 0);
    assert!(paused.stats().live.is_none());

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

// A pause ends the run, not the peer: the torrent resumed dials the peer
// it let go at once, before any time passes, rather than a reconnect time
// after the pause.
#[tokio::test(start_paused = true)]
async fn test_a_resumed_torrent_dials_the_peer_it_let_go_at_once() -> anyhow::Result<()> {
    let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let (tracker, mut heard) = Tracker::start(Some(peer_addr));
    let (tx, mut dials) = mpsc::unbounded_channel();
    let out = TempDir::with_prefix("resumed_dials_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            socket_factory: Arc::new(Dialed(tx)),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
                peer_opts: None,
            }),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let torrent = TestTorrent::new("resumed_dials", 4096, 1024).await?;
    let torrent = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![tracker.url()]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    assert_eq!(next(&mut heard).await?, announce(Some("started")));
    let (dialed, stream) = dials.recv().await.context("no dial")?;
    assert_eq!(dialed, peer_addr);
    let mut peer = MockPeer::greet(stream, torrent.shared().info_hashes).await;
    let run = torrent.live().context("the torrent is not live")?;
    while run.stats_snapshot().peer_stats.live != 1 {
        tokio::task::yield_now().await;
    }

    session.pause(&torrent).await?;
    assert_eq!(next(&mut heard).await?, announce(Some("stopped")));
    assert_eq!(peer.next_request().await, None);

    let resumed_at = tokio::time::Instant::now();
    session.unpause(&torrent).await?;
    let (dialed, _stream) = dials.recv().await.context("no dial")?;
    assert_eq!(dialed, peer_addr);
    assert_eq!(tokio::time::Instant::now(), resumed_at);
    drop(tracker);
    session.stop().await;
    Ok(())
}

// The whole chain on the mocked clock: a pause cuts the one connection in
// the middle of a block, names the run's tasks before anything ends, and the
// `stopped` announce waits for the connection to close and count the part
// of the block it received as redundant before it reads the counters, as
// libtorrent's `torrent::do_pause` disconnects every connection, each
// counting the block it was cut in (`peer_connection::disconnect`), before
// `stop_announcing` builds `stopped`. Once the run's tasks have ended the
// torrent forgets them.
#[tokio::test(start_paused = true)]
async fn test_a_pause_reports_the_block_it_cut_in_the_stopped_announce() -> anyhow::Result<()> {
    use crate::buffers::ByteBuf;
    use crate::peer_protocol::Message;
    const RECEIVED: usize = 500;

    let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let (tracker, mut heard) = Tracker::start(Some(peer_addr));
    let (tx, mut dials) = mpsc::unbounded_channel();
    let out = TempDir::with_prefix("pause_cut_block_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            socket_factory: Arc::new(WatchedDials(tx)),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
                peer_opts: None,
            }),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let torrent = TestTorrent::new("pause_cut_block", 4096, 1024).await?;
    let torrent = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![tracker.url()]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    assert_eq!(next(&mut heard).await?, announce(Some("started")));
    let (dialed, stream, mut taken) = dials.recv().await.context("no dial")?;
    assert_eq!(dialed, peer_addr);
    let mut peer = MockPeer::greet(stream, torrent.shared().info_hashes).await;

    // The peer has every piece and unchokes us; we ask it for a block, and
    // it sends that block's header and part of its data.
    peer.send(Message::Bitfield(ByteBuf(&[0xf0]))).await;
    peer.send(Message::Unchoke).await;
    let request = peer.next_block_request().await.context("no request")?;
    let mut frame = (9 + request.length).to_be_bytes().to_vec();
    frame.push(7);
    frame.extend(request.index.to_be_bytes());
    frame.extend(request.begin.to_be_bytes());
    frame.extend(vec![0xab; RECEIVED]);
    peer.write_raw(&frame).await;
    let sent = peer.sent;
    taken.wait_for(|n| *n == sent).await?;

    session.pause(&torrent).await?;
    assert_eq!(next(&mut heard).await?, announce(Some("stopped")));
    let stopped = tracker.queries.lock().last().cloned().context("no query")?;
    assert_eq!(
        super::e2e_tracker_announces::param(&stopped, "redundant"),
        Some(RECEIVED.to_string().as_str())
    );
    assert!(torrent.shared().ending_run.lock().is_none());
    drop(tracker);
    session.stop().await;
    Ok(())
}
