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
pub(crate) type Heard = (String, Option<String>);

/// An HTTP tracker on loopback that hands out `peer`, if any, and hears
/// every request.
/// Its accept loop runs on the blocking pool, which holds the paused clock
/// still: time moves only when the test moves it.
pub(crate) struct Tracker {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    // The query of every request heard, in order, each recorded before it
    // is heard.
    pub(crate) queries: Arc<parking_lot::Mutex<Vec<String>>>,
}

impl Tracker {
    pub(crate) fn start(peer: Option<SocketAddr>) -> (Self, mpsc::UnboundedReceiver<Heard>) {
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

    pub(crate) fn url(&self) -> String {
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
pub(crate) async fn next(heard: &mut mpsc::UnboundedReceiver<Heard>) -> anyhow::Result<Heard> {
    heard.recv().await.context("the tracker stopped")
}

pub(crate) fn announce(event: Option<&str>) -> Heard {
    ("/announce".into(), event.map(str::to_owned))
}

/// Waits until the announce `torrent`'s tracker heard is answered and
/// recorded, its next one scheduled.
pub(crate) async fn answered(torrent: &crate::ManagedTorrent) -> anyhow::Result<()> {
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
                utp_on_demand: false,
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
                utp_on_demand: false,
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
                utp_on_demand: false,
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

// Files that change under a seeding torrent, as a move that keeps a file
// already at the new place changes them: the torrent forgets every piece
// before they change, as libtorrent's `torrent::force_recheck` does
// (`disconnect_all`, then `set_have_all(false)`). Its peer, served a block a
// moment before, is let go, and the block it asks for once the files have
// changed is not sent it: no block of the other file's bytes is. The peer
// dialed after the check is told of no piece: the check passed none.
#[tokio::test(start_paused = true)]
async fn test_a_torrent_whose_files_changed_serves_none_of_them_unchecked() -> anyhow::Result<()> {
    use crate::buffers::ByteBuf;
    use crate::peer_info_reader::tests::Offered;
    use crate::peer_protocol::{Message, Request};
    const FOREIGN: u8 = 0xa5;

    let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let (tracker, mut heard) = Tracker::start(Some(peer_addr));
    let (tx, mut dials) = mpsc::unbounded_channel();
    let out = TempDir::with_prefix("changed_files_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            socket_factory: Arc::new(Dialed(tx)),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
                utp_on_demand: false,
                peer_opts: None,
            }),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let t = TestTorrent::new("changed_files", 4096, 1024).await?;
    let storage = MemoryStorageFactory::holding(vec![t.content.clone()]);
    let torrent = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
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
    assert!(torrent.stats().finished, "the torrent does not seed");
    // The peer wants the first block, and is sent it: its requests reach
    // the torrent and are served while the files are the torrent's own.
    peer.send(Message::Interested).await;
    peer.send(Message::Request(Request::new(0, 0, 1024))).await;
    let (piece, begin, data) = peer.next_block().await.context("the seed sent no block")?;
    assert_eq!((piece, begin), (0, 0));
    assert_eq!(data[..], t.content[..1024]);

    session.forget_for_recheck(&torrent).await?;
    storage.log.overwrite_file(0, FOREIGN);

    // It asks for the next block, of the files as they are now. The
    // connection ends, and no block is sent on it: were the pieces still
    // had, this one would go out with the other file's bytes.
    peer.send_if_open(Message::Request(Request::new(1, 0, 1024)))
        .await;
    assert_eq!(
        peer.next_block().await,
        None,
        "a block was served unchecked"
    );

    // Checked, the torrent has no piece and says so to the peer it dials:
    // nothing it has, and no block, before it asks that peer for one.
    session.restart_check(&torrent).await?;
    torrent.wait_until_initialized().await?;
    let (dialed, stream) = dials.recv().await.context("no dial after the check")?;
    assert_eq!(dialed, peer_addr);
    let mut peer = MockPeer::greet(stream, torrent.shared().info_hashes).await;
    peer.send(Message::Bitfield(ByteBuf(&[0xf0]))).await;
    peer.send(Message::Unchoke).await;
    let (offered, request) = peer.offered_until_request().await;
    assert!(request.is_some(), "the torrent asked for nothing");
    assert!(
        offered.iter().all(|said| match said {
            Offered::HaveNone => true,
            Offered::Bitfield(bits) => bits.iter().all(|byte| *byte == 0),
            _ => false,
        }),
        "{offered:?}"
    );
    drop(tracker);
    session.stop().await;
    Ok(())
}

// A recheck of a running torrent says `stopped` for the run it ends, with
// that run's counters, and `started` for the run after the check, counted
// from zero, as libtorrent does: `torrent::force_recheck` calls
// `stop_announcing` (torrent.cpp:2407), which announces `stopped`
// (torrent.cpp:10384), and `files_checked` calls `start_announcing`
// (torrent.cpp:8884), which resets every tracker's `start_sent`
// (torrent.cpp:10346) and the session's counters (torrent.cpp:10351-10353,
// v2.0.15). Neither waits for the other, there or here: the tracker may
// read the two in either order, so each is told apart by its event.
#[tokio::test(start_paused = true)]
async fn test_a_recheck_says_stopped_with_the_runs_counters_then_started_from_zero()
-> anyhow::Result<()> {
    use super::e2e_tracker_announces::param;
    use crate::peer_protocol::{Message, Request};

    let peer_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let (tracker, mut heard) = Tracker::start(Some(peer_addr));
    let (tx, mut dials) = mpsc::unbounded_channel();
    let out = TempDir::with_prefix("recheck_events_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            socket_factory: Arc::new(Dialed(tx)),
            connect: Some(ConnectionOptions {
                enable_tcp: true,
                enable_utp: None,
                utp_on_demand: false,
                peer_opts: None,
            }),
            ..tracker_session_opts()?
        },
    )
    .await?;
    let t = TestTorrent::new("recheck_events", 4096, 1024).await?;
    let torrent = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(
                    MemoryStorageFactory::holding(vec![t.content.clone()]).clone_box(),
                ),
                trackers: Some(vec![tracker.url()]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    assert_eq!(next(&mut heard).await?, announce(Some("started")));
    answered(&torrent).await?;

    // The run uploads one block before the recheck ends it.
    let (dialed, stream) = dials.recv().await.context("no dial")?;
    assert_eq!(dialed, peer_addr);
    let mut peer = MockPeer::greet(stream, torrent.shared().info_hashes).await;
    peer.send(Message::Interested).await;
    peer.send(Message::Request(Request::new(0, 0, 1024))).await;
    peer.next_block().await.context("the seed sent no block")?;

    session.force_recheck(&torrent).await?;
    let mut events = [next(&mut heard).await?, next(&mut heard).await?];
    events.sort();
    assert_eq!(
        events,
        [announce(Some("started")), announce(Some("stopped"))]
    );
    let queries = tracker.queries.lock().clone();
    assert_eq!(queries.len(), 3, "{queries:?}");
    let of = |event: &str| {
        queries[1..]
            .iter()
            .find(|query| param(query, "event") == Some(event))
            .context("no such announce")
    };
    let stopped = of("stopped")?;
    assert_eq!(param(stopped, "uploaded"), Some("1024"), "{stopped}");
    let started = of("started")?;
    assert_eq!(param(started, "uploaded"), Some("0"), "{started}");
    assert_eq!(param(started, "downloaded"), Some("0"), "{started}");
    assert_eq!(param(started, "left"), Some("0"), "{started}");
    drop(tracker);
    session.stop().await;
    Ok(())
}
