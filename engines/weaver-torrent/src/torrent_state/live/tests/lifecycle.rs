// SPDX-License-Identifier: GPL-3.0-or-later

use super::*;

// wait_until_completed takes its wait before it checks: a finish rung
// between the check and the await wakes it, where a wait taken after the
// ring sleeps on (tokio `Notify::notify_waiters` wakes only the waits taken
// by then). The window lies inside one poll, so the ring is put there by
// hand on the torrent's own notify.
#[tokio::test(start_paused = true)]
async fn a_finish_between_the_check_and_the_wait_is_not_lost() {
    use futures::FutureExt;
    let h = Harness::new("finish-between").await;
    let mut taken = std::pin::pin!(h.finished_notify.notified());
    taken.as_mut().enable();
    assert!(!h.is_finished());
    h.finished_notify.notify_waiters();
    let after = h.finished_notify.notified();
    assert!(taken.now_or_never().is_some());
    assert!(after.now_or_never().is_none());
}

impl Harness {
    /// A hybrid torrent of `files` live, each file padded to whole pieces,
    /// none of its pieces had, and no peers.
    async fn hybrid(name: &str, files: Vec<(&str, Vec<u8>)>) -> Self {
        use crate::core::v2_test_torrent::TestTorrent as V2TestTorrent;

        let mut torrent = V2TestTorrent::new(files, u64::from(PIECE_LENGTH));
        torrent.hybrid = true;
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let pieces = torrent.v1_payload().len() / PIECE_LENGTH as usize;
        let have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent_bytes()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    checkpoint: Some(crate::Checkpoint::pieces(have)),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        Self {
            live: handle.live().unwrap(),
            content: torrent.v1_payload(),
            _session: session,
            _torrent: None,
            _out: out,
        }
    }

    /// A web seed of the torrent, connected and live with every piece of
    /// the files it serves, the queue of what we send it, and its link,
    /// whose hold keeps the torrent from connecting to the seed itself.
    fn web_seed(&self) -> (PeerHandler, PeerRx, web_seed::SeedLink) {
        use crate::core::torrent_metainfo::{WebSeed, WebSeedKind};

        let seeds = &self.shared.web_seeds;
        assert!(seeds.add(WebSeed {
            url: "http://192.0.2.1/s/".into(),
            kind: WebSeedKind::Url,
            named_by: crate::core::net::NamedBy::Operator,
        }));
        // The torrent's metainfo names no seed: this one is the first.
        let link = seeds.link(0).unwrap();
        let addr = link.handle();
        let (rx, tx) = self.peers.admit_web_seed(addr).unwrap();
        let counters = self
            .peers
            .with_peer(addr, |p| p.stats.counters.clone())
            .unwrap();
        let handler = PeerHandler {
            addr,
            incoming: false,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            let_go_in_upload_mode: AtomicU64::new(0),
            pex_started: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.live.max_request_window())),
            state: self.live.clone(),
            tx,
            answers: Default::default(),
            counters,
            first_message_received: AtomicBool::new(true),
            transport: AtomicU8::new(0),
            disk: Default::default(),
            upload: Default::default(),
            cancel_token: self.live.cancellation_token.child_token(),
            client_name_and_version: String::new(),
        };
        (&handler)
            .on_handshake(handshake(false), ConnectionKind::Tcp, false)
            .unwrap();
        let serves = self.web_layout.bitfield(&Default::default());
        handler.on_bitfield(serves.as_raw_slice()).unwrap();
        (handler, rx, link)
    }

    /// The pieces the peer has, one bool each.
    fn has(&self, peer: &PeerHandler) -> Vec<bool> {
        let total = self.lengths.total_pieces() as usize;
        self.peers
            .with_live(peer.addr, |live| {
                live.bitfield[..total].iter().by_vals().collect()
            })
            .unwrap()
    }
}

// libtorrent web_peer_connection::received_invalid_data: a web seed that
// sent the whole of a piece lying in one file holds another copy of that
// file, so it no longer has the file's pieces, and is not banned while it
// has others (torrent::penalize_peers bans a known bad peer only when its
// connection allows it). A pad file is no part of what the seed serves.
#[tokio::test]
async fn a_web_seed_that_fails_a_piece_loses_its_file_and_serves_the_rest() {
    use crate::core::v2_test_torrent::payload;

    // "a" fills piece 0 and starts piece 1, which a pad file ends; "b" is
    // piece 2.
    let h = Harness::hybrid(
        "web_seed_loses_a_file",
        vec![
            ("a", payload(PIECE_LENGTH as usize + 1000, 1)),
            ("b", payload(PIECE_LENGTH as usize, 2)),
        ],
    )
    .await;
    tokio::time::pause();
    let (seed, mut rx, _link) = h.web_seed();
    assert_eq!(h.has(&seed), [true, true, true]);

    h.take(&seed, 1);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (1, chunk), chunk == 0).await.unwrap();
    }

    assert_eq!(h.missing_chunks(1), vec![0, 1, 2, 3]);
    assert_eq!(h.trust(&seed), (-2, 1, true));
    assert!(!h.banned(&seed) && !told_to_disconnect(&mut rx));
    assert_eq!(h.has(&seed), [false, false, true]);
    assert_eq!(h.shared.web_seeds.list().len(), 1);

    // It goes on serving "b", and is asked for nothing of "a": what is
    // left to take of it is piece 2, unless taking piece 1 took it first.
    let left: Vec<u32> = std::iter::from_fn(|| seed.acquire_next_piece().unwrap().piece())
        .map(|p| p.get())
        .collect();
    assert!(left.iter().all(|&p| p == 2), "{left:?}");
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (2, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(2).is_empty());
    assert_eq!(h.trust(&seed), (-1, 1, false));
}

// libtorrent web_peer_connection::received_invalid_data: the seed of a
// single-file torrent that sends a bad piece has nothing left to serve,
// and is banned as any peer that sent a whole bad piece is.
#[tokio::test]
async fn the_web_seed_of_a_single_file_torrent_that_fails_a_piece_is_banned() {
    let h = Harness::new("web_seed_single_file_banned").await;
    tokio::time::pause();
    let (seed, mut rx, link) = h.web_seed();
    assert_eq!(h.has(&seed), [true, true]);

    h.take(&seed, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&seed, (0, chunk), chunk == 2).await.unwrap();
    }

    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert!(h.banned(&seed));
    assert!(told_to_disconnect(&mut rx));
    // The ban is the seed's, on this torrent alone: no address is banned
    // for the session, and none is saved for a restart.
    assert!(h._session.bad_data_bans.is_empty());
    assert!(h.peers.resume_peers().banned.is_empty());
    assert!(h.shared.web_seeds.list().is_empty());
    drop(link);
    let due = h.shared.web_seeds.due(
        tokio::time::Instant::now(),
        |lacks| h.web_layout.bitfield(lacks).any(),
        false,
    );
    assert!(due.is_empty());
}

// libtorrent `torrent::write_resume_data` records the finished blocks of each
// downloading piece (`unfinished_pieces`), and `torrent::read_resume_data`
// marks them finished again (`piece_picker::mark_as_finished`): a restarted
// torrent asks for none of them, and the piece's hash is checked once the
// rest arrive.
#[tokio::test]
async fn a_checkpoint_keeps_the_written_blocks_of_a_piece_across_a_restart() {
    let storage = MemoryStorageFactory::default();
    let add = || AddTorrentOptions {
        storage_factory: Some(storage.clone_box()),
        ..Default::default()
    };
    // Piece 1 had: piece 0 is the one fetched.
    let h = Harness::build_added(
        "checkpoint_keeps_written_blocks",
        PIECES,
        0x40,
        isolated_session_opts(false),
        add(),
    )
    .await;
    tokio::time::pause();
    {
        let (peer, mut rx) = h.peer("127.0.0.1:6881");
        let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());
        peer.on_i_am_unchoked();
        assert_eq!(
            next_requests(&mut requester, &mut rx, CHUNKS_PER_PIECE as usize).await,
            chunks(0, 0..CHUNKS_PER_PIECE)
        );
        h.deliver(&peer, (0, 0)).await;
        h.deliver(&peer, (0, CHUNK_SIZE)).await;
    }

    let checkpoint = h.handle().checkpoint().unwrap();
    assert_eq!(checkpoint.have.as_raw_slice(), &[0x40]);
    let blocks = checkpoint.blocks.as_ref().unwrap();
    assert_eq!(
        blocks.iter_ones().collect::<Vec<_>>(),
        [0, 1, 4, 5, 6, 7],
        "the two blocks written of piece 0, and every block of piece 1"
    );
    assert_eq!(storage.log.written_pieces(), [0, 0]);

    let h = h.restarted(checkpoint, add()).await;
    assert_eq!(h.missing_chunks(0), [2, 3]);
    let (peer, mut rx) = h.peer("127.0.0.2:6881");
    let mut requester: Task<'_> = Box::pin(peer.task_peer_chunk_requester());
    peer.on_i_am_unchoked();
    assert_eq!(
        next_requests(&mut requester, &mut rx, 2).await,
        chunks(0, 2..CHUNKS_PER_PIECE)
    );
    assert_eq!(h.inflight_count(&peer), 2);
    h.deliver(&peer, (0, 2 * CHUNK_SIZE)).await;
    h.deliver(&peer, (0, 3 * CHUNK_SIZE)).await;

    assert!(
        h.lock_read("test")
            .get_chunks()
            .unwrap()
            .is_piece_have(h.piece(0))
    );
    assert_eq!(storage.log.completed_pieces(), [0]);
    assert_eq!(
        storage.log.written_pieces(),
        [0, 0, 0, 0],
        "no block was fetched twice"
    );
}

// libtorrent `torrent::write_resume_data` saves the peers and the bans, and
// `torrent::read_resume_data` takes them back: each peer through
// `torrent::add_peer` as `peer_info::resume_data`, each ban through
// `peer_list::ban_peer`. A restarted torrent gets its saved peers as its
// initial peers and its saved bans as its banned addresses.
#[tokio::test]
async fn saved_peers_and_bans_come_back_as_initial_peers_and_bans() {
    use futures::StreamExt;

    let h = Harness::seeding("saved_peers_come_back").await;
    // Connected to us over uTP: we can connect back to where it is.
    let connectable: SocketAddr = "127.0.0.3:6881".parse().unwrap();
    let (tx, _rx) = unbounded_channel();
    h.peers
        .add_incoming(connectable, Id20::default(), tx, ConnectionKind::Utp, 6881)
        .unwrap();
    let banned: SocketAddr = "127.0.0.4:6881".parse().unwrap();
    h.peers.ban(banned.ip());
    let saved = h.handle().resume_peers().unwrap();
    assert_eq!(
        saved,
        crate::ResumePeers {
            peers: vec![connectable],
            banned: vec![banned.ip()],
        }
    );

    // Restarted paused, so no peer is connected to.
    let session = h._session.clone();
    let bytes = h._torrent.as_ref().unwrap().torrent.clone();
    session
        .delete(crate::api::TorrentIdOrHash::Id(h.shared.id), false)
        .await
        .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                paused: true,
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(BF::from_boxed_slice(
                    vec![0xc0].into_boxed_slice(),
                ))),
                initial_peers: Some(saved.peers.clone()),
                banned_peers: saved.banned.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let named: Vec<_> = session
        .make_peer_rx_managed_torrent(&handle, false)
        .unwrap()
        .take(saved.peers.len())
        .collect()
        .await;
    assert_eq!(named, [(connectable, PeerSourceFlag::ResumeData)]);
    // Saved again before it runs, the torrent keeps both.
    assert_eq!(handle.resume_peers().unwrap(), saved);
    let paused_list = handle.with_state(|s| match s {
        crate::ManagedTorrentState::Paused(p) => p.peers.resume_peers(),
        _ => panic!("the restarted torrent is not paused"),
    });
    assert_eq!(paused_list, saved);
}

// libtorrent keeps a torrent's peer list in the torrent itself through every
// state, so `torrent::write_resume_data` reads it whenever it is asked. The
// list stays with a torrent being checked and moves on only with the change
// of state: a save between the end of the check and that change reads the
// list, not an empty one.
#[tokio::test]
async fn a_checked_torrent_keeps_its_peer_list_until_its_state_changes() {
    let h = Harness::seeding("peers_through_the_check").await;
    let session = h._session.clone();
    let bytes = h._torrent.as_ref().unwrap().torrent.clone();
    session
        .delete(crate::api::TorrentIdOrHash::Id(h.shared.id), false)
        .await
        .unwrap();
    let saved = crate::ResumePeers {
        peers: vec!["127.0.0.3:6881".parse().unwrap()],
        banned: vec!["127.0.0.4".parse().unwrap()],
    };
    // Every check waits here, so the session's own check of the torrent
    // does not start; the test runs the check itself.
    let semaphore = session.concurrent_initialize_semaphore.clone();
    let permits = u32::try_from(semaphore.available_permits()).unwrap();
    let _held = semaphore.acquire_many_owned(permits).await.unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                paused: true,
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(BF::from_boxed_slice(
                    vec![0xc0].into_boxed_slice(),
                ))),
                initial_peers: Some(saved.peers.clone()),
                banned_peers: saved.banned.clone(),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    let init = handle.with_state(|s| match s {
        crate::ManagedTorrentState::Initializing(init) => init.clone(),
        _ => panic!("the added torrent is not being checked"),
    });

    let paused = init.check(&|| {}).await.unwrap();
    assert_eq!(handle.resume_peers().unwrap(), saved);
    drop(paused);
}

// Idle torrents cost the session's rate tick nothing: with `n` torrents
// and no peer, the tick samples none however long the clock runs; once one
// torrent has a live peer, the tick samples that torrent alone (libtorrent
// `session_impl::on_tick` visits `m_torrent_lists[torrent_want_tick]` only).
async fn idle_torrents_cost_the_rate_tick_nothing(n: usize) {
    let h = Harness::new("idle_rate_tick").await;
    let mut idle = Vec::with_capacity(n);
    for i in 0..n {
        idle.push(h.sibling(&format!("idle_rate_tick_{i}"), 0x00).await);
    }
    let tick = h._session.rate_tick.clone();
    tokio::time::pause();
    for _ in 0..50 {
        tokio::time::advance(peer::RATE_SAMPLE_INTERVAL).await;
    }
    assert_eq!(tick.samples(), 0);
    assert_eq!(tick.ticking(), 0);

    let (_peer, _rx) = h.peer("127.0.0.1:6881");
    while tick.samples() < 10 {
        tokio::time::advance(peer::RATE_SAMPLE_INTERVAL).await;
    }
    assert_eq!(tick.ticking(), 1);
    assert!(h.live.rate_ticked.load(Ordering::SeqCst));
    assert!(
        idle.iter()
            .all(|t| !t.live.rate_ticked.load(Ordering::SeqCst))
    );
}

#[tokio::test]
async fn a_hundred_idle_torrents_cost_the_rate_tick_nothing() {
    idle_torrents_cost_the_rate_tick_nothing(100).await;
}

// The same at scale, which takes half a minute: run on request.
#[tokio::test]
#[ignore = "scale: builds ten thousand torrents"]
async fn ten_thousand_idle_torrents_cost_the_rate_tick_nothing() {
    idle_torrents_cost_the_rate_tick_nothing(10_000).await;
}

// A watch on what the run uploads is rung once a write that carried block
// bytes is written: the woken waiter reads the write's bytes in the run's
// total and the write's stamp. Blocks told one by one move neither until
// their write is. A torrent nobody watches has no listener to ring.
#[tokio::test]
async fn an_upload_watch_is_rung_by_a_written_batch_and_reads_its_stamp() {
    use crate::peer_connection::PeerConnectionHandler;
    let h = Harness::seeding("upload_watch").await;
    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let watch = h.live.upload_watch();
    assert_eq!(h.upload_watches.load(Ordering::SeqCst), 1);
    let seen = watch.uploaded();
    assert_eq!(watch.last_uploaded_at(), None);

    let before = tokio::time::Instant::now();
    {
        let mut rung = std::pin::pin!(watch.changed_from(seen));
        assert!(futures::poll!(rung.as_mut()).is_pending());
        (&peer).on_uploaded_bytes(CHUNK_SIZE);
        (&peer).on_uploaded_bytes(CHUNK_SIZE);
        assert!(futures::poll!(rung.as_mut()).is_pending());
        assert_eq!(watch.last_uploaded_at(), None);
        (&peer).on_payload_written(2 * u64::from(CHUNK_SIZE));
        rung.await;
    }
    let after = tokio::time::Instant::now();

    assert_eq!(watch.uploaded(), seen + 2 * u64::from(CHUNK_SIZE));
    let stamp = watch.last_uploaded_at().unwrap();
    assert!(
        before <= stamp && stamp <= after,
        "{before:?} {stamp:?} {after:?}"
    );
    drop(watch);
    assert_eq!(h.upload_watches.load(Ordering::SeqCst), 0);
}

// The checks of the last two pieces end together. Each is marked and
// counted in one hold of the lock, so the torrent finishes with the second
// and rings once, and whoever sees it finished sees both pieces counted,
// in the totals and in the files.
#[tokio::test]
async fn pieces_that_finish_together_finish_the_torrent_once() {
    use futures::FutureExt;
    let h = Harness::build("finish_once", 0x00, 1).await;
    let mut rung = std::pin::pin!(h.live.finished_notify.notified());
    rung.as_mut().enable();
    h.live.on_piece_completed(h.piece(0)).unwrap();
    assert!(rung.as_mut().now_or_never().is_none());
    h.live.on_piece_completed(h.piece(1)).unwrap();
    assert!(rung.now_or_never().is_some());

    let had: u64 = [0, 1]
        .map(|piece| u64::from(h.live.lengths.piece_length(h.piece(piece))))
        .iter()
        .sum();
    assert_eq!(h.live.get_downloaded_bytes(), had);
    assert_eq!(h.live.stats_snapshot().downloaded_and_checked_pieces, 2);
    let in_files: u64 = h
        .lock_read("test")
        .get_chunks()
        .unwrap()
        .per_file_have_bytes()
        .iter()
        .sum();
    assert_eq!(in_files, had);
}

/// Whether `f` rings the torrent's finish.
fn rings_the_finish(h: &Harness, f: impl FnOnce()) -> bool {
    use futures::FutureExt;
    let mut rung = std::pin::pin!(h.live.finished_notify.notified());
    rung.as_mut().enable();
    f();
    rung.now_or_never().is_some()
}

// The one piece the torrent lacks is wanted no longer: that finishes the
// torrent as its last piece had does, so the finish rings and whoever
// waits for the torrent to complete is woken.
#[tokio::test]
async fn a_skip_that_finishes_the_torrent_rings_the_finish() {
    use futures::FutureExt;
    let h = Harness::build("finish_by_skip", 0x00, 1).await;
    let handle = h.handle();
    let mut waiter = std::pin::pin!(handle.wait_until_completed());
    assert!(waiter.as_mut().now_or_never().is_none());
    assert!(!rings_the_finish(&h, || {
        h.live.on_piece_completed(h.piece(0)).unwrap()
    }));
    assert!(waiter.as_mut().now_or_never().is_none());
    assert!(rings_the_finish(&h, || {
        h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
            .unwrap()
    }));
    assert!(h.live.is_finished());
    assert!(waiter.now_or_never().is_some());
}

// A piece skipped while its check was in flight is had after the torrent
// finished: the finish, rung by the last wanted piece, is not rung again.
// The torrent wanting a piece again and then having it finishes again.
#[tokio::test]
async fn the_finish_rings_once_for_each_time_the_torrent_finishes() {
    let h = Harness::build("finish_each_time", 0x00, 1).await;
    assert!(!rings_the_finish(&h, || {
        h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
            .unwrap()
    }));
    assert!(rings_the_finish(&h, || {
        h.live.on_piece_completed(h.piece(0)).unwrap()
    }));
    assert!(!rings_the_finish(&h, || {
        h.live.on_piece_completed(h.piece(1)).unwrap()
    }));
    // Skipping what is had, or wanting it again, finishes nothing anew.
    assert!(!rings_the_finish(&h, || {
        h.set_piece_priorities([(h.piece(1), PiecePriority::Normal)])
            .unwrap()
    }));
}

// A finished torrent that wants a piece again is finished no longer, and
// rings when that piece is had.
#[tokio::test]
async fn a_torrent_that_wants_a_piece_again_finishes_again() {
    let h = Harness::build("finish_again", 0x00, 1).await;
    h.set_piece_priorities([(h.piece(1), PiecePriority::Skip)])
        .unwrap();
    assert!(rings_the_finish(&h, || {
        h.live.on_piece_completed(h.piece(0)).unwrap()
    }));
    assert!(!rings_the_finish(&h, || {
        h.set_piece_priorities([(h.piece(1), PiecePriority::Normal)])
            .unwrap()
    }));
    assert!(!h.live.is_finished());
    assert!(rings_the_finish(&h, || {
        h.live.on_piece_completed(h.piece(1)).unwrap()
    }));
}
