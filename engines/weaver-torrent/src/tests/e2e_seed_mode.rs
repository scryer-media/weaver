// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, BF, ManagedTorrent, Session, TorrentStatsState,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

const PIECE_LENGTH: u32 = 1024;
const PIECES: usize = 8;

// A session seeding `t` in seed mode from `storage`, and where it listens.
async fn seeding(
    t: &TestTorrent,
    dir: &TempDir,
    storage: &MemoryStorageFactory,
) -> anyhow::Result<(
    std::sync::Arc<Session>,
    std::sync::Arc<ManagedTorrent>,
    std::net::SocketAddr,
)> {
    let session =
        Session::new_with_opts(dir.path().join("seed"), isolated_session_opts(true)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                seed_mode: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let addr = session.listen_addr().context("the seed is not listening")?;
    Ok((session, handle, addr))
}

// libtorrent's `verified_pieces`, restored on a seed-mode add: the pieces
// that passed before are passed again without a read, and the torrent gives
// back the same set; one naming every piece adds the torrent out of seed
// mode, complete.
#[tokio::test(flavor = "multi_thread")]
async fn test_seed_mode_restores_the_pieces_that_passed_before() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_seed_mode_restored",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let dir = TempDir::with_prefix("test_seed_mode_restored_out")?;
    let verified = |pieces: &[usize]| {
        let mut bits = BF::from_boxed_slice(vec![0u8; PIECES.div_ceil(8)].into_boxed_slice());
        for &piece in pieces {
            bits.set(piece, true);
        }
        bits
    };
    let add = |name: &'static str, set: BF| {
        let t = &t;
        let dir = &dir;
        async move {
            let held = MemoryStorageFactory::holding(vec![t.content.clone()]);
            let session =
                Session::new_with_opts(dir.path().join(name), isolated_session_opts(false)).await?;
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(held.clone_box()),
                        seed_mode: true,
                        seed_verified: Some(set),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            handle.wait_until_completed().await?;
            anyhow::Ok((session, handle, held))
        }
    };

    let (_some, some, held) = add("some", verified(&[1, 4])).await?;
    assert!(some.seed_mode());
    assert_eq!(some.seed_verified(), Some(verified(&[1, 4])));
    assert_eq!(held.log.preads(), 0);

    let every: Vec<usize> = (0..PIECES).collect();
    let (_all, all, held) = add("all", verified(&every)).await?;
    assert!(!all.seed_mode());
    assert_eq!(all.seed_verified(), None);
    assert!(all.stats().finished);
    assert_eq!(held.log.preads(), 0);
    Ok(())
}

// A session fetching `t` from `peer` into `storage`.
async fn leeching(
    t: &TestTorrent,
    dir: &TempDir,
    storage: &MemoryStorageFactory,
    peer: std::net::SocketAddr,
) -> anyhow::Result<(std::sync::Arc<Session>, std::sync::Arc<ManagedTorrent>)> {
    let session =
        Session::new_with_opts(dir.path().join("leech"), isolated_session_opts(false)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_peers: Some(vec![peer]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    Ok((session, handle))
}

// libtorrent `torrent_flags::seed_mode`: the files are taken as complete
// without a read, and each piece is checked as a peer first asks for it.
// Once every piece passed, seed mode ends.
#[tokio::test(flavor = "multi_thread")]
async fn test_seed_mode_seeds_at_once_and_checks_each_piece_as_it_is_asked_for()
-> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_seed_mode_intact",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let dir = TempDir::with_prefix("test_seed_mode_intact_out")?;
    let held = MemoryStorageFactory::holding(vec![t.content.clone()]);
    let (_seeding, seed, addr) = seeding(&t, &dir, &held).await?;

    seed.wait_until_completed().await?;
    assert!(seed.seed_mode());
    assert!(seed.stats().finished);
    assert_eq!(
        held.log.preads(),
        0,
        "seed mode reads nothing before a peer asks"
    );

    let got = MemoryStorageFactory::default();
    let (_leeching, leech) = leeching(&t, &dir, &got, addr).await?;
    leech.wait_until_completed().await?;
    assert!(got.log.file_contents(0) == t.content);
    // Every piece was checked before it was sent, so every one passed.
    assert!(!seed.seed_mode());
    assert!(seed.stats().finished);
    Ok(())
}

// libtorrent `torrent::leave_seed_mode` with `check_files`: the first piece
// that fails its check ends seed mode and every piece is checked again. The
// damaged piece is not sent, and the torrent is no longer complete.
#[tokio::test(flavor = "multi_thread")]
async fn test_seed_mode_piece_that_fails_its_check_ends_seed_mode_and_checks_every_piece()
-> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_seed_mode_damaged",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let dir = TempDir::with_prefix("test_seed_mode_damaged_out")?;
    const DAMAGED: usize = 3;
    let mut content = t.content.clone();
    content[DAMAGED * PIECE_LENGTH as usize] ^= 0xff;
    let held = MemoryStorageFactory::holding(vec![content]);
    let (_seeding, seed, addr) = seeding(&t, &dir, &held).await?;
    seed.wait_until_completed().await?;
    assert!(seed.seed_mode());

    let got = MemoryStorageFactory::default();
    let (_leeching, _leech) = leeching(&t, &dir, &got, addr).await?;

    // The check of every piece that follows finds all but the damaged one.
    let total = (PIECES * PIECE_LENGTH as usize) as u64;
    loop {
        let changed = seed.next_state_change();
        let stats = seed.stats();
        if matches!(stats.state, TorrentStatsState::Live)
            && stats.progress_bytes == total - PIECE_LENGTH as u64
        {
            assert!(!stats.finished);
            break;
        }
        changed.await;
    }
    assert!(!seed.seed_mode());
    assert!(
        !got.log
            .completed_pieces()
            .contains(&u32::try_from(DAMAGED).unwrap()),
        "the damaged piece was not sent"
    );
    Ok(())
}

// One file's bytes, read only. Every read waits at a gate until the test
// opens it, and the gate counts the reads: how many were asked for, how
// many wait at it now, and the most that waited at once.
#[derive(Default)]
struct Gate {
    state: std::sync::Mutex<Gated>,
    changed: std::sync::Condvar,
}

#[derive(Default)]
struct Gated {
    open: bool,
    reads: usize,
    reading: usize,
    most: usize,
}

impl Gate {
    fn open(&self) {
        self.state.lock().unwrap().open = true;
        self.changed.notify_all();
    }

    fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock().unwrap();
        (state.reads, state.reading, state.most)
    }
}

#[derive(Clone)]
struct GatedFactory {
    content: std::sync::Arc<Vec<u8>>,
    gate: std::sync::Arc<Gate>,
    // Told each time a read reaches the gate.
    reached: tokio::sync::mpsc::UnboundedSender<()>,
}

impl StorageFactory for GatedFactory {
    type Storage = GatedFactory;

    fn create(
        &self,
        _shared: &crate::ManagedTorrentShared,
        _metadata: &crate::TorrentMetadata,
    ) -> anyhow::Result<GatedFactory> {
        Ok(self.clone())
    }

    fn clone_box(&self) -> crate::storage::BoxStorageFactory {
        crate::storage::StorageFactoryExt::boxed(self.clone())
    }
}

impl crate::storage::TorrentStorage for GatedFactory {
    fn init(
        &mut self,
        _shared: &crate::ManagedTorrentShared,
        _metadata: &crate::TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, _file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let mut state = self.gate.state.lock().unwrap();
        state.reads += 1;
        state.reading += 1;
        state.most = state.most.max(state.reading);
        let _ = self.reached.send(());
        while !state.open {
            state = self.gate.changed.wait(state).unwrap();
        }
        state.reading -= 1;
        drop(state);
        let start = usize::try_from(offset)?;
        buf.copy_from_slice(
            self.content
                .get(start..start + buf.len())
                .context("read past the end")?,
        );
        Ok(())
    }

    fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("read only")
    }

    fn remove_file(&self, _file_id: usize, _filename: &std::path::Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &std::path::Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn crate::storage::TorrentStorage>> {
        Ok(Box::new(self.clone()))
    }
}

fn piece_of(index: u32) -> anyhow::Result<crate::core::lengths::ValidPieceIndex> {
    crate::core::lengths::Lengths::new(PIECES as u64 * u64::from(PIECE_LENGTH), PIECE_LENGTH)?
        .validate_piece_index(index)
        .context("no such piece")
}

// A session of `permits` blocking permits seeding `t` in seed mode from a
// gated storage, the torrent running.
async fn seeding_gated(
    t: &TestTorrent,
    dir: &TempDir,
    permits: usize,
) -> anyhow::Result<(
    std::sync::Arc<Session>,
    std::sync::Arc<ManagedTorrent>,
    std::sync::Arc<Gate>,
    tokio::sync::mpsc::UnboundedReceiver<()>,
)> {
    let (reached, reaches) = tokio::sync::mpsc::unbounded_channel();
    let gate = std::sync::Arc::new(Gate::default());
    let storage = GatedFactory {
        content: std::sync::Arc::new(t.content.clone()),
        gate: gate.clone(),
        reached,
    };
    let session = Session::new_with_opts(
        dir.path().join("seed"),
        crate::SessionOptions {
            runtime_worker_threads: Some(permits),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                seed_mode: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;
    Ok((session, handle, gate, reaches))
}

// libtorrent `peer_connection::fill_send_buffer` skips a request whose
// piece is `verifying_piece` (peer_connection.cpp:5316, v2.0.15): a piece
// several ask for while it is checked is read and hashed once, in one read
// call, and each of them hears what that check found.
#[tokio::test(flavor = "multi_thread")]
async fn test_seed_mode_piece_asked_for_while_it_is_checked_is_read_once() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_seed_mode_checked_once",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let dir = TempDir::with_prefix("test_seed_mode_checked_once_out")?;
    let (_session, seed, gate, mut reaches) = seeding_gated(&t, &dir, 4).await?;
    let live = seed.live().context("the seed is not live")?;
    let piece = piece_of(3)?;
    let seed_mode = &seed.shared().options.seed;

    // The first peer's check reads the piece, and waits at the gate.
    let first = tokio::spawn({
        let live = live.clone();
        async move { live.serve_seeded(piece).await }
    });
    reaches.recv().await.context("no read came")?;
    // A second peer, and a caller off the runtime, ask for the same piece.
    let second = tokio::spawn({
        let live = live.clone();
        async move { live.serve_seeded(piece).await }
    });
    let third = tokio::task::spawn_blocking({
        let live = live.clone();
        move || live.check_seeded(piece)
    });
    while seed_mode.waiting(piece) < 2 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.counts(), (1, 1, 1), "the piece is read by one check");

    gate.open();
    assert!(first.await??);
    assert!(second.await??);
    assert!(third.await??);
    assert_eq!(gate.counts(), (1, 0, 1), "one read call for the piece");
    assert!(!seed_mode.unchecked(piece));
    assert!(seed.seed_mode());
    Ok(())
}

// The checks of seed-mode pieces that run at once are as many as the
// session's blocking permits, each holding one, however many peers ask:
// six pieces asked for at once on two permits are read two at a time.
#[tokio::test(flavor = "multi_thread")]
async fn test_seed_mode_checks_at_once_are_bounded_by_the_blocking_permits() -> anyhow::Result<()> {
    setup_test_logging();
    const PERMITS: usize = 2;
    const ASKED: u32 = 6;
    let t = TestTorrent::new(
        "test_seed_mode_bounded",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let dir = TempDir::with_prefix("test_seed_mode_bounded_out")?;
    let (_session, seed, gate, mut reaches) = seeding_gated(&t, &dir, PERMITS).await?;
    let live = seed.live().context("the seed is not live")?;
    let permits = live.shared.spawner.semaphore();
    assert_eq!(permits.available_permits(), PERMITS);

    let asked: Vec<_> = (0..ASKED)
        .map(|index| {
            let live = live.clone();
            tokio::spawn(async move { live.serve_seeded(piece_of(index)?).await })
        })
        .collect();
    for _ in 0..PERMITS {
        reaches.recv().await.context("no read came")?;
    }
    // Every permit is held by a check at the gate: no other piece is read
    // until one of them is done.
    assert_eq!(permits.available_permits(), 0);
    assert_eq!(gate.counts(), (PERMITS, PERMITS, PERMITS));

    gate.open();
    for served in asked {
        assert!(served.await??);
    }
    let (reads, reading, most) = gate.counts();
    assert_eq!((reads, reading), (ASKED as usize, 0));
    assert_eq!(most, PERMITS);
    assert!(seed.seed_mode(), "two pieces are not yet checked");
    Ok(())
}
