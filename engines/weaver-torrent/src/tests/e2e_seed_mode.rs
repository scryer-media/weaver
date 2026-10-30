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
        !got.log.completed_pieces().contains(&(DAMAGED as u32)),
        "the damaged piece was not sent"
    );
    Ok(())
}
