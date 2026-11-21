// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::BTreeSet;

use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, BF, ManagedTorrentState, Session, TorrentStatsState,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

const PIECE_LENGTH: u32 = 1024;
const PIECES: usize = 8;

fn have_all_but(missing: &[usize]) -> BF {
    let mut have = BF::from_boxed_slice(vec![0xffu8; PIECES.div_ceil(8)].into_boxed_slice());
    for p in missing {
        have.set(*p, false);
    }
    have
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_initial_have_goes_live_without_reading_storage() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_full_initial_have",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let out = TempDir::with_prefix("test_full_initial_have_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(have_all_but(&[]))),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    handle.wait_until_initialized().await?;
    handle.wait_until_completed().await?;

    assert!(handle.with_state(|s| matches!(s, ManagedTorrentState::Live(_))));
    let stats = handle.stats();
    assert!(matches!(stats.state, TorrentStatsState::Live));
    assert!(stats.finished);
    assert_eq!(stats.progress_bytes, (PIECES as u64) * PIECE_LENGTH as u64);
    assert_eq!(
        storage.log.preads(),
        0,
        "the supplied have-bitfield must stand in for the initial check"
    );
    assert!(storage.log.written_pieces().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_initial_have_fetches_exactly_the_missing_pieces() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_partial_initial_have",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let (_seeder, seeder_addr) = t.seed().await?;

    let out = TempDir::with_prefix("test_partial_initial_have_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    let missing = [2usize, 5];

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(have_all_but(&missing))),
                initial_peers: Some(vec![seeder_addr]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;

    let expected: BTreeSet<u32> = missing.iter().map(|p| u32::try_from(*p).unwrap()).collect();
    let completed: BTreeSet<u32> = storage.log.completed_pieces().into_iter().collect();
    let written: BTreeSet<u32> = storage.log.written_pieces().into_iter().collect();
    assert_eq!(completed, expected);
    assert_eq!(written, expected);
    assert_eq!(storage.log.completed_pieces().len(), missing.len());

    let got = storage.log.file_contents(0);
    for p in missing {
        let range = p * PIECE_LENGTH as usize..(p + 1) * PIECE_LENGTH as usize;
        assert!(got[range.clone()] == t.content[range], "piece {p} differs");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_have_of_the_wrong_length_is_refused() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_initial_have_wrong_length",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let out = TempDir::with_prefix("test_initial_have_wrong_length_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;

    for have in [
        BF::from_boxed_slice(vec![0xffu8; 2].into_boxed_slice()),
        BF::from_boxed_slice(Box::new([])),
    ] {
        let storage = MemoryStorageFactory::default();
        let err = match session
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(storage.clone_box()),
                    checkpoint: Some(crate::Checkpoint::pieces(have)),
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(_) => anyhow::bail!("a have-bitfield of the wrong length was accepted"),
            Err(e) => format!("{e:#}"),
        };
        assert!(err.contains("checkpoint"), "unclear error: {err}");
        assert_eq!(storage.log.preads(), 0);
    }
    assert!(
        session.with_torrents(|ts| ts.count()) == 0,
        "a refused torrent must not be added"
    );
    Ok(())
}

// A pause asked for while the torrent is still being checked holds once the
// check ends: the torrent ends paused, not live. The test holds the check at
// its last step, so the pause lands while the torrent is initializing.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_pause_during_the_check_ends_paused() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_pause_during_check",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let out = TempDir::with_prefix("test_pause_during_check_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    let release = storage.log.hold_file_lengths();

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                checkpoint: Some(crate::Checkpoint::pieces(have_all_but(&[]))),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    assert!(handle.with_state(|s| matches!(s, ManagedTorrentState::Initializing(_))));
    session.pause(&handle).await?;
    drop(release);

    handle.wait_until_initialized().await?;
    assert!(
        handle.with_state(|s| matches!(s, ManagedTorrentState::Paused(_))),
        "a torrent paused during its check went live"
    );
    assert!(handle.is_paused());
    Ok(())
}
