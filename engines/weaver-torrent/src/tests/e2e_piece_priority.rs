// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, BF, PiecePriority, Session,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

const PIECE_LENGTH: u32 = 1024;
/// The first index past the last piece.
const OUT_OF_RANGE: u32 = 8;
const PIECES: usize = OUT_OF_RANGE as usize;

// Download the two missing pieces from a loopback seeder and return the order the
// pieces were written in. One peer over one connection serves requests in the order
// they were sent, so the write order is the request order.
async fn fetch_two_missing(
    name: &str,
    piece_priorities: Option<Vec<(u32, PiecePriority)>>,
) -> anyhow::Result<Vec<u32>> {
    setup_test_logging();
    let t = TestTorrent::new(name, PIECES * PIECE_LENGTH as usize, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = t.seed().await?;

    let out = TempDir::with_prefix(name)?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    // Pieces 0 and 4 are missing.
    let mut have = BF::from_boxed_slice(vec![0xffu8; PIECES.div_ceil(8)].into_boxed_slice());
    have.set(0, false);
    have.set(4, false);

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_have: Some(have),
                initial_peers: Some(vec![seeder_addr]),
                piece_priorities,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_completed().await?;

    let mut order = storage.log.written_pieces();
    order.dedup();
    Ok(order)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_without_priorities_both_missing_pieces_arrive() -> anyhow::Result<()> {
    // As rare as each other, so the picker takes either first.
    let mut order = fetch_two_missing("test_piece_priority_default", None).await?;
    order.sort();
    assert_eq!(order, vec![0, 4]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_the_high_priority_piece_is_requested_first() -> anyhow::Result<()> {
    let order = fetch_two_missing(
        "test_piece_priority_high",
        Some(vec![(4, PiecePriority::High)]),
    )
    .await?;
    assert_eq!(order, vec![4, 0]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_piece_priorities_out_of_range_are_refused() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_piece_priority_range",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let out = TempDir::with_prefix("test_piece_priority_range_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();

    let refused = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_have: Some(BF::from_boxed_slice(vec![0xffu8].into_boxed_slice())),
                piece_priorities: Some(vec![(OUT_OF_RANGE, PiecePriority::High)]),
                ..Default::default()
            }),
        )
        .await;
    assert!(refused.is_err());

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_have: Some(BF::from_boxed_slice(vec![0xffu8].into_boxed_slice())),
                piece_priorities: Some(vec![(1, PiecePriority::Low)]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    assert_eq!(handle.piece_priority(1), Some(PiecePriority::Low));
    assert_eq!(handle.piece_priority(2), Some(PiecePriority::Normal));

    assert!(
        handle
            .set_piece_priorities(&[
                (2, PiecePriority::Critical),
                (OUT_OF_RANGE, PiecePriority::High)
            ])
            .is_err()
    );
    // Refused as a whole: nothing changed.
    assert_eq!(handle.piece_priority(2), Some(PiecePriority::Normal));

    handle.set_piece_priorities(&[(2, PiecePriority::Critical)])?;
    assert_eq!(handle.piece_priority(2), Some(PiecePriority::Critical));
    assert_eq!(handle.piece_priority(OUT_OF_RANGE), None);
    Ok(())
}
