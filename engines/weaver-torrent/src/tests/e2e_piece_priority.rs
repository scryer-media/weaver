// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::net::Ipv4Addr;

use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, BF, PiecePriority, Session,
    core::torrent_metainfo::torrent_from_bytes,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
    torrent_state::live::peer::{ALLOWED_FAST_SET_SIZE, allowed_fast_set},
};

const PIECE_LENGTH: u32 = 1024;
/// The first index past the last piece.
const OUT_OF_RANGE: u32 = 8;
const PIECES: usize = OUT_OF_RANGE as usize;

// The two pieces the seeder does not let a choked leecher fetch (BEP 6 allowed
// fast). Missing only these, the leecher waits for the unchoke and then asks in
// the picker's order alone.
fn two_not_allowed_fast(t: &TestTorrent) -> anyhow::Result<[u32; 2]> {
    let hashes = torrent_from_bytes(&t.torrent)?.info_hashes;
    // Both ends are Weaver, so a torrent with a v2 hash is spoken as v2.
    let info_hash = hashes.for_peer(hashes.v2.is_some());
    let allowed = allowed_fast_set(
        Ipv4Addr::LOCALHOST.into(),
        info_hash,
        OUT_OF_RANGE,
        ALLOWED_FAST_SET_SIZE,
    );
    let mut rest = (0..OUT_OF_RANGE).filter(|p| !allowed.contains(p));
    Ok([
        rest.next().context("a piece outside the set")?,
        rest.next().context("a second piece outside the set")?,
    ])
}

// Download the two missing pieces from a loopback seeder and return them with the
// order they were written in, `priority` set on the second. One peer over one
// connection serves requests in the order they were sent, so the write order is the
// request order.
async fn fetch_two_missing(
    name: &str,
    priority: Option<PiecePriority>,
) -> anyhow::Result<([u32; 2], Vec<u32>)> {
    setup_test_logging();
    let t = TestTorrent::new(name, PIECES * PIECE_LENGTH as usize, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let missing = two_not_allowed_fast(&t)?;
    let piece_priorities = priority.map(|priority| vec![(missing[1], priority)]);

    let out = TempDir::with_prefix(name)?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    let mut have = BF::from_boxed_slice(vec![0xffu8; PIECES.div_ceil(8)].into_boxed_slice());
    for piece in missing {
        have.set(piece as usize, false);
    }

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
    Ok((missing, order))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_without_priorities_both_missing_pieces_arrive() -> anyhow::Result<()> {
    // As rare as each other, so the picker takes either first.
    let (missing, mut order) = fetch_two_missing("test_piece_priority_default", None).await?;
    order.sort();
    assert_eq!(order, missing);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_the_high_priority_piece_is_requested_first() -> anyhow::Result<()> {
    let (missing, order) =
        fetch_two_missing("test_piece_priority_high", Some(PiecePriority::High)).await?;
    assert_eq!(order, vec![missing[1], missing[0]]);
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
