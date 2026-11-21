// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::num::NonZeroU32;

use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, Session,
    limits::LimitsConfig,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

// One chunk per piece, so the low cap below lets one chunk through per second.
const PIECE_LENGTH: u32 = 1024;
const PIECES: usize = 64;
const TOTAL: u64 = PIECES as u64 * PIECE_LENGTH as u64;
// Chunks' worth the leecher must have pulled through the low cap before it is
// raised. The limiter has no burst: every byte of them waited for a round.
const CHUNKS_UNDER_LOW_CAP: u64 = 3;

#[tokio::test(flavor = "multi_thread")]
async fn test_download_cap_changes_while_the_torrent_runs() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new(
        "test_ratelimits",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let (_seeder, seeder_addr) = t.seed().await?;

    let out = TempDir::with_prefix("test_ratelimits_out")?;
    let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
    let storage = MemoryStorageFactory::default();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_peers: Some(vec![seeder_addr]),
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    assert_eq!(handle.ratelimits(), LimitsConfig::default());

    // Set while paused: the live run started by unpause must pick it up.
    let low = LimitsConfig {
        upload_bps: Default::default(),
        download_bps: NonZeroU32::new(PIECE_LENGTH).into(),
    };
    session.update_ratelimits(&handle, low).await?;
    assert_eq!(handle.ratelimits(), low);

    let limits = &handle.shared.options.ratelimits;
    let mut granted = limits.watch_download_granted();
    session.unpause(&handle).await?;

    granted
        .wait_for(|g| *g >= CHUNKS_UNDER_LOW_CAP * u64::from(PIECE_LENGTH))
        .await?;
    // Every byte the leecher has requested went through the torrent's limiter.
    let progress = handle.stats().progress_bytes;
    let through_limiter = *granted.borrow();
    assert!(
        progress <= through_limiter,
        "{progress} bytes arrived but the limiter let only {through_limiter} through"
    );
    assert_eq!(limits.get_download_bps(), low.download_bps);

    // Raise the cap on the running torrent: same live state, same peer connection.
    let high = LimitsConfig {
        upload_bps: Default::default(),
        download_bps: NonZeroU32::new(1 << 30).into(),
    };
    session.update_ratelimits(&handle, high).await?;
    assert_eq!(handle.ratelimits(), high);
    handle.wait_until_completed().await?;

    assert!(*granted.borrow() >= TOTAL);
    assert_eq!(handle.stats().progress_bytes, TOTAL);
    assert_eq!(storage.log.file_contents(0), t.content);
    assert_eq!(handle.ratelimits(), high);
    Ok(())
}
