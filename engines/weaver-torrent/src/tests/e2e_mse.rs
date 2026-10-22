// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Message stream encryption between two sessions on loopback.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use crate::core::hash_id::Id20;
use crate::peer_protocol::Handshake;
use anyhow::Context;
use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, EncryptionPolicy, Error, MseError, PeerConnectionOptions,
    Session, SessionOptions,
    storage::StorageFactory,
    stream_connect::{EncryptionStatsSnapshot, Transports},
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

const PIECE_LENGTH: u32 = 16384;
const FILE_SIZE: usize = 8 * PIECE_LENGTH as usize + 1000;

fn opts(listen: bool, encryption: EncryptionPolicy) -> SessionOptions {
    SessionOptions {
        encryption,
        ..isolated_session_opts(listen)
    }
}

/// Seed `t` from its directory with `encryption`; returns the session and its address.
async fn seed(
    t: &TestTorrent,
    encryption: EncryptionPolicy,
) -> anyhow::Result<(Arc<Session>, SocketAddr)> {
    let session = Session::new_with_opts(t.dir.path().into(), opts(true, encryption)).await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(t.dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?
        .wait_until_completed()
        .await?;
    let addr = session.listen_addr().context("seeder is not listening")?;
    Ok((session, addr))
}

struct Downloader {
    session: Arc<Session>,
    handle: crate::torrent_state::ManagedTorrentHandle,
    storage: MemoryStorageFactory,
    _out: TempDir,
}

async fn download_from(
    t: &TestTorrent,
    seeder: SocketAddr,
    encryption: EncryptionPolicy,
) -> anyhow::Result<Downloader> {
    download_under(t, seeder, encryption, None).await
}

/// [`download_from`], the torrent's connections under `torrent` over the
/// session's `encryption`.
async fn download_under(
    t: &TestTorrent,
    seeder: SocketAddr,
    encryption: EncryptionPolicy,
    torrent: Option<EncryptionPolicy>,
) -> anyhow::Result<Downloader> {
    let out = TempDir::with_prefix("test_mse_out")?;
    let session = Session::new_with_opts(out.path().into(), opts(false, encryption)).await?;
    let storage = MemoryStorageFactory::default();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_peers: Some(vec![seeder]),
                peer_opts: Some(PeerConnectionOptions {
                    encryption: torrent,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    Ok(Downloader {
        session,
        handle,
        storage,
        _out: out,
    })
}

fn encryption_stats(session: &Session) -> EncryptionStatsSnapshot {
    session.connector.stats().encryption.snapshot()
}

/// Wait until the downloader has seen an encrypted handshake fail.
async fn wait_for_handshake_failure(session: &Session) {
    while encryption_stats(session).failures == 0 {
        tokio::task::yield_now().await;
    }
}

async fn assert_downloads_encrypted(
    downloader_policy: EncryptionPolicy,
    seeder_policy: EncryptionPolicy,
) -> anyhow::Result<()> {
    let t = TestTorrent::new("test_mse_encrypted", FILE_SIZE, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = seed(&t, seeder_policy).await?;
    let d = download_from(&t, seeder_addr, downloader_policy).await?;
    d.handle.wait_until_completed().await?;
    assert!(
        d.storage.log.file_contents(0) == t.content,
        "content differs"
    );

    let stats = encryption_stats(&d.session);
    assert!(stats.rc4 >= 1, "{stats:?}");
    assert_eq!(stats.failures, 0, "{stats:?}");
    assert_eq!(stats.plaintext, 0, "{stats:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_require_downloader_from_prefer_seeder_is_encrypted() -> anyhow::Result<()> {
    setup_test_logging();
    assert_downloads_encrypted(EncryptionPolicy::Require, EncryptionPolicy::Prefer).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_prefer_downloader_from_require_seeder_is_encrypted() -> anyhow::Result<()> {
    setup_test_logging();
    assert_downloads_encrypted(EncryptionPolicy::Prefer, EncryptionPolicy::Require).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_require_downloader_against_disabled_seeder_is_refused() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_mse_require_vs_disabled", FILE_SIZE, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = seed(&t, EncryptionPolicy::Disabled).await?;
    let d = download_from(&t, seeder_addr, EncryptionPolicy::Require).await?;

    wait_for_handshake_failure(&d.session).await;
    let stats = encryption_stats(&d.session);
    assert_eq!(stats.rc4, 0, "{stats:?}");
    assert_eq!(stats.header_only, 0, "{stats:?}");
    assert_eq!(stats.plaintext, 0, "{stats:?}");
    assert_eq!(d.handle.stats().progress_bytes, 0);
    assert!(d.storage.log.written_pieces().is_empty());

    // The same connection attempt, made directly, says why: the seeder hung
    // up on the key exchange.
    let info_hash: Id20 = d.handle.info_hash();
    let mut hs = vec![0u8; 68];
    let len = Handshake::new(info_hash, Id20::new([9; 20])).serialize_unchecked_len(&mut hs);
    let err = d
        .session
        .connector
        .connect_and_send_handshake(
            seeder_addr,
            Transports::Any,
            d.session.connector.encryption(),
            &info_hash,
            &hs[..len],
            || unreachable!("encryption is required, so nothing is asked"),
            Duration::MAX,
            Duration::MAX,
        )
        .await
        .err()
        .context("expected the encrypted handshake to be refused")?;
    assert!(
        matches!(err, Error::Mse(MseError::Closed { .. })),
        "unexpected error: {err:#}"
    );
    Ok(())
}

// libtorrent `bt_peer_connection::on_connected` and
// `peer_connection::fast_reconnect`: the encrypted handshake the seeder
// refuses ends that connection, and the next one, made at once, tries
// plaintext.
#[tokio::test(flavor = "multi_thread")]
async fn test_prefer_downloader_tries_plaintext_against_disabled_seeder() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_mse_prefer_vs_disabled", FILE_SIZE, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = seed(&t, EncryptionPolicy::Disabled).await?;
    let d = download_from(&t, seeder_addr, EncryptionPolicy::Prefer).await?;
    d.handle.wait_until_completed().await?;
    assert!(
        d.storage.log.file_contents(0) == t.content,
        "content differs"
    );

    let stats = encryption_stats(&d.session);
    assert_eq!(stats.rc4, 0, "{stats:?}");
    assert!(stats.failures >= 1, "{stats:?}");
    assert!(stats.plaintext >= 1, "{stats:?}");
    Ok(())
}

// A torrent's own encryption is the one its connections are made under,
// over the session's: one that requires it downloads encrypted in a session
// that would send plaintext.
#[tokio::test(flavor = "multi_thread")]
async fn test_require_torrent_in_disabled_session_downloads_encrypted() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_mse_torrent_require", FILE_SIZE, PIECE_LENGTH).await?;
    let (_seeder, seeder_addr) = seed(&t, EncryptionPolicy::Prefer).await?;
    let d = download_under(
        &t,
        seeder_addr,
        EncryptionPolicy::Disabled,
        Some(EncryptionPolicy::Require),
    )
    .await?;
    d.handle.wait_until_completed().await?;
    assert!(
        d.storage.log.file_contents(0) == t.content,
        "content differs"
    );
    let stats = encryption_stats(&d.session);
    assert!(stats.rc4 >= 1, "{stats:?}");
    assert_eq!(stats.plaintext, 0, "{stats:?}");
    Ok(())
}
