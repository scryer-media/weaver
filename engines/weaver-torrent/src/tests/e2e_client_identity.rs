// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The client a torrent names itself as is the client its web seeds hear.

use std::net::Ipv4Addr;

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    AddTorrent, AddTorrentOptions, Identity, NamedBy, Session, SessionOptions, WebSeed,
    WebSeedKind,
    core::peer_id::generate_azereus_style,
    storage::StorageFactory,
    tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
};

/// The User-Agent of the first request the seed at `listener` is sent. It is
/// answered busy, so the torrent asks no more for a while.
async fn first_user_agent(listener: &TcpListener) -> anyhow::Result<Option<String>> {
    let (mut conn, _) = listener.accept().await?;
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = conn.read(&mut chunk).await?;
        anyhow::ensure!(n > 0, "connection closed before the request was read");
        head.extend_from_slice(&chunk[..n]);
    }
    conn.write_all(
        b"HTTP/1.1 503 Busy\r\nRetry-After: 3600\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await?;
    let head = String::from_utf8(head)?;
    Ok(head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("user-agent")
            .then(|| value.trim().to_owned())
    }))
}

// A torrent that names itself as another client asks its web seed as that
// client; one that names no client of its own asks as the session does,
// which by default is this build.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_web_seed_is_asked_as_the_client_the_torrent_names_itself_as() -> anyhow::Result<()>
{
    let emulated = Identity {
        peer_id: generate_azereus_style(*b"qB", (5, 2, 3, 0)),
        tracker_key: 1,
        version: "qBittorrent/5.2.3".into(),
        user_agent: "qBittorrent/5.2.3".into(),
        tracker_ids: Default::default(),
        completion: Default::default(),
        shape: &crate::tracker::qbittorrent::SHAPE,
    };
    for (identity, expected) in [
        (Some(emulated), "qBittorrent/5.2.3"),
        (None, crate::client_name_and_version()),
    ] {
        let t = TestTorrent::new("client_identity", 4096, 1024).await?;
        let seed = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let out = TempDir::with_prefix("client_identity_out")?;
        let session = Session::new_with_opts(
            out.path().into(),
            SessionOptions {
                http_client: Some(crate::test_support::loopback()),
                ..isolated_session_opts(false)
            },
        )
        .await?;
        session
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    web_seeds: vec![WebSeed {
                        url: format!("http://{}/seed/", seed.local_addr()?),
                        kind: WebSeedKind::Url,
                        // The seed listens on loopback.
                        named_by: NamedBy::Operator,
                    }],
                    identity,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?;

        assert_eq!(first_user_agent(&seed).await?.as_deref(), Some(expected));
        session.stop().await;
    }
    Ok(())
}
