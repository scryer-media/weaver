// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    AddTorrent, AddTorrentOptions, AnnounceOk, BF, CreateTorrentOptions, ListenerMode,
    ListenerOptions, Session, SessionOptions, TrackerAnnounce, create_torrent,
    spawn_utils::BlockingSpawner,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::{create_default_random_dir_with_torrents, setup_test_logging},
    },
};

/// The next HTTP request to the tracker, unanswered, and its `event`.
async fn read_announce(listener: &TcpListener) -> anyhow::Result<(TcpStream, Option<String>)> {
    let (conn, query) = read_announce_query(listener).await?;
    let event = param(&query, "event").map(str::to_owned);
    Ok((conn, event))
}

/// One parameter of an announce's query string.
fn param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

/// The next HTTP request to the tracker, unanswered, and its query string.
async fn read_announce_query(listener: &TcpListener) -> anyhow::Result<(TcpStream, String)> {
    let (mut conn, _) = listener.accept().await?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = conn.read(&mut chunk).await?;
        anyhow::ensure!(n > 0, "connection closed before the request was read");
        buf.extend_from_slice(&chunk[..n]);
    }
    let request = String::from_utf8(buf)?;
    let path = request.split(' ').nth(1).context("no request path")?;
    let query = path.split_once('?').context("no query")?.1.to_owned();
    Ok((conn, query))
}

async fn respond(conn: TcpStream, body: &str) -> anyhow::Result<()> {
    respond_bytes(conn, body.as_bytes()).await
}

async fn respond_bytes(mut conn: TcpStream, body: &[u8]) -> anyhow::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    conn.write_all(head.as_bytes()).await?;
    conn.write_all(body).await?;
    Ok(())
}

/// A tracker answer listing one IPv4 peer, compact (BEP 23).
fn answer_with_peer(peer: SocketAddr) -> anyhow::Result<Vec<u8>> {
    let SocketAddr::V4(peer) = peer else {
        anyhow::bail!("{peer} is not IPv4");
    };
    let mut body = b"d8:intervali1800e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    Ok(body)
}

fn tracker_session_opts() -> anyhow::Result<SessionOptions> {
    Ok(SessionOptions {
        disable_trackers: false,
        http_client: Some(std::sync::Arc::new(|| {
            reqwest::Client::builder().no_proxy()
        })),
        ..isolated_session_opts(false)
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn test_resumed_torrent_announces_only_this_runs_bytes() -> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    const PIECES: usize = 10;
    const HAVE: usize = 4;
    let t = TestTorrent::new(
        "test_resumed_announces",
        PIECES * PIECE_LENGTH as usize,
        PIECE_LENGTH,
    )
    .await?;
    let (_seeder, seeder_addr) = t.seed().await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    // Resumed with the first 40 % verified and every file selected.
    let mut have = BF::from_boxed_slice(vec![0u8; PIECES.div_ceil(8)].into_boxed_slice());
    for p in 0..HAVE {
        have.set(p, true);
    }
    let out = TempDir::with_prefix("test_resumed_announces_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                checkpoint: Some(crate::Checkpoint::pieces(have)),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    let missing = ((PIECES - HAVE) * PIECE_LENGTH as usize).to_string();
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "downloaded"), Some("0"));
    assert_eq!(param(&query, "uploaded"), Some("0"));
    assert_eq!(param(&query, "left"), Some(missing.as_str()));
    // The tracker hands out the seeder: only now can anything be fetched.
    respond_bytes(conn, &answer_with_peer(seeder_addr)?).await?;

    // Finishing during the run sends `completed` at once.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("completed"));
    assert_eq!(param(&query, "downloaded"), Some(missing.as_str()));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;

    // The run's counters, as it ended, go out with `stopped`.
    session.pause(&handle).await?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("stopped"));
    assert_eq!(param(&query, "downloaded"), Some(missing.as_str()));
    assert_eq!(param(&query, "left"), Some("0"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

/// The `port` of the `started` announce a session makes for a new torrent.
async fn started_port(opts: SessionOptions) -> anyhow::Result<(String, Option<u16>)> {
    let t = TestTorrent::new("test_announced_port", 2048, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);
    let out = TempDir::with_prefix("test_announced_port_out")?;
    let session = Session::new_with_opts(out.path().into(), opts).await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    let port = param(&query, "port").context("no port")?.to_owned();
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok((port, session.listen_addr().map(|a| a.port())))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_announced_port_is_the_one_the_session_listens_on() -> anyhow::Result<()> {
    setup_test_logging();

    // No listener: port 1, which no peer is listening on, never a made-up one.
    let (port, listening) = started_port(tracker_session_opts()?).await?;
    assert_eq!(listening, None);
    assert_eq!(port, "1");

    // A listener: its port.
    let (port, listening) = started_port(SessionOptions {
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (Ipv4Addr::UNSPECIFIED, 0).into(),
            ..Default::default()
        }),
        ..tracker_session_opts()?
    })
    .await?;
    let listening = listening.context("the session listens")?;
    assert_eq!(port, listening.to_string());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_selection_announces_what_the_whole_torrent_lacks() -> anyhow::Result<()> {
    setup_test_logging();
    const PIECE_LENGTH: u32 = 1024;
    let dir = create_default_random_dir_with_torrents(3, 4 * PIECE_LENGTH as usize, Some("sel"));
    let torrent = create_torrent(
        dir.path(),
        CreateTorrentOptions {
            name: None,
            piece_length: Some(PIECE_LENGTH),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?
    .as_bytes()?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_partial_selection_announces_out")?;
    let session = Session::new_with_opts(out.path().into(), tracker_session_opts()?).await?;
    let _handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                only_files: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;

    // One of three piece-aligned files selected: `left` is still every byte
    // of the torrent, as libtorrent's `torrent::bytes_left` counts it, so
    // finishing the selection never reads as a seed.
    let (conn, query) = read_announce_query(&tracker).await?;
    assert_eq!(param(&query, "event"), Some("started"));
    assert_eq!(param(&query, "downloaded"), Some("0"));
    assert_eq!(
        param(&query, "left"),
        Some((3 * 4 * PIECE_LENGTH).to_string().as_str())
    );
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tracker_announces_follow_the_live_run() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_tracker_announces", 4096, 1024).await?;
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("http://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_tracker_announces_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_trackers: false,
            http_client: Some(std::sync::Arc::new(|| {
                reqwest::Client::builder().no_proxy()
            })),
            ..isolated_session_opts(false)
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url.clone()]),
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    handle.wait_until_initialized().await?;
    // Never run: no tracker task, no table.
    assert_eq!(handle.tracker_announces(), Vec::new());

    // First live run: the tracker is listed, then announced to.
    session.unpause(&handle).await?;
    let first = handle.tracker_status().context("a live run has a table")?;
    let listed = first.announces();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].url, url);
    assert_eq!(listed[0].last, None);

    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    assert!(handle.tracker_announces()[0].in_flight);
    respond(
        conn,
        "d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e",
    )
    .await?;
    first.subscribe().wait_for(|a| a[0].last.is_some()).await?;
    let answered = handle.tracker_announces();
    assert!(!answered[0].in_flight);
    assert_eq!(
        answered[0].last.as_ref().context("answered")?.result,
        Ok(AnnounceOk {
            interval: Duration::from_secs(1800),
            // Named by none, libtorrent's parse_tracker_response default.
            min_interval: Duration::from_secs(30),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: None,
            peers: 0,
            warning: None,
        })
    );

    // Paused: the task ends (it sends `stopped`), the outcome stays readable.
    session.pause(&handle).await?;
    let (conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("stopped"));
    respond(conn, "d8:intervali1800e5:peers0:e").await?;
    assert_eq!(handle.tracker_announces(), answered);

    // Second live run: a new table for the new task.
    session.unpause(&handle).await?;
    let second = handle.tracker_status().context("a live run has a table")?;
    let (_conn, event) = read_announce(&tracker).await?;
    assert_eq!(event.as_deref(), Some("started"));
    assert_eq!(
        handle.tracker_announces(),
        vec![TrackerAnnounce {
            url: url.clone(),
            in_flight: true,
            ..Default::default()
        }]
    );
    assert_eq!(first.announces(), answered);

    // Paused mid-announce: nothing stays in flight.
    session.pause(&handle).await?;
    second.subscribe().wait_for(|a| !a[0].in_flight).await?;
    assert_eq!(
        handle.tracker_announces(),
        vec![TrackerAnnounce {
            url,
            ..Default::default()
        }]
    );
    Ok(())
}
