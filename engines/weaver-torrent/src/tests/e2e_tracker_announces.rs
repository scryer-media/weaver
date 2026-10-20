// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{net::Ipv4Addr, time::Duration};

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    AddTorrent, AddTorrentOptions, AnnounceOk, Session, SessionOptions, TrackerAnnounce,
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
};

/// The next HTTP request to the tracker, unanswered, and its `event`.
async fn read_announce(listener: &TcpListener) -> anyhow::Result<(TcpStream, Option<String>)> {
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
    let event = path
        .split_once('?')
        .context("no query")?
        .1
        .split('&')
        .find_map(|kv| kv.strip_prefix("event="))
        .map(str::to_owned);
    Ok((conn, event))
}

async fn respond(mut conn: TcpStream, body: &str) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    conn.write_all(response.as_bytes()).await?;
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
            http_client: Some(reqwest::Client::builder().no_proxy().build()?),
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
            min_interval: None,
            seeders: Some(3),
            leechers: Some(5),
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
            last: None,
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
            in_flight: false,
            last: None,
        }]
    );
    Ok(())
}
