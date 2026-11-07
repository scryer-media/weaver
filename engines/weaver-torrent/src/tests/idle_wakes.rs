// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session of idle torrents wakes for, counted on the paused clock
//! one second at a time. A wake is a poll of a task the engine's spawn
//! helper started, counted by the task's name; a task started with
//! `tokio::spawn` itself is not counted.

use std::{collections::BTreeMap, net::Ipv4Addr, path::Path, sync::Arc, time::Duration};

use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, FilePriority, IpFilter, IpRanges,
    ManagedTorrent, Session, SessionOptions,
    core::{
        spawn_utils::polls,
        torrent_metainfo::{WebSeed, WebSeedKind},
    },
    create_torrent,
    spawn_utils::BlockingSpawner,
    tests::embedder_util::isolated_session_opts,
};

const TORRENTS: usize = 1000;
const SECONDS: u32 = 300;

// The session's own timers over SECONDS: the speed estimator each second,
// the choker each UNCHOKE_INTERVAL, the peer turnover each
// PEER_TURNOVER_INTERVAL.
fn the_sessions_timers() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("choker".to_owned(), 20),
        ("peer_turnover".to_owned(), 1),
        ("speed_estimator".to_owned(), 300),
    ])
}

// A web seed on loopback, which the session's filter keeps it from asking.
fn web_seed() -> WebSeed {
    WebSeed::new(b"http://127.0.0.1:9/", WebSeedKind::Url, false)
}

// A session of no DHT, trackers, LSD or listener, whose filter blocks
// loopback, so a web seed due is never asked.
async fn session(dir: &Path) -> Arc<Session> {
    Session::new_with_opts(
        dir.join("session"),
        SessionOptions {
            ip_filter: IpFilter {
                blocked: Arc::new(IpRanges::new(
                    std::iter::once(Ipv4Addr::LOCALHOST..Ipv4Addr::new(127, 0, 0, 2)),
                    [],
                )),
                ..Default::default()
            },
            ..isolated_session_opts(false)
        },
    )
    .await
    .unwrap()
}

async fn torrent_of(path: &Path) -> bytes::Bytes {
    create_torrent(
        path,
        CreateTorrentOptions {
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap()
}

async fn add(
    session: &Arc<Session>,
    torrent: bytes::Bytes,
    out: &Path,
    opts: AddTorrentOptions,
) -> Arc<ManagedTorrent> {
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(out.to_str().unwrap().to_owned()),
                overwrite: true,
                ..opts
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap()
}

// The wakes in each of `seconds` seconds from a half second on, which no
// whole-second timer started at a whole second lands on, and the wakes
// over all of them by task.
async fn count(seconds: u32) -> (Vec<u64>, BTreeMap<String, u64>) {
    let start = tokio::time::Instant::now() + Duration::from_millis(500);
    tokio::time::sleep_until(start).await;
    let first = polls::counted();
    let mut before = first.clone();
    let mut each = Vec::new();
    for second in 1..=seconds {
        tokio::time::sleep_until(start + Duration::from_secs(second.into())).await;
        let now = polls::counted();
        each.push(woken(&before, &now).values().sum());
        before = now;
    }
    (each, woken(&first, &before))
}

fn woken(before: &BTreeMap<String, u64>, after: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    after
        .iter()
        .map(|(task, n)| (task.clone(), n - before.get(task).copied().unwrap_or(0)))
        .filter(|(_, n)| *n > 0)
        .collect()
}

// A thousand finished torrents with no peers, each with a web seed or not,
// counted once whatever the adds started is past.
async fn idle_wakes(with_web_seed: bool) -> (Vec<u64>, BTreeMap<String, u64>) {
    let dir = TempDir::with_prefix("idle_wakes").unwrap();
    let session = session(dir.path()).await;
    let mut handles = Vec::with_capacity(TORRENTS);
    for i in 0..TORRENTS {
        let file = dir.path().join(format!("{i}.data"));
        std::fs::write(&file, format!("idle torrent {i}").repeat(64)).unwrap();
        let opts = AddTorrentOptions {
            web_seeds: if with_web_seed {
                vec![web_seed()]
            } else {
                Vec::new()
            },
            ..Default::default()
        };
        handles.push(add(&session, torrent_of(&file).await, dir.path(), opts).await);
    }
    for handle in &handles {
        handle.wait_until_completed().await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    let counted = count(SECONDS).await;
    drop(handles);
    session.stop().await;
    counted
}

// A thousand finished torrents with no peers and no web seed wake for the
// session's timers only, as one torrent would. No torrent wakes.
#[tokio::test(start_paused = true)]
async fn a_thousand_idle_finished_torrents_wake_only_for_the_sessions_timers() {
    let (seconds, by_task) = idle_wakes(false).await;
    assert!(seconds.iter().all(|&n| (1..=3).contains(&n)), "{seconds:?}");
    assert_eq!(by_task, the_sessions_timers());
}

// Each naming a web seed, the same thousand wake no more: a finished
// torrent wants nothing of a web seed and runs no web seed tick.
#[tokio::test(start_paused = true)]
async fn a_thousand_idle_finished_torrents_with_a_web_seed_wake_only_for_the_sessions_timers() {
    let (seconds, by_task) = idle_wakes(true).await;
    assert!(seconds.iter().all(|&n| (1..=3).contains(&n)), "{seconds:?}");
    assert_eq!(by_task, the_sessions_timers());
}

// A finished torrent with a web seed runs no web seed tick; once it wants
// a file it skipped, it ticks again each WEB_SEED_TICK.
#[tokio::test(start_paused = true)]
async fn a_finished_torrent_wanting_a_file_again_ticks_its_web_seeds_again() {
    let dir = TempDir::with_prefix("web_seed_wanting").unwrap();
    let content = dir.path().join("content");
    std::fs::create_dir(&content).unwrap();
    std::fs::write(content.join("0.data"), vec![0u8; 1024]).unwrap();
    std::fs::write(content.join("1.data"), vec![1u8; 1024]).unwrap();
    let torrent = torrent_of(&content).await;
    std::fs::remove_file(content.join("1.data")).unwrap();
    let session = session(dir.path()).await;
    let handle = add(
        &session,
        torrent,
        &content,
        AddTorrentOptions {
            only_files: Some(vec![0]),
            web_seeds: vec![WebSeed::new(b"http://127.0.0.1:9/", WebSeedKind::Url, true)],
            ..Default::default()
        },
    )
    .await;
    handle.wait_until_completed().await.unwrap();

    let (_, finished) = count(10).await;
    assert_eq!(finished.get("web_seeds"), None, "{finished:?}");

    handle
        .set_file_priorities(&[(1, FilePriority::Normal)])
        .unwrap();
    let (_, wanting) = count(10).await;
    assert!(
        wanting.get("web_seeds").is_some_and(|&n| n >= 10),
        "{wanting:?}"
    );
    assert_eq!(handle.web_seeds().len(), 1);
    session.stop().await;
}

// A torrent still downloading, with no web seed, runs no web seed tick;
// given one, it ticks each WEB_SEED_TICK.
#[tokio::test(start_paused = true)]
async fn a_downloading_torrent_given_a_web_seed_ticks_its_web_seeds() {
    let dir = TempDir::with_prefix("web_seed_given").unwrap();
    let file = dir.path().join("source.data");
    std::fs::write(&file, vec![2u8; 4096]).unwrap();
    let torrent = torrent_of(&file).await;
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    let session = session(dir.path()).await;
    let handle = add(&session, torrent, &out, AddTorrentOptions::default()).await;
    handle.wait_until_initialized().await.unwrap();

    let (_, bare) = count(10).await;
    assert_eq!(bare.get("web_seeds"), None, "{bare:?}");

    assert!(handle.add_web_seed(web_seed()));
    let (_, given) = count(10).await;
    assert!(
        given.get("web_seeds").is_some_and(|&n| n >= 10),
        "{given:?}"
    );
    session.stop().await;
}
