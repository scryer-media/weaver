// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session of idle torrents wakes for: a thousand finished torrents
//! with no peers, counted on the paused clock one second at a time. A wake
//! is a poll of a task the engine's spawn helper started, counted by where
//! it was started; a task started with `tokio::spawn` itself is not counted.

use std::{collections::BTreeMap, time::Duration};

use tempfile::TempDir;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session,
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

// The wakes in each of SECONDS seconds once a thousand finished torrents
// sit idle, and the wakes over all of them by the file that spawned the
// task woken.
async fn idle_wakes(web_seed: bool) -> (Vec<u64>, BTreeMap<String, u64>) {
    let dir = TempDir::with_prefix("idle_wakes").unwrap();
    let spawner = BlockingSpawner::new(1);
    let session = Session::new_with_opts(dir.path().join("session"), isolated_session_opts(false))
        .await
        .unwrap();
    let mut handles = Vec::with_capacity(TORRENTS);
    for i in 0..TORRENTS {
        let file = dir.path().join(format!("{i}.data"));
        std::fs::write(&file, format!("idle torrent {i}").repeat(64)).unwrap();
        let torrent = create_torrent(
            &file,
            CreateTorrentOptions {
                piece_length: Some(1024),
                ..Default::default()
            },
            &spawner,
        )
        .await
        .unwrap()
        .as_bytes()
        .unwrap();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                    overwrite: true,
                    web_seeds: if web_seed {
                        vec![WebSeed::new(
                            b"http://127.0.0.1:9/",
                            WebSeedKind::Url,
                            false,
                        )]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handles.push(handle);
    }
    for handle in &handles {
        handle.wait_until_completed().await.unwrap();
    }

    // Past whatever the adds started, then on a half second, which no
    // whole-second timer started at a whole second lands on.
    let start = tokio::time::Instant::now() + Duration::from_millis(60_500);
    tokio::time::sleep_until(start).await;
    let mut seconds = Vec::new();
    let mut before = polls::counted();
    let first = before.clone();
    for second in 1..=SECONDS {
        tokio::time::sleep_until(start + Duration::from_secs(second.into())).await;
        let now = polls::counted();
        seconds.push(
            now.iter()
                .map(|(at, n)| n - before.get(at).copied().unwrap_or(0))
                .sum(),
        );
        before = now;
    }
    let mut by_file = BTreeMap::new();
    for (at, n) in &before {
        let woken = n - first.get(at).copied().unwrap_or(0);
        if woken > 0 {
            let file = at.file().replace('\\', "/");
            let file = &file[file.find("src/").unwrap_or(0)..];
            *by_file.entry(file.to_owned()).or_default() += woken;
        }
    }
    drop(handles);
    session.stop().await;
    (seconds, by_file)
}

// A thousand finished torrents with no peers and no web seed wake for the
// session's timers only, as one torrent would: the speed estimator each
// second, the choker each UNCHOKE_INTERVAL, the peer turnover each
// PEER_TURNOVER_INTERVAL. No torrent wakes.
#[tokio::test(start_paused = true)]
async fn a_thousand_idle_finished_torrents_wake_only_for_the_sessions_timers() {
    let (seconds, by_file) = idle_wakes(false).await;
    assert!(seconds.iter().all(|&n| (1..=3).contains(&n)), "{seconds:?}");
    assert_eq!(
        by_file,
        BTreeMap::from([
            ("src/session.rs".to_owned(), 21),
            ("src/session_stats/mod.rs".to_owned(), 300),
        ])
    );
}

// Each naming a web seed it never needs, the same thousand wake once a
// second each besides: the web seed tick runs while the torrent has a web
// seed, finished or not.
#[tokio::test(start_paused = true)]
async fn a_thousand_idle_finished_torrents_with_a_web_seed_wake_once_a_second_each() {
    let (seconds, by_file) = idle_wakes(true).await;
    assert!(
        seconds.iter().all(|&n| (1001..=1003).contains(&n)),
        "{seconds:?}"
    );
    assert_eq!(
        by_file,
        BTreeMap::from([
            ("src/session.rs".to_owned(), 21),
            ("src/session_stats/mod.rs".to_owned(), 300),
            ("src/torrent_state/live/mod.rs".to_owned(), 300_000),
        ])
    );
}
