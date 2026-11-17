// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent added with a naming of its own looks its names up by it, not
//! by the session's.

use std::{io, sync::Arc};

use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions,
    core::{
        net::{BoxFuture, Resolved, Resolver, TorrentNaming},
        torrent_metainfo::{WebSeed, WebSeedKind},
    },
    create_torrent,
    spawn_utils::BlockingSpawner,
    tests::embedder_util::isolated_session_opts,
};

// Says each name it is asked for, and finds none.
struct Asked(mpsc::UnboundedSender<String>);

impl Resolver for Asked {
    fn resolve<'a>(&'a self, host: &'a str, _port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        let _ = self.0.send(host.to_owned());
        Box::pin(async { Err(io::Error::new(io::ErrorKind::NotFound, "no name found")) })
    }
}

// A downloading torrent whose web seed is named by a host name asks the
// torrent's resolver for that name, and never the session's.
#[tokio::test(start_paused = true)]
async fn a_torrents_web_seed_is_looked_up_by_the_torrents_own_resolver() {
    let dir = TempDir::with_prefix("torrent_naming").unwrap();
    let file = dir.path().join("source.data");
    std::fs::write(&file, vec![3u8; 4096]).unwrap();
    let torrent = create_torrent(
        &file,
        CreateTorrentOptions {
            piece_length: Some(1024),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();

    let (session_tx, mut session_asked) = mpsc::unbounded_channel();
    let session = Session::new_with_opts(
        dir.path().join("session"),
        SessionOptions {
            resolver: Some(Arc::new(Asked(session_tx))),
            ..isolated_session_opts(false)
        },
    )
    .await
    .unwrap();
    let (torrent_tx, mut torrent_asked) = mpsc::unbounded_channel();
    let naming = TorrentNaming {
        resolver: Arc::new(Asked(torrent_tx)),
        http_clients: session.naming().http_clients,
    };
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(out.to_str().unwrap().to_owned()),
                overwrite: true,
                web_seeds: vec![WebSeed::new(
                    b"http://seed.invalid/source.data",
                    WebSeedKind::Url,
                    false,
                )],
                naming: Some(naming),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();

    assert_eq!(torrent_asked.recv().await.as_deref(), Some("seed.invalid"));
    while let Ok(host) = session_asked.try_recv() {
        assert_ne!(host, "seed.invalid");
    }
    session.stop().await;
}
