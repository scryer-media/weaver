// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline watch_folder/scanner.rs (tests::torrents):
// a dropped torrent goes to the torrents crate's service.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::watch_folder::WatchFolderConfig;
use crate::watch_folder::scanner::*;
use crate::{Database, SchedulerHandle};
use weaver_server_settings::categories::CategoryConfig;

fn category(name: &str) -> CategoryConfig {
    CategoryConfig {
        upload_cap: Default::default(),
        seeding: None,
        torrent_route: false,
        unpack: None,
        id: 1,
        name: name.to_string(),
        dest_dir: None,
        aliases: String::new(),
    }
}
use crate::settings::Config;
use crate::torrents::queries::TorrentsQueries;
use crate::torrents::service::{JobEnv, TorrentService, TorrentSettings};
use crate::torrents::sessions::tests::{FakeRoutes, env};
use crate::torrents::sessions::{Routes, Sessions};
use crate::{PipelineMetrics, SharedPipelineState};
use sha1::{Digest, Sha1};
use weaver_server_base::clock::SystemClock;

struct Rig {
    scanner: WatchFolderScanner,
    torrents: TorrentService,
    routes: Arc<FakeRoutes>,
    db: Database,
    folder: tempfile::TempDir,
    _dir: tempfile::TempDir,
}

fn tv() -> CategoryConfig {
    CategoryConfig {
        torrent_route: true,
        unpack: None,
        ..category("tv")
    }
}

async fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let folder = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let sessions = Sessions::new(
        Arc::clone(&routes) as Arc<dyn Routes>,
        env(&dir.path().join("sessions")),
        db.clone(),
    );
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: dir.path().join("complete"),
        categories: vec![tv()].into(),
        seeding: None,
        torrent: crate::settings::TorrentConfig {
            enabled: true,
            ..Default::default()
        },
    };
    let torrents = TorrentService::new(
        db.clone(),
        sessions,
        settings,
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.open().await.unwrap();
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let handle = SchedulerHandle::new(event_tx, state);
    let config = Arc::new(tokio::sync::RwLock::new(Config {
        upload_cap: None,
        seeding: None,
        torrent: Default::default(),
        data_dir: dir.path().display().to_string(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: None,
        delete_failed_job_files: None,
        history_retention_days: None,
        servers: vec![],
        categories: vec![tv()],
        retry: None,
        bandwidth: Default::default(),
        cleanup_after_extract: None,
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        watch_folder: WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));
    let scanner = WatchFolderScanner::new(db.clone(), handle, config);
    Rig {
        scanner,
        torrents,
        routes,
        db,
        folder,
        _dir: dir,
    }
}

impl Rig {
    async fn scan(&self) -> WatchFolderScanReport {
        self.scanner
            .scan_once(WatchFolderConfig {
                mode: crate::watch_folder::WatchFolderMode::Polling,
                path: Some(self.folder.path().display().to_string()),
                poll_interval_secs: 60,
                stability_secs: 0,
                category_from_subfolders: true,
                scanning_paused: false,
            })
            .await
            .unwrap()
    }

    fn put(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let tv = self.folder.path().join("tv");
        fs::create_dir_all(&tv).unwrap();
        let path = tv.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo.
fn metainfo(name: &str) -> Vec<u8> {
    let data = [7u8; 3000];
    let pieces: Vec<u8> = data
        .chunks(1024)
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect();
    let mut out = b"d4:infod6:lengthi3000e".to_vec();
    bstr(&mut out, b"name");
    bstr(&mut out, name.as_bytes());
    out.extend_from_slice(b"12:piece lengthi1024e");
    bstr(&mut out, b"pieces");
    bstr(&mut out, &pieces);
    out.extend_from_slice(b"ee");
    out
}

// The file's name says nothing: what it holds decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_file_is_added_with_the_folders_category_and_marked_queued() {
    let rig = rig().await;
    rig.torrents.publish_to(rig.scanner.handle.clone());
    let source = rig.put("set.torrent", &metainfo("set.bin"));

    let report = rig.scan().await;

    assert_eq!(report.queued_nzbs.len(), 1, "{report:?}");
    assert!(!source.exists());
    assert!(rig.folder.path().join("tv/set.torrent.queued").exists());
    let stored = rig.db.list_torrents().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].name, "set.bin");
    assert_eq!(stored[0].category_id, Some(1));

    // The same torrent again is refused for good.
    let again = rig.put("again.nzb", &metainfo("set.bin"));
    let report = rig.scan().await;
    assert!(report.queued_nzbs.is_empty());
    assert!(!again.exists());
    assert!(rig.folder.path().join("tv/again.nzb.error").exists());
}

// A magnet waits in the folder while there is no torrent client to
// take it. The client takes it whatever its route's state: on a
// route that is down the torrent is held there, not left in the
// folder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_file_waits_for_the_torrent_client_and_is_taken_on_a_route_that_is_down() {
    let rig = rig().await;
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let source = rig.put(
        "set.magnet",
        format!("magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce\n").as_bytes(),
    );

    let report = rig.scan().await;
    assert!(source.exists(), "{report:?}");
    assert!(
        report
            .transient_errors
            .iter()
            .any(|issue| issue.reason.contains("the torrent client is not running")),
        "{report:?}"
    );

    rig.torrents.publish_to(rig.scanner.handle.clone());
    rig.routes.set_down(Some("tunnel gone"));
    let report = rig.scan().await;
    assert!(!source.exists(), "{report:?}");
    assert_eq!(report.queued_nzbs.len(), 1, "{report:?}");
    assert_eq!(rig.db.list_torrents().unwrap().len(), 1);
}
