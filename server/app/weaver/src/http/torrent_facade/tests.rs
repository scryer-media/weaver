// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::Value;
use tokio::sync::{RwLock, broadcast, mpsc};
use tower::ServiceExt;

use weaver_server_core::auth::{ApiKeyAuthRow, ApiKeyCache, LoginAuthCache, hash_api_key};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::operations::metrics::PipelineMetrics;
use weaver_server_core::post_processing::model::PostProcessingSettings;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::model::{Config, SharedConfig};
use weaver_server_core::settings::{SeedingPolicy, TorrentConfig};
use weaver_server_core::torrents::identity::IDENTITY;
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{
    JobEnv, SystemClock, TorrentService, TorrentSettings, TorrentView,
};
use weaver_server_core::torrents::sessions::{
    OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};
use weaver_server_core::torrents::{TorrentFile, TorrentSource, TorrentState, status};
use weaver_server_core::watch_folder::WatchFolderService;
use weaver_server_core::{Database, SchedulerCommand, SchedulerHandle, SharedPipelineState};

use super::*;

const ADMIN: &str = "facade-admin-key";
const READER: &str = "facade-read-key";

// A route that is always down: nothing here reaches a network.
struct Offline;

impl Routes for Offline {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String> {
        Ok(RoutePlan {
            scope,
            policy: RoutingPolicy::default(),
            egress_id: SYSTEM_EGRESS_ID,
            kind: LegKind::System,
            key: RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct),
            down: Some("offline".into()),
        })
    }

    fn open<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: &'life1 RoutePlan,
    ) -> Pin<Box<dyn Future<Output = Result<OpenLeg, String>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Err("offline".to_owned()) })
    }
}

// A scheduler that answers every bandwidth change and puts holds in force.
fn scheduler() -> SchedulerHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
    let (event_tx, _) = broadcast::channel(1);
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let holds = state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                SchedulerCommand::SetBandwidth { reply, .. } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::Pause { hold, reply } => {
                    holds.hold(hold);
                    let _ = reply.send(());
                }
                SchedulerCommand::Resume {
                    origin,
                    owner,
                    reply,
                } => {
                    holds.release(origin, owner);
                    let _ = reply.send(());
                }
                _ => {}
            }
        }
    });
    SchedulerHandle::new(cmd_tx, event_tx, state)
}

fn config(complete_dir: &std::path::Path) -> SharedConfig {
    Arc::new(RwLock::new(Config {
        seeding: None,
        torrent: Default::default(),
        data_dir: complete_dir.to_string_lossy().into_owned(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: Some(complete_dir.to_string_lossy().into_owned()),
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        bandwidth: Default::default(),
        cleanup_after_extract: None,
        propagation_delay_secs: None,
        watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }))
}

// A scheduler that is gone: every command to it fails.
fn closed_scheduler() -> SchedulerHandle {
    let (cmd_tx, _) = mpsc::channel(8);
    let (event_tx, _) = broadcast::channel(1);
    SchedulerHandle::new(
        cmd_tx,
        event_tx,
        SharedPipelineState::new(PipelineMetrics::new(), vec![]),
    )
}

struct Rig {
    app: Router,
    context: TorrentFacadeContext,
    db: Database,
    config: SharedConfig,
    keys: ApiKeyCache,
    _dir: tempfile::TempDir,
}

async fn rig_with(torrents_on: bool) -> Rig {
    rig_from(torrents_on, RuntimeSecurityConfig::default(), scheduler()).await
}

async fn rig_from(
    torrents_on: bool,
    security: RuntimeSecurityConfig,
    handle: SchedulerHandle,
) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let complete = dir.path().join("complete");
    let config = config(&complete);
    let keys = ApiKeyCache::default();
    for (id, key, scope) in [(1, ADMIN, "admin"), (2, READER, "read")] {
        keys.upsert(ApiKeyAuthRow {
            key_hash: hash_api_key(key),
            id,
            scope: scope.to_owned(),
        });
    }
    let torrents = if torrents_on {
        let sessions = Sessions::new(
            Arc::new(Offline),
            SessionEnv {
                root: dir.path().join("sessions"),
                base_port: 0,
                announce_port: None,
                upnp: false,
                max_peers_total: 100,
                identity: IDENTITY,
            },
            db.clone(),
        );
        let torrents = TorrentService::new(
            db.clone(),
            sessions,
            TorrentSettings {
                complete_dir: complete.clone(),
                categories: Vec::new(),
                seeding: None,
                torrent: TorrentConfig::default(),
            },
            JobEnv::unbounded(),
            Arc::new(SystemClock),
        );
        torrents.start().await.unwrap();
        torrents.publish_to(handle.clone());
        Some(torrents)
    } else {
        None
    };
    let context = TorrentFacadeContext::new(
        db.clone(),
        handle.clone(),
        config.clone(),
        LoginAuthCache::default(),
        keys.clone(),
        super::super::SessionToken(Arc::new("browser-session-token".to_owned())),
        security,
        WatchFolderService::new(db.clone(), handle, config.clone()),
        torrents,
    );
    Rig {
        app: build_torrent_facade_routes(context.clone())
            .layer(Extension(LoginRateLimiter::default())),
        context,
        db,
        config,
        keys,
        _dir: dir,
    }
}

async fn rig() -> Rig {
    rig_with(true).await
}

struct Reply {
    status: StatusCode,
    cookie: Option<String>,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|_| panic!("not JSON: {}", self.body))
    }
}

impl Rig {
    async fn send(&self, request: Request<Body>) -> Reply {
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .map(|value| value.to_str().unwrap().to_owned());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        Reply {
            status,
            cookie,
            body: String::from_utf8(body.to_vec()).unwrap(),
        }
    }

    async fn get(&self, path: &str) -> Reply {
        self.send(
            Request::get(path)
                .header("x-api-key", ADMIN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }

    async fn post(&self, path: &str, form: &str) -> Reply {
        self.post_as(path, form, ADMIN).await
    }

    async fn post_as(&self, path: &str, form: &str, key: &str) -> Reply {
        self.send(
            Request::post(path)
                .header(header::AUTHORIZATION, format!("Bearer {key}"))
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(form.to_owned()))
                .unwrap(),
        )
        .await
    }

    async fn add_file(&self, metainfo: &[u8], fields: &[(&str, &str)]) -> Reply {
        let boundary = "facade-boundary";
        let mut body = Vec::new();
        for (name, value) in fields {
            body.extend_from_slice(
                format!(
                    "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
                )
                .as_bytes(),
            );
        }
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; filename=\"a.torrent\"\r\nContent-Type: application/x-bittorrent\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(metainfo);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        self.send(
            Request::post("/api/v2/torrents/add")
                .header("x-api-key", ADMIN)
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }

    async fn torrents(&self) -> Vec<Value> {
        self.get("/api/v2/torrents/info")
            .await
            .json()
            .as_array()
            .unwrap()
            .clone()
    }
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo.
fn metainfo(name: &str, length: usize, private: bool) -> Vec<u8> {
    // No content is ever fetched, so the piece hashes only need the right length.
    let pieces = vec![0u8; length.div_ceil(1024) * 20];
    let mut info = b"d".to_vec();
    bstr(&mut info, b"length");
    info.extend_from_slice(format!("i{length}e").as_bytes());
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    bstr(&mut info, b"piece length");
    info.extend_from_slice(b"i1024e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &pieces);
    if private {
        bstr(&mut info, b"private");
        info.extend_from_slice(b"i1e");
    }
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    out
}

fn view(state: TorrentState) -> TorrentView {
    TorrentView {
        job_id: JobId(1),
        name: "a".into(),
        infohash: "AB".repeat(20),
        source: TorrentSource::Metainfo,
        state,
        detail: None,
        category: None,
        private: false,
        forced: false,
        progress: 0.5,
        total_bytes: 10,
        have_bytes: 5,
        uploaded_bytes: 0,
        downloaded_bytes: 5,
        ratio: 0.0,
        download_bps: 0,
        upload_bps: 0,
        eta_seconds: None,
        peers: 0,
        seeds: 0,
        leechers: 0,
        added_at_ms: 1_700_000_000_000,
        completed_at_ms: None,
        storage_dir: Some("/complete/tv/a".into()),
        seeding_left: None,
        seeding_override: None,
        seeding_policy: SeedingPolicy::default(),
        seeding_source: SeedingSource::Global,
        route: None,
        files: Vec::new(),
        file_have: Vec::new(),
        trackers: Vec::new(),
    }
}

// Each tracker status reads as the client's number for it, with the
// tracker's own counts and message.
#[test]
fn a_trackers_status_reads_as_the_clients_number() {
    let tracker = |status: TrackerStatus| TorrentTracker {
        tier: 1,
        url: "http://tracker.invalid/announce".into(),
        status,
        last_announce_ms: None,
        next_announce_ms: None,
        message: None,
        seeders: None,
        leechers: None,
    };
    let numbers: Vec<Value> = [
        TrackerStatus::Disabled,
        TrackerStatus::NotContacted,
        TrackerStatus::Working,
        TrackerStatus::Updating,
        TrackerStatus::NotWorking,
    ]
    .into_iter()
    .map(|status| tracker_json(&tracker(status))["status"].clone())
    .collect();
    assert_eq!(numbers, [0, 1, 2, 3, 4]);

    let working = tracker_json(&TorrentTracker {
        seeders: Some(3),
        leechers: Some(5),
        ..tracker(TrackerStatus::Working)
    });
    assert_eq!(
        working,
        serde_json::json!({
            "url": "http://tracker.invalid/announce",
            "status": 2,
            "tier": 1,
            "num_peers": 8,
            "num_seeds": 3,
            "num_leeches": 5,
            "num_downloaded": -1,
            "msg": "",
        })
    );
    let failed = tracker_json(&TorrentTracker {
        message: Some("refused".into()),
        ..tracker(TrackerStatus::NotWorking)
    });
    assert_eq!(
        (&failed["status"], &failed["num_seeds"], &failed["msg"]),
        (&Value::from(4), &Value::from(-1), &Value::from("refused"))
    );
}

#[test]
fn every_weaver_state_maps_to_one_client_state() {
    let done = Some(1_700_000_100_000);
    let cases: Vec<(TorrentView, &str)> = vec![
        (view(TorrentState::Queued), "queuedDL"),
        (
            TorrentView {
                completed_at_ms: done,
                ..view(TorrentState::Queued)
            },
            "queuedUP",
        ),
        (view(TorrentState::FetchingMetadata), "metaDL"),
        (view(TorrentState::CheckingFiles), "checkingDL"),
        (
            TorrentView {
                completed_at_ms: done,
                ..view(TorrentState::CheckingFiles)
            },
            "checkingUP",
        ),
        (view(TorrentState::Downloading), "downloading"),
        (
            TorrentView {
                forced: true,
                ..view(TorrentState::Downloading)
            },
            "forcedDL",
        ),
        (view(TorrentState::Stalled), "stalledDL"),
        (
            TorrentView {
                completed_at_ms: done,
                ..view(TorrentState::Stalled)
            },
            "stalledUP",
        ),
        (
            TorrentView {
                completed_at_ms: done,
                upload_bps: 9,
                ..view(TorrentState::Seeding)
            },
            "uploading",
        ),
        (
            TorrentView {
                completed_at_ms: done,
                ..view(TorrentState::Seeding)
            },
            "stalledUP",
        ),
        (
            TorrentView {
                completed_at_ms: done,
                forced: true,
                ..view(TorrentState::Seeding)
            },
            "forcedUP",
        ),
        (view(TorrentState::Paused), "stoppedDL"),
        (
            TorrentView {
                completed_at_ms: done,
                ..view(TorrentState::Paused)
            },
            "stoppedUP",
        ),
        (
            TorrentView {
                completed_at_ms: done,
                detail: Some(status::GOAL_MET.into()),
                ..view(TorrentState::Stopped)
            },
            "stoppedUP",
        ),
        (
            TorrentView {
                detail: Some("disk failed".into()),
                ..view(TorrentState::Stopped)
            },
            "error",
        ),
    ];
    for (view, expected) in cases {
        assert_eq!(state(&view), expected, "{:?}", view.state);
    }
}

fn file(index: u32, path: &str) -> TorrentFile {
    TorrentFile {
        index,
        path: path.into(),
        length: 5,
        selected: true,
    }
}

#[test]
fn content_path_is_the_output_and_save_path_holds_it() {
    let info = info(&view(TorrentState::Downloading));
    assert_eq!(info["content_path"], "/complete/tv/a");
    assert_eq!(info["save_path"], "/complete/tv");
    assert_eq!(info["hash"], "ab".repeat(20));
    assert_eq!(info["eta"], ETA_UNKNOWN);
    assert_eq!(info["added_on"], 1_700_000_000);
    assert_eq!(info["completion_on"], -1);
}

#[test]
fn a_one_file_torrent_reads_as_its_file_and_a_folder_as_its_folder() {
    let one = TorrentView {
        name: "a.mkv".into(),
        storage_dir: Some("/complete/tv/a".into()),
        files: vec![file(0, "a.mkv")],
        ..view(TorrentState::Seeding)
    };
    let info_one = info(&one);
    assert_eq!(info_one["content_path"], "/complete/tv/a/a.mkv");
    assert_eq!(info_one["save_path"], "/complete/tv/a");

    // A folder that holds one file is still a folder.
    for files in [
        vec![file(0, "a.mkv")],
        vec![file(0, "a.mkv"), file(1, "a.nfo")],
    ] {
        let folder = TorrentView {
            name: "Folder".into(),
            storage_dir: Some("/complete/tv/Folder".into()),
            files,
            ..view(TorrentState::Seeding)
        };
        let info = info(&folder);
        assert_eq!(info["content_path"], "/complete/tv/Folder");
        assert_eq!(info["save_path"], "/complete/tv");
    }
}

#[test]
fn a_magnet_name_shapes_no_path_before_weaver_places_it() {
    let magnet = TorrentView {
        name: "../../x".into(),
        source: TorrentSource::Magnet,
        storage_dir: None,
        ..view(TorrentState::FetchingMetadata)
    };
    let info = info(&magnet);
    assert_eq!(info["content_path"], "");
    assert_eq!(info["save_path"], "");
    assert_eq!(info["name"], "../../x");
}

#[test]
fn share_limits_report_the_policy_in_force_and_where_it_comes_from() {
    let policy = SeedingPolicy {
        ratio_goal: Some(2.0),
        seed_time_goal_minutes: Some(60),
        ..SeedingPolicy::default()
    };
    let global = info(&TorrentView {
        seeding_policy: policy,
        ..view(TorrentState::Seeding)
    });
    assert_eq!(global["ratio_limit"], -2);
    assert_eq!(global["seeding_time_limit"], -2);
    assert_eq!(global["max_ratio"], 2.0);
    assert_eq!(global["max_seeding_time"], 60);
    assert_eq!(global["max_inactive_seeding_time"], -1);
    let own = info(&TorrentView {
        seeding_policy: policy,
        seeding_source: SeedingSource::Category,
        ..view(TorrentState::Seeding)
    });
    assert_eq!(own["ratio_limit"], 2.0);
    assert_eq!(own["seeding_time_limit"], 60);
    assert_eq!(own["inactive_seeding_time_limit"], -1);
    // No counter for these is kept, so they are not claimed.
    assert!(own.get("seeding_time").is_none());
    assert!(own.get("last_activity").is_none());
}

#[tokio::test]
async fn an_unauthenticated_probe_is_forbidden_never_missing() {
    let rig = rig().await;
    for path in [
        "/api/v2/app/webapiVersion",
        "/api/v2/torrents/info",
        "/api/v2/nope/nope",
    ] {
        let reply = rig
            .send(Request::get(path).body(Body::empty()).unwrap())
            .await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{path}");
    }
}

#[tokio::test]
async fn login_issues_a_cookie_that_logout_ends() {
    let rig = rig().await;
    let login = |password: &str| {
        Request::post("/api/v2/auth/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(format!("username=any&password={password}")))
            .unwrap()
    };
    let failed = rig.send(login("wrong")).await;
    assert_eq!(
        (failed.status, failed.body.as_str()),
        (StatusCode::OK, "Fails.")
    );
    assert!(failed.cookie.is_none());

    let signed_in = rig.send(login(ADMIN)).await;
    assert_eq!(signed_in.body, "Ok.");
    let cookie = signed_in.cookie.unwrap();
    let sid = cookie.split(';').next().unwrap().to_owned();
    assert!(sid.starts_with("SID="));

    let with_cookie = |path: &str| {
        Request::get(path)
            .header(header::COOKIE, sid.clone())
            .body(Body::empty())
            .unwrap()
    };
    let version = rig.send(with_cookie("/api/v2/app/webapiVersion")).await;
    assert_eq!(
        (version.status, version.body.as_str()),
        (StatusCode::OK, WEBAPI_VERSION)
    );
    let app = rig.send(with_cookie("/api/v2/app/version")).await;
    assert_eq!(app.body, format!("v{}", env!("CARGO_PKG_VERSION")));

    rig.send(with_cookie("/api/v2/auth/logout")).await;
    let after = rig.send(with_cookie("/api/v2/app/webapiVersion")).await;
    assert_eq!(after.status, StatusCode::FORBIDDEN);
}

fn login_from(peer: [u8; 4], username: &str, password: &str) -> Request<Body> {
    let mut request = Request::post("/api/v2/auth/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(format!(
            "username={username}&password={password}"
        )))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from((peer, 40_000))));
    request
}

#[tokio::test]
async fn repeated_failed_logins_from_one_client_are_throttled() {
    let rig = rig().await;
    let client = [198, 51, 100, 7];
    for _ in 0..super::super::auth::LOGIN_MAX_FAILURES {
        let failed = rig.send(login_from(client, "any", "wrong")).await;
        assert_eq!(
            (failed.status, failed.body.as_str()),
            (StatusCode::OK, "Fails.")
        );
    }

    // Throttled before the key is checked: even the right key is refused,
    // with the status and body the first-party login answers.
    let throttled = rig.send(login_from(client, "any", ADMIN)).await;
    assert_eq!(throttled.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        throttled.json(),
        serde_json::json!({ "error": "too many login attempts" })
    );
    assert!(throttled.cookie.is_none());

    // Another client, under another name, is not held back.
    let other = rig
        .send(login_from([198, 51, 100, 8], "other", ADMIN))
        .await;
    assert_eq!(other.body, "Ok.");
}

#[tokio::test]
async fn a_successful_login_clears_the_failures_counted_against_it() {
    let rig = rig().await;
    let client = [203, 0, 113, 5];
    let below_limit = super::super::auth::LOGIN_MAX_FAILURES - 1;
    for _ in 0..below_limit {
        assert_eq!(
            rig.send(login_from(client, "any", "wrong")).await.body,
            "Fails."
        );
    }
    assert_eq!(rig.send(login_from(client, "any", ADMIN)).await.body, "Ok.");

    // Had the success not cleared the count, these would reach the limit.
    for _ in 0..below_limit {
        assert_eq!(
            rig.send(login_from(client, "any", "wrong")).await.body,
            "Fails."
        );
    }
    let signed_in = rig.send(login_from(client, "any", ADMIN)).await;
    assert_eq!(
        (signed_in.status, signed_in.body.as_str()),
        (StatusCode::OK, "Ok.")
    );
}

#[tokio::test]
async fn basic_auth_carries_the_key_and_a_read_key_cannot_change_anything() {
    let rig = rig().await;
    let basic = format!("Basic {}", BASE64_STANDARD.encode(format!("x:{READER}")));
    let read = rig
        .send(
            Request::get("/api/v2/torrents/info")
                .header(header::AUTHORIZATION, basic)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!((read.status, read.body.as_str()), (StatusCode::OK, "[]"));
    let refused = rig
        .post_as("/api/v2/torrents/pause", "hashes=all", READER)
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_added_torrent_is_listed_with_its_paths_and_state() {
    let rig = rig().await;
    let added = rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    assert_eq!((added.status, added.body.as_str()), (StatusCode::OK, "Ok."));

    let torrents = rig.torrents().await;
    assert_eq!(torrents.len(), 1);
    let torrent = &torrents[0];
    let hash = torrent["hash"].as_str().unwrap();
    assert_eq!(hash.len(), 40);
    assert_eq!(hash, hash.to_ascii_lowercase());
    assert_eq!(torrent["name"], "one.mkv");
    assert_eq!(torrent["size"], 3000);
    // Its route is down.
    assert_eq!(torrent["state"], "stalledDL");
    // One file: the content is the file, in the folder weaver made for it.
    let content = PathBuf::from(torrent["content_path"].as_str().unwrap());
    assert_eq!(content.file_name().unwrap(), "one.mkv");
    assert_eq!(
        content.parent().unwrap(),
        Path::new(torrent["save_path"].as_str().unwrap())
    );
    assert_eq!(
        Path::new(torrent["save_path"].as_str().unwrap())
            .file_name()
            .unwrap(),
        "one"
    );

    let properties = rig
        .get(&format!("/api/v2/torrents/properties?hash={hash}"))
        .await
        .json();
    assert_eq!(properties["total_size"], 3000);
    assert_eq!(properties["save_path"], torrent["save_path"]);
    let files = rig
        .get(&format!("/api/v2/torrents/files?hash={hash}"))
        .await
        .json();
    assert_eq!(files[0]["name"], "one.mkv");
    assert_eq!(files[0]["size"], 3000);
    let trackers = rig
        .get(&format!("/api/v2/torrents/trackers?hash={hash}"))
        .await
        .json();
    assert_eq!(trackers, Value::Array(Vec::new()));
    let missing = rig
        .get(&format!("/api/v2/torrents/files?hash={}", "0".repeat(40)))
        .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);

    let filtered = rig
        .get(&format!(
            "/api/v2/torrents/info?hashes={hash}&filter=stalled"
        ))
        .await;
    assert_eq!(filtered.json().as_array().unwrap().len(), 1);
    let none = rig.get("/api/v2/torrents/info?filter=completed").await;
    assert_eq!(none.json(), Value::Array(Vec::new()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_fails_and_a_refusal_conflicts() {
    let rig = rig().await;
    let bytes = metainfo("one.mkv", 3000, false);
    assert_eq!(rig.add_file(&bytes, &[]).await.body, "Ok.");
    let again = rig.add_file(&bytes, &[]).await;
    assert_eq!(
        (again.status, again.body.as_str()),
        (StatusCode::OK, "Fails.")
    );

    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["iso".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let refused = rig.add_file(&metainfo("disc.iso", 2000, false), &[]).await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(
        refused.body.contains("unwanted extension"),
        "{}",
        refused.body
    );

    let magnet = rig
        .post(
            "/api/v2/torrents/add",
            "urls=magnet%3A%3Fxt%3Durn%3Abtih%3A0123456789abcdef0123456789abcdef01234567",
        )
        .await;
    assert_eq!(magnet.status, StatusCode::CONFLICT);
    // The same hash in base32 reaches the same refusal, not a parse error.
    let base32 = rig
        .post(
            "/api/v2/torrents/add",
            "urls=magnet%3A%3Fxt%3Durn%3Abtih%3AAERUKZ4JVPG66AJDIVTYTK6N54ASGRLH",
        )
        .await;
    assert_eq!((base32.status, &base32.body), (magnet.status, &magnet.body));
    assert_eq!(rig.torrents().await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_start_and_force_change_the_state() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[("stopped", "true")])
        .await;
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    assert_eq!(rig.torrents().await[0]["state"], "stoppedDL");

    assert_eq!(
        rig.post("/api/v2/torrents/start", &format!("hashes={hash}"))
            .await
            .status,
        StatusCode::OK
    );
    assert_ne!(rig.torrents().await[0]["state"], "stoppedDL");
    rig.post("/api/v2/torrents/pause", &format!("hashes={hash}"))
        .await;
    assert_eq!(rig.torrents().await[0]["state"], "stoppedDL");
    rig.post("/api/v2/torrents/resume", &format!("hashes={hash}"))
        .await;
    rig.post(
        "/api/v2/torrents/setForceStart",
        &format!("hashes={hash}&value=true"),
    )
    .await;
    assert_eq!(rig.torrents().await[0]["force_start"], true);
}

// Pausing every torrent is one hold, not a pause of each: a torrent's own
// pause stands under it and outlasts it, and the hold writes no torrent's
// intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pausing_all_is_one_hold_over_each_torrents_own_pause() {
    use weaver_server_core::jobs::{HoldOrigin, HoldOwner};
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    rig.add_file(&metainfo("two.mkv", 4000, false), &[]).await;
    let queue = rig.context.inner.handle.state().clone();
    let held = || {
        queue
            .holds()
            .held_by(HoldOrigin::Manual, HoldOwner::Torrents)
    };
    let named = |torrents: &[Value], name: &str, field: &str| {
        torrents
            .iter()
            .find(|torrent| torrent["name"] == name)
            .unwrap()[field]
            .as_str()
            .unwrap()
            .to_owned()
    };
    // Send `form` to `path` and wait for the torrents to show whether each
    // is stopped.
    let send = |path: &'static str, form: String, stopped: (bool, bool)| {
        let rig = &rig;
        let mut published = queue.subscribe_job_changes();
        published.borrow_and_update();
        async move {
            assert_eq!(rig.post(path, &form).await.status, StatusCode::OK);
            loop {
                let listed = rig.torrents().await;
                let shown = (
                    named(&listed, "one.mkv", "state") == "stoppedDL",
                    named(&listed, "two.mkv", "state") == "stoppedDL",
                );
                if shown == stopped {
                    break;
                }
                published.changed().await.unwrap();
            }
        }
    };
    let one = named(&rig.torrents().await, "one.mkv", "hash");

    // A pause of one hash is that torrent's own and sets no hold.
    send(
        "/api/v2/torrents/pause",
        format!("hashes={one}"),
        (true, false),
    )
    .await;
    assert!(!held());

    for (pause, resume) in [
        ("/api/v2/torrents/pause", "/api/v2/torrents/resume"),
        ("/api/v2/torrents/stop", "/api/v2/torrents/start"),
    ] {
        send(pause, "hashes=all".to_owned(), (true, true)).await;
        assert!(held());
        // The torrent paused on its own stays paused once the hold lifts.
        send(resume, "hashes=all".to_owned(), (true, false)).await;
        assert!(!held());
    }
    let two = named(&rig.torrents().await, "two.mkv", "hash");
    for (hash, intent) in [
        (
            one,
            weaver_server_core::torrents::model::TorrentIntent::Paused,
        ),
        (
            two,
            weaver_server_core::torrents::model::TorrentIntent::Resumed,
        ),
    ] {
        let job_id = rig.context.by_hash(&hash).await.unwrap().job_id;
        assert_eq!(rig.db.get_torrent(job_id).unwrap().unwrap().intent, intent);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_private_torrent_short_of_its_goal_needs_confirmation_to_delete() {
    let rig = rig().await;
    rig.add_file(&metainfo("p.bin", 2000, true), &[]).await;
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let refused = rig
        .post(
            "/api/v2/torrents/delete",
            &format!("hashes={hash}&deleteFiles=false"),
        )
        .await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(refused.body.contains("seeding goal"), "{}", refused.body);
    assert_eq!(rig.torrents().await.len(), 1);
    let removed = rig
        .post(
            "/api/v2/torrents/delete",
            &format!("hashes={hash}&confirmUnmetGoal=true"),
        )
        .await;
    assert_eq!(removed.status, StatusCode::OK);
    assert!(rig.torrents().await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn categories_are_created_listed_and_assigned() {
    let rig = rig().await;
    assert_eq!(
        rig.post("/api/v2/torrents/createCategory", "category=tv")
            .await
            .status,
        StatusCode::OK
    );
    let conflict = rig
        .post("/api/v2/torrents/createCategory", "category=TV")
        .await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    let invalid = rig
        .post("/api/v2/torrents/createCategory", "category=a/b")
        .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);

    let categories = rig.get("/api/v2/torrents/categories").await.json();
    let complete = rig.config.read().await.complete_dir();
    assert_eq!(categories["tv"]["name"], "tv");
    assert_eq!(
        categories["tv"]["savePath"],
        PathBuf::from(&complete)
            .join("tv")
            .to_string_lossy()
            .as_ref()
    );

    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let moved = rig
        .post(
            "/api/v2/torrents/setCategory",
            &format!("hashes={hash}&category=tv"),
        )
        .await;
    assert_eq!(moved.status, StatusCode::OK);
    let listed = rig.get("/api/v2/torrents/info?category=tv").await.json();
    assert_eq!(listed[0]["category"], "tv");
    let unknown = rig
        .post(
            "/api/v2/torrents/setCategory",
            &format!("hashes={hash}&category=films"),
        )
        .await;
    assert_eq!(unknown.status, StatusCode::CONFLICT);
    assert_eq!(
        rig.post("/api/v2/torrents/setLocation", "hashes=all&location=/x")
            .await
            .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn preferences_read_and_apply_the_mapped_keys() {
    let rig = rig().await;
    let before = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(before["temp_path_enabled"], false);
    assert_eq!(before["dl_limit"], 0);
    assert_eq!(before["encryption"], 0);
    assert_eq!(
        before["max_active_downloads"],
        TorrentConfig::default().max_active_downloads
    );

    let json = r#"{"dl_limit":1048576,"up_limit":2048,"max_active_downloads":7,"listen_port":1}"#;
    let set = rig
        .post(
            "/api/v2/app/setPreferences",
            &format!("json={}", url_encode(json)),
        )
        .await;
    assert_eq!(set.status, StatusCode::OK);
    let after = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(after["dl_limit"], 1_048_576);
    assert_eq!(after["up_limit"], 2048);
    assert_eq!(after["max_active_downloads"], 7);
    assert_eq!(after["listen_port"], before["listen_port"]);

    let path = rig.get("/api/v2/app/defaultSavePath").await;
    assert_eq!(path.body, rig.config.read().await.complete_dir());
    let build = rig.get("/api/v2/app/buildInfo").await.json();
    assert_eq!(build["bitness"], 64);
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' => (byte as char).to_string(),
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maindata_is_a_full_update_with_a_rising_rid() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let first = rig.get("/api/v2/sync/maindata").await.json();
    let second = rig.get("/api/v2/sync/maindata?rid=1").await.json();
    assert_eq!(first["full_update"], true);
    assert!(second["rid"].as_u64().unwrap() > first["rid"].as_u64().unwrap());
    let torrents = first["torrents"].as_object().unwrap();
    assert_eq!(torrents.len(), 1);
    let (hash, torrent) = torrents.iter().next().unwrap();
    assert_eq!(hash.len(), 40);
    assert_eq!(torrent["name"], "one.mkv");
    assert!(torrent.get("hash").is_none());
    assert_eq!(first["server_state"]["connection_status"], "connected");

    let transfer = rig.get("/api/v2/transfer/info").await.json();
    assert_eq!(transfer["dl_info_speed"], 0);
    assert_eq!(transfer["dl_rate_limit"], 0);
}

#[tokio::test]
async fn without_a_torrent_client_the_lists_are_empty_and_adds_conflict() {
    let rig = rig_with(false).await;
    assert!(rig.torrents().await.is_empty());
    let added = rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    assert_eq!(added.status, StatusCode::CONFLICT);
    assert_eq!(
        rig.post("/api/v2/torrents/pause", "hashes=all")
            .await
            .status,
        StatusCode::OK
    );
}

// A body that counts every read of it.
struct Counted(Arc<AtomicUsize>);

impl tokio::io::AsyncRead for Counted {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::task::Poll::Ready(Ok(()))
    }
}

fn counted_add(reads: &Arc<AtomicUsize>, key: Option<&str>) -> Request<Body> {
    let mut request = Request::post("/api/v2/torrents/add")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    request
        .body(Body::from_stream(tokio_util::io::ReaderStream::new(
            Counted(Arc::clone(reads)),
        )))
        .unwrap()
}

#[tokio::test]
async fn a_caller_is_refused_before_its_body_is_read() {
    let rig = rig().await;
    let reads = Arc::new(AtomicUsize::new(0));
    for key in [None, Some(READER)] {
        let refused = rig.send(counted_add(&reads, key)).await;
        assert_eq!(refused.status, StatusCode::FORBIDDEN, "{key:?}");
    }
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    let read = rig.send(counted_add(&reads, Some(ADMIN))).await;
    assert_eq!(read.status, StatusCode::BAD_REQUEST);
    assert!(reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn an_internal_failure_is_logged_not_shown() {
    let rig = rig_from(false, RuntimeSecurityConfig::default(), closed_scheduler()).await;
    let failed = rig
        .post(
            "/api/v2/app/setPreferences",
            &format!("json={}", url_encode(r#"{"dl_limit":1024}"#)),
        )
        .await;
    assert_eq!(
        (failed.status, failed.body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    );
}

fn login(password: &str) -> Request<Body> {
    Request::post("/api/v2/auth/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(format!("username=any&password={password}")))
        .unwrap()
}

fn version_with(cookie: &str, key: Option<&str>) -> Request<Body> {
    let mut request = Request::get("/api/v2/app/webapiVersion").header(header::COOKIE, cookie);
    if let Some(key) = key {
        request = request.header("x-api-key", key);
    }
    request.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn a_session_keeps_no_key_and_an_unknown_cookie_falls_through() {
    let mut security = RuntimeSecurityConfig::default();
    security.secure_cookies = true;
    let rig = rig_from(true, security, scheduler()).await;
    let cookie = rig.send(login(ADMIN)).await.cookie.unwrap();
    assert!(cookie.contains("; Secure"), "{cookie}");
    let sid = cookie.split(';').next().unwrap().to_owned();
    let stored: Vec<[u8; 32]> = rig
        .context
        .sessions()
        .keys
        .values()
        .map(|(_, key_hash)| *key_hash)
        .collect();
    assert_eq!(stored, vec![hash_api_key(ADMIN)]);

    let unknown = rig.send(version_with("SID=unknown", Some(READER))).await;
    assert_eq!(unknown.status, StatusCode::OK);
    let alone = rig.send(version_with("SID=unknown", None)).await;
    assert_eq!(alone.status, StatusCode::FORBIDDEN);

    assert_eq!(
        rig.send(version_with(&sid, None)).await.status,
        StatusCode::OK
    );
    rig.keys.remove_by_id(1);
    let revoked = rig.send(version_with(&sid, None)).await;
    assert_eq!(revoked.status, StatusCode::FORBIDDEN);
    assert!(rig.context.sessions().keys.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn share_limits_are_set_on_add_and_after() {
    let rig = rig().await;
    rig.add_file(
        &metainfo("one.mkv", 3000, false),
        &[("ratioLimit", "2"), ("seedingTimeLimit", "60")],
    )
    .await;
    let torrent = &rig.torrents().await[0];
    assert_eq!(torrent["ratio_limit"], 2.0);
    assert_eq!(torrent["seeding_time_limit"], 60);
    assert_eq!(torrent["inactive_seeding_time_limit"], -1);
    let hash = torrent["hash"].as_str().unwrap().to_owned();
    let limits = |form: &str| format!("hashes={hash}&{form}");

    let inherit = rig
        .post(
            "/api/v2/torrents/setShareLimits",
            &limits("ratioLimit=-2&seedingTimeLimit=-2&inactiveSeedingTimeLimit=-2"),
        )
        .await;
    assert_eq!(inherit.status, StatusCode::OK);
    assert_eq!(rig.torrents().await[0]["ratio_limit"], -2);

    // No goal where none is inherited.
    rig.post(
        "/api/v2/torrents/setShareLimits",
        &limits("ratioLimit=-1&seedingTimeLimit=30"),
    )
    .await;
    let torrent = &rig.torrents().await[0];
    assert_eq!(torrent["ratio_limit"], -1);
    assert_eq!(torrent["seeding_time_limit"], 30);

    rig.config.write().await.seeding = Some(SeedingPolicy {
        ratio_goal: Some(1.0),
        ..SeedingPolicy::default()
    });
    let refused = rig
        .post("/api/v2/torrents/setShareLimits", &limits("ratioLimit=-1"))
        .await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    let invalid = rig
        .post("/api/v2/torrents/setShareLimits", &limits("ratioLimit=-3"))
        .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn the_stub_endpoints_answer_and_a_url_must_be_a_magnet() {
    let rig = rig().await;
    let top = rig.post("/api/v2/torrents/topPrio", "hashes=all").await;
    assert_eq!(
        (top.status, top.body.as_str()),
        (StatusCode::CONFLICT, "queueing is off")
    );
    for action in ["addTags", "removeTags", "recheck", "reannounce"] {
        let reply = rig
            .post(&format!("/api/v2/torrents/{action}"), "hashes=all&tags=x")
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{action}");
    }
    for path in ["/api/v2/torrents/tags", "/api/v2/log/main"] {
        assert_eq!(
            rig.get(path).await.json(),
            Value::Array(Vec::new()),
            "{path}"
        );
    }
    let url = rig
        .post(
            "/api/v2/torrents/add",
            "urls=http%3A%2F%2F127.0.0.1%2Fa.torrent",
        )
        .await;
    assert_eq!(
        (url.status, url.body.as_str()),
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "url must be a magnet link"
        )
    );
    assert!(rig.torrents().await.is_empty());
    // The offline rig's route is a system leg, which carries DHT.
    let preferences = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(preferences["dht"], true);
}

#[tokio::test]
async fn a_cap_of_nothing_is_never_read_as_no_limit() {
    let rig = rig().await;
    rig.config.write().await.bandwidth = Bandwidth {
        download_bps: Some(0),
        upload_bps: None,
        split: None,
    };
    rig.context.inner.handle.state().set_bandwidth_split(
        weaver_server_core::bandwidth::SplitState {
            cap: Some(0),
            rates: weaver_server_core::bandwidth::SplitRates {
                usenet: Some(0),
                torrent: Some(0),
            },
            ..Default::default()
        },
    );
    let preferences = rig.get("/api/v2/app/preferences").await.json();
    assert!(preferences.get("dl_limit").is_none());
    assert_eq!(preferences["up_limit"], 0);
    let transfer = rig.get("/api/v2/transfer/info").await.json();
    assert!(transfer.get("dl_rate_limit").is_none());
    assert_eq!(transfer["up_rate_limit"], 0);
}
