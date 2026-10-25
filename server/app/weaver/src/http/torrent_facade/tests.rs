// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tokio::sync::{RwLock, broadcast, mpsc};
use tower::ServiceExt;

use weaver_server_core::auth::{ApiKeyAuthRow, ApiKeyCache, LoginAuthCache, hash_api_key};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::operations::metrics::PipelineMetrics;
use weaver_server_core::post_processing::model::PostProcessingSettings;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, ResolvedBy, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::model::{Config, SharedConfig};
use weaver_server_core::settings::{BlocklistSource, SeedingPolicy, TorrentConfig, TorrentLimit};
use weaver_server_core::torrents::identity::IDENTITY;
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{
    JobEnv, SystemClock, TorrentService, TorrentSettings, TorrentView,
};
use weaver_server_core::torrents::sessions::{
    IpRanges, OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};
use weaver_server_core::torrents::{TorrentFile, TorrentSource, TorrentState, status};
use weaver_server_core::watch_folder::WatchFolderService;
use weaver_server_core::{Database, SchedulerCommand, SchedulerHandle, SharedPipelineState};

use super::*;

const ADMIN: &str = "facade-admin-key";
const READER: &str = "facade-read-key";

// A route that opens no session: nothing here reaches a network. It plans
// down unless `up`, which lets a URL be fetched over its leg.
#[derive(Default)]
struct Offline {
    up: bool,
    stream_only: Option<TorrentScope>,
}

impl Routes for Offline {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String> {
        Ok(RoutePlan {
            scope,
            policy: RoutingPolicy::default(),
            egress_id: SYSTEM_EGRESS_ID,
            kind: if self.stream_only == Some(scope) {
                LegKind::Ladder
            } else {
                LegKind::System
            },
            key: RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct),
            names: Some(ResolvedBy::Host),
            down: (!self.up).then(|| {
                weaver_server_core::torrents::sessions::RouteFailure::leg_down("offline".into())
            }),
        })
    }

    fn leg_down(
        &self,
        _: &RoutePlan,
    ) -> Option<weaver_server_core::torrents::sessions::RouteFailure> {
        (!self.up).then(|| {
            weaver_server_core::torrents::sessions::RouteFailure::leg_down("offline".into())
        })
    }

    fn open<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: &'life1 RoutePlan,
        _: usize,
    ) -> Pin<Box<dyn Future<Output = Result<OpenLeg, String>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Err("offline".to_owned()) })
    }

    fn fetch_blocklist<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: &'life1 reqwest::Url,
    ) -> Pin<Box<dyn Future<Output = Result<IpRanges, String>> + Send + 'async_trait>>
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
    rig_on(torrents_on, security, handle, Offline::default()).await
}

async fn rig_on(
    torrents_on: bool,
    security: RuntimeSecurityConfig,
    handle: SchedulerHandle,
    routes: Offline,
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
            Arc::new(routes),
            SessionEnv {
                root: dir.path().join("sessions"),
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

    // The status of a `setPreferences` of `json`.
    async fn set_preferences(&self, json: &str) -> StatusCode {
        self.post(
            "/api/v2/app/setPreferences",
            &format!("json={}", url_encode(json)),
        )
        .await
        .status
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
        infohash_v1: Some("ab".repeat(20)),
        infohash_v2: None,
        version: None,
        magnet: format!("magnet:?xt=urn:btih:{}", "ab".repeat(20)),
        source: TorrentSource::Metainfo,
        state,
        detail: None,
        paused_by: None,
        category: None,
        scope: TorrentScope::Default,
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
        upload_limit: None,
        download_limit: None,
        eta_seconds: None,
        peers: 0,
        seeds: 0,
        leechers: 0,
        max_peers: 100,
        swarm_seeds: None,
        swarm_leechers: None,
        reannounce_seconds: None,
        active_seconds: 0,
        download_avg_bps: 0,
        upload_avg_bps: 0,
        wasted_bytes: 0,
        last_seen_complete_ms: None,
        pieces: None,
        created_by: None,
        created_at_ms: None,
        comment: None,
        added_at_ms: 1_700_000_000_000,
        completed_at_ms: None,
        storage_dir: Some("/complete/tv/a".into()),
        seeding_left: None,
        seeding_override: None,
        seeding_inherited: None,
        seeding_policy: SeedingPolicy::default(),
        seeding_source: SeedingSource::Global,
        unpack: true,
        route: None,
        files: Vec::new(),
        sequential: false,
        first_last_piece_first: false,
        file_have: Vec::new(),
        trackers: Vec::new(),
        metadata: Vec::new(),
    }
}

// Each tracker status reads as the client's number for it, with the
// tracker's own counts and message, and who named it.
#[test]
fn a_trackers_status_reads_as_the_clients_number() {
    let tracker = |status: TrackerStatus| TorrentTracker {
        tier: 1,
        url: "http://tracker.invalid/announce".into(),
        named_by: weaver_server_core::torrents::NamedBy::Torrent,
        status,
        updating: false,
        last_announce_ms: None,
        next_announce_ms: None,
        min_announce_ms: None,
        message: None,
        seeders: None,
        leechers: None,
        downloaded: None,
        peers: None,
        fails: 0,
        resolved_by: None,
    };
    let numbers: Vec<Value> = [
        TrackerStatus::Disabled,
        TrackerStatus::NotContacted,
        TrackerStatus::Working,
        TrackerStatus::NotWorking,
        TrackerStatus::TrackerError,
        TrackerStatus::Unreachable,
    ]
    .into_iter()
    .map(|status| tracker_json(&tracker(status), CallerScope::Admin)["status"].clone())
    .collect();
    assert_eq!(numbers, [0, 1, 2, 4, 5, 6]);

    let working = tracker_json(
        &TorrentTracker {
            updating: true,
            seeders: Some(3),
            leechers: Some(5),
            downloaded: Some(9),
            peers: Some(7),
            next_announce_ms: Some(1_800_500),
            min_announce_ms: Some(30_000),
            resolved_by: Some(ResolvedBy::Proxy),
            ..tracker(TrackerStatus::Working)
        },
        CallerScope::Admin,
    );
    assert_eq!(
        working,
        serde_json::json!({
            "url": "http://tracker.invalid/announce",
            "tier": 1,
            "source": "TORRENT",
            "updating": true,
            "status": 2,
            "msg": "",
            "num_peers": 7,
            "num_seeds": 3,
            "num_leeches": 5,
            "num_downloaded": 9,
            "next_announce": 1_800,
            "min_announce": 30,
            "endpoints": [],
            "resolved_by": "PROXY",
        })
    );
    let failed = tracker_json(
        &TorrentTracker {
            message: Some("refused".into()),
            ..tracker(TrackerStatus::NotWorking)
        },
        CallerScope::Admin,
    );
    assert_eq!(
        (
            &failed["status"],
            &failed["num_seeds"],
            &failed["msg"],
            &failed["resolved_by"]
        ),
        (
            &Value::from(4),
            &Value::from(-1),
            &Value::from("refused"),
            &Value::Null
        )
    );
    let added = tracker_json(
        &TorrentTracker {
            named_by: weaver_server_core::torrents::NamedBy::Operator,
            ..tracker(TrackerStatus::Working)
        },
        CallerScope::Admin,
    );
    assert_eq!(added["source"], "OPERATOR");
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
        priority: weaver_server_core::torrents::FilePriority::Normal,
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
    // Three pieces of 1 KiB, none in hand; a route that is down runs
    // nothing, wastes nothing and has seen no complete copy.
    assert_eq!(
        [
            &properties["pieces_num"],
            &properties["piece_size"],
            &properties["pieces_have"],
        ],
        [&json!(3), &json!(1024), &json!(0)]
    );
    assert_eq!(
        [
            &properties["time_elapsed"],
            &properties["total_wasted"],
            &properties["last_seen"],
            &properties["dl_speed_avg"],
            &properties["up_speed_avg"],
        ],
        [&json!(0), &json!(0), &json!(-1), &json!(0), &json!(0)]
    );
    assert_eq!(
        properties["nb_connections_limit"],
        TorrentConfig::default().max_peers_total
    );
    // The metainfo names no maker, date or comment.
    assert_eq!(
        [
            &properties["created_by"],
            &properties["creation_date"],
            &properties["comment"],
        ],
        [&json!(""), &json!(-1), &json!("")]
    );
    let files = rig
        .get(&format!("/api/v2/torrents/files?hash={hash}"))
        .await
        .json();
    assert_eq!(files[0]["name"], "one.mkv");
    assert_eq!(files[0]["size"], 3000);
    // The DHT, PeX and LSD rows; the offline rig's route carries DHT.
    let trackers = rig
        .get(&format!("/api/v2/torrents/trackers?hash={hash}"))
        .await
        .json();
    let sticky = |url: &str, status: u8| {
        json!({
            "url": url, "tier": -1, "msg": "", "status": status, "num_peers": 0,
            "num_downloaded": 0, "num_seeds": 0, "num_leeches": 0,
        })
    };
    assert_eq!(
        trackers,
        json!([
            sticky("** [DHT] **", 2),
            sticky("** [PeX] **", 0),
            sticky("** [LSD] **", 0),
        ])
    );
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
async fn the_dht_row_follows_the_torrents_rule_before_its_category() {
    use weaver_server_core::categories::CategoryConfig;
    use weaver_server_core::settings::model::TrackerRule;

    let rig = rig_on(
        true,
        RuntimeSecurityConfig::default(),
        scheduler(),
        Offline {
            stream_only: Some(TorrentScope::TrackerRule(1)),
            ..Default::default()
        },
    )
    .await;
    let torrents = rig.context.inner.torrents.as_ref().unwrap();
    torrents
        .set_settings(TorrentSettings {
            complete_dir: rig._dir.path().join("complete"),
            categories: vec![CategoryConfig {
                id: 7,
                name: "tv".into(),
                dest_dir: None,
                aliases: String::new(),
                seeding: None,
                torrent_route: true,
                unpack: None,
            }],
            seeding: None,
            torrent: TorrentConfig::default(),
        })
        .await;
    let mut rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "tracker.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                discovery: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
            },
            0,
        )
        .unwrap();
    assert_eq!(rule.id, 1);
    torrents.reload_tracker_rules().await.unwrap();
    let tracker = "http://tracker.test/announce";
    let mut bytes = format!("d8:announce{}:{tracker}", tracker.len()).into_bytes();
    bytes.extend_from_slice(&metainfo("ruled.mkv", 3000, false)[1..]);
    let added = rig
        .add_file(&bytes, &[("category", "tv"), ("stopped", "true")])
        .await;
    assert_eq!(added.status, StatusCode::OK);
    let listed = rig.torrents().await;
    let hash = listed[0]["hash"].as_str().unwrap();
    let path = format!("/api/v2/torrents/trackers?hash={hash}");
    assert!(torrents.carries_dht(TorrentScope::Category(7)).await);
    let trackers = rig.get(&path).await.json();
    assert_eq!(trackers[0]["url"], "** [DHT] **");
    assert_eq!(trackers[0]["status"], 0);

    rule.torrent_route = false;
    rig.db.update_tracker_rule(&rule, 0).unwrap();
    torrents.reload_tracker_rules().await.unwrap();
    let trackers = rig.get(&path).await.json();
    assert_eq!(trackers[0]["status"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_url_is_fetched_with_the_cookie_and_read_for_what_it_is() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // One server that answers each request with the next body, and hands
    // back the requests it read.
    async fn serve(bodies: Vec<Vec<u8>>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in bodies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buf = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buf).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..read]);
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(head.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
                requests.push(String::from_utf8_lossy(&request).to_ascii_lowercase());
            }
            requests
        });
        (format!("http%3A%2F%2F{addr}%2Fa.torrent"), server)
    }

    let handle = scheduler();
    handle.set_proxy_runtime(
        weaver_server_core::proxies::ProxyRuntime::new(
            Database::open_in_memory().unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap(),
    );
    let rig = rig_on(
        true,
        RuntimeSecurityConfig::default(),
        handle,
        Offline {
            up: true,
            ..Default::default()
        },
    )
    .await;

    let (url, server) = serve(vec![metainfo("fetched.mkv", 4096, false)]).await;
    let added = rig
        .post(
            "/api/v2/torrents/add",
            &format!("urls={url}&cookie=uid%3D1%3B%20pass%3D2"),
        )
        .await;
    assert_eq!((added.status, added.body.as_str()), (StatusCode::OK, "Ok."));
    let requests = server.await.unwrap();
    assert!(
        requests[0].contains("cookie: uid=1; pass=2"),
        "{requests:?}"
    );
    let listed = rig.torrents().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["name"], "fetched.mkv");

    // A body that is not a torrent is refused for what it is, and the cookie
    // went with its request only.
    let (url, server) = serve(vec![b"<?xml version=\"1.0\"?><nzb/>".to_vec()]).await;
    let refused = rig
        .post("/api/v2/torrents/add", &format!("urls={url}"))
        .await;
    assert_eq!(
        (refused.status, refused.body.as_str()),
        (StatusCode::UNSUPPORTED_MEDIA_TYPE, "not a torrent")
    );
    let requests = server.await.unwrap();
    assert!(!requests[0].contains("cookie:"), "{requests:?}");

    // A file is read for what it holds, whatever it is called.
    let not_torrent = rig.add_file(b"<nzb/>", &[]).await;
    assert_eq!(not_torrent.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(rig.torrents().await.len(), 1);
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

// qBittorrent's `torrents/recheck` reaches the service's recheck for each
// torrent it names: an unknown hash is skipped, and a torrent the offline
// rig holds in no session has nothing to check it, which conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recheck_reaches_each_named_torrent() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let unknown = rig
        .post(
            "/api/v2/torrents/recheck",
            &format!("hashes={}", "0".repeat(40)),
        )
        .await;
    assert_eq!(unknown.status, StatusCode::OK);
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let held = rig
        .post("/api/v2/torrents/recheck", &format!("hashes={hash}"))
        .await;
    assert_eq!(
        (held.status, held.body.as_str()),
        (
            StatusCode::CONFLICT,
            "the torrent is in no session to check it"
        )
    );
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
}

// qBittorrent `TorrentsController::setLocationAction`: the torrent moves
// under the location and is listed there; an empty location is a bad
// request, and one no directory can be made under is a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_location_moves_the_torrent_under_the_location() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let elsewhere = rig._dir.path().join("elsewhere");
    let moved = rig
        .post(
            "/api/v2/torrents/setLocation",
            &format!("hashes=all&location={}", elsewhere.to_string_lossy()),
        )
        .await;
    assert_eq!(moved.status, StatusCode::OK);
    let listed = rig.torrents().await;
    let content = PathBuf::from(listed[0]["content_path"].as_str().unwrap());
    assert!(content.starts_with(&elsewhere));
    assert!(content.parent().unwrap().is_dir());

    let empty = rig
        .post("/api/v2/torrents/setLocation", "hashes=all&location=%20")
        .await;
    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
    let file = rig._dir.path().join("a-file");
    std::fs::write(&file, b"").unwrap();
    let unmakeable = rig
        .post(
            "/api/v2/torrents/setLocation",
            &format!(
                "hashes=all&location={}",
                file.join("under").to_string_lossy()
            ),
        )
        .await;
    assert_eq!(unmakeable.status, StatusCode::CONFLICT);
    let listed = rig.torrents().await;
    assert_eq!(
        PathBuf::from(listed[0]["content_path"].as_str().unwrap()),
        content
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
    assert_eq!(
        before["max_connec_per_torrent"],
        TorrentConfig::default().max_peers_total
    );
    assert_eq!(before["max_connec"], 200);
    assert_eq!(before["max_uploads"], 8);
    assert_eq!(before["connection_speed"], 30);
    assert_eq!(before["enable_multi_connections_from_same_ip"], false);
    assert_eq!(before["bittorrent_protocol"], 1);
    assert_eq!(
        before["dht_bootstrap_nodes"],
        "dht.transmissionbt.com:6881,dht.libtorrent.org:25401"
    );
    assert_eq!(before["ip_filter_enabled"], false);
    assert_eq!(before["ip_filter_path"], "");
    assert_eq!(before["stop_tracker_timeout"], 5);
    assert_eq!(before["banned_IPs"], "");
    assert_eq!(before["ip_filter_trackers"], false);
    assert_eq!(before["block_peers_on_privileged_ports"], false);

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

    let json = r#"{"max_connec":400,"max_connec_per_torrent":80,"max_uploads":12,"connection_speed":50,"enable_multi_connections_from_same_ip":true,"bittorrent_protocol":0,"dht_bootstrap_nodes":" router.example:6881 ","ip_filter_enabled":true,"ip_filter_path":"/var/lib/weaver/level1.p2p","stop_tracker_timeout":0}"#;
    assert_eq!(rig.set_preferences(json).await, StatusCode::OK);
    let after = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(
        [
            &after["max_connec"],
            &after["max_connec_per_torrent"],
            &after["max_uploads"],
            &after["connection_speed"],
            &after["stop_tracker_timeout"],
            &after["bittorrent_protocol"],
        ],
        [
            &json!(400),
            &json!(80),
            &json!(12),
            &json!(50),
            &json!(0),
            &json!(0)
        ]
    );
    assert_eq!(after["enable_multi_connections_from_same_ip"], true);
    assert_eq!(after["dht_bootstrap_nodes"], "router.example:6881");
    assert_eq!(after["ip_filter_enabled"], true);
    assert_eq!(after["ip_filter_path"], "/var/lib/weaver/level1.p2p");
    let saved = rig.config.read().await.torrent.clone();
    assert_eq!(
        saved.peer_protocol,
        weaver_server_core::settings::TorrentPeerProtocol::Both
    );

    // qBittorrent's zero or less is no limit for connections and upload
    // slots, and a negative stop tracker timeout is none.
    let json = r#"{"max_connec":-1,"max_uploads":0,"stop_tracker_timeout":-1}"#;
    assert_eq!(rig.set_preferences(json).await, StatusCode::OK);
    let after = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(after["max_connec"], -1);
    assert_eq!(after["max_uploads"], -1);
    assert_eq!(after["stop_tracker_timeout"], 0);
    let saved = rig.config.read().await.torrent.clone();
    assert_eq!(saved.connections_limit, TorrentLimit::Unlimited);
    assert_eq!(saved.unchoke_slots_limit, TorrentLimit::Unlimited);

    // Every other count is one at least, and a request with one that is not
    // takes nothing.
    for key in [
        "max_active_downloads",
        "max_connec_per_torrent",
        "connection_speed",
    ] {
        for value in [0, -1] {
            let json = format!(r#"{{"{key}":{value},"dl_limit":4096,"max_uploads":3}}"#);
            assert_eq!(
                rig.set_preferences(&json).await,
                StatusCode::BAD_REQUEST,
                "{json}"
            );
            assert_eq!(rig.config.read().await.torrent, saved, "{json}");
            assert_eq!(
                rig.config.read().await.bandwidth.download_bps,
                Some(1_048_576)
            );
        }
    }

    // The switch and the path are two settings: either is taken alone, in
    // either order, and a switched off filter keeps its path.
    assert_eq!(
        rig.set_preferences(r#"{"ip_filter_enabled":false}"#).await,
        StatusCode::OK
    );
    let off = rig.config.read().await.torrent.clone();
    assert!(!off.blocklist_enabled);
    assert_eq!(off.blocklist.as_deref(), Some("/var/lib/weaver/level1.p2p"));
    let after = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(after["ip_filter_path"], "/var/lib/weaver/level1.p2p");

    assert_eq!(
        rig.set_preferences(r#"{"ip_filter_path":"/var/lib/weaver/level2.p2p"}"#)
            .await,
        StatusCode::OK
    );
    let path = rig.config.read().await.torrent.clone();
    assert!(!path.blocklist_enabled);
    assert_eq!(
        path.blocklist.as_deref(),
        Some("/var/lib/weaver/level2.p2p")
    );

    assert_eq!(
        rig.set_preferences(r#"{"ip_filter_enabled":true}"#).await,
        StatusCode::OK
    );
    let on = rig.config.read().await.torrent.clone();
    assert!(on.blocklist_enabled);
    assert_eq!(on.blocklist.as_deref(), Some("/var/lib/weaver/level2.p2p"));

    // The banned addresses are one a line, blank lines skipped, kept in one
    // form and sorted (`SessionImpl::setBannedIPs`); an entry that names no
    // address refuses the request.
    let json = r#"{"banned_IPs":"192.0.2.7\n\n10.0.0.0/24\n","ip_filter_trackers":true,"block_peers_on_privileged_ports":true}"#;
    assert_eq!(rig.set_preferences(json).await, StatusCode::OK);
    let after = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(after["banned_IPs"], "10.0.0.0 - 10.0.0.255\n192.0.2.7");
    assert_eq!(after["ip_filter_trackers"], true);
    assert_eq!(after["block_peers_on_privileged_ports"], true);
    let banned = rig.config.read().await.torrent.clone();
    assert!(banned.filter_trackers && banned.block_privileged_ports);
    assert_eq!(rig.db.load_config().unwrap().torrent, banned);
    let json = r#"{"banned_IPs":"192.0.2.8\nlists.example"}"#;
    assert_eq!(rig.set_preferences(json).await, StatusCode::BAD_REQUEST);
    assert_eq!(rig.config.read().await.torrent, banned);

    // A Windows path is a path, not a URL of scheme `c`.
    let json = r#"{"ip_filter_path":"C:\\lists\\level1.p2p"}"#;
    assert_eq!(rig.set_preferences(json).await, StatusCode::OK);
    let windows = rig.config.read().await.torrent.clone();
    assert_eq!(windows.blocklist.as_deref(), Some(r"C:\lists\level1.p2p"));
    assert_eq!(
        windows.blocklist_source(),
        Some(BlocklistSource::File(r"C:\lists\level1.p2p".into()))
    );

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
    // The torrents learn of the setting the way the server hands it on.
    let settings = TorrentSettings::of(&*rig.config.read().await);
    rig.context
        .inner
        .torrents
        .as_ref()
        .unwrap()
        .set_settings(settings)
        .await;
    let refused = rig
        .post("/api/v2/torrents/setShareLimits", &limits("ratioLimit=-1"))
        .await;
    assert_eq!(refused.status, StatusCode::CONFLICT);
    let invalid = rig
        .post("/api/v2/torrents/setShareLimits", &limits("ratioLimit=-3"))
        .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
}

// Each limit is set alone and read back as the client reads it: 0 and any
// negative number are no limit, which the list and the map read as 0 and
// the properties as -1; a hash no torrent has reads -1.
#[tokio::test]
async fn a_torrents_rate_limits_are_set_and_read_back() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    rig.add_file(&metainfo("two.mkv", 3000, false), &[]).await;
    let mut hashes: Vec<String> = rig
        .torrents()
        .await
        .iter()
        .map(|torrent| torrent["hash"].as_str().unwrap().to_owned())
        .collect();
    hashes.sort();
    let (one, two) = (hashes[0].clone(), hashes[1].clone());
    let row =
        |rows: Vec<Value>, hash: &str| rows.into_iter().find(|row| row["hash"] == hash).unwrap();
    let missing = "cd".repeat(20);

    let first = row(rig.torrents().await, &one);
    assert_eq!(
        (first["up_limit"].clone(), first["dl_limit"].clone()),
        (json!(0), json!(0))
    );

    let set = rig
        .post(
            "/api/v2/torrents/setUploadLimit",
            &format!("hashes={one}&limit=65536"),
        )
        .await;
    assert_eq!(set.status, StatusCode::OK);
    rig.post(
        "/api/v2/torrents/setDownloadLimit",
        &format!("hashes={one}|{two}&limit=1048576"),
    )
    .await;
    let first = row(rig.torrents().await, &one);
    assert_eq!(first["up_limit"], 65_536);
    assert_eq!(first["dl_limit"], 1_048_576);
    let second = row(rig.torrents().await, &two);
    assert_eq!(second["up_limit"], 0);
    assert_eq!(second["dl_limit"], 1_048_576);

    // The map is keyed by each hash as it was sent.
    let upper = one.to_ascii_uppercase();
    let upload = rig
        .post(
            "/api/v2/torrents/uploadLimit",
            &format!("hashes={upper}|{two}|{missing}"),
        )
        .await
        .json();
    assert_eq!(upload, json!({ &upper: 65_536, &two: 0, &missing: -1 }));
    let download = rig
        .post("/api/v2/torrents/downloadLimit", "hashes=all")
        .await
        .json();
    assert_eq!(download, json!({ &one: 1_048_576, &two: 1_048_576 }));

    let properties = rig
        .get(&format!("/api/v2/torrents/properties?hash={two}"))
        .await
        .json();
    assert_eq!(properties["up_limit"], -1);
    assert_eq!(properties["dl_limit"], 1_048_576);

    // 0 and -1 are both no limit, and `all` reaches every torrent.
    rig.post("/api/v2/torrents/setDownloadLimit", "hashes=all&limit=0")
        .await;
    rig.post(
        "/api/v2/torrents/setUploadLimit",
        &format!("hashes={one}&limit=-1"),
    )
    .await;
    let all = rig
        .post("/api/v2/torrents/downloadLimit", "hashes=all")
        .await
        .json();
    assert_eq!(all, json!({ &one: 0, &two: 0 }));
    let properties = rig
        .get(&format!("/api/v2/torrents/properties?hash={one}"))
        .await
        .json();
    assert_eq!(
        (
            properties["up_limit"].clone(),
            properties["dl_limit"].clone()
        ),
        (json!(-1), json!(-1))
    );

    let invalid = rig
        .post(
            "/api/v2/torrents/setUploadLimit",
            &format!("hashes={one}&limit=fast"),
        )
        .await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
}

// A torrent held closed has no reading the client knows: its field and its
// entry are left out, never read as no limit.
#[tokio::test]
async fn a_closed_torrent_limit_is_never_read_as_no_limit() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let torrents = rig.context.inner.torrents.as_ref().unwrap();
    let job_id = torrents.torrents(None).await[0].job_id;
    torrents
        .set_rate_limits(job_id, Some(Some(0)), None)
        .await
        .unwrap();
    let row = &rig.torrents().await[0];
    assert!(row.get("up_limit").is_none());
    assert_eq!(row["dl_limit"], 0);
    let hash = row["hash"].as_str().unwrap().to_owned();
    let upload = rig
        .post("/api/v2/torrents/uploadLimit", &format!("hashes={hash}"))
        .await
        .json();
    assert_eq!(upload, json!({}));
    let properties = rig
        .get(&format!("/api/v2/torrents/properties?hash={hash}"))
        .await
        .json();
    assert!(properties.get("up_limit").is_none());
    assert_eq!(properties["dl_limit"], -1);
}

#[tokio::test]
async fn the_stub_endpoints_answer_and_a_url_must_be_a_magnet() {
    let rig = rig().await;
    let top = rig.post("/api/v2/torrents/topPrio", "hashes=all").await;
    assert_eq!(
        (top.status, top.body.as_str()),
        (StatusCode::CONFLICT, "queueing is off")
    );
    for action in ["addTags", "removeTags"] {
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
        (StatusCode::CONFLICT, "torrent request has no route")
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

#[test]
fn a_peer_id_reads_as_its_first_eight_bytes() {
    assert_eq!(peer_id_client(&[0; 20]), "");
    let mut id = *b"-qB4650-abcdefghijkl";
    assert_eq!(peer_id_client(&id), "-qB4650-");
    id[3] = 0x01;
    assert_eq!(peer_id_client(&id), "Unknown");
}

// qBittorrent `SyncController::torrentPeersAction` and
// `PeerInfo::determineFlags`: a peer as the listing shows it, its letters
// in qBittorrent's order.
#[test]
fn a_listed_peer_shows_its_letters_connection_and_contribution() {
    let peer_id = IDENTITY.identity().peer_id;
    let peer = PeerInfo {
        addr: "203.0.113.1:6881".parse().unwrap(),
        client: "Weaver 1.0".to_owned(),
        peer_id,
        connection: ConnectionKind::Utp,
        outgoing: false,
        encryption: Encryption::HeaderOnly,
        flags: weaver_server_core::torrents::PeerFlags {
            interesting: true,
            remote_interested: true,
            optimistic_unchoke: true,
            holepunched: true,
            ..Default::default()
        },
        sources: [
            PeerSourceFlag::Dht,
            PeerSourceFlag::Pex,
            PeerSourceFlag::Lsd,
        ]
        .into_iter()
        .collect(),
        pieces: 2,
        progress: 0.5,
        download_rate: 10,
        upload_rate: 20,
        downloaded: 30,
        uploaded: 250,
    };
    let listed = peer_json(&peer, 1000);
    assert_eq!(listed["flags"], "D U O I H X L e P h");
    let described = listed["flags_desc"].as_str().unwrap();
    assert_eq!(described.lines().count(), 10);
    assert_eq!(
        described.lines().next(),
        Some("D = interested, unchoked by the peer")
    );
    assert_eq!(listed["connection"], "μTP");
    // 250 bytes of the 500 the peer has came from us.
    assert_eq!(listed["contribution"], 0.5);
    assert_eq!(
        listed["peer_id_client"],
        peer_id_client(&peer_id.0).as_str()
    );
    assert_eq!(listed["client"], "Weaver 1.0");
    assert_eq!(
        (listed["ip"].as_str(), listed["port"].as_u64()),
        (Some("203.0.113.1"), Some(6881))
    );
    assert_eq!(
        (listed["dl_speed"].as_u64(), listed["up_speed"].as_u64()),
        (Some(10), Some(20))
    );

    // Choked both ways, outgoing, over TCP, plaintext, named by a tracker:
    // only what qBittorrent marks.
    let quiet = PeerInfo {
        connection: ConnectionKind::Tcp,
        outgoing: true,
        encryption: Encryption::Rc4,
        flags: weaver_server_core::torrents::PeerFlags {
            choked: true,
            remote_choked: true,
            snubbed: true,
            ..Default::default()
        },
        sources: PeerSourceFlag::Tracker.into(),
        uploaded: 0,
        ..peer
    };
    let listed = peer_json(&quiet, 1000);
    assert_eq!(listed["flags"], "S E");
    assert_eq!(listed["connection"], "BT");
    assert_eq!(listed["contribution"], 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peers_are_listed_by_hash_and_a_ban_reaches_every_torrent() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    rig.add_file(&metainfo("two.mkv", 3000, false), &[]).await;
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();

    let listed = rig
        .get(&format!("/api/v2/sync/torrentPeers?hash={hash}&rid=0"))
        .await
        .json();
    assert_eq!(listed["full_update"], true);
    assert_eq!(listed["peers"], serde_json::json!({}));
    assert_eq!(
        rig.get("/api/v2/sync/torrentPeers?hash=00").await.status,
        StatusCode::NOT_FOUND
    );

    let banned = rig
        .post(
            "/api/v2/transfer/banPeers",
            "peers=127.0.0.3%3A6881%7C%5B%3A%3A1%5D%3A6881%7Cnonsense",
        )
        .await;
    assert_eq!(banned.status, StatusCode::OK);
    let views = rig.context.views().await;
    assert_eq!(views.len(), 2);
    for job in views.iter().map(|view| view.job_id) {
        let banned: Vec<String> = rig
            .db
            .torrent_peers(job)
            .unwrap()
            .banned
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(banned, ["127.0.0.3", "::1"], "torrent {job:?}");
    }
    // Each joins the banned addresses every session filters, once
    // (`SessionImpl::banIP`).
    assert_eq!(
        rig.post("/api/v2/transfer/banPeers", "peers=127.0.0.3%3A51413")
            .await
            .status,
        StatusCode::OK
    );
    let torrent = rig.config.read().await.torrent.clone();
    assert_eq!(torrent.banned_ips, ["127.0.0.3", "::1"]);
    assert_eq!(rig.db.load_config().unwrap().torrent, torrent);
    let preferences = rig.get("/api/v2/app/preferences").await.json();
    assert_eq!(preferences["banned_IPs"], "127.0.0.3\n::1");
    assert_eq!(
        rig.post("/api/v2/transfer/banPeers", "").await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        rig.post_as("/api/v2/transfer/banPeers", "peers=127.0.0.3%3A1", READER)
            .await
            .status,
        StatusCode::FORBIDDEN
    );
}

// A file's level reads as the client's number and back; the client's other
// numbers are no level (qBittorrent `downloadpriority.h`).
#[test]
fn a_files_level_reads_as_the_clients_number_and_back() {
    use weaver_server_core::torrents::FilePriority::*;
    for (level, number) in [(Ignored, 0), (Normal, 1), (High, 6), (Maximum, 7)] {
        assert_eq!(qbt_priority(level), number);
        assert_eq!(level_of(&number.to_string()), Some(level));
    }
    for number in ["2", "4", "8", "-1", "", "high"] {
        assert_eq!(level_of(number), None, "{number:?}");
    }
}

// filePrio sets the named files' level, which files reports; the two
// toggles flip each named torrent, which info and properties report.
#[tokio::test(start_paused = true)]
async fn file_levels_and_picking_toggles_are_set_and_reported() {
    let rig = rig().await;
    rig.add_file(&metainfo("one.mkv", 3000, false), &[]).await;
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let level = async || {
        rig.get(&format!("/api/v2/torrents/files?hash={hash}"))
            .await
            .json()[0]["priority"]
            .clone()
    };
    let picking = async || {
        let info = rig.torrents().await[0].clone();
        let properties = rig
            .get(&format!("/api/v2/torrents/properties?hash={hash}"))
            .await
            .json();
        assert_eq!(info["seq_dl"], properties["seq_dl"]);
        assert_eq!(info["f_l_piece_prio"], properties["f_l_piece_prio"]);
        (info["seq_dl"].clone(), info["f_l_piece_prio"].clone())
    };
    assert_eq!(level().await, 1);
    assert_eq!(picking().await, (json!(false), json!(false)));

    for number in [6, 7, 0, 1] {
        let set = rig
            .post(
                "/api/v2/torrents/filePrio",
                &format!("hash={hash}&id=0&priority={number}"),
            )
            .await;
        assert_eq!(set.status, StatusCode::OK);
        assert_eq!(level().await, number);
    }
    let refused = [
        (
            format!("hash={hash}&id=0&priority=2"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("hash={hash}&id=0|x&priority=6"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("hash={hash}&id=0|1&priority=6"),
            StatusCode::CONFLICT,
        ),
        (
            format!("hash={hash}&id=-1&priority=6"),
            StatusCode::CONFLICT,
        ),
        ("id=0&priority=6".to_owned(), StatusCode::BAD_REQUEST),
        (
            format!("hash={}&id=0&priority=6", "0".repeat(40)),
            StatusCode::NOT_FOUND,
        ),
    ];
    for (form, status) in refused {
        assert_eq!(
            rig.post("/api/v2/torrents/filePrio", &form).await.status,
            status,
            "{form}"
        );
    }
    // Nothing refused changed the level.
    assert_eq!(level().await, 1);
    let read = rig
        .post_as(
            "/api/v2/torrents/filePrio",
            &format!("hash={hash}&id=0&priority=6"),
            READER,
        )
        .await;
    assert_eq!(read.status, StatusCode::FORBIDDEN);

    rig.post(
        "/api/v2/torrents/toggleSequentialDownload",
        &format!("hashes={hash}"),
    )
    .await;
    assert_eq!(picking().await, (json!(true), json!(false)));
    rig.post(
        "/api/v2/torrents/toggleFirstLastPiecePrio",
        &format!("hashes={hash}"),
    )
    .await;
    assert_eq!(picking().await, (json!(true), json!(true)));
    rig.post(
        "/api/v2/torrents/toggleSequentialDownload",
        &format!("hashes={hash}"),
    )
    .await;
    assert_eq!(picking().await, (json!(false), json!(true)));
}

// Trackers are added by tier, edited, removed and reannounced to by hash,
// as qBittorrent's endpoints take them, and an edit naming a tracker the
// torrent lacks or one it has already conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trackers_are_added_edited_removed_and_reannounced() {
    let rig = rig().await;
    let added = rig.add_file(&metainfo("one.mkv", 3000, true), &[]).await;
    assert_eq!(added.status, StatusCode::OK);
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let listed = || async {
        let rows = rig
            .get(&format!("/api/v2/torrents/trackers?hash={hash}"))
            .await
            .json();
        rows.as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row["tier"].as_i64().unwrap(),
                    row["url"].as_str().unwrap().to_owned(),
                    row["status"].as_u64().unwrap(),
                    row["msg"].as_str().unwrap().to_owned(),
                )
            })
            .collect::<Vec<_>>()
    };
    let a = "http://a.invalid/announce";
    let b = "udp://b.invalid:80";
    let c = "http://c.invalid/announce";

    // A blank line starts the next tier.
    let reply = rig
        .post(
            "/api/v2/torrents/addTrackers",
            &format!(
                "hash={hash}&urls={}",
                url_encode(&format!("{a}\n{b}\n\n{c}\n"))
            ),
        )
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    let private = "This torrent is private".to_owned();
    let row = |tier: i64, url: &str| (tier, url.to_owned(), 1, String::new());
    assert_eq!(
        listed().await,
        [
            (-1, "** [DHT] **".into(), 0, private.clone()),
            (-1, "** [PeX] **".into(), 0, private.clone()),
            (-1, "** [LSD] **".into(), 0, private.clone()),
            row(0, a),
            row(0, b),
            row(1, c),
        ]
    );

    let edit = |form: String| {
        let rig = &rig;
        async move { rig.post("/api/v2/torrents/editTracker", &form).await }
    };
    let missing = edit(format!(
        "hash={hash}&url={}&newUrl={}",
        url_encode("http://d.invalid/announce"),
        url_encode("http://e.invalid/announce")
    ))
    .await;
    assert_eq!(
        (missing.status, missing.body.as_str()),
        (StatusCode::CONFLICT, "Tracker not found")
    );
    let taken = edit(format!(
        "hash={hash}&url={}&newUrl={}",
        url_encode(a),
        url_encode(c)
    ))
    .await;
    assert_eq!(
        (taken.status, taken.body.as_str()),
        (StatusCode::CONFLICT, "New tracker URL already exists")
    );
    let invalid = edit(format!("hash={hash}&url={}&newUrl=nothing", url_encode(a))).await;
    assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
    let bare = edit(format!("hash={hash}&url={}", url_encode(a))).await;
    assert_eq!(bare.status, StatusCode::BAD_REQUEST);
    let unknown = edit(format!(
        "hash={}&url={}&tier=2",
        "0".repeat(40),
        url_encode(a)
    ))
    .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    let moved = edit(format!(
        "hash={hash}&url={}&newUrl={}&tier=3",
        url_encode(a),
        url_encode("http://d.invalid/announce")
    ))
    .await;
    assert_eq!(moved.status, StatusCode::OK);
    assert_eq!(
        listed().await[3..],
        [row(0, b), row(1, c), row(3, "http://d.invalid/announce")]
    );

    // `urls` parts may be percent-encoded once more.
    let removed = rig
        .post(
            "/api/v2/torrents/removeTrackers",
            &format!(
                "hash=*&urls={}",
                url_encode(&format!("{}|{c}", url_encode(b)))
            ),
        )
        .await;
    assert_eq!(removed.status, StatusCode::OK);
    assert_eq!(listed().await[3..], [row(3, "http://d.invalid/announce")]);

    for form in [
        format!("hashes={hash}"),
        "hashes=all".to_owned(),
        format!(
            "hashes={hash}|{}&urls={}",
            "0".repeat(40),
            url_encode("http://d.invalid/announce")
        ),
    ] {
        let reply = rig.post("/api/v2/torrents/reannounce", &form).await;
        assert_eq!(reply.status, StatusCode::OK, "{form}");
    }

    // A key that reads cannot change trackers.
    let refused = rig
        .post_as(
            "/api/v2/torrents/removeTrackers",
            &format!("hash={hash}&urls={}", url_encode(a)),
            READER,
        )
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_read_key_sees_no_trackers_passkey() {
    const PRIVATE: &str = "https://tracker.invalid:8443/0123passkey/announce?pk=s3cret";
    let rig = rig().await;
    let added = rig.add_file(&metainfo("one.mkv", 3000, true), &[]).await;
    assert_eq!(added.status, StatusCode::OK);
    let hash = rig.torrents().await[0]["hash"].as_str().unwrap().to_owned();
    let reply = rig
        .post(
            "/api/v2/torrents/addTrackers",
            &format!("hash={hash}&urls={}", url_encode(PRIVATE)),
        )
        .await;
    assert_eq!(reply.status, StatusCode::OK);
    let urls = |key: &'static str| {
        let (rig, hash) = (&rig, &hash);
        async move {
            let rows = rig
                .send(
                    Request::get(format!("/api/v2/torrents/trackers?hash={hash}"))
                        .header("x-api-key", key)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .json();
            rows.as_array().unwrap()[3..]
                .iter()
                .map(|row| row["url"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(urls(READER).await, ["https://tracker.invalid:8443/…"]);
    assert_eq!(urls(ADMIN).await, [PRIVATE]);
}

// A request with the login cookie that changes anything must come from a
// page of this listener, whatever its method; one that only reads, or one
// carrying its key, need not.
#[tokio::test]
async fn a_cookie_changes_nothing_from_another_origin() {
    let rig = rig().await;
    let cookie = rig.send(login(ADMIN)).await.cookie.unwrap();
    let sid = cookie.split(';').next().unwrap().to_owned();
    let send = |path: &'static str,
                credential: (&'static str, String),
                headers: &[(&'static str, &'static str)]| {
        let mut request = Request::get(path)
            .header(header::HOST, "weaver.test:8080")
            .header(credential.0, credential.1);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let rig = &rig;
        let request = request.body(Body::empty()).unwrap();
        async move { rig.send(request).await.status }
    };
    let by_cookie = || (header::COOKIE.as_str(), sid.clone());
    let pause = "/api/v2/torrents/pause?hashes=all";
    let read = "/api/v2/torrents/info";
    let cases: [(&str, &[(&str, &str)], StatusCode); 9] = [
        (
            pause,
            &[("sec-fetch-site", "cross-site")],
            StatusCode::FORBIDDEN,
        ),
        (
            pause,
            &[("sec-fetch-site", "same-site")],
            StatusCode::FORBIDDEN,
        ),
        (pause, &[("sec-fetch-site", "same-origin")], StatusCode::OK),
        (pause, &[("sec-fetch-site", "none")], StatusCode::OK),
        (
            pause,
            &[("origin", "http://elsewhere.test:8080")],
            StatusCode::FORBIDDEN,
        ),
        (
            pause,
            &[("referer", "http://weaver.test:9090/page")],
            StatusCode::FORBIDDEN,
        ),
        (
            pause,
            &[("origin", "http://weaver.test:8080")],
            StatusCode::OK,
        ),
        (
            pause,
            &[("referer", "http://weaver.test:8080/page")],
            StatusCode::OK,
        ),
        (read, &[("sec-fetch-site", "cross-site")], StatusCode::OK),
    ];
    for (path, headers, status) in cases {
        assert_eq!(
            send(path, by_cookie(), headers).await,
            status,
            "{path} {headers:?}"
        );
    }
    // An automation client sends none of them.
    assert_eq!(send(pause, by_cookie(), &[]).await, StatusCode::OK);
    // A key is no cookie a page can borrow.
    let by_key = || ("x-api-key", ADMIN.to_owned());
    assert_eq!(
        send(pause, by_key(), &[("sec-fetch-site", "cross-site")]).await,
        StatusCode::OK
    );
    assert_eq!(
        send(pause, by_key(), &[("origin", "http://elsewhere.test:8080")]).await,
        StatusCode::OK
    );
    // Nor does another page end the session.
    let logout = "/api/v2/auth/logout";
    assert_eq!(
        send(logout, by_cookie(), &[("sec-fetch-site", "cross-site")]).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(send(read, by_cookie(), &[]).await, StatusCode::OK);
    assert_eq!(
        send(logout, by_cookie(), &[("sec-fetch-site", "same-origin")]).await,
        StatusCode::OK
    );
    assert_eq!(send(read, by_cookie(), &[]).await, StatusCode::FORBIDDEN);
}
