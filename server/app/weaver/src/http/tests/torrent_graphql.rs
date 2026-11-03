// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The GraphQL endpoint of a server that runs torrents: the schema carries the
// torrent client the server started with.

use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock as StdRwLock;

use base64::Engine;
use weaver_server_api::{SchemaContext, SystemRuntimeContext, build_schema};
use weaver_server_core::proxies::{
    EgressBinding, LegPath, ResolvedBy, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::{
    Leaf, SeedingAction, SeedingGoalMode, SeedingPolicy, TorrentConfig,
};
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    IpRanges, OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};

use super::*;

const KEY: &str = "torrent-admin-key";

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
            names: Some(ResolvedBy::Host),
            down: Some(
                weaver_server_core::torrents::sessions::RouteFailure::leg_down("offline".into()),
            ),
        })
    }

    fn leg_down(
        &self,
        _: &RoutePlan,
    ) -> Option<weaver_server_core::torrents::sessions::RouteFailure> {
        Some(weaver_server_core::torrents::sessions::RouteFailure::leg_down("offline".into()))
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

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo, base64. Nothing is fetched, so the piece
// hashes only need the right length.
fn metainfo(name: &str, length: usize) -> String {
    let mut info = b"d".to_vec();
    bstr(&mut info, b"length");
    info.extend_from_slice(format!("i{length}e").as_bytes());
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    bstr(&mut info, b"piece length");
    info.extend_from_slice(b"i1024e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &vec![0u8; length.div_ceil(1024) * 20]);
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    base64::engine::general_purpose::STANDARD.encode(out)
}

fn profile() -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: 4,
            logical_cores: 8,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 8 << 30,
            available_bytes: 4 << 30,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Unknown,
            filesystem: FilesystemType::Unknown(String::new()),
            sequential_write_mbps: 0.0,
            random_read_iops: 0.0,
            same_filesystem: true,
        },
    }
}

// The endpoint as the server mounts it, over a schema built with `torrents`.
async fn endpoint(dir: &std::path::Path, db: Database, torrents: TorrentService) -> Router {
    let handle = test_scheduler_handle();
    let config = test_config();
    let server_transfer_policy = Arc::new(
        weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            db.clone(),
            &[],
        )
        .unwrap(),
    );
    let proxies = weaver_server_core::proxies::ProxyRuntime::new(
        db.clone(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    handle.set_proxy_runtime(proxies.clone());
    let update_check =
        weaver_server_core::update_check::UpdateCheckService::new(db.clone(), proxies.clone());
    let application_upgrade =
        weaver_server_core::application_upgrade::ApplicationUpgradeService::new(
            db.clone(),
            update_check.clone(),
            dir,
            weaver_server_core::application_upgrade::collect_installation_assessment(),
            None,
            proxies,
        );
    let auth_cache =
        LoginAuthCache::from_credentials(None, db.get_or_create_jwt_signing_secret().unwrap());
    let api_key_cache = ApiKeyCache::default();
    api_key_cache.upsert(ApiKeyAuthRow {
        key_hash: hash_api_key(KEY),
        id: 1,
        scope: "admin".to_owned(),
    });
    let schema = build_schema(SchemaContext {
        handle: handle.handle.clone(),
        pipeline_state: handle.state.clone(),
        scheduled_resume: weaver_server_api::ScheduledResumeCoordinator::new(
            db.clone(),
            handle.handle.clone(),
        ),
        config: config.clone(),
        db: db.clone(),
        server_transfer_policy,
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        security: RuntimeSecurityConfig::default(),
        rss: weaver_server_api::RssService::new(handle.handle.clone(), config.clone(), db.clone()),
        watch_folder: weaver_server_core::watch_folder::WatchFolderService::new(
            db.clone(),
            handle.handle.clone(),
            config.clone(),
        ),
        update_check,
        application_upgrade,
        schedules: Arc::new(RwLock::new(vec![])),
        log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer::with_default_capacity(),
        system_runtime: SystemRuntimeContext {
            profile: Arc::new(StdRwLock::new(profile())),
            started_at: std::time::Instant::now(),
        },
        nntp_pool: None,
        spawn_history_delete_worker: false,
        post_processing_executor: None,
        torrents: Some(torrents),
        usage_recorder: None,
    });
    Router::new()
        .route("/graphql", post(super::super::graphql::graphql_handler))
        .layer(Extension(schema))
        .layer(Extension(RequestAuthContext {
            db,
            auth_cache,
            api_key_cache,
            session_token: SessionToken(Arc::new("browser-session-token".to_owned())),
            security: Arc::new(RuntimeSecurityConfig::default()),
        }))
}

async fn graphql(app: &Router, query: &str) -> serde_json::Value {
    let body = serde_json::json!({ "query": query }).to_string();
    let response = app
        .clone()
        .oneshot(
            Request::post("/graphql")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-api-key", KEY)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// A torrent client under the global seeding policy `seeding`, started.
async fn started(
    dir: &std::path::Path,
    db: &Database,
    seeding: Option<SeedingPolicy>,
) -> TorrentService {
    let torrents = TorrentService::new(
        db.clone(),
        Sessions::new(
            Arc::new(Offline),
            SessionEnv {
                root: dir.join("sessions"),
            },
            db.clone(),
        ),
        TorrentSettings {
            upload_cap: None,
            complete_dir: dir.join("complete"),
            categories: Vec::new(),
            seeding,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        },
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.start().await.unwrap();
    torrents
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_running_torrents_takes_a_metainfo_and_lists_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let torrents = started(dir.path(), &db, None).await;
    let app = endpoint(dir.path(), db, torrents.clone()).await;

    let submitted = graphql(
        &app,
        &format!(
            r#"mutation {{ submitTorrent(input: {{ metainfoBase64: "{}" }}) {{ id name totalBytes }} }}"#,
            metainfo("one.mkv", 3000)
        ),
    )
    .await;
    assert_eq!(submitted.get("errors"), None, "{submitted}");
    let torrent = &submitted["data"]["submitTorrent"];
    assert_eq!(torrent["name"], "one.mkv");
    assert_eq!(torrent["totalBytes"], 3000);
    let id = torrent["id"].as_u64().unwrap();

    let listed = graphql(&app, "{ torrents { id name } }").await;
    assert_eq!(listed.get("errors"), None, "{listed}");
    assert_eq!(listed["data"]["torrents"][0]["id"], id);
    assert_eq!(listed["data"]["torrents"][0]["name"], "one.mkv");
    torrents.stop().await;
}

// Seeding terms that set nothing, on submit or after, leave the torrent no
// terms of its own: it seeds to the global goal, mode and action, never with
// no goal; terms with a goal are its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_seeding_terms_inherit_the_global_policy() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let global = SeedingPolicy {
        ratio_goal: Leaf::Set(1.0),
        mode: Leaf::Set(SeedingGoalMode::All),
        action: Leaf::Set(SeedingAction::Remove),
        ..SeedingPolicy::default()
    }
    .at_global();
    let torrents = started(dir.path(), &db, Some(global)).await;
    let app = endpoint(dir.path(), db, torrents.clone()).await;
    let seeding = "seedingOverride { ratioGoal { state value } } \
                   seedingPolicy { ratioGoal mode action levels { ratioGoal } }";
    let inherits = |torrent: &serde_json::Value| {
        assert_eq!(torrent["seedingOverride"], serde_json::Value::Null);
        assert_eq!(
            torrent["seedingPolicy"],
            serde_json::json!({
                "ratioGoal": 1.0, "mode": "ALL", "action": "REMOVE",
                "levels": { "ratioGoal": "GLOBAL" },
            })
        );
    };

    let submitted = graphql(
        &app,
        &format!(
            r#"mutation {{ submitTorrent(input: {{ metainfoBase64: "{}", seeding: {{}} }}) {{ id {seeding} }} }}"#,
            metainfo("one.mkv", 3000)
        ),
    )
    .await;
    assert_eq!(submitted.get("errors"), None, "{submitted}");
    let torrent = &submitted["data"]["submitTorrent"];
    inherits(torrent);
    let id = torrent["id"].as_u64().unwrap();
    let read = |app: Router| async move {
        let read = graphql(&app, &format!("{{ torrent(id: {id}) {{ {seeding} }} }}")).await;
        assert_eq!(read.get("errors"), None, "{read}");
        read["data"]["torrent"].clone()
    };

    let set = |terms: &'static str| {
        let app = app.clone();
        async move {
            let set = graphql(
                &app,
                &format!("mutation {{ setTorrentSeeding(id: {id}, seeding: {terms}) }}"),
            )
            .await;
            assert_eq!(set.get("errors"), None, "{set}");
        }
    };
    set("{ ratioGoal: { state: SET, value: 3 } }").await;
    let own = read(app.clone()).await;
    assert_eq!(
        own["seedingPolicy"]["levels"]["ratioGoal"], "TORRENT",
        "{own}"
    );
    assert_eq!(own["seedingOverride"]["ratioGoal"]["value"], 3.0);

    // Terms that only inherit are none.
    set("{ ratioGoal: { state: INHERIT } }").await;
    inherits(&read(app.clone()).await);

    set("{}").await;
    inherits(&read(app.clone()).await);
    set("{ ratioGoal: { state: SET, value: 3 } }").await;
    set("null").await;
    inherits(&read(app.clone()).await);
    torrents.stop().await;
}
