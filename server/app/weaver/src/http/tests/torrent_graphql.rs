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
    EgressBinding, LegPath, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::TorrentConfig;
use weaver_server_core::torrents::identity::IDENTITY;
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
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
        handle: handle.clone(),
        scheduled_resume: weaver_server_api::ScheduledResumeCoordinator::new(
            db.clone(),
            handle.clone(),
        ),
        config: config.clone(),
        db: db.clone(),
        server_transfer_policy,
        auth_cache: auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        security: RuntimeSecurityConfig::default(),
        rss: weaver_server_api::RssService::new(handle.clone(), config.clone(), db.clone()),
        watch_folder: weaver_server_core::watch_folder::WatchFolderService::new(
            db.clone(),
            handle.clone(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_running_torrents_takes_a_metainfo_and_lists_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let torrents = TorrentService::new(
        db.clone(),
        Sessions::new(
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
        ),
        TorrentSettings {
            complete_dir: dir.path().join("complete"),
            categories: Vec::new(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        },
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.start().await.unwrap();
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
