// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_graphql::{Request, Response};
use base64::Engine;
use common::TestHarness;
use serde_json::Value;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::categories::CategoryConfig;
use weaver_server_core::post_processing::model::PostProcessingSettings;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::{SeedingPolicy, TorrentConfig};
use weaver_server_core::torrents::identity::IDENTITY;
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};

// A route that is always down: nothing in these tests reaches a network.
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

struct Rig {
    harness: TestHarness,
    torrents: TorrentService,
    _dir: tempfile::TempDir,
}

async fn rig() -> Rig {
    let harness = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let db = harness.db.clone();
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
    let complete = dir.path().join("complete");
    let torrents = TorrentService::new(
        db,
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
    Rig {
        harness,
        torrents,
        _dir: dir,
    }
}

impl Rig {
    async fn execute(&self, query: &str) -> Response {
        let request = Request::new(query)
            .data(CallerScope::Local)
            .data(CallerIdentity::Local([7; 32]))
            .data(self.torrents.clone());
        self.harness.schema.execute(request).await
    }
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo, base64.
fn metainfo(name: &str, length: usize, private: bool) -> String {
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
    base64::engine::general_purpose::STANDARD.encode(out)
}

fn submit(metainfo: &str, extra: &str) -> String {
    format!(
        r#"mutation {{ submitTorrent(input: {{ metainfoBase64: "{metainfo}" {extra} }}) {{
            id name state detail totalBytes private files {{ path length }}
        }} }}"#
    )
}

fn code(response: &Response) -> Option<String> {
    let error = response.errors.first()?;
    let extensions = serde_json::to_value(&error.extensions).ok()?;
    extensions
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metainfo_is_added_and_read_back() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    let torrent = &data["submitTorrent"];
    assert_eq!(torrent["name"], "one.mkv");
    assert_eq!(torrent["totalBytes"], 3000);
    assert_eq!(torrent["state"], "STALLED");
    assert_eq!(torrent["files"][0]["path"], "one.mkv");
    let id = torrent["id"].as_u64().unwrap();

    let listed = rig
        .execute(&format!(
            "{{ torrents {{ id }} torrent(id: {id}) {{ name infohash }} }}"
        ))
        .await;
    assert!(listed.errors.is_empty(), "{:?}", listed.errors);
    let listed = listed.data.into_json().unwrap();
    assert_eq!(listed["torrents"][0]["id"], id);
    assert_eq!(listed["torrent"]["infohash"].as_str().unwrap().len(), 40);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_is_refused() {
    let rig = rig().await;
    let bytes = metainfo("one.mkv", 3000, false);
    assert!(rig.execute(&submit(&bytes, "")).await.errors.is_empty());
    let again = rig.execute(&submit(&bytes, "")).await;
    assert_eq!(code(&again).as_deref(), Some("DUPLICATE"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_is_refused_without_a_network() {
    let rig = rig().await;
    let response = rig
        .execute(
            r#"mutation { submitTorrent(input: {
                magnet: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce"
            }) { id } }"#,
        )
        .await;
    assert_eq!(code(&response).as_deref(), Some("NO_NETWORK"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_extension_is_refused() {
    let rig = rig().await;
    rig.harness
        .db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["iso".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let response = rig
        .execute(&submit(&metainfo("disc.iso", 2000, false), ""))
        .await;
    assert_eq!(code(&response).as_deref(), Some("REFUSED"));
    let listed = rig.execute("{ torrents { id } }").await;
    assert_eq!(
        listed.data.into_json().unwrap()["torrents"],
        Value::Array(Vec::new())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_a_private_torrent_short_of_its_goal_needs_confirmation() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(
            &metainfo("p.bin", 2000, true),
            "seeding: { ratioGoal: 1.0 }",
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let refused = rig
        .execute(&format!("mutation {{ removeTorrent(id: {id}) }}"))
        .await;
    assert_eq!(code(&refused).as_deref(), Some("SEEDING_GOAL_UNMET"));
    let removed = rig
        .execute(&format!(
            "mutation {{ removeTorrent(id: {id}, confirmUnmetGoal: true) }}"
        ))
        .await;
    assert!(removed.errors.is_empty(), "{:?}", removed.errors);
    let history = rig.harness.db.get_job_history(id).unwrap().unwrap();
    assert_eq!(history.status, "cancelled");
    let gone = rig
        .execute(&format!("{{ torrent(id: {id}) {{ id }} }}"))
        .await;
    assert_eq!(gone.data.into_json().unwrap()["torrent"], Value::Null);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_torrent_surface_says_when_torrents_are_off() {
    let harness = TestHarness::new().await;
    let response = harness.execute("{ torrents { id } }").await;
    assert_eq!(code(&response).as_deref(), Some("TORRENTS_DISABLED"));
}

// The queue names a torrent's phase as itself, the state its history would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_names_a_torrents_phase() {
    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let queue = rig.execute("{ queueItems { id state } }").await;
    assert!(queue.errors.is_empty(), "{:?}", queue.errors);
    let queue = queue.data.into_json().unwrap();
    let item = queue["queueItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"].as_u64() == Some(id))
        .cloned()
        .unwrap();
    assert_eq!(item["state"], "STALLED");
}

const DETAIL: &str = "leechers peers seeds files { path progress }
    seedingOverride { ratioGoal } seedingPolicy { ratioGoal } seedingSource";

// A torrent reads its leechers, each file's progress, and its seeding policy
// with where that policy comes from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_reads_its_leechers_file_progress_and_seeding_policy() {
    let rig = rig().await;
    let policy = |ratio_goal| SeedingPolicy {
        ratio_goal: Some(ratio_goal),
        ..SeedingPolicy::default()
    };
    let mut settings = TorrentSettings {
        complete_dir: rig._dir.path().join("complete"),
        categories: vec![CategoryConfig {
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: Some(policy(3.0)),
            torrent_route: false,
        }],
        seeding: Some(policy(1.5)),
        torrent: TorrentConfig::default(),
    };
    rig.torrents.set_settings(settings.clone()).await;

    let rig = &rig;
    let read = |query: String| async move {
        let response = rig.execute(&query).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    };
    let id = |data: &Value| data["submitTorrent"]["id"].as_u64().unwrap();
    let detail = |id: u64| format!("{{ torrent(id: {id}) {{ {DETAIL} }} }}");

    let global = id(&read(submit(&metainfo("a.mkv", 3000, false), "")).await);
    let torrent = &read(detail(global)).await["torrent"];
    assert_eq!(torrent["leechers"], 0);
    assert_eq!(torrent["files"][0]["path"], "a.mkv");
    assert_eq!(torrent["files"][0]["progress"], 0.0);
    assert_eq!(torrent["seedingOverride"], Value::Null);
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 1.5);
    assert_eq!(torrent["seedingSource"], "GLOBAL");

    let category = id(&read(submit(&metainfo("b.mkv", 3000, false), "category: \"tv\"")).await);
    let torrent = &read(detail(category)).await["torrent"];
    assert_eq!(torrent["seedingOverride"], Value::Null);
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 3.0);
    assert_eq!(torrent["seedingSource"], "CATEGORY");

    let own = id(&read(submit(
        &metainfo("c.mkv", 3000, false),
        "category: \"tv\" seeding: { ratioGoal: 2.0 }",
    ))
    .await);
    let torrent = &read(detail(own)).await["torrent"];
    assert_eq!(torrent["seedingOverride"]["ratioGoal"], 2.0);
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 2.0);
    assert_eq!(torrent["seedingSource"], "TORRENT");

    // The global policy changes and the inheriting torrent follows it.
    settings.seeding = Some(policy(4.0));
    rig.torrents.set_settings(settings).await;
    let torrent = &read(detail(global)).await["torrent"];
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 4.0);
}
