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
use weaver_server_api::networking::ResolvedBy as ResolvedByGql;
use weaver_server_core::categories::CategoryConfig;
use weaver_server_core::post_processing::model::PostProcessingSettings;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, ResolvedBy, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::{SeedingPolicy, TorrentConfig};
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    IpRanges, OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
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

struct Rig {
    harness: TestHarness,
    torrents: TorrentService,
    _dir: tempfile::TempDir,
}

async fn rig() -> Rig {
    rig_on(|_| Arc::new(Offline)).await
}

// A rig whose torrents run on the routes `routes` makes of its database.
async fn rig_on(routes: impl FnOnce(&weaver_server_core::Database) -> Arc<dyn Routes>) -> Rig {
    let harness = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let db = harness.db.clone();
    let sessions = Sessions::new(
        routes(&db),
        SessionEnv {
            root: dir.path().join("sessions"),
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

    // `query` as an API key of `scope` asks it.
    async fn execute_as_key(&self, query: &str, scope: CallerScope) -> Response {
        let request = Request::new(query)
            .data(scope)
            .data(CallerIdentity::ApiKey([9; 32]))
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
    // Turned on and then off, every route shows that torrents are off.
    let rig = rig().await;
    let settings = |enabled| TorrentSettings {
        complete_dir: rig._dir.path().join("complete"),
        categories: Vec::new(),
        seeding: None,
        torrent: TorrentConfig {
            enabled,
            ..TorrentConfig::default()
        },
    };
    rig.torrents.set_settings(settings(true)).await;
    rig.torrents.set_settings(settings(false)).await;
    let response = rig.execute("{ torrentRoutes { reason } }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["torrentRoutes"],
        serde_json::json!([
            { "reason": "TORRENTS_DISABLED" }
        ])
    );
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
            unpack: None,
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

// A torrent's place in the queue reaches the queue's events as a job's does:
// its arrival, a change of state, and its leaving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_events_carry_a_torrent() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    let cursor = rig
        .harness
        .execute_as("query { latestQueueCursor }", CallerScope::Read)
        .await
        .data
        .into_json()
        .unwrap()["latestQueueCursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut events = rig.harness.schema.execute_stream(
        Request::new(format!(
            r#"subscription {{ queueEvents(after: "{cursor}") {{ kind itemId state }} }}"#
        ))
        .data(CallerScope::Read),
    );
    // The next event for `id` other than a progress tick, as (kind, state).
    let mut next = async |id: u64| loop {
        let response = events.next().await.expect("the queue's events stay open");
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let event = response.data.into_json().unwrap()["queueEvents"].clone();
        if event["itemId"].as_u64() == Some(id) && event["kind"] != "ITEM_PROGRESS" {
            return (
                event["kind"].as_str().unwrap().to_owned(),
                event["state"].as_str().unwrap_or_default().to_owned(),
            );
        }
    };

    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    assert_eq!(next(id).await, ("ITEM_CREATED".into(), "STALLED".into()));

    let paused = rig
        .execute(&format!("mutation {{ pauseTorrent(id: {id}) }}"))
        .await;
    assert!(paused.errors.is_empty(), "{:?}", paused.errors);
    assert_eq!(
        next(id).await,
        ("ITEM_STATE_CHANGED".into(), "PAUSED".into())
    );

    let removed = rig
        .execute(&format!("mutation {{ removeTorrent(id: {id}) }}"))
        .await;
    assert!(removed.errors.is_empty(), "{:?}", removed.errors);
    assert_eq!(
        next(id).await,
        ("ITEM_STATE_CHANGED".into(), "FAILED".into())
    );
}

// Pausing every torrent is one hold the queue reports, apart from its own
// pause, and resuming lifts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pausing_every_torrent_is_one_hold_apart_from_the_queues() {
    use weaver_server_core::jobs::{HoldOrigin, HoldOwner};
    let harness = TestHarness::new().await;
    let state = "{ globalQueueState { isPaused torrentsPaused } }";
    let read = |response: Response| {
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().unwrap();
        (
            data["globalQueueState"]["isPaused"].as_bool().unwrap(),
            data["globalQueueState"]["torrentsPaused"]
                .as_bool()
                .unwrap(),
        )
    };

    let response = harness.execute("mutation { pauseTorrents }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert!(
        harness
            .handle
            .state()
            .holds()
            .held_by(HoldOrigin::Manual, HoldOwner::Torrents)
    );
    assert_eq!(read(harness.execute(state).await), (false, true));

    let response = harness.execute("mutation { pauseQueue { success } }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let response = harness
        .execute("mutation { resumeQueue { success } }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(read(harness.execute(state).await), (false, true));

    let response = harness.execute("mutation { resumeTorrents }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(read(harness.execute(state).await), (false, false));
}

// A follower of the torrents gets them as they are, then each change the
// service publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_updates_push_what_the_service_publishes() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();

    let request = Request::new(format!(
        "subscription {{ torrentUpdates(id: {id}) {{ id name state }} }}"
    ))
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .data(rig.torrents.clone());
    let mut updates = rig.harness.schema.execute_stream(request);
    let first = updates.next().await.unwrap();
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let first = first.data.into_json().unwrap();
    assert_eq!(first["torrentUpdates"][0]["id"], id);
    assert_eq!(first["torrentUpdates"][0]["name"], "one.mkv");
    assert_eq!(first["torrentUpdates"][0]["state"], "STALLED");

    let paused = rig
        .execute(&format!("mutation {{ pauseTorrent(id: {id}) }}"))
        .await;
    assert!(paused.errors.is_empty(), "{:?}", paused.errors);
    loop {
        let next = updates.next().await.expect("the updates stay open");
        assert!(next.errors.is_empty(), "{:?}", next.errors);
        if next.data.into_json().unwrap()["torrentUpdates"][0]["state"] == "PAUSED" {
            break;
        }
    }
}

// A tracker rule keeps the client its torrents name themselves as, and
// absent is Weaver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_the_client_its_torrents_name_themselves_as() {
    let rig = rig().await;
    let save = |id: &str, client: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                hostPattern: "tracker.test", client: {client}
            }}) {{ id client }} }}"#
        )
    };

    let response = rig.execute(&save("", "QBITTORRENT")).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    assert_eq!(saved["saveTrackerRule"][0]["client"], "QBITTORRENT");
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();

    for (client, read) in [
        ("TRANSMISSION", serde_json::json!("TRANSMISSION")),
        ("null", serde_json::Value::Null),
    ] {
        let response = rig.execute(&save(&format!("id: {id},"), client)).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let response = rig.execute("{ trackerRules { id client } }").await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let rules = response.data.into_json().unwrap();
        assert_eq!(
            rules["trackerRules"],
            serde_json::json!([{ "id": id, "client": read }])
        );
    }
    assert!(!rig.execute(&save("", "DELUGE")).await.errors.is_empty());
}

// A tracker rule keeps its torrents' discovery, encryption, upload cap and
// forced start; absent inherits, and an upload cap without bytes per second
// is unlimited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_its_torrents_peer_terms() {
    let rig = rig().await;
    let fields = "id discovery encryption uploadLimit { bps } forced";
    let save = |id: &str, terms: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                hostPattern: "*.example-tracker.test", client: QBITTORRENT, {terms}
            }}) {{ {fields} }} }}"#
        )
    };

    let response = rig
        .execute(&save(
            "",
            "discovery: false, encryption: PREFER, uploadLimit: { bps: null }, forced: true",
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();
    assert_eq!(
        saved["saveTrackerRule"][0],
        serde_json::json!({
            "id": id, "discovery": false, "encryption": "PREFER",
            "uploadLimit": { "bps": null }, "forced": true,
        })
    );

    for (terms, read) in [
        (
            "discovery: true, encryption: REQUIRE, uploadLimit: { bps: 4096 }",
            serde_json::json!({
                "id": id, "discovery": true, "encryption": "REQUIRE",
                "uploadLimit": { "bps": 4096 }, "forced": false,
            }),
        ),
        (
            "",
            serde_json::json!({
                "id": id, "discovery": null, "encryption": null,
                "uploadLimit": null, "forced": false,
            }),
        ),
    ] {
        let response = rig.execute(&save(&format!("id: {id},"), terms)).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let response = rig
            .execute(&format!("{{ trackerRules {{ {fields} }} }}"))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let rules = response.data.into_json().unwrap();
        assert_eq!(rules["trackerRules"], serde_json::json!([read]));
    }
}

// A tracker rule keeps the certificate its SSL torrents present: read back
// as whom it names and until when, never its key; an input naming none
// keeps it, null clears it, and a key of another certificate is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_its_ssl_certificate() {
    let rig = rig().await;
    let issue = |subject: &str| {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, subject);
        params.not_after = rcgen::date_time_ymd(2031, 1, 1);
        (params.self_signed(&key).unwrap().pem(), key.serialize_pem())
    };
    let ssl = |(cert, key): &(String, String)| {
        format!(
            "ssl: {{ certificatePem: {}, privateKeyPem: {} }}",
            serde_json::to_string(cert).unwrap(),
            serde_json::to_string(key).unwrap()
        )
    };
    let fields = "id hasSsl sslSubject sslNotAfter";
    let save = |id: &str, ssl: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                hostPattern: "ssl-tracker.test", forced: true, {ssl}
            }}) {{ {fields} }} }}"#
        )
    };

    let issued = issue("user-1");
    let response = rig.execute(&save("", &ssl(&issued))).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();
    let held = serde_json::json!({
        "id": id, "hasSsl": true, "sslSubject": "CN=user-1",
        "sslNotAfter": 1_924_992_000_000_i64,
    });
    assert_eq!(saved["saveTrackerRule"][0], held);

    // Another certificate's key is refused, and the rule keeps its own.
    let other = issue("user-2");
    let mismatched = (issued.0.clone(), other.1.clone());
    let refused = rig
        .execute(&save(&format!("id: {id},"), &ssl(&mismatched)))
        .await;
    assert!(!refused.errors.is_empty());

    for (given, read) in [
        ("", held.clone()),
        (
            "ssl: null",
            serde_json::json!({
                "id": id, "hasSsl": false, "sslSubject": null, "sslNotAfter": null,
            }),
        ),
    ] {
        let response = rig.execute(&save(&format!("id: {id},"), given)).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let response = rig
            .execute(&format!("{{ trackerRules {{ {fields} }} }}"))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let rules = response.data.into_json().unwrap();
        assert_eq!(rules["trackerRules"], serde_json::json!([read]));
    }
}

// A tracker rule keeps whether its torrents route through their own
// consumer, and absent is off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_whether_its_torrents_route_themselves() {
    let rig = rig().await;
    let save = |id: &str, route: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                hostPattern: "tracker.test"{route}
            }}) {{ id torrentRoute }} }}"#
        )
    };

    let response = rig.execute(&save("", ", torrentRoute: true")).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    assert_eq!(saved["saveTrackerRule"][0]["torrentRoute"], true);
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();

    for (route, read) in [
        (", torrentRoute: false", false),
        (", torrentRoute: true", true),
        ("", false),
    ] {
        let response = rig.execute(&save(&format!("id: {id},"), route)).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let response = rig.execute("{ trackerRules { id torrentRoute } }").await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let rules = response.data.into_json().unwrap();
        assert_eq!(
            rules["trackerRules"],
            serde_json::json!([{ "id": id, "torrentRoute": read }])
        );
    }
}

// A torrent announcing to a host a tracker rule names takes the rule's
// goals, and names the rule as their source until the rule is deleted. A
// pattern that is no host, or one another rule has, is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_sets_the_goals_of_the_torrents_announcing_to_its_hosts() {
    let rig = rig().await;
    let rig = &rig;
    let read = |query: String| async move {
        let response = rig.execute(&query).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    };
    let rule = |id: &str, pattern: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                hostPattern: "{pattern}", neverPause: true, seeding: {{ ratioGoal: 3 }}
            }}) {{ id hostPattern neverPause seeding {{ ratioGoal }} }} }}"#
        )
    };

    let saved = read(rule("", " *.Tracker.TEST. ")).await;
    let saved = &saved["saveTrackerRule"][0];
    assert_eq!(saved["hostPattern"], "*.tracker.test");
    assert_eq!(saved["neverPause"], true);
    assert_eq!(saved["seeding"]["ratioGoal"], 3.0);
    let rule_id = saved["id"].as_u64().unwrap();
    for refused in [rule("", "*.tracker.test"), rule("", "not a host")] {
        assert!(!rig.execute(&refused).await.errors.is_empty());
    }

    // The metainfo with an announce URL under the rule's host.
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(metainfo("ruled.mkv", 3000, false))
        .unwrap();
    let mut announcing = b"d".to_vec();
    bstr(&mut announcing, b"announce");
    bstr(&mut announcing, b"http://a.tracker.test/announce");
    announcing.extend_from_slice(&bytes[1..]);
    let announcing = base64::engine::general_purpose::STANDARD.encode(announcing);
    let id = read(submit(&announcing, "")).await["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let source = || async move {
        let data = read(format!(
            "{{ torrent(id: {id}) {{ seedingSource seedingRule seedingPolicy {{ ratioGoal }} }} }}"
        ))
        .await;
        data["torrent"].clone()
    };
    let torrent = source().await;
    assert_eq!(torrent["seedingSource"], "TRACKER_RULE");
    assert_eq!(torrent["seedingRule"], "*.tracker.test");
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 3.0);
    // No schedule pause is in force, so none is left seeding through one.
    assert_eq!(
        read("{ scheduleSeeding { hostPattern torrents } }".into()).await["scheduleSeeding"],
        serde_json::json!([])
    );

    let left = read(format!(
        "mutation {{ deleteTrackerRule(id: {rule_id}) {{ id }} }}"
    ))
    .await;
    assert_eq!(left["deleteTrackerRule"], serde_json::json!([]));
    let torrent = source().await;
    assert_eq!(torrent["seedingSource"], "GLOBAL");
    assert_eq!(torrent["seedingRule"], Value::Null);
    assert_eq!(
        read("{ trackerRules { id } }".into()).await["trackerRules"],
        serde_json::json!([])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_peers_are_listed_and_a_ban_is_kept() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();

    let listed = rig
        .execute(&format!(
            "{{ torrentPeers(id: {id}) {{ address client peerId connection encryption \
             flags {{ choked remoteChoked }} pieces progress }} }}"
        ))
        .await;
    assert!(listed.errors.is_empty(), "{:?}", listed.errors);
    assert_eq!(
        listed.data.into_json().unwrap()["torrentPeers"],
        Value::Array(Vec::new())
    );

    for address in ["127.0.0.3:6881", "[::1]:6881", "127.0.0.4"] {
        let banned = rig
            .execute(&format!(
                r#"mutation {{ banTorrentPeer(id: {id}, address: "{address}") }}"#
            ))
            .await;
        assert!(banned.errors.is_empty(), "{:?}", banned.errors);
    }
    let saved = rig
        .harness
        .db
        .torrent_peers(weaver_server_core::jobs::ids::JobId(id))
        .unwrap();
    let banned: Vec<String> = saved.banned.iter().map(ToString::to_string).collect();
    assert_eq!(banned, ["127.0.0.3", "127.0.0.4", "::1"]);

    let refused = rig
        .execute(&format!(
            r#"mutation {{ banTorrentPeer(id: {id}, address: "somewhere") }}"#
        ))
        .await;
    assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"));
    let unknown = rig
        .execute(r#"mutation { banTorrentPeer(id: 999999, address: "127.0.0.3") }"#)
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"));
    let unknown = rig
        .execute("{ torrentPeers(id: 999999) { address } }")
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"));
}

// libtorrent `torrent::set_upload_limit` / `set_download_limit`: a torrent's
// own limits are set together and read back; null is unlimited and 0 holds
// that direction closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_rate_limits_are_set_and_read_back() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let limits = || async {
        let read = rig
            .execute(&format!(
                "{{ torrent(id: {id}) {{ uploadLimit downloadLimit }} }}"
            ))
            .await;
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        let data = read.data.into_json().unwrap();
        (
            data["torrent"]["uploadLimit"].clone(),
            data["torrent"]["downloadLimit"].clone(),
        )
    };
    assert_eq!(limits().await, (Value::Null, Value::Null));

    // A limit left out is kept; null lifts it.
    for (limits_set, read) in [
        (
            "upload: 65536, download: 0",
            (Value::from(65_536), Value::from(0)),
        ),
        (
            "download: 1048576",
            (Value::from(65_536), Value::from(1_048_576)),
        ),
        ("upload: null", (Value::Null, Value::from(1_048_576))),
    ] {
        let set = rig
            .execute(&format!(
                "mutation {{ setTorrentRateLimits(id: {id}, {limits_set}) }}"
            ))
            .await;
        assert!(set.errors.is_empty(), "{:?}", set.errors);
        assert_eq!(limits().await, read);
    }
    let stored = rig
        .harness
        .db
        .get_torrent(weaver_server_core::jobs::ids::JobId(id))
        .unwrap()
        .unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (None, Some(1_048_576))
    );

    let unknown = rig
        .execute("mutation { setTorrentRateLimits(id: 999999, upload: 1) }")
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"));
}

// A file's level and the torrent's two picking toggles are set by their
// mutations and read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_levels_and_picking_toggles_round_trip() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let read = |id: u64| {
        format!(
            "{{ torrent(id: {id}) {{ sequential firstLastPieceFirst files {{ selected priority }} }} }}"
        )
    };
    let before = rig.execute(&read(id)).await.data.into_json().unwrap();
    assert_eq!(before["torrent"]["files"][0]["priority"], "NORMAL");
    assert_eq!(before["torrent"]["sequential"], false);
    assert_eq!(before["torrent"]["firstLastPieceFirst"], false);

    for mutation in [
        format!(
            "mutation {{ setTorrentFilePriorities(id: {id}, priorities: [{{ index: 0, priority: IGNORED }}]) }}"
        ),
        format!("mutation {{ setTorrentSequential(id: {id}, sequential: true) }}"),
        format!(
            "mutation {{ setTorrentFirstLastPieceFirst(id: {id}, firstLastPieceFirst: true) }}"
        ),
    ] {
        let response = rig.execute(&mutation).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }
    let after = rig.execute(&read(id)).await.data.into_json().unwrap();
    assert_eq!(after["torrent"]["files"][0]["priority"], "IGNORED");
    assert_eq!(after["torrent"]["files"][0]["selected"], false);
    assert_eq!(after["torrent"]["sequential"], true);
    assert_eq!(after["torrent"]["firstLastPieceFirst"], true);

    let out_of_range = rig
        .execute(&format!(
            "mutation {{ setTorrentFilePriorities(id: {id}, priorities: [{{ index: 1, priority: HIGH }}]) }}"
        ))
        .await;
    assert_eq!(code(&out_of_range).as_deref(), Some("INVALID_INPUT"));
}

// A torrent's trackers are listed, added, replaced and removed, and a
// reannounce or DHT announce is taken; an unknown torrent or tracker is
// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_trackers_are_listed_and_edited() {
    let rig = rig().await;
    // The metainfo names a tracker of its own.
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(metainfo("one.mkv", 3000, false))
        .unwrap();
    let mut announcing = b"d".to_vec();
    bstr(&mut announcing, b"announce");
    bstr(&mut announcing, b"http://own.invalid/announce");
    announcing.extend_from_slice(&bytes[1..]);
    let announcing = base64::engine::general_purpose::STANDARD.encode(announcing);
    let response = rig.execute(&submit(&announcing, "")).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let run = |query: String| {
        let rig = &rig;
        async move {
            let response = rig.execute(&query).await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            response.data.into_json().unwrap()
        }
    };
    let listed = || {
        run(format!(
            "{{ torrentTrackers(id: {id}) {{ tier url source status updating message \
             seeders leechers downloaded peers fails nextAnnounceMs }} }}"
        ))
    };

    run(format!(
        r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["http://a.invalid/announce", "udp://b.invalid:80"]) }}"#
    ))
    .await;
    run(format!(
        r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["http://a.invalid/announce", "http://c.invalid/announce"]) }}"#
    ))
    .await;
    let trackers = listed().await;
    let rows: Vec<(u64, &str, &str, &str)> = trackers["torrentTrackers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tracker| {
            (
                tracker["tier"].as_u64().unwrap(),
                tracker["url"].as_str().unwrap(),
                tracker["source"].as_str().unwrap(),
                tracker["status"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (0, "http://own.invalid/announce", "TORRENT", "NOT_CONTACTED"),
            (1, "http://a.invalid/announce", "OPERATOR", "NOT_CONTACTED"),
            (1, "udp://b.invalid:80", "OPERATOR", "NOT_CONTACTED"),
            (2, "http://c.invalid/announce", "OPERATOR", "NOT_CONTACTED"),
        ]
    );
    assert_eq!(trackers["torrentTrackers"][0]["updating"], false);
    assert_eq!(trackers["torrentTrackers"][0]["fails"], 0);

    run(format!(
        r#"mutation {{ replaceTorrentTrackers(id: {id}, trackers: [{{ tier: 3, url: "http://c.invalid/announce" }}, {{ tier: 1, url: "udp://b.invalid:80" }}]) }}"#
    ))
    .await;
    run(format!(
        r#"mutation {{ removeTorrentTrackers(id: {id}, urls: ["udp://b.invalid:80"]) }}"#
    ))
    .await;
    let trackers = listed().await;
    assert_eq!(trackers["torrentTrackers"].as_array().unwrap().len(), 1);
    assert_eq!(trackers["torrentTrackers"][0]["tier"], 3);
    assert_eq!(
        trackers["torrentTrackers"][0]["url"],
        "http://c.invalid/announce"
    );
    assert_eq!(trackers["torrentTrackers"][0]["source"], "OPERATOR");

    run(format!("mutation {{ reannounceTorrent(id: {id}) }}")).await;
    run(format!(
        r#"mutation {{ reannounceTorrent(id: {id}, tracker: "http://c.invalid/announce") }}"#
    ))
    .await;
    run(format!("mutation {{ dhtAnnounceTorrent(id: {id}) }}")).await;

    let unknown = rig
        .execute(&format!(
            r#"mutation {{ reannounceTorrent(id: {id}, tracker: "http://d.invalid/announce") }}"#
        ))
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("INVALID_INPUT"));
    let refused = rig
        .execute(&format!(
            r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["not a tracker"]) }}"#
        ))
        .await;
    assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"));
    for query in [
        "{ torrentTrackers(id: 999999) { url } }",
        r#"mutation { addTorrentTrackers(id: 999999, urls: ["http://a.invalid/"]) }"#,
        r#"mutation { replaceTorrentTrackers(id: 999999, trackers: []) }"#,
        r#"mutation { removeTorrentTrackers(id: 999999, urls: []) }"#,
        "mutation { reannounceTorrent(id: 999999) }",
        "mutation { dhtAnnounceTorrent(id: 999999) }",
    ] {
        let unknown = rig.execute(query).await;
        assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"), "{query}");
    }
}

// A key that only reads sees a tracker's scheme, host and port, never the
// passkey in its path or query; a key that controls sees the whole URL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_key_sees_no_trackers_passkey() {
    const PRIVATE: &str = "https://tracker.invalid:8443/0123passkey/announce?pk=s3cret";
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let added = rig
        .execute(&format!(
            r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["{PRIVATE}"]) }}"#
        ))
        .await;
    assert!(added.errors.is_empty(), "{:?}", added.errors);
    let urls = |scope: CallerScope| {
        let rig = &rig;
        async move {
            let response = rig
                .execute_as_key(
                    &format!(
                        "{{ torrentTrackers(id: {id}) {{ url }} torrent(id: {id}) {{ trackers {{ url }} }} }}"
                    ),
                    scope,
                )
                .await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            let data = response.data.into_json().unwrap();
            [
                data["torrentTrackers"][0]["url"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                data["torrent"]["trackers"][0]["url"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ]
        }
    };
    let shown = "https://tracker.invalid:8443/…".to_owned();
    assert_eq!(urls(CallerScope::Read).await, [shown.clone(), shown]);
    assert_eq!(
        urls(CallerScope::Admin).await,
        [PRIVATE.to_owned(), PRIVATE.to_owned()]
    );
    assert_eq!(
        urls(CallerScope::Control).await,
        [PRIVATE.to_owned(), PRIVATE.to_owned()]
    );
}

// Every route is listed with the scopes that run on it; one whose leg is
// down shows why, and no address or mapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_torrent_routes_are_listed_with_their_scopes_and_reasons() {
    let rig = rig().await;
    rig.torrents
        .set_settings(TorrentSettings {
            complete_dir: rig._dir.path().join("complete"),
            categories: Vec::new(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        })
        .await;
    let response = rig
        .execute(
            r#"mutation { saveTrackerRule(input: {
                hostPattern: "tracker.test", torrentRoute: true
            }) { id } }"#,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["saveTrackerRule"][0]["id"]
        .as_u64()
        .unwrap();

    let response = rig
        .execute(
            "{ torrentRoutes { key scopes { scope id name } open listenAddress utp \
             mappings { mapper protocol localPort externalPort gatewayAddress error } \
             externalAddresses reason } }",
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let key = RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct);
    assert_eq!(
        response.data.into_json().unwrap()["torrentRoutes"],
        serde_json::json!([{
            "key": key.as_str(),
            "scopes": [
                { "scope": "DEFAULT", "id": null, "name": null },
                { "scope": "TRACKER_RULE", "id": id, "name": "tracker.test" },
            ],
            "open": false,
            "listenAddress": null,
            "utp": false,
            "mappings": [],
            "externalAddresses": [],
            "reason": "LEG_DOWN",
        }])
    );
}

// An open route shows where its session listens, each mapping's last report
// and the address the internet sees.
#[test]
fn an_open_route_shows_where_it_listens_and_its_mappings() {
    use weaver_server_api::torrents::types::TorrentRouteGql;
    use weaver_server_core::torrents::service::TorrentRoute;
    use weaver_server_core::torrents::sessions::{
        PortMapped, PortmapProtocol, PortmapTransport, SessionView,
    };

    let key = RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct);
    let route = TorrentRouteGql::from(TorrentRoute {
        key: key.clone(),
        names: Some(ResolvedBy::Host),
        scopes: vec![(TorrentScope::Category(3), Some("tv".into()))],
        session: Some(SessionView {
            key: key.clone(),
            kind: LegKind::System,
            port: Some(6881),
            listen: Some("127.0.0.1:6881".parse().unwrap()),
            utp: true,
            mappings: vec![
                PortMapped {
                    transport: PortmapTransport::Upnp,
                    protocol: PortmapProtocol::Tcp,
                    local_port: 6881,
                    external_port: None,
                    external_ip: None,
                    error: Some("ConflictInMappingEntry".into()),
                },
                PortMapped {
                    transport: PortmapTransport::NatPmp,
                    protocol: PortmapProtocol::Udp,
                    local_port: 6881,
                    external_port: Some(40_001),
                    external_ip: Some("203.0.113.7".parse().unwrap()),
                    error: None,
                },
            ],
            external: vec!["203.0.113.7".parse().unwrap()],
            exposure: Some(exposure()),
            held: None,
        }),
        down: None,
    });
    assert_eq!(route.key, key.as_str());
    assert!(route.open);
    assert_eq!(route.listen_address.as_deref(), Some("127.0.0.1:6881"));
    assert!(route.utp);
    assert_eq!(route.scopes[0].id, Some(3));
    assert_eq!(route.scopes[0].name.as_deref(), Some("tv"));
    assert_eq!(route.mappings.len(), 2);
    assert_eq!(route.mappings[0].external_port, None);
    assert_eq!(
        route.mappings[0].error.as_deref(),
        Some("ConflictInMappingEntry")
    );
    assert_eq!(route.mappings[1].external_port, Some(40_001));
    assert_eq!(
        route.mappings[1].gateway_address.as_deref(),
        Some("203.0.113.7")
    );
    assert_eq!(route.external_addresses, ["203.0.113.7"]);
    assert_eq!(route.reason, None);
    let shown = route.exposure.unwrap();
    assert_eq!(shown.resolved_by, Some(ResolvedByGql::Host));
    assert_eq!(shown.address.as_deref(), Some("203.0.113.7"));
    assert_eq!(shown.announced_port, 40_001);
    assert!(shown.inbound);
    assert!(shown.udp_trackers && shown.dht && shown.utp);
}

// What an open session on a System leg shows: the host resolves its names,
// a gateway mapped its port.
fn exposure() -> weaver_server_core::torrents::net::Exposure {
    weaver_server_core::torrents::net::Exposure {
        resolved_by: Some(ResolvedBy::Host),
        address: Some("203.0.113.7".parse().unwrap()),
        announced_port: 40_001,
        inbound: true,
        udp_trackers: true,
        dht: true,
        utp: true,
    }
}

// The speed test's report shows a session's exposure in the very values the
// route view shows it in.
#[test]
fn the_speed_test_shows_exposure_as_the_route_view_does() {
    use weaver_server_api::networking::P2pTestResult;
    use weaver_server_api::torrents::types::TorrentRouteGql;
    use weaver_server_core::speed_test::P2pReport;
    use weaver_server_core::torrents::service::TorrentRoute;
    use weaver_server_core::torrents::sessions::SessionView;

    let key = RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct);
    let route = TorrentRouteGql::from(TorrentRoute {
        key: key.clone(),
        names: Some(ResolvedBy::Host),
        scopes: vec![(TorrentScope::Default, None)],
        session: Some(SessionView {
            key,
            kind: LegKind::System,
            port: Some(6881),
            listen: Some("0.0.0.0:6881".parse().unwrap()),
            utp: true,
            mappings: Vec::new(),
            external: Vec::new(),
            exposure: Some(exposure()),
            held: None,
        }),
        down: None,
    });
    let report = P2pTestResult::from(P2pReport {
        rungs: Vec::new(),
        mapped_port: Some(40_001),
        exposure: Some(exposure()),
        upload: None,
    });
    assert!(report.exposure.is_some());
    assert_eq!(report.exposure, route.exposure);
}

// A route whose leg is up and that nothing has asked to open says so.
#[test]
fn a_route_nothing_asked_to_open_says_so() {
    use weaver_server_api::torrents::types::{TorrentRouteGql, TorrentRouteReasonGql};
    use weaver_server_core::torrents::service::TorrentRoute;

    let key = RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct);
    let route = TorrentRouteGql::from(TorrentRoute {
        key,
        names: None,
        scopes: vec![(TorrentScope::Default, None)],
        session: None,
        down: None,
    });
    assert!(!route.open);
    assert_eq!(route.listen_address, None);
    assert_eq!(route.reason, Some(TorrentRouteReasonGql::NotAskedToOpen));
}

#[test]
fn a_down_route_overrides_its_previous_session_state() {
    use weaver_server_api::torrents::types::TorrentRouteGql;
    use weaver_server_core::torrents::service::TorrentRoute;
    use weaver_server_core::torrents::sessions::{RouteFailure, SessionView, TorrentRouteReason};

    for reason in [
        TorrentRouteReason::LegDown,
        TorrentRouteReason::TorrentsDisabled,
    ] {
        for held in [None, Some(TorrentRouteReason::SessionBuildFailed.into())] {
            let key = RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct);
            let route = TorrentRouteGql::from(TorrentRoute {
                key: key.clone(),
                names: Some(ResolvedBy::Host),
                scopes: vec![],
                down: Some(RouteFailure {
                    reason,
                    detail: Some("link lost".into()),
                }),
                session: Some(SessionView {
                    key,
                    kind: LegKind::System,
                    port: Some(6881),
                    listen: Some("127.0.0.1:6881".parse().unwrap()),
                    utp: true,
                    mappings: vec![],
                    external: vec!["203.0.113.7".parse().unwrap()],
                    exposure: Some(exposure()),
                    held,
                }),
            });
            assert_eq!(route.reason, Some(reason.into()));
            assert!(!route.open);
            assert_eq!(route.listen_address, None);
            assert!(!route.utp);
            assert!(route.external_addresses.is_empty());
            assert_eq!(route.exposure, None);
        }
    }
}

// The routes the network plans, on legs that never open: nothing in these
// tests reaches a network.
struct Planned(weaver_server_core::torrents::sessions::NetworkRoutes);

impl Routes for Planned {
    fn plan(&self, scope: TorrentScope) -> Result<RoutePlan, String> {
        self.0.plan(scope)
    }

    fn leg_down(
        &self,
        plan: &RoutePlan,
    ) -> Option<weaver_server_core::torrents::sessions::RouteFailure> {
        self.0.leg_down(plan)
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
        Box::pin(async { Err("never opened".to_owned()) })
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
        Box::pin(async { Err("never fetched".to_owned()) })
    }
}

// `metainfo` announcing to `announce`.
fn announcing(name: &str, announce: &str) -> String {
    let engine = base64::engine::general_purpose::STANDARD;
    let plain = engine.decode(metainfo(name, 3000, false)).unwrap();
    let mut out = b"d".to_vec();
    bstr(&mut out, b"announce");
    bstr(&mut out, announce.as_bytes());
    out.extend_from_slice(&plain[1..]);
    engine.encode(out)
}

// The route view and each tracker row say who resolves the names: the
// host's DNS on the System leg, the proxy on a ladder whose exit proxy is
// handed the names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_routes_and_trackers_say_who_resolves_their_names() {
    use weaver_server_core::proxies::{
        Consumer, ProxyKind, ProxyProfile, ProxyRuntime, ProxySecrets, Resolver, RouteLeg, Rung,
    };
    use weaver_server_core::torrents::sessions::NetworkRoutes;

    let rig = rig_on(|db| {
        db.save_proxy_profile(&ProxyProfile {
            id: 1,
            name: "tunnel".into(),
            kind: ProxyKind::Socks5,
            enabled: true,
            host: "127.0.0.1".into(),
            port: 1,
            resolver: Resolver::Proxy,
            dns_servers: vec![],
            tunnel_addresses: vec![],
            peer_public_key: None,
            mtu: 1280,
            keepalive_seconds: None,
            timeout_seconds: 30,
            host_key_fingerprint: None,
            revision: 1,
            secrets: ProxySecrets::default(),
        })
        .unwrap();
        let proxies = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
        Arc::new(Planned(NetworkRoutes::new(
            db.clone(),
            proxies,
            std::time::Duration::from_secs(30),
        )))
    })
    .await;
    let response = rig
        .execute(
            r#"mutation { saveTrackerRule(input: {
                hostPattern: "tracker.test", torrentRoute: true
            }) { id } }"#,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let rule = response.data.into_json().unwrap()["saveTrackerRule"][0]["id"]
        .as_u64()
        .unwrap();
    rig.harness
        .db
        .save_proxy_routing_policy(
            Consumer::Torrent(TorrentScope::TrackerRule(rule as u32)),
            &RoutingPolicy {
                legs: vec![RouteLeg {
                    egress_id: SYSTEM_EGRESS_ID,
                    weight: 100,
                    path: LegPath::Ladder {
                        rungs: vec![Rung::Proxy { id: 1 }],
                        direct_fallback: false,
                    },
                }],
                ..Default::default()
            },
        )
        .unwrap();

    let response = rig
        .execute("{ torrentRoutes { scopes { scope } resolvedBy } }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["torrentRoutes"],
        serde_json::json!([
            { "scopes": [{ "scope": "DEFAULT" }], "resolvedBy": "HOST" },
            { "scopes": [{ "scope": "TRACKER_RULE" }], "resolvedBy": "PROXY" },
        ])
    );

    for (name, announce, by) in [
        ("host.mkv", "http://other.invalid/announce", "HOST"),
        ("proxy.mkv", "http://tracker.test/announce", "PROXY"),
    ] {
        let response = rig.execute(&submit(&announcing(name, announce), "")).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
            .as_u64()
            .unwrap();
        let response = rig
            .execute(&format!(
                "{{ torrentTrackers(id: {id}) {{ url resolvedBy }} }}"
            ))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data.into_json().unwrap()["torrentTrackers"],
            serde_json::json!([{ "url": announce, "resolvedBy": by }])
        );
    }
}

// A torrent its route holds says why as the route's reason code, not in
// words, in the list and in its own view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_torrent_carries_its_routes_reason_code() {
    let offline = rig().await;
    let response = offline
        .execute(&format!(
            r#"mutation {{ submitTorrent(input: {{ metainfoBase64: "{}" }}) {{
                id state detail routeReason
            }} }}"#,
            metainfo("offline.mkv", 3000, false)
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let submitted = response.data.into_json().unwrap()["submitTorrent"].clone();
    let id = submitted["id"].as_u64().unwrap();
    let held = serde_json::json!({
        "id": id, "state": "STALLED", "detail": null, "routeReason": "LEG_DOWN"
    });
    assert_eq!(submitted, held);
    let response = offline
        .execute(&format!(
            "{{ torrents {{ id state detail routeReason }} \
               torrent(id: {id}) {{ id state detail routeReason }} }}"
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        serde_json::json!({ "torrents": [held.clone()], "torrent": held })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_torrent_has_a_support_report_without_its_name() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("zqxsentinel.mkv", 3000, true), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["id"]
        .as_u64()
        .unwrap();
    let response = rig
        .execute_as_key(
            &format!("{{ jobSupportReport(jobId: {id}) {{ text json }} }}"),
            CallerScope::Read,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    let text = data["jobSupportReport"]["text"].as_str().unwrap();
    let json = data["jobSupportReport"]["json"].as_str().unwrap();
    assert!(!text.contains("zqxsentinel"), "{text}");
    assert!(!json.contains("zqxsentinel"), "{json}");
    assert!(text.contains("job (queue, torrent)"), "{text}");
    let json: Value = serde_json::from_str(json).unwrap();
    assert_eq!(json["torrent"]["identity"]["source"], "metainfo");
    assert_eq!(json["torrent"]["trackers"]["private"], true);
    assert_eq!(json["job"]["source"], "queue");
    assert_eq!(json["job"]["state"], "stalled");
    assert_eq!(json["job"]["route_reason"], "leg_down");
    assert_eq!(json["job"]["private"], true);
    assert_eq!(json["job"]["pieces"]["total"], 3);
}
