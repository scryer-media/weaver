// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_core::history::queries::HistoryQueries;
use weaver_server_core::post_processing::settings::PostProcessingSettingsQueries;
use weaver_server_core::proxies::ProxiesDnsPersistenceQueries;
use weaver_server_core::proxies::ProxiesPersistenceQueries;
use weaver_server_core::settings::persistence::SettingsPersistenceQueries;
use weaver_server_core::settings::queries::SettingsQueries;
use weaver_server_core::tags::TagsQueries;
use weaver_server_core::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use weaver_server_core::torrents::queries::TorrentsQueries;
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
use weaver_server_core::clock::SystemClock;
use weaver_server_core::post_processing::model::PostProcessingSettings;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, ResolvedBy, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::{Leaf, SeedingPolicy, TorrentConfig};
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    IpRanges, OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};

// A route that is always down: nothing in these tests reaches a network.
struct Offline;

impl Routes for Offline {
    fn plan(&self, scope: TorrentScope) -> RoutePlan {
        RoutePlan {
            scope,
            policy: RoutingPolicy::default(),
            egress_id: SYSTEM_EGRESS_ID,
            kind: LegKind::System,
            key: RouteKey::of(SYSTEM_EGRESS_ID, &EgressBinding::System, &LegPath::Direct),
            names: Some(ResolvedBy::Host),
            down: Some(
                weaver_server_core::torrents::sessions::RouteFailure::leg_down("offline".into()),
            ),
            members: Vec::new(),
            member: None,
        }
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
            upload_cap: None,
            complete_dir: complete.clone(),
            categories: Default::default(),
            seeding: None,
            // Torrents on, as the open sessions say: a torrent is added only then.
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
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

// A torrent of `count` files `f0.bin`, `f1.bin`, ... of 1500 bytes each in
// a folder `name`, its metainfo base64.
fn metainfo_of_files(name: &str, count: usize) -> String {
    let length = 1500;
    let pieces = vec![0u8; (count * length).div_ceil(1024) * 20];
    let mut info = b"d".to_vec();
    bstr(&mut info, b"files");
    info.push(b'l');
    for index in 0..count {
        info.push(b'd');
        bstr(&mut info, b"length");
        info.extend_from_slice(format!("i{length}e").as_bytes());
        bstr(&mut info, b"path");
        info.push(b'l');
        bstr(&mut info, format!("f{index}.bin").as_bytes());
        info.extend_from_slice(b"ee");
    }
    info.push(b'e');
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    bstr(&mut info, b"piece length");
    info.extend_from_slice(b"i1024e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &pieces);
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    base64::engine::general_purpose::STANDARD.encode(out)
}

fn submit(metainfo: &str, extra: &str) -> String {
    format!(
        r#"mutation {{ submitTorrent(input: {{ source: {{ metainfoBase64: "{metainfo}" }} {extra} }}) {{
            status accepted clientRequestId
            torrent {{ id name state stateDetail totalBytes private files {{ items {{ path length }} }} }}
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
    let torrent = &data["submitTorrent"]["torrent"];
    assert_eq!(torrent["name"], "one.mkv");
    assert_eq!(torrent["totalBytes"], 3000);
    assert_eq!(torrent["state"], "STALLED");
    assert_eq!(torrent["files"]["items"][0]["path"], "one.mkv");
    let id = torrent["id"].as_u64().unwrap();

    let listed = rig
        .execute(&format!(
            "{{ torrents {{ items {{ id }} }} torrent(id: {id}) {{ name infohashV1 }} }}"
        ))
        .await;
    assert!(listed.errors.is_empty(), "{:?}", listed.errors);
    let listed = listed.data.into_json().unwrap();
    assert_eq!(listed["torrents"]["items"][0]["id"], id);
    assert_eq!(listed["torrent"]["infohashV1"].as_str().unwrap().len(), 40);
}

// A metainfo comes as an upload as it comes as base64; a source names one
// arm, never two or none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_is_submitted_from_one_source_arm() {
    use async_graphql::{UploadValue, Variables};

    let rig = rig().await;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(metainfo("uploaded.mkv", 3000, false))
        .unwrap();
    let mut request = Request::new(
        "mutation Submit($input: SubmitTorrentInput!) { submitTorrent(input: $input) { torrent { id name totalBytes } } }",
    )
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .data(rig.torrents.clone())
    .variables(Variables::from_json(
        serde_json::json!({ "input": { "source": { "metainfoUpload": null } } }),
    ));
    request.set_upload(
        "variables.input.source.metainfoUpload",
        UploadValue {
            filename: "uploaded.torrent".to_owned(),
            content_type: Some("application/x-bittorrent".to_owned()),
            content: bytes.into(),
        },
    );
    let response = rig.harness.schema.execute(request).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let added = response.data.into_json().unwrap()["submitTorrent"]["torrent"].clone();
    assert_eq!(added["name"], "uploaded.mkv");
    assert_eq!(added["totalBytes"], 3000);

    // An upload that is no torrent is refused.
    let mut request = Request::new(
        "mutation Submit($input: SubmitTorrentInput!) { submitTorrent(input: $input) { torrent { id } } }",
    )
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .data(rig.torrents.clone())
    .variables(Variables::from_json(
        serde_json::json!({ "input": { "source": { "metainfoUpload": null } } }),
    ));
    request.set_upload(
        "variables.input.source.metainfoUpload",
        UploadValue {
            filename: "notes.nzb".to_owned(),
            content_type: None,
            content: b"<?xml version=\"1.0\"?><nzb></nzb>".to_vec().into(),
        },
    );
    let refused = rig.harness.schema.execute(request).await;
    assert!(!refused.errors.is_empty());

    let two = rig
        .execute(&format!(
            r#"mutation {{ submitTorrent(input: {{ source: {{ metainfoBase64: "{}", magnet: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" }} }}) {{ torrent {{ id }} }} }}"#,
            metainfo("two.mkv", 3000, false)
        ))
        .await;
    assert!(!two.errors.is_empty());
    let none = rig
        .execute("mutation { submitTorrent(input: { source: {} }) { torrent { id } } }")
        .await;
    assert!(!none.errors.is_empty());
    // Neither refused source added a torrent.
    let listed = rig.execute("{ torrents { items { name } } }").await;
    assert_eq!(
        listed.data.into_json().unwrap()["torrents"]["items"],
        serde_json::json!([{ "name": "uploaded.mkv" }])
    );
}

// A torrent's files page by `first` and an offset cursor, in the torrent's
// order, each page with the whole count; the last page's cursor is null, and
// `first: 0` is no files with the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_files_page_by_first_and_an_offset_cursor() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo_of_files("folder", 5), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let files = |arguments: String| {
        let rig = &rig;
        async move {
            let response = rig
                .execute(&format!(
                    "{{ torrent(id: {id}) {{ files{arguments} {{ items {{ path length }} totalCount nextCursor }} }} }}"
                ))
                .await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            response.data.into_json().unwrap()["torrent"]["files"].clone()
        }
    };
    let every = files(String::new()).await;
    assert_eq!(every["totalCount"], 5);
    assert_eq!(every["nextCursor"], Value::Null);
    let names: Vec<String> = every["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names.len(), 5);
    assert!(
        names[0].ends_with("f0.bin") && names[4].ends_with("f4.bin"),
        "{names:?}"
    );

    let mut walked = Vec::new();
    let mut after = None::<String>;
    loop {
        let arguments = match &after {
            Some(cursor) => format!("(first: 2, after: \"{cursor}\")"),
            None => "(first: 2)".to_owned(),
        };
        let page = files(arguments).await;
        assert_eq!(page["totalCount"], 5);
        walked.extend(page["items"].as_array().unwrap().iter().cloned());
        match page["nextCursor"].as_str() {
            Some(cursor) => after = Some(cursor.to_owned()),
            None => break,
        }
    }
    assert_eq!(Value::Array(walked), every["items"]);

    let none = files("(first: 0)".to_owned()).await;
    assert_eq!(
        none,
        serde_json::json!({ "items": [], "totalCount": 5, "nextCursor": null })
    );
    let bad = rig
        .execute(&format!(
            "{{ torrent(id: {id}) {{ files(after: \"not-a-cursor\") {{ totalCount }} }} }}"
        ))
        .await;
    assert_eq!(code(&bad).as_deref(), Some("CURSOR_INVALID"));
}

// One torrent's follower reads what its query selects and never its files
// whole: it is followed without them even when they are asked for, a
// selected `files` is read as its page, and each push is what `torrent(id)`
// reads for the same selection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_updates_read_only_what_the_subscription_selects() {
    use tokio_stream::StreamExt;
    use weaver_server_core::jobs::ids::JobId;
    use weaver_server_core::torrents::service::listing::Shown;
    use weaver_server_core::torrents::service::updates::Follow;

    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo_of_files("folder", 3), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let replaced = rig
        .execute(&format!(
            r#"mutation {{ replaceTorrentTrackers(id: {id}, trackers: [{{ tier: 0, url: "udp://b.invalid:80" }}]) {{ id }} }}"#
        ))
        .await;
    assert!(replaced.errors.is_empty(), "{:?}", replaced.errors);

    // What the subscription follows with nothing beside its own fields.
    let mut bare = rig
        .torrents
        .follow(Follow::One(JobId(id), Shown::BARE))
        .unwrap();
    let first = bare.next().await;
    assert_eq!(first.torrents.len(), 1);
    assert!(first.torrents[0].files.is_empty() && first.torrents[0].trackers.is_empty());
    // Asked for, its files are still not read whole.
    let mut asked = rig
        .torrents
        .follow(Follow::One(
            JobId(id),
            Shown {
                files: true,
                trackers: true,
                tags: true,
            },
        ))
        .unwrap();
    let first = asked.next().await;
    assert!(first.torrents[0].files.is_empty() && first.torrents[0].file_have.is_empty());
    assert!(!first.torrents[0].trackers.is_empty());

    let push = |selection: &'static str| {
        let rig = &rig;
        async move {
            let request = Request::new(format!(
                "subscription {{ torrentUpdates(id: {id}) {{ {selection} }} }}"
            ))
            .data(CallerScope::Local)
            .data(CallerIdentity::Local([7; 32]))
            .data(rig.torrents.clone());
            let next = rig
                .harness
                .schema
                .execute_stream(request)
                .next()
                .await
                .expect("a push");
            assert!(next.errors.is_empty(), "{:?}", next.errors);
            next.data.into_json().unwrap()["torrentUpdates"].clone()
        }
    };
    let own = push("id name state").await;
    assert_eq!(own["id"], id);
    let whole = "id name totalBytes savePath tags files { items { path length } totalCount nextCursor } trackers { tier url }";
    let pushed = push(whole).await;
    let read = rig
        .execute(&format!("{{ torrent(id: {id}) {{ {whole} }} }}"))
        .await;
    assert!(read.errors.is_empty(), "{:?}", read.errors);
    assert_eq!(pushed, read.data.into_json().unwrap()["torrent"]);
    assert_eq!(pushed["files"]["totalCount"], 3);
    assert_eq!(pushed["trackers"][0]["url"], "udp://b.invalid:80");
    let paged = push("files(first: 2) { items { index } totalCount }").await;
    assert_eq!(
        paged["files"],
        serde_json::json!({ "items": [{ "index": 0 }, { "index": 1 }], "totalCount": 3 })
    );
}

// A torrent's files follower gives its page as it is, and refuses a torrent
// there is none of.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_file_updates_give_a_page_of_the_files() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo_of_files("folder", 4), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let follow = |arguments: String| {
        let request = Request::new(format!(
            "subscription {{ torrentFileUpdates({arguments}) {{ items {{ path }} totalCount nextCursor }} }}"
        ))
        .data(CallerScope::Local)
        .data(CallerIdentity::Local([7; 32]))
        .data(rig.torrents.clone());
        rig.harness.schema.execute_stream(request)
    };
    let first = follow(format!("id: {id}, first: 2"))
        .next()
        .await
        .expect("a page");
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let page = first.data.into_json().unwrap()["torrentFileUpdates"].clone();
    let read = rig
        .execute(&format!(
            "{{ torrent(id: {id}) {{ files(first: 2) {{ items {{ path }} totalCount nextCursor }} }} }}"
        ))
        .await;
    assert_eq!(page, read.data.into_json().unwrap()["torrent"]["files"]);
    assert_eq!(page["totalCount"], 4);
    assert!(page["nextCursor"].is_string());

    let unknown = follow("id: 999999".to_owned())
        .next()
        .await
        .expect("an answer");
    assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"));
}

// A torrent's files, peer connections and unpacked files are read on
// `torrent(id)` alone: a list or a row follower that asks for one is
// refused before anything is read, naming the field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_list_refuses_the_fields_read_on_one_torrent() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("listed.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    for (field, selection) in [
        ("files", "files { items { path } }"),
        ("peerConnections", "peerConnections { address }"),
        ("unpacking.files", "unpacking { files { items { path } } }"),
    ] {
        let listed = rig
            .execute(&format!("{{ torrents {{ items {{ id {selection} }} }} }}"))
            .await;
        assert_eq!(code(&listed).as_deref(), Some("INVALID_INPUT"), "{field}");
        let message = &listed.errors[0].message;
        assert!(
            message.contains(field) && message.contains("torrent(id)"),
            "{message}"
        );
        let request = Request::new(format!(
            "subscription {{ torrentRowUpdates(ids: [{id}]) {{ torrents {{ id {selection} }} }} }}"
        ))
        .data(CallerScope::Local)
        .data(CallerIdentity::Local([7; 32]))
        .data(rig.torrents.clone());
        let refused = rig
            .harness
            .schema
            .execute_stream(request)
            .next()
            .await
            .expect("an answer");
        assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"), "{field}");
        assert!(refused.errors[0].message.contains(field), "{field}");
        // The same field is read on the one torrent.
        let one = rig
            .execute(&format!("{{ torrent(id: {id}) {{ id {selection} }} }}"))
            .await;
        assert!(one.errors.is_empty(), "{field}: {:?}", one.errors);
    }
}

// One torrent's read builds only what the query selects: its own fields
// alone read no files, trackers or tags, and a query that selects them gets
// what the whole torrent's read gives, its save path included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_torrent_reads_only_what_the_query_selects() {
    use weaver_server_core::jobs::ids::JobId;
    use weaver_server_core::torrents::service::listing::Shown;

    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let replaced = rig
        .execute(&format!(
            r#"mutation {{ replaceTorrentTrackers(id: {id}, trackers: [{{ tier: 0, url: "udp://b.invalid:80" }}]) {{ id }} }}"#
        ))
        .await;
    assert!(replaced.errors.is_empty(), "{:?}", replaced.errors);
    rig.harness
        .db
        .add_job_tags(&[JobId(id)], ["tv", "HD"])
        .unwrap();

    let whole = rig.torrents.torrent(JobId(id)).await.unwrap();
    assert!(!whole.files.is_empty() && !whole.trackers.is_empty() && !whole.tags.is_empty());
    let bare = rig
        .torrents
        .torrent_shown(JobId(id), Shown::BARE)
        .await
        .unwrap();
    assert!(bare.files.is_empty() && bare.trackers.is_empty() && bare.tags.is_empty());
    // Its own fields are the whole read's.
    assert_eq!(
        (
            bare.job_id,
            &bare.name,
            &bare.infohash,
            bare.state,
            bare.total_bytes,
            &bare.storage_dir,
            &bare.files_dir
        ),
        (
            whole.job_id,
            &whole.name,
            &whole.infohash,
            whole.state,
            whole.total_bytes,
            &whole.storage_dir,
            &whole.files_dir
        )
    );

    let read = |query: String| {
        let rig = &rig;
        async move {
            let response = rig.execute(&query).await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            response.data.into_json().unwrap()["torrent"].clone()
        }
    };
    let own = read(format!("{{ torrent(id: {id}) {{ id state name }} }}")).await;
    assert_eq!(own["id"], id);
    assert_eq!(own["name"], whole.name);
    let full = read(format!(
        "{{ torrent(id: {id}) {{ id savePath tags files {{ items {{ path length }} totalCount }} trackers {{ tier url }} }} }}"
    ))
    .await;
    let (save_path, _) = whole.paths();
    assert_eq!(
        full["savePath"],
        serde_json::json!((!save_path.is_empty()).then_some(save_path))
    );
    assert_eq!(full["tags"], serde_json::json!(whole.tags));
    assert_eq!(full["tags"], serde_json::json!(["HD", "tv"]));
    assert_eq!(full["files"]["items"][0]["path"], "one.mkv");
    assert_eq!(full["files"]["items"][0]["length"], 3000);
    assert_eq!(full["files"]["totalCount"], 1);
    assert_eq!(
        full["trackers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tracker| tracker["url"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        whole
            .trackers
            .iter()
            .map(|tracker| tracker.url.clone())
            .collect::<Vec<_>>()
    );
    // The save path alone still reads the files it is made from.
    let saved = read(format!("{{ torrent(id: {id}) {{ savePath }} }}")).await;
    assert_eq!(saved["savePath"], full["savePath"]);
    // And a list's save path is the one torrent's: a torrent of one file
    // saves in its folder.
    let response = rig.execute("{ torrents { items { savePath } } }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let listed = response.data.into_json().unwrap();
    assert_eq!(listed["torrents"]["items"][0]["savePath"], full["savePath"]);
}

// A new torrent is ACCEPTED, and its client request id is echoed and kept
// with its job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_torrent_is_accepted() {
    let rig = rig().await;
    let bytes = metainfo("one.mkv", 3000, false);
    let response = rig
        .execute(&submit(&bytes, r#"clientRequestId: "req-1""#))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let result = response.data.into_json().unwrap()["submitTorrent"].clone();
    assert_eq!(result["status"], "ACCEPTED");
    assert_eq!(result["accepted"], true);
    assert_eq!(result["clientRequestId"], "req-1");
    assert_eq!(result["torrent"]["name"], "one.mkv");
}

// The same torrent again with the client request id it was added with is
// a replay: accepted, answered with the torrent already added, and nothing
// of that torrent changes, though the replay asks for other settings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_of_the_same_request_answers_the_torrent_unchanged() {
    let rig = rig().await;
    let bytes = metainfo("one.mkv", 3000, false);
    let first = rig
        .execute(&submit(&bytes, r#"clientRequestId: "req-1" forced: false"#))
        .await;
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let id = first.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let settings = |id: u64| {
        format!(
            "{{ torrent(id: {id}) {{ forced paused: state uploadLimitBps seedingPolicy {{ ratioGoal }} }} }}"
        )
    };
    let before = rig.execute(&settings(id)).await.data.into_json().unwrap();

    let again = rig
        .execute(&submit(
            &bytes,
            r#"clientRequestId: " req-1 " forced: true paused: true uploadLimitBps: 5
               seeding: { ratioGoal: { state: SET, value: 9.0 } }"#,
        ))
        .await;
    assert!(again.errors.is_empty(), "{:?}", again.errors);
    let result = again.data.into_json().unwrap()["submitTorrent"].clone();
    assert_eq!(result["status"], "IDEMPOTENT_REPLAY");
    assert_eq!(result["accepted"], true);
    assert_eq!(result["clientRequestId"], " req-1 ");
    assert_eq!(result["torrent"]["id"], id);
    assert_eq!(
        rig.execute(&settings(id)).await.data.into_json().unwrap(),
        before
    );
}

// The same torrent again with another client request id, or none, is a
// duplicate: not accepted, answered with the torrent already added, and no
// error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_is_answered_and_not_accepted() {
    let rig = rig().await;
    let bytes = metainfo("one.mkv", 3000, false);
    let first = rig
        .execute(&submit(&bytes, r#"clientRequestId: "req-1""#))
        .await;
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let id = first.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"].clone();
    for extra in [
        r#"clientRequestId: "req-2""#,
        "",
        r#"clientRequestId: "  ""#,
    ] {
        let again = rig.execute(&submit(&bytes, extra)).await;
        assert!(again.errors.is_empty(), "{extra}: {:?}", again.errors);
        let result = again.data.into_json().unwrap()["submitTorrent"].clone();
        assert_eq!(result["status"], "DUPLICATE", "{extra}");
        assert_eq!(result["accepted"], false, "{extra}");
        assert_eq!(result["torrent"]["id"], id, "{extra}");
    }
    // A torrent added with no client request id is a duplicate for any.
    let other = metainfo("two.mkv", 3000, false);
    assert!(rig.execute(&submit(&other, "")).await.errors.is_empty());
    let again = rig
        .execute(&submit(&other, r#"clientRequestId: "req-3""#))
        .await;
    let result = again.data.into_json().unwrap()["submitTorrent"].clone();
    assert_eq!(result["status"], "DUPLICATE");
    assert_eq!(result["clientRequestId"], "req-3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_is_added_without_a_network() {
    let rig = rig().await;
    let response = rig
        .execute(
            r#"mutation { submitTorrent(input: { source: {
                magnet: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce"
            } }) { torrent { id } } }"#,
        )
        .await;
    // The add does not wait on its route: the torrent is held until the
    // route opens.
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert!(
        response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
            .as_u64()
            .is_some()
    );
}

// A torrent whose every file has an unwanted extension is added with
// nothing wanted, as qBittorrent adds it, not refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_extension_is_left_out_not_refused() {
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
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let listed = rig.execute("{ torrents { items { id } } }").await;
    assert_eq!(
        listed.data.into_json().unwrap()["torrents"]["items"],
        serde_json::json!([{ "id": id }])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_a_private_torrent_short_of_its_goal_is_never_refused() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(
            &metainfo("p.bin", 2000, true),
            "seeding: { ratioGoal: { state: SET, value: 1.0 } }",
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    // The page names what is left before it asks; the server takes the
    // removal as asked.
    let left = rig
        .execute(&format!("{{ torrent(id: {id}) {{ seedingGoalLeft }} }}"))
        .await;
    assert!(left.errors.is_empty(), "{:?}", left.errors);
    assert_eq!(
        left.data.into_json().unwrap()["torrent"]["seedingGoalLeft"],
        "ratio 0.00 of 1.00"
    );
    let removed = rig
        .execute(&format!("mutation {{ removeTorrent(id: {id}) }}"))
        .await;
    assert!(removed.errors.is_empty(), "{:?}", removed.errors);
    assert_eq!(removed.data.into_json().unwrap()["removeTorrent"], id);
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
    for query in [
        "{ torrents { items { id } } }",
        "{ torrentScheduleSeeding { ruleId } }",
    ] {
        let response = harness.execute(query).await;
        assert_eq!(
            code(&response).as_deref(),
            Some("TORRENTS_DISABLED"),
            "{query}"
        );
    }
    // Turned on and then off, every route shows that torrents are off.
    let rig = rig().await;
    let settings = |enabled| TorrentSettings {
        upload_cap: None,
        complete_dir: rig._dir.path().join("complete"),
        categories: Default::default(),
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
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
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

const DETAIL: &str = "connectedLeechers connectedPeers connectedSeeds files { items { path progress } }
    seedingOverride { ratioGoal { state value } seedTimeGoalMinutes { state value } }
    seedingPolicy { ratioGoal seedTimeGoalMinutes mode levels { ratioGoal seedTimeGoalMinutes mode } }";

// A torrent reads its leechers, each file's progress, and its seeding terms
// with the level each comes from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_reads_its_leechers_file_progress_and_seeding_policy() {
    let rig = rig().await;
    let policy = |ratio_goal| SeedingPolicy {
        ratio_goal: Leaf::Set(ratio_goal),
        ..SeedingPolicy::default()
    };
    let mut settings = TorrentSettings {
        upload_cap: None,
        complete_dir: rig._dir.path().join("complete"),
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: Some(policy(3.0)),
            torrent_route: false,
            unpack: None,
        }]
        .into(),
        seeding: Some(policy(1.5)),
        torrent: TorrentConfig {
            enabled: true,
            ..TorrentConfig::default()
        },
    };
    rig.torrents.set_settings(settings.clone()).await;

    let rig = &rig;
    let read = |query: String| async move {
        let response = rig.execute(&query).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    };
    let id = |data: &Value| data["submitTorrent"]["torrent"]["id"].as_u64().unwrap();
    let detail = |id: u64| format!("{{ torrent(id: {id}) {{ {DETAIL} }} }}");

    let global = id(&read(submit(&metainfo("a.mkv", 3000, false), "")).await);
    let torrent = &read(detail(global)).await["torrent"];
    assert_eq!(torrent["connectedLeechers"], 0);
    assert_eq!(torrent["files"]["items"][0]["path"], "a.mkv");
    assert_eq!(torrent["files"]["items"][0]["progress"], 0.0);
    assert_eq!(torrent["seedingOverride"], Value::Null);
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 1.5);
    assert_eq!(torrent["seedingPolicy"]["levels"]["ratioGoal"], "GLOBAL");

    let category = id(&read(submit(&metainfo("b.mkv", 3000, false), "category: \"tv\"")).await);
    let torrent = &read(detail(category)).await["torrent"];
    assert_eq!(torrent["seedingOverride"], Value::Null);
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 3.0);
    assert_eq!(torrent["seedingPolicy"]["levels"]["ratioGoal"], "CATEGORY");

    let own = id(&read(submit(
        &metainfo("c.mkv", 3000, false),
        "category: \"tv\" seeding: { ratioGoal: { state: SET, value: 2.0 } }",
    ))
    .await);
    let torrent = &read(detail(own)).await["torrent"];
    assert_eq!(torrent["seedingOverride"]["ratioGoal"]["state"], "SET");
    assert_eq!(torrent["seedingOverride"]["ratioGoal"]["value"], 2.0);
    assert_eq!(
        torrent["seedingOverride"]["seedTimeGoalMinutes"]["state"],
        "INHERIT"
    );
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 2.0);
    assert_eq!(torrent["seedingPolicy"]["levels"]["ratioGoal"], "TORRENT");
    // The mode is never inherited: the torrent sets a goal, so its own
    // mode holds.
    assert_eq!(torrent["seedingPolicy"]["levels"]["mode"], "TORRENT");

    // A goal switched off at the torrent drops the one its category sets.
    let off = id(&read(submit(
        &metainfo("d.mkv", 3000, false),
        "category: \"tv\" seeding: { ratioGoal: { state: OFF } }",
    ))
    .await);
    let torrent = &read(detail(off)).await["torrent"];
    assert_eq!(torrent["seedingOverride"]["ratioGoal"]["state"], "OFF");
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], Value::Null);
    assert_eq!(torrent["seedingPolicy"]["levels"]["ratioGoal"], "TORRENT");

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
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    assert_eq!(next(id).await, ("ITEM_CREATED".into(), "STALLED".into()));

    let paused = rig
        .execute(&format!(
            "mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"
        ))
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
    let state = "{ globalQueueState { isPaused pausedClasses } }";
    let read = |response: Response| {
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().unwrap();
        (
            data["globalQueueState"]["isPaused"].as_bool().unwrap(),
            data["globalQueueState"]["pausedClasses"] == serde_json::json!(["TORRENT"]),
        )
    };

    let response = harness
        .execute("mutation { pauseClass(class: TORRENT) }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert!(harness.handle.state().holds().held_by(
        HoldOrigin::Manual,
        HoldOwner::Class(weaver_server_core::torrents::CLASS)
    ));
    assert_eq!(read(harness.execute(state).await), (false, true));

    let response = harness.execute("mutation { pauseQueue { success } }").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let response = harness
        .execute("mutation { resumeQueue { success } }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(read(harness.execute(state).await), (false, true));

    let response = harness
        .execute("mutation { resumeClass(class: TORRENT) }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(read(harness.execute(state).await), (false, false));

    // The pipeline has no pause of its own apart from the queue's.
    let response = harness.execute("mutation { pauseClass(class: NZB) }").await;
    assert!(!response.errors.is_empty());
}

// A timed pause of every torrent lifts by itself at its deadline, apart from
// the queue's own pause, and is kept in the settings until then.
#[tokio::test(start_paused = true)]
async fn a_timed_pause_of_every_torrent_lifts_by_itself() {
    use weaver_server_core::jobs::{HoldOrigin, HoldOwner};
    let harness = TestHarness::new().await;
    let held = || {
        harness.handle.state().holds().held_by(
            HoldOrigin::Manual,
            HoldOwner::Class(weaver_server_core::torrents::CLASS),
        )
    };
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let response = harness
        .execute("mutation { pauseClass(class: TORRENT, resumeAfterSecs: 1800) }")
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert!(held());
    assert!(!harness.handle.is_globally_paused());

    let resume_at = harness
        .scheduled_resume
        .class_resume_at(weaver_server_core::torrents::CLASS)
        .await;
    assert!(resume_at >= before + 1800);
    assert_eq!(
        harness
            .db
            .get_setting("scheduled_resume_at.class.torrent")
            .unwrap(),
        Some(resume_at.to_string())
    );
    let state = "{ globalQueueState { resumeAtEpochMs classResumes { class resumeAtEpochMs } } }";
    let response = harness.execute(state).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    assert!(data["globalQueueState"]["resumeAtEpochMs"].is_null());
    assert_eq!(
        data["globalQueueState"]["classResumes"],
        serde_json::json!([{ "class": "TORRENT", "resumeAtEpochMs": resume_at * 1000 }])
    );

    // The timer sleeps on the paused clock from whenever its task first runs,
    // so the clock steps a second at a time until the hold lifts.
    while held() {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
    }
    let response = harness.execute(state).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["globalQueueState"]["classResumes"],
        serde_json::json!([])
    );
    assert_eq!(
        harness
            .db
            .get_setting("scheduled_resume_at.class.torrent")
            .unwrap(),
        None
    );
}

// A timed torrent pause saved before a restart is armed again on start-up;
// one whose deadline passed while Weaver was down lifts at once.
#[tokio::test(start_paused = true)]
async fn a_saved_torrent_timer_is_recovered_on_start_up() {
    use weaver_server_core::jobs::{HoldOrigin, HoldOwner};
    let harness = TestHarness::new().await;
    let class = weaver_server_core::torrents::CLASS;
    let held = || {
        harness
            .handle
            .state()
            .holds()
            .held_by(HoldOrigin::Manual, HoldOwner::Class(class))
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    harness.handle.pause_class(class).await.unwrap();
    harness
        .db
        .set_setting(
            "scheduled_resume_at.class.torrent",
            &(now + 600).to_string(),
        )
        .unwrap();
    harness.scheduled_resume.recover().await.unwrap();
    assert_eq!(
        harness.scheduled_resume.class_resume_at(class).await,
        now + 600
    );
    assert_eq!(harness.scheduled_resume.resume_at().await, 0);
    assert!(held());

    harness
        .db
        .set_setting("scheduled_resume_at.class.torrent", &(now - 1).to_string())
        .unwrap();
    harness.scheduled_resume.recover().await.unwrap();
    assert!(!held());
    assert_eq!(
        harness
            .db
            .get_setting("scheduled_resume_at.class.torrent")
            .unwrap(),
        None
    );
}

// Resuming every torrent by hand, or pausing them again without a timer,
// ends the timed pause, so its deadline lifts nothing later.
#[tokio::test(start_paused = true)]
async fn a_hand_resume_or_pause_ends_the_torrent_timer() {
    use weaver_server_core::jobs::{HoldOrigin, HoldOwner};
    let harness = TestHarness::new().await;
    let held = || {
        harness.handle.state().holds().held_by(
            HoldOrigin::Manual,
            HoldOwner::Class(weaver_server_core::torrents::CLASS),
        )
    };
    for (follow, expect_held) in [
        ("mutation { resumeClass(class: TORRENT) }", false),
        ("mutation { pauseClass(class: TORRENT) }", true),
    ] {
        let response = harness
            .execute("mutation { pauseClass(class: TORRENT, resumeAfterSecs: 60) }")
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        tokio::task::yield_now().await;
        let response = harness.execute(follow).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            harness
                .scheduled_resume
                .class_resume_at(weaver_server_core::torrents::CLASS)
                .await,
            0
        );
        assert_eq!(
            harness
                .db
                .get_setting("scheduled_resume_at.class.torrent")
                .unwrap(),
            None
        );
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert_eq!(held(), expect_held, "{follow}");
    }
    for secs in [0, 2_592_001] {
        let response = harness
            .execute(&format!(
                "mutation {{ pauseClass(class: TORRENT, resumeAfterSecs: {secs}) }}"
            ))
            .await;
        assert!(!response.errors.is_empty(), "{secs} accepted");
    }
}

// The live metrics say what every torrent downloads and uploads, added up:
// the read and the push give the service's own sums, and Usenet's speed
// stays its own figure.
#[tokio::test]
async fn live_metrics_carry_what_every_torrent_downloads_and_uploads() {
    use tokio_stream::StreamExt;
    use weaver_server_core::torrents::service::views::TorrentView;

    let rig = rig().await;
    const RATES: &str = "{ torrentRates { downloadBps uploadBps } }";
    let none = rig.execute(RATES).await;
    assert!(none.errors.is_empty(), "{:?}", none.errors);
    assert_eq!(
        none.data.into_json().unwrap()["torrentRates"],
        serde_json::json!({ "downloadBps": 0, "uploadBps": 0 })
    );

    let response = rig
        .execute(&submit(&metainfo("rates.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();

    // No tick reads the torrents again, and the rates sit on two rows no
    // torrent has, so no publish of the one added sets them back.
    let _held = rig.torrents.hold_tick().await;
    let snapshot = rig.torrents.snapshot();
    let row = snapshot
        .row(weaver_server_core::JobId(id))
        .expect("the added torrent has a row");
    let moving = |job: u64, download_bps: u64, upload_bps: u64| TorrentView {
        job_id: weaver_server_core::JobId(job),
        download_bps,
        upload_bps,
        ..row.view.clone()
    };
    for (job, down, up) in [(id + 1000, 3000, 700), (id + 1001, 500, 0)] {
        snapshot.put_one(
            weaver_server_core::JobId(job),
            Some(&moving(job, down, up)),
            snapshot.stamp(),
        );
    }
    let sums = serde_json::json!({ "downloadBps": 3500, "uploadBps": 700 });

    let read = rig.execute(RATES).await;
    assert!(read.errors.is_empty(), "{:?}", read.errors);
    assert_eq!(read.data.into_json().unwrap()["torrentRates"], sums);

    let request = Request::new(
        "subscription { systemMetricsUpdates { metrics { currentDownloadSpeed } torrentRates { downloadBps uploadBps } } }",
    )
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .data(rig.torrents.clone());
    let mut updates = rig.harness.schema.execute_stream(request);
    let first = updates.next().await.expect("the updates stay open");
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let pushed = first.data.into_json().unwrap()["systemMetricsUpdates"].clone();
    assert_eq!(pushed["torrentRates"], sums);
    assert_eq!(pushed["metrics"]["currentDownloadSpeed"], 0);
}

// One torrent's follower gets it as it is, then again each time it moves,
// and a last null once it is gone, after which the stream ends. The queue's
// own mutations pause, resume and recategorize a torrent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_updates_push_one_torrent_until_it_is_gone() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();

    let request = Request::new(format!(
        "subscription {{ torrentUpdates(id: {id}) {{ id name state category }} }}"
    ))
    .data(CallerScope::Local)
    .data(CallerIdentity::Local([7; 32]))
    .data(rig.torrents.clone());
    let mut updates = rig.harness.schema.execute_stream(request);
    let mut push = async || {
        let next = updates.next().await?;
        assert!(next.errors.is_empty(), "{:?}", next.errors);
        Some(next.data.into_json().unwrap()["torrentUpdates"].clone())
    };
    let first = push().await.expect("the updates stay open");
    assert_eq!(first["id"], id);
    assert_eq!(first["name"], "one.mkv");
    assert_eq!(first["state"], "STALLED");

    let run = |query: String| {
        let rig = &rig;
        async move {
            let response = rig.execute(&query).await;
            assert!(response.errors.is_empty(), "{query}: {:?}", response.errors);
        }
    };
    run(format!(
        "mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"
    ))
    .await;
    while push().await.expect("the updates stay open")["state"] != "PAUSED" {}
    run(format!(
        "mutation {{ resumeQueueItem(id: {id}) {{ success }} }}"
    ))
    .await;
    while push().await.expect("the updates stay open")["state"] == "PAUSED" {}

    let category = |name: &str| {
        format!(
            r#"mutation {{ setQueueItemsCategory(target: {{ ids: [{id}] }}, category: "{name}") {{ applied }} }}"#
        )
    };
    run(r#"mutation { addCategory(input: { name: "tv" }) { id } }"#.to_owned()).await;
    let applied = rig.execute(&category("tv")).await;
    assert!(applied.errors.is_empty(), "{:?}", applied.errors);
    assert_eq!(
        applied.data.into_json().unwrap()["setQueueItemsCategory"]["applied"],
        1
    );
    while push().await.expect("the updates stay open")["category"] != "tv" {}
    // The empty name clears it.
    let cleared = rig.execute(&category("")).await;
    assert!(cleared.errors.is_empty(), "{:?}", cleared.errors);
    assert_eq!(
        cleared.data.into_json().unwrap()["setQueueItemsCategory"]["applied"],
        1
    );
    while !push().await.expect("the updates stay open")["category"].is_null() {}

    run(format!("mutation {{ removeTorrent(id: {id}) }}")).await;
    loop {
        let next = push().await.expect("a last push");
        if next.is_null() {
            break;
        }
    }
    assert!(push().await.is_none(), "the stream ends after the null");
}

// `torrents` pages as `queueItems` does: `first` after an offset cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_page_by_first_and_an_offset_cursor() {
    let rig = rig().await;
    for name in ["one.mkv", "two.mkv", "three.mkv"] {
        let response = rig.execute(&submit(&metainfo(name, 3000, false), "")).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }
    let names = |query: String| {
        let rig = &rig;
        async move {
            let response = rig.execute(&query).await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            response.data.into_json().unwrap()["torrents"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|torrent| torrent["name"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        }
    };
    let cursor = |offset: usize| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("off:{offset}"))
    };
    assert_eq!(
        names("{ torrents { items { name } } }".to_owned()).await,
        ["one.mkv", "two.mkv", "three.mkv"]
    );
    assert_eq!(
        names("{ torrents(first: 2) { items { name } } }".to_owned()).await,
        ["one.mkv", "two.mkv"]
    );
    assert_eq!(
        names(format!(
            "{{ torrents(first: 2, after: \"{}\") {{ items {{ name }} }} }}",
            cursor(2)
        ))
        .await,
        ["three.mkv"]
    );
    let invalid = rig
        .execute("{ torrents(after: \"not-a-cursor\") { nextCursor } }")
        .await;
    assert_eq!(
        invalid.errors[0]
            .extensions
            .as_ref()
            .and_then(|ext| ext.get("code")),
        Some(&async_graphql::Value::String("CURSOR_INVALID".to_owned()))
    );
}

// A tracker rule keeps the client its torrents name themselves as, and
// absent is Weaver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_the_client_its_torrents_name_themselves_as() {
    let rig = rig().await;
    let save = |id: &str, client: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                name: "tracker.test", hostPatterns: ["tracker.test"], client: {client}
            }}) {{ id client }} }}"#
        )
    };

    let response = rig.execute(&save("", "QBITTORRENT")).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    assert_eq!(saved["saveTrackerRule"][0]["client"], "QBITTORRENT");
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();

    for (client, read) in [
        ("WEAVER", serde_json::json!("WEAVER")),
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
    assert!(
        !rig.execute(&save("", "TRANSMISSION"))
            .await
            .errors
            .is_empty()
    );
}

// A tracker rule keeps its torrents' DHT, PEX and LSD, each on its own,
// their encryption, transports, upload cap and forced start; absent
// inherits, and an upload cap switched off is none. A preference for a
// transport the rule leaves off is kept as set, and a rule that leaves both
// off over the global transports is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rule_keeps_its_torrents_peer_terms() {
    let rig = rig().await;
    let fields = "id dht pex lsd encryption peerTcp peerUtp preferDownloading preferSeeding uploadCap { state value } forced";
    let save = |id: &str, terms: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                name: "*.example-tracker.test", hostPatterns: ["*.example-tracker.test"], client: QBITTORRENT, {terms}
            }}) {{ {fields} }} }}"#
        )
    };

    let response = rig
        .execute(&save(
            "",
            "dht: false, pex: false, lsd: true, encryption: PREFER, peerUtp: false, preferSeeding: UTP, uploadCap: { state: OFF }, forced: true",
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let saved = response.data.into_json().unwrap();
    let id = saved["saveTrackerRule"][0]["id"].as_u64().unwrap();
    assert_eq!(
        saved["saveTrackerRule"][0],
        serde_json::json!({
            "id": id, "dht": false, "pex": false, "lsd": true, "encryption": "PREFER",
            "peerTcp": null, "peerUtp": false, "preferDownloading": null, "preferSeeding": "UTP",
            "uploadCap": { "state": "OFF", "value": null }, "forced": true,
        })
    );

    for (terms, read) in [
        (
            "dht: true, lsd: false, encryption: REQUIRE, peerTcp: false, preferDownloading: UTP, uploadCap: { state: SET, value: 4096 }",
            serde_json::json!({
                "id": id, "dht": true, "pex": null, "lsd": false, "encryption": "REQUIRE",
                "peerTcp": false, "peerUtp": null, "preferDownloading": "UTP", "preferSeeding": null,
                "uploadCap": { "state": "SET", "value": 4096 }, "forced": false,
            }),
        ),
        (
            "encryption: DISABLED",
            serde_json::json!({
                "id": id, "dht": null, "pex": null, "lsd": null, "encryption": "DISABLED",
                "peerTcp": null, "peerUtp": null, "preferDownloading": null, "preferSeeding": null,
                "uploadCap": { "state": "INHERIT", "value": null }, "forced": false,
            }),
        ),
        (
            "",
            serde_json::json!({
                "id": id, "dht": null, "pex": null, "lsd": null, "encryption": null,
                "peerTcp": null, "peerUtp": null, "preferDownloading": null, "preferSeeding": null,
                "uploadCap": { "state": "INHERIT", "value": null }, "forced": false,
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

    // Both off is refused, on the rule's own fields or over the global
    // ones, and the rule stays as it was.
    let kept = serde_json::json!([{
        "id": id, "dht": null, "pex": null, "lsd": null, "encryption": null,
        "peerTcp": null, "peerUtp": null, "preferDownloading": null, "preferSeeding": null,
        "uploadCap": { "state": "INHERIT", "value": null }, "forced": false,
    }]);
    let refused = rig
        .execute(&save(
            &format!("id: {id},"),
            "peerTcp: false, peerUtp: false",
        ))
        .await;
    assert!(!refused.errors.is_empty());
    let global_utp_off = rig
        .execute("mutation { updateSettings(input: { torrent: { peerUtp: false } }) { torrent { peerUtp } } }")
        .await;
    assert!(
        global_utp_off.errors.is_empty(),
        "{:?}",
        global_utp_off.errors
    );
    let refused = rig
        .execute(&save(&format!("id: {id},"), "peerTcp: false"))
        .await;
    assert!(!refused.errors.is_empty());
    // A rule that turns uTP back on broadens the global setting.
    let broadened = rig
        .execute(&save(
            &format!("id: {id},"),
            "peerTcp: false, peerUtp: true",
        ))
        .await;
    assert!(broadened.errors.is_empty(), "{:?}", broadened.errors);
    let saved = broadened.data.into_json().unwrap();
    assert_eq!(saved["saveTrackerRule"][0]["peerTcp"], false);
    assert_eq!(saved["saveTrackerRule"][0]["peerUtp"], true);
    let response = rig.execute(&save(&format!("id: {id},"), "")).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let response = rig
        .execute(&format!("{{ trackerRules {{ {fields} }} }}"))
        .await;
    assert_eq!(response.data.into_json().unwrap()["trackerRules"], kept);
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
                name: "ssl-tracker.test", hostPatterns: ["ssl-tracker.test"], forced: true, {ssl}
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
                name: "tracker.test", hostPatterns: ["tracker.test"]{route}
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
    let rule = |id: &str, name: &str, pattern: &str| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                name: "{name}", hostPatterns: ["{pattern}"], neverPause: true,
                seeding: {{ ratioGoal: {{ state: SET, value: 3 }} }}
            }}) {{ id name hostPatterns neverPause seeding {{ ratioGoal {{ value }} }} }} }}"#
        )
    };

    let saved = read(rule("", " Example ", " *.Tracker.TEST. ")).await;
    let saved = &saved["saveTrackerRule"][0];
    assert_eq!(saved["name"], "Example");
    assert_eq!(saved["hostPatterns"], serde_json::json!(["*.tracker.test"]));
    assert_eq!(saved["neverPause"], true);
    assert_eq!(saved["seeding"]["ratioGoal"]["value"], 3.0);
    let rule_id = saved["id"].as_u64().unwrap();
    for refused in [
        rule("", "Other", "*.tracker.test"),
        rule("", "Other", "not a host"),
        rule("", "EXAMPLE", "*.other.test"),
    ] {
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
    let id = read(submit(&announcing, "")).await["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let source = || async move {
        let data = read(format!(
            "{{ torrent(id: {id}) {{ trackerRuleName trackerRule {{ id name }} \
             seedingPolicy {{ ratioGoal levels {{ ratioGoal }} }} }} }}"
        ))
        .await;
        data["torrent"].clone()
    };
    let torrent = source().await;
    assert_eq!(
        torrent["seedingPolicy"]["levels"]["ratioGoal"],
        "TRACKER_RULE"
    );
    assert_eq!(torrent["trackerRuleName"], "Example");
    assert_eq!(
        torrent["trackerRule"],
        serde_json::json!({ "id": rule_id, "name": "Example" })
    );
    assert_eq!(torrent["seedingPolicy"]["ratioGoal"], 3.0);
    // The rule itself is an admin's to read; its name is not.
    let response = rig
        .execute_as_key(
            &format!("{{ torrent(id: {id}) {{ trackerRuleName trackerRule {{ id }} }} }}"),
            CallerScope::Read,
        )
        .await;
    assert!(
        response
            .errors
            .iter()
            .any(|error| error.message.contains("admin scope")),
        "{:?}",
        response.errors
    );
    let response = rig
        .execute_as_key(
            &format!("{{ torrent(id: {id}) {{ trackerRuleName }} }}"),
            CallerScope::Read,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["torrent"]["trackerRuleName"],
        "Example"
    );
    // No schedule pause is in force, so none is left seeding through one.
    assert_eq!(
        read("{ torrentScheduleSeeding { ruleId name torrents } }".into()).await["torrentScheduleSeeding"],
        serde_json::json!([])
    );

    let left = read(format!(
        "mutation {{ deleteTrackerRule(id: {rule_id}) {{ id }} }}"
    ))
    .await;
    assert_eq!(left["deleteTrackerRule"], serde_json::json!([]));
    let torrent = source().await;
    assert_eq!(torrent["seedingPolicy"]["levels"]["ratioGoal"], "GLOBAL");
    assert_eq!(torrent["trackerRuleName"], Value::Null);
    assert_eq!(torrent["trackerRule"], Value::Null);
    assert_eq!(
        read("{ trackerRules { id } }".into()).await["trackerRules"],
        serde_json::json!([])
    );
}

// After each save of a tracker rule the rules in force are the saved ones:
// an edit that leaves the routes alone, one that gives the rule its own
// route, and one that takes it away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rules_in_force_are_the_ones_each_save_leaves() {
    let rig = rig().await;
    let rig = &rig;
    let read = |query: String| async move {
        let response = rig.execute(&query).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    };
    let rule = |id: &str, ratio: u32, route: bool| {
        format!(
            r#"mutation {{ saveTrackerRule({id} input: {{
                name: "*.tracker.test", hostPatterns: ["*.tracker.test"], torrentRoute: {route},
                seeding: {{ ratioGoal: {{ state: SET, value: {ratio} }} }}
            }}) {{ id }} }}"#
        )
    };
    let rule_id = read(rule("", 3, false)).await["saveTrackerRule"][0]["id"]
        .as_u64()
        .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(metainfo("ruled.mkv", 3000, false))
        .unwrap();
    let mut announcing = b"d".to_vec();
    bstr(&mut announcing, b"announce");
    bstr(&mut announcing, b"http://a.tracker.test/announce");
    announcing.extend_from_slice(&bytes[1..]);
    let announcing = base64::engine::general_purpose::STANDARD.encode(announcing);
    let id = read(submit(&announcing, "")).await["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let ratio = || async move {
        read(format!(
            "{{ torrent(id: {id}) {{ seedingPolicy {{ ratioGoal }} }} }}"
        ))
        .await["torrent"]["seedingPolicy"]["ratioGoal"]
            .clone()
    };
    assert_eq!(ratio().await, 3.0);
    let edit = format!("id: {rule_id},");
    for (goal, route) in [(5, false), (6, true), (7, true), (8, false)] {
        read(rule(&edit, goal, route)).await;
        assert_eq!(ratio().await, f64::from(goal), "route {route}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_peers_are_listed_and_a_ban_is_kept() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();

    let listed = rig
        .execute(&format!(
            "{{ torrent(id: {id}) {{ peerConnections {{ address client peerId connection \
             encryption flags {{ choked remoteChoked }} pieces progress }} }} }}"
        ))
        .await;
    assert!(listed.errors.is_empty(), "{:?}", listed.errors);
    assert_eq!(
        listed.data.into_json().unwrap()["torrent"]["peerConnections"],
        Value::Array(Vec::new())
    );

    for address in ["127.0.0.3:6881", "[::1]:6881", "127.0.0.4"] {
        let banned = rig
            .execute(&format!(
                r#"mutation {{ banTorrentPeer(id: {id}, address: "{address}") {{ id }} }}"#
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
            r#"mutation {{ banTorrentPeer(id: {id}, address: "somewhere") {{ id }} }}"#
        ))
        .await;
    assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"));
    let unknown = rig
        .execute(r#"mutation { banTorrentPeer(id: 999999, address: "127.0.0.3") { id } }"#)
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("NOT_FOUND"));
    let unknown = rig
        .execute("{ torrent(id: 999999) { peerConnections { address } } }")
        .await;
    assert!(unknown.errors.is_empty(), "{:?}", unknown.errors);
    assert_eq!(unknown.data.into_json().unwrap()["torrent"], Value::Null);
}

// libtorrent `torrent::set_upload_limit` / `set_download_limit`: a torrent's
// own limits are set together and read back, in the mutation's answer and
// after; null or left out is unlimited and 0 holds that direction closed,
// and a value past the largest stored one is refused as `setBandwidth`
// refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_rate_limits_are_set_and_read_back() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let limits = |torrent: &Value| {
        (
            torrent["uploadLimitBps"].clone(),
            torrent["downloadLimitBps"].clone(),
        )
    };
    let read = || async {
        let read = rig
            .execute(&format!(
                "{{ torrent(id: {id}) {{ uploadLimitBps downloadLimitBps }} }}"
            ))
            .await;
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        limits(&read.data.into_json().unwrap()["torrent"])
    };
    assert_eq!(read().await, (Value::Null, Value::Null));

    for (limits_set, want) in [
        (
            "uploadBps: 65536, downloadBps: 0",
            (Value::from(65_536), Value::from(0)),
        ),
        (
            "downloadBps: 1048576",
            (Value::Null, Value::from(1_048_576)),
        ),
        (
            "uploadBps: 4096, downloadBps: null",
            (Value::from(4096), Value::Null),
        ),
    ] {
        let set = rig
            .execute(&format!(
                "mutation {{ setTorrentRateLimits(id: {id}, {limits_set}) {{ uploadLimitBps downloadLimitBps }} }}"
            ))
            .await;
        assert!(set.errors.is_empty(), "{:?}", set.errors);
        assert_eq!(
            limits(&set.data.into_json().unwrap()["setTorrentRateLimits"]),
            want,
            "{limits_set}"
        );
        assert_eq!(read().await, want, "{limits_set}");
    }
    let stored = rig
        .harness
        .db
        .get_torrent(weaver_server_core::jobs::ids::JobId(id))
        .unwrap()
        .unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (Some(4096), None)
    );

    // Past the largest stored value, as `setBandwidth` refuses it.
    let bandwidth = rig
        .execute("mutation { setBandwidth(input: { downloadBps: 9223372036854775808 }) { downloadBps } }")
        .await;
    let oversize = rig
        .execute(&format!(
            "mutation {{ setTorrentRateLimits(id: {id}, downloadBps: 9223372036854775808) {{ id }} }}"
        ))
        .await;
    assert!(!bandwidth.errors.is_empty());
    assert!(!oversize.errors.is_empty());
    assert_eq!(code(&oversize), code(&bandwidth));
    assert_eq!(read().await, (Value::from(4096), Value::Null));

    let unknown = rig
        .execute("mutation { setTorrentRateLimits(id: 999999, uploadBps: 1) { id } }")
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
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let read = |id: u64| {
        format!(
            "{{ torrent(id: {id}) {{ sequential firstLastPieceFirst files {{ items {{ selected priority }} }} }} }}"
        )
    };
    let before = rig.execute(&read(id)).await.data.into_json().unwrap();
    assert_eq!(before["torrent"]["files"]["items"][0]["priority"], "NORMAL");
    assert_eq!(before["torrent"]["sequential"], false);
    assert_eq!(before["torrent"]["firstLastPieceFirst"], false);

    // Each answers with the torrent as it changed it.
    for (mutation, field, want) in [
        (
            format!(
                "mutation {{ setTorrentFilePriorities(id: {id}, priorities: [{{ index: 0, priority: IGNORED }}]) {{ files {{ items {{ priority }} }} }} }}"
            ),
            "setTorrentFilePriorities",
            serde_json::json!({ "files": { "items": [{ "priority": "IGNORED" }] } }),
        ),
        (
            format!(
                "mutation {{ setTorrentSequential(id: {id}, sequential: true) {{ sequential }} }}"
            ),
            "setTorrentSequential",
            serde_json::json!({ "sequential": true }),
        ),
        (
            format!(
                "mutation {{ setTorrentFirstLastPieceFirst(id: {id}, firstLastPieceFirst: true) {{ firstLastPieceFirst }} }}"
            ),
            "setTorrentFirstLastPieceFirst",
            serde_json::json!({ "firstLastPieceFirst": true }),
        ),
        (
            format!("mutation {{ setTorrentForced(id: {id}, forced: true) {{ forced }} }}"),
            "setTorrentForced",
            serde_json::json!({ "forced": true }),
        ),
        (
            format!(
                "mutation {{ setTorrentSeeding(id: {id}, seeding: {{ ratioGoal: {{ state: SET, value: 2.5 }} }}) {{ seedingPolicy {{ ratioGoal }} }} }}"
            ),
            "setTorrentSeeding",
            serde_json::json!({ "seedingPolicy": { "ratioGoal": 2.5 } }),
        ),
    ] {
        let response = rig.execute(&mutation).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(response.data.into_json().unwrap()[field], want, "{field}");
    }
    let after = rig.execute(&read(id)).await.data.into_json().unwrap();
    assert_eq!(after["torrent"]["files"]["items"][0]["priority"], "IGNORED");
    assert_eq!(after["torrent"]["files"]["items"][0]["selected"], false);
    assert_eq!(after["torrent"]["sequential"], true);
    assert_eq!(after["torrent"]["firstLastPieceFirst"], true);

    let out_of_range = rig
        .execute(&format!(
            "mutation {{ setTorrentFilePriorities(id: {id}, priorities: [{{ index: 1, priority: HIGH }}]) {{ id }} }}"
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
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
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
            "{{ torrent(id: {id}) {{ trackers {{ tier url source status updating message \
             seeders leechers completedDownloads announcedPeers fails nextAnnounceAt }} }} }}"
        ))
    };

    run(format!(
        r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["http://a.invalid/announce", "udp://b.invalid:80"]) {{ id }} }}"#
    ))
    .await;
    run(format!(
        r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["http://a.invalid/announce", "http://c.invalid/announce"]) {{ id }} }}"#
    ))
    .await;
    let trackers = listed().await;
    let rows: Vec<(u64, &str, &str, &str)> = trackers["torrent"]["trackers"]
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
    assert_eq!(trackers["torrent"]["trackers"][0]["updating"], false);
    assert_eq!(trackers["torrent"]["trackers"][0]["fails"], 0);

    run(format!(
        r#"mutation {{ replaceTorrentTrackers(id: {id}, trackers: [{{ tier: 3, url: "http://c.invalid/announce" }}, {{ tier: 1, url: "udp://b.invalid:80" }}]) {{ id }} }}"#
    ))
    .await;
    // The removal answers with the list as it left it.
    let removed = run(format!(
        r#"mutation {{ removeTorrentTrackers(id: {id}, urls: ["udp://b.invalid:80"]) {{ trackers {{ url }} }} }}"#
    ))
    .await;
    assert_eq!(
        removed["removeTorrentTrackers"]["trackers"],
        serde_json::json!([{ "url": "http://c.invalid/announce" }])
    );
    let trackers = listed().await;
    assert_eq!(trackers["torrent"]["trackers"].as_array().unwrap().len(), 1);
    assert_eq!(trackers["torrent"]["trackers"][0]["tier"], 3);
    assert_eq!(
        trackers["torrent"]["trackers"][0]["url"],
        "http://c.invalid/announce"
    );
    assert_eq!(trackers["torrent"]["trackers"][0]["source"], "OPERATOR");

    for (mutation, field) in [
        (
            format!("mutation {{ reannounceTorrent(id: {id}) {{ id }} }}"),
            "reannounceTorrent",
        ),
        (
            format!(
                r#"mutation {{ reannounceTorrent(id: {id}, tracker: "http://c.invalid/announce") {{ id }} }}"#
            ),
            "reannounceTorrent",
        ),
        (
            format!("mutation {{ dhtAnnounceTorrent(id: {id}) {{ id }} }}"),
            "dhtAnnounceTorrent",
        ),
    ] {
        assert_eq!(run(mutation).await[field]["id"], id, "{field}");
    }

    let unknown = rig
        .execute(&format!(
            r#"mutation {{ reannounceTorrent(id: {id}, tracker: "http://d.invalid/announce") {{ id }} }}"#
        ))
        .await;
    assert_eq!(code(&unknown).as_deref(), Some("INVALID_INPUT"));
    let refused = rig
        .execute(&format!(
            r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["not a tracker"]) {{ id }} }}"#
        ))
        .await;
    assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"));
    for query in [
        r#"mutation { addTorrentTrackers(id: 999999, urls: ["http://a.invalid/"]) { id } }"#,
        r#"mutation { replaceTorrentTrackers(id: 999999, trackers: []) { id } }"#,
        r#"mutation { removeTorrentTrackers(id: 999999, urls: []) { id } }"#,
        "mutation { reannounceTorrent(id: 999999) { id } }",
        "mutation { dhtAnnounceTorrent(id: 999999) { id } }",
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
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let added = rig
        .execute(&format!(
            r#"mutation {{ addTorrentTrackers(id: {id}, urls: ["{PRIVATE}"]) {{ id }} }}"#
        ))
        .await;
    assert!(added.errors.is_empty(), "{:?}", added.errors);
    let urls = |scope: CallerScope| {
        let rig = &rig;
        async move {
            let response = rig
                .execute_as_key(
                    &format!(
                        "{{ torrents {{ items {{ trackers {{ url }} }} }} torrent(id: {id}) {{ trackers {{ url }} }} }}"
                    ),
                    scope,
                )
                .await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            let data = response.data.into_json().unwrap();
            [
                data["torrents"]["items"][0]["trackers"][0]["url"]
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
            upload_cap: None,
            complete_dir: rig._dir.path().join("complete"),
            categories: Default::default(),
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
                name: "tracker.test", hostPatterns: ["tracker.test"], torrentRoute: true
            }) { id } }"#,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["saveTrackerRule"][0]["id"]
        .as_u64()
        .unwrap();

    let response = rig
        .execute(
            "{ torrentRoutes { key scopes { scope name category { id name } trackerRule { id name } } \
             open listenAddress utp \
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
                { "scope": "DEFAULT", "name": null, "category": null, "trackerRule": null },
                {
                    "scope": "TRACKER_RULE",
                    "name": "tracker.test",
                    "category": null,
                    "trackerRule": { "id": id, "name": "tracker.test" },
                },
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
    use weaver_server_api::torrents::types::{TorrentPortMappingStateGql, TorrentRouteGql};
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
                PortMapped {
                    transport: PortmapTransport::PcpIpv6,
                    protocol: PortmapProtocol::Tcp,
                    local_port: 6881,
                    external_port: None,
                    external_ip: None,
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
    assert_eq!(route.scopes[0].of, TorrentScope::Category(3));
    assert_eq!(route.scopes[0].name.as_deref(), Some("tv"));
    assert_eq!(route.mappings.len(), 3);
    assert_eq!(
        route
            .mappings
            .iter()
            .map(|mapping| mapping.state)
            .collect::<Vec<_>>(),
        [
            TorrentPortMappingStateGql::Failed,
            TorrentPortMappingStateGql::Mapped,
            TorrentPortMappingStateGql::Removed,
        ]
    );
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
    assert_eq!(shown.announced_port, Some(40_001));
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
    fn plan(&self, scope: TorrentScope) -> RoutePlan {
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
        Consumer, DnsProfile, DnsProfileKind, DnsTrust, DnsWho, ProxyKind, ProxyProfile,
        ProxyRuntime, ProxySecrets, RouteLeg, Rung,
    };
    use weaver_server_core::torrents::sessions::NetworkRoutes;

    // The runtime the routes plan from, reloaded after a policy is saved
    // straight to its row.
    let runtime = std::sync::OnceLock::new();
    let rig = rig_on(|db| {
        db.save_proxy_profile(&ProxyProfile {
            id: 1,
            name: "tunnel".into(),
            kind: ProxyKind::Socks5,
            enabled: true,
            host: "127.0.0.1".into(),
            port: 1,
            dns_servers: vec![],
            tunnel_addresses: vec![],
            peer_public_key: None,
            mtu: 1280,
            keepalive_seconds: None,
            timeout_seconds: 30,
            host_key_fingerprint: None,
            natpmp: false,
            forwarded_port: None,
            revision: 1,
            secrets: ProxySecrets::default(),
            dns_profile_id: 0,
        })
        .unwrap();
        let proxies = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
        runtime.set(Arc::clone(&proxies)).ok().unwrap();
        Arc::new(Planned(NetworkRoutes::new(
            proxies,
            std::time::Duration::from_secs(30),
        )))
    })
    .await;
    let response = rig
        .execute(
            r#"mutation { saveTrackerRule(input: {
                name: "tracker.test", hostPatterns: ["tracker.test"], torrentRoute: true
            }) { id } }"#,
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let rule = response.data.into_json().unwrap()["saveTrackerRule"][0]["id"]
        .as_u64()
        .unwrap();
    // The ladder's route has its exit proxy answer names.
    let by_proxy = rig
        .harness
        .db
        .save_dns_profile(
            DnsProfile {
                id: 0,
                name: "by proxy".into(),
                kind: DnsProfileKind::Route,
                who: DnsWho::Proxy,
                transit: None,
                trust: DnsTrust::AsSent,
                server_list_id: None,
                system_fallback: false,
            },
            true,
        )
        .unwrap();
    rig.harness
        .db
        .save_proxy_routing_policy(
            Consumer::Torrent(TorrentScope::TrackerRule(rule as u32)),
            &RoutingPolicy {
                dns_profile_id: Some(by_proxy.id),
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
    runtime.get().unwrap().reload().await.unwrap();

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
        let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
            .as_u64()
            .unwrap();
        let response = rig
            .execute(&format!(
                "{{ torrent(id: {id}) {{ trackers {{ url resolvedBy }} }} }}"
            ))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data.into_json().unwrap()["torrent"]["trackers"],
            serde_json::json!([{ "url": announce, "resolvedBy": by }])
        );
    }
}

// A torrent its route holds says why as the route's reason code, with the
// cause behind it as its detail, in the list and in its own view.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_torrent_carries_its_routes_reason_code() {
    let offline = rig().await;
    let response = offline
        .execute(&format!(
            r#"mutation {{ submitTorrent(input: {{ source: {{ metainfoBase64: "{}" }} }}) {{
                torrent {{ id state stateDetail routeReason }}
            }} }}"#,
            metainfo("offline.mkv", 3000, false)
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let submitted = response.data.into_json().unwrap()["submitTorrent"]["torrent"].clone();
    let id = submitted["id"].as_u64().unwrap();
    let held = serde_json::json!({
        "id": id, "state": "STALLED", "stateDetail": "offline", "routeReason": "LEG_DOWN"
    });
    assert_eq!(submitted, held);
    let response = offline
        .execute(&format!(
            "{{ torrents {{ items {{ id state stateDetail routeReason }} }} \
               torrent(id: {id}) {{ id state stateDetail routeReason }} }}"
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        serde_json::json!({ "torrents": { "items": [held.clone()] }, "torrent": held })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_torrent_has_a_support_report_without_its_name() {
    let rig = rig().await;
    let response = rig
        .execute(&submit(&metainfo("zqxsentinel.mkv", 3000, true), ""))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let id = response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
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

// A tracker file, base64: required DHT off, recommended PEX off and a ratio,
// an identity for user-1 and one archive password, at `revision`.
fn tracker_file(revision: &str) -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "user-1");
    params.not_after = rcgen::date_time_ymd(2031, 1, 1);
    let text = format!(
        "version = 1
name = \"Example Tracker\"
revision = \"{revision}\"
host_patterns = [\"*.tracker.test\"]
[required]
dht = false
[recommended]
pex = false
seeding = {{ ratio_goal = 1.5 }}
[archive_passwords]
values = [\"archive-secret-word\"]
[ssl]
certificate_pem = '''
{}'''
private_key_pem = '''
{}'''
",
        params.self_signed(&key).unwrap().pem(),
        key.serialize_pem()
    );
    (
        base64::engine::general_purpose::STANDARD.encode(&text),
        key.serialize_pem(),
    )
}

// Importing a tracker file is previewed first, secret-free, and applied by
// the preview's fingerprint; a file or rule changed since is a stale
// preview. The rule it fills shows the file's name and the fields it locked,
// and unbinding keeps every value. Only an admin may do any of it, and no
// response carries the identity's key or a password.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_file_is_previewed_then_applied_by_its_fingerprint() {
    let rig = rig().await;
    let (file, private_key) = tracker_file("1");
    let secrets = [
        private_key.trim().lines().nth(1).unwrap().to_owned(),
        "archive-secret-word".to_owned(),
        "upload-secret-word".to_owned(),
    ];
    let mut responses = Vec::new();
    let preview_query = |file: &str| {
        format!(
            r#"mutation {{ previewTrackerFile(file: "{file}", password: "upload-secret-word") {{
                name revision hostPatterns ruleId ruleName hasSsl sslSubject sslNotAfter
                archivePasswordCount fingerprint changes {{ field locked old new }}
            }} }}"#
        )
    };
    let apply_query = |file: &str, fingerprint: &str| {
        format!(
            r#"mutation {{ applyTrackerFile(file: "{file}", password: "upload-secret-word",
                fingerprint: "{fingerprint}") {{
                id name hostPatterns dht pex seeding {{ ratioGoal {{ state value }} }} hasSsl sslSubject
                bound {{ name revision locked }} archivePasswordCount
            }} }}"#
        )
    };

    let response = rig.execute(&preview_query(&file)).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let preview = response.data.into_json().unwrap()["previewTrackerFile"].clone();
    responses.push(preview.to_string());
    let fingerprint = preview["fingerprint"].as_str().unwrap().to_owned();
    assert_eq!(
        preview,
        serde_json::json!({
            "name": "Example Tracker", "revision": "1", "hostPatterns": ["*.tracker.test"],
            "ruleId": null, "ruleName": null, "hasSsl": true, "sslSubject": "CN=user-1",
            "sslNotAfter": 1_924_992_000_000_i64, "archivePasswordCount": 1,
            "fingerprint": fingerprint,
            "changes": [
                { "field": "dht", "locked": true, "old": null, "new": "false" },
                { "field": "pex", "locked": false, "old": null, "new": "false" },
                { "field": "seeding.ratio_goal", "locked": false, "old": null, "new": "1.5" },
            ],
        })
    );

    // A preview keeps nothing.
    let response = rig.execute("{ trackerRules { id } }").await;
    assert_eq!(
        response.data.into_json().unwrap()["trackerRules"],
        serde_json::json!([])
    );

    // A file other than the one previewed is a stale preview.
    let (other, _) = tracker_file("2");
    let stale = rig.execute(&apply_query(&other, &fingerprint)).await;
    assert_eq!(stale.errors.len(), 1);
    assert_eq!(stale.errors[0].message, "stale preview");
    responses.push(format!("{:?}", stale.errors));

    let response = rig.execute(&apply_query(&file, &fingerprint)).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let rules = response.data.into_json().unwrap()["applyTrackerFile"].clone();
    responses.push(rules.to_string());
    let id = rules[0]["id"].as_u64().unwrap();
    assert_eq!(
        rules,
        serde_json::json!([{
            "id": id, "name": "Example Tracker", "hostPatterns": ["*.tracker.test"], "dht": false, "pex": false,
            "seeding": { "ratioGoal": { "state": "SET", "value": 1.5 } }, "hasSsl": true, "sslSubject": "CN=user-1",
            "bound": { "name": "Example Tracker", "revision": "1", "locked": ["dht", "ssl"] },
            "archivePasswordCount": 1,
        }])
    );

    // The rule changed since the preview: applying it again is stale, and a
    // new preview names the rule.
    let again = rig.execute(&apply_query(&file, &fingerprint)).await;
    assert_eq!(again.errors[0].message, "stale preview");
    let response = rig.execute(&preview_query(&file)).await;
    let preview = response.data.into_json().unwrap()["previewTrackerFile"].clone();
    responses.push(preview.to_string());
    assert_eq!(preview["ruleId"], id);
    assert_eq!(preview["changes"][0]["old"], "false");

    // The ordinary save refuses a locked field.
    let refused = rig
        .execute(&format!(
            r#"mutation {{ saveTrackerRule(id: {id}, input: {{
                name: "*.tracker.test", hostPatterns: ["*.tracker.test"], dht: true, pex: false
            }}) {{ id }} }}"#
        ))
        .await;
    assert_eq!(
        refused.errors[0].message,
        "dht is locked by Example Tracker's tracker file; unbind the rule to change it."
    );

    // Only an admin previews, applies or unbinds.
    for query in [
        preview_query(&file),
        apply_query(&file, &fingerprint),
        format!("mutation {{ unbindTrackerRule(id: {id}) {{ id }} }}"),
    ] {
        let response = rig.execute_as_key(&query, CallerScope::Read).await;
        assert!(
            response
                .errors
                .iter()
                .any(|error| error.message.contains("admin scope")),
            "{query}: {:?}",
            response.errors
        );
    }

    let response = rig
        .execute(&format!(
            "mutation {{ unbindTrackerRule(id: {id}) {{ id dht pex hasSsl bound {{ name }} archivePasswordCount }} }}"
        ))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let rules = response.data.into_json().unwrap()["unbindTrackerRule"].clone();
    responses.push(rules.to_string());
    assert_eq!(
        rules,
        serde_json::json!([{
            "id": id, "dht": false, "pex": false, "hasSsl": true, "bound": null,
            "archivePasswordCount": 1,
        }])
    );

    // The editor's save keeps the rule's archive passwords, unless it clears
    // them.
    let save = |clear: bool| {
        format!(
            r#"mutation {{ saveTrackerRule(id: {id}, input: {{
                name: "*.tracker.test", hostPatterns: ["*.tracker.test"], dht: false, pex: false,
                clearArchivePasswords: {clear}
            }}) {{ archivePasswordCount }} }}"#
        )
    };
    for (clear, count) in [(false, 1), (true, 0)] {
        let response = rig.execute(&save(clear)).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let rules = response.data.into_json().unwrap()["saveTrackerRule"].clone();
        responses.push(rules.to_string());
        assert_eq!(
            rules,
            serde_json::json!([{ "archivePasswordCount": count }])
        );
    }
    let missing = rig
        .execute(&format!(
            "mutation {{ unbindTrackerRule(id: {}) {{ id }} }}",
            id + 1
        ))
        .await;
    assert_eq!(
        missing.errors[0].message,
        format!("no tracker rule {}", id + 1)
    );

    for response in &responses {
        for secret in &secrets {
            assert!(!response.contains(secret.as_str()), "{response}");
        }
    }
}

// A tracker file that cannot be read is refused in one sentence that names
// no password, and a file past the largest is refused before it is decoded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_file_that_cannot_be_read_is_refused_plainly() {
    let rig = rig().await;
    let preview = |file: &str| {
        format!(
            r#"mutation {{ previewTrackerFile(file: "{file}", password: "upload-secret-word") {{ name }} }}"#
        )
    };
    let unknown = base64::engine::general_purpose::STANDARD.encode(
        "version = 1\nname = \"T\"\nrevision = \"1\"\nhost_patterns = [\"t.test\"]\nclient = \"x\"\n",
    );
    let huge = "A".repeat(2 << 20);
    for (file, refusal) in [
        (unknown.as_str(), "client"),
        ("not base64!", "The tracker file is not base64."),
        (huge.as_str(), "The tracker file is larger than 1024 KiB."),
    ] {
        let response = rig.execute(&preview(file)).await;
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
        let message = &response.errors[0].message;
        assert!(message.contains(refusal), "{message}");
        assert!(!message.contains("upload-secret-word"), "{message}");
    }
}

// `torrents` pages as the whole list does: 100 when `first` is not given,
// at most 500, each page the torrents after its cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_torrents_list_pages_with_a_default_and_a_most() {
    let rig = rig().await;
    for index in 0..505 {
        let response = rig
            .execute(&submit(&metainfo(&format!("{index}.bin"), 2000, false), ""))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }
    let ids = |response: Response| -> Vec<u64> {
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()["torrents"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|torrent| torrent["id"].as_u64().unwrap())
            .collect()
    };
    let every: Vec<u64> = rig.torrents.job_ids().await.iter().map(|id| id.0).collect();
    assert_eq!(every.len(), 505);
    assert_eq!(
        ids(rig.execute("{ torrents { items { id } } }").await),
        every[..100]
    );
    assert_eq!(
        ids(rig
            .execute("{ torrents(first: 1000) { items { id tags } } }")
            .await),
        every[..500]
    );
    let cursor = weaver_server_api::jobs::types::encode_offset_cursor(480);
    assert_eq!(
        ids(rig
            .execute(&format!(
                r#"{{ torrents(first: 50, after: "{cursor}") {{ items {{ id name trackers {{ url }} }} }} }}"#
            ))
            .await),
        every[480..]
    );
}

// `torrents` walks the list to its end by `nextCursor`, with no gap and no
// repeat, and ends on a null cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_torrent_page_walks_the_list_by_its_next_cursor() {
    let rig = rig().await;
    rig.torrents
        .set_settings(TorrentSettings {
            upload_cap: None,
            complete_dir: rig._dir.path().join("complete"),
            categories: vec![CategoryConfig {
                upload_cap: Default::default(),
                id: 1,
                name: "tv".into(),
                dest_dir: None,
                aliases: String::new(),
                seeding: None,
                torrent_route: false,
                unpack: None,
            }]
            .into(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        })
        .await;
    for index in 0..5 {
        let category = if index % 2 == 0 {
            "category: \"tv\""
        } else {
            ""
        };
        let response = rig
            .execute(&submit(
                &metainfo(&format!("{index}.bin"), 2000, false),
                category,
            ))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }
    let every: Vec<u64> = rig.torrents.job_ids().await.iter().map(|id| id.0).collect();
    assert_eq!(every.len(), 5);
    let data = |response: Response| -> Value {
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    };

    for (category, want) in [("", 5), ("category: \"tv\",", 3)] {
        let mut after = String::new();
        let mut walked = Vec::new();
        loop {
            let arguments = format!("{category} first: 2 {after}");
            let page = data(
                rig.execute(&format!(
                    "{{ torrents({arguments}) {{ items {{ id name savePath }} nextCursor }} }}"
                ))
                .await,
            )["torrents"]
                .clone();
            let items = page["items"].as_array().unwrap();
            assert!(!items.is_empty(), "{arguments}");
            walked.extend(items.iter().map(|item| item["id"].as_u64().unwrap()));
            match page["nextCursor"].as_str() {
                Some(cursor) => after = format!("after: \"{cursor}\""),
                None => break,
            }
        }
        assert_eq!(walked.len(), want, "{category}");
        let whole = data(
            rig.execute(&format!(
                "{{ torrents({category} first: 500) {{ items {{ id }} }} }}"
            ))
            .await,
        )["torrents"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["id"].as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(walked, whole, "{category}");
        if category.is_empty() {
            assert_eq!(walked, every);
        }
    }

    // A page that ends exactly at the list's end has no next.
    let exact = data(
        rig.execute("{ torrents(first: 5) { items { id } nextCursor } }")
            .await,
    );
    assert_eq!(exact["torrents"]["items"].as_array().unwrap().len(), 5);
    assert!(exact["torrents"]["nextCursor"].is_null());
    // A page with no items has no next, even with torrents still to come.
    let empty = data(
        rig.execute("{ torrents(first: 0) { items { id } nextCursor } }")
            .await,
    );
    assert!(empty["torrents"]["items"].as_array().unwrap().is_empty());
    assert!(empty["torrents"]["nextCursor"].is_null());
    let invalid = rig
        .execute("{ torrents(after: \"not-a-cursor\") { nextCursor } }")
        .await;
    assert_eq!(code(&invalid).as_deref(), Some("CURSOR_INVALID"));
}

// A follow of a few torrents is given those of them there are, in the
// list's order, and names at most a page of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrent_updates_follow_the_torrents_named() {
    use tokio_stream::StreamExt;

    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    for index in 0..6 {
        let response = rig
            .execute(&submit(&metainfo(&format!("{index}.bin"), 2000, false), ""))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }
    let every: Vec<u64> = rig.torrents.job_ids().await.iter().map(|id| id.0).collect();
    let subscribe = |arguments: String| {
        let request = Request::new(format!(
            "subscription {{ torrentRowUpdates({arguments}) {{ fullUpdate torrents {{ id }} }} }}"
        ))
        .data(CallerScope::Local)
        .data(CallerIdentity::Local([7; 32]))
        .data(rig.torrents.clone());
        rig.harness.schema.execute_stream(request)
    };
    let named = [every[4], every[1], 999_999, every[4]];
    let mut updates = subscribe(format!("ids: {named:?}"));
    let first = updates.next().await.expect("the updates stay open");
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let first = first.data.into_json().unwrap()["torrentRowUpdates"].clone();
    assert_eq!(first["fullUpdate"], true);
    assert_eq!(
        first["torrents"],
        serde_json::json!([{ "id": every[1] }, { "id": every[4] }])
    );

    let refused = subscribe(format!("ids: {:?}", (1..=501).collect::<Vec<u64>>()))
        .next()
        .await
        .expect("an answer");
    assert_eq!(code(&refused).as_deref(), Some("INVALID_INPUT"));
}

// While the stored torrents are still being read at start, a page of the
// list is read and answered, and the updates of a page's torrents are
// refused under their own code, never begun from some of them; the updates
// of one torrent are given. Once the last is in, the page's are. Every
// torrent is never followed: a follow names one torrent or a page of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_updates_of_a_page_wait_for_the_load() {
    use tokio_stream::StreamExt;

    let before = rig().await;
    let mut ids = Vec::new();
    for name in ["one.mkv", "two.mkv", "three.mkv"] {
        let response = before
            .execute(&submit(&metainfo(name, 3000, false), ""))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        ids.push(
            response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
                .as_u64()
                .unwrap(),
        );
    }
    before.torrents.stop().await;
    let Rig { harness, _dir, .. } = before;
    let sessions = Sessions::new(
        Arc::new(Offline),
        SessionEnv {
            root: _dir.path().join("sessions"),
        },
        harness.db.clone(),
    );
    let torrents = TorrentService::new(
        harness.db.clone(),
        sessions,
        TorrentSettings {
            upload_cap: None,
            complete_dir: _dir.path().join("complete"),
            categories: Default::default(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        },
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.hold_load(true);
    torrents.start_loading().await.unwrap();
    let rig = Rig {
        harness,
        torrents,
        _dir,
    };

    let paged = rig.execute("{ torrents(first: 2) { items { id } } }").await;
    assert!(paged.errors.is_empty(), "{:?}", paged.errors);
    assert_eq!(
        paged.data.into_json().unwrap()["torrents"]["items"],
        serde_json::json!([{ "id": ids[0] }, { "id": ids[1] }])
    );
    let updates = |of: &str| {
        let request = Request::new(format!("subscription {{ {of} }}"))
            .data(CallerScope::Local)
            .data(CallerIdentity::Local([7; 32]))
            .data(rig.torrents.clone());
        rig.harness.schema.execute_stream(request)
    };
    let page = format!("torrentRowUpdates(ids: {ids:?}) {{ fullUpdate torrents {{ id }} }}");
    let refused = updates(&page).next().await.expect("an answer");
    assert_eq!(code(&refused).as_deref(), Some("TORRENTS_LOADING"));
    let every = updates("torrentRowUpdates { fullUpdate }")
        .next()
        .await
        .expect("an answer");
    assert!(!every.errors.is_empty());
    assert_ne!(code(&every).as_deref(), Some("TORRENTS_LOADING"));
    let one = updates(&format!("torrentUpdates(id: {}) {{ id }}", ids[2]))
        .next()
        .await
        .expect("an answer");
    assert!(one.errors.is_empty(), "{:?}", one.errors);
    assert_eq!(
        one.data.into_json().unwrap()["torrentUpdates"],
        serde_json::json!({ "id": ids[2] })
    );

    rig.torrents.hold_load(false);
    rig.torrents.loaded().await;
    let every = updates(&page).next().await.expect("an answer");
    assert!(every.errors.is_empty(), "{:?}", every.errors);
    assert_eq!(
        every.data.into_json().unwrap()["torrentRowUpdates"]["torrents"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}

// Operator ruling, 2026-11-16 (PR 129 question 2): a control key moves a
// torrent only to a folder at or under the default save folder or a
// category's folder; elsewhere it is forbidden and nothing moves. An admin
// key moves it anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_control_key_moves_a_torrent_only_under_the_save_folders() {
    let rig = rig().await;
    let complete = rig._dir.path().join("complete");
    let tv = rig._dir.path().join("media").join("tv");
    rig.torrents
        .set_settings(TorrentSettings {
            upload_cap: None,
            complete_dir: complete.clone(),
            categories: vec![CategoryConfig {
                upload_cap: Default::default(),
                id: 1,
                name: "tv".into(),
                dest_dir: Some(tv.to_string_lossy().into_owned()),
                aliases: String::new(),
                seeding: None,
                torrent_route: false,
                unpack: None,
            }]
            .into(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            },
        })
        .await;
    let added = rig
        .execute(&submit(&metainfo("one.mkv", 3000, false), ""))
        .await;
    assert!(added.errors.is_empty(), "{:?}", added.errors);
    let id = added.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
        .as_u64()
        .unwrap();
    let rig = &rig;
    let dir = || async move {
        let view = rig
            .torrents
            .torrent(weaver_server_core::jobs::ids::JobId(id))
            .await
            .unwrap();
        std::path::PathBuf::from(view.storage_dir.unwrap())
    };
    let move_to = |path: std::path::PathBuf, scope: CallerScope| async move {
        let path = path.to_string_lossy().replace('\\', "\\\\");
        rig.execute_as_key(
            &format!(r#"mutation {{ setTorrentLocation(id: {id}, path: "{path}") {{ id }} }}"#),
            scope,
        )
        .await
    };

    let under_default = complete.join("moved");
    let moved = move_to(under_default.clone(), CallerScope::Control).await;
    assert!(moved.errors.is_empty(), "{:?}", moved.errors);
    assert_eq!(dir().await.parent(), Some(under_default.as_path()));
    let under_category = tv.join("Show");
    let moved = move_to(under_category.clone(), CallerScope::Control).await;
    assert!(moved.errors.is_empty(), "{:?}", moved.errors);
    let there = dir().await;
    assert_eq!(there.parent(), Some(under_category.as_path()));

    let elsewhere = rig._dir.path().join("elsewhere");
    for path in [elsewhere.clone(), tv.join("..").join("elsewhere")] {
        let refused = move_to(path, CallerScope::Control).await;
        assert_eq!(code(&refused).as_deref(), Some("FORBIDDEN"));
    }
    assert_eq!(dir().await, there);
    assert!(!elsewhere.exists());

    let moved = move_to(elsewhere.clone(), CallerScope::Admin).await;
    assert!(moved.errors.is_empty(), "{:?}", moved.errors);
    assert_eq!(dir().await.parent(), Some(elsewhere.as_path()));
}

// Each ids-only batch answers as the queue's batches do: what it matched,
// what it applied, and a refusal per id it could not act on. A list past
// the queue batch limit is refused whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_torrent_batches_act_on_each_id_and_refuse_the_unknown() {
    let rig = rig().await;
    let mut ids = Vec::new();
    for name in ["one.mkv", "two.mkv"] {
        let response = rig.execute(&submit(&metainfo(name, 3000, false), "")).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        ids.push(
            response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
                .as_u64()
                .unwrap(),
        );
    }
    let [one, two] = [ids[0], ids[1]];
    let batch =
        |call: &str| format!("mutation {{ {call} {{ matched applied refused {{ id code }} }} }}");

    for (mutation, field) in [
        (
            format!("setTorrentsForced(ids: [{one}, {two}], forced: true)"),
            "setTorrentsForced",
        ),
        (
            format!(
                "setTorrentsRateLimits(ids: [{one}, {two}], uploadBps: 1000, downloadBps: 2000)"
            ),
            "setTorrentsRateLimits",
        ),
        (
            format!(
                "setTorrentsSeeding(ids: [{one}, {two}], seeding: {{ ratioGoal: {{ state: SET, value: 2.5 }} }})"
            ),
            "setTorrentsSeeding",
        ),
    ] {
        let response = rig.execute(&batch(&mutation)).await;
        assert!(response.errors.is_empty(), "{field}: {:?}", response.errors);
        let got = response.data.into_json().unwrap()[field].clone();
        assert_eq!(
            got,
            serde_json::json!({ "matched": 2, "applied": 2, "refused": [] }),
            "{field}"
        );
    }
    for id in [one, two] {
        let read = rig
            .execute(&format!(
                "{{ torrent(id: {id}) {{ forced uploadLimitBps downloadLimitBps seedingPolicy {{ ratioGoal }} }} }}"
            ))
            .await;
        assert!(read.errors.is_empty(), "{:?}", read.errors);
        assert_eq!(
            read.data.into_json().unwrap()["torrent"],
            serde_json::json!({
                "forced": true,
                "uploadLimitBps": 1000,
                "downloadLimitBps": 2000,
                "seedingPolicy": { "ratioGoal": 2.5 },
            })
        );
    }

    // An id no torrent holds is refused by name, and the batch goes on.
    let response = rig
        .execute(&batch(&format!("recheckTorrents(ids: [999999, {one}])")))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let got = &response.data.into_json().unwrap()["recheckTorrents"];
    assert_eq!(got["refused"][0]["id"], 999999);
    assert_eq!(got["refused"][0]["code"], "NOT_FOUND");

    // Past the limit nothing is attempted.
    let many = (1..=1001)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let response = rig
        .execute(&batch(&format!("removeTorrents(ids: [{many}])")))
        .await;
    assert_eq!(code(&response).as_deref(), Some("BATCH_TOO_LARGE"));

    let response = rig
        .execute(&batch(&format!("removeTorrents(ids: [{one}, {two}])")))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap()["removeTorrents"],
        serde_json::json!({ "matched": 2, "applied": 2, "refused": [] })
    );
    let left = rig.execute("{ torrents { items { id } } }").await;
    assert_eq!(
        left.data.into_json().unwrap()["torrents"]["items"],
        serde_json::json!([])
    );
}

// A queue item's `torrent` is its published torrent row, read once for the
// whole list and only when asked: a list that does not ask reads no row. A
// torrent's client request id and attributes reach its queue item.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_items_torrent_is_read_once_per_list_and_only_when_asked() {
    let rig = rig().await;
    rig.torrents.publish_to(rig.harness.handle.clone());
    let mut ids = Vec::new();
    for name in ["one.mkv", "two.mkv"] {
        let response = rig
            .execute(&submit(
                &metainfo(name, 3000, false),
                &format!(
                    r#"clientRequestId: "req-{name}" attributes: [{{ key: "tvdbId", value: "7" }}]"#
                ),
            ))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        ids.push(
            response.data.into_json().unwrap()["submitTorrent"]["torrent"]["id"]
                .as_u64()
                .unwrap(),
        );
    }
    let reads = || rig.torrents.snapshot().rows_of_reads();

    let before = reads();
    let plain = rig
        .execute("{ queueItems { id clientRequestId attributes { key value } } }")
        .await;
    assert!(plain.errors.is_empty(), "{:?}", plain.errors);
    assert_eq!(reads(), before);
    let plain = plain.data.into_json().unwrap();
    for (id, name) in ids.iter().zip(["one.mkv", "two.mkv"]) {
        let item = plain["queueItems"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"].as_u64() == Some(*id))
            .cloned()
            .unwrap();
        assert_eq!(item["clientRequestId"], format!("req-{name}"));
        assert_eq!(
            item["attributes"],
            serde_json::json!([{ "key": "tvdbId", "value": "7" }])
        );
    }

    let before = reads();
    let linked = rig
        .execute("{ queueItems { id torrent { id name state } } }")
        .await;
    assert!(linked.errors.is_empty(), "{:?}", linked.errors);
    assert_eq!(reads(), before + 1);
    let linked = linked.data.into_json().unwrap();
    for (id, name) in ids.iter().zip(["one.mkv", "two.mkv"]) {
        let item = linked["queueItems"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"].as_u64() == Some(*id))
            .cloned()
            .unwrap();
        assert_eq!(item["torrent"]["id"], *id);
        assert_eq!(item["torrent"]["name"], name);
    }

    // One item read alone reads its own row.
    let before = reads();
    let one = rig
        .execute(&format!(
            "{{ queueItem(id: {}) {{ torrent {{ name }} }} }}",
            ids[0]
        ))
        .await;
    assert!(one.errors.is_empty(), "{:?}", one.errors);
    assert_eq!(reads(), before + 1);
    assert_eq!(
        one.data.into_json().unwrap()["queueItem"]["torrent"]["name"],
        "one.mkv"
    );

    for selection in [
        "files { items { path } }",
        "peerConnections { address }",
        "unpacking { files { items { path } } }",
    ] {
        let refused = rig
            .execute(&format!("{{ queueItems {{ torrent {{ {selection} }} }} }}"))
            .await;
        assert_eq!(
            code(&refused).as_deref(),
            Some("INVALID_INPUT"),
            "{selection}"
        );
    }
}
