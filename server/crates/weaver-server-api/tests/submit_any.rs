// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `submit` reads what it is given for what it is: text is an NZB or torrent
// URL, a magnet link or an infohash, and an upload is an NZB, torrent or
// magnet file whatever its name.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_graphql::{Request, Response, UploadValue, Variables};
use common::{TestHarness, assert_no_errors, response_data};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::proxies::{
    EgressBinding, LegPath, RoutingPolicy, SYSTEM_EGRESS_ID, TorrentScope,
};
use weaver_server_core::settings::TorrentConfig;
use weaver_server_core::torrents::identity::IDENTITY;
use weaver_server_core::torrents::net::LegKind;
use weaver_server_core::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use weaver_server_core::torrents::sessions::{
    OpenLeg, RouteKey, RoutePlan, Routes, SessionEnv, Sessions,
};

// A route that is always down: no torrent here reaches a network.
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
    let torrents = TorrentService::new(
        db,
        sessions,
        TorrentSettings {
            complete_dir: dir.path().join("complete"),
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

const SUBMIT: &str = r#"
    mutation Submit($input: SubmitInput!) {
        submit(input: $input) { accepted status jobId item { id state } }
    }
"#;

const STAGE: &str = r#"
    mutation Stage($input: StageNzbUploadInput!) {
        stageNzbUpload(input: $input) { kind accepted stagedUploadId error }
    }
"#;

impl Rig {
    async fn run(&self, query: &str, variables: Value, upload: Option<(&str, &[u8])>) -> Response {
        let mut request = Request::new(query)
            .data(CallerScope::Local)
            .data(CallerIdentity::Local([7; 32]))
            .data(self.torrents.clone())
            .variables(Variables::from_json(variables));
        if let Some((name, bytes)) = upload {
            let path = if query.contains("stageNzbUpload") {
                "variables.input.nzbUpload"
            } else {
                "variables.input.upload"
            };
            request.set_upload(
                path,
                UploadValue {
                    filename: name.to_owned(),
                    content_type: None,
                    content: bytes.to_vec().into(),
                },
            );
        }
        self.harness.schema.execute(request).await
    }

    async fn submit_text(&self, text: &str) -> Response {
        self.run(SUBMIT, json!({ "input": { "text": text } }), None)
            .await
    }

    async fn submit_file(&self, name: &str, bytes: &[u8], paused: bool) -> Response {
        self.run(
            SUBMIT,
            json!({ "input": { "upload": null, "paused": paused } }),
            Some((name, bytes)),
        )
        .await
    }
}

fn code(response: &Response) -> Option<String> {
    let error = response.errors.first()?;
    let extensions = serde_json::to_value(&error.extensions).ok()?;
    extensions
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo.
fn metainfo(name: &str) -> Vec<u8> {
    // No content is ever fetched, so the piece hashes only need the right length.
    let mut info = b"d6:lengthi3000e".to_vec();
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    info.extend_from_slice(b"12:piece lengthi1024e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &[0u8; 60]);
    info.push(b'e');
    let mut out = b"d4:info".to_vec();
    out.extend_from_slice(&info);
    out.push(b'e');
    out
}

fn nzb(name: &str) -> Vec<u8> {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
    )
    .into_bytes()
}

// A loopback server that answers one request with `body`.
async fn serve(body: Vec<u8>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
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
    });
    format!("http://{addr}/get?id=1")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_or_an_infohash_goes_to_the_torrent_client() {
    let rig = rig().await;
    let hash = "0123456789abcdef0123456789abcdef01234567";
    // The offline route refuses it in the client's own words.
    for text in [format!("magnet:?xt=urn:btih:{hash}"), hash.to_owned()] {
        let response = rig.submit_text(&text).await;
        assert_eq!(code(&response).as_deref(), Some("NO_NETWORK"), "{text}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upload_is_read_for_what_it_holds_not_its_name() {
    let rig = rig().await;

    let torrent = rig
        .submit_file("named.nzb", &metainfo("one.mkv"), true)
        .await;
    assert_no_errors(&torrent);
    let data = response_data(&torrent)["submit"].clone();
    assert_eq!(data["accepted"], true);
    let id = data["jobId"].as_u64().unwrap();
    let view = rig.torrents.torrent(JobId(id)).await.unwrap();
    assert_eq!(view.name, "one.mkv");

    let job = rig.submit_file("named.torrent", &nzb("an-nzb"), true).await;
    assert_no_errors(&job);
    let data = response_data(&job)["submit"].clone();
    assert_eq!(data["accepted"], true);
    assert_eq!(data["item"]["state"], "PAUSED");
    assert!(
        rig.torrents
            .torrent(JobId(data["jobId"].as_u64().unwrap()))
            .await
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_url_is_fetched_and_read_for_what_it_holds() {
    let rig = rig().await;

    let url = serve(metainfo("fetched.mkv")).await;
    let torrent = rig.submit_text(&url).await;
    assert_no_errors(&torrent);
    let id = response_data(&torrent)["submit"]["jobId"].as_u64().unwrap();
    assert_eq!(
        rig.torrents.torrent(JobId(id)).await.unwrap().name,
        "fetched.mkv"
    );

    let url = serve(nzb("fetched-nzb")).await;
    let job = rig.submit_text(&url).await;
    assert_no_errors(&job);
    let data = response_data(&job)["submit"].clone();
    assert_eq!(data["accepted"], true);
    assert!(data["item"]["id"].as_u64().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_names_a_torrent_and_leaves_it_to_submit() {
    let rig = rig().await;
    let staged = rig
        .run(
            STAGE,
            json!({ "input": { "nzbUpload": null } }),
            Some(("a.nzb", &metainfo("one.mkv"))),
        )
        .await;
    assert_no_errors(&staged);
    let data = response_data(&staged)["stageNzbUpload"].clone();
    assert_eq!(data["kind"], "TORRENT");
    assert_eq!(data["accepted"], false);
    assert_eq!(data["error"], Value::Null);

    let staged = rig
        .run(
            STAGE,
            json!({ "input": { "nzbUpload": null } }),
            Some(("a.torrent", &nzb("staged"))),
        )
        .await;
    assert_no_errors(&staged);
    let data = response_data(&staged)["stageNzbUpload"].clone();
    assert_eq!(data["kind"], "NZB");
    assert_eq!(data["accepted"], true);
    let id = data["stagedUploadId"].as_str().unwrap().to_owned();

    // A staged NZB is added paused when asked.
    let submitted = rig
        .run(
            r#"mutation Submit($input: SubmitStagedNzbsInput!) {
                submitStagedNzbs(input: $input) { results { accepted item { state } } }
            }"#,
            json!({ "input": { "stagedUploadIds": [id], "paused": true } }),
            None,
        )
        .await;
    assert_no_errors(&submitted);
    let result = response_data(&submitted)["submitStagedNzbs"]["results"][0].clone();
    assert_eq!(result["accepted"], true);
    assert_eq!(result["item"]["state"], "PAUSED");
}
