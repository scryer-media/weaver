// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use std::io::Write;
use std::path::PathBuf;

use async_graphql::{Request, UploadValue, Variables};
use base64::Engine;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use serde_json::json;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_core::{ActiveJob, JobHistoryRow, JobId};

const SENTINEL: &str = "wkxsentinel";

fn sentinel_nzb() -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head><meta type="password">{s}</meta></head>
  <file poster="{s}@{s}.invalid" date="1767000000" subject="[1/1] - &quot;{s}.mkv&quot; yEnc (1/2)">
    <groups><group>alt.binaries.{s}</group></groups>
    <segments>
      <segment bytes="1000" number="1">{s}-1@{s}.invalid</segment>
      <segment bytes="1000" number="2">{s}-2@{s}.invalid</segment>
    </segments>
  </file>
</nzb>"#,
        s = SENTINEL
    )
}

fn assert_redacted(report: &serde_json::Value) {
    let text = report["text"].as_str().expect("text");
    let json = report["json"].as_str().expect("json");
    assert!(!text.to_ascii_lowercase().contains(SENTINEL), "{text}");
    assert!(!json.to_ascii_lowercase().contains(SENTINEL), "{json}");
    assert!(text.contains("fingerprint "));
}

fn seed_history_job(h: &TestHarness, job_id: u64) {
    seed_history_job_with_facts(h, job_id, None);
}

fn seed_history_job_with_facts(h: &TestHarness, job_id: u64, facts: Option<String>) {
    h.db.create_active_job(&ActiveJob {
        engine: weaver_server_core::pipeline::graph::Engine::Classic,
        job_id: JobId(job_id),
        nzb_hash: [0x22; 32],
        nzb_path: PathBuf::from(format!("/data/{SENTINEL}.nzb")),
        nzb_zstd: weaver_server_core::ingest::compress_nzb_bytes(sentinel_nzb().as_bytes())
            .unwrap(),
        output_dir: PathBuf::from(format!("/data/{SENTINEL}")),
        created_at: 1_767_000_000,
        category: None,
        metadata: vec![],
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
    })
    .unwrap();
    if facts.is_some() {
        h.db.save_active_support_facts(vec![(JobId(job_id), facts)])
            .unwrap();
    }
    h.db.archive_job(
        JobId(job_id),
        &JobHistoryRow {
            protocol: weaver_server_core::Protocol::Nzb,
            job_id,
            job_hash: None,
            name: SENTINEL.to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 2_000,
            downloaded_bytes: 2_000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some(format!("/data/{SENTINEL}")),
            nzb_path: None,
            created_at: 1_767_000_000,
            completed_at: 1_767_000_010,
            metadata: None,
            server_attribution: None,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn read_scope_can_read_a_job_support_report() {
    let h = TestHarness::new().await;
    seed_history_job(&h, 41);
    let response = h
        .execute_as(
            "{ jobSupportReport(jobId: 41) { text json } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let report = &data["jobSupportReport"];
    assert_redacted(report);
    let json: serde_json::Value = serde_json::from_str(report["json"].as_str().unwrap()).unwrap();
    assert_eq!(json["job"]["source"], "history");
    assert_eq!(json["job"]["status"], "complete");
    assert_eq!(json["nzb"]["shape"]["file_count"], 1);
    assert!(json["environment"]["weaver_version"].is_string());
}

#[tokio::test]
async fn a_job_support_report_carries_the_demotion_and_article_gaps() {
    use weaver_server_core::jobs::support_facts::{GapKind, GapPosition, JobSupportFacts};

    let h = TestHarness::new().await;
    let mut facts = JobSupportFacts::default();
    facts.note_demotion("par2_damaged", "downloading", 1_767_000_005);
    facts.note_gap(GapKind::Missing, GapPosition(0, 1));
    facts.note_gap_servers([3]);
    seed_history_job_with_facts(&h, 42, facts.to_storage_json());
    let response = h
        .execute_as(
            "{ jobSupportReport(jobId: 42) { text json } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let report = &data["jobSupportReport"];
    assert_redacted(report);
    let text = report["text"].as_str().unwrap();
    assert!(
        text.contains("  demoted par2_damaged  in downloading  sets 1"),
        "{text}"
    );
    assert!(
        text.contains("  gaps missing 1  failed 0  refused s3 1"),
        "{text}"
    );
    assert!(text.contains("  gap at f01#2"), "{text}");
    let json: serde_json::Value = serde_json::from_str(report["json"].as_str().unwrap()).unwrap();
    assert_eq!(json["job"]["demotion"]["reason"], "par2_damaged");
    assert_eq!(json["job"]["gaps"]["missing"], 1);
    assert_eq!(json["job"]["gaps"]["sample"][0], "f01#2");
}

#[tokio::test]
async fn unknown_job_reports_not_found() {
    let h = TestHarness::new().await;
    let response = h
        .execute_as(
            "{ jobSupportReport(jobId: 404) { text } }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);
    let code = response.errors[0]
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get("code"))
        .cloned();
    assert_eq!(code, Some(async_graphql::Value::from("NOT_FOUND")));
}

#[tokio::test]
async fn read_scope_can_analyze_gzipped_base64() {
    let h = TestHarness::new().await;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(sentinel_nzb().as_bytes()).unwrap();
    let gzipped = encoder.finish().unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(gzipped);
    let response = h
        .execute_as(
            &format!(
                r#"mutation {{ analyzeNzb(input: {{ nzbBase64: "{encoded}" }}) {{ text json }} }}"#
            ),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_redacted(&data["analyzeNzb"]);
    let json: serde_json::Value =
        serde_json::from_str(data["analyzeNzb"]["json"].as_str().unwrap()).unwrap();
    assert!(json["job"].is_null());
    assert_eq!(json["nzb"]["layout"]["password"]["in_meta"], true);
}

#[tokio::test]
async fn analyze_accepts_an_upload_and_refuses_two_sources() {
    let h = TestHarness::new().await;
    let mut request = Request::new(
        "mutation Analyze($input: AnalyzeNzbInput!) { analyzeNzb(input: $input) { text json } }",
    )
    .data(CallerScope::Read)
    .data(CallerIdentity::Local([9; 32]))
    .variables(Variables::from_json(
        json!({ "input": { "nzbUpload": null } }),
    ));
    request.set_upload(
        "variables.input.nzbUpload",
        UploadValue {
            filename: format!("{SENTINEL}.nzb"),
            content_type: Some("application/x-nzb".to_string()),
            content: sentinel_nzb().into_bytes().into(),
        },
    );
    let response = h.schema.execute(request).await;
    assert_no_errors(&response);
    assert_redacted(&response_data(&response)["analyzeNzb"]);

    let response = h
        .execute_as(
            r#"mutation { analyzeNzb(input: {}) { text } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);

    let response = h
        .execute_as(
            r#"mutation { analyzeNzb(input: { nzbBase64: "bm90IGFuIG56Yg==" }) { text } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A private one-file torrent named after the sentinel.
fn sentinel_torrent() -> Vec<u8> {
    let mut info = b"d".to_vec();
    bstr(&mut info, b"length");
    info.extend_from_slice(b"i40000e");
    bstr(&mut info, b"name");
    bstr(&mut info, format!("{SENTINEL}.mkv").as_bytes());
    bstr(&mut info, b"piece length");
    info.extend_from_slice(b"i16384e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &[3u8; 60]);
    bstr(&mut info, b"private");
    info.extend_from_slice(b"i1e");
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"announce");
    bstr(
        &mut out,
        format!("https://tracker.example/{SENTINEL}/announce").as_bytes(),
    );
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    out
}

// The hash a magnet in these tests goes by: the server tests tie it to a
// file's.
const HASH: [u8; 20] = [0x5a; 20];

#[tokio::test]
async fn read_scope_can_analyze_a_torrent_and_its_magnet() {
    let h = TestHarness::new().await;
    let metainfo = sentinel_torrent();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&metainfo);
    let response = h
        .execute_as(
            &format!(
                r#"mutation {{ analyzeTorrent(input: {{ torrentBase64: "{encoded}" }}) {{ text json }} }}"#
            ),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let file = &data["analyzeTorrent"];
    assert_redacted(file);
    let file_json: serde_json::Value =
        serde_json::from_str(file["json"].as_str().unwrap()).unwrap();
    assert!(file_json["job"].is_null());
    assert_eq!(file_json["torrent"]["identity"]["source"], "metainfo");
    assert_eq!(file_json["torrent"]["trackers"]["private"], true);

    let magnet = format!("magnet:?xt=urn:btih:{}&dn={SENTINEL}", hex::encode(HASH));
    let response = h
        .execute_as(
            &format!(
                r#"mutation {{ analyzeTorrent(input: {{ magnet: "{magnet}" }}) {{ text json }} }}"#
            ),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_redacted(&data["analyzeTorrent"]);
    let json: serde_json::Value =
        serde_json::from_str(data["analyzeTorrent"]["json"].as_str().unwrap()).unwrap();
    assert_eq!(json["torrent"]["identity"]["source"], "magnet");
    assert!(
        !data["analyzeTorrent"]["json"]
            .as_str()
            .unwrap()
            .contains(&hex::encode(HASH))
    );
}

#[tokio::test]
async fn analyze_torrent_accepts_an_upload_and_refuses_two_sources() {
    let h = TestHarness::new().await;
    let metainfo = sentinel_torrent();
    let mut request = Request::new(
        "mutation Analyze($input: AnalyzeTorrentInput!) { analyzeTorrent(input: $input) { text json } }",
    )
    .data(CallerScope::Read)
    .data(CallerIdentity::Local([9; 32]))
    .variables(Variables::from_json(
        json!({ "input": { "torrentUpload": null } }),
    ));
    request.set_upload(
        "variables.input.torrentUpload",
        UploadValue {
            filename: format!("{SENTINEL}.torrent"),
            content_type: Some("application/x-bittorrent".to_string()),
            content: metainfo.into(),
        },
    );
    let response = h.schema.execute(request).await;
    assert_no_errors(&response);
    assert_redacted(&response_data(&response)["analyzeTorrent"]);

    let response = h
        .execute_as(
            r#"mutation { analyzeTorrent(input: {}) { text } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);

    let magnet = format!("magnet:?xt=urn:btih:{}", hex::encode(HASH));
    let response = h
        .execute_as(
            &format!(
                r#"mutation {{ analyzeTorrent(input: {{ torrentBase64: "ZGU=", magnet: "{magnet}" }}) {{ text }} }}"#
            ),
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);

    let response = h
        .execute_as(
            r#"mutation { analyzeTorrent(input: { torrentBase64: "bm90IGEgdG9ycmVudA==" }) { text } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&response);
}

#[tokio::test]
async fn a_retired_torrent_job_reports_from_its_hash() {
    let h = TestHarness::new().await;
    // Torrents are archived with the hash they went by and no NZB.
    h.db.archive_job(
        JobId(44),
        &JobHistoryRow {
            protocol: weaver_server_core::Protocol::Torrent,
            job_id: 44,
            job_hash: Some(HASH.to_vec()),
            name: SENTINEL.to_string(),
            status: "failed".to_string(),
            error_message: Some(format!("metadata for {SENTINEL} never arrived")),
            total_bytes: 0,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1_767_000_000,
            completed_at: 1_767_000_600,
            metadata: None,
            server_attribution: None,
        },
    )
    .unwrap();
    let response = h
        .execute_as(
            "{ jobSupportReport(jobId: 44) { text json } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let report = &data["jobSupportReport"];
    assert_redacted(report);
    let json: serde_json::Value = serde_json::from_str(report["json"].as_str().unwrap()).unwrap();
    assert_eq!(json["job"]["source"], "history");
    assert_eq!(json["job"]["failure"], "metadata");
    assert_eq!(json["torrent"]["identity"]["source"], "magnet");
    assert!(json["nzb"].is_null());
}
