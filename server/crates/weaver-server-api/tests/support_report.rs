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
    h.db.create_active_job(&ActiveJob {
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
    })
    .unwrap();
    h.db.archive_job(
        JobId(job_id),
        &JobHistoryRow {
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
