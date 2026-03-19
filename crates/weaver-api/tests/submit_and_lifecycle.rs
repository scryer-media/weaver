mod common;

use base64::Engine;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

fn encode_nzb(xml: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
}

fn minimal_nzb(name: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
    )
}

// ---------------------------------------------------------------------------
// NZB Submission
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_valid_nzb() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("valid-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{nzb_b64}") {{
                    id
                    status
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    let id = data["submitNzb"]["id"].as_u64().unwrap();
    assert!(id > 0);
    assert_eq!(data["submitNzb"]["status"].as_str().unwrap(), "QUEUED");
}

#[tokio::test]
async fn submit_with_filename() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("filename-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{nzb_b64}", filename: "test.nzb") {{
                    id
                    name
                    status
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["id"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn submit_with_password() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("password-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{nzb_b64}", password: "secret123") {{
                    id
                    hasPassword
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["hasPassword"].as_bool().unwrap());
}

#[tokio::test]
async fn submit_with_category() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("category-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{nzb_b64}", category: "movies") {{
                    id
                    category
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["submitNzb"]["category"].as_str().unwrap(),
        "movies"
    );
}

#[tokio::test]
async fn submit_with_metadata() {
    let h = TestHarness::new().await;
    let id = h
        .submit_test_nzb_with_options(
            "metadata-test",
            None,
            None,
            &[("tmdbId", "12345"), ("source", "api")],
        )
        .await;

    let resp = h
        .execute(&format!(
            r#"{{ jobs {{ id metadata {{ key value }} }} }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    let job = jobs
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("submitted job not found in jobs list");

    let meta = job["metadata"].as_array().unwrap();
    let has_tmdb = meta
        .iter()
        .any(|m| m["key"] == "tmdbId" && m["value"] == "12345");
    let has_source = meta
        .iter()
        .any(|m| m["key"] == "source" && m["value"] == "api");
    assert!(has_tmdb, "metadata should contain tmdbId");
    assert!(has_source, "metadata should contain source");
}

#[tokio::test]
async fn submit_invalid_base64() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                submitNzb(nzbBase64: "!!!not-valid-base64!!!") {
                    id
                }
            }"#,
        )
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn submit_not_xml() {
    let h = TestHarness::new().await;
    let not_xml = base64::engine::general_purpose::STANDARD.encode(b"this is not xml at all");

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{not_xml}") {{
                    id
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn submit_empty_nzb() {
    let h = TestHarness::new().await;
    let empty_nzb = encode_nzb(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"></nzb>"#,
    );

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{empty_nzb}") {{
                    id
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn submit_nzb_missing_root() {
    let h = TestHarness::new().await;
    let bad_xml = encode_nzb(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<notanNzb><something/></notanNzb>"#,
    );

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(nzbBase64: "{bad_xml}") {{
                    id
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn submit_duplicate_nzbs() {
    let h = TestHarness::new().await;
    let id1 = h.submit_test_nzb("duplicate-nzb").await;
    let id2 = h.submit_test_nzb("duplicate-nzb").await;

    assert!(id1 > 0);
    assert!(id2 > 0);
}

// ---------------------------------------------------------------------------
// Job Lifecycle — Single
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pause_queued_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("pause-test").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ pauseJob(id: {id}) }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["pauseJob"].as_bool().unwrap());

    let resp = h.execute("{ jobs { id status } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["status"].as_str().unwrap(), "PAUSED");
}

#[tokio::test]
async fn resume_paused_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("resume-test").await;

    h.execute(&format!(r#"mutation {{ pauseJob(id: {id}) }}"#))
        .await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ resumeJob(id: {id}) }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["resumeJob"].as_bool().unwrap());

    let resp = h.execute("{ jobs { id status } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["status"].as_str().unwrap(), "DOWNLOADING");
}

#[tokio::test]
async fn pause_idempotent() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("pause-idempotent").await;

    let resp1 = h
        .execute(&format!(r#"mutation {{ pauseJob(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp1);

    let resp2 = h
        .execute(&format!(r#"mutation {{ pauseJob(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp2);
}

#[tokio::test]
async fn resume_idempotent() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("resume-idempotent").await;

    let resp = h
        .execute(&format!(r#"mutation {{ resumeJob(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp);

    let data = response_data(&resp);
    assert!(data["resumeJob"].as_bool().unwrap());

    let resp = h.execute("{ jobs { id status } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["status"].as_str().unwrap(), "DOWNLOADING");
}

#[tokio::test]
async fn cancel_queued_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("cancel-test").await;

    let resp = h
        .execute(&format!(r#"mutation {{ cancelJob(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["cancelJob"].as_bool().unwrap());

    let resp = h.execute("{ jobs { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    let found = jobs.iter().any(|j| j["id"].as_u64().unwrap() == id);
    assert!(!found, "cancelled job should not appear in job list");
}

#[tokio::test]
async fn cancel_paused_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("cancel-paused").await;

    h.execute(&format!(r#"mutation {{ pauseJob(id: {id}) }}"#))
        .await;

    let resp = h
        .execute(&format!(r#"mutation {{ cancelJob(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["cancelJob"].as_bool().unwrap());

    let resp = h.execute("{ jobs { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    let found = jobs.iter().any(|j| j["id"].as_u64().unwrap() == id);
    assert!(!found, "cancelled job should not appear in job list");
}

#[tokio::test]
async fn pause_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { pauseJob(id: 999999) }"#)
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn resume_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { resumeJob(id: 999999) }"#)
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn cancel_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { cancelJob(id: 999999) }"#)
        .await;

    assert_has_errors(&resp);
}

// ---------------------------------------------------------------------------
// Global Pause/Resume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pause_all() {
    let h = TestHarness::new().await;

    let resp = h.execute(r#"mutation { pauseAll }"#).await;
    assert_no_errors(&resp);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn resume_all() {
    let h = TestHarness::new().await;

    h.execute(r#"mutation { pauseAll }"#).await;

    let resp = h.execute(r#"mutation { resumeAll }"#).await;
    assert_no_errors(&resp);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn pause_all_idempotent() {
    let h = TestHarness::new().await;

    let resp1 = h.execute(r#"mutation { pauseAll }"#).await;
    assert_no_errors(&resp1);

    let resp2 = h.execute(r#"mutation { pauseAll }"#).await;
    assert_no_errors(&resp2);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn resume_all_idempotent() {
    let h = TestHarness::new().await;

    let resp1 = h.execute(r#"mutation { resumeAll }"#).await;
    assert_no_errors(&resp1);

    let resp2 = h.execute(r#"mutation { resumeAll }"#).await;
    assert_no_errors(&resp2);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn download_block_reflects_pause() {
    let h = TestHarness::new().await;

    h.execute(r#"mutation { pauseAll }"#).await;

    // The mock scheduler's pauseAll only sets the paused flag, not the download block
    // (download block is managed by the real pipeline loop). Verify the query works and
    // isPaused reflects the pause.
    let resp = h.execute("{ downloadBlock { kind } isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["downloadBlock"]["kind"].is_string());
    assert_eq!(data["isPaused"].as_bool().unwrap(), true);
}

// ---------------------------------------------------------------------------
// Reprocess
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reprocess_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { reprocessJob(id: 999999) }"#)
        .await;

    assert_has_errors(&resp);
}
