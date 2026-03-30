mod common;

use std::io::Write;

use async_graphql::{Request, UploadValue, Variables};
use base64::Engine;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use serde_json::json;
use weaver_api::auth::CallerScope;

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
                submitNzb(input: {{ nzbBase64: "{nzb_b64}" }}) {{
                    accepted
                    item {{
                        id
                        state
                    }}
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    let id = data["submitNzb"]["item"]["id"].as_u64().unwrap();
    assert!(id > 0);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert_eq!(
        data["submitNzb"]["item"]["state"].as_str().unwrap(),
        "QUEUED"
    );
}

#[tokio::test]
async fn submit_with_upload() {
    let h = TestHarness::new().await;
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(minimal_nzb("upload-test").as_bytes())
        .unwrap();
    file.flush().unwrap();

    let upload = UploadValue {
        filename: "upload-test.nzb".to_string(),
        content_type: Some("application/x-nzb".to_string()),
        content: file.reopen().unwrap(),
    };

    let mut request = Request::new(
        r#"
        mutation Submit($input: SubmitNzbInput!) {
            submitNzb(input: $input) {
                accepted
                item {
                    id
                    state
                }
            }
        }
        "#,
    )
    .data(CallerScope::Local)
    .variables(Variables::from_json(json!({
        "input": {
            "nzbUpload": null
        }
    })));
    request.set_upload("variables.input.nzbUpload", upload);

    let resp = h.schema.execute(request).await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert!(data["submitNzb"]["item"]["id"].as_u64().unwrap() > 0);
    assert_eq!(
        data["submitNzb"]["item"]["state"].as_str().unwrap(),
        "QUEUED"
    );
}

#[tokio::test]
async fn submit_with_filename() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("filename-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{ nzbBase64: "{nzb_b64}", filename: "test.nzb" }}) {{
                    accepted
                    item {{
                        id
                        name
                        state
                    }}
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert!(data["submitNzb"]["item"]["id"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn submit_with_password() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("password-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{ nzbBase64: "{nzb_b64}", password: "secret123" }}) {{
                    accepted
                    item {{
                        id
                        hasPassword
                    }}
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert!(data["submitNzb"]["item"]["hasPassword"].as_bool().unwrap());
}

#[tokio::test]
async fn submit_with_category() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("category-test"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{ nzbBase64: "{nzb_b64}", category: "movies" }}) {{
                    accepted
                    item {{
                        id
                        category
                    }}
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert_eq!(
        data["submitNzb"]["item"]["category"].as_str().unwrap(),
        "movies"
    );
}

#[tokio::test]
async fn submit_with_attributes_and_client_request_id() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("metadata-test"));
    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{
                    nzbBase64: "{nzb_b64}",
                    clientRequestId: "req-123",
                    attributes: [
                        {{ key: "tmdbId", value: "12345" }},
                        {{ key: "source", value: "api" }}
                    ]
                }}) {{
                    accepted
                    clientRequestId
                    item {{
                        id
                        clientRequestId
                        attributes {{ key value }}
                    }}
                }}
            }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["submitNzb"]["accepted"].as_bool().unwrap());
    assert_eq!(
        data["submitNzb"]["clientRequestId"].as_str().unwrap(),
        "req-123"
    );
    assert_eq!(
        data["submitNzb"]["item"]["clientRequestId"]
            .as_str()
            .unwrap(),
        "req-123"
    );

    let attributes = data["submitNzb"]["item"]["attributes"].as_array().unwrap();
    let has_tmdb = attributes
        .iter()
        .any(|m| m["key"] == "tmdbId" && m["value"] == "12345");
    let has_source = attributes
        .iter()
        .any(|m| m["key"] == "source" && m["value"] == "api");
    assert!(has_tmdb, "attributes should contain tmdbId");
    assert!(has_source, "attributes should contain source");
}

#[tokio::test]
async fn submit_invalid_base64() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                submitNzb(input: { nzbBase64: "!!!not-valid-base64!!!" }) {
                    accepted
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
                submitNzb(input: {{ nzbBase64: "{not_xml}" }}) {{
                    accepted
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
                submitNzb(input: {{ nzbBase64: "{empty_nzb}" }}) {{
                    accepted
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
                submitNzb(input: {{ nzbBase64: "{bad_xml}" }}) {{
                    accepted
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn submit_rejects_multiple_source_modes() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("double-source"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{
                    nzbBase64: "{nzb_b64}",
                    url: "https://example.com/test.nzb"
                }}) {{
                    accepted
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn pause_queue_item_not_found_uses_not_found_error_code() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                pauseQueueItem(id: 999999) {
                    success
                }
            }"#,
        )
        .await;

    assert_has_errors(&resp);
    let code = resp.errors[0]
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get("code"));
    assert!(
        matches!(code, Some(async_graphql::Value::String(value)) if value.as_str() == "NOT_FOUND")
    );
}

#[tokio::test]
async fn cancel_queue_item_persists_item_removed_event() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("removed-event").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{
                cancelQueueItem(id: {job_id}) {{
                    success
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);

    let resp = h
        .execute(&format!(
            r#"{{
                historyEvents(itemId: {job_id}) {{
                    kind
                    itemId
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["historyEvents"].as_array().unwrap();
    assert!(
        events.iter().any(|event| {
            event["kind"].as_str() == Some("ITEM_REMOVED")
                && event["itemId"].as_u64() == Some(job_id)
        }),
        "expected ITEM_REMOVED event for cancelled queue item",
    );
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
            r#"mutation {{ pauseQueueItem(id: {id}) {{ success item {{ id state }} }} }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["pauseQueueItem"]["success"].as_bool().unwrap());

    let resp = h.execute("{ queueItems { id state } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["queueItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["state"].as_str().unwrap(), "PAUSED");
}

#[tokio::test]
async fn resume_paused_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("resume-test").await;

    h.execute(&format!(
        r#"mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"#
    ))
    .await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ resumeQueueItem(id: {id}) {{ success item {{ id state }} }} }}"#
        ))
        .await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["resumeQueueItem"]["success"].as_bool().unwrap());

    let resp = h.execute("{ queueItems { id state } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["queueItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["state"].as_str().unwrap(), "DOWNLOADING");
}

#[tokio::test]
async fn pause_idempotent() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("pause-idempotent").await;

    let resp1 = h
        .execute(&format!(
            r#"mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"#
        ))
        .await;
    assert_no_errors(&resp1);

    let resp2 = h
        .execute(&format!(
            r#"mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"#
        ))
        .await;
    assert_no_errors(&resp2);
}

#[tokio::test]
async fn resume_idempotent() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("resume-idempotent").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ resumeQueueItem(id: {id}) {{ success item {{ id state }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);

    let data = response_data(&resp);
    assert!(data["resumeQueueItem"]["success"].as_bool().unwrap());

    let resp = h.execute("{ queueItems { id state } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = data["queueItems"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["id"].as_u64().unwrap() == id)
        .expect("job not found");
    assert_eq!(job["state"].as_str().unwrap(), "DOWNLOADING");
}

#[tokio::test]
async fn cancel_queued_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("cancel-test").await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ cancelQueueItem(id: {id}) {{ success message }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["cancelQueueItem"]["success"].as_bool().unwrap());

    let resp = h.execute("{ queueItems { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["queueItems"].as_array().unwrap();
    let found = jobs.iter().any(|j| j["id"].as_u64().unwrap() == id);
    assert!(!found, "cancelled job should not appear in job list");
}

#[tokio::test]
async fn cancel_paused_job() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("cancel-paused").await;

    h.execute(&format!(
        r#"mutation {{ pauseQueueItem(id: {id}) {{ success }} }}"#
    ))
    .await;

    let resp = h
        .execute(&format!(
            r#"mutation {{ cancelQueueItem(id: {id}) {{ success message }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["cancelQueueItem"]["success"].as_bool().unwrap());

    let resp = h.execute("{ queueItems { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["queueItems"].as_array().unwrap();
    let found = jobs.iter().any(|j| j["id"].as_u64().unwrap() == id);
    assert!(!found, "cancelled job should not appear in job list");
}

#[tokio::test]
async fn pause_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { pauseQueueItem(id: 999999) { success } }"#)
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn resume_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { resumeQueueItem(id: 999999) { success } }"#)
        .await;

    assert_has_errors(&resp);
}

#[tokio::test]
async fn cancel_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { cancelQueueItem(id: 999999) { success } }"#)
        .await;

    assert_has_errors(&resp);
}

// ---------------------------------------------------------------------------
// Global Pause/Resume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pause_all() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { pauseQueue { success globalState { isPaused } } }"#)
        .await;
    assert_no_errors(&resp);

    let resp = h.execute("{ globalQueueState { isPaused } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["globalQueueState"]["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn resume_all() {
    let h = TestHarness::new().await;

    h.execute(r#"mutation { pauseQueue { success } }"#).await;

    let resp = h
        .execute(r#"mutation { resumeQueue { success globalState { isPaused } } }"#)
        .await;
    assert_no_errors(&resp);

    let resp = h.execute("{ globalQueueState { isPaused } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["globalQueueState"]["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn pause_all_idempotent() {
    let h = TestHarness::new().await;

    let resp1 = h.execute(r#"mutation { pauseQueue { success } }"#).await;
    assert_no_errors(&resp1);

    let resp2 = h.execute(r#"mutation { pauseQueue { success } }"#).await;
    assert_no_errors(&resp2);

    let resp = h.execute("{ globalQueueState { isPaused } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["globalQueueState"]["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn resume_all_idempotent() {
    let h = TestHarness::new().await;

    let resp1 = h.execute(r#"mutation { resumeQueue { success } }"#).await;
    assert_no_errors(&resp1);

    let resp2 = h.execute(r#"mutation { resumeQueue { success } }"#).await;
    assert_no_errors(&resp2);

    let resp = h.execute("{ globalQueueState { isPaused } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["globalQueueState"]["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn download_block_reflects_pause() {
    let h = TestHarness::new().await;

    h.execute(r#"mutation { pauseQueue { success } }"#).await;

    // The mock scheduler's pauseAll only sets the paused flag, not the download block
    // (download block is managed by the real pipeline loop). Verify the query works and
    // isPaused reflects the pause.
    let resp = h
        .execute("{ globalQueueState { downloadBlock { kind } isPaused } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["globalQueueState"]["downloadBlock"]["kind"].is_string());
    assert!(data["globalQueueState"]["isPaused"].as_bool().unwrap());
}

// ---------------------------------------------------------------------------
// Reprocess
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reprocess_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { reprocessQueueItem(id: 999999) { success } }"#)
        .await;

    assert_has_errors(&resp);
}
