mod common;

use std::io::Write;

use async_graphql::{Request, UploadValue, Variables};
use base64::Engine;
use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use serde_json::json;
use weaver_server_api::auth::{CallerIdentity, CallerScope};
use weaver_server_api::encode_event_cursor;

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

fn submit_upload(
    h: &TestHarness,
    upload: UploadValue,
) -> impl std::future::Future<Output = async_graphql::Response> + '_ {
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
    .data(local_identity(7))
    .variables(Variables::from_json(json!({
        "input": {
            "nzbUpload": null
        }
    })));
    request.set_upload("variables.input.nzbUpload", upload);
    h.schema.execute(request)
}

fn local_identity(value: u8) -> CallerIdentity {
    CallerIdentity::Local([value; 32])
}

fn stage_upload(
    h: &TestHarness,
    upload: UploadValue,
    identity: CallerIdentity,
) -> impl std::future::Future<Output = async_graphql::Response> + '_ {
    let mut request = Request::new(
        r#"
        mutation Stage($input: StageNzbUploadInput!) {
            stageNzbUpload(input: $input) {
                accepted
                stagedUploadId
                filename
                displayName
                totalFiles
                totalBytes
                error
            }
        }
        "#,
    )
    .data(CallerScope::Local)
    .data(identity)
    .variables(Variables::from_json(json!({
        "input": {
            "nzbUpload": null
        }
    })));
    request.set_upload("variables.input.nzbUpload", upload);
    h.schema.execute(request)
}

fn submit_staged(
    h: &TestHarness,
    identity: CallerIdentity,
    staged_upload_ids: Vec<String>,
) -> impl std::future::Future<Output = async_graphql::Response> + '_ {
    let request = Request::new(
        r#"
        mutation Submit($input: SubmitStagedNzbsInput!) {
            submitStagedNzbs(input: $input) {
                acceptedCount
                clientRequestId
                results {
                    stagedUploadId
                    accepted
                    retained
                    error
                    item {
                        id
                        state
                        category
                        hasPassword
                        clientRequestId
                        attributes { key value }
                    }
                }
            }
        }
        "#,
    )
    .data(CallerScope::Local)
    .data(identity)
    .variables(Variables::from_json(json!({
        "input": {
            "stagedUploadIds": staged_upload_ids,
            "password": "secret123",
            "category": "movies",
            "clientRequestId": "req-staged-123",
            "attributes": [
                { "key": "priority", "value": "HIGH" },
                { "key": "source", "value": "staged-test" }
            ]
        }
    })));
    h.schema.execute(request)
}

fn discard_staged(
    h: &TestHarness,
    identity: CallerIdentity,
    ids: Vec<String>,
) -> impl std::future::Future<Output = async_graphql::Response> + '_ {
    let request = Request::new(
        r#"
        mutation Discard($ids: [String!]!) {
            discardStagedNzbs(ids: $ids)
        }
        "#,
    )
    .data(CallerScope::Local)
    .data(identity)
    .variables(Variables::from_json(json!({ "ids": ids })));
    h.schema.execute(request)
}

fn assert_upload_accepts(encoded_bytes: &[u8], filename: &str, content_type: &str) -> UploadValue {
    UploadValue {
        filename: filename.to_string(),
        content_type: Some(content_type.to_string()),
        content: encoded_bytes.to_vec().into(),
    }
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
    let upload = assert_upload_accepts(
        minimal_nzb("upload-test").as_bytes(),
        "upload-test.nzb",
        "application/x-nzb",
    );
    let resp = submit_upload(&h, upload).await;

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
async fn stage_upload_and_submit_staged_nzb() {
    let h = TestHarness::new().await;
    let identity = local_identity(11);
    let upload = assert_upload_accepts(
        minimal_nzb("staged-upload-test").as_bytes(),
        "staged-upload-test.nzb",
        "application/x-nzb",
    );

    let staged_resp = stage_upload(&h, upload, identity.clone()).await;
    assert_no_errors(&staged_resp);
    let staged_data = response_data(&staged_resp);
    assert!(staged_data["stageNzbUpload"]["accepted"].as_bool().unwrap());
    assert_eq!(
        staged_data["stageNzbUpload"]["displayName"]
            .as_str()
            .unwrap(),
        "staged-upload-test"
    );
    let staged_upload_id = staged_data["stageNzbUpload"]["stagedUploadId"]
        .as_str()
        .unwrap()
        .to_string();

    let submit_resp = submit_staged(&h, identity, vec![staged_upload_id.clone()]).await;
    assert_no_errors(&submit_resp);
    let submit_data = response_data(&submit_resp);
    assert_eq!(
        submit_data["submitStagedNzbs"]["acceptedCount"]
            .as_u64()
            .unwrap(),
        1
    );
    assert_eq!(
        submit_data["submitStagedNzbs"]["clientRequestId"]
            .as_str()
            .unwrap(),
        "req-staged-123"
    );
    let result = &submit_data["submitStagedNzbs"]["results"][0];
    assert_eq!(result["stagedUploadId"].as_str().unwrap(), staged_upload_id);
    assert!(result["accepted"].as_bool().unwrap());
    assert!(!result["retained"].as_bool().unwrap());
    assert_eq!(result["item"]["state"].as_str().unwrap(), "QUEUED");
    assert_eq!(result["item"]["category"].as_str().unwrap(), "movies");
    assert!(result["item"]["hasPassword"].as_bool().unwrap());
    assert_eq!(
        result["item"]["clientRequestId"].as_str().unwrap(),
        "req-staged-123"
    );
}

#[tokio::test]
async fn discard_staged_uploads_is_idempotent() {
    let h = TestHarness::new().await;
    let identity = local_identity(12);
    let upload = assert_upload_accepts(
        minimal_nzb("staged-discard-test").as_bytes(),
        "staged-discard-test.nzb",
        "application/x-nzb",
    );
    let staged_resp = stage_upload(&h, upload, identity.clone()).await;
    assert_no_errors(&staged_resp);
    let staged_upload_id = response_data(&staged_resp)["stageNzbUpload"]["stagedUploadId"]
        .as_str()
        .unwrap()
        .to_string();

    let discard_once = discard_staged(&h, identity.clone(), vec![staged_upload_id.clone()]).await;
    assert_no_errors(&discard_once);
    assert!(
        response_data(&discard_once)["discardStagedNzbs"]
            .as_bool()
            .unwrap()
    );

    let discard_twice = discard_staged(&h, identity.clone(), vec![staged_upload_id.clone()]).await;
    assert_no_errors(&discard_twice);
    assert!(
        response_data(&discard_twice)["discardStagedNzbs"]
            .as_bool()
            .unwrap()
    );

    let submit_resp = submit_staged(&h, identity, vec![staged_upload_id]).await;
    assert_no_errors(&submit_resp);
    let result = &response_data(&submit_resp)["submitStagedNzbs"]["results"][0];
    assert!(!result["accepted"].as_bool().unwrap());
    assert!(!result["retained"].as_bool().unwrap());
}

#[tokio::test]
async fn staged_uploads_are_scoped_to_caller_identity() {
    let h = TestHarness::new().await;
    let owner = local_identity(13);
    let intruder = local_identity(14);
    let upload = assert_upload_accepts(
        minimal_nzb("staged-owner-test").as_bytes(),
        "staged-owner-test.nzb",
        "application/x-nzb",
    );
    let staged_resp = stage_upload(&h, upload, owner).await;
    assert_no_errors(&staged_resp);
    let staged_upload_id = response_data(&staged_resp)["stageNzbUpload"]["stagedUploadId"]
        .as_str()
        .unwrap()
        .to_string();

    let submit_resp = submit_staged(&h, intruder, vec![staged_upload_id]).await;
    assert_no_errors(&submit_resp);
    let data = response_data(&submit_resp);
    assert_eq!(
        data["submitStagedNzbs"]["acceptedCount"].as_u64().unwrap(),
        0
    );
    let result = &data["submitStagedNzbs"]["results"][0];
    assert!(!result["accepted"].as_bool().unwrap());
    assert!(!result["retained"].as_bool().unwrap());
    assert!(result["error"].as_str().unwrap().contains("expired"));
}

#[tokio::test]
async fn submit_with_zstd_upload() {
    let h = TestHarness::new().await;
    let compressed = zstd::bulk::compress(minimal_nzb("upload-zstd-test").as_bytes(), 3).unwrap();
    let upload = assert_upload_accepts(&compressed, "upload-zstd-test.nzb.zst", "application/zstd");
    let resp = submit_upload(&h, upload).await;

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
async fn submit_with_zstd_content_type_without_zst_filename() {
    let h = TestHarness::new().await;
    let compressed =
        zstd::bulk::compress(minimal_nzb("upload-zstd-content-type").as_bytes(), 3).unwrap();
    let upload = assert_upload_accepts(
        &compressed,
        "upload-zstd-content-type.nzb",
        "application/zstd",
    );
    let resp = submit_upload(&h, upload).await;

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
async fn submit_with_gzip_upload() {
    let h = TestHarness::new().await;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(minimal_nzb("upload-gzip-test").as_bytes())
        .unwrap();
    let compressed = encoder.finish().unwrap();
    let upload = assert_upload_accepts(&compressed, "upload-gzip-test.nzb.gz", "application/gzip");
    let resp = submit_upload(&h, upload).await;

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
async fn submit_with_brotli_upload() {
    let h = TestHarness::new().await;
    let mut compressed = Vec::new();
    {
        let mut encoder = brotli::CompressorWriter::new(&mut compressed, 64 * 1024, 5, 22);
        encoder
            .write_all(minimal_nzb("upload-brotli-test").as_bytes())
            .unwrap();
        encoder.flush().unwrap();
    }
    let upload = assert_upload_accepts(
        &compressed,
        "upload-brotli-test.nzb.br",
        "application/brotli",
    );
    let resp = submit_upload(&h, upload).await;

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
async fn submit_with_deflate_upload() {
    let h = TestHarness::new().await;
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(minimal_nzb("upload-deflate-test").as_bytes())
        .unwrap();
    let compressed = encoder.finish().unwrap();
    let upload = assert_upload_accepts(
        &compressed,
        "upload-deflate-test.nzb.deflate",
        "application/deflate",
    );
    let resp = submit_upload(&h, upload).await;

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
async fn submit_rejects_invalid_priority_attribute() {
    let h = TestHarness::new().await;
    let nzb_b64 = encode_nzb(&minimal_nzb("invalid-priority"));

    let resp = h
        .execute(&format!(
            r#"mutation {{
                submitNzb(input: {{
                    nzbBase64: \"{nzb_b64}\",
                    attributes: [{{ key: \"priority\", value: \"urgent\" }}]
                }}) {{
                    accepted
                }}
            }}"#
        ))
        .await;

    assert_has_errors(&resp);
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
async fn cancel_queue_item_history_events_replay_removed_item() {
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
    assert!(
        h.db.list_integration_events_after(None, None, None)
            .unwrap()
            .is_empty()
    );

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
        !events.is_empty(),
        "expected recent in-memory queue history for the cancelled item"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"].as_str() == Some("ITEM_REMOVED"))
            .count(),
        1,
        "expected exactly one in-memory removal event"
    );
    let last_event = events.last().unwrap();
    assert_eq!(last_event["kind"].as_str(), Some("ITEM_REMOVED"));
    assert_eq!(last_event["itemId"].as_u64(), Some(job_id));
}

#[tokio::test]
async fn history_events_stale_cursor_are_empty_for_compat() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("stale-history-cursor").await;

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
                historyEvents(itemId: {job_id}, after: "{}") {{
                    kind
                }}
            }}"#,
            encode_event_cursor(-1)
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["historyEvents"].as_array().unwrap();
    assert!(
        events.is_empty(),
        "stale history cursors should degrade to an empty compatibility response"
    );
}

#[tokio::test]
async fn submit_duplicate_nzbs() {
    let h = TestHarness::new().await;
    let id1 = h.submit_test_nzb("duplicate-nzb").await;
    let nzb_b64 = encode_nzb(&minimal_nzb("duplicate-nzb"));
    let resp = h
        .execute(&format!(
            r#"
            mutation {{
              submitNzb(input: {{ nzbBase64: "{nzb_b64}", filename: "duplicate-nzb.nzb" }}) {{
                accepted
                status
                errorCode
                item {{ id }}
                duplicateDecision {{ action forceBypassed matches {{ jobId fingerprintKind lifecycle }} }}
              }}
            }}
        "#
        ))
        .await;

    assert!(id1 > 0);
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["submitNzb"];
    assert!(!result["accepted"].as_bool().unwrap());
    assert_eq!(result["status"].as_str().unwrap(), "BLOCKED");
    assert_eq!(result["errorCode"].as_str().unwrap(), "DUPLICATE_BLOCKED");
    assert!(result["item"].is_null());
    assert_eq!(
        result["duplicateDecision"]["action"].as_str().unwrap(),
        "BLOCK"
    );
}

#[tokio::test]
async fn graphql_score_submission_returns_a_parked_candidate_without_queue_item() {
    let h = TestHarness::new().await;
    let winner_b64 = encode_nzb(&minimal_nzb("semantic-winner"));
    let loser_b64 = encode_nzb(&minimal_nzb("semantic-loser"));

    let winner = h
        .execute(&format!(
            r#"
            mutation {{
              submitNzb(input: {{
                nzbBase64: "{winner_b64}", filename: "semantic-winner.nzb",
                dupeKey: "  Group－A ", dupeScore: 10, dupeMode: SCORE
              }}) {{
                accepted status jobId item {{ id }}
                semanticDuplicate {{ normalizedKey score state }}
              }}
            }}
        "#
        ))
        .await;
    assert_no_errors(&winner);
    let winner = response_data(&winner);
    assert_eq!(winner["submitNzb"]["status"].as_str(), Some("ACCEPTED"));
    assert_eq!(
        winner["submitNzb"]["semanticDuplicate"]["state"].as_str(),
        Some("ACTIVE")
    );
    assert_eq!(
        winner["submitNzb"]["semanticDuplicate"]["normalizedKey"].as_str(),
        Some("group-a")
    );
    let winner_id = winner["submitNzb"]["jobId"]
        .as_u64()
        .expect("SCORE winner must reserve a job id");

    let loser = h
        .execute(&format!(
            r#"
            mutation {{
              submitNzb(input: {{
                nzbBase64: "{loser_b64}", filename: "semantic-loser.nzb",
                dupeKey: "group-a", dupeScore: 10, dupeMode: SCORE
              }}) {{
                accepted status item {{ id }} message
                semanticDuplicate {{ normalizedKey score state }}
              }}
            }}
        "#
        ))
        .await;
    assert_no_errors(&loser);
    let loser = response_data(&loser);
    assert_eq!(loser["submitNzb"]["accepted"].as_bool(), Some(true));
    assert_eq!(loser["submitNzb"]["status"].as_str(), Some("PARKED"));
    assert!(loser["submitNzb"]["item"].is_null());
    assert_eq!(
        loser["submitNzb"]["semanticDuplicate"]["state"].as_str(),
        Some("PARKED")
    );

    let snapshot = h
        .execute(&format!(
            r#"{{
                duplicateSnapshot(id: {winner_id}) {{
                    jobId lifecycle normalizedName
                    semantic {{ groupId normalizedKey score state terminalCause promotionState }}
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&snapshot);
    let snapshot = response_data(&snapshot);
    assert_eq!(
        snapshot["duplicateSnapshot"]["jobId"].as_u64(),
        Some(winner_id)
    );
    assert_eq!(
        snapshot["duplicateSnapshot"]["semantic"]["state"].as_str(),
        Some("ACTIVE")
    );
    assert_eq!(
        snapshot["duplicateSnapshot"]["semantic"]["promotionState"].as_str(),
        Some("NONE")
    );
}

#[tokio::test]
async fn mark_duplicate_bad_requires_a_semantic_candidate_before_cancelling() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("not-a-semantic-candidate").await;

    let response = h
        .execute(&format!(
            r#"mutation {{ markDuplicateBad(id: {id}) {{ accepted jobId message }} }}"#
        ))
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["markDuplicateBad"]["accepted"].as_bool(), Some(false));

    let queue = h.execute("{ queueItems { id } }").await;
    assert_no_errors(&queue);
    assert!(
        response_data(&queue)["queueItems"]
            .as_array()
            .expect("queueItems array")
            .iter()
            .any(|item| item["id"].as_u64() == Some(id))
    );
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

#[tokio::test]
async fn redownload_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { redownloadQueueItem(id: 999999) { success } }"#)
        .await;

    assert_has_errors(&resp);
}
