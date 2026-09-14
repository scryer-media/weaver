mod common;

use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_core::JobHistoryRow;

#[tokio::test]
async fn job_events_after_submit() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("events-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobEvents(jobId: {job_id}) {{ kind jobId message timestamp }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    // The mock scheduler does not persist events to the DB, so the list may
    // be empty. We simply verify the query succeeds and returns an array.
    assert!(data["jobEvents"].as_array().is_some());
}

#[tokio::test]
async fn job_events_have_required_fields() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("events-fields-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobEvents(jobId: {job_id}) {{ kind jobId message timestamp }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["jobEvents"].as_array().unwrap();
    for event in events {
        assert!(
            event["kind"].as_str().is_some(),
            "event should have a kind field"
        );
        assert!(
            event["jobId"].as_u64().is_some(),
            "event should have a jobId field"
        );
        assert!(
            event["message"].as_str().is_some(),
            "event should have a message field"
        );
        assert!(
            event["timestamp"].as_f64().is_some(),
            "event should have a timestamp field"
        );
    }
}

#[tokio::test]
async fn job_events_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"{ jobEvents(jobId: 999999) { kind jobId message timestamp } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["jobEvents"].as_array().unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn job_timeline_after_submit() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("timeline-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobTimeline(jobId: {job_id}) {{ startedAt outcome }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(
        !data["jobTimeline"].is_null(),
        "timeline should be non-null for an existing job"
    );
}

#[tokio::test]
async fn job_timeline_nonexistent() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"{ jobTimeline(jobId: 999999) { startedAt outcome } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(
        data["jobTimeline"].is_null(),
        "timeline should be null for a nonexistent job"
    );
}

#[tokio::test]
async fn job_detail_snapshot_query_returns_live_job_data() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("job-detail-snapshot").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {job_id}) {{ queueItem {{ id state }} historyItem {{ id }} jobTimeline {{ outcome }} jobEvents {{ kind }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["id"].as_u64(),
        Some(job_id)
    );
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["state"].as_str(),
        Some("QUEUED")
    );
    assert!(data["jobDetailSnapshot"]["historyItem"].is_null());
    assert!(!data["jobDetailSnapshot"]["jobTimeline"].is_null());
    assert!(data["jobDetailSnapshot"]["jobEvents"].as_array().is_some());
}

#[tokio::test]
async fn job_detail_snapshot_names_the_servers_that_served_an_archived_job() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.invalid",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let server_id = response_data(&resp)["addServer"]["id"]
        .as_u64()
        .expect("the new server's id");

    // Credit the configured server plus one that no longer exists: a deleted
    // server keeps its counts and reports no host.
    let stale_server_id = server_id + 1_000;
    let job_id = 8_150_u64;
    h.insert_history_row(&JobHistoryRow {
        job_id,
        job_hash: None,
        name: "attributed-job".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 4_096,
        downloaded_bytes: 4_096,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_060,
        metadata: None,
        server_attribution: Some(format!(
            r#"[{{"s":{stale_server_id},"a":2,"b":2048}},{{"s":{server_id},"a":7,"b":7168}}]"#
        )),
    });

    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {job_id}) {{ serverAttribution {{ serverId serverHost articles wireBytes }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let contributions = response_data(&resp)["jobDetailSnapshot"]["serverAttribution"]
        .as_array()
        .expect("an attribution list")
        .clone();
    assert_eq!(contributions.len(), 2);
    // Highest article count first, whatever order storage happened to use.
    assert_eq!(contributions[0]["serverId"].as_u64(), Some(server_id));
    assert_eq!(
        contributions[0]["serverHost"].as_str(),
        Some("news.example.invalid")
    );
    assert_eq!(contributions[0]["articles"].as_u64(), Some(7));
    assert_eq!(contributions[0]["wireBytes"].as_u64(), Some(7_168));
    assert_eq!(contributions[1]["serverId"].as_u64(), Some(stale_server_id));
    assert!(contributions[1]["serverHost"].is_null());
}
