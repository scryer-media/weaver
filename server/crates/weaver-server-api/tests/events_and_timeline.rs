mod common;

use common::{TestHarness, assert_no_errors, response_data};

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
