mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use async_graphql::Request;
use common::TestHarness;
use tokio_stream::StreamExt;
use weaver_server_api::auth::CallerScope;
use weaver_server_api::encode_event_cursor;

async fn latest_queue_cursor(harness: &TestHarness) -> String {
    let response = harness
        .execute_as("query { latestQueueCursor }", CallerScope::Read)
        .await;
    assert!(response.errors.is_empty());
    response.data.into_json().unwrap()["latestQueueCursor"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn wait_for_queue_cursor_change(harness: &TestHarness, previous: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let cursor = latest_queue_cursor(harness).await;
        if cursor != previous {
            return cursor;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "queue replay cursor should advance within 3 seconds"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn queue_snapshots_subscription_emits_snapshot() {
    let h = TestHarness::new().await;

    let request = Request::new(
        "subscription { queueSnapshots { items { id } summary { totalItems } globalState { isPaused } latestCursor } }",
    )
    .data(CallerScope::Read);

    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok(), "subscription should emit within 3 seconds");
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
}

#[tokio::test]
async fn queue_snapshots_include_new_job() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("sub-test").await;

    let request = Request::new(
        "subscription { queueSnapshots { items { id name state } summary { totalItems } } }",
    )
    .data(CallerScope::Read);

    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok());
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    let items = data["queueSnapshots"]["items"].as_array().unwrap();
    assert!(
        !items.is_empty(),
        "queueSnapshots should include the submitted job"
    );
}

#[tokio::test]
async fn queue_snapshots_reflect_pause_state() {
    let h = TestHarness::new().await;

    h.execute("mutation { pauseQueue { success } }").await;

    let request = Request::new("subscription { queueSnapshots { globalState { isPaused } } }")
        .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok());
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    assert!(
        data["queueSnapshots"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn queue_snapshots_reflect_paused_job_item_state() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("pause-job-snapshot").await;

    let pause = h
        .execute(&format!("mutation {{ pauseJob(id: {job_id}) }}"))
        .await;
    assert!(pause.errors.is_empty());

    let request = Request::new("subscription { queueSnapshots { items { id state } } }")
        .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok());
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    let items = data["queueSnapshots"]["items"].as_array().unwrap();
    let paused_item = items
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("queueSnapshots should include the paused job");
    assert_eq!(paused_item["state"].as_str().unwrap(), "PAUSED");
}

#[tokio::test]
async fn queue_snapshots_keep_cached_speed_on_unrelated_events() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("speed-cache-snapshot").await;

    tokio::time::sleep(Duration::from_millis(60)).await;
    h.metrics
        .bytes_downloaded
        .fetch_add(256 * 1024, Ordering::Relaxed);
    h.shared_state.refresh_metrics_snapshot();
    let expected_speed = h.handle.get_metrics().current_download_speed;
    assert!(expected_speed > 0);

    let request = Request::new(
        "subscription { queueSnapshots { items { id state } summary { currentDownloadSpeed } } }",
    )
    .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit an initial snapshot")
        .expect("stream should stay open");
    assert!(first.errors.is_empty());
    let first_data = first.data.into_json().unwrap();
    assert_eq!(
        first_data["queueSnapshots"]["summary"]["currentDownloadSpeed"]
            .as_u64()
            .unwrap(),
        expected_speed
    );

    let pause = h
        .execute(&format!("mutation {{ pauseJob(id: {job_id}) }}"))
        .await;
    assert!(pause.errors.is_empty());

    let second = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit after pause")
        .expect("stream should stay open");
    assert!(second.errors.is_empty());
    let second_data = second.data.into_json().unwrap();
    assert_eq!(
        second_data["queueSnapshots"]["summary"]["currentDownloadSpeed"]
            .as_u64()
            .unwrap(),
        expected_speed
    );
    let items = second_data["queueSnapshots"]["items"].as_array().unwrap();
    let paused_item = items
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("queueSnapshots should include the paused job");
    assert_eq!(paused_item["state"].as_str().unwrap(), "PAUSED");
}

#[tokio::test]
async fn queue_snapshots_drop_cancelled_job_while_globally_paused() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("cancel-paused-subscription").await;

    h.execute("mutation { pauseAll }").await;

    let request =
        Request::new("subscription { queueSnapshots { items { id } globalState { isPaused } } }")
            .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit an initial snapshot")
        .expect("stream should stay open");
    assert!(first.errors.is_empty());
    let first_data = first.data.into_json().unwrap();
    assert!(
        first_data["queueSnapshots"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );
    assert!(
        first_data["queueSnapshots"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"].as_u64() == Some(job_id))
    );

    let cancel = h
        .execute(&format!("mutation {{ cancelJob(id: {job_id}) }}"))
        .await;
    assert!(cancel.errors.is_empty());

    let second = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit after cancellation")
        .expect("stream should stay open");
    assert!(second.errors.is_empty());
    let second_data = second.data.into_json().unwrap();
    assert!(
        second_data["queueSnapshots"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );
    assert!(
        second_data["queueSnapshots"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["id"].as_u64() != Some(job_id)),
        "cancelled job should be removed from the live queue snapshot",
    );
}

#[tokio::test]
async fn queue_snapshot_query_returns_items_and_cursor_together() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("queue-snapshot-query").await;

    let response = h
        .execute_as(
            "query { queueSnapshot { items { id state } latestCursor globalState { isPaused } } latestQueueCursor }",
            CallerScope::Read,
        )
        .await;
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    let items = data["queueSnapshot"]["items"].as_array().unwrap();
    let item = items
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("queueSnapshot query should include the submitted job");
    assert_eq!(item["state"].as_str().unwrap(), "QUEUED");
    assert!(
        data["queueSnapshot"]["latestCursor"].as_str().is_some(),
        "queueSnapshot should include the replay cursor"
    );
    assert_eq!(
        data["queueSnapshot"]["latestCursor"].as_str(),
        data["latestQueueCursor"].as_str()
    );
    assert!(
        !data["queueSnapshot"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn latest_queue_cursor_query_returns_encoded_cursor() {
    let h = TestHarness::new().await;

    let response = h
        .execute_as("query { latestQueueCursor }", CallerScope::Read)
        .await;
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    assert!(
        data["latestQueueCursor"].as_str().is_some(),
        "latestQueueCursor should be encoded as a string"
    );
    assert_eq!(
        data["latestQueueCursor"].as_str(),
        Some(encode_event_cursor(0).as_str())
    );
}

#[tokio::test]
async fn system_metrics_updates_emit_metrics_and_global_state() {
    let h = TestHarness::new().await;

    tokio::time::sleep(Duration::from_millis(60)).await;
    h.metrics
        .bytes_downloaded
        .fetch_add(256 * 1024, Ordering::Relaxed);
    h.shared_state.refresh_metrics_snapshot();
    let expected_speed = h.handle.get_metrics().current_download_speed;
    assert!(expected_speed > 0);

    let request = Request::new(
        "subscription { systemMetricsUpdates { metrics { currentDownloadSpeed } globalState { isPaused } } }",
    )
    .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("metrics subscription should emit an initial snapshot")
        .expect("stream should stay open");
    assert!(first.errors.is_empty());
    let first_data = first.data.into_json().unwrap();
    assert_eq!(
        first_data["systemMetricsUpdates"]["metrics"]["currentDownloadSpeed"]
            .as_u64()
            .unwrap(),
        expected_speed
    );
    assert!(
        !first_data["systemMetricsUpdates"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );

    let pause = h.execute("mutation { pauseQueue { success } }").await;
    assert!(pause.errors.is_empty());

    let second = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("metrics subscription should emit after cadence tick")
        .expect("stream should stay open");
    assert!(second.errors.is_empty());
    let second_data = second.data.into_json().unwrap();
    assert!(
        second_data["systemMetricsUpdates"]["globalState"]["isPaused"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn queue_events_replay_buffered_state_change_after_cursor() {
    let h = TestHarness::new().await;
    let item_id = h.submit_test_nzb("event-test").await;
    assert!(h
        .db
        .list_integration_events_after(None, None, None)
        .unwrap()
        .is_empty());

    let cursor_before = latest_queue_cursor(&h).await;

    let pause = h
        .execute(&format!(
            "mutation {{ pauseQueueItem(id: {item_id}) {{ success }} }}"
        ))
        .await;
    assert!(pause.errors.is_empty());

    let expected_cursor = wait_for_queue_cursor_change(&h, &cursor_before).await;

    let request = Request::new(format!(
        r#"subscription {{
            queueEvents(after: "{}") {{
                cursor
                kind
                itemId
                state
                item {{ id state }}
            }}
        }}"#,
        cursor_before
    ))
    .data(CallerScope::Read);

    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok(), "queueEvents should replay the buffered state change");
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    assert_eq!(
        data["queueEvents"]["kind"].as_str().unwrap(),
        "ITEM_STATE_CHANGED"
    );
    assert_eq!(data["queueEvents"]["itemId"].as_u64().unwrap(), item_id);
    assert_eq!(data["queueEvents"]["state"].as_str().unwrap(), "PAUSED");
    assert_eq!(
        data["queueEvents"]["cursor"].as_str(),
        Some(expected_cursor.as_str())
    );
    assert!(h
        .db
        .list_integration_events_after(None, None, None)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn queue_events_reject_malformed_cursor() {
    let h = TestHarness::new().await;

    let request = Request::new(
        r#"subscription {
            queueEvents(after: "not-base64") {
                kind
            }
        }"#,
    )
    .data(CallerScope::Read);

    let mut stream = h.schema.execute_stream(request);
    let response = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should fail immediately")
        .expect("stream should yield an error response");

    assert!(!response.errors.is_empty());
    let code = response.errors[0]
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get("code"));
    assert!(matches!(
        code,
        Some(async_graphql::Value::String(value)) if value.as_str() == "CURSOR_INVALID"
    ));
}

#[tokio::test]
async fn job_detail_updates_emit_live_state_changes() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("job-detail-updates").await;

    let request = Request::new(format!(
        "subscription {{ jobDetailUpdates(jobId: {job_id}) {{ queueItem {{ id state }} jobTimeline {{ outcome }} jobEvents {{ kind }} }} }}"
    ))
    .data(CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);

    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit an initial snapshot")
        .expect("stream should stay open");
    assert!(first.errors.is_empty());
    let first_data = first.data.into_json().unwrap();
    assert_eq!(
        first_data["jobDetailUpdates"]["queueItem"]["id"].as_u64(),
        Some(job_id)
    );
    assert_eq!(
        first_data["jobDetailUpdates"]["queueItem"]["state"].as_str(),
        Some("QUEUED")
    );
    assert!(
        first_data["jobDetailUpdates"]["jobEvents"]
            .as_array()
            .is_some()
    );

    let pause = h
        .execute(&format!("mutation {{ pauseJob(id: {job_id}) }}"))
        .await;
    assert!(pause.errors.is_empty());

    let second = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription should emit after pause")
        .expect("stream should stay open");
    assert!(second.errors.is_empty());
    let second_data = second.data.into_json().unwrap();
    assert_eq!(
        second_data["jobDetailUpdates"]["queueItem"]["state"].as_str(),
        Some("PAUSED")
    );
}
