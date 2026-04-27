mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use async_graphql::Request;
use common::TestHarness;
use tokio_stream::StreamExt;
use weaver_server_api::auth::CallerScope;
use weaver_server_api::{
    PersistedQueueEvent, QueueEventKind, QueueItemState, encode_event_cursor, queue_item_from_job,
};
use weaver_server_core::IntegrationEventRow;
use weaver_server_core::jobs::ids::JobId;

fn persist_event(
    h: &TestHarness,
    item_id: u64,
    kind: QueueEventKind,
    state: QueueItemState,
) -> String {
    let info = h.handle.get_job(JobId(item_id)).expect("job should exist");
    let occurred_at_ms = chrono::Utc::now().timestamp_millis();
    let payload = PersistedQueueEvent {
        occurred_at_ms,
        kind,
        item_id: Some(item_id),
        item: Some(queue_item_from_job(&info)),
        state: Some(state),
        previous_state: None,
        attention: None,
        global_state: None,
    };
    h.db.insert_integration_events(&[IntegrationEventRow {
        id: 0,
        timestamp: occurred_at_ms,
        kind: format!("{kind:?}"),
        item_id: Some(item_id),
        payload_json: serde_json::to_string(&payload).unwrap(),
    }])
    .unwrap();
    let latest = h.db.latest_integration_event_id().unwrap().unwrap();
    encode_event_cursor(latest)
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
            "query { queueSnapshot { items { id state } latestCursor globalState { isPaused } } }",
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
async fn queue_events_replay_from_cursor_then_tail() {
    let h = TestHarness::new().await;
    let item_id = h.submit_test_nzb("event-test").await;

    let after = persist_event(
        &h,
        item_id,
        QueueEventKind::ItemCreated,
        QueueItemState::Queued,
    );
    persist_event(
        &h,
        item_id,
        QueueEventKind::ItemStateChanged,
        QueueItemState::Downloading,
    );

    let request = Request::new(format!(
        r#"subscription {{
            queueEvents(after: "{after}") {{
                cursor
                kind
                itemId
                state
                item {{ id state }}
            }}
        }}"#
    ))
    .data(CallerScope::Read);

    let mut stream = h.schema.execute_stream(request);

    let item = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    assert!(item.is_ok(), "queueEvents should replay after cursor");
    let response = item.unwrap().unwrap();
    assert!(response.errors.is_empty());
    let data = response.data.into_json().unwrap();
    assert_eq!(
        data["queueEvents"]["kind"].as_str().unwrap(),
        "ITEM_STATE_CHANGED"
    );
    assert_eq!(data["queueEvents"]["itemId"].as_u64().unwrap(), item_id);
    assert_eq!(
        data["queueEvents"]["state"].as_str().unwrap(),
        "DOWNLOADING"
    );
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
    assert!(first_data["jobDetailUpdates"]["jobEvents"].as_array().is_some());

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
