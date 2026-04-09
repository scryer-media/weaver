mod common;

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
