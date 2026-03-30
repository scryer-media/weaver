mod common;

use std::time::Duration;

use async_graphql::Request;
use common::TestHarness;
use tokio_stream::StreamExt;
use weaver_api::auth::CallerScope;
use weaver_api::{
    PersistedQueueEvent, QueueEventKind, QueueItemState, encode_event_cursor, queue_item_from_job,
};
use weaver_core::id::JobId;
use weaver_state::IntegrationEventRow;

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
