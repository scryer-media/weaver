use super::*;

#[test]
fn insert_and_list_events_after_cursor() {
    let db = Database::open_in_memory().unwrap();
    let events = vec![
        IntegrationEventRow {
            id: 0,
            timestamp: 1000,
            kind: "ITEM_CREATED".to_string(),
            item_id: Some(1),
            payload_json: "{\"kind\":\"ITEM_CREATED\"}".to_string(),
        },
        IntegrationEventRow {
            id: 0,
            timestamp: 1001,
            kind: "ITEM_PROGRESS".to_string(),
            item_id: Some(1),
            payload_json: "{\"kind\":\"ITEM_PROGRESS\"}".to_string(),
        },
        IntegrationEventRow {
            id: 0,
            timestamp: 1002,
            kind: "GLOBAL_STATE_CHANGED".to_string(),
            item_id: None,
            payload_json: "{\"kind\":\"GLOBAL_STATE_CHANGED\"}".to_string(),
        },
    ];

    db.insert_integration_events(&events).unwrap();

    let listed = db
        .list_integration_events_after(None, Some(1), None)
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].kind, "ITEM_CREATED");
    assert_eq!(listed[1].kind, "ITEM_PROGRESS");

    let latest = db.latest_integration_event_id().unwrap();
    assert!(latest.is_some());
    let after_latest = db
        .list_integration_events_after(latest, None, None)
        .unwrap();
    assert!(after_latest.is_empty());

    let limited = db
        .list_integration_events_after(None, None, Some(1))
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].kind, "ITEM_CREATED");
}
