mod common;

use common::{TestHarness, assert_no_errors, response_data};

#[tokio::test]
async fn list_schedules_empty() {
    let h = TestHarness::new().await;
    let resp = h
        .execute("{ schedules { id enabled label days time actionType speedLimitBytes } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["schedules"].as_array().unwrap();
    assert!(schedules.is_empty());
}

#[tokio::test]
async fn create_schedule_pause() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "Night pause",
                    days: ["mon", "tue"],
                    time: "22:00",
                    actionType: "pause"
                }) {
                    id enabled label days time actionType speedLimitBytes
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["createSchedule"].as_array().unwrap();
    assert_eq!(schedules.len(), 1);
    let sched = &schedules[0];
    assert!(sched["enabled"].as_bool().unwrap());
    assert_eq!(sched["label"].as_str().unwrap(), "Night pause");
    assert_eq!(sched["actionType"].as_str().unwrap(), "pause");
    assert_eq!(sched["time"].as_str().unwrap(), "22:00");
    let days = sched["days"].as_array().unwrap();
    assert_eq!(days.len(), 2);
}

#[tokio::test]
async fn create_schedule_speed_limit() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "Daytime limit",
                    days: ["wed"],
                    time: "08:00",
                    actionType: "speed_limit",
                    speedLimitBytes: 1048576
                }) {
                    id enabled label actionType speedLimitBytes
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["createSchedule"].as_array().unwrap();
    assert_eq!(schedules.len(), 1);
    let sched = &schedules[0];
    assert_eq!(sched["actionType"].as_str().unwrap(), "speed_limit");
    assert_eq!(sched["speedLimitBytes"].as_u64().unwrap(), 1_048_576);
}

#[tokio::test]
async fn update_schedule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "Original",
                    days: [],
                    time: "08:00",
                    actionType: "pause"
                }) {
                    id label
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let id = data["createSchedule"].as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateSchedule(id: "{id}", input: {{
                    enabled: true,
                    label: "Updated",
                    days: [],
                    time: "09:00",
                    actionType: "pause"
                }}) {{
                    id label time
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["updateSchedule"].as_array().unwrap();
    let sched = schedules.iter().find(|s| s["id"].as_str().unwrap() == id).unwrap();
    assert_eq!(sched["label"].as_str().unwrap(), "Updated");
    assert_eq!(sched["time"].as_str().unwrap(), "09:00");
}

#[tokio::test]
async fn delete_schedule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "To delete",
                    days: [],
                    time: "08:00",
                    actionType: "pause"
                }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let id = data["createSchedule"].as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = h
        .execute(&format!(r#"mutation {{ deleteSchedule(id: "{id}") {{ id }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["deleteSchedule"].as_array().unwrap();
    assert!(schedules.is_empty());
}

#[tokio::test]
async fn toggle_schedule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "Toggle me",
                    days: [],
                    time: "08:00",
                    actionType: "pause"
                }) {
                    id enabled
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["createSchedule"].as_array().unwrap();
    let id = schedules[0]["id"].as_str().unwrap().to_string();
    assert!(schedules[0]["enabled"].as_bool().unwrap());

    let resp = h
        .execute(&format!(
            r#"mutation {{ toggleSchedule(id: "{id}", enabled: false) {{ id enabled }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let schedules = data["toggleSchedule"].as_array().unwrap();
    let sched = schedules.iter().find(|s| s["id"].as_str().unwrap() == id).unwrap();
    assert!(!sched["enabled"].as_bool().unwrap());
}
