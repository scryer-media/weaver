mod common;

#[test]
fn schedule_conversion_refuses_missing_action_parameters_without_fallbacks() {
    use async_graphql::{InputType, value};
    use weaver_server_api::settings::types::ScheduleInput;

    for action in [
        "set_server_active",
        "set_quota_metering",
        "hardware_profile",
        "unknown_action",
    ] {
        let input = ScheduleInput::parse(Some(value!({ "time": "08:00", "actionType": action })))
            .unwrap_or_else(|_| panic!("failed to parse schedule test input"));
        assert!(
            input.into_entry().is_err(),
            "converted {action} without its required parameters"
        );
    }
}

#[tokio::test]
async fn rss_targets_are_validated_and_feed_deletion_removes_only_targeted_rules() {
    use std::sync::Arc;
    use weaver_server_core::bandwidth::{ScheduleAction, schedule::SharedSchedules};

    let h = common::TestHarness::new().await;
    let schedules: SharedSchedules = Arc::new(tokio::sync::RwLock::new(Vec::new()));
    let request = |query: String| {
        async_graphql::Request::new(query)
            .data(weaver_server_core::auth::CallerScope::Local)
            .data(schedules.clone())
    };
    let invalid = h.schema.execute(request(r#"mutation { createSchedule(input: { enabled: false, time: "08:00", actionType: "fetch_rss", feedId: 123456 }) { id } }"#.into())).await;
    assert!(
        invalid
            .errors
            .iter()
            .any(|error| error.message.contains("RSS feed 123456 not found"))
    );
    let feed = h.execute(r#"mutation { addRssFeed(input: { name: "scheduled feed", url: "https://example.com/rss", enabled: false }) { id } }"#).await;
    common::assert_no_errors(&feed);
    let id = common::response_data(&feed)["addRssFeed"]["id"]
        .as_u64()
        .unwrap();
    for target in [format!(", feedId: {id}"), String::new()] {
        let response = h.schema.execute(request(format!(r#"mutation {{ createSchedule(input: {{ enabled: false, time: "08:00", actionType: "fetch_rss" {target} }}) {{ id }} }}"#))).await;
        common::assert_no_errors(&response);
    }
    let (deleted, added) = tokio::join!(
        h.schema.execute(request(format!("mutation {{ deleteRssFeed(id: {id}) }}"))),
        h.schema.execute(request(r#"mutation { createSchedule(input: { enabled: false, time: "09:00", actionType: "pause" }) { id } }"#.into())),
    );
    common::assert_no_errors(&deleted);
    common::assert_no_errors(&added);
    let persisted = h.db.list_schedules().unwrap();
    assert_eq!(persisted.len(), 2);
    assert!(
        persisted
            .iter()
            .any(|entry| entry.action == ScheduleAction::FetchRss { feed_id: None })
    );
    assert!(
        persisted
            .iter()
            .any(|entry| entry.action == ScheduleAction::Pause)
    );
    assert_eq!(*schedules.read().await, persisted);
}

#[tokio::test]
async fn concurrent_schedule_creates_preserve_every_rule_and_runtime_publication() {
    use std::sync::Arc;
    use weaver_server_core::bandwidth::schedule::SharedSchedules;

    let h = common::TestHarness::new().await;
    let schedules: SharedSchedules = Arc::new(tokio::sync::RwLock::new(Vec::new()));
    let mut tasks = tokio::task::JoinSet::new();
    for number in 0..12 {
        let schema = h.schema.clone();
        let schedules = schedules.clone();
        tasks.spawn(async move {
            schema
                .execute(
                    async_graphql::Request::new(format!(
                        r#"mutation {{ createSchedule(input: {{ enabled: false, label: "rule-{number}", time: "08:00", actionType: "pause" }}) {{ id }} }}"#
                    ))
                    .data(weaver_server_core::auth::CallerScope::Local)
                    .data(schedules),
                )
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        common::assert_no_errors(&result.unwrap());
    }
    let persisted = h.db.list_schedules().unwrap();
    assert_eq!(persisted.len(), 12);
    assert_eq!(*schedules.read().await, persisted);
}

#[tokio::test]
async fn server_deletion_and_schedule_creation_publish_the_same_rule_set() {
    use std::sync::Arc;
    use weaver_server_core::bandwidth::schedule::SharedSchedules;

    let h = common::TestHarness::new().await;
    let schedules: SharedSchedules = Arc::new(tokio::sync::RwLock::new(Vec::new()));
    let response = h.execute(r#"mutation {
        addServer(input: { host: "news.example.com", port: 119, tls: false, connections: 1, active: false }) { id }
    }"#).await;
    common::assert_no_errors(&response);
    let id = common::response_data(&response)["addServer"]["id"]
        .as_u64()
        .unwrap();
    let request = |query: String| {
        async_graphql::Request::new(query)
            .data(weaver_server_core::auth::CallerScope::Local)
            .data(schedules.clone())
    };
    let response = h.schema.execute(request(format!(r#"mutation {{
        createSchedule(input: {{ enabled: false, time: "08:00", actionType: "set_server_active", serverId: {id}, serverActive: false }}) {{ id }}
    }}"#))).await;
    common::assert_no_errors(&response);
    let (removed, created) = tokio::join!(
        h.schema.execute(request(format!(
            "mutation {{ removeServer(id: {id}) {{ id }} }}"
        ))),
        h.schema.execute(request(
            r#"mutation { createSchedule(input: { enabled: false, time: "09:00", actionType: "pause" }) { id } }"#.into()
        )),
    );
    common::assert_no_errors(&removed);
    common::assert_no_errors(&created);
    let persisted = h.db.list_schedules().unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(
        persisted[0].action,
        weaver_server_core::bandwidth::ScheduleAction::Pause
    );
    assert_eq!(*schedules.read().await, persisted);
}

#[tokio::test]
async fn one_shot_and_hourly_inputs_validate_and_update_every_field() {
    let h = common::TestHarness::new().await;
    for input in [
        r#"{ time: "08:00", actionType: "pause", everyHourAtMinute: 10 }"#,
        r#"{ time: "08:00", actionType: "fetch_rss", everyHourAtMinute: 60 }"#,
        r#"{ time: "08:00", actionType: "set_server_active" }"#,
        r#"{ time: "08:00", actionType: "set_quota_metering" }"#,
        r#"{ time: "08:00", actionType: "prune_history" }"#,
    ] {
        let response = h
            .execute(&format!(
                "mutation {{ createSchedule(input: {input}) {{ id }} }}"
            ))
            .await;
        assert!(
            !response.errors.is_empty(),
            "accepted invalid input {input}"
        );
    }
    let response = h
        .execute(
            r#"mutation { createSchedule(input: {
        enabled: false, time: "08:00", times: ["09:00", "17:00"], actionType: "fetch_rss"
    }) { id time times track } }"#,
        )
        .await;
    common::assert_no_errors(&response);
    let data = common::response_data(&response);
    let rule = &data["createSchedule"][0];
    assert_eq!(rule["time"], "09:00");
    assert_eq!(rule["track"], "ONE_SHOT");
    let id = rule["id"].as_str().unwrap();
    let response = h.execute(&format!(r#"mutation {{ updateSchedule(id: "{id}", input: {{
        enabled: false, time: "00:00", everyHourAtMinute: 15, actionType: "prune_history",
        pruneFailed: {{ deleteFiles: true }}, pruneCompleted: {{ deleteFiles: false }}
    }}) {{ id times everyHourAtMinute actionType pruneFailed {{ deleteFiles }} pruneCompleted {{ deleteFiles }} }} }}"#)).await;
    common::assert_no_errors(&response);
    let data = common::response_data(&response);
    let rule = &data["updateSchedule"][0];
    assert_eq!(rule["id"], id);
    assert_eq!(rule["everyHourAtMinute"], 15);
    assert_eq!(rule["times"], serde_json::json!([]));
    assert_eq!(rule["pruneCompleted"]["deleteFiles"], false);
}

use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_core::auth::CallerScope;

fn assert_forbidden(resp: &async_graphql::Response) {
    assert!(
        format!("{:?}", resp.errors).contains("FORBIDDEN"),
        "expected FORBIDDEN error, got {:?}",
        resp.errors
    );
}

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
async fn unknown_schedule_action_is_refused_without_saving_a_resume() {
    let h = TestHarness::new().await;
    let response = h
        .execute(
            r#"mutation {
        createSchedule(input: { time: "08:00", actionType: "typo" }) { id }
    }"#,
        )
        .await;
    assert!(
        response
            .errors
            .iter()
            .any(|error| error.message.contains("unknown schedule actionType"))
    );
    let response = h.execute("{ schedules { id } }").await;
    assert_no_errors(&response);
    assert!(
        response_data(&response)["schedules"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn schedules_require_admin_scope() {
    let h = TestHarness::new().await;
    let query = "{ schedules { id } }";

    assert_forbidden(&h.execute_as(query, CallerScope::Read).await);
    assert_forbidden(&h.execute_as(query, CallerScope::Control).await);

    let resp = h.execute_as(query, CallerScope::Admin).await;
    assert_no_errors(&resp);
}

#[tokio::test]
async fn configured_speed_limit_is_a_distinct_speed_track_action() {
    let h = TestHarness::new().await;
    let response = h.execute(r#"mutation {
        createSchedule(input: { enabled: false, time: "08:00", actionType: "configured_speed_limit" }) {
            actionType track speedLimitBytes
        }
    }"#).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let schedule = &data["createSchedule"][0];
    assert_eq!(schedule["actionType"], "configured_speed_limit");
    assert_eq!(schedule["track"], "SPEED");
    assert!(schedule["speedLimitBytes"].is_null());
}

#[tokio::test]
async fn schedule_mutations_require_admin_scope() {
    let h = TestHarness::new().await;
    let mutation = r#"mutation {
        createSchedule(input: {
            enabled: true,
            label: "Admin only",
            days: [],
            time: "08:00",
            actionType: "pause"
        }) { id }
    }"#;

    assert_forbidden(&h.execute_as(mutation, CallerScope::Read).await);
    assert_forbidden(&h.execute_as(mutation, CallerScope::Control).await);

    let resp = h.execute_as(mutation, CallerScope::Admin).await;
    assert_no_errors(&resp);
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
async fn create_watch_folder_scanning_schedules() {
    let h = TestHarness::new().await;

    for (label, action_type) in [
        ("Pause watcher", "pause_watch_folder_scanning"),
        ("Resume watcher", "resume_watch_folder_scanning"),
    ] {
        let resp = h
            .execute(&format!(
                r#"mutation {{
                    createSchedule(input: {{
                        enabled: true,
                        label: "{label}",
                        days: [],
                        time: "08:00",
                        actionType: "{action_type}"
                    }}) {{
                        label actionType speedLimitBytes
                    }}
                }}"#
            ))
            .await;
        assert_no_errors(&resp);
        let data = response_data(&resp);
        let schedules = data["createSchedule"].as_array().unwrap();
        let sched = schedules
            .iter()
            .find(|schedule| schedule["label"].as_str().unwrap() == label)
            .unwrap();
        assert_eq!(sched["actionType"].as_str().unwrap(), action_type);
        assert!(sched["speedLimitBytes"].is_null());
    }
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
    let sched = schedules
        .iter()
        .find(|s| s["id"].as_str().unwrap() == id)
        .unwrap();
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
        .execute(&format!(
            r#"mutation {{ deleteSchedule(id: "{id}") {{ id }} }}"#
        ))
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
    let sched = schedules
        .iter()
        .find(|s| s["id"].as_str().unwrap() == id)
        .unwrap();
    assert!(!sched["enabled"].as_bool().unwrap());
}

#[tokio::test]
async fn create_hardware_profile_schedule() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    enabled: true,
                    label: "Evening",
                    days: [],
                    time: "17:00",
                    actionType: "hardware_profile",
                    hardwareProfile: EFFICIENT
                }) {
                    actionType speedLimitBytes hardwareProfile
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let sched = &data["createSchedule"].as_array().unwrap()[0];
    assert_eq!(sched["actionType"].as_str().unwrap(), "hardware_profile");
    assert_eq!(sched["hardwareProfile"].as_str().unwrap(), "EFFICIENT");
    assert!(sched["speedLimitBytes"].is_null());

    let resp = h
        .execute("{ schedules { actionType hardwareProfile } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["schedules"][0]["hardwareProfile"].as_str().unwrap(),
        "EFFICIENT"
    );
}

#[tokio::test]
async fn other_schedule_actions_carry_no_hardware_profile() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    time: "08:00",
                    actionType: "pause",
                    hardwareProfile: EFFICIENT
                }) {
                    actionType hardwareProfile track
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let sched = &data["createSchedule"].as_array().unwrap()[0];
    assert_eq!(sched["actionType"].as_str().unwrap(), "pause");
    assert_eq!(sched["track"].as_str().unwrap(), "DOWNLOADS");
    assert!(sched["hardwareProfile"].is_null());
}

#[tokio::test]
async fn a_hardware_profile_schedule_without_a_profile_is_refused() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    time: "17:00",
                    actionType: "hardware_profile"
                }) { id }
            }"#,
        )
        .await;
    let message = resp
        .errors
        .first()
        .map(|error| error.message.clone())
        .expect("a profile rule without a profile must be refused");
    assert!(message.contains("hardwareProfile"), "{message}");
    assert!(h.db.list_schedules().unwrap().is_empty());
}

#[tokio::test]
async fn a_hardware_profile_the_machine_cannot_honour_is_refused_in_a_schedule() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    time: "23:00",
                    actionType: "hardware_profile",
                    hardwareProfile: PERFORMANCE
                }) { id }
            }"#,
        )
        .await;
    let message = resp
        .errors
        .first()
        .map(|error| error.message.clone())
        .expect("an unavailable profile must be refused");
    // The harness machine has 8 GiB; performance needs 16 GiB.
    assert!(
        message.contains("performance") && message.contains("16 GiB"),
        "{message}"
    );
    assert!(h.db.list_schedules().unwrap().is_empty());

    // An update is judged the same way and leaves the rule as it was.
    let resp = h
        .execute(
            r#"mutation {
                createSchedule(input: {
                    time: "23:00",
                    actionType: "hardware_profile",
                    hardwareProfile: BALANCED
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["createSchedule"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateSchedule(id: "{id}", input: {{
                    time: "23:00",
                    actionType: "hardware_profile",
                    hardwareProfile: PERFORMANCE
                }}) {{ id }}
            }}"#
        ))
        .await;
    assert!(!resp.errors.is_empty(), "the update must be refused");
    let resp = h.execute("{ schedules { hardwareProfile } }").await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["schedules"][0]["hardwareProfile"]
            .as_str()
            .unwrap(),
        "BALANCED"
    );
}
