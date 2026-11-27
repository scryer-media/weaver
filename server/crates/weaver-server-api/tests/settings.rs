// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde_json::json;
use weaver_server_api::settings::appearance::AppearanceStore;
use weaver_server_core::post_processing::settings::PostProcessingSettingsQueries;
use weaver_server_core::settings::SettingsSchedulesQueries;
use weaver_server_core::settings::queries::SettingsQueries;
use weaver_server_core::settings::service::SettingsServiceQueries;
mod common;

use common::{
    BlockingDbOperation, TestHarness, assert_has_errors, assert_no_errors, local_request,
    response_data,
};
use weaver_server_core::auth::CallerScope;

#[tokio::test]
async fn nzb_limit_defaults_to_ten_and_accepts_only_positive_integers() {
    let h = TestHarness::new().await;
    let response = h.execute("{ settings { maxConcurrentNzbs } }").await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["settings"]["maxConcurrentNzbs"],
        10
    );
    for value in ["0", "-1", "1.5", "2147483648"] {
        let response = h.execute(&format!(
            "mutation {{ updateSettings(input: {{ maxConcurrentNzbs: {value} }}) {{ maxConcurrentNzbs }} }}"
        )).await;
        assert!(!response.errors.is_empty(), "{value} must be rejected");
        assert_eq!(h.db.load_config().unwrap().max_concurrent_nzbs(), 10);
    }
    let response = h
        .execute(
            "mutation { updateSettings(input: { maxConcurrentNzbs: 3 }) { maxConcurrentNzbs } }",
        )
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["updateSettings"]["maxConcurrentNzbs"],
        3
    );
    assert_eq!(h.db.load_config().unwrap().max_concurrent_nzbs(), 3);
    let response = h
        .execute("mutation { updateSettings(input: { maxRetries: 4 }) { maxConcurrentNzbs } }")
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["updateSettings"]["maxConcurrentNzbs"],
        3
    );
}

#[tokio::test]
async fn history_retention_defaults_to_ninety_days_and_takes_zero_to_36500() {
    let h = TestHarness::new().await;
    let response = h.execute("{ settings { historyRetentionDays } }").await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["settings"]["historyRetentionDays"],
        90
    );
    for value in ["-1", "36501", "1.5"] {
        let response = h
            .execute(&format!(
                "mutation {{ updateSettings(input: {{ historyRetentionDays: {value} }}) {{ historyRetentionDays }} }}"
            ))
            .await;
        assert!(!response.errors.is_empty(), "{value} must be rejected");
        assert_eq!(h.db.load_config().unwrap().history_retention_days(), 90);
    }
    for days in [0, 36500, 30] {
        let response = h
            .execute(&format!(
                "mutation {{ updateSettings(input: {{ historyRetentionDays: {days} }}) {{ historyRetentionDays }} }}"
            ))
            .await;
        assert_no_errors(&response);
        assert_eq!(
            response_data(&response)["updateSettings"]["historyRetentionDays"],
            days
        );
        assert_eq!(h.db.load_config().unwrap().history_retention_days(), days);
    }
    // A save that leaves it out keeps it.
    let response = h
        .execute("mutation { updateSettings(input: { maxRetries: 4 }) { historyRetentionDays } }")
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["updateSettings"]["historyRetentionDays"],
        30
    );
    let response = h.execute("{ settings { historyRetentionDays } }").await;
    assert_eq!(
        response_data(&response)["settings"]["historyRetentionDays"],
        30
    );
}

#[tokio::test]
async fn create_schedule_refuses_a_client_supplied_id_and_generates_one() {
    let h = TestHarness::new().await;
    let data_dir = std::path::PathBuf::from(&h.config.read().await.data_dir);
    h.db.initialize_post_processing_script_directory(&data_dir, None)
        .unwrap();
    let rejected = h.execute(r#"mutation {
        createSchedule(input: { id: "implicit-script:task.sh:12:00", time: "12:00", actionType: "pause" }) { id }
    }"#).await;
    assert!(
        rejected.errors.iter().any(|error| error
            .message
            .contains("unknown field \"id\" of type \"ScheduleInput\"")),
        "unexpected schema errors: {:?}",
        rejected.errors
    );
    assert!(h.db.list_schedules().unwrap().is_empty());

    let created = h.execute(r#"mutation {
        createSchedule(input: { label: "implicit-script:display-only", time: "12:00", actionType: "pause" }) { id label }
    }"#).await;
    assert_no_errors(&created);
    let data = response_data(&created);
    let entry = &data["createSchedule"][0];
    assert!(entry["id"].as_str().unwrap().starts_with("sched-"));
    assert_eq!(entry["label"], "implicit-script:display-only");
    assert_eq!(
        h.db.list_schedules().unwrap()[0].id,
        entry["id"].as_str().unwrap()
    );
}

#[tokio::test]
async fn the_schedules_screen_takes_no_script_rules() {
    let h = TestHarness::new().await;
    for refused in [
        r#"time: "03:30", actionType: "run_script""#,
        r#"time: "03:30", actionType: "run_script", instanceId: "nope""#,
    ] {
        let response = h
            .execute(&format!(
                "mutation {{ createSchedule(input: {{ {refused} }}) {{ id }} }}"
            ))
            .await;
        assert!(
            !response.errors.is_empty(),
            "{refused} was saved: {:?}",
            response.data
        );
        assert!(h.db.list_schedules().unwrap().is_empty());
    }
}

#[tokio::test]
async fn archive_password_settings_never_return_passwords_and_support_replace_clear() {
    let h = TestHarness::new().await;
    let saved = h.execute(r#"mutation { updateArchivePasswordSettings(passwords: ["synthetic-key", "enc:v1:literal"], passwordFile: "/fixture/passwords") { hasPasswords passwordFile } }"#).await;
    assert_no_errors(&saved);
    assert_eq!(
        response_data(&saved)["updateArchivePasswordSettings"]["hasPasswords"],
        true
    );
    assert!(!response_data(&saved).to_string().contains("synthetic-key"));
    assert!(
        !h.db
            .get_setting("archive_passwords")
            .unwrap()
            .unwrap()
            .contains("synthetic-key")
    );
    let kept = h.execute(r#"mutation { updateArchivePasswordSettings(passwordFile: null) { hasPasswords passwordFile } }"#).await;
    assert_no_errors(&kept);
    assert_eq!(
        response_data(&kept)["updateArchivePasswordSettings"]["hasPasswords"],
        true
    );
    assert!(response_data(&kept)["updateArchivePasswordSettings"]["passwordFile"].is_null());
    let cleared = h
        .execute(r#"mutation { updateArchivePasswordSettings(passwords: []) { hasPasswords } }"#)
        .await;
    assert_no_errors(&cleared);
    assert_eq!(
        response_data(&cleared)["updateArchivePasswordSettings"]["hasPasswords"],
        false
    );
}

#[tokio::test]
async fn archive_password_settings_require_admin() {
    let h = TestHarness::new().await;
    for scope in [CallerScope::Read, CallerScope::Control] {
        for query in [
            "{ archivePasswordSettings { hasPasswords passwordFile } }",
            "mutation { updateArchivePasswordSettings(passwords: [\"synthetic\"]) { hasPasswords } }",
        ] {
            assert!(!h.execute_as(query, scope).await.errors.is_empty());
        }
    }
}

#[tokio::test]
async fn web_appearance_requires_admin_for_reads_and_writes() {
    let h = TestHarness::new().await;
    let query = "{ webAppearance { palette mode accent } }";
    let mutation = "mutation { updateWebAppearance(input: { palette: AMBER }) { palette } }";

    for scope in [CallerScope::Read, CallerScope::Control] {
        for operation in [query, mutation] {
            let response = h.execute_as(operation, scope).await;
            assert_eq!(response.errors.len(), 1);
            assert_eq!(
                response.errors[0]
                    .extensions
                    .as_ref()
                    .and_then(|extensions| extensions.get("code")),
                Some(&async_graphql::Value::String("FORBIDDEN".into()))
            );
        }
    }

    let response = h.execute_as(query, CallerScope::Admin).await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["webAppearance"]["palette"],
        "WEAVER"
    );
}

#[tokio::test]
async fn web_appearance_defaults_and_persists_partial_updates_and_reset() {
    let h = TestHarness::new().await;
    // Existing installations have their settings but no saved appearance.
    let defaults = json!({ "palette": "WEAVER", "mode": "SYSTEM", "accent": null });
    assert_eq!(saved_appearance(&h), defaults);

    let query = "{ webAppearance { palette mode accent } }";
    let response = h.execute_as(query, CallerScope::Admin).await;
    assert_no_errors(&response);
    assert_eq!(response_data(&response)["webAppearance"], defaults);

    for (input, expected) in [
        (
            "palette: PHOSPHOR, mode: LIGHT, accent: VIOLET",
            json!({ "palette": "PHOSPHOR", "mode": "LIGHT", "accent": "VIOLET" }),
        ),
        (
            "mode: SYSTEM",
            json!({ "palette": "PHOSPHOR", "mode": "SYSTEM", "accent": "VIOLET" }),
        ),
        (
            "accent: null",
            json!({ "palette": "PHOSPHOR", "mode": "SYSTEM", "accent": null }),
        ),
        (
            "palette: TERMINAL",
            json!({ "palette": "TERMINAL", "mode": "SYSTEM", "accent": null }),
        ),
        (
            "palette: WEAVER, mode: SYSTEM, accent: null",
            defaults.clone(),
        ),
    ] {
        let mutation = format!(
            "mutation {{ updateWebAppearance(input: {{ {input} }}) {{ palette mode accent }} }}"
        );
        let updated = h.execute_as(&mutation, CallerScope::Admin).await;
        assert_no_errors(&updated);
        assert_eq!(response_data(&updated)["updateWebAppearance"], expected);

        let fetched = h.execute_as(query, CallerScope::Admin).await;
        assert_no_errors(&fetched);
        assert_eq!(response_data(&fetched)["webAppearance"], expected);
        assert_eq!(saved_appearance(&h), expected);
    }

    for invalid in [
        "palette: UNKNOWN",
        "palette: AMBER, mode: UNKNOWN",
        "palette: AMBER, accent: UNKNOWN",
    ] {
        let mutation =
            format!("mutation {{ updateWebAppearance(input: {{ {invalid} }}) {{ palette }} }}");
        assert!(
            !h.execute_as(&mutation, CallerScope::Admin)
                .await
                .errors
                .is_empty()
        );
        assert_eq!(saved_appearance(&h), defaults);
    }
}

/// The saved palette, mode and accent, as the GraphQL fields read them.
fn saved_appearance(h: &TestHarness) -> serde_json::Value {
    let saved = serde_json::to_value(AppearanceStore::load(&h.db).unwrap().current()).unwrap();
    json!({ "palette": saved["palette"], "mode": saved["mode"], "accent": saved["accent"] })
}

#[tokio::test]
async fn web_appearance_keeps_progress_colours_per_palette() {
    let h = TestHarness::new().await;
    let query = "{ webAppearance { palette progressColors { palette stage color } } }";
    let response = h.execute_as(query, CallerScope::Admin).await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["webAppearance"]["progressColors"],
        json!([])
    );

    let save = |changes: &str| {
        format!(
            "mutation {{ updateWebAppearance(input: {{ progressColors: [{changes}] }}) \
             {{ palette progressColors {{ palette stage color }} }} }}"
        )
    };
    // Picks for two palettes, given out of order, come back ordered by palette then stage.
    let picked = h
        .execute_as(
            &save(
                "{ palette: TERMINAL, stage: COMPLETED, color: GREY }, \
                 { palette: WEAVER, stage: FAILED, color: MAGENTA }, \
                 { palette: WEAVER, stage: DOWNLOADING, color: CYAN }",
            ),
            CallerScope::Admin,
        )
        .await;
    assert_no_errors(&picked);
    let weaver_and_terminal = json!([
        { "palette": "WEAVER", "stage": "DOWNLOADING", "color": "CYAN" },
        { "palette": "WEAVER", "stage": "FAILED", "color": "MAGENTA" },
        { "palette": "TERMINAL", "stage": "COMPLETED", "color": "GREY" },
    ]);
    assert_eq!(
        response_data(&picked)["updateWebAppearance"]["progressColors"],
        weaver_and_terminal
    );

    // Changing the palette keeps every palette's picks.
    let switched = h
        .execute_as(
            "mutation { updateWebAppearance(input: { palette: TERMINAL }) \
             { palette progressColors { palette stage color } } }",
            CallerScope::Admin,
        )
        .await;
    assert_no_errors(&switched);
    assert_eq!(
        response_data(&switched)["updateWebAppearance"]["progressColors"],
        weaver_and_terminal
    );

    // A new colour replaces the pick; null restores one stage of one palette.
    let changed = h
        .execute_as(
            &save(
                "{ palette: WEAVER, stage: DOWNLOADING, color: LIME }, \
                 { palette: WEAVER, stage: FAILED, color: null }",
            ),
            CallerScope::Admin,
        )
        .await;
    assert_no_errors(&changed);
    let expected = json!([
        { "palette": "WEAVER", "stage": "DOWNLOADING", "color": "LIME" },
        { "palette": "TERMINAL", "stage": "COMPLETED", "color": "GREY" },
    ]);
    assert_eq!(
        response_data(&changed)["updateWebAppearance"]["progressColors"],
        expected
    );
    let reloaded = serde_json::to_value(AppearanceStore::load(&h.db).unwrap().current()).unwrap();
    assert_eq!(reloaded["progressColors"], expected);

    // More changes than palette and stage pairs are refused before anything is saved.
    let too_many = vec!["{ palette: AMBER, stage: PAUSED, color: RED }"; 29].join(", ");
    assert!(
        !h.execute_as(&save(&too_many), CallerScope::Admin)
            .await
            .errors
            .is_empty()
    );
    let after = serde_json::to_value(AppearanceStore::load(&h.db).unwrap().current()).unwrap();
    assert_eq!(after["progressColors"], expected);
}

#[test]
fn appearance_saved_before_progress_colours_still_loads() {
    let saved: weaver_server_api::settings::appearance::WebAppearance =
        serde_json::from_str(r#"{"palette":"AMBER","mode":"DARK","accent":"ROSE"}"#).unwrap();
    assert!(saved.progress_colors.is_empty());
    assert_eq!(
        saved.palette,
        weaver_server_api::settings::appearance::WebPalette::Amber
    );
}

#[tokio::test]
async fn get_settings_defaults() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"{ settings { dataDir intermediateDir completeDir cleanupAfterExtract maxRetries propagationDelaySecs enableSrrdbLookup } }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let s = &data["settings"];
    // dataDir is the tempdir path — just check it's a non-empty string.
    assert!(!s["dataDir"].as_str().unwrap().is_empty());
    // intermediateDir and completeDir default to paths derived from dataDir.
    assert!(!s["intermediateDir"].as_str().unwrap().is_empty());
    assert!(!s["completeDir"].as_str().unwrap().is_empty());
    // cleanupAfterExtract defaults to true.
    assert!(s["cleanupAfterExtract"].as_bool().unwrap());
    // maxRetries defaults to 3.
    assert_eq!(s["maxRetries"].as_u64().unwrap(), 3);
    assert_eq!(s["propagationDelaySecs"].as_u64().unwrap(), 0);
    assert!(!s["enableSrrdbLookup"].as_bool().unwrap());
}

#[tokio::test]
async fn propagation_delay_persists_and_can_be_disabled() {
    let h = TestHarness::new().await;
    for seconds in [300, 0] {
        let response = h.execute(&format!(
            "mutation {{ updateSettings(input: {{ propagationDelaySecs: {seconds} }}) {{ propagationDelaySecs }} }}"
        )).await;
        assert_no_errors(&response);
        assert_eq!(
            response_data(&response)["updateSettings"]["propagationDelaySecs"],
            seconds
        );
        assert_eq!(
            h.db.load_config().unwrap().propagation_delay_secs(),
            seconds
        );
        assert_eq!(h.config.read().await.propagation_delay_secs(), seconds);
    }
    let invalid = h.execute("mutation { updateSettings(input: { propagationDelaySecs: -1 }) { propagationDelaySecs } }").await;
    assert!(!invalid.errors.is_empty());
    assert_eq!(h.db.load_config().unwrap().propagation_delay_secs(), 0);
}

#[tokio::test]
async fn update_complete_dir() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: "/tmp/complete" }) {
                    completeDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["updateSettings"]["completeDir"].as_str().unwrap(),
        "/tmp/complete"
    );
}

#[tokio::test]
async fn update_intermediate_dir() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { intermediateDir: "/tmp/inter" }) {
                    intermediateDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["updateSettings"]["intermediateDir"].as_str().unwrap(),
        "/tmp/inter"
    );
}

#[tokio::test]
async fn update_cleanup_flag() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { cleanupAfterExtract: false }) {
                    cleanupAfterExtract
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(
        !data["updateSettings"]["cleanupAfterExtract"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn update_max_retries() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { maxRetries: 5 }) {
                    maxRetries
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["updateSettings"]["maxRetries"].as_u64().unwrap(), 5);
}

#[tokio::test]
async fn the_old_speed_setting_is_gone_from_general_settings() {
    let h = TestHarness::new().await;
    assert_has_errors(&h.execute(r#"{ settings { maxDownloadSpeed } }"#).await);
    assert_has_errors(
        &h.execute(r#"mutation { updateSettings(input: { maxDownloadSpeed: 1 }) { maxRetries } }"#)
            .await,
    );
    let resp = h
        .execute(r#"{ networking { bandwidth { downloadBps uploadBps } } }"#)
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["networking"]["bandwidth"],
        serde_json::json!({ "downloadBps": null, "uploadBps": null })
    );
}

#[tokio::test]
async fn srrdb_lookup_is_admin_only_and_persists_to_the_live_config() {
    let h = TestHarness::new().await;
    let query = "{ settings { enableSrrdbLookup } }";
    let mutation =
        "mutation { updateSettings(input: { enableSrrdbLookup: true }) { enableSrrdbLookup } }";

    for scope in [CallerScope::Read, CallerScope::Control] {
        for document in [query, mutation] {
            let response = h.execute_as(document, scope).await;
            assert!(
                response
                    .errors
                    .iter()
                    .any(|error| error.message.contains("admin scope required")),
                "{scope:?} was not refused for {document}: {:?}",
                response.errors
            );
        }
    }

    let response = h.execute(mutation).await;
    assert_no_errors(&response);
    assert!(
        response_data(&response)["updateSettings"]["enableSrrdbLookup"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        h.db.get_setting("delivery_naming.enable_srrdb_lookup")
            .unwrap()
            .as_deref(),
        Some("true")
    );
    assert!(h.config.read().await.enable_srrdb_lookup());

    let response = h.execute(query).await;
    assert_no_errors(&response);
    assert!(
        response_data(&response)["settings"]["enableSrrdbLookup"]
            .as_bool()
            .unwrap()
    );
}

#[tokio::test]
async fn update_watch_folder_settings() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: {
                    watchFolder: {
                        mode: "polling",
                        path: "/tmp/watch-nzbs",
                        pollIntervalSecs: 30,
                        stabilitySecs: 5,
                        categoryFromSubfolders: false,
                        scanningPaused: true
                    }
                }) {
                    watchFolder {
                        mode
                        path
                        pollIntervalSecs
                        stabilitySecs
                        categoryFromSubfolders
                        scanningPaused
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let watch = &data["updateSettings"]["watchFolder"];
    assert_eq!(watch["mode"].as_str().unwrap(), "polling");
    assert_eq!(watch["path"].as_str().unwrap(), "/tmp/watch-nzbs");
    assert_eq!(watch["pollIntervalSecs"].as_u64().unwrap(), 30);
    assert_eq!(watch["stabilitySecs"].as_u64().unwrap(), 5);
    assert!(!watch["categoryFromSubfolders"].as_bool().unwrap());
    assert!(watch["scanningPaused"].as_bool().unwrap());

    let resp = h
        .execute(
            r#"{
                settings {
                    watchFolder {
                        mode
                        path
                        pollIntervalSecs
                        stabilitySecs
                        categoryFromSubfolders
                        scanningPaused
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(&data["settings"]["watchFolder"], watch);
}

#[tokio::test]
async fn settings_persist_across_queries() {
    let h = TestHarness::new().await;

    // Update a setting.
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: "/tmp/persist" }) {
                    completeDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);

    // Query twice and verify the value is the same both times.
    for _ in 0..2 {
        let resp = h.execute(r#"{ settings { completeDir } }"#).await;
        assert_no_errors(&resp);
        let data = response_data(&resp);
        assert_eq!(
            data["settings"]["completeDir"].as_str().unwrap(),
            "/tmp/persist"
        );
    }
}

#[tokio::test]
async fn clear_complete_dir_with_null() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: "/tmp/explicit-complete" }) {
                    completeDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        h.db.get_setting("complete_dir").unwrap().as_deref(),
        Some("/tmp/explicit-complete")
    );

    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: null }) {
                    dataDir
                    completeDir
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let data_dir = data["updateSettings"]["dataDir"].as_str().unwrap();
    let expected = format!("{data_dir}/complete");
    assert_eq!(
        data["updateSettings"]["completeDir"].as_str().unwrap(),
        expected
    );
    assert!(h.db.get_setting("complete_dir").unwrap().is_none());
}

#[tokio::test]
async fn partial_settings_update() {
    let h = TestHarness::new().await;

    // Get defaults first.
    let resp = h
        .execute(r#"{ settings { intermediateDir cleanupAfterExtract maxRetries } }"#)
        .await;
    assert_no_errors(&resp);
    let before = response_data(&resp);
    let before_intermediate = before["settings"]["intermediateDir"]
        .as_str()
        .unwrap()
        .to_string();
    let before_cleanup = before["settings"]["cleanupAfterExtract"].as_bool().unwrap();
    let before_retries = before["settings"]["maxRetries"].as_u64().unwrap();

    // Update only completeDir.
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: "/tmp/partial" }) {
                    completeDir intermediateDir cleanupAfterExtract maxRetries
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let s = &data["updateSettings"];

    // completeDir changed.
    assert_eq!(s["completeDir"].as_str().unwrap(), "/tmp/partial");
    // Everything else unchanged.
    assert_eq!(s["intermediateDir"].as_str().unwrap(), before_intermediate);
    assert_eq!(s["cleanupAfterExtract"].as_bool().unwrap(), before_cleanup);
    assert_eq!(s["maxRetries"].as_u64().unwrap(), before_retries);
}

#[tokio::test]
async fn settings_query_stays_responsive_during_update_settings_persist() {
    let h = TestHarness::new().await;
    let blocker = BlockingDbOperation::new("settings.mutation.update_settings.persist");
    let schema = h.schema.clone();

    let mutation = tokio::spawn(async move {
        schema
            .execute(local_request(
                r#"mutation {
                    updateSettings(input: { completeDir: "/tmp/blocked" }) {
                        completeDir
                    }
                }"#,
            ))
            .await
    });

    blocker.wait_until_started().await;

    let resp = h.execute(r#"{ settings { completeDir } }"#).await;
    assert_no_errors(&resp);
    assert_ne!(
        response_data(&resp)["settings"]["completeDir"]
            .as_str()
            .unwrap(),
        "/tmp/blocked"
    );

    blocker.release();

    let mutation = mutation.await.unwrap();
    assert_no_errors(&mutation);
    assert_eq!(
        response_data(&mutation)["updateSettings"]["completeDir"]
            .as_str()
            .unwrap(),
        "/tmp/blocked"
    );
}

async fn first_run_pending(h: &TestHarness) -> bool {
    let resp = h.execute("{ firstRunSetup { pending } }").await;
    assert_no_errors(&resp);
    response_data(&resp)["firstRunSetup"]["pending"]
        .as_bool()
        .unwrap()
}

async fn add_inactive_server(h: &TestHarness) -> u64 {
    let resp = h
        .execute(
            r#"mutation { addServer(input: { host: "news.example.com", port: 119, tls: false, connections: 5, active: false }) { id } }"#,
        )
        .await;
    assert_no_errors(&resp);
    response_data(&resp)["addServer"]["id"].as_u64().unwrap()
}

#[tokio::test]
async fn first_run_setup_stays_open_until_finished() {
    let h = TestHarness::new().await;
    assert!(first_run_pending(&h).await);

    let resp = h
        .execute("mutation { beginFirstRunSetup { pending } }")
        .await;
    assert_no_errors(&resp);
    assert!(
        response_data(&resp)["beginFirstRunSetup"]["pending"]
            .as_bool()
            .unwrap()
    );

    // The wizard's own first step adds a provider; a reload must not lose it.
    let server_id = add_inactive_server(&h).await;
    assert!(first_run_pending(&h).await);

    let resp = h
        .execute("mutation { finishFirstRunSetup { pending } }")
        .await;
    assert_no_errors(&resp);
    assert!(!first_run_pending(&h).await);

    // Nothing reopens a finished wizard, not even with every provider gone.
    let resp = h
        .execute(&format!(
            "mutation {{ removeServer(id: {server_id}) {{ id }} }}"
        ))
        .await;
    assert_no_errors(&resp);
    let resp = h
        .execute("mutation { beginFirstRunSetup { pending } }")
        .await;
    assert_no_errors(&resp);
    assert!(!first_run_pending(&h).await);
}

#[tokio::test]
async fn first_run_setup_is_not_owed_by_an_install_with_a_provider() {
    let h = TestHarness::new().await;
    add_inactive_server(&h).await;
    assert!(!first_run_pending(&h).await);

    let resp = h
        .execute("mutation { beginFirstRunSetup { pending } }")
        .await;
    assert_no_errors(&resp);
    assert!(
        !response_data(&resp)["beginFirstRunSetup"]["pending"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(h.db.get_setting("first_run_setup").unwrap(), None);
}

#[tokio::test]
async fn first_run_setup_requires_admin() {
    let h = TestHarness::new().await;
    for query in [
        "{ firstRunSetup { pending } }",
        "mutation { finishFirstRunSetup { pending } }",
    ] {
        let resp = h.execute_as(query, CallerScope::Read).await;
        assert!(
            !resp.errors.is_empty(),
            "{query} must refuse a read-scoped caller"
        );
    }
}

async fn security_upgrade_notice_pending(h: &TestHarness) -> bool {
    let resp = h
        .execute("{ securityUpgradeNotice { pending deployment operatingSystem loginEnabled } }")
        .await;
    assert_no_errors(&resp);
    response_data(&resp)["securityUpgradeNotice"]["pending"]
        .as_bool()
        .unwrap()
}

#[tokio::test]
async fn security_upgrade_notice_shows_once_on_the_older_access_settings() {
    let h = TestHarness::new().await;
    assert!(security_upgrade_notice_pending(&h).await);

    let resp = h
        .execute("mutation { dismissSecurityUpgradeNotice { pending } }")
        .await;
    assert_no_errors(&resp);
    assert!(!security_upgrade_notice_pending(&h).await);
}

#[tokio::test]
async fn security_upgrade_notice_is_not_owed_on_the_current_access_model() {
    let security = weaver_server_core::security::RuntimeSecurityConfig::default();
    security.apply_stored_access_policy_revision(
        None,
        Some(weaver_server_core::security::AUTHENTICATED_POLICY_REVISION),
        true,
    );
    let h = TestHarness::new_with_security(security).await;
    assert!(!security_upgrade_notice_pending(&h).await);
}

const HARDWARE_PROFILE_FIELDS: &str = "{ selected recommended available \
     options { profile sevenzDecodeMemoryBytes decodeThreads extractThreads maxConcurrentDownloads } \
     detected { memoryBytes cores } }";

#[tokio::test]
async fn the_machine_is_offered_only_the_profiles_it_can_honour() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!("{{ hardwareProfile {HARDWARE_PROFILE_FIELDS} }}"))
        .await;
    assert_no_errors(&resp);
    let profile = &response_data(&resp)["hardwareProfile"];

    // The harness machine has four cores and 8 GiB: enough for balanced,
    // short of the sixteen gibibytes performance asks for.
    assert!(profile["selected"].is_null());
    assert_eq!(profile["recommended"], "BALANCED");
    assert_eq!(
        profile["available"],
        serde_json::json!(["EFFICIENT", "BALANCED"])
    );
    assert_eq!(
        profile["detected"]["memoryBytes"].as_u64().unwrap(),
        8 * 1024 * 1024 * 1024
    );
    assert_eq!(profile["detected"]["cores"].as_u64().unwrap(), 4);

    // The cards' numbers come from the server's table, so an interface never
    // keeps its own copy of them.
    assert_eq!(
        profile["options"],
        serde_json::json!([
            {
                "profile": "EFFICIENT",
                "sevenzDecodeMemoryBytes": 512 * 1024 * 1024,
                "decodeThreads": 2,
                "extractThreads": 2,
                "maxConcurrentDownloads": 10,
            },
            {
                "profile": "BALANCED",
                "sevenzDecodeMemoryBytes": 1024 * 1024 * 1024,
                "decodeThreads": 4,
                "extractThreads": 2,
                "maxConcurrentDownloads": null,
            },
        ])
    );
}

#[tokio::test]
async fn a_chosen_profile_is_persisted_and_read_back() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            "mutation {{ setHardwareProfile(profile: EFFICIENT) {HARDWARE_PROFILE_FIELDS} }}"
        ))
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["setHardwareProfile"]["selected"],
        "EFFICIENT"
    );

    assert_eq!(
        h.config.read().await.hardware_profile,
        Some(weaver_server_core::runtime::HardwareProfile::Efficient)
    );
    assert_eq!(
        h.db.load_config().unwrap().hardware_profile,
        Some(weaver_server_core::runtime::HardwareProfile::Efficient)
    );

    let read_back = h
        .execute(&format!("{{ hardwareProfile {HARDWARE_PROFILE_FIELDS} }}"))
        .await;
    assert_no_errors(&read_back);
    assert_eq!(
        response_data(&read_back)["hardwareProfile"]["selected"],
        "EFFICIENT"
    );
}

#[tokio::test]
async fn a_profile_the_machine_cannot_honour_is_refused_by_name() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(&format!(
            "mutation {{ setHardwareProfile(profile: PERFORMANCE) {HARDWARE_PROFILE_FIELDS} }}"
        ))
        .await;
    let message = resp
        .errors
        .first()
        .map(|error| error.message.clone())
        .expect("an unavailable profile must be refused");
    assert!(
        message.contains("performance") && message.contains("16 GiB") && message.contains("8 GiB"),
        "the refusal must name the requirement and what the machine has: {message}"
    );
    assert!(h.config.read().await.hardware_profile.is_none());
    assert!(h.db.load_config().unwrap().hardware_profile.is_none());
}

// A choice saved on a machine that could honour it, read back on one that no
// longer can, is not what runs: startup falls back to the recommendation, and
// the answer says so rather than naming a profile it does not offer.
#[tokio::test]
async fn a_saved_profile_the_machine_can_no_longer_honour_reads_as_not_chosen() {
    let h = TestHarness::new().await;
    h.config.write().await.hardware_profile =
        Some(weaver_server_core::runtime::HardwareProfile::Performance);

    let resp = h
        .execute(&format!("{{ hardwareProfile {HARDWARE_PROFILE_FIELDS} }}"))
        .await;
    assert_no_errors(&resp);
    let profile = &response_data(&resp)["hardwareProfile"];
    assert!(
        profile["selected"].is_null(),
        "an unavailable saved profile is not reported as selected: {profile}"
    );
    assert_eq!(profile["recommended"], "BALANCED");
}

#[tokio::test]
async fn the_hardware_profile_is_an_administrator_surface() {
    let h = TestHarness::new().await;
    for query in [
        "{ hardwareProfile { recommended } }",
        "mutation { setHardwareProfile(profile: EFFICIENT) { selected } }",
    ] {
        let resp = h.execute_as(query, CallerScope::Read).await;
        assert!(
            !resp.errors.is_empty(),
            "{query} must refuse a read-scoped caller"
        );
    }
    assert!(h.config.read().await.hardware_profile.is_none());
}

#[tokio::test]
async fn the_profile_in_force_is_reported_beside_the_choice() {
    let h = TestHarness::new().await;
    let resp = h
        .execute("{ hardwareProfile { selected active scheduled } }")
        .await;
    assert_no_errors(&resp);
    let profile = &response_data(&resp)["hardwareProfile"];
    // Never chosen: the recommendation is what is in force.
    assert!(profile["selected"].is_null());
    assert_eq!(profile["active"], "BALANCED");
    assert!(profile["scheduled"].is_null());

    let resp = h
        .execute(
            "mutation { setHardwareProfile(profile: EFFICIENT) { selected active scheduled } }",
        )
        .await;
    assert_no_errors(&resp);
    let profile = &response_data(&resp)["setHardwareProfile"];
    assert_eq!(profile["selected"], "EFFICIENT");
    let mut revisions = h.handle.subscribe_job_changes();
    loop {
        revisions.mark_unchanged();
        if h.handle.hardware_profile_in_force().is_some_and(|profile| {
            profile.active == weaver_server_core::runtime::HardwareProfile::Efficient
        }) {
            break;
        }
        revisions.changed().await.unwrap();
    }
    let resp = h
        .execute("{ hardwareProfile { selected active scheduled } }")
        .await;
    assert_no_errors(&resp);
    let profile = &response_data(&resp)["hardwareProfile"];
    assert_eq!(profile["active"], "EFFICIENT");
    assert!(profile["scheduled"].is_null());
}

#[tokio::test]
async fn seeding_and_torrent_settings_default_then_persist_and_clear() {
    let h = TestHarness::new().await;
    let defaults = h
        .execute(
            "{ settings { seeding { ratioGoal { value } } uploadCap torrent { enabled basePort announcePort upnp encryption maxActiveDownloads maxPeersTotal sessionDir } } }",
        )
        .await;
    assert_no_errors(&defaults);
    let data = response_data(&defaults);
    assert!(data["settings"]["seeding"].is_null());
    assert_eq!(data["settings"]["torrent"]["enabled"], false);
    assert_eq!(data["settings"]["torrent"]["basePort"], 6881);
    assert_eq!(data["settings"]["torrent"]["encryption"], "PREFER");

    let saved = h
        .execute(
            r#"mutation { updateSettings(input: {
                seeding: {
                    ratioGoal: { state: SET, value: 2.0 }
                    seedTimeGoalMinutes: { state: SET, value: 90 }
                    mode: { state: SET, value: ALL }
                    action: { state: SET, value: REMOVE_WITH_DATA }
                }
                uploadCap: 65536
                torrent: { enabled: true, basePort: 51413, announcePort: 40000, encryption: REQUIRE, sessionDir: "/data/session" }
            }) {
                seeding {
                    ratioGoal { value } seedTimeGoalMinutes { value }
                    inactiveGoalMinutes { state value } mode { value } action { value }
                }
                uploadCap
                torrent { enabled basePort announcePort upnp encryption sessionDir }
            } }"#,
        )
        .await;
    assert_no_errors(&saved);
    let data = response_data(&saved);
    let seeding = &data["updateSettings"]["seeding"];
    assert_eq!(seeding["ratioGoal"]["value"], 2.0);
    assert_eq!(seeding["seedTimeGoalMinutes"]["value"], 90);
    // At the global level a goal left out is off: nothing is above it.
    assert_eq!(seeding["inactiveGoalMinutes"]["state"], "OFF");
    assert!(seeding["inactiveGoalMinutes"]["value"].is_null());
    assert_eq!(seeding["mode"]["value"], "ALL");
    assert_eq!(seeding["action"]["value"], "REMOVE_WITH_DATA");
    assert_eq!(data["updateSettings"]["uploadCap"], 65536);
    let torrent = &data["updateSettings"]["torrent"];
    assert_eq!(torrent["enabled"], true);
    assert_eq!(torrent["basePort"], 51413);
    assert_eq!(torrent["announcePort"], 40000);
    assert_eq!(torrent["upnp"], true);
    assert_eq!(torrent["encryption"], "REQUIRE");
    assert_eq!(torrent["sessionDir"], "/data/session");

    let stored = h.db.load_config().unwrap();
    assert_eq!(stored.seeding, h.config.read().await.seeding);
    assert_eq!(stored.upload_cap, Some(65_536));
    assert_eq!(stored.upload_cap, h.config.read().await.upload_cap);
    assert_eq!(stored.torrent, h.config.read().await.torrent);
    assert_eq!(stored.torrent.base_port, 51413);

    // Leaving a field out keeps it; null clears it.
    let cleared = h
        .execute(
            r#"mutation { updateSettings(input: { seeding: null, uploadCap: null, torrent: { announcePort: null } }) {
                seeding { ratioGoal { value } } uploadCap torrent { enabled announcePort }
            } }"#,
        )
        .await;
    assert_no_errors(&cleared);
    let data = response_data(&cleared);
    assert!(data["updateSettings"]["seeding"].is_null());
    assert_eq!(data["updateSettings"]["torrent"]["enabled"], true);
    assert!(data["updateSettings"]["torrent"]["announcePort"].is_null());
    let stored = h.db.load_config().unwrap();
    assert_eq!(stored.seeding, None);
    assert_eq!(stored.upload_cap, None);
    assert_eq!(stored.torrent.announce_port, None);

    let refused = h
        .execute(r#"mutation { updateSettings(input: { seeding: { ratioGoal: { state: SET, value: -1.0 } } }) { seeding { ratioGoal { value } } } }"#)
        .await;
    assert!(!refused.errors.is_empty());
    assert_eq!(h.db.load_config().unwrap().seeding, None);
}

#[tokio::test]
async fn torrent_session_settings_default_to_libtorrents_then_persist_and_refuse_out_of_range() {
    let h = TestHarness::new().await;
    const FIELDS: &str = "connectionsLimit unchokeSlotsLimit connectionSpeed allowMultipleConnectionsPerIp peerTcp peerUtp preferDownloading preferSeeding dhtBootstrapNodes blocklistEnabled blocklist stopTrackerTimeoutSecs";
    let defaults = h
        .execute(&format!("{{ settings {{ torrent {{ {FIELDS} }} }} }}"))
        .await;
    assert_no_errors(&defaults);
    let torrent = &response_data(&defaults)["settings"]["torrent"];
    assert_eq!(torrent["connectionsLimit"], 200);
    assert_eq!(torrent["unchokeSlotsLimit"], 8);
    assert_eq!(torrent["connectionSpeed"], 30);
    assert_eq!(torrent["allowMultipleConnectionsPerIp"], false);
    assert_eq!(torrent["peerTcp"], true);
    assert_eq!(torrent["peerUtp"], true);
    assert_eq!(torrent["preferDownloading"], "TCP");
    assert_eq!(torrent["preferSeeding"], "UTP");
    assert_eq!(
        torrent["dhtBootstrapNodes"],
        "dht.transmissionbt.com:6881,dht.libtorrent.org:25401"
    );
    assert_eq!(torrent["blocklistEnabled"], false);
    assert!(torrent["blocklist"].is_null());
    assert_eq!(torrent["stopTrackerTimeoutSecs"], 5);

    let saved = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{
                connectionsLimit: null, unchokeSlotsLimit: 16, connectionSpeed: 60,
                allowMultipleConnectionsPerIp: true, preferDownloading: UTP,
                dhtBootstrapNodes: " router.example:6881 ,, 192.0.2.9:25401", blocklistEnabled: true,
                blocklist: " C:\\lists\\level1.p2p ", stopTrackerTimeoutSecs: 0
            }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&saved);
    let torrent = &response_data(&saved)["updateSettings"]["torrent"];
    assert!(torrent["connectionsLimit"].is_null(), "no limit");
    assert_eq!(torrent["unchokeSlotsLimit"], 16);
    assert_eq!(torrent["connectionSpeed"], 60);
    assert_eq!(torrent["allowMultipleConnectionsPerIp"], true);
    assert_eq!(torrent["preferDownloading"], "UTP");
    assert_eq!(torrent["preferSeeding"], "UTP");
    assert_eq!(
        torrent["dhtBootstrapNodes"],
        "router.example:6881,192.0.2.9:25401"
    );
    assert_eq!(torrent["blocklistEnabled"], true);
    assert_eq!(torrent["blocklist"], r"C:\lists\level1.p2p");
    assert_eq!(torrent["stopTrackerTimeoutSecs"], 0);
    let stored = h.db.load_config().unwrap();
    assert_eq!(stored.torrent, h.config.read().await.torrent);
    assert_eq!(
        stored.torrent.transports.dial_orders(),
        weaver_server_core::settings::PeerTransports {
            prefer_downloading: weaver_server_core::settings::PeerTransport::Utp,
            ..Default::default()
        }
        .dial_orders()
    );
    assert_eq!(
        stored.torrent.connections_limit,
        weaver_server_core::settings::TorrentLimit::Unlimited
    );

    for input in [
        "connectionsLimit: 0",
        "unchokeSlotsLimit: 0",
        "connectionSpeed: 0",
        r#"dhtBootstrapNodes: "router.example""#,
        // TCP and uTP cannot both be off.
        "peerTcp: false, peerUtp: false",
    ] {
        let refused = h
            .execute(&format!(
                "mutation {{ updateSettings(input: {{ torrent: {{ {input} }} }}) {{ torrent {{ connectionsLimit }} }} }}"
            ))
            .await;
        assert!(!refused.errors.is_empty(), "{input}");
        assert_eq!(
            h.db.load_config().unwrap().torrent,
            stored.torrent,
            "{input}"
        );
        assert_eq!(h.config.read().await.torrent, stored.torrent, "{input}");
    }

    // With one transport off, the preferences are kept as set and the
    // dials use the one left on; turned on again, the kept ones are used.
    use weaver_server_core::settings::{PeerTransport, PeerTransports};
    // The orders of TCP alone, and of uTP first in both states.
    let tcp_only = PeerTransports {
        utp: false,
        ..PeerTransports::default()
    }
    .dial_orders();
    let utp_first = PeerTransports {
        prefer_downloading: PeerTransport::Utp,
        ..PeerTransports::default()
    }
    .dial_orders();
    let off = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{
                peerUtp: false, preferDownloading: UTP, preferSeeding: UTP
            }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&off);
    let torrent = &response_data(&off)["updateSettings"]["torrent"];
    assert_eq!(torrent["peerTcp"], true);
    assert_eq!(torrent["peerUtp"], false);
    assert_eq!(torrent["preferDownloading"], "UTP");
    assert_eq!(torrent["preferSeeding"], "UTP");
    let kept = h.db.load_config().unwrap().torrent.transports;
    assert_eq!(
        (kept.prefer_downloading, kept.prefer_seeding),
        (PeerTransport::Utp, PeerTransport::Utp)
    );
    assert_eq!(kept.dial_orders(), tcp_only);
    let on = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{ peerUtp: true }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&on);
    let torrent = &response_data(&on)["updateSettings"]["torrent"];
    assert_eq!(torrent["preferDownloading"], "UTP");
    assert_eq!(torrent["preferSeeding"], "UTP");
    assert_eq!(
        h.db.load_config().unwrap().torrent.transports.dial_orders(),
        utp_first
    );

    // An empty blocklist clears it, and the switch stays as it was.
    let cleared = h
        .execute(r#"mutation { updateSettings(input: { torrent: { blocklist: "" } }) { torrent { blocklistEnabled blocklist } } }"#)
        .await;
    assert_no_errors(&cleared);
    let torrent = &response_data(&cleared)["updateSettings"]["torrent"];
    assert!(torrent["blocklist"].is_null());
    assert_eq!(torrent["blocklistEnabled"], true);
    assert_eq!(h.db.load_config().unwrap().torrent.blocklist, None);
}

// qBittorrent's defaults (`BannedIPs` empty, `BlockPeersOnPrivilegedPorts`
// and `IPFilterTrackers` off); banned entries are kept in one form, sorted
// and once, and an entry that names no address is refused whole.
#[tokio::test]
async fn banned_addresses_and_filter_switches_default_off_then_persist() {
    let h = TestHarness::new().await;
    const FIELDS: &str = "bannedIps blockPrivilegedPorts filterTrackers";
    let defaults = h
        .execute(&format!("{{ settings {{ torrent {{ {FIELDS} }} }} }}"))
        .await;
    assert_no_errors(&defaults);
    let torrent = &response_data(&defaults)["settings"]["torrent"];
    assert_eq!(torrent["bannedIps"], serde_json::json!([]));
    assert_eq!(torrent["blockPrivilegedPorts"], false);
    assert_eq!(torrent["filterTrackers"], false);

    let saved = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{
                bannedIps: [" 192.0.2.7 ", "10.0.0.0/24", "192.0.2.7", ""],
                blockPrivilegedPorts: true, filterTrackers: true
            }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&saved);
    let torrent = &response_data(&saved)["updateSettings"]["torrent"];
    assert_eq!(
        torrent["bannedIps"],
        serde_json::json!(["10.0.0.0 - 10.0.0.255", "192.0.2.7"])
    );
    assert_eq!(torrent["blockPrivilegedPorts"], true);
    assert_eq!(torrent["filterTrackers"], true);
    let stored = h.db.load_config().unwrap();
    assert_eq!(stored.torrent, h.config.read().await.torrent);
    assert_eq!(
        stored.torrent.banned_ips,
        ["10.0.0.0 - 10.0.0.255", "192.0.2.7"]
    );

    let refused = h
        .execute(r#"mutation { updateSettings(input: { torrent: { bannedIps: ["192.0.2.8", "lists.example"] } }) { torrent { bannedIps } } }"#)
        .await;
    assert!(!refused.errors.is_empty());
    assert_eq!(h.db.load_config().unwrap().torrent, stored.torrent);

    let cleared = h
        .execute(r#"mutation { updateSettings(input: { torrent: { bannedIps: [] } }) { torrent { bannedIps } } }"#)
        .await;
    assert_no_errors(&cleared);
    assert!(h.db.load_config().unwrap().torrent.banned_ips.is_empty());
}

// SSL torrents' listener is off until turned on and names no port until
// one is given; a port given is kept, and null leaves the choice to the
// sessions again.
#[tokio::test]
async fn the_ssl_listener_keeps_its_port() {
    let h = TestHarness::new().await;
    const FIELDS: &str = "sslEnabled sslPort";
    let defaults = h
        .execute(&format!("{{ settings {{ torrent {{ {FIELDS} }} }} }}"))
        .await;
    assert_no_errors(&defaults);
    assert_eq!(
        response_data(&defaults)["settings"]["torrent"],
        serde_json::json!({ "sslEnabled": false, "sslPort": null })
    );

    let saved = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{
                sslEnabled: true, sslPort: 44330
            }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&saved);
    assert_eq!(
        response_data(&saved)["updateSettings"]["torrent"],
        serde_json::json!({ "sslEnabled": true, "sslPort": 44330 })
    );
    let stored = h.db.load_config().unwrap().torrent;
    assert!(stored.ssl_enabled);
    assert_eq!(stored.ssl_port, Some(44_330));
    assert_eq!(stored, h.config.read().await.torrent);

    let cleared = h
        .execute(&format!(
            r#"mutation {{ updateSettings(input: {{ torrent: {{ sslPort: null }} }}) {{ torrent {{ {FIELDS} }} }} }}"#
        ))
        .await;
    assert_no_errors(&cleared);
    assert_eq!(h.db.load_config().unwrap().torrent.ssl_port, None);
}

// A request on a server started without `WEAVER_TORRENTS`.
fn without_torrents(query: &str) -> async_graphql::Request {
    local_request(query).data(weaver_server_core::torrents::TorrentAvailability::UNAVAILABLE)
}

fn error_code(response: &async_graphql::Response) -> Option<String> {
    let error = response.errors.first()?;
    serde_json::to_value(&error.extensions)
        .ok()?
        .get("code")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

const PRESENCE: &str =
    "{ torrentPresence { available enabled jobs running windingDown windDown } }";

#[tokio::test]
async fn a_server_without_torrents_refuses_turning_them_on_and_names_the_variable() {
    let h = TestHarness::new().await;
    let before = h.config.read().await.clone();
    let refused = h
        .schema
        .execute(without_torrents(
            "mutation { updateSettings(input: { uploadCap: 4096, torrent: { enabled: true, basePort: 51413 } }) { torrent { enabled } } }",
        ))
        .await;
    assert_eq!(
        error_code(&refused).as_deref(),
        Some("TORRENTS_UNAVAILABLE")
    );
    assert!(
        refused.errors[0].message.contains("WEAVER_TORRENTS"),
        "{}",
        refused.errors[0].message
    );
    // Nothing of the refused save was kept, in memory or stored.
    let stored = h.db.load_config().unwrap();
    assert!(!stored.torrent.enabled);
    assert_ne!(stored.torrent.base_port, 51413);
    assert_eq!(stored.upload_cap, None);
    let live = h.config.read().await.clone();
    assert_eq!(live.torrent, before.torrent);
    assert_eq!(live.upload_cap, before.upload_cap);

    // The settings still read, and every other torrent setting still saves.
    let read = h
        .schema
        .execute(without_torrents(
            "{ settings { torrent { enabled basePort } } }",
        ))
        .await;
    assert_no_errors(&read);
    assert_eq!(
        response_data(&read)["settings"]["torrent"]["enabled"],
        false
    );
    let saved = h
        .schema
        .execute(without_torrents(
            "mutation { updateSettings(input: { torrent: { enabled: false, basePort: 51413 } }) { torrent { enabled basePort } } }",
        ))
        .await;
    assert_no_errors(&saved);
    assert_eq!(
        response_data(&saved)["updateSettings"]["torrent"]["basePort"],
        51413
    );
    assert!(!h.db.load_config().unwrap().torrent.enabled);
}

#[tokio::test]
async fn a_server_without_torrents_says_so_whatever_is_saved() {
    let h = TestHarness::new().await;
    // Saved on while the server had torrents.
    let on = h
        .execute("mutation { updateSettings(input: { torrent: { enabled: true } }) { torrent { enabled } } }")
        .await;
    assert_no_errors(&on);
    let with = h.execute(PRESENCE).await;
    assert_no_errors(&with);
    let presence = &response_data(&with)["torrentPresence"];
    assert_eq!(presence["available"], true);
    assert_eq!(presence["enabled"], true);

    let without = h.schema.execute(without_torrents(PRESENCE)).await;
    assert_no_errors(&without);
    assert_eq!(
        response_data(&without)["torrentPresence"],
        serde_json::json!({
            "available": false, "enabled": false, "jobs": false,
            "running": false, "windingDown": false, "windDown": null
        })
    );
    // The saved setting is as it was: read, and stored.
    let read = h
        .schema
        .execute(without_torrents("{ settings { torrent { enabled } } }"))
        .await;
    assert_eq!(response_data(&read)["settings"]["torrent"]["enabled"], true);
    assert!(h.db.load_config().unwrap().torrent.enabled);
    // Every torrent query answers as it does while torrents are off.
    let listed = h
        .schema
        .execute(without_torrents("{ torrents { id } }"))
        .await;
    assert_eq!(error_code(&listed).as_deref(), Some("TORRENTS_DISABLED"));
}
