mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;

/// Write a bare script into the harness's `data_dir/scripts`.
async fn write_script(harness: &TestHarness, name: &str, body: &str) {
    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    let scripts = data_dir.join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    std::fs::write(scripts.join(name), body).unwrap();
}

#[tokio::test]
async fn settings_are_admin_only_and_execution_is_off_by_default() {
    let harness = TestHarness::new().await;
    let denied = harness
        .execute_as(
            "{ postProcessingSettings { executionEnabled } }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);

    let response = harness
        .execute(
            "{ postProcessingSettings { scriptDirectory executionEnabled concurrency terminationGraceSeconds unacceptableExtensions strictSecurityRefusesExecution lists { global { script } } } }",
        )
        .await;
    assert_no_errors(&response);
    let settings = &response_data(&response)["postProcessingSettings"];
    assert!(std::path::Path::new(settings["scriptDirectory"].as_str().unwrap()).is_absolute());
    assert_eq!(settings["executionEnabled"], false);
    assert_eq!(settings["concurrency"], 1);
    assert_eq!(settings["terminationGraceSeconds"], 10);
    assert_eq!(settings["unacceptableExtensions"], serde_json::json!([]));
    assert_eq!(settings["strictSecurityRefusesExecution"], false);
    assert_eq!(settings["lists"]["global"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn scripts_directory_is_admin_owned_and_clears_assignments_when_changed() {
    let harness = TestHarness::new().await;
    let denied = harness
        .execute_as(
            r#"mutation { setPostProcessingScriptDirectory(directory: "/tmp/scripts") { scriptDirectory } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);

    let lists = harness
        .execute(
            r#"mutation { setScriptLists(input: { global: [{ script: "notify.sh" }] }) { global { script } } }"#,
        )
        .await;
    assert_no_errors(&lists);

    let root = tempfile::tempdir().unwrap();
    let requested = root.path().join("nested/scripts");
    let requested_gql = serde_json::to_string(&requested).unwrap();
    let response = harness
        .execute(&format!(
            "mutation {{ setPostProcessingScriptDirectory(directory: {requested_gql}) {{ scriptDirectory lists {{ global {{ script }} }} }} }}"
        ))
        .await;
    assert_no_errors(&response);
    let settings = &response_data(&response)["setPostProcessingScriptDirectory"];
    let canonical = std::fs::canonicalize(&requested).unwrap();
    assert_eq!(
        settings["scriptDirectory"],
        canonical.to_string_lossy().as_ref()
    );
    assert!(settings["lists"]["global"].as_array().unwrap().is_empty());

    std::fs::write(
        canonical.join("replacement.sh"),
        "#!/bin/sh\necho replacement\n",
    )
    .unwrap();
    let listing = harness.execute("{ scripts { scripts { name } } }").await;
    assert_no_errors(&listing);
    assert_eq!(
        response_data(&listing)["scripts"]["scripts"][0]["name"],
        "replacement.sh"
    );
}

#[tokio::test]
async fn settings_round_trip_preserves_omitted_extensions_and_rejects_invalid_updates() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(
            r#"
            mutation {
              setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 2
                terminationGraceSeconds: 15
                pythonInterpreter: "/usr/bin/python3"
                unacceptableExtensions: ["EXE", "r??"]
              }) {
                executionEnabled
                concurrency
                terminationGraceSeconds
                pythonInterpreter
                unacceptableExtensions
              }
            }
            "#,
        )
        .await;
    assert_no_errors(&response);
    let settings = &response_data(&response)["setPostProcessingSettings"];
    assert_eq!(settings["executionEnabled"], true);
    assert_eq!(settings["concurrency"], 2);
    assert_eq!(settings["terminationGraceSeconds"], 15);
    assert_eq!(settings["pythonInterpreter"], "/usr/bin/python3");
    assert_eq!(
        settings["unacceptableExtensions"],
        serde_json::json!(["exe", "r??"])
    );

    let omitted = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 3
                terminationGraceSeconds: 20
            }) { concurrency unacceptableExtensions } }"#,
        )
        .await;
    assert_no_errors(&omitted);
    assert_eq!(
        response_data(&omitted)["setPostProcessingSettings"]["unacceptableExtensions"],
        serde_json::json!(["exe", "r??"])
    );

    let rejected = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 99
                terminationGraceSeconds: 15
            }) { concurrency } }"#,
        )
        .await;
    assert_has_errors(&rejected);

    let invalid_pattern = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: false
                concurrency: 1
                terminationGraceSeconds: 10
                unacceptableExtensions: [".exe"]
            }) { unacceptableExtensions } }"#,
        )
        .await;
    assert_has_errors(&invalid_pattern);

    let null_policy = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 3
                terminationGraceSeconds: 20
                unacceptableExtensions: null
            }) { unacceptableExtensions } }"#,
        )
        .await;
    assert_has_errors(&null_policy);

    let persisted = harness
        .execute("{ postProcessingSettings { unacceptableExtensions } }")
        .await;
    assert_no_errors(&persisted);
    assert_eq!(
        response_data(&persisted)["postProcessingSettings"]["unacceptableExtensions"],
        serde_json::json!(["exe", "r??"])
    );

    let disabled = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 3
                terminationGraceSeconds: 20
                unacceptableExtensions: []
            }) { unacceptableExtensions } }"#,
        )
        .await;
    assert_no_errors(&disabled);
    assert_eq!(
        response_data(&disabled)["setPostProcessingSettings"]["unacceptableExtensions"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn scripts_are_listed_live_from_the_directory_with_their_problems() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    write_script(
        &harness,
        "legacy.py",
        "#!/usr/bin/env python3\n### NZBGET POST-PROCESSING SCRIPT ###\n",
    )
    .await;
    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    let broken = data_dir.join("scripts/broken");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("manifest.json"), "{ not json").unwrap();

    let denied = harness
        .execute_as("{ scripts { scripts { name } } }", CallerScope::Read)
        .await;
    assert_has_errors(&denied);

    let response = harness
        .execute("{ scripts { scripts { name displayName adapter } problems { name message } } }")
        .await;
    assert_no_errors(&response);
    let listing = &response_data(&response)["scripts"];
    let scripts = listing["scripts"].as_array().unwrap();
    assert_eq!(scripts.len(), 2);
    let by_name = |name: &str| {
        scripts
            .iter()
            .find(|script| script["name"] == name)
            .unwrap_or_else(|| panic!("{name} was not listed"))
    };
    assert_eq!(by_name("notify.sh")["adapter"], "SABNZBD");
    assert_eq!(by_name("legacy.py")["adapter"], "NZBGET");
    assert_eq!(listing["problems"].as_array().unwrap().len(), 1);
    assert_eq!(listing["problems"][0]["name"], "broken");
}

#[tokio::test]
async fn script_lists_round_trip_with_a_category_override() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(
            r#"
            mutation {
              setScriptLists(input: {
                global: [{ script: "notify.sh", enabled: true, timeoutSeconds: 30 }]
                categories: [{ category: "movies", entries: [{ script: "sort.sh", enabled: false }] }]
              }) {
                global { script enabled timeoutSeconds }
                categories { category entries { script enabled } }
              }
            }
            "#,
        )
        .await;
    assert_no_errors(&response);
    let lists = &response_data(&response)["setScriptLists"];
    assert_eq!(lists["global"][0]["script"], "notify.sh");
    assert_eq!(lists["global"][0]["timeoutSeconds"], 30);
    assert_eq!(lists["categories"][0]["category"], "movies");
    assert_eq!(lists["categories"][0]["entries"][0]["enabled"], false);

    let settings = harness
        .execute(
            "{ postProcessingSettings { lists { global { script } categories { category } } } }",
        )
        .await;
    assert_no_errors(&settings);
    let lists = &response_data(&settings)["postProcessingSettings"]["lists"];
    assert_eq!(lists["global"][0]["script"], "notify.sh");
    assert_eq!(lists["categories"][0]["category"], "movies");

    // A script name that could escape the directory is refused outright.
    let rejected = harness
        .execute(r#"mutation { setScriptLists(input: { global: [{ script: "../escape" }] }) { global { script } } }"#)
        .await;
    assert_has_errors(&rejected);
}

#[tokio::test]
async fn script_options_are_validated_against_the_manifest_and_masked_when_secret() {
    let harness = TestHarness::new().await;
    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    let package = data_dir.join("scripts/email");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("manifest.json"),
        serde_json::json!({
            "main": "email.py",
            "name": "email",
            "kind": "POST-PROCESSING",
            "displayName": "Email",
            "version": "1.0.0",
            "author": "Author",
            "homepage": "https://example.invalid",
            "license": "GNU",
            "about": "About",
            "description": [],
            "requirements": [],
            "queueEvents": "",
            "taskTime": "",
            "sections": [],
            "commands": [],
            "options": [
                {"name": "Host", "displayName": "Host", "value": "mail.example.invalid", "description": [], "select": []},
                {"name": "Token", "displayName": "Token", "value": "", "description": [], "select": [], "secret": true}
            ]
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(package.join("email.py"), "#!/usr/bin/env python3\n").unwrap();

    let response = harness
        .execute(
            r#"
            mutation {
              setScriptOptions(script: "email", options: [
                { name: "Host", optionType: STRING, value: "smtp.example.invalid" }
                { name: "Token", optionType: SECRET, value: "hunter2" }
              ]) {
                name
                options { name optionType value defaultValue }
              }
            }
            "#,
        )
        .await;
    assert_no_errors(&response);
    let script = &response_data(&response)["setScriptOptions"];
    assert_eq!(script["name"], "email");
    let options = script["options"].as_array().unwrap();
    let host = options.iter().find(|o| o["name"] == "Host").unwrap();
    assert_eq!(host["value"], "smtp.example.invalid");
    assert_eq!(host["defaultValue"], "mail.example.invalid");
    let token = options.iter().find(|o| o["name"] == "Token").unwrap();
    assert_eq!(token["optionType"], "SECRET");
    assert_eq!(
        token["value"], "[REDACTED]",
        "a stored secret must never be echoed back"
    );

    // Options the manifest does not declare are refused rather than stored.
    let rejected = harness
        .execute(
            r#"mutation { setScriptOptions(script: "email", options: [
                { name: "Nope", optionType: STRING, value: "x" }
            ]) { name } }"#,
        )
        .await;
    assert_has_errors(&rejected);

    // A script that is not in the directory cannot have options at all.
    let missing = harness
        .execute(r#"mutation { setScriptOptions(script: "gone.sh", options: []) { name } }"#)
        .await;
    assert_has_errors(&missing);
}

#[tokio::test]
async fn results_are_readable_and_control_scope_owns_rerun_and_cancel() {
    let harness = TestHarness::new().await;
    let empty = harness
        .execute_as(
            "{ postProcessingResults(jobId: 1) { script status } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&empty);
    assert_eq!(
        response_data(&empty)["postProcessingResults"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    let denied = harness
        .execute_as(
            "mutation { rerunPostProcessing(jobId: 1) }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);
    // Control scope is allowed to ask, and is told the job has no history.
    let no_history = harness
        .execute_as(
            "mutation { rerunPostProcessing(jobId: 1) }",
            CallerScope::Control,
        )
        .await;
    assert_has_errors(&no_history);
    assert!(no_history.errors[0].message.contains("history"));

    let denied = harness
        .execute_as(
            "mutation { cancelJobPostProcessing(jobId: 1) }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);
}
