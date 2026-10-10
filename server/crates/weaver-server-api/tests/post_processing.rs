mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use serde_json::{Value, json};
use weaver_server_api::auth::CallerScope;

// Every field of a saved instance.
const INSTANCE: &str = "id name script trigger queueEvent inputs { name value secret { id name } sealed } categories enabled blocking timeoutSeconds runOrder scriptProblem headerDrift";

// Write a bare script into the harness's `data_dir/scripts`.
async fn write_script(harness: &TestHarness, name: &str, body: &str) {
    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    let scripts = data_dir.join("scripts");
    std::fs::create_dir_all(&scripts).unwrap();
    std::fs::write(scripts.join(name), body).unwrap();
}

// Create an instance from the fields of a `ScriptInstanceInput`.
async fn create_instance(harness: &TestHarness, input: &str) -> Value {
    let response = harness
        .execute(&format!(
            "mutation {{ createScriptInstance(input: {{ {input} }}) {{ {INSTANCE} }} }}"
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["createScriptInstance"].clone()
}

// Every saved instance, in run order.
async fn instances(harness: &TestHarness) -> Vec<Value> {
    let response = harness
        .execute(&format!("{{ scriptInstances {{ {INSTANCE} }} }}"))
        .await;
    assert_no_errors(&response);
    response_data(&response)["scriptInstances"]
        .as_array()
        .unwrap()
        .clone()
}

fn id(instance: &Value) -> String {
    instance["id"].as_str().unwrap().to_string()
}

fn ids(instances: &[Value]) -> Vec<String> {
    instances.iter().map(id).collect()
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
            "{ postProcessingSettings { scriptDirectory executionEnabled concurrency terminationGraceSeconds unacceptableExtensions strictSecurityRefusesExecution globalScriptsRun } }",
        )
        .await;
    assert_no_errors(&response);
    let settings = &response_data(&response)["postProcessingSettings"];
    assert!(std::path::Path::new(settings["scriptDirectory"].as_str().unwrap()).is_absolute());
    assert_eq!(settings["executionEnabled"], false);
    assert_eq!(settings["concurrency"], 4);
    assert_eq!(settings["terminationGraceSeconds"], 10);
    assert_eq!(
        settings["unacceptableExtensions"],
        serde_json::json!([
            "bat", "cmd", "com", "exe", "js", "lnk", "msi", "ps1", "scr", "vbs"
        ])
    );
    assert_eq!(settings["strictSecurityRefusesExecution"], false);
    assert_eq!(settings["globalScriptsRun"], "ALWAYS");
}

#[tokio::test]
async fn scripts_directory_is_admin_owned_and_turns_every_instance_off_when_changed() {
    let harness = TestHarness::new().await;
    let denied = harness
        .execute_as(
            r#"mutation { setPostProcessingScriptDirectory(directory: "/tmp/scripts") { scriptDirectory } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);

    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    let instance = create_instance(
        &harness,
        r#"script: "notify.sh", trigger: POST_PROCESSING, inputs: [{ name: "Host", value: "example.invalid" }]"#,
    )
    .await;
    assert_eq!(instance["enabled"], true);
    assert!(instance["scriptProblem"].is_null());

    let root = tempfile::tempdir().unwrap();
    let requested = root.path().join("nested/scripts");
    let requested_gql = serde_json::to_string(&requested).unwrap();
    let response = harness
        .execute(&format!(
            "mutation {{ setPostProcessingScriptDirectory(directory: {requested_gql}) {{ scriptDirectory }} }}"
        ))
        .await;
    assert_no_errors(&response);
    let canonical = std::fs::canonicalize(&requested).unwrap();
    assert_eq!(
        response_data(&response)["setPostProcessingScriptDirectory"]["scriptDirectory"],
        &*canonical.to_string_lossy()
    );

    // A name in the new directory is not the script that was wired up, so the
    // instance is kept as it was saved and turned off.
    let saved = instances(&harness).await;
    assert_eq!(ids(&saved), [id(&instance)]);
    assert_eq!(saved[0]["enabled"], false);
    assert_eq!(
        saved[0]["inputs"],
        json!([{ "name": "Host", "value": "example.invalid", "secret": null, "sealed": false }])
    );
    assert!(
        saved[0]["scriptProblem"]
            .as_str()
            .unwrap()
            .contains("no longer in the scripts directory")
    );

    std::fs::write(
        canonical.join("replacement.sh"),
        "#!/bin/sh\necho replacement\n",
    )
    .unwrap();
    let listing = harness
        .execute("{ discoveredScripts { scripts { name } } }")
        .await;
    assert_no_errors(&listing);
    assert_eq!(
        response_data(&listing)["discoveredScripts"]["scripts"][0]["name"],
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
                goInterpreter: "/usr/local/go/bin/go"
                unacceptableExtensions: ["EXE", "r??"]
              }) {
                executionEnabled
                concurrency
                terminationGraceSeconds
                pythonInterpreter
                goInterpreter
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
    assert_eq!(settings["goInterpreter"], "/usr/local/go/bin/go");
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
async fn global_scripts_run_is_kept_until_it_is_sent_again() {
    let harness = TestHarness::new().await;
    let set = |value: &'static str| {
        let harness = &harness;
        async move {
            let response = harness
                .execute(&format!(
                    "mutation {{ setPostProcessingSettings(input: {{
                        executionEnabled: false
                        concurrency: 1
                        terminationGraceSeconds: 10
                        {value}
                    }}) {{ globalScriptsRun }} }}"
                ))
                .await;
            assert_no_errors(&response);
            response_data(&response)["setPostProcessingSettings"]["globalScriptsRun"].clone()
        }
    };
    assert_eq!(
        set("globalScriptsRun: ONLY_WITHOUT_CATEGORY_SCRIPTS").await,
        "ONLY_WITHOUT_CATEGORY_SCRIPTS"
    );
    assert_eq!(
        set("").await,
        "ONLY_WITHOUT_CATEGORY_SCRIPTS",
        "left out, it stays as it was"
    );
    assert_eq!(set("globalScriptsRun: ALWAYS").await, "ALWAYS");

    let persisted = harness
        .execute("{ postProcessingSettings { globalScriptsRun } }")
        .await;
    assert_no_errors(&persisted);
    assert_eq!(
        response_data(&persisted)["postProcessingSettings"]["globalScriptsRun"],
        "ALWAYS"
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
        .execute_as(
            "{ discoveredScripts { scripts { name } } }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);

    let response = harness
        .execute(
            "{ discoveredScripts { scripts { name displayName adapter } problems { name message } } }",
        )
        .await;
    assert_no_errors(&response);
    let listing = &response_data(&response)["discoveredScripts"];
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
async fn instances_are_created_read_reordered_and_deleted() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    write_script(&harness, "sort.sh", "#!/bin/sh\necho hi\n").await;

    let denied = harness
        .execute_as("{ scriptInstances { id } }", CallerScope::Read)
        .await;
    assert_has_errors(&denied);
    let denied = harness
        .execute_as(
            r#"mutation { createScriptInstance(input: { script: "notify.sh", trigger: POST_PROCESSING }) { id } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);
    assert!(instances(&harness).await.is_empty());

    let notify = create_instance(
        &harness,
        r#"name: "Tell me", script: "notify.sh", trigger: POST_PROCESSING, timeoutSeconds: 30"#,
    )
    .await;
    assert_eq!(notify["name"], "Tell me");
    assert_eq!(notify["script"], "notify.sh");
    assert_eq!(notify["trigger"], "POST_PROCESSING");
    assert!(notify["queueEvent"].is_null());
    assert_eq!(notify["timeoutSeconds"], 30);
    assert_eq!(notify["enabled"], true);
    assert_eq!(notify["categories"], json!([]));
    assert_eq!(notify["inputs"], json!([]));
    assert!(notify["scriptProblem"].is_null());
    assert_eq!(notify["headerDrift"], false);

    let sort = create_instance(
        &harness,
        r#"script: "sort.sh", trigger: POST_PROCESSING, categories: ["movies", "Movies", "tv"], enabled: false"#,
    )
    .await;
    assert_eq!(
        sort["name"], "sort.sh",
        "an instance without a name takes its script's"
    );
    assert_eq!(sort["categories"], json!(["movies", "tv"]));
    assert_eq!(sort["enabled"], false);
    assert!(sort["timeoutSeconds"].is_null());

    // The same script can be wired up as often as it is wanted.
    let added = create_instance(
        &harness,
        r#"script: "notify.sh", trigger: QUEUE, queueEvent: NZB_ADDED, categories: ["tv"]"#,
    )
    .await;
    assert_eq!(added["trigger"], "QUEUE");
    assert_eq!(added["queueEvent"], "NZB_ADDED");
    assert_eq!(added["categories"], json!(["tv"]));

    let all = instances(&harness).await;
    assert_eq!(ids(&all), [id(&notify), id(&sort), id(&added)]);
    assert_eq!(all[1], sort, "what was saved is what is read back");

    let one = harness
        .execute(&format!(
            r#"{{ scriptInstance(id: "{}") {{ name }} }}"#,
            id(&sort)
        ))
        .await;
    assert_no_errors(&one);
    assert_eq!(response_data(&one)["scriptInstance"]["name"], "sort.sh");
    let none = harness
        .execute(r#"{ scriptInstance(id: "nope") { name } }"#)
        .await;
    assert_no_errors(&none);
    assert!(response_data(&none)["scriptInstance"].is_null());

    // One trigger is put in order without moving the others.
    let reordered = harness
        .execute(&format!(
            r#"mutation {{ reorderScriptInstances(trigger: POST_PROCESSING, ids: ["{}"]) {{ id }} }}"#,
            id(&sort)
        ))
        .await;
    assert_no_errors(&reordered);
    let order = [id(&sort), id(&notify), id(&added)];
    assert_eq!(
        ids(response_data(&reordered)["reorderScriptInstances"]
            .as_array()
            .unwrap()),
        order
    );
    assert_eq!(ids(&instances(&harness).await), order);

    for refused in [
        // An instance of another trigger has no place among them.
        format!(r#"["{}"]"#, id(&added)),
        format!(r#"["{0}", "{0}"]"#, id(&sort)),
        r#"["nope"]"#.to_string(),
    ] {
        let response = harness
            .execute(&format!(
                "mutation {{ reorderScriptInstances(trigger: POST_PROCESSING, ids: {refused}) {{ id }} }}"
            ))
            .await;
        assert_has_errors(&response);
    }
    assert_eq!(ids(&instances(&harness).await), order);

    let delete = format!(
        r#"mutation {{ deleteScriptInstance(id: "{}") }}"#,
        id(&notify)
    );
    let denied = harness.execute_as(&delete, CallerScope::Read).await;
    assert_has_errors(&denied);
    let deleted = harness.execute(&delete).await;
    assert_no_errors(&deleted);
    assert_eq!(response_data(&deleted)["deleteScriptInstance"], true);
    let again = harness.execute(&delete).await;
    assert_no_errors(&again);
    assert_eq!(response_data(&again)["deleteScriptInstance"], false);
    assert_eq!(ids(&instances(&harness).await), [id(&sort), id(&added)]);
}

#[tokio::test]
async fn an_instance_that_cannot_be_saved_as_asked_is_refused() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    for refused in [
        // A script name that could escape the directory is refused outright.
        r#"script: "../escape", trigger: POST_PROCESSING"#,
        // An instance can only be pointed at a script that is there to run.
        r#"script: "gone.sh", trigger: POST_PROCESSING"#,
        r#"script: "notify.sh", trigger: QUEUE"#,
        r#"script: "notify.sh", trigger: SCAN, categories: ["movies"]"#,
        r#"script: "notify.sh", trigger: FEED, categories: ["movies"]"#,
        r#"script: "notify.sh", trigger: POST_PROCESSING, categories: [" "]"#,
        r#"script: "notify.sh", trigger: POST_PROCESSING, timeoutSeconds: 0"#,
        r#"script: "notify.sh", trigger: POST_PROCESSING, inputs: [{ name: "Host", value: "a" }, { name: "HOST", value: "b" }]"#,
        r#"script: "notify.sh", trigger: POST_PROCESSING, inputs: [{ name: "", value: "a" }]"#,
    ] {
        let response = harness
            .execute(&format!(
                "mutation {{ createScriptInstance(input: {{ {refused} }}) {{ id }} }}"
            ))
            .await;
        assert!(
            !response.errors.is_empty(),
            "{refused} was saved: {:?}",
            response.data
        );
    }
    assert!(instances(&harness).await.is_empty());

    let nothing_there = harness
        .execute(
            r#"mutation { updateScriptInstance(id: "nope", input: { script: "notify.sh", trigger: POST_PROCESSING }) { id } }"#,
        )
        .await;
    assert_has_errors(&nothing_there);
    assert!(instances(&harness).await.is_empty());
}

#[tokio::test]
async fn an_update_replaces_what_is_saved_and_may_keep_naming_a_script_that_has_gone() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    let created = create_instance(
        &harness,
        r#"script: "notify.sh", trigger: POST_PROCESSING, categories: ["movies"], inputs: [{ name: "Host", value: "a" }]"#,
    )
    .await;

    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    std::fs::remove_file(data_dir.join("scripts/notify.sh")).unwrap();
    let saved = instances(&harness).await;
    assert_eq!(
        saved[0]["scriptProblem"],
        "the script is no longer in the scripts directory"
    );
    assert_eq!(saved[0]["enabled"], true, "nothing is changed on reading");

    let update = |input: &'static str| {
        let harness = &harness;
        let id = id(&created);
        async move {
            harness
                .execute(&format!(
                    r#"mutation {{ updateScriptInstance(id: "{id}", input: {{ {input} }}) {{ {INSTANCE} }} }}"#
                ))
                .await
        }
    };
    let updated = update(
        r#"name: "Later", script: "notify.sh", trigger: SCAN, enabled: false, blocking: false, timeoutSeconds: 45, inputs: [{ name: "Port", value: "25" }]"#,
    )
    .await;
    assert_no_errors(&updated);
    let updated = &response_data(&updated)["updateScriptInstance"];
    assert_eq!(updated["id"], created["id"]);
    assert_eq!(updated["name"], "Later");
    assert_eq!(updated["trigger"], "SCAN");
    assert_eq!(updated["categories"], json!([]));
    assert_eq!(updated["enabled"], false);
    assert_eq!(updated["blocking"], false);
    assert_eq!(updated["timeoutSeconds"], 45);
    assert_eq!(updated["runOrder"], created["runOrder"]);
    assert_eq!(
        updated["inputs"],
        json!([{ "name": "Port", "value": "25", "secret": null, "sealed": false }])
    );
    assert_eq!(instances(&harness).await, vec![updated.clone()]);

    // Pointing it at another script needs that script to be there.
    let moved = update(r#"script: "other.sh", trigger: SCAN"#).await;
    assert_has_errors(&moved);
    assert_eq!(instances(&harness).await, vec![updated.clone()]);
}

fn error_code(response: &async_graphql::Response) -> String {
    match response.errors[0]
        .extensions
        .as_ref()
        .and_then(|extensions| extensions.get("code"))
    {
        Some(async_graphql::Value::String(code)) => code.clone(),
        other => panic!("no error code: {other:?}"),
    }
}

// Create a named secret and return what the API reports for it.
async fn create_secret(harness: &TestHarness, name: &str, value: &str) -> Value {
    let response = harness
        .execute(&format!(
            "mutation {{ createSecret(name: {}, value: {}) {{ id name createdAt updatedAt usedBy {{ id name }} }} }}",
            serde_json::to_string(name).unwrap(),
            serde_json::to_string(value).unwrap(),
        ))
        .await;
    assert_no_errors(&response);
    response_data(&response)["createSecret"].clone()
}

#[tokio::test]
async fn inputs_are_saved_as_sent_and_a_secret_is_never_read_back() {
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

    // The header is offered as a starting point and nothing more. A secret
    // input is a slot for a secret, never a value.
    let listing = harness
        .execute(
            "{ discoveredScripts { scripts { name options { name optionType defaultValue } preset { triggers { trigger queueEvent } taskTimes inputs { name value secret } } } } }",
        )
        .await;
    assert_no_errors(&listing);
    let script = &response_data(&listing)["discoveredScripts"]["scripts"][0];
    assert_eq!(script["name"], "email");
    let options = script["options"].as_array().unwrap();
    let host = options.iter().find(|o| o["name"] == "Host").unwrap();
    assert_eq!(host["defaultValue"], "mail.example.invalid");
    let token = options.iter().find(|o| o["name"] == "Token").unwrap();
    assert_eq!(token["optionType"], "SECRET");
    assert_eq!(
        script["preset"],
        json!({
            "triggers": [{ "trigger": "POST_PROCESSING", "queueEvent": null }],
            "taskTimes": [],
            "inputs": [
                { "name": "Host", "value": "mail.example.invalid", "secret": false },
                { "name": "Token", "value": "", "secret": true }
            ]
        })
    );

    // What is sent is what is saved, whatever the header declares. A secret
    // input is a link to a named secret.
    let mail = create_secret(&harness, "Mail token", "hunter2").await;
    let mail_id = mail["id"].as_str().unwrap().to_string();
    let response = harness
        .execute(&format!(
            r#"mutation {{ createScriptInstance(input: {{ script: "email", trigger: POST_PROCESSING, inputs: [
                {{ name: "Host", value: "smtp.example.invalid" }}
                {{ name: "Token", secretId: "{mail_id}" }}
                {{ name: "Extra", value: "x" }}
            ] }}) {{ {INSTANCE} }} }}"#
        ))
        .await;
    assert_no_errors(&response);
    assert!(!format!("{:?}", response.data).contains("hunter2"));
    let created = response_data(&response)["createScriptInstance"].clone();
    let linked = json!({
        "name": "Token",
        "value": "",
        "secret": { "id": mail_id, "name": "Mail token" },
        "sealed": false
    });
    assert_eq!(
        created["inputs"],
        json!([
            { "name": "Host", "value": "smtp.example.invalid", "secret": null, "sealed": false },
            linked,
            { "name": "Extra", "value": "x", "secret": null, "sealed": false }
        ]),
    );
    assert_eq!(
        created["headerDrift"], true,
        "the instance holds an input the header does not declare"
    );

    // An input is a value or a link, never both or neither, and a link has
    // to name a secret that exists.
    for inputs in [
        format!(r#"{{ name: "Token", value: "x", secretId: "{mail_id}" }}"#),
        r#"{ name: "Token" }"#.to_string(),
        r#"{ name: "Token", secretId: "missing" }"#.to_string(),
    ] {
        let refused = harness
            .execute(&format!(
                r#"mutation {{ createScriptInstance(input: {{ script: "email", trigger: POST_PROCESSING, inputs: [{inputs}] }}) {{ id }} }}"#
            ))
            .await;
        assert_has_errors(&refused);
    }

    // Everything is replaced by what is sent; a link is sent as a link.
    let updated = harness
        .execute(&format!(
            r#"mutation {{ updateScriptInstance(id: "{}", input: {{ script: "email", trigger: POST_PROCESSING, inputs: [
                {{ name: "Token", secretId: "{mail_id}" }}
                {{ name: "Extra", value: "y" }}
            ] }}) {{ inputs {{ name value secret {{ id name }} sealed }} headerDrift }} }}"#,
            id(&created)
        ))
        .await;
    assert_no_errors(&updated);
    let updated = &response_data(&updated)["updateScriptInstance"];
    assert_eq!(
        updated["inputs"],
        json!([linked, { "name": "Extra", "value": "y", "secret": null, "sealed": false }])
    );
    assert_eq!(updated["headerDrift"], true);

    // Bringing it back in line with the header keeps what the operator saved
    // under a name the header still declares, link included, and drops the
    // rest.
    let denied = harness
        .execute_as(
            &format!(
                r#"mutation {{ reapplyScriptHeader(id: "{}") {{ id }} }}"#,
                id(&created)
            ),
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);
    let reapplied = harness
        .execute(&format!(
            r#"mutation {{ reapplyScriptHeader(id: "{}") {{ inputs {{ name value secret {{ id name }} sealed }} headerDrift }} }}"#,
            id(&created)
        ))
        .await;
    assert_no_errors(&reapplied);
    let reapplied = &response_data(&reapplied)["reapplyScriptHeader"];
    assert_eq!(
        reapplied["inputs"],
        json!([
            { "name": "Host", "value": "mail.example.invalid", "secret": null, "sealed": false },
            linked
        ])
    );
    assert_eq!(reapplied["headerDrift"], false);

    let nothing_there = harness
        .execute(r#"mutation { reapplyScriptHeader(id: "nope") { id } }"#)
        .await;
    assert_has_errors(&nothing_there);

    // A secret of the instance's own is kept under a name the header
    // declares, in the header's spelling, and dropped with any other input
    // the header does not declare.
    let own = harness
        .execute(&format!(
            r#"mutation {{ updateScriptInstance(id: "{}", input: {{ script: "email", trigger: POST_PROCESSING, inputs: [
                {{ name: "token", value: "hunter5", secret: true }}
                {{ name: "Extra", value: "hunter6", secret: true }}
            ] }}) {{ inputs {{ name value secret {{ id name }} sealed }} headerDrift }} }}"#,
            id(&created)
        ))
        .await;
    assert_no_errors(&own);
    carries_no_secret(&own);
    let own = &response_data(&own)["updateScriptInstance"];
    assert_eq!(
        own["inputs"],
        json!([own_secret("token"), own_secret("Extra")])
    );
    assert_eq!(own["headerDrift"], true);
    let reapplied = harness
        .execute(&format!(
            r#"mutation {{ reapplyScriptHeader(id: "{}") {{ inputs {{ name value secret {{ id name }} sealed }} headerDrift }} }}"#,
            id(&created)
        ))
        .await;
    assert_no_errors(&reapplied);
    carries_no_secret(&reapplied);
    let reapplied = &response_data(&reapplied)["reapplyScriptHeader"];
    assert_eq!(
        reapplied["inputs"],
        json!([
            { "name": "Host", "value": "mail.example.invalid", "secret": null, "sealed": false },
            own_secret("Token")
        ])
    );
    assert_eq!(reapplied["headerDrift"], false);
}

// What is read back for an input holding a secret of the instance's own.
fn own_secret(name: &str) -> Value {
    json!({ "name": name, "value": "", "secret": null, "sealed": true })
}

// What is read back for an input holding a plain value.
fn plain(name: &str, value: &str) -> Value {
    json!({ "name": name, "value": value, "secret": null, "sealed": false })
}

// Every secret value in these tests starts `hunter`, and anything sealed
// starts `enc:v1:`; a response carries neither.
fn carries_no_secret(response: &async_graphql::Response) {
    let text = format!("{response:?}");
    assert!(
        !text.contains("hunter") && !text.contains("enc:v1:"),
        "a secret was read back: {text}"
    );
}

#[tokio::test]
async fn an_input_may_be_a_secret_of_the_instances_own_and_is_never_read_back() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\nexit 0\n").await;
    let named = create_secret(&harness, "Notify token", "hunter2").await;
    let named_id = named["id"].as_str().unwrap().to_string();
    let linked = |name: &str| {
        json!({
            "name": name,
            "value": "",
            "secret": { "id": named_id, "name": "Notify token" },
            "sealed": false
        })
    };
    let kept_named =
        |used_by: Value| json!([{ "id": named_id, "name": "Notify token", "usedBy": used_by }]);

    // An input is a plain value, a link to a named secret, or a secret of the
    // instance's own. Leaving `secret` out, or sending it as false or null,
    // is a plain value.
    let created = harness
        .execute(&format!(
            r#"mutation {{ createScriptInstance(input: {{ name: "Notify", script: "notify.sh", trigger: POST_PROCESSING, inputs: [
                {{ name: "Host", value: "example.invalid" }}
                {{ name: "Token", secretId: "{named_id}" }}
                {{ name: "Password", value: "hunter7", secret: true }}
                {{ name: "Shown", value: "a", secret: false }}
                {{ name: "Unmarked", value: "b", secret: null }}
            ] }}) {{ {INSTANCE} }} }}"#
        ))
        .await;
    assert_no_errors(&created);
    carries_no_secret(&created);
    let instance = response_data(&created)["createScriptInstance"].clone();
    let instance_id = id(&instance);
    assert_eq!(
        instance["inputs"],
        json!([
            plain("Host", "example.invalid"),
            linked("Token"),
            own_secret("Password"),
            plain("Shown", "a"),
            plain("Unmarked", "b")
        ])
    );
    let read = harness
        .execute(&format!("{{ scriptInstances {{ {INSTANCE} }} }}"))
        .await;
    assert_no_errors(&read);
    carries_no_secret(&read);
    assert_eq!(
        response_data(&read)["scriptInstances"],
        json!([instance.clone()])
    );

    // It is no named secret: nothing lists it, so nothing else can link it.
    let secrets = "{ secrets { id name usedBy { id name } } }";
    let listing = harness.execute(secrets).await;
    assert_no_errors(&listing);
    carries_no_secret(&listing);
    assert_eq!(
        response_data(&listing)["secrets"],
        kept_named(json!([{ "id": instance_id, "name": "Notify" }]))
    );

    // A new instance holds nothing to keep, and a secret of its own is never
    // also a link.
    let kept_nothing = |name: &str| {
        format!(
            r#"input "{name}" is marked secret but was given no value, and none is saved for it"#
        )
    };
    let not_both = "an input is a secret of its own or a link to a named secret, not both";
    for (inputs, message) in [
        (
            r#"{ name: "Password", secret: true }"#.to_string(),
            kept_nothing("Password"),
        ),
        (
            format!(r#"{{ name: "Password", secret: true, secretId: "{named_id}" }}"#),
            not_both.to_string(),
        ),
        (
            format!(
                r#"{{ name: "Password", value: "hunter9", secret: true, secretId: "{named_id}" }}"#
            ),
            not_both.to_string(),
        ),
    ] {
        let refused = harness
            .execute(&format!(
                r#"mutation {{ createScriptInstance(input: {{ script: "notify.sh", trigger: POST_PROCESSING, inputs: [{inputs}] }}) {{ id }} }}"#
            ))
            .await;
        assert_has_errors(&refused);
        carries_no_secret(&refused);
        assert!(
            refused.errors[0].message.contains(&message),
            "{inputs} was refused with {:?}",
            refused.errors[0].message
        );
    }
    assert_eq!(
        ids(&instances(&harness).await),
        std::slice::from_ref(&instance_id)
    );

    let update = |inputs: String| {
        let harness = &harness;
        let instance_id = instance_id.clone();
        async move {
            let response = harness
                .execute(&format!(
                    r#"mutation {{ updateScriptInstance(id: "{instance_id}", input: {{ name: "Notify", script: "notify.sh", trigger: POST_PROCESSING, inputs: [{inputs}] }}) {{ {INSTANCE} }} }}"#
                ))
                .await;
            carries_no_secret(&response);
            response
        }
    };
    let saved = |response: async_graphql::Response| {
        assert_no_errors(&response);
        response_data(&response)["updateScriptInstance"]["inputs"].clone()
    };

    // Marked secret and sent without a value, an input keeps the secret the
    // instance already holds under that name, however the name is spelled
    // and wherever it now stands.
    let kept = update(format!(
        r#"{{ name: "PASSWORD", secret: true }} {{ name: "Token", secretId: "{named_id}" }} {{ name: "Host", value: "b" }}"#
    ))
    .await;
    assert_eq!(
        saved(kept),
        json!([own_secret("PASSWORD"), linked("Token"), plain("Host", "b")])
    );

    // There is nothing to keep under a name that holds a plain value, a
    // link, or nothing at all, and the refusal names the input. What is
    // saved stays as it was.
    let before = instances(&harness).await;
    for name in ["Host", "Token", "Other"] {
        let refused = update(format!(
            r#"{{ name: "PASSWORD", secret: true }} {{ name: "{name}", secret: true }}"#
        ))
        .await;
        assert_has_errors(&refused);
        assert!(
            refused.errors[0].message.contains(&kept_nothing(name)),
            "{name} was refused with {:?}",
            refused.errors[0].message
        );
    }
    let both = update(format!(
        r#"{{ name: "PASSWORD", secret: true, secretId: "{named_id}" }}"#
    ))
    .await;
    assert_has_errors(&both);
    assert!(both.errors[0].message.contains(not_both));
    let nothing_there = harness
        .execute(
            r#"mutation { updateScriptInstance(id: "nope", input: { script: "notify.sh", trigger: POST_PROCESSING, inputs: [{ name: "PASSWORD", secret: true }] }) { id } }"#,
        )
        .await;
    assert_has_errors(&nothing_there);
    carries_no_secret(&nothing_there);
    assert_eq!(instances(&harness).await, before);

    // A new value replaces the one that was kept.
    let replaced = update(r#"{ name: "Password", value: "hunter8", secret: true }"#.to_string());
    assert_eq!(saved(replaced.await), json!([own_secret("Password")]));
    let listing = harness.execute(secrets).await;
    assert_eq!(response_data(&listing)["secrets"], kept_named(json!([])));

    // Sent as a plain value or as a link, the input stops being a secret of
    // the instance's own, and nothing is left to keep.
    let keep = || update(r#"{ name: "Password", secret: true }"#.to_string());
    let own = || update(r#"{ name: "Password", value: "hunter7", secret: true }"#.to_string());
    let shown = update(r#"{ name: "Password", value: "shown" }"#.to_string()).await;
    assert_eq!(saved(shown), json!([plain("Password", "shown")]));
    assert_has_errors(&keep().await);
    assert_eq!(saved(own().await), json!([own_secret("Password")]));
    let relinked = update(format!(r#"{{ name: "Password", secretId: "{named_id}" }}"#)).await;
    assert_eq!(saved(relinked), json!([linked("Password")]));
    assert_has_errors(&keep().await);

    // Left out, it is removed with every other input that was not sent.
    assert_eq!(saved(own().await), json!([own_secret("Password")]));
    assert_eq!(saved(keep().await), json!([own_secret("Password")]));
    assert_eq!(saved(update(String::new()).await), json!([]));
    assert_has_errors(&keep().await);

    // It goes with its instance, and was never a named secret to be left
    // behind.
    assert_eq!(saved(own().await), json!([own_secret("Password")]));
    let deleted = harness
        .execute(&format!(
            r#"mutation {{ deleteScriptInstance(id: "{instance_id}") }}"#
        ))
        .await;
    assert_no_errors(&deleted);
    assert_eq!(response_data(&deleted)["deleteScriptInstance"], true);
    assert!(instances(&harness).await.is_empty());
    let listing = harness.execute(secrets).await;
    assert_eq!(response_data(&listing)["secrets"], kept_named(json!([])));
    assert_eq!(harness.db.secrets().unwrap().len(), 1);
}

#[tokio::test]
async fn secrets_are_admin_only_named_once_kept_while_linked_and_never_read_back() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\nexit 0\n").await;

    // Listing names needs an administrator; changing them a fresh one.
    let listed = harness
        .execute_as("{ secrets { id name } }", CallerScope::Read)
        .await;
    assert_has_errors(&listed);
    let listed = harness
        .execute_as("{ secrets { id name } }", CallerScope::Control)
        .await;
    assert_has_errors(&listed);
    let denied = harness
        .execute_as(
            r#"mutation { createSecret(name: "Token", value: "hunter2") { id } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);
    assert!(harness.db.secrets().unwrap().is_empty());

    let mut responses = Vec::new();
    let token = create_secret(&harness, "Notify token", "hunter2").await;
    let token_id = token["id"].as_str().unwrap().to_string();
    assert_eq!(token["name"], "Notify token");
    assert_eq!(token["usedBy"], json!([]));
    assert_eq!(token["createdAt"], token["updatedAt"]);

    let taken = harness
        .execute(r#"mutation { createSecret(name: "NOTIFY TOKEN", value: "other") { id } }"#)
        .await;
    assert_has_errors(&taken);
    assert_eq!(error_code(&taken), "NAME_TAKEN");
    responses.push(taken);
    let blank = harness
        .execute(r#"mutation { createSecret(name: "  ", value: "other") { id } }"#)
        .await;
    assert_eq!(error_code(&blank), "INVALID_INPUT");

    let instance = create_instance(
        &harness,
        &format!(
            r#"name: "Notify", script: "notify.sh", trigger: POST_PROCESSING, inputs: [{{ name: "Token", secretId: "{token_id}" }}]"#
        ),
    )
    .await;

    // Renaming it shows on every link.
    let rotated = harness
        .execute(&format!(
            r#"mutation {{ updateSecret(id: "{token_id}", name: "Renamed token", value: "hunter3") {{ id name usedBy {{ id name }} }} }}"#
        ))
        .await;
    assert_no_errors(&rotated);
    assert_eq!(
        response_data(&rotated)["updateSecret"],
        json!({
            "id": token_id,
            "name": "Renamed token",
            "usedBy": [{ "id": id(&instance), "name": "Notify" }]
        })
    );
    responses.push(rotated);
    let missing = harness
        .execute(r#"mutation { updateSecret(id: "missing", value: "x") { id } }"#)
        .await;
    assert_eq!(error_code(&missing), "NOT_FOUND");

    // A linked secret cannot be deleted, and the refusal names who links it.
    let in_use = harness
        .execute(&format!(r#"mutation {{ deleteSecret(id: "{token_id}") }}"#))
        .await;
    assert_has_errors(&in_use);
    assert_eq!(error_code(&in_use), "SECRET_IN_USE");
    assert!(in_use.errors[0].message.contains("Notify"));
    responses.push(in_use);

    let listing = harness
        .execute("{ secrets { id name createdAt updatedAt usedBy { id name } } }")
        .await;
    assert_no_errors(&listing);
    assert_eq!(
        response_data(&listing)["secrets"][0]["usedBy"],
        json!([{ "id": id(&instance), "name": "Notify" }])
    );
    responses.push(listing);
    responses.push(
        harness
            .execute(&format!("{{ scriptInstances {{ {INSTANCE} }} }}"))
            .await,
    );

    // No response ever carries a value, however the secret was reached.
    for response in &responses {
        let text = format!("{response:?}");
        assert!(!text.contains("hunter2") && !text.contains("hunter3"));
    }

    let deleted = harness
        .execute(&format!(
            r#"mutation {{ deleteScriptInstance(id: "{}") }}"#,
            id(&instance)
        ))
        .await;
    assert_no_errors(&deleted);
    let removed = harness
        .execute(&format!(r#"mutation {{ deleteSecret(id: "{token_id}") }}"#))
        .await;
    assert_no_errors(&removed);
    let again = harness
        .execute(&format!(r#"mutation {{ deleteSecret(id: "{token_id}") }}"#))
        .await;
    assert_eq!(error_code(&again), "NOT_FOUND");
    assert!(harness.db.secrets().unwrap().is_empty());
}

// The secret mutations, each aimed at `id`.
fn secret_mutations(id: &str) -> [String; 3] {
    [
        r#"mutation { createSecret(name: "From outside", value: "hunter9") { id } }"#.to_string(),
        format!(
            r#"mutation {{ updateSecret(id: "{id}", name: "Changed", value: "hunter9") {{ id }} }}"#
        ),
        format!(r#"mutation {{ deleteSecret(id: "{id}") }}"#),
    ]
}

// The names of the stored secrets, with when each was last changed.
fn stored_secrets(harness: &TestHarness) -> Vec<(String, i64)> {
    harness
        .db
        .secrets()
        .unwrap()
        .into_iter()
        .map(|secret| (secret.name, secret.updated_at_ms))
        .collect()
}

#[tokio::test]
async fn a_script_runs_token_may_not_create_change_or_delete_a_secret() {
    let harness = TestHarness::new().await;
    let kept = harness.db.create_secret("Kept", "hunter2").unwrap();
    let before = stored_secrets(&harness);
    for mutation in secret_mutations(&kept.id) {
        let response = harness
            .schema
            .execute(
                async_graphql::Request::new(mutation.as_str())
                    .data(CallerScope::ScriptRun)
                    .data(weaver_server_api::auth::CallerIdentity::ScriptRun(
                        "run-1".into(),
                    )),
            )
            .await;
        assert_has_errors(&response);
        assert_eq!(
            error_code(&response),
            "NOT_ALLOWED_FOR_SCRIPT_RUN",
            "{mutation}"
        );
    }
    assert_eq!(stored_secrets(&harness), before);
}

#[tokio::test]
async fn a_browser_session_without_a_recent_password_check_may_add_a_secret_but_not_change_one() {
    use weaver_server_core::security::{AUTHENTICATED_POLICY_REVISION, RuntimeSecurityConfig};

    let security = RuntimeSecurityConfig::default();
    security.apply_stored_access_policy_revision(None, Some(AUTHENTICATED_POLICY_REVISION), true);
    let harness = TestHarness::new_with_security(security).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // Signed in an hour ago, and the password not typed since.
    harness
        .db
        .create_browser_session(&weaver_server_core::auth::BrowserSession {
            token_hash: "07".repeat(32),
            csrf_verifier: "test-verifier".into(),
            origin: "https://test".into(),
            client_ip: None,
            remembered: false,
            created_at: now - 3600,
            expires_at: now + 3600,
            revoked_at: None,
        })
        .unwrap();
    let kept = harness.db.create_secret("Kept", "hunter2").unwrap();
    let before = stored_secrets(&harness);
    let [create, change, delete] = secret_mutations(&kept.id);
    for mutation in [change, delete] {
        let response = harness
            .schema
            .execute(
                async_graphql::Request::new(mutation.as_str())
                    .data(CallerScope::Admin)
                    .data(weaver_server_api::auth::CallerIdentity::Jwt([7; 32])),
            )
            .await;
        assert_has_errors(&response);
        assert_eq!(error_code(&response), "REAUTH_REQUIRED", "{mutation}");
    }
    assert_eq!(stored_secrets(&harness), before);

    // Adding one touches nothing already saved, and is not held back.
    let added = harness
        .schema
        .execute(
            async_graphql::Request::new(create.as_str())
                .data(CallerScope::Admin)
                .data(weaver_server_api::auth::CallerIdentity::Jwt([7; 32])),
        )
        .await;
    assert_no_errors(&added);
    let mut after = stored_secrets(&harness);
    after.retain(|(name, _)| name != "From outside");
    assert_eq!(after, before);

    // Listing names needs no fresh password check.
    let listed = harness
        .schema
        .execute(
            async_graphql::Request::new("{ secrets { name } }")
                .data(CallerScope::Admin)
                .data(weaver_server_api::auth::CallerIdentity::Jwt([7; 32])),
        )
        .await;
    assert_no_errors(&listed);
    let mut names: Vec<String> = response_data(&listed)["secrets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|secret| secret["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, ["From outside", "Kept"]);
}

#[tokio::test]
async fn a_script_is_set_up_from_its_header_once_and_its_run_times_go_on_the_job() {
    let harness = TestHarness::new().await;
    write_script(
        &harness,
        "nightly.sh",
        "#!/bin/sh\n### NZBGET POST-PROCESSING/SCHEDULER SCRIPT ###\n### TASK TIME: 03:30;*:15;* ###\nexit 93\n",
    )
    .await;

    let listing = harness
        .execute("{ discoveredScripts { scripts { name preset { triggers { trigger queueEvent } taskTimes } } } }")
        .await;
    assert_no_errors(&listing);
    assert_eq!(
        response_data(&listing)["discoveredScripts"]["scripts"][0]["preset"]["triggers"],
        json!([
            { "trigger": "POST_PROCESSING", "queueEvent": null },
            { "trigger": "SCHEDULER", "queueEvent": null }
        ])
    );

    let set_up = r#"mutation { setUpScriptFromHeader(script: "nightly.sh") { id name script trigger enabled schedule { days times runAtStartup } } }"#;
    let denied = harness.execute_as(set_up, CallerScope::Read).await;
    assert_has_errors(&denied);
    let added = harness.execute(set_up).await;
    assert_no_errors(&added);
    let added = response_data(&added)["setUpScriptFromHeader"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        added
            .iter()
            .map(|instance| instance["trigger"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["POST_PROCESSING", "SCHEDULER"]
    );
    assert!(added.iter().all(|instance| instance["enabled"] == true));
    assert_eq!(ids(&instances(&harness).await), ids(&added));
    assert_eq!(
        added[0]["schedule"],
        json!({ "days": [], "times": [], "runAtStartup": false })
    );
    assert_eq!(
        added[1]["schedule"],
        json!({ "days": [], "times": ["*:15", "03:30"], "runAtStartup": true })
    );

    // The Schedules screen never lists scripts.
    let rules = harness.execute("{ schedules { id } }").await;
    assert_no_errors(&rules);
    assert_eq!(response_data(&rules)["schedules"], json!([]));

    // The Schedules screen never lists scripts.
    let rules = harness.execute("{ schedules { id } }").await;
    assert_no_errors(&rules);
    assert_eq!(response_data(&rules)["schedules"], json!([]));

    // The header is not read again for what is already wired up.
    let again = harness.execute(set_up).await;
    assert_no_errors(&again);
    assert_eq!(response_data(&again)["setUpScriptFromHeader"], json!([]));
    assert_eq!(instances(&harness).await.len(), 2);

    let missing = harness
        .execute(r#"mutation { setUpScriptFromHeader(script: "gone.sh") { id } }"#)
        .await;
    assert_has_errors(&missing);
}

#[tokio::test]
async fn a_schedule_job_keeps_its_run_times_and_another_trigger_drops_them() {
    let harness = TestHarness::new().await;
    write_script(
        &harness,
        "nightly.sh",
        "#!/bin/sh\n### NZBGET SCHEDULER SCRIPT ###\nexit 93\n",
    )
    .await;
    let created = create_instance(
        &harness,
        r#"script: "nightly.sh", trigger: SCHEDULER, schedule: { days: ["sat", "mon"], times: ["23:00", "*:5", "01:00"], runAtStartup: true }"#,
    )
    .await;
    let job = id(&created);
    let read = harness
        .execute("{ scriptInstances { id schedule { days times runAtStartup } } }")
        .await;
    assert_no_errors(&read);
    assert_eq!(
        response_data(&read)["scriptInstances"][0]["schedule"],
        json!({ "days": ["mon", "sat"], "times": ["*:05", "01:00", "23:00"], "runAtStartup": true })
    );

    for refused in [
        r#"schedule: { times: ["25:00"] }"#,
        r#"schedule: { times: ["*"] }"#,
        r#"schedule: { days: ["someday"] }"#,
    ] {
        let response = harness
            .execute(&format!(
                r#"mutation {{ updateScriptInstance(id: "{job}", input: {{ script: "nightly.sh", trigger: SCHEDULER, {refused} }}) {{ id }} }}"#
            ))
            .await;
        assert_has_errors(&response);
    }

    let moved = harness
        .execute(&format!(
            r#"mutation {{ updateScriptInstance(id: "{job}", input: {{ script: "nightly.sh", trigger: SCAN, schedule: {{ times: ["01:00"] }} }}) {{ schedule {{ days times runAtStartup }} }} }}"#
        ))
        .await;
    assert_no_errors(&moved);
    assert_eq!(
        response_data(&moved)["updateScriptInstance"]["schedule"],
        json!({ "days": [], "times": [], "runAtStartup": false })
    );

    let deleted = harness
        .execute(&format!(
            r#"mutation {{ deleteScriptInstance(id: "{job}") }}"#
        ))
        .await;
    assert_no_errors(&deleted);
    assert_eq!(response_data(&deleted)["deleteScriptInstance"], true);
    let missing = harness
        .execute(r#"mutation { deleteScriptInstance(id: "missing") }"#)
        .await;
    assert_no_errors(&missing);
    assert_eq!(response_data(&missing)["deleteScriptInstance"], false);
}

#[tokio::test]
async fn an_update_that_leaves_out_the_schedule_keeps_the_saved_one() {
    let harness = TestHarness::new().await;
    write_script(
        &harness,
        "nightly.sh",
        "#!/bin/sh\n### NZBGET SCHEDULER SCRIPT ###\nexit 93\n",
    )
    .await;
    let created = create_instance(
        &harness,
        r#"script: "nightly.sh", trigger: SCHEDULER, schedule: { days: ["mon"], times: ["23:00"], runAtStartup: true }"#,
    )
    .await;
    let job = id(&created);
    let kept = json!({ "days": ["mon"], "times": ["23:00"], "runAtStartup": true });

    for left_out in ["", "schedule: null,"] {
        let updated = harness
            .execute(&format!(
                r#"mutation {{ updateScriptInstance(id: "{job}", input: {{ {left_out} name: "Renamed", script: "nightly.sh", trigger: SCHEDULER }}) {{ name schedule {{ days times runAtStartup }} }} }}"#
            ))
            .await;
        assert_no_errors(&updated);
        assert_eq!(
            response_data(&updated)["updateScriptInstance"]["name"],
            "Renamed"
        );
        assert_eq!(
            response_data(&updated)["updateScriptInstance"]["schedule"],
            kept
        );
    }

    // Given, it replaces the saved one.
    let replaced = harness
        .execute(&format!(
            r#"mutation {{ updateScriptInstance(id: "{job}", input: {{ script: "nightly.sh", trigger: SCHEDULER, schedule: {{ times: ["01:00"] }} }}) {{ schedule {{ days times runAtStartup }} }} }}"#
        ))
        .await;
    assert_no_errors(&replaced);
    assert_eq!(
        response_data(&replaced)["updateScriptInstance"]["schedule"],
        json!({ "days": [], "times": ["01:00"], "runAtStartup": false })
    );
}

#[tokio::test]
async fn an_instance_is_waited_for_unless_it_says_otherwise() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    let waited =
        create_instance(&harness, r#"script: "notify.sh", trigger: POST_PROCESSING"#).await;
    assert_eq!(
        waited["blocking"], true,
        "an instance is waited for unless it says otherwise"
    );
    for trigger in [
        "POST_PROCESSING",
        "QUEUE, queueEvent: NZB_DOWNLOADED",
        "SCAN",
        "SCHEDULER",
        "FEED",
    ] {
        let detached = create_instance(
            &harness,
            &format!(r#"script: "notify.sh", trigger: {trigger}, blocking: false"#),
        )
        .await;
        assert_eq!(detached["blocking"], false, "{trigger}");
    }
    let saved = instances(&harness).await;
    assert_eq!(saved.len(), 6);
    assert_eq!(saved[0]["blocking"], true);
    assert!(
        saved[1..]
            .iter()
            .all(|instance| instance["blocking"] == false)
    );
}

#[tokio::test]
async fn a_test_run_needs_scripts_switched_on_and_an_instance_to_run() {
    let harness = TestHarness::new().await;
    write_script(&harness, "notify.sh", "#!/bin/sh\necho hi\n").await;
    let instance = create_instance(&harness, r#"script: "notify.sh", trigger: SCAN"#).await;
    let test = format!(
        r#"mutation {{ testScriptInstance(id: "{}") {{ id }} }}"#,
        id(&instance)
    );

    let denied = harness.execute_as(&test, CallerScope::Read).await;
    assert_has_errors(&denied);
    // Execution is off until an administrator turns it on.
    let switched_off = harness.execute(&test).await;
    assert_has_errors(&switched_off);

    let switched_on = harness
        .execute(
            r#"mutation { setPostProcessingSettings(input: {
                executionEnabled: true
                concurrency: 1
                terminationGraceSeconds: 10
            }) { executionEnabled } }"#,
        )
        .await;
    assert_no_errors(&switched_on);
    let nothing_there = harness
        .execute(r#"mutation { testScriptInstance(id: "nope") { id } }"#)
        .await;
    assert_has_errors(&nothing_there);

    let run = harness
        .execute(r#"{ scriptTestRun(id: "nope") { id } }"#)
        .await;
    assert_no_errors(&run);
    assert!(response_data(&run)["scriptTestRun"].is_null());
    let cancelled = harness
        .execute(r#"mutation { cancelScriptTest(id: "nope") }"#)
        .await;
    assert_no_errors(&cancelled);
    assert_eq!(response_data(&cancelled)["cancelScriptTest"], false);
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

#[tokio::test]
async fn recorded_runs_are_listed_in_pages_for_any_reader() {
    use weaver_server_core::post_processing::model::{
        ScriptAdapter, ScriptEventLabel, ScriptName, ScriptResult, ScriptStatus,
    };
    use weaver_server_core::post_processing::output::retain_output;

    let harness = TestHarness::new().await;
    write_script(
        &harness,
        "hourly.sh",
        "#!/bin/sh\n### NZBGET SCHEDULER SCRIPT ###\n",
    )
    .await;
    let hourly = create_instance(
        &harness,
        r#"name: "Every hour", script: "hourly.sh", trigger: SCHEDULER"#,
    )
    .await;
    let hourly_id = id(&hourly);
    let recorded = [
        (
            ScriptEventLabel::Scheduler(1),
            "nightly.sh",
            false,
            None,
            ScriptStatus::Succeeded,
        ),
        (
            ScriptEventLabel::Scan,
            "scan.sh",
            false,
            None,
            ScriptStatus::Failed,
        ),
        (
            ScriptEventLabel::Scheduler(2),
            "hourly.sh",
            true,
            Some((hourly_id.as_str(), "Every hour")),
            ScriptStatus::Succeeded,
        ),
    ];
    for (event, script, background, instance, status) in recorded {
        retain_output(
            harness.db.clone(),
            None,
            ScriptResult {
                script: ScriptName::new(script).unwrap(),
                instance_id: instance.map(|(id, _)| id.to_string()),
                instance_name: instance.map(|(_, name)| name.to_string()),
                event,
                output_id: None,
                background,
                adapter: ScriptAdapter::Nzbget,
                status,
                exit_code: Some(93),
                duration_ms: 5,
                output_tail: String::new(),
                output_truncated: false,
                error_message: None,
                finished_at_epoch_ms: 1_000,
            },
            format!("{script} output").into_bytes(),
            format!("{script} output").len() as u64,
            Default::default(),
        )
        .await
        .unwrap();
    }

    let fields = "runs { id jobId jobName script instanceId instanceName event kind background adapter status exitCode durationMs outputTail outputTruncated outputRetained errorMessage finishedAtEpochMs } nextBefore total";
    let first = harness
        .execute_as(
            &format!("{{ scriptRuns(limit: 2) {{ {fields} }} }}"),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&first);
    let first = &response_data(&first)["scriptRuns"];
    let runs = first["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(
        first["total"], 3,
        "the total counts every page, not this one"
    );
    assert_eq!(runs[0]["script"], "hourly.sh");
    assert_eq!(runs[0]["instanceId"], hourly_id);
    assert_eq!(runs[0]["instanceName"], "Every hour");
    assert_eq!(runs[0]["event"], "scheduler:2");
    assert_eq!(runs[0]["kind"], "SCHEDULER");
    assert_eq!(runs[0]["background"], true);
    assert_eq!(runs[0]["status"], "SUCCEEDED");
    assert_eq!(runs[0]["exitCode"], 93);
    assert_eq!(runs[0]["outputTail"], "hourly.sh output");
    assert_eq!(runs[0]["outputRetained"], true);
    assert!(runs[0]["jobId"].is_null());
    assert!(runs[0]["jobName"].is_null());
    assert_eq!(runs[1]["script"], "scan.sh");
    assert!(runs[1]["instanceId"].is_null());
    assert!(runs[1]["instanceName"].is_null());
    assert_eq!(runs[1]["kind"], "SCAN");
    assert_eq!(runs[1]["background"], false);

    // A run that belongs to no job has its output behind the same id.
    let output = harness
        .execute_as(
            &format!(
                r#"{{ scriptOutput(outputId: "{}") }}"#,
                runs[1]["id"].as_str().unwrap()
            ),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&output);
    assert_eq!(response_data(&output)["scriptOutput"], "scan.sh output");

    let before = first["nextBefore"].as_str().unwrap();
    let rest = harness
        .execute_as(
            &format!(r#"{{ scriptRuns(limit: 2, before: "{before}") {{ {fields} }} }}"#),
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&rest);
    let rest = &response_data(&rest)["scriptRuns"];
    assert_eq!(rest["runs"].as_array().unwrap().len(), 1);
    assert_eq!(rest["runs"][0]["script"], "nightly.sh");
    assert_eq!(rest["total"], 3);
    assert!(
        rest["nextBefore"].is_null(),
        "nothing follows the last page"
    );

    let scheduled = harness
        .execute_as(
            "{ scriptRuns(kind: SCHEDULER, script: \"nightly.sh\") { runs { script } nextBefore total } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&scheduled);
    let scheduled = &response_data(&scheduled)["scriptRuns"];
    assert_eq!(scheduled["runs"].as_array().unwrap().len(), 1);
    assert_eq!(scheduled["total"], 1, "the total is of the filtered runs");
    assert!(scheduled["nextBefore"].is_null());

    let failed = harness
        .execute_as(
            "{ scriptRuns(status: FAILED) { runs { script status } total statusCounts { status count } } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&failed);
    let failed = &response_data(&failed)["scriptRuns"];
    assert_eq!(failed["runs"].as_array().unwrap().len(), 1);
    assert_eq!(failed["runs"][0]["script"], "scan.sh");
    assert_eq!(failed["runs"][0]["status"], "FAILED");
    assert_eq!(failed["total"], 1);
    // The counts cover every status, so each quick filter can show its own.
    let mut counts = failed["statusCounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["status"].as_str().unwrap().to_string(),
                entry["count"].as_u64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    counts.sort();
    assert_eq!(
        counts,
        [("FAILED".to_string(), 1), ("SUCCEEDED".to_string(), 2)]
    );

    let of_a_job = harness
        .execute_as(
            "{ scriptRuns(jobId: 7) { runs { script } } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&of_a_job);
    assert!(
        response_data(&of_a_job)["scriptRuns"]["runs"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let nowhere = harness
        .execute_as(
            r#"{ scriptRuns(before: "latest") { nextBefore } }"#,
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&nowhere);
}

// Record a finished run of `script` on the scan event, with no job.
async fn record_scan_run(
    harness: &TestHarness,
    script: &str,
    status: weaver_server_core::post_processing::model::ScriptStatus,
    output_truncated: bool,
) {
    use weaver_server_core::post_processing::model::{
        ScriptAdapter, ScriptEventLabel, ScriptName, ScriptResult,
    };
    use weaver_server_core::post_processing::output::retain_output;

    let output = format!("{script} output").into_bytes();
    let written = if output_truncated {
        64 * 1024
    } else {
        output.len() as u64
    };
    retain_output(
        harness.db.clone(),
        None,
        ScriptResult {
            script: ScriptName::new(script).unwrap(),
            instance_id: None,
            instance_name: None,
            event: ScriptEventLabel::Scan,
            output_id: None,
            background: false,
            adapter: ScriptAdapter::Nzbget,
            status,
            exit_code: Some(0),
            duration_ms: 5,
            output_tail: String::new(),
            output_truncated,
            error_message: None,
            finished_at_epoch_ms: 1_000,
        },
        output,
        written,
        Default::default(),
    )
    .await
    .unwrap();
}

// The scripts of every recorded run, newest first.
async fn recorded_scripts(harness: &TestHarness) -> Vec<String> {
    let response = harness
        .execute("{ scriptRuns(limit: 50) { runs { script } } }")
        .await;
    assert_no_errors(&response);
    response_data(&response)["scriptRuns"]["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| run["script"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn lowering_the_retained_runs_deletes_older_runs_after_the_save_and_raising_deletes_none() {
    use weaver_server_core::post_processing::model::ScriptStatus;

    let harness = TestHarness::new().await;
    for (script, status) in [
        ("one.sh", ScriptStatus::Failed),
        ("two.sh", ScriptStatus::Succeeded),
        ("three.sh", ScriptStatus::TimedOut),
        ("four.sh", ScriptStatus::Succeeded),
        ("five.sh", ScriptStatus::Succeeded),
        ("six.sh", ScriptStatus::Succeeded),
    ] {
        record_scan_run(&harness, script, status, false).await;
    }
    let save = |limits: &'static str| {
        let harness = &harness;
        async move {
            let response = harness
                .execute(&format!(
                    "mutation {{ setPostProcessingSettings(input: {{
                        executionEnabled: false
                        concurrency: 1
                        terminationGraceSeconds: 10
                        {limits}
                    }}) {{ scriptOutputRunsPerJob scriptOutputFailedRunsPerJob }} }}"
                ))
                .await;
            assert_no_errors(&response);
            let saved = &response_data(&response)["setPostProcessingSettings"];
            (
                saved["scriptOutputRunsPerJob"].as_u64().unwrap(),
                saved["scriptOutputFailedRunsPerJob"].as_u64().unwrap(),
            )
        }
    };

    assert_eq!(save("").await, (32, 8), "the defaults");
    assert_eq!(
        save("scriptOutputRunsPerJob: 64, scriptOutputFailedRunsPerJob: 16").await,
        (64, 16)
    );
    harness.db.script_output_trims_settled().await;
    assert_eq!(
        recorded_scripts(&harness).await.len(),
        6,
        "raising deletes nothing"
    );

    // The two newest runs, then the newest failed run of the older ones.
    assert_eq!(
        save("scriptOutputRunsPerJob: 2, scriptOutputFailedRunsPerJob: 1").await,
        (2, 1)
    );
    harness.db.script_output_trims_settled().await;
    assert_eq!(
        recorded_scripts(&harness).await,
        ["six.sh", "five.sh", "three.sh"]
    );

    // Left out, both limits stay as they were.
    assert_eq!(save("").await, (2, 1));
    let persisted = harness
        .execute(
            "{ postProcessingSettings { scriptOutputRunsPerJob scriptOutputFailedRunsPerJob } }",
        )
        .await;
    assert_no_errors(&persisted);
    assert_eq!(
        response_data(&persisted)["postProcessingSettings"],
        json!({ "scriptOutputRunsPerJob": 2, "scriptOutputFailedRunsPerJob": 1 })
    );

    for invalid in [
        "scriptOutputRunsPerJob: 0",
        "scriptOutputRunsPerJob: 129",
        "scriptOutputFailedRunsPerJob: 129",
    ] {
        let response = harness
            .execute(&format!(
                "mutation {{ setPostProcessingSettings(input: {{
                    executionEnabled: false
                    concurrency: 1
                    terminationGraceSeconds: 10
                    {invalid}
                }}) {{ scriptOutputRunsPerJob }} }}"
            ))
            .await;
        assert_has_errors(&response);
    }
    assert_eq!(save("scriptOutputFailedRunsPerJob: 0").await, (2, 0));
    harness.db.script_output_trims_settled().await;
    assert_eq!(recorded_scripts(&harness).await, ["six.sh", "five.sh"]);
}

#[tokio::test]
async fn a_run_says_whether_it_printed_more_than_was_kept() {
    use weaver_server_core::post_processing::model::ScriptStatus;

    let harness = TestHarness::new().await;
    record_scan_run(&harness, "under.sh", ScriptStatus::Succeeded, false).await;
    record_scan_run(&harness, "over.sh", ScriptStatus::Cancelled, true).await;

    let response = harness
        .execute_as(
            "{ scriptRuns { runs { script status outputTruncated outputRetained } } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["scriptRuns"]["runs"],
        json!([
            { "script": "over.sh", "status": "CANCELLED", "outputTruncated": true, "outputRetained": true },
            { "script": "under.sh", "status": "SUCCEEDED", "outputTruncated": false, "outputRetained": true },
        ])
    );
}
