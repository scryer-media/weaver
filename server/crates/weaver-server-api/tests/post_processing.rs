mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use weaver_server_api::auth::CallerScope;
use weaver_server_core::post_processing::discovery::{
    DiscoveryOptions, discover_and_record_extensions,
};
use weaver_server_core::post_processing::model::FrozenPlanProvenance;

#[tokio::test]
async fn settings_are_admin_only_and_disabled_by_default() {
    let harness = TestHarness::new().await;
    let denied = harness
        .execute_as(
            "{ postProcessingSettings { discoveryEnabled executionEnabled } }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied);

    let response = harness
        .execute("{ postProcessingSettings { discoveryEnabled executionEnabled concurrency } }")
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["postProcessingSettings"]["discoveryEnabled"], false);
    assert_eq!(data["postProcessingSettings"]["executionEnabled"], false);
    assert_eq!(data["postProcessingSettings"]["concurrency"], 1);
}

#[tokio::test]
async fn settings_update_round_trips_and_control_scope_can_operate_queue() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute(
            r#"
            mutation {
              updatePostProcessingSettings(input: {
                discoveryEnabled: true
                executionEnabled: true
                concurrency: 2
                terminationGraceSeconds: 15
                pythonInterpreter: "/usr/bin/python3"
                allowedRoots: ["/downloads"]
              }) {
                discoveryEnabled
                executionEnabled
                concurrency
                terminationGraceSeconds
                pythonInterpreter
                allowedRoots
              }
            }
            "#,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    let settings = &data["updatePostProcessingSettings"];
    assert_eq!(settings["discoveryEnabled"], true);
    assert_eq!(settings["executionEnabled"], true);
    assert_eq!(settings["concurrency"], 2);
    assert_eq!(settings["terminationGraceSeconds"], 15);
    assert_eq!(settings["pythonInterpreter"], "/usr/bin/python3");
    assert_eq!(settings["allowedRoots"][0], "/downloads");

    let denied = harness
        .execute_as("mutation { pausePostProcessingQueue }", CallerScope::Read)
        .await;
    assert_has_errors(&denied);
    let paused = harness
        .execute_as(
            "mutation { pausePostProcessingQueue }",
            CallerScope::Control,
        )
        .await;
    assert_no_errors(&paused);
    let resumed = harness
        .execute_as(
            "mutation { resumePostProcessingQueue }",
            CallerScope::Control,
        )
        .await;
    assert_no_errors(&resumed);
    let denied_reorder = harness
        .execute_as(
            "mutation { reorderPostProcessingQueue(runIds: []) }",
            CallerScope::Read,
        )
        .await;
    assert_has_errors(&denied_reorder);
    let reordered = harness
        .execute_as(
            "mutation { reorderPostProcessingQueue(runIds: []) }",
            CallerScope::Control,
        )
        .await;
    assert_no_errors(&reordered);
}

#[tokio::test]
async fn omitted_submission_selection_freezes_the_legacy_empty_plan() {
    let harness = TestHarness::new().await;
    let job_id = harness.submit_test_nzb("post-processing-compat").await;
    let plan = harness
        .db
        .frozen_post_processing_plan(job_id)
        .unwrap()
        .expect("submission should freeze an explicit empty plan");
    assert!(matches!(plan.provenance(), FrozenPlanProvenance::Empty));
    assert!(plan.steps().is_empty());
}

#[tokio::test]
async fn readable_state_queries_do_not_require_admin_scope() {
    let harness = TestHarness::new().await;
    let response = harness
        .execute_as(
            "{ postProcessingRevisions { extensionId } postProcessingProfiles { profileId } postProcessingQueue { runId } postProcessingArtifacts(runId: \"run-missing\") { path } }",
            CallerScope::Read,
        )
        .await;
    assert_no_errors(&response);
}

#[tokio::test]
async fn manifest_diagnostics_are_admin_only_and_require_approval() {
    let harness = TestHarness::new().await;
    let data_dir = std::path::PathBuf::from(harness.config.read().await.data_dir.clone());
    let extension_dir = data_dir.join("scripts/diagnostic");
    std::fs::create_dir_all(&extension_dir).unwrap();
    std::fs::write(
        extension_dir.join("weaver-extension.json"),
        r#"{
            "schema_version": 1,
            "kind": "native",
            "id": "example.diagnostic",
            "name": "Diagnostic Example",
            "version": "1.0.0",
            "entrypoint": "diagnostic",
            "commands": [{
                "name": "connectionTest",
                "action": "Run connection test"
            }],
            "options": []
        }"#,
    )
    .unwrap();
    std::fs::write(extension_dir.join("diagnostic"), b"diagnostic fixture").unwrap();
    let manifest = discover_and_record_extensions(
        &harness.db,
        &data_dir,
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: None,
        },
        1,
    )
    .unwrap()
    .into_iter()
    .next()
    .unwrap()
    .manifest;
    let revision = manifest.revision();
    let mutation = format!(
        r#"mutation {{
            runPostProcessingDiagnostic(input: {{
                extensionId: "{}"
                revisionId: "{}"
                command: "connectionTest"
            }}) {{ succeeded }}
        }}"#,
        revision.extension_id().as_str(),
        revision.revision_id().as_str()
    );

    let denied = harness.execute_as(&mutation, CallerScope::Control).await;
    assert_has_errors(&denied);
    let unapproved = harness.execute(&mutation).await;
    assert_has_errors(&unapproved);
    assert!(
        unapproved.errors[0]
            .message
            .contains("approved extension revision")
    );
}
