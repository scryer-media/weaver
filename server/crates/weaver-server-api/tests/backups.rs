mod common;
use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_core::auth::CallerScope;

#[tokio::test]
async fn backup_sizes_above_two_gibibytes_serialize_without_truncation() {
    struct SizeQuery;
    #[async_graphql::Object]
    impl SizeQuery {
        async fn backup(&self) -> weaver_server_api::backup::types::BackupInfoGql {
            use weaver_server_api::backup::types::*;
            BackupInfoGql {
                filename: "large.enc".into(),
                size_bytes: 5 * 1024 * 1024 * 1024,
                created_at: "2026-01-01T00:00:00Z".into(),
                format_version: "1".into(),
                source_weaver_version: "0.14.2".into(),
                source_engine: "sqlite".into(),
                encrypted: true,
                row_counts: async_graphql::Json(Default::default()),
                trigger: BackupTriggerGql::Manual,
                status: BackupArtifactStatusGql::Ready,
                error: None,
            }
        }
    }
    let schema = async_graphql::Schema::build(
        SizeQuery,
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
    )
    .finish();
    let response = schema.execute("{ backup { sizeBytes } }").await;
    assert_no_errors(&response);
    assert_eq!(
        response_data(&response)["backup"]["sizeBytes"],
        5_u64 * 1024 * 1024 * 1024
    );
}

#[tokio::test]
async fn backup_queries_and_mutations_require_admin_scope() {
    let h = TestHarness::new().await;
    for query in [
        "{ backups { filename } }",
        "{ backupSettings { backupPath } }",
        "{ autoBackupSettings { enabled } }",
        "mutation { updateBackupSettings(customBackupPath: null) { backupPath } }",
        "mutation { updateAutoBackupSettings(input: { enabled: false, dailyTimeLocal: \"03:00\" }) { enabled } }",
        "mutation { deleteBackup(filename: \"missing.enc\") }",
        "mutation { createBackupDownloadToken(filename: \"missing.enc\") }",
    ] {
        let response = h.execute_as(query, CallerScope::Read).await;
        assert!(
            response
                .errors
                .iter()
                .any(|error| error.message.contains("admin scope")),
            "{query}: {:?}",
            response.errors
        );
    }
}

#[tokio::test]
async fn backup_settings_defaults_and_validation_are_available_through_graphql() {
    let h = TestHarness::new().await;
    let response = h.execute("{ backups { filename } backupSettings { customBackupPath backupPath } autoBackupSettings { enabled dailyTimeLocal autoBackupKeyPresent nextRunAt } }").await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["backups"], serde_json::json!([]));
    assert_eq!(data["autoBackupSettings"]["enabled"], false);
    assert_eq!(data["autoBackupSettings"]["dailyTimeLocal"], "03:00");
    assert_eq!(data["autoBackupSettings"]["autoBackupKeyPresent"], false);
    assert!(
        data["backupSettings"]["backupPath"]
            .as_str()
            .unwrap()
            .ends_with("backups")
    );
    for input in [
        "{ enabled: true, dailyTimeLocal: \"03:00\" }",
        "{ enabled: false, dailyTimeLocal: \"25:00\" }",
        "{ enabled: false, dailyTimeLocal: \"03:00\", setAutoBackupKey: \"short\" }",
        "{ enabled: true, dailyTimeLocal: \"03:00\", clearAutoBackupKey: true }",
    ] {
        let response = h
            .execute(&format!(
                "mutation {{ updateAutoBackupSettings(input: {input}) {{ enabled }} }}"
            ))
            .await;
        assert!(!response.errors.is_empty(), "accepted {input}");
    }
    let response = h
        .execute("mutation { updateBackupSettings(customBackupPath: \"relative\") { backupPath } }")
        .await;
    assert!(!response.errors.is_empty());
    let response = h
        .execute("mutation { deleteBackup(filename: \"missing.enc\") }")
        .await;
    assert_no_errors(&response);
    assert_eq!(response_data(&response)["deleteBackup"], false);
}
