mod common;

use common::{TestHarness, assert_no_errors, response_data};
use tempfile::NamedTempFile;
use weaver_server_core::JobHistoryRow;

fn sample_history_row(job_id: u64, name: &str) -> JobHistoryRow {
    JobHistoryRow {
        job_id,
        name: name.to_string(),
        status: "failed".to_string(),
        error_message: Some("simulated failure".to_string()),
        total_bytes: 1_000_000 + job_id,
        downloaded_bytes: 900_000 + job_id,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 10,
        health: 640,
        category: Some("tv".to_string()),
        output_dir: Some(format!("/downloads/{name}")),
        nzb_path: Some(format!("/nzb/{name}.nzb")),
        created_at: 1_717_171_700,
        completed_at: 1_717_171_760,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    }
}

#[tokio::test]
async fn diagnostic_redownload_accepts_without_runtime_blocking() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("diagnostic-source").await;
    h.insert_history_row(&sample_history_row(job_id, "diagnostic-source"));
    h.config.write().await.diagnostic_upload_url =
        Some("https://example.test/diagnostics".to_string());

    let response = h
        .execute(&format!(
            r#"mutation {{
                startDiagnosticRedownload(id: {job_id}, includeServerHostnames: true) {{
                    sourceJobId
                    diagnosticJobId
                    stage
                    includeServerHostnames
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&response);

    let data = response_data(&response);
    assert_eq!(
        data["startDiagnosticRedownload"]["sourceJobId"].as_u64(),
        Some(job_id)
    );
    assert_eq!(
        data["startDiagnosticRedownload"]["stage"].as_str(),
        Some("QUEUED")
    );

    let row =
        h.db.get_diagnostic_run_for_source(job_id)
            .expect("load diagnostic run")
            .expect("diagnostic run row");
    assert_eq!(row.stage.as_str(), "queued");
    assert_eq!(
        data["startDiagnosticRedownload"]["diagnosticJobId"].as_u64(),
        Some(row.diagnostic_job_id)
    );
}

#[tokio::test]
async fn diagnostic_redownload_cleans_up_row_when_staging_setup_fails() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("staging-failure").await;
    h.insert_history_row(&sample_history_row(job_id, "staging-failure"));
    let temp_file = NamedTempFile::new().expect("temp file");
    {
        let mut config = h.config.write().await;
        config.diagnostic_upload_url = Some("https://example.test/diagnostics".to_string());
        config.data_dir = temp_file.path().to_string_lossy().to_string();
    }

    let response = h
        .execute(&format!(
            r#"mutation {{
                startDiagnosticRedownload(id: {job_id}, includeServerHostnames: false) {{
                    sourceJobId
                }}
            }}"#
        ))
        .await;
    assert!(
        !response.errors.is_empty(),
        "mutation should surface a GraphQL error"
    );

    let row =
        h.db.get_diagnostic_run_for_source(job_id)
            .expect("load diagnostic run");
    assert!(
        row.is_none(),
        "failed staging setup should not strand a row"
    );
}
