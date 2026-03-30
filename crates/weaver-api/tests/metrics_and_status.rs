mod common;

use common::{TestHarness, assert_no_errors, response_data};

fn now_epoch_sec() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[tokio::test]
async fn version_returns_nonempty_string() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ version }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let version = data["version"].as_str().unwrap();
    assert!(!version.is_empty());
}

#[tokio::test]
async fn metrics_returns_default_counters() {
    let h = TestHarness::new().await;
    let resp = h
        .execute("{ metrics { bytesDownloaded currentDownloadSpeed } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let metrics = &data["metrics"];
    assert!(metrics["bytesDownloaded"].is_number());
}

#[tokio::test]
async fn is_paused_false_by_default() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn is_paused_true_after_pause_all() {
    let h = TestHarness::new().await;
    let resp = h.execute("mutation { pauseAll }").await;
    assert_no_errors(&resp);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn is_paused_false_after_resume_all() {
    let h = TestHarness::new().await;
    h.execute("mutation { pauseAll }").await;
    h.execute("mutation { resumeAll }").await;

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn download_block_idle_by_default() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ downloadBlock { kind } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let kind = data["downloadBlock"]["kind"].as_str().unwrap();
    assert_eq!(kind, "NONE");
}

#[tokio::test]
async fn download_block_queryable_after_pause() {
    let h = TestHarness::new().await;
    h.execute("mutation { pauseAll }").await;

    let resp = h.execute("{ downloadBlock { kind capEnabled } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    // The mock scheduler's pauseAll only updates the paused flag, not the download block.
    // This test verifies the query is valid and returns correct types.
    assert!(data["downloadBlock"]["kind"].is_string());
    assert!(data["downloadBlock"]["capEnabled"].is_boolean());
}

#[tokio::test]
async fn metrics_has_expected_fields() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            "{ metrics { bytesDownloaded bytesDecoded bytesCommitted downloadQueueDepth segmentsDownloaded currentDownloadSpeed } }",
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let m = &data["metrics"];
    assert!(m["bytesDownloaded"].is_number());
    assert!(m["bytesDecoded"].is_number());
    assert!(m["downloadQueueDepth"].is_number());
    assert!(m["currentDownloadSpeed"].is_number());
}

#[tokio::test]
async fn metrics_history_returns_labeled_series() {
    let h = TestHarness::new().await;
    let now = now_epoch_sec();
    h.db.record_metrics_scrape(
        now - 20,
        "# TYPE weaver_pipeline_jobs gauge\n\
         weaver_pipeline_jobs{status=\"queued\"} 2\n\
         weaver_pipeline_current_download_speed_bytes_per_second 128\n",
    )
    .unwrap();
    h.db.record_metrics_scrape(
        now - 10,
        "weaver_pipeline_jobs{status=\"queued\"} 3\n\
         weaver_pipeline_jobs{status=\"downloading\"} 1\n\
         weaver_pipeline_current_download_speed_bytes_per_second 256\n",
    )
    .unwrap();

    let resp = h
        .execute(
            r#"{
                metricsHistory(
                    minutes: 60
                    metrics: [
                        "weaver_pipeline_jobs"
                        "weaver_pipeline_current_download_speed_bytes_per_second"
                    ]
                ) {
                    timestamps
                    series {
                        metric
                        labels {
                            key
                            value
                        }
                        values
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["metricsHistory"];
    assert_eq!(result["timestamps"].as_array().unwrap().len(), 2);
    let series = result["series"].as_array().unwrap();
    assert_eq!(series.len(), 3);
    assert_eq!(
        series[0]["metric"].as_str().unwrap(),
        "weaver_pipeline_jobs"
    );
    assert_eq!(series[1]["labels"][0]["value"].as_str().unwrap(), "queued");
    assert_eq!(series[1]["values"][0].as_f64().unwrap(), 2.0);
    assert_eq!(series[1]["values"][1].as_f64().unwrap(), 3.0);
    assert_eq!(
        series[0]["labels"][0]["value"].as_str().unwrap(),
        "downloading"
    );
    assert_eq!(series[0]["values"][0].as_f64().unwrap(), 0.0);
    assert_eq!(series[0]["values"][1].as_f64().unwrap(), 1.0);
    assert_eq!(
        series[2]["metric"].as_str().unwrap(),
        "weaver_pipeline_current_download_speed_bytes_per_second"
    );
    assert!(series[2]["labels"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn metrics_history_clamps_requested_minutes_to_24_hours() {
    let h = TestHarness::new().await;
    let now = now_epoch_sec();
    h.db.record_metrics_scrape(
        now - (48 * 60 * 60),
        "weaver_pipeline_current_download_speed_bytes_per_second 64\n",
    )
    .unwrap();
    h.db.record_metrics_scrape(
        now - 30,
        "weaver_pipeline_current_download_speed_bytes_per_second 128\n",
    )
    .unwrap();

    let resp = h
        .execute(
            r#"{
                metricsHistory(
                    minutes: 99999
                    metrics: ["weaver_pipeline_current_download_speed_bytes_per_second"]
                ) {
                    timestamps
                    series {
                        metric
                        values
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["metricsHistory"];
    assert_eq!(result["timestamps"].as_array().unwrap().len(), 1);
    assert_eq!(result["series"][0]["values"][0].as_f64().unwrap(), 128.0);
}
