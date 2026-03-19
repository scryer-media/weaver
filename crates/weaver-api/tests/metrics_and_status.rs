mod common;

use common::{TestHarness, assert_no_errors, response_data};

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
    assert_eq!(data["isPaused"].as_bool().unwrap(), false);
}

#[tokio::test]
async fn is_paused_true_after_pause_all() {
    let h = TestHarness::new().await;
    let resp = h.execute("mutation { pauseAll }").await;
    assert_no_errors(&resp);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["isPaused"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn is_paused_false_after_resume_all() {
    let h = TestHarness::new().await;
    h.execute("mutation { pauseAll }").await;
    h.execute("mutation { resumeAll }").await;

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["isPaused"].as_bool().unwrap(), false);
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
