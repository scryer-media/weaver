mod common;

use common::{TestHarness, assert_no_errors, response_data};

#[tokio::test]
async fn get_settings_defaults() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"{ settings { dataDir intermediateDir completeDir cleanupAfterExtract maxDownloadSpeed maxRetries } }"#,
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
    // maxDownloadSpeed defaults to 0 (unlimited).
    assert_eq!(s["maxDownloadSpeed"].as_u64().unwrap(), 0);
    // maxRetries defaults to 3.
    assert_eq!(s["maxRetries"].as_u64().unwrap(), 3);
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
async fn update_max_download_speed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { maxDownloadSpeed: 5242880 }) {
                    maxDownloadSpeed
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["updateSettings"]["maxDownloadSpeed"].as_u64().unwrap(),
        5242880
    );
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
async fn partial_settings_update() {
    let h = TestHarness::new().await;

    // Get defaults first.
    let resp = h
        .execute(
            r#"{ settings { intermediateDir cleanupAfterExtract maxRetries maxDownloadSpeed } }"#,
        )
        .await;
    assert_no_errors(&resp);
    let before = response_data(&resp);
    let before_intermediate = before["settings"]["intermediateDir"]
        .as_str()
        .unwrap()
        .to_string();
    let before_cleanup = before["settings"]["cleanupAfterExtract"].as_bool().unwrap();
    let before_retries = before["settings"]["maxRetries"].as_u64().unwrap();
    let before_speed = before["settings"]["maxDownloadSpeed"].as_u64().unwrap();

    // Update only completeDir.
    let resp = h
        .execute(
            r#"mutation {
                updateSettings(input: { completeDir: "/tmp/partial" }) {
                    completeDir intermediateDir cleanupAfterExtract maxRetries maxDownloadSpeed
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
    assert_eq!(s["maxDownloadSpeed"].as_u64().unwrap(), before_speed);
}
