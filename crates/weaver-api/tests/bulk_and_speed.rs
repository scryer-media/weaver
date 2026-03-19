mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

#[tokio::test]
async fn set_speed_limit() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 1048576) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["setSpeedLimit"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn set_speed_limit_unlimited() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 0) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["setSpeedLimit"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn set_speed_limit_very_high() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"mutation { setSpeedLimit(bytesPerSec: 999999999999) }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["setSpeedLimit"].as_bool().unwrap(), true);
}

#[tokio::test]
async fn update_jobs_category() {
    let h = TestHarness::new().await;

    // Submit two jobs.
    let id1 = h.submit_test_nzb("bulk-cat-1").await;
    let id2 = h.submit_test_nzb("bulk-cat-2").await;

    // Update both to category "movies".
    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id1}, {id2}], category: "movies") }}"#
        ))
        .await;
    assert_no_errors(&resp);

    // Verify both jobs have the new category.
    let resp = h
        .execute(&format!(
            r#"{{ job(id: {id1}) {{ category }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["job"]["category"].as_str().unwrap(), "movies");

    let resp = h
        .execute(&format!(
            r#"{{ job(id: {id2}) {{ category }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["job"]["category"].as_str().unwrap(), "movies");
}

#[tokio::test]
async fn update_jobs_clear_category() {
    let h = TestHarness::new().await;

    // Submit a job with a category.
    let id = h
        .submit_test_nzb_with_options("clear-cat", Some("tv"), None, &[])
        .await;

    // Clear the category by setting it to empty string.
    let resp = h
        .execute(&format!(
            r#"mutation {{ updateJobs(ids: [{id}], category: "") }}"#
        ))
        .await;
    assert_no_errors(&resp);

    // Verify category is cleared (null).
    let resp = h
        .execute(&format!(r#"{{ job(id: {id}) {{ category }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["job"]["category"].is_null());
}

#[tokio::test]
async fn update_jobs_nonexistent() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"mutation { updateJobs(ids: [999999], category: "x") }"#)
        .await;
    assert_has_errors(&resp);
}
