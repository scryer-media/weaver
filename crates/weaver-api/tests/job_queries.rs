mod common;

use common::{TestHarness, assert_no_errors, response_data};

// ---------------------------------------------------------------------------
// List Jobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jobs_empty_on_fresh_db() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ jobs { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn jobs_returns_submitted() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("alpha").await;
    h.submit_test_nzb("bravo").await;
    h.submit_test_nzb("charlie").await;

    let resp = h.execute("{ jobs { id name } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 3);
}

#[tokio::test]
async fn jobs_filter_by_queued_status() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("queued-one").await;
    h.submit_test_nzb("queued-two").await;

    let resp = h.execute("{ jobs(status: [QUEUED]) { id status } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2);
    for job in jobs {
        assert_eq!(job["status"].as_str().unwrap(), "QUEUED");
    }
}

#[tokio::test]
async fn jobs_filter_by_downloading_status() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("still-queued").await;

    let resp = h.execute("{ jobs(status: [DOWNLOADING]) { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn jobs_filter_by_paused_status() {
    let h = TestHarness::new().await;
    let id_a = h.submit_test_nzb("will-pause").await;
    h.submit_test_nzb("stays-queued").await;

    // Pause the first job.
    let pause_query = format!("mutation {{ pauseJob(id: {id_a}) }}");
    let pause_resp = h.execute(&pause_query).await;
    assert_no_errors(&pause_resp);

    let resp = h.execute("{ jobs(status: [PAUSED]) { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["id"].as_u64().unwrap(), id_a);
}

#[tokio::test]
async fn jobs_filter_multiple_statuses() {
    let h = TestHarness::new().await;
    let id_paused = h.submit_test_nzb("to-pause").await;
    h.submit_test_nzb("stays-queued").await;

    let pause_query = format!("mutation {{ pauseJob(id: {id_paused}) }}");
    let pause_resp = h.execute(&pause_query).await;
    assert_no_errors(&pause_resp);

    let resp = h.execute("{ jobs(status: [QUEUED, PAUSED]) { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2);
}

#[tokio::test]
async fn jobs_filter_empty_result() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("queued-job").await;

    let resp = h.execute("{ jobs(status: [COMPLETE]) { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert!(jobs.is_empty());
}

// ---------------------------------------------------------------------------
// Category Filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jobs_filter_by_category() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("tv-show", Some("tv"), None, &[])
        .await;
    h.submit_test_nzb_with_options("movie", Some("movies"), None, &[])
        .await;

    let resp = h
        .execute(r#"{ jobs(category: "tv") { id name category } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["category"].as_str().unwrap(), "tv");
}

#[tokio::test]
async fn jobs_filter_nonexistent_category() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("tv-show", Some("tv"), None, &[])
        .await;

    let resp = h
        .execute(r#"{ jobs(category: "nonexistent") { id } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert!(jobs.is_empty());
}

// ---------------------------------------------------------------------------
// Metadata Filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jobs_filter_by_metadata_key() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("with-meta", None, None, &[("tvdbId", "12345")])
        .await;
    h.submit_test_nzb("no-meta").await;

    let resp = h
        .execute(r#"{ jobs(hasMetadataKey: "tvdbId") { id metadata { key value } } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    let meta = jobs[0]["metadata"].as_array().unwrap();
    assert!(meta.iter().any(|m| m["key"].as_str().unwrap() == "tvdbId"));
}

#[tokio::test]
async fn jobs_filter_nonexistent_metadata_key() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("with-meta", None, None, &[("tvdbId", "12345")])
        .await;

    let resp = h
        .execute(r#"{ jobs(hasMetadataKey: "missing") { id } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert!(jobs.is_empty());
}

// ---------------------------------------------------------------------------
// Combined Filters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jobs_filter_status_and_category() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("tv-queued", Some("tv"), None, &[])
        .await;
    let id_paused = h
        .submit_test_nzb_with_options("tv-paused", Some("tv"), None, &[])
        .await;
    h.submit_test_nzb_with_options("movie-queued", Some("movies"), None, &[])
        .await;

    let pause_query = format!("mutation {{ pauseJob(id: {id_paused}) }}");
    let pause_resp = h.execute(&pause_query).await;
    assert_no_errors(&pause_resp);

    let resp = h
        .execute(r#"{ jobs(status: [QUEUED], category: "tv") { id } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
}

#[tokio::test]
async fn jobs_filter_all_three() {
    let h = TestHarness::new().await;
    h.submit_test_nzb_with_options("match", Some("tv"), None, &[("tvdbId", "999")])
        .await;
    h.submit_test_nzb_with_options("wrong-cat", Some("movies"), None, &[("tvdbId", "999")])
        .await;
    h.submit_test_nzb_with_options("no-meta", Some("tv"), None, &[])
        .await;

    let resp = h
        .execute(
            r#"{ jobs(status: [QUEUED], category: "tv", hasMetadataKey: "tvdbId") { id name } }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0]["name"].as_str().unwrap().contains("match"));
}

// ---------------------------------------------------------------------------
// Get Single Job
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_by_id() {
    let h = TestHarness::new().await;
    let id = h.submit_test_nzb("single-job").await;

    let query =
        format!(r#"{{ job(id: {id}) {{ id name status totalBytes progress health createdAt }} }}"#);
    let resp = h.execute(&query).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = &data["job"];
    assert_eq!(job["id"].as_u64().unwrap(), id);
    assert!(job["name"].as_str().unwrap().contains("single-job"));
    assert_eq!(job["status"].as_str().unwrap(), "QUEUED");
    assert!(job["totalBytes"].as_u64().is_some());
    assert!(job["createdAt"].as_f64().is_some());
}

#[tokio::test]
async fn job_nonexistent_returns_null() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ job(id: 999999) { id name } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["job"].is_null());
}

// ---------------------------------------------------------------------------
// History Delete
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_history_nonexistent() {
    let h = TestHarness::new().await;
    // Deleting a nonexistent history entry succeeds silently (idempotent).
    let resp = h
        .execute("mutation { deleteHistory(id: 999999) { id } }")
        .await;
    assert_no_errors(&resp);
}

#[tokio::test]
async fn delete_all_history_when_empty() {
    let h = TestHarness::new().await;
    let resp = h.execute("mutation { deleteAllHistory { id } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let jobs = data["deleteAllHistory"].as_array().unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn delete_all_history_idempotent() {
    let h = TestHarness::new().await;

    let resp1 = h.execute("mutation { deleteAllHistory { id } }").await;
    assert_no_errors(&resp1);

    let resp2 = h.execute("mutation { deleteAllHistory { id } }").await;
    assert_no_errors(&resp2);
    let data = response_data(&resp2);
    let jobs = data["deleteAllHistory"].as_array().unwrap();
    assert!(jobs.is_empty());
}

// ---------------------------------------------------------------------------
// Job Fields Verification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn job_has_all_fields() {
    let h = TestHarness::new().await;
    let id = h
        .submit_test_nzb_with_options(
            "full-fields",
            Some("tv"),
            Some("secret123"),
            &[("tvdbId", "42"), ("season", "1")],
        )
        .await;

    let query = format!(
        r#"{{
            job(id: {id}) {{
                id
                name
                status
                hasPassword
                category
                metadata {{ key value }}
                totalBytes
                progress
                health
                createdAt
                outputDir
                error
            }}
        }}"#
    );
    let resp = h.execute(&query).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let job = &data["job"];

    assert_eq!(job["id"].as_u64().unwrap(), id);
    assert!(job["name"].as_str().unwrap().contains("full-fields"));
    assert_eq!(job["status"].as_str().unwrap(), "QUEUED");
    assert!(job["hasPassword"].as_bool().unwrap());
    assert_eq!(job["category"].as_str().unwrap(), "tv");

    let meta = job["metadata"].as_array().unwrap();
    // submit_nzb_bytes may add internal metadata (e.g. *original_title), so check >= 2.
    assert!(
        meta.len() >= 2,
        "expected at least 2 metadata entries, got {}",
        meta.len()
    );
    let keys: Vec<&str> = meta.iter().map(|m| m["key"].as_str().unwrap()).collect();
    assert!(keys.contains(&"tvdbId"));
    assert!(keys.contains(&"season"));
    let tvdb_entry = meta
        .iter()
        .find(|m| m["key"].as_str().unwrap() == "tvdbId")
        .unwrap();
    assert_eq!(tvdb_entry["value"].as_str().unwrap(), "42");

    assert!(job["totalBytes"].as_u64().is_some());
    assert!(job["progress"].as_f64().is_some());
    assert!(job["health"].as_u64().is_some());
    assert!(job["createdAt"].as_f64().is_some());
    // outputDir and error should be null for a freshly submitted job.
    assert!(job["outputDir"].is_null());
    assert!(job["error"].is_null());
}
