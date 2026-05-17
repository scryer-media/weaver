mod common;

use common::{TestHarness, assert_no_errors, response_data};
use tokio::time::{Duration, sleep};
use weaver_server_core::JobHistoryRow;

fn sample_history_row(job_id: u64, name: &str, status: &str, completed_at: i64) -> JobHistoryRow {
    JobHistoryRow {
        job_id,
        job_hash: None,
        name: name.to_string(),
        status: status.to_string(),
        error_message: (status == "failed").then(|| "simulated failure".to_string()),
        total_bytes: 1_000_000 + job_id,
        downloaded_bytes: 900_000 + job_id,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: if status == "failed" { 10 } else { 0 },
        health: if status == "failed" { 640 } else { 1000 },
        category: Some(
            if job_id.is_multiple_of(2) {
                "tv"
            } else {
                "movies"
            }
            .to_string(),
        ),
        output_dir: Some(format!("/downloads/{name}")),
        nzb_path: Some(format!("/nzb/{name}.nzb")),
        created_at: completed_at - 60,
        completed_at,
        metadata: None,
        last_diagnostic_id: None,
        last_diagnostic_uploaded_at_epoch_ms: None,
    }
}

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
// History Page
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_page_supports_search_counts_and_sorting() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        1,
        "alpha.release",
        "complete",
        1_700_000_001,
    ));
    h.insert_history_row(&sample_history_row(
        2,
        "alpha.failure",
        "failed",
        1_700_000_002,
    ));
    h.insert_history_row(&sample_history_row(
        3,
        "bravo.release",
        "complete",
        1_700_000_003,
    ));

    let resp = h
        .execute(
            r#"{
              historyPage(
                input: {
                  pageIndex: 0
                  pageSize: 25
                  search: "alpha"
                  status: SUCCESS
                  sortField: NAME
                  sortDirection: ASC
                }
              ) {
                totalCount
                counts {
                  all
                  success
                  failure
                }
                items {
                  id
                  name
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let page = &data["historyPage"];
    assert_eq!(page["totalCount"].as_u64().unwrap(), 1);
    assert_eq!(page["counts"]["all"].as_u64().unwrap(), 2);
    assert_eq!(page["counts"]["success"].as_u64().unwrap(), 1);
    assert_eq!(page["counts"]["failure"].as_u64().unwrap(), 1);

    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"].as_str().unwrap(), "alpha.release");
}

#[tokio::test]
async fn history_page_caps_page_size_at_five_hundred() {
    let h = TestHarness::new().await;
    for job_id in 1..=550 {
        h.insert_history_row(&sample_history_row(
            job_id,
            &format!("bulk-{job_id}"),
            "complete",
            1_700_000_000 + job_id as i64,
        ));
    }

    let resp = h
        .execute(
            r#"{
              historyPage(input: { pageIndex: 0, pageSize: 600 }) {
                totalCount
                items {
                  id
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let page = &data["historyPage"];
    assert_eq!(page["totalCount"].as_u64().unwrap(), 550);
    assert_eq!(page["items"].as_array().unwrap().len(), 500);
}

#[tokio::test]
async fn history_page_remains_paged_after_deleting_the_first_page() {
    let h = TestHarness::new().await;
    for job_id in 1..=329 {
        h.insert_history_row(&sample_history_row(
            job_id,
            &format!("history-{job_id}"),
            "complete",
            1_700_000_000 + job_id as i64,
        ));
    }

    let page_one_resp = h
        .execute(
            r#"{
              historyPage(input: { pageIndex: 0, pageSize: 100 }) {
                items {
                  id
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&page_one_resp);
    let page_one = response_data(&page_one_resp);
    let ids: Vec<String> = page_one["historyPage"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 100);

    let delete_resp = h
        .execute(&format!(
            "mutation {{ deleteHistoryBatch(ids: [{}]) {{ id }} }}",
            ids.join(", ")
        ))
        .await;
    assert_no_errors(&delete_resp);

    let second_page_resp = h
        .execute(
            r#"{
              historyPage(input: { pageIndex: 1, pageSize: 100 }) {
                totalCount
                items {
                  id
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&second_page_resp);
    let second_page = response_data(&second_page_resp);
    let page = &second_page["historyPage"];
    assert_eq!(page["totalCount"].as_u64().unwrap(), 229);
    assert_eq!(page["items"].as_array().unwrap().len(), 100);
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

#[tokio::test]
async fn history_page_exposes_delete_operation_state() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        1,
        "alpha.release",
        "complete",
        1_700_000_001,
    ));
    let operation_id =
        h.db.insert_history_delete_operation(&[1], true)
            .expect("failed to seed delete operation");

    let resp = h
        .execute(
            r#"{
              historyPage(input: { pageIndex: 0, pageSize: 25 }) {
                items {
                  id
                  deleteOperation {
                    operationId
                    state
                    locked
                    deleteFiles
                    errorMessage
                  }
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let item = &data["historyPage"]["items"].as_array().unwrap()[0];
    let delete_operation = &item["deleteOperation"];
    assert_eq!(item["id"].as_u64().unwrap(), 1);
    assert_eq!(
        delete_operation["operationId"].as_u64().unwrap(),
        operation_id
    );
    assert_eq!(delete_operation["state"].as_str().unwrap(), "QUEUED");
    assert!(delete_operation["locked"].as_bool().unwrap());
    assert!(delete_operation["deleteFiles"].as_bool().unwrap());
    assert!(delete_operation["errorMessage"].is_null());
}

#[tokio::test]
async fn job_detail_snapshot_exposes_history_delete_state() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        7,
        "detail.release",
        "failed",
        1_700_000_007,
    ));
    let operation_id =
        h.db.insert_history_delete_operation(&[7], false)
            .expect("failed to seed delete operation");

    let resp = h
        .execute(
            r#"{
              jobDetailSnapshot(jobId: 7) {
                historyItem {
                  id
                  deleteOperation {
                    operationId
                    state
                    locked
                    deleteFiles
                  }
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let delete_operation = &data["jobDetailSnapshot"]["historyItem"]["deleteOperation"];
    assert_eq!(
        delete_operation["operationId"].as_u64().unwrap(),
        operation_id
    );
    assert_eq!(delete_operation["state"].as_str().unwrap(), "QUEUED");
    assert!(delete_operation["locked"].as_bool().unwrap());
    assert!(!delete_operation["deleteFiles"].as_bool().unwrap());
}

#[tokio::test]
async fn accept_history_delete_queues_background_operation() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        11,
        "batch.one",
        "complete",
        1_700_000_011,
    ));
    h.insert_history_row(&sample_history_row(
        12,
        "batch.two",
        "failed",
        1_700_000_012,
    ));

    let resp = h
        .execute(
            r#"mutation {
              acceptHistoryDelete(
                input: { mode: IDS, ids: [11, 12], deleteFiles: true }
              ) {
                operationId
                acceptedIds
                totalTargets
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let acceptance = &data["acceptHistoryDelete"];
    assert_eq!(acceptance["acceptedIds"].as_array().unwrap().len(), 2);
    assert_eq!(acceptance["totalTargets"].as_u64().unwrap(), 2);
    let operation_id = acceptance["operationId"].as_u64().unwrap();

    let mut completion_seen = false;
    for _ in 0..20 {
        let query_resp = h
            .execute(
                r#"{
                  historyDeleteOperations(activeOnly: false) {
                    id
                    state
                    totalTargets
                    completedTargets
                    failedTargets
                  }
                  first: historyItem(id: 11) { id }
                  second: historyItem(id: 12) { id }
                }"#,
            )
            .await;
        assert_no_errors(&query_resp);
        let query_data = response_data(&query_resp);
        let operations = query_data["historyDeleteOperations"].as_array().unwrap();
        if let Some(operation) = operations
            .iter()
            .find(|entry| entry["id"].as_u64() == Some(operation_id))
        {
            assert_eq!(operation["totalTargets"].as_u64().unwrap(), 2);
            if operation["state"].as_str() == Some("COMPLETED")
                && query_data["first"].is_null()
                && query_data["second"].is_null()
            {
                completion_seen = true;
                break;
            }
        }
        sleep(Duration::from_millis(25)).await;
    }

    assert!(
        completion_seen,
        "expected background delete operation to complete"
    );
}

#[tokio::test]
async fn accept_history_delete_conflicts_with_locked_rows() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        21,
        "locked.one",
        "complete",
        1_700_000_021,
    ));
    h.insert_history_row(&sample_history_row(
        22,
        "locked.two",
        "complete",
        1_700_000_022,
    ));
    h.db.insert_history_delete_operation(&[21], false)
        .expect("failed to seed locked delete operation");

    let resp = h
        .execute(
            r#"mutation {
              acceptHistoryDelete(
                input: { mode: ALL_HISTORY, deleteFiles: false }
              ) {
                operationId
              }
            }"#,
        )
        .await;

    assert!(
        !resp.errors.is_empty(),
        "expected conflict when accepting delete-all with locked rows"
    );
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
