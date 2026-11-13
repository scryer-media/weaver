// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};
use tokio::time::{Duration, sleep};
use weaver_server_core::{CLIENT_REQUEST_ID_ATTRIBUTE_KEY, JobHistoryRow};

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
        server_attribution: None,
        files_removed_at: None,
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
async fn queue_page_default_order_preserves_scheduler_order_for_cold_jobs() {
    let h = TestHarness::new().await;
    h.submit_test_nzb("first-download").await;
    h.submit_test_nzb("second-download").await;

    let scheduler_response = h.execute("{ jobs { id } }").await;
    assert_no_errors(&scheduler_response);
    let scheduler_data = response_data(&scheduler_response);
    let scheduler_first_id = scheduler_data["jobs"][0]["id"].as_u64();

    let response = h
        .execute(
            r#"{
              queuePage(input: { pageIndex: 0, pageSize: 50 }) {
                items { id }
              }
            }"#,
        )
        .await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(
        data["queuePage"]["items"][0]["id"].as_u64(),
        scheduler_first_id,
    );
}

#[tokio::test]
async fn queue_page_bounds_large_queues_and_filters_before_slicing() {
    const JOB_COUNT: usize = 1_000;

    let h = TestHarness::new().await;
    for index in 0..JOB_COUNT {
        let category = if index.is_multiple_of(2) {
            "tv"
        } else {
            "movies"
        };
        let priority = if index.is_multiple_of(2) {
            "HIGH"
        } else {
            "LOW"
        };
        h.submit_test_nzb_with_options(
            &format!("queue-page-{index:04}"),
            Some(category),
            None,
            &[("priority", priority)],
        )
        .await;
    }

    let first_page = h
        .execute(
            r#"{
              queuePage(input: {
                pageIndex: 0
                pageSize: 1000
                sortField: NAME
                sortDirection: ASC
              }) {
                totalCount
                categories
                items { id duplicateSummary { action } }
              }
            }"#,
        )
        .await;
    assert_no_errors(&first_page);
    let first_page_data = response_data(&first_page);
    let first_page = &first_page_data["queuePage"];
    assert_eq!(first_page["totalCount"].as_u64(), Some(JOB_COUNT as u64));
    assert_eq!(first_page["items"].as_array().unwrap().len(), 500);
    assert_eq!(
        first_page["categories"],
        serde_json::json!(["movies", "tv"])
    );
    assert!(
        first_page["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["duplicateSummary"]["action"] == "ACCEPT")
    );

    let second_page = h
        .execute(
            r#"{
              queuePage(input: {
                pageIndex: 1
                pageSize: 500
                sortField: NAME
                sortDirection: ASC
              }) {
                totalCount
                items { id }
              }
            }"#,
        )
        .await;
    assert_no_errors(&second_page);
    let second_page_data = response_data(&second_page);
    let second_page = &second_page_data["queuePage"];
    assert_eq!(second_page["totalCount"].as_u64(), Some(JOB_COUNT as u64));
    let first_page_ids: std::collections::BTreeSet<u64> = first_page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect();
    let second_page_ids: std::collections::BTreeSet<u64> = second_page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect();
    assert_eq!(second_page_ids.len(), 500);
    assert!(first_page_ids.is_disjoint(&second_page_ids));

    let filtered = h
        .execute(
            r#"{
              queuePage(input: {
                pageIndex: 0
                pageSize: 500
                search: "queue-page-0999"
                states: [QUEUED]
                priorities: [LOW]
                categories: ["movies"]
                sortField: NAME
                sortDirection: ASC
              }) {
                totalCount
                items { name }
              }
            }"#,
        )
        .await;
    assert_no_errors(&filtered);
    let filtered_data = response_data(&filtered);
    let filtered = &filtered_data["queuePage"];
    assert_eq!(filtered["totalCount"].as_u64(), Some(1));
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);

    let first_tie_id = h.submit_test_nzb("queue-page.tie").await;
    let second_tie_id = h.submit_test_nzb("queue-page tie").await;
    let tied_items = h
        .execute(
            r#"{
              queuePage(input: {
                pageIndex: 0
                pageSize: 500
                search: "queue-page tie"
                sortField: NAME
                sortDirection: ASC
              }) {
                items { id }
              }
            }"#,
        )
        .await;
    assert_no_errors(&tied_items);
    let tied_items_data = response_data(&tied_items);
    let tied_ids = tied_items_data["queuePage"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tied_ids, vec![first_tie_id, second_tie_id]);
}

#[tokio::test]
async fn queue_items_project_bulk_duplicate_summary() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("duplicate-summary-queue").await;

    let resp = h
        .execute(
            r#"{
              queueItems {
                id
                duplicateSummary {
                  lifecycle
                  action
                  primaryReason
                  semantic { score state }
                }
              }
              queueSnapshot {
                items {
                  id
                  duplicateSummary {
                    lifecycle
                    action
                    primaryReason
                  }
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["queueItems"].as_array().unwrap();
    let item = items
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("submitted job should be in queue");
    let summary = &item["duplicateSummary"];
    assert!(matches!(
        summary["lifecycle"].as_str(),
        Some("RESERVED" | "ACTIVE")
    ));
    assert_eq!(summary["action"].as_str(), Some("ACCEPT"));
    assert!(summary["primaryReason"].is_null());
    assert!(summary["semantic"].is_null());

    let snapshot_item = data["queueSnapshot"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("queue snapshot should include the submitted job");
    assert_eq!(
        snapshot_item["duplicateSummary"]["action"].as_str(),
        Some("ACCEPT")
    );
}

#[tokio::test]
async fn duplicate_summary_projections_chunk_more_than_256_jobs() {
    const JOB_COUNT: usize = 257;

    let h = TestHarness::new().await;
    let mut job_ids = Vec::with_capacity(JOB_COUNT);
    for index in 0..JOB_COUNT {
        job_ids.push(
            h.submit_test_nzb(&format!("duplicate-summary-batch-{index:03}"))
                .await,
        );
    }

    let queue = h
        .execute(
            r#"{
              queueItems(first: 512) {
                id
                duplicateSummary { action primaryReason }
              }
            }"#,
        )
        .await;
    assert_no_errors(&queue);
    assert_duplicate_summaries_for_ids(
        response_data(&queue)["queueItems"].as_array().unwrap(),
        &job_ids,
    );

    for (index, job_id) in job_ids.iter().copied().enumerate() {
        h.insert_history_row(&sample_history_row(
            job_id,
            &format!("duplicate-summary-batch-{index:03}"),
            "complete",
            1_800_000_000 + index as i64,
        ));
    }
    // Archiving a job releases it from the scheduler; a row whose job is still
    // resident and non-terminal is not history and is excluded from both reads.
    h.shared_state.publish_jobs(Vec::new());
    let history = h
        .execute(
            r#"{
              historyItems(first: 512) {
                id
                duplicateSummary { action primaryReason }
              }
              historyPage(input: { pageIndex: 0, pageSize: 512 }) {
                items {
                  id
                  duplicateSummary { action primaryReason }
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&history);
    let data = response_data(&history);
    assert_duplicate_summaries_for_ids(data["historyItems"].as_array().unwrap(), &job_ids);
    assert_duplicate_summaries_for_ids(data["historyPage"]["items"].as_array().unwrap(), &job_ids);
}

#[tokio::test]
async fn history_items_is_bounded_by_default_and_at_most() {
    let h = TestHarness::new().await;
    for index in 0..1_005u64 {
        h.insert_history_row(&sample_history_row(
            10_000 + index,
            &format!("bounded-history-{index:04}"),
            "complete",
            1_800_000_000 + index as i64,
        ));
    }
    let resp = h
        .execute(
            r#"{
              unnamed: historyItems { id }
              large: historyItems(first: 5000) { id }
              small: historyItems(first: 3) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["unnamed"].as_array().unwrap().len(), 1_000);
    assert_eq!(data["large"].as_array().unwrap().len(), 1_000);
    let small: Vec<u64> = data["small"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect();
    assert_eq!(small, vec![11_004, 11_003, 11_002]);
}

fn assert_duplicate_summaries_for_ids(items: &[serde_json::Value], expected_ids: &[u64]) {
    assert_eq!(items.len(), expected_ids.len());
    for job_id in expected_ids {
        let item = items
            .iter()
            .find(|item| item["id"].as_u64() == Some(*job_id))
            .expect("every requested job should be returned");
        assert_eq!(item["duplicateSummary"]["action"].as_str(), Some("ACCEPT"));
        assert!(item["duplicateSummary"]["primaryReason"].is_null());
    }
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
    h.wait_for_job_status(id_a, weaver_server_core::JobStatus::Paused)
        .await;

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
    h.wait_for_job_status(id_paused, weaver_server_core::JobStatus::Paused)
        .await;

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
    h.wait_for_job_status(id_paused, weaver_server_core::JobStatus::Paused)
        .await;

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
async fn queue_facades_hold_a_terminal_job_until_history_shows_it() {
    use weaver_server_core::{JobId, JobStatus};

    let h = TestHarness::new().await;
    let template_id = h.submit_test_nzb("retirement-template").await;
    let mut job = h.handle.get_job(JobId(template_id)).unwrap();
    let id = JobId(900_001);
    job.job_id = id;
    let mut row = sample_history_row(id.0, "retiring-history", "complete", 1_700_000_001);
    row.output_dir = None;
    row.nzb_path = None;
    h.insert_history_row(&row);
    let query = format!(
        "{{ historyItem(id: {0}) {{ id }} historyItems {{ id }} historyItemsCount
             historyPage(input: {{ pageIndex: 0, pageSize: 25 }}) {{ totalCount items {{ id }} }}
             queueItem(id: {0}) {{ id }} queueItems {{ id }} queueSummary {{ totalItems }}
             queueSnapshot {{ items {{ id }} }}
             queuePage(input: {{ pageIndex: 0, pageSize: 25 }}) {{ totalCount items {{ id }} }} }}",
        id.0
    );
    let queued = |data: &serde_json::Value| {
        assert_eq!(data["queueItem"]["id"].as_u64(), Some(id.0));
        assert_eq!(data["queueItems"][0]["id"].as_u64(), Some(id.0));
        assert_eq!(data["queueSummary"]["totalItems"].as_u64(), Some(1));
        assert_eq!(data["queueSnapshot"]["items"][0]["id"].as_u64(), Some(id.0));
        assert_eq!(data["queuePage"]["totalCount"].as_u64(), Some(1));
    };

    // This fixture holds queue membership independently of the runtime, just as
    // an archive committed before its retirement acknowledgement is consumed.
    for status in [
        JobStatus::Complete,
        JobStatus::Failed {
            error: "fixture failure".into(),
        },
        JobStatus::Queued,
    ] {
        let cancelled = status == JobStatus::Queued;
        job.status = status;
        h.shared_state.publish_jobs(vec![job.clone()]);
        if cancelled {
            h.handle.cancel_job(id).await.unwrap();
            assert!(h.handle.get_job(id).is_ok());
        }
        let response = h.execute(&query).await;
        assert_no_errors(&response);
        let data = response_data(&response);
        assert!(data["historyItem"].is_null());
        assert!(data["historyItems"].as_array().unwrap().is_empty());
        assert_eq!(data["historyItemsCount"].as_u64(), Some(0));
        assert_eq!(data["historyPage"]["totalCount"].as_u64(), Some(0));
        assert!(data["historyPage"]["items"].as_array().unwrap().is_empty());
        queued(&data);
    }

    h.shared_state.publish_jobs(vec![]);
    let response = h.execute(&query).await;
    assert_no_errors(&response);
    let data = response_data(&response);
    assert_eq!(data["historyItem"]["id"].as_u64(), Some(id.0));
    assert_eq!(data["historyItemsCount"].as_u64(), Some(1));
    assert_eq!(data["historyPage"]["totalCount"].as_u64(), Some(1));
    assert!(data["queueItem"].is_null());
    assert!(data["queueItems"].as_array().unwrap().is_empty());
    assert_eq!(data["queueSummary"]["totalItems"].as_u64(), Some(0));
    assert!(
        data["queueSnapshot"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(data["queuePage"]["totalCount"].as_u64(), Some(0));
    let deleted = h
        .execute(&format!(
            "mutation {{ deleteHistory(id: {}) {{ id }} }}",
            id.0
        ))
        .await;
    assert_no_errors(&deleted);
    assert!(h.db.get_job_history(id.0).unwrap().is_none());
}

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
async fn history_items_respects_bounded_pagination_before_returning_rows() {
    let h = TestHarness::new().await;
    for job_id in 1..=5 {
        h.insert_history_row(&sample_history_row(
            job_id,
            &format!("facade-{job_id}"),
            "complete",
            1_700_000_000 + if job_id <= 2 { 1 } else { job_id as i64 },
        ));
    }

    let resp = h
        .execute(
            r#"{
              historyItems(first: 2) {
                id
                name
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["historyItems"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["id"].as_u64().unwrap(), 5);
    assert_eq!(items[1]["id"].as_u64().unwrap(), 4);
}

#[tokio::test]
async fn history_items_project_bulk_duplicate_summary() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("duplicate-summary-history").await;
    h.insert_history_row(&sample_history_row(
        job_id,
        "duplicate-summary-history",
        "complete",
        1_700_000_123,
    ));
    // Archiving a job releases it from the scheduler; a row whose job is still
    // resident and non-terminal is not history and is excluded from both reads.
    h.shared_state.publish_jobs(Vec::new());

    let resp = h
        .execute(
            r#"{
              historyItems(first: 10) {
                id
                duplicateSummary {
                  lifecycle
                  action
                  primaryReason
                }
              }
              historyPage(input: { pageIndex: 0, pageSize: 25 }) {
                items {
                  id
                  duplicateSummary {
                    lifecycle
                    action
                    primaryReason
                  }
                }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["historyItems"].as_array().unwrap();
    let item = items
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("history row should be returned");
    let summary = &item["duplicateSummary"];
    assert_eq!(summary["lifecycle"].as_str(), Some("SUCCEEDED"));
    assert_eq!(summary["action"].as_str(), Some("ACCEPT"));
    assert!(summary["primaryReason"].is_null());

    let page_item = data["historyPage"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"].as_u64() == Some(job_id))
        .expect("history page should include the history row");
    assert_eq!(
        page_item["duplicateSummary"]["action"].as_str(),
        Some("ACCEPT")
    );
}

// Clients do not model a partial delivery yet: its row reads as failed, and
// asking for failed rows brings it back.
#[tokio::test]
async fn history_reports_a_partial_job_as_failed() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        1,
        "whole-history",
        "complete",
        1_700_000_001,
    ));
    h.insert_history_row(&sample_history_row(
        2,
        "partial-history",
        "partial",
        1_700_000_002,
    ));

    let resp = h
        .execute(
            r#"{
              historyItems(filter: { states: [FAILED] }, first: 10) {
                id
                state
              }
              historyItemsCount(filter: { states: [FAILED] })
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["historyItems"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"].as_u64().unwrap(), 2);
    assert_eq!(items[0]["state"].as_str().unwrap(), "FAILED");
    assert_eq!(data["historyItemsCount"].as_u64().unwrap(), 1);
}

#[tokio::test]
async fn history_items_active_only_state_filter_returns_empty() {
    let h = TestHarness::new().await;
    h.insert_history_row(&sample_history_row(
        1,
        "completed-history",
        "complete",
        1_700_000_001,
    ));

    let resp = h
        .execute(
            r#"{
              historyItems(filter: { states: [DOWNLOADING, EXTRACTING] }, first: 10) {
                id
              }
              historyItemsCount(filter: { states: [DOWNLOADING, EXTRACTING] })
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["historyItems"].as_array().unwrap().is_empty());
    assert_eq!(data["historyItemsCount"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn history_items_title_attribute_filter_matches_count() {
    let h = TestHarness::new().await;
    for (job_id, title_id) in [(1, "title-a"), (2, "title-b"), (3, "title-a")] {
        let mut row = sample_history_row(
            job_id,
            &format!("title-history-{job_id}"),
            "complete",
            1_700_000_000 + job_id as i64,
        );
        row.metadata = Some(
            serde_json::to_string(&vec![
                ("*scryer_title_id".to_string(), title_id.to_string()),
                ("source".to_string(), "scryer".to_string()),
            ])
            .unwrap(),
        );
        h.insert_history_row(&row);
    }

    let resp = h
        .execute(
            r#"{
              historyItems(
                filter: { attributeEquals: { key: "*scryer_title_id", value: "title-a" } }
                first: 10
              ) {
                id
                attributes { key value }
              }
              historyItemsCount(
                filter: { attributeEquals: { key: "*scryer_title_id", value: "title-a" } }
              )
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["historyItems"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(data["historyItemsCount"].as_u64().unwrap(), 2);
    assert_eq!(items[0]["id"].as_u64().unwrap(), 3);
    assert_eq!(items[1]["id"].as_u64().unwrap(), 1);
}

#[tokio::test]
async fn history_items_hidden_metadata_keys_are_not_attributes() {
    let h = TestHarness::new().await;
    let mut row = sample_history_row(1, "hidden-history", "complete", 1_700_000_001);
    row.metadata = Some(
        serde_json::to_string(&vec![
            (
                CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                "req-1".to_string(),
            ),
            ("public".to_string(), "yes".to_string()),
        ])
        .unwrap(),
    );
    h.insert_history_row(&row);

    let resp = h
        .execute(
            r#"{
              hiddenClient: historyItems(filter: { hasAttributeKey: "__weaver_client_request_id" }, first: 10) { id }
              publicMatch: historyItems(filter: { hasAttributeKey: "public" }, first: 10) {
                id
                attributes { key value }
                clientRequestId
              }
              hiddenCount: historyItemsCount(filter: { attributeEquals: { key: "__weaver_client_request_id", value: "req-1" } })
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["hiddenClient"].as_array().unwrap().is_empty());
    assert_eq!(data["hiddenCount"].as_u64().unwrap(), 0);
    let public = data["publicMatch"].as_array().unwrap();
    assert_eq!(public.len(), 1);
    assert_eq!(public[0]["clientRequestId"].as_str().unwrap(), "req-1");
    assert_eq!(public[0]["attributes"].as_array().unwrap().len(), 1);
    assert_eq!(
        public[0]["attributes"][0]["key"].as_str().unwrap(),
        "public"
    );
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

fn assert_file_delete_forbidden(response: &async_graphql::Response) {
    assert_has_errors(response);
    assert_eq!(response.errors[0].message, "control scope required");
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.get("code"))
            .and_then(|value| match value {
                async_graphql::Value::String(code) => Some(code.as_str()),
                _ => None,
            }),
        Some("FORBIDDEN")
    );
}

fn file_delete_mutations() -> [(&'static str, &'static str); 6] {
    [
        (
            "acceptHistoryDelete",
            "mutation { acceptHistoryDelete(input: { mode: IDS, ids: [1, 2], deleteFiles: true }) { operationId } }",
        ),
        (
            "acceptHistoryDeleteAll",
            "mutation { acceptHistoryDelete(input: { mode: ALL_HISTORY, deleteFiles: true }) { operationId } }",
        ),
        (
            "deleteHistory",
            "mutation { deleteHistory(id: 1, deleteFiles: true) { id } }",
        ),
        (
            "deleteHistoryBatch",
            "mutation { deleteHistoryBatch(ids: [1, 2], deleteFiles: true) { id } }",
        ),
        (
            "deleteAllHistory",
            "mutation { deleteAllHistory(deleteFiles: true) { id } }",
        ),
        (
            "removeHistoryItems",
            "mutation { removeHistoryItems(ids: [1, 2], deleteFiles: true) { success } }",
        ),
    ]
}

#[tokio::test]
async fn read_scope_cannot_delete_completed_files_through_any_history_mutation() {
    for (name, mutation) in file_delete_mutations() {
        let h = TestHarness::new_without_history_delete_worker().await;
        let output_root = tempfile::tempdir().unwrap();
        for id in [1, 2] {
            let output_dir = output_root.path().join(format!("job-{id}"));
            std::fs::create_dir(&output_dir).unwrap();
            std::fs::write(output_dir.join("payload.bin"), b"retained").unwrap();
            let mut row =
                sample_history_row(id, &format!("history-{id}"), "complete", 100 + id as i64);
            row.output_dir = Some(output_dir.to_string_lossy().to_string());
            h.insert_history_row(&row);
        }

        let response = h
            .execute_as(mutation, weaver_server_core::auth::CallerScope::Read)
            .await;
        assert_file_delete_forbidden(&response);
        assert!(
            h.db.get_job_history(1).unwrap().is_some()
                && h.db.get_job_history(2).unwrap().is_some(),
            "{name} must reject atomically before removing history"
        );
        assert!(
            output_root.path().join("job-1/payload.bin").is_file(),
            "{name}"
        );
        assert!(
            output_root.path().join("job-2/payload.bin").is_file(),
            "{name}"
        );
        assert!(
            h.db.list_history_delete_operations(false)
                .unwrap()
                .is_empty(),
            "{name} must not queue a durable operation"
        );
        assert!(
            h.db.list_history_delete_row_states(&[1, 2])
                .unwrap()
                .is_empty(),
            "{name} must not lock history rows"
        );
    }
}

#[tokio::test]
async fn control_scope_deletes_completed_files_through_every_history_mutation() {
    for (name, mutation) in file_delete_mutations() {
        let h = TestHarness::new().await;
        let output_root = tempfile::tempdir().unwrap();
        for id in [1, 2] {
            let output_dir = output_root.path().join(format!("job-{id}"));
            std::fs::create_dir(&output_dir).unwrap();
            std::fs::write(output_dir.join("payload.bin"), b"delete me").unwrap();
            weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(
                &output_dir,
                weaver_server_core::JobId(id),
            )
            .unwrap();
            let mut row =
                sample_history_row(id, &format!("history-{id}"), "complete", 100 + id as i64);
            row.output_dir = Some(output_dir.to_string_lossy().to_string());
            h.insert_history_row(&row);
        }

        let response = h
            .execute_as(mutation, weaver_server_core::auth::CallerScope::Control)
            .await;
        assert_no_errors(&response);

        // Durable acceptance returns before the worker deletes the output.
        let deleted_ids: &[u64] = if name == "deleteHistory" {
            &[1]
        } else {
            &[1, 2]
        };
        for _ in 0..40 {
            if deleted_ids.iter().all(|id| {
                h.db.get_job_history(*id).unwrap().is_none()
                    && !output_root.path().join(format!("job-{id}")).exists()
            }) {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
        for id in deleted_ids {
            assert!(h.db.get_job_history(*id).unwrap().is_none(), "{name}: {id}");
            assert!(
                !output_root.path().join(format!("job-{id}")).exists(),
                "{name}: {id}"
            );
        }
        if name == "deleteHistory" {
            assert!(h.db.get_job_history(2).unwrap().is_some());
            assert!(output_root.path().join("job-2/payload.bin").is_file());
        }
    }
}

#[tokio::test]
async fn control_scope_can_remove_history_without_deleting_completed_files() {
    let h = TestHarness::new().await;
    let output_root = tempfile::tempdir().unwrap();
    let output_dir = output_root.path().join("completed-job");
    std::fs::create_dir(&output_dir).unwrap();
    std::fs::write(output_dir.join("payload.bin"), b"retained").unwrap();
    let mut row = sample_history_row(1, "history-only", "complete", 101);
    row.output_dir = Some(output_dir.to_string_lossy().to_string());
    h.insert_history_row(&row);

    let response = h
        .execute_as(
            "mutation { deleteHistory(id: 1, deleteFiles: false) { id } }",
            weaver_server_core::auth::CallerScope::Control,
        )
        .await;

    assert_no_errors(&response);
    assert!(h.db.get_job_history(1).unwrap().is_none());
    assert!(output_dir.join("payload.bin").is_file());
}

#[tokio::test]
async fn admin_scope_deletes_completed_output_through_the_direct_path() {
    let h = TestHarness::new().await;
    let output_root = tempfile::tempdir().unwrap();
    let output_dir = output_root.path().join("completed-job");
    std::fs::create_dir(&output_dir).unwrap();
    std::fs::write(output_dir.join("payload.bin"), b"delete me").unwrap();
    weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(
        &output_dir,
        weaver_server_core::JobId(1),
    )
    .unwrap();
    let mut row = sample_history_row(1, "admin-direct", "complete", 101);
    row.output_dir = Some(output_dir.to_string_lossy().to_string());
    h.insert_history_row(&row);

    let response = h
        .execute_as(
            "mutation { deleteHistory(id: 1, deleteFiles: true) { id } }",
            weaver_server_core::auth::CallerScope::Admin,
        )
        .await;

    assert_no_errors(&response);
    assert!(h.db.get_job_history(1).unwrap().is_none());
    assert!(!output_dir.exists());
}

#[tokio::test]
async fn authorized_durable_file_delete_removes_completed_output() {
    let h = TestHarness::new().await;
    let output_root = tempfile::tempdir().unwrap();
    let output_dir = output_root.path().join("completed-job");
    std::fs::create_dir(&output_dir).unwrap();
    std::fs::write(output_dir.join("payload.bin"), b"delete me").unwrap();
    let mut row = sample_history_row(1, "durable-local", "complete", 101);
    weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(
        &output_dir,
        weaver_server_core::JobId(1),
    )
    .unwrap();
    row.output_dir = Some(output_dir.to_string_lossy().to_string());
    h.insert_history_row(&row);

    let response = h
        .execute(
            "mutation { acceptHistoryDelete(input: { mode: IDS, ids: [1], deleteFiles: true }) { operationId } }",
        )
        .await;
    assert_no_errors(&response);

    for _ in 0..20 {
        if h.db.get_job_history(1).unwrap().is_none() && !output_dir.exists() {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }

    panic!("authorized durable delete did not remove the history row and output directory");
}

#[tokio::test]
async fn delete_history_nonexistent() {
    let h = TestHarness::new().await;
    // Deleting a nonexistent history entry succeeds silently (idempotent).
    let resp = h
        .execute("mutation { deleteHistory(id: 999999) { id } }")
        .await;
    assert_no_errors(&resp);
}

// The kinds of the in-memory queue events recorded for one item.
async fn item_event_kinds(h: &TestHarness, item_id: u64) -> Vec<String> {
    let resp = h
        .execute(&format!(
            "{{ historyEvents(itemId: {item_id}) {{ kind itemId }} }}"
        ))
        .await;
    assert_no_errors(&resp);
    response_data(&resp)["historyEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["kind"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn remove_history_items_announces_every_removed_id_before_a_refusal() {
    let h = TestHarness::new_without_history_delete_worker().await;
    let removed = 41_001;
    h.insert_history_row(&sample_history_row(removed, "removed", "complete", 100));
    let active = h.submit_test_nzb("still-active").await;

    let resp = h
        .execute(&format!(
            "mutation {{ removeHistoryItems(ids: [{removed}, {active}]) {{ success }} }}"
        ))
        .await;

    // The refusal still surfaces, and the id that was deleted says so.
    assert_has_errors(&resp);
    assert!(h.db.get_job_history(removed).unwrap().is_none());
    assert_eq!(item_event_kinds(&h, removed).await, vec!["ITEM_REMOVED"]);
    assert!(
        !item_event_kinds(&h, active)
            .await
            .iter()
            .any(|kind| kind == "ITEM_REMOVED")
    );
}

#[tokio::test]
async fn delete_all_history_deletes_every_listed_row_across_batches() {
    let h = TestHarness::new_without_history_delete_worker().await;
    // More rows than one batch holds, so the listing goes in several.
    let rows = weaver_server_core::HISTORY_DELETE_BATCH_SIZE as u64 + 1;
    for id in 1..=rows {
        h.insert_history_row(&sample_history_row(
            42_000 + id,
            &format!("history-{id}"),
            "complete",
            100 + id as i64,
        ));
    }
    // A live job that has not finished is not history, so delete-all neither
    // lists nor refuses it.
    let active = h.submit_test_nzb("delete-all-active").await;

    let resp = h.execute("mutation { deleteAllHistory { id } }").await;

    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["deleteAllHistory"].as_array().unwrap().is_empty());
    for id in 1..=rows {
        assert!(h.db.get_job_history(42_000 + id).unwrap().is_none());
    }
    assert!(h.handle.get_job(weaver_server_core::JobId(active)).is_ok());
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
    // The worker must stay off: this test seeds a QUEUED delete operation and
    // asserts it is still QUEUED, which the background worker would otherwise
    // claim and flip to RUNNING.
    let h = TestHarness::new_without_history_delete_worker().await;
    h.insert_history_row(&sample_history_row(
        1,
        "alpha.release",
        "complete",
        1_700_000_001,
    ));
    let operation_id =
        h.db.insert_history_delete_operation(&[1], true, true)
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
    // See history_page_exposes_delete_operation_state: the worker would race the
    // seeded QUEUED operation, so keep it off for this initial-state assertion.
    let h = TestHarness::new_without_history_delete_worker().await;
    h.insert_history_row(&sample_history_row(
        7,
        "detail.release",
        "failed",
        1_700_000_007,
    ));
    let operation_id =
        h.db.insert_history_delete_operation(&[7], false, false)
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
    h.db.insert_history_delete_operation(&[21], false, false)
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
    // Admission creates the working directory before returning the row.
    assert!(std::path::Path::new(job["outputDir"].as_str().unwrap()).is_dir());
    assert!(job["error"].is_null());
}

// The jobs the pipeline holds installed, once its window has settled on the
// first `window` rows.
async fn installed_jobs(h: &TestHarness, window: usize) -> Vec<u64> {
    let mut changes = h.handle.subscribe_job_changes();
    loop {
        changes.mark_unchanged();
        let diagnostics = h.handle.pipeline_diagnostics().await.unwrap();
        if diagnostics.jobs.len() == window {
            let mut ids: Vec<u64> = diagnostics.jobs.iter().map(|job| job.job_id).collect();
            ids.sort_unstable();
            return ids;
        }
        changes.changed().await.unwrap();
    }
}

#[tokio::test]
async fn a_job_past_the_queue_window_answers_from_its_row_and_installs_nothing() {
    const WINDOW: usize = 100;
    const QUEUED: usize = WINDOW + 10;
    let h = TestHarness::new().await;
    let mut ids = Vec::with_capacity(QUEUED);
    for index in 0..QUEUED {
        ids.push(h.submit_test_nzb(&format!("window-{index}")).await);
    }
    let installed = installed_jobs(&h, WINDOW).await;
    assert_eq!(installed, ids[..WINDOW]);
    let past = *ids.last().unwrap();

    // The queue page lists every row in queue order.
    let resp = h
        .execute(&format!(
            r#"{{ queuePage(input: {{ pageIndex: 0, pageSize: {QUEUED} }}) {{ totalCount items {{ id }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["queuePage"]["totalCount"].as_u64(),
        Some(QUEUED as u64)
    );
    let listed: Vec<u64> = data["queuePage"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_u64().unwrap())
        .collect();
    assert_eq!(listed, ids);

    // The detail of a job past the window comes from its row and the
    // database.
    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {past}) {{ queueItem {{ id state }} historyItem {{ id }} jobEvents {{ kind }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["id"].as_u64(),
        Some(past)
    );
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["state"].as_str(),
        Some("QUEUED")
    );
    assert!(data["jobDetailSnapshot"]["historyItem"].is_null());
    // Its events are the ones an installed queued job shows.
    let first = ids[0];
    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {first}) {{ jobEvents {{ kind }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["jobEvents"],
        response_data(&resp)["jobDetailSnapshot"]["jobEvents"]
    );
    assert_eq!(installed_jobs(&h, WINDOW).await, ids[..WINDOW]);
}

// ---------------------------------------------------------------------------
// Remove history files
// ---------------------------------------------------------------------------

fn error_code(resp: &async_graphql::Response) -> Option<String> {
    resp.errors
        .first()?
        .extensions
        .as_ref()?
        .get("code")
        .and_then(|value| match value {
            async_graphql::Value::String(code) => Some(code.clone()),
            _ => None,
        })
}

// A files removal emits no pipeline event and runs on the background
// worker; an open job page still shows the entry's removal time once the
// files are gone.
#[tokio::test(start_paused = true)]
async fn an_open_job_page_sees_the_files_removed() {
    use tokio_stream::StreamExt;

    let h = TestHarness::new().await;
    let temp = tempfile::tempdir().unwrap();
    let owned = temp.path().join("Watched.Release");
    std::fs::create_dir_all(&owned).unwrap();
    std::fs::write(owned.join("movie.mkv"), b"movie").unwrap();
    weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(
        &owned,
        weaver_server_core::JobId(1),
    )
    .unwrap();
    let mut row = sample_history_row(1, "Watched.Release", "complete", 1_700_000_001);
    row.output_dir = Some(owned.to_string_lossy().into_owned());
    h.insert_history_row(&row);

    let request = async_graphql::Request::new(
        "subscription { jobDetailUpdates(jobId: 1) { historyItem { id filesRemovedAt } } }",
    )
    .data(weaver_server_api::auth::CallerScope::Read);
    let mut stream = h.schema.execute_stream(request);
    let first = stream.next().await.expect("stream should stay open");
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    let first = first.data.into_json().unwrap();
    assert!(first["jobDetailUpdates"]["historyItem"]["filesRemovedAt"].is_null());

    let resp = h
        .execute("mutation { removeHistoryFiles(ids: [1]) { results { id outcome } } }")
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["removeHistoryFiles"]["results"][0]["outcome"].as_str(),
        Some("ACCEPTED")
    );

    // Each snapshot the stream sends until the one that shows the stamp.
    loop {
        let next = stream.next().await.expect("stream should stay open");
        assert!(next.errors.is_empty(), "{:?}", next.errors);
        let next = next.data.into_json().unwrap();
        if next["jobDetailUpdates"]["historyItem"]["filesRemovedAt"].is_string() {
            break;
        }
    }
    assert!(!owned.exists());
}

// The request only queues the work: each distinct id is answered in the
// order given, nothing on disk is touched before the worker runs, and the
// accepted entries show the queued removal.
#[tokio::test]
async fn remove_history_files_answers_each_id_once_in_the_order_given() {
    let h = TestHarness::new_without_history_delete_worker().await;
    let temp = tempfile::tempdir().unwrap();
    let owned = temp.path().join("Owned.Release");
    std::fs::create_dir_all(&owned).unwrap();
    std::fs::write(owned.join("movie.mkv"), b"movie").unwrap();
    weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(
        &owned,
        weaver_server_core::JobId(1),
    )
    .unwrap();
    let mut owned_row = sample_history_row(1, "Owned.Release", "complete", 1_700_000_001);
    owned_row.output_dir = Some(owned.to_string_lossy().into_owned());
    h.insert_history_row(&owned_row);
    h.insert_history_row(&sample_history_row(
        2,
        "Removed.Release",
        "failed",
        1_700_000_002,
    ));
    weaver_server_core::pipeline::test_support::stamp_files_removed(
        &h.db,
        weaver_server_core::JobId(2),
        Default::default(),
        1_700_000_100,
    );
    h.insert_history_row(&sample_history_row(
        3,
        "Deleting.Release",
        "complete",
        1_700_000_003,
    ));
    h.db.insert_history_delete_operation(&[3], false, false)
        .unwrap();

    let resp = h
        .execute(
            r#"mutation {
                removeHistoryFiles(ids: [3, 99, 1, 3, 2, 1]) {
                    operationId
                    acceptedIds
                    totalTargets
                    results { id outcome }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let answer = response_data(&resp)["removeHistoryFiles"].clone();
    let answered: Vec<(u64, &str)> = answer["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| {
            (
                result["id"].as_u64().unwrap(),
                result["outcome"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        answered,
        vec![
            (3, "BUSY"),
            (99, "NOT_FOUND"),
            (1, "ACCEPTED"),
            (2, "ALREADY_REMOVED"),
        ]
    );
    assert!(answer["operationId"].is_u64());
    assert_eq!(answer["acceptedIds"], serde_json::json!([1]));
    assert_eq!(answer["totalTargets"].as_u64(), Some(1));
    assert!(owned.join("movie.mkv").is_file());

    let resp = h
        .execute(
            r#"{
              historyItem(id: 1) {
                filesRemovedAt
                deleteOperation { state locked action filesRemovalOutcome errorMessage }
              }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let item = response_data(&resp)["historyItem"].clone();
    assert!(item["filesRemovedAt"].is_null());
    assert_eq!(
        item["deleteOperation"],
        serde_json::json!({
            "state": "QUEUED",
            "locked": true,
            "action": "REMOVE_FILES",
            "filesRemovalOutcome": null,
            "errorMessage": null,
        })
    );
}

#[tokio::test]
async fn remove_history_files_takes_one_to_a_hundred_distinct_ids() {
    let h = TestHarness::new().await;
    let too_many = (1..=101)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    for ids in [String::new(), too_many] {
        let resp = h
            .execute(&format!(
                "mutation {{ removeHistoryFiles(ids: [{ids}]) {{ operationId }} }}"
            ))
            .await;
        assert_has_errors(&resp);
        assert_eq!(error_code(&resp).as_deref(), Some("INVALID_INPUT"));
    }
    // A hundred distinct ids, repeated, is a hundred.
    let hundred = (1..=100)
        .chain(1..=100)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let resp = h
        .execute(&format!(
            "mutation {{ removeHistoryFiles(ids: [{hundred}]) {{ results {{ id outcome }} }} }}"
        ))
        .await;
    assert_no_errors(&resp);
    let results = response_data(&resp)["removeHistoryFiles"]["results"]
        .as_array()
        .cloned()
        .unwrap();
    assert_eq!(results.len(), 100);
    assert!(
        results
            .iter()
            .all(|result| result["outcome"] == "NOT_FOUND")
    );
}

// ---------------------------------------------------------------------------
// Output files
// ---------------------------------------------------------------------------

// The listing the field gave before it was paged: every file, following
// links, by file name ignoring ASCII case.
fn unpaged_listing(dir: &std::path::Path) -> Vec<(String, String, u64)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String, u64)>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            let meta = std::fs::metadata(&path).unwrap();
            if meta.is_dir() {
                walk(&path, out);
            } else {
                out.push((
                    path.file_name().unwrap().to_string_lossy().into_owned(),
                    path.to_string_lossy().into_owned(),
                    meta.len(),
                ));
            }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.sort_by_key(|file| file.0.to_ascii_lowercase());
    files
}

async fn output_files_page(h: &TestHarness, job_id: u64, args: &str) -> serde_json::Value {
    let resp = h
        .execute(&format!(
            "{{ jobOutputFiles(jobId: {job_id}{args}) {{ outputDir totalBytes totalCount removed nextCursor files {{ name path sizeBytes }} }} }}"
        ))
        .await;
    assert_no_errors(&resp);
    response_data(&resp)["jobOutputFiles"].clone()
}

fn page_files(page: &serde_json::Value) -> Vec<(String, String, u64)> {
    page["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| {
            (
                file["name"].as_str().unwrap().to_string(),
                file["path"].as_str().unwrap().to_string(),
                file["sizeBytes"].as_u64().unwrap(),
            )
        })
        .collect()
}

// A folder on disk pages 100 files at a time in the order the unpaged
// answer gave, with the totals of every file on each page; a larger page is
// held to 1000.
#[tokio::test]
async fn output_files_page_through_the_folder_on_disk() {
    let h = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let mut total_bytes = 0;
    for n in 0..1_005u64 {
        let folder = dir.path().join(format!("Part{}", n % 3));
        std::fs::create_dir_all(&folder).unwrap();
        let name = if n % 2 == 0 {
            format!("File-{n:04}.bin")
        } else {
            format!("file-{n:04}.BIN")
        };
        std::fs::write(folder.join(name), vec![0u8; (n % 7) as usize]).unwrap();
        total_bytes += n % 7;
    }
    let mut row = sample_history_row(7_101, "Paged", "complete", 1_700_000_000);
    row.output_dir = Some(dir.path().to_string_lossy().into_owned());
    h.insert_history_row(&row);
    let unpaged = unpaged_listing(dir.path());

    let first = output_files_page(&h, 7_101, "").await;
    assert_eq!(page_files(&first), unpaged[..100]);
    assert_eq!(first["totalCount"].as_u64(), Some(1_005));
    assert_eq!(first["totalBytes"].as_u64(), Some(total_bytes));
    assert_eq!(first["removed"].as_bool(), Some(false));

    // Every page after it, to the end: each file once, in order.
    let mut listed = page_files(&first);
    let mut cursor = first["nextCursor"].as_str().map(str::to_string);
    while let Some(after) = cursor {
        let page = output_files_page(&h, 7_101, &format!(", first: 300, after: \"{after}\"")).await;
        assert_eq!(page["totalCount"].as_u64(), Some(1_005));
        assert_eq!(page["totalBytes"].as_u64(), Some(total_bytes));
        listed.extend(page_files(&page));
        cursor = page["nextCursor"].as_str().map(str::to_string);
    }
    assert_eq!(listed, unpaged);

    let largest = output_files_page(&h, 7_101, ", first: 5000").await;
    assert_eq!(page_files(&largest), unpaged[..1_000]);
    assert!(largest["nextCursor"].is_string());

    let exact = output_files_page(&h, 7_101, ", first: 1005").await;
    assert_eq!(page_files(&exact).len(), 1_000);
}

// Once the files were removed the answer is the record kept of them: the
// same paging, absolute paths under the folder, the totals of the whole
// folder, and no page past the last file kept.
#[tokio::test]
async fn output_files_of_removed_files_page_through_the_record() {
    use weaver_server_core::history::output_files::{OutputFile, OutputFileListing};
    let h = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let gone = dir.path().join("Gone");
    let mut row = sample_history_row(7_102, "Gone", "complete", 1_700_000_000);
    row.output_dir = Some(gone.to_string_lossy().into_owned());
    h.insert_history_row(&row);
    let kept = ["a.nfo", "Sub/b.mkv", "c.srt"];
    weaver_server_core::pipeline::test_support::stamp_files_removed(
        &h.db,
        weaver_server_core::JobId(7_102),
        OutputFileListing {
            files: kept
                .iter()
                .enumerate()
                .map(|(n, rel)| OutputFile {
                    rel_path: rel.to_string(),
                    name: rel.rsplit('/').next().unwrap().to_string(),
                    size_bytes: n as u64 + 1,
                })
                .collect(),
            // The folder held more than the record kept.
            total_count: 5,
            total_bytes: 40,
        },
        1_700_000_100,
    );

    let first = output_files_page(&h, 7_102, ", first: 2").await;
    assert_eq!(first["removed"].as_bool(), Some(true));
    assert_eq!(first["totalCount"].as_u64(), Some(5));
    assert_eq!(first["totalBytes"].as_u64(), Some(40));
    assert_eq!(
        page_files(&first),
        vec![
            (
                "a.nfo".into(),
                gone.join("a.nfo").to_string_lossy().into_owned(),
                1
            ),
            (
                "b.mkv".into(),
                gone.join("Sub/b.mkv").to_string_lossy().into_owned(),
                2
            ),
        ]
    );
    let after = first["nextCursor"].as_str().unwrap().to_string();

    let second = output_files_page(&h, 7_102, &format!(", first: 2, after: \"{after}\"")).await;
    assert_eq!(page_files(&second).len(), 1);
    assert_eq!(second["files"][0]["name"].as_str(), Some("c.srt"));
    assert!(
        second["nextCursor"].is_null(),
        "the record ends at its last file"
    );

    // The totals alone read no file.
    let resp = h
        .execute("{ jobOutputFiles(jobId: 7102) { totalCount removed nextCursor } }")
        .await;
    assert_no_errors(&resp);
    assert_eq!(
        response_data(&resp)["jobOutputFiles"]["totalCount"].as_u64(),
        Some(5)
    );
}

// A cursor that is not one is refused.
#[tokio::test]
async fn output_files_refuse_a_bad_cursor() {
    let h = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let mut row = sample_history_row(7_103, "Bad", "complete", 1_700_000_000);
    row.output_dir = Some(dir.path().to_string_lossy().into_owned());
    h.insert_history_row(&row);
    let resp = h
        .execute("{ jobOutputFiles(jobId: 7103, after: \"not a cursor\") { totalCount } }")
        .await;
    assert_has_errors(&resp);
}

// The folder on disk lists no link and walks no linked folder, as the record
// a files removal keeps does not.
#[cfg(unix)]
#[tokio::test]
async fn output_files_on_disk_skip_symbolic_links() {
    let h = TestHarness::new().await;
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real.mkv"), b"ab").unwrap();
    std::fs::write(outside.path().join("elsewhere.mkv"), b"1234567").unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("linked")).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("elsewhere.mkv"),
        dir.path().join("link.mkv"),
    )
    .unwrap();
    let mut row = sample_history_row(7_104, "Linked", "complete", 1_700_000_000);
    row.output_dir = Some(dir.path().to_string_lossy().into_owned());
    h.insert_history_row(&row);

    let page = output_files_page(&h, 7_104, "").await;

    assert_eq!(
        page_files(&page),
        vec![(
            "real.mkv".into(),
            dir.path().join("real.mkv").to_string_lossy().into_owned(),
            2
        )]
    );
    assert_eq!(page["totalCount"].as_u64(), Some(1));
    assert_eq!(page["totalBytes"].as_u64(), Some(2));
}

// A job's password is never given out: the old reveal is refused, and the
// queue and history lists carry no password attribute.
#[tokio::test]
async fn no_read_returns_a_job_password() {
    let h = TestHarness::new().await;
    let mut row = sample_history_row(7_201, "Locked", "complete", 1_700_000_000);
    row.metadata = Some(
        serde_json::to_string(&vec![
            (
                weaver_server_core::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY,
                "sealed-secret",
            ),
            ("*Unpack:Password", "hunter2"),
            ("source", "api"),
        ])
        .unwrap(),
    );
    h.insert_history_row(&row);

    // The old reveal is gone from the schema.
    assert_has_errors(&h.execute("{ validatedArchivePassword(id: 7201) }").await);

    let resp = h
        .execute("{ historyItems(first: 10) { id hasPassword attributes { key value } } }")
        .await;
    assert_no_errors(&resp);
    let text = serde_json::to_string(&response_data(&resp)).unwrap();
    assert!(!text.contains("hunter2"), "{text}");
    assert!(!text.contains("sealed-secret"), "{text}");
    assert!(text.contains("\"hasPassword\":true"), "{text}");
}

// The job page's download summary: null for an entry that finished before
// summaries were kept, and for a job still in the queue. The detail of the
// locked entry carries no password either.
#[tokio::test]
async fn download_summary_is_null_for_a_legacy_entry_and_a_queued_job() {
    let h = TestHarness::new().await;
    let mut row = sample_history_row(7_202, "Legacy", "complete", 1_700_000_000);
    row.metadata = Some(
        serde_json::to_string(&vec![
            (
                weaver_server_core::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY,
                "sealed-secret",
            ),
            ("*Unpack:Password", "hunter2"),
        ])
        .unwrap(),
    );
    h.insert_history_row(&row);
    let resp = h
        .execute(
            "{ jobDetailSnapshot(jobId: 7202) { historyItem { id hasPassword attributes { key value } } \
             jobEvents { kind message } downloadSummary { nodeCount } } }",
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["historyItem"]["id"].as_u64(),
        Some(7_202)
    );
    assert!(data["jobDetailSnapshot"]["downloadSummary"].is_null());
    let text = serde_json::to_string(&data).unwrap();
    assert!(
        !text.contains("hunter2") && !text.contains("sealed-secret"),
        "{text}"
    );

    let queued = h.submit_test_nzb("Queued").await;
    let resp = h
        .execute(&format!(
            "{{ jobDetailSnapshot(jobId: {queued}) {{ queueItem {{ id }} downloadSummary {{ nodeCount }} }} }}"
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["id"].as_u64(),
        Some(queued)
    );
    assert!(data["jobDetailSnapshot"]["downloadSummary"].is_null());
}

// A finished job's stored summary reads back through the job page: its
// layout, its nodes and where the password came from, and never the
// password itself.
#[tokio::test]
async fn download_summary_reads_back_with_the_password_origin_and_never_the_password() {
    use weaver_server_core::jobs::download_summary::{
        DOWNLOAD_SUMMARY_VERSION, DownloadSummary, Layout, NodeMode, SummaryNode, SummaryPost,
    };

    let h = TestHarness::new().await;
    let mut row = sample_history_row(7_203, "Locked", "complete", 1_700_000_000);
    row.metadata = Some(
        serde_json::to_string(&vec![(
            weaver_server_core::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY,
            "hunter2",
        )])
        .unwrap(),
    );
    h.insert_history_row(&row);
    let node = |id: u32, parent: Option<u32>, depth: u32, source: Option<&str>| SummaryNode {
        id,
        parent,
        kind: "rar_store".into(),
        name: Some(format!("volume{id}.rar")),
        volumes: 2,
        depth,
        mode: NodeMode::Streamed,
        given_up: false,
        reason: None,
        encrypted: source.is_some(),
        encrypted_headers: false,
        password_needed: source.is_some(),
        password_source: source.map(str::to_string),
    };
    let summary = DownloadSummary {
        version: DOWNLOAD_SUMMARY_VERSION,
        layout: Some(Layout::Archive),
        nodes: vec![
            node(1, None, 1, Some("settings")),
            node(2, Some(1), 2, None),
        ],
        node_count: 2,
        max_depth: 2,
        recovery: Vec::new(),
        recovery_count: 0,
        post: SummaryPost {
            files: 4,
            articles: 40,
            ..SummaryPost::default()
        },
    };
    weaver_server_core::pipeline::test_support::put_history_download_summary(
        &h.db,
        weaver_server_core::JobId(7_203),
        &summary,
    );

    let resp = h
        .execute(
            "{ jobDetailSnapshot(jobId: 7203) { historyItem { id hasPassword } \
             downloadSummary { layout nodeCount maxDepth \
             nodes { id parentId kind name volumes depth extracted givenUp reason \
             encrypted encryptedHeaders passwordNeeded passwordOrigin } \
             recovery { nodeId } recoveryCount \
             post { files articles missingArticles failedArticles groups } } } }",
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let got = &data["jobDetailSnapshot"]["downloadSummary"];
    assert!(!got.is_null(), "{data}");
    assert_eq!(got["layout"], "ARCHIVE");
    assert_eq!(got["nodeCount"], 2);
    assert_eq!(got["maxDepth"], 2);
    assert_eq!(
        got["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| (
                node["id"].as_u64().unwrap(),
                node["parentId"].as_u64(),
                node["depth"].as_u64().unwrap(),
                node["extracted"].as_bool().unwrap(),
                node["passwordNeeded"].as_bool().unwrap(),
                node["passwordOrigin"].as_str(),
            ))
            .collect::<Vec<_>>(),
        [
            (1, None, 1, false, true, Some("SETTINGS")),
            (2, Some(1), 2, false, false, None),
        ]
    );
    assert_eq!(got["post"]["files"], 4);
    assert_eq!(got["post"]["articles"], 40);
    assert_eq!(
        data["jobDetailSnapshot"]["historyItem"]["hasPassword"],
        true
    );
    let text = serde_json::to_string(&data).unwrap();
    assert!(!text.contains("hunter2"), "{text}");
}
