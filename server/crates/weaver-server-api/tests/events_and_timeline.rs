// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod common;

use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_core::JobHistoryRow;

#[tokio::test]
async fn job_events_after_submit() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("events-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobEvents(jobId: {job_id}) {{ kind jobId message timestamp }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    // The mock scheduler does not persist events to the DB, so the list may
    // be empty. We simply verify the query succeeds and returns an array.
    assert!(data["jobEvents"].as_array().is_some());
}

#[tokio::test]
async fn job_events_have_required_fields() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("events-fields-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobEvents(jobId: {job_id}) {{ kind jobId message timestamp }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["jobEvents"].as_array().unwrap();
    for event in events {
        assert!(
            event["kind"].as_str().is_some(),
            "event should have a kind field"
        );
        assert!(
            event["jobId"].as_u64().is_some(),
            "event should have a jobId field"
        );
        assert!(
            event["message"].as_str().is_some(),
            "event should have a message field"
        );
        assert!(
            event["timestamp"].as_f64().is_some(),
            "event should have a timestamp field"
        );
    }
}

#[tokio::test]
async fn job_events_nonexistent_job() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"{ jobEvents(jobId: 999999) { kind jobId message timestamp } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let events = data["jobEvents"].as_array().unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn job_timeline_after_submit() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("timeline-test").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobTimeline(jobId: {job_id}) {{ startedAt outcome }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(
        !data["jobTimeline"].is_null(),
        "timeline should be non-null for an existing job"
    );
}

#[tokio::test]
async fn job_timeline_nonexistent() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(r#"{ jobTimeline(jobId: 999999) { startedAt outcome } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(
        data["jobTimeline"].is_null(),
        "timeline should be null for a nonexistent job"
    );
}

#[tokio::test]
async fn job_detail_snapshot_query_returns_live_job_data() {
    let h = TestHarness::new().await;
    let job_id = h.submit_test_nzb("job-detail-snapshot").await;

    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {job_id}) {{ queueItem {{ id state }} historyItem {{ id }} jobTimeline {{ outcome }} jobEvents {{ kind }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["id"].as_u64(),
        Some(job_id)
    );
    assert_eq!(
        data["jobDetailSnapshot"]["queueItem"]["state"].as_str(),
        Some("QUEUED")
    );
    assert!(data["jobDetailSnapshot"]["historyItem"].is_null());
    assert!(!data["jobDetailSnapshot"]["jobTimeline"].is_null());
    assert!(data["jobDetailSnapshot"]["jobEvents"].as_array().is_some());
}

#[tokio::test]
async fn job_detail_snapshot_names_the_servers_that_served_an_archived_job() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addServer(input: {
                    host: "news.example.invalid",
                    port: 119,
                    tls: false,
                    connections: 5,
                    active: false
                }) { id }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let server_id = response_data(&resp)["addServer"]["id"]
        .as_u64()
        .expect("the new server's id");

    // Credit the configured server plus one that no longer exists: a deleted
    // server keeps its counts and reports no host.
    let stale_server_id = server_id + 1_000;
    let job_id = 8_150_u64;
    h.insert_history_row(&JobHistoryRow {
        job_id,
        job_hash: None,
        name: "attributed-job".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 4_096,
        downloaded_bytes: 4_096,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_700_000_000,
        completed_at: 1_700_000_060,
        metadata: None,
        server_attribution: Some(format!(
            r#"[{{"s":{stale_server_id},"a":2,"b":2048}},{{"s":{server_id},"a":7,"b":7168}}]"#
        )),
        files_removed_at: None,
    });

    let resp = h
        .execute(&format!(
            r#"{{ jobDetailSnapshot(jobId: {job_id}) {{ serverAttribution {{ serverId serverHost articles wireBytes }} }} }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let contributions = response_data(&resp)["jobDetailSnapshot"]["serverAttribution"]
        .as_array()
        .expect("an attribution list")
        .clone();
    assert_eq!(contributions.len(), 2);
    // Highest article count first, whatever order storage happened to use.
    assert_eq!(contributions[0]["serverId"].as_u64(), Some(server_id));
    assert_eq!(
        contributions[0]["serverHost"].as_str(),
        Some("news.example.invalid")
    );
    assert_eq!(contributions[0]["articles"].as_u64(), Some(7));
    assert_eq!(contributions[0]["wireBytes"].as_u64(), Some(7_168));
    assert_eq!(contributions[1]["serverId"].as_u64(), Some(stale_server_id));
    assert!(contributions[1]["serverHost"].is_null());
}

// A script's job event keeps its own kind; it is never reported as the job's
// creation, and a kind the API does not name is left out rather than relabelled.
#[tokio::test]
async fn script_job_events_keep_their_own_kind() {
    use weaver_server_core::post_processing::directives::ScriptLogLevel;
    use weaver_server_core::post_processing::executor::SCRIPT_EVENT_KIND;

    let h = TestHarness::new().await;
    let job_id = 424_242;
    let stored = [
        (SCRIPT_EVENT_KIND, "POST_PROCESSING_SCRIPT"),
        (ScriptLogLevel::Error.event_kind(), "SCRIPT_ERROR"),
        (ScriptLogLevel::Warning.event_kind(), "SCRIPT_WARNING"),
        (ScriptLogLevel::Info.event_kind(), "SCRIPT_OUTPUT"),
        (ScriptLogLevel::Detail.event_kind(), "SCRIPT_DETAIL"),
        (ScriptLogLevel::Debug.event_kind(), "SCRIPT_DEBUG"),
    ];
    let db = h.db.clone();
    tokio::task::spawn_blocking(move || {
        for (index, (kind, _)) in stored.iter().enumerate() {
            db.insert_job_event(job_id, 1_000 + index as i64, kind, kind, None)
                .unwrap();
        }
        db.insert_job_event(job_id, 2_000, "NotAKindTheApiNames", "unnamed", None)
            .unwrap();
    })
    .await
    .unwrap();

    for query in [
        format!(r#"{{ jobEvents(jobId: {job_id}) {{ kind message }} }}"#),
        format!(r#"{{ jobDetailSnapshot(jobId: {job_id}) {{ jobEvents {{ kind message }} }} }}"#),
    ] {
        let resp = h.execute(&query).await;
        assert_no_errors(&resp);
        let data = response_data(&resp);
        let events = data["jobEvents"]
            .as_array()
            .or_else(|| data["jobDetailSnapshot"]["jobEvents"].as_array())
            .unwrap();
        let kinds: Vec<(&str, &str)> = events
            .iter()
            .map(|event| {
                (
                    event["message"].as_str().unwrap(),
                    event["kind"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(kinds, stored.to_vec(), "{query}");
    }
}

#[tokio::test]
async fn job_events_return_the_newest_window_and_page_back_from_it() {
    let h = TestHarness::new().await;
    let job_id = 4_242;
    let events: Vec<weaver_server_core::JobEvent> = (0..2_005)
        .map(|n| weaver_server_core::JobEvent {
            job_id,
            timestamp: 1_000 + n,
            kind: "JobCreated".to_string(),
            message: format!("event {n}"),
            file_id: None,
        })
        .collect();
    h.db.insert_job_events(&events).unwrap();

    let messages = |query: String| {
        let h = &h;
        async move {
            let resp = h.execute(&query).await;
            assert_no_errors(&resp);
            response_data(&resp)["jobEvents"]
                .as_array()
                .unwrap()
                .iter()
                .map(|event| event["message"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        }
    };
    let expected =
        |from: i64, to: i64| (from..to).map(|n| format!("event {n}")).collect::<Vec<_>>();

    // The newest 2000 when no size is named, as before the log was paged.
    let latest = messages(format!("{{ jobEvents(jobId: {job_id}) {{ message }} }}")).await;
    assert_eq!(latest, expected(5, 2_005));

    let page = messages(format!(
        "{{ jobEvents(jobId: {job_id}, limit: 3, offset: 2) {{ message }} }}"
    ))
    .await;
    assert_eq!(page, expected(2_000, 2_003));

    let oldest = messages(format!(
        "{{ jobEvents(jobId: {job_id}, limit: 10, offset: 2000) {{ message }} }}"
    ))
    .await;
    assert_eq!(oldest, expected(0, 5));

    let capped = messages(format!(
        "{{ jobEvents(jobId: {job_id}, limit: 5000) {{ message }} }}"
    ))
    .await;
    assert_eq!(capped, expected(5, 2_005));

    let resp = h
        .execute(&format!("{{ jobTimeline(jobId: {job_id}) {{ outcome }} }}"))
        .await;
    assert_no_errors(&resp);
}

fn stored_events(job_id: u64, range: std::ops::Range<i64>) -> Vec<weaver_server_core::JobEvent> {
    range
        .map(|n| weaver_server_core::JobEvent {
            job_id,
            timestamp: 1_000 + n,
            kind: "JobCreated".to_string(),
            message: format!("event {n}"),
            file_id: None,
        })
        .collect()
}

async fn event_page(h: &TestHarness, query: String) -> Vec<(String, String)> {
    let resp = h.execute(&query).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    data["jobEvents"]
        .as_array()
        .or_else(|| data["jobDetailSnapshot"]["jobEvents"].as_array())
        .unwrap()
        .iter()
        .map(|event| {
            (
                event["message"].as_str().unwrap().to_string(),
                event["cursor"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

// Paging back by cursor gives each event once, oldest first within a page,
// however many events arrive between pages.
#[tokio::test]
async fn job_events_page_back_by_cursor_while_events_arrive() {
    let h = TestHarness::new().await;
    let job_id = 4_343;
    h.db.insert_job_events(&stored_events(job_id, 0..250))
        .unwrap();

    let newest = event_page(
        &h,
        format!("{{ jobEvents(jobId: {job_id}, limit: 100) {{ message cursor }} }}"),
    )
    .await;
    assert_eq!(newest.len(), 100);
    let mut seen: Vec<String> = newest.iter().map(|(message, _)| message.clone()).collect();
    let mut before = newest[0].1.clone();

    // Events arrive between pages; the pages behind the cursor do not move.
    h.db.insert_job_events(&stored_events(job_id, 250..400))
        .unwrap();
    loop {
        let page = event_page(
            &h,
            format!(
                "{{ jobEvents(jobId: {job_id}, before: \"{before}\", limit: 100) {{ message cursor }} }}"
            ),
        )
        .await;
        let Some((_, oldest)) = page.first() else {
            break;
        };
        before = oldest.clone();
        let mut older: Vec<String> = page.into_iter().map(|(message, _)| message).collect();
        older.extend(seen);
        seen = older;
    }
    let expected: Vec<String> = (0..250).map(|n| format!("event {n}")).collect();
    assert_eq!(seen, expected);

    // A page is at most 2000.
    h.db.insert_job_events(&stored_events(job_id, 400..2_500))
        .unwrap();
    let largest = event_page(
        &h,
        format!("{{ jobEvents(jobId: {job_id}, limit: 5000) {{ message cursor }} }}"),
    )
    .await;
    assert_eq!(largest.len(), 2_000);
}

// The detail snapshot carries the newest 100 events, with the count of all.
#[tokio::test]
async fn job_detail_snapshot_carries_the_newest_100_events_and_the_count() {
    let h = TestHarness::new().await;
    let job_id = 4_444;
    let mut row = JobHistoryRow {
        job_id,
        job_hash: None,
        name: "Windowed".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_000,
        completed_at: 2_000,
        metadata: None,
        server_attribution: None,
        files_removed_at: None,
    };
    h.insert_history_row(&row);
    h.db.insert_job_events(&stored_events(job_id, 0..101))
        .unwrap();

    let query = format!(
        "{{ jobDetailSnapshot(jobId: {job_id}) {{ jobEventCount jobEvents {{ message cursor }} }} }}"
    );
    let events = event_page(&h, query.clone()).await;
    let messages: Vec<String> = events.into_iter().map(|(message, _)| message).collect();
    let expected: Vec<String> = (1..101).map(|n| format!("event {n}")).collect();
    assert_eq!(messages, expected);
    let resp = h.execute(&query).await;
    assert_eq!(
        response_data(&resp)["jobDetailSnapshot"]["jobEventCount"].as_u64(),
        Some(101)
    );

    // Exactly 100 is the whole log.
    row.job_id = 4_445;
    h.insert_history_row(&row);
    h.db.insert_job_events(&stored_events(4_445, 0..100))
        .unwrap();
    let resp = h
        .execute("{ jobDetailSnapshot(jobId: 4445) { jobEventCount jobEvents { message } } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(
        data["jobDetailSnapshot"]["jobEventCount"].as_u64(),
        Some(100)
    );
    assert_eq!(
        data["jobDetailSnapshot"]["jobEvents"]
            .as_array()
            .unwrap()
            .len(),
        100
    );
}
