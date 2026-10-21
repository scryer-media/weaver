// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use super::*;
use crate::jobs::ids::JobId;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

fn sample_history() -> JobHistoryRow {
    JobHistoryRow {
        job_id: 1,
        job_hash: None,
        name: "test.nzb".to_string(),
        status: "complete".to_string(),
        error_message: None,
        total_bytes: 1_000_000,
        downloaded_bytes: 1_000_000,
        optional_recovery_bytes: 200_000,
        optional_recovery_downloaded_bytes: 50_000,
        failed_bytes: 0,
        health: 1000,
        category: Some("movies".to_string()),
        output_dir: Some("/tmp/output".to_string()),
        nzb_path: Some("/tmp/test.nzb".to_string()),
        created_at: 1700000000,
        completed_at: 1700001000,
        metadata: None,
        server_attribution: None,
    }
}

fn metadata_json(pairs: &[(&str, &str)]) -> String {
    serde_json::to_string(
        &pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn insert_and_list() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();

    let entries = db.list_job_history(&HistoryFilter::default()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "test.nzb");
    assert_eq!(entries[0].total_bytes, 1_000_000);
    assert_eq!(entries[0].optional_recovery_bytes, 200_000);
    assert_eq!(entries[0].optional_recovery_downloaded_bytes, 50_000);
    assert_eq!(db.get_job_history(1).unwrap().unwrap().name, "test.nzb");
}

#[test]
fn upsert_preserves_persisted_nzb_bytes_when_update_omits_them() {
    let db = Database::open_in_memory().unwrap();
    let mut history = sample_history();
    history.nzb_path = Some("/tmp/original.nzb".to_string());
    db.insert_job_history(&history).unwrap();

    let expected_nzb = crate::ingest::compress_nzb_bytes(b"<nzb></nzb>").unwrap();
    let datastore = db.datastore();
    let expected_for_update = expected_nzb.clone();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE job_history SET nzb_zstd = {} WHERE job_id = {}",
            &[SqlArg::Bytes(expected_for_update), SqlArg::I64(1)],
        )
        .await?;
        Ok(())
    })
    .unwrap();

    let mut update = sample_history();
    update.name = "updated.nzb".to_string();
    update.status = "failed".to_string();
    update.error_message = Some("later metadata rewrite".to_string());
    update.nzb_path = None;
    db.insert_job_history(&update).unwrap();

    let (path, nzb_zstd) = db.load_history_job_persisted_nzb(1).unwrap().unwrap();
    assert_eq!(path, PathBuf::from("job-1.nzb"));
    assert_eq!(nzb_zstd.as_deref(), Some(expected_nzb.as_slice()));
    assert!(db.get_job_history(1).unwrap().unwrap().nzb_path.is_none());
}

#[test]
fn filter_by_status() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();

    let mut failed = sample_history();
    failed.job_id = 2;
    failed.status = "failed".to_string();
    failed.error_message = Some("missing volumes".to_string());
    db.insert_job_history(&failed).unwrap();

    let filter = HistoryFilter {
        statuses: Some(vec!["complete".to_string()]),
        ..Default::default()
    };
    let entries = db.list_job_history(&filter).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].job_id, 1);
}

#[test]
fn delete_history() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&sample_history()).unwrap();
    assert!(db.delete_job_history(1).unwrap());
    assert!(!db.delete_job_history(1).unwrap());
}

fn child_row_count(db: &Database, table: &'static str, job_id: u64) -> i64 {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        let rows = SqlRuntime::fetch_all(
            datastore.read_exec(),
            &format!("SELECT COUNT(*) AS n FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(job_id as i64)],
        )
        .await?;
        rows[0].i64("n")
    })
    .unwrap()
}

#[test]
fn batched_bundle_delete_clears_every_child_table_and_leaves_other_jobs() {
    const CHILD_TABLES: [&str; 5] = [
        "job_events",
        "job_history_attributes",
        "script_outputs",
        "script_event_queue",
        "script_job_state",
    ];
    let db = Database::open_in_memory().unwrap();
    for job_id in [1_u64, 2, 3] {
        let mut row = sample_history();
        row.job_id = job_id;
        db.insert_job_history(&row).unwrap();
        db.insert_job_event(job_id, 1, "note", "event", None)
            .unwrap();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            let id = job_id as i64;
            for (template, args) in [
                (
                    "INSERT INTO job_history_attributes (job_id, key, value, completed_at)
                     VALUES ({}, 'k', 'v', 2)",
                    vec![SqlArg::I64(id)],
                ),
                (
                    "INSERT INTO script_outputs (id, job_id, event, script, seq, raw_bytes,
                        truncated, output, stored_bytes, result_json, status, created_at)
                     VALUES ({}, {}, 'complete', 's', {}, 0, false, {}, 0, '[]', 'ok', 1)",
                    vec![
                        SqlArg::Text(format!("out-{id}")),
                        SqlArg::I64(id),
                        SqlArg::I64(id),
                        SqlArg::Bytes(Vec::new()),
                    ],
                ),
                (
                    "INSERT INTO script_event_queue (run_id, job_id, event, priority, seq,
                        state, payload, created_at)
                     VALUES ({}, {}, 'complete', 0, {}, 'done', '[]', 1)",
                    vec![
                        SqlArg::Text(format!("run-{id}")),
                        SqlArg::I64(id),
                        SqlArg::I64(id),
                    ],
                ),
                (
                    "INSERT INTO script_job_state (job_id, state) VALUES ({}, '[]')",
                    vec![SqlArg::I64(id)],
                ),
            ] {
                SqlRuntime::execute(datastore.read_exec(), template, &args).await?;
            }
            Ok(())
        })
        .unwrap();
    }
    for table in CHILD_TABLES {
        assert_eq!(child_row_count(&db, table, 2), 1, "{table} seeded");
    }

    // Job 9 never existed: the batch skips it.
    assert_eq!(
        db.delete_job_histories_and_forget_duplicate_identities(&[JobId(1), JobId(3), JobId(9)])
            .unwrap(),
        vec![JobId(1), JobId(3)]
    );

    for job_id in [1, 3] {
        assert!(db.get_job_history(job_id).unwrap().is_none());
        for table in CHILD_TABLES {
            assert_eq!(
                child_row_count(&db, table, job_id),
                0,
                "{table} for {job_id}"
            );
        }
    }
    assert!(db.get_job_history(2).unwrap().is_some());
    for table in CHILD_TABLES {
        assert_eq!(child_row_count(&db, table, 2), 1, "{table} kept for job 2");
    }
}

#[test]
fn max_job_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.max_job_id().unwrap(), 0);
}

#[test]
fn max_job_id_with_entries() {
    let db = Database::open_in_memory().unwrap();
    let mut entry = sample_history();
    entry.job_id = 42;
    db.insert_job_history(&entry).unwrap();
    assert_eq!(db.max_job_id().unwrap(), 42);
}

#[test]
fn list_with_limit_and_offset() {
    let db = Database::open_in_memory().unwrap();
    for i in 1..=5 {
        let mut entry = sample_history();
        entry.job_id = i;
        entry.completed_at = 1700000000 + i as i64;
        db.insert_job_history(&entry).unwrap();
    }

    let filter = HistoryFilter {
        limit: Some(2),
        offset: Some(1),
        ..Default::default()
    };
    let entries = db.list_job_history(&filter).unwrap();
    assert_eq!(entries.len(), 2);
    // Ordered by completed_at DESC, so job_ids: 5, 4, 3, 2, 1
    // offset=1, limit=2 → jobs 4, 3
    assert_eq!(entries[0].job_id, 4);
    assert_eq!(entries[1].job_id, 3);
}

#[test]
fn list_with_large_limit_is_stably_ordered_by_completion_and_id() {
    let db = Database::open_in_memory().unwrap();
    for i in 1..=1_000 {
        let mut entry = sample_history();
        entry.job_id = i;
        entry.completed_at = 1_700_000_000 + (i % 10) as i64;
        db.insert_job_history(&entry).unwrap();
    }

    let entries = db
        .list_job_history(&HistoryFilter {
            limit: Some(100),
            ..Default::default()
        })
        .unwrap();

    assert_eq!(entries.len(), 100);
    for pair in entries.windows(2) {
        assert!(
            pair[0].completed_at > pair[1].completed_at
                || (pair[0].completed_at == pair[1].completed_at
                    && pair[0].job_id > pair[1].job_id),
            "history rows should be ordered by completed_at desc, job_id desc"
        );
    }
}

#[test]
fn list_filters_by_multiple_statuses_category_ids_and_metadata() {
    let db = Database::open_in_memory().unwrap();
    for i in 1..=5 {
        let mut entry = sample_history();
        entry.job_id = i;
        entry.status = match i {
            2 => "failed".to_string(),
            3 => "cancelled".to_string(),
            _ => "complete".to_string(),
        };
        entry.category = Some(if i % 2 == 0 { "tv" } else { "movies" }.to_string());
        entry.metadata = Some(
            serde_json::to_string(&vec![
                ("scryer_title_id".to_string(), format!("title-{i}")),
                ("source".to_string(), "scryer".to_string()),
            ])
            .unwrap(),
        );
        db.insert_job_history(&entry).unwrap();
    }

    let entries = db
        .list_job_history(&HistoryFilter {
            statuses: Some(vec!["failed".to_string(), "cancelled".to_string()]),
            item_ids: Some(vec![2, 3, 4]),
            category: Some("tv".to_string()),
            metadata_has_key: Some("source".to_string()),
            metadata_equals: Some(HistoryMetadataEquals {
                key: "scryer_title_id".to_string(),
                value: "title-2".to_string(),
            }),
            ..Default::default()
        })
        .unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].job_id, 2);
    assert_eq!(
        db.count_job_history(&HistoryFilter {
            statuses: Some(vec!["failed".to_string(), "cancelled".to_string()]),
            metadata_equals: Some(HistoryMetadataEquals {
                key: "source".to_string(),
                value: "scryer".to_string(),
            }),
            ..Default::default()
        })
        .unwrap(),
        2
    );
}

#[test]
fn history_attribute_filters_are_exact_and_indexed() {
    let db = Database::open_in_memory().unwrap();

    let mut false_positive = sample_history();
    false_positive.job_id = 1;
    false_positive.completed_at = 100;
    false_positive.metadata = Some(metadata_json(&[(
        "source",
        r#"["*scryer_title_id","title-a"]"#,
    )]));
    db.insert_job_history(&false_positive).unwrap();

    let mut match_a = sample_history();
    match_a.job_id = 2;
    match_a.completed_at = 90;
    match_a.metadata = Some(metadata_json(&[("*scryer_title_id", "title-a")]));
    db.insert_job_history(&match_a).unwrap();

    let filter = HistoryFilter {
        metadata_equals: Some(HistoryMetadataEquals {
            key: "*scryer_title_id".to_string(),
            value: "title-a".to_string(),
        }),
        limit: Some(1),
        ..Default::default()
    };
    let entries = db.list_job_history(&filter).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].job_id, 2);
    assert_eq!(db.count_job_history(&filter).unwrap(), 1);
}

#[test]
fn history_attribute_filters_exclude_hidden_metadata_keys() {
    let db = Database::open_in_memory().unwrap();
    let mut entry = sample_history();
    entry.metadata = Some(metadata_json(&[
        (CLIENT_REQUEST_ID_ATTRIBUTE_KEY, "req-1"),
        ("public", "yes"),
    ]));
    db.insert_job_history(&entry).unwrap();

    assert!(
        db.list_job_history(&HistoryFilter {
            metadata_has_key: Some(CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string()),
            ..Default::default()
        })
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        db.count_job_history(&HistoryFilter {
            metadata_equals: Some(HistoryMetadataEquals {
                key: CLIENT_REQUEST_ID_ATTRIBUTE_KEY.to_string(),
                value: "req-1".to_string(),
            }),
            ..Default::default()
        })
        .unwrap(),
        0
    );

    assert_eq!(
        db.count_job_history(&HistoryFilter {
            metadata_has_key: Some("public".to_string()),
            ..Default::default()
        })
        .unwrap(),
        1
    );
}
