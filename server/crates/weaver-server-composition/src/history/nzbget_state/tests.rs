// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use crate::history::queries::HistoryQueries;
use crate::jobs::{FieldUpdate, JobUpdate};
use crate::operations::async_ops::OperationsAsyncOpsQueries;
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;

fn database_with_history() -> Database {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&crate::JobHistoryRow {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: 41,
        job_hash: None,
        name: "history fixture".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 100,
        downloaded_bytes: 100,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: Some(
            serde_json::to_string(&[
                ("priority", "NORMAL"),
                ("obsolete", "remove me"),
                ("untouched", "original"),
            ])
            .unwrap(),
        ),
        server_attribution: None,
        archived_at: None,
    })
    .unwrap();
    db
}

// A test's own statement, written through the writer gate.
fn execute_sql(db: &Database, sql: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, "nzbget_state_test_sql", |tx| {
            Box::pin(async move {
                tx.execute(sql, &[]).await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap()
}

fn hidden(db: &Database, job_id: u64) -> bool {
    db.nzbget_history_states(&[job_id]).unwrap()[&job_id].hidden
}

// An archive that cannot be accepted hides nothing, so the client still
// lists the entry and its next remove tries again.
#[test]
fn a_history_delete_whose_archive_fails_hides_nothing() {
    let db = database_with_history();
    execute_sql(
        &db,
        "CREATE TRIGGER reject_archive BEFORE INSERT ON async_operations \
         BEGIN SELECT RAISE(ABORT, 'injected archive failure'); END",
    );
    let error = db.archive_nzbget_history(&[41]).unwrap_err();
    assert!(error.to_string().contains("injected archive failure"));
    assert!(!hidden(&db, 41));

    execute_sql(&db, "DROP TRIGGER reject_archive");
    assert_eq!(db.archive_nzbget_history(&[41]).unwrap(), 1);
    assert!(hidden(&db, 41));
}

// An entry another operation holds is not archived, so it stays listed:
// the answer counts only the entries the archive took.
#[test]
fn a_history_delete_of_a_busy_entry_leaves_it_listed() {
    let db = database_with_history();
    let mut other = db.get_job_history(41).unwrap().unwrap();
    other.job_id = 42;
    db.insert_job_history(&other).unwrap();
    // A first archive holds 41 until the worker runs it.
    assert!(
        db.accept_archive_history(&[41])
            .unwrap()
            .operation_id
            .is_some()
    );

    assert_eq!(db.archive_nzbget_history(&[41]).unwrap(), 0);
    assert!(!hidden(&db, 41));
    assert_eq!(db.archive_nzbget_history(&[41, 42, 99]).unwrap(), 1);
    assert!(!hidden(&db, 41));
    assert!(hidden(&db, 42));
}

// One request's ids past SQLite's 32,766 binds hide every row named, in
// one call, and count only the rows that exist.
#[test]
fn hiding_many_rows_is_bind_safe() {
    let db = database_with_history();
    let mut ids: Vec<u64> = (1_000..34_000).collect();
    ids.push(41);
    assert_eq!(db.set_nzbget_history_rows_hidden(&ids, true).unwrap(), 1);
    assert!(db.nzbget_history_states(&[41]).unwrap()[&41].hidden);
    assert_eq!(db.set_nzbget_history_rows_hidden(&[], true).unwrap(), 0);
    assert_eq!(db.set_nzbget_history_rows_hidden(&[41], false).unwrap(), 1);
    assert!(!db.nzbget_history_states(&[41]).unwrap()[&41].hidden);
}

#[test]
fn history_metadata_edits_and_rename_preserve_intervening_updates() {
    let db = database_with_history();
    // Warm the row cache before either edit; both updates invalidate it.
    db.get_job_history(41).unwrap().unwrap();
    let update = JobUpdate {
        metadata_edits: vec![
            ("PRIORITY".into(), Some("HIGH".into())),
            ("OBSOLETE".into(), None),
        ],
        name: Some("renamed history".into()),
        ..Default::default()
    };
    db.update_job_history_fields(
        41,
        &JobUpdate {
            metadata_edits: vec![("later".into(), Some("other client".into()))],
            ..Default::default()
        },
    )
    .unwrap();
    db.update_job_history_fields(41, &update).unwrap();

    let row = db.get_job_history(41).unwrap().unwrap();
    assert_eq!(row.name, "renamed history");
    let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
    assert!(metadata.contains(&("untouched".into(), "original".into())));
    assert!(metadata.contains(&("later".into(), "other client".into())));
    assert!(metadata.contains(&("PRIORITY".into(), "HIGH".into())));
    assert_eq!(
        metadata
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case("priority"))
            .count(),
        1
    );
    assert!(
        !metadata
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("obsolete"))
    );
    assert_eq!(
        crate::ingest::renamed_title(&metadata),
        Some("renamed history")
    );
}

#[test]
fn history_rename_only_and_metadata_replacement_keep_their_semantics() {
    let db = database_with_history();
    db.update_job_history_fields(
        41,
        &JobUpdate {
            name: Some("rename only".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let row = db.get_job_history(41).unwrap().unwrap();
    let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
    assert!(metadata.contains(&("untouched".into(), "original".into())));
    assert_eq!(crate::ingest::renamed_title(&metadata), Some("rename only"));

    db.update_job_history_fields(
        41,
        &JobUpdate {
            metadata: FieldUpdate::Set(vec![("replacement".into(), "initial".into())]),
            metadata_edits: vec![("REPLACEMENT".into(), Some("final".into()))],
            name: Some("replacement title".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let row = db.get_job_history(41).unwrap().unwrap();
    let metadata = crate::history::parse_history_metadata(row.metadata.as_deref());
    assert_eq!(metadata.len(), 2);
    assert!(metadata.contains(&("REPLACEMENT".into(), "final".into())));
    assert_eq!(
        crate::ingest::renamed_title(&metadata),
        Some("replacement title")
    );

    db.update_job_history_fields(
        41,
        &JobUpdate {
            metadata: FieldUpdate::Clear,
            metadata_edits: vec![("after-clear".into(), Some("value".into()))],
            ..Default::default()
        },
    )
    .unwrap();
    let row = db.get_job_history(41).unwrap().unwrap();
    assert_eq!(
        crate::history::parse_history_metadata(row.metadata.as_deref()),
        vec![("after-clear".into(), "value".into())]
    );
    assert!(
        !db.update_job_history_fields(42, &JobUpdate::default())
            .unwrap()
    );
}
