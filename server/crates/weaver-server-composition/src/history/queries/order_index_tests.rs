// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::model::{HistoryFilter, HistoryOrder, HistoryOrderKey};
use crate::history::nzbget_state::HistoryNzbgetStateQueries;
use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use crate::history::record::JobHistoryRow;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlEngine, SqlRuntime};
use weaver_server_history::history::queries::{
    append_history_row_filter_predicates, archived_history_files_sql, history_completed_by_sql,
    history_order_sql, history_status_ids_sql,
};

// The retention read is a range of a completed-at index, with and
// without its cursor, and sorts nothing.
#[test]
fn the_retention_read_is_served_by_the_completed_index() {
    let db = Database::open_in_memory().unwrap();
    for after in [None, Some((1_700_000_000, 9))] {
        let (sql, args) = history_completed_by_sql(1_800_000_000, after, 1000);
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        let datastore = db.datastore();
        let detail = db
            .run_sql_blocking_read(async move {
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                rows.iter()
                    .map(|row| row.text("detail"))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .join("; ");
        println!("retention read plan, cursor {after:?}: {detail}");
        // Either completed-at index holds `(completed_at, job_id)` in
        // order: `job_id` is the rowid, which every SQLite index carries.
        assert!(
            detail.contains("USING COVERING INDEX idx_job_history_completed_at")
                || detail.contains("USING COVERING INDEX idx_job_history_completed_job"),
            "{detail}"
        );
        assert!(!detail.contains("TEMP B-TREE"), "{detail}");
    }
}

// The scheduled retention read of one status is a range of the status
// index, with and without its cursor, and sorts nothing.
#[test]
fn the_status_retention_read_is_served_by_the_status_index() {
    let db = Database::open_in_memory().unwrap();
    for status in ["failed", "complete"] {
        for after in [None, Some((1_700_000_000, 9))] {
            let (sql, args) = history_status_ids_sql(status, after, 1000);
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            let datastore = db.datastore();
            let detail = db
                .run_sql_blocking_read(async move {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    rows.iter()
                        .map(|row| row.text("detail"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .join("; ");
            println!("status retention plan, {status}, cursor {after:?}: {detail}");
            assert!(
                detail.contains("USING COVERING INDEX idx_job_history_status_completed_job"),
                "{detail}"
            );
            assert!(!detail.contains("TEMP B-TREE"), "{detail}");
        }
    }
}

// The archived-files page is a range of the table's primary key, read in
// its order, so it sorts nothing.
#[test]
fn the_archived_files_page_is_served_by_the_primary_key() {
    let db = Database::open_in_memory().unwrap();
    for from in [0, 4_000] {
        let (sql, args) = archived_history_files_sql(7, from, 100);
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        let datastore = db.datastore();
        let detail = db
            .run_sql_blocking_read(async move {
                let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                rows.iter()
                    .map(|row| row.text("detail"))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .join("; ");
        println!("archived files page plan, from {from}: {detail}");
        assert!(
            detail.contains(
                "SEARCH job_history_archived_files USING PRIMARY KEY (job_id=? AND ordinal>?)"
            ),
            "{detail}"
        );
        assert!(!detail.contains("TEMP B-TREE"), "{detail}");
    }
}

// The id-only status read returns the ids, in the order, that decoding
// whole rows did.
#[test]
fn the_status_id_read_matches_the_full_row_read() {
    let db = Database::open_in_memory().unwrap();
    for (job_id, status, completed_at) in [
        (1, "complete", 50),
        (2, "failed", 50),
        (3, "partial", 10),
        (4, "cancelled", 70),
        (5, "complete", 90),
        (6, "failed", 5),
    ] {
        db.insert_job_history(&JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("status-{job_id}"),
            status: status.into(),
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
            created_at: 1,
            completed_at,
            metadata: None,
            server_attribution: None,
            archived_at: None,
            protocol: crate::jobs::Protocol::Nzb,
        })
        .unwrap();
    }
    db.set_nzbget_history_hidden(2, true).unwrap();
    for statuses in [
        vec!["failed", "partial"],
        vec!["complete"],
        vec!["failed", "partial", "complete", "cancelled"],
    ] {
        let full = db
            .list_job_history(&HistoryFilter {
                statuses: Some(statuses.iter().map(|status| status.to_string()).collect()),
                ..Default::default()
            })
            .unwrap()
            .into_iter()
            .map(|row| row.job_id)
            .collect::<Vec<_>>();
        assert!(!full.is_empty());
        assert_eq!(db.list_job_history_ids_by_status(&statuses).unwrap(), full);
    }
}

// Every History page order is read through its index: SQLite walks the
// index in order instead of sorting the table in a temporary b-tree.
#[test]
fn every_history_order_is_served_by_an_index() {
    let db = Database::open_in_memory().unwrap();
    let keys = [
        HistoryOrderKey::CompletedAt,
        HistoryOrderKey::TotalBytes,
        HistoryOrderKey::Health,
        HistoryOrderKey::Name,
        HistoryOrderKey::State,
        HistoryOrderKey::Category,
    ];
    // A search is a substring of the name, which no index seeks, so a
    // searched page walks its order's index and stops at the page. The
    // same holds for Weaver's listing, which reads every row, and for the
    // NZBGet facade's, which leaves out the rows it hid.
    for ((key, search), nzbget_shown_only) in keys
        .into_iter()
        .flat_map(|key| [(key, None), (key, Some("release".to_string()))])
        .flat_map(|pair| [(pair.clone(), false), (pair, true)])
    {
        for descending in [true, false] {
            let order = HistoryOrder { key, descending };
            let filter = HistoryFilter {
                name_contains: search.clone(),
                nzbget_shown_only,
                ..Default::default()
            };
            let mut sql =
                String::from("EXPLAIN QUERY PLAN SELECT h.job_id FROM job_history h WHERE 1=1");
            let mut args = Vec::new();
            append_history_row_filter_predicates(&mut sql, &mut args, &filter, SqlEngine::Sqlite);
            sql.push_str(&history_order_sql(order, SqlEngine::Sqlite));
            sql.push_str(" LIMIT 25");
            let datastore = db.datastore();
            let detail = db
                .run_sql_blocking_read(async move {
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
                    rows.iter()
                        .map(|row| row.text("detail"))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .join("; ");
            assert!(
                detail.contains("USING INDEX") || detail.contains("USING COVERING INDEX"),
                "{order:?} {search:?} {nzbget_shown_only}: {detail}"
            );
            assert!(
                !detail.contains("TEMP B-TREE"),
                "{order:?} {search:?} {nzbget_shown_only}: {detail}"
            );
        }
    }
}

// The order the database returns is the order `sort_history_rows`
// gives, so pages merged from several statements stay in order.
#[test]
fn sort_history_rows_matches_the_database_order() {
    let db = Database::open_in_memory().unwrap();
    let names = [
        "b.Release",
        "B.release",
        "a.Release",
        "Ä.release",
        "é",
        "Z",
        "z",
        "",
    ];
    let statuses = [
        "complete", "Paused", "failed", "COMPLETE", "partial", "weird",
    ];
    let categories = [
        None,
        Some(""),
        Some("TV"),
        Some("tv"),
        Some("Ébc"),
        Some("anime"),
    ];
    for job_id in 1..=48u64 {
        db.insert_job_history(&JobHistoryRow {
            job_id,
            job_hash: None,
            name: names[(job_id % 8) as usize].to_string(),
            status: statuses[(job_id % 6) as usize].to_string(),
            error_message: None,
            total_bytes: job_id % 5,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: (job_id % 3) as u32,
            category: categories[(job_id % 6) as usize].map(str::to_string),
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: (job_id % 7) as i64,
            // The name sort is by the full release name, which the
            // original or renamed title in the metadata can replace.
            metadata: match job_id % 5 {
                1 => Some(r#"[["weaver.original_title","A.Full.Release.1080p"]]"#.into()),
                2 => Some(
                    r#"[["weaver.original_title","Y.Full"],["weaver.renamed_title","m.Renamed"]]"#
                        .into(),
                ),
                _ => None,
            },
            server_attribution: None,
            archived_at: None,
            protocol: crate::jobs::Protocol::Nzb,
        })
        .unwrap();
    }
    let keys = [
        HistoryOrderKey::CompletedAt,
        HistoryOrderKey::TotalBytes,
        HistoryOrderKey::Health,
        HistoryOrderKey::Name,
        HistoryOrderKey::State,
        HistoryOrderKey::Category,
    ];
    for key in keys {
        for descending in [true, false] {
            let order = HistoryOrder { key, descending };
            let filter = HistoryFilter {
                order,
                ..Default::default()
            };
            let read = db.list_job_history(&filter).unwrap();
            let mut sorted = read.clone();
            sort_history_rows(order, &mut sorted);
            let ids =
                |rows: &[JobHistoryRow]| rows.iter().map(|row| row.job_id).collect::<Vec<_>>();
            assert_eq!(ids(&read), ids(&sorted), "{order:?}");
        }
    }
}
