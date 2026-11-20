// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::jobs::persistence::JobsPersistenceQueries;
use crate::jobs::repository::JobsRepositoryQueries;
use std::path::PathBuf;

use super::reference::*;
use super::*;
use crate::{ActiveJob, JobId};

fn active_job(id: u64, metadata: Vec<(String, String)>) -> ActiveJob {
    ActiveJob {
        job_id: JobId(id),
        nzb_hash: [0xBC; 32],
        nzb_path: PathBuf::from(format!("/srv/nzbs/Job.{id}.nzb")),
        nzb_zstd: Vec::new(),
        output_dir: PathBuf::from(format!("/srv/complete/Job.{id}")),
        created_at: 1,
        category: None,
        metadata,
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
    }
}

// Every page, as the package reads them: active pages to the end, then
// the recent history in chunks.
fn paged(db: &Database) -> BTreeMap<u64, JobNameRecord> {
    let mut jobs = BTreeMap::new();
    let mut after = 0;
    loop {
        let page = db
            .diagnostic_job_names_page(DiagnosticJobs::ActiveAfter(after))
            .unwrap();
        jobs.extend(page.jobs);
        match page.last_active {
            Some(last) => after = last,
            None => break,
        }
    }
    let mut recent = db.diagnostic_recent_history_ids().unwrap();
    recent.sort_unstable();
    for chunk in recent.chunks(DIAGNOSTIC_NAME_PAGE) {
        jobs.extend(
            db.diagnostic_job_names_page(DiagnosticJobs::History(chunk))
                .unwrap()
                .jobs,
        );
    }
    jobs
}

// The paged read returns the names the whole read returns, over active
// jobs that span several pages and a history longer than the cap, their
// ids interleaved, and no name statement reads a blob column.
#[test]
fn the_paged_read_returns_the_names_of_the_whole_read() {
    let db = Database::open_in_memory().unwrap();
    db.diagnostic_seed_sql(diagnostic_seed_script(1_234, 2_100, 3))
        .unwrap();
    let whole = db.diagnostic_job_names_whole().unwrap();
    assert_eq!(whole.len(), 1_234 + DIAGNOSTIC_RECENT_HISTORY);
    assert_eq!(paged(&db), whole);

    let mut statements = Vec::new();
    let active = db
        .diagnostic_job_names_page(DiagnosticJobs::ActiveAfter(0))
        .unwrap();
    statements.extend(active.statements);
    let recent = db.diagnostic_recent_history_ids().unwrap();
    let history = db
        .diagnostic_job_names_page(DiagnosticJobs::History(&recent[..10]))
        .unwrap();
    statements.extend(history.statements);
    for sql in &statements {
        for blob in ["nzb_zstd", "outline", "source_nzb_zstd"] {
            assert!(!sql.contains(blob), "{sql}");
        }
    }
}

// Every statement the read issues is served by a primary key or an
// index: no step of its plan scans a table.
#[test]
fn every_read_statement_is_served_by_a_key() {
    let db = Database::open_in_memory().unwrap();
    db.diagnostic_seed_sql(diagnostic_seed_script(20, 20, 2))
        .unwrap();
    let recent = db.diagnostic_recent_history_ids().unwrap();
    let mut statements = db
        .diagnostic_job_names_page(DiagnosticJobs::ActiveAfter(0))
        .unwrap()
        .statements;
    statements.extend(
        db.diagnostic_job_names_page(DiagnosticJobs::History(&recent))
            .unwrap()
            .statements,
    );
    let list = ["{}"; 3].join(", ");
    statements.push(format!(
        "SELECT job_id FROM job_history
          ORDER BY completed_at DESC, job_id DESC LIMIT {DIAGNOSTIC_RECENT_HISTORY}"
    ));
    statements.push(format!(
        "SELECT job_id, length(nzb_zstd) AS bytes FROM job_nzbs WHERE job_id IN ({list}) ORDER BY job_id"
    ));
    statements.push(format!(
        "SELECT job_id, nzb_zstd FROM job_nzbs WHERE job_id IN ({list}) ORDER BY job_id"
    ));
    for sql in statements {
        let binds = sql.matches("{}").count();
        let plan = db
            .diagnostic_query_plan(&sql, (1..=binds as i64).collect())
            .unwrap();
        eprintln!(
            "{}\n  -> {}",
            sql.split_whitespace().collect::<Vec<_>>().join(" "),
            plan.join(" | ")
        );
        // A step either seeks by a key, or walks an index in the order
        // the statement asks for (the recent history, which stops at its
        // LIMIT). No table scan, and no sort of the rows read.
        for step in &plan {
            assert!(
                step.starts_with("SEARCH ")
                    || (step.starts_with("SCAN ")
                        && step.contains(" USING ")
                        && step.contains("INDEX")),
                "{sql}: {plan:?}"
            );
            assert!(!step.contains("TEMP B-TREE"), "{sql}: {plan:?}");
        }
    }
}

fn execute_sql(db: &Database, sql: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.write_exec(), sql, &[]).await?;
        Ok(())
    })
    .unwrap();
}

// Every table that holds a file, member or set name, and the history
// attributes and duplicate keys, reach the job's record.
#[test]
fn every_name_table_reaches_its_job() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(7, Vec::new())).unwrap();
    db.create_active_job(&active_job(8, Vec::new())).unwrap();
    execute_sql(
        &db,
        "INSERT INTO active_file_identities (
             job_id, file_index, source_filename, current_filename,
             canonical_filename, classification_set_name, classification_source
         ) VALUES (7, 0, 'Source.Name.bin', 'Current.Name.bin', 'Canonical.Name.mkv',
                   'Identity.Set', 'declared');
         INSERT INTO active_detected_archives VALUES (7, 0, 'rar', 'Detected.Set', 0);
         INSERT INTO active_extracted (job_id, member_name, output_path, output_size)
             VALUES (7, 'Extracted.Member.mkv', '/srv/complete/Job.7/Extracted.Member.mkv', 10);
         INSERT INTO graph_members (
             job_id, node_id, member_id, name, output_name, closed, undecided, has_file
         ) VALUES (7, 1, 1, 'Graph.Member.mkv', 'Graph.Output.mkv', 0, 0, 0);
         INSERT INTO duplicate_job_snapshots (
             job_id, lifecycle, normalized_name, admission_action, origin,
             created_at, updated_at
         ) VALUES (7, 'active', 'shared', 'admit', 'test', 1, 1),
                  (8, 'active', 'shared', 'admit', 'test', 1, 1);
         INSERT INTO semantic_duplicate_groups (group_id, normalized_key, created_at, updated_at)
             VALUES (1, 'shared-normalized-key', 1, 1);
         INSERT INTO semantic_duplicate_candidates
             (job_id, group_id, score, candidate_state, source_filename, created_at, updated_at)
             VALUES (7, 1, 100, 'active', 'Duplicate.Source.nzb', 1, 1),
                    (8, 1, 90, 'active', NULL, 1, 1);",
    );
    db.archive_job(
        JobId(8),
        &crate::JobHistoryRow {
            job_id: 8,
            protocol: Default::default(),
            job_hash: None,
            name: "History.Name".to_string(),
            status: "completed".to_string(),
            error_message: None,
            total_bytes: 0,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: Some(r#"[["request","History Attribute"]]"#.to_string()),
            server_attribution: None,
            archived_at: None,
        },
    )
    .unwrap();

    let jobs = db.diagnostic_job_names_whole().unwrap();
    assert_eq!(paged(&db), jobs);
    let files = &jobs[&7].files;
    for name in [
        "Duplicate.Source.nzb",
        "Source.Name.bin",
        "Current.Name.bin",
        "Canonical.Name.mkv",
        "Identity.Set",
        "Detected.Set",
        "Extracted.Member.mkv",
        "/srv/complete/Job.7/Extracted.Member.mkv",
        "Graph.Member.mkv",
        "Graph.Output.mkv",
    ] {
        assert!(files.iter().any(|file| file == name), "{name}: {files:?}");
    }
    assert_eq!(jobs[&7].keys, vec!["shared-normalized-key"]);
    assert_eq!(jobs[&8].keys, vec!["shared-normalized-key"]);
    assert_eq!(jobs[&7].normalized_names, vec!["shared"]);
    assert!(
        jobs[&8]
            .metadata
            .contains(&("request".to_string(), "History Attribute".to_string())),
        "{:?}",
        jobs[&8].metadata
    );
}

// Only the most recent finished jobs are read; the history is counted
// whole.
#[test]
fn history_is_read_up_to_the_cap_and_counted_whole() {
    let db = Database::open_in_memory().unwrap();
    execute_sql(
        &db,
        "WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < 2001)
         INSERT INTO job_history (job_id, name, status, nzb_path, created_at, completed_at)
         SELECT id, 'History.' || id, 'completed', '/srv/nzbs/History.' || id || '.nzb',
                1, id FROM n;",
    );
    let jobs = paged(&db);
    assert_eq!(jobs, db.diagnostic_job_names_whole().unwrap());
    assert_eq!(jobs.len(), DIAGNOSTIC_RECENT_HISTORY);
    let recent = db.diagnostic_recent_history_ids().unwrap();
    assert_eq!(recent.len(), DIAGNOSTIC_RECENT_HISTORY);
    assert_eq!(recent.first(), Some(&2001));
    assert!(!jobs.contains_key(&1), "the oldest job is past the cap");
    assert_eq!(jobs[&2001].name.as_deref(), Some("History.2001"));
    assert_eq!(db.diagnostic_history_total().unwrap(), 2001);
    let page = db
        .diagnostic_persisted_nzbs(DiagnosticNzbSource::History, 0)
        .unwrap();
    assert_eq!(page.first().map(|nzb| nzb.job_id), Some(2));
}
