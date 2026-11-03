// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every name the database holds for a job, active or finished, read for the
// stand-in table of the admin diagnostics package. Nothing here is written
// anywhere: the package replaces each name with a stand-in before it leaves
// the host.

use std::collections::BTreeMap;

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{
    SqlArg, SqlRuntime, SqlTx, StoreDatastore, db_err, pg_db_err,
};

// How many persisted NZBs one page of [`Database::diagnostic_persisted_nzbs`]
// returns, so a long history is read without holding every NZB at once.
pub const DIAGNOSTIC_NZB_PAGE: usize = 64;

// How many of the most recently finished jobs are read, more than the
// package's job list carries (at most 1,000). Older history is counted, not
// read.
pub const DIAGNOSTIC_RECENT_HISTORY: usize = 2000;

// The finished jobs the read takes: the most recent, by completion.
fn recent_history() -> String {
    format!(
        "job_id IN (SELECT job_id FROM job_history
            ORDER BY completed_at DESC, job_id DESC LIMIT {DIAGNOSTIC_RECENT_HISTORY})"
    )
}

// The names one job is known by in the database.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct JobNameRecord {
    // The name a finished job was recorded under.
    pub name: Option<String>,
    pub nzb_path: Option<String>,
    pub category: Option<String>,
    pub output_dir: Option<String>,
    // The job's content hash, as the API spells it: lowercase hex.
    pub hash: Option<String>,
    // Metadata and attribute pairs, in the order they were stored.
    pub metadata: Vec<(String, String)>,
    // The keys duplicate detection grouped the job under.
    pub keys: Vec<String>,
    // The normalized names duplicate detection recorded for the job.
    pub normalized_names: Vec<String>,
    // Every file and member name the job's rows and facts hold: posted,
    // renamed, canonical, extracted, archive set and graph member names.
    pub files: Vec<String>,
}

// One persisted NZB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticPersistedNzb {
    pub job_id: u64,
    // The path it was submitted under.
    pub nzb_path: Option<String>,
    // The compressed document.
    pub nzb: Option<Vec<u8>>,
}

// Which table a persisted NZB is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticNzbSource {
    Active,
    History,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// A metadata column as stored: a JSON list of key and value pairs. A column
// that is not one fails the read, so no value is silently left out.
fn metadata_pairs(raw: Option<String>) -> Result<Vec<(String, String)>, StateError> {
    match raw {
        None => Ok(Vec::new()),
        Some(raw) if raw.trim().is_empty() => Ok(Vec::new()),
        Some(raw) => serde_json::from_str(&raw).map_err(|error| {
            StateError::Database(format!("a job's metadata could not be read: {error}"))
        }),
    }
}

impl Database {
    // The names of every active job and of the [`DIAGNOSTIC_RECENT_HISTORY`]
    // most recently finished, by job id: one read of each table that carries
    // a name, every one in the same read transaction, so a job archived
    // while it runs is in exactly one of active or history.
    pub fn diagnostic_job_names(&self) -> Result<BTreeMap<u64, JobNameRecord>, StateError> {
        let datastore = self.datastore();
        let recent = recent_history();
        self.run_sql_blocking_read(async move {
            // SQLite in WAL takes its snapshot at the first read and holds it
            // to the end; Postgres needs repeatable read for the same.
            let mut tx = match &datastore {
                StoreDatastore::Sqlite { pool, .. } => {
                    SqlTx::Sqlite(pool.begin().await.map_err(db_err)?)
                }
                StoreDatastore::Postgres { pool } => {
                    let mut tx = SqlTx::Postgres(pool.begin().await.map_err(pg_db_err)?);
                    tx.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                        &[],
                    )
                    .await?;
                    tx
                }
            };
            let mut jobs: BTreeMap<u64, JobNameRecord> = BTreeMap::new();

            for row in tx
                .fetch_all(
                    &format!(
                        "SELECT job_id, job_hash, name, category, output_dir, nzb_path, metadata
                       FROM job_history WHERE {recent}"
                    ),
                    &[],
                )
                .await?
            {
                let job = jobs.entry(row.i64("job_id")? as u64).or_default();
                job.name = Some(row.text("name")?);
                job.hash = row.opt_bytes("job_hash")?.map(|bytes| hex(&bytes));
                job.category = row.opt_text("category")?;
                job.output_dir = row.opt_text("output_dir")?;
                job.nzb_path = row.opt_text("nzb_path")?;
                job.metadata = metadata_pairs(row.opt_text("metadata")?)?;
            }
            for row in tx
                .fetch_all(
                    &format!(
                        "SELECT job_id, key, value FROM job_history_attributes
                      WHERE {recent} ORDER BY job_id, key, value"
                    ),
                    &[],
                )
                .await?
            {
                jobs.entry(row.i64("job_id")? as u64)
                    .or_default()
                    .metadata
                    .push((row.text("key")?, row.text("value")?));
            }
            for row in tx
                .fetch_all(
                    "SELECT job_id, nzb_hash, nzb_path, output_dir, category, metadata
                   FROM active_jobs",
                    &[],
                )
                .await?
            {
                let job = jobs.entry(row.i64("job_id")? as u64).or_default();
                job.hash = Some(hex(&row.bytes("nzb_hash")?));
                job.nzb_path = Some(row.text("nzb_path")?);
                job.output_dir = Some(row.text("output_dir")?);
                job.category = row.opt_text("category")?;
                job.metadata = metadata_pairs(row.opt_text("metadata")?)?;
            }
            for row in tx
                .fetch_all(
                    "SELECT c.job_id, g.normalized_key
                   FROM semantic_duplicate_candidates AS c
                   INNER JOIN semantic_duplicate_groups AS g ON g.group_id = c.group_id
                  ORDER BY c.job_id",
                    &[],
                )
                .await?
            {
                jobs.entry(row.i64("job_id")? as u64)
                    .or_default()
                    .keys
                    .push(row.text("normalized_key")?);
            }
            for row in tx
                .fetch_all(
                    "SELECT job_id, normalized_name FROM duplicate_job_snapshots ORDER BY job_id",
                    &[],
                )
                .await?
            {
                jobs.entry(row.i64("job_id")? as u64)
                    .or_default()
                    .normalized_names
                    .push(row.text("normalized_name")?);
            }

            // File and member names, each table in a stable order so the
            // stand-ins they are given follow it.
            let file_queries: [(&str, &[&str]); 6] = [
                (
                    "SELECT job_id, source_filename FROM semantic_duplicate_candidates
                      ORDER BY job_id",
                    &["source_filename"],
                ),
                (
                    "SELECT job_id, filename FROM active_files ORDER BY job_id, file_index",
                    &["filename"],
                ),
                (
                    "SELECT job_id, source_filename, current_filename, canonical_filename,
                            classification_set_name
                       FROM active_file_identities ORDER BY job_id, file_index",
                    &[
                        "source_filename",
                        "current_filename",
                        "canonical_filename",
                        "classification_set_name",
                    ],
                ),
                (
                    "SELECT job_id, set_name FROM active_detected_archives
                      ORDER BY job_id, file_index",
                    &["set_name"],
                ),
                (
                    "SELECT job_id, member_name, output_path FROM active_extracted
                      ORDER BY job_id, member_name",
                    &["member_name", "output_path"],
                ),
                (
                    "SELECT job_id, name, output_name FROM graph_members
                      ORDER BY job_id, node_id, member_id",
                    &["name", "output_name"],
                ),
            ];
            for (sql, columns) in file_queries {
                for row in tx.fetch_all(sql, &[]).await? {
                    let job = jobs.entry(row.i64("job_id")? as u64).or_default();
                    for column in columns {
                        if let Some(name) = row.opt_text(column)? {
                            job.files.push(name);
                        }
                    }
                }
            }
            // A duplicate row of a job past the cap names no job the read
            // took.
            jobs.retain(|_, job| job.name.is_some() || job.nzb_path.is_some());
            tx.commit().await?;
            Ok(jobs)
        })
    }

    // One page of persisted NZBs from one table, in job id order, starting
    // after `after_job_id`, for the jobs [`Self::diagnostic_job_names`]
    // reads. An empty page is the end of the table.
    pub fn diagnostic_persisted_nzbs(
        &self,
        source: DiagnosticNzbSource,
        after_job_id: u64,
    ) -> Result<Vec<DiagnosticPersistedNzb>, StateError> {
        let datastore = self.datastore();
        let args = vec![SqlArg::I64(after_job_id as i64)];
        let (table, only) = match source {
            DiagnosticNzbSource::Active => ("active_jobs", "1 = 1".to_string()),
            DiagnosticNzbSource::History => ("job_history", recent_history()),
        };
        let sql = format!(
            "SELECT job_id, nzb_path, nzb_zstd FROM {table}
              WHERE job_id > {{}} AND {only} ORDER BY job_id LIMIT {DIAGNOSTIC_NZB_PAGE}"
        );
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(DiagnosticPersistedNzb {
                        job_id: row.i64("job_id")? as u64,
                        nzb_path: row.opt_text("nzb_path")?,
                        nzb: row.opt_bytes("nzb_zstd")?,
                    })
                })
                .collect()
        })
    }

    // How many finished jobs the history holds, read or not.
    pub fn diagnostic_history_total(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS total FROM job_history",
                &[],
            )
            .await?;
            Ok(row.map_or(Ok(0), |row| row.i64("total"))? as u64)
        })
    }

    // The post-processing scripts directory, or `None` when none was set.
    pub fn diagnostic_script_directory(&self) -> Result<Option<String>, StateError> {
        self.get_setting(crate::post_processing::settings::SCRIPT_DIRECTORY_KEY)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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

    fn execute_sql(db: &Database, sql: &'static str) {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.read_exec(), sql, &[]).await?;
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
            },
        )
        .unwrap();

        let jobs = db.diagnostic_job_names().unwrap();
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
        let jobs = db.diagnostic_job_names().unwrap();
        assert_eq!(jobs.len(), DIAGNOSTIC_RECENT_HISTORY);
        assert!(!jobs.contains_key(&1), "the oldest job is past the cap");
        assert_eq!(jobs[&2001].name.as_deref(), Some("History.2001"));
        assert_eq!(db.diagnostic_history_total().unwrap(), 2001);
        let page = db
            .diagnostic_persisted_nzbs(DiagnosticNzbSource::History, 0)
            .unwrap();
        assert_eq!(page.first().map(|nzb| nzb.job_id), Some(2));
    }
}
