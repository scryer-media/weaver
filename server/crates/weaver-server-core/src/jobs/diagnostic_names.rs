// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every name the database holds for a job, active or finished, read for the
// stand-in table of the admin diagnostics package. Nothing here is written
// anywhere: the package replaces each name with a stand-in before it leaves
// the host.
//
// The read is a run of short statements, each its own snapshot and each
// served by a primary key or an index, so no read holds the database for
// longer than one page and a caller can stop between any two. Names are read
// a page of active jobs at a time, by job id, and for the most recent
// finished jobs by their ids; stored NZBs are sized first and then read in
// small batches, so a caller can keep the bytes it holds under a budget.

use std::collections::BTreeMap;

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine};

// How many active jobs one page of [`Database::diagnostic_job_names_page`]
// covers, and how many ids one statement binds.
pub const DIAGNOSTIC_NAME_PAGE: usize = 500;

// How many of the most recently finished jobs are read, more than the
// package's job list carries (at most 1,000). Older history is counted, not
// read.
pub const DIAGNOSTIC_RECENT_HISTORY: usize = 2000;

// The finished jobs the read takes: the most recent, by completion.
#[cfg(any(test, feature = "test-support"))]
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

impl JobNameRecord {
    // The names a job in memory is known by: its name, category, metadata,
    // output directory and hash.
    pub fn of_info(job: &crate::jobs::handle::JobInfo) -> Self {
        Self {
            name: Some(job.name.clone()),
            category: job.category.clone(),
            output_dir: job.output_dir.clone(),
            hash: job.job_hash.map(|hash| hex(&hash)),
            metadata: job.metadata.clone(),
            ..Self::default()
        }
    }
}

// Which jobs one page of names covers.
#[derive(Debug, Clone, Copy)]
pub enum DiagnosticJobs<'a> {
    // The next [`DIAGNOSTIC_NAME_PAGE`] active jobs after this job id.
    ActiveAfter(u64),
    // These finished jobs, at most [`DIAGNOSTIC_NAME_PAGE`] of them.
    History(&'a [u64]),
}

// One page of job names, and what reading it cost.
#[derive(Debug, Default)]
pub struct DiagnosticNamePage {
    pub jobs: BTreeMap<u64, JobNameRecord>,
    // For an active page, the last active job id it covered; `None` when no
    // active job is left after the page's start.
    pub last_active: Option<u64>,
    // The active job ids the page covered, in id order.
    pub active_ids: Vec<u64>,
    pub statements: Vec<String>,
    pub rows: u64,
}

// One stored NZB, sized before it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticNzbSize {
    pub job_id: u64,
    pub bytes: u64,
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

// A predicate on a `job_id` column, with its arguments: a range of ids, or a
// list of them.
struct JobFilter {
    sql: String,
    args: Vec<SqlArg>,
}

impl JobFilter {
    fn range(column: &str, after: u64, last: u64) -> Self {
        Self {
            sql: format!("{column} > {{}} AND {column} <= {{}}"),
            args: vec![SqlArg::I64(after as i64), SqlArg::I64(last as i64)],
        }
    }

    fn list(column: &str, ids: &[u64]) -> Self {
        let marks = vec!["{}"; ids.len()].join(", ");
        Self {
            sql: format!("{column} IN ({marks})"),
            args: ids.iter().map(|id| SqlArg::I64(*id as i64)).collect(),
        }
    }
}

// The file and member name statements, each table in a stable order so the
// stand-ins they are given follow it, and the columns each carries.
fn file_statements(
    filter: impl Fn(&str) -> JobFilter,
) -> Vec<(String, Vec<SqlArg>, &'static [&'static str])> {
    let tables: [(&str, &str, &str, &'static [&'static str]); 6] = [
        (
            "semantic_duplicate_candidates",
            "source_filename",
            "job_id",
            &["source_filename"],
        ),
        (
            "active_files",
            "filename",
            "job_id, file_index",
            &["filename"],
        ),
        (
            "active_file_identities",
            "source_filename, current_filename, canonical_filename, classification_set_name",
            "job_id, file_index",
            &[
                "source_filename",
                "current_filename",
                "canonical_filename",
                "classification_set_name",
            ],
        ),
        (
            "active_detected_archives",
            "set_name",
            "job_id, file_index",
            &["set_name"],
        ),
        (
            "active_extracted",
            "member_name, output_path",
            "job_id, member_name",
            &["member_name", "output_path"],
        ),
        (
            "graph_members",
            "name, output_name",
            "job_id, node_id, member_id",
            &["name", "output_name"],
        ),
    ];
    tables
        .into_iter()
        .map(|(table, columns, order, read)| {
            let filter = filter("job_id");
            (
                format!(
                    "SELECT job_id, {columns} FROM {table} WHERE {} ORDER BY {order}",
                    filter.sql
                ),
                filter.args,
                read,
            )
        })
        .collect()
}

impl Database {
    // The ids of the [`DIAGNOSTIC_RECENT_HISTORY`] most recently finished
    // jobs, by completion, newest first. Served by the history's completion
    // index.
    pub fn diagnostic_recent_history_ids(&self) -> Result<Vec<u64>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!(
                    "SELECT job_id FROM job_history
                      ORDER BY completed_at DESC, job_id DESC LIMIT {DIAGNOSTIC_RECENT_HISTORY}"
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| Ok(row.i64("job_id")? as u64))
            .collect()
        })
    }

    // The names of one page of jobs: every table that carries a name, each
    // read with one statement over the page's ids, by primary key. Each
    // statement is its own snapshot; a job archived between two pages can be
    // read in both, and the table built from them gives it one stand-in.
    pub fn diagnostic_job_names_page(
        &self,
        jobs: DiagnosticJobs<'_>,
    ) -> Result<DiagnosticNamePage, StateError> {
        let datastore = self.datastore();
        let page = DIAGNOSTIC_NAME_PAGE.min(max_rows_for_engine(datastore.engine(), 1));
        enum Owned {
            ActiveAfter(u64),
            History(Vec<u64>),
        }
        let jobs = match jobs {
            DiagnosticJobs::ActiveAfter(after) => Owned::ActiveAfter(after),
            DiagnosticJobs::History(ids) => {
                Owned::History(ids.iter().take(page).copied().collect())
            }
        };
        self.run_sql_blocking_read(async move {
            let exec = || datastore.read_exec();
            let mut out = DiagnosticNamePage::default();
            let filter: Box<dyn Fn(&str) -> JobFilter + Send> = match jobs {
                Owned::ActiveAfter(after) => {
                    let sql = format!(
                        "SELECT job_id, nzb_hash, nzb_path, output_dir, category, metadata
                           FROM active_jobs WHERE job_id > {{}} ORDER BY job_id LIMIT {page}"
                    );
                    let rows =
                        SqlRuntime::fetch_all(exec(), &sql, &[SqlArg::I64(after as i64)]).await?;
                    out.statements.push(sql);
                    out.rows += rows.len() as u64;
                    for row in rows {
                        let id = row.i64("job_id")? as u64;
                        out.active_ids.push(id);
                        let job = out.jobs.entry(id).or_default();
                        job.hash = Some(hex(&row.bytes("nzb_hash")?));
                        job.nzb_path = Some(row.text("nzb_path")?);
                        job.output_dir = Some(row.text("output_dir")?);
                        job.category = row.opt_text("category")?;
                        job.metadata = metadata_pairs(row.opt_text("metadata")?)?;
                    }
                    let Some(&last) = out.active_ids.last() else {
                        return Ok(out);
                    };
                    out.last_active = Some(last);
                    Box::new(move |column| JobFilter::range(column, after, last))
                }
                Owned::History(ids) => {
                    if ids.is_empty() {
                        return Ok(out);
                    }
                    let history = JobFilter::list("job_id", &ids);
                    let sql = format!(
                        "SELECT job_id, job_hash, name, category, output_dir, nzb_path, metadata
                           FROM job_history WHERE {} ORDER BY job_id",
                        history.sql
                    );
                    let rows = SqlRuntime::fetch_all(exec(), &sql, &history.args).await?;
                    out.statements.push(sql);
                    out.rows += rows.len() as u64;
                    for row in rows {
                        let job = out.jobs.entry(row.i64("job_id")? as u64).or_default();
                        job.name = Some(row.text("name")?);
                        job.hash = row.opt_bytes("job_hash")?.map(|bytes| hex(&bytes));
                        job.category = row.opt_text("category")?;
                        job.output_dir = row.opt_text("output_dir")?;
                        job.nzb_path = row.opt_text("nzb_path")?;
                        job.metadata = metadata_pairs(row.opt_text("metadata")?)?;
                    }
                    let sql = format!(
                        "SELECT job_id, key, value FROM job_history_attributes
                          WHERE {} ORDER BY job_id, key, value",
                        history.sql
                    );
                    let rows = SqlRuntime::fetch_all(exec(), &sql, &history.args).await?;
                    out.statements.push(sql);
                    out.rows += rows.len() as u64;
                    for row in rows {
                        out.jobs
                            .entry(row.i64("job_id")? as u64)
                            .or_default()
                            .metadata
                            .push((row.text("key")?, row.text("value")?));
                    }
                    Box::new(move |column| JobFilter::list(column, &ids))
                }
            };

            let keys = filter("c.job_id");
            let sql = format!(
                "SELECT c.job_id, g.normalized_key
                   FROM semantic_duplicate_candidates AS c
                   INNER JOIN semantic_duplicate_groups AS g ON g.group_id = c.group_id
                  WHERE {} ORDER BY c.job_id",
                keys.sql
            );
            let rows = SqlRuntime::fetch_all(exec(), &sql, &keys.args).await?;
            out.statements.push(sql);
            out.rows += rows.len() as u64;
            for row in rows {
                out.jobs
                    .entry(row.i64("job_id")? as u64)
                    .or_default()
                    .keys
                    .push(row.text("normalized_key")?);
            }
            let snapshots = filter("job_id");
            let sql = format!(
                "SELECT job_id, normalized_name FROM duplicate_job_snapshots
                  WHERE {} ORDER BY job_id",
                snapshots.sql
            );
            let rows = SqlRuntime::fetch_all(exec(), &sql, &snapshots.args).await?;
            out.statements.push(sql);
            out.rows += rows.len() as u64;
            for row in rows {
                out.jobs
                    .entry(row.i64("job_id")? as u64)
                    .or_default()
                    .normalized_names
                    .push(row.text("normalized_name")?);
            }
            for (sql, args, columns) in file_statements(&filter) {
                let rows = SqlRuntime::fetch_all(exec(), &sql, &args).await?;
                out.statements.push(sql);
                out.rows += rows.len() as u64;
                for row in rows {
                    let job = out.jobs.entry(row.i64("job_id")? as u64).or_default();
                    for column in columns {
                        if let Some(name) = row.opt_text(column)? {
                            job.files.push(name);
                        }
                    }
                }
            }
            // A row of a job the page does not cover names no job it read.
            out.jobs
                .retain(|_, job| job.name.is_some() || job.nzb_path.is_some());
            Ok(out)
        })
    }

    // The stored size of each listed job's NZB, by primary key, without
    // reading the NZB. A job with no stored NZB is not listed.
    pub fn diagnostic_nzb_sizes(&self, ids: &[u64]) -> Result<Vec<DiagnosticNzbSize>, StateError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let filter = JobFilter::list("job_id", ids);
        let sql = format!(
            "SELECT job_id, length(nzb_zstd) AS bytes FROM job_nzbs WHERE {} ORDER BY job_id",
            filter.sql
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &filter.args)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(DiagnosticNzbSize {
                        job_id: row.i64("job_id")? as u64,
                        bytes: row.opt_i64("bytes")?.unwrap_or(0).max(0) as u64,
                    })
                })
                .collect()
        })
    }

    // The stored NZB of each listed job, by primary key, in job id order.
    pub fn diagnostic_nzbs(&self, ids: &[u64]) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let filter = JobFilter::list("job_id", ids);
        let sql = format!(
            "SELECT job_id, nzb_zstd FROM job_nzbs WHERE {} ORDER BY job_id",
            filter.sql
        );
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &filter.args)
                .await?
                .into_iter()
                .map(|row| Ok((row.i64("job_id")? as u64, row.bytes("nzb_zstd")?)))
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

// The read the paged one replaced, kept for tests to compare against: one
// read transaction over every name table, whole, and every stored NZB a page
// at a time.
#[cfg(any(test, feature = "test-support"))]
pub mod reference {
    use super::*;
    use crate::persistence::sql_runtime::{SqlTx, StoreDatastore, db_err, pg_db_err};

    pub const DIAGNOSTIC_NZB_PAGE: usize = 64;

    // One persisted NZB.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DiagnosticPersistedNzb {
        pub job_id: u64,
        pub nzb_path: Option<String>,
        pub nzb: Option<Vec<u8>>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DiagnosticNzbSource {
        Active,
        History,
    }

    // A seeding script for tests and measurements, SQLite only: `active`
    // active jobs (even ids) with `files` file rows in each file table,
    // `history` finished jobs (odd ids) with one attribute each, and
    // duplicate rows for every job.
    pub fn diagnostic_seed_script(active: u64, history: u64, files: u64) -> String {
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {active})
             INSERT INTO active_jobs (job_id, nzb_hash, nzb_path, output_dir, status,
                 download_state, post_state, run_state, created_at, category, metadata)
             SELECT i * 2, zeroblob(32), '/srv/seed-nzbs/Seed.Job.' || (i * 2) || '.nzb',
                    '/srv/seed-complete/Seed.Job.' || (i * 2), 'queued', 'queued', 'idle',
                    'active', 1, 'seedcat',
                    '[[\"request\",\"Seed Request ' || (i * 2) || '\"]]'
               FROM n;
             WITH RECURSIVE f(k) AS (SELECT 0 UNION ALL SELECT k + 1 FROM f WHERE k + 1 < {files})
             INSERT INTO active_files (job_id, file_index, filename, md5)
             SELECT a.job_id, f.k, 'Seed.Job.' || a.job_id || '.part' || f.k || '.rar', zeroblob(16)
               FROM active_jobs AS a, f;
             WITH RECURSIVE f(k) AS (SELECT 0 UNION ALL SELECT k + 1 FROM f WHERE k + 1 < {files})
             INSERT INTO active_file_identities (job_id, file_index, source_filename,
                 current_filename, canonical_filename, classification_set_name,
                 classification_source)
             SELECT a.job_id, f.k, 'Seed.Source.' || a.job_id || '.' || f.k || '.bin',
                    'Seed.Current.' || a.job_id || '.' || f.k || '.bin',
                    'Seed.Canonical.' || a.job_id || '.' || f.k || '.mkv',
                    'Seed.Set.' || a.job_id, 'declared'
               FROM active_jobs AS a, f;
             INSERT INTO active_detected_archives
             SELECT job_id, 0, 'rar', 'Seed.Detected.' || job_id, 0 FROM active_jobs;
             INSERT INTO active_extracted (job_id, member_name, output_path, output_size)
             SELECT job_id, 'Seed.Member.' || job_id || '.mkv',
                    output_dir || '/Seed.Member.' || job_id || '.mkv', 10
               FROM active_jobs;
             INSERT INTO graph_members (job_id, node_id, member_id, name, output_name, closed,
                 undecided, has_file)
             SELECT job_id, 1, 1, 'Seed.Graph.' || job_id || '.mkv',
                    'Seed.Output.' || job_id || '.mkv', 0, 0, 0
               FROM active_jobs;
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {history})
             INSERT INTO job_history (job_id, name, status, nzb_path, created_at, completed_at)
             SELECT i * 2 - 1, 'Seed.History.' || (i * 2 - 1), 'completed',
                    '/srv/seed-nzbs/Seed.History.' || (i * 2 - 1) || '.nzb', 1, i
               FROM n;
             INSERT INTO job_history_attributes (job_id, key, value, completed_at)
             SELECT job_id, 'request', 'Seed Attribute ' || job_id, completed_at FROM job_history;
             INSERT INTO duplicate_job_snapshots (job_id, lifecycle, normalized_name,
                 admission_action, origin, created_at, updated_at)
             SELECT job_id, 'active', 'seed normalized ' || job_id, 'admit', 'test', 1, 1
               FROM (SELECT job_id FROM active_jobs UNION ALL SELECT job_id FROM job_history);
             INSERT INTO semantic_duplicate_groups (group_id, normalized_key, created_at, updated_at)
             SELECT job_id, 'seed-key-' || job_id, 1, 1 FROM duplicate_job_snapshots
              WHERE job_id % 10 = 0;
             INSERT INTO semantic_duplicate_candidates (job_id, group_id, score,
                 candidate_state, source_filename, created_at, updated_at)
             SELECT job_id, job_id - job_id % 10, 100, 'active',
                    'Seed.Duplicate.' || job_id || '.nzb', 1, 1
               FROM duplicate_job_snapshots
              WHERE job_id - job_id % 10 IN (SELECT group_id FROM semantic_duplicate_groups);"
        )
    }

    impl Database {
        #[expect(
            clippy::disallowed_methods,
            reason = "writer gate: a read-only snapshot transaction on the read lane, it writes nothing; Postgres has no writer gate"
        )]
        pub fn diagnostic_job_names_whole(
            &self,
        ) -> Result<BTreeMap<u64, JobNameRecord>, StateError> {
            let datastore = self.datastore();
            let recent = recent_history();
            self.run_sql_blocking_read(async move {
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
                jobs.retain(|_, job| job.name.is_some() || job.nzb_path.is_some());
                tx.commit().await?;
                Ok(jobs)
            })
        }

        pub fn diagnostic_persisted_nzbs(
            &self,
            source: DiagnosticNzbSource,
            after_job_id: u64,
        ) -> Result<Vec<DiagnosticPersistedNzb>, StateError> {
            let datastore = self.datastore();
            let args = vec![SqlArg::I64(after_job_id as i64)];
            let sql = match source {
                DiagnosticNzbSource::Active => format!(
                    "SELECT a.job_id, a.nzb_path, n.nzb_zstd FROM active_jobs AS a
                       LEFT JOIN job_nzbs AS n ON n.job_id = a.job_id
                      WHERE a.job_id > {{}} ORDER BY a.job_id LIMIT {DIAGNOSTIC_NZB_PAGE}"
                ),
                DiagnosticNzbSource::History => format!(
                    "SELECT h.job_id, h.nzb_path, n.nzb_zstd FROM job_history AS h
                       LEFT JOIN job_nzbs AS n ON n.job_id = h.job_id
                      WHERE h.job_id > {{}} AND h.{} ORDER BY h.job_id LIMIT {DIAGNOSTIC_NZB_PAGE}",
                    recent_history()
                ),
            };
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

        // Runs a seeding script for a test, in one transaction.
        pub fn diagnostic_seed_sql(&self, sql: String) -> Result<(), StateError> {
            let datastore = self.datastore();
            self.run_sql_blocking(async move {
                SqlRuntime::run_in_transaction(&datastore, "diagnostic_seed_sql", |tx| {
                    let sql = sql.clone();
                    Box::pin(async move {
                        tx.execute(&sql, &[]).await?;
                        Ok(())
                    })
                })
                .await
            })
        }

        // Stores NZBs for a test, in one transaction.
        pub fn diagnostic_seed_nzbs(&self, rows: Vec<(u64, Vec<u8>)>) -> Result<(), StateError> {
            let datastore = self.datastore();
            let rows = std::sync::Arc::new(rows);
            self.run_sql_blocking(async move {
                SqlRuntime::run_in_transaction(&datastore, "diagnostic_seed_nzbs", |tx| {
                    let rows = rows.clone();
                    Box::pin(async move {
                        for (id, nzb) in rows.iter() {
                            tx.execute(
                                "INSERT INTO job_nzbs (job_id, nzb_zstd) VALUES ({}, {})
                                 ON CONFLICT(job_id) DO UPDATE SET nzb_zstd = excluded.nzb_zstd",
                                &[SqlArg::I64(*id as i64), SqlArg::Bytes(nzb.clone())],
                            )
                            .await?;
                        }
                        Ok(())
                    })
                })
                .await
            })
        }

        // The query plan SQLite gives a statement, one line per step.
        pub fn diagnostic_query_plan(
            &self,
            sql: &str,
            args: Vec<i64>,
        ) -> Result<Vec<String>, StateError> {
            let datastore = self.datastore();
            let sql = format!("EXPLAIN QUERY PLAN {sql}");
            self.run_sql_blocking_read(async move {
                let args: Vec<SqlArg> = args.into_iter().map(SqlArg::I64).collect();
                SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                    .await?
                    .into_iter()
                    .map(|row| row.text("detail"))
                    .collect()
            })
        }
    }
}

#[cfg(test)]
mod tests {
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
        let list = vec!["{}"; 3].join(", ");
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
}
