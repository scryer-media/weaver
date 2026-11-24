// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::queries::{history_completed_by_sql, history_status_ids_sql};
use std::borrow::Borrow;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_db::persistence::{
    StateError,
    connection::Database,
    sql_runtime::{SqlArg, SqlRuntime, max_rows_for_engine},
};

// The failed-job sweep's page, first and following. The predicate is spelled
// out literally so the partial unswept-failed index serves the filter and
// the sort. A job whose torrent row stands is live, its folders its own,
// and is passed: the torrent's key answers that for each row. The plan test
// reads these same texts.
pub const FAILED_SWEEP_FIRST_PAGE_SQL: &str = "SELECT job_id, completed_at, output_dir FROM job_history \
     WHERE status = 'failed' AND files_swept_at IS NULL AND completed_at <= {} \
     AND NOT EXISTS (SELECT 1 FROM torrents WHERE torrents.job_id = job_history.job_id) \
     ORDER BY completed_at DESC, job_id DESC LIMIT {}";
pub const FAILED_SWEEP_NEXT_PAGE_SQL: &str = "SELECT job_id, completed_at, output_dir FROM job_history \
     WHERE status = 'failed' AND files_swept_at IS NULL AND completed_at <= {} \
     AND (completed_at < {} OR (completed_at = {} AND job_id < {})) \
     AND NOT EXISTS (SELECT 1 FROM torrents WHERE torrents.job_id = job_history.job_id) \
     ORDER BY completed_at DESC, job_id DESC LIMIT {}";

pub trait HistoryInternalQueries {
    fn job_history_ids_by_status_page(
        &self,
        status: &str,
        after: Option<(i64, u64)>,
        limit: usize,
    ) -> Result<Vec<(i64, u64)>, StateError>;
    fn list_history_completed_by(
        &self,
        cutoff: i64,
        after: Option<(i64, u64)>,
        limit: usize,
    ) -> Result<Vec<(i64, u64)>, StateError>;
    fn failed_job_history_page(
        &self,
        finished_by: i64,
        after: Option<(i64, JobId)>,
        limit: usize,
    ) -> Result<Vec<(i64, JobId, Option<String>)>, StateError>;
    fn failed_jobs_finished_by(
        &self,
        finished_by: i64,
        job_ids: &[JobId],
    ) -> Result<std::collections::HashSet<JobId>, StateError>;
}

impl<T: Borrow<Database>> HistoryInternalQueries for T {
    // One page of the history rows, hidden ones too, whose status is
    // `status`, newest first, as `(completed_at, job_id)`, at most `limit`
    // of them. `after` is the last pair of the page before, so a page never
    // repeats a row. A range of the status index, read in its order.
    fn job_history_ids_by_status_page(
        &self,
        status: &str,
        after: Option<(i64, u64)>,
        limit: usize,
    ) -> Result<Vec<(i64, u64)>, StateError> {
        let database = self.borrow();
        let datastore = database.datastore();
        let (sql, args) = history_status_ids_sql(status, after, limit);
        database.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| Ok((row.i64("completed_at")?, row.i64("job_id")? as u64)))
                .collect()
        })
    }

    // One page of the history rows that finished at or before `cutoff`, in
    // epoch seconds, oldest first, as `(completed_at, job_id)`. `after` is
    // the last pair of the page before, so a page never repeats a row.
    fn list_history_completed_by(
        &self,
        cutoff: i64,
        after: Option<(i64, u64)>,
        limit: usize,
    ) -> Result<Vec<(i64, u64)>, StateError> {
        let database = self.borrow();
        let datastore = database.datastore();
        database.run_sql_blocking_read(async move {
            let (sql, args) = history_completed_by_sql(cutoff, after, limit);
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| Ok((row.i64("completed_at")?, row.i64("job_id")? as u64)))
                .collect()
        })
    }

    // One page of the failed jobs whose record says they finished at or
    // before `finished_by`, in epoch seconds, and that no sweep has yet left
    // nothing of, newest first, at most `limit` of them, each with its
    // finish time and the output directory it records. `after` is the last
    // `(completed_at, job_id)` of the page before. The predicate is spelled
    // out literally so the partial unswept-failed index serves the filter
    // and the sort.
    fn failed_job_history_page(
        &self,
        finished_by: i64,
        after: Option<(i64, JobId)>,
        limit: usize,
    ) -> Result<Vec<(i64, JobId, Option<String>)>, StateError> {
        let database = self.borrow();
        let datastore = database.datastore();
        database.run_sql_blocking_read(async move {
            let limit = SqlArg::I64(limit as i64);
            let (sql, args) = match after {
                None => (
                    FAILED_SWEEP_FIRST_PAGE_SQL,
                    vec![SqlArg::I64(finished_by), limit],
                ),
                Some((completed_at, job_id)) => (
                    FAILED_SWEEP_NEXT_PAGE_SQL,
                    vec![
                        SqlArg::I64(finished_by),
                        SqlArg::I64(completed_at),
                        SqlArg::I64(completed_at),
                        SqlArg::I64(job_id.0 as i64),
                        limit,
                    ],
                ),
            };
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), sql, &args).await?;
            rows.into_iter()
                .map(|row| {
                    Ok((
                        row.i64("completed_at")?,
                        JobId(row.i64("job_id")? as u64),
                        row.opt_text("output_dir")?,
                    ))
                })
                .collect()
        })
    }

    // Which of `job_ids` are still failed jobs that finished at or before
    // `finished_by`, not yet swept: one statement per bind-sized chunk.
    fn failed_jobs_finished_by(
        &self,
        finished_by: i64,
        job_ids: &[JobId],
    ) -> Result<std::collections::HashSet<JobId>, StateError> {
        if job_ids.is_empty() {
            return Ok(Default::default());
        }
        let database = self.borrow();
        let datastore = database.datastore();
        let ids = job_ids.iter().map(|id| id.0 as i64).collect::<Vec<_>>();
        database.run_sql_blocking_read(async move {
            let mut failed = std::collections::HashSet::new();
            for chunk in ids.chunks(max_rows_for_engine(datastore.engine(), 1) - 1) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args = std::iter::once(SqlArg::I64(finished_by))
                    .chain(chunk.iter().copied().map(SqlArg::I64))
                    .collect::<Vec<_>>();
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id FROM job_history WHERE status = 'failed' AND files_swept_at IS NULL \
                         AND completed_at <= {{}} AND job_id IN ({placeholders})"
                    ),
                    &args,
                )
                .await?;
                for row in rows {
                    failed.insert(JobId(row.i64("job_id")? as u64));
                }
            }
            Ok(failed)
        })
    }
}
