use std::path::PathBuf;

use crate::StateError;
use crate::history::model::HistoryFilter;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

type PersistedNzbRecord = (PathBuf, Option<Vec<u8>>);

const JOB_HISTORY_SELECT: &str =
    "SELECT job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
        optional_recovery_bytes, optional_recovery_downloaded_bytes,
        failed_bytes, health, category, output_dir, nzb_path,
        created_at, completed_at, metadata,
        last_diagnostic_id, last_diagnostic_uploaded_at_epoch_ms
   FROM job_history";

const JOB_HISTORY_SELECT_ALIASED: &str =
    "SELECT h.job_id, h.job_hash, h.name, h.status, h.error_message, h.total_bytes, h.downloaded_bytes,
        h.optional_recovery_bytes, h.optional_recovery_downloaded_bytes,
        h.failed_bytes, h.health, h.category, h.output_dir, h.nzb_path,
        h.created_at, h.completed_at, h.metadata,
        h.last_diagnostic_id, h.last_diagnostic_uploaded_at_epoch_ms
   FROM ";

impl Database {
    pub fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        self.get_job_history_inner(job_id)
    }

    pub fn get_job_history_profiled(
        &self,
        job_id: u64,
        label: &'static str,
    ) -> Result<Option<JobHistoryRow>, StateError> {
        let _probe = crate::runtime::perf_probe::scope(label);
        self.get_job_history_inner(job_id)
    }

    fn get_job_history_inner(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        if let Some(row) = self.get_cached_job_history(job_id) {
            crate::runtime::perf_probe::record(
                "db.get_job_history.cache_hit",
                std::time::Duration::ZERO,
            );
            return Ok(Some(row));
        }
        crate::runtime::perf_probe::record(
            "db.get_job_history.cache_miss",
            std::time::Duration::ZERO,
        );
        let datastore = self.datastore();
        let row = self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("{JOB_HISTORY_SELECT} WHERE job_id = {{}} LIMIT 1"),
                &[SqlArg::I64(job_id as i64)],
            )
            .await?
            .map(job_history_row_from_sql)
            .transpose()
        })?;
        if let Some(row) = &row {
            self.cache_job_history(row.clone());
        }
        Ok(row)
    }

    pub fn list_job_history(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        let filter = filter.clone();
        self.run_sql_blocking(async move {
            let mut sql = String::from(JOB_HISTORY_SELECT_ALIASED);
            let mut args = Vec::new();

            append_history_from_clause(&mut sql, &mut args, &filter);
            append_history_row_filter_predicates(&mut sql, &mut args, &filter);

            sql.push_str(" ORDER BY h.completed_at DESC, h.job_id DESC");

            append_history_pagination(&mut sql, &mut args, &filter);

            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter().map(job_history_row_from_sql).collect()
        })
    }

    pub fn count_job_history(&self, filter: &HistoryFilter) -> Result<u32, StateError> {
        let datastore = self.datastore();
        let filter = filter.clone();
        self.run_sql_blocking(async move {
            let mut sql = String::from("SELECT COUNT(*) AS count FROM ");
            let mut args = Vec::new();

            append_history_from_clause(&mut sql, &mut args, &filter);
            append_history_row_filter_predicates(&mut sql, &mut args, &filter);

            let count = SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args)
                .await?
                .map(|row| row.i64("count"))
                .transpose()?
                .unwrap_or(0)
                .max(0) as u32;
            Ok(count)
        })
    }

    pub fn list_integration_events_after(
        &self,
        after_id: Option<i64>,
        item_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Vec<IntegrationEventRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let mut sql = String::from(
                "SELECT id, timestamp, kind, item_id, payload_json
                   FROM integration_events
                  WHERE 1=1",
            );
            let mut args = Vec::new();

            if let Some(after_id) = after_id {
                sql.push_str(" AND id > {}");
                args.push(SqlArg::I64(after_id));
            }

            if let Some(item_id) = item_id {
                sql.push_str(" AND item_id = {}");
                args.push(SqlArg::I64(item_id as i64));
            }

            sql.push_str(" ORDER BY id ASC");

            if let Some(limit) = limit {
                sql.push_str(" LIMIT {}");
                args.push(SqlArg::I64(i64::from(limit)));
            }

            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| {
                    Ok(IntegrationEventRow {
                        id: row.i64("id")?,
                        timestamp: row.i64("timestamp")?,
                        kind: row.text("kind")?,
                        item_id: row.opt_i64("item_id")?.map(|value| value as u64),
                        payload_json: row.text("payload_json")?,
                    })
                })
                .collect()
        })
    }

    pub fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM integration_events",
                &[],
            )
            .await?
            .map(|row| row.opt_i64("id"))
            .transpose()
            .map(Option::flatten)
        })
    }

    pub fn max_job_id(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let max = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(job_id) AS job_id FROM job_history",
                &[],
            )
            .await?
            .map(|row| row.opt_i64("job_id"))
            .transpose()?
            .flatten()
            .unwrap_or(0);
            Ok(max as u64)
        })
    }

    pub fn load_history_job_persisted_nzb(
        &self,
        job_id: u64,
    ) -> Result<Option<PersistedNzbRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT nzb_path, nzb_zstd
                   FROM job_history
                  WHERE job_id = {}
                  LIMIT 1",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?
            .map(|row| {
                let path = row
                    .opt_text("nzb_path")?
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from(format!("job-{job_id}.nzb")));
                Ok((path, row.opt_bytes("nzb_zstd")?))
            })
            .transpose()
        })
    }
}

fn append_history_from_clause(sql: &mut String, args: &mut Vec<SqlArg>, filter: &HistoryFilter) {
    match (&filter.metadata_equals, &filter.metadata_has_key) {
        (Some(metadata_equals), maybe_key) => {
            sql.push_str(
                "job_history_attributes a
                 JOIN job_history h ON h.job_id = a.job_id
                 WHERE a.key = {} AND a.value = {}",
            );
            args.push(SqlArg::Text(metadata_equals.key.clone()));
            args.push(SqlArg::Text(metadata_equals.value.clone()));
            if let Some(key) = maybe_key
                && key != &metadata_equals.key
            {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1
                          FROM job_history_attributes ak
                         WHERE ak.job_id = h.job_id
                           AND ak.key = {}
                     )",
                );
                args.push(SqlArg::Text(key.clone()));
            }
        }
        (None, Some(key)) => {
            sql.push_str(
                "(SELECT DISTINCT job_id
                    FROM job_history_attributes
                   WHERE key = {}) a
                 JOIN job_history h ON h.job_id = a.job_id
                 WHERE 1=1",
            );
            args.push(SqlArg::Text(key.clone()));
        }
        (None, None) => {
            sql.push_str("job_history h WHERE 1=1");
        }
    }
}

fn append_history_row_filter_predicates(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    filter: &HistoryFilter,
) {
    if let Some(statuses) = &filter.statuses {
        append_in_i64_or_text_filter(
            sql,
            args,
            "h.status",
            statuses.iter().cloned().map(SqlArg::Text).collect(),
        );
    }
    if let Some(item_ids) = &filter.item_ids {
        append_in_i64_or_text_filter(
            sql,
            args,
            "h.job_id",
            item_ids
                .iter()
                .copied()
                .map(|value| SqlArg::I64(value as i64))
                .collect(),
        );
    }
    if let Some(category) = &filter.category {
        sql.push_str(" AND h.category = {}");
        args.push(SqlArg::Text(category.clone()));
    }
}

fn append_in_i64_or_text_filter(
    sql: &mut String,
    args: &mut Vec<SqlArg>,
    column: &str,
    values: Vec<SqlArg>,
) {
    if values.is_empty() {
        sql.push_str(" AND 1=0");
        return;
    }

    sql.push_str(" AND ");
    sql.push_str(column);
    sql.push_str(" IN (");
    sql.push_str(
        &std::iter::repeat_n("{}", values.len())
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    args.extend(values);
}

fn append_history_pagination(sql: &mut String, args: &mut Vec<SqlArg>, filter: &HistoryFilter) {
    match (filter.limit, filter.offset) {
        (Some(limit), Some(offset)) => {
            sql.push_str(" LIMIT {} OFFSET {}");
            args.push(SqlArg::I64(i64::from(limit)));
            args.push(SqlArg::I64(i64::from(offset)));
        }
        (Some(limit), None) => {
            sql.push_str(" LIMIT {}");
            args.push(SqlArg::I64(i64::from(limit)));
        }
        (None, Some(offset)) => {
            sql.push_str(" LIMIT {} OFFSET {}");
            args.push(SqlArg::I64(i64::from(u32::MAX)));
            args.push(SqlArg::I64(i64::from(offset)));
        }
        (None, None) => {}
    }
}

pub(crate) fn job_history_row_from_sql(
    row: crate::persistence::sql_runtime::SqlRow,
) -> Result<JobHistoryRow, StateError> {
    Ok(JobHistoryRow {
        job_id: row.i64("job_id")? as u64,
        job_hash: row.opt_bytes("job_hash")?,
        name: row.text("name")?,
        status: row.text("status")?,
        error_message: row.opt_text("error_message")?,
        total_bytes: row.i64("total_bytes")? as u64,
        downloaded_bytes: row.i64("downloaded_bytes")? as u64,
        optional_recovery_bytes: row.i64("optional_recovery_bytes")? as u64,
        optional_recovery_downloaded_bytes: row.i64("optional_recovery_downloaded_bytes")? as u64,
        failed_bytes: row.i64("failed_bytes")? as u64,
        health: row.i32("health")? as u32,
        category: row.opt_text("category")?,
        output_dir: row.opt_text("output_dir")?,
        nzb_path: row.opt_text("nzb_path")?,
        created_at: row.i64("created_at")?,
        completed_at: row.i64("completed_at")?,
        metadata: row.opt_text("metadata")?,
        last_diagnostic_id: row.opt_text("last_diagnostic_id")?,
        last_diagnostic_uploaded_at_epoch_ms: row
            .opt_i64("last_diagnostic_uploaded_at_epoch_ms")?,
    })
}
