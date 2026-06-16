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
        let status = filter.status.clone();
        let category = filter.category.clone();
        let limit = filter.limit;
        let offset = filter.offset;
        self.run_sql_blocking(async move {
            let mut sql = format!("{JOB_HISTORY_SELECT} WHERE 1=1");
            let mut args = Vec::new();

            if let Some(status) = status {
                sql.push_str(" AND status = {}");
                args.push(SqlArg::Text(status));
            }
            if let Some(category) = category {
                sql.push_str(" AND category = {}");
                args.push(SqlArg::Text(category));
            }

            sql.push_str(" ORDER BY completed_at DESC");

            if let Some(limit) = limit {
                sql.push_str(&format!(" LIMIT {}", limit));
            }
            if let Some(offset) = offset {
                sql.push_str(&format!(" OFFSET {}", offset));
            }

            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter().map(job_history_row_from_sql).collect()
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
