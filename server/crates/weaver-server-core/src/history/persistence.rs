use crate::StateError;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

impl Database {
    pub fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = job_history_args(entry);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO job_history
                    (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                     optional_recovery_bytes, optional_recovery_downloaded_bytes,
                     failed_bytes, health, category, output_dir, nzb_path, nzb_zstd,
                     created_at, completed_at, metadata, last_diagnostic_id, last_diagnostic_uploaded_at_epoch_ms)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                 ON CONFLICT(job_id) DO UPDATE SET
                    job_hash = excluded.job_hash,
                    name = excluded.name,
                    status = excluded.status,
                    error_message = excluded.error_message,
                    total_bytes = excluded.total_bytes,
                    downloaded_bytes = excluded.downloaded_bytes,
                    optional_recovery_bytes = excluded.optional_recovery_bytes,
                    optional_recovery_downloaded_bytes = excluded.optional_recovery_downloaded_bytes,
                    failed_bytes = excluded.failed_bytes,
                    health = excluded.health,
                    category = excluded.category,
                    output_dir = excluded.output_dir,
                    nzb_path = excluded.nzb_path,
                    nzb_zstd = excluded.nzb_zstd,
                    created_at = excluded.created_at,
                    completed_at = excluded.completed_at,
                    metadata = excluded.metadata,
                    last_diagnostic_id = excluded.last_diagnostic_id,
                    last_diagnostic_uploaded_at_epoch_ms = excluded.last_diagnostic_uploaded_at_epoch_ms",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM job_history WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn delete_all_job_history(&self) -> Result<usize, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed =
                SqlRuntime::execute(datastore.read_exec(), "DELETE FROM job_history", &[]).await?;
            Ok(changed as usize)
        })
    }

    pub fn insert_integration_events(
        &self,
        events: &[IntegrationEventRow],
    ) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_integration_events", |tx| {
                let events = events.clone();
                Box::pin(async move {
                    for event in events {
                        tx.execute(
                            "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
                             VALUES ({}, {}, {}, {})",
                            &[
                                SqlArg::I64(event.timestamp),
                                SqlArg::Text(event.kind),
                                SqlArg::OptI64(event.item_id.map(|value| value as i64)),
                                SqlArg::Text(event.payload_json),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn delete_all_integration_events(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.read_exec(), "DELETE FROM integration_events", &[])
                .await?;
            Ok(())
        })
    }
}

fn job_history_args(entry: &JobHistoryRow) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(entry.job_id as i64),
        SqlArg::OptBytes(entry.job_hash.clone()),
        SqlArg::Text(entry.name.clone()),
        SqlArg::Text(entry.status.clone()),
        SqlArg::OptText(entry.error_message.clone()),
        SqlArg::I64(entry.total_bytes as i64),
        SqlArg::I64(entry.downloaded_bytes as i64),
        SqlArg::I64(entry.optional_recovery_bytes as i64),
        SqlArg::I64(entry.optional_recovery_downloaded_bytes as i64),
        SqlArg::I64(entry.failed_bytes as i64),
        SqlArg::I64(i64::from(entry.health)),
        SqlArg::OptText(entry.category.clone()),
        SqlArg::OptText(entry.output_dir.clone()),
        SqlArg::OptText(entry.nzb_path.clone()),
        SqlArg::OptBytes(None),
        SqlArg::I64(entry.created_at),
        SqlArg::I64(entry.completed_at),
        SqlArg::OptText(entry.metadata.clone()),
        SqlArg::OptText(entry.last_diagnostic_id.clone()),
        SqlArg::OptI64(entry.last_diagnostic_uploaded_at_epoch_ms),
    ]
}
