use crate::StateError;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::history::{parse_history_metadata, public_history_attributes};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use sqlx::{Postgres, QueryBuilder, Sqlite};

const SQLITE_BATCH_BIND_LIMIT: usize = 900;
const POSTGRES_BATCH_BIND_LIMIT: usize = 16_000;

fn max_rows_for_tx(tx: &SqlTx<'_>, binds_per_row: usize) -> usize {
    let bind_limit = match tx {
        SqlTx::Sqlite(_) => SQLITE_BATCH_BIND_LIMIT,
        SqlTx::Postgres(_) => POSTGRES_BATCH_BIND_LIMIT,
    };
    (bind_limit / binds_per_row.max(1)).max(1)
}

async fn bulk_insert_integration_events_tx(
    tx: &mut SqlTx<'_>,
    events: &[IntegrationEventRow],
) -> Result<(), StateError> {
    if events.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 4);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(event.item_id.map(|value| value as i64))
                        .push_bind(&event.payload_json);
                });
                builder
                    .build()
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))?;
            }
        }
        SqlTx::Postgres(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(event.item_id.map(|value| value as i64))
                        .push_bind(&event.payload_json);
                });
                builder
                    .build()
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| StateError::Database(error.to_string()))?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn replace_job_history_attributes_tx(
    tx: &mut SqlTx<'_>,
    entry: &JobHistoryRow,
) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM job_history_attributes WHERE job_id = {}",
        &[SqlArg::I64(entry.job_id as i64)],
    )
    .await?;

    let metadata = parse_history_metadata(entry.metadata.as_deref());
    for (key, value) in public_history_attributes(&metadata) {
        tx.execute(
            "INSERT INTO job_history_attributes (job_id, key, value, completed_at)
             VALUES ({}, {}, {}, {})
             ON CONFLICT(job_id, key, value) DO UPDATE SET
                completed_at = excluded.completed_at",
            &[
                SqlArg::I64(entry.job_id as i64),
                SqlArg::Text(key),
                SqlArg::Text(value),
                SqlArg::I64(entry.completed_at),
            ],
        )
        .await?;
    }

    Ok(())
}

impl Database {
    pub fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let cache_entry = entry.clone();
        let args = job_history_args(entry);
        let attribute_entry = entry.clone();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_job_history", |tx| {
                let args = args.clone();
                let attribute_entry = attribute_entry.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO job_history
                            (job_id, job_hash, name, status, error_message, total_bytes, downloaded_bytes,
                             optional_recovery_bytes, optional_recovery_downloaded_bytes,
                             failed_bytes, health, category, output_dir, nzb_path, nzb_zstd,
                             created_at, completed_at, metadata)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
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
                            nzb_zstd = COALESCE(excluded.nzb_zstd, job_history.nzb_zstd),
                            created_at = excluded.created_at,
                            completed_at = excluded.completed_at,
                            metadata = excluded.metadata",
                        &args,
                    )
                    .await?;
                    replace_job_history_attributes_tx(tx, &attribute_entry).await?;
                    Ok(())
                })
            })
            .await
        });
        if result.is_ok() {
            self.cache_job_history(cache_entry);
        }
        result
    }

    pub fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let result = self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM job_history WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            Ok(changed > 0)
        });
        if result.as_ref().is_ok_and(|changed| *changed) {
            self.invalidate_job_history_cache(job_id);
        }
        result
    }

    pub fn delete_all_job_history(&self) -> Result<usize, StateError> {
        let datastore = self.datastore();
        let result = self.run_sql_blocking(async move {
            let changed =
                SqlRuntime::execute(datastore.read_exec(), "DELETE FROM job_history", &[]).await?;
            Ok(changed as usize)
        });
        if result.as_ref().is_ok_and(|changed| *changed > 0) {
            self.clear_job_history_cache();
        }
        result
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
                    bulk_insert_integration_events_tx(tx, &events).await?;
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
    ]
}
