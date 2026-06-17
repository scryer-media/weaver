use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, StoreDatastore};
use crate::persistence::{Database, DatabaseWriterExecutor};
use sqlx::{Postgres, QueryBuilder, Sqlite};

const SQLITE_BATCH_BIND_LIMIT: usize = 900;
const POSTGRES_BATCH_BIND_LIMIT: usize = 16_000;

/// A persisted job event.
#[derive(Debug, Clone)]
pub struct JobEvent {
    pub job_id: u64,
    pub timestamp: i64,
    pub kind: String,
    pub message: String,
    pub file_id: Option<String>,
}

pub const JOB_EVENT_DOWNLOAD_FINALIZATION_MARKER: &str = "__timeline:finalizing-download";

fn max_rows_for_tx(tx: &SqlTx<'_>, binds_per_row: usize) -> usize {
    let bind_limit = match tx {
        SqlTx::Sqlite(_) => SQLITE_BATCH_BIND_LIMIT,
        SqlTx::Postgres(_) => POSTGRES_BATCH_BIND_LIMIT,
    };
    (bind_limit / binds_per_row.max(1)).max(1)
}

async fn bulk_insert_job_events_tx(
    tx: &mut SqlTx<'_>,
    events: &[JobEvent],
) -> Result<(), StateError> {
    if events.is_empty() {
        return Ok(());
    }

    let chunk_size = max_rows_for_tx(tx, 5);
    match tx {
        SqlTx::Sqlite(tx) => {
            for chunk in events.chunks(chunk_size) {
                let mut builder = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO job_events (job_id, timestamp, kind, message, file_id) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.job_id as i64)
                        .push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(&event.message)
                        .push_bind(&event.file_id);
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
                    "INSERT INTO job_events (job_id, timestamp, kind, message, file_id) ",
                );
                builder.push_values(chunk, |mut row, event| {
                    row.push_bind(event.job_id as i64)
                        .push_bind(event.timestamp)
                        .push_bind(&event.kind)
                        .push_bind(&event.message)
                        .push_bind(&event.file_id);
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

async fn insert_job_events_sql(
    datastore: StoreDatastore,
    events: Vec<JobEvent>,
) -> Result<(), StateError> {
    SqlRuntime::run_in_transaction(&datastore, "insert_job_events", |tx| {
        let events = events.clone();
        Box::pin(async move {
            bulk_insert_job_events_tx(tx, &events).await?;
            Ok(())
        })
    })
    .await
}

impl Database {
    /// Insert a single job event.
    pub fn insert_job_event(
        &self,
        job_id: u64,
        timestamp: i64,
        kind: &str,
        message: &str,
        file_id: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let kind = kind.to_string();
        let message = message.to_string();
        let file_id = file_id.map(str::to_string);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO job_events (job_id, timestamp, kind, message, file_id)
                 VALUES ({}, {}, {}, {}, {})",
                &[
                    SqlArg::I64(job_id as i64),
                    SqlArg::I64(timestamp),
                    SqlArg::Text(kind),
                    SqlArg::Text(message),
                    SqlArg::OptText(file_id),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Batch-insert job events in a single transaction.
    pub fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(insert_job_events_sql(datastore, events))
    }

    /// Load all events for a specific job, ordered by insertion order.
    pub fn get_job_events(&self, job_id: u64) -> Result<Vec<JobEvent>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, timestamp, kind, message, file_id
                   FROM job_events
                  WHERE job_id = {}
                  ORDER BY id ASC",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(JobEvent {
                        job_id: row.i64("job_id")? as u64,
                        timestamp: row.i64("timestamp")?,
                        kind: row.text("kind")?,
                        message: row.text("message")?,
                        file_id: row.opt_text("file_id")?,
                    })
                })
                .collect()
        })
    }

    /// Delete all events for a job.
    pub fn delete_job_events(&self, job_id: u64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM job_events WHERE job_id = {}",
                &[SqlArg::I64(job_id as i64)],
            )
            .await?;
            Ok(())
        })
    }

    /// Delete all job events across all jobs.
    pub fn delete_all_job_events(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.read_exec(), "DELETE FROM job_events", &[]).await?;
            Ok(())
        })
    }
}

impl DatabaseWriterExecutor {
    pub(crate) fn insert_job_events(&self, events: &[JobEvent]) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let datastore = self.datastore();
        let events = events.to_vec();
        self.run_sql_blocking(insert_job_events_sql(datastore, events))
    }
}

#[cfg(test)]
mod tests;
