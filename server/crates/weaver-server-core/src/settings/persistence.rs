use crate::StateError;
use crate::bandwidth::ScheduleEntry;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx, StoreDatastore};

const NEXT_JOB_ID_SETTING_KEY: &str = "next_job_id";

impl Database {
    pub fn set_setting(&self, key: &str, value: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        let value = value.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[SqlArg::Text(key), SqlArg::Text(value)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_setting(&self, key: &str) -> Result<(), StateError> {
        let datastore = self.datastore();
        let key = key.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM settings WHERE key = {}",
                &[SqlArg::Text(key)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn save_schedules(&self, entries: &[ScheduleEntry]) -> Result<(), StateError> {
        let json =
            serde_json::to_string(entries).map_err(|e| StateError::Database(e.to_string()))?;
        self.set_setting("schedules", &json)
    }

    pub fn initialize_next_job_id_counter(&self) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let next_job_id = next_job_id_floor_target(&datastore).await?;
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[
                    SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
                    SqlArg::Text(next_job_id.to_string()),
                ],
            )
            .await?;
            Ok(next_job_id)
        })
    }

    pub fn reserve_next_job_id(&self) -> Result<JobId, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "reserve_next_job_id", |tx| {
                Box::pin(async move {
                    let next_job_id = reserve_next_job_id_tx(tx).await?;
                    Ok(JobId(next_job_id))
                })
            })
            .await
        })
    }
}

async fn next_job_id_floor_target(datastore: &StoreDatastore) -> Result<u64, StateError> {
    let persisted = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT value FROM settings WHERE key = {}",
        &[SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string())],
    )
    .await?
    .and_then(|row| row.text("value").ok())
    .and_then(|value| value.parse::<u64>().ok())
    .unwrap_or(10_000);

    let row = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT MAX(id) AS id FROM (
             SELECT MAX(job_id) AS id FROM active_jobs
             UNION ALL
             SELECT MAX(job_id) AS id FROM job_history
         ) ids",
        &[],
    )
    .await?;
    let max_seen = row
        .map(|row| row.opt_i64("id"))
        .transpose()?
        .flatten()
        .unwrap_or(0);

    Ok(persisted.max(max_seen as u64 + 1).max(10_000))
}

async fn max_seen_job_id_floor_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let row = tx
        .fetch_optional(
            "SELECT MAX(id) AS id FROM (
                 SELECT MAX(job_id) AS id FROM active_jobs
                 UNION ALL
                 SELECT MAX(job_id) AS id FROM job_history
             ) ids",
            &[],
        )
        .await?;
    let max_seen = row
        .map(|row| row.opt_i64("id"))
        .transpose()?
        .flatten()
        .unwrap_or(0);

    Ok((max_seen as u64 + 1).max(10_000))
}

async fn reserve_next_job_id_tx(tx: &mut SqlTx<'_>) -> Result<u64, StateError> {
    let floor = max_seen_job_id_floor_tx(tx).await?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {})
         ON CONFLICT(key) DO NOTHING",
        &[
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
            SqlArg::Text(floor.to_string()),
        ],
    )
    .await?;

    let select_sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    let persisted = tx
        .fetch_optional(
            select_sql,
            &[SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string())],
        )
        .await?
        .and_then(|row| row.text("value").ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(floor);
    let next_job_id = persisted.max(floor).max(10_000);
    tx.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text((next_job_id + 1).to_string()),
            SqlArg::Text(NEXT_JOB_ID_SETTING_KEY.to_string()),
        ],
    )
    .await?;

    Ok(next_job_id)
}
