use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

impl Database {
    pub fn add_bandwidth_usage_minute(
        &self,
        bucket_epoch_minute: i64,
        payload_bytes: u64,
    ) -> Result<(), StateError> {
        self.add_bandwidth_usage_minutes(&[(bucket_epoch_minute, payload_bytes)])
    }

    pub(crate) fn add_bandwidth_usage_minutes(
        &self,
        entries: &[(i64, u64)],
    ) -> Result<(), StateError> {
        if entries.is_empty() {
            return Ok(());
        }
        let datastore = self.datastore();
        let entries = entries.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "add_bandwidth_usage_minutes", |tx| {
                let entries = entries.clone();
                Box::pin(async move {
                    for (bucket_epoch_minute, payload_bytes) in entries {
                        tx.execute(
                            "INSERT INTO bandwidth_usage_minute_buckets (bucket_epoch_minute, payload_bytes)
                             VALUES ({}, {})
                             ON CONFLICT(bucket_epoch_minute)
                             DO UPDATE SET payload_bytes = bandwidth_usage_minute_buckets.payload_bytes + excluded.payload_bytes",
                            &[
                                SqlArg::I64(bucket_epoch_minute),
                                SqlArg::I64(payload_bytes as i64),
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

    pub fn prune_bandwidth_usage_before(
        &self,
        cutoff_bucket_epoch_minute: i64,
    ) -> Result<usize, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM bandwidth_usage_minute_buckets WHERE bucket_epoch_minute < {}",
                &[SqlArg::I64(cutoff_bucket_epoch_minute)],
            )
            .await?;
            Ok(changed as usize)
        })
    }
}
