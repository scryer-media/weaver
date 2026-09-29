use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRuntime};

impl Database {
    pub fn sum_bandwidth_usage_minutes(
        &self,
        start_bucket_epoch_minute: i64,
        end_bucket_epoch_minute: i64,
    ) -> Result<u64, StateError> {
        self.sum_bandwidth_usage(start_bucket_epoch_minute, end_bucket_epoch_minute, false)
    }

    pub fn sum_metered_bandwidth_usage_minutes(
        &self,
        start: i64,
        end: i64,
    ) -> Result<u64, StateError> {
        self.sum_bandwidth_usage(start, end, true)
    }

    fn sum_bandwidth_usage(
        &self,
        start_bucket_epoch_minute: i64,
        end_bucket_epoch_minute: i64,
        metered_only: bool,
    ) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let sql = match datastore.engine() {
                SqlEngine::Sqlite => {
                    "SELECT SUM(payload_bytes) AS total
                       FROM bandwidth_usage_minute_buckets
                      WHERE bucket_epoch_minute >= {} AND bucket_epoch_minute < {}"
                }
                SqlEngine::Postgres => {
                    "SELECT COALESCE(SUM(payload_bytes), 0)::BIGINT AS total
                       FROM bandwidth_usage_minute_buckets
                      WHERE bucket_epoch_minute >= {} AND bucket_epoch_minute < {}"
                }
            };
            let sql = if metered_only {
                format!("{sql} AND metered = 1")
            } else {
                sql.to_owned()
            };
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &sql,
                &[
                    SqlArg::I64(start_bucket_epoch_minute),
                    SqlArg::I64(end_bucket_epoch_minute),
                ],
            )
            .await?;
            let total = row
                .map(|row| row.opt_i64("total"))
                .transpose()?
                .flatten()
                .unwrap_or(0);
            Ok(total as u64)
        })
    }
}
