use crate::StateError;
use crate::persistence::Database;

use super::repository::db_err;

impl Database {
    pub fn sum_bandwidth_usage_minutes(
        &self,
        start_bucket_epoch_minute: i64,
        end_bucket_epoch_minute: i64,
    ) -> Result<u64, StateError> {
        let conn = self.conn();
        let total: Option<u64> = conn
            .query_row(
                "SELECT SUM(payload_bytes)
                 FROM bandwidth_usage_minute_buckets
                 WHERE bucket_epoch_minute >= ?1 AND bucket_epoch_minute < ?2",
                rusqlite::params![start_bucket_epoch_minute, end_bucket_epoch_minute],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        Ok(total.unwrap_or(0))
    }
}
