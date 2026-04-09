use crate::StateError;
use crate::persistence::Database;

use super::repository::db_err;

impl Database {
    pub fn add_bandwidth_usage_minute(
        &self,
        bucket_epoch_minute: i64,
        payload_bytes: u64,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO bandwidth_usage_minute_buckets (bucket_epoch_minute, payload_bytes)
             VALUES (?1, ?2)
             ON CONFLICT(bucket_epoch_minute)
             DO UPDATE SET payload_bytes = payload_bytes + excluded.payload_bytes",
            rusqlite::params![bucket_epoch_minute, payload_bytes],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn prune_bandwidth_usage_before(
        &self,
        cutoff_bucket_epoch_minute: i64,
    ) -> Result<usize, StateError> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM bandwidth_usage_minute_buckets WHERE bucket_epoch_minute < ?1",
            [cutoff_bucket_epoch_minute],
        )
        .map_err(db_err)
    }
}
