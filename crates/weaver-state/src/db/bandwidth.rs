use crate::StateError;

use super::Database;

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bandwidth_usage_minute_buckets_roundtrip_and_prune() {
        let db = Database::open_in_memory().unwrap();

        db.add_bandwidth_usage_minute(100, 10).unwrap();
        db.add_bandwidth_usage_minute(100, 5).unwrap();
        db.add_bandwidth_usage_minute(101, 20).unwrap();

        assert_eq!(db.sum_bandwidth_usage_minutes(100, 101).unwrap(), 15);
        assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 35);
        assert_eq!(db.sum_bandwidth_usage_minutes(99, 100).unwrap(), 0);

        let deleted = db.prune_bandwidth_usage_before(101).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 20);
    }
}
