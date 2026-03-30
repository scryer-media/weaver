use super::Database;
use crate::StateError;

const METRICS_SCRAPE_COMPRESSION_LEVEL: i32 = 3;
const METRICS_RETENTION_SECS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsScrapeRow {
    pub scraped_at_epoch_sec: i64,
    pub body_zstd: Vec<u8>,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

impl Database {
    pub fn record_metrics_scrape(
        &self,
        scraped_at_epoch_sec: i64,
        raw_body: &str,
    ) -> Result<(), StateError> {
        let body_zstd = zstd::bulk::compress(raw_body.as_bytes(), METRICS_SCRAPE_COMPRESSION_LEVEL)
            .map_err(db_err)?;
        let prune_before_epoch_sec = scraped_at_epoch_sec - METRICS_RETENTION_SECS;

        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        tx.execute(
            "INSERT OR REPLACE INTO metrics_scrapes (scraped_at_epoch_sec, body_zstd)
             VALUES (?1, ?2)",
            rusqlite::params![scraped_at_epoch_sec, body_zstd],
        )
        .map_err(db_err)?;
        tx.execute(
            "DELETE FROM metrics_scrapes WHERE scraped_at_epoch_sec < ?1",
            [prune_before_epoch_sec],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn list_metrics_scrapes_since(
        &self,
        since_epoch_sec: i64,
    ) -> Result<Vec<MetricsScrapeRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT scraped_at_epoch_sec, body_zstd
                 FROM metrics_scrapes
                 WHERE scraped_at_epoch_sec >= ?1
                 ORDER BY scraped_at_epoch_sec ASC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([since_epoch_sec], |row| {
                Ok(MetricsScrapeRow {
                    scraped_at_epoch_sec: row.get(0)?,
                    body_zstd: row.get(1)?,
                })
            })
            .map_err(db_err)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn delete_all_metrics_scrapes(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM metrics_scrapes", [])
            .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn decompress_blob(blob: &[u8]) -> String {
        let decoded = zstd::stream::decode_all(Cursor::new(blob)).unwrap();
        String::from_utf8(decoded).unwrap()
    }

    #[test]
    fn record_metrics_scrape_round_trips_and_orders() {
        let db = Database::open_in_memory().unwrap();
        db.record_metrics_scrape(100, "metric_one 1\n").unwrap();
        db.record_metrics_scrape(110, "metric_one 2\n").unwrap();

        let rows = db.list_metrics_scrapes_since(0).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].scraped_at_epoch_sec, 100);
        assert_eq!(rows[1].scraped_at_epoch_sec, 110);
        assert_eq!(decompress_blob(&rows[0].body_zstd), "metric_one 1\n");
        assert_eq!(decompress_blob(&rows[1].body_zstd), "metric_one 2\n");
    }

    #[test]
    fn record_metrics_scrape_prunes_rows_older_than_24_hours() {
        let db = Database::open_in_memory().unwrap();
        let retention = METRICS_RETENTION_SECS;

        db.record_metrics_scrape(100, "old 1\n").unwrap();
        db.record_metrics_scrape(100 + retention, "boundary 1\n")
            .unwrap();
        db.record_metrics_scrape(100 + retention + 1, "new 1\n")
            .unwrap();

        let rows = db.list_metrics_scrapes_since(0).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].scraped_at_epoch_sec, 100 + retention);
        assert_eq!(rows[1].scraped_at_epoch_sec, 100 + retention + 1);
    }
}
