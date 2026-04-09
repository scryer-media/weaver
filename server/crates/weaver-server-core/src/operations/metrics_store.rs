use crate::StateError;
use crate::persistence::Database;

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
mod tests;
