use crate::StateError;
use crate::history::model::HistoryFilter;
use crate::history::record::{IntegrationEventRow, JobHistoryRow};
use crate::persistence::Database;

use super::repository::db_err;

impl Database {
    pub fn get_job_history(&self, job_id: u64) -> Result<Option<JobHistoryRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT job_id, name, status, error_message, total_bytes, downloaded_bytes,
                        optional_recovery_bytes, optional_recovery_downloaded_bytes,
                        failed_bytes, health, category, output_dir, nzb_path,
                        created_at, completed_at, metadata
                 FROM job_history
                 WHERE job_id = ?1
                 LIMIT 1",
            )
            .map_err(db_err)?;

        let mut rows = stmt.query([job_id as i64]).map_err(db_err)?;
        let Some(row) = rows.next().map_err(db_err)? else {
            return Ok(None);
        };

        Ok(Some(JobHistoryRow {
            job_id: row.get::<_, i64>(0).map_err(db_err)? as u64,
            name: row.get(1).map_err(db_err)?,
            status: row.get(2).map_err(db_err)?,
            error_message: row.get(3).map_err(db_err)?,
            total_bytes: row.get::<_, i64>(4).map_err(db_err)? as u64,
            downloaded_bytes: row.get::<_, i64>(5).map_err(db_err)? as u64,
            optional_recovery_bytes: row.get::<_, i64>(6).map_err(db_err)? as u64,
            optional_recovery_downloaded_bytes: row.get::<_, i64>(7).map_err(db_err)? as u64,
            failed_bytes: row.get::<_, i64>(8).map_err(db_err)? as u64,
            health: row.get(9).map_err(db_err)?,
            category: row.get(10).map_err(db_err)?,
            output_dir: row.get(11).map_err(db_err)?,
            nzb_path: row.get(12).map_err(db_err)?,
            created_at: row.get(13).map_err(db_err)?,
            completed_at: row.get(14).map_err(db_err)?,
            metadata: row.get(15).map_err(db_err)?,
        }))
    }

    pub fn list_job_history(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
        let conn = self.conn();

        let mut sql = String::from(
            "SELECT job_id, name, status, error_message, total_bytes, downloaded_bytes,
                    optional_recovery_bytes, optional_recovery_downloaded_bytes,
                    failed_bytes, health, category, output_dir, nzb_path,
                    created_at, completed_at, metadata
             FROM job_history WHERE 1=1",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref status) = filter.status {
            sql.push_str(&format!(" AND status = ?{}", params.len() + 1));
            params.push(Box::new(status.clone()));
        }
        if let Some(ref category) = filter.category {
            sql.push_str(&format!(" AND category = ?{}", params.len() + 1));
            params.push(Box::new(category.clone()));
        }

        sql.push_str(" ORDER BY completed_at DESC");

        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = filter.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(JobHistoryRow {
                    job_id: row.get::<_, i64>(0)? as u64,
                    name: row.get(1)?,
                    status: row.get(2)?,
                    error_message: row.get(3)?,
                    total_bytes: row.get::<_, i64>(4)? as u64,
                    downloaded_bytes: row.get::<_, i64>(5)? as u64,
                    optional_recovery_bytes: row.get::<_, i64>(6)? as u64,
                    optional_recovery_downloaded_bytes: row.get::<_, i64>(7)? as u64,
                    failed_bytes: row.get::<_, i64>(8)? as u64,
                    health: row.get(9)?,
                    category: row.get(10)?,
                    output_dir: row.get(11)?,
                    nzb_path: row.get(12)?,
                    created_at: row.get(13)?,
                    completed_at: row.get(14)?,
                    metadata: row.get(15)?,
                })
            })
            .map_err(db_err)?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(db_err)?);
        }
        Ok(entries)
    }

    pub fn list_integration_events_after(
        &self,
        after_id: Option<i64>,
        item_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Vec<IntegrationEventRow>, StateError> {
        let conn = self.conn();

        let mut sql = String::from(
            "SELECT id, timestamp, kind, item_id, payload_json
             FROM integration_events
             WHERE 1=1",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(after_id) = after_id {
            sql.push_str(&format!(" AND id > ?{}", params.len() + 1));
            params.push(Box::new(after_id));
        }

        if let Some(item_id) = item_id {
            sql.push_str(&format!(" AND item_id = ?{}", params.len() + 1));
            params.push(Box::new(item_id as i64));
        }

        sql.push_str(" ORDER BY id ASC");

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT ?{}", params.len() + 1));
            params.push(Box::new(limit as i64));
        }

        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|value| value.as_ref()).collect();

        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(IntegrationEventRow {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    kind: row.get(2)?,
                    item_id: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    payload_json: row.get(4)?,
                })
            })
            .map_err(db_err)?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError> {
        let conn = self.conn();
        conn.query_row("SELECT MAX(id) FROM integration_events", [], |row| {
            row.get(0)
        })
        .map_err(db_err)
    }

    pub fn max_job_id(&self) -> Result<u64, StateError> {
        let conn = self.conn();
        let max: Option<i64> = conn
            .query_row("SELECT MAX(job_id) FROM job_history", [], |row| row.get(0))
            .map_err(db_err)?;
        Ok(max.unwrap_or(0) as u64)
    }
}
