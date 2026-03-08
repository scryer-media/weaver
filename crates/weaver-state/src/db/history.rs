use crate::StateError;

use super::Database;

/// A row in the `job_history` table.
#[derive(Debug, Clone)]
pub struct JobHistoryRow {
    pub job_id: u64,
    pub name: String,
    pub status: String,
    pub error_message: Option<String>,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub health: u32,
    pub category: Option<String>,
    pub output_dir: Option<String>,
    pub nzb_path: Option<String>,
    pub created_at: i64,
    pub completed_at: i64,
    pub metadata: Option<String>,
}

/// Filter options for listing job history.
#[derive(Debug, Default)]
pub struct HistoryFilter {
    pub status: Option<String>,
    pub category: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Database {
    pub fn insert_job_history(&self, entry: &JobHistoryRow) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO job_history
             (job_id, name, status, error_message, total_bytes, downloaded_bytes,
              failed_bytes, health, category, output_dir, nzb_path,
              created_at, completed_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                entry.job_id as i64,
                entry.name,
                entry.status,
                entry.error_message,
                entry.total_bytes as i64,
                entry.downloaded_bytes as i64,
                entry.failed_bytes as i64,
                entry.health,
                entry.category,
                entry.output_dir,
                entry.nzb_path,
                entry.created_at,
                entry.completed_at,
                entry.metadata,
            ],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn list_job_history(
        &self,
        filter: &HistoryFilter,
    ) -> Result<Vec<JobHistoryRow>, StateError> {
        let conn = self.conn();

        let mut sql = String::from(
            "SELECT job_id, name, status, error_message, total_bytes, downloaded_bytes,
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

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| StateError::Database(e.to_string()))?;

        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(JobHistoryRow {
                    job_id: row.get::<_, i64>(0)? as u64,
                    name: row.get(1)?,
                    status: row.get(2)?,
                    error_message: row.get(3)?,
                    total_bytes: row.get::<_, i64>(4)? as u64,
                    downloaded_bytes: row.get::<_, i64>(5)? as u64,
                    failed_bytes: row.get::<_, i64>(6)? as u64,
                    health: row.get(7)?,
                    category: row.get(8)?,
                    output_dir: row.get(9)?,
                    nzb_path: row.get(10)?,
                    created_at: row.get(11)?,
                    completed_at: row.get(12)?,
                    metadata: row.get(13)?,
                })
            })
            .map_err(|e| StateError::Database(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| StateError::Database(e.to_string()))?);
        }
        Ok(entries)
    }

    pub fn delete_job_history(&self, job_id: u64) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "DELETE FROM job_history WHERE job_id = ?1",
                [job_id as i64],
            )
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(changed > 0)
    }

    /// Delete all job history entries. Returns the number of rows deleted.
    pub fn delete_all_job_history(&self) -> Result<usize, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM job_history", [])
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(changed)
    }

    /// Return the highest job_id in history, or 0 if empty.
    pub fn max_job_id(&self) -> Result<u64, StateError> {
        let conn = self.conn();
        let max: Option<i64> = conn
            .query_row("SELECT MAX(job_id) FROM job_history", [], |row| row.get(0))
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(max.unwrap_or(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_history() -> JobHistoryRow {
        JobHistoryRow {
            job_id: 1,
            name: "test.nzb".to_string(),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1_000_000,
            downloaded_bytes: 1_000_000,
            failed_bytes: 0,
            health: 1000,
            category: Some("movies".to_string()),
            output_dir: Some("/tmp/output".to_string()),
            nzb_path: Some("/tmp/test.nzb".to_string()),
            created_at: 1700000000,
            completed_at: 1700001000,
            metadata: None,
        }
    }

    #[test]
    fn insert_and_list() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&sample_history()).unwrap();

        let entries = db
            .list_job_history(&HistoryFilter::default())
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "test.nzb");
        assert_eq!(entries[0].total_bytes, 1_000_000);
    }

    #[test]
    fn filter_by_status() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&sample_history()).unwrap();

        let mut failed = sample_history();
        failed.job_id = 2;
        failed.status = "failed".to_string();
        failed.error_message = Some("missing volumes".to_string());
        db.insert_job_history(&failed).unwrap();

        let filter = HistoryFilter {
            status: Some("complete".to_string()),
            ..Default::default()
        };
        let entries = db.list_job_history(&filter).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].job_id, 1);
    }

    #[test]
    fn delete_history() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&sample_history()).unwrap();
        assert!(db.delete_job_history(1).unwrap());
        assert!(!db.delete_job_history(1).unwrap());
    }

    #[test]
    fn max_job_id_empty() {
        let db = Database::open_in_memory().unwrap();
        assert_eq!(db.max_job_id().unwrap(), 0);
    }

    #[test]
    fn max_job_id_with_entries() {
        let db = Database::open_in_memory().unwrap();
        let mut entry = sample_history();
        entry.job_id = 42;
        db.insert_job_history(&entry).unwrap();
        assert_eq!(db.max_job_id().unwrap(), 42);
    }

    #[test]
    fn list_with_limit_and_offset() {
        let db = Database::open_in_memory().unwrap();
        for i in 1..=5 {
            let mut entry = sample_history();
            entry.job_id = i;
            entry.completed_at = 1700000000 + i as i64;
            db.insert_job_history(&entry).unwrap();
        }

        let filter = HistoryFilter {
            limit: Some(2),
            offset: Some(1),
            ..Default::default()
        };
        let entries = db.list_job_history(&filter).unwrap();
        assert_eq!(entries.len(), 2);
        // Ordered by completed_at DESC, so job_ids: 5, 4, 3, 2, 1
        // offset=1, limit=2 → jobs 4, 3
        assert_eq!(entries[0].job_id, 4);
        assert_eq!(entries[1].job_id, 3);
    }
}
