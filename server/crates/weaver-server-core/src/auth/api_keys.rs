use rusqlite::OptionalExtension;

use crate::StateError;

use crate::persistence::Database;

fn db_err(e: rusqlite::Error) -> StateError {
    StateError::Database(e.to_string())
}

/// A row in the `api_keys` table.
#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub id: i64,
    pub name: String,
    pub scope: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

impl Database {
    /// Insert a new API key. Returns the new row ID.
    pub fn insert_api_key(
        &self,
        name: &str,
        key_hash: &[u8; 32],
        scope: &str,
    ) -> Result<i64, StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO api_keys (name, key_hash, scope, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                name,
                key_hash.as_slice(),
                scope,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            ],
        )
        .map_err(db_err)?;
        Ok(conn.last_insert_rowid())
    }

    /// Look up an API key by its SHA-256 hash.
    pub fn lookup_api_key(&self, key_hash: &[u8; 32]) -> Result<Option<ApiKeyRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT id, name, scope, created_at, last_used_at
                 FROM api_keys WHERE key_hash = ?1",
            )
            .map_err(db_err)?;
        let row = stmt
            .query_row(rusqlite::params![key_hash.as_slice()], |row| {
                Ok(ApiKeyRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    scope: row.get(2)?,
                    created_at: row.get(3)?,
                    last_used_at: row.get(4)?,
                })
            })
            .optional()
            .map_err(db_err)?;
        Ok(row)
    }

    /// List all API keys (without hashes).
    pub fn list_api_keys(&self) -> Result<Vec<ApiKeyRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT id, name, scope, created_at, last_used_at
                 FROM api_keys ORDER BY created_at DESC",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ApiKeyRow {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    scope: row.get(2)?,
                    created_at: row.get(3)?,
                    last_used_at: row.get(4)?,
                })
            })
            .map_err(db_err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db_err)?;
        Ok(rows)
    }

    /// Delete an API key by ID. Returns true if a row was deleted.
    pub fn delete_api_key(&self, id: i64) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM api_keys WHERE id = ?1", [id])
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    /// Update last_used_at timestamp for an API key.
    pub fn touch_api_key_last_used(&self, id: i64, now: i64) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE api_keys SET last_used_at = ?1 WHERE id = ?2",
            rusqlite::params![now, id],
        )
        .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
