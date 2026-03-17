use rusqlite::OptionalExtension;

use crate::StateError;

use super::Database;

fn db_err(e: rusqlite::Error) -> StateError {
    StateError::Database(e.to_string())
}

/// Stored login credentials (single user).
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    pub username: String,
    pub password_hash: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Database {
    /// Get the stored login credentials, if any.
    pub fn get_auth_credentials(&self) -> Result<Option<AuthCredentials>, StateError> {
        let conn = self.conn();
        conn.query_row(
            "SELECT username, password_hash, created_at, updated_at FROM auth_credentials WHERE id = 1",
            [],
            |row| {
                Ok(AuthCredentials {
                    username: row.get(0)?,
                    password_hash: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(db_err)
    }

    /// Set (insert or replace) login credentials.
    pub fn set_auth_credentials(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        conn.execute(
            "INSERT INTO auth_credentials (id, username, password_hash, created_at, updated_at)
             VALUES (1, ?1, ?2, ?3, ?3)
             ON CONFLICT(id) DO UPDATE SET username = ?1, password_hash = ?2, updated_at = ?3",
            rusqlite::params![username, password_hash, now],
        )
        .map_err(db_err)?;
        Ok(())
    }

    /// Clear login credentials (disable login).
    pub fn clear_auth_credentials(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM auth_credentials WHERE id = 1", [])
            .map_err(db_err)?;
        Ok(())
    }
}
