use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

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
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT username, password_hash, created_at, updated_at FROM auth_credentials WHERE id = 1",
                &[],
            )
            .await?
            .map(|row| {
                Ok(AuthCredentials {
                    username: row.text("username")?,
                    password_hash: row.text("password_hash")?,
                    created_at: row.i64("created_at")?,
                    updated_at: row.i64("updated_at")?,
                })
            })
            .transpose()
        })
    }

    /// Set (insert or replace) login credentials.
    pub fn set_auth_credentials(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let username = username.to_string();
        let password_hash = password_hash.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO auth_credentials (id, username, password_hash, created_at, updated_at)
                 VALUES (1, {}, {}, {}, {})
                 ON CONFLICT(id) DO UPDATE SET username = excluded.username, password_hash = excluded.password_hash, updated_at = excluded.updated_at",
                &[
                    SqlArg::Text(username),
                    SqlArg::Text(password_hash),
                    SqlArg::I64(now),
                    SqlArg::I64(now),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Clear login credentials (disable login).
    pub fn clear_auth_credentials(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM auth_credentials WHERE id = 1",
                &[],
            )
            .await?;
            Ok(())
        })
    }
}
