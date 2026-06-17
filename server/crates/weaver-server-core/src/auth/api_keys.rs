use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

/// A row in the `api_keys` table.
#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub id: i64,
    pub name: String,
    pub scope: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAuthRow {
    pub key_hash: [u8; 32],
    pub id: i64,
    pub scope: String,
}

impl Database {
    /// Insert a new API key. Returns the new row ID.
    pub fn insert_api_key(
        &self,
        name: &str,
        key_hash: &[u8; 32],
        scope: &str,
    ) -> Result<i64, StateError> {
        let datastore = self.datastore();
        let name = name.to_string();
        let key_hash = key_hash.to_vec();
        let scope = scope.to_string();
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "INSERT INTO api_keys (name, key_hash, scope, created_at)
                 VALUES ({}, {}, {}, {})
                 RETURNING id",
                &[
                    SqlArg::Text(name),
                    SqlArg::OptBytes(Some(key_hash)),
                    SqlArg::Text(scope),
                    SqlArg::I64(created_at),
                ],
            )
            .await?
            .ok_or_else(|| StateError::Database("api key insert returned no id".to_string()))?;
            row.i64("id")
        })
    }

    /// Look up an API key by its SHA-256 hash.
    pub fn lookup_api_key(&self, key_hash: &[u8; 32]) -> Result<Option<ApiKeyRow>, StateError> {
        let datastore = self.datastore();
        let key_hash = key_hash.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT id, name, scope, created_at, last_used_at
                   FROM api_keys WHERE key_hash = {}",
                &[SqlArg::OptBytes(Some(key_hash))],
            )
            .await?
            .map(api_key_row_from_sql)
            .transpose()
        })
    }

    /// List all API keys (without hashes).
    pub fn list_api_keys(&self) -> Result<Vec<ApiKeyRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, name, scope, created_at, last_used_at
                   FROM api_keys ORDER BY created_at DESC",
                &[],
            )
            .await?;
            rows.into_iter().map(api_key_row_from_sql).collect()
        })
    }

    pub fn list_api_key_auth_rows(&self) -> Result<Vec<ApiKeyAuthRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT key_hash, id, scope FROM api_keys",
                &[],
            )
            .await?;
            rows.into_iter()
                .map(|row| {
                    let key_hash_bytes = row.opt_bytes("key_hash")?.ok_or_else(|| {
                        StateError::Database("api key hash cannot be NULL".to_string())
                    })?;
                    let key_hash =
                        <[u8; 32]>::try_from(key_hash_bytes.as_slice()).map_err(|_| {
                            StateError::Database("api key hash must be 32 bytes".to_string())
                        })?;
                    Ok(ApiKeyAuthRow {
                        key_hash,
                        id: row.i64("id")?,
                        scope: row.text("scope")?,
                    })
                })
                .collect()
        })
    }

    /// Delete an API key by ID. Returns true if a row was deleted.
    pub fn delete_api_key(&self, id: i64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM api_keys WHERE id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    /// Update last_used_at timestamp for an API key.
    pub fn touch_api_key_last_used(&self, id: i64, now: i64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE api_keys SET last_used_at = {} WHERE id = {}",
                &[SqlArg::I64(now), SqlArg::I64(id)],
            )
            .await?;
            Ok(())
        })
    }
}

fn api_key_row_from_sql(
    row: crate::persistence::sql_runtime::SqlRow,
) -> Result<ApiKeyRow, StateError> {
    Ok(ApiKeyRow {
        id: row.i64("id")?,
        name: row.text("name")?,
        scope: row.text("scope")?,
        created_at: row.i64("created_at")?,
        last_used_at: row.opt_i64("last_used_at")?,
    })
}

#[cfg(test)]
mod tests;
