use async_graphql::{Context, Object, Result};
use tracing::info;

use crate::auth::types::{ApiKey, ApiKeyScope, CreateApiKeyResult};
use crate::auth::{AdminGuard, generate_api_key, hash_api_key};
use weaver_server_core::Database;
use weaver_server_core::auth::{ApiKeyAuthRow, ApiKeyCache};

#[derive(Default)]
pub(crate) struct AuthMutation;

#[Object]
impl AuthMutation {
    /// Create a new API key. Returns the raw key (shown only once).
    #[graphql(guard = "AdminGuard")]
    async fn create_api_key(
        &self,
        ctx: &Context<'_>,
        name: String,
        scope: ApiKeyScope,
    ) -> Result<CreateApiKeyResult> {
        let db = ctx.data::<Database>()?;
        let raw_key = generate_api_key();
        let key_hash = hash_api_key(&raw_key);
        let scope_str = match scope {
            ApiKeyScope::Read => "read",
            ApiKeyScope::Control => "control",
            ApiKeyScope::Admin => "admin",
        };
        let db = db.clone();
        let name_clone = name.clone();
        let id = tokio::task::spawn_blocking(move || {
            db.insert_api_key(&name_clone, &key_hash, scope_str)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        ctx.data::<ApiKeyCache>()?.upsert(ApiKeyAuthRow {
            key_hash,
            id,
            scope: scope_str.to_string(),
        });

        info!(id, name = %name, scope = scope_str, "API key created");

        Ok(CreateApiKeyResult {
            key: ApiKey {
                id,
                name,
                scope,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64,
                last_used_at: None,
            },
            raw_key,
        })
    }
    /// Delete an API key by ID.
    #[graphql(guard = "AdminGuard")]
    async fn delete_api_key(&self, ctx: &Context<'_>, id: i64) -> Result<Vec<ApiKey>> {
        let db = ctx.data::<Database>()?;
        let db = db.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_api_key(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        if deleted {
            ctx.data::<ApiKeyCache>()?.remove_by_id(id);
            info!(id, "API key deleted");
        }
        let db = ctx.data::<Database>()?.clone();
        let rows = tokio::task::spawn_blocking(move || db.list_api_keys())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|row| ApiKey {
                id: row.id,
                name: row.name,
                scope: match row.scope.as_str() {
                    "read" => ApiKeyScope::Read,
                    "control" | "integration" => ApiKeyScope::Control,
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Control,
                },
                created_at: row.created_at as f64 * 1000.0,
                last_used_at: row.last_used_at.map(|value| value as f64 * 1000.0),
            })
            .collect())
    }
    /// Enable login protection with a username and password.
    #[graphql(guard = "AdminGuard")]
    async fn enable_login(
        &self,
        ctx: &Context<'_>,
        username: String,
        password: String,
    ) -> Result<bool> {
        if username.is_empty() || password.is_empty() {
            return Err(async_graphql::Error::new(
                "username and password must not be empty",
            ));
        }
        let hash = tokio::task::spawn_blocking(move || crate::auth::hash_password(&password))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(async_graphql::Error::new)?;
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        let username_for_db = username.clone();
        let hash_for_db = hash.clone();
        tokio::task::spawn_blocking(move || {
            db.set_auth_credentials(&username_for_db, &hash_for_db)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(username, hash)));
        info!("login protection enabled");
        Ok(true)
    }
    /// Disable login protection.
    #[graphql(guard = "AdminGuard")]
    async fn disable_login(&self, ctx: &Context<'_>) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        tokio::task::spawn_blocking(move || db.clear_auth_credentials())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.clear();
        info!("login protection disabled");
        Ok(true)
    }
    /// Change the login password. Requires the current password for verification.
    #[graphql(guard = "AdminGuard")]
    async fn change_password(
        &self,
        ctx: &Context<'_>,
        current_password: String,
        new_password: String,
    ) -> Result<bool> {
        if new_password.is_empty() {
            return Err(async_graphql::Error::new("new password must not be empty"));
        }
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        let db2 = db.clone();
        let creds = auth_cache
            .snapshot()
            .ok_or_else(|| async_graphql::Error::new("login is not enabled"))?;

        let hash = creds.password_hash.clone();
        let current = current_password.clone();
        let valid =
            tokio::task::spawn_blocking(move || crate::auth::verify_password(&current, &hash))
                .await
                .unwrap_or(false);
        if !valid {
            return Err(async_graphql::Error::new("current password is incorrect"));
        }

        let new_hash =
            tokio::task::spawn_blocking(move || crate::auth::hash_password(&new_password))
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(async_graphql::Error::new)?;
        let username = creds.username.clone();
        let new_hash_for_db = new_hash.clone();
        tokio::task::spawn_blocking(move || db2.set_auth_credentials(&username, &new_hash_for_db))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            creds.username,
            new_hash,
        )));
        info!("login password changed");
        Ok(true)
    }
}
