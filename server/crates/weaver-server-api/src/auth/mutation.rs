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
    /// Set the address Weaver listens on, applied at the next restart.
    ///
    /// Refused when `WEAVER_HTTP_BIND_ADDRESS` is set, rather than stored and
    /// silently ignored: an operator who pinned the address in their
    /// deployment should be told their edit would not take effect, not left to
    /// discover it after a restart. An empty value clears the setting back to
    /// the loopback default.
    #[graphql(guard = "AdminGuard")]
    async fn set_http_bind_address(&self, ctx: &Context<'_>, address: String) -> Result<bool> {
        use weaver_server_core::security::{
            BindAddressSource, RuntimeSecurityConfig, SETTING_HTTP_BIND_ADDRESS, ip_is_loopback,
            resolve_bind_address,
        };

        let security = ctx.data::<RuntimeSecurityConfig>()?;
        if matches!(security.bind_address_source, BindAddressSource::Environment) {
            return Err(async_graphql::Error::new(
                "WEAVER_HTTP_BIND_ADDRESS is set in this deployment's environment, \
                 which takes precedence; change it there instead",
            ));
        }

        let trimmed = address.trim().to_string();
        // Syntactic validation through the same resolver startup uses. Note
        // what this does NOT promise: bindability. A syntactically valid
        // address this host cannot bind (a moved DHCP lease, a downed VPN
        // interface) is only discoverable by binding — which is why startup
        // falls back to loopback with a banner instead of refusing to start,
        // and why this mutation may accept a value the next boot cannot use.
        let (parsed, _) = resolve_bind_address(None, Some(trimmed.as_str()))
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;

        // Refuse a combination strict security will refuse at the next boot,
        // rather than storing a time bomb: under WEAVER_STRICT_SECURITY=1 a
        // non-loopback bind without login makes startup fail.
        if security.strict_security && !trimmed.is_empty() && !ip_is_loopback(parsed) {
            let login_enabled = ctx
                .data::<crate::auth::LoginAuthCache>()?
                .snapshot()
                .is_some();
            if !login_enabled {
                return Err(async_graphql::Error::new(
                    "WEAVER_STRICT_SECURITY=1 refuses a non-loopback bind address while \
                     login is disabled; enable login first",
                ));
            }
        }

        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let stored = trimmed.clone();
        tokio::task::spawn_blocking(move || {
            if stored.is_empty() {
                db.delete_setting(SETTING_HTTP_BIND_ADDRESS)
            } else {
                db.set_setting(SETTING_HTTP_BIND_ADDRESS, &stored)
            }
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        // The one mutation whose effect is invisible until a restart is the
        // one that most needs a log line when someone later asks why the
        // instance started listening somewhere new.
        if trimmed.is_empty() {
            info!("http bind address setting cleared; next restart binds loopback");
        } else {
            info!(address = %trimmed, "http bind address setting changed; applies at next restart");
        }

        Ok(true)
    }

    /// Change the browser-admission policy: mode plus trusted networks.
    /// Applies immediately — trust is live state, unlike the bind address.
    #[graphql(guard = "AdminGuard")]
    async fn set_access_policy(
        &self,
        ctx: &Context<'_>,
        mode: String,
        trusted_networks: Option<Vec<String>>,
    ) -> Result<bool> {
        use weaver_server_core::security::{
            AccessMode, LOCAL_NETWORK_PRESETS, LOOPBACK_NETWORKS, RuntimeSecurityConfig,
            SETTING_ACCESS_MODE, SETTING_TRUSTED_NETWORKS,
        };

        let security = ctx.data::<RuntimeSecurityConfig>()?;
        if security.trust_env_pinned {
            return Err(async_graphql::Error::new(
                "WEAVER_TRUSTED_CIDRS is set in this deployment's environment, which takes \
                 precedence; change it there instead",
            ));
        }
        let Some(parsed_mode) = AccessMode::parse_setting_value(&mode) else {
            return Err(async_graphql::Error::new("unknown access mode"));
        };
        if security.strict_security && !matches!(parsed_mode, AccessMode::LoginRequired) {
            return Err(async_graphql::Error::new(
                "WEAVER_STRICT_SECURITY=1 refuses trusting access modes",
            ));
        }
        // No-login means no credentials; switching to it while a login exists
        // would strand a stored password that silently stops mattering. Force
        // the explicit order: disable login first, then loosen the policy.
        let login_enabled = ctx
            .data::<crate::auth::LoginAuthCache>()?
            .snapshot()
            .is_some();
        if matches!(parsed_mode, AccessMode::NoLogin) && login_enabled {
            return Err(async_graphql::Error::new(
                "disable login before switching to no-login mode",
            ));
        }

        let networks: Vec<String> = match (parsed_mode, trusted_networks) {
            (AccessMode::LoginExceptLocal, Some(entries)) => {
                if entries.iter().all(|entry| entry.trim().is_empty()) {
                    return Err(async_graphql::Error::new(
                        "trusted networks must not be empty for this access mode",
                    ));
                }
                entries
                    .iter()
                    .map(|entry| entry.trim().to_string())
                    .filter(|entry| !entry.is_empty())
                    .collect()
            }
            (AccessMode::LoginExceptLocal, None) => LOCAL_NETWORK_PRESETS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            (AccessMode::NoLogin, _) => LOOPBACK_NETWORKS.iter().map(|s| s.to_string()).collect(),
            (AccessMode::LoginRequired, _) => Vec::new(),
        };

        // Validate through the exact parser startup settles with — one JSON
        // round-trip, all-or-nothing, no second grammar to drift.
        let networks_json = serde_json::to_string(&networks)
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        let parsed_networks =
            weaver_server_core::security::parse_trusted_networks_json(&networks_json)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;

        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let mode_value = parsed_mode.as_setting_value().to_string();
        let json_for_store = networks_json.clone();
        let store_networks = matches!(parsed_mode, AccessMode::LoginExceptLocal);
        tokio::task::spawn_blocking(move || {
            db.set_setting(SETTING_ACCESS_MODE, &mode_value)?;
            if store_networks {
                db.set_setting(SETTING_TRUSTED_NETWORKS, &json_for_store)?;
            }
            Ok::<_, weaver_server_core::StateError>(())
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        security.set_trusted_cidrs(parsed_networks);
        info!(
            mode = parsed_mode.as_setting_value(),
            networks = ?networks,
            "access policy changed; effective immediately"
        );
        Ok(true)
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
        let jwt_secret = tokio::task::spawn_blocking(move || {
            db.set_auth_credentials(&username_for_db, &hash_for_db)?;
            db.rotate_jwt_signing_secret()
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            username, hash, jwt_secret,
        )));
        info!("login protection enabled");
        Ok(true)
    }
    /// Disable login protection.
    #[graphql(guard = "AdminGuard")]
    async fn disable_login(&self, ctx: &Context<'_>) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        tokio::task::spawn_blocking(move || {
            db.clear_auth_credentials()?;
            db.rotate_jwt_signing_secret()?;
            Ok::<_, weaver_server_core::StateError>(())
        })
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
        let jwt_secret = tokio::task::spawn_blocking(move || {
            db2.set_auth_credentials(&username, &new_hash_for_db)?;
            db2.rotate_jwt_signing_secret()
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            creds.username,
            new_hash,
            jwt_secret,
        )));
        info!("login password changed");
        Ok(true)
    }
}
