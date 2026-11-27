// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use tracing::info;
use weaver_server_core::auth::api_keys::AuthApiKeysQueries;
use weaver_server_core::auth::repository::AuthRepositoryQueries;
use weaver_server_core::settings::persistence::SettingsPersistenceQueries;

use crate::auth::types::{ApiKey, ApiKeyScope, CreateApiKeyResult};
use crate::auth::{FreshAdminGuard, generate_api_key, graphql_error, hash_api_key, invalid_input};
use crate::observability::{spawn_blocking_with, state_error};
use weaver_server_core::Database;
use weaver_server_core::auth::{ApiKeyAuthRow, ApiKeyCache, SigningKeys};

#[derive(Default)]
pub(crate) struct AuthMutation;

#[Object]
impl AuthMutation {
    /// Create a new API key. Returns the raw key (shown only once).
    #[graphql(guard = "FreshAdminGuard")]
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
        let id = spawn_blocking_with(
            move || db.insert_api_key(&name_clone, &key_hash, scope_str),
            state_error,
        )
        .await?;
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
    #[graphql(guard = "FreshAdminGuard")]
    async fn delete_api_key(&self, ctx: &Context<'_>, id: i64) -> Result<Vec<ApiKey>> {
        let db = ctx.data::<Database>()?;
        let db = db.clone();
        let deleted = spawn_blocking_with(move || db.delete_api_key(id), state_error).await?;

        if deleted {
            ctx.data::<ApiKeyCache>()?.remove_by_id(id);
            info!(id, "API key deleted");
        }
        let db = ctx.data::<Database>()?.clone();
        let rows = spawn_blocking_with(move || db.list_api_keys(), state_error).await?;
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
    #[graphql(guard = "FreshAdminGuard")]
    async fn set_http_bind_address(&self, ctx: &Context<'_>, address: String) -> Result<bool> {
        use weaver_server_core::security::{
            BindAddressSource, RuntimeSecurityConfig, SETTING_HTTP_BIND_ADDRESS, ip_is_loopback,
            resolve_bind_address,
        };

        let security = ctx.data::<RuntimeSecurityConfig>()?;
        if matches!(security.bind_address_source, BindAddressSource::Environment) {
            return Err(graphql_error(
                "ENV_PINNED",
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
            .map_err(|error| graphql_error("INVALID_BIND_ADDRESS", error.to_string()))?;

        // Refuse a combination strict security will refuse at the next boot,
        // rather than storing a time bomb: under WEAVER_STRICT_SECURITY=1 a
        // non-loopback bind without login makes startup fail.
        if security.strict_security && !trimmed.is_empty() && !ip_is_loopback(parsed) {
            let login_enabled = ctx
                .data::<crate::auth::LoginAuthCache>()?
                .snapshot()
                .is_some();
            if !login_enabled {
                return Err(graphql_error(
                    "STRICT_SECURITY",
                    "WEAVER_STRICT_SECURITY=1 refuses a non-loopback bind address while \
                     login is disabled; enable login first",
                ));
            }
        }

        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let stored = trimmed.clone();
        spawn_blocking_with(
            move || {
                if stored.is_empty() {
                    db.delete_setting(SETTING_HTTP_BIND_ADDRESS)
                } else {
                    db.set_setting(SETTING_HTTP_BIND_ADDRESS, &stored)
                }
            },
            state_error,
        )
        .await?;

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

    /// Turn the MCP endpoint on or off. Applies to the next request.
    ///
    /// Refused when `WEAVER_MCP_ENABLED` is set, rather than stored and
    /// silently overridden, as the bind address is.
    #[graphql(guard = "FreshAdminGuard")]
    async fn set_mcp_enabled(
        &self,
        ctx: &Context<'_>,
        enabled: bool,
    ) -> Result<crate::auth::types::McpServerStatus> {
        use weaver_server_core::security::{
            ENV_MCP_ENABLED, RuntimeSecurityConfig, SETTING_MCP_ENABLED,
        };

        let security = ctx.data::<RuntimeSecurityConfig>()?;
        if security.mcp_enabled_env.is_some() {
            return Err(graphql_error(
                "ENV_PINNED",
                format!(
                    "{ENV_MCP_ENABLED} is set in this deployment's environment, which takes \
                 precedence; change it there instead"
                ),
            ));
        }
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(
            move || db.set_setting(SETTING_MCP_ENABLED, if enabled { "true" } else { "false" }),
            state_error,
        )
        .await?;
        // Published after the write commits, so a failed write leaves the
        // endpoint as it was.
        security.set_stored_mcp_enabled(enabled);
        info!(enabled, "MCP endpoint switch changed");
        Ok(crate::auth::types::McpServerStatus::of(security))
    }

    /// Atomically update the authenticated browser policy and optional next
    /// listener address. Legacy access-policy mutations remain available for
    /// installations that have not migrated to authenticated browser access.
    #[graphql(guard = "FreshAdminGuard")]
    async fn update_network_access(
        &self,
        ctx: &Context<'_>,
        input: crate::auth::types::NetworkAccessInput,
    ) -> Result<crate::auth::types::NetworkAccessStatus> {
        use weaver_server_core::settings::persistence::{
            AuthenticatedNetworkAccessUpdate, NetworkBindAddressUpdate,
        };

        let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
        if !security.authenticated_access_mode() {
            return Err(crate::auth::graphql_error(
                "LEGACY_NETWORK_ACCESS",
                "updateNetworkAccess requires authenticated browser access; use the legacy access-policy settings until migration is enabled",
            ));
        }
        let update_guard = security
            .network_policy_update_lock
            .clone()
            .lock_owned()
            .await;
        if !security.remembered_policy_valid() && input.trusted_proxies.is_none() {
            return Err(crate::auth::graphql_error(
                "INVALID_NETWORK_POLICY",
                "Review and submit both trusted networks and trusted proxies to repair the stored policy",
            ));
        }
        let bind_status = crate::schema::auth_query::http_bind_address_status(ctx).await?;
        let request = ctx.data_opt::<crate::auth::types::NetworkRequestSecurityContext>();
        // Validate both values before the transaction begins. A malformed bind
        // cannot leave a newly saved trusted-network list behind.
        let preview =
            crate::auth::types::preview_network_access(&input, security, &bind_status, request)?;
        let trusted_networks_json = (!security.trust_env_pinned)
            .then(|| serde_json::to_string(&preview.trusted_networks))
            .transpose()
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        let trusted_proxies_json = (!security.proxies_env_pinned()
            && input.trusted_proxies.is_some())
        .then(|| serde_json::to_string(&preview.trusted_proxies))
        .transpose()
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        let bind_address = if bind_status.editable {
            match input.bind_address.as_deref() {
                None => NetworkBindAddressUpdate::Unchanged,
                Some(value) if value.trim().is_empty() => NetworkBindAddressUpdate::Clear,
                Some(_) => NetworkBindAddressUpdate::Set(
                    preview
                        .bind_address
                        .clone()
                        .expect("validated non-empty bind address"),
                ),
            }
        } else {
            NetworkBindAddressUpdate::Unchanged
        };
        let parsed_networks = preview
            .trusted_networks
            .iter()
            .filter_map(|entry| weaver_server_core::security::parse_ip_or_cidr(entry))
            .collect();
        let parsed_proxies = preview
            .trusted_proxies
            .iter()
            .filter_map(|entry| weaver_server_core::security::parse_ip_or_cidr(entry))
            .collect();
        let db = ctx.data::<Database>()?.clone();
        let live_security = security.clone();
        spawn_blocking_with(
            move || {
                let _update_guard = update_guard;
                db.update_authenticated_network_access(&AuthenticatedNetworkAccessUpdate {
                    trusted_proxies_json,
                    trusted_networks_json,
                    bind_address,
                })?;
                // Keep commit and publication together even if the HTTP request is cancelled.
                live_security.set_authenticated_network_policy(parsed_networks, parsed_proxies);
                live_security.mark_security_configured();
                Ok::<_, weaver_server_core::StateError>(())
            },
            state_error,
        )
        .await?;

        // Publish after commit only. Failed writes leave every router clone
        // on the prior policy and make a retry safe.
        crate::schema::auth_query::network_access_status(ctx).await
    }

    /// Change the browser-admission policy: mode plus trusted networks.
    /// Applies immediately — trust is live state, unlike the bind address.
    #[graphql(guard = "FreshAdminGuard")]
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
        if security.authenticated_access_mode() {
            return Err(crate::auth::graphql_error(
                "AUTHENTICATED_NETWORK_ACCESS",
                "authenticated browser access does not support legacy access modes; use updateNetworkAccess to restrict remembered browser sessions",
            ));
        }
        if security.trust_env_pinned {
            return Err(graphql_error(
                "ENV_PINNED",
                "WEAVER_TRUSTED_CIDRS is set in this deployment's environment, which takes \
                 precedence; change it there instead",
            ));
        }
        let Some(parsed_mode) = AccessMode::parse_setting_value(&mode) else {
            return Err(invalid_input("unknown access mode"));
        };
        if security.strict_security && !matches!(parsed_mode, AccessMode::LoginRequired) {
            return Err(graphql_error(
                "STRICT_SECURITY",
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
            return Err(graphql_error(
                "CONFLICT",
                "disable login before switching to no-login mode",
            ));
        }

        let networks: Vec<String> = match (parsed_mode, trusted_networks) {
            (AccessMode::LoginExceptLocal, Some(entries)) => {
                if entries.iter().all(|entry| entry.trim().is_empty()) {
                    return Err(invalid_input(
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
            .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        let parsed_networks =
            weaver_server_core::security::parse_trusted_networks_json(&networks_json)
                .map_err(|error| invalid_input(error.to_string()))?;

        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let mode_value = parsed_mode.as_setting_value().to_string();
        let json_for_store = networks_json.clone();
        let store_networks = matches!(parsed_mode, AccessMode::LoginExceptLocal);
        spawn_blocking_with(
            move || {
                db.set_setting(SETTING_ACCESS_MODE, &mode_value)?;
                if store_networks {
                    db.set_setting(SETTING_TRUSTED_NETWORKS, &json_for_store)?;
                }
                Ok::<_, weaver_server_core::StateError>(())
            },
            state_error,
        )
        .await?;

        security.set_trusted_cidrs(parsed_networks);
        // The stored mode is now a decision, not a default: an operator who
        // picks no-login here must not have the first-run wizard served back to
        // every browser outside the machine on the next page load.
        security.mark_security_configured();
        info!(
            mode = parsed_mode.as_setting_value(),
            networks = ?networks,
            "access policy changed; effective immediately"
        );
        Ok(true)
    }

    /// Enable login protection with a username and password.
    #[graphql(guard = "FreshAdminGuard")]
    async fn enable_login(
        &self,
        ctx: &Context<'_>,
        username: String,
        password: String,
    ) -> Result<bool> {
        if ctx
            .data::<weaver_server_core::security::RuntimeSecurityConfig>()?
            .authenticated_access_mode()
        {
            return Err(graphql_error(
                "CONFLICT",
                "login is already required; use changePassword to change credentials",
            ));
        }
        if username.is_empty() || password.is_empty() {
            return Err(invalid_input("username and password must not be empty"));
        }
        crate::auth::check_password_length(&password).map_err(invalid_input)?;
        let permit = weaver_server_core::auth::service::password_work_permit()
            .map_err(|message| crate::auth::guards::graphql_error("AUTH_BUSY", message))?;
        // Hashing fails only inside Weaver: a salt or argon2 fault.
        let hash = spawn_blocking_with(
            move || {
                let _permit = permit;
                crate::auth::hash_password(&password)
            },
            |message| graphql_error("INTERNAL", message),
        )
        .await?;
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        // The new hash is a new signing key: nothing is stored for it.
        let signing_keys = SigningKeys::for_database(&db)
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        let username_for_db = username.clone();
        let hash_for_db = hash.clone();
        spawn_blocking_with(
            move || db.set_auth_credentials(&username_for_db, &hash_for_db),
            state_error,
        )
        .await?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            username,
            hash,
            &signing_keys,
        )));
        info!("login protection enabled");
        Ok(true)
    }
    /// Disable login protection.
    #[graphql(guard = "FreshAdminGuard")]
    async fn disable_login(&self, ctx: &Context<'_>) -> Result<bool> {
        if ctx
            .data::<weaver_server_core::security::RuntimeSecurityConfig>()?
            .authenticated_access_mode()
        {
            return Err(graphql_error(
                "CONFLICT",
                "authenticated browser access cannot be downgraded by disabling login",
            ));
        }
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        spawn_blocking_with(move || db.clear_auth_credentials(), state_error).await?;
        auth_cache.clear();
        info!("login protection disabled");
        Ok(true)
    }
    /// Change the login password. Requires the current password for verification.
    #[graphql(guard = "FreshAdminGuard")]
    async fn change_password(
        &self,
        ctx: &Context<'_>,
        current_password: String,
        new_password: String,
    ) -> Result<bool> {
        crate::auth::check_password_length(&new_password).map_err(invalid_input)?;
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        let db2 = db.clone();
        let creds = auth_cache
            .snapshot()
            .ok_or_else(|| graphql_error("LOGIN_NOT_ENABLED", "login is not enabled"))?;

        let hash = creds.password_hash.clone();
        let permit = weaver_server_core::auth::service::password_work_permit()
            .map_err(|message| crate::auth::guards::graphql_error("AUTH_BUSY", message))?;
        // A wrong current password is the caller's; a hash that failed is a
        // fault inside Weaver.
        let new_hash = spawn_blocking_with(
            move || {
                let _permit = permit;
                if !crate::auth::verify_password(&current_password, &hash) {
                    return Err(graphql_error(
                        "INVALID_PASSWORD",
                        "current password is incorrect",
                    ));
                }
                crate::auth::hash_password(&new_password)
                    .map_err(|message| graphql_error("INTERNAL", message))
            },
            |error| error,
        )
        .await?;
        // The new hash is a new signing key, so every token signed under the
        // old password stops verifying once the cache holds it.
        let signing_keys = SigningKeys::for_database(&db)
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        let username = creds.username.clone();
        let new_hash_for_db = new_hash.clone();
        spawn_blocking_with(
            move || {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                db2.change_auth_credentials_and_revoke_sessions(&username, &new_hash_for_db, now)
            },
            state_error,
        )
        .await?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            creds.username,
            new_hash,
            &signing_keys,
        )));
        info!("login password changed");
        Ok(true)
    }

    /// Revoke every durable browser session after a recent password check.
    #[graphql(guard = "FreshAdminGuard")]
    async fn sign_out_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        spawn_blocking_with(move || db.revoke_all_browser_sessions(now), state_error).await?;
        Ok(true)
    }
}
