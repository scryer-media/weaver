use super::*;

#[derive(Default)]
pub(crate) struct AuthQuery;

#[Object]
impl AuthQuery {
    /// Admin-facing auth status moved to query semantics.
    #[graphql(guard = "AdminGuard")]
    async fn admin_login_status(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::auth::types::LoginStatusResult> {
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        Ok(match auth_cache.snapshot() {
            Some(creds) => crate::auth::types::LoginStatusResult {
                enabled: true,
                username: Some(creds.username),
            },
            None => crate::auth::types::LoginStatusResult {
                enabled: false,
                username: None,
            },
        })
    }
    /// The address Weaver is listening on, and whether it can be changed here.
    ///
    /// Exposed so the binding can be widened from the UI. That matters because
    /// the loopback default is always reachable from the machine Weaver runs
    /// on, which makes this the one place an operator can reliably get to —
    /// unlike a service unit, a shortcut's environment, or a file under
    /// Program Files.
    #[graphql(guard = "AdminGuard")]
    async fn http_bind_address(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::auth::types::HttpBindAddressStatus> {
        http_bind_address_status(ctx).await
    }

    /// The browser-admission policy: who gets in without a login.
    #[graphql(guard = "AdminGuard")]
    async fn access_policy(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::auth::types::AccessPolicyStatus> {
        use weaver_server_core::security::{AccessMode, SETTING_ACCESS_MODE};

        let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        let stored_mode = tokio::task::spawn_blocking(move || db.get_setting(SETTING_ACCESS_MODE))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        // Read before the default is applied: an install upgraded from a
        // version without this setting must be distinguishable from one whose
        // operator chose login-required, or the wizard could never know it has
        // something to ask.
        let configured = security.authenticated_access_mode()
            || stored_mode
                .as_deref()
                .and_then(AccessMode::parse_setting_value)
                .is_some();
        let mode = stored_mode.unwrap_or_else(|| "login_required".to_string());

        Ok(crate::auth::types::AccessPolicyStatus {
            mode: if security.authenticated_access_mode() {
                "login_required".to_string()
            } else if security.trust_env_pinned {
                "env".to_string()
            } else {
                mode
            },
            trusted_networks: security
                .trusted_cidrs()
                .iter()
                .map(|network| network.to_string())
                .collect(),
            editable: !security.trust_env_pinned && !security.authenticated_access_mode(),
            env_pinned: security.trust_env_pinned,
            configured,
            strict_security: security.strict_security,
        })
    }

    /// The effective browser network policy and a non-persistent draft
    /// validator for the accompanying network-access mutation.
    #[graphql(guard = "AdminGuard")]
    async fn network_access(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::auth::types::NetworkAccessStatus> {
        network_access_status(ctx).await
    }

    /// Validate and canonicalize a network draft without changing live policy
    /// or stored settings. The update mutation consumes this same input shape.
    #[graphql(guard = "AdminGuard")]
    async fn preview_network_access(
        &self,
        ctx: &Context<'_>,
        input: crate::auth::types::NetworkAccessInput,
    ) -> Result<crate::auth::types::NetworkAccessPreview> {
        let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
        let bind_address = http_bind_address_status(ctx).await?;
        let request = ctx.data_opt::<crate::auth::types::NetworkRequestSecurityContext>();
        crate::auth::types::preview_network_access(&input, security, &bind_address, request)
    }

    /// List all API keys (without raw key values).
    #[graphql(guard = "AdminGuard")]
    async fn api_keys(&self, ctx: &Context<'_>) -> Result<Vec<ApiKey>> {
        let db = ctx.data::<weaver_server_core::Database>()?;
        let db = db.clone();
        let rows = tokio::task::spawn_blocking(move || db.list_api_keys())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|r| ApiKey {
                id: r.id,
                name: r.name,
                scope: match r.scope.as_str() {
                    "read" => ApiKeyScope::Read,
                    "control" | "integration" => ApiKeyScope::Control,
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Control,
                },
                created_at: r.created_at as f64 * 1000.0,
                last_used_at: r.last_used_at.map(|t| t as f64 * 1000.0),
            })
            .collect())
    }
}

pub(crate) async fn http_bind_address_status(
    ctx: &Context<'_>,
) -> Result<crate::auth::types::HttpBindAddressStatus> {
    use weaver_server_core::security::SETTING_HTTP_BIND_ADDRESS;

    let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
    let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
    let db = ctx.data::<weaver_server_core::Database>()?.clone();
    let stored = tokio::task::spawn_blocking(move || db.get_setting(SETTING_HTTP_BIND_ADDRESS))
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let editable = security.bind_address_source.is_editable();
    let (pending, restart_required) = crate::auth::types::pending_bind_state(
        security.http_bind_address,
        stored.as_deref(),
        editable,
    );
    Ok(crate::auth::types::HttpBindAddressStatus {
        address: security.http_bind_address.to_string(),
        stored_address: stored,
        source: security.bind_address_source.into(),
        editable,
        exposed_without_login: crate::auth::types::exposed_without_login(
            auth_cache.snapshot().is_some(),
            pending,
        ),
        restart_required,
        bind_fallback: security.bind_fallback.clone(),
    })
}

pub(crate) async fn network_access_status(
    ctx: &Context<'_>,
) -> Result<crate::auth::types::NetworkAccessStatus> {
    let security = ctx.data::<weaver_server_core::security::RuntimeSecurityConfig>()?;
    let bind_address = http_bind_address_status(ctx).await?;
    let request = ctx.data_opt::<crate::auth::types::NetworkRequestSecurityContext>();
    Ok(crate::auth::types::NetworkAccessStatus {
        proxies_editable: security.authenticated_access_mode() && !security.proxies_env_pinned(),
        proxies_env_pinned: security.proxies_env_pinned(),
        proxies_source: if security.proxies_env_pinned() {
            crate::auth::types::NetworkAccessSource::Environment
        } else {
            crate::auth::types::NetworkAccessSource::Stored
        },
        authenticated_access: security.authenticated_access_mode(),
        legacy_compatibility: !security.authenticated_access_mode(),
        trusted_networks: security
            .trusted_cidrs()
            .iter()
            .map(ToString::to_string)
            .collect(),
        trusted_proxies: security
            .trusted_proxies()
            .iter()
            .map(ToString::to_string)
            .collect(),
        trusted_networks_source: if security.trust_env_pinned {
            crate::auth::types::NetworkAccessSource::Environment
        } else {
            crate::auth::types::NetworkAccessSource::Stored
        },
        editable: !security.trust_env_pinned,
        env_pinned: security.trust_env_pinned,
        remembered_policy_valid: security.remembered_policy_valid(),
        current_client: crate::auth::types::network_client_diagnostics(security, request),
        bind_address,
    })
}
