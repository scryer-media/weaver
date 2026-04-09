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
