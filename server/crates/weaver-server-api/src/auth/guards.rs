use async_graphql::{Context, Error, ErrorExtensions, Guard, Result};

use crate::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;

pub struct ReadGuard;

impl Guard for ReadGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.can_read() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "read scope required"))
        }
    }
}

pub struct AdminGuard;

impl Guard for AdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.is_admin() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "admin scope required"))
        }
    }
}

pub struct ControlGuard;

pub struct FreshAdminGuard;

impl Guard for FreshAdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        AdminGuard.check(ctx).await?;
        let security = ctx
            .data::<weaver_server_core::security::RuntimeSecurityConfig>()
            .map_err(|_| internal_error("missing runtime security"))?;
        if !security.authenticated_access_mode() {
            return Ok(());
        }
        let identity = ctx
            .data::<CallerIdentity>()
            .map_err(|_| internal_error("missing caller identity"))?;
        if matches!(identity, CallerIdentity::ApiKey(_)) {
            return Ok(());
        }
        let CallerIdentity::Jwt(hash) = identity else {
            return Err(graphql_error(
                "REAUTH_REQUIRED",
                "recent password verification required",
            ));
        };
        let token_hash: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
        let db = ctx
            .data::<weaver_server_core::Database>()
            .map_err(|_| internal_error("missing database"))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let verified_at = db
            .browser_session_password_verified_at(&token_hash, now)
            .map_err(|error| internal_error(error.to_string()))?;
        if verified_at.is_none_or(|then| then > now || now - then >= 15 * 60) {
            return Err(graphql_error(
                "REAUTH_REQUIRED",
                "recent password verification required",
            ));
        }
        Ok(())
    }
}

impl Guard for ControlGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if scope.can_control() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "control scope required"))
        }
    }
}

/// Require an administrator only for mutations that also remove completed
/// output. History-only removal remains available to control callers.
pub fn require_admin_for_file_delete(ctx: &Context<'_>, delete_files: bool) -> Result<()> {
    if !delete_files {
        return Ok(());
    }

    let scope = ctx
        .data::<CallerScope>()
        .map_err(|_| internal_error("missing caller scope"))?;
    if scope.is_admin() {
        Ok(())
    } else {
        Err(graphql_error(
            "FORBIDDEN",
            "admin scope required to delete completed files",
        ))
    }
}

pub fn graphql_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, ext| {
        ext.set("code", code);
    })
}

pub fn internal_error(message: impl Into<String>) -> Error {
    graphql_error("INTERNAL", format!("internal: {}", message.into()))
}
