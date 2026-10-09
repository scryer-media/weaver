use async_graphql::parser::types::OperationType;
use async_graphql::{Context, Error, ErrorExtensions, Guard, Result};

use crate::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;

/// What every guard says to a running script: it may read what is guarded,
/// and change nothing. `Mutation.scriptRun`, which carries no guard, is the
/// one thing it may ask for.
fn script_run_verdict(ctx: &Context<'_>) -> Result<()> {
    if ctx.query_env.operation.node.ty == OperationType::Mutation {
        Err(graphql_error(
            "NOT_ALLOWED_FOR_SCRIPT_RUN",
            "a script run's token may read, and change nothing but through scriptRun",
        ))
    } else {
        Ok(())
    }
}

pub struct ReadGuard;

impl Guard for ReadGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let scope = ctx
            .data::<CallerScope>()
            .map_err(|_| internal_error("missing caller scope"))?;
        if *scope == CallerScope::ScriptRun {
            return script_run_verdict(ctx);
        }
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
        if *scope == CallerScope::ScriptRun {
            return script_run_verdict(ctx);
        }
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
        // A machine credential has no password to have typed recently. A
        // script run only gets this far on a query: the admin check above
        // refuses it any mutation.
        if matches!(
            identity,
            CallerIdentity::ApiKey(_) | CallerIdentity::ScriptRun(_)
        ) {
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
        if *scope == CallerScope::ScriptRun {
            return script_run_verdict(ctx);
        }
        if scope.can_control() {
            Ok(())
        } else {
            Err(graphql_error("FORBIDDEN", "control scope required"))
        }
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
