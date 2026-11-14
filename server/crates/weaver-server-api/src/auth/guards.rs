// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::parser::types::OperationType;
use async_graphql::{Context, Error, ErrorExtensions, Guard, Result};

use crate::auth::CallerIdentity;
use weaver_server_core::auth::CallerScope;

// The only general queries exposed to run credentials: the two lists, and
// the details of the one job the run is about. The unguarded `scriptRun`
// field separately checks the live run and scopes its callbacks.
fn script_run_verdict(ctx: &Context<'_>) -> Result<()> {
    let allowed = ctx.query_env.operation.node.ty == OperationType::Query
        && match ctx.field().name() {
            "queueItems" | "historyItems" => true,
            "jobDetailSnapshot" | "jobTimeline" | "jobEvents" => asks_for_own_job(ctx),
            _ => false,
        };
    if allowed {
        Ok(())
    } else {
        Err(graphql_error(
            "NOT_ALLOWED_FOR_SCRIPT_RUN",
            "a script run's token may only read queueItems, historyItems, its own job's details and its own scriptRun",
        ))
    }
}

// Whether the field's `jobId` is the job the calling run is about. A run
// about no job, or one that has ended, has no job of its own.
fn asks_for_own_job(ctx: &Context<'_>) -> bool {
    let Some(CallerIdentity::ScriptRun(run_id)) = ctx.data_opt::<CallerIdentity>() else {
        return false;
    };
    let own = ctx
        .data_opt::<weaver_server_core::Database>()
        .and_then(|db| db.live_script_run(run_id))
        .and_then(|run| run.job_id);
    let asked = ctx.field().arguments().ok().and_then(|arguments| {
        arguments
            .into_iter()
            .find(|(name, _)| name.as_str() == "jobId")
            .and_then(|(_, value)| match value {
                async_graphql::Value::Number(number) => number.as_u64(),
                _ => None,
            })
    });
    own.is_some() && own == asked
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
        // A machine credential has no password to have typed recently.
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
