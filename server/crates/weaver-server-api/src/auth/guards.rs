use async_graphql::{Context, Error, ErrorExtensions, Guard, Result};

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

pub fn graphql_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, ext| {
        ext.set("code", code);
    })
}

pub fn internal_error(message: impl Into<String>) -> Error {
    graphql_error("INTERNAL", format!("internal: {}", message.into()))
}
