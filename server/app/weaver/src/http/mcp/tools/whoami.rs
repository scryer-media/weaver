// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `whoami`: the scope of the admitted API key. Weaver has no user accounts
// and GraphQL has no field that names the caller, so this reads the scope
// admission resolved rather than running a catalog operation.

use serde_json::json;
use weaver_server_core::auth::CallerScope;

use super::{Arguments, ToolContext, ToolEntry, ToolFuture, local_read, tool};
use crate::http::mcp::server::json_result;

pub(super) fn entry() -> ToolEntry {
    ToolEntry {
        tool: tool(
            "whoami",
            "The scope this API key acts with: READ, CONTROL or ADMIN.",
            json!({"type": "object", "properties": {}}),
            local_read(),
        ),
        accepts: &[],
        run,
    }
}

// The scope's name as the `ApiKey.scope` field spells it.
pub(crate) fn scope_name(scope: CallerScope) -> &'static str {
    match scope {
        CallerScope::Read => "READ",
        CallerScope::Control => "CONTROL",
        CallerScope::Admin => "ADMIN",
        CallerScope::Local => "LOCAL",
        CallerScope::ScriptRun => "SCRIPT_RUN",
    }
}

fn run(context: ToolContext, _arguments: Arguments) -> ToolFuture {
    Box::pin(async move {
        Ok(json_result(
            json!({"scope": scope_name(context.caller.scope)}),
        ))
    })
}
