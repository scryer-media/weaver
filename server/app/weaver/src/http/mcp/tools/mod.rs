// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tool table. `list_operations` and `describe_operation` read the
// operation catalog in `super::catalog`; `run_query` and `run_mutation` run
// one of its operations through GraphQL with the key's scope, so the guards
// and refusals match `/graphql`. `whoami` reports the admitted key's scope.
//
// Each tool checks its own arguments; a key the tool does not take is
// refused without being echoed.

mod describe_operation;
mod diagnostics_section;
mod list_operations;
mod run_operation;
mod service_logs;
mod whoami;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};

use axum::http::HeaderMap;
use rmcp::ErrorData;
use rmcp::model::{CallToolResult, JsonObject, Tool, ToolAnnotations};
use serde_json::{Map, Value, json};
use weaver_server_api::WeaverSchema;
use weaver_server_api::auth::CallerIdentity;
use weaver_server_api::auth::types::NetworkRequestSecurityContext;

use super::McpCaller;
use super::catalog::Catalog;
use super::confirm::Confirmations;
use super::server::json_error;
use crate::http::diagnostics::DiagnosticsSources;
use crate::http::graphql::refusal;

// Serialized bytes a `run_query` or `run_mutation` result may hold.
pub(crate) const RESULT_MAX_BYTES: usize = 32 * 1024;

// What a tool runs with: the schema `/graphql` serves, the admitted caller
// and the request's headers, which GraphQL reads as `/graphql` would.
#[derive(Clone)]
pub(crate) struct ToolContext {
    pub schema: WeaverSchema,
    pub caller: McpCaller,
    pub headers: HeaderMap,
    pub catalog: &'static Catalog,
    // The cap on a run tool's serialized result, `RESULT_MAX_BYTES` outside
    // tests.
    pub result_max_bytes: usize,
    // Pending mutation confirmations, shared by every request.
    pub confirmations: Arc<Confirmations>,
    // What the diagnostics package reads.
    pub diagnostics: Arc<DiagnosticsSources>,
    // Raised when the client goes away; blocking work checks it.
    pub cancelled: Arc<AtomicBool>,
}

pub(super) type ToolFuture =
    Pin<Box<dyn Future<Output = Result<CallToolResult, ErrorData>> + Send + 'static>>;

pub(crate) struct ToolEntry {
    pub tool: Tool,
    // The argument names the tool takes; any other is refused.
    pub accepts: &'static [&'static str],
    pub run: fn(ToolContext, Arguments) -> ToolFuture,
}

// Every tool, in the order `tools/list` returns them.
pub(crate) static TOOLS: LazyLock<Vec<ToolEntry>> = LazyLock::new(|| {
    vec![
        whoami::entry(),
        list_operations::entry(),
        describe_operation::entry(),
        run_operation::query_entry(),
        run_operation::mutation_entry(),
        diagnostics_section::entry(),
        service_logs::entry(),
    ]
});

pub(crate) fn find(name: &str) -> Option<&'static ToolEntry> {
    TOOLS.iter().find(|entry| entry.tool.name == name)
}

// A tool with its input schema; no title, so `tools/list` stays small.
pub(super) fn tool(
    name: &'static str,
    description: &'static str,
    schema: Value,
    annotations: ToolAnnotations,
) -> Tool {
    let Value::Object(schema) = schema else {
        unreachable!("a tool's input schema is an object");
    };
    Tool::new(name, description, Arc::new(schema)).with_annotations(annotations)
}

// Annotations of a tool that reads only this instance.
pub(super) fn local_read() -> ToolAnnotations {
    ToolAnnotations::new().read_only(true).open_world(false)
}

// A tool call's arguments, checked against the names the tool takes.
#[derive(Debug, Default)]
pub(crate) struct Arguments(Map<String, Value>);

impl Arguments {
    pub(crate) fn new(arguments: Option<JsonObject>, accepts: &[&str]) -> Result<Self, ErrorData> {
        let mut arguments = arguments.unwrap_or_default();
        // A null argument is an absent one.
        arguments.retain(|_, value| !value.is_null());
        if arguments.keys().any(|key| !accepts.contains(&key.as_str())) {
            return Err(ErrorData::invalid_params(
                "an argument is not one this tool takes",
                Some(json!({"reason": refusal::MCP_ARGUMENTS_INVALID})),
            ));
        }
        Ok(Self(arguments))
    }

    pub(super) fn str(
        &self,
        name: &'static str,
        max_chars: usize,
    ) -> Result<Option<&str>, ErrorData> {
        match self.0.get(name) {
            None => Ok(None),
            Some(Value::String(text)) if text.chars().count() <= max_chars => Ok(Some(text)),
            Some(Value::String(_)) => Err(invalid_argument(name, "is too long")),
            Some(_) => Err(invalid_argument(name, "must be a string")),
        }
    }

    pub(super) fn required_str(
        &self,
        name: &'static str,
        max_chars: usize,
    ) -> Result<&str, ErrorData> {
        self.str(name, max_chars)?
            .ok_or_else(|| invalid_argument(name, "is required"))
    }

    pub(super) fn bool(&self, name: &'static str) -> Result<Option<bool>, ErrorData> {
        match self.0.get(name) {
            None => Ok(None),
            Some(Value::Bool(value)) => Ok(Some(*value)),
            Some(_) => Err(invalid_argument(name, "must be a boolean")),
        }
    }

    pub(super) fn int(
        &self,
        name: &'static str,
        min: i64,
        max: i64,
    ) -> Result<Option<i64>, ErrorData> {
        match self.0.get(name) {
            None => Ok(None),
            Some(value) => match value.as_i64() {
                Some(number) if (min..=max).contains(&number) => Ok(Some(number)),
                Some(_) => Err(invalid_argument(name, "is out of range")),
                None => Err(invalid_argument(name, "must be an integer")),
            },
        }
    }

    // A list of at most `max_items` strings, each at most `max_chars`.
    pub(super) fn str_list(
        &self,
        name: &'static str,
        max_items: usize,
        max_chars: usize,
    ) -> Result<Option<Vec<String>>, ErrorData> {
        let Some(value) = self.0.get(name) else {
            return Ok(None);
        };
        let Some(items) = value.as_array() else {
            return Err(invalid_argument(name, "must be a list of strings"));
        };
        if items.is_empty() || items.len() > max_items {
            return Err(invalid_argument(name, "has too few or too many items"));
        }
        items
            .iter()
            .map(|item| match item.as_str() {
                Some(text) if text.chars().count() <= max_chars => Ok(text.to_string()),
                Some(_) => Err(invalid_argument(name, "has an item that is too long")),
                None => Err(invalid_argument(name, "must be a list of strings")),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    // The named argument, taken out unchanged.
    pub(super) fn take(&mut self, name: &str) -> Option<Value> {
        self.0.remove(name)
    }
}

// A refused argument. The message names the declared argument, never the
// value sent.
pub(super) fn invalid_argument(argument: &'static str, problem: &str) -> ErrorData {
    ErrorData::invalid_params(
        format!("argument `{argument}` {problem}"),
        Some(json!({"reason": refusal::MCP_ARGUMENTS_INVALID, "argument": argument})),
    )
}

// Runs one GraphQL request as the caller, with the same request data
// `/graphql` gives an API-key caller.
pub(super) async fn execute(
    context: &ToolContext,
    request: async_graphql::Request,
) -> async_graphql::Response {
    let request = request
        .data(context.caller.scope)
        .data(CallerIdentity::ApiKey(context.caller.key))
        .data(NetworkRequestSecurityContext {
            peer: context.caller.peer,
            headers: context.headers.clone(),
        });
    context.schema.execute(request).await
}

// `value` as a result when its serialized text fits `max_bytes`; otherwise
// the coded error the run tools give, with both sizes, never a cut result.
pub(super) fn capped_result(value: &Value, max_bytes: usize) -> CallToolResult {
    let text = value.to_string();
    if text.len() <= max_bytes {
        return super::server::text_result(text, false);
    }
    let mut facts = Map::new();
    facts.insert("result_bytes".into(), json!(text.len()));
    facts.insert("max_bytes".into(), json!(max_bytes));
    coded_error_with(
        "PAYLOAD_TOO_LARGE",
        refusal::MCP_RESULT_TOO_LARGE,
        "The result is too large; narrow it.",
        facts,
    )
}

// The diagnostics tools' scope rule, as the package's: Admin only. A lower
// scope gets the refusal as a tool error.
pub(super) fn admit_diagnostics(context: &ToolContext) -> Option<CallToolResult> {
    (!context.caller.scope.is_admin()).then(|| {
        coded_error(
            "FORBIDDEN",
            refusal::MCP_ADMIN_REQUIRED,
            "Diagnostics need an API key with the ADMIN scope.",
        )
    })
}

// A tool error built here: a code, a reason, a short message and any facts
// in their own fields.
pub(super) fn coded_error(code: &str, reason: &str, message: &str) -> CallToolResult {
    coded_error_with(code, reason, message, Map::new())
}

pub(super) fn coded_error_with(
    code: &str,
    reason: &str,
    message: &str,
    facts: Map<String, Value>,
) -> CallToolResult {
    json_error(json!({"errors": [error_entry(code, reason, message, facts)]}))
}

// One entry of a tool error's `errors` list.
pub(super) fn error_entry(
    code: &str,
    reason: &str,
    message: &str,
    facts: Map<String, Value>,
) -> Value {
    let mut error = Map::new();
    error.insert("code".into(), json!(code));
    error.insert("reason".into(), json!(reason));
    error.insert("message".into(), json!(message));
    error.extend(facts);
    Value::Object(error)
}
