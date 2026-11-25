// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `list_operations`: the catalog as compact rows, grouped by skill.

use serde_json::{Map, Value, json};

use super::{Arguments, ToolContext, ToolEntry, ToolFuture, coded_error_with, local_read, tool};
use crate::http::graphql::refusal;
use crate::http::mcp::catalog::{Operation, OperationKind, one_line};
use crate::http::mcp::schema_index::{SchemaIndex, schema_index};
use crate::http::mcp::server::json_result;

pub(super) fn entry() -> ToolEntry {
    ToolEntry {
        tool: tool(
            "list_operations",
            "Operations run_query and run_mutation accept, by skill, as [name, kind q|m, purpose] rows.",
            json!({"type": "object", "properties": {"skill": {"type": "string"}}}),
            local_read(),
        ),
        accepts: &["skill"],
        run,
    }
}

fn run(context: ToolContext, arguments: Arguments) -> ToolFuture {
    Box::pin(async move {
        let catalog = context.catalog;
        let skill = arguments.str("skill", 100)?;
        let skills = catalog.skills();
        if let Some(skill) = skill
            && !skills.contains(&skill)
        {
            let mut facts = Map::new();
            facts.insert("skills".into(), json!(skills));
            return Ok(coded_error_with(
                "INVALID_INPUT",
                refusal::MCP_SKILL_UNKNOWN,
                "No served operation is in this skill.",
                facts,
            ));
        }
        // The schema is read only for an operation with no comment of its own.
        let wanted =
            |operation: &Operation| skill.is_none_or(|skill| operation.skills.contains(&skill));
        let index = if catalog
            .operations()
            .iter()
            .any(|operation| wanted(operation) && operation.purpose.is_none())
        {
            schema_index(&context.schema).await
        } else {
            None
        };
        let mut groups = Map::new();
        for name in skills
            .iter()
            .filter(|name| skill.is_none_or(|skill| skill == **name))
        {
            let rows = catalog
                .operations()
                .iter()
                .filter(|operation| operation.skills.contains(name))
                .map(|operation| row(operation, index.as_deref()))
                .collect::<Vec<_>>();
            groups.insert((*name).to_string(), Value::Array(rows));
        }
        Ok(json_result(Value::Object(groups)))
    })
}

fn row(operation: &Operation, index: Option<&SchemaIndex>) -> Value {
    let purpose = operation.purpose.clone().or_else(|| {
        let index = index?;
        let mutation = operation.kind == OperationKind::Mutation;
        let described = operation
            .root_fields
            .iter()
            .filter_map(|field| index.root_field(mutation, field))
            .collect::<Vec<_>>()
            .join(" ");
        one_line(&described)
    });
    match purpose {
        Some(purpose) => json!([operation.name, operation.kind.token(), purpose]),
        None => json!([operation.name, operation.kind.token()]),
    }
}
