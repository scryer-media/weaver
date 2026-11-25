// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `read_diagnostics_section`: one section of the diagnostics package,
// collected alone by the package's own collector and sanitized by its pass.
// ADMIN only, as the package is.

use serde_json::{Map, Value, json};
use weaver_server_api::auth::CallerIdentity;

use super::{
    Arguments, ToolContext, ToolEntry, ToolFuture, admit_diagnostics, capped_result,
    invalid_argument, local_read, tool,
};
use crate::http::diagnostics::{DiagnosticsSection, SectionRead, SectionValue};

const MAX_METRIC_FAMILIES: usize = 10;

pub(super) fn entry() -> ToolEntry {
    let sections = DiagnosticsSection::ALL
        .iter()
        .map(|section| section.as_str())
        .collect::<Vec<_>>();
    ToolEntry {
        tool: tool(
            "read_diagnostics_section",
            "One sanitized diagnostics package section; metrics takes up to 10 metric_families. Needs an ADMIN key.",
            json!({
                "type": "object",
                "properties": {
                    "section": {"enum": sections},
                    "metric_families": {"type": "array", "items": {"type": "string"}, "maxItems": MAX_METRIC_FAMILIES},
                },
                "required": ["section"],
            }),
            local_read(),
        ),
        accepts: &["section", "metric_families"],
        run,
    }
}

fn run(context: ToolContext, arguments: Arguments) -> ToolFuture {
    Box::pin(async move {
        let name = arguments.required_str("section", 100)?;
        let Some(section) = DiagnosticsSection::parse(name) else {
            return Err(invalid_argument(
                "section",
                "must be one of the listed values",
            ));
        };
        let families = arguments.str_list("metric_families", MAX_METRIC_FAMILIES, 200)?;
        match (section, &families) {
            (DiagnosticsSection::Metrics, None) => {
                return Err(invalid_argument(
                    "metric_families",
                    "is required for the metrics section",
                ));
            }
            (DiagnosticsSection::Metrics, Some(_)) | (_, None) => {}
            (_, Some(_)) => {
                return Err(invalid_argument(
                    "metric_families",
                    "is only accepted for the metrics section",
                ));
            }
        }
        if let Some(refused) = admit_diagnostics(&context) {
            return Ok(refused);
        }
        let families = families.unwrap_or_default();
        let read = context
            .diagnostics
            .read_section(
                context.caller.scope,
                CallerIdentity::ApiKey(context.caller.key),
                section,
                &families,
            )
            .await;
        let result = if section == DiagnosticsSection::Metrics {
            metrics_result(read, families)
        } else {
            section_result(section, read, context.result_max_bytes)
        };
        Ok(capped_result(&result, context.result_max_bytes))
    })
}

// The section's files. Each is its JSON value, its text, or its error; a
// file that would take the result past three quarters of the cap is
// withheld with its size, so the rest still fit.
fn section_result(section: DiagnosticsSection, read: SectionRead, max_bytes: usize) -> Value {
    let mut budget = max_bytes / 4 * 3;
    let files = read
        .files
        .into_iter()
        .map(|file| {
            let mut entry = Map::new();
            entry.insert("name".into(), json!(file.name));
            let (key, value) = match file.value {
                SectionValue::Json(value) => ("value", value),
                SectionValue::Text(text) => ("text", Value::String(text)),
                SectionValue::Failed(error) => ("error", Value::String(error)),
            };
            let bytes = value.to_string().len();
            if bytes <= budget {
                budget -= bytes;
                entry.insert(key.into(), value);
            } else {
                entry.insert("withheld_for_size_bytes".into(), json!(bytes));
            }
            if let Some(reason) = file.reason {
                entry.insert("reason".into(), json!(reason));
            }
            if let Some(partial) = file.partial {
                entry.insert("partial".into(), json!(partial));
            }
            Value::Object(entry)
        })
        .collect::<Vec<_>>();
    json!({
        "section": section.as_str(),
        "files": files,
        "stand_in_table_complete": read.table_complete,
    })
}

// The filtered exposition as lines, as Scryer's `metrics` section gives it.
fn metrics_result(read: SectionRead, families: Vec<String>) -> Value {
    let mut result = Map::new();
    result.insert(
        "section".into(),
        json!(DiagnosticsSection::Metrics.as_str()),
    );
    result.insert("families".into(), json!(families));
    match read.files.into_iter().next().map(|file| file.value) {
        Some(SectionValue::Text(text)) => {
            let lines = text
                .lines()
                .map(|line| cut_chars(line, 300).to_string())
                .collect::<Vec<_>>();
            result.insert("returned".into(), json!(lines.len()));
            result.insert("lines".into(), json!(lines));
            if read.metric_lines_left_out > 0 {
                result.insert("truncated".into(), json!(read.metric_lines_left_out));
            }
        }
        Some(SectionValue::Failed(error)) => {
            result.insert("metrics_unavailable".into(), json!(error));
        }
        Some(SectionValue::Json(_)) | None => {
            result.insert("metrics_unavailable".into(), json!("render_failed"));
        }
    }
    result.insert("stand_in_table_complete".into(), json!(read.table_complete));
    Value::Object(result)
}

// `text` cut to at most `max` characters, on a character boundary.
pub(super) fn cut_chars(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((index, _)) => &text[..index],
        None => text,
    }
}
