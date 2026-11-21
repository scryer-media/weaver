// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::record::{RssRuleAction, RssSeenItemRow};
use weaver_server_db::persistence::{StateError, sql_runtime::SqlRow};
pub fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

pub fn encode_metadata(entries: &[(String, String)]) -> Result<Option<String>, StateError> {
    if entries.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(entries).map(Some).map_err(db_err)
    }
}

pub fn decode_metadata(raw: Option<String>) -> Vec<(String, String)> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub fn encode_categories(values: &[String]) -> Result<Option<String>, StateError> {
    if values.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(values).map(Some).map_err(db_err)
    }
}

pub fn decode_categories(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub fn parse_action_sql(value: String) -> Result<RssRuleAction, StateError> {
    RssRuleAction::parse(&value)
}

pub fn map_seen_item_row(row: SqlRow) -> Result<RssSeenItemRow, StateError> {
    Ok(RssSeenItemRow {
        feed_id: row.i32("feed_id")? as u32,
        item_id: row.text("item_id")?,
        item_title: row.text("item_title")?,
        published_at: row.opt_i64("published_at")?,
        size_bytes: row.opt_i64("size_bytes")?.map(|value| value as u64),
        decision: crate::rss::RssSeenDecision::parse(&row.text("decision")?),
        seen_at: row.i64("seen_at")?,
        job_id: row.opt_i64("job_id")?.map(|value| value as u64),
        item_url: row.opt_text("item_url")?,
        error: row.opt_text("error")?,
    })
}
