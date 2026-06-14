use crate::StateError;
#[cfg(test)]
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRow;
use crate::rss::record::{RssRuleAction, RssSeenItemRow};
#[cfg(test)]
use crate::rss::{RssFeedRow, RssRuleRow};

pub(crate) fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

pub(crate) fn encode_metadata(entries: &[(String, String)]) -> Result<Option<String>, StateError> {
    if entries.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(entries).map(Some).map_err(db_err)
    }
}

pub(crate) fn decode_metadata(raw: Option<String>) -> Vec<(String, String)> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub(crate) fn encode_categories(values: &[String]) -> Result<Option<String>, StateError> {
    if values.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(values).map(Some).map_err(db_err)
    }
}

pub(crate) fn decode_categories(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub(crate) fn parse_action_sql(value: String) -> Result<RssRuleAction, StateError> {
    RssRuleAction::parse(&value)
}

pub(crate) fn map_seen_item_row(row: SqlRow) -> Result<RssSeenItemRow, StateError> {
    Ok(RssSeenItemRow {
        feed_id: row.i32("feed_id")? as u32,
        item_id: row.text("item_id")?,
        item_title: row.text("item_title")?,
        published_at: row.opt_i64("published_at")?,
        size_bytes: row.opt_i64("size_bytes")?.map(|value| value as u64),
        decision: row.text("decision")?,
        seen_at: row.i64("seen_at")?,
        job_id: row.opt_i64("job_id")?.map(|value| value as u64),
        item_url: row.opt_text("item_url")?,
        error: row.opt_text("error")?,
    })
}

#[cfg(test)]
mod tests;
