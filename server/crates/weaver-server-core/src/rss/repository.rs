use crate::StateError;
#[cfg(test)]
use crate::persistence::Database;
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

pub(crate) fn parse_action_sql(
    value: String,
    column_index: usize,
) -> rusqlite::Result<RssRuleAction> {
    match value.as_str() {
        "accept" => Ok(RssRuleAction::Accept),
        "reject" => Ok(RssRuleAction::Reject),
        other => Err(rusqlite::Error::FromSqlConversionFailure(
            column_index,
            rusqlite::types::Type::Text,
            format!("invalid RSS rule action: {other}").into(),
        )),
    }
}

pub(crate) fn map_seen_item_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RssSeenItemRow> {
    Ok(RssSeenItemRow {
        feed_id: row.get::<_, u32>(0)?,
        item_id: row.get(1)?,
        item_title: row.get(2)?,
        published_at: row.get(3)?,
        size_bytes: row.get(4)?,
        decision: row.get(5)?,
        seen_at: row.get(6)?,
        job_id: row.get(7)?,
        item_url: row.get(8)?,
        error: row.get(9)?,
    })
}

#[cfg(test)]
mod tests;
