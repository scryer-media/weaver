use rusqlite::OptionalExtension;

use crate::StateError;

use super::Database;

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RssRuleAction {
    Accept,
    Reject,
}

impl RssRuleAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
        }
    }

    pub fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "accept" => Ok(Self::Accept),
            "reject" => Ok(Self::Reject),
            other => Err(StateError::Database(format!(
                "invalid RSS rule action: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssFeedRow {
    pub id: u32,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub poll_interval_secs: u32,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_category: Option<String>,
    pub default_metadata: Vec<(String, String)>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_polled_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssRuleRow {
    pub id: u32,
    pub feed_id: u32,
    pub sort_order: i32,
    pub enabled: bool,
    pub action: RssRuleAction,
    pub title_regex: Option<String>,
    pub item_categories: Vec<String>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssSeenItemRow {
    pub feed_id: u32,
    pub item_id: String,
    pub item_title: String,
    pub published_at: Option<i64>,
    pub size_bytes: Option<u64>,
    pub decision: String,
    pub seen_at: i64,
    pub job_id: Option<u64>,
    pub item_url: Option<String>,
    pub error: Option<String>,
}

fn encode_metadata(entries: &[(String, String)]) -> Result<Option<String>, StateError> {
    if entries.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(entries).map(Some).map_err(db_err)
    }
}

fn decode_metadata(raw: Option<String>) -> Vec<(String, String)> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

fn encode_categories(values: &[String]) -> Result<Option<String>, StateError> {
    if values.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(values).map(Some).map_err(db_err)
    }
}

fn decode_categories(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

fn parse_action_sql(value: String, column_index: usize) -> rusqlite::Result<RssRuleAction> {
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

impl Database {
    pub fn next_rss_feed_id(&self) -> Result<u32, StateError> {
        let conn = self.conn();
        let max: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM rss_feeds", [], |row| row.get(0))
            .map_err(db_err)?;
        Ok(max.unwrap_or(0) + 1)
    }

    pub fn next_rss_rule_id(&self) -> Result<u32, StateError> {
        let conn = self.conn();
        let max: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM rss_rules", [], |row| row.get(0))
            .map_err(db_err)?;
        Ok(max.unwrap_or(0) + 1)
    }

    pub fn list_rss_feeds(&self) -> Result<Vec<RssFeedRow>, StateError> {
        use crate::encryption::maybe_decrypt;
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, name, url, enabled, poll_interval_secs, username, password,
                        default_category, default_metadata, etag, last_modified, last_polled_at,
                        last_success_at, last_error, consecutive_failures
                 FROM rss_feeds
                 ORDER BY id",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(RssFeedRow {
                    id: row.get::<_, u32>(0)?,
                    name: row.get(1)?,
                    url: row.get(2)?,
                    enabled: row.get::<_, bool>(3)?,
                    poll_interval_secs: row.get::<_, u32>(4)?,
                    username: row.get(5)?,
                    password: row.get(6)?,
                    default_category: row.get(7)?,
                    default_metadata: decode_metadata(row.get(8)?),
                    etag: row.get(9)?,
                    last_modified: row.get(10)?,
                    last_polled_at: row.get(11)?,
                    last_success_at: row.get(12)?,
                    last_error: row.get(13)?,
                    consecutive_failures: row.get::<_, u32>(14)?,
                })
            })
            .map_err(db_err)?;

        let mut out = Vec::new();
        for row in rows {
            let mut feed = row.map_err(db_err)?;
            feed.password = maybe_decrypt(self.encryption_key(), feed.password);
            out.push(feed);
        }
        Ok(out)
    }

    pub fn get_rss_feed(&self, id: u32) -> Result<Option<RssFeedRow>, StateError> {
        use crate::encryption::maybe_decrypt;
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, name, url, enabled, poll_interval_secs, username, password,
                        default_category, default_metadata, etag, last_modified, last_polled_at,
                        last_success_at, last_error, consecutive_failures
                 FROM rss_feeds
                 WHERE id = ?1",
            )
            .map_err(db_err)?;
        let row = stmt
            .query_row([id], |row| {
                Ok(RssFeedRow {
                    id: row.get::<_, u32>(0)?,
                    name: row.get(1)?,
                    url: row.get(2)?,
                    enabled: row.get::<_, bool>(3)?,
                    poll_interval_secs: row.get::<_, u32>(4)?,
                    username: row.get(5)?,
                    password: row.get(6)?,
                    default_category: row.get(7)?,
                    default_metadata: decode_metadata(row.get(8)?),
                    etag: row.get(9)?,
                    last_modified: row.get(10)?,
                    last_polled_at: row.get(11)?,
                    last_success_at: row.get(12)?,
                    last_error: row.get(13)?,
                    consecutive_failures: row.get::<_, u32>(14)?,
                })
            })
            .optional()
            .map_err(db_err)?;
        Ok(row.map(|mut feed| {
            feed.password = maybe_decrypt(self.encryption_key(), feed.password);
            feed
        }))
    }

    pub fn insert_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        use crate::encryption::maybe_encrypt;
        let conn = self.conn();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = maybe_encrypt(self.encryption_key(), &feed.password);
        conn.execute(
            "INSERT INTO rss_feeds
             (id, name, url, enabled, poll_interval_secs, username, password, default_category,
              default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error,
              consecutive_failures)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            rusqlite::params![
                feed.id,
                feed.name,
                feed.url,
                feed.enabled,
                feed.poll_interval_secs,
                feed.username,
                encrypted_password,
                feed.default_category,
                metadata,
                feed.etag,
                feed.last_modified,
                feed.last_polled_at,
                feed.last_success_at,
                feed.last_error,
                feed.consecutive_failures,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn update_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        use crate::encryption::maybe_encrypt;
        let conn = self.conn();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = maybe_encrypt(self.encryption_key(), &feed.password);
        conn.execute(
            "UPDATE rss_feeds
             SET name = ?2, url = ?3, enabled = ?4, poll_interval_secs = ?5, username = ?6,
                 password = ?7, default_category = ?8, default_metadata = ?9, etag = ?10,
                 last_modified = ?11, last_polled_at = ?12, last_success_at = ?13,
                 last_error = ?14, consecutive_failures = ?15
             WHERE id = ?1",
            rusqlite::params![
                feed.id,
                feed.name,
                feed.url,
                feed.enabled,
                feed.poll_interval_secs,
                feed.username,
                encrypted_password,
                feed.default_category,
                metadata,
                feed.etag,
                feed.last_modified,
                feed.last_polled_at,
                feed.last_success_at,
                feed.last_error,
                feed.consecutive_failures,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_rss_feed(&self, id: u32) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM rss_feeds WHERE id = ?1", [id])
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn list_rss_rules(&self, feed_id: u32) -> Result<Vec<RssRuleRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, feed_id, sort_order, enabled, action, title_regex, item_categories,
                        min_size_bytes, max_size_bytes, category_override, metadata
                 FROM rss_rules
                 WHERE feed_id = ?1
                 ORDER BY sort_order, id",
            )
            .map_err(db_err)?;
        let rows = stmt
            .query_map([feed_id], |row| {
                let action = parse_action_sql(row.get::<_, String>(4)?, 4)?;
                Ok(RssRuleRow {
                    id: row.get::<_, u32>(0)?,
                    feed_id: row.get::<_, u32>(1)?,
                    sort_order: row.get(2)?,
                    enabled: row.get::<_, bool>(3)?,
                    action,
                    title_regex: row.get(5)?,
                    item_categories: decode_categories(row.get(6)?),
                    min_size_bytes: row.get(7)?,
                    max_size_bytes: row.get(8)?,
                    category_override: row.get(9)?,
                    metadata: decode_metadata(row.get(10)?),
                })
            })
            .map_err(db_err)?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    pub fn get_rss_rule(&self, id: u32) -> Result<Option<RssRuleRow>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, feed_id, sort_order, enabled, action, title_regex, item_categories,
                        min_size_bytes, max_size_bytes, category_override, metadata
                 FROM rss_rules
                 WHERE id = ?1",
            )
            .map_err(db_err)?;
        let row = stmt
            .query_row([id], |row| {
                let action = parse_action_sql(row.get::<_, String>(4)?, 4)?;
                Ok(RssRuleRow {
                    id: row.get::<_, u32>(0)?,
                    feed_id: row.get::<_, u32>(1)?,
                    sort_order: row.get(2)?,
                    enabled: row.get::<_, bool>(3)?,
                    action,
                    title_regex: row.get(5)?,
                    item_categories: decode_categories(row.get(6)?),
                    min_size_bytes: row.get(7)?,
                    max_size_bytes: row.get(8)?,
                    category_override: row.get(9)?,
                    metadata: decode_metadata(row.get(10)?),
                })
            })
            .optional()
            .map_err(db_err)?;
        Ok(row)
    }

    pub fn insert_rss_rule(&self, rule: &RssRuleRow) -> Result<(), StateError> {
        let conn = self.conn();
        let categories = encode_categories(&rule.item_categories)?;
        let metadata = encode_metadata(&rule.metadata)?;
        conn.execute(
            "INSERT INTO rss_rules
             (id, feed_id, sort_order, enabled, action, title_regex, item_categories,
              min_size_bytes, max_size_bytes, category_override, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                rule.id,
                rule.feed_id,
                rule.sort_order,
                rule.enabled,
                rule.action.as_str(),
                rule.title_regex,
                categories,
                rule.min_size_bytes.map(|v| v as i64),
                rule.max_size_bytes.map(|v| v as i64),
                rule.category_override,
                metadata,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn update_rss_rule(&self, rule: &RssRuleRow) -> Result<(), StateError> {
        let conn = self.conn();
        let categories = encode_categories(&rule.item_categories)?;
        let metadata = encode_metadata(&rule.metadata)?;
        conn.execute(
            "UPDATE rss_rules
             SET feed_id = ?2, sort_order = ?3, enabled = ?4, action = ?5, title_regex = ?6,
                 item_categories = ?7, min_size_bytes = ?8, max_size_bytes = ?9,
                 category_override = ?10, metadata = ?11
             WHERE id = ?1",
            rusqlite::params![
                rule.id,
                rule.feed_id,
                rule.sort_order,
                rule.enabled,
                rule.action.as_str(),
                rule.title_regex,
                categories,
                rule.min_size_bytes.map(|v| v as i64),
                rule.max_size_bytes.map(|v| v as i64),
                rule.category_override,
                metadata,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn delete_rss_rule(&self, id: u32) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM rss_rules WHERE id = ?1", [id])
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn rss_seen_item_exists(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let conn = self.conn();
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM rss_seen_items WHERE feed_id = ?1 AND item_id = ?2",
                rusqlite::params![feed_id, item_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(db_err)?;
        Ok(exists.is_some())
    }

    pub fn insert_rss_seen_item(&self, item: &RssSeenItemRow) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO rss_seen_items
             (feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at,
              job_id, item_url, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                item.feed_id,
                item.item_id,
                item.item_title,
                item.published_at,
                item.size_bytes.map(|v| v as i64),
                item.decision,
                item.seen_at,
                item.job_id.map(|v| v as i64),
                item.item_url,
                item.error,
            ],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn list_rss_seen_items(
        &self,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItemRow>, StateError> {
        let conn = self.conn();
        let mut out = Vec::new();

        match (feed_id, limit) {
            (Some(feed_id), Some(limit)) => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
                                seen_at, job_id, item_url, error
                         FROM rss_seen_items
                         WHERE feed_id = ?1
                         ORDER BY seen_at DESC
                         LIMIT ?2",
                    )
                    .map_err(db_err)?;
                let rows = stmt
                    .query_map(rusqlite::params![feed_id, limit], map_seen_item_row)
                    .map_err(db_err)?;
                for row in rows {
                    out.push(row.map_err(db_err)?);
                }
            }
            (Some(feed_id), None) => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
                                seen_at, job_id, item_url, error
                         FROM rss_seen_items
                         WHERE feed_id = ?1
                         ORDER BY seen_at DESC",
                    )
                    .map_err(db_err)?;
                let rows = stmt
                    .query_map([feed_id], map_seen_item_row)
                    .map_err(db_err)?;
                for row in rows {
                    out.push(row.map_err(db_err)?);
                }
            }
            (None, Some(limit)) => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
                                seen_at, job_id, item_url, error
                         FROM rss_seen_items
                         ORDER BY seen_at DESC
                         LIMIT ?1",
                    )
                    .map_err(db_err)?;
                let rows = stmt.query_map([limit], map_seen_item_row).map_err(db_err)?;
                for row in rows {
                    out.push(row.map_err(db_err)?);
                }
            }
            (None, None) => {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
                                seen_at, job_id, item_url, error
                         FROM rss_seen_items
                         ORDER BY seen_at DESC",
                    )
                    .map_err(db_err)?;
                let rows = stmt.query_map([], map_seen_item_row).map_err(db_err)?;
                for row in rows {
                    out.push(row.map_err(db_err)?);
                }
            }
        }

        Ok(out)
    }

    pub fn delete_rss_seen_item(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute(
                "DELETE FROM rss_seen_items WHERE feed_id = ?1 AND item_id = ?2",
                rusqlite::params![feed_id, item_id],
            )
            .map_err(db_err)?;
        Ok(changed > 0)
    }

    pub fn clear_rss_seen_items(&self, feed_id: Option<u32>) -> Result<u64, StateError> {
        let conn = self.conn();
        let changed = match feed_id {
            Some(feed_id) => conn
                .execute("DELETE FROM rss_seen_items WHERE feed_id = ?1", [feed_id])
                .map_err(db_err)?,
            None => conn
                .execute("DELETE FROM rss_seen_items", [])
                .map_err(db_err)?,
        };
        Ok(changed as u64)
    }

    pub fn purge_old_rss_seen_items(&self, cutoff_ts: i64) -> Result<u64, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM rss_seen_items WHERE seen_at < ?1", [cutoff_ts])
            .map_err(db_err)?;
        Ok(changed as u64)
    }

    pub fn record_rss_poll_success(
        &self,
        feed_id: u32,
        polled_at: i64,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE rss_feeds
             SET last_polled_at = ?2, last_success_at = ?2, last_error = NULL,
                 consecutive_failures = 0, etag = ?3, last_modified = ?4
             WHERE id = ?1",
            rusqlite::params![feed_id, polled_at, etag, last_modified],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn record_rss_poll_failure(
        &self,
        feed_id: u32,
        polled_at: i64,
        error: &str,
    ) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute(
            "UPDATE rss_feeds
             SET last_polled_at = ?2,
                 last_error = ?3,
                 consecutive_failures = consecutive_failures + 1
             WHERE id = ?1",
            rusqlite::params![feed_id, polled_at, error],
        )
        .map_err(db_err)?;
        Ok(())
    }
}

fn map_seen_item_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RssSeenItemRow> {
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
mod tests {
    use super::*;

    fn sample_feed(id: u32) -> RssFeedRow {
        RssFeedRow {
            id,
            name: format!("Feed {id}"),
            url: format!("https://example.com/{id}.xml"),
            enabled: true,
            poll_interval_secs: 900,
            username: Some("user".to_string()),
            password: Some("secret".to_string()),
            default_category: Some("tv".to_string()),
            default_metadata: vec![("source".to_string(), "rss".to_string())],
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        }
    }

    fn sample_rule(id: u32, feed_id: u32) -> RssRuleRow {
        RssRuleRow {
            id,
            feed_id,
            sort_order: id as i32,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec!["tv".to_string()],
            min_size_bytes: Some(100),
            max_size_bytes: Some(1000),
            category_override: Some("anime".to_string()),
            metadata: vec![("profile".to_string(), "hevc".to_string())],
        }
    }

    #[test]
    fn rss_feed_and_rule_roundtrip() {
        let db = Database::open_in_memory().unwrap();
        let feed = sample_feed(1);
        db.insert_rss_feed(&feed).unwrap();
        assert_eq!(db.get_rss_feed(1).unwrap(), Some(feed.clone()));

        let rule = sample_rule(10, 1);
        db.insert_rss_rule(&rule).unwrap();
        assert_eq!(db.list_rss_rules(1).unwrap(), vec![rule.clone()]);

        let mut updated = feed.clone();
        updated.last_error = Some("boom".to_string());
        updated.consecutive_failures = 2;
        db.update_rss_feed(&updated).unwrap();
        assert_eq!(db.get_rss_feed(1).unwrap(), Some(updated));

        assert!(db.delete_rss_rule(rule.id).unwrap());
        assert!(db.list_rss_rules(1).unwrap().is_empty());
    }

    #[test]
    fn rss_seen_items_dedupe_and_purge() {
        let db = Database::open_in_memory().unwrap();
        db.insert_rss_feed(&sample_feed(1)).unwrap();
        let item = RssSeenItemRow {
            feed_id: 1,
            item_id: "guid-1".to_string(),
            item_title: "Example".to_string(),
            published_at: Some(123),
            size_bytes: Some(456),
            decision: "accepted".to_string(),
            seen_at: 1_000,
            job_id: Some(42),
            item_url: Some("https://example.com/a.nzb".to_string()),
            error: None,
        };
        db.insert_rss_seen_item(&item).unwrap();
        assert!(db.rss_seen_item_exists(1, "guid-1").unwrap());
        assert_eq!(
            db.list_rss_seen_items(Some(1), Some(10)).unwrap(),
            vec![item]
        );
        assert!(db.delete_rss_seen_item(1, "guid-1").unwrap());
        assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());

        db.insert_rss_seen_item(&RssSeenItemRow {
            feed_id: 1,
            item_id: "guid-1".to_string(),
            item_title: "Example".to_string(),
            published_at: Some(123),
            size_bytes: Some(456),
            decision: "accepted".to_string(),
            seen_at: 1_000,
            job_id: Some(42),
            item_url: Some("https://example.com/a.nzb".to_string()),
            error: None,
        })
        .unwrap();
        assert_eq!(db.purge_old_rss_seen_items(2_000).unwrap(), 1);
        assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());
    }

    #[test]
    fn clear_rss_seen_items_can_scope_to_feed() {
        let db = Database::open_in_memory().unwrap();
        db.insert_rss_feed(&sample_feed(1)).unwrap();
        db.insert_rss_feed(&sample_feed(2)).unwrap();
        db.insert_rss_seen_item(&RssSeenItemRow {
            feed_id: 1,
            item_id: "guid-1".to_string(),
            item_title: "Example".to_string(),
            published_at: None,
            size_bytes: None,
            decision: "submitted".to_string(),
            seen_at: 1_000,
            job_id: Some(1),
            item_url: None,
            error: None,
        })
        .unwrap();
        db.insert_rss_seen_item(&RssSeenItemRow {
            feed_id: 2,
            item_id: "guid-2".to_string(),
            item_title: "Example Two".to_string(),
            published_at: None,
            size_bytes: None,
            decision: "ignored".to_string(),
            seen_at: 2_000,
            job_id: None,
            item_url: None,
            error: None,
        })
        .unwrap();

        assert_eq!(db.clear_rss_seen_items(Some(1)).unwrap(), 1);
        let remaining = db.list_rss_seen_items(None, None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].feed_id, 2);
        assert_eq!(db.clear_rss_seen_items(None).unwrap(), 1);
        assert!(db.list_rss_seen_items(None, None).unwrap().is_empty());
    }
}
