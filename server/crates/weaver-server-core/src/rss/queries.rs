use rusqlite::OptionalExtension;

use crate::StateError;
use crate::persistence::Database;
use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};

use super::repository::{
    db_err, decode_categories, decode_metadata, map_seen_item_row, parse_action_sql,
};

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
        use crate::persistence::encryption::maybe_decrypt;

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
        use crate::persistence::encryption::maybe_decrypt;

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
}
