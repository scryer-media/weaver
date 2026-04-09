use crate::StateError;
use crate::persistence::Database;
use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};

use super::repository::{db_err, encode_categories, encode_metadata};

impl Database {
    pub fn insert_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;
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
        use crate::persistence::encryption::maybe_encrypt;
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
