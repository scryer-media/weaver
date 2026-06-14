use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};

use super::repository::{encode_categories, encode_metadata};

impl Database {
    pub fn insert_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let datastore = self.datastore();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = maybe_encrypt(self.encryption_key(), &feed.password);
        let args = rss_feed_args(feed, metadata, encrypted_password);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO rss_feeds
                    (id, name, url, enabled, poll_interval_secs, username, password, default_category,
                     default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error,
                     consecutive_failures)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let datastore = self.datastore();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = maybe_encrypt(self.encryption_key(), &feed.password);
        let mut args = rss_feed_args(feed, metadata, encrypted_password);
        let id = args.remove(0);
        args.push(id);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE rss_feeds
                    SET name = {}, url = {}, enabled = {}, poll_interval_secs = {}, username = {},
                        password = {}, default_category = {}, default_metadata = {}, etag = {},
                        last_modified = {}, last_polled_at = {}, last_success_at = {},
                        last_error = {}, consecutive_failures = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_rss_feed(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM rss_feeds WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn insert_rss_rule(&self, rule: &RssRuleRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = rss_rule_args(rule)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO rss_rules
                    (id, feed_id, sort_order, enabled, action, title_regex, item_categories,
                     min_size_bytes, max_size_bytes, category_override, metadata)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_rss_rule(&self, rule: &RssRuleRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let mut args = rss_rule_args(rule)?;
        let id = args.remove(0);
        args.push(id);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE rss_rules
                    SET feed_id = {}, sort_order = {}, enabled = {}, action = {}, title_regex = {},
                        item_categories = {}, min_size_bytes = {}, max_size_bytes = {},
                        category_override = {}, metadata = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_rss_rule(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM rss_rules WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn insert_rss_seen_item(&self, item: &RssSeenItemRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = rss_seen_item_args(item);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO rss_seen_items
                    (feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at,
                     job_id, item_url, error)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                 ON CONFLICT(feed_id, item_id) DO UPDATE SET
                    item_title = excluded.item_title,
                    published_at = excluded.published_at,
                    size_bytes = excluded.size_bytes,
                    decision = excluded.decision,
                    seen_at = excluded.seen_at,
                    job_id = excluded.job_id,
                    item_url = excluded.item_url,
                    error = excluded.error",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_rss_seen_item(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let item_id = item_id.to_string();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM rss_seen_items WHERE feed_id = {} AND item_id = {}",
                &[SqlArg::I64(i64::from(feed_id)), SqlArg::Text(item_id)],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn clear_rss_seen_items(&self, feed_id: Option<u32>) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = match feed_id {
                Some(feed_id) => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM rss_seen_items WHERE feed_id = {}",
                        &[SqlArg::I64(i64::from(feed_id))],
                    )
                    .await?
                }
                None => {
                    SqlRuntime::execute(datastore.read_exec(), "DELETE FROM rss_seen_items", &[])
                        .await?
                }
            };
            Ok(changed)
        })
    }

    pub fn purge_old_rss_seen_items(&self, cutoff_ts: i64) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM rss_seen_items WHERE seen_at < {}",
                &[SqlArg::I64(cutoff_ts)],
            )
            .await
        })
    }

    pub fn record_rss_poll_success(
        &self,
        feed_id: u32,
        polled_at: i64,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let etag = etag.map(str::to_string);
        let last_modified = last_modified.map(str::to_string);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE rss_feeds
                    SET last_polled_at = {}, last_success_at = {}, last_error = NULL,
                        consecutive_failures = 0, etag = {}, last_modified = {}
                  WHERE id = {}",
                &[
                    SqlArg::I64(polled_at),
                    SqlArg::I64(polled_at),
                    SqlArg::OptText(etag),
                    SqlArg::OptText(last_modified),
                    SqlArg::I64(i64::from(feed_id)),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn record_rss_poll_failure(
        &self,
        feed_id: u32,
        polled_at: i64,
        error: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let error = error.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE rss_feeds
                    SET last_polled_at = {},
                        last_error = {},
                        consecutive_failures = consecutive_failures + 1
                  WHERE id = {}",
                &[
                    SqlArg::I64(polled_at),
                    SqlArg::Text(error),
                    SqlArg::I64(i64::from(feed_id)),
                ],
            )
            .await?;
            Ok(())
        })
    }
}

fn rss_feed_args(
    feed: &RssFeedRow,
    metadata: Option<String>,
    encrypted_password: Option<String>,
) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(i64::from(feed.id)),
        SqlArg::Text(feed.name.clone()),
        SqlArg::Text(feed.url.clone()),
        SqlArg::Bool(feed.enabled),
        SqlArg::I64(i64::from(feed.poll_interval_secs)),
        SqlArg::OptText(feed.username.clone()),
        SqlArg::OptText(encrypted_password),
        SqlArg::OptText(feed.default_category.clone()),
        SqlArg::OptText(metadata),
        SqlArg::OptText(feed.etag.clone()),
        SqlArg::OptText(feed.last_modified.clone()),
        SqlArg::OptI64(feed.last_polled_at),
        SqlArg::OptI64(feed.last_success_at),
        SqlArg::OptText(feed.last_error.clone()),
        SqlArg::I64(i64::from(feed.consecutive_failures)),
    ]
}

fn rss_rule_args(rule: &RssRuleRow) -> Result<Vec<SqlArg>, StateError> {
    let categories = encode_categories(&rule.item_categories)?;
    let metadata = encode_metadata(&rule.metadata)?;
    Ok(vec![
        SqlArg::I64(i64::from(rule.id)),
        SqlArg::I64(i64::from(rule.feed_id)),
        SqlArg::I64(i64::from(rule.sort_order)),
        SqlArg::Bool(rule.enabled),
        SqlArg::Text(rule.action.as_str().to_string()),
        SqlArg::OptText(rule.title_regex.clone()),
        SqlArg::OptText(categories),
        SqlArg::OptI64(rule.min_size_bytes.map(|value| value as i64)),
        SqlArg::OptI64(rule.max_size_bytes.map(|value| value as i64)),
        SqlArg::OptText(rule.category_override.clone()),
        SqlArg::OptText(metadata),
    ])
}

fn rss_seen_item_args(item: &RssSeenItemRow) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(i64::from(item.feed_id)),
        SqlArg::Text(item.item_id.clone()),
        SqlArg::Text(item.item_title.clone()),
        SqlArg::OptI64(item.published_at),
        SqlArg::OptI64(item.size_bytes.map(|value| value as i64)),
        SqlArg::Text(item.decision.clone()),
        SqlArg::I64(item.seen_at),
        SqlArg::OptI64(item.job_id.map(|value| value as i64)),
        SqlArg::OptText(item.item_url.clone()),
        SqlArg::OptText(item.error.clone()),
    ]
}
