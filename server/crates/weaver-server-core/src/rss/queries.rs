use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};
use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};

use super::repository::{decode_categories, decode_metadata, map_seen_item_row, parse_action_sql};

pub(crate) struct ScheduleCache {
    rows: std::sync::Mutex<Option<Vec<super::model::RssFeedSchedule>>>,
    pub(crate) changed: tokio::sync::watch::Sender<u64>,
    #[cfg(test)]
    pub(crate) loads: std::sync::atomic::AtomicU64,
}

impl Default for ScheduleCache {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            changed: tokio::sync::watch::channel(0).0,
            #[cfg(test)]
            loads: Default::default(),
        }
    }
}

const RSS_FEED_SELECT: &str =
    "SELECT id, name, url, enabled, poll_interval_secs, username, password,
        default_category, default_metadata, etag, last_modified, last_polled_at,
        last_success_at, last_error, consecutive_failures
   FROM rss_feeds";

const RSS_RULE_SELECT: &str =
    "SELECT id, feed_id, sort_order, enabled, action, title_regex, item_categories,
        min_size_bytes, max_size_bytes, category_override, metadata
   FROM rss_rules";

const RSS_SEEN_SELECT: &str =
    "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
        seen_at, job_id, item_url, error
   FROM rss_seen_items";

impl Database {
    pub fn next_rss_feed_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM rss_feeds",
                &[],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0) as u32
                + 1)
        })
    }

    pub fn next_rss_rule_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM rss_rules",
                &[],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0) as u32
                + 1)
        })
    }

    pub fn list_rss_feeds(&self) -> Result<Vec<RssFeedRow>, StateError> {
        use crate::persistence::encryption::maybe_decrypt;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("{RSS_FEED_SELECT} ORDER BY id"),
                &[],
            )
            .await?;

            let mut attached = std::collections::BTreeMap::<u32, Vec<String>>::new();
            for row in SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT feed_id, instance_id FROM feed_scripts
                  ORDER BY feed_id, run_order, instance_id",
                &[],
            )
            .await?
            {
                attached
                    .entry(row.i64("feed_id")? as u32)
                    .or_default()
                    .push(row.text("instance_id")?);
            }
            rows.into_iter()
                .map(|row| {
                    let mut feed = rss_feed_from_sql(row)?;
                    feed.password = maybe_decrypt(encryption_key.as_ref(), feed.password);
                    feed.scripts = attached.remove(&feed.id).unwrap_or_default();
                    Ok(feed)
                })
                .collect()
        })
    }

    pub fn get_rss_feed(&self, id: u32) -> Result<Option<RssFeedRow>, StateError> {
        use crate::persistence::encryption::maybe_decrypt;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let Some(row) = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("{RSS_FEED_SELECT} WHERE id = {{}}"),
                &[SqlArg::I64(i64::from(id))],
            )
            .await?
            else {
                return Ok(None);
            };
            let mut feed = rss_feed_from_sql(row)?;
            feed.password = maybe_decrypt(encryption_key.as_ref(), feed.password);
            feed.scripts = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT instance_id FROM feed_scripts WHERE feed_id = {}
                  ORDER BY run_order, instance_id",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?
            .into_iter()
            .map(|row| row.text("instance_id"))
            .collect::<Result<_, _>>()?;
            Ok(Some(feed))
        })
    }

    /// When each feed is next due, without loading or decrypting the feeds.
    pub(crate) fn list_rss_feed_schedules(
        &self,
    ) -> Result<Vec<crate::rss::model::RssFeedSchedule>, StateError> {
        let mut cached = self
            .rss_schedule_cache
            .rows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(rows) = cached.as_ref() {
            return Ok(rows.clone());
        }
        let datastore = self.datastore();
        let rows: Vec<_> = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, enabled, poll_interval_secs, last_polled_at
                   FROM rss_feeds ORDER BY id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                Ok(crate::rss::model::RssFeedSchedule {
                    id: row.i32("id")? as u32,
                    enabled: row.bool("enabled")?,
                    poll_interval_secs: row.i32("poll_interval_secs")? as u32,
                    last_polled_at: row.opt_i64("last_polled_at")?,
                })
            })
            .collect()
        })?;
        #[cfg(test)]
        self.rss_schedule_cache
            .loads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *cached = Some(rows.clone());
        Ok(rows)
    }

    pub(crate) fn invalidate_rss_schedules(&self) {
        *self
            .rss_schedule_cache
            .rows
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.rss_schedule_cache
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub fn list_rss_rules(&self, feed_id: u32) -> Result<Vec<RssRuleRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("{RSS_RULE_SELECT} WHERE feed_id = {{}} ORDER BY sort_order, id"),
                &[SqlArg::I64(i64::from(feed_id))],
            )
            .await?;
            rows.into_iter().map(rss_rule_from_sql).collect()
        })
    }

    pub fn get_rss_rule(&self, id: u32) -> Result<Option<RssRuleRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("{RSS_RULE_SELECT} WHERE id = {{}}"),
                &[SqlArg::I64(i64::from(id))],
            )
            .await?
            .map(rss_rule_from_sql)
            .transpose()
        })
    }

    pub fn rss_seen_item_exists(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let item_id = item_id.to_string();
        self.run_sql_blocking_read(async move {
            let exists = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS exists_value FROM rss_seen_items WHERE feed_id = {} AND item_id = {}",
                &[SqlArg::I64(i64::from(feed_id)), SqlArg::Text(item_id)],
            )
            .await?;
            Ok(exists.is_some())
        })
    }

    pub fn list_rss_seen_items(
        &self,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItemRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut sql = String::from(RSS_SEEN_SELECT);
            let mut args = Vec::new();
            if let Some(feed_id) = feed_id {
                sql.push_str(" WHERE feed_id = {}");
                args.push(SqlArg::I64(i64::from(feed_id)));
            }
            sql.push_str(" ORDER BY seen_at DESC");
            if let Some(limit) = limit {
                sql.push_str(" LIMIT {}");
                args.push(SqlArg::I64(i64::from(limit)));
            }
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter().map(map_seen_item_row).collect()
        })
    }
}

fn rss_feed_from_sql(row: SqlRow) -> Result<RssFeedRow, StateError> {
    Ok(RssFeedRow {
        id: row.i32("id")? as u32,
        name: row.text("name")?,
        url: row.text("url")?,
        enabled: row.bool("enabled")?,
        poll_interval_secs: row.i32("poll_interval_secs")? as u32,
        username: row.opt_text("username")?,
        password: row.opt_text("password")?,
        default_category: row.opt_text("default_category")?,
        default_metadata: decode_metadata(row.opt_text("default_metadata")?),
        etag: row.opt_text("etag")?,
        last_modified: row.opt_text("last_modified")?,
        last_polled_at: row.opt_i64("last_polled_at")?,
        last_success_at: row.opt_i64("last_success_at")?,
        last_error: row.opt_text("last_error")?,
        consecutive_failures: row.i32("consecutive_failures")? as u32,
        scripts: Vec::new(),
    })
}

fn rss_rule_from_sql(row: SqlRow) -> Result<RssRuleRow, StateError> {
    Ok(RssRuleRow {
        id: row.i32("id")? as u32,
        feed_id: row.i32("feed_id")? as u32,
        sort_order: row.i32("sort_order")?,
        enabled: row.bool("enabled")?,
        action: parse_action_sql(row.text("action")?)?,
        title_regex: row.opt_text("title_regex")?,
        item_categories: decode_categories(row.opt_text("item_categories")?),
        min_size_bytes: row.opt_i64("min_size_bytes")?.map(|value| value as u64),
        max_size_bytes: row.opt_i64("max_size_bytes")?.map(|value| value as u64),
        category_override: row.opt_text("category_override")?,
        metadata: decode_metadata(row.opt_text("metadata")?),
    })
}
