// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};

use super::repository::{decode_categories, decode_metadata, map_seen_item_row, parse_action_sql};

pub trait ScheduleStore: std::ops::Deref<Target = Database> {
    fn schedule_cache(&self) -> &ScheduleCache;
}

pub struct ScheduleCache {
    rows: std::sync::Mutex<Option<Vec<super::model::RssFeedSchedule>>>,
    pub changed: tokio::sync::watch::Sender<u64>,
    #[cfg(any(test, feature = "test-support"))]
    pub loads: std::sync::atomic::AtomicU64,
}

impl Default for ScheduleCache {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            changed: tokio::sync::watch::channel(0).0,
            #[cfg(any(test, feature = "test-support"))]
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

pub trait RssQueries {
    fn next_rss_feed_id(&self) -> Result<u32, StateError>;
    fn next_rss_rule_id(&self) -> Result<u32, StateError>;
    fn list_rss_feeds(&self) -> Result<Vec<RssFeedRow>, StateError>;
    fn get_rss_feed(&self, id: u32) -> Result<Option<RssFeedRow>, StateError>;
    fn list_rss_rules(&self, feed_id: u32) -> Result<Vec<RssRuleRow>, StateError>;
    fn get_rss_rule(&self, id: u32) -> Result<Option<RssRuleRow>, StateError>;
    fn rss_seen_item_key(&self, item_id: &str) -> String;
    fn rss_seen_item_exists(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError>;
    fn seen_rss_item_ids(
        &self,
        feed_id: u32,
        item_ids: &[String],
    ) -> Result<std::collections::HashSet<String>, StateError>;
    fn list_rss_seen_items(
        &self,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItemRow>, StateError>;
    fn list_rss_seen_items_per_feed(&self, limit: u32) -> Result<Vec<RssSeenItemRow>, StateError>;
}

impl RssQueries for Database {
    fn next_rss_feed_id(&self) -> Result<u32, StateError> {
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

    fn next_rss_rule_id(&self) -> Result<u32, StateError> {
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

    fn list_rss_feeds(&self) -> Result<Vec<RssFeedRow>, StateError> {
        use weaver_server_db::persistence::encryption::maybe_decrypt;

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
                    feed.url =
                        maybe_decrypt(encryption_key.as_ref(), Some(feed.url)).unwrap_or_default();
                    feed.scripts = attached.remove(&feed.id).unwrap_or_default();
                    Ok(feed)
                })
                .collect()
        })
    }

    fn get_rss_feed(&self, id: u32) -> Result<Option<RssFeedRow>, StateError> {
        use weaver_server_db::persistence::encryption::maybe_decrypt;

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
            feed.url = maybe_decrypt(encryption_key.as_ref(), Some(feed.url)).unwrap_or_default();
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

    fn list_rss_rules(&self, feed_id: u32) -> Result<Vec<RssRuleRow>, StateError> {
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

    fn get_rss_rule(&self, id: u32) -> Result<Option<RssRuleRow>, StateError> {
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

    /// The id a seen item is stored and listed under: the BLAKE3 digest of
    /// the id its feed gives it.
    fn rss_seen_item_key(&self, item_id: &str) -> String {
        super::persistence::seen_item_key(super::persistence::SeenId::Id(item_id))
    }

    fn rss_seen_item_exists(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let [keyed, item_id] = super::persistence::seen_item_keys(item_id);
        self.run_sql_blocking_read(async move {
            let exists = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                &format!("{} LIMIT 1", seen_lookup_sql(2)),
                &[
                    SqlArg::I64(i64::from(feed_id)),
                    SqlArg::Text(keyed),
                    SqlArg::Text(item_id),
                ],
            )
            .await?;
            Ok(exists.is_some())
        })
    }

    /// Which of `item_ids` the feed has seen: one statement per bind-sized
    /// chunk, served by the table's key. The ids are the feed's own; each
    /// is looked up by the digest it is stored as, and by itself, as a row
    /// stored before the digest holds it until the sealing pass.
    fn seen_rss_item_ids(
        &self,
        feed_id: u32,
        item_ids: &[String],
    ) -> Result<std::collections::HashSet<String>, StateError> {
        if item_ids.is_empty() {
            return Ok(Default::default());
        }
        let datastore = self.datastore();
        // Each stored id, and the feed's id it stands for.
        let stored: std::collections::HashMap<String, String> = item_ids
            .iter()
            .flat_map(|item_id| {
                super::persistence::seen_item_keys(item_id).map(|stored| (stored, item_id.clone()))
            })
            .collect();
        let item_ids: Vec<String> = stored.keys().cloned().collect();
        self.run_sql_blocking_read(async move {
            let mut seen = std::collections::HashSet::new();
            // One bind of each chunk is the feed's id.
            let per_chunk = (weaver_server_db::persistence::sql_runtime::max_rows_for_engine(
                datastore.engine(),
                1,
            ) - 1)
                .max(1);
            for chunk in item_ids.chunks(per_chunk) {
                let mut args = vec![SqlArg::I64(i64::from(feed_id))];
                args.extend(chunk.iter().cloned().map(SqlArg::Text));
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &seen_lookup_sql(chunk.len()),
                    &args,
                )
                .await?;
                for row in rows {
                    if let Some(item_id) = stored.get(&row.text("item_id")?) {
                        seen.insert(item_id.clone());
                    }
                }
            }
            Ok(seen)
        })
    }

    fn list_rss_seen_items(
        &self,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItemRow>, StateError> {
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut sql = String::from(RSS_SEEN_SELECT);
            let mut args = Vec::new();
            if let Some(feed_id) = feed_id {
                sql.push_str(" WHERE feed_id = {}");
                args.push(SqlArg::I64(i64::from(feed_id)));
            }
            sql.push_str(" ORDER BY seen_at DESC, item_id DESC");
            if let Some(limit) = limit {
                sql.push_str(" LIMIT {}");
                args.push(SqlArg::I64(i64::from(limit)));
            }
            let rows = SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await?;
            rows.into_iter()
                .map(|row| {
                    let mut item = map_seen_item_row(row)?;
                    item.item_url = super::persistence::open_seen_link(key.as_ref(), item.item_url);
                    Ok(item)
                })
                .collect()
        })
    }

    /// The `limit` items each feed saw last: feed by feed, each feed's items
    /// in the order `list_rss_seen_items` gives for that feed alone. One
    /// statement per bind-sized chunk of feeds, each feed's part read from
    /// the newest of its items and stopped at `limit`, so the read grows
    /// with the answer, not with the history kept.
    fn list_rss_seen_items_per_feed(&self, limit: u32) -> Result<Vec<RssSeenItemRow>, StateError> {
        let key = self.encryption_key().cloned();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let feed_ids = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id FROM rss_feeds ORDER BY id",
                &[],
            )
            .await?
            .iter()
            .map(|row| row.i64("id"))
            .collect::<Result<Vec<_>, _>>()?;
            // Two binds for each feed: its id and the limit.
            let per_chunk = weaver_server_db::persistence::sql_runtime::max_rows_for_engine(
                datastore.engine(),
                2,
            );
            let mut items = Vec::new();
            for chunk in feed_ids.chunks(per_chunk) {
                let mut args = Vec::with_capacity(chunk.len() * 2);
                for &feed_id in chunk {
                    args.push(SqlArg::I64(feed_id));
                    args.push(SqlArg::I64(i64::from(limit)));
                }
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &latest_per_feed_sql(chunk.len()),
                    &args,
                )
                .await?;
                for row in rows {
                    let mut item = map_seen_item_row(row)?;
                    item.item_url = super::persistence::open_seen_link(key.as_ref(), item.item_url);
                    items.push(item);
                }
            }
            Ok(items)
        })
    }
}

// The newest `{}` items of each of `feeds` feeds, feed by feed, each part
// served by `idx_rss_seen_feed_seen_at` and stopped at its limit.
fn latest_per_feed_sql(feeds: usize) -> String {
    let part = format!(
        "SELECT * FROM ({RSS_SEEN_SELECT} WHERE feed_id = {{}} \
         ORDER BY seen_at DESC, item_id DESC LIMIT {{}}) AS feed_part"
    );
    let parts = vec![part; feeds].join(" UNION ALL ");
    format!("SELECT * FROM ({parts}) AS latest ORDER BY feed_id, seen_at DESC, item_id DESC")
}

pub trait RssInternalQueries {
    fn list_rss_feed_schedules(
        &self,
    ) -> Result<Vec<crate::rss::model::RssFeedSchedule>, StateError>;
    fn invalidate_rss_schedules(&self);
    fn wake_rss_scheduler(&self);
}

impl<T: ScheduleStore> RssInternalQueries for T {
    // When each feed is next due, without loading or decrypting the feeds.
    fn list_rss_feed_schedules(
        &self,
    ) -> Result<Vec<crate::rss::model::RssFeedSchedule>, StateError> {
        let mut cached = self
            .schedule_cache()
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
        #[cfg(any(test, feature = "test-support"))]
        self.schedule_cache()
            .loads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *cached = Some(rows.clone());
        Ok(rows)
    }

    fn invalidate_rss_schedules(&self) {
        *self
            .schedule_cache()
            .rows
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.schedule_cache()
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    // Re-run the due check without discarding the cached schedules.
    fn wake_rss_scheduler(&self) {
        self.schedule_cache()
            .changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

// Which of `count` stored ids a feed has seen: an equality on the feed and
// on each id, which the table's key `(feed_id, item_id)` serves.
pub fn seen_lookup_sql(count: usize) -> String {
    format!(
        "SELECT item_id FROM rss_seen_items WHERE feed_id = {{}} AND item_id IN ({})",
        vec!["{}"; count].join(", ")
    )
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
