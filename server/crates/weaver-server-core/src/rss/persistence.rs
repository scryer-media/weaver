// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, max_rows_for_tx};
use crate::rss::record::{RssFeedRow, RssRuleRow, RssSeenItemRow};

use super::repository::{encode_categories, encode_metadata};

use crate::persistence::encryption::EncryptionKey;
use crate::torrents::sealed::SEAL_PAGE;

// What a seen item's stored id starts with: `wvr1:`, then its keyed digest
// in hex.
const SEEN_KEY_PREFIX: &str = "wvr1:";
const SEEN_KEY_LEN: usize = SEEN_KEY_PREFIX.len() + 64;
const SEEN_KEY_DOMAIN: &str = "weaver/rss-seen-item/v1";

// A seen item's id as it is stored. A feed's item id is often the item's
// link, and a private tracker's link carries the passkey, so the id is kept
// as its keyed digest: the same for the same id, which is all a seen lookup
// needs, and no use without the instance key. With no key a store keeps,
// the id is stored as the feed gave it, as it was before.
pub(crate) fn seen_item_key(key: Option<&EncryptionKey>, item_id: &str) -> String {
    match key {
        Some(key) => format!(
            "{SEEN_KEY_PREFIX}{}",
            hex::encode(crate::persistence::encryption::keyed_digest(
                key,
                SEEN_KEY_DOMAIN,
                item_id.as_bytes()
            ))
        ),
        None => item_id.to_owned(),
    }
}

// A link as it is stored: encrypted with the instance key, whatever it
// starts with. Under a key held in memory only it is stored as it is, as it
// was before links were encrypted: one encrypted under that key would be
// lost at the next start.
fn sealed_link(key: Option<&EncryptionKey>, link: &str) -> Result<String, StateError> {
    if link.is_empty() {
        return Ok(String::new());
    }
    match key {
        None => Err(StateError::Database(
            "encryption key is required to store secrets".to_string(),
        )),
        Some(key) if key.is_ephemeral() => Ok(link.to_owned()),
        Some(key) => {
            crate::persistence::encryption::encrypt_value(key, link).map_err(StateError::Database)
        }
    }
}

impl Database {
    // The RSS part of [`Database::seal_stored_secrets`]: every feed URL and
    // seen item link stored in the clear encrypted, and every seen item id
    // replaced by its keyed digest. Each step selects in SQL only the rows
    // it has yet to do, a page at a time, each page in one write
    // transaction, so a pass cut short goes on at the next start.
    pub(crate) fn seal_rss_links(&self, key: &EncryptionKey) -> Result<(), StateError> {
        use crate::persistence::encryption::{decrypt_value, encrypt_value, is_encrypted};

        let key = key.clone();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let mut after = i64::MIN;
            loop {
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT id, url FROM rss_feeds \
                      WHERE id > {} AND url <> '' AND url NOT LIKE 'enc:v1:%' \
                      ORDER BY id LIMIT {}",
                    &[SqlArg::I64(after), SqlArg::I64(SEAL_PAGE)],
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = last.i64("id")?;
                let mut page = Vec::new();
                for row in &rows {
                    page.push((
                        row.i64("id")?,
                        encrypt_value(&key, &row.text("url")?).map_err(StateError::Database)?,
                    ));
                }
                let sql = format!(
                    "UPDATE rss_feeds SET url = CASE id {}END WHERE id IN ({})",
                    "WHEN {} THEN {} ".repeat(page.len()),
                    vec!["{}"; page.len()].join(", ")
                );
                let mut args = Vec::with_capacity(page.len() * 3);
                for (id, url) in &page {
                    args.extend([SqlArg::I64(*id), SqlArg::Text(url.clone())]);
                }
                args.extend(page.iter().map(|(id, _)| SqlArg::I64(*id)));
                SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            }

            // Each seen item whose id is not yet its digest: taken out and
            // put back under the digest, its link encrypted, so an item
            // seen before stays seen. The rows put back are passed by: the
            // read names only ids of another shape.
            let mut after: Option<(i64, String)> = None;
            loop {
                let (filter, mut args) = match &after {
                    None => (String::new(), Vec::new()),
                    Some((feed, item)) => (
                        "AND (feed_id > {} OR (feed_id = {} AND item_id > {}))".to_string(),
                        vec![
                            SqlArg::I64(*feed),
                            SqlArg::I64(*feed),
                            SqlArg::Text(item.clone()),
                        ],
                    ),
                };
                args.push(SqlArg::I64(SEAL_PAGE));
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT feed_id, item_id, item_title, published_at, size_bytes, decision,
                                seen_at, job_id, item_url, error
                           FROM rss_seen_items
                          WHERE NOT (item_id LIKE '{SEEN_KEY_PREFIX}%' AND length(item_id) = {SEEN_KEY_LEN})
                                {filter}
                          ORDER BY feed_id, item_id LIMIT {{}}"
                    ),
                    &args,
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = Some((last.i64("feed_id")?, last.text("item_id")?));
                let mut gone = Vec::with_capacity(rows.len());
                let mut page = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut item = super::repository::map_seen_item_row(row)?;
                    gone.push((i64::from(item.feed_id), item.item_id.clone()));
                    // A link this key already encrypted goes back as it is.
                    let link = item.item_url.take().filter(|url| !url.is_empty());
                    let link = match link {
                        Some(url) if is_encrypted(&url) && decrypt_value(&key, &url).is_ok() => {
                            Some(url)
                        }
                        Some(url) => {
                            Some(encrypt_value(&key, &url).map_err(StateError::Database)?)
                        }
                        None => None,
                    };
                    page.push(seen_row_args(
                        &item,
                        seen_item_key(Some(&key), &item.item_id),
                        link,
                    ));
                }
                SqlRuntime::run_in_transaction(&datastore, "seal_rss_links", |tx| {
                    let (gone, page) = (gone.clone(), page.clone());
                    Box::pin(async move {
                        for chunk in gone.chunks(max_rows_for_tx(tx, 2)) {
                            let named =
                                vec!["(feed_id = {} AND item_id = {})"; chunk.len()].join(" OR ");
                            let mut args = Vec::with_capacity(chunk.len() * 2);
                            for (feed_id, item_id) in chunk {
                                args.extend([SqlArg::I64(*feed_id), SqlArg::Text(item_id.clone())]);
                            }
                            tx.execute(&format!("DELETE FROM rss_seen_items WHERE {named}"), &args)
                                .await?;
                        }
                        insert_rss_seen_rows(tx, &page).await
                    })
                })
                .await?;
            }
            Ok(())
        })
    }

    pub fn insert_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        self.insert_rss_feed_with_routing(feed, None)
    }

    pub fn insert_rss_feed_with_routing(
        &self,
        feed: &RssFeedRow,
        routing: Option<&crate::proxies::RoutingPolicy>,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &feed.password)
            .map_err(StateError::Database)?;
        let encrypted_url = sealed_feed_url(self, feed)?;
        let args = rss_feed_args(feed, metadata, encrypted_password, encrypted_url);
        let routing = routing.cloned();
        let consumer = crate::proxies::Consumer::Rss(feed.id);
        let (feed_id, scripts) = (feed.id, feed.scripts.clone());
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_consumer_routing", |tx| {
                let args = args.clone(); let routing = routing.clone();
                let scripts = scripts.clone();
                Box::pin(async move {
            let before = crate::proxies::lock_network_asks(tx).await?;
            tx.execute(
                "INSERT INTO rss_feeds
                    (id, name, url, enabled, poll_interval_secs, username, password, default_category,
                     default_metadata, etag, last_modified, last_polled_at, last_success_at, last_error,
                     consecutive_failures)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            crate::post_processing::instances::set_feed_scripts_tx(tx, feed_id, &scripts).await?;
            crate::proxies::persistence::write_routing(tx, consumer, routing.as_ref(), &before)
                .await?;
            Ok(())
                })
            }).await
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }

    pub fn update_rss_feed(&self, feed: &RssFeedRow) -> Result<(), StateError> {
        self.update_rss_feed_with_routing(feed, None)
    }

    pub fn update_rss_feed_with_routing(
        &self,
        feed: &RssFeedRow,
        routing: Option<&crate::proxies::RoutingPolicy>,
    ) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let metadata = encode_metadata(&feed.default_metadata)?;
        let encrypted_password = encrypt_secret_for_write(self.encryption_key(), &feed.password)
            .map_err(StateError::Database)?;
        let encrypted_url = sealed_feed_url(self, feed)?;
        let mut args = rss_feed_args(feed, metadata, encrypted_password, encrypted_url);
        let id = args.remove(0);
        args.push(id);
        let routing = routing.cloned();
        let consumer = crate::proxies::Consumer::Rss(feed.id);
        let (feed_id, scripts) = (feed.id, feed.scripts.clone());
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_consumer_routing", |tx| {
                let args = args.clone();
                let routing = routing.clone();
                let scripts = scripts.clone();
                Box::pin(async move {
                    let before = crate::proxies::lock_network_asks(tx).await?;
                    tx.execute(
                        "UPDATE rss_feeds
                    SET name = {}, url = {}, enabled = {}, poll_interval_secs = {}, username = {},
                        password = {}, default_category = {}, default_metadata = {}, etag = {},
                        last_modified = {}, last_polled_at = {}, last_success_at = {},
                        last_error = {}, consecutive_failures = {}
                  WHERE id = {}",
                        &args,
                    )
                    .await?;
                    crate::post_processing::instances::set_feed_scripts_tx(tx, feed_id, &scripts)
                        .await?;
                    crate::proxies::persistence::write_routing(
                        tx,
                        consumer,
                        routing.as_ref(),
                        &before,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }

    pub fn delete_rss_feed(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_routed_consumer", |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
                        &[],
                    )
                    .await?;
                    tx.execute(
                        "DELETE FROM script_outputs WHERE job_id IS NULL AND event = {}",
                        &[SqlArg::Text(format!("feed:{id}"))],
                    )
                    .await?;
                    let changed = tx
                        .execute(
                            "DELETE FROM rss_feeds WHERE id = {}",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?;
                    tx.execute(
                        "DELETE FROM feed_scripts WHERE feed_id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    tx.execute(
                        "DELETE FROM proxy_routes WHERE consumer = {}",
                        &[SqlArg::Text(crate::proxies::Consumer::Rss(id).key())],
                    )
                    .await?;
                    Ok(changed > 0)
                })
            })
            .await
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }

    pub fn insert_rss_rule(&self, rule: &RssRuleRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = rss_rule_args(rule)?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
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
                datastore.write_exec(),
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
                datastore.write_exec(),
                "DELETE FROM rss_rules WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    pub fn insert_rss_seen_item(&self, item: &RssSeenItemRow) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = rss_seen_item_args(item, self.encryption_key())?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
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

    // Forget one seen item, named by its id as the seen list gives it, or
    // as the feed does.
    pub fn delete_rss_seen_item(&self, feed_id: u32, item_id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let keyed = seen_item_key(self.sealing_key(), item_id);
        let item_id = item_id.to_string();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "DELETE FROM rss_seen_items WHERE feed_id = {} AND item_id IN ({}, {})",
                &[
                    SqlArg::I64(i64::from(feed_id)),
                    SqlArg::Text(item_id),
                    SqlArg::Text(keyed),
                ],
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
                        datastore.write_exec(),
                        "DELETE FROM rss_seen_items WHERE feed_id = {}",
                        &[SqlArg::I64(i64::from(feed_id))],
                    )
                    .await?
                }
                None => {
                    SqlRuntime::execute(datastore.write_exec(), "DELETE FROM rss_seen_items", &[])
                        .await?
                }
            };
            Ok(changed)
        })
    }

    /// Items held while torrents were off are taken up again: their rows
    /// go, and each feed that held one is read whole at its next poll, not
    /// answered "not modified", so the items are judged anew. Whether any
    /// were held. Nothing is written when none were. Both statements, and the
    /// read before them, are served by the partial index on held items: the
    /// state is a literal, never a bound parameter.
    pub fn release_rss_waiting_items(&self) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let held = self.run_sql_blocking_read(async move {
            let held = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS held FROM rss_seen_items WHERE decision = 'waiting' LIMIT 1",
                &[],
            )
            .await?;
            Ok(held.is_some())
        })?;
        if !held {
            return Ok(false);
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "release_rss_waiting_items", |tx| {
                Box::pin(async move {
                    tx.execute(
                        "UPDATE rss_feeds SET etag = NULL, last_modified = NULL
                          WHERE id IN (SELECT feed_id FROM rss_seen_items
                                        WHERE decision = 'waiting')",
                        &[],
                    )
                    .await?;
                    tx.execute("DELETE FROM rss_seen_items WHERE decision = 'waiting'", &[])
                        .await?;
                    Ok(())
                })
            })
            .await
        })?;
        Ok(true)
    }

    /// One feed poll's seen items and its success, in one transaction: the
    /// items in multi-row statements of bind-sized chunks.
    pub fn record_rss_feed_sync(
        &self,
        feed_id: u32,
        items: &[RssSeenItemRow],
        polled_at: i64,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let rows = items
            .iter()
            .map(|item| rss_seen_item_args(item, self.encryption_key()))
            .collect::<Result<Vec<_>, _>>()?;
        let etag = etag.map(str::to_string);
        let last_modified = last_modified.map(str::to_string);
        let result = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "record_rss_feed_sync", |tx| {
                let rows = rows.clone();
                let etag = etag.clone();
                let last_modified = last_modified.clone();
                Box::pin(async move {
                    insert_rss_seen_rows(tx, &rows).await?;
                    tx.execute(
                        RSS_POLL_SUCCESS_SQL,
                        &poll_success_args(feed_id, polled_at, etag, last_modified),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }

    /// Seen items recorded without a poll's success: those of a poll cut
    /// short, written together.
    pub fn insert_rss_seen_items(&self, items: &[RssSeenItemRow]) -> Result<(), StateError> {
        if items.is_empty() {
            return Ok(());
        }
        let datastore = self.datastore();
        let rows = items
            .iter()
            .map(|item| rss_seen_item_args(item, self.encryption_key()))
            .collect::<Result<Vec<_>, _>>()?;
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_rss_seen_items", |tx| {
                let rows = rows.clone();
                Box::pin(async move { insert_rss_seen_rows(tx, &rows).await })
            })
            .await
        })
    }

    /// Rows seen before `cutoff_ts` go. An indexed read first, so a pass
    /// with none past retention writes nothing.
    pub fn purge_old_rss_seen_items(&self, cutoff_ts: i64) -> Result<u64, StateError> {
        let datastore = self.datastore();
        let due = self.run_sql_blocking_read(async move {
            let due = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT 1 AS due FROM rss_seen_items WHERE seen_at < {} LIMIT 1",
                &[SqlArg::I64(cutoff_ts)],
            )
            .await?;
            Ok(due.is_some())
        })?;
        if !due {
            return Ok(0);
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
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
        let result = self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                RSS_POLL_SUCCESS_SQL,
                &poll_success_args(feed_id, polled_at, etag, last_modified),
            )
            .await?;
            Ok(())
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }

    pub fn record_rss_poll_failure(
        &self,
        feed_id: u32,
        polled_at: i64,
        error: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let error = error.to_string();
        let result = self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
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
        });
        if result.is_ok() {
            self.invalidate_rss_schedules();
        }
        result
    }
}

// A feed's URL as it is stored: encrypted, as its password is, for a
// private tracker's feed URL carries the passkey.
fn sealed_feed_url(db: &crate::Database, feed: &RssFeedRow) -> Result<String, StateError> {
    sealed_link(db.encryption_key(), &feed.url)
}

fn rss_feed_args(
    feed: &RssFeedRow,
    metadata: Option<String>,
    encrypted_password: Option<String>,
    encrypted_url: String,
) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(i64::from(feed.id)),
        SqlArg::Text(feed.name.clone()),
        SqlArg::Text(encrypted_url),
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

const RSS_POLL_SUCCESS_SQL: &str = "UPDATE rss_feeds
    SET last_polled_at = {}, last_success_at = {}, last_error = NULL,
        consecutive_failures = 0, etag = {}, last_modified = {}
  WHERE id = {}";

fn poll_success_args(
    feed_id: u32,
    polled_at: i64,
    etag: Option<String>,
    last_modified: Option<String>,
) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(polled_at),
        SqlArg::I64(polled_at),
        SqlArg::OptText(etag),
        SqlArg::OptText(last_modified),
        SqlArg::I64(i64::from(feed_id)),
    ]
}

const RSS_SEEN_COLUMNS: usize = 10;

// The seen rows, upserted as one statement per bind-sized chunk.
async fn insert_rss_seen_rows(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    rows: &[Vec<SqlArg>],
) -> Result<(), StateError> {
    for chunk in rows.chunks(max_rows_for_tx(tx, RSS_SEEN_COLUMNS)) {
        let values = vec!["({}, {}, {}, {}, {}, {}, {}, {}, {}, {})"; chunk.len()].join(", ");
        let args = chunk.iter().flatten().cloned().collect::<Vec<_>>();
        tx.execute(
            &format!(
                "INSERT INTO rss_seen_items
                    (feed_id, item_id, item_title, published_at, size_bytes, decision, seen_at,
                     job_id, item_url, error)
                 VALUES {values}
                 ON CONFLICT(feed_id, item_id) DO UPDATE SET
                    item_title = excluded.item_title,
                    published_at = excluded.published_at,
                    size_bytes = excluded.size_bytes,
                    decision = excluded.decision,
                    seen_at = excluded.seen_at,
                    job_id = excluded.job_id,
                    item_url = excluded.item_url,
                    error = excluded.error"
            ),
            &args,
        )
        .await?;
    }
    Ok(())
}

// A seen item's row as a feed's item makes it: its link encrypted and its
// id its keyed digest, as a private tracker's link carries the passkey and
// a feed often gives the link as the id.
fn rss_seen_item_args(
    item: &RssSeenItemRow,
    key: Option<&EncryptionKey>,
) -> Result<Vec<SqlArg>, StateError> {
    let item_url = item
        .item_url
        .as_deref()
        .map(|url| sealed_link(key, url))
        .transpose()?;
    let kept = key.filter(|key| !key.is_ephemeral());
    Ok(seen_row_args(
        item,
        seen_item_key(kept, &item.item_id),
        item_url,
    ))
}

// A seen item's row under the id and the link it is stored with.
fn seen_row_args(item: &RssSeenItemRow, item_id: String, item_url: Option<String>) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(i64::from(item.feed_id)),
        SqlArg::Text(item_id),
        SqlArg::Text(item.item_title.clone()),
        SqlArg::OptI64(item.published_at),
        SqlArg::OptI64(item.size_bytes.map(|value| value as i64)),
        SqlArg::Text(item.decision.as_str().to_owned()),
        SqlArg::I64(item.seen_at),
        SqlArg::OptI64(item.job_id.map(|value| value as i64)),
        SqlArg::OptText(item_url),
        SqlArg::OptText(item.error.clone()),
    ]
}
