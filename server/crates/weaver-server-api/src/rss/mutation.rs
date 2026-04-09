use async_graphql::{Context, Object, Result};
use regex::Regex;

use crate::auth::AdminGuard;
use crate::jobs::types::MetadataInput;
use crate::rss::RssService;
use crate::rss::types::{
    RssFeed, RssFeedInput, RssRule, RssRuleActionGql, RssRuleInput, RssSyncReport,
};
use weaver_server_core::{Database, RssFeedRow, RssRuleRow};

#[derive(Default)]
pub(crate) struct RssMutation;

#[Object]
impl RssMutation {
    /// Add a new RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn add_rss_feed(&self, ctx: &Context<'_>, input: RssFeedInput) -> Result<RssFeed> {
        validate_feed_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        let feed = tokio::task::spawn_blocking(move || {
            let id = db.next_rss_feed_id()?;
            let row = rss_feed_row_from_create(id, input);
            db.insert_rss_feed(&row)?;
            let rules = db
                .list_rss_rules(row.id)?
                .iter()
                .map(RssRule::from_row)
                .collect();
            Ok::<_, weaver_server_core::StateError>(RssFeed::from_row(&row, rules))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(feed)
    }
    /// Update an RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn update_rss_feed(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: RssFeedInput,
    ) -> Result<RssFeed> {
        validate_feed_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        let feed = tokio::task::spawn_blocking(move || {
            let Some(existing) = db.get_rss_feed(id)? else {
                return Err(weaver_server_core::StateError::Database(format!(
                    "RSS feed {id} not found"
                )));
            };
            let row = rss_feed_row_from_update(existing, input);
            db.update_rss_feed(&row)?;
            let rules = db
                .list_rss_rules(row.id)?
                .iter()
                .map(RssRule::from_row)
                .collect();
            Ok::<_, weaver_server_core::StateError>(RssFeed::from_row(&row, rules))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(feed)
    }
    /// Delete an RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_feed(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_rss_feed(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(deleted)
    }
    /// Add a new RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn add_rss_rule(
        &self,
        ctx: &Context<'_>,
        feed_id: u32,
        input: RssRuleInput,
    ) -> Result<RssRule> {
        validate_rule_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            if db.get_rss_feed(feed_id)?.is_none() {
                return Err(weaver_server_core::StateError::Database(format!(
                    "RSS feed {feed_id} not found"
                )));
            }
            let id = db.next_rss_rule_id()?;
            let row = rss_rule_row_from_input(id, feed_id, input);
            db.insert_rss_rule(&row)?;
            Ok::<_, weaver_server_core::StateError>(RssRule::from_row(&row))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
    /// Update an RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn update_rss_rule(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: RssRuleInput,
    ) -> Result<RssRule> {
        validate_rule_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let Some(existing) = db.get_rss_rule(id)? else {
                return Err(weaver_server_core::StateError::Database(format!(
                    "RSS rule {id} not found"
                )));
            };
            let row = rss_rule_row_from_input(id, existing.feed_id, input);
            db.update_rss_rule(&row)?;
            Ok::<_, weaver_server_core::StateError>(RssRule::from_row(&row))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
    /// Delete an RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_rule(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_rss_rule(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(deleted)
    }
    /// Run RSS sync immediately for all enabled feeds or one specific feed.
    #[graphql(guard = "AdminGuard")]
    async fn run_rss_sync(&self, ctx: &Context<'_>, feed_id: Option<u32>) -> Result<RssSyncReport> {
        let rss = ctx.data::<RssService>()?;
        let report = match feed_id {
            Some(feed_id) => rss.run_feed_sync(feed_id).await,
            None => rss.run_all_sync().await,
        }
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(RssSyncReport::from_domain(&report))
    }
    /// Forget one seen RSS item so it can be reconsidered on a future sync.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_seen_item(
        &self,
        ctx: &Context<'_>,
        feed_id: u32,
        item_id: String,
    ) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.delete_rss_seen_item(feed_id, &item_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
    /// Clear seen RSS items, either globally or for one feed.
    #[graphql(guard = "AdminGuard")]
    async fn clear_rss_seen_items(&self, ctx: &Context<'_>, feed_id: Option<u32>) -> Result<u32> {
        let db = ctx.data::<Database>()?.clone();
        let cleared = tokio::task::spawn_blocking(move || db.clear_rss_seen_items(feed_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(cleared as u32)
    }

    // ── Schedules ───────────────────────────────────────────────────────
}

fn validate_feed_input(input: &RssFeedInput) -> Result<()> {
    let url = reqwest::Url::parse(&input.url)
        .map_err(|e| async_graphql::Error::new(format!("invalid RSS feed URL: {e}")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(async_graphql::Error::new(format!(
                "unsupported RSS feed URL scheme: {other}"
            )));
        }
    }
    if let Some(interval) = input.poll_interval_secs
        && interval == 0
    {
        return Err(async_graphql::Error::new(
            "poll_interval_secs must be greater than 0",
        ));
    }
    Ok(())
}

fn validate_rule_input(input: &RssRuleInput) -> Result<()> {
    if let Some(pattern) = &input.title_regex {
        Regex::new(pattern)
            .map_err(|e| async_graphql::Error::new(format!("invalid title_regex: {e}")))?;
    }
    if let (Some(min), Some(max)) = (input.min_size_bytes, input.max_size_bytes)
        && min > max
    {
        return Err(async_graphql::Error::new(
            "min_size_bytes cannot be greater than max_size_bytes",
        ));
    }
    Ok(())
}

fn rss_feed_row_from_create(id: u32, input: RssFeedInput) -> RssFeedRow {
    RssFeedRow {
        id,
        name: input.name,
        url: input.url,
        enabled: input.enabled,
        poll_interval_secs: input.poll_interval_secs.unwrap_or(900),
        username: normalize_optional_string(input.username),
        password: normalize_optional_string(input.password),
        default_category: normalize_optional_string(input.default_category),
        default_metadata: metadata_input_to_pairs(input.default_metadata),
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    }
}

fn rss_feed_row_from_update(existing: RssFeedRow, input: RssFeedInput) -> RssFeedRow {
    RssFeedRow {
        id: existing.id,
        name: input.name,
        url: input.url,
        enabled: input.enabled,
        poll_interval_secs: input
            .poll_interval_secs
            .unwrap_or(existing.poll_interval_secs.max(1)),
        username: merge_optional_string(existing.username, input.username),
        password: merge_optional_string(existing.password, input.password),
        default_category: merge_optional_string(existing.default_category, input.default_category),
        default_metadata: input
            .default_metadata
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect()
            })
            .unwrap_or(existing.default_metadata),
        etag: existing.etag,
        last_modified: existing.last_modified,
        last_polled_at: existing.last_polled_at,
        last_success_at: existing.last_success_at,
        last_error: existing.last_error,
        consecutive_failures: existing.consecutive_failures,
    }
}

fn rss_rule_row_from_input(id: u32, feed_id: u32, input: RssRuleInput) -> RssRuleRow {
    RssRuleRow {
        id,
        feed_id,
        sort_order: input.sort_order,
        enabled: input.enabled,
        action: match input.action {
            RssRuleActionGql::Accept => weaver_server_core::RssRuleAction::Accept,
            RssRuleActionGql::Reject => weaver_server_core::RssRuleAction::Reject,
        },
        title_regex: normalize_optional_string(input.title_regex),
        item_categories: input.item_categories.unwrap_or_default(),
        min_size_bytes: input.min_size_bytes,
        max_size_bytes: input.max_size_bytes,
        category_override: normalize_optional_string(input.category_override),
        metadata: metadata_input_to_pairs(input.metadata),
    }
}

fn metadata_input_to_pairs(entries: Option<Vec<MetadataInput>>) -> Vec<(String, String)> {
    entries
        .unwrap_or_default()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect()
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn merge_optional_string(existing: Option<String>, incoming: Option<String>) -> Option<String> {
    match incoming {
        Some(value) => normalize_optional_string(Some(value)),
        None => existing,
    }
}
