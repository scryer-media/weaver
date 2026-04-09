use async_graphql::{Enum, InputObject, SimpleObject};

use crate::jobs::types::{MetadataEntry, MetadataInput};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum RssRuleActionGql {
    Accept,
    Reject,
}

impl From<weaver_server_core::RssRuleAction> for RssRuleActionGql {
    fn from(value: weaver_server_core::RssRuleAction) -> Self {
        match value {
            weaver_server_core::RssRuleAction::Accept => Self::Accept,
            weaver_server_core::RssRuleAction::Reject => Self::Reject,
        }
    }
}

impl From<RssRuleActionGql> for weaver_server_core::RssRuleAction {
    fn from(value: RssRuleActionGql) -> Self {
        match value {
            RssRuleActionGql::Accept => Self::Accept,
            RssRuleActionGql::Reject => Self::Reject,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssRule {
    pub id: u32,
    pub feed_id: u32,
    pub sort_order: i32,
    pub enabled: bool,
    pub action: RssRuleActionGql,
    pub title_regex: Option<String>,
    pub item_categories: Vec<String>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Vec<MetadataEntry>,
}

impl RssRule {
    pub fn from_row(rule: &weaver_server_core::RssRuleRow) -> Self {
        Self {
            id: rule.id,
            feed_id: rule.feed_id,
            sort_order: rule.sort_order,
            enabled: rule.enabled,
            action: rule.action.into(),
            title_regex: rule.title_regex.clone(),
            item_categories: rule.item_categories.clone(),
            min_size_bytes: rule.min_size_bytes,
            max_size_bytes: rule.max_size_bytes,
            category_override: rule.category_override.clone(),
            metadata: rule
                .metadata
                .iter()
                .map(|(key, value)| MetadataEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, InputObject)]
pub struct RssRuleInput {
    #[graphql(default = true)]
    pub enabled: bool,
    pub sort_order: i32,
    pub action: RssRuleActionGql,
    pub title_regex: Option<String>,
    pub item_categories: Option<Vec<String>>,
    pub min_size_bytes: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub category_override: Option<String>,
    pub metadata: Option<Vec<MetadataInput>>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssFeed {
    pub id: u32,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub poll_interval_secs: u32,
    pub username: Option<String>,
    pub has_password: bool,
    pub default_category: Option<String>,
    pub default_metadata: Vec<MetadataEntry>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_polled_at: Option<f64>,
    pub last_success_at: Option<f64>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub rules: Vec<RssRule>,
}

impl RssFeed {
    pub fn from_row(feed: &weaver_server_core::RssFeedRow, rules: Vec<RssRule>) -> Self {
        Self {
            id: feed.id,
            name: feed.name.clone(),
            url: feed.url.clone(),
            enabled: feed.enabled,
            poll_interval_secs: feed.poll_interval_secs,
            username: feed.username.clone(),
            has_password: feed.password.is_some(),
            default_category: feed.default_category.clone(),
            default_metadata: feed
                .default_metadata
                .iter()
                .map(|(key, value)| MetadataEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            etag: feed.etag.clone(),
            last_modified: feed.last_modified.clone(),
            last_polled_at: feed.last_polled_at.map(|value| value as f64 * 1000.0),
            last_success_at: feed.last_success_at.map(|value| value as f64 * 1000.0),
            last_error: feed.last_error.clone(),
            consecutive_failures: feed.consecutive_failures,
            rules,
        }
    }
}

#[derive(Debug, InputObject)]
pub struct RssFeedInput {
    pub name: String,
    pub url: String,
    #[graphql(default = true)]
    pub enabled: bool,
    pub poll_interval_secs: Option<u32>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_category: Option<String>,
    pub default_metadata: Option<Vec<MetadataInput>>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssFeedSyncResult {
    pub feed_id: u32,
    pub feed_name: String,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssSyncReport {
    pub feeds_polled: u32,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
    pub feed_results: Vec<RssFeedSyncResult>,
}

impl RssSyncReport {
    pub fn from_domain(report: &crate::rss::RssSyncReport) -> Self {
        Self {
            feeds_polled: report.feeds_polled,
            items_fetched: report.items_fetched,
            items_new: report.items_new,
            items_accepted: report.items_accepted,
            items_submitted: report.items_submitted,
            items_ignored: report.items_ignored,
            errors: report.errors.clone(),
            feed_results: report
                .feed_results
                .iter()
                .map(|feed| RssFeedSyncResult {
                    feed_id: feed.feed_id,
                    feed_name: feed.feed_name.clone(),
                    items_fetched: feed.items_fetched,
                    items_new: feed.items_new,
                    items_accepted: feed.items_accepted,
                    items_submitted: feed.items_submitted,
                    items_ignored: feed.items_ignored,
                    errors: feed.errors.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RssSeenItem {
    pub feed_id: u32,
    pub item_id: String,
    pub item_title: String,
    pub published_at: Option<f64>,
    pub size_bytes: Option<u64>,
    pub decision: String,
    pub seen_at: f64,
    pub job_id: Option<u64>,
    pub item_url: Option<String>,
    pub error: Option<String>,
}

impl RssSeenItem {
    pub fn from_row(item: &weaver_server_core::RssSeenItemRow) -> Self {
        Self {
            feed_id: item.feed_id,
            item_id: item.item_id.clone(),
            item_title: item.item_title.clone(),
            published_at: item.published_at.map(|value| value as f64 * 1000.0),
            size_bytes: item.size_bytes,
            decision: item.decision.clone(),
            seen_at: item.seen_at as f64 * 1000.0,
            job_id: item.job_id,
            item_url: item.item_url.clone(),
            error: item.error.clone(),
        }
    }
}
