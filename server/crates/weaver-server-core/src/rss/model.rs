use std::collections::BTreeMap;

use regex::Regex;
use tracing::warn;

use crate::rss::{RssFeedRow, RssRuleRow};

pub(crate) const DEFAULT_POLL_INTERVAL_SECS: u32 = 900;
pub(crate) const RSS_SYNC_TICK_SECS: u64 = 60;
pub(crate) const RSS_SEEN_RETENTION_SECS: i64 = 180 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub(crate) struct FeedItem {
    pub(crate) item_id: String,
    pub(crate) title: String,
    pub(crate) published_at: Option<i64>,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) categories: Vec<String>,
    pub(crate) download_url: Option<String>,
    pub(crate) display_url: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledRule {
    pub(crate) row: RssRuleRow,
    pub(crate) title_regex: Option<Regex>,
}

pub(crate) fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn is_due(feed: &RssFeedRow, now: i64) -> bool {
    let last_polled_at = feed.last_polled_at.unwrap_or(0);
    let interval = if feed.poll_interval_secs == 0 {
        DEFAULT_POLL_INTERVAL_SECS
    } else {
        feed.poll_interval_secs
    };
    now.saturating_sub(last_polled_at) >= interval as i64
}

pub(crate) fn apply_basic_auth(
    request: reqwest::RequestBuilder,
    feed: &RssFeedRow,
) -> reqwest::RequestBuilder {
    if let Some(username) = &feed.username {
        request.basic_auth(username, feed.password.clone())
    } else {
        request
    }
}

pub(crate) fn compile_rules(rules: Vec<RssRuleRow>) -> Vec<CompiledRule> {
    rules
        .into_iter()
        .filter(|rule| rule.enabled)
        .filter_map(|rule| {
            let title_regex = match &rule.title_regex {
                Some(pattern) => match Regex::new(pattern) {
                    Ok(regex) => Some(regex),
                    Err(error) => {
                        warn!(rule_id = rule.id, error = %error, "invalid RSS regex ignored");
                        return None;
                    }
                },
                None => None,
            };
            Some(CompiledRule {
                row: rule,
                title_regex,
            })
        })
        .collect()
}

pub(crate) fn evaluate_item<'a>(
    rules: &'a [CompiledRule],
    item: &FeedItem,
) -> Option<&'a CompiledRule> {
    rules.iter().find(|rule| rule_matches(rule, item))
}

pub(crate) fn rule_matches(rule: &CompiledRule, item: &FeedItem) -> bool {
    if let Some(regex) = &rule.title_regex
        && !regex.is_match(&item.title)
    {
        return false;
    }

    if !rule.row.item_categories.is_empty() {
        let wanted: Vec<String> = rule
            .row
            .item_categories
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        let item_categories: Vec<String> = item
            .categories
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        if !wanted
            .iter()
            .any(|wanted| item_categories.iter().any(|item| item == wanted))
        {
            return false;
        }
    }

    if let Some(min_size) = rule.row.min_size_bytes {
        let Some(size) = item.size_bytes else {
            return false;
        };
        if size < min_size {
            return false;
        }
    }

    if let Some(max_size) = rule.row.max_size_bytes {
        let Some(size) = item.size_bytes else {
            return false;
        };
        if size > max_size {
            return false;
        }
    }

    true
}

pub(crate) fn build_submission_metadata(
    feed: &RssFeedRow,
    rule: &RssRuleRow,
    item: &FeedItem,
) -> Vec<(String, String)> {
    let mut merged = BTreeMap::new();
    for (key, value) in &feed.default_metadata {
        merged.insert(key.clone(), value.clone());
    }
    for (key, value) in &rule.metadata {
        merged.insert(key.clone(), value.clone());
    }
    merged.insert("rss.feed_id".to_string(), feed.id.to_string());
    merged.insert("rss.feed_name".to_string(), feed.name.clone());
    merged.insert("rss.item_id".to_string(), item.item_id.clone());
    merged.insert("rss.item_title".to_string(), item.title.clone());
    if let Some(url) = item
        .download_url
        .clone()
        .or_else(|| item.display_url.clone())
    {
        merged.insert("rss.item_url".to_string(), url);
    }
    merged.into_iter().collect()
}

pub(crate) fn feed_item_from_entry(entry: feed_rs::model::Entry) -> FeedItem {
    let title = entry
        .title
        .as_ref()
        .map(|value| value.content.clone())
        .unwrap_or_else(|| "Untitled RSS Item".to_string());
    let media_content = entry
        .media
        .iter()
        .flat_map(|media| media.content.iter())
        .find_map(|content| {
            content
                .url
                .as_ref()
                .map(|url| (url.to_string(), content.size))
        });
    let download_url = media_content
        .as_ref()
        .map(|(url, _)| url.clone())
        .or_else(|| {
            entry
                .links
                .iter()
                .find(|link| looks_like_direct_nzb_url(&link.href))
                .map(|link| link.href.clone())
        });
    let display_url = entry.links.first().map(|link| link.href.clone());
    let item_id = if !entry.id.is_empty() {
        entry.id
    } else if let Some(url) = download_url.clone().or_else(|| display_url.clone()) {
        url
    } else {
        fallback_item_id(
            &title,
            entry.published.as_ref().map(|ts| ts.timestamp()),
            None,
        )
    };

    let size_bytes = media_content.and_then(|(_, size)| size).or_else(|| {
        entry
            .links
            .iter()
            .find(|link| download_url.as_deref() == Some(link.href.as_str()))
            .and_then(|link| link.length)
    });

    FeedItem {
        item_id,
        title,
        published_at: entry.published.or(entry.updated).map(|ts| ts.timestamp()),
        size_bytes,
        categories: entry.categories.into_iter().map(|cat| cat.term).collect(),
        download_url,
        display_url,
    }
}

pub(crate) fn looks_like_direct_nzb_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.path().to_ascii_lowercase().ends_with(".nzb") {
        return true;
    }
    parsed
        .query_pairs()
        .any(|(key, value)| key.eq_ignore_ascii_case("t") && value.eq_ignore_ascii_case("get"))
}

pub(crate) fn fallback_item_id(
    title: &str,
    published_at: Option<i64>,
    size_bytes: Option<u64>,
) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(title.as_bytes());
    if let Some(published_at) = published_at {
        hasher.update(published_at.to_le_bytes());
    }
    if let Some(size_bytes) = size_bytes {
        hasher.update(size_bytes.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}
