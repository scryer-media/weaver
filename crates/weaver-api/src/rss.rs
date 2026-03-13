use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use tokio::sync::Mutex;
use tracing::{info, warn};

use weaver_core::config::SharedConfig;
use weaver_scheduler::SchedulerHandle;
use weaver_state::{Database, RssFeedRow, RssRuleAction, RssRuleRow, RssSeenItemRow};

use crate::submit::submit_nzb_bytes;

const DEFAULT_POLL_INTERVAL_SECS: u32 = 900;
const RSS_SYNC_TICK_SECS: u64 = 60;
const RSS_SEEN_RETENTION_SECS: i64 = 180 * 24 * 60 * 60;

#[derive(Debug, Clone, Default)]
pub struct RssFeedSyncReport {
    pub feed_id: u32,
    pub feed_name: String,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RssSyncReport {
    pub feeds_polled: u32,
    pub items_fetched: u32,
    pub items_new: u32,
    pub items_accepted: u32,
    pub items_submitted: u32,
    pub items_ignored: u32,
    pub errors: Vec<String>,
    pub feed_results: Vec<RssFeedSyncReport>,
}

#[derive(Debug, thiserror::Error)]
pub enum RssServiceError {
    #[error("feed {0} not found")]
    FeedNotFound(u32),
    #[error("feed parse error: {0}")]
    Parse(String),
    #[error("HTTP error: {0}")]
    Http(String),
}

#[derive(Clone)]
pub struct RssService {
    inner: Arc<RssServiceInner>,
}

struct RssServiceInner {
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
    client: reqwest::Client,
    sync_lock: Mutex<()>,
}

#[derive(Debug, Clone)]
struct FeedItem {
    item_id: String,
    title: String,
    published_at: Option<i64>,
    size_bytes: Option<u64>,
    categories: Vec<String>,
    download_url: Option<String>,
    display_url: Option<String>,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    row: RssRuleRow,
    title_regex: Option<Regex>,
}

impl RssService {
    pub fn new(handle: SchedulerHandle, config: SharedConfig, db: Database) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("weaver-rss/0.1")
            .gzip(true)
            .build()
            .expect("reqwest client build should succeed");

        Self {
            inner: Arc::new(RssServiceInner {
                db,
                handle,
                config,
                client,
                sync_lock: Mutex::new(()),
            }),
        }
    }

    pub fn start_background_loop(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(RSS_SYNC_TICK_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match service.run_due_sync().await {
                    Ok(Some(report)) if report.feeds_polled > 0 => {
                        info!(
                            feeds_polled = report.feeds_polled,
                            items_submitted = report.items_submitted,
                            "RSS due sync complete"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => warn!(error = %error, "RSS due sync failed"),
                }
            }
        })
    }

    /// RSS state is read from SQLite on demand, so a restore only needs a
    /// logical refresh hook, not an in-memory cache rebuild.
    pub async fn reload_state(&self) {}

    pub async fn run_sync(&self, feed_id: Option<u32>) -> Result<RssSyncReport, RssServiceError> {
        let _guard = self.inner.sync_lock.lock().await;
        self.run_sync_inner(feed_id, false).await
    }

    async fn run_due_sync(&self) -> Result<Option<RssSyncReport>, RssServiceError> {
        let Ok(_guard) = self.inner.sync_lock.try_lock() else {
            return Ok(None);
        };
        let report = self.run_sync_inner(None, true).await?;
        if report.feeds_polled == 0 {
            Ok(None)
        } else {
            Ok(Some(report))
        }
    }

    async fn run_sync_inner(
        &self,
        feed_id: Option<u32>,
        due_only: bool,
    ) -> Result<RssSyncReport, RssServiceError> {
        let feeds = self.load_target_feeds(feed_id, due_only)?;
        if feeds.is_empty() {
            return Ok(RssSyncReport::default());
        }

        let mut report = RssSyncReport::default();
        for feed in feeds {
            let feed_report = self.sync_feed(&feed).await;
            match feed_report {
                Ok(feed_report) => {
                    report.feeds_polled += 1;
                    report.items_fetched += feed_report.items_fetched;
                    report.items_new += feed_report.items_new;
                    report.items_accepted += feed_report.items_accepted;
                    report.items_submitted += feed_report.items_submitted;
                    report.items_ignored += feed_report.items_ignored;
                    report.errors.extend(feed_report.errors.iter().cloned());
                    report.feed_results.push(feed_report);
                }
                Err(error) => {
                    let now = unix_now_secs();
                    let message = error.to_string();
                    let _ = self
                        .inner
                        .db
                        .record_rss_poll_failure(feed.id, now, &message);
                    report.feeds_polled += 1;
                    report.errors.push(format!("{}: {message}", feed.name));
                    report.feed_results.push(RssFeedSyncReport {
                        feed_id: feed.id,
                        feed_name: feed.name.clone(),
                        errors: vec![message],
                        ..Default::default()
                    });
                }
            }
        }

        let _ = self
            .inner
            .db
            .purge_old_rss_seen_items(unix_now_secs() - RSS_SEEN_RETENTION_SECS);

        Ok(report)
    }

    fn load_target_feeds(
        &self,
        feed_id: Option<u32>,
        due_only: bool,
    ) -> Result<Vec<RssFeedRow>, RssServiceError> {
        let feeds = if let Some(feed_id) = feed_id {
            let Some(feed) = self
                .inner
                .db
                .get_rss_feed(feed_id)
                .map_err(|e| RssServiceError::Http(e.to_string()))?
            else {
                return Err(RssServiceError::FeedNotFound(feed_id));
            };
            vec![feed]
        } else {
            self.inner
                .db
                .list_rss_feeds()
                .map_err(|e| RssServiceError::Http(e.to_string()))?
                .into_iter()
                .filter(|feed| feed.enabled)
                .collect()
        };

        if !due_only {
            return Ok(feeds);
        }

        let now = unix_now_secs();
        Ok(feeds.into_iter().filter(|feed| is_due(feed, now)).collect())
    }

    async fn sync_feed(&self, feed: &RssFeedRow) -> Result<RssFeedSyncReport, RssServiceError> {
        let now = unix_now_secs();
        let response = self.fetch_feed_response(feed).await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            self.inner
                .db
                .record_rss_poll_success(
                    feed.id,
                    now,
                    feed.etag.as_deref(),
                    feed.last_modified.as_deref(),
                )
                .map_err(|e| RssServiceError::Http(e.to_string()))?;
            return Ok(RssFeedSyncReport {
                feed_id: feed.id,
                feed_name: feed.name.clone(),
                ..Default::default()
            });
        }
        if !response.status().is_success() {
            return Err(RssServiceError::Http(format!(
                "feed {} returned HTTP {}",
                feed.id,
                response.status()
            )));
        }

        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| feed.etag.clone());
        let last_modified = response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| feed.last_modified.clone());
        let body = response
            .bytes()
            .await
            .map_err(|e| RssServiceError::Http(e.to_string()))?;
        let parsed = feed_rs::parser::parse(Cursor::new(body))
            .map_err(|e| RssServiceError::Parse(e.to_string()))?;

        let rules = self
            .inner
            .db
            .list_rss_rules(feed.id)
            .map_err(|e| RssServiceError::Http(e.to_string()))?;
        let compiled_rules = compile_rules(rules);

        let mut report = RssFeedSyncReport {
            feed_id: feed.id,
            feed_name: feed.name.clone(),
            items_fetched: parsed.entries.len() as u32,
            ..Default::default()
        };

        for entry in parsed.entries {
            let item = feed_item_from_entry(entry);
            if self
                .inner
                .db
                .rss_seen_item_exists(feed.id, &item.item_id)
                .map_err(|e| RssServiceError::Http(e.to_string()))?
            {
                continue;
            }

            report.items_new += 1;
            let decision = evaluate_item(&compiled_rules, &item);
            match decision {
                Some(rule) if rule.row.action == RssRuleAction::Reject => {
                    report.items_ignored += 1;
                    self.record_seen(feed.id, &item, "rejected", None, None)?;
                }
                Some(rule) => {
                    report.items_accepted += 1;
                    match self.accept_item(feed, &rule.row, &item).await {
                        Ok(job_id) => {
                            report.items_submitted += 1;
                            self.record_seen(feed.id, &item, "submitted", Some(job_id), None)?;
                        }
                        Err(error) => {
                            report.items_ignored += 1;
                            report.errors.push(error.clone());
                            self.record_seen(feed.id, &item, "error", None, Some(&error))?;
                        }
                    }
                }
                None => {
                    report.items_ignored += 1;
                    self.record_seen(feed.id, &item, "ignored", None, None)?;
                }
            }
        }

        self.inner
            .db
            .record_rss_poll_success(feed.id, now, etag.as_deref(), last_modified.as_deref())
            .map_err(|e| RssServiceError::Http(e.to_string()))?;
        Ok(report)
    }

    async fn accept_item(
        &self,
        feed: &RssFeedRow,
        rule: &RssRuleRow,
        item: &FeedItem,
    ) -> Result<u64, String> {
        let Some(download_url) = item.download_url.clone() else {
            return Err("accepted item has no direct NZB URL".to_string());
        };

        let request = apply_basic_auth(self.inner.client.get(&download_url), feed);
        let response = request
            .send()
            .await
            .map_err(|e| format!("failed to fetch NZB: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("failed to fetch NZB: HTTP {}", response.status()));
        }

        let nzb_bytes = response
            .bytes()
            .await
            .map_err(|e| format!("failed to read NZB body: {e}"))?;
        weaver_nzb::parse_nzb(&nzb_bytes).map_err(|e| format!("invalid NZB: {e}"))?;

        let raw_category = rule
            .category_override
            .clone()
            .or_else(|| feed.default_category.clone());

        // Pre-resolve category so we can gracefully fall back to None for RSS
        // (rather than failing the whole sync on an unknown category).
        let category = if let Some(ref cat) = raw_category {
            let cfg = self.inner.config.read().await;
            if cfg.categories.is_empty() {
                raw_category
            } else {
                match weaver_core::config::resolve_category(&cfg.categories, cat) {
                    Some(canonical) => Some(canonical),
                    None => {
                        tracing::warn!(
                            feed = %feed.name,
                            category = %cat,
                            "RSS category not found, submitting without category"
                        );
                        None
                    }
                }
            }
        } else {
            None
        };

        let metadata = build_submission_metadata(feed, rule, item);
        let submitted = submit_nzb_bytes(
            &self.inner.handle,
            &self.inner.config,
            &nzb_bytes,
            Some(format!("{}.nzb", item.title)),
            None,
            category,
            metadata,
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(submitted.job_id.0)
    }

    async fn fetch_feed_response(
        &self,
        feed: &RssFeedRow,
    ) -> Result<reqwest::Response, RssServiceError> {
        let mut request = self.inner.client.get(&feed.url);
        request = apply_basic_auth(request, feed);
        if let Some(etag) = &feed.etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = &feed.last_modified {
            request = request.header(IF_MODIFIED_SINCE, last_modified);
        }

        request
            .send()
            .await
            .map_err(|e| RssServiceError::Http(e.to_string()))
    }

    fn record_seen(
        &self,
        feed_id: u32,
        item: &FeedItem,
        decision: &str,
        job_id: Option<u64>,
        error: Option<&str>,
    ) -> Result<(), RssServiceError> {
        self.inner
            .db
            .insert_rss_seen_item(&RssSeenItemRow {
                feed_id,
                item_id: item.item_id.clone(),
                item_title: item.title.clone(),
                published_at: item.published_at,
                size_bytes: item.size_bytes,
                decision: decision.to_string(),
                seen_at: unix_now_secs(),
                job_id,
                item_url: item
                    .download_url
                    .clone()
                    .or_else(|| item.display_url.clone()),
                error: error.map(str::to_string),
            })
            .map_err(|e| RssServiceError::Http(e.to_string()))
    }
}

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn is_due(feed: &RssFeedRow, now: i64) -> bool {
    let last_polled_at = feed.last_polled_at.unwrap_or(0);
    let interval = if feed.poll_interval_secs == 0 {
        DEFAULT_POLL_INTERVAL_SECS
    } else {
        feed.poll_interval_secs
    };
    now.saturating_sub(last_polled_at) >= interval as i64
}

fn apply_basic_auth(
    request: reqwest::RequestBuilder,
    feed: &RssFeedRow,
) -> reqwest::RequestBuilder {
    if let Some(username) = &feed.username {
        request.basic_auth(username, feed.password.clone())
    } else {
        request
    }
}

fn compile_rules(rules: Vec<RssRuleRow>) -> Vec<CompiledRule> {
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

fn evaluate_item<'a>(rules: &'a [CompiledRule], item: &FeedItem) -> Option<&'a CompiledRule> {
    rules.iter().find(|rule| rule_matches(rule, item))
}

fn rule_matches(rule: &CompiledRule, item: &FeedItem) -> bool {
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

fn build_submission_metadata(
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

fn feed_item_from_entry(entry: feed_rs::model::Entry) -> FeedItem {
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

fn looks_like_direct_nzb_url(url: &str) -> bool {
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

fn fallback_item_id(title: &str, published_at: Option<i64>, size_bytes: Option<u64>) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::Router;
    use axum::extract::State;
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tempfile::TempDir;
    use tokio::sync::{RwLock, mpsc};

    use weaver_core::config::Config;
    use weaver_scheduler::{JobSpec, PipelineMetrics, SchedulerCommand, SharedPipelineState};

    #[derive(Clone)]
    struct TestHttpState {
        feed_body: String,
        nzb_body: Vec<u8>,
        etag: Option<String>,
        require_auth: bool,
        feed_delay_ms: u64,
        feed_requests: Arc<AtomicUsize>,
        nzb_requests: Arc<AtomicUsize>,
        auth_failures: Arc<AtomicUsize>,
        conditional_hits: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CapturedSubmission {
        spec: JobSpec,
    }

    #[tokio::test]
    async fn feed_item_parsing_prefers_enclosure_and_supports_direct_link_fallback() {
        let rss = r#"<?xml version="1.0"?>
            <rss version="2.0">
              <channel>
                <title>Example</title>
                <item>
                  <guid>guid-1</guid>
                  <title>Frieren - enclosure</title>
                  <link>https://example.com/detail</link>
                  <enclosure url="https://example.com/download.nzb" length="1234" type="application/x-nzb"/>
                  <category>TV</category>
                </item>
              </channel>
            </rss>"#
            .to_string();
        let feed = feed_rs::parser::parse(Cursor::new(rss.as_bytes())).unwrap();
        let item = feed_item_from_entry(feed.entries.into_iter().next().unwrap());
        assert_eq!(
            item.download_url.as_deref(),
            Some("https://example.com/download.nzb")
        );
        assert_eq!(item.categories, vec!["TV".to_string()]);

        let atom = r#"<?xml version="1.0" encoding="utf-8"?>
            <feed xmlns="http://www.w3.org/2005/Atom">
              <title>Atom Example</title>
              <entry>
                <id>tag:example.com,2026:test</id>
                <title>Frieren - link fallback</title>
                <link href="https://example.com/api?t=get&id=1" />
              </entry>
            </feed>"#;
        let feed = feed_rs::parser::parse(Cursor::new(atom.as_bytes())).unwrap();
        let item = feed_item_from_entry(feed.entries.into_iter().next().unwrap());
        assert_eq!(
            item.download_url.as_deref(),
            Some("https://example.com/api?t=get&id=1")
        );
    }

    #[test]
    fn first_matching_rule_wins() {
        let item = FeedItem {
            item_id: "guid-1".to_string(),
            title: "Frieren".to_string(),
            published_at: None,
            size_bytes: Some(500),
            categories: vec!["tv".to_string()],
            download_url: Some("https://example.com/download.nzb".to_string()),
            display_url: None,
        };
        let rules = compile_rules(vec![
            RssRuleRow {
                id: 1,
                feed_id: 1,
                sort_order: 0,
                enabled: true,
                action: RssRuleAction::Reject,
                title_regex: Some("Frieren".to_string()),
                item_categories: vec![],
                min_size_bytes: None,
                max_size_bytes: None,
                category_override: None,
                metadata: vec![],
            },
            RssRuleRow {
                id: 2,
                feed_id: 1,
                sort_order: 1,
                enabled: true,
                action: RssRuleAction::Accept,
                title_regex: Some("Frieren".to_string()),
                item_categories: vec![],
                min_size_bytes: None,
                max_size_bytes: None,
                category_override: None,
                metadata: vec![],
            },
        ]);
        let decision = evaluate_item(&rules, &item).unwrap();
        assert_eq!(decision.row.action, RssRuleAction::Reject);
    }

    #[tokio::test]
    async fn run_sync_submits_matching_items_and_dedupes_across_restart() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("rss.sqlite");
        let state = TestHttpState {
            feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
            nzb_body: sample_nzb_bytes(),
            etag: None,
            require_auth: false,
            feed_delay_ms: 0,
            feed_requests: Arc::new(AtomicUsize::new(0)),
            nzb_requests: Arc::new(AtomicUsize::new(0)),
            auth_failures: Arc::new(AtomicUsize::new(0)),
            conditional_hits: Arc::new(AtomicUsize::new(0)),
        };
        let (base_url, server_task) = start_test_server(state.clone()).await;

        let db = Database::open(&db_path).unwrap();
        let feed = RssFeedRow {
            id: 1,
            name: "Test Feed".to_string(),
            url: format!("{base_url}/feed"),
            enabled: true,
            poll_interval_secs: 900,
            username: None,
            password: None,
            default_category: Some("tv".to_string()),
            default_metadata: vec![("source".to_string(), "rss".to_string())],
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        };
        db.insert_rss_feed(&feed).unwrap();
        db.insert_rss_rule(&RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec!["TV".to_string()],
            min_size_bytes: Some(1),
            max_size_bytes: Some(10_000),
            category_override: Some("anime".to_string()),
            metadata: vec![("profile".to_string(), "hevc".to_string())],
        })
        .unwrap();

        let submissions = Arc::new(StdMutex::new(Vec::new()));
        let service = build_service(temp.path(), db.clone(), submissions.clone());
        let report = service.run_sync(None).await.unwrap();
        assert_eq!(report.feeds_polled, 1);
        assert_eq!(report.items_submitted, 1);
        assert_eq!(submissions.lock().unwrap().len(), 1);
        let first = submissions.lock().unwrap()[0].clone();
        assert_eq!(first.spec.category.as_deref(), Some("anime"));
        assert!(
            first
                .spec
                .metadata
                .iter()
                .any(|(k, v)| k == "rss.feed_name" && v == "Test Feed")
        );

        drop(service);
        drop(db);

        let db = Database::open(&db_path).unwrap();
        let submissions_restart = Arc::new(StdMutex::new(Vec::new()));
        let service_restart = build_service(temp.path(), db, submissions_restart.clone());
        let report = service_restart.run_sync(None).await.unwrap();
        assert_eq!(report.items_new, 0);
        assert!(submissions_restart.lock().unwrap().is_empty());

        server_task.abort();
    }

    #[tokio::test]
    async fn run_sync_uses_conditional_get_and_basic_auth() {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("rss-auth.sqlite")).unwrap();
        let state = TestHttpState {
            feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
            nzb_body: sample_nzb_bytes(),
            etag: Some("v1".to_string()),
            require_auth: true,
            feed_delay_ms: 0,
            feed_requests: Arc::new(AtomicUsize::new(0)),
            nzb_requests: Arc::new(AtomicUsize::new(0)),
            auth_failures: Arc::new(AtomicUsize::new(0)),
            conditional_hits: Arc::new(AtomicUsize::new(0)),
        };
        let (base_url, server_task) = start_test_server(state.clone()).await;

        db.insert_rss_feed(&RssFeedRow {
            id: 1,
            name: "Auth Feed".to_string(),
            url: format!("{base_url}/feed"),
            enabled: true,
            poll_interval_secs: 900,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            default_category: None,
            default_metadata: vec![],
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        })
        .unwrap();
        db.insert_rss_rule(&RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        })
        .unwrap();

        let submissions = Arc::new(StdMutex::new(Vec::new()));
        let service = build_service(temp.path(), db.clone(), submissions);
        let first = service.run_sync(None).await.unwrap();
        assert_eq!(first.items_submitted, 1);
        let second = service.run_sync(None).await.unwrap();
        assert_eq!(second.items_fetched, 0);
        assert_eq!(state.conditional_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.auth_failures.load(Ordering::SeqCst), 0);
        server_task.abort();
    }

    #[tokio::test]
    async fn failed_fetch_is_marked_seen_and_not_retried_immediately() {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("rss-failure.sqlite")).unwrap();
        let state = TestHttpState {
            feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
            nzb_body: b"not an nzb".to_vec(),
            etag: None,
            require_auth: false,
            feed_delay_ms: 0,
            feed_requests: Arc::new(AtomicUsize::new(0)),
            nzb_requests: Arc::new(AtomicUsize::new(0)),
            auth_failures: Arc::new(AtomicUsize::new(0)),
            conditional_hits: Arc::new(AtomicUsize::new(0)),
        };
        let (base_url, server_task) = start_test_server(state.clone()).await;
        db.insert_rss_feed(&RssFeedRow {
            id: 1,
            name: "Broken Feed".to_string(),
            url: format!("{base_url}/feed"),
            enabled: true,
            poll_interval_secs: 900,
            username: None,
            password: None,
            default_category: None,
            default_metadata: vec![],
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        })
        .unwrap();
        db.insert_rss_rule(&RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        })
        .unwrap();

        let service = build_service(temp.path(), db, Arc::new(StdMutex::new(Vec::new())));
        let first = service.run_sync(None).await.unwrap();
        assert_eq!(first.items_submitted, 0);
        assert_eq!(first.items_new, 1);
        assert_eq!(first.errors.len(), 1);

        let second = service.run_sync(None).await.unwrap();
        assert_eq!(second.items_new, 0);
        assert_eq!(second.items_submitted, 0);
        assert_eq!(state.nzb_requests.load(Ordering::SeqCst), 1);
        server_task.abort();
    }

    #[tokio::test]
    async fn background_due_sync_skips_when_manual_sync_is_active() {
        let temp = TempDir::new().unwrap();
        let db = Database::open(&temp.path().join("rss-due.sqlite")).unwrap();
        let state = TestHttpState {
            feed_body: sample_rss_feed("guid-1", "Frieren 01", "/download.nzb"),
            nzb_body: sample_nzb_bytes(),
            etag: None,
            require_auth: false,
            feed_delay_ms: 250,
            feed_requests: Arc::new(AtomicUsize::new(0)),
            nzb_requests: Arc::new(AtomicUsize::new(0)),
            auth_failures: Arc::new(AtomicUsize::new(0)),
            conditional_hits: Arc::new(AtomicUsize::new(0)),
        };
        let (base_url, server_task) = start_test_server(state).await;
        db.insert_rss_feed(&RssFeedRow {
            id: 1,
            name: "Due Feed".to_string(),
            url: format!("{base_url}/feed"),
            enabled: true,
            poll_interval_secs: 1,
            username: None,
            password: None,
            default_category: None,
            default_metadata: vec![],
            etag: None,
            last_modified: None,
            last_polled_at: Some(unix_now_secs() - 10),
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        })
        .unwrap();
        db.insert_rss_rule(&RssRuleRow {
            id: 1,
            feed_id: 1,
            sort_order: 0,
            enabled: true,
            action: RssRuleAction::Accept,
            title_regex: Some("Frieren".to_string()),
            item_categories: vec![],
            min_size_bytes: None,
            max_size_bytes: None,
            category_override: None,
            metadata: vec![],
        })
        .unwrap();

        let service = build_service(temp.path(), db, Arc::new(StdMutex::new(Vec::new())));
        let running = {
            let service = service.clone();
            tokio::spawn(async move { service.run_sync(None).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(service.run_due_sync().await.unwrap().is_none());
        let _ = running.await.unwrap();
        server_task.abort();
    }

    fn build_service(
        data_dir: &Path,
        db: Database,
        submissions: Arc<StdMutex<Vec<CapturedSubmission>>>,
    ) -> RssService {
        let config = Arc::new(RwLock::new(Config {
            data_dir: data_dir.display().to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            isp_bandwidth_cap: None,
            cleanup_after_extract: Some(true),
            config_path: None,
        }));
        let handle = test_scheduler_handle(submissions);
        crate::submit::init_job_counter(20_000);
        RssService::new(handle, config, db)
    }

    fn test_scheduler_handle(
        submissions: Arc<StdMutex<Vec<CapturedSubmission>>>,
    ) -> SchedulerHandle {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(8);
        let (event_tx, _) = tokio::sync::broadcast::channel(8);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        tokio::spawn(async move {
            while let Some(command) = cmd_rx.recv().await {
                if let SchedulerCommand::AddJob { spec, reply, .. } = command {
                    submissions
                        .lock()
                        .unwrap()
                        .push(CapturedSubmission { spec });
                    let _ = reply.send(Ok(()));
                }
            }
        });
        SchedulerHandle::new(cmd_tx, event_tx, state)
    }

    async fn start_test_server(state: TestHttpState) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);
        let mut state = state;
        state.feed_body = state.feed_body.replace("http://localhost", &base_url);
        let app = Router::new()
            .route("/feed", get(feed_handler))
            .route("/download.nzb", get(nzb_handler))
            .route("/api", get(nzb_handler))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (base_url, task)
    }

    async fn feed_handler(
        State(state): State<TestHttpState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        state.feed_requests.fetch_add(1, Ordering::SeqCst);
        if state.require_auth && !auth_ok(&headers) {
            state.auth_failures.fetch_add(1, Ordering::SeqCst);
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if state.feed_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(state.feed_delay_ms)).await;
        }
        if let Some(etag) = &state.etag
            && headers
                .get(IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                == Some(etag.as_str())
        {
            state.conditional_hits.fetch_add(1, Ordering::SeqCst);
            return StatusCode::NOT_MODIFIED.into_response();
        }
        let mut response_headers = HeaderMap::new();
        if let Some(etag) = &state.etag {
            response_headers.insert(ETAG, HeaderValue::from_str(etag).unwrap());
        }
        (response_headers, state.feed_body.clone()).into_response()
    }

    async fn nzb_handler(
        State(state): State<TestHttpState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        state.nzb_requests.fetch_add(1, Ordering::SeqCst);
        if state.require_auth && !auth_ok(&headers) {
            state.auth_failures.fetch_add(1, Ordering::SeqCst);
            return StatusCode::UNAUTHORIZED.into_response();
        }
        (
            [(axum::http::header::CONTENT_TYPE, "application/x-nzb")],
            state.nzb_body,
        )
            .into_response()
    }

    fn auth_ok(headers: &HeaderMap) -> bool {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some("Basic dXNlcjpwYXNz")
    }

    fn sample_rss_feed(guid: &str, title: &str, path: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <rss version="2.0">
              <channel>
                <title>Example</title>
                <item>
                  <guid>{guid}</guid>
                  <title>{title}</title>
                  <link>https://example.com/details</link>
                  <enclosure url="http://localhost{path}" length="321" type="application/x-nzb"/>
                  <category>TV</category>
                  <pubDate>Mon, 10 Mar 2025 12:00:00 GMT</pubDate>
                </item>
              </channel>
            </rss>"#
        )
    }

    fn sample_nzb_bytes() -> Vec<u8> {
        br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Frieren.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
            .to_vec()
    }
}
