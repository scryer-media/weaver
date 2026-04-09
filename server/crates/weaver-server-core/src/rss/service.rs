#[cfg(test)]
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use reqwest::header::{ETAG, IF_NONE_MATCH};
use tokio::sync::Mutex;
use tracing::warn;

#[cfg(test)]
use crate::RssRuleAction;
use crate::SchedulerHandle;
use crate::ingest::submit_nzb_bytes;
use crate::rss::model::{FeedItem, apply_basic_auth, build_submission_metadata};
#[cfg(test)]
use crate::rss::model::{compile_rules, evaluate_item, feed_item_from_entry, unix_now_secs};
use crate::settings::SharedConfig;
use crate::{Database, RssFeedRow, RssRuleRow, RssSeenItemRow};

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

#[derive(Debug, Clone)]
pub(crate) enum DueSyncOutcome {
    SkippedActiveSync,
    NoFeedsDue,
    Completed(RssSyncReport),
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
    pub(super) inner: Arc<RssServiceInner>,
}

pub(super) struct RssServiceInner {
    pub(super) db: Database,
    pub(super) handle: SchedulerHandle,
    pub(super) config: SharedConfig,
    pub(super) client: reqwest::Client,
    pub(super) sync_lock: Mutex<()>,
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

    pub(super) async fn accept_item(
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

        let category = if let Some(ref cat) = raw_category {
            let cfg = self.inner.config.read().await;
            if cfg.categories.is_empty() {
                raw_category
            } else {
                match crate::categories::resolve_category(&cfg.categories, cat) {
                    Some(canonical) => Some(canonical),
                    None => {
                        warn!(
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

    pub(super) async fn fetch_feed_response(
        &self,
        feed: &RssFeedRow,
    ) -> Result<reqwest::Response, RssServiceError> {
        let mut request = self.inner.client.get(&feed.url);
        request = apply_basic_auth(request, feed);
        if let Some(etag) = &feed.etag {
            request = request.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = &feed.last_modified {
            request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
        }

        request
            .send()
            .await
            .map_err(|e| RssServiceError::Http(e.to_string()))
    }

    pub(super) fn record_seen(
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
                seen_at: crate::rss::model::unix_now_secs(),
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

#[cfg(test)]
mod tests;
