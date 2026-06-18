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
use crate::security::{RuntimeSecurityConfig, resolve_fetch_target};
use crate::settings::SharedConfig;
use crate::{Database, RssFeedRow, RssRuleRow, RssSeenItemRow};

const MAX_RSS_REDIRECTS: usize = 10;

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
    pub(super) security: RuntimeSecurityConfig,
    pub(super) sync_lock: Mutex<()>,
}

impl RssService {
    pub fn new(handle: SchedulerHandle, config: SharedConfig, db: Database) -> Self {
        Self::new_with_security(
            handle,
            config,
            db,
            RuntimeSecurityConfig::from_env_or_default_for_tests(),
        )
    }

    fn new_with_security(
        handle: SchedulerHandle,
        config: SharedConfig,
        db: Database,
        security: RuntimeSecurityConfig,
    ) -> Self {
        Self {
            inner: Arc::new(RssServiceInner {
                db,
                handle,
                config,
                security,
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

        let response = self
            .send_rss_request(feed, &download_url, false)
            .await
            .map_err(|e| format!("failed to fetch NZB: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("failed to fetch NZB: HTTP {}", response.status()));
        }

        let nzb_bytes =
            read_response_with_limit(response, self.inner.security.nzb_decompressed_limit_bytes)
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
            &self.inner.db,
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
        self.send_rss_request(feed, &feed.url, true).await
    }

    async fn send_rss_request(
        &self,
        feed: &RssFeedRow,
        url: &str,
        conditional: bool,
    ) -> Result<reqwest::Response, RssServiceError> {
        let original_url =
            reqwest::Url::parse(url).map_err(|e| RssServiceError::Http(e.to_string()))?;
        let mut current_url = original_url.clone();

        for redirect_count in 0..=MAX_RSS_REDIRECTS {
            let target =
                resolve_fetch_target(&current_url, self.inner.security.rss_allow_private_network)
                    .await
                    .map_err(RssServiceError::Http)?;
            let client = target
                .apply_dns_override(
                    reqwest::Client::builder()
                        .timeout(Duration::from_secs(30))
                        .user_agent("weaver-rss/0.1")
                        .redirect(reqwest::redirect::Policy::none())
                        .gzip(true),
                )
                .build()
                .map_err(|e| RssServiceError::Http(e.to_string()))?;

            let mut request = client.get(target.url.clone());
            if current_url.origin() == original_url.origin() {
                request = apply_basic_auth(request, feed);
            }
            if conditional && redirect_count == 0 {
                if let Some(etag) = &feed.etag {
                    request = request.header(reqwest::header::IF_NONE_MATCH, etag);
                }
                if let Some(last_modified) = &feed.last_modified {
                    request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
                }
            }

            let response = request
                .send()
                .await
                .map_err(|e| RssServiceError::Http(e.to_string()))?;
            if response.status().is_redirection() {
                if redirect_count == MAX_RSS_REDIRECTS {
                    return Err(RssServiceError::Http("too many redirects".to_string()));
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        RssServiceError::Http("redirect missing Location".to_string())
                    })?;
                current_url = current_url
                    .join(location)
                    .map_err(|e| RssServiceError::Http(format!("invalid redirect: {e}")))?;
                continue;
            }

            return Ok(response);
        }

        Err(RssServiceError::Http("too many redirects".to_string()))
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

async fn read_response_with_limit(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(format!("response exceeds {limit} bytes"));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("body read failed: {e}"))?
    {
        let next_len = body.len().saturating_add(chunk.len());
        if next_len as u64 > limit {
            return Err(format!("response exceeds {limit} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests;
