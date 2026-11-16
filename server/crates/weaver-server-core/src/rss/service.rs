// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use reqwest::header::{ETAG, IF_NONE_MATCH};
use tokio::sync::Mutex;
use tracing::warn;

#[cfg(test)]
use crate::RssRuleAction;
use crate::SchedulerHandle;
use crate::ingest::{Kind, SubmissionOptions, classify, submit_nzb_bytes_with_options};
use crate::jobs::class::{AddRefusal, ClassIntake, RefusalReason, Route, Submission};
use crate::jobs::{CallerScopedIdempotency, SubmissionOrigin};
use crate::proxies::{Consumer, FetchOptions, read_body, routed_fetch};
use crate::rss::model::{FeedItem, apply_basic_auth, build_submission_metadata, unix_now_secs};
#[cfg(test)]
use crate::rss::model::{compile_rules, evaluate_item, parse_feed_items};
use crate::security::RuntimeSecurityConfig;
use crate::settings::SharedConfig;
use crate::{Database, RssFeedRow, RssRuleRow, RssSeenDecision, RssSeenItemRow};

const MAX_RSS_REDIRECTS: usize = 10;
pub(super) const MAX_RSS_FEED_BODY_BYTES: u64 = 16 * 1024 * 1024;

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

// Why an accepted item made no job: one already holds it, its class takes
// no new jobs now, or it failed.
#[derive(Debug)]
pub(super) enum NotAdded {
    Ignored(String),
    // Held, not seen, with the class's word on why: judged anew once its
    // class takes new jobs.
    Waiting(String),
    Failed(String),
}

impl From<AddRefusal> for NotAdded {
    fn from(refusal: AddRefusal) -> Self {
        match refusal.reason {
            RefusalReason::Duplicate(_) | RefusalReason::Removing => Self::Ignored(refusal.text),
            RefusalReason::Off => Self::Waiting(refusal.text),
            RefusalReason::NotRunning
            | RefusalReason::NoIntake
            | RefusalReason::Invalid
            | RefusalReason::Refused
            | RefusalReason::NoNetwork
            | RefusalReason::State => Self::Failed(refusal.text),
        }
    }
}

impl From<String> for NotAdded {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
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
    pub(super) scheduled_paused: tokio::sync::watch::Sender<bool>,
    #[cfg(test)]
    clock_origin: tokio::time::Instant,
    #[cfg(test)]
    clock_epoch: i64,
    // Full feed rows the due-sync path has loaded.
    #[cfg(test)]
    pub(super) full_feed_loads: std::sync::atomic::AtomicU64,
}

impl RssService {
    pub fn set_scheduled_paused(&self, paused: bool) {
        self.inner.scheduled_paused.send_if_modified(|value| {
            if *value == paused {
                return false;
            }
            *value = paused;
            true
        });
    }

    // Whether any class the feeds' items go to takes new jobs now.
    pub(super) fn classes_take_new(&self) -> bool {
        self.inner
            .handle
            .state()
            .classes()
            .iter()
            .filter_map(|class| class.intake())
            .any(|intake| intake.takes_new().is_ok())
    }

    pub fn is_scheduled_paused(&self) -> bool {
        *self.inner.scheduled_paused.borrow()
    }

    // Wall-clock seconds, so a host suspend or clock change is seen.
    #[cfg(not(test))]
    pub(super) fn now(&self) -> i64 {
        unix_now_secs()
    }

    // Unit tests drive the poller with tokio's paused clock.
    #[cfg(test)]
    pub(super) fn now(&self) -> i64 {
        self.inner.clock_epoch.saturating_add(
            self.inner
                .clock_origin
                .elapsed()
                .as_secs()
                .try_into()
                .unwrap_or(i64::MAX),
        )
    }

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
                scheduled_paused: tokio::sync::watch::channel(false).0,
                #[cfg(test)]
                clock_origin: tokio::time::Instant::now(),
                #[cfg(test)]
                clock_epoch: unix_now_secs(),
                #[cfg(test)]
                full_feed_loads: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    // An accepted item's job. A magnet goes to its class as it is; anything
    // else is fetched on the feed's route, and its bytes say which class it
    // goes to, or that it is an NZB.
    pub(super) async fn accept_item_cancellable(
        &self,
        feed: &RssFeedRow,
        rule: &RssRuleRow,
        item: &FeedItem,
        cancellation: &crate::bandwidth::schedule::ScheduleCancellation,
    ) -> Result<u64, NotAdded> {
        let Some(download_url) = item.download_url.clone() else {
            return Err("accepted item has no download URL".to_string().into());
        };
        let category = self.category(feed, rule).await;
        let metadata = build_submission_metadata(feed, rule, item);

        let classes = self.inner.handle.state().classes();
        let (kind, body, supplied_password_candidates) = match classify(download_url.as_bytes()) {
            kind @ Kind::Magnet(_) => (kind, Vec::new(), Vec::new()),
            _ => {
                // A link whose declared type names a class is not fetched
                // while that class would refuse it: it waits unfetched while
                // the class takes no new jobs, and is fetched once it does;
                // with no such class running it fails as its bytes would.
                if let Some(declared) = item.declared {
                    let refused = match classes.route_to(Some(declared)) {
                        Ok(Route::Class(intake)) => intake.takes_new().err(),
                        Ok(Route::Pipeline) => None,
                        Err(refusal) => Some(refusal),
                    };
                    if let Some(refusal) = refused {
                        return Err(refusal.into());
                    }
                }
                let response = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err("scheduled RSS sync cancelled".to_string().into()),
                    result = self.send_rss_request(feed, &download_url, false) => result.map_err(|e| format!("failed to fetch item: {e}"))?,
                };
                if !response.status().is_success() {
                    return Err(format!("failed to fetch item: HTTP {}", response.status()).into());
                }
                let supplied_password_candidates =
                    crate::ingest::header_archive_password_candidates(
                        crate::ingest::dnzb_header_passwords(response.headers())
                            .iter()
                            .map(String::as_str),
                    );
                let body = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err("scheduled RSS sync cancelled".to_string().into()),
                    result = read_body(response, self.inner.security.nzb_decompressed_limit_bytes) => result.map_err(|e| format!("failed to read item body: {e}"))?,
                };
                (classify(&body), body, supplied_password_candidates)
            }
        };
        if cancellation.is_cancelled() {
            return Err("scheduled RSS sync cancelled".to_string().into());
        }
        match classes.route(&kind)? {
            Route::Class(intake) => {
                let mut submission = Submission::of(kind, body);
                submission.category = category;
                submission.metadata = metadata;
                self.add_to_class(intake, submission).await
            }
            Route::Pipeline => {
                let nzb = body;
                weaver_nzb::parse_nzb(&nzb).map_err(|e| format!("invalid NZB: {e}"))?;
                // Submission owns durable writes. Once started it must finish before shutdown.
                let idempotency = CallerScopedIdempotency::new(
                    format!("rss:feed:{}", feed.id),
                    item.item_id.clone(),
                );
                let submitted = submit_nzb_bytes_with_options(
                    &self.inner.db,
                    &self.inner.handle,
                    &self.inner.config,
                    &nzb,
                    Some(format!("{}.nzb", item.title)),
                    None,
                    category,
                    metadata,
                    SubmissionOptions {
                        origin: SubmissionOrigin::Rss,
                        idempotency,
                        supplied_password_candidates,
                        ..SubmissionOptions::default()
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
                Ok(submitted.job_id.0)
            }
        }
    }

    // The rule's category, else the feed's, as a configured category names it.
    async fn category(&self, feed: &RssFeedRow, rule: &RssRuleRow) -> Option<String> {
        let raw_category = rule
            .category_override
            .clone()
            .or_else(|| feed.default_category.clone())?;
        let cfg = self.inner.config.read().await;
        if cfg.categories.is_empty() {
            return Some(raw_category);
        }
        let canonical = crate::categories::resolve_category(&cfg.categories, &raw_category);
        if canonical.is_none() {
            warn!(
                feed = %feed.name,
                category = %raw_category,
                "RSS category not found, submitting without category"
            );
        }
        canonical
    }

    // An item another class takes goes to it with the item's category,
    // whose route it takes. One the class already holds is left to it.
    async fn add_to_class(
        &self,
        intake: &dyn ClassIntake,
        submission: Submission,
    ) -> Result<u64, NotAdded> {
        Ok(intake.add(submission).await?.0)
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
        let feed_url =
            reqwest::Url::parse(&feed.url).map_err(|e| RssServiceError::Http(e.to_string()))?;
        let url = reqwest::Url::parse(url).map_err(|e| RssServiceError::Http(e.to_string()))?;
        let runtime = self
            .inner
            .handle
            .proxy_runtime()
            .ok_or_else(|| RssServiceError::Http("RSS request has no route".into()))?;
        let options = FetchOptions {
            label: "RSS",
            timeout: Duration::from_secs(30),
            redirects: MAX_RSS_REDIRECTS,
            user_agent: "weaver-rss/0.1",
            https_only: false,
            allow_private_network: self.inner.security.rss_allow_private_network,
        };
        routed_fetch(
            &runtime,
            Consumer::Rss(feed.id),
            &url,
            &options,
            |mut request, hop_url, hop| {
                if same_rss_origin(hop_url, &feed_url) {
                    request = apply_basic_auth(request, feed);
                }
                if conditional && hop == 0 {
                    if let Some(etag) = &feed.etag {
                        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
                    }
                    if let Some(modified) = &feed.last_modified {
                        request = request.header(reqwest::header::IF_MODIFIED_SINCE, modified);
                    }
                }
                request
            },
        )
        .await
        .map_err(RssServiceError::Http)
    }

    // The seen row of `item`, written with the rest of its poll's.
    pub(super) fn seen_row(
        feed_id: u32,
        item: &FeedItem,
        decision: RssSeenDecision,
        job_id: Option<u64>,
        error: Option<&str>,
    ) -> RssSeenItemRow {
        RssSeenItemRow {
            feed_id,
            item_id: item.item_id.clone(),
            item_title: item.title.clone(),
            published_at: item.published_at,
            size_bytes: item.size_bytes,
            decision,
            seen_at: crate::rss::model::unix_now_secs(),
            job_id,
            item_url: item
                .download_url
                .clone()
                .or_else(|| item.display_url.clone()),
            error: error.map(str::to_string),
        }
    }
}

pub(super) fn same_rss_origin(request_url: &reqwest::Url, feed_url: &reqwest::Url) -> bool {
    request_url.scheme() == feed_url.scheme()
        && request_url
            .host_str()
            .zip(feed_url.host_str())
            .is_some_and(|(request_host, feed_host)| request_host.eq_ignore_ascii_case(feed_host))
        && request_url.port_or_known_default() == feed_url.port_or_known_default()
}

#[cfg(test)]
mod tests;
