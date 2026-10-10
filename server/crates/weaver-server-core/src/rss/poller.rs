use tracing::{info, warn};

use crate::rss::model::{
    RSS_SEEN_RETENTION_SECS, RSS_SYNC_TICK_SECS, compile_rules, evaluate_item, is_due,
    parse_feed_items,
};
use crate::rss::service::{
    DueSyncOutcome, MAX_RSS_FEED_BODY_BYTES, RssFeedSyncReport, RssService, RssServiceError,
    RssSyncReport, read_response_with_limit,
};
use crate::{RssFeedRow, RssRuleAction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RssSyncTarget {
    AllEnabledFeeds,
    Feed(u32),
}

impl RssService {
    /// Fetch and filter without recording seen items or submitting downloads.
    pub async fn preview_feed(
        &self,
        feed_id: u32,
    ) -> Result<Vec<crate::RssSeenItemRow>, RssServiceError> {
        let db = self.inner.db.clone();
        let mut feed = tokio::task::spawn_blocking(move || db.get_rss_feed(feed_id))
            .await
            .map_err(|error| RssServiceError::Http(error.to_string()))?
            .map_err(|error| RssServiceError::Http(error.to_string()))?
            .ok_or(RssServiceError::FeedNotFound(feed_id))?;
        feed.etag = None;
        feed.last_modified = None;
        let response = self.fetch_feed_response(&feed).await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(RssServiceError::Http(format!(
                "feed returned HTTP {}",
                response.status()
            )));
        }
        let body = read_response_with_limit(response, MAX_RSS_FEED_BODY_BYTES)
            .await
            .map_err(RssServiceError::Http)?;
        let body = crate::post_processing::feed::transform_feed(
            &self.inner.db,
            &self.inner.config,
            &feed,
            body,
            MAX_RSS_FEED_BODY_BYTES,
        )
        .await
        .map_err(RssServiceError::Parse)?;
        let items = parse_feed_items(&body).map_err(RssServiceError::Parse)?;
        let db = self.inner.db.clone();
        let rules =
            tokio::task::spawn_blocking(move || db.list_rss_rules(feed_id).map(compile_rules))
                .await
                .map_err(|error| RssServiceError::Http(error.to_string()))?
                .map_err(|error| RssServiceError::Http(error.to_string()))?;
        Ok(items
            .into_iter()
            .map(|item| {
                let decision =
                    evaluate_item(&rules, &item).map_or("ignored", |rule| match rule.row.action {
                        RssRuleAction::Accept => "accepted",
                        RssRuleAction::Reject => "rejected",
                    });
                crate::RssSeenItemRow {
                    feed_id,
                    item_id: item.item_id,
                    item_title: item.title,
                    published_at: item.published_at,
                    size_bytes: item.size_bytes,
                    decision: decision.into(),
                    seen_at: self.now(),
                    job_id: None,
                    item_url: item.download_url.or(item.display_url),
                    error: None,
                }
            })
            .collect())
    }

    pub fn start_background_loop(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut paused = service.inner.scheduled_paused.subscribe();
            let mut changed = service.inner.db.rss_schedule_cache.changed.subscribe();
            loop {
                paused.borrow_and_update();
                changed.borrow_and_update();
                let svc = service.clone();
                match tokio::spawn(async move { svc.try_run_due_sync().await }).await {
                    Ok(Ok(DueSyncOutcome::Completed(report))) => {
                        if report.feeds_polled > 0 {
                            info!(
                                feeds_polled = report.feeds_polled,
                                items_submitted = report.items_submitted,
                                "RSS due sync complete"
                            );
                        }
                    }
                    Ok(Ok(DueSyncOutcome::SkippedActiveSync | DueSyncOutcome::NoFeedsDue)) => {}
                    Ok(Err(error)) => warn!(error = %error, "RSS due sync failed"),
                    Err(panic) => {
                        tracing::error!(error = %panic, "CRITICAL: RSS sync task panicked — loop continues");
                    }
                }
                let svc = service.clone();
                let delay = tokio::task::spawn_blocking(move || svc.next_due_sync_delay())
                    .await
                    .unwrap_or(Some(std::time::Duration::from_secs(RSS_SYNC_TICK_SECS)));
                tokio::select! {
                    _ = paused.changed() => {},
                    _ = changed.changed() => {},
                    _ = async {
                        match delay {
                            Some(delay) => tokio::time::sleep(delay).await,
                            None => std::future::pending().await,
                        }
                    } => {},
                }
            }
        })
    }

    /// How long the poller sleeps before looking again: until the earliest
    /// enabled feed falls due. Pauses and an empty feed list park the timer;
    /// edits and resume transitions wake the loop through watch channels.
    pub(super) fn next_due_sync_delay(&self) -> Option<std::time::Duration> {
        if self.is_scheduled_paused() {
            return None;
        }
        let tick = std::time::Duration::from_secs(RSS_SYNC_TICK_SECS);
        let Ok(schedules) = self.inner.db.list_rss_feed_schedules() else {
            return Some(tick);
        };
        let now = self.now();
        schedules
            .iter()
            .filter(|schedule| schedule.enabled)
            .map(|schedule| schedule.next_due_at().saturating_sub(now))
            .min()
            .map(|secs| {
                // A feed already due is retried on the next second rather
                // than in a tight loop, should its poll keep failing to land.
                std::time::Duration::from_secs(secs.max(1) as u64)
            })
    }

    pub async fn reload_state(&self) {
        self.inner.db.invalidate_rss_schedules();
    }

    pub async fn run_all_sync(&self) -> Result<RssSyncReport, RssServiceError> {
        let _guard = self.inner.sync_lock.lock().await;
        self.run_sync_inner(RssSyncTarget::AllEnabledFeeds, false)
            .await
    }

    pub async fn run_feed_sync(&self, feed_id: u32) -> Result<RssSyncReport, RssServiceError> {
        let _guard = self.inner.sync_lock.lock().await;
        self.run_sync_inner(RssSyncTarget::Feed(feed_id), false)
            .await
    }

    pub(crate) async fn try_run_due_sync(&self) -> Result<DueSyncOutcome, RssServiceError> {
        if self.is_scheduled_paused() {
            return Ok(DueSyncOutcome::NoFeedsDue);
        }
        let Ok(_guard) = self.inner.sync_lock.try_lock() else {
            return Ok(DueSyncOutcome::SkippedActiveSync);
        };
        let report = self
            .run_sync_inner(RssSyncTarget::AllEnabledFeeds, true)
            .await?;
        if report.feeds_polled == 0 {
            Ok(DueSyncOutcome::NoFeedsDue)
        } else {
            Ok(DueSyncOutcome::Completed(report))
        }
    }

    /// Fire-and-forget trigger used by the NZBGet `fetchfeeds` facade.
    ///
    /// Spawns a background sync and returns immediately. Concurrent callers
    /// are coalesced: if a sync is already running, this drops the request
    /// instead of queueing another full indexer sweep behind `sync_lock`.
    pub fn request_background_sync(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            let Ok(_guard) = service.inner.sync_lock.try_lock() else {
                // A sync is already in flight; drop this request rather than
                // queueing another one behind the lock.
                return;
            };
            if let Err(error) = service
                .run_sync_inner(RssSyncTarget::AllEnabledFeeds, false)
                .await
            {
                warn!(error = %error, "RSS background sync failed");
            }
        });
    }

    async fn run_sync_inner(
        &self,
        target: RssSyncTarget,
        due_only: bool,
    ) -> Result<RssSyncReport, RssServiceError> {
        self.run_sync_inner_cancellable(
            target,
            due_only,
            crate::bandwidth::schedule::ScheduleCancellation::new(),
        )
        .await
    }

    async fn run_sync_inner_cancellable(
        &self,
        target: RssSyncTarget,
        due_only: bool,
        cancellation: crate::bandwidth::schedule::ScheduleCancellation,
    ) -> Result<RssSyncReport, RssServiceError> {
        let service = self.clone();
        let feeds =
            tokio::task::spawn_blocking(move || service.load_target_feeds(target, due_only))
                .await
                .map_err(|error| RssServiceError::Http(error.to_string()))??;
        if feeds.is_empty() {
            return Ok(RssSyncReport::default());
        }

        let mut report = RssSyncReport::default();
        for feed in feeds {
            if cancellation.is_cancelled() {
                break;
            }
            let feed_report = self.sync_feed(&feed, &cancellation).await;
            if cancellation.is_cancelled() {
                break;
            }
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
                    let now = self.now();
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
            .purge_old_rss_seen_items(self.now() - RSS_SEEN_RETENTION_SECS);

        Ok(report)
    }

    pub(super) fn load_target_feeds(
        &self,
        target: RssSyncTarget,
        due_only: bool,
    ) -> Result<Vec<RssFeedRow>, RssServiceError> {
        let feeds = match target {
            RssSyncTarget::Feed(feed_id) => {
                let Some(feed) = self
                    .inner
                    .db
                    .get_rss_feed(feed_id)
                    .map_err(|e| RssServiceError::Http(e.to_string()))?
                else {
                    return Err(RssServiceError::FeedNotFound(feed_id));
                };
                vec![feed]
            }
            RssSyncTarget::AllEnabledFeeds if due_only => {
                // The due check needs four columns; the full rows, with their
                // decrypted credentials and attached scripts, are read only
                // for the feeds that are actually due.
                let now = self.now();
                let due: Vec<u32> = self
                    .inner
                    .db
                    .list_rss_feed_schedules()
                    .map_err(|e| RssServiceError::Http(e.to_string()))?
                    .into_iter()
                    .filter(|schedule| schedule.enabled && schedule.is_due(now))
                    .map(|schedule| schedule.id)
                    .collect();
                let mut feeds = Vec::with_capacity(due.len());
                for feed_id in due {
                    #[cfg(test)]
                    self.inner
                        .full_feed_loads
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if let Some(feed) = self
                        .inner
                        .db
                        .get_rss_feed(feed_id)
                        .map_err(|e| RssServiceError::Http(e.to_string()))?
                        // Re-checked on the full row: it may have been
                        // edited between the two reads.
                        .filter(|feed| feed.enabled && is_due(feed, now))
                    {
                        feeds.push(feed);
                    }
                }
                return Ok(feeds);
            }
            RssSyncTarget::AllEnabledFeeds => self
                .inner
                .db
                .list_rss_feeds()
                .map_err(|e| RssServiceError::Http(e.to_string()))?
                .into_iter()
                .filter(|feed| feed.enabled)
                .collect(),
        };

        if !due_only {
            return Ok(feeds);
        }

        let now = self.now();
        Ok(feeds.into_iter().filter(|feed| is_due(feed, now)).collect())
    }

    async fn sync_feed(
        &self,
        feed: &RssFeedRow,
        cancellation: &crate::bandwidth::schedule::ScheduleCancellation,
    ) -> Result<RssFeedSyncReport, RssServiceError> {
        let now = self.now();
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(RssFeedSyncReport::default()),
            result = self.fetch_feed_response(feed) => result?,
        };
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
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| feed.etag.clone());
        let last_modified = response
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| feed.last_modified.clone());
        let body = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(RssFeedSyncReport::default()),
            result = read_response_with_limit(response, MAX_RSS_FEED_BODY_BYTES) => result.map_err(RssServiceError::Http)?,
        };
        let body = crate::post_processing::feed::transform_feed(
            &self.inner.db,
            &self.inner.config,
            feed,
            body,
            MAX_RSS_FEED_BODY_BYTES,
        )
        .await
        .map_err(RssServiceError::Parse)?;
        let items = parse_feed_items(&body).map_err(RssServiceError::Parse)?;

        let rules = self
            .inner
            .db
            .list_rss_rules(feed.id)
            .map_err(|e| RssServiceError::Http(e.to_string()))?;
        let compiled_rules = compile_rules(rules);

        let mut report = RssFeedSyncReport {
            feed_id: feed.id,
            feed_name: feed.name.clone(),
            items_fetched: items.len() as u32,
            ..Default::default()
        };

        for item in items {
            if cancellation.is_cancelled() {
                return Ok(report);
            }
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
                    match self
                        .accept_item_cancellable(feed, &rule.row, &item, cancellation)
                        .await
                    {
                        Ok(job_id) => {
                            report.items_submitted += 1;
                            self.record_seen(feed.id, &item, "submitted", Some(job_id), None)?;
                        }
                        Err(error) => {
                            if cancellation.is_cancelled() {
                                return Ok(report);
                            }
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
}
