use tracing::{info, warn};

use crate::rss::model::{
    RSS_SEEN_RETENTION_SECS, RSS_SYNC_TICK_SECS, compile_rules, evaluate_item,
    feed_item_from_entry, is_due, unix_now_secs,
};
use crate::rss::service::{
    DueSyncOutcome, RssFeedSyncReport, RssService, RssServiceError, RssSyncReport,
};
use crate::{RssFeedRow, RssRuleAction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RssSyncTarget {
    AllEnabledFeeds,
    Feed(u32),
}

impl RssService {
    pub fn start_background_loop(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(RSS_SYNC_TICK_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
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
            }
        })
    }

    pub async fn reload_state(&self) {}

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

    async fn run_sync_inner(
        &self,
        target: RssSyncTarget,
        due_only: bool,
    ) -> Result<RssSyncReport, RssServiceError> {
        let feeds = self.load_target_feeds(target, due_only)?;
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
        let body = response
            .bytes()
            .await
            .map_err(|e| RssServiceError::Http(e.to_string()))?;
        let parsed = feed_rs::parser::parse(std::io::Cursor::new(body))
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
}
