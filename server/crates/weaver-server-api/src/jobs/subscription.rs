// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::jobs::types::{
    DuplicateSummaryInfo, PreparedQueueFilter, load_duplicate_summaries_chunked,
    matches_queue_event_filter_prepared,
};

#[derive(Default)]
pub(crate) struct JobsSubscription;

#[Subscription]
impl JobsSubscription {
    /// Subscribe to real-time job state snapshots.
    ///
    /// Pushes a full snapshot when the job list or the global pause changes,
    /// at most once per second however fast the queue moves. Also ticks every
    /// 2s so speed gauges stay fresh; a tick that finds the jobs, metrics,
    /// pause and block state unchanged sends nothing.
    #[graphql(guard = "AdminGuard")]
    async fn job_updates(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = JobsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let throttled = Box::pin(snapshot_triggers(&handle, FULL_SNAPSHOT_PERIOD));

        let mut last_state: Option<SnapshotState> = None;
        let stream = throttled.filter_map(move |trigger| {
            let state = SnapshotState::capture(&handle);
            let unchanged = last_state.as_ref() == Some(&state);
            if matches!(trigger, SnapshotTrigger::Heartbeat) && unchanged {
                return None;
            }
            let jobs = handle.list_jobs().iter().map(Job::from).collect();
            let metrics = Metrics::from(&state.metrics);
            let is_paused = state.is_paused;
            let download_block = DownloadBlock::from(&state.download_block);
            last_state = Some(state);

            Some(JobsSnapshot {
                jobs,
                metrics,
                is_paused,
                download_block,
            })
        });

        Ok(stream)
    }
    /// Subscribe to the public queue snapshot facade.
    #[graphql(guard = "ReadGuard")]
    async fn queue_snapshots(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<impl Stream<Item = Result<QueueSnapshot>>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx
            .data::<weaver_server_core::settings::SharedConfig>()?
            .clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let state_handle = ctx
            .data::<weaver_server_core::SharedPipelineState>()?
            .clone();
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());
        let throttled = Box::pin(snapshot_triggers(&handle, FULL_SNAPSHOT_PERIOD));

        let memo = Arc::new(std::sync::Mutex::new(QueueSnapshotMemo::default()));
        let stream = throttled
            .then(move |trigger| {
                let handle = handle.clone();
                let config = config.clone();
                let replay = replay.clone();
                let db = db.clone();
                let prepared_filter = prepared_filter.clone();
                let memo = memo.clone();
                let state_handle = state_handle.clone();
                async move {
                    let state = SnapshotState::capture(&handle);
                    let max_download_speed = config.read().await.max_download_speed.unwrap_or(0);
                    {
                        let mut memo = memo.lock().expect("queue snapshot memo poisoned");
                        let unchanged = memo.last.as_ref().is_some_and(|(last, speed)| {
                            *last == state && *speed == max_download_speed
                        });
                        if matches!(trigger, SnapshotTrigger::Heartbeat) && unchanged {
                            return None;
                        }
                        memo.last = Some((state.clone(), max_download_speed));
                    }
                    let mut items: Vec<QueueItem> =
                        crate::jobs::paging::read_listing(
                            &state_handle,
                            prepared_filter.as_ref(),
                            0,
                            usize::MAX,
                        )
                        .iter()
                        .map(queue_item_from_job)
                        .collect();
                    let cached = {
                        let memo = memo.lock().expect("queue snapshot memo poisoned");
                        memo.summaries
                            .as_ref()
                            .filter(|(revision, _)| *revision == state.revision)
                            .map(|(_, summaries)| summaries.clone())
                    };
                    let summaries = match cached {
                        Some(summaries) => summaries,
                        None => {
                            // Duplicate summaries follow the job set, so they are
                            // read once per job revision rather than once per
                            // segment-driven snapshot.
                            let summaries = load_duplicate_summary_infos(db, &items).await;
                            let summaries = match summaries {
                                Ok(summaries) => Arc::new(summaries),
                                Err(error) => return Some(Err(error)),
                            };
                            memo.lock().expect("queue snapshot memo poisoned").summaries =
                                Some((state.revision, summaries.clone()));
                            summaries
                        }
                    };
                    for item in &mut items {
                        if let Some(summary) = summaries.get(&item.id) {
                            item.duplicate_summary = Some(summary.clone());
                        }
                    }
                    let latest_cursor = replay.latest_cursor().await;
                    // Read the gauges after the awaits above, so the snapshot
                    // carries the speeds published as it goes out.
                    let metrics = &handle.get_metrics();
                    Some(Ok(QueueSnapshot {
                        summary: queue_summary(&items, metrics),
                        metrics: metrics_from_snapshot(metrics),
                        global_state: global_queue_state(
                            state.is_paused,
                            &state.download_block,
                            max_download_speed,
                        ),
                        items,
                        latest_cursor,
                        generated_at: chrono::Utc::now(),
                    }))
                }
            })
            .filter_map(|snapshot| snapshot);

        Ok(stream)
    }
    /// Subscribe to one queue page: the page `input` names, sent at once and
    /// again when the queue, the global pause or the speeds move, at most
    /// twice a second. Only that page's rows travel, never the whole queue.
    #[graphql(guard = "ReadGuard")]
    async fn queue_page_updates(
        &self,
        ctx: &Context<'_>,
        input: QueuePageInput,
    ) -> Result<impl Stream<Item = Result<QueuePage>>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let state = ctx
            .data::<weaver_server_core::SharedPipelineState>()?
            .clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let wants = crate::jobs::query::page_wants(ctx);
        let mut triggers = Box::pin(snapshot_triggers(&handle, PAGE_UPDATE_PERIOD));
        Ok(async_stream::stream! {
            // What a page shows beyond its rows' revision: the live speed in
            // its summary, and the global pause its row states read. A wake
            // that moves none of it sends nothing.
            let mut last: Option<(u64, u64, bool)> = None;
            let mut duplicates = crate::jobs::query::DuplicateSummaryCache::default();
            while triggers.next().await.is_some() {
                let captured = (
                    handle.job_revision(),
                    handle.get_metrics().current_download_speed,
                    handle.is_globally_paused(),
                );
                if last == Some(captured) {
                    continue;
                }
                last = Some(captured);
                yield crate::jobs::query::build_queue_page(
                    &handle, &state, &replay, db.clone(), &input, wants, &mut duplicates,
                ).await;
            }
        })
    }
    /// Subscribe to live public queue events.
    ///
    /// Replays recent in-memory queue events after `after`, then continues
    /// streaming live events from the same bounded replay buffer.
    #[graphql(guard = "ReadGuard")]
    async fn queue_events(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        filter: Option<QueueFilterInput>,
    ) -> Result<impl Stream<Item = Result<QueueEvent>>> {
        let after = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let mut rx = replay.subscribe();
        let initial = replay
            .replay_after(after)
            .await
            .map_err(|error| graphql_error("CURSOR_EXPIRED", error.to_string()))?;
        let filter_for_stream = filter.clone();

        Ok(async_stream::stream! {
            let mut last_seen = after.unwrap_or(0);
            let prepared_filter = PreparedQueueFilter::new(filter_for_stream.as_ref());

            for notification in initial {
                if notification.id <= last_seen {
                    continue;
                }
                last_seen = notification.id;
                if matches_queue_event_filter_prepared(&notification.event, prepared_filter.as_ref()) {
                    yield enrich_queue_event(db.clone(), notification.event).await;
                }
            }

            loop {
                match rx.recv().await {
                    Ok(notification) => {
                        if notification.id <= last_seen {
                            continue;
                        }
                        last_seen = notification.id;
                        if matches_queue_event_filter_prepared(&notification.event, prepared_filter.as_ref()) {
                            yield enrich_queue_event(db.clone(), notification.event).await;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(
                            skipped,
                            "queue event subscription lagged; replaying from bounded buffer"
                        );
                        match replay.replay_after(Some(last_seen)).await {
                            Ok(notifications) => {
                                for notification in notifications {
                                    if notification.id <= last_seen {
                                        continue;
                                    }
                                    last_seen = notification.id;
                                    if matches_queue_event_filter_prepared(&notification.event, prepared_filter.as_ref()) {
                                        yield enrich_queue_event(db.clone(), notification.event).await;
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    error = %error,
                                    "queue event subscription cursor expired while recovering from lag"
                                );
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}

async fn load_duplicate_summary_infos(
    db: Database,
    items: &[QueueItem],
) -> Result<HashMap<u64, DuplicateSummaryInfo>> {
    let job_ids = items
        .iter()
        .map(|item| weaver_server_core::jobs::ids::JobId(item.id))
        .collect::<Vec<_>>();
    let summaries =
        tokio::task::spawn_blocking(move || load_duplicate_summaries_chunked(&db, job_ids))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
    Ok(summaries
        .iter()
        .map(|(job_id, summary)| (job_id.0, DuplicateSummaryInfo::from_summary(summary)))
        .collect())
}

async fn attach_duplicate_summaries(db: Database, items: &mut [QueueItem]) -> Result<()> {
    let summaries = load_duplicate_summary_infos(db, items).await?;
    for item in items {
        if let Some(summary) = summaries.get(&item.id) {
            // Preserve an event's already-enriched summary if the current
            // lookup has no row, rather than replacing a client badge with
            // null during a transient/replayed state transition.
            item.duplicate_summary = Some(summary.clone());
        }
    }
    Ok(())
}

// What a heartbeat compares against the last snapshot it sent: a tick that
// finds all of it unchanged has nothing new to tell the client.
#[derive(Clone, PartialEq)]
struct SnapshotState {
    revision: u64,
    metrics: weaver_server_core::operations::metrics::MetricsSnapshot,
    is_paused: bool,
    download_block: weaver_server_core::jobs::handle::DownloadBlockState,
}

impl SnapshotState {
    fn capture(handle: &SchedulerHandle) -> Self {
        Self {
            revision: handle.job_revision(),
            metrics: handle.get_metrics(),
            is_paused: handle.is_globally_paused(),
            download_block: handle.get_download_block(),
        }
    }
}

#[derive(Default)]
struct QueueSnapshotMemo {
    last: Option<(SnapshotState, u64)>,
    summaries: Option<(u64, Arc<HashMap<u64, DuplicateSummaryInfo>>)>,
}

async fn enrich_queue_event(db: Database, mut event: QueueEvent) -> Result<QueueEvent> {
    if let Some(item) = event.item.as_mut() {
        // Queue events represent one item each, so this is a single bounded
        // lookup rather than an N+1 list resolver.
        attach_duplicate_summaries(db, std::slice::from_mut(item)).await?;
    }
    Ok(event)
}

#[derive(Clone, Copy)]
enum SnapshotTrigger {
    ItemsChanged,
    Heartbeat,
}

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(2);
/// The fastest a whole-queue snapshot is rebuilt for one subscriber.
const FULL_SNAPSHOT_PERIOD: Duration = Duration::from_secs(1);
/// The fastest one subscriber's page is rebuilt.
const PAGE_UPDATE_PERIOD: Duration = Duration::from_millis(500);

/// What wakes a snapshot subscriber: the published job list moving, the
/// global pause flipping, or the heartbeat. Everything that moves within one
/// `period` folds into one trigger, so a subscriber builds at most one answer
/// per period however fast the queue publishes. The first trigger fires at
/// once.
fn snapshot_triggers(
    handle: &SchedulerHandle,
    period: Duration,
) -> impl Stream<Item = SnapshotTrigger> + Send + use<> {
    use tokio::sync::broadcast::error::RecvError;
    use weaver_server_core::events::model::PipelineEvent;

    let mut changes = handle.subscribe_job_changes();
    let mut events = handle.subscribe_events();
    async_stream::stream! {
        changes.borrow_and_update();
        yield SnapshotTrigger::ItemsChanged;
        let mut last_sent = tokio::time::Instant::now();
        let mut heartbeat = tokio::time::interval_at(
            last_sent + SNAPSHOT_HEARTBEAT,
            SNAPSHOT_HEARTBEAT,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let trigger = tokio::select! {
                changed = changes.changed() => match changed {
                    Ok(()) => SnapshotTrigger::ItemsChanged,
                    Err(_) => break,
                },
                event = events.recv() => match event {
                    Ok(PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed)
                    | Err(RecvError::Lagged(_)) => SnapshotTrigger::ItemsChanged,
                    Ok(_) => continue,
                    Err(RecvError::Closed) => break,
                },
                _ = heartbeat.tick() => SnapshotTrigger::Heartbeat,
            };
            // Hold until a period has passed since the last answer; every
            // publish in between is read by the one answer that follows.
            tokio::time::sleep_until(last_sent + period).await;
            changes.borrow_and_update();
            last_sent = tokio::time::Instant::now();
            yield trigger;
        }
    }
}
