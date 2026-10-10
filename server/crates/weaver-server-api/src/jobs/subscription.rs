use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::jobs::types::{
    DuplicateSummaryInfo, PreparedQueueFilter, load_duplicate_summaries_chunked,
    matches_queue_event_filter_prepared, matches_queue_filter_prepared,
};

#[derive(Default)]
pub(crate) struct JobsSubscription;

#[Subscription]
impl JobsSubscription {
    /// Subscribe to real-time job state snapshots.
    ///
    /// Pushes a full snapshot whenever a pipeline event fires, throttled to
    /// at most once per 100ms so rapid-fire segment events don't flood the
    /// client. Also ticks every 2s so speed gauges stay fresh; a tick that
    /// finds the jobs, metrics, pause and block state unchanged sends nothing.
    #[graphql(guard = "AdminGuard")]
    async fn job_updates(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = JobsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        // Per-article events trigger a snapshot too (throttled below); they
        // arrive on their own channel.
        let event_stream = tokio_stream::wrappers::BroadcastStream::new(handle.subscribe_events())
            .merge(tokio_stream::wrappers::BroadcastStream::new(
                handle.subscribe_segment_events(),
            ))
            .filter_map(|r| r.ok().map(|_| SnapshotTrigger::ItemsChanged));

        // Tick every 2s as a heartbeat so speed/metrics stay fresh even when
        // no events are firing (e.g. all jobs paused while gauges decay).
        let heartbeat =
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(SNAPSHOT_HEARTBEAT))
                .map(|_| SnapshotTrigger::Heartbeat);

        // Merge event triggers with heartbeat, then throttle to 100ms.
        let merged = event_stream.merge(heartbeat);
        let throttled = throttle(merged, SNAPSHOT_THROTTLE);

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
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());
        let event_stream = tokio_stream::wrappers::BroadcastStream::new(handle.subscribe_events())
            .merge(tokio_stream::wrappers::BroadcastStream::new(
                handle.subscribe_segment_events(),
            ))
            .filter_map(|r| r.ok().map(|_| SnapshotTrigger::ItemsChanged));
        let heartbeat =
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(SNAPSHOT_HEARTBEAT))
                .map(|_| SnapshotTrigger::Heartbeat);
        let initial = tokio_stream::once(SnapshotTrigger::ItemsChanged);
        let merged = initial.merge(event_stream).merge(heartbeat);
        let throttled = throttle(merged, SNAPSHOT_THROTTLE);

        let memo = Arc::new(std::sync::Mutex::new(QueueSnapshotMemo::default()));
        let stream = throttled
            .then(move |trigger| {
                let handle = handle.clone();
                let config = config.clone();
                let replay = replay.clone();
                let db = db.clone();
                let prepared_filter = prepared_filter.clone();
                let memo = memo.clone();
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
                    let mut items: Vec<QueueItem> = handle
                        .list_jobs()
                        .into_iter()
                        .filter(|info| {
                            !matches!(
                                info.status,
                                weaver_server_core::JobStatus::Complete
                                    | weaver_server_core::JobStatus::Failed { .. }
                            )
                        })
                        .map(|info| queue_item_from_job(&info))
                        .filter(|item| {
                            matches_queue_filter_prepared(item, prepared_filter.as_ref())
                        })
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
                    let metrics = &state.metrics;
                    let latest_cursor = replay.latest_cursor().await;
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
const SNAPSHOT_THROTTLE: Duration = Duration::from_millis(100);

fn throttle<S, T>(stream: S, period: Duration) -> impl Stream<Item = T>
where
    S: Stream<Item = T> + Unpin,
{
    async_stream::stream! {
        tokio::pin!(stream);
        let mut last = tokio::time::Instant::now() - period;

        while let Some(item) = stream.next().await {
            let now = tokio::time::Instant::now();
            let elapsed = now.duration_since(last);
            if elapsed < period {
                tokio::time::sleep(period - elapsed).await;
            }
            last = tokio::time::Instant::now();
            yield item;
        }
    }
}
