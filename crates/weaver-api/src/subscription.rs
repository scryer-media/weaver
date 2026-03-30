use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Context, Result, SimpleObject, Subscription};
use tokio_stream::{Stream, StreamExt};

use weaver_core::log_buffer::LogRingBuffer;
use weaver_scheduler::SchedulerHandle;

use crate::auth::{AdminGuard, ReadGuard, graphql_error};
use crate::facade::{
    PersistedQueueEvent, QueueEvent, QueueFilterInput, QueueItem, QueueSnapshot,
    decode_event_cursor, encode_event_cursor, global_queue_state, matches_queue_event_filter,
    matches_queue_filter, metrics_from_snapshot, queue_event_from_record, queue_item_from_job,
    queue_summary,
};
use crate::types::{DownloadBlock, Job, Metrics, PipelineEventGql};

/// Snapshot of all jobs + metrics, pushed over WebSocket.
#[derive(Debug, Clone, SimpleObject)]
pub struct JobsSnapshot {
    pub jobs: Vec<Job>,
    pub metrics: Metrics,
    pub is_paused: bool,
    pub download_block: DownloadBlock,
}

pub struct SubscriptionRoot;

#[derive(Clone, Copy)]
enum SnapshotTrigger {
    ItemsChanged,
    Heartbeat,
}

const SNAPSHOT_HEARTBEAT: Duration = Duration::from_secs(2);
const SNAPSHOT_THROTTLE: Duration = Duration::from_millis(100);
const EVENT_REPLAY_SETTLE_DELAY: Duration = Duration::from_millis(25);

fn raw_pipeline_event_stream(handle: SchedulerHandle) -> impl Stream<Item = PipelineEventGql> {
    let rx = handle.subscribe_events();
    tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|result| result.ok().map(|event| PipelineEventGql::from(&event)))
}

#[Subscription]
impl SubscriptionRoot {
    /// Subscribe to all pipeline events.
    #[graphql(guard = "AdminGuard")]
    async fn events(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        Ok(raw_pipeline_event_stream(handle))
    }

    /// Subscribe to live log lines from the service.
    #[graphql(guard = "AdminGuard")]
    async fn service_log_lines(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = String>> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let rx = buffer.subscribe();
        Ok(tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|result| result.ok()))
    }

    /// Admin-facing raw event stream.
    #[graphql(guard = "AdminGuard")]
    async fn admin_events(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        Ok(raw_pipeline_event_stream(handle))
    }

    /// Subscribe to real-time job state snapshots.
    ///
    /// Pushes a full snapshot whenever a pipeline event fires, throttled to
    /// at most once per 100ms so rapid-fire segment events don't flood the
    /// client. Also ticks every 2s during idle so speed gauges stay fresh.
    #[graphql(guard = "AdminGuard")]
    async fn job_updates(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = JobsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let event_rx = handle.subscribe_events();

        let event_stream = tokio_stream::wrappers::BroadcastStream::new(event_rx)
            .filter_map(|r| r.ok().map(|_| ()));

        // Tick every 2s as a heartbeat so speed/metrics stay fresh even when
        // no events are firing (e.g. idle, all jobs paused).
        let heartbeat =
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(SNAPSHOT_HEARTBEAT))
                .map(|_| ());

        // Merge event triggers with heartbeat, then throttle to 100ms.
        let merged = event_stream.merge(heartbeat);
        let throttled = throttle(merged, SNAPSHOT_THROTTLE);

        let stream = throttled.map(move |_| {
            let jobs = handle.list_jobs().iter().map(Job::from).collect();
            let metrics = Metrics::from(&handle.get_metrics());
            let is_paused = handle.is_globally_paused();
            let download_block = DownloadBlock::from(&handle.get_download_block());

            JobsSnapshot {
                jobs,
                metrics,
                is_paused,
                download_block,
            }
        });

        Ok(stream)
    }

    /// Subscribe to the public queue snapshot facade.
    #[graphql(guard = "ReadGuard")]
    async fn queue_snapshots(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<impl Stream<Item = QueueSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_state::Database>()?.clone();
        let config = ctx.data::<weaver_core::config::SharedConfig>()?.clone();
        let event_rx = handle.subscribe_events();
        let cached_items = Arc::new(tokio::sync::Mutex::new(Vec::<QueueItem>::new()));

        let event_stream = tokio_stream::wrappers::BroadcastStream::new(event_rx)
            .filter_map(|r| r.ok().map(|_| SnapshotTrigger::ItemsChanged));
        let heartbeat =
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(SNAPSHOT_HEARTBEAT))
                .map(|_| SnapshotTrigger::Heartbeat);
        let initial = tokio_stream::once(SnapshotTrigger::ItemsChanged);
        let merged = initial.merge(event_stream).merge(heartbeat);
        let throttled = throttle(merged, SNAPSHOT_THROTTLE);

        let stream = throttled.then(move |trigger| {
            let handle = handle.clone();
            let db = db.clone();
            let config = config.clone();
            let filter = filter.clone();
            let cached_items = cached_items.clone();
            async move {
                let items = if matches!(trigger, SnapshotTrigger::ItemsChanged) {
                    let rebuilt: Vec<QueueItem> = handle
                        .list_jobs()
                        .into_iter()
                        .filter(|info| {
                            !matches!(
                                info.status,
                                weaver_scheduler::JobStatus::Complete
                                    | weaver_scheduler::JobStatus::Failed { .. }
                            )
                        })
                        .map(|info| queue_item_from_job(&info))
                        .filter(|item| matches_queue_filter(item, filter.as_ref()))
                        .collect();
                    *cached_items.lock().await = rebuilt.clone();
                    rebuilt
                } else {
                    cached_items.lock().await.clone()
                };
                let metrics = handle.get_metrics();
                let latest_cursor =
                    tokio::task::spawn_blocking(move || db.latest_integration_event_id())
                        .await
                        .ok()
                        .and_then(|result| result.ok())
                        .flatten()
                        .map(encode_event_cursor)
                        .unwrap_or_else(|| encode_event_cursor(0));
                let cfg = config.read().await;
                QueueSnapshot {
                    summary: queue_summary(&items, &metrics),
                    metrics: metrics_from_snapshot(&metrics),
                    global_state: global_queue_state(
                        handle.is_globally_paused(),
                        &handle.get_download_block(),
                        cfg.max_download_speed.unwrap_or(0),
                    ),
                    items,
                    latest_cursor,
                    generated_at: chrono::Utc::now(),
                }
            }
        });

        Ok(stream)
    }

    /// Subscribe to replayable public queue events.
    #[graphql(guard = "ReadGuard")]
    async fn queue_events(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        filter: Option<QueueFilterInput>,
    ) -> Result<impl Stream<Item = QueueEvent>> {
        let mut cursor = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_state::Database>()?.clone();
        let mut rx = handle.subscribe_events();
        let filter_for_stream = filter.clone();

        Ok(async_stream::stream! {
            loop {
                let rows = match db.list_integration_events_after(cursor, None, Some(256)) {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to read integration events for subscription replay");
                        Vec::new()
                    }
                };

                let mut emitted_any = false;
                for row in rows {
                    cursor = Some(row.id);
                    let Ok(record) = serde_json::from_str::<PersistedQueueEvent>(&row.payload_json) else {
                        tracing::warn!(cursor = row.id, "failed to decode persisted integration event payload");
                        continue;
                    };
                    let event = queue_event_from_record(row.id, record);
                    if matches_queue_event_filter(&event, filter_for_stream.as_ref()) {
                        emitted_any = true;
                        yield event;
                    }
                }

                if emitted_any {
                    continue;
                }

                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Ok(_) => {
                                tokio::time::sleep(EVENT_REPLAY_SETTLE_DELAY).await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
        })
    }
}

/// Throttle a stream: emit at most once per `period`. When items arrive
/// faster than the period, only the latest trigger is kept and the snapshot
/// is produced after the cooldown expires.
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
