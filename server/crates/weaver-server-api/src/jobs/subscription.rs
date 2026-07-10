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
    ) -> Result<impl Stream<Item = Result<QueueSnapshot>>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx
            .data::<weaver_server_core::settings::SharedConfig>()?
            .clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());
        let event_rx = handle.subscribe_events();

        let event_stream = tokio_stream::wrappers::BroadcastStream::new(event_rx)
            .filter_map(|r| r.ok().map(|_| SnapshotTrigger::ItemsChanged));
        let heartbeat =
            tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(SNAPSHOT_HEARTBEAT))
                .map(|_| SnapshotTrigger::Heartbeat);
        let initial = tokio_stream::once(SnapshotTrigger::ItemsChanged);
        let merged = initial.merge(event_stream).merge(heartbeat);
        let throttled = throttle(merged, SNAPSHOT_THROTTLE);

        let stream = throttled.then(move |_| {
            let handle = handle.clone();
            let config = config.clone();
            let replay = replay.clone();
            let db = db.clone();
            let prepared_filter = prepared_filter.clone();
            async move {
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
                    .filter(|item| matches_queue_filter_prepared(item, prepared_filter.as_ref()))
                    .collect();
                attach_duplicate_summaries(db, &mut items).await?;
                let metrics = handle.get_metrics();
                let latest_cursor = replay.latest_cursor().await;
                let cfg = config.read().await;
                Ok(QueueSnapshot {
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
                })
            }
        });

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

async fn attach_duplicate_summaries(db: Database, items: &mut [QueueItem]) -> Result<()> {
    let job_ids = items
        .iter()
        .map(|item| weaver_server_core::jobs::ids::JobId(item.id))
        .collect::<Vec<_>>();
    let summaries =
        tokio::task::spawn_blocking(move || load_duplicate_summaries_chunked(&db, job_ids))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
    for item in items {
        if let Some(summary) = summaries.get(&weaver_server_core::jobs::ids::JobId(item.id)) {
            // Preserve an event's already-enriched summary if the current
            // lookup has no row, rather than replacing a client badge with
            // null during a transient/replayed state transition.
            item.duplicate_summary = Some(DuplicateSummaryInfo::from_summary(summary));
        }
    }
    Ok(())
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
