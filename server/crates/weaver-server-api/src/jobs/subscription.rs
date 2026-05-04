use super::*;

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
    ) -> Result<impl Stream<Item = QueueSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx
            .data::<weaver_server_core::settings::SharedConfig>()?
            .clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
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
            let filter = filter.clone();
            async move {
                let items: Vec<QueueItem> = handle
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
                    .filter(|item| matches_queue_filter(item, filter.as_ref()))
                    .collect();
                let metrics = handle.get_metrics();
                let latest_cursor = replay.latest_cursor().await;
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
    ) -> Result<impl Stream<Item = QueueEvent>> {
        let after = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let mut rx = replay.subscribe();
        let initial = replay
            .replay_after(after)
            .await
            .map_err(|error| graphql_error("CURSOR_EXPIRED", error.to_string()))?;
        let filter_for_stream = filter.clone();

        Ok(async_stream::stream! {
            let mut last_seen = after.unwrap_or(0);

            for notification in initial {
                if notification.id <= last_seen {
                    continue;
                }
                last_seen = notification.id;
                if matches_queue_event_filter(&notification.event, filter_for_stream.as_ref()) {
                    yield notification.event;
                }
            }

            loop {
                match rx.recv().await {
                    Ok(notification) => {
                        if notification.id <= last_seen {
                            continue;
                        }
                        last_seen = notification.id;
                        if matches_queue_event_filter(&notification.event, filter_for_stream.as_ref()) {
                            yield notification.event;
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
                                    if matches_queue_event_filter(&notification.event, filter_for_stream.as_ref()) {
                                        yield notification.event;
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
