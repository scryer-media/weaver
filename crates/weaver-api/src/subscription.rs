use std::time::Duration;

use async_graphql::{Context, Result, SimpleObject, Subscription};
use tokio_stream::{Stream, StreamExt};

use weaver_scheduler::SchedulerHandle;

use crate::types::{Job, Metrics, PipelineEventGql};

/// Snapshot of all jobs + metrics, pushed over WebSocket.
#[derive(Debug, Clone, SimpleObject)]
pub struct JobsSnapshot {
    pub jobs: Vec<Job>,
    pub metrics: Metrics,
    pub is_paused: bool,
}

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// Subscribe to all pipeline events.
    async fn events(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let rx = handle.subscribe_events();

        Ok(
            tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|result| {
                result
                    .ok()
                    .map(|event| PipelineEventGql::from(&event))
            }),
        )
    }

    /// Subscribe to real-time job state snapshots.
    ///
    /// Pushes a full snapshot whenever a pipeline event fires, throttled to
    /// at most once per 500ms so rapid-fire segment events don't flood the
    /// client. Also ticks every 2s during idle so speed gauges stay fresh.
    async fn job_updates(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = JobsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let event_rx = handle.subscribe_events();

        let event_stream = tokio_stream::wrappers::BroadcastStream::new(event_rx)
            .filter_map(|r| r.ok().map(|_| ()));

        // Tick every 2s as a heartbeat so speed/metrics stay fresh even when
        // no events are firing (e.g. idle, all jobs paused).
        let heartbeat = tokio_stream::wrappers::IntervalStream::new(
            tokio::time::interval(Duration::from_secs(2)),
        )
        .map(|_| ());

        // Merge event triggers with heartbeat, then throttle to 100ms.
        let merged = event_stream.merge(heartbeat);
        let throttled = throttle(merged, Duration::from_millis(100));

        let stream = throttled.then(move |_| {
            let h = handle.clone();
            async move {
                let jobs = h
                    .list_jobs()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(Job::from)
                    .collect();
                let metrics = h
                    .get_metrics()
                    .await
                    .map(|s| Metrics::from(&s))
                    .unwrap_or_else(|_| Metrics::default());
                let is_paused = h.is_globally_paused().await.unwrap_or(false);

                JobsSnapshot {
                    jobs,
                    metrics,
                    is_paused,
                }
            }
        });

        Ok(stream)
    }
}

/// Throttle a stream: emit at most once per `period`. When items arrive
/// faster than the period, only the latest trigger is kept and the snapshot
/// is produced after the cooldown expires.
fn throttle<S>(stream: S, period: Duration) -> impl Stream<Item = ()>
where
    S: Stream<Item = ()> + Unpin,
{
    async_stream::stream! {
        tokio::pin!(stream);
        let mut last = tokio::time::Instant::now() - period;

        while let Some(()) = stream.next().await {
            let now = tokio::time::Instant::now();
            let elapsed = now.duration_since(last);
            if elapsed < period {
                tokio::time::sleep(period - elapsed).await;
            }
            last = tokio::time::Instant::now();
            yield ();
        }
    }
}
