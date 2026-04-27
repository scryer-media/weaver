use super::*;

#[derive(Default)]
pub(crate) struct HistorySubscription;

const JOB_DETAIL_HEARTBEAT: Duration = Duration::from_millis(500);
const JOB_DETAIL_THROTTLE: Duration = Duration::from_millis(250);
const JOB_DETAIL_SETTLE_DELAY: Duration = Duration::from_millis(25);

#[Subscription]
impl HistorySubscription {
    /// Subscribe to all pipeline events.
    #[graphql(guard = "AdminGuard")]
    async fn events(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        Ok(raw_pipeline_event_stream(handle))
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

    /// Read-facing live snapshot for the job detail page.
    #[graphql(guard = "ReadGuard")]
    async fn job_detail_updates(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<impl Stream<Item = JobDetailSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        let event_rx = handle.subscribe_events();

        let event_stream = tokio_stream::wrappers::BroadcastStream::new(event_rx)
            .filter_map(|result| result.ok().map(|_| JobDetailTrigger::Event));
        let heartbeat = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
            JOB_DETAIL_HEARTBEAT,
        ))
        .map(|_| JobDetailTrigger::Heartbeat);
        let initial = tokio_stream::once(JobDetailTrigger::Initial);
        let triggers = throttle(initial.merge(event_stream).merge(heartbeat), JOB_DETAIL_THROTTLE);

        Ok(async_stream::stream! {
            tokio::pin!(triggers);

            while let Some(trigger) = triggers.next().await {
                if matches!(trigger, JobDetailTrigger::Event) {
                    tokio::time::sleep(JOB_DETAIL_SETTLE_DELAY).await;
                }

                match crate::schema::history_query::load_job_detail_snapshot(
                    handle.clone(),
                    db.clone(),
                    job_id,
                ).await {
                    Ok(snapshot) => yield snapshot,
                    Err(error) => tracing::warn!(job_id, error = ?error, "failed to build job detail snapshot"),
                }
            }
        })
    }
}

fn raw_pipeline_event_stream(handle: SchedulerHandle) -> impl Stream<Item = PipelineEventGql> {
    let rx = handle.subscribe_events();
    tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|result| result.ok().map(|event| PipelineEventGql::from(&event)))
}

#[derive(Clone, Copy)]
enum JobDetailTrigger {
    Initial,
    Event,
    Heartbeat,
}

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
