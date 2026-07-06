use super::*;

#[derive(Default)]
pub(crate) struct HistorySubscription;

const JOB_DETAIL_HEARTBEAT: Duration = Duration::from_millis(500);
const JOB_DETAIL_THROTTLE: Duration = Duration::from_millis(250);
const JOB_DETAIL_SETTLE_DELAY: Duration = Duration::from_millis(25);
/// Slow cadence at which a terminal (archived) job's detail is still re-read, to
/// pick up DB-only changes that emit no pipeline event (e.g. a background
/// history-delete operation) without the fast per-tab heartbeat load.
const JOB_DETAIL_TERMINAL_HEARTBEAT: Duration = Duration::from_secs(5);

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

        // Only events for THIS job should force a database reload; an unrelated
        // job's event must not trigger a full re-read of this job's event log.
        let event_stream =
            tokio_stream::wrappers::BroadcastStream::new(event_rx).filter_map(move |result| {
                result.ok().and_then(|event| {
                    // Reload only on meaningful events for THIS job. `should_record_job_event`
                    // is the same predicate that gates the persisted event log the detail view
                    // reads, so it triggers exactly when the log or lifecycle changes while
                    // excluding the high-frequency per-article/segment events (the broadcast
                    // carries those too). Live progress is covered by the heartbeat. Uses the
                    // core job_id accessor (all variants, no allocation).
                    use weaver_server_core::events::publish::{
                        pipeline_job_id, should_record_job_event,
                    };
                    (should_record_job_event(&event) && pipeline_job_id(&event) == Some(job_id))
                        .then_some(JobDetailTrigger::Event)
                })
            });
        let heartbeat = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
            JOB_DETAIL_HEARTBEAT,
        ))
        .map(|_| JobDetailTrigger::Heartbeat);
        let initial = tokio_stream::once(JobDetailTrigger::Initial);
        let triggers = throttle(
            initial.merge(event_stream).merge(heartbeat),
            JOB_DETAIL_THROTTLE,
        );

        Ok(async_stream::stream! {
            tokio::pin!(triggers);
            let mut terminal_settled = false;
            let mut last_terminal_reload = tokio::time::Instant::now();

            while let Some(trigger) = triggers.next().await {
                // A terminal (archived) job's event log and history are
                // immutable, so the fast per-tab heartbeat need not re-read the
                // database. But DB-only changes that emit no pipeline event
                // (e.g. a background history-delete) must still surface, so a
                // terminal job is polled at a slow cadence instead of having its
                // heartbeats suppressed outright.
                if matches!(trigger, JobDetailTrigger::Heartbeat)
                    && terminal_settled
                    && last_terminal_reload.elapsed() < JOB_DETAIL_TERMINAL_HEARTBEAT
                {
                    continue;
                }
                if matches!(trigger, JobDetailTrigger::Event) {
                    tokio::time::sleep(JOB_DETAIL_SETTLE_DELAY).await;
                }

                match crate::schema::history_query::load_job_detail_snapshot(
                    handle.clone(),
                    db.clone(),
                    job_id,
                ).await {
                    Ok(snapshot) => {
                        terminal_settled =
                            snapshot.queue_item.is_none() && snapshot.history_item.is_some();
                        if terminal_settled {
                            last_terminal_reload = tokio::time::Instant::now();
                        }
                        yield snapshot;
                    }
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
