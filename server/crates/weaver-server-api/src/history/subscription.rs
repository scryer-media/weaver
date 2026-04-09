use super::*;

#[derive(Default)]
pub(crate) struct HistorySubscription;

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
}

fn raw_pipeline_event_stream(handle: SchedulerHandle) -> impl Stream<Item = PipelineEventGql> {
    let rx = handle.subscribe_events();
    tokio_stream::wrappers::BroadcastStream::new(rx)
        .filter_map(|result| result.ok().map(|event| PipelineEventGql::from(&event)))
}
