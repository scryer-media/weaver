use super::*;

#[derive(Default)]
pub(crate) struct SystemSubscription;

const METRICS_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[Subscription]
impl SystemSubscription {
    /// Subscribe to live log lines from the service.
    #[graphql(guard = "AdminGuard")]
    async fn service_log_lines(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = String>> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let rx = buffer.subscribe();
        Ok(tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|result| result.ok()))
    }

    /// Subscribe to cadence-driven system metrics and global queue state.
    #[graphql(guard = "ReadGuard")]
    async fn system_metrics_updates(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = SystemMetricsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx.data::<SharedConfig>()?.clone();

        Ok(async_stream::stream! {
            yield build_system_metrics_snapshot(&handle, &config).await;

            let mut interval = tokio::time::interval(METRICS_UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                yield build_system_metrics_snapshot(&handle, &config).await;
            }
        })
    }
}

async fn build_system_metrics_snapshot(
    handle: &SchedulerHandle,
    config: &SharedConfig,
) -> SystemMetricsSnapshot {
    let metrics = Metrics::from(&handle.get_metrics());
    let download_block = handle.get_download_block();
    let is_paused = handle.is_globally_paused();
    let speed_limit_bytes_per_sec = config.read().await.max_download_speed.unwrap_or(0);

    SystemMetricsSnapshot {
        metrics,
        global_state: global_queue_state(is_paused, &download_block, speed_limit_bytes_per_sec),
    }
}
