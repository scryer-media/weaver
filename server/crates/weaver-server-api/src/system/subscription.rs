use super::*;
use crate::system::types::{JobDownloadRate, ProviderHoldoff};

#[derive(Default)]
pub(crate) struct SystemSubscription;

const METRICS_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

#[Subscription]
impl SystemSubscription {
    /// Subscribe to live log lines from the service.
    #[graphql(guard = "AdminGuard")]
    async fn service_log_lines(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = String>> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let rx = buffer.subscribe();
        Ok(tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|result| result.ok()))
    }

    /// Subscribe to release-check state changes, including the initial snapshot.
    #[graphql(guard = "ReadGuard")]
    async fn update_status_updates(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = UpdateStatus>> {
        let mut receiver = ctx
            .data::<weaver_server_core::update_check::UpdateCheckService>()?
            .subscribe();
        Ok(async_stream::stream! {
            let initial = receiver.borrow().clone();
            yield initial.into();
            while receiver.changed().await.is_ok() {
                let status = receiver.borrow().clone();
                yield status.into();
            }
        })
    }

    /// Subscribe to cadence-driven system metrics and global queue state.
    #[graphql(guard = "ReadGuard")]
    async fn system_metrics_updates(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = SystemMetricsSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx.data::<SharedConfig>()?.clone();
        let initial_metrics = handle.get_metrics();

        Ok(async_stream::stream! {
            yield build_system_metrics_snapshot(&handle, &config, initial_metrics).await;

            let mut interval = tokio::time::interval(METRICS_UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let metrics = handle.get_metrics();
                yield build_system_metrics_snapshot(&handle, &config, metrics).await;
            }
        })
    }
}

async fn build_system_metrics_snapshot(
    handle: &SchedulerHandle,
    config: &SharedConfig,
    metrics: weaver_server_core::MetricsSnapshot,
) -> SystemMetricsSnapshot {
    let metrics = Metrics::from(&metrics);
    let download_block = handle.get_download_block();
    let is_paused = handle.is_globally_paused();
    let speed_limit_bytes_per_sec = config.read().await.max_download_speed.unwrap_or(0);
    let provider_holdoffs = handle
        .nntp_pool()
        .map(|pool| provider_holdoffs(&pool))
        .unwrap_or_default();
    // Same read the queue readers make, minus the clone: the job list and the
    // metrics snapshot are both written by the orchestrator's 100 ms tick, so
    // the two figures here are at most one tick apart.
    let job_download_rates = handle
        .job_download_rates()
        .into_iter()
        .map(|(job_id, rate_bps)| JobDownloadRate {
            job_id: job_id.0,
            rate_bps,
        })
        .collect();

    SystemMetricsSnapshot {
        metrics,
        global_state: global_queue_state(is_paused, &download_block, speed_limit_bytes_per_sec),
        provider_holdoffs,
        job_download_rates,
    }
}

/// One atomic load per server; empty in the steady state.
fn provider_holdoffs(pool: &weaver_nntp::pool::NntpPool) -> Vec<ProviderHoldoff> {
    pool.server_configs()
        .iter()
        .enumerate()
        .filter_map(|(idx, cfg)| {
            let until_epoch_ms = pool.over_limit_until_epoch_ms(weaver_nntp::ServerId(idx))?;
            Some(ProviderHoldoff {
                label: format!("{}:{}", cfg.host, cfg.port),
                until_epoch_ms,
            })
        })
        .collect()
}
