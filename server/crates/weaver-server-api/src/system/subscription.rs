use super::*;
use crate::system::types::{JobDownloadRate, ProviderConnections, ProviderHoldoff};

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

    /// Subscribe to upgrade progress, including the current snapshot.
    #[graphql(guard = "ReadGuard")]
    async fn application_upgrade_updates(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = crate::system::types::ApplicationUpgradeStatus>> {
        let service = ctx
            .data::<weaver_server_core::application_upgrade::ApplicationUpgradeService>()?
            .clone();
        let mut runs = service.subscribe();
        // The snapshot carries the release check as well as the run, so a
        // release found after the page subscribed has to reach it too, or the
        // install action stays hidden until the page is remounted.
        let mut releases = ctx
            .data::<weaver_server_core::update_check::UpdateCheckService>()?
            .subscribe();
        Ok(async_stream::stream! {
            yield service.snapshot().into();
            loop {
                let changed = tokio::select! {
                    changed = runs.changed() => changed,
                    changed = releases.changed() => changed,
                };
                if changed.is_err() {
                    break;
                }
                yield service.snapshot().into();
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
    let pool = handle.nntp_pool();
    let provider_holdoffs = pool.as_deref().map(provider_holdoffs).unwrap_or_default();
    let provider_connections = pool
        .as_deref()
        .map(provider_connections)
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
        provider_connections,
    }
}

/// One atomic load per server, the same count `serverHealth` reports.
fn provider_connections(pool: &weaver_nntp::pool::NntpPool) -> Vec<ProviderConnections> {
    pool.server_configs()
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let max = pool
                .configured_connections(weaver_nntp::ServerId(idx))
                .unwrap_or_else(|| pool.server_load(idx).1);
            ProviderConnections {
                label: format!("{}:{}", cfg.host, cfg.port),
                active: pool.active_connections(idx) as u32,
                max: max as u32,
            }
        })
        .collect()
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
