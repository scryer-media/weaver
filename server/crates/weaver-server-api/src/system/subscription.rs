use super::*;
use std::time::Instant;

#[derive(Default)]
pub(crate) struct SystemSubscription;

const METRICS_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const SPEED_SAMPLE_MIN_ELAPSED_SEC: f64 = 0.05;
const SPEED_EMA_HALF_LIFE_SECS: f64 = 1.0;

#[derive(Debug)]
struct LiveSpeedSampler {
    last_sample: (Instant, u64),
    speed: u64,
    ema_speed: f64,
    last_ema_at: Option<Instant>,
}

impl LiveSpeedSampler {
    fn seeded(bytes_downloaded: u64, speed: u64) -> Self {
        let now = Instant::now();
        Self {
            last_sample: (now, bytes_downloaded),
            speed,
            ema_speed: speed as f64,
            last_ema_at: (speed > 0).then_some(now),
        }
    }

    fn sample(&mut self, bytes_downloaded: u64) -> u64 {
        let now = Instant::now();
        let (previous_at, previous_bytes) = self.last_sample;
        self.last_sample = (now, bytes_downloaded);

        let elapsed = now.duration_since(previous_at).as_secs_f64();
        if elapsed <= SPEED_SAMPLE_MIN_ELAPSED_SEC {
            return self.speed;
        }

        let delta_bytes = bytes_downloaded.saturating_sub(previous_bytes);
        let raw_speed = delta_bytes as f64 / elapsed;

        if let Some(last_ema_at) = self.last_ema_at {
            let ema_elapsed = now.duration_since(last_ema_at).as_secs_f64();
            if ema_elapsed > 0.0 {
                let alpha = 1.0 - 0.5_f64.powf(ema_elapsed / SPEED_EMA_HALF_LIFE_SECS);
                self.ema_speed += alpha * (raw_speed - self.ema_speed);
            } else {
                self.ema_speed = raw_speed;
            }
        } else {
            self.ema_speed = raw_speed;
        }

        self.last_ema_at = Some(now);
        if self.ema_speed < 1.0 {
            self.ema_speed = 0.0;
        }
        self.speed = self.ema_speed as u64;
        self.speed
    }
}

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
        let initial_metrics = handle.get_metrics();
        let mut speed_sampler = LiveSpeedSampler::seeded(
            initial_metrics.bytes_downloaded,
            initial_metrics.current_download_speed,
        );

        Ok(async_stream::stream! {
            yield build_system_metrics_snapshot(&handle, &config, initial_metrics).await;

            let mut interval = tokio::time::interval(METRICS_UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;
                let mut metrics = handle.get_live_metrics();
                metrics.current_download_speed = speed_sampler.sample(metrics.bytes_downloaded);
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

    SystemMetricsSnapshot {
        metrics,
        global_state: global_queue_state(is_paused, &download_block, speed_limit_bytes_per_sec),
    }
}
