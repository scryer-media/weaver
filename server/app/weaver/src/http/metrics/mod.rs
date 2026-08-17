//! The Prometheus `/metrics` endpoint.
//!
//! Layout:
//! - [`encode`] owns the text-exposition writer. A sample can only be written
//!   through a [`encode::MetricFamily`], which is what makes "sample with no
//!   HELP/TYPE" impossible to express.
//! - [`catalog`] is the catalogue of every family the exporter can emit. It is
//!   data, not code, and `docs/metrics.md` is checked against it.
//! - [`render`] turns a runtime snapshot into exposition text.
//!
//! Everything here runs at scrape time. Nothing in this module may add work to
//! a pipeline path, and the brief hold of the NNTP health lock in
//! [`collect_server_health`] reads fields only — every allocation happens
//! before the lock is taken.

pub(super) mod catalog;
pub(super) mod encode;
mod render;

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::extract::Extension;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;

use weaver_nntp::pool::NntpPool;
use weaver_server_core::operations::disk::DiskSpaceCollector;
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::{PerJobSeries, SharedConfig};
use weaver_server_core::{Database, SchedulerHandle};

#[cfg(test)]
pub(super) use encode::{Encoder, escape_prometheus_label_value};
pub(super) use render::PrometheusRenderInput;
#[cfg(test)]
pub(super) use render::{
    OBSERVED_LIMITERS, job_status_label, render_post_processing, render_prometheus_metrics,
    render_prometheus_metrics_input,
};

/// Reasons the extraction guardrails refuse an archive entry, in the order
/// `SchedulerHandle::get_extraction_rejections` returns their counters.
pub(super) const EXTRACTION_REJECTION_REASONS: [&str; 9] = [
    "unsafe_path",
    "unsupported_entry",
    "member_bytes",
    "job_bytes",
    "ratio",
    "entries",
    "deadline",
    "memory",
    "disk_reserve",
];

/// Process start, captured when the exporter is built during startup.
static PROCESS_START_EPOCH_SECONDS: OnceLock<f64> = OnceLock::new();

fn process_start_epoch_seconds() -> f64 {
    *PROCESS_START_EPOCH_SECONDS.get_or_init(unix_epoch_seconds_now)
}

fn unix_epoch_seconds_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or(0.0)
}

/// A single reading of both clocks, used to place monotonic deadlines on the
/// unix timeline.
///
/// `Instant` has no epoch of its own, so the wall clock is anchored once and
/// each deadline's remaining monotonic distance is added to it. Sampling once
/// per scrape also keeps a many-server render from making one clock syscall per
/// server.
#[derive(Clone, Copy)]
struct EpochClock {
    epoch_seconds: f64,
    instant: Instant,
}

impl EpochClock {
    fn sample() -> Self {
        Self {
            epoch_seconds: unix_epoch_seconds_now(),
            instant: Instant::now(),
        }
    }

    /// Deadlines already in the past collapse to zero, which is also what "no
    /// deadline" renders as.
    fn epoch_seconds_at(self, deadline: Option<Instant>) -> f64 {
        let Some(deadline) = deadline else {
            return 0.0;
        };
        if deadline <= self.instant {
            return 0.0;
        }
        self.epoch_seconds + (deadline - self.instant).as_secs_f64()
    }
}

/// Immutable facts about this binary, rendered as `weaver_build_info` labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BuildInfo {
    pub(super) version: &'static str,
    pub(super) commit: &'static str,
    pub(super) target_arch: &'static str,
    pub(super) target_os: &'static str,
    pub(super) decoder_tier: &'static str,
    pub(super) database_backend: &'static str,
    pub(super) tls_backend: &'static str,
}

impl BuildInfo {
    fn new(database_backend: &'static str) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            // `build.rs` always sets this, falling back to "unknown" outside a
            // git checkout; `option_env!` keeps a stale build cache honest too.
            commit: option_env!("WEAVER_GIT_COMMIT").unwrap_or("unknown"),
            target_arch: std::env::consts::ARCH,
            target_os: std::env::consts::OS,
            decoder_tier: weaver_yenc::simd::selected_decoder_tier().as_str(),
            database_backend,
            // Sits next to `decoder_tier` for the same reason: both are
            // runtime-resolved choices that decide how fast this build can go,
            // and both are invisible from the outside without a label.
            tls_backend: weaver_nntp::tls::selected_tls_backend_name(),
        }
    }
}

#[cfg(test)]
impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            version: "test-version",
            commit: "unknown",
            target_arch: std::env::consts::ARCH,
            target_os: std::env::consts::OS,
            decoder_tier: "scalar",
            database_backend: "sqlite",
            tls_backend: "rustls",
        }
    }
}

#[derive(Clone)]
pub(crate) struct PrometheusMetricsExporter {
    handle: SchedulerHandle,
    db: Database,
    nntp_pool: Arc<NntpPool>,
    transfer_policy:
        Arc<weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry>,
    config: SharedConfig,
    build: BuildInfo,
}

impl PrometheusMetricsExporter {
    pub(crate) fn new(
        handle: SchedulerHandle,
        db: Database,
        nntp_pool: Arc<NntpPool>,
        transfer_policy: Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >,
        config: SharedConfig,
    ) -> Self {
        let build = BuildInfo::new(db.engine_name());
        process_start_epoch_seconds();
        Self {
            handle,
            db,
            nntp_pool,
            transfer_policy,
            config,
            build,
        }
    }

    pub(crate) async fn render(
        &self,
        disk_space: &Arc<DiskSpaceCollector>,
        http_metrics: &super::HttpMetricsHandle,
    ) -> String {
        let snapshot = self.handle.get_metrics();
        let jobs = self.handle.list_jobs();
        let download_block = self.handle.get_download_block();
        let nntp_pool = self
            .handle
            .nntp_pool()
            .unwrap_or_else(|| Arc::clone(&self.nntp_pool));
        let runtime_generation = self
            .handle
            .nntp_runtime_activation()
            .map(|activation| activation.generation)
            .unwrap_or(0);
        let server_health = collect_server_health(&nntp_pool).await;
        let server_transfers = self.transfer_policy.transfer_registry().snapshots();
        let per_job_series = self.config.read().await.metrics.per_job_series;

        let extraction_rejections: Vec<(&'static str, u64)> = EXTRACTION_REJECTION_REASONS
            .into_iter()
            .zip(self.handle.get_extraction_rejections())
            .collect();

        // Both of these touch a blocking resource — the database executor and
        // the filesystem — so they share one blocking task rather than stalling
        // the async runtime. The free-space sample is TTL-cached, so most
        // scrapes do not stat anything at all.
        let db = self.db.clone();
        let disk_collector = Arc::clone(disk_space);
        let (post_processing, disk_space) = match tokio::task::spawn_blocking(move || {
            (
                db.post_processing_metrics_snapshot(),
                disk_collector.sample(super::DISK_SPACE_SAMPLE_TTL),
            )
        })
        .await
        {
            Ok((Ok(metrics), disk)) => (Some(metrics), disk),
            Ok((Err(error), disk)) => {
                tracing::debug!(error = %error, "failed to collect post-processing metrics");
                (None, disk)
            }
            Err(error) => {
                tracing::debug!(error = %error, "scrape-time blocking collection failed");
                (None, Vec::new())
            }
        };

        let server_metrics = self.handle.server_metrics_snapshot();
        let job_lifecycle = self.handle.job_lifecycle_metrics_snapshot();
        let pipeline_histograms = self.handle.pipeline_histograms_snapshot();
        let db_runtime = self.db.runtime_metrics_snapshot();
        let process = weaver_server_core::runtime::process_metrics::sample();
        let http_metrics = http_metrics.snapshot();

        let duplicate_admission: Vec<_> =
            weaver_server_core::jobs::duplicate_admission_metrics_snapshot()
                .into_iter()
                .map(|metric| (metric.origin, metric.status, metric.count))
                .collect();
        let semantic_duplicate_lifecycle: Vec<_> =
            weaver_server_core::jobs::semantic_duplicate_lifecycle_metrics_snapshot()
                .into_iter()
                .map(|metric| (metric.event, metric.count))
                .collect();

        let mut input = PrometheusRenderInput::new(&snapshot, &download_block);
        input.jobs = &jobs;
        input.pipeline_paused = self.handle.is_globally_paused();
        input.server_health = &server_health;
        input.runtime_generation = runtime_generation;
        input.server_transfers = &server_transfers;
        input.duplicate_admission = &duplicate_admission;
        input.semantic_duplicate_lifecycle = &semantic_duplicate_lifecycle;
        input.extraction_rejections = &extraction_rejections;
        input.post_processing = post_processing.as_ref();
        input.build = self.build;
        input.start_time_seconds = process_start_epoch_seconds();
        input.per_job_series = per_job_series;
        input.server_metrics = &server_metrics;
        input.job_lifecycle = Some(&job_lifecycle);
        input.pipeline_histograms = Some(&pipeline_histograms);
        input.db_runtime = Some(&db_runtime);
        input.process = Some(&process);
        input.disk_space = &disk_space;
        input.http_metrics = Some(&http_metrics);
        render::render_prometheus_metrics_input(&input)
    }
}

pub(super) async fn metrics_handler(
    Extension(exporter): Extension<PrometheusMetricsExporter>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    // `build_router` consumes the `ServerRuntime` and re-publishes these two
    // scrape-time collectors as extensions; taking them here is how the
    // exporter reaches them without a second copy in its own state.
    Extension(disk_space): Extension<Arc<DiskSpaceCollector>>,
    Extension(http_metrics): Extension<super::HttpMetricsHandle>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if security.metrics_auth_required {
        let scope = super::auth::resolve_scope(
            &request_auth.db,
            &request_auth.auth_cache,
            &request_auth.api_key_cache,
            request_auth.session_token.0.as_str(),
            &security,
            // A scrape is not a browser: Prometheus presents a Read-scoped API
            // key, never the SPA's session cookie, so the browser-session path
            // stays closed here.
            super::auth::BrowserSessionPolicy::Denied,
            &headers,
        )
        .await?;
        if !scope.can_read() {
            return Err(StatusCode::FORBIDDEN);
        }
    }

    let body = exporter.render(&disk_space, &http_metrics).await;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8".to_string(),
        )],
        body,
    ))
}

/// Coarse server health state, as a state-set label rather than a boolean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ServerStateKind {
    Healthy,
    Degraded,
    CoolingDown,
    Disabled,
}

impl ServerStateKind {
    pub(super) const ALL: [Self; 4] = [
        Self::Healthy,
        Self::Degraded,
        Self::CoolingDown,
        Self::Disabled,
    ];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::CoolingDown => "cooling_down",
            Self::Disabled => "disabled",
        }
    }
}

/// Why a server left the healthy state. `None` covers healthy and degraded,
/// where the state machine records no distinguishing cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ServerStateReason {
    None,
    Transport,
    Capacity,
    AuthFailure,
    ConsecutiveFailures,
    FailureRatio,
}

impl ServerStateReason {
    pub(super) const ALL: [Self; 6] = [
        Self::None,
        Self::Transport,
        Self::Capacity,
        Self::AuthFailure,
        Self::ConsecutiveFailures,
        Self::FailureRatio,
    ];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Transport => "transport",
            Self::Capacity => "capacity",
            Self::AuthFailure => "auth_failure",
            Self::ConsecutiveFailures => "consecutive_failures",
            Self::FailureRatio => "failure_ratio",
        }
    }
}

pub(super) struct ServerHealthInfo {
    /// `host:port`, kept as the `server` label for backwards compatibility.
    pub(super) label: String,
    /// Stable durable server id as a decimal string, matching the `server_id`
    /// label the transfer-policy metrics already used.
    pub(super) server_id: String,
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) tls: bool,
    pub(super) priority: u32,
    pub(super) backfill: bool,
    pub(super) state: ServerStateKind,
    pub(super) state_reason: ServerStateReason,
    /// Unix timestamp when the current cooldown/disable lifts; 0 when neither.
    pub(super) state_until_epoch_seconds: f64,
    pub(super) disable_count: u32,
    pub(super) success_count: u64,
    pub(super) failure_count: u64,
    pub(super) consecutive_failures: u32,
    pub(super) latency_ms: f64,
    pub(super) connections_available: usize,
    pub(super) connections_active: usize,
    pub(super) connections_max: usize,
    pub(super) connections_configured: usize,
    pub(super) capacity_penalty_until_epoch_ms: u64,
    pub(super) capacity_reductions: u64,
    pub(super) premature_deaths: usize,
}

/// Per-server facts gathered before the health lock is taken.
struct ServerPreamble {
    label: String,
    server_id: String,
    host: String,
    port: u16,
    tls: bool,
    priority: u32,
    backfill: bool,
    connections_available: usize,
    connections_active: usize,
    connections_max: usize,
    connections_configured: usize,
    capacity_penalty_until_epoch_ms: u64,
    capacity_reductions: u64,
}

/// Per-server facts read under the health lock. Every field is `Copy`: the
/// lock is on the NNTP hot path, so nothing inside it may allocate.
#[derive(Clone, Copy)]
struct ServerHealthReading {
    state: ServerStateKind,
    state_reason: ServerStateReason,
    state_until: Option<Instant>,
    disable_count: u32,
    success_count: u64,
    failure_count: u64,
    consecutive_failures: u32,
    latency_ms: f64,
    premature_deaths: usize,
}

async fn collect_server_health(pool: &NntpPool) -> Vec<ServerHealthInfo> {
    let configs = pool.server_configs();
    let groups = pool.server_groups();
    let backfill_flags = pool.server_backfill_flags();

    // Build labels and read load outside the health lock.
    let pre: Vec<ServerPreamble> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let server = weaver_nntp::ServerId(idx);
            let (avail, effective) = pool.server_load(idx);
            ServerPreamble {
                label: format!("{}:{}", cfg.host, cfg.port),
                server_id: pool
                    .stable_server_id(server)
                    .map(|id| id.0.to_string())
                    .unwrap_or_else(|| idx.to_string()),
                host: cfg.host.clone(),
                port: cfg.port,
                tls: cfg.tls || cfg.starttls,
                priority: groups.get(idx).copied().unwrap_or(0),
                backfill: backfill_flags.get(idx).copied().unwrap_or(false),
                connections_available: avail,
                connections_active: pool.active_connections(idx),
                connections_max: effective,
                connections_configured: pool.configured_connections(server).unwrap_or(effective),
                capacity_penalty_until_epoch_ms: pool
                    .capacity_penalty_until_epoch_ms(server)
                    .unwrap_or(0),
                capacity_reductions: pool.capacity_reductions(server).unwrap_or(0),
            }
        })
        .collect();

    // Hold the health lock only for field reads - no allocations inside. The
    // destination vector is sized up front so even the `push` below cannot
    // reach the allocator while the lock is held; this mutex is on the NNTP
    // connection path, and a scrape must never make a download wait on it.
    let mut readings: Vec<ServerHealthReading> = Vec::with_capacity(pre.len());
    {
        let health = pool.health().lock().await;
        for idx in 0..pre.len() {
            let srv = health.server(idx);
            let (state, state_reason, state_until) = match srv.state() {
                weaver_nntp::ServerState::Healthy => {
                    (ServerStateKind::Healthy, ServerStateReason::None, None)
                }
                weaver_nntp::ServerState::Degraded { .. } => {
                    (ServerStateKind::Degraded, ServerStateReason::None, None)
                }
                weaver_nntp::ServerState::CoolingDown { until, reason, .. } => (
                    ServerStateKind::CoolingDown,
                    match reason {
                        weaver_nntp::health::CooldownReason::Transport => {
                            ServerStateReason::Transport
                        }
                        weaver_nntp::health::CooldownReason::Capacity => {
                            ServerStateReason::Capacity
                        }
                    },
                    Some(*until),
                ),
                weaver_nntp::ServerState::Disabled { until, reason } => (
                    ServerStateKind::Disabled,
                    match reason {
                        weaver_nntp::health::DisableReason::AuthFailure => {
                            ServerStateReason::AuthFailure
                        }
                        weaver_nntp::health::DisableReason::ConsecutiveFailures => {
                            ServerStateReason::ConsecutiveFailures
                        }
                        weaver_nntp::health::DisableReason::FailureRatio => {
                            ServerStateReason::FailureRatio
                        }
                    },
                    Some(*until),
                ),
            };
            readings.push(ServerHealthReading {
                state,
                state_reason,
                state_until,
                disable_count: srv.disable_count(),
                success_count: srv.success_count,
                failure_count: srv.failure_count,
                consecutive_failures: srv.consecutive_failures,
                latency_ms: health.latency_ms(idx),
                premature_deaths: health.recent_premature_deaths(idx),
            });
        }
    }

    // Anchor the monotonic deadlines to the wall clock once, rather than
    // asking the OS for the time again for every server.
    let clock = EpochClock::sample();

    pre.into_iter()
        .zip(readings)
        .map(|(pre, reading)| ServerHealthInfo {
            label: pre.label,
            server_id: pre.server_id,
            host: pre.host,
            port: pre.port,
            tls: pre.tls,
            priority: pre.priority,
            backfill: pre.backfill,
            state: reading.state,
            state_reason: reading.state_reason,
            state_until_epoch_seconds: clock.epoch_seconds_at(reading.state_until),
            disable_count: reading.disable_count,
            success_count: reading.success_count,
            failure_count: reading.failure_count,
            consecutive_failures: reading.consecutive_failures,
            latency_ms: reading.latency_ms,
            connections_available: pre.connections_available,
            connections_active: pre.connections_active,
            connections_max: pre.connections_max,
            connections_configured: pre.connections_configured,
            capacity_penalty_until_epoch_ms: pre.capacity_penalty_until_epoch_ms,
            capacity_reductions: pre.capacity_reductions,
            premature_deaths: reading.premature_deaths,
        })
        .collect()
}

/// Which jobs earn their own `weaver_job_*` series under the configured mode.
pub(super) fn job_is_exported(status: &weaver_server_core::JobStatus, mode: PerJobSeries) -> bool {
    match mode {
        PerJobSeries::Off => false,
        PerJobSeries::All => true,
        // Finished jobs linger in the runtime list by the thousand; their
        // series would never change again but would be scraped forever.
        PerJobSeries::Active => !matches!(
            status,
            weaver_server_core::JobStatus::Complete | weaver_server_core::JobStatus::Failed { .. }
        ),
    }
}
