use std::sync::Arc;

use axum::extract::Extension;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;

use weaver_nntp::pool::NntpPool;
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::{
    DownloadPressureState, JobInfo, JobStatus, MetricsSnapshot, SchedulerHandle,
};

#[derive(Clone)]
pub(crate) struct PrometheusMetricsExporter {
    handle: SchedulerHandle,
    nntp_pool: Arc<NntpPool>,
}

impl PrometheusMetricsExporter {
    pub(crate) fn new(handle: SchedulerHandle, nntp_pool: Arc<NntpPool>) -> Self {
        Self { handle, nntp_pool }
    }

    pub(crate) async fn render(&self) -> String {
        let snapshot = self.handle.get_metrics();
        let jobs = self.handle.list_jobs();
        let download_block = self.handle.get_download_block();
        let server_health = collect_server_health(&self.nntp_pool).await;
        render_prometheus_metrics(
            &snapshot,
            &jobs,
            self.handle.is_globally_paused(),
            &download_block,
            &server_health,
        )
    }
}

pub(super) async fn metrics_handler(
    Extension(exporter): Extension<PrometheusMetricsExporter>,
    Extension(request_auth): Extension<super::RequestAuthContext>,
    Extension(security): Extension<RuntimeSecurityConfig>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if security.metrics_auth_required {
        let scope = super::auth::resolve_scope(
            &request_auth.db,
            &request_auth.auth_cache,
            &request_auth.api_key_cache,
            request_auth.session_token.0.as_str(),
            &headers,
        )
        .await?;
        if !scope.can_read() {
            return Err(StatusCode::FORBIDDEN);
        }
    }

    let body = exporter.render().await;
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8".to_string(),
        )],
        body,
    ))
}

pub(super) struct ServerHealthInfo {
    pub(super) label: String,
    pub(super) state: &'static str,
    pub(super) success_count: u64,
    pub(super) failure_count: u64,
    pub(super) consecutive_failures: u32,
    pub(super) latency_ms: f64,
    pub(super) connections_available: usize,
    pub(super) connections_max: usize,
    pub(super) premature_deaths: usize,
}

async fn collect_server_health(pool: &NntpPool) -> Vec<ServerHealthInfo> {
    let configs = pool.server_configs();
    // Build labels and read load outside the health lock.
    let pre: Vec<(String, usize, usize)> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let (avail, max) = pool.server_load(idx);
            (format!("{}:{}", cfg.host, cfg.port), avail, max)
        })
        .collect();

    // Hold the health lock only for field reads - no allocations inside.
    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(|(idx, (label, avail, max))| {
            let srv = health.server(idx);
            ServerHealthInfo {
                label,
                state: match srv.state() {
                    weaver_nntp::ServerState::Healthy => "healthy",
                    weaver_nntp::ServerState::Degraded { .. } => "degraded",
                    weaver_nntp::ServerState::CoolingDown { .. } => "cooling_down",
                    weaver_nntp::ServerState::Disabled { .. } => "disabled",
                },
                success_count: srv.success_count,
                failure_count: srv.failure_count,
                consecutive_failures: srv.consecutive_failures,
                latency_ms: health.latency_ms(idx),
                connections_available: avail,
                connections_max: max,
                premature_deaths: health.recent_premature_deaths(idx),
            }
        })
        .collect()
}

pub(super) fn render_prometheus_metrics(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
) -> String {
    let mut out = String::with_capacity(16 * 1024);
    out.push_str("# HELP weaver_build_info Static build information.\n");
    out.push_str("# TYPE weaver_build_info gauge\n");
    append_labeled_metric(
        &mut out,
        "weaver_build_info",
        &[("version", env!("CARGO_PKG_VERSION"))],
        1,
    );

    out.push_str("# HELP weaver_pipeline_paused Whether the entire pipeline is globally paused.\n");
    out.push_str("# TYPE weaver_pipeline_paused gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_paused",
        if pipeline_paused { 1 } else { 0 },
    );

    out.push_str("# HELP weaver_pipeline_download_gate Current global download gate by reason.\n");
    out.push_str("# TYPE weaver_pipeline_download_gate gauge\n");
    for reason in ["none", "manual_pause", "isp_cap"] {
        let active = match (reason, download_block.kind) {
            ("none", DownloadBlockKind::None) => 1,
            ("manual_pause", DownloadBlockKind::ManualPause) => 1,
            ("isp_cap", DownloadBlockKind::IspCap) => 1,
            _ => 0,
        };
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_gate",
            &[("reason", reason)],
            active,
        );
    }

    out.push_str(
        "# HELP weaver_bandwidth_cap_enabled Whether the ISP bandwidth cap policy is enabled.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_enabled gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_enabled",
        if download_block.cap_enabled { 1 } else { 0 },
    );

    out.push_str(
        "# HELP weaver_bandwidth_cap_used_bytes Current ISP bandwidth cap usage in bytes.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_used_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_used_bytes",
        download_block.used_bytes,
    );

    out.push_str(
        "# HELP weaver_bandwidth_cap_limit_bytes Configured ISP bandwidth cap limit in bytes.\n",
    );
    out.push_str("# TYPE weaver_bandwidth_cap_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_limit_bytes",
        download_block.limit_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_remaining_bytes Remaining ISP bandwidth cap bytes in the active window.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_remaining_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_remaining_bytes",
        download_block.remaining_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_reserved_bytes Bytes conservatively reserved for in-flight downloads against the active cap window.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_reserved_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_reserved_bytes",
        download_block.reserved_bytes,
    );

    out.push_str("# HELP weaver_bandwidth_cap_window_end_seconds Active ISP bandwidth cap window end as a unix timestamp.\n");
    out.push_str("# TYPE weaver_bandwidth_cap_window_end_seconds gauge\n");
    append_metric(
        &mut out,
        "weaver_bandwidth_cap_window_end_seconds",
        download_block
            .window_ends_at_epoch_ms
            .map(|value| (value / 1000.0) as u64)
            .unwrap_or(0),
    );

    out.push_str("# HELP weaver_pipeline_jobs Number of active jobs by status.\n");
    out.push_str("# TYPE weaver_pipeline_jobs gauge\n");
    for status in all_job_statuses() {
        let count = jobs
            .iter()
            .filter(|job| job_status_label(&job.status) == status)
            .count();
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_jobs",
            &[("status", status)],
            count,
        );
    }

    out.push_str(
        "# HELP weaver_pipeline_bytes_downloaded_total Total bytes downloaded by the pipeline.\n",
    );
    out.push_str("# TYPE weaver_pipeline_bytes_downloaded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_downloaded_total",
        snapshot.bytes_downloaded,
    );

    out.push_str(
        "# HELP weaver_pipeline_bytes_decoded_total Total bytes decoded by the pipeline.\n",
    );
    out.push_str("# TYPE weaver_pipeline_bytes_decoded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_decoded_total",
        snapshot.bytes_decoded,
    );

    out.push_str("# HELP weaver_pipeline_bytes_committed_total Total bytes committed to disk by the pipeline.\n");
    out.push_str("# TYPE weaver_pipeline_bytes_committed_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_bytes_committed_total",
        snapshot.bytes_committed,
    );

    out.push_str("# HELP weaver_pipeline_segments_downloaded_total Total segments downloaded.\n");
    out.push_str("# TYPE weaver_pipeline_segments_downloaded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_downloaded_total",
        snapshot.segments_downloaded,
    );

    out.push_str("# HELP weaver_pipeline_segments_decoded_total Total segments decoded.\n");
    out.push_str("# TYPE weaver_pipeline_segments_decoded_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_decoded_total",
        snapshot.segments_decoded,
    );

    out.push_str("# HELP weaver_pipeline_segments_committed_total Total segments committed.\n");
    out.push_str("# TYPE weaver_pipeline_segments_committed_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_committed_total",
        snapshot.segments_committed,
    );

    out.push_str("# HELP weaver_pipeline_segments_retried_total Total segments retried.\n");
    out.push_str("# TYPE weaver_pipeline_segments_retried_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_retried_total",
        snapshot.segments_retried,
    );

    out.push_str("# HELP weaver_pipeline_segments_failed_permanent_total Total segments permanently failed.\n");
    out.push_str("# TYPE weaver_pipeline_segments_failed_permanent_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_segments_failed_permanent_total",
        snapshot.segments_failed_permanent,
    );

    out.push_str(
        "# HELP weaver_pipeline_download_failures_total Failed article download attempts by kind.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_failures_total counter\n");
    for (kind, value) in [
        (
            "article_not_found",
            snapshot.download_failures_article_not_found,
        ),
        (
            "capacity_unavailable",
            snapshot.download_failures_capacity_unavailable,
        ),
        ("transient", snapshot.download_failures_transient),
        ("auth", snapshot.download_failures_auth),
        ("permanent", snapshot.download_failures_permanent),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_failures_total",
            &[("kind", kind)],
            value,
        );
    }

    out.push_str("# HELP weaver_pipeline_articles_not_found_total Total articles not found.\n");
    out.push_str("# TYPE weaver_pipeline_articles_not_found_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_articles_not_found_total",
        snapshot.articles_not_found,
    );

    out.push_str("# HELP weaver_pipeline_decode_errors_total Total decode errors.\n");
    out.push_str("# TYPE weaver_pipeline_decode_errors_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_errors_total",
        snapshot.decode_errors,
    );

    out.push_str("# HELP weaver_pipeline_crc_errors_total Total CRC errors.\n");
    out.push_str("# TYPE weaver_pipeline_crc_errors_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_crc_errors_total",
        snapshot.crc_errors,
    );

    out.push_str("# HELP weaver_pipeline_download_queue_depth Download queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_download_queue_depth gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_queue_depth",
        snapshot.download_queue_depth,
    );

    out.push_str("# HELP weaver_pipeline_active_downloads Active article downloads.\n");
    out.push_str("# TYPE weaver_pipeline_active_downloads gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_active_downloads",
        snapshot.active_downloads,
    );

    out.push_str("# HELP weaver_pipeline_active_decodes Active decode tasks.\n");
    out.push_str("# TYPE weaver_pipeline_active_decodes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_active_decodes",
        snapshot.active_decodes,
    );

    out.push_str("# HELP weaver_pipeline_decode_pending Decode pending queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_decode_pending gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_pending",
        snapshot.decode_pending,
    );

    out.push_str(
        "# HELP weaver_pipeline_decode_pending_bytes Raw article bytes queued for decode.\n",
    );
    out.push_str("# TYPE weaver_pipeline_decode_pending_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_pending_bytes",
        snapshot.decode_pending_bytes,
    );

    out.push_str(
        "# HELP weaver_pipeline_decode_active_bytes Raw article bytes currently being decoded.\n",
    );
    out.push_str("# TYPE weaver_pipeline_decode_active_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_active_bytes",
        snapshot.decode_active_bytes,
    );

    out.push_str("# HELP weaver_pipeline_commit_pending Commit pending queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_commit_pending gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_commit_pending",
        snapshot.commit_pending,
    );

    out.push_str("# HELP weaver_pipeline_recovery_queue_depth Recovery queue depth.\n");
    out.push_str("# TYPE weaver_pipeline_recovery_queue_depth gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_recovery_queue_depth",
        snapshot.recovery_queue_depth,
    );

    out.push_str("# HELP weaver_pipeline_write_buffered_bytes Buffered write bytes.\n");
    out.push_str("# TYPE weaver_pipeline_write_buffered_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_buffered_bytes",
        snapshot.write_buffered_bytes,
    );

    out.push_str("# HELP weaver_pipeline_write_buffered_segments Buffered write segments.\n");
    out.push_str("# TYPE weaver_pipeline_write_buffered_segments gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_buffered_segments",
        snapshot.write_buffered_segments,
    );

    out.push_str("# HELP weaver_pipeline_decode_pressure_soft_limit_bytes Decode soft pressure limit in bytes.\n");
    out.push_str("# TYPE weaver_pipeline_decode_pressure_soft_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_pressure_soft_limit_bytes",
        snapshot.decode_pressure_soft_limit_bytes,
    );

    out.push_str("# HELP weaver_pipeline_decode_pressure_hard_limit_bytes Decode hard pressure limit in bytes.\n");
    out.push_str("# TYPE weaver_pipeline_decode_pressure_hard_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_decode_pressure_hard_limit_bytes",
        snapshot.decode_pressure_hard_limit_bytes,
    );

    out.push_str("# HELP weaver_pipeline_write_pressure_soft_limit_bytes Write soft pressure limit in bytes.\n");
    out.push_str("# TYPE weaver_pipeline_write_pressure_soft_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_pressure_soft_limit_bytes",
        snapshot.write_pressure_soft_limit_bytes,
    );

    out.push_str("# HELP weaver_pipeline_write_pressure_hard_limit_bytes Write hard pressure limit in bytes.\n");
    out.push_str("# TYPE weaver_pipeline_write_pressure_hard_limit_bytes gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_write_pressure_hard_limit_bytes",
        snapshot.write_pressure_hard_limit_bytes,
    );

    out.push_str("# HELP weaver_pipeline_download_pressure_state Download backpressure state.\n");
    out.push_str("# TYPE weaver_pipeline_download_pressure_state gauge\n");
    for state in ["clear", "soft", "hard"] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_pressure_state",
            &[("state", state)],
            u8::from(snapshot.download_pressure_state.as_str() == state),
        );
    }

    out.push_str("# HELP weaver_pipeline_download_pressure_reason Download backpressure reason.\n");
    out.push_str("# TYPE weaver_pipeline_download_pressure_reason gauge\n");
    for reason in ["none", "decode", "write", "decode_and_write"] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_pressure_reason",
            &[("reason", reason)],
            u8::from(snapshot.download_pressure_reason.as_str() == reason),
        );
    }

    out.push_str("# HELP weaver_pipeline_hot_dispatch_job_id Current hot-dispatch job id, or 0 when no job owns hot dispatch.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_job_id gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_job_id",
        snapshot.hot_dispatch_job_id,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_mode Current hot-dispatch sharing mode.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_mode gauge\n");
    for mode in ["exclusive", "shared"] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_hot_dispatch_mode",
            &[("mode", mode)],
            u8::from(snapshot.hot_dispatch_mode.as_str() == mode),
        );
    }

    out.push_str("# HELP weaver_pipeline_hot_dispatch_underfill_milliseconds Current hot-job unused-capacity underfill window age.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_underfill_milliseconds gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_underfill_milliseconds",
        snapshot.hot_dispatch_underfill_ms,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_lent_connections Active NNTP connection tasks lent to spillover jobs.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_lent_connections gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_lent_connections",
        snapshot.hot_dispatch_lent_connections,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_warmup_complete Whether the current hot-dispatch warmup gate is complete.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_warmup_complete gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_warmup_complete",
        u8::from(snapshot.hot_dispatch_warmup_complete),
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_last_spillover_decision Last hot-dispatch spillover decision.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_last_spillover_decision gauge\n");
    for decision in [
        "none",
        "blocked_warmup",
        "blocked_pressure",
        "blocked_near_cap",
        "blocked_hot_can_use_capacity",
        "allowed_underfill",
        "reclaimed",
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_hot_dispatch_last_spillover_decision",
            &[("decision", decision)],
            u8::from(snapshot.hot_dispatch_last_spillover_decision.as_str() == decision),
        );
    }

    out.push_str("# HELP weaver_pipeline_hot_dispatch_spillover_decisions_total Hot-dispatch spillover decisions by reason.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_spillover_decisions_total counter\n");
    for (decision, value) in [
        (
            "blocked_warmup",
            snapshot.hot_dispatch_spillover_blocked_warmup_total,
        ),
        (
            "blocked_pressure",
            snapshot.hot_dispatch_spillover_blocked_pressure_total,
        ),
        (
            "blocked_near_cap",
            snapshot.hot_dispatch_spillover_blocked_near_cap_total,
        ),
        (
            "blocked_hot_can_use_capacity",
            snapshot.hot_dispatch_spillover_blocked_hot_can_use_capacity_total,
        ),
        (
            "allowed_underfill",
            snapshot.hot_dispatch_spillover_allowed_underfill_total,
        ),
        ("reclaimed", snapshot.hot_dispatch_spillover_reclaimed_total),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_hot_dispatch_spillover_decisions_total",
            &[("decision", decision)],
            value,
        );
    }

    out.push_str(
        "# HELP weaver_pipeline_download_lanes_active Active article download lanes by mode.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lanes_active gauge\n");
    for (mode, value) in [
        ("sequential", snapshot.download_lanes_sequential_active),
        ("pipeline_depth2", snapshot.download_lanes_depth2_active),
        ("pipeline_depth4", snapshot.download_lanes_depth4_active),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_lanes_active",
            &[("mode", mode)],
            value,
        );
    }
    out.push_str(
        "# HELP weaver_pipeline_download_lanes_active_total Total active article download lanes.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lanes_active_total gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_lanes_active_total",
        snapshot.download_lanes_active,
    );

    out.push_str(
        "# HELP weaver_pipeline_download_lane_parks_total Article download lane parks by reason.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lane_parks_total counter\n");
    for (reason, value) in [
        ("no_work", snapshot.download_lane_parks_no_work_total),
        ("error", snapshot.download_lane_parks_error_total),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_lane_parks_total",
            &[("reason", reason)],
            value,
        );
    }

    out.push_str("# HELP weaver_pipeline_body_proof_events_total BODY pipelining proof events.\n");
    out.push_str("# TYPE weaver_pipeline_body_proof_events_total counter\n");
    for (event, value) in [
        (
            "trial_success",
            snapshot.download_pipeline_trial_success_total,
        ),
        (
            "trial_failure",
            snapshot.download_pipeline_trial_failure_total,
        ),
        ("proof_pass", snapshot.download_pipeline_proof_pass_total),
        ("cooldown", snapshot.download_pipeline_cooldown_total),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_body_proof_events_total",
            &[("event", event)],
            value,
        );
    }

    out.push_str("# HELP weaver_pipeline_body_replay_items_total BODY items returned unresolved after a lane reset/failure.\n");
    out.push_str("# TYPE weaver_pipeline_body_replay_items_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_body_replay_items_total",
        snapshot.download_pipeline_replay_items_total,
    );

    out.push_str("# HELP weaver_pipeline_download_observed_limiter Observed downloader limiter derived from pressure, queue, and server permits.\n");
    out.push_str("# TYPE weaver_pipeline_download_observed_limiter gauge\n");
    let observed_limiter =
        observed_download_limiter(snapshot, pipeline_paused, download_block, server_health);
    for limiter in [
        "active",
        "decode_lagging",
        "dispatch_limited",
        "gated",
        "idle",
        "network_limited",
        "pressure_limited",
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_observed_limiter",
            &[("limiter", limiter)],
            u8::from(observed_limiter == limiter),
        );
    }

    out.push_str(
        "# HELP weaver_pipeline_download_pressure_stalls_total Hard pressure stalls started.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_pressure_stalls_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_pressure_stalls_total",
        snapshot.download_pressure_stalls_total,
    );

    out.push_str("# HELP weaver_pipeline_download_pressure_stall_duration_seconds Completed hard pressure stall duration.\n");
    out.push_str("# TYPE weaver_pipeline_download_pressure_stall_duration_seconds counter\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_download_pressure_stall_duration_seconds",
        snapshot.download_pressure_stall_duration_ms as f64 / 1000.0,
    );

    out.push_str("# HELP weaver_pipeline_download_pressure_current_stall_seconds Current hard pressure stall duration.\n");
    out.push_str("# TYPE weaver_pipeline_download_pressure_current_stall_seconds gauge\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_download_pressure_current_stall_seconds",
        snapshot.download_pressure_current_stall_ms as f64 / 1000.0,
    );

    out.push_str("# HELP weaver_pipeline_direct_write_evictions_total Direct write evictions.\n");
    out.push_str("# TYPE weaver_pipeline_direct_write_evictions_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_direct_write_evictions_total",
        snapshot.direct_write_evictions,
    );

    out.push_str("# HELP weaver_pipeline_verify_active Active verification workers.\n");
    out.push_str("# TYPE weaver_pipeline_verify_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_verify_active",
        snapshot.verify_active,
    );

    out.push_str("# HELP weaver_pipeline_repair_active Active repair workers.\n");
    out.push_str("# TYPE weaver_pipeline_repair_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_repair_active",
        snapshot.repair_active,
    );

    out.push_str("# HELP weaver_pipeline_extract_active Active extraction workers.\n");
    out.push_str("# TYPE weaver_pipeline_extract_active gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_extract_active",
        snapshot.extract_active,
    );

    out.push_str("# HELP weaver_pipeline_disk_write_latency_microseconds Disk write latency in microseconds.\n");
    out.push_str("# TYPE weaver_pipeline_disk_write_latency_microseconds gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_disk_write_latency_microseconds",
        snapshot.disk_write_latency_us,
    );

    out.push_str("# HELP weaver_pipeline_current_download_speed_bytes_per_second Current download speed in bytes per second.\n");
    out.push_str("# TYPE weaver_pipeline_current_download_speed_bytes_per_second gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_current_download_speed_bytes_per_second",
        snapshot.current_download_speed,
    );

    out.push_str(
        "# HELP weaver_pipeline_articles_per_second Current effective article throughput.\n",
    );
    out.push_str("# TYPE weaver_pipeline_articles_per_second gauge\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_articles_per_second",
        snapshot.articles_per_sec,
    );

    out.push_str("# HELP weaver_pipeline_decode_rate_mebibytes_per_second Current decode throughput in MiB per second.\n");
    out.push_str("# TYPE weaver_pipeline_decode_rate_mebibytes_per_second gauge\n");
    append_metric_f64(
        &mut out,
        "weaver_pipeline_decode_rate_mebibytes_per_second",
        snapshot.decode_rate_mbps,
    );

    out.push_str("# HELP weaver_job_info Static information for active jobs.\n");
    out.push_str("# TYPE weaver_job_info gauge\n");
    out.push_str("# HELP weaver_job_progress_ratio Fractional job progress from 0 to 1.\n");
    out.push_str("# TYPE weaver_job_progress_ratio gauge\n");
    out.push_str("# HELP weaver_job_total_bytes Expected total bytes for the job.\n");
    out.push_str("# TYPE weaver_job_total_bytes gauge\n");
    out.push_str("# HELP weaver_job_downloaded_bytes Downloaded bytes for the job.\n");
    out.push_str("# TYPE weaver_job_downloaded_bytes gauge\n");
    out.push_str("# HELP weaver_job_optional_recovery_bytes Optional recovery bytes available for the job.\n");
    out.push_str("# TYPE weaver_job_optional_recovery_bytes gauge\n");
    out.push_str("# HELP weaver_job_optional_recovery_downloaded_bytes Optional recovery bytes downloaded for the job.\n");
    out.push_str("# TYPE weaver_job_optional_recovery_downloaded_bytes gauge\n");
    out.push_str("# HELP weaver_job_failed_bytes Permanently failed bytes for the job.\n");
    out.push_str("# TYPE weaver_job_failed_bytes gauge\n");
    out.push_str("# HELP weaver_job_health_per_mille Job health in per-mille.\n");
    out.push_str("# TYPE weaver_job_health_per_mille gauge\n");
    out.push_str("# HELP weaver_job_created_at_seconds Unix creation timestamp for the job.\n");
    out.push_str("# TYPE weaver_job_created_at_seconds gauge\n");

    for job in jobs {
        append_job_metric(&mut out, "weaver_job_info", job, 1);
        append_job_metric_f64(&mut out, "weaver_job_progress_ratio", job, job.progress);
        append_job_metric(&mut out, "weaver_job_total_bytes", job, job.total_bytes);
        append_job_metric(
            &mut out,
            "weaver_job_downloaded_bytes",
            job,
            job.downloaded_bytes,
        );
        append_job_metric(
            &mut out,
            "weaver_job_optional_recovery_bytes",
            job,
            job.optional_recovery_bytes,
        );
        append_job_metric(
            &mut out,
            "weaver_job_optional_recovery_downloaded_bytes",
            job,
            job.optional_recovery_downloaded_bytes,
        );
        append_job_metric(&mut out, "weaver_job_failed_bytes", job, job.failed_bytes);
        append_job_metric(&mut out, "weaver_job_health_per_mille", job, job.health);
        append_job_metric_f64(
            &mut out,
            "weaver_job_created_at_seconds",
            job,
            job.created_at_epoch_ms / 1000.0,
        );
    }

    // Per-server NNTP health metrics.
    if !server_health.is_empty() {
        out.push_str("# HELP weaver_server_state Server health state (1=healthy, 0=disabled).\n");
        out.push_str("# TYPE weaver_server_state gauge\n");
        out.push_str(
            "# HELP weaver_server_success_total Total successful operations per server.\n",
        );
        out.push_str("# TYPE weaver_server_success_total counter\n");
        out.push_str("# HELP weaver_server_failure_total Total failed operations per server.\n");
        out.push_str("# TYPE weaver_server_failure_total counter\n");
        out.push_str("# HELP weaver_server_consecutive_failures Current run of consecutive failures per server.\n");
        out.push_str("# TYPE weaver_server_consecutive_failures gauge\n");
        out.push_str("# HELP weaver_server_latency_ms EWMA latency in milliseconds per server.\n");
        out.push_str("# TYPE weaver_server_latency_ms gauge\n");
        out.push_str(
            "# HELP weaver_server_connections_available Available connection permits per server.\n",
        );
        out.push_str("# TYPE weaver_server_connections_available gauge\n");
        out.push_str("# HELP weaver_server_connections_max Maximum connections per server.\n");
        out.push_str("# TYPE weaver_server_connections_max gauge\n");
        out.push_str(
            "# HELP weaver_server_premature_deaths Recent connections that died before 60s age.\n",
        );
        out.push_str("# TYPE weaver_server_premature_deaths gauge\n");

        for srv in server_health {
            let labels: &[(&str, &str)] = &[("server", &srv.label)];
            append_labeled_metric(
                &mut out,
                "weaver_server_state",
                labels,
                if srv.state == "healthy" { 1 } else { 0 },
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_success_total",
                labels,
                srv.success_count,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_failure_total",
                labels,
                srv.failure_count,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_consecutive_failures",
                labels,
                srv.consecutive_failures,
            );
            append_labeled_metric_f64(&mut out, "weaver_server_latency_ms", labels, srv.latency_ms);
            append_labeled_metric(
                &mut out,
                "weaver_server_connections_available",
                labels,
                srv.connections_available,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_connections_max",
                labels,
                srv.connections_max,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_premature_deaths",
                labels,
                srv.premature_deaths,
            );
        }
    }

    out
}

fn observed_download_limiter(
    snapshot: &MetricsSnapshot,
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
) -> &'static str {
    if pipeline_paused || !matches!(download_block.kind, DownloadBlockKind::None) {
        return "gated";
    }
    let required_queue_depth = snapshot
        .download_queue_depth
        .saturating_sub(snapshot.recovery_queue_depth);
    if required_queue_depth == 0 && snapshot.active_downloads == 0 {
        return "idle";
    }
    if snapshot.download_pressure_state != DownloadPressureState::Clear {
        return "pressure_limited";
    }
    let decode_backlog_bytes = snapshot
        .decode_pending_bytes
        .saturating_add(snapshot.decode_active_bytes);
    let decode_lagging_backlog =
        decode_backlog_bytes >= (snapshot.decode_pressure_soft_limit_bytes / 2).max(1);
    if decode_lagging_backlog {
        return "decode_lagging";
    }
    if snapshot.decode_pending_bytes > 0 && snapshot.current_download_speed > 0 {
        let decode_bytes_per_second = snapshot.decode_rate_mbps.max(0.0) * 1024.0 * 1024.0;
        if (snapshot.current_download_speed as f64) > decode_bytes_per_second * 1.2 {
            return "decode_lagging";
        }
    }

    let total_connections = server_health
        .iter()
        .map(|server| server.connections_max)
        .sum::<usize>();
    let available_connections = server_health
        .iter()
        .map(|server| server.connections_available)
        .sum::<usize>();
    if required_queue_depth > 0
        && snapshot.active_downloads > 0
        && total_connections > 0
        && available_connections == 0
    {
        return "network_limited";
    }
    if snapshot.active_downloads > 0 {
        return "active";
    }
    "dispatch_limited"
}

fn append_metric<T: std::fmt::Display>(out: &mut String, name: &str, value: T) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_metric_f64(out: &mut String, name: &str, value: f64) {
    out.push_str(name);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
    out.push('\n');
}

fn append_labeled_metric<T: std::fmt::Display>(
    out: &mut String,
    name: &str,
    labels: &[(&str, &str)],
    value: T,
) {
    out.push_str(name);
    append_labels(out, labels);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_labeled_metric_f64(out: &mut String, name: &str, labels: &[(&str, &str)], value: f64) {
    out.push_str(name);
    append_labels(out, labels);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
    out.push('\n');
}

fn append_job_metric<T: std::fmt::Display>(out: &mut String, name: &str, job: &JobInfo, value: T) {
    out.push_str(name);
    append_job_labels(out, job);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_job_metric_f64(out: &mut String, name: &str, job: &JobInfo, value: f64) {
    out.push_str(name);
    append_job_labels(out, job);
    out.push(' ');
    out.push_str(&format_prometheus_f64(value));
    out.push('\n');
}

fn append_labels(out: &mut String, labels: &[(&str, &str)]) {
    if labels.is_empty() {
        return;
    }
    out.push('{');
    for (idx, (key, value)) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&escape_prometheus_label_value(value));
        out.push('"');
    }
    out.push('}');
}

fn append_job_labels(out: &mut String, job: &JobInfo) {
    append_labels(
        out,
        &[
            ("job_id", &job.job_id.0.to_string()),
            ("job_name", &job.name),
            ("status", job_status_label(&job.status)),
            ("category", job.category.as_deref().unwrap_or("")),
            (
                "has_password",
                if job.password.is_some() {
                    "true"
                } else {
                    "false"
                },
            ),
        ],
    );
}

pub(super) fn escape_prometheus_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn format_prometheus_f64(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else if value.is_nan() {
        "NaN".to_string()
    } else if value.is_sign_negative() {
        "-Inf".to_string()
    } else {
        "+Inf".to_string()
    }
}

fn all_job_statuses() -> [&'static str; 12] {
    [
        "queued",
        "downloading",
        "checking",
        "verifying",
        "queued_repair",
        "repairing",
        "queued_extract",
        "extracting",
        "moving",
        "complete",
        "failed",
        "paused",
    ]
}

fn job_status_label(status: &JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Downloading => "downloading",
        JobStatus::Checking => "checking",
        JobStatus::Verifying => "verifying",
        JobStatus::QueuedRepair => "queued_repair",
        JobStatus::Repairing => "repairing",
        JobStatus::QueuedExtract => "queued_extract",
        JobStatus::Extracting => "extracting",
        JobStatus::Moving => "moving",
        JobStatus::Complete => "complete",
        JobStatus::Failed { .. } => "failed",
        JobStatus::Paused => "paused",
    }
}
