use std::sync::Arc;

use axum::extract::Extension;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;

use weaver_nntp::pool::NntpPool;
use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::{
    Database, DownloadPressureState, JobInfo, JobStatus, MetricsSnapshot, SchedulerHandle,
};

#[derive(Clone)]
pub(crate) struct PrometheusMetricsExporter {
    handle: SchedulerHandle,
    db: Database,
    nntp_pool: Arc<NntpPool>,
    transfer_policy:
        Arc<weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry>,
}

impl PrometheusMetricsExporter {
    pub(crate) fn new(
        handle: SchedulerHandle,
        db: Database,
        nntp_pool: Arc<NntpPool>,
        transfer_policy: Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >,
    ) -> Self {
        Self {
            handle,
            db,
            nntp_pool,
            transfer_policy,
        }
    }

    pub(crate) async fn render(&self) -> String {
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
        let mut output = render_prometheus_metrics_with_transfers(
            &snapshot,
            &jobs,
            self.handle.is_globally_paused(),
            &download_block,
            &server_health,
            runtime_generation,
            &server_transfers,
        );
        let db = self.db.clone();
        match tokio::task::spawn_blocking(move || db.post_processing_metrics_snapshot()).await {
            Ok(Ok(metrics)) => append_post_processing_metrics(&mut output, &metrics),
            Ok(Err(error)) => {
                tracing::debug!(error = %error, "failed to collect post-processing metrics")
            }
            Err(error) => {
                tracing::debug!(error = %error, "post-processing metrics task failed")
            }
        }
        output
    }
}

fn append_post_processing_metrics(
    output: &mut String,
    metrics: &weaver_server_core::post_processing::persistence::PostProcessingMetricsSnapshot,
) {
    append_metric(
        output,
        "weaver_post_processing_queue_depth",
        metrics.queue_depth,
    );
    append_metric(
        output,
        "weaver_post_processing_active_attempts",
        metrics.active_attempts,
    );
    append_metric(
        output,
        "weaver_post_processing_attempt_duration_seconds_count",
        metrics.duration_count,
    );
    append_metric_f64(
        output,
        "weaver_post_processing_attempt_duration_seconds_sum",
        metrics.duration_sum_millis as f64 / 1_000.0,
    );
    for (result, count) in [
        ("succeeded", metrics.succeeded),
        ("failed", metrics.failed),
        ("skipped", metrics.skipped),
        ("timed_out", metrics.timed_out),
        ("cancelled", metrics.cancelled),
        ("interrupted", metrics.interrupted),
    ] {
        append_labeled_metric(
            output,
            "weaver_post_processing_attempt_results",
            &[("result", result)],
            count,
        );
    }
    append_metric(
        output,
        "weaver_post_processing_output_truncations",
        metrics.truncated,
    );
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
    pub(super) connections_configured: usize,
    pub(super) capacity_penalty_until_epoch_ms: u64,
    pub(super) capacity_reductions: u64,
    pub(super) premature_deaths: usize,
}

async fn collect_server_health(pool: &NntpPool) -> Vec<ServerHealthInfo> {
    let configs = pool.server_configs();
    // Build labels and read load outside the health lock.
    let pre: Vec<(String, usize, usize, usize, u64, u64)> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let server = weaver_nntp::ServerId(idx);
            let (avail, effective) = pool.server_load(idx);
            let configured = pool.configured_connections(server).unwrap_or(effective);
            let penalty_until = pool.capacity_penalty_until_epoch_ms(server).unwrap_or(0);
            let reductions = pool.capacity_reductions(server).unwrap_or(0);
            (
                format!("{}:{}", cfg.host, cfg.port),
                avail,
                effective,
                configured,
                penalty_until,
                reductions,
            )
        })
        .collect();

    // Hold the health lock only for field reads - no allocations inside.
    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(
            |(idx, (label, avail, effective, configured, penalty_until, reductions))| {
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
                    connections_max: effective,
                    connections_configured: configured,
                    capacity_penalty_until_epoch_ms: penalty_until,
                    capacity_reductions: reductions,
                    premature_deaths: health.recent_premature_deaths(idx),
                }
            },
        )
        .collect()
}

#[cfg(test)]
pub(super) fn render_prometheus_metrics(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
    runtime_generation: u64,
) -> String {
    render_prometheus_metrics_with_transfers(
        snapshot,
        jobs,
        pipeline_paused,
        download_block,
        server_health,
        runtime_generation,
        &[],
    )
}

fn render_prometheus_metrics_with_transfers(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
    runtime_generation: u64,
    server_transfers: &[weaver_nntp::transfer::ServerTransferSnapshot],
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
    for reason in ["none", "manual_pause", "isp_cap", "server_quota"] {
        let active = match (reason, download_block.kind) {
            ("none", DownloadBlockKind::None) => 1,
            ("manual_pause", DownloadBlockKind::ManualPause) => 1,
            ("isp_cap", DownloadBlockKind::IspCap) => 1,
            ("server_quota", DownloadBlockKind::ServerQuota) => 1,
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

    out.push_str("# HELP weaver_duplicate_admission_decisions_total Duplicate admission decisions by intake origin and public status.\n");
    out.push_str("# TYPE weaver_duplicate_admission_decisions_total counter\n");
    for metric in weaver_server_core::jobs::duplicate_admission_metrics_snapshot() {
        append_labeled_metric(
            &mut out,
            "weaver_duplicate_admission_decisions_total",
            &[("origin", metric.origin), ("status", metric.status)],
            metric.count,
        );
    }

    out.push_str("# HELP weaver_semantic_duplicate_lifecycle_total Semantic duplicate arbitration lifecycle events.\n");
    out.push_str("# TYPE weaver_semantic_duplicate_lifecycle_total counter\n");
    for metric in weaver_server_core::jobs::semantic_duplicate_lifecycle_metrics_snapshot() {
        append_labeled_metric(
            &mut out,
            "weaver_semantic_duplicate_lifecycle_total",
            &[("event", metric.event)],
            metric.count,
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

    out.push_str("# HELP weaver_pipeline_parked_infrastructure_work Segments parked while NNTP infrastructure is unavailable.\n");
    out.push_str("# TYPE weaver_pipeline_parked_infrastructure_work gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_parked_infrastructure_work",
        snapshot.parked_infrastructure_work,
    );

    out.push_str("# HELP weaver_nntp_generation_recovery_requeues_total Segments requeued after stale NNTP generation failures.\n");
    out.push_str("# TYPE weaver_nntp_generation_recovery_requeues_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_generation_recovery_requeues_total",
        snapshot.nntp_generation_recovery_requeues,
    );

    out.push_str("# HELP weaver_nntp_capacity_probe_attempts_total Adaptive-capacity provider connection probes attempted.\n");
    out.push_str("# TYPE weaver_nntp_capacity_probe_attempts_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_capacity_probe_attempts_total",
        snapshot.nntp_capacity_probe_attempts_total,
    );
    out.push_str("# HELP weaver_nntp_capacity_probe_successes_total Adaptive-capacity probes that restored one connection.\n");
    out.push_str("# TYPE weaver_nntp_capacity_probe_successes_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_capacity_probe_successes_total",
        snapshot.nntp_capacity_probe_successes_total,
    );
    out.push_str("# HELP weaver_nntp_capacity_probe_rejections_total Adaptive-capacity probes rejected by provider limits.\n");
    out.push_str("# TYPE weaver_nntp_capacity_probe_rejections_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_capacity_probe_rejections_total",
        snapshot.nntp_capacity_probe_rejections_total,
    );
    out.push_str("# HELP weaver_nntp_capacity_probe_transport_failures_total Adaptive-capacity probes that failed during transport setup.\n");
    out.push_str("# TYPE weaver_nntp_capacity_probe_transport_failures_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_capacity_probe_transport_failures_total",
        snapshot.nntp_capacity_probe_transport_failures_total,
    );
    out.push_str("# HELP weaver_nntp_capacity_probe_stale_generation_total Probe results ignored after an NNTP generation replacement.\n");
    out.push_str("# TYPE weaver_nntp_capacity_probe_stale_generation_total counter\n");
    append_metric(
        &mut out,
        "weaver_nntp_capacity_probe_stale_generation_total",
        snapshot.nntp_capacity_probe_stale_generation_total,
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
        "blocked_best_mode_pending",
        "blocked_recent_expansion_helped",
        "blocked_cap_speed",
        "allowed_measured_underfill",
        "reclaimed_speed_harm",
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
            "blocked_best_mode_pending",
            snapshot.hot_dispatch_spillover_blocked_best_mode_pending_total,
        ),
        (
            "blocked_recent_expansion_helped",
            snapshot.hot_dispatch_spillover_blocked_recent_expansion_helped_total,
        ),
        (
            "blocked_cap_speed",
            snapshot.hot_dispatch_spillover_blocked_cap_speed_total,
        ),
        (
            "allowed_underfill",
            snapshot.hot_dispatch_spillover_allowed_underfill_total,
        ),
        ("reclaimed", snapshot.hot_dispatch_spillover_reclaimed_total),
        (
            "allowed_measured_underfill",
            snapshot.hot_dispatch_spillover_allowed_measured_underfill_total,
        ),
        (
            "reclaimed_speed_harm",
            snapshot.hot_dispatch_spillover_reclaimed_speed_harm_total,
        ),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_hot_dispatch_spillover_decisions_total",
            &[("decision", decision)],
            value,
        );
    }

    out.push_str("# HELP weaver_pipeline_hot_dispatch_speed_bytes_per_second Two-second hot-job BODY throughput.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_speed_bytes_per_second gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_speed_bytes_per_second",
        snapshot.hot_dispatch_hot_speed_bps,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_last_expansion_kind Last hot-job expansion event kind code.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_last_expansion_kind gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_last_expansion_kind",
        snapshot.hot_dispatch_last_expansion_kind,
    );
    out.push_str("# HELP weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second Last hot-job expansion before/after speeds.\n");
    out.push_str(
        "# TYPE weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second gauge\n",
    );
    append_labeled_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second",
        &[("phase", "before")],
        snapshot.hot_dispatch_last_expansion_before_bps,
    );
    append_labeled_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_last_expansion_speed_bytes_per_second",
        &[("phase", "after")],
        snapshot.hot_dispatch_last_expansion_after_bps,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_exclusive_peak_bytes_per_second Peak hot-job speed observed while exclusive.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_exclusive_peak_bytes_per_second gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_exclusive_peak_bytes_per_second",
        snapshot.hot_dispatch_exclusive_peak_bps,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_spillover_speed_bytes_per_second Hot-job speed before and after current spillover loan.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_spillover_speed_bytes_per_second gauge\n");
    for (phase, value) in [
        ("pre_lend", snapshot.hot_dispatch_spillover_pre_speed_bps),
        ("post_lend", snapshot.hot_dispatch_spillover_post_speed_bps),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_hot_dispatch_spillover_speed_bytes_per_second",
            &[("phase", phase)],
            value,
        );
    }

    out.push_str("# HELP weaver_pipeline_hot_dispatch_spillover_active_loans Active measured spillover loans.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_spillover_active_loans gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_spillover_active_loans",
        snapshot.hot_dispatch_spillover_active_loans,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent Best recent lane/pipeline expansion improvement percent.\n");
    out.push_str(
        "# TYPE weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent gauge\n",
    );
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_recent_expansion_improvement_percent",
        snapshot.hot_dispatch_recent_expansion_improvement_pct,
    );

    out.push_str("# HELP weaver_pipeline_hot_dispatch_best_mode_block_reason Last best-mode spillover block reason code.\n");
    out.push_str("# TYPE weaver_pipeline_hot_dispatch_best_mode_block_reason gauge\n");
    append_metric(
        &mut out,
        "weaver_pipeline_hot_dispatch_best_mode_block_reason",
        snapshot.hot_dispatch_best_mode_block_reason,
    );

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
        "# HELP weaver_pipeline_download_lane_states_active Active article download lanes by scheduler state.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lane_states_active gauge\n");
    for (state, value) in [
        ("idle", snapshot.download_lanes_idle_active),
        (
            "awaiting_work",
            snapshot.download_lanes_awaiting_work_active,
        ),
        (
            "binding_server",
            snapshot.download_lanes_binding_server_active,
        ),
        ("acquired", snapshot.download_lanes_acquired_active),
        ("issuing", snapshot.download_lanes_issuing_active),
        ("draining", snapshot.download_lanes_draining_active),
        (
            "yield_after_batch",
            snapshot.download_lanes_yield_after_batch_active,
        ),
        ("parking", snapshot.download_lanes_parking_active),
        ("recovering", snapshot.download_lanes_recovering_active),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_lane_states_active",
            &[("state", state)],
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
        ("pressure", snapshot.download_lane_parks_pressure_total),
        (
            "probe_yield",
            snapshot.download_lane_parks_probe_yield_total,
        ),
        (
            "hot_reclaim",
            snapshot.download_lane_parks_hot_reclaim_total,
        ),
        (
            "spillover_withdraw",
            snapshot.download_lane_parks_spillover_withdraw_total,
        ),
        (
            "spillover_speed_harm",
            snapshot.download_lane_parks_spillover_speed_harm_total,
        ),
        (
            "ip_replacement_retired",
            snapshot.download_lane_parks_ip_replacement_retired_total,
        ),
        (
            "server_tier_changed",
            snapshot.download_lane_parks_server_tier_changed_total,
        ),
        (
            "proof_failure",
            snapshot.download_lane_parks_proof_failure_total,
        ),
        ("error", snapshot.download_lane_parks_error_total),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_lane_parks_total",
            &[("reason", reason)],
            value,
        );
    }

    out.push_str(
        "# HELP weaver_pipeline_download_lane_lease_items_total Article work items leased to download lanes.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lane_lease_items_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_lane_lease_items_total",
        snapshot.download_lane_lease_items_total,
    );

    out.push_str(
        "# HELP weaver_pipeline_download_lane_refills_total Lane refill scheduler decisions.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_lane_refills_total counter\n");
    for (result, value) in [
        ("granted", snapshot.download_lane_refill_granted_total),
        ("parked", snapshot.download_lane_refill_parked_total),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_pipeline_download_lane_refills_total",
            &[("result", result)],
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

    out.push_str("# HELP weaver_ip_replacement_trial_extra_connections Configured over-max IP replacement trial burst budget.\\n");
    out.push_str("# TYPE weaver_ip_replacement_trial_extra_connections gauge\\n");
    append_metric(
        &mut out,
        "weaver_ip_replacement_trial_extra_connections",
        snapshot.ip_replacement_trial_extra_connections,
    );
    out.push_str("# HELP weaver_ip_replacement_burst_active Whether an over-max IP replacement trial is active.\\n");
    out.push_str("# TYPE weaver_ip_replacement_burst_active gauge\\n");
    append_metric(
        &mut out,
        "weaver_ip_replacement_burst_active",
        u64::from(snapshot.ip_replacement_burst_active),
    );
    out.push_str("# HELP weaver_ip_replacement_over_max_connections Current over-max IP replacement trial connections.\\n");
    out.push_str("# TYPE weaver_ip_replacement_over_max_connections gauge\\n");
    append_metric(
        &mut out,
        "weaver_ip_replacement_over_max_connections",
        snapshot.ip_replacement_over_max_connections,
    );
    out.push_str(
        "# HELP weaver_ip_rtt_ewma_entries Number of tracked per-server/per-IP BODY RTT EWMAs.\\n",
    );
    out.push_str("# TYPE weaver_ip_rtt_ewma_entries gauge\\n");
    append_metric(
        &mut out,
        "weaver_ip_rtt_ewma_entries",
        snapshot.ip_rtt_ewma_entries,
    );
    out.push_str("# HELP weaver_ip_rtt_ewma_slowest_ms Slowest tracked per-IP BODY RTT EWMA in milliseconds.\\n");
    out.push_str("# TYPE weaver_ip_rtt_ewma_slowest_ms gauge\\n");
    append_metric(
        &mut out,
        "weaver_ip_rtt_ewma_slowest_ms",
        snapshot.ip_rtt_ewma_slowest_ms,
    );
    out.push_str("# HELP weaver_ip_replacement_trials_total IP replacement trial outcomes.\\n");
    out.push_str("# TYPE weaver_ip_replacement_trials_total counter\\n");
    for (outcome, value) in [
        ("started", snapshot.ip_replacement_trials_started_total),
        ("rejected", snapshot.ip_replacement_trials_rejected_total),
        ("accepted", snapshot.ip_replacement_trials_accepted_total),
        ("blocked", snapshot.ip_replacement_trials_blocked_total),
        (
            "acquire_failed",
            snapshot.ip_replacement_trials_acquire_failed_total,
        ),
        (
            "same_ip_rejected",
            snapshot.ip_replacement_trials_same_ip_rejected_total,
        ),
    ] {
        append_labeled_metric(
            &mut out,
            "weaver_ip_replacement_trials_total",
            &[("outcome", outcome)],
            value,
        );
    }
    out.push_str("# HELP weaver_ip_replacement_old_connections_retired_total Old-IP connections retired after accepted replacement trials.\\n");
    out.push_str("# TYPE weaver_ip_replacement_old_connections_retired_total counter\\n");
    append_metric(
        &mut out,
        "weaver_ip_replacement_old_connections_retired_total",
        snapshot.ip_replacement_old_connections_retired_total,
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

    out.push_str(
        "# HELP weaver_pipeline_download_restart_durable_lead_blocked_total Restart durable lead dispatch blocks.\n",
    );
    out.push_str("# TYPE weaver_pipeline_download_restart_durable_lead_blocked_total counter\n");
    append_metric(
        &mut out,
        "weaver_pipeline_download_restart_durable_lead_blocked_total",
        snapshot.download_restart_durable_lead_blocked_total,
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
        out.push_str("# HELP weaver_server_connections_configured Operator-configured maximum connections per server.\n");
        out.push_str("# TYPE weaver_server_connections_configured gauge\n");
        out.push_str("# HELP weaver_server_connections_effective Runtime maximum connections after provider capacity adaptation.\n");
        out.push_str("# TYPE weaver_server_connections_effective gauge\n");
        out.push_str("# HELP weaver_server_capacity_penalty_until_epoch_ms Provider capacity penalty deadline in Unix epoch milliseconds.\n");
        out.push_str("# TYPE weaver_server_capacity_penalty_until_epoch_ms gauge\n");
        out.push_str("# HELP weaver_server_capacity_reductions_total Runtime connection-cap reductions caused by provider rejections.\n");
        out.push_str("# TYPE weaver_server_capacity_reductions_total counter\n");
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
                "weaver_server_connections_configured",
                labels,
                srv.connections_configured,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_connections_effective",
                labels,
                srv.connections_max,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_capacity_penalty_until_epoch_ms",
                labels,
                srv.capacity_penalty_until_epoch_ms,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_capacity_reductions_total",
                labels,
                srv.capacity_reductions,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_premature_deaths",
                labels,
                srv.premature_deaths,
            );
        }
    }

    out.push_str("# HELP weaver_nntp_runtime_generation Active NNTP runtime generation.\n");
    out.push_str("# TYPE weaver_nntp_runtime_generation gauge\n");
    append_metric(
        &mut out,
        "weaver_nntp_runtime_generation",
        runtime_generation,
    );

    if !server_transfers.is_empty() {
        out.push_str("# HELP weaver_server_download_lifetime_bytes Raw NNTP BODY bytes received per durable server.\n");
        out.push_str("# TYPE weaver_server_download_lifetime_bytes counter\n");
        out.push_str("# HELP weaver_server_download_rate_limit_bytes_per_second Configured aggregate per-server BODY rate limit; zero is unlimited.\n");
        out.push_str("# TYPE weaver_server_download_rate_limit_bytes_per_second gauge\n");
        out.push_str("# HELP weaver_server_download_throttle_seconds_total Time spent waiting on a per-server download rate limit.\n");
        out.push_str("# TYPE weaver_server_download_throttle_seconds_total counter\n");
        out.push_str("# HELP weaver_server_download_quota_enabled Whether a per-server BODY quota is enabled.\n");
        out.push_str("# TYPE weaver_server_download_quota_enabled gauge\n");
        out.push_str("# HELP weaver_server_download_quota_used_bytes BODY bytes charged in the current server quota window.\n");
        out.push_str("# TYPE weaver_server_download_quota_used_bytes gauge\n");
        out.push_str("# HELP weaver_server_download_quota_reserved_bytes Estimated BODY bytes reserved by in-flight work.\n");
        out.push_str("# TYPE weaver_server_download_quota_reserved_bytes gauge\n");
        out.push_str("# HELP weaver_server_download_quota_remaining_bytes Remaining admissible BODY bytes in the current server quota window.\n");
        out.push_str("# TYPE weaver_server_download_quota_remaining_bytes gauge\n");
        out.push_str("# HELP weaver_server_download_quota_blocked Whether the server currently rejects new BODY requests due to quota.\n");
        out.push_str("# TYPE weaver_server_download_quota_blocked gauge\n");

        for transfer in server_transfers {
            let server_id = transfer.stable_server_id.0.to_string();
            let labels: &[(&str, &str)] = &[("server_id", &server_id)];
            append_labeled_metric(
                &mut out,
                "weaver_server_download_lifetime_bytes",
                labels,
                transfer.lifetime_body_bytes,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_rate_limit_bytes_per_second",
                labels,
                transfer.rate_bytes_per_sec,
            );
            append_labeled_metric_f64(
                &mut out,
                "weaver_server_download_throttle_seconds_total",
                labels,
                transfer.throttle_wait.as_secs_f64(),
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_quota_enabled",
                labels,
                u64::from(transfer.quota_enabled),
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_quota_used_bytes",
                labels,
                transfer.quota_used_bytes,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_quota_reserved_bytes",
                labels,
                transfer.quota_reserved_bytes,
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_quota_remaining_bytes",
                labels,
                if transfer.quota_enabled {
                    transfer.quota_remaining_bytes
                } else {
                    0
                },
            );
            append_labeled_metric(
                &mut out,
                "weaver_server_download_quota_blocked",
                labels,
                u64::from(transfer.quota_blocked),
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
        JobStatus::QueuedPostProcessing => "queued_post_processing",
        JobStatus::PostProcessing => "post_processing",
        JobStatus::Complete => "complete",
        JobStatus::Failed { .. } => "failed",
        JobStatus::Paused => "paused",
    }
}
