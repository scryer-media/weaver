//! Turns a runtime snapshot into Prometheus exposition text.
//!
//! Every label set that mirrors an enum is derived from that enum's `ALL`
//! constant, and every label set that mirrors a group of snapshot fields comes
//! from an exhaustive `match`. Both exist so that adding a variant or a counter
//! upstream is a compile error here rather than a silently missing series.

use std::collections::HashMap;

use weaver_server_core::jobs::handle::{DownloadBlockKind, DownloadBlockState};
use weaver_server_core::operations::instrumentation::{
    DbRuntimeMetricsSnapshot, DiskSpaceSnapshot, HistogramSnapshot, HttpMetricsSnapshot,
    JobLifecycleMetricsSnapshot, PipelineHistogramsSnapshot, ProcessMetricsSnapshot,
    ServerMetricsSnapshot,
};
use weaver_server_core::operations::metrics_store::JOB_STATUS_KEYS;
use weaver_server_core::pipeline::{HOT_BEST_MODE_BLOCK_REASON_LABELS, HOT_EXPANSION_KIND_LABELS};
use weaver_server_core::post_processing::executor::PostProcessingMetricsSnapshot;
use weaver_server_core::settings::PerJobSeries;
use weaver_server_core::{
    DispatchShareMode, DownloadPressureReason, DownloadPressureState, JobInfo, JobStatus,
    MetricsSnapshot, SpilloverDecision,
};

use super::catalog as f;
use super::encode::{Encoder, MetricFamily};
use super::{BuildInfo, ServerHealthInfo, ServerStateKind, ServerStateReason, job_is_exported};

const BYTES_PER_MEBIBYTE: f64 = 1_048_576.0;

/// Limiter labels for `weaver_pipeline_download_observed_limiter`, sorted so
/// the exposition order is stable.
pub(crate) const OBSERVED_LIMITERS: [&str; 8] = [
    "active",
    "decode_lagging",
    "dispatch_limited",
    "gated",
    "idle",
    "infrastructure_unavailable",
    "network_limited",
    "pressure_limited",
];

#[derive(Clone, Copy)]
pub(crate) struct PrometheusRenderInput<'a> {
    pub(crate) snapshot: &'a MetricsSnapshot,
    pub(crate) jobs: &'a [JobInfo],
    pub(crate) pipeline_paused: bool,
    pub(crate) download_block: &'a DownloadBlockState,
    pub(crate) server_health: &'a [ServerHealthInfo],
    pub(crate) runtime_generation: u64,
    pub(crate) server_transfers: &'a [weaver_nntp::transfer::ServerTransferSnapshot],
    pub(crate) duplicate_admission: &'a [(&'static str, &'static str, u64)],
    pub(crate) semantic_duplicate_lifecycle: &'a [(&'static str, u64)],
    pub(crate) extraction_rejections: &'a [(&'static str, u64)],
    pub(crate) post_processing: Option<&'a PostProcessingMetricsSnapshot>,
    pub(crate) build: BuildInfo,
    pub(crate) start_time_seconds: f64,
    pub(crate) per_job_series: PerJobSeries,
    /// Per-server article outcomes and latency. Keyed by durable server id, so
    /// it is joined to `server_health` for the `server` label rather than by
    /// position.
    pub(crate) server_metrics: &'a [ServerMetricsSnapshot],
    pub(crate) job_lifecycle: Option<&'a JobLifecycleMetricsSnapshot>,
    pub(crate) pipeline_histograms: Option<&'a PipelineHistogramsSnapshot>,
    pub(crate) db_runtime: Option<&'a DbRuntimeMetricsSnapshot>,
    pub(crate) process: Option<&'a ProcessMetricsSnapshot>,
    pub(crate) disk_space: &'a [DiskSpaceSnapshot],
    pub(crate) http_metrics: Option<&'a HttpMetricsSnapshot>,
}

impl<'a> PrometheusRenderInput<'a> {
    /// Start from the two inputs that have no meaningful empty value; every
    /// other source is optional and defaults to "nothing to report".
    pub(crate) fn new(
        snapshot: &'a MetricsSnapshot,
        download_block: &'a DownloadBlockState,
    ) -> Self {
        Self {
            snapshot,
            jobs: &[],
            pipeline_paused: false,
            download_block,
            server_health: &[],
            runtime_generation: 0,
            server_transfers: &[],
            duplicate_admission: &[],
            semantic_duplicate_lifecycle: &[],
            extraction_rejections: &[],
            post_processing: None,
            build: BuildInfo {
                version: env!("CARGO_PKG_VERSION"),
                commit: option_env!("WEAVER_GIT_COMMIT").unwrap_or("unknown"),
                target_arch: std::env::consts::ARCH,
                target_os: std::env::consts::OS,
                decoder_tier: "unknown",
                database_backend: "unknown",
                tls_backend: "unknown",
            },
            start_time_seconds: 0.0,
            per_job_series: PerJobSeries::Active,
            server_metrics: &[],
            job_lifecycle: None,
            pipeline_histograms: None,
            db_runtime: None,
            process: None,
            disk_space: &[],
            http_metrics: None,
        }
    }
}

/// Test-only entry point kept at its original shape so behavioural tests do
/// not have to restate every optional input.
#[cfg(test)]
pub(crate) fn render_prometheus_metrics(
    snapshot: &MetricsSnapshot,
    jobs: &[JobInfo],
    pipeline_paused: bool,
    download_block: &DownloadBlockState,
    server_health: &[ServerHealthInfo],
    runtime_generation: u64,
) -> String {
    let mut input = PrometheusRenderInput::new(snapshot, download_block);
    input.jobs = jobs;
    input.pipeline_paused = pipeline_paused;
    input.server_health = server_health;
    input.runtime_generation = runtime_generation;
    input.build = BuildInfo::default();
    render_prometheus_metrics_input(&input)
}

pub(crate) fn render_prometheus_metrics_input(input: &PrometheusRenderInput<'_>) -> String {
    let PrometheusRenderInput {
        snapshot,
        jobs,
        pipeline_paused,
        download_block,
        server_health,
        runtime_generation,
        server_transfers,
        duplicate_admission,
        semantic_duplicate_lifecycle,
        extraction_rejections,
        post_processing,
        build,
        start_time_seconds,
        per_job_series,
        server_metrics,
        job_lifecycle,
        pipeline_histograms,
        db_runtime,
        process,
        disk_space,
        http_metrics,
    } = *input;

    let mut out = Encoder::new();

    out.sample(
        &f::BUILD_INFO,
        &[
            ("version", build.version),
            ("commit", build.commit),
            ("target_arch", build.target_arch),
            ("target_os", build.target_os),
            ("decoder_tier", build.decoder_tier),
            ("database_backend", build.database_backend),
            ("tls_backend", build.tls_backend),
        ],
        1,
    );
    out.sample_f64(&f::START_TIME_SECONDS, &[], start_time_seconds);

    render_gate(&mut out, pipeline_paused, download_block);
    render_pipeline_totals(&mut out, snapshot);
    render_queues(&mut out, snapshot);
    render_hot_dispatch(&mut out, snapshot);
    render_lanes(&mut out, snapshot);
    render_ip_replacement(&mut out, snapshot);

    for &(origin, status, count) in duplicate_admission {
        out.sample(
            &f::DUPLICATE_ADMISSION,
            &[("origin", origin), ("status", status)],
            count,
        );
    }
    for &(event, count) in semantic_duplicate_lifecycle {
        out.sample(&f::SEMANTIC_DUPLICATE_LIFECYCLE, &[("event", event)], count);
    }

    for status in JOB_STATUS_KEYS {
        let count = jobs
            .iter()
            .filter(|job| job_status_label(&job.status) == status)
            .count();
        out.sample(&f::PIPELINE_JOBS, &[("status", status)], count);
    }

    let observed_limiter =
        observed_download_limiter(snapshot, pipeline_paused, download_block, server_health);
    for limiter in OBSERVED_LIMITERS {
        out.sample(
            &f::OBSERVED_LIMITER,
            &[("limiter", limiter)],
            u8::from(observed_limiter == limiter),
        );
    }

    render_stalls_and_workers(&mut out, snapshot);
    render_rates(&mut out, snapshot);
    render_jobs(&mut out, jobs, per_job_series);
    render_servers(&mut out, server_health, runtime_generation);
    render_server_transfers(&mut out, server_transfers, server_health);
    render_server_articles(&mut out, server_metrics, server_health);

    if let Some(lifecycle) = job_lifecycle {
        render_job_lifecycle(&mut out, lifecycle);
    }
    if let Some(histograms) = pipeline_histograms {
        render_pipeline_histograms(&mut out, histograms);
    }
    if let Some(db) = db_runtime {
        render_db_runtime(&mut out, db);
    }
    if let Some(process) = process {
        render_process(&mut out, process, start_time_seconds);
    }
    render_disk_space(&mut out, disk_space);
    if let Some(http) = http_metrics {
        render_http(&mut out, http);
    }

    for &(reason, count) in extraction_rejections {
        out.sample(&f::EXTRACTION_REJECTIONS, &[("reason", reason)], count);
    }
    if let Some(metrics) = post_processing {
        render_post_processing(&mut out, metrics);
    }

    out.finish()
}

/// Render a collected histogram through the encoder.
///
/// The snapshot's `counts` are per-bucket; [`Encoder::histogram`] owns the
/// conversion to Prometheus' cumulative `le` series, so every call site stays a
/// one-liner and none of them can get the accumulation wrong.
fn render_histogram(
    out: &mut Encoder,
    family: &'static MetricFamily,
    labels: &[(&str, &str)],
    snapshot: &HistogramSnapshot,
) {
    out.histogram(
        family,
        labels,
        snapshot.bounds,
        &snapshot.counts,
        snapshot.sum,
        snapshot.count,
    );
}

fn render_gate(out: &mut Encoder, pipeline_paused: bool, download_block: &DownloadBlockState) {
    out.sample(&f::PIPELINE_PAUSED, &[], u8::from(pipeline_paused));

    for kind in DownloadBlockKind::ALL {
        out.sample(
            &f::DOWNLOAD_GATE,
            &[("reason", kind.as_str())],
            u8::from(kind == download_block.kind),
        );
    }
    out.sample(
        &f::SCHEDULED_SPEED_LIMIT,
        &[],
        download_block.scheduled_speed_limit,
    );

    out.sample(&f::CAP_ENABLED, &[], u64::from(download_block.cap_enabled));
    out.sample(&f::CAP_USED_BYTES, &[], download_block.used_bytes);
    out.sample(&f::CAP_LIMIT_BYTES, &[], download_block.limit_bytes);
    out.sample(&f::CAP_REMAINING_BYTES, &[], download_block.remaining_bytes);
    out.sample(&f::CAP_RESERVED_BYTES, &[], download_block.reserved_bytes);
    out.sample(
        &f::CAP_WINDOW_END_SECONDS,
        &[],
        download_block
            .window_ends_at_epoch_ms
            .map(|value| (value / 1000.0) as u64)
            .unwrap_or(0),
    );
}

fn render_pipeline_totals(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(&f::BYTES_DOWNLOADED, &[], snapshot.bytes_downloaded);
    out.sample(&f::BYTES_DECODED, &[], snapshot.bytes_decoded);
    out.sample(&f::BYTES_COMMITTED, &[], snapshot.bytes_committed);
    out.sample(&f::SEGMENTS_DOWNLOADED, &[], snapshot.segments_downloaded);
    out.sample(&f::SEGMENTS_DECODED, &[], snapshot.segments_decoded);
    out.sample(&f::SEGMENTS_COMMITTED, &[], snapshot.segments_committed);
    out.sample(&f::SEGMENTS_RETRIED, &[], snapshot.segments_retried);
    out.sample(
        &f::SEGMENTS_FAILED_PERMANENT,
        &[],
        snapshot.segments_failed_permanent,
    );
    out.sample(
        &f::PARKED_INFRASTRUCTURE_WORK,
        &[],
        snapshot.parked_infrastructure_work as u64,
    );
    out.sample(
        &f::GENERATION_RECOVERY_REQUEUES,
        &[],
        snapshot.nntp_generation_recovery_requeues,
    );

    out.sample(
        &f::PROBE_ATTEMPTS,
        &[],
        snapshot.nntp_capacity_probe_attempts_total,
    );
    out.sample(
        &f::PROBE_SUCCESSES,
        &[],
        snapshot.nntp_capacity_probe_successes_total,
    );
    out.sample(
        &f::PROBE_REJECTIONS,
        &[],
        snapshot.nntp_capacity_probe_rejections_total,
    );
    out.sample(
        &f::PROBE_TRANSPORT_FAILURES,
        &[],
        snapshot.nntp_capacity_probe_transport_failures_total,
    );
    out.sample(
        &f::PROBE_STALE_GENERATION,
        &[],
        snapshot.nntp_capacity_probe_stale_generation_total,
    );

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
        out.sample(&f::DOWNLOAD_FAILURES, &[("kind", kind)], value);
    }

    out.sample(&f::ARTICLES_NOT_FOUND, &[], snapshot.articles_not_found);
    out.sample(&f::DECODE_ERRORS, &[], snapshot.decode_errors);
    out.sample(&f::CRC_ERRORS, &[], snapshot.crc_errors);
}

fn render_queues(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(
        &f::DOWNLOAD_QUEUE_DEPTH,
        &[],
        snapshot.download_queue_depth as u64,
    );
    out.sample(&f::ACTIVE_DOWNLOADS, &[], snapshot.active_downloads as u64);
    out.sample(&f::ACTIVE_DECODES, &[], snapshot.active_decodes as u64);
    out.sample(&f::DECODE_PENDING, &[], snapshot.decode_pending as u64);
    out.sample(&f::DECODE_PENDING_BYTES, &[], snapshot.decode_pending_bytes);
    out.sample(&f::DECODE_ACTIVE_BYTES, &[], snapshot.decode_active_bytes);
    out.sample(&f::COMMIT_PENDING, &[], snapshot.commit_pending as u64);
    out.sample(
        &f::RECOVERY_QUEUE_DEPTH,
        &[],
        snapshot.recovery_queue_depth as u64,
    );
    out.sample(&f::WRITE_BUFFERED_BYTES, &[], snapshot.write_buffered_bytes);
    out.sample(
        &f::WRITE_BUFFERED_SEGMENTS,
        &[],
        snapshot.write_buffered_segments as u64,
    );
    out.sample(
        &f::DECODE_SOFT_LIMIT,
        &[],
        snapshot.decode_pressure_soft_limit_bytes,
    );
    out.sample(
        &f::DECODE_HARD_LIMIT,
        &[],
        snapshot.decode_pressure_hard_limit_bytes,
    );
    out.sample(
        &f::WRITE_SOFT_LIMIT,
        &[],
        snapshot.write_pressure_soft_limit_bytes,
    );
    out.sample(
        &f::WRITE_HARD_LIMIT,
        &[],
        snapshot.write_pressure_hard_limit_bytes,
    );

    for state in DownloadPressureState::ALL {
        out.sample(
            &f::PRESSURE_STATE,
            &[("state", state.as_str())],
            u8::from(snapshot.download_pressure_state == state),
        );
    }
    for reason in DownloadPressureReason::ALL {
        out.sample(
            &f::PRESSURE_REASON,
            &[("reason", reason.as_str())],
            u8::from(snapshot.download_pressure_reason == reason),
        );
    }
}

/// Cumulative counter behind each spillover decision.
///
/// `None` is a resting state, not an event, so it has no counter. The
/// exhaustive match is the point: a new [`SpilloverDecision`] variant fails to
/// compile here instead of quietly vanishing from the exposition.
fn spillover_decision_total(
    snapshot: &MetricsSnapshot,
    decision: SpilloverDecision,
) -> Option<u64> {
    match decision {
        SpilloverDecision::None => None,
        SpilloverDecision::BlockedWarmup => {
            Some(snapshot.hot_dispatch_spillover_blocked_warmup_total)
        }
        SpilloverDecision::BlockedPressure => {
            Some(snapshot.hot_dispatch_spillover_blocked_pressure_total)
        }
        SpilloverDecision::BlockedNearCap => {
            Some(snapshot.hot_dispatch_spillover_blocked_near_cap_total)
        }
        SpilloverDecision::BlockedHotCanUseCapacity => {
            Some(snapshot.hot_dispatch_spillover_blocked_hot_can_use_capacity_total)
        }
        SpilloverDecision::AllowedUnderfill => {
            Some(snapshot.hot_dispatch_spillover_allowed_underfill_total)
        }
        SpilloverDecision::Reclaimed => Some(snapshot.hot_dispatch_spillover_reclaimed_total),
        SpilloverDecision::BlockedBestModePending => {
            Some(snapshot.hot_dispatch_spillover_blocked_best_mode_pending_total)
        }
        SpilloverDecision::BlockedRecentExpansionHelped => {
            Some(snapshot.hot_dispatch_spillover_blocked_recent_expansion_helped_total)
        }
        SpilloverDecision::BlockedCapSpeed => {
            Some(snapshot.hot_dispatch_spillover_blocked_cap_speed_total)
        }
        SpilloverDecision::AllowedMeasuredUnderfill => {
            Some(snapshot.hot_dispatch_spillover_allowed_measured_underfill_total)
        }
        SpilloverDecision::AllowedBoundedSameBand => {
            Some(snapshot.hot_dispatch_spillover_allowed_bounded_same_band_total)
        }
        SpilloverDecision::ReclaimedSpeedHarm => {
            Some(snapshot.hot_dispatch_spillover_reclaimed_speed_harm_total)
        }
    }
}

fn render_hot_dispatch(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(&f::HOT_JOB_ID, &[], snapshot.hot_dispatch_job_id);
    for mode in DispatchShareMode::ALL {
        out.sample(
            &f::HOT_MODE,
            &[("mode", mode.as_str())],
            u8::from(snapshot.hot_dispatch_mode == mode),
        );
    }
    out.sample(
        &f::HOT_UNDERFILL_MS,
        &[],
        snapshot.hot_dispatch_underfill_ms,
    );
    out.sample_f64(
        &f::HOT_UNDERFILL_SECONDS,
        &[],
        snapshot.hot_dispatch_underfill_ms as f64 / 1000.0,
    );
    out.sample(
        &f::HOT_LENT_CONNECTIONS,
        &[],
        snapshot.hot_dispatch_lent_connections,
    );
    out.sample(
        &f::HOT_WARMUP_COMPLETE,
        &[],
        u8::from(snapshot.hot_dispatch_warmup_complete),
    );

    for decision in SpilloverDecision::ALL {
        out.sample(
            &f::HOT_LAST_SPILLOVER_DECISION,
            &[("decision", decision.as_str())],
            u8::from(snapshot.hot_dispatch_last_spillover_decision == decision),
        );
        if let Some(value) = spillover_decision_total(snapshot, decision) {
            out.sample(
                &f::HOT_SPILLOVER_DECISIONS,
                &[("decision", decision.as_str())],
                value,
            );
        }
    }

    out.sample(&f::HOT_SPEED, &[], snapshot.hot_dispatch_hot_speed_bps);
    out.sample(
        &f::HOT_LAST_EXPANSION_KIND_CODE,
        &[],
        snapshot.hot_dispatch_last_expansion_kind,
    );
    let expansion_kind = snapshot.hot_dispatch_last_expansion_kind;
    for (code, kind) in HOT_EXPANSION_KIND_LABELS.into_iter().enumerate() {
        out.sample(
            &f::HOT_EXPANSION_KIND,
            &[("kind", kind)],
            u8::from(code == expansion_kind),
        );
    }
    for (phase, value) in [
        ("before", snapshot.hot_dispatch_last_expansion_before_bps),
        ("after", snapshot.hot_dispatch_last_expansion_after_bps),
    ] {
        out.sample(&f::HOT_LAST_EXPANSION_SPEED, &[("phase", phase)], value);
    }
    out.sample(
        &f::HOT_EXCLUSIVE_PEAK,
        &[],
        snapshot.hot_dispatch_exclusive_peak_bps,
    );
    for (phase, value) in [
        ("pre_lend", snapshot.hot_dispatch_spillover_pre_speed_bps),
        ("post_lend", snapshot.hot_dispatch_spillover_post_speed_bps),
    ] {
        out.sample(&f::HOT_SPILLOVER_SPEED, &[("phase", phase)], value);
    }
    out.sample(
        &f::HOT_SPILLOVER_ACTIVE_LOANS,
        &[],
        snapshot.hot_dispatch_spillover_active_loans,
    );
    out.sample(
        &f::HOT_EXPANSION_IMPROVEMENT_PCT,
        &[],
        snapshot.hot_dispatch_recent_expansion_improvement_pct,
    );
    out.sample_f64(
        &f::HOT_EXPANSION_IMPROVEMENT_RATIO,
        &[],
        snapshot.hot_dispatch_recent_expansion_improvement_pct as f64 / 100.0,
    );
    out.sample(
        &f::HOT_BEST_MODE_BLOCK_CODE,
        &[],
        snapshot.hot_dispatch_best_mode_block_reason,
    );
    let block_reason = snapshot.hot_dispatch_best_mode_block_reason;
    for (code, reason) in HOT_BEST_MODE_BLOCK_REASON_LABELS.into_iter().enumerate() {
        out.sample(
            &f::HOT_BEST_MODE_BLOCK,
            &[("reason", reason)],
            u8::from(code == block_reason),
        );
    }
}

fn render_lanes(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    for (mode, value) in [
        ("sequential", snapshot.download_lanes_sequential_active),
        ("pipeline_depth2", snapshot.download_lanes_depth2_active),
        ("pipeline_depth4", snapshot.download_lanes_depth4_active),
    ] {
        out.sample(&f::LANES_ACTIVE_BY_MODE, &[("mode", mode)], value);
    }
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
        out.sample(&f::LANE_STATES_ACTIVE, &[("state", state)], value);
    }
    out.sample(&f::LANES_ACTIVE_TOTAL, &[], snapshot.download_lanes_active);
    out.sample(&f::LANES, &[], snapshot.download_lanes_active);

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
            "hot_share_yield",
            snapshot.download_lane_parks_hot_share_yield_total,
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
        out.sample(&f::LANE_PARKS, &[("reason", reason)], value);
    }

    out.sample(
        &f::LANE_LEASE_ITEMS,
        &[],
        snapshot.download_lane_lease_items_total,
    );
    for (result, value) in [
        ("granted", snapshot.download_lane_refill_granted_total),
        ("parked", snapshot.download_lane_refill_parked_total),
        ("deferred", snapshot.download_lane_refill_deferred_total),
    ] {
        out.sample(&f::LANE_REFILLS, &[("result", result)], value);
    }

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
        out.sample(&f::BODY_PROOF_EVENTS, &[("event", event)], value);
    }
    out.sample(
        &f::BODY_REPLAY_ITEMS,
        &[],
        snapshot.download_pipeline_replay_items_total,
    );
}

fn render_ip_replacement(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(
        &f::IP_TRIAL_EXTRA_CONNECTIONS,
        &[],
        snapshot.ip_replacement_trial_extra_connections,
    );
    out.sample(
        &f::IP_BURST_ACTIVE,
        &[],
        u64::from(snapshot.ip_replacement_burst_active),
    );
    out.sample(
        &f::IP_OVER_MAX_CONNECTIONS,
        &[],
        snapshot.ip_replacement_over_max_connections,
    );
    out.sample(&f::IP_RTT_ENTRIES, &[], snapshot.ip_rtt_ewma_entries);
    out.sample(&f::IP_RTT_SLOWEST_MS, &[], snapshot.ip_rtt_ewma_slowest_ms);
    out.sample_f64(
        &f::IP_RTT_SLOWEST_SECONDS,
        &[],
        snapshot.ip_rtt_ewma_slowest_ms as f64 / 1000.0,
    );
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
        out.sample(&f::IP_TRIALS, &[("outcome", outcome)], value);
    }
    out.sample(
        &f::IP_OLD_CONNECTIONS_RETIRED,
        &[],
        snapshot.ip_replacement_old_connections_retired_total,
    );
}

fn render_stalls_and_workers(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(
        &f::PRESSURE_STALLS,
        &[],
        snapshot.download_pressure_stalls_total,
    );
    out.sample(
        &f::RESTART_DURABLE_LEAD_BLOCKED,
        &[],
        snapshot.download_restart_durable_lead_blocked_total,
    );
    let stall_seconds = snapshot.download_pressure_stall_duration_ms as f64 / 1000.0;
    out.sample_f64(&f::PRESSURE_STALL_DURATION, &[], stall_seconds);
    out.sample_f64(&f::PRESSURE_STALL_SECONDS, &[], stall_seconds);
    out.sample_f64(
        &f::PRESSURE_CURRENT_STALL,
        &[],
        snapshot.download_pressure_current_stall_ms as f64 / 1000.0,
    );
    out.sample(
        &f::DIRECT_WRITE_EVICTIONS,
        &[],
        snapshot.direct_write_evictions,
    );

    for (event, value) in [
        ("admitted", snapshot.direct_sets_admitted),
        ("demoted", snapshot.direct_sets_demoted),
        ("finalized_direct", snapshot.direct_sets_finalized_direct),
        (
            "repaired_while_direct",
            snapshot.direct_sets_repaired_while_direct,
        ),
    ] {
        out.sample(&f::DIRECT_STORE_SETS, &[("event", event)], value);
    }

    out.sample(
        &f::DEOBFUSCATED_MEMBERS,
        &[],
        snapshot.deobfuscated_members_renamed,
    );

    out.sample(&f::VERIFY_ACTIVE, &[], snapshot.verify_active);
    out.sample(&f::REPAIR_ACTIVE, &[], snapshot.repair_active);
    out.sample(&f::EXTRACT_ACTIVE, &[], snapshot.extract_active);
    out.sample(
        &f::DISK_WRITE_LATENCY_US,
        &[],
        snapshot.disk_write_latency_us,
    );
    out.sample_f64(
        &f::DISK_WRITE_LATENCY_SECONDS,
        &[],
        snapshot.disk_write_latency_us as f64 / 1_000_000.0,
    );
}

fn render_rates(out: &mut Encoder, snapshot: &MetricsSnapshot) {
    out.sample(
        &f::CURRENT_DOWNLOAD_SPEED,
        &[],
        snapshot.current_download_speed,
    );
    out.sample_f64(&f::ARTICLES_PER_SECOND, &[], snapshot.articles_per_sec);
    out.sample_f64(&f::DECODE_RATE_MIB, &[], snapshot.decode_rate_mbps);
    out.sample_f64(
        &f::DECODE_RATE_BYTES,
        &[],
        snapshot.decode_rate_mbps * BYTES_PER_MEBIBYTE,
    );
}

fn render_jobs(out: &mut Encoder, jobs: &[JobInfo], mode: PerJobSeries) {
    for job in jobs {
        if !job_is_exported(&job.status, mode) {
            continue;
        }
        let job_id = job.job_id.0.to_string();
        let id: &[(&str, &str)] = &[("job_id", job_id.as_str())];

        out.sample(
            &f::JOB_INFO,
            &[
                ("job_id", job_id.as_str()),
                ("job_name", job.name.as_str()),
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
            1,
        );
        // Only the active status, not a zero for each of the other thirteen:
        // a job's status series would otherwise be 14x the job count, and the
        // aggregate mix is already on `weaver_pipeline_jobs`.
        out.sample(
            &f::JOB_STATUS,
            &[
                ("job_id", job_id.as_str()),
                ("status", job_status_label(&job.status)),
            ],
            1,
        );
        out.sample_f64(&f::JOB_PROGRESS_RATIO, id, job.progress);
        out.sample(&f::JOB_TOTAL_BYTES, id, job.total_bytes);
        out.sample(&f::JOB_DOWNLOADED_BYTES, id, job.downloaded_bytes);
        out.sample(
            &f::JOB_OPTIONAL_RECOVERY_BYTES,
            id,
            job.optional_recovery_bytes,
        );
        out.sample(
            &f::JOB_OPTIONAL_RECOVERY_DOWNLOADED_BYTES,
            id,
            job.optional_recovery_downloaded_bytes,
        );
        out.sample(&f::JOB_FAILED_BYTES, id, job.failed_bytes);
        out.sample(&f::JOB_HEALTH_PER_MILLE, id, job.health);
        out.sample_f64(
            &f::JOB_CREATED_AT_SECONDS,
            id,
            job.created_at_epoch_ms / 1000.0,
        );
    }
}

fn render_servers(out: &mut Encoder, server_health: &[ServerHealthInfo], runtime_generation: u64) {
    for srv in server_health {
        let port = srv.port.to_string();
        let priority = srv.priority.to_string();
        let id: &[(&str, &str)] = &[
            ("server_id", srv.server_id.as_str()),
            ("server", srv.label.as_str()),
        ];

        out.sample(
            &f::SERVER_INFO,
            &[
                ("server_id", srv.server_id.as_str()),
                ("server", srv.label.as_str()),
                ("host", srv.host.as_str()),
                ("port", port.as_str()),
                ("tls", bool_label(srv.tls)),
                ("priority", priority.as_str()),
                ("backfill", bool_label(srv.backfill)),
            ],
            1,
        );
        for state in ServerStateKind::ALL {
            out.sample(
                &f::SERVER_STATE,
                &[
                    ("server_id", srv.server_id.as_str()),
                    ("server", srv.label.as_str()),
                    ("state", state.as_str()),
                ],
                u8::from(srv.state == state),
            );
        }
        for reason in ServerStateReason::ALL {
            out.sample(
                &f::SERVER_STATE_REASON,
                &[
                    ("server_id", srv.server_id.as_str()),
                    ("server", srv.label.as_str()),
                    ("reason", reason.as_str()),
                ],
                u8::from(srv.state_reason == reason),
            );
        }
        out.sample_f64(
            &f::SERVER_STATE_UNTIL_SECONDS,
            id,
            srv.state_until_epoch_seconds,
        );
        out.sample(&f::SERVER_DISABLED_TOTAL, id, srv.disable_count);
        out.sample(&f::SERVER_SUCCESS_TOTAL, id, srv.success_count);
        out.sample(&f::SERVER_FAILURE_TOTAL, id, srv.failure_count);
        out.sample(
            &f::SERVER_CONSECUTIVE_FAILURES,
            id,
            srv.consecutive_failures,
        );
        out.sample_f64(&f::SERVER_LATENCY_MS, id, srv.latency_ms);
        out.sample_f64(&f::SERVER_LATENCY_SECONDS, id, srv.latency_ms / 1000.0);
        out.sample(
            &f::SERVER_CONNECTIONS_AVAILABLE,
            id,
            srv.connections_available,
        );
        out.sample(&f::SERVER_CONNECTIONS_ACTIVE, id, srv.connections_active);
        out.sample(&f::SERVER_CONNECTIONS_MAX, id, srv.connections_max);
        out.sample(
            &f::SERVER_CONNECTIONS_CONFIGURED,
            id,
            srv.connections_configured,
        );
        out.sample(&f::SERVER_CONNECTIONS_EFFECTIVE, id, srv.connections_max);
        out.sample(
            &f::SERVER_CAPACITY_PENALTY_MS,
            id,
            srv.capacity_penalty_until_epoch_ms,
        );
        out.sample_f64(
            &f::SERVER_CAPACITY_PENALTY_SECONDS,
            id,
            srv.capacity_penalty_until_epoch_ms as f64 / 1000.0,
        );
        out.sample(&f::SERVER_CAPACITY_REDUCTIONS, id, srv.capacity_reductions);
        out.sample(&f::SERVER_PREMATURE_DEATHS, id, srv.premature_deaths);
    }

    out.sample(&f::NNTP_RUNTIME_GENERATION, &[], runtime_generation);
}

fn render_server_transfers(
    out: &mut Encoder,
    server_transfers: &[weaver_nntp::transfer::ServerTransferSnapshot],
    server_health: &[ServerHealthInfo],
) {
    // `server_id` is the durable key the transfer registry uses; `server` is
    // the host:port label the health metrics have always carried. Emitting both
    // lets a dashboard join the two families without a translation table.
    let labels_by_id: HashMap<&str, &str> = server_health
        .iter()
        .map(|srv| (srv.server_id.as_str(), srv.label.as_str()))
        .collect();

    for transfer in server_transfers {
        let server_id = transfer.stable_server_id.0.to_string();
        let label = labels_by_id
            .get(server_id.as_str())
            .copied()
            .unwrap_or_default();
        let id: &[(&str, &str)] = &[("server_id", server_id.as_str()), ("server", label)];

        out.sample(
            &f::SERVER_DOWNLOAD_LIFETIME_BYTES,
            id,
            transfer.lifetime_body_bytes,
        );
        out.sample(&f::SERVER_DOWNLOAD_BYTES, id, transfer.lifetime_body_bytes);
        out.sample(
            &f::SERVER_DOWNLOAD_RATE_LIMIT,
            id,
            transfer.rate_bytes_per_sec,
        );
        out.sample_f64(
            &f::SERVER_DOWNLOAD_THROTTLE_SECONDS,
            id,
            transfer.throttle_wait.as_secs_f64(),
        );
        out.sample(
            &f::SERVER_QUOTA_ENABLED,
            id,
            u64::from(transfer.quota_enabled),
        );
        out.sample(
            &f::SERVER_QUOTA_LIMIT_BYTES,
            id,
            if transfer.quota_enabled {
                transfer.quota_limit_bytes
            } else {
                0
            },
        );
        out.sample(&f::SERVER_QUOTA_USED_BYTES, id, transfer.quota_used_bytes);
        out.sample(
            &f::SERVER_QUOTA_RESERVED_BYTES,
            id,
            transfer.quota_reserved_bytes,
        );
        out.sample(
            &f::SERVER_QUOTA_REMAINING_BYTES,
            id,
            if transfer.quota_enabled {
                transfer.quota_remaining_bytes
            } else {
                0
            },
        );
        out.sample(
            &f::SERVER_QUOTA_BLOCKED,
            id,
            u64::from(transfer.quota_blocked),
        );
    }
}

/// Per-server article outcomes and latency.
///
/// The collector keys by durable `stable_server_id`; the `server` host:port
/// label is looked up from the health list so the two per-server families join
/// without a translation table, exactly as the transfer metrics do.
fn render_server_articles(
    out: &mut Encoder,
    server_metrics: &[ServerMetricsSnapshot],
    server_health: &[ServerHealthInfo],
) {
    let labels_by_id: HashMap<&str, &str> = server_health
        .iter()
        .map(|srv| (srv.server_id.as_str(), srv.label.as_str()))
        .collect();

    for server in server_metrics {
        let server_id = server.stable_server_id.to_string();
        let label = labels_by_id
            .get(server_id.as_str())
            .copied()
            .unwrap_or_default();
        let id: &[(&str, &str)] = &[("server_id", server_id.as_str()), ("server", label)];

        for attempt in &server.attempts {
            out.sample(
                &f::SERVER_ARTICLE_ATTEMPTS,
                &[
                    ("server_id", server_id.as_str()),
                    ("server", label),
                    ("outcome", attempt.outcome),
                    ("recovery", bool_label(attempt.recovery)),
                ],
                attempt.count,
            );
        }
        render_histogram(out, &f::SERVER_ARTICLE_LATENCY, id, &server.article_latency);
    }
}

fn render_job_lifecycle(out: &mut Encoder, lifecycle: &JobLifecycleMetricsSnapshot) {
    for submission in &lifecycle.submitted {
        out.sample(
            &f::JOBS_SUBMITTED,
            &[
                ("origin", submission.origin),
                ("category", submission.category.as_str()),
            ],
            submission.count,
        );
    }
    for finish in &lifecycle.finished {
        out.sample(
            &f::JOBS_FINISHED,
            &[
                ("result", finish.result),
                ("category", finish.category.as_str()),
            ],
            finish.count,
        );
    }
    for (result, histogram) in &lifecycle.job_duration {
        render_histogram(out, &f::JOB_DURATION, &[("result", result)], histogram);
    }
    for (stage, histogram) in &lifecycle.stage_duration {
        render_histogram(out, &f::JOB_STAGE_DURATION, &[("stage", stage)], histogram);
    }
    for &(result, count) in &lifecycle.verifications {
        out.sample(&f::VERIFICATIONS, &[("result", result)], count);
    }
    for &(result, count) in &lifecycle.repairs {
        out.sample(&f::REPAIRS, &[("result", result)], count);
    }
    out.sample(
        &f::REPAIR_SLICES_REPAIRED,
        &[],
        lifecycle.repair_slices_repaired_total,
    );
    for &(result, count) in &lifecycle.extractions {
        out.sample(&f::EXTRACTIONS, &[("result", result)], count);
    }
    out.sample(&f::FILES_MISSING, &[], lifecycle.files_missing_total);
    out.sample(&f::MISSING_SEGMENTS, &[], lifecycle.missing_segments_total);
    for (category, bytes) in &lifecycle.bytes_by_category {
        out.sample(
            &f::BYTES_BY_CATEGORY,
            &[("category", category.as_str())],
            *bytes,
        );
    }
}

fn render_pipeline_histograms(out: &mut Encoder, histograms: &PipelineHistogramsSnapshot) {
    render_histogram(
        out,
        &f::DISK_WRITE_DURATION,
        &[],
        &histograms.disk_write_duration,
    );
    // These two stay absent until their stage has actually been timed: an
    // all-zero histogram would read as "measured, nothing happened" rather than
    // "not measured".
    if let Some(histogram) = &histograms.decode_task_duration {
        render_histogram(out, &f::DECODE_TASK_DURATION, &[], histogram);
    }
    if let Some(histogram) = &histograms.extract_member_duration {
        render_histogram(out, &f::EXTRACT_MEMBER_DURATION, &[], histogram);
    }
}

fn render_db_runtime(out: &mut Encoder, db: &DbRuntimeMetricsSnapshot) {
    out.sample(&f::DB_RUNTIME_INFO, &[("engine", db.engine)], 1);
    out.sample(&f::DB_RUNTIME_CONCURRENCY, &[], db.concurrency);
    out.sample(&f::DB_RUNTIME_IN_FLIGHT, &[], db.in_flight);
    out.sample(
        &f::DB_RUNTIME_BLOCKED_SUBMISSIONS,
        &[],
        db.blocked_submissions_total,
    );
    render_histogram(
        out,
        &f::DB_OP_DURATION,
        &[("engine", db.engine)],
        &db.op_duration,
    );
}

/// Standard `process_*` collector series.
///
/// Every field is optional and an absent one is omitted rather than zeroed:
/// "this platform cannot report RSS" and "this process uses no memory" must not
/// look the same on a dashboard.
fn render_process(out: &mut Encoder, process: &ProcessMetricsSnapshot, exporter_start: f64) {
    if let Some(cpu_seconds) = process.cpu_seconds_total {
        out.sample_f64(&f::PROCESS_CPU_SECONDS, &[], cpu_seconds);
    }
    if let Some(bytes) = process.resident_memory_bytes {
        out.sample(&f::PROCESS_RESIDENT_MEMORY, &[], bytes);
    }
    if let Some(bytes) = process.virtual_memory_bytes {
        out.sample(&f::PROCESS_VIRTUAL_MEMORY, &[], bytes);
    }
    if let Some(open_fds) = process.open_fds {
        out.sample(&f::PROCESS_OPEN_FDS, &[], open_fds);
    }
    if let Some(max_fds) = process.max_fds {
        out.sample(&f::PROCESS_MAX_FDS, &[], max_fds);
    }
    if let Some(threads) = process.threads {
        out.sample(&f::PROCESS_THREADS, &[], threads);
    }
    // The platform's own answer is the true process start; the exporter's
    // construction time is a close upper bound and better than no series.
    out.sample_f64(
        &f::PROCESS_START_TIME_SECONDS,
        &[],
        process.start_time_seconds.unwrap_or(exporter_start),
    );
}

fn render_disk_space(out: &mut Encoder, disk_space: &[DiskSpaceSnapshot]) {
    for disk in disk_space {
        let labels: &[(&str, &str)] = &[("role", disk.role), ("path", disk.path.as_str())];
        out.sample(&f::DISK_TOTAL_BYTES, labels, disk.total_bytes);
        out.sample(&f::DISK_AVAILABLE_BYTES, labels, disk.available_bytes);
    }
}

fn render_http(out: &mut Encoder, http: &HttpMetricsSnapshot) {
    for request in &http.requests {
        let status = request.status.to_string();
        out.sample(
            &f::HTTP_REQUESTS,
            &[
                ("route", request.route),
                ("method", request.method),
                ("status", status.as_str()),
            ],
            request.count,
        );
    }
    for (route, histogram) in &http.duration {
        render_histogram(
            out,
            &f::HTTP_REQUEST_DURATION,
            &[("route", route)],
            histogram,
        );
    }
}

pub(crate) fn render_post_processing(out: &mut Encoder, metrics: &PostProcessingMetricsSnapshot) {
    out.sample(&f::PP_QUEUE_DEPTH, &[], metrics.queue_depth);
    out.sample(&f::PP_ACTIVE_ATTEMPTS, &[], metrics.active_attempts);
    out.summary(
        &f::PP_ATTEMPT_DURATION,
        &[],
        metrics.duration_sum_millis as f64 / 1_000.0,
        metrics.duration_count,
    );
    for (result, count) in [
        ("succeeded", metrics.succeeded),
        ("failed", metrics.failed),
        ("skipped", metrics.skipped),
        ("timed_out", metrics.timed_out),
        ("cancelled", metrics.cancelled),
        ("interrupted", metrics.interrupted),
    ] {
        out.sample(&f::PP_ATTEMPT_RESULTS, &[("result", result)], count);
        out.sample(&f::PP_ATTEMPTS, &[("result", result)], count);
    }
    out.sample(&f::PP_OUTPUT_TRUNCATIONS_LEGACY, &[], metrics.truncated);
    out.sample(&f::PP_OUTPUT_TRUNCATIONS, &[], metrics.truncated);
}

const fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
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
    // Parked infrastructure retries are work: they are not in the download
    // queue, because the orchestrator holds them until a server comes back.
    // Without that third term a job whose every segment is parked reports
    // `idle` — "nothing to do" — when the truth is "nowhere to fetch from",
    // which is the exact state the next check exists to name.
    if required_queue_depth == 0
        && snapshot.active_downloads == 0
        && snapshot.parked_infrastructure_work == 0
    {
        return "idle";
    }
    // Nothing is on the wire and articles are parked on an NNTP infrastructure
    // retry — no server reachable, DNS gone, the whole pool cooling down.
    // Asked before the pressure and queue questions below because those
    // describe a downloader that is running: answering `pressure_limited` or
    // `dispatch_limited` here would point the operator at the decode/write
    // path or at Weaver's own scheduler, when the problem is upstream of both.
    // It cannot mask an ordinary busy pipeline, which has active downloads.
    if snapshot.parked_infrastructure_work > 0 && snapshot.active_downloads == 0 {
        return "infrastructure_unavailable";
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
        let decode_bytes_per_second = snapshot.decode_rate_mbps.max(0.0) * BYTES_PER_MEBIBYTE;
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

pub(crate) fn job_status_label(status: &JobStatus) -> &'static str {
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
