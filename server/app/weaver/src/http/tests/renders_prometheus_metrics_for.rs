//! `tests` tests, part of a mechanical split of the original file.

use super::*;

#[test]
fn renders_prometheus_metrics_for_pipeline_and_jobs() {
    let snapshot = populated_metrics_snapshot();
    let jobs = vec![sample_job(42, "Silver Horizon", JobStatus::Downloading)];

    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &jobs, true, &manual_pause_block(), &[], 0);

    assert_valid_prometheus_exposition(&rendered);

    let mut post_processing = String::new();
    {
        let mut encoder = metrics::Encoder::new();
        metrics::render_post_processing(&mut encoder, &sample_post_processing_metrics());
        post_processing.push_str(&encoder.finish());
    }
    assert_valid_prometheus_exposition(&post_processing);
    for expected in [
        "weaver_post_processing_queue_depth 1",
        "weaver_post_processing_active_attempts 2",
        "# TYPE weaver_post_processing_attempt_duration_seconds summary",
        "weaver_post_processing_attempt_duration_seconds_sum 4.5",
        "weaver_post_processing_attempt_duration_seconds_count 3",
        "weaver_post_processing_attempt_results{result=\"succeeded\"} 5",
        "weaver_post_processing_attempts_total{result=\"succeeded\"} 5",
        "weaver_post_processing_attempts_total{result=\"interrupted\"} 10",
        "weaver_post_processing_output_truncations 11",
        "weaver_post_processing_output_truncations_total 11",
    ] {
        assert!(
            post_processing.contains(expected),
            "post-processing exposition is missing {expected:?}:\n{post_processing}"
        );
    }

    assert!(rendered.contains("weaver_pipeline_paused 1"));
    assert!(rendered.contains("weaver_pipeline_current_download_speed_bytes_per_second 19"));
    assert!(rendered.contains("weaver_pipeline_active_downloads 6"));
    assert!(rendered.contains("weaver_pipeline_decode_pending_bytes 4096"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_state{state=\"soft\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_reason{reason=\"decode\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"gated\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_stalls_total 24"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_stall_duration_seconds 1.5"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_job_id 42"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_mode{mode=\"shared\"} 1"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_underfill_milliseconds 2500"));
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_lent_connections 2"));
    assert!(rendered.contains(
        "weaver_pipeline_hot_dispatch_last_spillover_decision{decision=\"allowed_underfill\"} 1"
    ));
    assert!(rendered.contains(
        "weaver_pipeline_hot_dispatch_spillover_decisions_total{decision=\"allowed_underfill\"} 33"
    ));
    assert!(rendered.contains("weaver_pipeline_download_lanes_active{mode=\"sequential\"} 1"));
    assert!(rendered.contains("weaver_pipeline_download_lanes_active{mode=\"pipeline_depth2\"} 2"));
    assert!(rendered.contains("weaver_pipeline_download_lane_states_active{state=\"issuing\"} 3"));
    assert!(
        rendered.contains("weaver_pipeline_download_lane_states_active{state=\"awaiting_work\"} 0")
    );
    assert!(rendered.contains("weaver_pipeline_download_lanes_active_total 3"));
    assert!(rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"no_work\"} 35"));
    assert!(rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"pressure\"} 36"));
    assert!(
        rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"probe_yield\"} 37")
    );
    assert!(
        rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"hot_reclaim\"} 38")
    );
    assert!(
        rendered.contains(
            "weaver_pipeline_download_lane_parks_total{reason=\"spillover_withdraw\"} 39"
        )
    );
    assert!(
        rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"proof_failure\"} 41")
    );
    assert!(rendered.contains("weaver_pipeline_download_lane_parks_total{reason=\"error\"} 42"));
    assert!(rendered.contains("weaver_pipeline_download_lane_lease_items_total 43"));
    assert!(
        rendered.contains("weaver_pipeline_download_lane_refills_total{result=\"granted\"} 44")
    );
    assert!(rendered.contains("weaver_pipeline_download_lane_refills_total{result=\"parked\"} 45"));
    assert!(
        rendered.contains("weaver_pipeline_body_proof_events_total{event=\"trial_success\"} 46")
    );
    assert!(rendered.contains("weaver_pipeline_body_proof_events_total{event=\"cooldown\"} 49"));
    assert!(rendered.contains("weaver_pipeline_body_replay_items_total 50"));
    assert!(rendered.contains("weaver_ip_replacement_trials_total{outcome=\"accepted\"} 53"));
    assert!(!rendered.contains("weaver_ip_replacement_trials_total{outcome=\"old_retired\"}"));
    assert!(rendered.contains("weaver_ip_replacement_old_connections_retired_total 55"));
    assert!(
        rendered.contains("weaver_pipeline_download_failures_total{kind=\"article_not_found\"} 24")
    );
    assert!(
        rendered
            .contains("weaver_pipeline_download_failures_total{kind=\"capacity_unavailable\"} 25")
    );
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"transient\"} 26"));
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"auth\"} 27"));
    assert!(rendered.contains("weaver_pipeline_download_failures_total{kind=\"permanent\"} 28"));
    assert!(rendered.contains("weaver_pipeline_parked_infrastructure_work 29"));
    assert!(rendered.contains("weaver_nntp_generation_recovery_requeues_total 30"));
    // The descriptive labels live on the info metric; the value series carry
    // job_id alone so a rename or a status change does not churn their identity.
    assert!(rendered.contains(
        "weaver_job_info{job_id=\"42\",job_name=\"Silver Horizon\",category=\"tv\",has_password=\"true\"} 1"
    ));
    // Only the active status is emitted, so a job costs one status series
    // rather than one per possible status.
    assert!(rendered.contains("weaver_job_status{job_id=\"42\",status=\"downloading\"} 1"));
    assert!(!rendered.contains("weaver_job_status{job_id=\"42\",status=\"complete\""));
    assert!(rendered.contains("weaver_job_progress_ratio{job_id=\"42\"} 0.5"));
    assert!(rendered.contains("weaver_job_downloaded_bytes{job_id=\"42\"} 50"));
    assert!(rendered.contains("weaver_pipeline_jobs{status=\"downloading\"} 1"));
    assert!(rendered.contains("weaver_pipeline_jobs{status=\"post_processing\"} 0"));

    // Fixed units and dual-emitted renames.
    assert!(rendered.contains("weaver_pipeline_hot_dispatch_underfill_seconds 2.5"));
    assert!(rendered.contains("weaver_pipeline_download_pressure_stall_seconds_total 1.5"));
    assert!(rendered.contains("weaver_pipeline_disk_write_latency_microseconds 16"));
    assert!(rendered.contains("weaver_pipeline_disk_write_latency_seconds 0.000016"));
    assert!(rendered.contains("weaver_ip_rtt_ewma_slowest_ms 123"));
    assert!(rendered.contains("weaver_ip_rtt_ewma_slowest_seconds 0.123"));
    assert!(rendered.contains("weaver_pipeline_download_lanes 3"));
    assert!(
        rendered.contains("weaver_pipeline_hot_dispatch_recent_expansion_improvement_ratio 0.05")
    );
    assert!(rendered.contains("weaver_pipeline_decode_rate_mebibytes_per_second 23.5"));
    assert!(rendered.contains("weaver_pipeline_decode_rate_bytes_per_second 24641536"));
    assert!(rendered.contains("weaver_pipeline_scheduled_speed_limit_bytes_per_second 4096"));

    // The literal-\n bug hid these entirely; the exposition validator now
    // rejects the shape that caused it, but pin the samples too.
    assert!(rendered.contains("# TYPE weaver_ip_replacement_trials_total counter\n"));
    assert!(rendered.contains("weaver_ip_replacement_trials_total{outcome=\"started\"} 51"));
    assert!(rendered.contains("weaver_ip_replacement_trial_extra_connections 1"));
    assert!(rendered.contains("weaver_ip_replacement_burst_active 1"));
    assert!(rendered.contains("weaver_ip_rtt_ewma_entries 2"));

    // Deprecated families announce their replacement in HELP.
    assert!(rendered.contains("(deprecated: use weaver_pipeline_decode_rate_bytes_per_second)"));

    // Direct-store counters were collected but never exported.
    for event in [
        "admitted",
        "demoted",
        "finalized_direct",
        "repaired_while_direct",
    ] {
        assert!(
            rendered.contains(&format!(
                "weaver_direct_store_sets_total{{event=\"{event}\"}}"
            )),
            "missing direct-store event {event}"
        );
    }

    assert!(rendered.contains("weaver_build_info{version=\"test-version\",commit="));
    // The two runtime-resolved choices that decide how fast this build can go
    // are only visible through these labels, so pin both rather than the
    // family name alone.
    assert!(
        rendered.contains(
            "decoder_tier=\"scalar\",database_backend=\"sqlite\",tls_backend=\"rustls\"} 1"
        ),
        "weaver_build_info lost its runtime-choice labels: {}",
        rendered
            .lines()
            .find(|line| line.starts_with("weaver_build_info"))
            .unwrap_or_default()
    );
    assert!(rendered.contains("weaver_start_time_seconds "));

    let quota_rendered = metrics::render_prometheus_metrics(
        &snapshot,
        &jobs,
        false,
        &DownloadBlockState {
            kind: DownloadBlockKind::ServerQuota,
            ..DownloadBlockState::default()
        },
        &[],
        0,
    );
    assert_valid_prometheus_exposition(&quota_rendered);
    assert!(quota_rendered.contains("weaver_pipeline_download_gate{reason=\"server_quota\"} 1"));
    assert!(quota_rendered.contains("weaver_pipeline_download_gate{reason=\"none\"} 0"));
    assert!(quota_rendered.contains("weaver_pipeline_download_gate{reason=\"manual_pause\"} 0"));
    assert!(quota_rendered.contains("weaver_pipeline_download_gate{reason=\"isp_cap\"} 0"));

    // The gate that shipped without a label: a schedule-imposed pause used to
    // render as no gate at all.
    let scheduled = metrics::render_prometheus_metrics(
        &snapshot,
        &jobs,
        false,
        &DownloadBlockState {
            kind: DownloadBlockKind::Scheduled,
            ..DownloadBlockState::default()
        },
        &[],
        0,
    );
    assert!(scheduled.contains("weaver_pipeline_download_gate{reason=\"scheduled\"} 1"));
    assert!(scheduled.contains("weaver_pipeline_download_gate{reason=\"none\"} 0"));
}

#[test]
fn renders_prometheus_download_observed_limiter_states() {
    let mut snapshot = MetricsSnapshot {
        bytes_downloaded: 0,
        bytes_decoded: 0,
        bytes_committed: 0,
        download_queue_depth: 10,
        active_downloads: 20,
        active_decodes: 0,
        decode_pending: 0,
        decode_pending_bytes: 0,
        decode_active_bytes: 0,
        commit_pending: 0,
        write_buffered_bytes: 0,
        write_buffered_segments: 0,
        write_pending_bytes: 0,
        uu_spooled_bytes: 0,
        uu_spooled_segments: 0,
        direct_write_evictions: 0,
        direct_sets_admitted: 0,
        direct_sets_demoted: 0,
        direct_sets_finalized_direct: 0,
        direct_sets_repaired_while_direct: 0,
        deobfuscated_members_renamed: 0,
        decode_pressure_soft_limit_bytes: 100,
        decode_pressure_hard_limit_bytes: 200,
        write_pressure_soft_limit_bytes: 100,
        write_pressure_hard_limit_bytes: 200,
        download_pressure_state: weaver_server_core::DownloadPressureState::Clear,
        download_pressure_reason: weaver_server_core::DownloadPressureReason::None,
        download_pressure_stalls_total: 0,
        download_pressure_stall_duration_ms: 0,
        download_pressure_current_stall_ms: 0,
        download_restart_durable_lead_blocked_total: 0,
        hot_dispatch_job_id: 0,
        hot_dispatch_mode: weaver_server_core::DispatchShareMode::Exclusive,
        hot_dispatch_underfill_ms: 0,
        hot_dispatch_lent_connections: 0,
        hot_dispatch_last_spillover_decision: weaver_server_core::SpilloverDecision::None,
        hot_dispatch_spillover_blocked_pressure_total: 0,
        hot_dispatch_spillover_blocked_near_cap_total: 0,
        hot_dispatch_spillover_blocked_hot_can_use_capacity_total: 0,
        hot_dispatch_spillover_blocked_best_mode_pending_total: 0,
        hot_dispatch_spillover_blocked_cap_speed_total: 0,
        hot_dispatch_spillover_allowed_underfill_total: 0,
        hot_dispatch_spillover_allowed_measured_underfill_total: 0,
        hot_dispatch_spillover_reclaimed_total: 0,
        hot_dispatch_hot_speed_bps: 0,
        hot_dispatch_exclusive_peak_bps: 0,
        hot_dispatch_spillover_pre_speed_bps: 0,
        hot_dispatch_spillover_post_speed_bps: 0,
        hot_dispatch_spillover_active_loans: 0,
        hot_dispatch_spillover_reclaimed_speed_harm_total: 0,
        hot_dispatch_recent_expansion_improvement_pct: 0,
        hot_dispatch_best_mode_block_reason: 0,
        hot_dispatch_last_expansion_kind: 0,
        hot_dispatch_last_expansion_before_bps: 0,
        hot_dispatch_last_expansion_after_bps: 0,
        download_lanes_active: 0,
        download_lanes_sequential_active: 0,
        download_lanes_depth2_active: 0,
        download_lanes_depth4_active: 0,
        download_lanes_depth8_active: 0,
        download_lanes_idle_active: 0,
        download_lanes_awaiting_work_active: 0,
        download_lanes_binding_server_active: 0,
        download_lanes_acquired_active: 0,
        download_lanes_issuing_active: 0,
        download_lanes_draining_active: 0,
        download_lanes_yield_after_batch_active: 0,
        download_lanes_parking_active: 0,
        download_lanes_recovering_active: 0,
        download_lane_parks_no_work_total: 0,
        download_lane_parks_pressure_total: 0,
        download_lane_parks_probe_yield_total: 0,
        download_lane_parks_hot_reclaim_total: 0,
        download_lane_parks_hot_share_yield_total: 0,
        download_lane_parks_spillover_withdraw_total: 0,
        download_lane_parks_spillover_speed_harm_total: 0,
        download_lane_parks_ip_replacement_retired_total: 0,
        download_lane_parks_proof_failure_total: 0,
        download_lane_parks_error_total: 0,
        download_lane_lease_items_total: 0,
        download_lane_refill_granted_total: 0,
        download_lane_refill_parked_total: 0,
        download_lane_refill_deferred_total: 0,
        download_pipeline_trial_success_total: 0,
        download_pipeline_trial_failure_total: 0,
        download_pipeline_proof_pass_total: 0,
        download_pipeline_cooldown_total: 0,
        download_pipeline_replay_items_total: 0,
        ip_replacement_trial_extra_connections: 0,
        ip_replacement_burst_active: false,
        ip_replacement_over_max_connections: 0,
        ip_rtt_ewma_entries: 0,
        ip_rtt_ewma_slowest_ms: 0,
        ip_replacement_trials_started_total: 0,
        ip_replacement_trials_rejected_total: 0,
        ip_replacement_trials_accepted_total: 0,
        ip_replacement_trials_blocked_total: 0,
        ip_replacement_trials_acquire_failed_total: 0,
        ip_replacement_trials_same_ip_rejected_total: 0,
        ip_replacement_old_connections_retired_total: 0,
        segments_downloaded: 0,
        segments_decoded: 0,
        segments_committed: 0,
        articles_not_found: 0,
        decode_errors: 0,
        verify_active: 0,
        repair_active: 0,
        extract_active: 0,
        disk_write_latency_us: 0,
        segments_retried: 0,
        segments_failed_permanent: 0,
        parked_infrastructure_work: 0,
        nntp_generation_recovery_requeues: 0,
        download_failures_article_not_found: 0,
        download_failures_capacity_unavailable: 0,
        download_failures_transient: 0,
        download_failures_auth: 0,
        download_failures_permanent: 0,
        current_download_speed: 0,
        crc_errors: 0,
        recovery_queue_depth: 0,
        articles_per_sec: 0.0,
        decode_rate_mbps: 0.0,
    };
    let unblocked = DownloadBlockState {
        kind: DownloadBlockKind::None,
        cap_enabled: false,
        period: None,
        used_bytes: 0,
        limit_bytes: 0,
        remaining_bytes: 0,
        reserved_bytes: 0,
        window_starts_at_epoch_ms: None,
        window_ends_at_epoch_ms: None,
        timezone_name: "MDT".into(),
        scheduled_speed_limit: 0,
    };
    let server_health = vec![sample_server_health()];

    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health, 2);
    assert_valid_prometheus_exposition(&rendered);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"network_limited\"} 1")
    );
    // Every per-server series now carries both identities.
    assert!(rendered.contains(
        "weaver_server_connections_configured{server_id=\"7\",server=\"news.example:563\"} 80"
    ));
    assert!(
        rendered.contains(
            "weaver_server_connections_max{server_id=\"7\",server=\"news.example:563\"} 80"
        )
    );
    assert!(rendered.contains(
        "weaver_server_info{server_id=\"7\",server=\"news.example:563\",host=\"news.example\",port=\"563\",tls=\"true\",priority=\"1\",backfill=\"false\"} 1"
    ));
    assert!(rendered.contains("weaver_nntp_runtime_generation 2"));

    snapshot.decode_pending_bytes = 128 * 1024 * 1024;
    snapshot.current_download_speed = 30 * 1024 * 1024;
    snapshot.decode_rate_mbps = 5.0;
    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health, 2);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"decode_lagging\"} 1")
    );
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"network_limited\"} 0")
    );

    snapshot.decode_pending_bytes = 64 * 1024 * 1024;
    snapshot.decode_active_bytes = 8 * 1024 * 1024;
    snapshot.current_download_speed = 4 * 1024 * 1024;
    snapshot.decode_rate_mbps = 5.0;
    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &server_health, 2);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"decode_lagging\"} 1")
    );

    snapshot.decode_pending_bytes = 0;
    snapshot.decode_active_bytes = 0;
    snapshot.current_download_speed = 0;
    snapshot.decode_rate_mbps = 0.0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Soft;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::Write;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"pressure_limited\"} 1")
    );

    // Work queued, nothing on the wire, every remaining article parked on an
    // NNTP infrastructure retry. Before this value the same shape rendered as
    // `pressure_limited` or `dispatch_limited` — both of which describe a
    // downloader that is running, and both of which send the operator to the
    // wrong subsystem.
    snapshot.decode_pending_bytes = 0;
    snapshot.decode_active_bytes = 0;
    snapshot.current_download_speed = 0;
    snapshot.decode_rate_mbps = 0.0;
    snapshot.download_queue_depth = 10;
    snapshot.recovery_queue_depth = 0;
    snapshot.active_downloads = 0;
    snapshot.parked_infrastructure_work = 10;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Soft;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::Write;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert_valid_prometheus_exposition(&rendered);
    assert!(rendered.contains(
        "weaver_pipeline_download_observed_limiter{limiter=\"infrastructure_unavailable\"} 1"
    ));
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"pressure_limited\"} 0")
    );
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"dispatch_limited\"} 0")
    );

    // The shape a live outage actually has: the parked segments are held by the
    // orchestrator rather than sitting in the download queue, so the queue
    // reads empty. This used to render as `idle` — "nothing to do" — for a job
    // that could not reach a single server.
    snapshot.download_queue_depth = 0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Clear;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::None;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(rendered.contains(
        "weaver_pipeline_download_observed_limiter{limiter=\"infrastructure_unavailable\"} 1"
    ));
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 0"));

    // Parked work alongside live downloads is an ordinary busy pipeline, not an
    // outage: the new value must not mask it.
    snapshot.download_queue_depth = 10;
    snapshot.active_downloads = 4;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Clear;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::None;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(rendered.contains(
        "weaver_pipeline_download_observed_limiter{limiter=\"infrastructure_unavailable\"} 0"
    ));
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"active\"} 1"));

    // A gate still wins: it is the reason, and the parked work is its effect.
    let gated = metrics::render_prometheus_metrics(&snapshot, &[], true, &unblocked, &[], 0);
    assert!(gated.contains("weaver_pipeline_download_observed_limiter{limiter=\"gated\"} 1"));
    assert!(gated.contains(
        "weaver_pipeline_download_observed_limiter{limiter=\"infrastructure_unavailable\"} 0"
    ));

    snapshot.parked_infrastructure_work = 0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Clear;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::None;
    snapshot.download_queue_depth = 0;
    snapshot.active_downloads = 0;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));

    snapshot.download_queue_depth = 242;
    snapshot.recovery_queue_depth = 242;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));
    assert!(
        rendered
            .contains("weaver_pipeline_download_observed_limiter{limiter=\"dispatch_limited\"} 0")
    );

    snapshot.download_queue_depth = 0;
    snapshot.recovery_queue_depth = 0;
    snapshot.download_pressure_state = weaver_server_core::DownloadPressureState::Soft;
    snapshot.download_pressure_reason = weaver_server_core::DownloadPressureReason::Write;
    let rendered = metrics::render_prometheus_metrics(&snapshot, &[], false, &unblocked, &[], 0);
    assert!(rendered.contains("weaver_pipeline_download_observed_limiter{limiter=\"idle\"} 1"));
}

#[test]
fn escapes_prometheus_label_values() {
    assert_eq!(
        metrics::escape_prometheus_label_value("a\"b\\c\nd"),
        "a\\\"b\\\\c\\nd"
    );
}

/// The regression that motivated the descriptor rewrite: label sets were
/// restated by hand next to the enum they mirrored, so `Scheduled`,
/// a spillover-decision variant since removed, `hot_share_yield`, `deferred`,
/// `queued_post_processing` and `post_processing` were all collected by the
/// runtime and then dropped on the floor at scrape time.
#[test]
fn rendered_label_sets_cover_every_enum_variant() {
    let snapshot = populated_metrics_snapshot();
    let jobs = vec![sample_job(42, "Silver Horizon", JobStatus::Downloading)];
    let server_health = vec![sample_server_health()];
    let rendered = metrics::render_prometheus_metrics(
        &snapshot,
        &jobs,
        false,
        &manual_pause_block(),
        &server_health,
        1,
    );
    assert_valid_prometheus_exposition(&rendered);

    let gate_reasons: Vec<&str> = DownloadBlockKind::ALL
        .iter()
        .map(|kind| kind.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_gate",
        "reason",
        &gate_reasons,
    );
    assert!(gate_reasons.contains(&"scheduled"));

    let pressure_states: Vec<&str> = weaver_server_core::DownloadPressureState::ALL
        .iter()
        .map(|state| state.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_pressure_state",
        "state",
        &pressure_states,
    );

    let pressure_reasons: Vec<&str> = weaver_server_core::DownloadPressureReason::ALL
        .iter()
        .map(|reason| reason.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_pressure_reason",
        "reason",
        &pressure_reasons,
    );

    let modes: Vec<&str> = weaver_server_core::DispatchShareMode::ALL
        .iter()
        .map(|mode| mode.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_hot_dispatch_mode",
        "mode",
        &modes,
    );

    let decisions: Vec<&str> = weaver_server_core::SpilloverDecision::ALL
        .iter()
        .map(|decision| decision.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_hot_dispatch_last_spillover_decision",
        "decision",
        &decisions,
    );
    // The totals family has no counter for the resting `none` state.
    let counted_decisions: Vec<&str> = decisions
        .iter()
        .copied()
        .filter(|decision| *decision != "none")
        .collect();
    assert_label_set(
        &rendered,
        "weaver_pipeline_hot_dispatch_spillover_decisions_total",
        "decision",
        &counted_decisions,
    );
    assert!(counted_decisions.contains(&"allowed_measured_underfill"));

    assert_label_set(
        &rendered,
        "weaver_pipeline_jobs",
        "status",
        &weaver_server_core::operations::metrics_store::JOB_STATUS_KEYS,
    );
    // The aggregate gauge covers every status; the per-job state-set carries
    // only the statuses actually held by a job in this render.
    assert_label_set(&rendered, "weaver_job_status", "status", &["downloading"]);

    assert_label_set(
        &rendered,
        "weaver_pipeline_download_observed_limiter",
        "limiter",
        &metrics::OBSERVED_LIMITERS,
    );

    let server_states: Vec<&str> = metrics::ServerStateKind::ALL
        .iter()
        .map(|state| state.as_str())
        .collect();
    assert_label_set(&rendered, "weaver_server_state", "state", &server_states);
    let server_reasons: Vec<&str> = metrics::ServerStateReason::ALL
        .iter()
        .map(|reason| reason.as_str())
        .collect();
    assert_label_set(
        &rendered,
        "weaver_server_state_reason",
        "reason",
        &server_reasons,
    );
}

/// Label sets backed by a group of snapshot counters rather than an enum. The
/// exporter derives these from exhaustive `match`/tuple lists; this pins the
/// three that had drifted.
#[test]
fn rendered_label_sets_cover_every_snapshot_counter() {
    let snapshot = populated_metrics_snapshot();
    let rendered = metrics::render_prometheus_metrics(
        &snapshot,
        &[],
        false,
        &DownloadBlockState::default(),
        &[],
        0,
    );

    assert_label_set(
        &rendered,
        "weaver_pipeline_download_lane_parks_total",
        "reason",
        &[
            "no_work",
            "pressure",
            "probe_yield",
            "hot_reclaim",
            "hot_share_yield",
            "spillover_withdraw",
            "spillover_speed_harm",
            "ip_replacement_retired",
            "proof_failure",
            "error",
        ],
    );
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_lane_refills_total",
        "result",
        &["granted", "parked", "deferred"],
    );
    assert_label_set(
        &rendered,
        "weaver_direct_store_sets_total",
        "event",
        &[
            "admitted",
            "demoted",
            "finalized_direct",
            "repaired_while_direct",
        ],
    );
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_failures_total",
        "kind",
        &[
            "article_not_found",
            "capacity_unavailable",
            "transient",
            "auth",
            "permanent",
        ],
    );
    assert_label_set(
        &rendered,
        "weaver_ip_replacement_trials_total",
        "outcome",
        &[
            "started",
            "rejected",
            "accepted",
            "blocked",
            "acquire_failed",
            "same_ip_rejected",
        ],
    );
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_lanes_active",
        "mode",
        &["sequential", "pipeline_depth2", "pipeline_depth4"],
    );
    assert_label_set(
        &rendered,
        "weaver_pipeline_download_lane_states_active",
        "state",
        &[
            "idle",
            "awaiting_work",
            "binding_server",
            "acquired",
            "issuing",
            "draining",
            "yield_after_batch",
            "parking",
            "recovering",
        ],
    );
}

/// Every `JobStatus` variant must land on a label the aggregate gauge also
/// emits, or a job silently stops being counted anywhere.
#[test]
fn job_status_labels_cover_every_variant() {
    let statuses = [
        JobStatus::Queued,
        JobStatus::Downloading,
        JobStatus::Checking,
        JobStatus::Verifying,
        JobStatus::QueuedRepair,
        JobStatus::Repairing,
        JobStatus::QueuedExtract,
        JobStatus::Extracting,
        JobStatus::Moving,
        JobStatus::QueuedPostProcessing,
        JobStatus::PostProcessing,
        JobStatus::Complete,
        JobStatus::Failed {
            error: "boom".into(),
        },
        JobStatus::Paused,
    ];
    let keys = weaver_server_core::operations::metrics_store::JOB_STATUS_KEYS;
    assert_eq!(statuses.len(), keys.len());

    let mut produced: Vec<&str> = statuses
        .iter()
        .map(|status| metrics::job_status_label(status))
        .collect();
    produced.sort_unstable();
    let mut expected: Vec<&str> = keys.to_vec();
    expected.sort_unstable();
    assert_eq!(produced, expected);
}

#[test]
fn per_job_series_knob_controls_job_cardinality() {
    let snapshot = populated_metrics_snapshot();
    let block = DownloadBlockState::default();
    let jobs = vec![
        sample_job(1, "Silver Horizon", JobStatus::Downloading),
        sample_job(2, "Amber Tide", JobStatus::Complete),
        sample_job(
            3,
            "Cobalt Drift",
            JobStatus::Failed {
                error: "boom".into(),
            },
        ),
    ];

    let render_with = |mode| {
        let mut input = metrics::PrometheusRenderInput::new(&snapshot, &block);
        input.jobs = &jobs;
        input.per_job_series = mode;
        metrics::render_prometheus_metrics_input(&input)
    };

    let active = render_with(weaver_server_core::settings::PerJobSeries::Active);
    assert_valid_prometheus_exposition(&active);
    assert_eq!(
        rendered_label_values(&active, "weaver_job_info", "job_id"),
        vec!["1".to_string()],
        "active mode must drop finished jobs"
    );

    let all = render_with(weaver_server_core::settings::PerJobSeries::All);
    assert_valid_prometheus_exposition(&all);
    assert_eq!(
        rendered_label_values(&all, "weaver_job_info", "job_id"),
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );

    let off = render_with(weaver_server_core::settings::PerJobSeries::Off);
    assert_valid_prometheus_exposition(&off);
    assert!(!off.contains("weaver_job_info"));
    assert!(!off.contains("weaver_job_downloaded_bytes"));
    // The aggregate queue mix survives every setting.
    assert!(off.contains("weaver_pipeline_jobs{status=\"downloading\"} 1"));
    assert!(off.contains("weaver_pipeline_jobs{status=\"complete\"} 1"));
    assert!(off.contains("weaver_pipeline_jobs{status=\"failed\"} 1"));
}

#[test]
fn server_state_renders_as_a_state_set_with_reasons() {
    let snapshot = populated_metrics_snapshot();
    let block = DownloadBlockState::default();
    let mut disabled = sample_server_health();
    disabled.state = metrics::ServerStateKind::Disabled;
    disabled.state_reason = metrics::ServerStateReason::AuthFailure;
    disabled.state_until_epoch_seconds = 1_700_000_100.0;
    disabled.disable_count = 4;
    disabled.latency_ms = 250.0;
    disabled.connections_active = 3;

    let rendered =
        metrics::render_prometheus_metrics(&snapshot, &[], false, &block, &[disabled], 0);
    assert_valid_prometheus_exposition(&rendered);

    assert!(rendered.contains(
        "weaver_server_state{server_id=\"7\",server=\"news.example:563\",state=\"disabled\"} 1"
    ));
    assert!(rendered.contains(
        "weaver_server_state{server_id=\"7\",server=\"news.example:563\",state=\"healthy\"} 0"
    ));
    assert!(rendered.contains(
        "weaver_server_state_reason{server_id=\"7\",server=\"news.example:563\",reason=\"auth_failure\"} 1"
    ));
    assert!(rendered.contains(
        "weaver_server_state_until_seconds{server_id=\"7\",server=\"news.example:563\"} 1700000100"
    ));
    assert!(
        rendered.contains(
            "weaver_server_disabled_total{server_id=\"7\",server=\"news.example:563\"} 4"
        )
    );
    assert!(rendered.contains(
        "weaver_server_latency_seconds{server_id=\"7\",server=\"news.example:563\"} 0.25"
    ));
    assert!(rendered.contains(
        "weaver_server_connections_active{server_id=\"7\",server=\"news.example:563\"} 3"
    ));
}

/// The collection API's snapshots must reach the exposition intact: the right
/// labels, and — for the six histogram families — cumulative `le` buckets with
/// a matching `_sum`/`_count`.
#[test]
fn renders_collected_instrumentation_snapshots() {
    let rendered = fully_populated_render();
    assert_valid_prometheus_exposition(&rendered);

    // Per-server attempts: 6 outcomes x recovery true/false, all present.
    assert_label_set(
        &rendered,
        "weaver_server_article_attempts_total",
        "outcome",
        &instr::ServerAttemptOutcomeKind::ALL
            .iter()
            .map(|outcome| outcome.as_str())
            .collect::<Vec<_>>(),
    );
    assert_label_set(
        &rendered,
        "weaver_server_article_attempts_total",
        "recovery",
        &["true", "false"],
    );
    assert_eq!(
        rendered_label_values(&rendered, "weaver_server_article_attempts_total", "server"),
        vec!["news.example:563".to_string()],
        "attempts must join to the health list for their host:port label"
    );
    assert!(rendered.contains(
        "weaver_server_article_attempts_total{server_id=\"7\",server=\"news.example:563\",outcome=\"not_found\",recovery=\"false\"} 11"
    ));
    assert!(rendered.contains(
        "weaver_server_article_attempts_total{server_id=\"7\",server=\"news.example:563\",outcome=\"success\",recovery=\"true\"} 1"
    ));

    // Histogram shape, checked once in full on the per-server latency family.
    for expected in [
        "weaver_server_article_latency_seconds_bucket{server_id=\"7\",server=\"news.example:563\",le=\"0.1\"} 2",
        "weaver_server_article_latency_seconds_bucket{server_id=\"7\",server=\"news.example:563\",le=\"1\"} 5",
        "weaver_server_article_latency_seconds_bucket{server_id=\"7\",server=\"news.example:563\",le=\"+Inf\"} 6",
        "weaver_server_article_latency_seconds_sum{server_id=\"7\",server=\"news.example:563\"} 4.5",
        "weaver_server_article_latency_seconds_count{server_id=\"7\",server=\"news.example:563\"} 6",
    ] {
        assert!(rendered.contains(expected), "missing {expected:?}");
    }

    // Every histogram family declares TYPE histogram and lands its +Inf bucket.
    for family in [
        "weaver_server_article_latency_seconds",
        "weaver_job_duration_seconds",
        "weaver_job_stage_duration_seconds",
        "weaver_pipeline_disk_write_duration_seconds",
        "weaver_pipeline_decode_task_duration_seconds",
        "weaver_pipeline_extract_member_duration_seconds",
        "weaver_db_op_duration_seconds",
        "weaver_http_request_duration_seconds",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {family} histogram\n")),
            "{family} is not declared a histogram"
        );
        assert!(
            rendered.contains(&format!("{family}_bucket")),
            "{family} emitted no buckets"
        );
    }

    // Job lifecycle.
    assert!(rendered.contains("weaver_jobs_submitted_total{origin=\"api\",category=\"tv\"} 5"));
    assert!(rendered.contains("weaver_jobs_finished_total{result=\"complete\",category=\"tv\"} 4"));
    assert_label_set(
        &rendered,
        "weaver_job_duration_seconds_bucket",
        "result",
        &instr::JobResultKind::ALL
            .iter()
            .map(|result| result.as_str())
            .collect::<Vec<_>>(),
    );
    assert_label_set(
        &rendered,
        "weaver_job_stage_duration_seconds_bucket",
        "stage",
        &instr::JobStageKind::ALL
            .iter()
            .map(|stage| stage.as_str())
            .collect::<Vec<_>>(),
    );
    assert_label_set(
        &rendered,
        "weaver_verifications_total",
        "result",
        &instr::VerificationOutcomeKind::ALL
            .iter()
            .map(|outcome| outcome.as_str())
            .collect::<Vec<_>>(),
    );
    assert_label_set(
        &rendered,
        "weaver_repairs_total",
        "result",
        &instr::StageOutcomeKind::ALL
            .iter()
            .map(|outcome| outcome.as_str())
            .collect::<Vec<_>>(),
    );
    assert!(rendered.contains("weaver_repair_slices_repaired_total 17"));
    assert!(rendered.contains("weaver_files_missing_total 6"));
    assert!(rendered.contains("weaver_missing_segments_total 61"));
    // An uncategorised job renders as the empty category rather than vanishing.
    assert!(rendered.contains("weaver_bytes_downloaded_by_category_total{category=\"tv\"} 4096"));
    assert!(rendered.contains("weaver_bytes_downloaded_by_category_total{category=\"\"} 512"));

    // Database runtime.
    assert!(rendered.contains("weaver_db_runtime_info{engine=\"sqlite\"} 1"));
    assert!(rendered.contains("weaver_db_runtime_concurrency 1"));
    assert!(rendered.contains("weaver_db_runtime_in_flight 2"));
    assert!(rendered.contains("weaver_db_runtime_blocked_submissions_total 9"));
    assert!(rendered.contains("weaver_db_op_duration_seconds_count{engine=\"sqlite\"} 6"));

    // Process collector names are deliberately unprefixed.
    assert!(rendered.contains("process_cpu_seconds_total 12.5"));
    assert!(rendered.contains("process_resident_memory_bytes 67108864"));
    assert!(rendered.contains("process_virtual_memory_bytes 536870912"));
    assert!(rendered.contains("process_open_fds 48"));
    assert!(rendered.contains("process_max_fds 1024"));
    assert!(rendered.contains("process_threads 16"));
    assert!(rendered.contains("process_start_time_seconds 1600000000"));
    // The exporter's own start time is a separate, still-supported series.
    assert!(rendered.contains("weaver_start_time_seconds 1700000000"));

    // Disk.
    assert!(rendered.contains(
        "weaver_disk_total_bytes{role=\"complete\",path=\"/var/lib/weaver/complete\"} 2000000"
    ));
    assert!(rendered.contains(
        "weaver_disk_available_bytes{role=\"complete\",path=\"/var/lib/weaver/complete\"} 50000"
    ));

    // HTTP.
    assert!(rendered.contains(
        "weaver_http_requests_total{route=\"/graphql\",method=\"POST\",status=\"200\"} 42"
    ));
    assert!(rendered.contains(
        "weaver_http_requests_total{route=\"/api/login\",method=\"POST\",status=\"401\"} 3"
    ));
    assert!(rendered.contains("weaver_http_request_duration_seconds_count{route=\"/graphql\"} 6"));
}

/// Collection surfaces that have not measured anything must be absent, not
/// zero: "this stage was never timed" and "this stage always took no time" are
/// different facts and must not render identically.
#[test]
fn absent_instrumentation_omits_its_families() {
    let snapshot = populated_metrics_snapshot();
    let block = DownloadBlockState::default();

    // Nothing supplied at all.
    let bare = metrics::render_prometheus_metrics_input(&metrics::PrometheusRenderInput::new(
        &snapshot, &block,
    ));
    assert_valid_prometheus_exposition(&bare);
    for family in [
        "weaver_server_article_attempts_total",
        "weaver_server_article_latency_seconds",
        "weaver_jobs_submitted_total",
        "weaver_job_duration_seconds",
        "weaver_pipeline_disk_write_duration_seconds",
        "weaver_db_runtime_info",
        "process_cpu_seconds_total",
        "process_start_time_seconds",
        "weaver_disk_total_bytes",
        "weaver_http_requests_total",
    ] {
        assert!(!bare.contains(family), "{family} should be absent");
    }

    // Pipeline histograms present, but the two optional stages unmeasured.
    let pipeline = instr::PipelineHistogramsSnapshot {
        disk_write_duration: sample_histogram(),
        decode_task_duration: None,
        extract_member_duration: None,
    };
    // Likewise a process sample where the platform answered nothing.
    let process = instr::ProcessMetricsSnapshot::default();
    let mut input = metrics::PrometheusRenderInput::new(&snapshot, &block);
    input.pipeline_histograms = Some(&pipeline);
    input.process = Some(&process);
    input.start_time_seconds = 1_700_000_000.0;
    let partial = metrics::render_prometheus_metrics_input(&input);
    assert_valid_prometheus_exposition(&partial);

    assert!(partial.contains("weaver_pipeline_disk_write_duration_seconds_bucket"));
    assert!(!partial.contains("weaver_pipeline_decode_task_duration_seconds"));
    assert!(!partial.contains("weaver_pipeline_extract_member_duration_seconds"));

    for family in [
        "process_cpu_seconds_total",
        "process_resident_memory_bytes",
        "process_virtual_memory_bytes",
        "process_open_fds",
        "process_max_fds",
        "process_threads",
    ] {
        assert!(!partial.contains(family), "{family} should be absent");
    }
    // Start time is the one process series with a usable fallback.
    assert!(partial.contains("process_start_time_seconds 1700000000"));
}

/// The catalogue and the renderer must describe the same set of families in
/// both directions: a family in the catalogue that nothing emits is dead
/// documentation, and a family emitted without a catalogue entry has escaped
/// the descriptor discipline entirely.
#[test]
fn metric_catalog_matches_rendered_families() {
    let rendered = fully_populated_render();
    assert_valid_prometheus_exposition(&rendered);

    let catalogued: std::collections::BTreeSet<String> = metrics::catalog::metric_catalog()
        .iter()
        .map(|family| family.name.to_string())
        .collect();
    let emitted = rendered_family_names(&rendered);

    let missing: Vec<&String> = catalogued.difference(&emitted).collect();
    assert!(
        missing.is_empty(),
        "catalogued but never emitted: {missing:?}"
    );
    let uncatalogued: Vec<&String> = emitted.difference(&catalogued).collect();
    assert!(
        uncatalogued.is_empty(),
        "emitted without a catalogue entry: {uncatalogued:?}"
    );
}

/// Print the catalogue as the markdown table `docs/metrics.md` carries.
///
/// Ignored by default because it produces output rather than checking
/// anything; run it with
/// `cargo test -p weaver regenerate_docs_metrics_table -- --ignored --nocapture`
/// and paste the result over the catalogue table when families change.
#[test]
#[ignore = "documentation generator; produces output instead of assertions"]
fn regenerate_docs_metrics_table() {
    println!("| Metric | Type | Labels | Description |");
    println!("| --- | --- | --- | --- |");
    for family in metrics::catalog::metric_catalog() {
        let labels = if family.labels.is_empty() {
            "—".to_string()
        } else {
            family
                .labels
                .iter()
                .map(|label| format!("`{label}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let help = match family.deprecated_by {
            Some(replacement) => {
                format!("{} **Deprecated — use `{replacement}`.**", family.help)
            }
            None => family.help.to_string(),
        };
        println!(
            "| `{}` | {} | {} | {} |",
            family.name,
            family.kind.as_str(),
            labels,
            help
        );
    }
}

/// `docs/metrics.md` is the operator-facing copy of the catalogue. Keeping the
/// two in sync by hand does not survive contact with a busy release, so make
/// the divergence a test failure with the exact edit spelled out.
#[test]
fn docs_metrics_table_matches_catalog() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../docs/metrics.md")
        .canonicalize()
        .expect("docs/metrics.md must exist");
    let doc = std::fs::read_to_string(&path).expect("docs/metrics.md must be readable");

    // The exporter emits in two namespaces: its own `weaver_` families, and the
    // standard unprefixed `process_` collector series. Pin that here so a new
    // namespace cannot slip past the prefix filter below and silently escape
    // the documentation check.
    const METRIC_PREFIXES: [&str; 2] = ["weaver_", "process_"];

    let catalogued: std::collections::BTreeSet<String> = metrics::catalog::metric_catalog()
        .iter()
        .map(|family| family.name.to_string())
        .collect();
    let unexpected_namespace: Vec<&String> = catalogued
        .iter()
        .filter(|name| {
            !METRIC_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .collect();
    assert!(
        unexpected_namespace.is_empty(),
        "catalogue uses a namespace this test cannot recognise: {unexpected_namespace:?}"
    );

    // Catalogue rows are markdown table lines whose first cell is a
    // backtick-quoted metric name. The prefix filter keeps prose tables (the
    // deprecation mapping, for instance) from being read as catalogue rows.
    let documented: std::collections::BTreeSet<String> = doc
        .lines()
        .filter_map(|line| line.trim().strip_prefix("| `"))
        .filter_map(|rest| rest.split_once('`'))
        .map(|(name, _)| name.to_string())
        .filter(|name| {
            METRIC_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .collect();

    let missing: Vec<&String> = catalogued.difference(&documented).collect();
    assert!(
        missing.is_empty(),
        "docs/metrics.md is missing rows for: {missing:?}"
    );
    let extra: Vec<&String> = documented.difference(&catalogued).collect();
    assert!(
        extra.is_empty(),
        "docs/metrics.md documents metrics the exporter cannot emit: {extra:?}"
    );
}

/// The encoder's histogram helper is the surface the pipeline's bucketed
/// latency snapshots will render through, so pin its cumulative-`le` output
/// before anything depends on it.
#[test]
fn encoder_renders_cumulative_histogram_buckets() {
    static SAMPLE_HISTOGRAM: metrics::encode::MetricFamily = metrics::encode::MetricFamily {
        name: "weaver_example_latency_seconds",
        kind: metrics::encode::MetricKind::Histogram,
        labels: &["lane"],
        help: "Example histogram used to pin the encoder's bucket arithmetic.",
        deprecated_by: None,
    };

    let mut encoder = metrics::Encoder::new();
    encoder.histogram(
        &SAMPLE_HISTOGRAM,
        &[("lane", "body")],
        &[0.1, 1.0],
        &[2, 3, 1],
        4.5,
        6,
    );
    let rendered = encoder.finish();
    assert_valid_prometheus_exposition(&rendered);

    // Per-bucket counts 2/3/1 become cumulative 2/5/6.
    assert!(
        rendered.contains("weaver_example_latency_seconds_bucket{lane=\"body\",le=\"0.1\"} 2"),
        "{rendered}"
    );
    assert!(rendered.contains("weaver_example_latency_seconds_bucket{lane=\"body\",le=\"1\"} 5"));
    assert!(
        rendered.contains("weaver_example_latency_seconds_bucket{lane=\"body\",le=\"+Inf\"} 6")
    );
    assert!(rendered.contains("weaver_example_latency_seconds_sum{lane=\"body\"} 4.5"));
    assert!(rendered.contains("weaver_example_latency_seconds_count{lane=\"body\"} 6"));
}

#[tokio::test]
async fn request_decompression_accepts_all_supported_encodings() {
    let app = Router::new()
        .route("/", post(|body: Bytes| async move { body }))
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        );
    let payload = br#"{"query":"query { __typename }"}"#;

    for encoding in ["gzip", "deflate", "br", "zstd"] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_ENCODING, encoding)
                    .body(Body::from(compress_request_body(encoding, payload)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "encoding {encoding}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], payload, "encoding {encoding}");
    }
}

#[tokio::test]
async fn response_compression_supports_deflate() {
    let payload = "deflate-me-please ".repeat(256);
    let app = Router::new()
        .route(
            "/",
            post(move || {
                let payload = payload.clone();
                async move { payload }
            }),
        )
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .deflate(true)
                .br(true)
                .zstd(true),
        );

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/")
                .header(header::ACCEPT_ENCODING, "deflate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok()),
        Some("deflate")
    );
}
