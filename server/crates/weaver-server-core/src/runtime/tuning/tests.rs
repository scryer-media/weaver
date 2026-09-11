use crate::runtime::system_profile::*;
use crate::{DownloadPressureReason, DownloadPressureState};

use super::*;

fn ssd_profile(cores: usize) -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 2000.0,
            random_read_iops: 50000.0,
            same_filesystem: true,
        },
    }
}

fn hdd_profile(cores: usize) -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Hdd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 150.0,
            random_read_iops: 200.0,
            same_filesystem: true,
        },
    }
}

fn empty_metrics() -> MetricsSnapshot {
    MetricsSnapshot {
        bytes_downloaded: 0,
        bytes_decoded: 0,
        bytes_committed: 0,
        download_queue_depth: 0,
        active_downloads: 0,
        active_decodes: 0,
        decode_pending: 0,
        decode_pending_bytes: 0,
        decode_active_bytes: 0,
        commit_pending: 0,
        write_pending_bytes: 0,
        write_buffered_bytes: 0,
        write_buffered_segments: 0,
        uu_spooled_bytes: 0,
        uu_spooled_segments: 0,
        direct_write_evictions: 0,
        direct_sets_admitted: 0,
        direct_sets_demoted: 0,
        direct_sets_finalized_direct: 0,
        direct_sets_repaired_while_direct: 0,
        deobfuscated_members_renamed: 0,
        decode_pressure_soft_limit_bytes: 0,
        decode_pressure_hard_limit_bytes: 0,
        write_pressure_soft_limit_bytes: 0,
        write_pressure_hard_limit_bytes: 0,
        download_pressure_state: DownloadPressureState::Clear,
        download_pressure_reason: DownloadPressureReason::None,
        download_pressure_stalls_total: 0,
        download_pressure_stall_duration_ms: 0,
        download_pressure_current_stall_ms: 0,
        download_restart_durable_lead_blocked_total: 0,
        hot_dispatch_job_id: 0,
        hot_dispatch_mode: crate::DispatchShareMode::Exclusive,
        hot_dispatch_underfill_ms: 0,
        hot_dispatch_lent_connections: 0,
        hot_dispatch_last_spillover_decision: crate::SpilloverDecision::None,
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
    }
}

/// Standard test connection limit (mimics a typical config).
const TEST_CONNECTIONS: usize = 20;

#[test]
fn initial_params_ssd() {
    let tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // SSD uses all connections
    assert_eq!(p.max_write_queue, 40);
    assert_eq!(p.min_free_buffers, 4);
    assert_eq!(p.decode_thread_count, 8);
}

#[test]
fn initial_params_hdd() {
    let tuner = RuntimeTuner::with_connection_limit(hdd_profile(8), TEST_CONNECTIONS);
    let p = tuner.params();
    assert_eq!(p.max_concurrent_downloads, 20); // all configured connections
    assert_eq!(p.max_write_queue, 40);
    assert_eq!(p.decode_thread_count, 8);
}

/// Hard byte pressure never moves the connection count. Pressure is flow
/// control on where decoded bytes go, not on how many articles are asked for.
#[test]
fn hard_pressure_never_reduces_downloads() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(4), TEST_CONNECTIONS);
    let mut m = empty_metrics();
    m.download_pressure_state = DownloadPressureState::Hard;
    for _ in 0..50 {
        tuner.observe(&m);
    }
    assert_eq!(tuner.params().max_concurrent_downloads, TEST_CONNECTIONS);
}

#[test]
fn parse_max_concurrent_extractions_override_accepts_positive_values() {
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("1")),
        Some(1)
    );
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("6")),
        Some(6)
    );
}

#[test]
fn parse_max_concurrent_extractions_override_rejects_invalid_values() {
    assert_eq!(parse_max_concurrent_extractions_override(None), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("")), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("0")), None);
    assert_eq!(parse_max_concurrent_extractions_override(Some("-1")), None);
    assert_eq!(
        parse_max_concurrent_extractions_override(Some("nope")),
        None
    );
}

#[test]
fn max_concurrent_extractions_honors_override() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
    tuner.max_concurrent_extractions_override = Some(1);
    assert_eq!(tuner.max_concurrent_extractions(), 1);
}

/// No bandwidth band holds a connection back for recovery work.
///
/// Recovery blocks ride the job's own lanes as completion-critical work, so a
/// reserved slot could only ever be an idle connection while the work it was
/// reserved for sat parked. Every band must leave the full configured budget
/// available to ordinary downloads.
#[test]
fn no_bandwidth_band_reserves_connections_for_recovery() {
    for speed_mb_s in [5u64, 20, 60] {
        let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), TEST_CONNECTIONS);
        let mut m = empty_metrics();
        m.current_download_speed = speed_mb_s * 1024 * 1024;

        for _ in 0..10 {
            tuner.observe(&m);
        }

        assert_eq!(
            tuner.params().max_concurrent_downloads,
            TEST_CONNECTIONS,
            "{speed_mb_s} MB/s must leave every configured connection to downloads"
        );
        assert!(tuner.bandwidth_ema() > 0.0);
    }
}

#[test]
fn set_connection_limit_increases_capacity() {
    // Start with 0 connections (no servers configured).
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 0);
    assert_eq!(tuner.params().max_concurrent_downloads, 0);

    // Add a server with 20 connections.
    tuner.set_connection_limit(20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20); // SSD uses all
}

#[test]
fn set_connection_limit_decreases_capacity() {
    let mut tuner = RuntimeTuner::with_connection_limit(ssd_profile(8), 20);
    assert_eq!(tuner.params().max_concurrent_downloads, 20);

    // Remove a server, now only 5 connections.
    tuner.set_connection_limit(5);
    assert_eq!(tuner.params().max_concurrent_downloads, 5);
}

#[test]
fn random_read_iops_update_changes_future_extraction_admission_limit() {
    let mut profile = hdd_profile(4);
    profile.disk.random_read_iops = 0.0;
    let mut tuner = RuntimeTuner::with_connection_limit(profile, 8);
    assert_eq!(tuner.max_concurrent_extractions(), 1);

    tuner.set_random_read_iops(1_500.0);
    assert_eq!(tuner.max_concurrent_extractions(), 4);

    tuner.set_random_read_iops(200.0);
    assert_eq!(tuner.max_concurrent_extractions(), 1);
}
