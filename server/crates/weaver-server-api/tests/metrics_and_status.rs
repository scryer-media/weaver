mod common;

use common::{TestHarness, assert_no_errors, response_data};
use weaver_server_core::{
    DownloadState, JobId, JobInfo, JobStatus, MetricsSnapshot, PostState, RunState,
};

fn now_epoch_sec() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn history_snapshot(speed: u64, bytes_downloaded: u64) -> MetricsSnapshot {
    MetricsSnapshot {
        bytes_downloaded,
        bytes_decoded: bytes_downloaded / 2,
        bytes_committed: bytes_downloaded / 3,
        download_queue_depth: 1,
        active_downloads: 0,
        active_decodes: 0,
        decode_pending: 0,
        decode_pending_bytes: 0,
        decode_active_bytes: 0,
        commit_pending: 0,
        write_buffered_bytes: speed * 2,
        write_buffered_segments: 1,
        direct_write_evictions: 0,
        decode_pressure_soft_limit_bytes: 0,
        decode_pressure_hard_limit_bytes: 0,
        write_pressure_soft_limit_bytes: 0,
        write_pressure_hard_limit_bytes: 0,
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
        hot_dispatch_warmup_complete: false,
        hot_dispatch_last_spillover_decision: weaver_server_core::SpilloverDecision::None,
        hot_dispatch_spillover_blocked_warmup_total: 0,
        hot_dispatch_spillover_blocked_pressure_total: 0,
        hot_dispatch_spillover_blocked_near_cap_total: 0,
        hot_dispatch_spillover_blocked_hot_can_use_capacity_total: 0,
        hot_dispatch_spillover_blocked_best_mode_pending_total: 0,
        hot_dispatch_spillover_blocked_recent_expansion_helped_total: 0,
        hot_dispatch_spillover_blocked_cap_speed_total: 0,
        hot_dispatch_spillover_allowed_underfill_total: 0,
        hot_dispatch_spillover_allowed_measured_underfill_total: 0,
        hot_dispatch_spillover_allowed_bounded_same_band_total: 0,
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
        download_lane_parks_server_tier_changed_total: 0,
        download_lane_parks_proof_failure_total: 0,
        download_lane_parks_error_total: 0,
        download_lane_lease_items_total: 0,
        download_lane_refill_granted_total: 0,
        download_lane_refill_parked_total: 0,
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
        segments_downloaded: bytes_downloaded / 100,
        segments_decoded: bytes_downloaded / 200,
        segments_committed: bytes_downloaded / 300,
        articles_not_found: 0,
        decode_errors: 0,
        verify_active: 0,
        repair_active: 0,
        extract_active: 0,
        disk_write_latency_us: 250,
        segments_retried: 0,
        segments_failed_permanent: 0,
        download_failures_article_not_found: 0,
        download_failures_capacity_unavailable: 0,
        download_failures_transient: 0,
        download_failures_auth: 0,
        download_failures_permanent: 0,
        current_download_speed: speed,
        crc_errors: 0,
        recovery_queue_depth: 0,
        articles_per_sec: 3.0,
        decode_rate_mbps: 1.5,
    }
}

fn history_job(job_id: u64, status: JobStatus) -> JobInfo {
    JobInfo {
        job_id: JobId(job_id),
        job_hash: None,
        name: format!("job-{job_id}"),
        status,
        download_state: DownloadState::Queued,
        post_state: PostState::Idle,
        run_state: RunState::Active,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        created_at_epoch_ms: 0.0,
    }
}

#[tokio::test]
async fn version_returns_nonempty_string() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ version }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let version = data["version"].as_str().unwrap();
    assert!(!version.is_empty());
}

#[tokio::test]
async fn metrics_returns_default_counters() {
    let h = TestHarness::new().await;
    let resp = h
        .execute("{ metrics { bytesDownloaded currentDownloadSpeed } }")
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let metrics = &data["metrics"];
    assert!(metrics["bytesDownloaded"].is_number());
}

#[tokio::test]
async fn is_paused_false_by_default() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn is_paused_true_after_pause_all() {
    let h = TestHarness::new().await;
    let resp = h.execute("mutation { pauseAll }").await;
    assert_no_errors(&resp);

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn is_paused_false_after_resume_all() {
    let h = TestHarness::new().await;
    h.execute("mutation { pauseAll }").await;
    h.execute("mutation { resumeAll }").await;

    let resp = h.execute("{ isPaused }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(!data["isPaused"].as_bool().unwrap());
}

#[tokio::test]
async fn download_block_idle_by_default() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ downloadBlock { kind } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let kind = data["downloadBlock"]["kind"].as_str().unwrap();
    assert_eq!(kind, "NONE");
}

#[tokio::test]
async fn download_block_queryable_after_pause() {
    let h = TestHarness::new().await;
    h.execute("mutation { pauseAll }").await;

    let resp = h.execute("{ downloadBlock { kind capEnabled } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    // The mock scheduler's pauseAll only updates the paused flag, not the download block.
    // This test verifies the query is valid and returns correct types.
    assert!(data["downloadBlock"]["kind"].is_string());
    assert!(data["downloadBlock"]["capEnabled"].is_boolean());
}

#[tokio::test]
async fn metrics_has_expected_fields() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            "{ metrics { bytesDownloaded bytesDecoded bytesCommitted downloadQueueDepth segmentsDownloaded currentDownloadSpeed hotDispatchJobId hotDispatchMode hotDispatchUnderfillMs hotDispatchLentConnections hotDispatchWarmupComplete hotDispatchLastSpilloverDecision hotDispatchSpilloverBlockedWarmupTotal hotDispatchSpilloverAllowedUnderfillTotal downloadLanesActive downloadLanesDepth2Active downloadLanesIssuingActive downloadLanesAwaitingWorkActive downloadLaneParksNoWorkTotal downloadPipelineProofPassTotal downloadPipelineReplayItemsTotal } }",
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let m = &data["metrics"];
    assert!(m["bytesDownloaded"].is_number());
    assert!(m["bytesDecoded"].is_number());
    assert!(m["downloadQueueDepth"].is_number());
    assert!(m["currentDownloadSpeed"].is_number());
    assert!(m["hotDispatchJobId"].is_number());
    assert!(m["hotDispatchMode"].is_string());
    assert!(m["hotDispatchUnderfillMs"].is_number());
    assert!(m["hotDispatchLentConnections"].is_number());
    assert!(m["hotDispatchWarmupComplete"].is_boolean());
    assert!(m["hotDispatchLastSpilloverDecision"].is_string());
    assert!(m["hotDispatchSpilloverBlockedWarmupTotal"].is_number());
    assert!(m["hotDispatchSpilloverAllowedUnderfillTotal"].is_number());
    assert!(m["downloadLanesActive"].is_number());
    assert!(m["downloadLanesDepth2Active"].is_number());
    assert!(m["downloadLanesIssuingActive"].is_number());
    assert!(m["downloadLanesAwaitingWorkActive"].is_number());
    assert!(m["downloadLaneParksNoWorkTotal"].is_number());
    assert!(m["downloadPipelineProofPassTotal"].is_number());
    assert!(m["downloadPipelineReplayItemsTotal"].is_number());
}

#[tokio::test]
async fn metrics_history_returns_labeled_series() {
    let h = TestHarness::new().await;
    let now = now_epoch_sec();
    h.db.record_metrics_history_sample(
        now - 20,
        &history_snapshot(128, 1_000),
        &[
            history_job(1, JobStatus::Queued),
            history_job(2, JobStatus::Queued),
        ],
    )
    .unwrap();
    h.db.record_metrics_history_sample(
        now - 10,
        &history_snapshot(256, 1_600),
        &[
            history_job(1, JobStatus::Queued),
            history_job(2, JobStatus::Queued),
            history_job(3, JobStatus::Queued),
            history_job(4, JobStatus::Downloading),
        ],
    )
    .unwrap();

    let resp = h
        .execute(
            r#"{
                metricsHistory(
                    range: ONE_HOUR
                ) {
                    timestamps
                    resolutionSec
                    series {
                        metric
                        variant
                        labels {
                            key
                            value
                        }
                        values
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["metricsHistory"];
    assert_eq!(result["timestamps"].as_array().unwrap().len(), 2);
    assert_eq!(result["resolutionSec"].as_i64().unwrap(), 10);
    let series = result["series"].as_array().unwrap();
    assert!(series.len() > 3);
    let queued = series
        .iter()
        .find(|entry| {
            entry["metric"].as_str() == Some("weaver_pipeline_jobs")
                && entry["variant"].as_str() == Some("ACTUAL")
                && entry["labels"][0]["value"].as_str() == Some("queued")
        })
        .unwrap();
    assert_eq!(queued["values"][0].as_f64().unwrap(), 2.0);
    assert_eq!(queued["values"][1].as_f64().unwrap(), 3.0);
    let downloading = series
        .iter()
        .find(|entry| {
            entry["metric"].as_str() == Some("weaver_pipeline_jobs")
                && entry["variant"].as_str() == Some("ACTUAL")
                && entry["labels"][0]["value"].as_str() == Some("downloading")
        })
        .unwrap();
    assert_eq!(downloading["values"][0].as_f64().unwrap(), 0.0);
    assert_eq!(downloading["values"][1].as_f64().unwrap(), 1.0);
    let speed = series
        .iter()
        .find(|entry| {
            entry["metric"].as_str()
                == Some("weaver_pipeline_current_download_speed_bytes_per_second")
                && entry["variant"].as_str() == Some("ACTUAL")
        })
        .unwrap();
    assert_eq!(speed["values"][0].as_f64().unwrap(), 128.0);
    assert_eq!(speed["values"][1].as_f64().unwrap(), 256.0);
}

#[tokio::test]
async fn metrics_history_uses_rollup_variants_for_24_hours() {
    let h = TestHarness::new().await;
    let now = now_epoch_sec();
    let closed_bucket_end = (now / 300) * 300;
    h.db.record_metrics_history_sample(
        closed_bucket_end - 240,
        &history_snapshot(64, 500),
        &[history_job(1, JobStatus::Queued)],
    )
    .unwrap();
    h.db.record_metrics_history_sample(
        closed_bucket_end - 60,
        &history_snapshot(128, 1_500),
        &[
            history_job(1, JobStatus::Queued),
            history_job(2, JobStatus::Downloading),
        ],
    )
    .unwrap();

    let resp = h
        .execute(
            r#"{
                metricsHistory(
                    range: TWENTY_FOUR_HOURS
                ) {
                    timestamps
                    resolutionSec
                    series {
                        metric
                        variant
                        values
                    }
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let result = &data["metricsHistory"];
    assert_eq!(result["resolutionSec"].as_i64().unwrap(), 300);
    assert_eq!(result["timestamps"].as_array().unwrap().len(), 1);
    let avg_series = result["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| {
            entry["metric"].as_str()
                == Some("weaver_pipeline_current_download_speed_bytes_per_second")
                && entry["variant"].as_str() == Some("AVG")
        })
        .unwrap();
    let peak_series = result["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| {
            entry["metric"].as_str()
                == Some("weaver_pipeline_current_download_speed_bytes_per_second")
                && entry["variant"].as_str() == Some("PEAK")
        })
        .unwrap();
    assert_eq!(avg_series["values"][0].as_f64().unwrap(), 96.0);
    assert_eq!(peak_series["values"][0].as_f64().unwrap(), 128.0);
}
