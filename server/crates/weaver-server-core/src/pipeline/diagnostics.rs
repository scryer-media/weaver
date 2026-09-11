//! A read-only snapshot of pipeline state that no other surface exposes.
//!
//! Everything here is copied out of the pipeline actor while it is blocked on
//! the reply channel, so the rules are the same as for any other command
//! handler: read fields, allocate bounded amounts, and never touch disk or the
//! network. Nothing in this module may mutate pipeline state — a diagnostics
//! read that changes what it is measuring is worse than no diagnostics at all.
//!
//! The snapshot exists because the fields that explain a stalled queue — the
//! pressure latches, the deferred refill backlog, the per-job queue lengths —
//! live inside the actor and are reachable nowhere else. The counters that are
//! already published as metrics are folded in too, so one artefact answers the
//! whole question rather than needing to be read alongside a scrape.

use serde::{Deserialize, Serialize};

use super::Pipeline;
use crate::jobs::JobStatus;
use crate::runtime::tuning::TunedParameters;

/// Everything the diagnostics package records about the running pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDiagnostics {
    pub captured_at_epoch_ms: f64,
    pub tuner: TunerDiagnostics,
    pub pressure: PressureDiagnostics,
    pub global_paused: bool,
    pub scheduled_pause: bool,
    pub active_downloads: usize,
    pub active_download_connections: usize,
    pub active_completion_critical_connections: usize,
    pub active_recovery: usize,
    pub hot_dispatch_job: Option<u64>,
    pub nntp_handoff_draining: bool,
    pub pool_generation: u64,
    pub jobs: Vec<JobPipelineDiagnostics>,
    pub direct_store: Vec<DirectStoreJobDiagnostics>,
    pub pool: Vec<PoolServerDiagnostics>,
    pub lanes: LaneDiagnostics,
}

/// Tuner-owned concurrency parameters and the connection ceiling they respect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunerDiagnostics {
    #[serde(flatten)]
    pub params: TunedParameters,
    /// Sum of the configured connections across every active server, as the
    /// last NNTP activation reported it.
    pub configured_connections: Option<usize>,
    /// Size of the owned download lane pool.
    pub owned_lane_workers: usize,
}

/// Byte-pressure limits and the latches they drive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureDiagnostics {
    pub write_buf_max_pending: usize,
    pub decode_backlog_budget_bytes: usize,
    pub write_backlog_budget_bytes: usize,
    pub download_decode_hard_pressure_latched: bool,
    pub download_write_hard_pressure_latched: bool,
    /// How long downloads have been blocked by hard pressure, if they are.
    pub hard_stall_seconds: Option<f64>,
    pub write_buffered_bytes: usize,
    pub write_buffered_segments: usize,
    pub write_buffers: usize,
    pub decode_pending: usize,
    pub decode_pending_bytes: u64,
    pub decode_active_bytes: u64,
    pub write_pending_bytes: u64,
    pub decode_pressure_soft_limit_bytes: u64,
    pub decode_pressure_hard_limit_bytes: u64,
    pub write_pressure_soft_limit_bytes: u64,
    pub write_pressure_hard_limit_bytes: u64,
    pub uu_spooled_bytes: usize,
    pub uu_spooled_segments: usize,
    pub uu_parked_segments: usize,
}

/// Per-job queue depths and in-flight work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPipelineDiagnostics {
    pub job_id: u64,
    pub status: JobStatus,
    pub download_queue_len: usize,
    pub recovery_queue_len: usize,
    pub held_segments: usize,
    pub active_downloads: usize,
    pub active_download_connections: usize,
    pub active_decodes: usize,
    pub pending_retries: usize,
    pub pending_released_results: usize,
    pub pending_released_result_bytes: u64,
    pub in_download_pass: bool,
    pub finalizing_download: bool,
    pub download_wait_reason: Option<String>,
    pub download_wait_pending: Option<usize>,
}

/// Direct-store admission state for one job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectStoreJobDiagnostics {
    pub job_id: u64,
    #[serde(flatten)]
    pub counts: DirectSetCounts,
    /// Reconstruction sweeps this job's demoted sets still have in flight.
    pub demotion_sweeps_in_flight: usize,
}

/// How the sets admitted for one job are currently split.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DirectSetCounts {
    pub total: usize,
    /// Sets still routing bytes directly into their members.
    pub admitted: usize,
    pub demoted: usize,
    pub finalized: usize,
}

/// Connection leases held against one configured server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolServerDiagnostics {
    pub server_index: usize,
    pub address: String,
    pub configured_connections: usize,
    pub available_permits: usize,
    pub leased_connections: usize,
    pub over_limit: bool,
}

/// Download lane occupancy, taken from the published lane counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneDiagnostics {
    pub active: usize,
    pub sequential: usize,
    pub pipeline_depth2: usize,
    pub pipeline_depth4: usize,
    pub pipeline_depth8: usize,
    pub idle: usize,
    pub awaiting_work: usize,
    pub binding_server: usize,
    pub acquired: usize,
    pub issuing: usize,
    pub draining: usize,
    pub yield_after_batch: usize,
    pub parking: usize,
    pub recovering: usize,
}

impl Pipeline {
    /// Copy the read-only diagnostics snapshot out of the actor.
    ///
    /// `&self`, deliberately: the command handler must not be able to mutate
    /// anything on the way to answering.
    pub(crate) fn diagnostics_snapshot(&self) -> PipelineDiagnostics {
        let metrics = self.metrics.snapshot();
        let now = std::time::Instant::now();

        let mut jobs: Vec<JobPipelineDiagnostics> = self
            .jobs
            .iter()
            .map(|(job_id, state)| {
                let wait = self.download_wait_by_job.get(job_id);
                JobPipelineDiagnostics {
                    job_id: job_id.0,
                    status: state.status.clone(),
                    download_queue_len: state.download_queue.len(),
                    recovery_queue_len: state.recovery_queue.len(),
                    held_segments: state.held_segments.len(),
                    active_downloads: self
                        .active_downloads_by_job
                        .get(job_id)
                        .copied()
                        .unwrap_or(0),
                    active_download_connections: self
                        .active_download_connections_by_job
                        .get(job_id)
                        .copied()
                        .unwrap_or(0),
                    active_decodes: self.active_decodes_by_job.get(job_id).copied().unwrap_or(0),
                    pending_retries: self.pending_retries_by_job.get(job_id).copied().unwrap_or(0),
                    pending_released_results: self
                        .pending_released_download_results_by_job
                        .get(job_id)
                        .copied()
                        .unwrap_or(0),
                    pending_released_result_bytes: self
                        .pending_released_download_result_bytes_by_job
                        .get(job_id)
                        .copied()
                        .unwrap_or(0),
                    in_download_pass: self.active_download_passes.contains(job_id),
                    finalizing_download: self.jobs_finalizing_download.contains(job_id),
                    download_wait_reason: wait.map(|wait| wait.reason.to_string()),
                    download_wait_pending: wait.map(|wait| wait.pending_count),
                }
            })
            .collect();
        jobs.sort_by_key(|job| job.job_id);

        let direct_store = self
            .direct_store
            .set_counts_by_job()
            .into_iter()
            .map(|(job_id, counts)| DirectStoreJobDiagnostics {
                job_id: job_id.0,
                counts,
                demotion_sweeps_in_flight: self
                    .direct_demotion_in_flight
                    .get(&job_id)
                    .map(|sweeps| sweeps.len())
                    .unwrap_or(0),
            })
            .collect();

        let pool = self
            .shared_state
            .nntp_pool()
            .map(|pool| {
                (0..pool.server_count())
                    .map(|index| {
                        let (available_permits, configured_connections) = pool.server_load(index);
                        PoolServerDiagnostics {
                            server_index: index,
                            address: pool.server_address(weaver_nntp::ServerId(index)),
                            configured_connections,
                            available_permits,
                            leased_connections: configured_connections
                                .saturating_sub(available_permits),
                            over_limit: pool.is_over_limit(weaver_nntp::ServerId(index)),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        PipelineDiagnostics {
            captured_at_epoch_ms: crate::jobs::epoch_ms_now(),
            tuner: TunerDiagnostics {
                params: self.tuner.params().clone(),
                configured_connections: self
                    .shared_state
                    .nntp_runtime_activation()
                    .map(|activation| activation.configured_connections),
                owned_lane_workers: self.owned_download_lane_pool.worker_count(),
            },
            pressure: PressureDiagnostics {
                write_buf_max_pending: self.write_buf_max_pending,
                decode_backlog_budget_bytes: self.decode_backlog_budget_bytes,
                write_backlog_budget_bytes: self.write_backlog_budget_bytes,
                download_decode_hard_pressure_latched: self
                    .download_decode_hard_pressure_latched,
                download_write_hard_pressure_latched: self.download_write_hard_pressure_latched,
                hard_stall_seconds: self
                    .download_pressure_hard_stall_started_at
                    .map(|started| now.saturating_duration_since(started).as_secs_f64()),
                write_buffered_bytes: self.write_buffered_bytes,
                write_buffered_segments: self.write_buffered_segments,
                write_buffers: self.write_buffers.len(),
                decode_pending: metrics.decode_pending,
                decode_pending_bytes: metrics.decode_pending_bytes,
                decode_active_bytes: metrics.decode_active_bytes,
                write_pending_bytes: metrics.write_pending_bytes,
                decode_pressure_soft_limit_bytes: metrics.decode_pressure_soft_limit_bytes,
                decode_pressure_hard_limit_bytes: metrics.decode_pressure_hard_limit_bytes,
                write_pressure_soft_limit_bytes: metrics.write_pressure_soft_limit_bytes,
                write_pressure_hard_limit_bytes: metrics.write_pressure_hard_limit_bytes,
                uu_spooled_bytes: self.uu_spooled_bytes,
                uu_spooled_segments: self.uu_spooled_segments,
                uu_parked_segments: self.uu_parked_segments,
            },
            global_paused: self.global_paused,
            scheduled_pause: self.scheduled_pause,
            active_downloads: self.active_downloads,
            active_download_connections: self.active_download_connections,
            active_completion_critical_connections: self.active_completion_critical_connections,
            active_recovery: self.active_recovery,
            hot_dispatch_job: self.hot_dispatch_job.map(|job_id| job_id.0),
            nntp_handoff_draining: self.nntp_handoff_draining,
            pool_generation: self.pool_generation,
            jobs,
            direct_store,
            pool,
            lanes: LaneDiagnostics {
                active: metrics.download_lanes_active,
                sequential: metrics.download_lanes_sequential_active,
                pipeline_depth2: metrics.download_lanes_depth2_active,
                pipeline_depth4: metrics.download_lanes_depth4_active,
                pipeline_depth8: metrics.download_lanes_depth8_active,
                idle: metrics.download_lanes_idle_active,
                awaiting_work: metrics.download_lanes_awaiting_work_active,
                binding_server: metrics.download_lanes_binding_server_active,
                acquired: metrics.download_lanes_acquired_active,
                issuing: metrics.download_lanes_issuing_active,
                draining: metrics.download_lanes_draining_active,
                yield_after_batch: metrics.download_lanes_yield_after_batch_active,
                parking: metrics.download_lanes_parking_active,
                recovering: metrics.download_lanes_recovering_active,
            },
        }
    }
}
