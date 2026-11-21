// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The fixtures the owned-lane tests share, moved here with them from the
// lib's owned_lane.rs unchanged.

use super::*;

// Run one lease to its park on a worker that holds no press.
pub(super) fn run_owned_blocking_download_lane(
    cached_lane: &mut Option<CachedOwnedLane>,
    run: OwnedLaneRun,
) {
    run_owned_lane(cached_lane, &mut None, run);
}

pub(super) fn tail_work(segment_number: u32, retry_count: u32) -> DownloadWork {
    DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(42),
                file_index: 0,
            },
            segment_number,
        },
        message_id: MessageId::new(&format!("tail-{segment_number}@example.invalid")),
        groups: Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 3,
        byte_estimate: 1024,
        retry_count,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        avoid_server: None,
    }
}

pub(super) fn test_admitted_state(job_id: JobId) -> crate::jobs::handle::SharedPipelineState {
    let state = crate::jobs::handle::SharedPipelineState::new(
        weaver_server_base::operations::metrics::PipelineMetrics::new(),
        vec![crate::jobs::handle::JobInfo {
            protocol: crate::jobs::Protocol::Nzb,
            job_id,
            job_hash: None,
            name: "lane-fixture".into(),
            status: crate::JobStatus::Downloading,
            download_state: crate::DownloadState::Downloading,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: crate::PostState::Idle,
            run_state: crate::RunState::Active,
            progress: 0.0,
            total_bytes: 1024,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 1,
            completed_files: 0,
            remaining_par_files: 0,
            file_remaining_bytes: Vec::new(),
            file_parked_bytes: Vec::new(),
            remaining_bytes: 0,
            wanted_bytes: 0,
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            server_attribution: Vec::new(),
            disk_need_bytes: None,
            disk_free_bytes: None,
            created_at_epoch_ms: 0.0,
        }],
    );
    state.queue.write(|inner| {
        inner.jobs[0].report.nzb_admitted = true;
        crate::pipeline::queue::Change::changed(())
    });
    state
}

pub(super) fn test_lease(
    job_id: JobId,
    runtime_generation: u64,
    exclude_servers: weaver_nntp::ServerSet,
    works: Vec<DownloadWork>,
) -> DownloadBatchLease {
    DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: 0,
        job_id,
        runtime_generation,
        lane_mode: DownloadLaneMode::Pipelined { depth: 4 },
        server_modes: vec![(0, DownloadLaneMode::Pipelined { depth: 4 })],
        completion_critical: false,
        effective_exclude_servers: exclude_servers,
        dial_exclude_servers: exclude_servers,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
        works,
    }
}

// An idle marker standing for a cached connection on `server`.
//
// The client is a dangling `Weak`, which is what an idle marker looks like
// once its client has been retired: it can serve nothing, so routing skips it.
pub(super) fn test_idle_lane(server: usize) -> IdleOwnedLane {
    IdleOwnedLane {
        nntp: std::sync::Weak::new(),
        server: weaver_nntp::pool::ServerId(server),
    }
}
