// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::{
    JobInfo, JobStatus, job_status_from_persisted_str, runtime_lanes_from_status_snapshot,
};

// A history row as the queue shows a finished job: for a test of a
// restore, and for a torrent a removal left retiring, written at a start.
pub fn job_info_from_history(row: crate::JobHistoryRow) -> JobInfo {
    let status = job_status_from_persisted_str(&row.status, row.error_message.as_deref());
    let (download_state, post_state, run_state) = runtime_lanes_from_status_snapshot(&status);

    JobInfo {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: weaver_server_base::jobs::ids::JobId(row.job_id),
        job_hash: row.job_hash.as_ref().and_then(|value| {
            (value.len() == 32).then(|| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(value);
                hash
            })
        }),
        name: row.name,
        status: status.clone(),
        download_state,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state,
        run_state,
        progress: 1.0,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: row.failed_bytes,
        health: row.health,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
        password: None,
        category: row.category,
        metadata: row
            .metadata
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        output_dir: row.output_dir,
        server_attribution: crate::jobs::server_attribution::contributions_from_storage(
            row.server_attribution.as_deref(),
        ),
        error: if let JobStatus::Failed { error } = &status {
            Some(error.clone())
        } else {
            None
        },
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        disk_need_bytes: None,
        disk_free_bytes: None,
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}
