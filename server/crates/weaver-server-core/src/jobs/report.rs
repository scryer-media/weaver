// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job as any root reports it, for the queue and for history. Only what
// every root knows is here: the queue's other figures are the ones an
// article root keeps, and a job whose root keeps none of them reads as whole,
// with nothing lost and nothing waiting.

use crate::history::JobHistoryRow;
use crate::jobs::JobId;
use crate::jobs::handle::JobInfo;
use crate::jobs::model::{JobStatus, Protocol, runtime_lanes_from_status_snapshot};
use crate::jobs::phase_progress::JobPhaseProgress;

// The health of a job that lost nothing.
const WHOLE: u32 = 1000;

#[derive(Debug, Clone, PartialEq)]
pub struct JobReport {
    pub job_id: JobId,
    pub protocol: Protocol,
    pub name: String,
    pub status: JobStatus,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub phase_progress: Vec<JobPhaseProgress>,
    pub category: Option<String>,
    pub output_dir: Option<String>,
    // Why the job waits, while it does.
    pub wait_reason: Option<String>,
    pub created_at_ms: i64,
}

impl JobReport {
    fn error(&self) -> Option<String> {
        match &self.status {
            JobStatus::Failed { error } => Some(error.clone()),
            _ => None,
        }
    }

    // The job as the queue lists it.
    pub fn job_info(self) -> JobInfo {
        let (download_state, post_state, run_state) =
            runtime_lanes_from_status_snapshot(&self.status);
        JobInfo {
            job_id: self.job_id,
            protocol: self.protocol,
            job_hash: None,
            error: self.error(),
            name: self.name,
            status: self.status,
            download_state,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state,
            run_state,
            progress: self.progress,
            total_bytes: self.total_bytes,
            downloaded_bytes: self.downloaded_bytes,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: self.phase_progress,
            failed_bytes: 0,
            health: WHOLE,
            terminal_discards: Vec::new(),
            total_files: 0,
            completed_files: 0,
            remaining_par_files: 0,
            password: None,
            category: self.category,
            metadata: Vec::new(),
            output_dir: self.output_dir,
            server_attribution: Vec::new(),
            download_wait_reason: self.wait_reason,
            download_retry_at_epoch_ms: None,
            created_at_epoch_ms: self.created_at_ms as f64,
        }
    }

    // The job as history keeps it, under its stored status name.
    pub fn history_row(
        &self,
        status: &str,
        job_hash: Option<Vec<u8>>,
        completed_at_ms: i64,
    ) -> JobHistoryRow {
        JobHistoryRow {
            job_id: self.job_id.0,
            job_hash,
            name: self.name.clone(),
            status: status.to_owned(),
            error_message: self.error(),
            total_bytes: self.total_bytes,
            downloaded_bytes: self.downloaded_bytes,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: WHOLE,
            category: self.category.clone(),
            output_dir: self.output_dir.clone(),
            nzb_path: None,
            created_at: self.created_at_ms / 1000,
            completed_at: completed_at_ms / 1000,
            metadata: None,
            server_attribution: None,
            protocol: self.protocol,
        }
    }
}
