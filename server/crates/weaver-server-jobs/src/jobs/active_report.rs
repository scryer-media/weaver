// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::control::JobInfo;
use serde::Serialize;

// Where a queued job stands for the reports the diagnostics package carries.
// Only jobs Weaver has started work on qualify: a job that is merely queued,
// waiting on its queue scripts, or paused does not, and neither does one that
// finished well and is on its way to history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveReportGroup {
    // Verifying, repairing, extracting, moving or post-processing, or
    // waiting in line for one of them. Listed first.
    PostProcessing,
    // Failed and still in the queue.
    Failed,
    // Downloading, or checking article availability before it does.
    Downloading,
}

impl ActiveReportGroup {
    pub fn of(status: &super::model::JobStatus) -> Option<Self> {
        use super::model::JobStatus;
        if status.past_download() {
            Some(Self::PostProcessing)
        } else {
            match status {
                JobStatus::Failed { .. } => Some(Self::Failed),
                JobStatus::Downloading | JobStatus::Checking => Some(Self::Downloading),
                _ => None,
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostProcessing => "post_processing",
            Self::Failed => "failed",
            Self::Downloading => "downloading",
        }
    }
}

// The jobs chosen for reports, in report order, and how many qualified
// before the cap.
#[derive(Debug, Clone)]
pub struct ActiveReportJobs {
    pub qualified: usize,
    pub jobs: Vec<(ActiveReportGroup, JobInfo)>,
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
