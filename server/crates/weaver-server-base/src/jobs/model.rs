// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostState {
    AwaitingQueueScripts,
    Idle,
    QueuedRepair,
    Repairing,
    QueuedExtract,
    Extracting,
    WaitingForVolumes,
    AwaitingRepair,
    Verifying,
    Finalizing,
    QueuedPostProcessing,
    PostProcessing,
    Completed,
    Failed,
}

impl PostState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingQueueScripts => "awaiting_queue_scripts",
            Self::Idle => "idle",
            Self::QueuedRepair => "queued_repair",
            Self::Repairing => "repairing",
            Self::QueuedExtract => "queued_extract",
            Self::Extracting => "extracting",
            Self::WaitingForVolumes => "waiting_for_volumes",
            Self::AwaitingRepair => "awaiting_repair",
            Self::Verifying => "verifying",
            Self::Finalizing => "finalizing",
            Self::QueuedPostProcessing => "queued_post_processing",
            Self::PostProcessing => "post_processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "awaiting_queue_scripts" => Some(Self::AwaitingQueueScripts),
            "idle" => Some(Self::Idle),
            "queued_repair" => Some(Self::QueuedRepair),
            "repairing" => Some(Self::Repairing),
            "queued_extract" => Some(Self::QueuedExtract),
            "extracting" => Some(Self::Extracting),
            "waiting_for_volumes" => Some(Self::WaitingForVolumes),
            "awaiting_repair" => Some(Self::AwaitingRepair),
            "verifying" => Some(Self::Verifying),
            "finalizing" => Some(Self::Finalizing),
            "queued_post_processing" => Some(Self::QueuedPostProcessing),
            "post_processing" => Some(Self::PostProcessing),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}
