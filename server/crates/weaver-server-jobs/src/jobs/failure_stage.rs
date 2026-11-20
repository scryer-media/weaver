// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use serde::{Deserialize, Serialize};

// Typed built-in pipeline failure stage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineFailureStage {
    Download,
    Verify,
    Repair,
    Extract,
    Move,
}
