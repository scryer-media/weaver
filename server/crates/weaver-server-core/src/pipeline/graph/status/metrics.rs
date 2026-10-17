// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's metric family, beside the classic `direct_store.*`
// names and aliasing none of them: `graph.<what>.<reason>[.<object kind>]`.

use weaver_graph::recovery::RepairOutcome;

use super::claims::{Claim, ClaimedMember};
use super::summary::RecoverySummary;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::runtime::perf_probe::record_value_owned;

pub(super) fn phase(phase: RootPhase) {
    record_value_owned(format!("graph.phase.{}", phase.as_str()), 1);
}

// One count per recovery object's outcome, keyed by the object's kind.
pub(super) fn repaired(summary: &RecoverySummary, outcomes: &[RepairOutcome]) {
    for (set, outcome) in summary.sets.iter().zip(outcomes) {
        let reason = match outcome {
            RepairOutcome::Clean => "clean",
            RepairOutcome::Repaired { .. } => "repaired",
            RepairOutcome::Impossible { .. } => "impossible",
            RepairOutcome::Waiting => "waiting",
        };
        record_value_owned(format!("graph.repair.{reason}.{}", set.kind), 1);
    }
}

pub(super) fn claimed(census: &[ClaimedMember]) {
    for row in census {
        let reason = match row.claim {
            Claim::Complete => "complete",
            Claim::Damaged { .. } => "damaged",
            Claim::Missing { .. } => "missing",
        };
        record_value_owned(format!("graph.claim.{reason}"), 1);
    }
}

pub(super) fn failed(reason: &str) {
    record_value_owned(format!("graph.failed.{reason}"), 1);
}
