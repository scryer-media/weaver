// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a namespace's recovery objects can say about its members, folded into
// numbers: per object, the blocks it can still reach, the slices it knows
// are damaged, and what its last repair came to. Health, the repair decision
// and the script-facing PAR status all read this one record, so they cannot
// disagree.

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Kind, NodeId};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    pub(crate) sets: Vec<SetSummary>,
    // Members that carry recovery data nothing here reads, by name: it is
    // there, and no repair counts on it.
    pub(crate) unusable: Vec<String>,
}

// One recovery object of the namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SetSummary {
    pub(crate) kind: Kind,
    // The namespace it lives in, at whatever depth.
    pub(crate) namespace: NodeId,
    // What the object is called where it lives, for the operator.
    pub(crate) name: String,
    pub(crate) blocks_reachable: u64,
    pub(crate) damaged_slices: u64,
    // `None` before it ran a repair.
    pub(crate) outcome: Option<RepairOutcome>,
}

impl SetSummary {
    // Its last repair found the members whole or made them so.
    pub(crate) fn vouched(&self) -> bool {
        vouches(self.outcome)
    }

    // More is damaged than it can reach blocks to rebuild.
    pub(crate) fn short(&self) -> bool {
        self.damaged_slices > self.blocks_reachable
    }
}

pub(crate) fn vouches(outcome: Option<RepairOutcome>) -> bool {
    matches!(
        outcome,
        Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
    )
}

impl RecoverySummary {
    pub(crate) fn blocks_reachable(&self) -> u64 {
        self.sets.iter().map(|set| set.blocks_reachable).sum()
    }

    // Nothing is left for a repair pass to say: every object vouched, or
    // there is no object to ask.
    pub(crate) fn settled(&self) -> bool {
        self.sets.iter().all(SetSummary::vouched)
    }

    // Recovery exists and every object could rebuild what it has found
    // damaged. Spare blocks in one object never repair another's members,
    // so each is judged alone.
    pub(crate) fn repairs_all(&self) -> bool {
        !self.sets.is_empty() && !self.sets.iter().any(SetSummary::short)
    }

    // Health's question: whether recovery can still arrive. No object says
    // nothing either way, as with a job that declared no recovery.
    pub(crate) fn obtainable(&self) -> bool {
        self.sets.is_empty() || self.blocks_reachable() > 0
    }

    // What the scripts are told: a recovery object vouched for the job.
    pub(crate) fn verified(&self) -> bool {
        !self.sets.is_empty() && self.settled()
    }
}
