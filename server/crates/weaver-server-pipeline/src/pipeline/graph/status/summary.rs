// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a namespace's recovery objects can say about its members: per object,
// what its last repair came to. The repair decision and the script-facing PAR
// status both read this one record, so they cannot disagree.

use weaver_graph::recovery::{RepairOutcome, ShortSets};
use weaver_graph::{Kind, MemberId, NodeId};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    pub(crate) sets: Vec<SetSummary>,
}

// One recovery object of the namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SetSummary {
    // The object's own node.
    pub(crate) node: NodeId,
    pub(crate) kind: Kind,
    // The namespace it lives in, at whatever depth.
    pub(crate) namespace: NodeId,
    // What the object is called where it lives, for the operator.
    pub(crate) name: String,
    // `None` before it ran a repair.
    pub(crate) outcome: Option<RepairOutcome>,
    // Its sets that cannot rebuild what they bind, set by set, read only
    // when the object rules impossible and can say so set by set. `None`
    // otherwise: then its outcome rules every member it covers.
    pub(crate) short: Option<ShortSets>,
}

impl SetSummary {
    // Whether the object's ruling leaves the member short: every member it
    // covers when it cannot say set by set, and otherwise those its short
    // sets bind. `covers` says whether the object covers the member.
    pub(crate) fn leaves_short(&self, member: MemberId, covers: impl FnOnce() -> bool) -> bool {
        match &self.short {
            Some(short) => short.sets.iter().any(|set| set.members.contains(&member)),
            None => covers(),
        }
    }

    // Its last repair found the members whole or made them so.
    pub(crate) fn vouched(&self) -> bool {
        vouches(self.outcome)
    }
}

pub(crate) fn vouches(outcome: Option<RepairOutcome>) -> bool {
    matches!(
        outcome,
        Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
    )
}

impl RecoverySummary {
    // What each object last ruled, in order; one that has not ruled yet
    // waits.
    pub(crate) fn outcomes(&self) -> Vec<RepairOutcome> {
        self.sets
            .iter()
            .map(|set| set.outcome.unwrap_or(RepairOutcome::Waiting))
            .collect()
    }

    // Nothing is left for a repair pass to say: every object vouched, or
    // there is no object to ask.
    pub(crate) fn settled(&self) -> bool {
        self.sets.iter().all(SetSummary::vouched)
    }

    // A recovery object rebuilt something.
    pub(crate) fn repaired(&self) -> bool {
        self.sets
            .iter()
            .any(|set| matches!(set.outcome, Some(RepairOutcome::Repaired { .. })))
    }

    // What the scripts are told: a recovery object vouched for the job.
    pub(crate) fn verified(&self) -> bool {
        !self.sets.is_empty() && self.settled()
    }
}
