// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a graph job stands once its articles are in, read from the root
// namespace: whether its recovery objects have ruled, the repair that takes
// a slot when they have not, what the job claims it delivered, and what the
// status, health and scripts are told. The job's status is its root's phase;
// deeper phases belong to member detail.
//
//   summary   what every recovery object can say, folded into numbers
//   claims    complete, damaged or missing, member by member
//   slots     the repair slots the graph jobs share
//   metrics   the `graph.*` family
//   seams     the pipeline's hooks: completion, the census, health, scripts

mod claims;
mod metrics;
mod seams;
mod slots;
mod summary;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::slots::AskerId;
use weaver_graph::{Kind, MemberId, Namespace, NodeId, ObjectId};

use super::job::GraphJob;
use super::root::phase::RootPhase;
use crate::jobs::model::JobStatus;
use crate::pipeline::extraction::ExtractionFailure;

#[cfg(test)]
pub(crate) use claims::Delivered;
pub(crate) use claims::Delivery;
pub(in crate::pipeline) use seams::advance;
pub(crate) use slots::RepairSlots;
pub(crate) use summary::{RecoverySummary, SetSummary};

// The pipeline's part of graph status: the repair slots every graph job
// shares.
#[derive(Debug, Default)]
pub(crate) struct GraphStatus {
    pub(crate) repairs: RepairSlots,
}

// What status reads of a graph job and the one thing it asks of it.
pub(crate) trait RootJob {
    fn phase(&self) -> RootPhase;
    // A namespace of the tree, at any depth: where a recovery object lives.
    fn namespace(&self, node: NodeId) -> Option<&Namespace>;
    // In the order `repair` reports outcomes.
    fn recovery_summary(&self) -> RecoverySummary;
    // Run every recovery object of the root namespace.
    fn repair(&mut self) -> Vec<RepairOutcome>;
    // An object below the root produced members.
    fn unpacked(&self) -> bool;
    // The breach of the job's extraction budget, once one stands.
    fn refusal(&self) -> Option<&ExtractionFailure>;
}

// The job's own seams. GraphJob repairs inside its article path, once the
// payload is in and again when recovery blocks a waiting repair asked for
// arrive, and keeps no public entry to run one; so its repair here reads
// back what the last one came to, and an object that has not ruled yet
// waits. The slot then bounds the verdict, not the work; it bounds the work
// once the job hands its repair to this seam.
impl RootJob for GraphJob {
    fn phase(&self) -> RootPhase {
        GraphJob::phase(self)
    }

    fn namespace(&self, node: NodeId) -> Option<&Namespace> {
        self.tree().namespace(node)
    }

    fn recovery_summary(&self) -> RecoverySummary {
        let objects = GraphJob::recovery_summary(self);
        let tree = self.tree();
        let sets = objects
            .into_iter()
            .map(|object| SetSummary {
                node: object.node,
                kind: object.kind,
                namespace: object.namespace,
                // The object is called what its first volume is, in the
                // namespace it lives in.
                name: tree
                    .volumes(object.node)
                    .and_then(|(parent, volumes)| {
                        tree.namespace(parent)?.get(*volumes.values().next()?)
                    })
                    .map_or_else(
                        || object.kind.to_string(),
                        |member| member.output_name.clone(),
                    ),
                blocks_reachable: object.blocks_reachable,
                damaged_slices: object.damaged_slices,
                outcome: object.outcome,
            })
            .collect();
        let unusable = self
            .writer()
            .unusable_recovery()
            .iter()
            .filter_map(|(node, member)| tree.namespace(*node)?.get(*member))
            .map(|member| member.output_name.clone())
            .collect();
        RecoverySummary {
            sets,
            unusable,
            forming: self.recovery_may_form(),
        }
    }

    fn repair(&mut self) -> Vec<RepairOutcome> {
        RootJob::recovery_summary(self).outcomes()
    }

    fn unpacked(&self) -> bool {
        let tree = self.tree();
        GraphJob::namespace(self).members().any(|member| {
            tree.child_of(NodeId::ROOT, member.id)
                .and_then(|(child, _)| tree.namespace(child))
                .is_some_and(|namespace| !namespace.is_empty())
        })
    }

    fn refusal(&self) -> Option<&ExtractionFailure> {
        self.writer().refusal()
    }
}

// What an advance came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    // For a slot, for recovery bytes, or for an object still producing
    // members. Whatever ends the wait advances the job again.
    Wait,
    // Nothing is left to wait for. What the job ends as is read off its
    // settled claims, whether every repair came good or not.
    Deliver,
}

// Verify, then repair under a slot, then deliver. Called once nothing is
// left for the queues to fetch. A repair that cannot rebuild what it found
// does not end the job here: the members it left short are part of what
// the job delivers, beside the whole ones.
pub(crate) fn drive(
    job: &mut dyn RootJob,
    slots: &mut RepairSlots,
    asker: AskerId,
    limit: usize,
) -> Step {
    let summary = job.recovery_summary();
    if !summary.settled() {
        let Some(ticket) = slots.admit(asker, limit) else {
            return Step::Wait;
        };
        let outcomes = job.repair();
        // The job ends at the first object that could not rebuild what it
        // found, as it did when that failed it: blocks another object still
        // waits on are not held for.
        let ruled_short = failure(&summary, &outcomes, &*job).is_some();
        if !ruled_short && outcomes.contains(&RepairOutcome::Waiting) {
            slots.yield_slot(ticket);
            return Step::Wait;
        }
        slots.release(ticket);
    }
    match job.phase() {
        RootPhase::Extracting => Step::Wait,
        _ => Step::Deliver,
    }
}

// What the job's repair came to once every object has ruled: the slices
// its objects rebuilt, when at least one rebuilt any and none fell short.
// One still waiting has not ruled, and one that could not rebuild means the
// repair did not hold, whatever the others did.
pub(crate) fn repaired_slices(outcomes: &[RepairOutcome]) -> Option<u32> {
    let mut repaired: Option<u64> = None;
    for outcome in outcomes {
        match outcome {
            RepairOutcome::Waiting | RepairOutcome::Impossible { .. } => return None,
            RepairOutcome::Repaired { slices } => {
                repaired = Some(repaired.unwrap_or(0).saturating_add(*slices));
            }
            RepairOutcome::Clean => {}
        }
    }
    repaired.map(|slices| u32::try_from(slices).unwrap_or(u32::MAX))
}

// The objects that could not rebuild what they found, and that failure
// worded as a recovery failure reads today, unless another object of its
// namespace vouched for that namespace and left no damage in it: an object
// that only checks never overrules one that repaired, but a repair of one
// set never excuses another's. Each set is judged in the namespace it lives
// in, at whatever depth. A breach of the job's extraction budget fails the
// job before any of that: it carries the budget's refusal, worded as the
// job's failure reason, and names no object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub(crate) objects: Vec<NodeId>,
    pub(crate) message: String,
    pub(crate) rejection: Option<ExtractionFailure>,
}

pub(crate) fn failure(
    summary: &RecoverySummary,
    outcomes: &[RepairOutcome],
    job: &dyn RootJob,
) -> Option<Failure> {
    if let Some(rejection) = job.refusal() {
        return Some(Failure {
            objects: Vec::new(),
            message: rejection.job_failure_reason(),
            rejection: Some(rejection.clone()),
        });
    }
    let ruled: Vec<(&SetSummary, RepairOutcome)> =
        summary.sets.iter().zip(outcomes.iter().copied()).collect();
    let excused = |set: &SetSummary| {
        ruled.iter().any(|(other, outcome)| {
            other.namespace == set.namespace && summary::vouches(Some(*outcome))
        }) && job.namespace(set.namespace).is_none_or(Namespace::is_clean)
    };
    let failed: Vec<(&SetSummary, u64, u64)> = ruled
        .iter()
        .filter_map(|(set, outcome)| match outcome {
            RepairOutcome::Impossible { damaged, reachable } if !excused(set) => {
                Some((*set, *damaged, *reachable))
            }
            _ => None,
        })
        .collect();
    if failed.is_empty() {
        return None;
    }
    let kinds: BTreeSet<String> = failed.iter().map(|(set, _, _)| label(set.kind)).collect();
    let described = failed
        .iter()
        .map(|(set, slices, reachable)| {
            // The members still damaged where the set lives, when its
            // namespace marks any; a check that failed on bytes that
            // arrived whole leaves none to name.
            let damaged = Some(damaged_names(job.namespace(set.namespace), set.node))
                .filter(|names| !names.is_empty())
                .map(|names| format!(" ({names})"))
                .unwrap_or_default();
            format!(
                "{}{damaged}: not repairable: {slices} damaged slices, only {reachable} recovery blocks reachable",
                set.name
            )
        })
        .collect::<Vec<_>>();
    let message = format!(
        "{} recovery failed for {} set(s): {}",
        kinds.into_iter().collect::<Vec<_>>().join("/"),
        failed.len(),
        described.join("; ")
    );
    Some(Failure {
        objects: failed.iter().map(|(set, _, _)| set.node).collect(),
        message,
        rejection: None,
    })
}

fn label(kind: Kind) -> String {
    kind.to_string().to_ascii_uppercase()
}

// The members still carrying damage, as they are written: the set's
// members, never its own volumes.
fn damaged_names(namespace: Option<&Namespace>, set: NodeId) -> String {
    let Some(namespace) = namespace else {
        return String::new();
    };
    let members: BTreeSet<MemberId> = namespace
        .damaged_ranges()
        .map(|(member, _)| member)
        .collect();
    members
        .into_iter()
        .filter_map(|member| namespace.get(member))
        .filter(|member| member.bound_to != Some(ObjectId(set.0)))
        .map(|member| member.output_name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

// The status a phase shows over the one the job has, when it may replace
// it. Done shows nothing: finalisation owns the status from there.
pub(crate) fn shown_status(phase: RootPhase, current: &JobStatus) -> Option<JobStatus> {
    phase
        .job_status()
        .filter(|status| RootPhase::may_replace(current) && status != current)
}
