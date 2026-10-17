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

use std::collections::{BTreeSet, HashMap};

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::slots::AskerId;
use weaver_graph::{Kind, MemberId, Namespace, NodeId};

use super::job::GraphJob;
use super::root::phase::RootPhase;
use crate::jobs::model::JobStatus;

pub(in crate::pipeline) use seams::advance;
pub(crate) use slots::RepairSlots;
pub(crate) use summary::{RecoverySummary, SetSummary};

use crate::jobs::ids::JobId;

// The pipeline's part of graph status: the repair slots every graph job
// shares, and what each job's recovery objects said when it last advanced,
// for the readers that hold the pipeline only to look.
#[derive(Debug, Default)]
pub(crate) struct GraphStatus {
    pub(crate) repairs: RepairSlots,
    pub(crate) summaries: HashMap<JobId, RecoverySummary>,
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
        RecoverySummary { sets, unusable }
    }

    fn repair(&mut self) -> Vec<RepairOutcome> {
        RootJob::recovery_summary(self)
            .sets
            .into_iter()
            .map(|set| set.outcome.unwrap_or(RepairOutcome::Waiting))
            .collect()
    }

    fn unpacked(&self) -> bool {
        let tree = self.tree();
        GraphJob::namespace(self).members().any(|member| {
            tree.child_of(NodeId::ROOT, member.id)
                .and_then(|(child, _)| tree.namespace(child))
                .is_some_and(|namespace| !namespace.is_empty())
        })
    }
}

// What an advance came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    // For a slot, for recovery bytes, or for an object still producing
    // members. Whatever ends the wait advances the job again.
    Wait,
    Deliver,
    Fail(String),
}

// Verify, then repair under a slot, then deliver. Called once nothing is
// left for the queues to fetch.
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
        metrics::repaired(&summary, &outcomes);
        if let Some(message) = failure(&summary, &outcomes, &*job) {
            slots.release(ticket);
            return Step::Fail(message);
        }
        if outcomes.contains(&RepairOutcome::Waiting) {
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

// The job fails, worded as a recovery failure reads today, when an object
// could not rebuild what it found, unless another object of its namespace
// vouched for that namespace and left no damage in it: an object that only
// checks never overrules one that repaired, but a repair of one set never
// excuses another's. Each set is judged in the namespace it lives in, at
// whatever depth.
fn failure(
    summary: &RecoverySummary,
    outcomes: &[RepairOutcome],
    job: &dyn RootJob,
) -> Option<String> {
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
            let damaged = Some(damaged_names(job.namespace(set.namespace)))
                .filter(|names| !names.is_empty())
                .map(|names| format!(" ({names})"))
                .unwrap_or_default();
            format!(
                "{}{damaged}: not repairable: {slices} damaged slices, only {reachable} recovery blocks reachable",
                set.name
            )
        })
        .collect::<Vec<_>>();
    Some(format!(
        "{} recovery failed for {} set(s): {}",
        kinds.into_iter().collect::<Vec<_>>().join("/"),
        failed.len(),
        described.join("; ")
    ))
}

fn label(kind: Kind) -> String {
    kind.to_string().to_ascii_uppercase()
}

// The members still carrying damage, as they are written.
fn damaged_names(namespace: Option<&Namespace>) -> String {
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
