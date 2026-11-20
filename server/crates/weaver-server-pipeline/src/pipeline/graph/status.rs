// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a graph job stands once its articles are in, read from the root
// namespace: whether its recovery objects have ruled, the repair that takes
// a slot when they have not, what the job claims it delivered, and what the
// status and scripts are told. The job's status is its root's phase;
// deeper phases belong to member detail.
//
//   summary   what every recovery object last ruled
//   claims    complete, damaged or missing, member by member
//   slots     the repair slots the graph jobs share
//   metrics   the `graph.*` family
//   seams     the pipeline's hooks: completion, the census, scripts
//   stall     a job that waits on nothing ends

pub mod claims;
mod metrics;
pub mod seams;
pub mod slots;
mod stall;
pub mod summary;

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::slots::AskerId;
use weaver_graph::{Kind, MemberId, Namespace, NodeId, ObjectId, Tree};

use super::job::GraphJob;
use super::root::phase::RootPhase;
use crate::jobs::model::JobStatus;
use crate::pipeline::extraction::ExtractionFailure;

pub use crate::pipeline::completion::finalize::check::configured_par2_ignore_extensions;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub use claims::Claim;
pub use claims::Delivered;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub use claims::Described;
pub use claims::{Delivery, Unarrived};
pub use seams::{Posted, delivery_of, shortfall_message};
pub(in crate::pipeline) use seams::{advance, recovery_volume};
pub use slots::RepairSlots;
pub(crate) use summary::{RecoverySummary, SetSummary};

// The pipeline's part of graph status: the repair slots every graph job
// shares.
#[derive(Debug, Default)]
pub struct GraphStatus {
    pub repairs: RepairSlots,
}

// What status reads of a graph job and the one thing it asks of it.
pub trait RootJob {
    fn phase(&self) -> RootPhase;
    // A namespace of the tree, at any depth: where a recovery object lives.
    fn namespace(&self, node: NodeId) -> Option<&Namespace>;
    // In the order `repair` reports outcomes.
    fn recovery_summary(&self) -> RecoverySummary;
    // Whether the recovery object at `set` covers `member` of its namespace.
    fn covers(&self, set: NodeId, member: MemberId) -> bool;
    // What the namespace `node` came out of, at some depth, a recovery set
    // proved (`Tree::vouched_above`). Never the root.
    fn vouched_above(&self, node: NodeId) -> bool;
    // Run every recovery object of the root namespace.
    fn repair(&mut self) -> Vec<RepairOutcome>;
    // An object below the root produced members.
    fn unpacked(&self) -> bool;
    // The breach of the job's extraction budget, once one stands.
    fn refusal(&self) -> Option<&ExtractionFailure>;
    // Recovery, replay or checksum work has not been acknowledged yet,
    // including a checksum check still waiting for worker admission.
    fn work_pending(&self) -> bool;
    // A member's file still waits to move to its name.
    fn moves_waiting(&self) -> bool;
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
                name: self
                    .writer()
                    .set_name(tree, object.node)
                    .map_or_else(|| object.kind.to_string(), str::to_owned),
                outcome: object.outcome,
                // Read set by set only for an object that ruled it cannot
                // rebuild: no other ruling fails or excuses anything.
                short: matches!(object.outcome, Some(RepairOutcome::Impossible { .. }))
                    .then(|| self.writer().short_sets(tree, object.node))
                    .flatten(),
            })
            .collect();
        RecoverySummary { sets }
    }

    fn covers(&self, set: NodeId, member: MemberId) -> bool {
        self.tree()
            .recovery(set)
            .is_some_and(|recovery| recovery.covers(member))
    }

    fn vouched_above(&self, node: NodeId) -> bool {
        self.tree().vouched_above(node)
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

    fn work_pending(&self) -> bool {
        GraphJob::work_pending(self)
    }

    fn moves_waiting(&self) -> bool {
        GraphJob::moves_waiting(self)
    }
}

// What an advance came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
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
pub fn drive(job: &mut dyn RootJob, slots: &mut RepairSlots, asker: AskerId, limit: usize) -> Step {
    // No delivery while a worker or queued checksum still has a say about
    // the file's bytes, even when no recovery set needs to run.
    if job.work_pending() {
        return Step::Wait;
    }
    let summary = job.recovery_summary();
    if !summary.settled() {
        let Some(ticket) = slots.admit(asker, limit) else {
            return Step::Wait;
        };
        let outcomes = job.repair();
        // The job ends at the first object that could not rebuild what it
        // found. Another set still waiting has nothing left that can arrive:
        // the job gets here only with no work pending, and every set's asks
        // reach the download queue on each step, so blocks it was asked for
        // are already in or lost.
        let ruled_short = failure(&summary, &outcomes, &*job).is_some();
        if !ruled_short && outcomes.contains(&RepairOutcome::Waiting) {
            slots.yield_slot(ticket);
            return Step::Wait;
        }
        slots.release(ticket);
    }
    match job.phase() {
        RootPhase::Extracting => Step::Wait,
        // The delivery takes every file from its name, so it waits for
        // each file still moving there.
        _ if job.moves_waiting() => Step::Wait,
        _ => Step::Deliver,
    }
}

// The objects that could not rebuild what they found, and that failure
// worded as a recovery failure reads today, unless another object of its
// namespace, or another set of its own object, vouched for that namespace
// and left no damage in what the set covers: an object that only checks
// never overrules one that repaired, but a repair of one set never excuses
// another's, and damage the set does not cover, a recovery volume nothing
// rebuilds, keeps no set's ruling standing. An object that says set by set
// fails for its short sets alone, each named as it was posted, and leaves
// short only what they bind (`SetSummary::leaves_short`).
// Each set is judged in the namespace it lives
// in, at whatever depth. An SFV in a namespace that came out of volumes a
// set above proved makes no claim: what it lists is what was posted, and
// its ruling fails nothing. A breach of the job's extraction budget fails the
// job before any of that: it carries the budget's refusal, worded as the
// job's failure reason, and names no object. When no set failed and the
// feed gave the post up, the failure is the feed's, in its words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub objects: Vec<NodeId>,
    pub message: String,
    pub rejection: Option<ExtractionFailure>,
    // The words are the feed's: it gave the post up.
    pub from_feed: bool,
}

impl Failure {
    // The feed gave the post up, in these words.
    pub fn post_given_up(words: String) -> Self {
        Self {
            objects: Vec::new(),
            message: words,
            rejection: None,
            from_feed: true,
        }
    }
}

pub fn failure(
    summary: &RecoverySummary,
    outcomes: &[RepairOutcome],
    job: &dyn RootJob,
) -> Option<Failure> {
    if let Some(rejection) = job.refusal() {
        return Some(Failure {
            objects: Vec::new(),
            message: rejection.job_failure_reason(),
            rejection: Some(rejection.clone()),
            from_feed: false,
        });
    }
    let ruled: Vec<(&SetSummary, RepairOutcome)> =
        summary.sets.iter().zip(outcomes.iter().copied()).collect();
    // The members not clean in each namespace a set ruled impossible in,
    // read once: a set is excused when none of them is one it covers.
    let unclean: BTreeMap<NodeId, Vec<MemberId>> = ruled
        .iter()
        .filter(|(_, outcome)| matches!(outcome, RepairOutcome::Impossible { .. }))
        .map(|(set, _)| set.namespace)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|node| Some((node, job.namespace(node)?)))
        .map(|(node, namespace)| {
            let members = namespace
                .members()
                .filter(|member| !member.coverage.is_clean());
            (node, members.map(|member| member.id).collect())
        })
        .collect();
    // A set of the same object that vouched for what it binds excuses its
    // short sibling on the same terms as another object does.
    let excused = |set: &SetSummary| {
        (ruled.iter().any(|(other, outcome)| {
            other.namespace == set.namespace && summary::vouches(Some(*outcome))
        }) || set.short.as_ref().is_some_and(|short| short.vouched))
            && unclean
                .get(&set.namespace)
                .is_none_or(|members| members.iter().all(|member| !job.covers(set.node, *member)))
    };
    let failed: Vec<(&SetSummary, u64, u64)> = ruled
        .iter()
        .filter_map(|(set, outcome)| match outcome {
            RepairOutcome::Impossible { damaged, reachable }
                if !excused(set)
                    && !(set.kind == Kind::Sfv && job.vouched_above(set.namespace)) =>
            {
                Some((*set, *damaged, *reachable))
            }
            _ => None,
        })
        .collect();
    if failed.is_empty() {
        return None;
    }
    let kinds: BTreeSet<String> = failed.iter().map(|(set, _, _)| label(set.kind)).collect();
    let mut described = Vec::new();
    for (set, slices, reachable) in &failed {
        let namespace = job.namespace(set.namespace);
        let worded = |name: &str, slices: u64, reachable: u64, damaged: String| {
            // The members still damaged where the set lives, when its
            // namespace marks any; a check that failed on bytes that
            // arrived whole leaves none to name.
            let damaged = Some(damaged)
                .filter(|names| !names.is_empty())
                .map(|names| format!(" ({names})"))
                .unwrap_or_default();
            format!(
                "{name}{damaged}: not repairable: {slices} damaged slices, only {reachable} recovery blocks reachable"
            )
        };
        // An object that says set by set is worded set by set: each short
        // set by the name it was posted under, over the damaged members it
        // binds and those no short set of the object binds.
        match set.short.as_ref().filter(|short| !short.sets.is_empty()) {
            Some(short) => described.extend(short.sets.iter().map(|one| {
                let names = damaged_names(namespace, set.node, |member| {
                    one.members.contains(&member)
                        || !short
                            .sets
                            .iter()
                            .any(|other| other.members.contains(&member))
                });
                worded(
                    one.name.as_deref().unwrap_or(&set.name),
                    one.damaged,
                    one.reachable,
                    names,
                )
            })),
            None => described.push(worded(
                &set.name,
                *slices,
                *reachable,
                damaged_names(namespace, set.node, |_| true),
            )),
        }
    }
    let message = format!(
        "{} recovery failed for {} set(s): {}",
        kinds.into_iter().collect::<Vec<_>>().join("/"),
        described.len(),
        described.join("; ")
    );
    Some(Failure {
        objects: failed.iter().map(|(set, _, _)| set.node).collect(),
        message,
        rejection: None,
        from_feed: false,
    })
}

// The failure that settles a job before its download ends: a set of the
// root ruled it cannot rebuild what it covers, every member of the root has
// said what it is, and no payload member of the root can still come whole.
// One can while it holds no damage, or while a set of the root that covers
// it has not ruled impossible, or while an object reading it may still
// produce a file below it (`may_come_whole_below`). A set below the root
// that rules impossible settles nothing here: the subtree it covers is
// short, and what else the job delivers whole is read once the job is done.
pub(crate) fn ruled_short(job: &GraphJob) -> Option<Failure> {
    let summary = RootJob::recovery_summary(job);
    let outcomes = summary.outcomes();
    let failure =
        failure(&summary, &outcomes, job).filter(|failure| failure.rejection.is_none())?;
    let at_root = summary
        .sets
        .iter()
        .any(|set| set.namespace == NodeId::ROOT && failure.objects.contains(&set.node));
    if !at_root || !job.identified() {
        return None;
    }
    let tree = job.tree();
    let writer = job.writer();
    let sets = tree.recovery_nodes(NodeId::ROOT);
    // A failed object that says set by set leaves short only what its short
    // sets bind; a member another of its sets binds may still come whole.
    let failed: Vec<&SetSummary> = summary
        .sets
        .iter()
        .filter(|set| failure.objects.contains(&set.node))
        .collect();
    let may_come_whole = job
        .namespace()
        .members()
        .filter(|member| !writer.carries_recovery(tree, NodeId::ROOT, member.id))
        .any(|member| {
            member.coverage.damaged_ranges().next().is_none()
                || writer.covering(tree, &sets, member.id).any(|set| {
                    failed
                        .iter()
                        .find(|failed| failed.node == set)
                        .is_none_or(|failed| !failed.leaves_short(member.id, || true))
                })
                || may_come_whole_below(tree, NodeId::ROOT, member.id)
        });
    (!may_come_whole).then_some(failure)
}

// Whether a member an object reads may yet be delivered whole below it,
// whatever its own bytes hold: the payload of an archive is what it
// unpacks, not its volumes. An object still reading has not said what it
// produces. One that gave its volumes up kept what it closed whole, and so
// may an object below it; the walk delivers that when the job ends. A
// recovery object produces no file.
fn may_come_whole_below(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    tree.readers(node, member).any(|object| {
        if tree.recovers(object) {
            return false;
        }
        if tree.is_live(object) {
            return true;
        }
        tree.namespace(object).is_some_and(|namespace| {
            namespace.members().any(|below| {
                !tree.holds_no_file(object, below.id)
                    && match tree.child_of(object, below.id) {
                        Some((child, _)) if tree.recovers(child) => false,
                        Some(_) => may_come_whole_below(tree, object, below.id),
                        None => below.is_whole(),
                    }
            })
        })
    })
}

fn label(kind: Kind) -> String {
    kind.to_string().to_ascii_uppercase()
}

// The members still carrying damage, as they are written: the set's
// members, never its own volumes.
fn damaged_names(
    namespace: Option<&Namespace>,
    set: NodeId,
    named: impl Fn(MemberId) -> bool,
) -> String {
    let Some(namespace) = namespace else {
        return String::new();
    };
    let members: BTreeSet<MemberId> = namespace
        .damaged_ranges()
        .map(|(member, _)| member)
        .collect();
    members
        .into_iter()
        .filter(|member| named(*member))
        .filter_map(|member| namespace.get(member))
        .filter(|member| member.bound_to != Some(ObjectId(set.0)))
        .map(|member| member.output_name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

// The status a phase shows over the one the job has, when it may replace
// it. Done shows nothing: finalisation owns the status from there. Nor does
// downloading show over queued: the tree says only that bytes are wanted,
// and a queued job starts downloading when the dispatcher sends its first
// article, whichever engine runs it.
pub fn shown_status(phase: RootPhase, current: &JobStatus) -> Option<JobStatus> {
    phase
        .job_status()
        .filter(|status| RootPhase::may_replace(current) && status != current)
        .filter(|status| !(*current == JobStatus::Queued && *status == JobStatus::Downloading))
}
