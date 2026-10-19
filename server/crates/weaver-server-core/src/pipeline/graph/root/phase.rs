// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The root's phase: where the job as a whole stands, read off the tree. It is
// what a graph job reports as its status, and it is stored with the job so a
// restart shows the same phase before anything has been re-derived.

use serde::{Deserialize, Serialize};
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{NodeId, Tree};

use crate::jobs::model::JobStatus;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RootPhase {
    Downloading,
    Verifying,
    Repairing,
    Extracting,
    Done,
}

// What the job's recovery objects are doing, read off what each has ruled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Activity {
    #[default]
    Idle,
    Verifying,
    Repairing,
}

// One waiting on recovery bytes is repairing; one that has not ruled is
// verifying; with every one ruled, or none to rule, nothing is at work.
pub(crate) fn activity(outcomes: impl IntoIterator<Item = Option<RepairOutcome>>) -> Activity {
    outcomes
        .into_iter()
        .fold(Activity::Idle, |at, outcome| match outcome {
            Some(RepairOutcome::Waiting) => Activity::Repairing,
            None if at == Activity::Idle => Activity::Verifying,
            _ => at,
        })
}

impl RootPhase {
    // The first that holds wins: bytes still to fetch, then a recovery
    // object at work, then an object still producing members, then done.
    pub(crate) fn derive(waiting: bool, activity: Activity, extracting: bool) -> Self {
        match (waiting, activity, extracting) {
            (true, _, _) => Self::Downloading,
            (false, Activity::Repairing, _) => Self::Repairing,
            (false, Activity::Verifying, _) => Self::Verifying,
            (false, Activity::Idle, true) => Self::Extracting,
            (false, Activity::Idle, false) => Self::Done,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Downloading => "downloading",
            Self::Verifying => "verifying",
            Self::Repairing => "repairing",
            Self::Extracting => "extracting",
            Self::Done => "done",
        }
    }

    // The job status the phase shows as. Done has none: from there the job
    // is finalised, and finalisation owns the status.
    pub(crate) fn job_status(self) -> Option<JobStatus> {
        match self {
            Self::Downloading => Some(JobStatus::Downloading),
            Self::Verifying => Some(JobStatus::Verifying),
            Self::Repairing => Some(JobStatus::Repairing),
            Self::Extracting => Some(JobStatus::Extracting),
            Self::Done => None,
        }
    }

    // Whether the phase may replace this status. The root speaks for the
    // job only while it is running its tree; a pause, a failure or anything
    // after the tree is done belongs to whoever set it.
    pub(crate) fn may_replace(status: &JobStatus) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Verifying
                | JobStatus::Repairing
                | JobStatus::Extracting
        )
    }
}

// Whether an object below the root is still producing members: it has a
// member not yet complete, or it has announced none while its volumes are
// still arriving. One whose volumes are all in and announced nothing is
// done, unless a run of it is still reading, which the job reads off its
// runs.
pub(crate) fn extracting(tree: &Tree) -> bool {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return false;
    };
    root.members().any(|member| {
        let Some((child, _)) = tree.child_of(NodeId::ROOT, member.id) else {
            return false;
        };
        if tree.recovers(child) {
            return false;
        }
        let Some(namespace) = tree.namespace(child) else {
            return false;
        };
        if namespace.is_empty() {
            return tree.volumes(child).is_some_and(|(_, volumes)| {
                volumes
                    .values()
                    .any(|volume| root.get(*volume).is_none_or(|record| !record.is_complete()))
            });
        }
        namespace.members().any(|inner| !inner.is_complete())
    })
}
