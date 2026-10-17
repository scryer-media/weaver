// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The root's phase: where the job as a whole stands, read off the tree. It is
// what a graph job reports as its status, and it is stored with the job so a
// restart shows the same phase before anything has been re-derived.

use std::collections::BTreeMap;

use weaver_graph::{Kind, NodeId, Tree};

use crate::jobs::model::JobStatus;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootPhase {
    Downloading,
    Verifying,
    Repairing,
    Extracting,
    Done,
}

// What the job's recovery objects are doing, as the namespace reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Activity {
    #[default]
    Idle,
    Verifying,
    Repairing,
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

// Whether an object below the root is still producing members: a member of
// the root is bound to an object that is not a recovery object, and that
// object's namespace is empty or has a member not yet complete.
pub(crate) fn extracting(tree: &Tree, kinds: &BTreeMap<NodeId, Kind>) -> bool {
    let Some(root) = tree.namespace(NodeId::ROOT) else {
        return false;
    };
    root.members().any(|member| {
        let Some((child, _)) = tree.child_of(NodeId::ROOT, member.id) else {
            return false;
        };
        if kinds.get(&child).is_none_or(|kind| kind.is_recovery()) {
            return false;
        }
        tree.namespace(child).is_some_and(|namespace| {
            namespace.is_empty() || namespace.members().any(|inner| !inner.is_complete())
        })
    })
}
