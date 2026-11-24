// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job that waits with nothing left to wait for. `drive` waits on a
// set that has not ruled, an object still producing members or a file
// still to move, and every such wait is lifted by an event of its own: a
// worker's mail, an article, a slot, room on a disk, memory, a resume. When
// none of those can come, nothing steps the job again and it keeps its
// admission for ever. The rule is read on the completion gate's turn that
// answered Wait, so on the turn the job's last work ended: no poller and
// no clock.
//
// A stalled job's objects first hear that nothing more arrives, as the
// job's own quiet tells them, and the job goes on from what that frees.
// One that still waits on nothing after that ends as every job ends: its
// status is read off what it delivered, as the completion gate reads it.

use tracing::warn;
use weaver_graph::recovery::RepairOutcome;

use super::seams::settle;
use super::{RootJob, Step};
use crate::pipeline::Pipeline;
use crate::pipeline::graph::job::GraphJob;
use crate::pipeline::graph::root::phase::RootPhase;
use weaver_server_base::jobs::ids::JobId;

impl Pipeline {
    // The completion gate's step answered Wait: a job that waits on
    // nothing is ended.
    pub(super) async fn end_if_stalled(&mut self, job_id: JobId) {
        if !self.stalled(job_id) {
            return;
        }
        // Every set's asks reach the download queue on each step: a round
        // now answers any the gate's own lends made, and a job it asks
        // articles for is not stalled.
        self.after_graph_step(job_id);
        if !self.stalled(job_id) {
            return;
        }
        let limit = self.graph_job_slot_limit(job_id);
        let Some(job) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        // Runs blocked for good end first, a layer at a time: their mail
        // steps the job, and the quiet waits until they have ended.
        if job.reads() {
            let result = job.end_blocked_input();
            if let Ok(nodes) = &result {
                warn!(
                    job_id = job_id.0,
                    nodes = ?nodes,
                    "graph job waits on nothing but runs no input reaches; their input ends"
                );
            }
            self.after_graph_result(job_id, result.map(|_| ()));
            return;
        }
        let phase = job.phase();
        let waited = waits_on(job, phase);
        match job.end_input(&mut self.graph_status.repairs, limit) {
            Ok(false) => self.end_stalled(job_id, &waited).await,
            result => {
                warn!(
                    job_id = job_id.0,
                    waited = %waited,
                    "graph job waits with nothing pending; its objects are told nothing more arrives"
                );
                self.after_graph_result(job_id, result.map(|_| ()));
                self.schedule_job_completion_check(job_id);
            }
        }
    }

    // Nothing the job is owed can still come: it is admitted and running,
    // not paused or held for room, owes the download stage nothing, has no
    // health probe out and no place in the repair slots' queue, and its
    // graph waits on nothing of its own but runs that each wait on their
    // shelf for a change that has not come. While a run still reads, the
    // job asks to hear of the next one to begin waiting, before it looks
    // at the shelves.
    fn stalled(&self, job_id: JobId) -> bool {
        let Some(job) = self.graph_jobs.get(&job_id) else {
            return false;
        };
        let idle = self.idle_outside_graph(job_id) && job.waits_on_runs_only();
        // The ask is stored before the shelves are read, and a run records
        // its wait before it reads the ask: a run that begins to wait after
        // this read is heard of, and one already waiting is seen here.
        job.watch_runs(idle && job.reads(), || {
            (self.graph_runs.written(), job_id.0)
        });
        let stalled = idle && job.runs_blocked();
        if stalled && job.reads() {
            // Every run is seen waiting: this turn ends their input itself.
            job.watch_runs(false, || unreachable!());
        }
        stalled
    }

    fn idle_outside_graph(&self, job_id: JobId) -> bool {
        self.graph_job_slot_limit(job_id) > 0
            && !self.job_failure_started(job_id)
            && self
                .jobs
                .get(&job_id)
                .is_some_and(|state| RootPhase::may_replace(&state.status))
            && !self.held_for_full_disk(job_id)
            && !self.disk_waiting.contains(&job_id)
            && !self.moves_awaiting_disk.contains_key(&job_id)
            && !self.job_has_pending_download_pipeline_work(job_id)
            && !self.post_probes.out(job_id)
            && !self.graph_status.repairs.waits(super::seams::asker(job_id))
    }

    // Nothing the quiet freed moved the job: it ends where it stands, under
    // the status every job ends in, as a gate that answered Deliver ends it.
    async fn end_stalled(&mut self, job_id: JobId, waited: &str) {
        warn!(
            job_id = job_id.0,
            waited = %waited,
            "graph job still waits with nothing pending after its objects heard nothing more arrives; it ends as delivered"
        );
        settle(self, job_id, Step::Deliver).await;
    }
}

// The job's phase and every set that has not vouched, with its ruling.
fn waits_on(job: &GraphJob, phase: RootPhase) -> String {
    let sets: Vec<String> = RootJob::recovery_summary(job)
        .sets
        .into_iter()
        .filter(|set| !set.vouched())
        .map(|set| {
            let ruled = match set.outcome {
                None => "not ruled",
                Some(RepairOutcome::Waiting) => "waiting for recovery blocks",
                Some(RepairOutcome::Unread) => "unread",
                Some(RepairOutcome::Impossible { .. }) => "impossible",
                Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. }) => "vouched",
            };
            format!("{} {} ({ruled})", set.kind, set.name)
        })
        .collect();
    if sets.is_empty() {
        return phase.as_str().to_owned();
    }
    format!("{}: {}", phase.as_str(), sets.join("; "))
}
