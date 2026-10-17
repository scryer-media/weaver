// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline's hooks into a graph job's status. Each classic gate asks one
// of these first and keeps its own path when the job has no graph runtime.

use tracing::{info, warn};
use weaver_graph::MemberId;
use weaver_graph::slots::AskerId;

use super::claims::{self, Claim, ClaimedMember};
use super::{RecoverySummary, RootJob, Step, drive, metrics, shown_status};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::model::JobStatus;
use crate::pipeline::completion::finalize::check::par2_damage_ignorable;
use crate::pipeline::graph::limits;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::{Pipeline, TerminalReconciliation, health_milli};

// The classic completion gate hands a graph job here instead of to its own
// passes: no whole-file verification, no listing check and no re-scan of the
// output, since the tree already ruled on every member as it arrived.
pub(in crate::pipeline) async fn advance(pipeline: &mut Pipeline, job_id: JobId) {
    let Some(state) = pipeline.jobs.get(&job_id) else {
        return;
    };
    // A pause, a failure or anything after the tree is done belongs to
    // whoever set it, and the queues still owe bytes while they hold work.
    if !RootPhase::may_replace(&state.status)
        || pipeline.job_has_pending_download_pipeline_work(job_id)
        || pipeline.queue_script_completion_gate(job_id, true)
    {
        return;
    }
    let (_, limit) = limits::budgets(&pipeline.direct_store.settings(), &pipeline.tuner);
    let Some(job) = pipeline.graph_jobs.get_mut(&job_id) else {
        return;
    };
    let status = &mut pipeline.graph_status;
    let step = drive(job, &mut status.repairs, asker(job_id), limit);
    status
        .summaries
        .insert(job_id, RootJob::recovery_summary(job));
    pipeline.report_graph_phase(job_id);
    for woken in pipeline.graph_status.repairs.take_woken() {
        pipeline.schedule_job_completion_check(JobId(woken.0));
    }
    settle(pipeline, job_id, step).await;
}

// What an advance came to, applied to the job.
pub(super) async fn settle(pipeline: &mut Pipeline, job_id: JobId, step: Step) {
    match step {
        Step::Wait => {}
        Step::Fail(message) => {
            metrics::failed("repair_impossible");
            // The repair is the stage that broke, whatever the root showed.
            pipeline.transition_postprocessing_status(job_id, JobStatus::Repairing, None);
            pipeline.fail_job(job_id, message);
        }
        Step::Deliver => {
            pipeline.remove_recovery_volumes(job_id).await;
            if let Err(error) = pipeline.start_move_to_complete(job_id).await {
                pipeline.fail_job(job_id, error);
            }
        }
    }
}

fn asker(job_id: JobId) -> AskerId {
    AskerId(job_id.0)
}

impl Pipeline {
    // PAR2 and PAR3 files, indexes and volumes alike, are spent once the job
    // delivers and are not part of the release, so they are left out of it
    // as a classic job's are. Root members are numbered as the posted files
    // they are.
    async fn remove_recovery_volumes(&self, job_id: JobId) {
        let (Some(graph), Some(state)) = (self.graph_jobs.get(&job_id), self.jobs.get(&job_id))
        else {
            return;
        };
        for (member, path) in graph.member_files() {
            let is_recovery = state
                .spec
                .files
                .get(member.0 as usize)
                .is_some_and(|file| !file.role.counts_toward_health());
            if !is_recovery {
                continue;
            }
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(file = %path.display(), error = %error, "could not remove a recovery volume");
                }
            }
        }
    }

    pub(in crate::pipeline) fn is_graph_job(&self, job_id: JobId) -> bool {
        self.graph_jobs.contains_key(&job_id)
    }

    // What the root's recovery objects said when a graph job last
    // advanced; nothing yet until it first does.
    pub(in crate::pipeline) fn graph_recovery_summary(
        &self,
        job_id: JobId,
    ) -> Option<RecoverySummary> {
        self.graph_jobs.contains_key(&job_id).then(|| {
            self.graph_status
                .summaries
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
        })
    }

    // The scripts' PAR status: a recovery object vouched for the job.
    pub(in crate::pipeline) fn graph_recovery_verified(&self, job_id: JobId) -> bool {
        self.graph_recovery_summary(job_id)
            .is_some_and(|summary| summary.verified())
    }

    // The scripts' unpack status: an object below the root produced members.
    pub(in crate::pipeline) fn graph_unpacked(&self, job_id: JobId) -> bool {
        self.graph_jobs.get(&job_id).is_some_and(RootJob::unpacked)
    }

    pub(in crate::pipeline) fn forget_graph_repair(&mut self, job_id: JobId) {
        self.graph_status.repairs.forget(asker(job_id));
        self.graph_status.summaries.remove(&job_id);
    }

    // The census of a graph job, from the root namespace's marks: the
    // terminal record, or the refusal of a delivery that has nothing in it.
    // `None` for a classic job, which keeps its own census.
    pub(in crate::pipeline) fn reconcile_graph_delivery(
        &mut self,
        job_id: JobId,
    ) -> Option<Result<(), String>> {
        let namespace = self.graph_jobs.get(&job_id)?.namespace();
        let state = self.jobs.get(&job_id)?;
        let ignore = self.par2_ignore_extensions();
        let census: Vec<ClaimedMember> = state
            .spec
            .files
            .iter()
            .zip(0u32..)
            .filter(|(file, _)| file.role.counts_toward_health())
            .filter_map(|(file, index)| {
                let member = namespace.get(MemberId(index))?;
                let file_id = NzbFileId {
                    job_id,
                    file_index: index,
                };
                let mut row = ClaimedMember::read(
                    member,
                    par2_damage_ignorable(&member.output_name, &ignore),
                );
                row.lost_articles = self
                    .segment_terminal_states
                    .keys()
                    .filter(|segment| segment.file_id == file_id)
                    .count();
                row.lost_bytes = self.file_terminal_failed_bytes(file_id);
                row.declared_bytes = file.segments.iter().map(|s| u64::from(s.bytes)).sum();
                Some(row)
            })
            .collect();
        let total_bytes = state.spec.total_bytes;
        metrics::claimed(&census);

        let refused = claims::refused(&census);
        if !refused.is_empty() {
            let missing_bytes: u64 = refused.iter().map(|row| row.declared_bytes).sum();
            let names = refused
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let message = format!(
                "{} payload file(s) were never delivered and no verification, repair or discard \
                 accounts for them ({missing_bytes} declared bytes): {names}",
                refused.len()
            );
            self.semantic_terminal_causes.insert(
                job_id,
                crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth,
            );
            return Some(Err(message));
        }

        for row in &census {
            match row.claim {
                Claim::Complete => {}
                Claim::Damaged { bytes } => info!(
                    job_id = job_id.0,
                    member = %row.name,
                    damaged_bytes = bytes,
                    lost_articles = row.lost_articles,
                    "delivering a member with damaged ranges as it stands"
                ),
                Claim::Missing { bytes } => warn!(
                    job_id = job_id.0,
                    member = %row.name,
                    missing_bytes = bytes,
                    lost_articles = row.lost_articles,
                    "delivering a member that never fully arrived"
                ),
            }
        }
        let failed_bytes = census
            .iter()
            .map(ClaimedMember::failed_bytes)
            .fold(0u64, u64::saturating_add)
            .min(total_bytes);
        let health = if total_bytes == 0 {
            1000
        } else {
            health_milli(total_bytes, failed_bytes)
        };
        self.terminal_reconciliations.insert(
            job_id,
            TerminalReconciliation {
                failed_bytes,
                health,
                discards: Vec::new(),
            },
        );
        Some(Ok(()))
    }

    // Show the root's phase as the job's status when it changed, the way an
    // article arriving does, and keep it with the job.
    fn report_graph_phase(&mut self, job_id: JobId) {
        let (Some(graph), Some(state)) =
            (self.graph_jobs.get_mut(&job_id), self.jobs.get_mut(&job_id))
        else {
            return;
        };
        let Some(phase) = graph.phase_change() else {
            return;
        };
        metrics::phase(phase);
        if let Some(status) = shown_status(phase, &state.status) {
            state.status = status;
            state.refresh_runtime_lanes_from_status();
        }
        if let Err(error) =
            self.db
                .try_queue_job_write(job_id, "save_active_job_root_phase", move |db| {
                    db.save_active_job_root_phase(job_id, phase.as_str())
                })
        {
            warn!(job_id = job_id.0, error = %error, "could not queue the root phase write");
        }
    }
}
