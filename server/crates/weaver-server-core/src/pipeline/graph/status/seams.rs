// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline's hooks into a graph job's status. Each classic gate asks one
// of these first and keeps its own path when the job has no graph runtime.

use tracing::{info, warn};
use weaver_graph::recovery::{RepairOutcome, SetRuling};
use weaver_graph::slots::AskerId;
use weaver_graph::{MemberId, NodeId};

use super::claims::{self, Claim, ClaimedMember};
use super::{
    Delivery, Failure, RecoverySummary, RootJob, Step, drive, failure, metrics, repaired_slices,
    shown_status,
};
use crate::events::model::PipelineEvent;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::model::JobStatus;
use crate::operations::instrumentation::StageOutcomeKind;
use crate::pipeline::completion::finalize::check::par2_damage_ignorable;
use crate::pipeline::extraction::ExtractionError;
use crate::pipeline::graph::job::GraphJob;
use crate::pipeline::graph::limits;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::{Pipeline, TerminalReconciliation, health_milli};
use crate::post_processing::model::PipelineFailureStage;

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
    let step = drive(
        job,
        &mut pipeline.graph_status.repairs,
        asker(job_id),
        limit,
    );
    pipeline.report_graph_phase(job_id);
    for woken in pipeline.graph_status.repairs.take_woken() {
        pipeline.schedule_job_completion_check(JobId(woken.0));
    }
    settle(pipeline, job_id, step).await;
}

// What an advance came to, applied to the job. A job with nothing left to
// wait for ends as its settled claims say: it fails when no file it hands
// over is whole.
pub(super) async fn settle(pipeline: &mut Pipeline, job_id: JobId, step: Step) {
    if pipeline.fail_graph_job_on_breach(job_id) {
        return;
    }
    match step {
        Step::Wait => {}
        Step::Deliver => match pipeline.graph_terminal_status(job_id) {
            Some(JobStatus::Failed { error }) => {
                // The stage that left the delivery short is the one that
                // broke, whatever the root showed.
                let stalled = match pipeline.graph_shortfall(job_id).map(|(stage, _)| stage) {
                    Some(PipelineFailureStage::Repair) => {
                        metrics::failed("repair_impossible");
                        JobStatus::Repairing
                    }
                    _ => {
                        metrics::failed("nothing_whole");
                        JobStatus::Extracting
                    }
                };
                pipeline.transition_postprocessing_status(job_id, stalled, None);
                pipeline.fail_job(job_id, error);
            }
            _ => {
                pipeline.log_deliveries(job_id);
                pipeline.remove_recovery_volumes(job_id).await;
                // A repair ruled before this delivery: the moment the e2e
                // post-repair hook names, between the ruling and the move.
                let hold = pipeline
                    .graph_recovery_summary(job_id)
                    .is_some_and(|summary| summary.repaired())
                    .then_some(crate::e2e_failpoint::DIRECT_STORE_POST_REPAIR_VERIFY);
                if let Err(error) = pipeline.start_move_to_complete_held(job_id, hold).await {
                    pipeline.fail_job(job_id, error);
                }
            }
        },
    }
}

// Every recovery set's ruling, said once, when the root is done and no set
// will rule again.
fn log_set_rulings(job_id: JobId, job: &GraphJob) {
    let mut rulings = Vec::new();
    for object in RootJob::recovery_summary(job).sets {
        rulings.clear();
        job.writer().rulings(job.tree(), object.node, &mut rulings);
        for ruling in &rulings {
            let SetRuling {
                outcome: Some(outcome),
                ..
            } = ruling
            else {
                continue;
            };
            let (outcome, slices_repaired, verification) = match *outcome {
                RepairOutcome::Waiting => continue,
                RepairOutcome::Clean if ruling.read => ("clean", 0, "read"),
                RepairOutcome::Clean => ("clean", 0, "grid"),
                RepairOutcome::Repaired { slices } => ("repaired", slices, ""),
                RepairOutcome::Impossible { .. } => ("impossible", 0, ""),
            };
            info!(
                job_id = job_id.0,
                node = object.node.0,
                kind = %object.kind,
                set = %object.name,
                recovery_set_id = %ruling.id,
                slice_size = ruling.slice_size,
                blocks_reachable = ruling.blocks_reachable,
                damaged_slices = ruling.damaged_slices,
                outcome = %outcome,
                slices_repaired = slices_repaired,
                verification = %verification,
                "recovery set ruled"
            );
        }
    }
}

fn asker(job_id: JobId) -> AskerId {
    AskerId(job_id.0)
}

impl Pipeline {
    // A graph job its extraction budget refused fails now, as classic fails
    // a job whose extraction budget refused it: at the extraction stage,
    // with the refusal as its reason. Asked after every step, so nothing
    // more is fetched or decoded for it. True when it failed.
    pub(in crate::pipeline::graph) fn fail_graph_job_on_breach(&mut self, job_id: JobId) -> bool {
        let breached = self
            .graph_jobs
            .get(&job_id)
            .is_some_and(|job| RootJob::refusal(job).is_some());
        if !breached {
            return false;
        }
        let Some((
            _,
            Some(Failure {
                rejection: Some(rejection),
                ..
            }),
        )) = self.graph_ruling(job_id)
        else {
            return false;
        };
        metrics::failed(rejection.reason().as_str());
        self.transition_postprocessing_status(job_id, JobStatus::Extracting, None);
        self.fail_job_on_extraction_error(job_id, ExtractionError::rejected(rejection));
        true
    }

    // PAR2 and PAR3 files, indexes and volumes alike, and the volumes of any
    // recovery object are spent once the job delivers and are not part of
    // the release, so they are left out of it as a classic job's are. Root
    // members are numbered as the posted files they are.
    async fn remove_recovery_volumes(&self, job_id: JobId) {
        let (Some(graph), Some(state)) = (self.graph_jobs.get(&job_id), self.jobs.get(&job_id))
        else {
            return;
        };
        for (member, path) in graph.member_files() {
            let is_recovery = graph.recovers(member)
                || state
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

    // What the root's recovery objects can say of a graph job now, read
    // off the tree whenever it is asked: health asks it between articles,
    // and the tree is what changed since.
    pub(in crate::pipeline) fn graph_recovery_summary(
        &self,
        job_id: JobId,
    ) -> Option<RecoverySummary> {
        self.graph_jobs.get(&job_id).map(RootJob::recovery_summary)
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
    }

    // The census of a graph job, from the root namespace's marks: the
    // terminal record, or the refusal of a delivery that has nothing in it.
    // `None` for a classic job, which keeps its own census.
    pub(in crate::pipeline) fn reconcile_graph_delivery(
        &mut self,
        job_id: JobId,
    ) -> Option<Result<(), String>> {
        let job = self.graph_jobs.get(&job_id)?;
        let namespace = job.namespace();
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

        let delivery = self.graph_delivery(job_id).unwrap_or_default();
        let refused = claims::refused(&census, &delivery);
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

    // Each file the job hands over, said once, as the job delivers: the
    // tree places names until then.
    fn log_deliveries(&self, job_id: JobId) {
        for file in self.graph_delivery(job_id).unwrap_or_default().files {
            info!(
                job_id = job_id.0,
                name = %file.name,
                from = %file.from,
                depth = file.depth,
                whole = file.whole,
                "member delivered"
            );
        }
    }

    // What a graph job hands over, file by file at every depth, off its
    // settled claims, with the repair failure that left any of it short.
    // `None` for a classic job.
    fn graph_ruling(&self, job_id: JobId) -> Option<(Delivery, Option<Failure>)> {
        let job = self.graph_jobs.get(&job_id)?;
        let state = self.jobs.get(&job_id)?;
        let summary = RootJob::recovery_summary(job);
        let outcomes = summary.outcomes();
        let failure = failure(&summary, &outcomes, job);
        let ignore = self.par2_ignore_extensions();
        // Root members are numbered as the posted files they are; one past
        // them is a file a recovery object announced, which is payload.
        let payload = |member: MemberId| {
            state
                .spec
                .files
                .get(member.0 as usize)
                .is_none_or(|file| file.role.counts_toward_health())
        };
        // A set whose repair failed covers the member.
        let unrepaired = |node: NodeId, member: MemberId| {
            failure.as_ref().is_some_and(|failure| {
                job.writer()
                    .covering(job.tree(), node, member)
                    .any(|set| failure.objects.contains(&set))
            })
        };
        let delivery = Delivery::read(
            job.tree(),
            payload,
            |name| par2_damage_ignorable(name, &ignore),
            unrepaired,
            |node, member| job.writer().refused(node, member),
        );
        Some((delivery, failure))
    }

    pub(in crate::pipeline) fn graph_delivery(&self, job_id: JobId) -> Option<Delivery> {
        self.graph_ruling(job_id).map(|(delivery, _)| delivery)
    }

    // Why a graph job delivered short of whole, for its terminal record: the
    // stage that left it short, and the error a recovery object that could
    // not rebuild what it found words, or else what was withheld. `None`
    // for a classic job.
    pub(in crate::pipeline) fn graph_shortfall(
        &self,
        job_id: JobId,
    ) -> Option<(PipelineFailureStage, Option<String>)> {
        let (delivery, failure) = self.graph_ruling(job_id)?;
        Some((
            delivery.shortfall_stage(),
            failure
                .map(|failure| failure.message)
                .or_else(|| delivery.withheld_error()),
        ))
    }

    // The status a graph job with nothing left to wait for ends in. `None`
    // for a classic job, which always completes.
    pub(in crate::pipeline) fn graph_terminal_status(&self, job_id: JobId) -> Option<JobStatus> {
        let (delivery, failure) = self.graph_ruling(job_id)?;
        let status = delivery.status(failure.as_ref().map(|failure| failure.message.as_str()));
        if status == JobStatus::Partial {
            info!(
                job_id = job_id.0,
                whole = ?delivery.whole,
                damaged = ?delivery.damaged,
                unrepaired = ?delivery.unrepaired,
                withheld = ?delivery.withheld,
                "the job delivered in part"
            );
        }
        Some(status)
    }

    // Show the root's phase as the job's status when it changed, and keep
    // it with the job. Reaching Done is the one moment every recovery object
    // that will ever rule has ruled: nothing waits on bytes, none is still
    // to rule, and no object below the root is still producing members that
    // could bring another. Each set's ruling and what the repair came to are
    // said then, once, as the phase is.
    pub(in crate::pipeline::graph) fn report_graph_phase(&mut self, job_id: JobId) {
        let (Some(graph), Some(state)) =
            (self.graph_jobs.get_mut(&job_id), self.jobs.get_mut(&job_id))
        else {
            return;
        };
        let Some(phase) = graph.phase_change() else {
            return;
        };
        metrics::phase(phase);
        if phase == RootPhase::Done {
            log_set_rulings(job_id, graph);
            let summary = RootJob::recovery_summary(&*graph);
            let outcomes = summary.outcomes();
            metrics::repaired(&summary, &outcomes);
            if let Some(slices_repaired) = repaired_slices(&summary, &outcomes, &*graph) {
                self.metrics
                    .job_lifecycle
                    .note_repair(StageOutcomeKind::Complete, u64::from(slices_repaired));
                let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                    job_id,
                    slices_repaired,
                });
            }
        }
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
