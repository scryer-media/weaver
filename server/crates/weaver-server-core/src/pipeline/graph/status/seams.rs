// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline's hooks into a graph job's status.

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
use crate::pipeline::graph::job::{GraphJob, Note};
use crate::pipeline::graph::limits;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::{Pipeline, TerminalReconciliation, health_milli};
use crate::post_processing::model::PipelineFailureStage;

// The completion gate hands the job here: there is no whole-file
// verification, no listing check and no re-scan of the output, since the
// tree already ruled on every member as it arrived.
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
    // A slot handed to the job while it waited goes to the repair it could
    // not lend for want of one, so the verdict below waits on that repair
    // rather than taking its slot. Every step lends what it made due.
    if pipeline.graph_status.repairs.handed(asker(job_id)) {
        pipeline.lend_graph_repairs(job_id);
    }
    let limit = limits::slots(&pipeline.tuner);
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
            Some(JobStatus::Failed { error })
                if pipeline.post_probes.dead.get(&job_id) == Some(&error) =>
            {
                // Nothing came whole of a post the feed gave up: it failed
                // at its download, in the feed's words.
                metrics::failed("post_dead");
                pipeline
                    .pipeline_failure_stages
                    .entry(job_id)
                    .or_insert(PipelineFailureStage::Download);
                pipeline.fail_job_on_health(job_id, error);
            }
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
                if pipeline.graph_failure_is_health(job_id) {
                    pipeline.fail_job_on_health(job_id, error);
                } else {
                    pipeline.fail_job(job_id, error);
                }
            }
            _ => {
                pipeline.record_validated_archive_password(job_id);
                pipeline.log_deliveries(job_id);
                pipeline.remove_spent_members(job_id).await;
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
                RepairOutcome::Waiting | RepairOutcome::Unread => continue,
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
    // A graph job its extraction budget refused fails now, at the
    // extraction stage, with the refusal as its reason. Asked after every
    // step, so nothing more is fetched or decoded for it. True when it
    // failed.
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
        self.metrics
            .note_extraction_rejection(rejection.reason().as_str());
        self.transition_postprocessing_status(job_id, JobStatus::Extracting, None);
        self.fail_job_on_extraction_error(job_id, ExtractionError::rejected(rejection));
        true
    }

    // A graph job its rulings settle before its download ends fails now,
    // in the words of the sets that could not rebuild what they cover, at
    // the repair stage. True when it failed.
    pub(in crate::pipeline) fn fail_graph_job_ruled_short(&mut self, job_id: JobId) -> bool {
        let Some(failure) = self.graph_jobs.get(&job_id).and_then(super::ruled_short) else {
            return false;
        };
        metrics::failed("repair_impossible");
        self.transition_postprocessing_status(job_id, JobStatus::Repairing, None);
        self.fail_job(job_id, failure.message);
        true
    }

    // What the job spent is not part of the release and is removed once it
    // delivers, before the move: recovery data, bound or named so, and the
    // volumes an object consumed to make what it hands over, such as the
    // pieces a join wrote out whole.
    async fn remove_spent_members(&self, job_id: JobId) {
        let Some(graph) = self.graph_jobs.get(&job_id) else {
            return;
        };
        for path in graph.spent_files() {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(file = %path.display(), error = %error, "could not remove a spent member");
                }
            }
        }
    }

    // What the root's recovery objects can say of a graph job now, read
    // off the tree whenever it is asked.
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

    // A job going away gives back the repair slot it holds, a repair still
    // out among them, and its place in the queue for one.
    pub(in crate::pipeline) fn forget_graph_repair(&mut self, job_id: JobId) {
        if let Some(ticket) = self
            .graph_jobs
            .get_mut(&job_id)
            .and_then(GraphJob::forget_slot)
        {
            self.graph_status.repairs.release(ticket);
        }
        self.graph_status.repairs.forget(asker(job_id));
    }

    // The terminal record of the job's delivery, from the census of its
    // root namespace at the last gate before the payload leaves. Returns the
    // operator-facing failure message when the census refuses the delivery.
    pub(in crate::pipeline) fn reconcile_terminal_delivery(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        let (Some(job), Some(state)) = (self.graph_jobs.get(&job_id), self.jobs.get(&job_id))
        else {
            return Ok(());
        };
        let namespace = job.namespace();
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
                // An article the feed stopped asking for was never lost.
                row.lost_articles = self
                    .segment_terminal_states
                    .iter()
                    .filter(|(segment, state)| {
                        segment.file_id == file_id
                            && **state != crate::pipeline::SegmentTerminalState::Abandoned
                    })
                    .count();
                row.lost_bytes = self.file_terminal_failed_bytes(file_id);
                row.declared_bytes = file.segments.iter().map(|s| u64::from(s.bytes)).sum();
                Some(row)
            })
            .collect();
        let total_bytes = state.spec.total_bytes;
        metrics::claimed(&census);
        // A file that lost articles on its way in is one missing file, with
        // every article it never received, however often the census runs.
        for row in census.iter().filter(|row| row.lost_articles > 0) {
            let file_id = NzbFileId {
                job_id,
                file_index: row.member.0,
            };
            if self.files_counted_missing.insert(file_id) {
                self.metrics
                    .job_lifecycle
                    .note_file_missing(row.lost_articles as u64);
            }
        }

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
            return Err(message);
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
        Ok(())
    }

    // The password an archive of the job opened with, kept sealed in the
    // job's metadata as the job delivers, so the history can say which
    // candidate worked. Nothing is kept when no archive needed one.
    fn record_validated_archive_password(&mut self, job_id: JobId) {
        let Some(password) = self
            .graph_jobs
            .get(&job_id)
            .and_then(GraphJob::opening_password)
        else {
            return;
        };
        let sealed = match self.db.seal_validated_archive_password(password) {
            Ok(sealed) => sealed,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "could not seal the validated archive password");
                return;
            }
        };
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let key = crate::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY;
        state.spec.metadata.retain(|(name, _)| name != key);
        state.spec.metadata.push((key.to_string(), sealed));
        let update = crate::jobs::JobUpdate {
            metadata: crate::jobs::FieldUpdate::Set(state.spec.metadata.clone()),
            ..Default::default()
        };
        if let Err(error) = self.db.update_active_job(job_id, &update) {
            warn!(job_id = job_id.0, error = %error, "could not persist the validated archive password");
        }
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
    // `None` once the job has gone.
    fn graph_ruling(&self, job_id: JobId) -> Option<(Delivery, Option<Failure>)> {
        let job = self.graph_jobs.get(&job_id)?;
        let state = self.jobs.get(&job_id)?;
        let summary = RootJob::recovery_summary(job);
        let outcomes = summary.outcomes();
        // The rulings' own failure, or else the feed's words when it gave
        // the post up.
        let failure = failure(&summary, &outcomes, job).or_else(|| {
            self.post_probes.dead.get(&job_id).map(|words| Failure {
                objects: Vec::new(),
                message: words.clone(),
                rejection: None,
            })
        });
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
        // A set whose repair failed covers the member: each failed set with
        // the namespace it lives in, as the summary read them once.
        let failed: Vec<(NodeId, NodeId)> = failure
            .as_ref()
            .map(|failure| {
                summary
                    .sets
                    .iter()
                    .filter(|set| failure.objects.contains(&set.node))
                    .map(|set| (set.namespace, set.node))
                    .collect()
            })
            .unwrap_or_default();
        let unrepaired = |node: NodeId, member: MemberId| {
            failed.iter().any(|(namespace, set)| {
                *namespace == node && job.writer().covers(job.tree(), *set, member)
            })
        };
        let delivery = Delivery::read(
            job.tree(),
            payload,
            |name| par2_damage_ignorable(name, &ignore),
            unrepaired,
            |node, member| job.writer().refused(node, member),
            // Read by the name the member is delivered under, so a file a
            // recovery set renamed to what it is counts as that.
            |node, member| {
                node == NodeId::ROOT
                    && job
                        .tree()
                        .namespace(node)
                        .and_then(|ns| ns.get(member))
                        .is_some_and(|member| {
                            crate::pipeline::post_health::names_a_piece(&member.output_name)
                        })
            },
        );
        Some((delivery, failure))
    }

    // Whether a graph job that delivered nothing whole failed for its post:
    // a recovery object could not rebuild what it found, or what it lacks
    // never arrived. A member an object withheld, a password it was not
    // given or a refusal of the job's extraction budget is the extraction's
    // failure, not the post's.
    pub(in crate::pipeline) fn graph_failure_is_health(&self, job_id: JobId) -> bool {
        self.graph_ruling(job_id)
            .is_some_and(|(delivery, failure)| match failure {
                Some(failure) => failure.rejection.is_none(),
                None => delivery.withheld.is_empty(),
            })
    }

    pub(in crate::pipeline) fn graph_delivery(&self, job_id: JobId) -> Option<Delivery> {
        self.graph_ruling(job_id).map(|(delivery, _)| delivery)
    }

    // Why a graph job delivered short of whole, for its terminal record: the
    // stage that left it short, and the error a recovery object that could
    // not rebuild what it found words, or else what was withheld. `None`
    // once the job has gone.
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
    // once the job has gone.
    pub(in crate::pipeline) fn graph_terminal_status(&self, job_id: JobId) -> Option<JobStatus> {
        let (delivery, failure) = self.graph_ruling(job_id)?;
        let status = delivery.status(failure.as_ref());
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
    // said then, once, as the phase is. The job leaves the root there, and
    // its health probe with it: nothing is downloading for a round to rule
    // on.
    pub(in crate::pipeline::graph) fn report_graph_phase(&mut self, job_id: JobId) {
        if !self.jobs.contains_key(&job_id) {
            return;
        }
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        let before = graph.reported_phase();
        let Some(phase) = graph.phase_change() else {
            return;
        };
        metrics::phase(phase);
        let stage = stage_events(job_id, before, phase, &RootJob::recovery_summary(&*graph));
        if phase == RootPhase::Done {
            self.post_probes.states.remove(&job_id);
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
        for event in stage {
            self.emit_job_event(event);
        }
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let entered = shown_status(phase, &state.status);
        if let Some(status) = entered.clone() {
            let lane = state.post_state;
            state.status = status;
            state.refresh_runtime_lanes_from_status();
            self.metrics.post_state_moved(lane, state.post_state);
        }
        if let Err(error) =
            self.db
                .try_queue_job_write(job_id, "save_active_job_root_phase", move |db| {
                    db.save_active_job_root_phase(job_id, phase.as_str())
                })
        {
            warn!(job_id = job_id.0, error = %error, "could not queue the root phase write");
        }
        // The status the job entered, as the e2e hooks name it, once the
        // phase it shows is queued for the database. Extracting is held on
        // the engine run that does it, off this loop.
        let hook = match entered {
            Some(JobStatus::Verifying) => Some(crate::e2e_failpoint::STATUS_ENTER_VERIFYING),
            Some(JobStatus::Repairing) => Some(crate::e2e_failpoint::STATUS_ENTER_REPAIRING),
            _ => None,
        };
        if let Some(hook) = hook {
            crate::e2e_failpoint::maybe_delay(hook);
            crate::e2e_failpoint::maybe_trip(hook);
        }
    }

    // What the job's last step did, as its events: a posted file that
    // landed is a file complete and the queue scripts' FILE_DOWNLOADED.
    pub(in crate::pipeline::graph) fn publish_graph_notes(&mut self, job_id: JobId) {
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        for note in graph.take_notes() {
            match note {
                Note::Landed(volume) => {
                    let Some(file) = self
                        .jobs
                        .get(&job_id)
                        .and_then(|state| state.spec.files.get(volume.0 as usize))
                    else {
                        continue;
                    };
                    let event = PipelineEvent::FileComplete {
                        file_id: NzbFileId {
                            job_id,
                            file_index: volume.0,
                        },
                        filename: file.filename.clone(),
                        total_bytes: file
                            .segments
                            .iter()
                            .map(|segment| u64::from(segment.bytes))
                            .sum(),
                    };
                    self.raise_queue_script_event(
                        job_id,
                        crate::post_processing::model::QueueEvent::FileDownloaded,
                        None,
                    );
                    self.emit_job_event(event);
                }
            }
        }
    }
}

// The stage events a change of the job's phase says: the stage it left
// ends, and the one it entered starts. Verification passed when it led to
// no repair and every set vouched; a repair failed when a set it left
// behind could not be rebuilt.
fn stage_events(
    job_id: JobId,
    before: Option<RootPhase>,
    phase: RootPhase,
    summary: &RecoverySummary,
) -> Vec<PipelineEvent> {
    let mut events = Vec::new();
    match before {
        Some(RootPhase::Verifying) => events.push(PipelineEvent::JobVerificationComplete {
            job_id,
            passed: phase != RootPhase::Repairing && summary.settled(),
        }),
        Some(RootPhase::Repairing) => {
            if let Some((damaged, reachable)) =
                summary
                    .outcomes()
                    .into_iter()
                    .find_map(|outcome| match outcome {
                        RepairOutcome::Impossible { damaged, reachable } => {
                            Some((damaged, reachable))
                        }
                        _ => None,
                    })
            {
                events.push(PipelineEvent::RepairFailed {
                    job_id,
                    error: format!(
                        "not repairable: {damaged} damaged slices, only {reachable} recovery blocks reachable"
                    ),
                });
            }
        }
        Some(RootPhase::Extracting) => events.push(PipelineEvent::ExtractionComplete { job_id }),
        Some(RootPhase::Downloading | RootPhase::Done) | None => {}
    }
    match phase {
        RootPhase::Verifying => events.push(PipelineEvent::JobVerificationStarted { job_id }),
        RootPhase::Repairing => events.push(PipelineEvent::RepairStarted { job_id }),
        RootPhase::Extracting => events.push(PipelineEvent::ExtractionReady { job_id }),
        RootPhase::Downloading | RootPhase::Done => {}
    }
    events
}
