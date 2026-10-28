// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline's hooks into a graph job. The download and decode paths call
// these and know nothing more about the job; a job that has gone has no
// runtime, so every hook is a lookup that finds nothing for it.
//
// The root works in volumes and ordinals; the queues work in segment ids.
// This file is where the two meet, and the only place the root's classes
// touch the queue's work items.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tracing::warn;
use weaver_graph::{ByteRange, Segment, VolumeIndex};

use super::intake::Decoded;
use super::source::{Class, Move};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobState;
use crate::pipeline::download::queue::DownloadWork;
use crate::pipeline::graph::engine::{Heard, Said};
use crate::pipeline::graph::job::{GraphError, GraphJob, RootFile, Storage};
use crate::pipeline::graph::limits;
use crate::pipeline::{DecodedChunk, Pipeline};

// The priority a window volume is raised to: ahead of every article at the
// neutral priority.
const WINDOW_PRIORITY: u32 = 3;

impl Pipeline {
    // Give a job admitted, or restored with no checkpoint, its graph
    // runtime. The job's state must already be in place: what its queues
    // hold says which articles this run still has to fetch.
    pub(crate) fn admit_graph_job(&mut self, job_id: JobId) {
        // Read before the job is borrowed, in the order they are tried.
        let passwords: Vec<String> = self
            .archive_password_candidates_for_job(job_id)
            .iter()
            .map(|candidate| candidate.value().to_owned())
            .collect();
        let slots = self.graph_slot_limit();
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let files: Vec<RootFile> = state
            .spec
            .files
            .iter()
            .map(|file| RootFile {
                name: file.filename.clone(),
                articles: file
                    .segments
                    .iter()
                    .map(|segment| (segment.ordinal, segment.bytes))
                    .collect(),
            })
            .collect();
        let caps = self.graph_caps(&files);
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let storage = Storage {
            working_dir: state.working_dir.clone(),
            accountant: Arc::clone(&self.graph_holds),
            free_disk: self
                .storage_capacity
                .reader(crate::operations::StorageRoot::Intermediate),
            slots,
            mail: self.graph_runs.sender(),
            passwords,
            caps,
            nesting: limits::NESTING,
            hardware: self.graph_hardware.clone(),
            metrics: Some(Arc::clone(&self.metrics)),
        };
        let mut graph = GraphJob::admit(job_id.0, &files, storage);
        let mut queued = std::collections::HashSet::new();
        if let Some(active) = state.articles.active() {
            active.download_queue.extend_segment_ids(&mut queued);
            active.recovery_queue.extend_segment_ids(&mut queued);
        }
        graph.arrived_before(|volume, ordinal| queued.contains(&segment(job_id, volume, ordinal)));
        self.install_graph_job(job_id, graph, Vec::new());
    }

    // Give a job the graph runtime a restore rebuilt, or one just admitted,
    // take back the held parts its checkpoint names, start the runs and
    // repairs the job's slots allow, and take its first checkpoint: from
    // here on, a graph job with no rows is one from before checkpoints. The
    // held parts are in place before that checkpoint, so it names them
    // again. Runs start only here and in later steps, on the pipeline thread
    // with the job in place, so everything a run says is heard.
    pub(crate) fn install_graph_job(
        &mut self,
        job_id: JobId,
        mut graph: GraphJob,
        held_parts: Vec<crate::pipeline::held::HeldPart>,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let moves = graph.sample();
        apply_moves(state, &moves);
        self.graph_jobs.insert(job_id, graph);
        self.adopt_held_parts(held_parts);
        let result = self.start_graph_runs(job_id);
        self.after_graph_result(job_id, result);
    }

    // Start the job's runs and lend its repairs under the slots it may hold
    // now: none while it holds no NZB slot or is paused.
    fn start_graph_runs(&mut self, job_id: JobId) -> Result<(), GraphError> {
        let limit = self.graph_job_slot_limit(job_id);
        match self.graph_jobs.get_mut(&job_id) {
            Some(graph) if limit > 0 => graph.resume_runs(&mut self.graph_status.repairs, limit),
            _ => Ok(()),
        }
    }

    // A decoded article of a graph job, as the decoder placed it. True when
    // the job is a graph job: its bytes went through the graph, which is
    // the only writer of a graph job's files, so the caller writes nothing.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::pipeline) fn graph_article_decoded(
        &mut self,
        segment_id: SegmentId,
        offset: u64,
        data: &DecodedChunk,
        intact: bool,
        positioned: bool,
        length: u64,
        segments: &[weaver_yenc::Segment],
    ) -> bool {
        let job_id = segment_id.file_id.job_id;
        let limit = self.graph_job_slot_limit(job_id);
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return false;
        };
        let decoded = Decoded {
            volume: VolumeIndex(segment_id.file_id.file_index),
            ordinal: segment_id.segment_number,
            offset,
            bytes: copy(data),
            intact,
            positioned,
            length: (positioned && length > 0).then_some(length),
            segments: segments
                .iter()
                .map(|segment| Segment {
                    range: ByteRange::at(segment.file_offset, segment.len),
                    crc32: segment.crc32,
                    verified: intact,
                })
                .collect(),
        };
        let committed = decoded.bytes.len() as u64;
        let result = graph.intake(decoded, &mut self.graph_status.repairs, limit);
        if result.is_ok() {
            self.metrics
                .bytes_committed
                .fetch_add(committed, std::sync::atomic::Ordering::Relaxed);
            self.metrics
                .segments_committed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.after_graph_result(job_id, result);
        true
    }

    // Whether the root of a graph job already has the article in hand or
    // has ruled it lost. An article the root does not hold, or a job that
    // has gone, has settled nothing.
    pub(in crate::pipeline) fn graph_article_settled(&self, segment_id: SegmentId) -> bool {
        self.graph_jobs
            .get(&segment_id.file_id.job_id)
            .is_some_and(|graph| {
                graph.article_settled(
                    VolumeIndex(segment_id.file_id.file_index),
                    segment_id.segment_number,
                )
            })
    }

    // Whether an article of a graph job will never arrive: every server
    // failed it, in this run or one before a restart.
    pub(in crate::pipeline) fn graph_article_given_up(&self, segment_id: SegmentId) -> bool {
        self.segment_terminal_states.contains_key(&segment_id)
            || self
                .graph_jobs
                .get(&segment_id.file_id.job_id)
                .is_some_and(|graph| {
                    graph.article_lost(
                        VolumeIndex(segment_id.file_id.file_index),
                        segment_id.segment_number,
                    )
                })
    }

    // Where the root of a graph job stands in laying a file in order, as
    // `ordered_front` says. `None` once the job has gone. Takes the graph
    // jobs alone, so a caller may hold another field of the pipeline.
    pub(in crate::pipeline) fn graph_ordered_front(
        graph_jobs: &std::collections::HashMap<JobId, GraphJob>,
        file_id: NzbFileId,
    ) -> Option<(u32, u64, bool)> {
        let graph = graph_jobs.get(&file_id.job_id)?;
        Some(graph.ordered_front(VolumeIndex(file_id.file_index)))
    }

    // Whether the root is waiting on an article of a graph job, which is
    // the whole of whether the queue fetches it as completion work: every
    // path that rebuilds or requeues the article asks this, so a retry on
    // another server keeps the class the root gave it. `None` once the job
    // has gone.
    pub(in crate::pipeline) fn graph_article_waited_on(
        &self,
        segment_id: SegmentId,
    ) -> Option<bool> {
        let graph = self.graph_jobs.get(&segment_id.file_id.job_id)?;
        Some(
            graph
                .article_class(
                    VolumeIndex(segment_id.file_id.file_index),
                    segment_id.segment_number,
                )
                .is_some_and(Class::waited_on),
        )
    }

    // Put an article of a graph job back on the queue under the class the
    // root holds it in now. Whatever built the work, a retry rebuilt from the
    // post's names or work leased before the root raised it, says nothing the
    // root does not. True when the job is a graph job.
    pub(in crate::pipeline) fn graph_requeued(&self, work: &mut DownloadWork) -> bool {
        let Some(graph) = self.graph_jobs.get(&work.segment_id.file_id.job_id) else {
            return false;
        };
        let class = graph.article_class(
            VolumeIndex(work.segment_id.file_id.file_index),
            work.segment_id.segment_number,
        );
        work.priority = crate::pipeline::graph::NEUTRAL_PRIORITY;
        work.is_recovery = false;
        work.completion_critical = false;
        if let Some(class) = class {
            classify(work, class);
        }
        true
    }

    // Whether the root holds an article of a graph job parked: an object
    // deferred it and nothing has wanted it since. `None` once the job has
    // gone.
    pub(in crate::pipeline) fn graph_article_parked(&self, segment_id: SegmentId) -> Option<bool> {
        let graph = self.graph_jobs.get(&segment_id.file_id.job_id)?;
        Some(
            graph.article_class(
                VolumeIndex(segment_id.file_id.file_index),
                segment_id.segment_number,
            ) == Some(Class::Parked),
        )
    }

    // Every server failed an article of a graph job. True when the job is a
    // graph job. The range is refused to the root, whose objects rule on
    // the refusal, and the feed is told the post lost an article.
    pub(in crate::pipeline) fn graph_article_lost(&mut self, segment_id: SegmentId) -> bool {
        let job_id = segment_id.file_id.job_id;
        let limit = self.graph_job_slot_limit(job_id);
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return false;
        };
        let result = graph.lost(
            VolumeIndex(segment_id.file_id.file_index),
            segment_id.segment_number,
            &mut self.graph_status.repairs,
            limit,
        );
        self.after_graph_result(job_id, result);
        self.post_lost(job_id);
        true
    }

    // What an engine's run said, or what a repair came back with. A run
    // that ended or a repair that ruled may have been the last thing the job
    // waited on, so the completion gate looks again.
    //
    // A stopped graph of a job that dropped its articles only hears its
    // mail out, starting nothing, and goes once the last of it is heard: the
    // job's next graph may be built from then on.
    pub(in crate::pipeline) fn graph_run_said(&mut self, said: Said) {
        let job_id = JobId(said.scope);
        if let Some(graph) = self.retiring_graph_jobs.get_mut(&job_id) {
            let _ = graph.run_said(said, &mut self.graph_status.repairs, 0);
            if !graph.awaits_mail() {
                self.retiring_graph_jobs.remove(&job_id);
                self.download_dispatch_wake = true;
            }
            return;
        }
        let limit = self.graph_job_slot_limit(job_id);
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        let ended = matches!(said.heard, Heard::Ended | Heard::Repaired(_));
        let result = graph.run_said(said, &mut self.graph_status.repairs, limit);
        self.after_graph_result(job_id, result);
        if ended {
            self.schedule_job_completion_check(job_id);
        }
    }

    // Everything the runs said since the last turn. A turn hears the mail
    // whole, not one piece of a run's output in a turn's spare moment: a run
    // paced by the pipeline's turns runs as slowly as they are busy.
    pub(in crate::pipeline) fn hear_graph_runs(&mut self) -> bool {
        let waiting = self.graph_runs.waiting();
        let heard = !waiting.is_empty();
        for said in waiting {
            self.graph_run_said(said);
        }
        heard
    }

    // How many runs a graph job may hold now: the extraction concurrency the
    // tuner allows at this moment, the limit the repair slots follow too, and
    // none while post-processing is paused. The pause is read as applied to
    // the executor, the state whose release walks every graph job again, so
    // a hold always has its release.
    pub(in crate::pipeline) fn graph_slot_limit(&self) -> usize {
        if self.terminal_post_processing_executor.is_paused() {
            0
        } else {
            limits::slots(&self.tuner)
        }
    }

    // How many runs and repairs the job may hold now: the pipeline's limit,
    // and none while the job is not admitted or is paused, so each of those
    // holds its extraction and repair and nothing else.
    pub(in crate::pipeline) fn graph_job_slot_limit(&self, job_id: JobId) -> usize {
        if self.nzb_admitted.contains(&job_id)
            && self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.run_state != crate::jobs::model::RunState::Paused)
        {
            self.graph_slot_limit()
        } else {
            0
        }
    }

    pub(in crate::pipeline) fn resume_graph_work(&mut self, job_id: JobId) {
        if self.graph_job_slot_limit(job_id) == 0 || !self.graph_jobs.contains_key(&job_id) {
            return;
        }
        let result = self.start_graph_runs(job_id);
        self.after_graph_result(job_id, result);
        self.schedule_job_completion_check(job_id);
    }

    pub(in crate::pipeline::graph) fn after_graph_result(
        &mut self,
        job_id: JobId,
        result: Result<(), GraphError>,
    ) {
        // What the step landed is said whatever the step came to: a file
        // whose download ended is said once, as it ended, before the job
        // fails on what the step did after.
        self.publish_graph_notes(job_id);
        // A step the job's extraction budget refused ends the job here, so
        // nothing more is fetched or decoded for it.
        if self.fail_graph_job_on_breach(job_id) {
            return;
        }
        // A step that failed otherwise fails the job with its error: what
        // it did not write, nothing after it can read.
        if let Err(error) = result {
            warn!(job_id = job_id.0, error = %error, "graph job step failed");
            self.fail_job_on_extraction_error(job_id, error.to_extraction());
            return;
        }
        self.wake_repair_waiters();
        self.after_graph_step(job_id);
        // The step's bytes are written: the checkpoint can say so.
        self.checkpoint_graph_job(job_id);
    }

    // Lend the job's due repairs under the repair slots every graph job
    // shares.
    pub(in crate::pipeline::graph) fn lend_graph_repairs(&mut self, job_id: JobId) {
        let limit = self.graph_job_slot_limit(job_id);
        if limit == 0 {
            return;
        }
        if let Some(graph) = self.graph_jobs.get_mut(&job_id)
            && let Err(error) = graph.lend(&mut self.graph_status.repairs, limit)
        {
            warn!(job_id = job_id.0, error = %error, "graph repair lending failed");
        }
        self.wake_repair_waiters();
    }

    // A slot a step gave back, or one that went to a job waiting for it,
    // wakes that job's completion gate, which lends for it in turn.
    fn wake_repair_waiters(&mut self) {
        for woken in self.graph_status.repairs.take_woken() {
            self.schedule_job_completion_check(JobId(woken.0));
        }
    }

    // After anything reached the tree: answer the objects' seeks, move the
    // articles they moved, and report the phase if it changed.
    fn after_graph_step(&mut self, job_id: JobId) {
        let (Some(graph), Some(state)) =
            (self.graph_jobs.get_mut(&job_id), self.jobs.get_mut(&job_id))
        else {
            return;
        };
        let Some(active) = state.articles.active() else {
            return;
        };
        let round = {
            let (download, recovery) = (&active.download_queue, &active.recovery_queue);
            graph.seek_round(|volume| {
                let file_id = file(job_id, volume);
                download.queued_count_for_file(file_id) + recovery.queued_count_for_file(file_id)
                    > 0
            })
        };
        match round {
            Ok(moves) => apply_moves(state, &moves),
            Err(error) => warn!(job_id = job_id.0, error = %error, "graph seek round failed"),
        }
        // What the step landed is said before the stage it moved the job to,
        // so a file landed by the step that ends the job is said first.
        self.publish_graph_notes(job_id);
        self.report_graph_phase(job_id);
    }
}

// Put each moved article in its new class. A parked article leaves the
// download queue for the recovery queue, which dispatches nothing; any other
// move brings parked work back to the main queue, since a moved article is
// wanted. A class change re-queues the work under its new flags; a window
// rank re-keys it in place, ranked ahead of everything else at its priority.
// A queue is walked only when the round holds a kind of move that can change
// it: the download queue once for a park, once for a raise and once for a
// window rank, the recovery queue for any move. Each walk reads the whole
// queue, so a round costs a pass per kind of move it holds, not one per move.
pub(super) fn apply_moves(state: &mut JobState, moves: &[Move]) {
    if moves.is_empty() {
        return;
    }
    let job_id = state.job_id;
    // Only a job holding its articles has a root to move them.
    let Some(state) = state.articles.active_mut() else {
        return;
    };
    // The latest class of each moved article, by its place in the post.
    let classes: BTreeMap<(u32, u32), Class> = moves
        .iter()
        .map(|found| ((found.volume.0, found.ordinal), found.class))
        .collect();
    let class_of = |work: &DownloadWork| {
        let id = work.segment_id;
        (id.file_id.job_id == job_id)
            .then(|| classes.get(&(id.file_id.file_index, id.segment_number)))
            .flatten()
            .copied()
    };
    let any = |pick: fn(Class) -> bool| classes.values().any(|class| pick(*class));
    for (&(volume, ordinal), class) in &classes {
        if *class == Class::FirstArticle {
            state
                .download_queue
                .note_first_article(segment(job_id, VolumeIndex(volume), ordinal));
        }
    }
    let parked = |work: &DownloadWork| class_of(work) == Some(Class::Parked);
    if any(|class| class == Class::Parked) {
        for work in state.download_queue.extract_matching(parked) {
            state.recovery_queue.push(work);
        }
    }
    let mut taken = state
        .recovery_queue
        .extract_matching(|work| class_of(work).is_some_and(|class| class != Class::Parked));
    // An ordinary move only brings parked work back: queued work keeps its
    // place. A window move re-keys in place below.
    let raised = |class: Class| class < Class::Ordinary && !matches!(class, Class::Window(_));
    if any(raised) {
        taken.extend(
            state
                .download_queue
                .extract_matching(|work| class_of(work).is_some_and(raised)),
        );
    }
    taken.sort_by_key(|work| class_of(work));
    for mut work in taken {
        if let Some(class) = class_of(&work) {
            classify(&mut work, class);
        }
        state.download_queue.push(work);
    }
    if any(|class| matches!(class, Class::Window(_))) {
        state
            .download_queue
            .reprioritize_matching_with_rank(|work| match class_of(work) {
                Some(Class::Window(rank)) => Some((WINDOW_PRIORITY, Some(rank))),
                _ => None,
            });
    }
}

// The queue flags a class is fetched under, and its priority: a window
// volume's, or the one neutral priority every other article holds.
fn classify(work: &mut DownloadWork, class: Class) {
    work.completion_critical |= class.waited_on();
    work.is_recovery |= class == Class::Recovery;
    work.priority = match class {
        Class::Window(_) => WINDOW_PRIORITY,
        _ => crate::pipeline::graph::NEUTRAL_PRIORITY,
    };
}

fn file(job_id: JobId, volume: VolumeIndex) -> NzbFileId {
    NzbFileId {
        job_id,
        file_index: volume.0,
    }
}

fn segment(job_id: JobId, volume: VolumeIndex, ordinal: u32) -> SegmentId {
    SegmentId {
        file_id: file(job_id, volume),
        segment_number: ordinal,
    }
}

fn copy(data: &DecodedChunk) -> Bytes {
    let mut bytes = BytesMut::with_capacity(data.len_bytes());
    data.for_each_slice(|slice| bytes.extend_from_slice(slice));
    bytes.freeze()
}
