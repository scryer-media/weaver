// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The pipeline's hooks into a graph job. The download and decode paths call
// these when the job has a graph runtime and know nothing more about it; a
// classic job has no runtime, so every hook is a lookup that finds nothing.
//
// The root works in volumes and ordinals; the queues work in segment ids.
// This file is where the two meet, and the only place the root's classes
// touch the queue's work items.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tracing::warn;
use weaver_graph::{ByteRange, Segment, VolumeIndex};

use super::intake::Decoded;
use super::source::{Class, Move};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobState;
use crate::pipeline::download::queue::DownloadWork;
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::engine::{Heard, Said};
use crate::pipeline::graph::job::{GraphError, GraphJob, RootFile, Storage};
use crate::pipeline::graph::limits;
use crate::pipeline::{DecodedChunk, Pipeline};

// The best priority a window volume is raised to: ahead of every ordinary
// file, behind the index and first volumes a post leads with.
const WINDOW_PRIORITY: u32 = 3;

impl Pipeline {
    // The engine the job was stamped with: a graph job is one with a graph
    // runtime, which admission and restore give it from the stamp.
    pub(in crate::pipeline) fn job_engine(&self, job_id: JobId) -> Engine {
        if self.graph_jobs.contains_key(&job_id) {
            Engine::Graph
        } else {
            Engine::Classic
        }
    }

    // Give a job admitted or restored under `engine` its graph runtime. The
    // job's state must already be in place: what its queues hold says which
    // articles this run still has to fetch.
    pub(crate) fn admit_graph_job(&mut self, job_id: JobId, engine: Engine) {
        // Read before the job is borrowed, in the order they are tried.
        let passwords: Vec<String> = if engine == Engine::Graph {
            self.archive_password_candidates_for_job(job_id)
                .candidates
                .iter()
                .map(|candidate| candidate.value().to_owned())
                .collect()
        } else {
            Vec::new()
        };
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
        };
        let Some(mut graph) = GraphJob::for_engine(engine, job_id.0, &files, storage) else {
            return;
        };
        let mut queued = std::collections::HashSet::new();
        state.download_queue.extend_segment_ids(&mut queued);
        state.recovery_queue.extend_segment_ids(&mut queued);
        graph.arrived_before(|volume, ordinal| queued.contains(&segment(job_id, volume, ordinal)));
        self.install_graph_job(job_id, graph);
    }

    // Give a job the graph runtime a restore rebuilt, or one just admitted,
    // and take its first checkpoint: from here on, a graph job with no rows
    // is one from before checkpoints.
    pub(crate) fn install_graph_job(&mut self, job_id: JobId, mut graph: GraphJob) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let moves = graph.sample();
        apply_moves(state, &moves);
        let mut parked = std::collections::HashSet::new();
        state.recovery_queue.extend_segment_ids(&mut parked);
        graph.parked(|volume, ordinal| parked.contains(&segment(job_id, volume, ordinal)));
        self.graph_jobs.insert(job_id, graph);
        self.after_graph_step(job_id);
        self.checkpoint_graph_job(job_id);
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
        let limit = self.graph_slot_limit();
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
        let result = graph.intake(decoded, limit);
        self.after_graph_result(job_id, result);
        true
    }

    // Whether the root is waiting on an article of a graph job, which is
    // the whole of whether the queue fetches it as completion work: every
    // path that rebuilds or requeues the article asks this, so a retry on
    // another server keeps the class the root gave it. `None` for a classic
    // job.
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

    // Every server failed an article of a graph job.
    pub(in crate::pipeline) fn graph_article_lost(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        let limit = self.graph_slot_limit();
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        let result = graph.lost(
            VolumeIndex(segment_id.file_id.file_index),
            segment_id.segment_number,
            limit,
        );
        self.after_graph_result(job_id, result);
    }

    // What an engine's run said. A run that ended may have been the last
    // thing the job waited on, so the completion gate looks again.
    pub(in crate::pipeline) fn graph_run_said(&mut self, said: Said) {
        let job_id = JobId(said.scope);
        let limit = self.graph_slot_limit();
        let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
            return;
        };
        let ended = matches!(said.heard, Heard::Ended);
        let result = graph.run_said(said, limit);
        self.after_graph_result(job_id, result);
        if ended {
            self.schedule_job_completion_check(job_id);
        }
    }

    // How many runs a graph job may hold now: the extraction concurrency the
    // tuner allows at this moment, the limit the repair slots follow too.
    pub(in crate::pipeline::graph) fn graph_slot_limit(&self) -> usize {
        limits::budgets(&self.direct_store.settings(), &self.tuner).1
    }

    fn after_graph_result(&mut self, job_id: JobId, result: Result<(), GraphError>) {
        if let Err(error) = result {
            warn!(job_id = job_id.0, error = %error, "graph job step failed");
        }
        // A step the job's extraction budget refused ends the job here, so
        // nothing more is fetched or decoded for it.
        if self.fail_graph_job_on_breach(job_id) {
            return;
        }
        self.after_graph_step(job_id);
        // The step's bytes are written: the checkpoint can say so.
        self.checkpoint_graph_job(job_id);
    }

    // After anything reached the tree: answer the objects' seeks, move the
    // articles they moved, and report the phase if it changed.
    fn after_graph_step(&mut self, job_id: JobId) {
        let (Some(graph), Some(state)) =
            (self.graph_jobs.get_mut(&job_id), self.jobs.get_mut(&job_id))
        else {
            return;
        };
        let round = {
            let (download, recovery) = (&state.download_queue, &state.recovery_queue);
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
        self.report_graph_phase(job_id);
    }
}

// Put each moved article in its new class. Parked recovery work joins the
// main queue, since a moved article is wanted. A class
// change re-queues the work under its new flags; a window rank re-keys it in
// place, ranked ahead of everything else at its priority.
pub(super) fn apply_moves(state: &mut JobState, moves: &[Move]) {
    if moves.is_empty() {
        return;
    }
    let job_id = state.job_id;
    let classes: HashMap<SegmentId, Class> = moves
        .iter()
        .map(|found| (segment(job_id, found.volume, found.ordinal), found.class))
        .collect();
    for (id, class) in &classes {
        if *class == Class::FirstArticle {
            state.download_queue.note_first_article(*id);
        }
    }
    let ranked =
        |work: &DownloadWork| matches!(classes.get(&work.segment_id), Some(Class::Window(_)));
    let mut taken = state
        .recovery_queue
        .extract_matching(|work| classes.contains_key(&work.segment_id));
    // An ordinary move only brings parked work back: queued work keeps its
    // place.
    let raised = |work: &DownloadWork| {
        classes
            .get(&work.segment_id)
            .is_some_and(|class| *class != Class::Ordinary)
    };
    taken.extend(
        state
            .download_queue
            .extract_matching(|work| raised(work) && !ranked(work)),
    );
    taken.sort_by_key(|work| classes.get(&work.segment_id).copied());
    for mut work in taken {
        if let Some(class) = classes.get(&work.segment_id) {
            classify(&mut work, *class);
        }
        state.download_queue.push(work);
    }
    state
        .download_queue
        .reprioritize_matching_with_rank(|work| match classes.get(&work.segment_id) {
            Some(Class::Window(rank)) => Some((work.priority.min(WINDOW_PRIORITY), Some(*rank))),
            _ => None,
        });
}

// The queue flags a class is fetched under.
fn classify(work: &mut DownloadWork, class: Class) {
    work.completion_critical |= class.waited_on();
    work.is_recovery |= class == Class::Recovery;
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
