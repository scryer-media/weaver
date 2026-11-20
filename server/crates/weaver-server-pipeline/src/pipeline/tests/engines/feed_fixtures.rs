// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::harness::{JOB, Scenario, admit};
pub use crate::pipeline::post_health::AbandonWork;
use crate::pipeline::post_health::{Gate, GivenUp, abandon_work, keep_booked};
use crate::pipeline::tests::retire_pipeline_database;
use crate::pipeline::{Pipeline, SegmentTerminalState};
use weaver_graph::VolumeIndex;
use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

fn segment(file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index,
        },
        segment_number,
    }
}

// What the abandon left: every terminal state booked, in order, the post's
// counted bytes, the verdict's files, what is still queued, the job's
// status, and every event sent, in order.
#[derive(Debug, PartialEq, Eq)]
pub struct Ended {
    pub booked: Vec<(u32, u32, String)>,
    pub failed_bytes: u64,
    pub derived_failed_bytes: u64,
    pub missing_bytes: u64,
    pub abandoned_files: Option<BTreeSet<u32>>,
    pub queued: Option<(usize, usize)>,
    pub status: String,
    pub events: Vec<String>,
}

// The post given up, then the first article of its first file lost: every
// queued article of that file is abandoned.
pub async fn abandon(scenario: &Scenario, downloading: bool) -> (AbandonWork, Ended) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    if downloading {
        // A download pass is open for the job, so the pass that drains its
        // queue closes it.
        pipeline.active_download_passes.insert(JOB);
    }
    let mut sent = pipeline.event_tx.subscribe();
    pipeline
        .post_probes
        .dead
        .insert(JOB, "fixture: the post is given up".into());
    keep_booked();
    let before = abandon_work();
    pipeline.book_terminal_segment(segment(0, 0), SegmentTerminalState::Missing);
    let after = abandon_work();
    let work = AbandonWork {
        passes: after.passes - before.passes,
        scanned: after.scanned - before.scanned,
        lost_files: after.lost_files - before.lost_files,
        checkpoints: after.checkpoints - before.checkpoints,
    };
    let booked = keep_booked()
        .into_iter()
        .map(|(segment, state)| {
            (
                segment.file_id.file_index,
                segment.segment_number,
                format!("{state:?}"),
            )
        })
        .collect();
    let mut events = Vec::new();
    loop {
        match sent.try_recv() {
            Ok(event) => events.push(format!("{event:?}")),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(error) => panic!("{error}"),
        }
    }
    let state = pipeline.jobs.get(&JOB);
    let ended = Ended {
        booked,
        failed_bytes: state.map_or(0, |state| state.failed_bytes),
        derived_failed_bytes: pipeline.derived_failed_bytes(JOB),
        missing_bytes: pipeline.missing_bytes(JOB),
        abandoned_files: pipeline
            .post_probes
            .verdicts
            .get(&JOB)
            .map(|verdict| verdict.abandoned_files.clone()),
        queued: state
            .and_then(|state| state.articles.active())
            .map(|active| (active.download_queue.len(), active.recovery_queue.len())),
        status: format!("{:?}", state.map(|state| &state.status)),
        events,
    };
    retire_pipeline_database(pipeline).await;
    (work, ended)
}

pub struct SampleStep {
    pub matches_read: Option<bool>,
    pub gate: Option<&'static str>,
    pub kept: Option<String>,
    pub walked: Option<String>,
}

// A small fixed-seed generator, so every run takes the same steps.
struct Steps(u64);

impl Steps {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % bound
    }
}

fn gate_name(gate: Gate) -> &'static str {
    match gate {
        Gate::Dead { .. } => "dead",
        Gate::Open => "open",
        Gate::Undecided => "undecided",
    }
}

// One seeded walk over the post's first articles: each step loses one,
// abandons one, loses a later article, lands one at the root, sends one
// back, lays one back as lost by a run before a restart, or drops every
// loss of the job. After each, the kept sample's verdict is the walked one,
// and the kept sample is the one a whole read finds.
pub async fn walk(
    scenario: &Scenario,
    seed: u64,
    steps: usize,
    mut inspect: impl FnMut(usize, SampleStep),
) {
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    assert!(pipeline.graph_jobs.contains_key(&JOB), "a graph job");
    let spec = pipeline.jobs.get(&JOB).unwrap().spec.clone();
    let files = spec.files.len();
    let mut steps_taken = Steps(seed);
    for step in 0..steps {
        let file = steps_taken.below(files) as u32;
        let articles = spec.files[file as usize].segments.len() as u32;
        match steps_taken.below(20) {
            0..=6 => {
                pipeline.book_terminal_segment(segment(file, 0), SegmentTerminalState::Missing);
            }
            7..=8 => {
                pipeline.book_terminal_segment(segment(file, 0), SegmentTerminalState::Abandoned);
            }
            9..=10 if articles > 1 => {
                let ordinal = 1 + steps_taken.below(articles as usize - 1) as u32;
                pipeline
                    .book_terminal_segment(segment(file, ordinal), SegmentTerminalState::Missing);
            }
            11..=14 => {
                if let Some(graph) = pipeline.graph_jobs.get_mut(&JOB) {
                    graph.arrived_before(|volume, ordinal| {
                        !(volume == VolumeIndex(file) && ordinal == 0)
                    });
                }
            }
            15..=16 => {
                let limit = pipeline.graph_job_slot_limit(JOB);
                if let Some(graph) = pipeline.graph_jobs.get_mut(&JOB) {
                    let _ = graph.send_back(VolumeIndex(file), 0, limit);
                }
            }
            17..=18 => {
                let bytes = u64::from(spec.files[file as usize].segments[0].bytes);
                pipeline
                    .given_up
                    .entry(JOB)
                    .or_insert_with(|| GivenUp::new(&spec))
                    .give_up(segment(file, 0), None, bytes);
            }
            _ => pipeline.clear_terminal_segment_failures(JOB),
        }
        // A post the feed gave up gives no verdict on either path; the walk
        // goes on weighing it.
        pipeline.post_probes.dead.remove(&JOB);
        let walked = pipeline.post_health_verdict_walked(JOB);
        let kept = pipeline.post_health_verdict(JOB);
        let (sample, read) = pipeline.first_articles_kept_and_read(JOB);
        inspect(
            step,
            SampleStep {
                matches_read: sample.map(|sample| Some(&sample) == read.as_ref()),
                gate: read.map(|sample| gate_name(sample.gate())),
                kept,
                walked,
            },
        );
    }
    retire_pipeline_database(pipeline).await;
}

pub fn lose_article(pipeline: &mut Pipeline, file: u32, ordinal: u32) {
    pipeline.book_terminal_segment(segment(file, ordinal), SegmentTerminalState::Missing);
}

pub fn heads_read() -> usize {
    crate::pipeline::post_health::heads_read()
}

pub fn walked_verdict(pipeline: &Pipeline) -> Option<String> {
    pipeline.post_health_verdict_walked(JOB)
}

pub fn kept_verdict(pipeline: &mut Pipeline) -> Option<String> {
    pipeline.post_health_verdict(JOB)
}
