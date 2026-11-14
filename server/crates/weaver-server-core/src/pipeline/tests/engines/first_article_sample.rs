// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The first-article sample the feed keeps as first articles settle or are
// lost. Whatever happens to the post's first articles, the verdict it gives
// is the one a walk of every file gives, and a lost article costs the head
// of its own file at most, never a walk of the post.

use std::collections::BTreeSet;

use weaver_graph::VolumeIndex;

use super::harness::{ARTICLE, JOB, Post, Scenario, admit, par2_set, payload};
use crate::jobs::ids::{NzbFileId, SegmentId};
use crate::pipeline::SegmentTerminalState;
use crate::pipeline::post_health::{Gate, GivenUp, heads_read};
use crate::pipeline::tests::retire_pipeline_database;

fn segment(file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index,
        },
        segment_number,
    }
}

// `files` payload files of `articles` articles each, and a PAR2 set of
// `blocks` blocks over them when `blocks` is not zero.
fn post(files: usize, articles: usize, blocks: u32) -> Scenario {
    let payloads: Vec<(String, Vec<u8>)> = (0..files)
        .map(|index| {
            (
                format!("lantern-{index:04}.mkv"),
                payload(articles * ARTICLE, index as u8),
            )
        })
        .collect();
    let mut posts: Vec<Post> = payloads
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    if blocks > 0 {
        let named: Vec<(&str, &[u8])> = payloads
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("lantern", &named, blocks));
    }
    Scenario::new(posts)
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

// What the verdicts a run compared were, so the run is known to have
// reached each of them.
#[derive(Default)]
struct Seen {
    gates: BTreeSet<&'static str>,
    verdicts: BTreeSet<&'static str>,
}

fn gate_name(gate: Gate) -> &'static str {
    match gate {
        Gate::Dead { .. } => "dead",
        Gate::Open => "open",
        Gate::Undecided => "undecided",
    }
}

fn verdict_name(verdict: &Option<String>) -> &'static str {
    match verdict {
        None => "none",
        Some(error) if error.contains("first articles are missing") => "first articles",
        Some(_) => "health",
    }
}

// One seeded walk over the post's first articles: each step loses one,
// abandons one, loses a later article, lands one at the root, sends one
// back, lays one back as lost by a run before a restart, or drops every
// loss of the job. After each, the kept sample's verdict is the walked one,
// and the kept sample is the one a whole read finds.
async fn walk(scenario: &Scenario, seed: u64, steps: usize, seen: &mut Seen) {
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
        assert_eq!(kept, walked, "seed {seed} step {step}");
        let (kept, read) = pipeline.first_articles_kept_and_read(JOB);
        if let Some(kept) = kept {
            assert_eq!(Some(&kept), read.as_ref(), "seed {seed} step {step}");
        }
        if let Some(read) = read {
            seen.gates.insert(gate_name(read.gate()));
        }
        seen.verdicts.insert(verdict_name(&walked));
    }
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn the_kept_sample_gives_the_verdict_a_walk_of_every_file_gives() {
    let mut seen = Seen::default();
    for (scenario, seeds) in [
        (post(14, 3, 0), 0..4u64),
        (post(14, 3, 6), 4..8),
        (post(12, 2, 40), 8..12),
    ] {
        for seed in seeds {
            walk(&scenario, seed, 120, &mut seen).await;
        }
    }
    assert_eq!(
        seen.gates,
        BTreeSet::from(["dead", "open", "undecided"]),
        "every gate reached"
    );
    assert_eq!(
        seen.verdicts,
        BTreeSet::from(["first articles", "health", "none"]),
        "every verdict reached"
    );
}

// Once the sample is kept, a lost article reads the head of its own file
// when it is that file's first and none otherwise, where a walk of the
// post reads every file's.
#[tokio::test]
async fn a_lost_article_reads_one_head_not_every_files() {
    let files = 1_000;
    let (_temp_dir, mut pipeline, _) = admit(&post(files, 2, 0)).await;
    pipeline.book_terminal_segment(segment(0, 0), SegmentTerminalState::Missing);
    let before = heads_read();
    pipeline.book_terminal_segment(segment(1, 1), SegmentTerminalState::Missing);
    assert_eq!(heads_read() - before, 0, "a later article");
    let before = heads_read();
    pipeline.book_terminal_segment(segment(2, 0), SegmentTerminalState::Missing);
    assert_eq!(heads_read() - before, 1, "a first article");
    let before = heads_read();
    let walked = pipeline.post_health_verdict_walked(JOB);
    assert_eq!(heads_read() - before, files, "the walk");
    assert_eq!(pipeline.post_health_verdict(JOB), walked);
    retire_pipeline_database(pipeline).await;
}
