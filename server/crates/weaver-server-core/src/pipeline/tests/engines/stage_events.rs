// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The events a graph job's run says, which the history timeline builds its
// stages from: each posted file that lands, and each stage the job enters
// and leaves.

use sevenz_turbo::encoder_options::Lzma2Options;

use super::harness::{ARTICLE, JOB, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar::posts;
use crate::events::model::PipelineEvent;
use crate::pipeline::tests::direct_store::single_member_store_set;

// The run's stage events as words a row can compare.
fn stages(outcome: &Outcome) -> Vec<String> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::JobVerificationStarted { job_id } if *job_id == JOB => {
                Some("JobVerificationStarted".to_string())
            }
            PipelineEvent::JobVerificationComplete { job_id, passed } if *job_id == JOB => {
                Some(format!("JobVerificationComplete {passed}"))
            }
            PipelineEvent::RepairStarted { job_id } if *job_id == JOB => {
                Some("RepairStarted".to_string())
            }
            PipelineEvent::RepairComplete { job_id, .. } if *job_id == JOB => {
                Some("RepairComplete".to_string())
            }
            PipelineEvent::RepairFailed { job_id, .. } if *job_id == JOB => {
                Some("RepairFailed".to_string())
            }
            PipelineEvent::ExtractionReady { job_id } if *job_id == JOB => {
                Some("ExtractionReady".to_string())
            }
            PipelineEvent::ExtractionComplete { job_id } if *job_id == JOB => {
                Some("ExtractionComplete".to_string())
            }
            _ => None,
        })
        .collect()
}

// The verify, repair and extract lanes the run left held.
fn lanes(outcome: &Outcome) -> (usize, usize, usize) {
    (
        outcome.metrics.verify_active,
        outcome.metrics.repair_active,
        outcome.metrics.extract_active,
    )
}

// The files the run said landed, by name, in the order they landed.
fn landed(outcome: &Outcome) -> Vec<(String, u64)> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::FileComplete {
                file_id,
                filename,
                total_bytes,
            } if file_id.job_id == JOB => Some((filename.clone(), *total_bytes)),
            _ => None,
        })
        .collect()
}

// A two-volume store set beside a PAR2 set, one article of its first volume
// damaged: every posted file lands once, at its posted size. The member
// streams out of the store volumes as they arrive, so no extraction stage
// is said; the repair of the damaged slice follows, and is said complete as
// the job ends.
#[tokio::test]
async fn a_repaired_store_set_says_each_file_and_each_stage() {
    let body = payload(7 * ARTICLE + 13, 91);
    let volumes = single_member_store_set("ember.bin", &body, 2);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut all = posts(volumes.clone());
    all.extend(par2_set("ember", &described, 2));
    let scenario = Scenario::new(all).damaged(0, 2);
    let mut expected: Vec<(String, u64)> = scenario
        .posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.len() as u64))
        .collect();
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let mut files = landed(&graph);
    files.sort();
    expected.sort();
    assert_eq!(files, expected, "each posted file lands once");
    assert_eq!(
        lanes(&graph),
        (0, 0, 0),
        "the job leaves no verify, repair or extract lane held"
    );
    assert_eq!(stages(&graph), ["RepairStarted", "RepairComplete"]);
}

// A coded 7z the engine reads after it lands: the job enters extraction and
// leaves it as it ends.
#[tokio::test]
async fn a_coded_7z_says_its_extraction() {
    let archive = super::sevenz::container(Lzma2Options::from_level(1).into(), None);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        landed(&graph),
        [(
            "lantern.7z".to_string(),
            scenario.posts[0].bytes.len() as u64
        )]
    );
    assert_eq!(stages(&graph), ["ExtractionReady", "ExtractionComplete"]);
    let articles = scenario.posts[0].bytes.len().div_ceil(ARTICLE) as u64;
    assert_eq!(
        graph.metrics.bytes_committed,
        scenario.posts[0].bytes.len() as u64
    );
    assert_eq!(graph.metrics.segments_committed, articles);
    assert_eq!(lanes(&graph), (0, 0, 0));
}
