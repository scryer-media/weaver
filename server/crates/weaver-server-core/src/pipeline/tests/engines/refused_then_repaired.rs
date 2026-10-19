// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A verdict on a member's bytes is void once its set repairs them. A
// compressed RAR posted with a byte flipped in its headers arrives whole by
// its articles' checksums, so the RAR engine reads it at once and refuses it
// for the damage before the PAR2 set beside it has arrived. The set then
// rules and repairs it, and the member is read again as the repaired bytes
// say: every member of the archive is delivered whole.

use std::collections::BTreeMap;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{MemberId, NodeId, Why};

use super::harness::{JOB, Post, Scenario, admit, collect, finished, par2_volumes, serve};
use super::rar::delivered;
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::{
    debug_job_state, job_status_for_assert, pump_pipeline_runtime_queues, rar5_fixture_bytes,
    retire_pipeline_database,
};

const MIXED: &str = "rar5_multifile_lz.rar";
// A byte inside the archive's headers: the engine's read of it ends in an
// error that names no volume as damaged, so it refuses the archive.
const FLIPPED: usize = 38;
const FRAME: usize = 16;

// The archive as posted, a byte flipped, then an index and a volume made
// over it as it was before the flip.
fn posts() -> Vec<Post> {
    let original = rar5_fixture_bytes(MIXED);
    let mut posted = original.clone();
    posted[FLIPPED] ^= 0xff;
    let mut posts = vec![Post::plain("mixed.rar", posted)];
    posts.extend(par2_volumes("mixed", &[("mixed.rar", &original)], 2, 1));
    posts
}

fn articles(post: &Post) -> u32 {
    post.bytes.len().div_ceil(FRAME) as u32
}

// Hear the job's runs until the archive is refused for its damage.
async fn until_refused_unrepairable(pipeline: &mut Pipeline) {
    loop {
        let refused = pipeline
            .graph_jobs
            .get(&JOB)
            .unwrap_or_else(|| panic!("{:?}", job_status_for_assert(pipeline, JOB)))
            .writer()
            .refused(NodeId::ROOT, MemberId(0));
        if refused == Some(Why::Unrepairable) {
            break;
        }
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
        pump_pipeline_runtime_queues(pipeline).await;
    }
}

// Run the completion gate and hear every run until the job finishes.
async fn drive_to_finish(pipeline: &mut Pipeline, scenario: &Scenario) {
    let mut idle = 0;
    while !finished(pipeline) {
        let mut moved = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.download_queue.pop())
        {
            moved = true;
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve(pipeline, scenario, article).await;
        }
        if finished(pipeline) {
            break;
        }
        pipeline.check_job_completion(JOB).await;
        pump_pipeline_runtime_queues(pipeline).await;
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pump_pipeline_runtime_queues(pipeline).await;
        }
        if !finished(pipeline)
            && pipeline
                .graph_jobs
                .get(&JOB)
                .is_some_and(|job| job.runs().live())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            moved = true;
        }
        idle = if moved { 0 } else { idle + 1 };
        assert!(
            idle < 2 || finished(pipeline),
            "stalled at {:?}: {}",
            job_status_for_assert(pipeline, JOB),
            debug_job_state(pipeline, JOB)
        );
    }
}

#[tokio::test]
async fn a_member_its_engine_refused_before_its_set_repaired_it_is_read_again_under_graph() {
    let posts = posts();
    let (archive, index, volume) = (
        articles(&posts[0]),
        articles(&posts[1]),
        articles(&posts[2]),
    );
    let scenario = Scenario::new(posts).framed(FRAME);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario, Engine::Graph).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
        .is_some()
    {}

    // The archive alone: the engine refuses it before any set is in.
    for ordinal in 0..archive {
        serve(&mut pipeline, &scenario, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    until_refused_unrepairable(&mut pipeline).await;
    assert!(
        pipeline.graph_jobs[&JOB].recovery_summary().is_empty(),
        "no set is in"
    );

    // The set arrives, rules, and repairs the flipped slice.
    let set = (0..index)
        .map(|ordinal| (1, ordinal))
        .chain((0..volume).map(|ordinal| (2, ordinal)));
    for article in set {
        serve(&mut pipeline, &scenario, article).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    let outcomes: Vec<_> = pipeline
        .graph_jobs
        .get(&JOB)
        .map(|job| {
            job.recovery_summary()
                .iter()
                .map(|set| set.outcome)
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(outcomes, [Some(RepairOutcome::Repaired { slices: 1 })]);

    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        ["hello.txt", "second.txt", "zeros_64k.bin"]
            .map(delivered)
            .to_vec()
    );
    assert!(files[&delivered("zeros_64k.bin")] == vec![0u8; 64 * 1024]);
}
