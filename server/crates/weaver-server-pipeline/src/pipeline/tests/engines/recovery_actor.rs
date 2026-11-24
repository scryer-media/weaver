// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;

use super::harness::{GraphView, JOB, Scenario, admit, collect, finished, observe, serve};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{
    hear_graph_mail, job_status_for_assert, pump_pipeline_runtime_queues, retire_pipeline_database,
};

// Serve one article as a download lane would: taken off the job's queues as
// it is fetched, so what is still queued is what the job still owes.
pub(super) async fn serve_queued(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
) {
    if let Some(state) = pipeline.jobs.get_mut(&JOB) {
        let this = |work: &crate::pipeline::download::queue::QueuedWork<'_>| {
            work.segment_id.file_id.file_index == file && work.segment_id.segment_number == ordinal
        };
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .extract_matching(this);
        state
            .articles
            .active_mut()
            .unwrap()
            .recovery_queue
            .extract_matching(this);
    }
    serve(pipeline, scenario, (file, ordinal)).await;
}

// Run the completion gate and hear every run until the job finishes.
pub async fn drive_to_finish(pipeline: &mut Pipeline, scenario: &Scenario) {
    let mut idle = 0;
    while !finished(pipeline) {
        let mut moved = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
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
                .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            moved = true;
        }
        idle = if moved { 0 } else { idle + 1 };
        assert!(
            idle < 2 || finished(pipeline),
            "stalled at {:?}",
            job_status_for_assert(pipeline, JOB)
        );
    }
}

// Every article of the posts at `files`, in order.
pub async fn serve_posts(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    files: std::ops::Range<u32>,
) {
    for file in files {
        let post = &scenario.posts[file as usize];
        for ordinal in 0..post.articles(scenario.article).count() as u32 {
            serve_queued(pipeline, scenario, (file, ordinal)).await;
            pump_pipeline_runtime_queues(pipeline).await;
        }
    }
}

// Hear the job's runs until none is out.
pub async fn until_no_run(pipeline: &mut Pipeline) {
    while pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.runs().live())
    {
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
    }
}

// Item 2: the archive's articles served first, every one checking, and the
// job's runs heard until none is out, so the extractor has said what it
// found before any recovery set is in; then the set's articles, and the job
// driven to its end. What the job ended as, what it delivered, and the
// set's outcome and the slices its repair read back, as the job reported
// them once the set had ruled.
pub struct Heard {
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub view: GraphView,
    pub reads: u64,
}

pub async fn archive_first(scenario: &Scenario, archive: u32) -> Heard {
    let (_temp, mut pipeline, complete_dir) = admit(scenario).await;
    serve_posts(&mut pipeline, scenario, 0..archive).await;
    until_no_run(&mut pipeline).await;
    serve_posts(
        &mut pipeline,
        scenario,
        archive..scenario.posts.len() as u32,
    )
    .await;
    hear_graph_mail(&mut pipeline, JOB).await;
    let mut view = GraphView::default();
    let mut reads = 0;
    observe(&pipeline, &mut view, &mut reads);
    drive_to_finish(&mut pipeline, scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Heard {
        status,
        files,
        view,
        reads,
    }
}

fn repair_out(pipeline: &Pipeline) -> bool {
    pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.repair_out())
}

pub struct Settled {
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub asked: BTreeMap<(u32, u32), usize>,
    pub graph: GraphView,
}

// The job served one article at a time, `held` kept out as a worker keeps
// an article it asked for, until a repair is out on its thread. Then `held`
// lands, so the volume it ends settles while the repair is out, and the
// repair is heard back after. Panics when no repair went out while `held`
// was kept.
pub async fn run_settling_while_lent(scenario: &Scenario, held: (u32, u32)) -> Settled {
    let (_temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    let mut asked = BTreeMap::new();
    let mut graph = GraphView::default();
    let mut reads = 0;
    let mut kept = None;
    let mut landed = false;
    let mut quiet = 0;
    loop {
        let mut served = 0;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *asked.entry(article).or_insert(0usize) += 1;
            if article == held && !landed {
                *pipeline.articles_in_flight_by_job.entry(JOB).or_insert(0) += 1;
                kept = Some(work);
                continue;
            }
            serve(&mut pipeline, scenario, article).await;
            served += 1;
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            observe(&pipeline, &mut graph, &mut reads);
        }
        // The repair is out and has not been heard: the held article lands
        // now, under it.
        if kept.is_some() && repair_out(&pipeline) {
            kept = None;
            landed = true;
            pipeline.articles_in_flight_by_job.insert(JOB, 0);
            serve(&mut pipeline, scenario, held).await;
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
        }
        if kept.is_some() && repair_out(&pipeline) {
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        if served == 0 {
            quiet += 1;
            assert!(
                quiet < 3,
                "the job stalled at {:?} with {held:?} {}",
                job_status_for_assert(&pipeline, JOB),
                if landed { "landed" } else { "still held" },
            );
        } else {
            quiet = 0;
        }
    }
    assert!(landed, "no repair went out while {held:?} was held");
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Settled {
        status,
        files,
        asked,
        graph,
    }
}

// Serve those posts in the given order, then drive the remaining work.
pub async fn serve_posts_in_order(
    scenario: &Scenario,
    batches: impl IntoIterator<Item = std::ops::Range<u32>>,
) -> (Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
    let (_temp, mut pipeline, complete_dir) = admit(scenario).await;
    for batch in batches {
        serve_posts(&mut pipeline, scenario, batch).await;
    }
    drive_to_finish(&mut pipeline, scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    (status, files)
}

pub async fn serve_and_pump(pipeline: &mut Pipeline, scenario: &Scenario, article: (u32, u32)) {
    serve_queued(pipeline, scenario, article).await;
    pump_pipeline_runtime_queues(pipeline).await;
}

pub struct StoreObservation {
    pub kind: Option<weaver_graph::Kind>,
    pub has_engine: bool,
    pub member_has_checksum: bool,
    pub refuted_member: bool,
    pub refuted_checksummed_member: bool,
}

pub fn first_store(pipeline: &Pipeline) -> Option<StoreObservation> {
    let tree = pipeline.graph_jobs.get(&JOB)?.tree();
    let (store, _) = tree.child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))?;
    let reader = tree.namespace(store)?;
    let mut result = StoreObservation {
        kind: tree.kind(store),
        has_engine: tree.engine(store).is_some(),
        member_has_checksum: false,
        refuted_member: false,
        refuted_checksummed_member: false,
    };
    for member in reader.members() {
        let checksum = member.crc32.is_some();
        let refuted = member
            .coverage
            .runs()
            .any(|run| run.provenance == weaver_graph::Provenance::Refuted);
        result.member_has_checksum |= checksum;
        result.refuted_member |= refuted;
        result.refuted_checksummed_member |= refuted && checksum;
    }
    Some(result)
}

pub fn status(pipeline: &Pipeline, id: crate::JobId) -> Option<JobStatus> {
    job_status_for_assert(pipeline, id)
}

pub async fn finish_and_collect(
    mut pipeline: Pipeline,
    scenario: &Scenario,
    complete_dir: &std::path::Path,
) -> (Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
    drive_to_finish(&mut pipeline, scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(complete_dir, complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    (status, files)
}
