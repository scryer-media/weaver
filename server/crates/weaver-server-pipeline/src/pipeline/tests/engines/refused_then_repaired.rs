// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A verdict on a member's bytes is void once its set repairs them. An
// archive posted with bytes spoiled before it was cut into articles arrives
// whole by the articles' checksums, so its engine reads it at once and
// rules on the damage before the PAR2 set beside it has arrived. The set
// then rules and repairs it, and the archive is read again as the repaired
// bytes say: every member is delivered whole.

use super::harness::{JOB, Scenario};
use super::recovery_actor::serve_queued;
use crate::pipeline::tests::{
    hear_graph_mail, job_status_for_assert, pump_pipeline_runtime_queues,
};
use crate::pipeline::{
    Pipeline,
    graph::engine::{Heard, Said},
};
use weaver_graph::{MemberId, NodeId, Why, recovery::RepairOutcome};

fn articles_of(scenario: &Scenario, file: usize) -> u32 {
    scenario.posts[file].articles(scenario.article).count() as u32
}

// Serve the archive's `volumes`, all but the last article of the volume
// whose bytes are `damaged`, and hear the job's runs until the engine has
// ruled on every volume, refusing it or saying damage on it; then serve
// that last article. Every article arrives clean, so damage on a volume is
// the engine's word. Holding the volume open keeps the run's read ahead of
// its close, however late the run's thread is.
pub async fn refused_while_open(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    volumes: u32,
    damaged: u32,
) {
    let held = (damaged, articles_of(scenario, damaged as usize) - 1);
    for file in 0..volumes {
        for ordinal in 0..articles_of(scenario, file as usize) {
            if (file, ordinal) != held {
                serve_queued(pipeline, scenario, (file, ordinal)).await;
                pump_pipeline_runtime_queues(pipeline).await;
            }
        }
    }
    loop {
        let writer = job(pipeline).writer();
        let root = job(pipeline).tree().namespace(NodeId::ROOT).expect("root");
        let damaged: std::collections::BTreeSet<MemberId> =
            root.damaged_ranges().map(|(member, _)| member).collect();
        if (0..volumes).map(MemberId).all(|volume| {
            writer.refused(NodeId::ROOT, volume) == Some(Why::Unrepairable)
                || damaged.contains(&volume)
        }) {
            break;
        }
        assert!(
            job(pipeline).awaits_mail(),
            "no run is out and nothing was ruled: {:?}; refused {:?}; damaged {:?}; awaits mail {}; runs {:?}",
            job_status_for_assert(pipeline, JOB),
            (0..volumes)
                .map(|volume| writer.refused(NodeId::ROOT, MemberId(volume)))
                .collect::<Vec<_>>(),
            damaged,
            job(pipeline).awaits_mail(),
            job(pipeline).runs()
        );
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
        pump_pipeline_runtime_queues(pipeline).await;
    }
    serve_queued(pipeline, scenario, held).await;
    pump_pipeline_runtime_queues(pipeline).await;
}

// Hear every run of the job while the repair that is out runs: what the
// repair came back with is kept, unheard, until no run is live.
pub async fn runs_before_the_repair(pipeline: &mut Pipeline) -> RepairMail {
    assert!(
        job(pipeline).repair_out(),
        "{:?}",
        job_status_for_assert(pipeline, JOB)
    );
    let mut repaired = None;
    while repaired.is_none() || job(pipeline).runs().live() {
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        if matches!(said.heard, Heard::Repaired(_)) {
            repaired = Some(said);
        } else {
            pipeline.graph_run_said(said);
            pump_pipeline_runtime_queues(pipeline).await;
        }
    }
    RepairMail(repaired.expect("the repair came back"))
}

fn job(pipeline: &Pipeline) -> &crate::pipeline::graph::job::GraphJob {
    pipeline
        .graph_jobs
        .get(&JOB)
        .unwrap_or_else(|| panic!("{:?}", job_status_for_assert(pipeline, JOB)))
}

// The set's outcomes as the job reports them.
pub fn outcomes(pipeline: &Pipeline) -> Vec<Option<RepairOutcome>> {
    job(pipeline)
        .recovery_summary()
        .iter()
        .map(|set| set.outcome)
        .collect()
}

// Every member file of every namespace below the root, by path: the live
// ones and those of the objects that read the archive and were buried.
pub fn extracted(pipeline: &Pipeline) -> Vec<std::path::PathBuf> {
    let job = job(pipeline);
    let tree = job.tree();
    tree.live()
        .into_iter()
        .filter(|node| *node != NodeId::ROOT)
        .chain(tree.kept(NodeId::ROOT, MemberId(0)))
        .flat_map(|node| job.writer().files(node).map(|(_, path)| path.to_path_buf()))
        .collect()
}

pub struct RepairMail(Said);

impl RepairMail {
    pub fn acknowledge(self, pipeline: &mut Pipeline) {
        pipeline.graph_run_said(self.0);
    }
}

pub async fn hear_recovery(pipeline: &mut Pipeline) {
    hear_graph_mail(pipeline, JOB).await;
}

pub fn root_refusal(pipeline: &Pipeline, member: MemberId) -> Option<Why> {
    job(pipeline).writer().refused(NodeId::ROOT, member)
}
