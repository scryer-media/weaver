// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job restarted after every new article it is served: the pipeline is
// dropped as a crash drops it, a new one stands up over the same database
// and directories, and the job is restored from its checkpoint. The job
// must finish as a run that never restarted finishes, and the only articles
// it asks for again are the ones it held in memory when it went down.

use std::collections::{BTreeMap, BTreeSet};

use super::harness::{
    GraphView, JOB, Outcome, Post, Scenario, collect, finished, observe, par2_set, par3_set, run,
    serve_work, take_retry,
};
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use crate::jobs::model::JobStatus;
use crate::pipeline::download::queue::DownloadWork;
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_backlog,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::pipeline::{Pipeline, PipelineEvent};

pub(super) fn backlog_base_bytes() -> usize {
    16 * 1024 * 1024
}

// What a restarted run came to.
pub struct Restarted {
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Vec<u8>>,
    // What the job left in the intermediate directory: a failed job's output.
    pub retained_files: BTreeMap<String, Vec<u8>>,
    // Every article asked for again though no restart since it was last
    // served found it in memory: its bytes were on disk when asked.
    pub from_disk: BTreeSet<(u32, u32)>,
    // Every article some restart found in memory.
    pub in_memory: BTreeSet<(u32, u32)>,
    pub restarts: usize,
    // Every article in the order it was asked for.
    pub order: Vec<(u32, u32)>,
    // The graph job as last seen before it finished, across every restart.
    pub graph: GraphView,
}

// Called with each checkpoint as it stood when the pipeline went down.
pub(in crate::pipeline::tests) type Inspect<'a> =
    &'a mut dyn FnMut(&crate::jobs::graph_checkpoint::GraphCheckpoint);

pub(in crate::pipeline::tests) async fn run_restarting_inspecting(
    scenario: &Scenario,
    inspect: Inspect<'_>,
) -> Restarted {
    run_restarting_at_inspecting(scenario, inspect, Down::Crash, &|_| true).await
}

// How the pipeline goes down: as a crash drops it, shut down, or as a crash
// that also loses the file written for the member of this output name, so
// the restore finds no article holding its bytes on disk, or that cuts
// that file to the length given, as a disk that lost its tail leaves it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Down<'a> {
    Crash,
    Shutdown,
    Losing(&'a str),
    Cutting(&'a str, u64),
    // As a crash, once the object over the first root member that has one
    // refused it for its holds, as an older build's did.
    RefusingForHolds,
    // As a crash, once, the first time the job holds a volume lifted out of
    // its namespace and is not finished: between a lift and the read of the
    // set the volume rose to. Only there, not after articles.
    Lifting,
}

// As `run_restarting_inspecting`, going down as `down` says and only after the articles
// `at` picks: the nth article asked for the first time, counted from one.
pub(in crate::pipeline::tests) async fn run_restarting_at_inspecting(
    scenario: &Scenario,
    inspect: Inspect<'_>,
    down: Down<'_>,
    at: &dyn Fn(usize) -> bool,
) -> Restarted {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes(), 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;

    let mut requests = BTreeMap::new();
    let mut order = Vec::new();
    // What some restart found in memory since the article was last served.
    let mut owed = BTreeSet::new();
    let mut from_disk = BTreeSet::new();
    let mut in_memory = BTreeSet::new();
    // What the last server to answer spoiled.
    let mut spoiled = BTreeSet::new();
    let mut restarts = 0;
    let mut quiet = 0;
    let mut graph = GraphView::default();
    let mut reads = 0;
    let mut lifting = down == Down::Lifting;
    loop {
        // Down between a lift and the read of the set it rose to, once.
        if lifting
            && !finished(&pipeline)
            && pipeline
                .graph_jobs
                .get(&JOB)
                .is_some_and(|job| job.tree().lifts() > 0)
        {
            lifting = false;
            pipeline = restart(&temp_dir, pipeline, scenario, &mut owed, &mut *inspect).await;
            crate::pipeline::tests::hear_rewalks(&mut pipeline, JOB).await;
            in_memory.extend(owed.iter().copied());
            restarts += 1;
        }
        while let Some(work) = next_work(&mut pipeline, scenario) {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            order.push(article);
            let asked = requests.entry(article).or_insert(0usize);
            *asked += 1;
            let first = *asked == 1;
            // A body a server spoiled was never held whole: asking for it
            // again, of any server, is no refetch of what is on disk.
            if !owed.remove(&article) && !first && !spoiled.contains(&article) {
                from_disk.insert(article);
            }
            let server = (0..)
                .find(|server| !work.exclude_servers.contains(*server))
                .unwrap();
            if scenario.spoils(server, article) {
                spoiled.insert(article);
            } else {
                spoiled.remove(&article);
            }
            serve_work(&mut pipeline, scenario, &work).await;
            observe(&pipeline, &mut graph, &mut reads);
            if finished(&pipeline) {
                break;
            }
            // Only after an article asked for the first time: what a restart
            // fetches again comes back before the next one, so the run moves.
            // A live run is no reason to wait: the checkpoint describes what
            // is on disk, never how far a run has read.
            if first && down != Down::Lifting && at(requests.len()) {
                if down == Down::RefusingForHolds {
                    pipeline
                        .graph_jobs
                        .get_mut(&JOB)
                        .expect("the graph job")
                        .refuse_for_holds()
                        .expect("refused for its holds");
                }
                if down == Down::Shutdown {
                    shut_down(&mut pipeline).await;
                }
                let working_dir = pipeline.jobs[&JOB].working_dir.clone();
                let mut going_down = |rows: &crate::jobs::graph_checkpoint::GraphCheckpoint| {
                    if let Down::Losing(name) = down {
                        std::fs::remove_file(working_dir.join(name))
                            .expect("the member's file is on disk to lose");
                    }
                    if let Down::Cutting(name, length) = down {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(working_dir.join(name))
                            .and_then(|file| file.set_len(length))
                            .expect("the member's file is on disk to cut");
                    }
                    inspect(rows);
                };
                let figures = |pipeline: &Pipeline| {
                    pipeline
                        .job_report(JOB)
                        .map(|report| (report.remaining_bytes, report.wanted_bytes))
                };
                let before = figures(&pipeline);
                pipeline = restart(&temp_dir, pipeline, scenario, &mut owed, &mut going_down).await;
                // Its recovery reads what arrived of it again before the job
                // fetches: the figures stand once that is heard.
                crate::pipeline::tests::hear_rewalks(&mut pipeline, JOB).await;
                if let (Some(before), Some(after)) = (before, figures(&pipeline)) {
                    graph.restored_remaining.push((before, after));
                }
                let running = pipeline
                    .graph_jobs
                    .get(&JOB)
                    .is_some_and(|job| job.awaits_mail());
                let queued = pipeline.jobs.get(&JOB).is_some_and(|state| {
                    state
                        .articles
                        .active()
                        .is_some_and(|active| !active.download_queue.is_empty())
                });
                if running && queued {
                    graph.fetching_while_running += 1;
                }
                in_memory.extend(owed.iter().copied());
                restarts += 1;
            }
        }
        if finished(&pipeline) {
            break;
        }
        let before = requests.values().sum::<usize>();
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
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
        if take_retry(&mut pipeline).await {
            continue;
        }
        let queued = pipeline.jobs.get(&JOB).map_or(0, |state| {
            state.articles.active().unwrap().download_queue.len()
        });
        if queued == 0 && requests.values().sum::<usize>() == before {
            quiet += 1;
            assert!(
                quiet < 3,
                "restarted job stalled at {:?}: {:?}",
                job_status_for_assert(&pipeline, JOB),
                pipeline
                    .graph_jobs
                    .get(&JOB)
                    .map(|job| job.recovery_summary())
            );
        } else {
            quiet = 0;
        }
    }
    let status = job_status_for_assert(&pipeline, JOB);
    pipeline.db.flush_write_queue().await.unwrap();
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Restarted {
        status,
        files,
        retained_files: BTreeMap::new(),
        from_disk,
        in_memory,
        restarts,
        order,
        graph,
    }
}

// The next article the download queue hands out: the earliest of the
// scenario's `first` it holds, else its next, or with `together` the one
// whose turn comes first, a turn of each named post at a time; the rest stay
// queued.
pub(super) fn next_work(pipeline: &mut Pipeline, scenario: &Scenario) -> Option<DownloadWork> {
    let crate::jobs::model::JobState { articles, spec, .. } = pipeline.jobs.get_mut(&JOB)?;
    let queue = &mut articles.active_mut().unwrap().download_queue;
    if scenario.together.is_empty() && scenario.first.is_empty() {
        return queue.pop().map(|item| item.dispatched(spec));
    }
    let mut queued = Vec::new();
    while let Some(work) = queue.pop() {
        queued.push(work.dispatched(spec));
    }
    let article = |work: &DownloadWork| {
        (
            work.segment_id.file_id.file_index,
            work.segment_id.segment_number,
        )
    };
    let early = scenario
        .first
        .iter()
        .find_map(|wanted| queued.iter().position(|work| article(work) == *wanted));
    if early.is_some() || scenario.together.is_empty() {
        let next = (!queued.is_empty()).then(|| queued.remove(early.unwrap_or(0)));
        for work in queued {
            queue.push(work);
        }
        return next;
    }
    let turn = |work: &DownloadWork| {
        let file = work.segment_id.file_id.file_index;
        let at = scenario.together.iter().position(|named| *named == file);
        (
            work.segment_id.segment_number,
            at.unwrap_or(scenario.together.len()),
        )
    };
    let first = (0..queued.len()).min_by_key(|at| turn(&queued[*at]))?;
    let next = queued.remove(first);
    for work in queued {
        queue.push(work);
    }
    Some(next)
}

// A shutdown hands every article in flight on to the job, hears every run
// out on a thread and stops it where it stands: what a run read is on its
// shelf for the restored job, and nothing of the job is out when the
// pipeline is down.
async fn shut_down(pipeline: &mut Pipeline) {
    pipeline.drain().await;
    assert!(
        !pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail()),
        "the drain left a run or a repair of the job out on its thread"
    );
}

// The pipeline goes down with what it held in memory, and a new one restores
// the job from what reached the database.
pub(in crate::pipeline::tests) async fn restart(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    in_memory: &mut BTreeSet<(u32, u32)>,
    inspect: Inspect<'_>,
) -> Pipeline {
    restart_heard(temp_dir, pipeline, scenario, in_memory, inspect)
        .await
        .0
}

// As `restart`, with every event the new pipeline sends from before it
// restores the job.
pub(super) async fn restart_heard(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    in_memory: &mut BTreeSet<(u32, u32)>,
    inspect: Inspect<'_>,
) -> (Pipeline, tokio::sync::broadcast::Receiver<PipelineEvent>) {
    let mut pipeline = pipeline;
    // These restarts exercise a committed step. The coalescing tests cover
    // the coverage a crash loses before the writer's barrier is due.
    pipeline.flush_graph_checkpoint(JOB);
    in_memory.extend(pipeline.graph_jobs[&JOB].in_memory());
    // Boxed: the restore's future is large, and a caller's frame holds it
    // once already.
    Box::pin(restart_as_it_stands(temp_dir, pipeline, scenario, inspect)).await
}

// The pipeline dropped as it stands and the next made over the same
// folders, the job restored from the database's rows: a restart of a job
// whose graph is not live, as one held for a full disk.
pub(in crate::pipeline::tests) async fn restart_as_it_stands(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    inspect: Inspect<'_>,
) -> (Pipeline, tokio::sync::broadcast::Receiver<PipelineEvent>) {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);

    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(temp_dir, backlog_base_bytes(), 1).await;
    let heard = pipeline.event_tx.subscribe();
    scenario.prepare(&mut pipeline);
    let rows = pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("the forced checkpoint committed");
    inspect(&rows);
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: [0; 32],
            spec: scenario.spec(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
            restart: None,
        })
        .await
        .unwrap();
    assert!(
        !pipeline.blocked_restores.contains_key(&JOB),
        "a checkpointed graph job restores"
    );
    assert!(
        pipeline.graph_jobs.contains_key(&JOB)
            || (pipeline.pending_graph_terminals.contains_key(&JOB)
                && pipeline.retiring_graph_jobs.contains_key(&JOB))
            || matches!(
                job_status_for_assert(&pipeline, JOB),
                Some(JobStatus::Failed { .. })
            ),
        "the restored graph is active or has failed; each scenario checks its expected outcome"
    );
    (pipeline, heard)
}

// The restarted run finished as the uninterrupted one did, and asked again
// only for what some restart since it last fetched it had in memory.
pub fn assert_restarted(restarted: &Restarted, whole: &Outcome) {
    assert!(restarted.restarts > 0, "the run restarted");
    assert_eq!(restarted.status, whole.status, "status");
    assert_eq!(
        restarted.files.keys().collect::<Vec<_>>(),
        whole.files.keys().collect::<Vec<_>>(),
        "delivered names"
    );
    for (name, bytes) in &whole.files {
        assert!(restarted.files[name] == *bytes, "{name}: delivered bytes");
    }
    assert!(
        restarted.from_disk.is_empty(),
        "fetched again though on disk: {:?}",
        restarted.from_disk
    );
}

// The run with no restart is given back, for a row to ask more of it.
pub async fn restart_row(scenario: Scenario) -> Outcome {
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting_inspecting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
    whole
}

pub const SECRET: &str = "lantern-restart-secret";

pub fn with_par2(mut posts: Vec<Post>, blocks: u32) -> Vec<Post> {
    let files: Vec<(String, Vec<u8>)> = posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    posts.extend(par2_set("lantern", &named, blocks));
    posts
}

pub struct CheckpointView<'a>(&'a crate::jobs::graph_checkpoint::GraphCheckpoint);

impl CheckpointView<'_> {
    pub fn has_lifted_members(&self) -> bool {
        self.0.members.iter().any(|member| member.lifted.is_some())
    }
}

pub async fn run_restarting(scenario: &Scenario) -> Restarted {
    run_restarting_inspecting(scenario, &mut |_| {}).await
}

pub async fn run_restarting_at(
    scenario: &Scenario,
    inspect: &mut dyn FnMut(CheckpointView<'_>),
    down: Down<'_>,
    at: &dyn Fn(usize) -> bool,
) -> Restarted {
    run_restarting_at_inspecting(
        scenario,
        &mut |rows| inspect(CheckpointView(rows)),
        down,
        at,
    )
    .await
}

// Each file posted under `names[i]`, then one PAR3 set describing them by
// their own names, posted under `set`.
pub fn with_par3(files: &[(&str, Vec<u8>)], names: &[&str], set: &str) -> Vec<Post> {
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .zip(names)
        .map(|((_, bytes), name)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par3_set(set, &described, 2));
    posts
}
