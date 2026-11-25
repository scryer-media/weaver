// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that goes down around the first move a rename asks for. A file
// moves only once the job is ready to deliver, and then only after the
// checkpoint that records the name it wants has committed, so wherever the
// job goes down, the restore finds every file at a name its checkpoint gives
// it, and fetches nothing it had on disk.

use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use std::sync::Arc;

use super::archive_scenario_fixtures::sets::{namespace_move_counts, namespace_rename_count};
use super::harness::{
    GraphView, JOB, Scenario, collect, finished, observe, run, serve_work, take_retry,
};
use super::restart_fixtures::{
    Restarted, assert_restarted, backlog_base_bytes, next_work, restart,
};
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint};
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_backlog,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use std::collections::{BTreeMap, BTreeSet};

// Where the job goes down, the first time the ledger renames a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    // The step that asked for the rename ran, and the checkpoint it took
    // never committed.
    Uncommitted,
    // The checkpoint that records the name committed, and the moves it
    // lets run never ran.
    Committed,
    // The job was ready to deliver, its moves ran until a file sat at
    // `.0`, and the checkpoint that says so never committed.
    Moved(&'static str),
    // The job was ready to deliver, `.0` of its files moved, and no
    // checkpoint since the one taken as the delivery began committed.
    Moves(u64),
    // As `Committed`, and the job comes back to a directory it never wrote
    // and cannot remove at `.0`, the name the file wants, so the move fails.
    Blocked(&'static str),
}

// A directory the job never wrote that holds a file it cannot remove while
// this lives: the restore cannot sweep it, and a move onto its name fails.
struct Unremovable {
    #[cfg(unix)]
    dir: std::path::PathBuf,
    #[cfg(windows)]
    _held: std::fs::File,
}

impl Unremovable {
    fn at(dir: std::path::PathBuf) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("kept");
        std::fs::write(&file, b"kept").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
            Self { dir }
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            let _held = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(&file)
                .unwrap();
            Self { _held }
        }
    }
}

impl Drop for Unremovable {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o755));
        }
    }
}

// The job's rows as they stand once every queued write has committed.
async fn rows(pipeline: &mut Pipeline) -> GraphCheckpoint {
    pipeline.flush_graph_checkpoint(JOB);
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("the forced checkpoint committed")
}

// The database goes back to `rows`, as when every write after them was
// lost. What the job holds and has not yet written goes the same way: it is
// written first, so the restart that stops the job finds nothing left to
// write over `rows`.
async fn lose_writes_after(pipeline: &mut Pipeline, rows: &GraphCheckpoint) {
    pipeline.flush_graph_checkpoint(JOB);
    pipeline.db.flush_write_queue().await.unwrap();
    let mut delta = Delta {
        whole: true,
        ..Delta::default()
    };
    delta.compare(&GraphCheckpoint::default(), rows);
    pipeline
        .db
        .save_graph_checkpoint(JOB, Arc::new(delta))
        .unwrap();
}

// Each member the checkpoint says sits at one name and wants another: the
// name it sits at and the name it wants.
pub fn waiting(rows: &MoveCheckpoint) -> Vec<(&str, &str)> {
    rows.0
        .members
        .iter()
        .filter_map(|member| Some((member.on_disk.as_deref()?, member.output_name.as_str())))
        .collect()
}

thread_local! {
    static STARTUP_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// How many restores on this thread brought back a job with files waiting to
// move and nothing left to fetch, each found queued for its check.
pub fn startup_checks() -> u64 {
    STARTUP_CHECKS.with(std::cell::Cell::get)
}

// When the job goes down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    // After the first step that renamed a member of the root.
    Rename,
    // After the first step that renamed a member with a file, in any
    // namespace.
    AnyRename,
    // At the first completion check that found the job ready to deliver,
    // with files waiting to move.
    Delivery,
}

// What a run that went down at `stop` came to, and the checkpoint it was
// restored from. A file moves only once the job is ready to deliver, so a
// run that goes down once a file moved goes down then; any other goes down
// at the first rename of a root member.
pub async fn run_stopping(scenario: &Scenario, stop: Stop) -> (Restarted, MoveCheckpoint) {
    let at = match stop {
        Stop::Moved(_) | Stop::Moves(_) => At::Delivery,
        _ => At::Rename,
    };
    run_stopping_at(scenario, stop, at).await
}

// As `run_stopping`, going down at `at`.
pub async fn run_stopping_at(
    scenario: &Scenario,
    stop: Stop,
    at: At,
) -> (Restarted, MoveCheckpoint) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes(), 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;

    let mut requests = BTreeMap::new();
    let mut order = Vec::new();
    let mut owed = BTreeSet::new();
    let mut from_disk = BTreeSet::new();
    let mut in_memory = BTreeSet::new();
    let mut graph = GraphView::default();
    let mut reads = 0;
    let mut quiet = 0;
    let mut restored = None;
    let mut seen = BTreeMap::new();
    let mut blocker = None;
    loop {
        while let Some(work) = next_work(&mut pipeline, scenario) {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            order.push(article);
            let asked = requests.entry(article).or_insert(0usize);
            *asked += 1;
            if !owed.remove(&article) && *asked > 1 {
                from_disk.insert(article);
            }
            let was = before(&mut pipeline, restored.as_ref()).await;
            let renames = namespace_rename_count();
            serve_work(&mut pipeline, scenario, &work).await;
            observe(&pipeline, &mut graph, &mut reads);
            if finished(&pipeline) {
                break;
            }
            if let Some(was) = was.filter(|_| renamed_at(at, &pipeline, &mut seen, renames)) {
                // The article this step wrote is described by no
                // checkpoint that commits: it is fetched again.
                owed.insert(article);
                let (down, rows) = go_down(
                    &temp_dir,
                    pipeline,
                    scenario,
                    stop,
                    (was, namespace_move_counts().0),
                    &mut owed,
                    &mut blocker,
                )
                .await;
                pipeline = down;
                restored = Some(rows);
                in_memory.extend(owed.iter().copied());
            }
        }
        if finished(&pipeline) {
            break;
        }
        let asked = requests.values().sum::<usize>();
        let was = before(&mut pipeline, restored.as_ref()).await;
        let renames = namespace_rename_count();
        let moved_from = namespace_move_counts().0;
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        if let Some(was) = was.clone().filter(|_| {
            at == At::Delivery
                && pipeline
                    .graph_jobs
                    .get(&JOB)
                    .is_some_and(|job| job.delivering_names() && job.moves_waiting())
        }) {
            let (down, rows) = go_down(
                &temp_dir,
                pipeline,
                scenario,
                stop,
                (was, moved_from),
                &mut owed,
                &mut blocker,
            )
            .await;
            pipeline = down;
            restored = Some(rows);
            in_memory.extend(owed.iter().copied());
            continue;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
        }
        // The check and the queues it pumped step the job too: a rename
        // there goes down as one in an article's step does.
        if let Some(was) = was.filter(|_| renamed_at(at, &pipeline, &mut seen, renames)) {
            let (down, rows) = go_down(
                &temp_dir,
                pipeline,
                scenario,
                stop,
                (was, namespace_move_counts().0),
                &mut owed,
                &mut blocker,
            )
            .await;
            pipeline = down;
            restored = Some(rows);
            in_memory.extend(owed.iter().copied());
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let was = before(&mut pipeline, restored.as_ref()).await;
            let renames = namespace_rename_count();
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            observe(&pipeline, &mut graph, &mut reads);
            if let Some(was) = was.filter(|_| renamed_at(at, &pipeline, &mut seen, renames)) {
                let (down, rows) = go_down(
                    &temp_dir,
                    pipeline,
                    scenario,
                    stop,
                    (was, namespace_move_counts().0),
                    &mut owed,
                    &mut blocker,
                )
                .await;
                pipeline = down;
                restored = Some(rows);
                in_memory.extend(owed.iter().copied());
            }
            continue;
        }
        if take_retry(&mut pipeline).await {
            continue;
        }
        let queued = pipeline
            .jobs
            .get(&JOB)
            .and_then(|state| state.articles.active())
            .map_or(0, |active| active.download_queue.len());
        if queued == 0 && requests.values().sum::<usize>() == asked {
            quiet += 1;
            assert!(
                quiet < 3,
                "restarted job stalled at {:?}",
                job_status_for_assert(&pipeline, JOB)
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
    let restarted = Restarted {
        status,
        files,
        retained_files: BTreeMap::new(),
        from_disk,
        in_memory,
        restarts: usize::from(restored.is_some()),
        order,
        graph,
        events: Vec::new(),
    };
    (
        restarted,
        MoveCheckpoint(restored.expect("the ledger renamed a member")),
    )
}

// The rows before the step under way, while the job has not gone down.
async fn before(
    pipeline: &mut Pipeline,
    restored: Option<&GraphCheckpoint>,
) -> Option<GraphCheckpoint> {
    match restored {
        None => Some(rows(pipeline).await),
        Some(_) => None,
    }
}

// Whether the step just taken is where the job goes down at `at`: the
// ledger renamed a member, of the root, or of any namespace since the
// writer's rename count read `renames`.
fn renamed_at(at: At, pipeline: &Pipeline, seen: &mut BTreeMap<u32, String>, renames: u64) -> bool {
    match at {
        At::Rename => renamed(pipeline, seen),
        At::AnyRename => namespace_rename_count() > renames,
        At::Delivery => false,
    }
}

// Whether a root member's output name changed since it was first seen:
// the ledger renamed it.
fn renamed(pipeline: &Pipeline, seen: &mut BTreeMap<u32, String>) -> bool {
    let Some(job) = pipeline.graph_jobs.get(&JOB) else {
        return false;
    };
    let mut renamed = false;
    for index in 0..pipeline.jobs[&JOB].spec.files.len() as u32 {
        let Some(member) = job.namespace().get(weaver_graph::MemberId(index)) else {
            continue;
        };
        let first = seen
            .entry(index)
            .or_insert_with(|| member.output_name.clone());
        renamed |= *first != member.output_name;
    }
    renamed
}

// The job goes down at `stop`, from the step whose rows before it were
// `was`, with `moved_from` moves run then, and a new pipeline restores it. The rows it restored from.
async fn go_down(
    temp_dir: &tempfile::TempDir,
    mut pipeline: Pipeline,
    scenario: &Scenario,
    stop: Stop,
    (was, moved_from): (GraphCheckpoint, u64),
    owed: &mut BTreeSet<(u32, u32)>,
    blocker: &mut Option<Unremovable>,
) -> (Pipeline, GraphCheckpoint) {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    match stop {
        Stop::Uncommitted => lose_writes_after(&mut pipeline, &was).await,
        Stop::Committed => {
            pipeline.db.flush_write_queue().await.unwrap();
        }
        Stop::Blocked(name) => {
            pipeline.db.flush_write_queue().await.unwrap();
            *blocker = Some(Unremovable::at(working_dir.join(name)));
        }
        Stop::Moved(name) => {
            // Each move runs as the continuation of a commit, so each is
            // heard in the run's mail; the job is ready to deliver, so every
            // move is under way. The first can run inside the checkpoint the
            // delivery takes: what records the moves is lost from the rows
            // `was` holds, taken before the delivery began.
            let recorded = was;
            while !working_dir.join(name).exists() {
                let said = pipeline
                    .graph_runs
                    .recv()
                    .await
                    .expect("the commit is said");
                pipeline.graph_run_said(said);
            }
            lose_writes_after(&mut pipeline, &recorded).await;
        }
        Stop::Moves(moves) => {
            // Each move runs once the checkpoint that records the move
            // before it has committed, so what the database holds as the
            // `moves`th move runs is every write before the mail that let
            // it run; what records it is lost. The first move can run inside
            // the checkpoint the delivery takes, before this is reached:
            // the moves count from the rows `was` holds.
            let mut committed = was;
            while namespace_move_counts().0 - moved_from < moves {
                pipeline.db.flush_write_queue().await.unwrap();
                committed = pipeline
                    .db
                    .load_graph_checkpoint(JOB)
                    .unwrap()
                    .expect("a checkpoint committed");
                let said = pipeline
                    .graph_runs
                    .recv()
                    .await
                    .expect("the commit is said");
                pipeline.graph_run_said(said);
            }
            lose_writes_after(&mut pipeline, &committed).await;
        }
    }
    let mut seen = None;
    let pipeline = restart(temp_dir, pipeline, scenario, owed, &mut |rows| {
        seen = Some(rows.clone());
    })
    .await;
    // A job restored with files waiting to move and nothing left to fetch
    // hears no article that would check it: the restore itself queues the
    // check that delivers it, as for every job it restores.
    if pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.moves_waiting() && !job.delivering_names())
        && !pipeline.jobs[&JOB].articles.has_queued_download_work()
    {
        STARTUP_CHECKS.with(|checks| {
            checks.set(checks.get() + 1);
        });
        assert!(
            pipeline.pending_completion_checks.contains(&JOB),
            "the restore checks a job with nothing left to fetch"
        );
    }
    (pipeline, seen.expect("the restore read a checkpoint"))
}

// The run that went down at `stop` delivers what the run that never went
// down delivers, and fetches nothing it had on disk.
pub async fn stopped_row(scenario: Scenario, stop: Stop) -> (MoveCheckpoint, Restarted) {
    // Boxed: each run's future is large, and two of them in one frame are
    // past a thread's stack where it is smallest.
    let whole = Box::pin(run(&scenario)).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let (restarted, rows) = Box::pin(run_stopping(&scenario, stop)).await;
    assert_restarted(&restarted, &whole);
    (rows, restarted)
}

pub struct MoveCheckpoint(GraphCheckpoint);

impl MoveCheckpoint {
    pub fn member(&self, node: i64, member: i64) -> Option<(&str, Option<i64>, bool)> {
        self.0
            .members
            .iter()
            .find(|row| row.node == node && row.member == member)
            .map(|row| (row.output_name.as_str(), row.bound, row.has_file))
    }
}

pub async fn restart_with_recorded_envelope(
    temp: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    mut inspect: impl FnMut(String),
) -> Pipeline {
    restart(
        temp,
        pipeline,
        scenario,
        &mut BTreeSet::new(),
        &mut |rows| {
            let engine = rows
                .nodes
                .iter()
                .find(|node| node.kind == "rar_engine")
                .expect("the complete compressed-member header bound an engine");
            let envelope = rows
                .envelopes
                .iter()
                .find(|envelope| envelope.node == engine.node)
                .expect("the engine kept its input");
            let name = envelope.file.clone().unwrap_or_else(|| {
                format!(
                    ".weaver-chunks/graph-envelope-{}-{}",
                    envelope.node, envelope.volume
                )
            });
            inspect(name);
        },
    )
    .await
}

pub fn awaits_graph_mail(pipeline: &Pipeline) -> bool {
    pipeline.graph_jobs[&JOB].awaits_mail()
}
pub async fn hear_one(pipeline: &mut Pipeline, reason: &str) {
    let said = pipeline.graph_runs.recv().await.expect(reason);
    pipeline.graph_run_said(said);
}
