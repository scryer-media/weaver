// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that goes down around the first move a rename asks for. A file
// moves only after the checkpoint that records the name it wants has
// committed, so wherever the job goes down, the restore finds every file
// at a name its checkpoint gives it, and fetches nothing it had on disk.

use std::sync::Arc;

use super::*;
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint};

// Where the job goes down, the first time the ledger renames a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    // The step that asked for the rename ran, and the checkpoint it took
    // never committed.
    Uncommitted,
    // The checkpoint that records the name committed, and the moves it
    // lets run never ran.
    Committed,
    // The moves ran until a file sat at `.0`, and the checkpoint that says
    // so never committed.
    Moved(&'static str),
    // As `Committed`, and the job comes back to a directory it never wrote
    // at `.0`, the name the file wants, so the move fails.
    Blocked(&'static str),
}

// The job's rows as they stand once every queued write has committed.
async fn rows(pipeline: &Pipeline) -> GraphCheckpoint {
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("every step leaves a checkpoint")
}

// The database goes back to `rows`, as when every write after them was
// lost.
async fn lose_writes_after(pipeline: &Pipeline, rows: &GraphCheckpoint) {
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
fn waiting(rows: &GraphCheckpoint) -> Vec<(&str, &str)> {
    rows.members
        .iter()
        .filter_map(|member| Some((member.on_disk.as_deref()?, member.output_name.as_str())))
        .collect()
}

// The delivered names, without the job's folder.
fn delivered(restarted: &Restarted) -> Vec<&str> {
    restarted
        .files
        .keys()
        .map(|name| {
            std::path::Path::new(name)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect()
}

// What a run that went down at `stop` came to, and the checkpoint it was
// restored from.
async fn run_stopping(scenario: &Scenario, stop: Stop) -> (Restarted, GraphCheckpoint) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
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
            let was = before(&pipeline, restored.as_ref()).await;
            serve_work(&mut pipeline, scenario, &work).await;
            observe(&pipeline, &mut graph, &mut reads);
            if finished(&pipeline) {
                break;
            }
            if let Some(was) = was.filter(|_| renamed(&pipeline, &mut seen)) {
                // The article this step wrote is described by no
                // checkpoint that commits: it is fetched again.
                owed.insert(article);
                let (down, rows) =
                    go_down(&temp_dir, pipeline, scenario, stop, was, &mut owed).await;
                pipeline = down;
                restored = Some(rows);
                in_memory.extend(owed.iter().copied());
            }
        }
        if finished(&pipeline) {
            break;
        }
        let asked = requests.values().sum::<usize>();
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
            let was = before(&pipeline, restored.as_ref()).await;
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            observe(&pipeline, &mut graph, &mut reads);
            if let Some(was) = was.filter(|_| renamed(&pipeline, &mut seen)) {
                let (down, rows) =
                    go_down(&temp_dir, pipeline, scenario, stop, was, &mut owed).await;
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
    };
    (restarted, restored.expect("the ledger renamed a member"))
}

// The rows before the step under way, while the job has not gone down.
async fn before(
    pipeline: &Pipeline,
    restored: Option<&GraphCheckpoint>,
) -> Option<GraphCheckpoint> {
    match restored {
        None => Some(rows(pipeline).await),
        Some(_) => None,
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
// `was`, and a new pipeline restores it. The rows it restored from.
async fn go_down(
    temp_dir: &tempfile::TempDir,
    mut pipeline: Pipeline,
    scenario: &Scenario,
    stop: Stop,
    was: GraphCheckpoint,
    owed: &mut BTreeSet<(u32, u32)>,
) -> (Pipeline, GraphCheckpoint) {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    match stop {
        Stop::Uncommitted => lose_writes_after(&pipeline, &was).await,
        Stop::Committed => {
            pipeline.db.flush_write_queue().await.unwrap();
        }
        Stop::Blocked(name) => {
            pipeline.db.flush_write_queue().await.unwrap();
            std::fs::create_dir_all(working_dir.join(name).join("kept")).unwrap();
        }
        Stop::Moved(name) => {
            let recorded = rows(&pipeline).await;
            while !working_dir.join(name).exists() {
                let said = pipeline
                    .graph_runs
                    .recv()
                    .await
                    .expect("the commit is said");
                pipeline.graph_run_said(said);
            }
            lose_writes_after(&pipeline, &recorded).await;
        }
    }
    let mut seen = None;
    let pipeline = restart(temp_dir, pipeline, scenario, owed, &mut |rows| {
        seen = Some(rows.clone());
    })
    .await;
    (pipeline, seen.expect("the restore read a checkpoint"))
}

// The run that went down at `stop` delivers what the run that never went
// down delivers, and fetches nothing it had on disk.
async fn stopped_row(scenario: Scenario, stop: Stop) -> (GraphCheckpoint, Restarted) {
    let whole = run(&scenario).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let (restarted, rows) = run_stopping(&scenario, stop).await;
    assert_restarted(&restarted, &whole);
    (rows, restarted)
}

// One file posted under a hex name, named by the PAR3 set beside it.
fn renamed_once() -> Scenario {
    let files = [("lantern.bin", payload(5 * ARTICLE, 91))];
    Scenario::new(with_par3(&files, &["3f9a0c71e25b84d6"], "e81b6d3f0a4c9725"))
}

// One file posted under a hex name that ends in a piece number, named by
// the PAR3 set beside it: it is held as a piece of a split set until the set
// names it, and is then handed back as a file of its own and renamed.
fn renamed_from_a_piece() -> Scenario {
    let files = [("lantern.bin", payload(5 * ARTICLE, 94))];
    Scenario::new(with_par3(
        &files,
        &["3f9a0c71e25b84d6.201"],
        "e81b6d3f0a4c9725",
    ))
}

// Two files the PAR3 set names over each other: the one posted under a hex
// name is `beacon.bin`, and the one posted as `BEACON.BIN`, the same name
// to a file system that folds case, is `lantern.bin`. The second moves off
// the name before the first moves in.
fn renamed_in_a_chain() -> Scenario {
    let files = [
        ("beacon.bin", payload(2 * ARTICLE + 7, 93)),
        ("lantern.bin", payload(5 * ARTICLE, 92)),
    ];
    Scenario::new(with_par3(
        &files,
        &["a07d5e19c4b26f83", "BEACON.BIN"],
        "e81b6d3f0a4c9725",
    ))
}

// Down after the step that renamed the member, before its checkpoint
// committed: the file still sits at the name the database gives it.
#[tokio::test]
async fn a_rename_whose_checkpoint_never_committed_leaves_the_file_where_the_database_says() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Uncommitted).await;
    assert_eq!(waiting(&rows), []);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the checkpoint that records the name committed, before the
// move ran: the restore opens the file where it sat and moves it later.
#[tokio::test]
async fn a_rename_committed_but_not_moved_is_opened_where_the_file_sat() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Committed).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the checkpoint that records the name committed, and back to a
// directory at the name the file wants: the restore runs the move as it
// comes back, the move fails, and the job fails with it rather than wait
// on a move that never runs.
#[tokio::test]
async fn a_move_that_fails_as_the_job_comes_back_fails_the_job() {
    let (restarted, rows) = run_stopping(&renamed_once(), Stop::Blocked("lantern.bin")).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(
        matches!(restarted.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        restarted.status
    );
}

// Down after the move ran, before the checkpoint that says so committed:
// the file is not where it sat and is at the name the database wants, so
// the restore adopts it there.
#[tokio::test]
async fn a_file_moved_before_its_checkpoint_said_so_is_adopted_at_its_name() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Moved("lantern.bin")).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the first file left `beacon.bin` and before the second moved
// in: each restored file opens where it is, and neither is taken for the
// other.
#[tokio::test]
async fn a_chain_of_renames_down_after_the_first_move_opens_each_file_where_it_is() {
    let (rows, restarted) = stopped_row(renamed_in_a_chain(), Stop::Moved("lantern.bin")).await;
    // Both renames are recorded, and only the first file has moved.
    let mut rows = waiting(&rows);
    rows.sort();
    assert_eq!(
        rows,
        [
            ("BEACON.BIN", "lantern.bin"),
            ("a07d5e19c4b26f83", "beacon.bin")
        ]
    );
    let mut names = delivered(&restarted);
    names.sort();
    assert!(
        names.starts_with(&["beacon.bin", "lantern.bin"]),
        "{names:?}"
    );
}

// Down after the checkpoint that records the name a piece is handed back
// under committed, before any move ran: the split store kept none of the
// piece's bytes in a file of its own, and the step that renamed the piece
// handed it back, so its bytes went into the member's own file at the name
// the set gives. Nothing waits to move, and the restore opens it there.
#[tokio::test]
async fn a_piece_handed_back_and_renamed_but_not_moved_is_opened_where_it_sat() {
    let (rows, restarted) = stopped_row(renamed_from_a_piece(), Stop::Committed).await;
    // The piece is a file of the job's own folder now, bound to nothing.
    let piece = rows
        .members
        .iter()
        .find(|member| member.node == 0 && member.member == 0)
        .expect("the piece's row");
    assert_eq!(
        (piece.output_name.as_str(), piece.bound, piece.has_file),
        ("lantern.bin", None, true)
    );
    assert_eq!(waiting(&rows), []);
    assert_eq!(delivered(&restarted), ["lantern.bin"]);
}
