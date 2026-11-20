// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Seeks: the RAR store's bids bringing volumes forward
// into the root's window, the PAR2 object's recovery seeks promoted over
// every bid, lost answers, a store read out of a stream, a seek lifted
// through a store's extent map, a store nothing names, an open-ended
// recovery seek, and a recovery seek standing across a restart. Where the
// engines part, the row names how.

use super::harness::{
    ARTICLE, JOB, Outcome, Post, Scenario, finished, par2_set, payload, run, serve,
};
use super::recovery_fixtures::seek::{assert_members, members, named, posts, store_set};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{
    insert_active_job, new_direct_pipeline_with_backlog, pump_pipeline_runtime_queues,
    retire_pipeline_database,
};
use std::collections::{BTreeMap, BTreeSet};

// Row 10. A graph run restarted once, the moment the recovery seek was
// answered: the pipeline drains, so the volume heads it held go to scratch
// and are read back; the restored objects restate their wants, and the run
// finishes in the order the whole run took, asking for nothing twice.
#[tokio::test]
async fn a_recovery_seek_stands_across_a_restart() {
    let (scenario, whole) = recovery_seek().await;
    let (status, order, requests) = run_restarting_once(&scenario, recovering, Stop::Drained).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(order, whole.order, "the restarted run's order");
    assert!(
        requests.values().all(|count| *count == 1),
        "the restarted run asks for nothing twice: {requests:?}"
    );
}

// Row 10 with the pipeline dropped instead of drained: the volume heads held
// in RAM are lost with it and fetched again, and nothing that had a place on
// disk is.
#[tokio::test]
async fn a_recovery_seek_across_a_crash_fetches_only_the_held_heads_again() {
    let (scenario, whole) = recovery_seek().await;
    let (status, _, requests) = run_restarting_once(&scenario, recovering, Stop::Crashed).await;
    assert_eq!(status, Some(JobStatus::Complete));
    let twice: Vec<(u32, u32)> = requests
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(article, _)| *article)
        .collect();
    assert!(!twice.is_empty(), "a crash loses the held heads");
    assert!(
        twice.iter().all(|(_, ordinal)| *ordinal == 0),
        "only volume heads are fetched again: {twice:?}"
    );
    assert!(
        requests.values().all(|count| *count <= 2),
        "nothing is fetched more than once again: {requests:?}"
    );
    assert_eq!(
        requests.keys().copied().collect::<BTreeSet<_>>(),
        whole.order.iter().copied().collect::<BTreeSet<_>>(),
        "the crashed run fetches what the whole run fetched"
    );
}

// The store and set of row 10, and the run of it with no restart.
async fn recovery_seek() -> (Scenario, Outcome) {
    let members = members(3, 151);
    let volumes = store_set(&members, 4);
    let mut posts = posts(&volumes);
    posts.extend(par2_set("silver.horizon", &named(&volumes), 2));
    let scenario = Scenario::new(posts).damaged(1, 2);
    let whole = run(&scenario).await;
    assert_members(&whole, &members);
    (scenario, whole)
}

fn recovering(pipeline: &Pipeline) -> bool {
    pipeline.jobs.get(&JOB).is_some_and(|state| {
        state
            .articles
            .active()
            .unwrap()
            .download_queue
            .count_matching(|work| work.is_recovery)
            > 0
    })
}

// How a restart takes the pipeline down: drained, as a graceful stop does,
// or dropped where it stands, as a crash does.
#[derive(Clone, Copy)]
enum Stop {
    Drained,
    Crashed,
}

// A file the set describes and no post carries is a root member the root
// announced itself, past the post's files. A run restarted once that member
// is in the checkpoint restores it beside the posted ones and rebuilds it.
#[tokio::test]
async fn a_root_member_the_set_announced_restores_across_a_restart() {
    let lantern = payload(4 * ARTICLE, 75);
    let quill = payload(3 * ARTICLE - 3, 76);
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    posts.extend(par2_set(
        "lantern",
        &[
            ("lantern.bin", lantern.as_slice()),
            ("quill.bin", quill.as_slice()),
        ],
        4,
    ));
    let posted = posts.len() as u32;
    let scenario = Scenario::new(posts);
    let announced = |pipeline: &Pipeline| {
        pipeline.graph_jobs.get(&JOB).is_some_and(|job| {
            job.tree()
                .namespace(weaver_graph::NodeId::ROOT)
                .is_some_and(|root| root.records().any(|member| member.id.0 >= posted))
        })
    };
    let (status, _, _) = run_restarting_once(&scenario, announced, Stop::Drained).await;
    assert_eq!(status, Some(JobStatus::Complete));
}

// A graph run restarted once, after the first article that leaves `due`
// true.
async fn run_restarting_once(
    scenario: &Scenario,
    due: impl Fn(&Pipeline) -> bool,
    stop: Stop,
) -> (
    Option<JobStatus>,
    Vec<(u32, u32)>,
    BTreeMap<(u32, u32), usize>,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes(), 1).await;
    // Admitted as the whole run's job is: its queues built as admission
    // builds a graph job's.
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    let mut order = Vec::new();
    let mut requests = BTreeMap::new();
    let mut restarted = false;
    loop {
        // A job whose restored recovery object is still to hear what of
        // its volumes arrived fetches nothing, as the scheduler holds it.
        while let Some(work) = pipeline
            .graph_jobs
            .get(&JOB)
            .is_none_or(|job| !job.rewalks_owed())
            .then(|| {
                pipeline
                    .jobs
                    .get_mut(&JOB)
                    .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
            })
            .flatten()
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *requests.entry(article).or_insert(0usize) += 1;
            order.push(article);
            serve(&mut pipeline, scenario, article).await;
            if !restarted && due(&pipeline) {
                restarted = true;
                pipeline = restart(&temp_dir, pipeline, scenario, stop).await;
            }
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
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
            continue;
        }
        let queued = pipeline.jobs.get(&JOB).map_or(0, |state| {
            state.articles.active().unwrap().download_queue.len()
        });
        assert!(queued > 0, "the restarted job stalled");
    }
    assert!(restarted, "the run restarted");
    let status = crate::pipeline::tests::job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (status, order, requests)
}

fn backlog_base_bytes() -> usize {
    16 * 1024 * 1024
}

// The pipeline goes down as `stop` says and a new one restores the job from
// what reached the database, as `restart.rs` does after every article.
async fn restart(
    temp_dir: &tempfile::TempDir,
    mut pipeline: Pipeline,
    scenario: &Scenario,
    stop: Stop,
) -> Pipeline {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    match stop {
        Stop::Drained => pipeline.drain().await,
        Stop::Crashed => pipeline.flush_graph_checkpoint(JOB),
    }
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(temp_dir, backlog_base_bytes(), 1).await;
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
    assert!(pipeline.graph_jobs.contains_key(&JOB), "the job restored");
    pipeline
}
