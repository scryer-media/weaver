// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Seeks under both engines: the RAR store's bids bringing volumes forward
// into the root's window, the PAR2 object's recovery seeks promoted over
// every bid, lost and unseekable answers, a seek lifted through a store's
// extent map, and a recovery seek standing across a restart.

use std::collections::{BTreeMap, HashSet};
use std::io::Write;

use super::harness::{
    ARTICLE, JOB, Outcome, Post, Scenario, finished, par2_set, par2_volumes, payload, run, serve,
};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::{multi_member_store_set, obfuscate_volumes};
use crate::pipeline::tests::{
    insert_active_job, new_direct_pipeline_with_buffers, pump_pipeline_runtime_queues,
    retire_pipeline_database,
};

const FOLDER: &str = "lantern beacon";

fn members(count: usize, seed: u8) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|index| {
            (
                format!("beacon{index}.bin"),
                payload(9 * ARTICLE + 7 * (index + 1), seed + index as u8),
            )
        })
        .collect()
}

fn store_set(members: &[(String, Vec<u8>)], volumes: usize) -> Vec<(String, Vec<u8>)> {
    let named: Vec<(&str, Vec<u8>)> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    multi_member_store_set(&named, volumes)
}

fn renamed(volumes: Vec<(String, Vec<u8>)>, base: &str) -> Vec<(String, Vec<u8>)> {
    volumes
        .into_iter()
        .map(|(name, bytes)| (name.replace("silver.horizon", base), bytes))
        .collect()
}

fn posts(volumes: &[(String, Vec<u8>)]) -> Vec<Post> {
    volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect()
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn find(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("the needle is in the haystack")
}

fn assert_members(outcome: &Outcome, members: &[(String, Vec<u8>)]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(name, _)| format!("{FOLDER}/{name}"))
        .collect();
    expected.sort();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected,
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in members {
        assert!(
            outcome.files[&format!("{FOLDER}/{name}")] == *bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
}

fn dump(label: &str, outcome: &Outcome) {
    eprintln!(
        "{label} {}: status {:?} failure {:?} kind {:?} refetched {:?} reads {} view {:?}\n  order {:?}\n  files {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure,
        outcome.failure_kind,
        outcome.refetched(),
        outcome.reads,
        outcome.graph,
        outcome.order,
        outcome.files.keys().collect::<Vec<_>>()
    );
}

async fn explore(label: &str, scenario: &Scenario) -> (Outcome, Outcome) {
    for (index, post) in scenario.posts.iter().enumerate() {
        eprintln!(
            "{label} post {index} {} {}",
            post.name,
            post.bytes.len().div_ceil(ARTICLE)
        );
    }
    let graph = run(scenario, Engine::Graph).await;
    dump(label, &graph);
    let classic = run(scenario, Engine::Classic).await;
    dump(label, &classic);
    (classic, graph)
}

#[tokio::test]
async fn r1_reverse() {
    let members = members(4, 11);
    let mut volumes = store_set(&members, 12);
    volumes.reverse();
    let mut posts = posts(&volumes);
    posts.insert(6, Post::plain("ember.bin", payload(6 * ARTICLE, 3)));
    explore("r1", &Scenario::new(posts)).await;
}

#[tokio::test]
async fn r2_two_stores() {
    let a = renamed(store_set(&members(4, 21), 10), "amber.dusk");
    let b = store_set(&members(4, 41), 10);
    let mut out = Vec::new();
    for (left, right) in a.iter().zip(&b) {
        out.push(Post::plain(&left.0, left.1.clone()));
        out.push(Post::plain(&right.0, right.1.clone()));
    }
    explore("r2", &Scenario::new(out)).await;
}

fn named(files: &[(String, Vec<u8>)]) -> Vec<(&str, &[u8])> {
    files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect()
}

#[tokio::test]
async fn r3_recovery_promoted() {
    let volumes = store_set(&members(3, 61), 6);
    let mut out = posts(&volumes);
    out.extend(par2_volumes("silver.horizon", &named(&volumes), 3, 3));
    explore("r3", &Scenario::new(out).damaged(3, 2)).await;
}

#[tokio::test]
async fn r4_lost_recovery_next_volume() {
    let volumes = store_set(&members(3, 71), 4);
    let mut out = posts(&volumes);
    out.extend(par2_volumes("silver.horizon", &named(&volumes), 3, 3));
    let first = super::harness::recovery_blocks(&out[5]);
    eprintln!("r4 blocks {first:?}");
    explore(
        "r4a",
        &Scenario::new(out).damaged(1, 2).lost(5, first[0][1]),
    )
    .await;
}

#[tokio::test]
async fn r4_lost_recovery_alone() {
    let volumes = store_set(&members(3, 81), 4);
    let mut out = posts(&volumes);
    out.extend(par2_set("silver.horizon", &named(&volumes), 1));
    let blocks = super::harness::recovery_blocks(&out[5]);
    eprintln!("r4b blocks {blocks:?}");
    let _ = super::harness::try_run(
        &Scenario::new(out.drain(..).collect())
            .damaged(1, 2)
            .lost(5, blocks[0][1]),
        Engine::Graph,
    )
    .await
    .map(|outcome| dump("r4b", &outcome))
    .map_err(|stall| eprintln!("r4b stall {:?} {}", stall.status, stall.state));
}

#[tokio::test]
async fn r5_lost_data() {
    let members = members(4, 91);
    let volumes = store_set(&members, 8);
    explore("r5", &Scenario::new(posts(&volumes)).lost(5, 2)).await;
}

#[tokio::test]
async fn r6_store_in_xz() {
    let members = members(3, 101);
    let volume = store_set(&members, 1).remove(0);
    let out = vec![Post::plain("silver.horizon.rar.xz", xz(&volume.1))];
    explore("r6a", &Scenario::new(out)).await;
}

#[tokio::test]
async fn r6_par2_in_store_in_xz() {
    let body = payload(6 * ARTICLE, 111);
    let set = par2_set("ember", &[("ember.bin", &body)], 2);
    let mut bad = body.clone();
    bad[2 * ARTICLE + 3] ^= 0x41;
    let inside: Vec<(String, Vec<u8>)> = vec![
        (set[0].name.clone(), set[0].bytes.clone()),
        ("ember.bin".to_string(), bad),
        (set[1].name.clone(), set[1].bytes.clone()),
    ];
    let volume = store_set(&inside, 1).remove(0);
    let out = vec![Post::plain("silver.horizon.rar.xz", xz(&volume.1))];
    explore("r6b", &Scenario::new(out)).await;
}

#[tokio::test]
async fn r7_lift_through_extent() {
    let a = payload(4 * ARTICLE, 121);
    let set = par2_set("ember", &[("ember.bin", &a)], 4);
    let (index, recovery) = (&set[0], &set[1]);
    let b_len = recovery.bytes.len() - index.bytes.len() - a.len();
    eprintln!(
        "r7 index {} recovery {} b {b_len}",
        index.bytes.len(),
        recovery.bytes.len()
    );
    let b = payload(b_len, 122);
    let inside: Vec<(String, Vec<u8>)> = vec![
        (index.name.clone(), index.bytes.clone()),
        ("ember.bin".to_string(), a.clone()),
        ("ash.bin".to_string(), b),
        ("e.par2".to_string(), recovery.bytes.clone()),
    ];
    let volumes = store_set(&inside, 2);
    let at = find(&volumes[0].1, &a[ARTICLE..2 * ARTICLE]);
    let ordinal = (at / ARTICLE + 1) as u32;
    eprintln!("r7 a at {at} damaged ordinal {ordinal}");
    explore("r7", &Scenario::new(posts(&volumes)).damaged(0, ordinal)).await;
}

#[tokio::test]
async fn r8_unbound() {
    let volumes = store_set(&members(2, 131), 2);
    let mut out = posts(&obfuscate_volumes(&volumes));
    out.swap(0, 1);
    let set = par2_set("silver.horizon", &named(&volumes), 1);
    out.extend(set);
    explore("r8", &Scenario::new(out)).await;
}

#[tokio::test]
async fn r9_open_ended() {
    let body = payload(5 * ARTICLE, 141);
    let mut out = vec![Post::plain("ember.bin", body.clone())];
    out.extend(par2_set("ember", &[("ember.bin", &body)], 1));
    explore("r9", &Scenario::new(out).damaged(0, 2)).await;
}

#[tokio::test]
async fn r10_restart() {
    let volumes = store_set(&members(3, 151), 4);
    let mut out = posts(&volumes);
    out.extend(par2_set("silver.horizon", &named(&volumes), 2));
    let scenario = Scenario::new(out).damaged(1, 2);
    let whole = run(&scenario, Engine::Graph).await;
    dump("r10 whole", &whole);
    let (status, order, requests) = run_restarting_once(&scenario).await;
    eprintln!("r10 restarted {status:?}\n  order {order:?}\n  {requests:?}");
}

// A graph run restarted once, the moment the recovery seek was answered:
// recovery work sits queued and none of it was handed out yet.
async fn run_restarting_once(
    scenario: &Scenario,
) -> (
    Option<JobStatus>,
    Vec<(u32, u32)>,
    BTreeMap<(u32, u32), usize>,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    stamp_graph(&pipeline.db);
    pipeline.admit_graph_job(JOB, Engine::Graph);
    let mut order = Vec::new();
    let mut requests = BTreeMap::new();
    let mut restarted = false;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *requests.entry(article).or_insert(0usize) += 1;
            order.push(article);
            serve(&mut pipeline, scenario, article).await;
            let promoted = pipeline
                .jobs
                .get(&JOB)
                .is_some_and(|state| state.download_queue.has_recovery_work());
            if !restarted && promoted {
                restarted = true;
                pipeline = restart(&temp_dir, pipeline, scenario).await;
            }
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.write_buffered_bytes > 0 {
            pipeline.flush_quiescent_write_backlog().await;
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
            .is_some_and(|job| job.runs().live())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let queued = pipeline
            .jobs
            .get(&JOB)
            .map_or(0, |state| state.download_queue.len());
        assert!(queued > 0, "the restarted job stalled");
    }
    assert!(restarted, "the run restarted");
    let status = crate::pipeline::tests::job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (status, order, requests)
}

fn buffers() -> crate::runtime::buffers::BufferPoolConfig {
    crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    }
}

// The pipeline goes down and a new one restores the job from what reached
// the database, as `restart.rs` does after every article.
async fn restart(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
) -> Pipeline {
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(temp_dir, buffers(), 1).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: [0; 32],
            spec: scenario.spec(),
            complete_files: HashSet::new(),
            file_progress: Default::default(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            extracted_members: HashSet::new(),
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
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&JOB), "the job restored");
    pipeline
}

// The row stamped graph as an admission stamps it.
fn stamp_graph(db: &crate::Database) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE active_jobs SET engine = {} WHERE job_id = {}",
            &[
                SqlArg::Text(Engine::Graph.as_str().to_string()),
                SqlArg::I64(JOB.0 as i64),
            ],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}
