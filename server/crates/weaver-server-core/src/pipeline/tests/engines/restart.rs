// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job restarted after every new article it is served: the pipeline is
// dropped as a crash drops it, a new one stands up over the same database
// and directories, and the job is restored from its checkpoint. The job
// must finish as a run that never restarted finishes, and the only articles
// it asks for again are the ones it held in memory when it went down.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Write;

use super::harness::{
    ARTICLE, JOB, Outcome, Post, Scenario, collect, finished, par2_set, payload, run, serve,
};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};

const SECRET: &str = "lantern-restart-secret";

fn buffers() -> crate::runtime::buffers::BufferPoolConfig {
    crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    }
}

// What a restarted run came to.
struct Restarted {
    status: Option<JobStatus>,
    files: BTreeMap<String, Vec<u8>>,
    requests: BTreeMap<(u32, u32), usize>,
    // Every article some restart found in memory.
    in_memory: BTreeSet<(u32, u32)>,
    restarts: usize,
}

// Called with each checkpoint as it stood when the pipeline went down.
type Inspect<'a> = &'a mut dyn FnMut(&crate::jobs::graph_checkpoint::GraphCheckpoint);

async fn run_restarting(scenario: &Scenario, inspect: Inspect<'_>) -> Restarted {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    if !scenario.global_passwords.is_empty() {
        pipeline
            .db
            .save_archive_password_settings(Some(scenario.global_passwords.clone()), None)
            .unwrap();
    }
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    stamp_graph(&pipeline.db);
    pipeline.admit_graph_job(JOB, Engine::Graph);
    assert!(pipeline.is_graph_job(JOB));

    let mut requests = BTreeMap::new();
    let mut in_memory = BTreeSet::new();
    let mut restarts = 0;
    let mut quiet = 0;
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
            let asked = requests.entry(article).or_insert(0usize);
            *asked += 1;
            let first = *asked == 1;
            serve(&mut pipeline, scenario, article).await;
            if finished(&pipeline) {
                break;
            }
            // Only after an article asked for the first time: what a restart
            // fetches again comes back before the next one, so the run moves.
            let idle = first
                && pipeline
                    .graph_jobs
                    .get(&JOB)
                    .is_some_and(|job| !job.runs().live());
            if idle {
                pipeline = restart(&temp_dir, pipeline, scenario, &mut in_memory, inspect).await;
                restarts += 1;
            }
        }
        if finished(&pipeline) {
            break;
        }
        let before = requests.values().sum::<usize>();
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
        requests,
        in_memory,
        restarts,
    }
}

// The pipeline goes down with what it held in memory, and a new one restores
// the job from what reached the database.
async fn restart(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    in_memory: &mut BTreeSet<(u32, u32)>,
    inspect: Inspect<'_>,
) -> Pipeline {
    in_memory.extend(pipeline.graph_jobs[&JOB].in_memory());
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);

    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(temp_dir, buffers(), 1).await;
    let rows = pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .expect("every step leaves a checkpoint");
    inspect(&rows);
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
    assert!(
        !pipeline.blocked_restores.contains_key(&JOB),
        "a checkpointed graph job restores"
    );
    assert!(pipeline.graph_jobs.contains_key(&JOB));
    pipeline
}

// The restarted run finished as the uninterrupted one did, and asked again
// only for what it had in memory.
fn assert_restarted(restarted: &Restarted, whole: &Outcome) {
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
    let again: Vec<_> = restarted
        .requests
        .iter()
        .filter(|(article, count)| **count > 1 && !restarted.in_memory.contains(article))
        .map(|(article, _)| *article)
        .collect();
    assert!(again.is_empty(), "fetched again though on disk: {again:?}");
}

async fn restart_row(scenario: Scenario) {
    let whole = run(&scenario, Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn with_par2(mut posts: Vec<Post>, blocks: u32) -> Vec<Post> {
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

fn volumes(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

#[tokio::test]
async fn a_plain_file_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("ember.bin", payload(9 * ARTICLE + 21, 71))],
        2,
    );
    restart_row(Scenario::new(posts).damaged(0, 3)).await;
}

#[tokio::test]
async fn an_xz_stream_under_par2_restarted_after_every_article_replays_from_its_start() {
    let posts = with_par2(
        vec![Post::plain(
            "ember.bin.xz",
            xz(&payload(40 * ARTICLE + 5, 72)),
        )],
        2,
    );
    restart_row(Scenario::new(posts)).await;
}

#[tokio::test]
async fn a_three_volume_stored_rar_restarted_after_every_article_delivers_its_member() {
    let body = payload(11 * ARTICLE + 9, 73);
    restart_row(Scenario::new(volumes(single_member_store_set(
        "beacon.bin",
        &body,
        3,
    ))))
    .await;
}

#[tokio::test]
async fn a_stored_rar_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let body = payload(8 * ARTICLE + 17, 74);
    let posts = with_par2(volumes(single_member_store_set("beacon.bin", &body, 2)), 2);
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

#[tokio::test]
async fn an_xz_stream_inside_a_rar_volume_restarted_after_every_article_delivers_the_file() {
    let compressed = xz(&payload(30 * ARTICLE + 3, 75));
    restart_row(Scenario::new(volumes(single_member_store_set(
        "beacon.bin.xz",
        &compressed,
        2,
    ))))
    .await;
}

// A job stamped graph whose checkpoint is gone is not restored wrong: it is
// held back as a restore that could not be made.
#[tokio::test]
async fn a_graph_job_with_no_checkpoint_is_refused_at_restore() {
    let scenario = Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE, 76))]);
    let temp_dir = tempfile::tempdir().unwrap();
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
        let working_dir = insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
        stamp_graph(&pipeline.db);
        retire_pipeline_database(pipeline).await;
        working_dir
    };
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    let _ = pipeline
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
        .await;
    assert!(pipeline.blocked_restores.contains_key(&JOB));
    assert!(!pipeline.graph_jobs.contains_key(&JOB));
}

// The row stamped graph as an admission stamps it, with no checkpoint
// written beside it.
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

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

// No checkpoint of an encrypted job holds the password or any key derived
// from it, in any row or any object's facts, and the job still finishes
// across restarts by being offered the password again.
#[tokio::test]
async fn no_checkpoint_of_an_encrypted_rar_holds_its_password_or_keys() {
    // The salt and KDF count every encrypted store fixture is written with.
    let material = unrar_rs::derive_rar5_material(SECRET, &[0x5A; 16], 4).unwrap();
    let secrets: Vec<Vec<u8>> = vec![
        SECRET.as_bytes().to_vec(),
        material.key.to_vec(),
        material.hash_key.to_vec(),
    ];
    let body = payload(6 * ARTICLE + 11, 77);
    let scenario = Scenario::new(volumes(encrypted_store_set(
        "beacon.bin",
        &body,
        2,
        SECRET,
        Some(SECRET),
        false,
    )))
    .password(SECRET);
    let mut seen = 0;
    let restarted = run_restarting(&scenario, &mut |rows| {
        seen += 1;
        let mut blobs: Vec<Vec<u8>> = Vec::new();
        for node in &rows.nodes {
            blobs.push(node.facts.clone());
            blobs.extend(node.volumes.iter().cloned());
            blobs.push(node.kind.as_bytes().to_vec());
            blobs.extend(node.reason.iter().map(|reason| reason.as_bytes().to_vec()));
            blobs.extend(
                node.winning_password
                    .iter()
                    .map(|index| index.to_le_bytes().to_vec()),
            );
        }
        for member in &rows.members {
            blobs.push(member.name.as_bytes().to_vec());
            blobs.push(member.output_name.as_bytes().to_vec());
        }
        for crypt in &rows.crypt {
            blobs.push(crypt.seed.clone());
        }
        for blob in &blobs {
            for secret in &secrets {
                assert!(
                    !contains(blob, secret),
                    "a checkpoint row holds key material"
                );
            }
        }
    })
    .await;
    assert!(seen > 0);
    let whole = run(&scenario, Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert_restarted(&restarted, &whole);
}
