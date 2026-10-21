// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job restarted after every new article it is served: the pipeline is
// dropped as a crash drops it, a new one stands up over the same database
// and directories, and the job is restored from its checkpoint. The job
// must finish as a run that never restarted finishes, and the only articles
// it asks for again are the ones it held in memory when it went down.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use sevenz_turbo::EncoderMethod;

use super::harness::{
    ARTICLE, JOB, Outcome, Post, Scenario, collect, finished, par2_set, par3_set, payload, run,
    serve,
};
use super::sfv::{crc, listing};
use super::streams::{brotli, bzip2, gzip, members, tar, xz, zstd};
use super::zip;
use crate::jobs::model::JobStatus;
use crate::pipeline::download::queue::DownloadWork;
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::pipeline::{Pipeline, PipelineEvent};

const SECRET: &str = "lantern-restart-secret";

fn buffers() -> crate::runtime::buffers::BufferPoolConfig {
    crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    }
}

// What a restarted run came to.
pub(super) struct Restarted {
    pub(super) status: Option<JobStatus>,
    pub(super) files: BTreeMap<String, Vec<u8>>,
    pub(super) requests: BTreeMap<(u32, u32), usize>,
    // Every article some restart found in memory.
    pub(super) in_memory: BTreeSet<(u32, u32)>,
    pub(super) restarts: usize,
}

// Called with each checkpoint as it stood when the pipeline went down.
pub(super) type Inspect<'a> = &'a mut dyn FnMut(&crate::jobs::graph_checkpoint::GraphCheckpoint);

pub(super) async fn run_restarting(scenario: &Scenario, inspect: Inspect<'_>) -> Restarted {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    stamp_graph(&pipeline.db);
    pipeline.admit_graph_job(JOB, Engine::Graph);
    assert!(pipeline.is_graph_job(JOB));

    let mut requests = BTreeMap::new();
    let mut in_memory = BTreeSet::new();
    let mut restarts = 0;
    let mut quiet = 0;
    loop {
        while let Some(work) = next_work(&mut pipeline, scenario) {
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
            // A live run is no reason to wait: the checkpoint describes what
            // is on disk, never how far a run has read.
            if first {
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
            .is_some_and(|job| job.awaits_mail())
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

// The next article the download queue hands out: its next, or with
// `together` the one whose turn comes first, a turn of each named post at
// a time; the rest stay queued.
fn next_work(pipeline: &mut Pipeline, scenario: &Scenario) -> Option<DownloadWork> {
    let queue = &mut pipeline.jobs.get_mut(&JOB)?.download_queue;
    if scenario.together.is_empty() {
        return queue.pop();
    }
    let mut queued = Vec::new();
    while let Some(work) = queue.pop() {
        queued.push(work);
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

// The pipeline goes down with what it held in memory, and a new one restores
// the job from what reached the database.
async fn restart(
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
async fn restart_heard(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    in_memory: &mut BTreeSet<(u32, u32)>,
    inspect: Inspect<'_>,
) -> (Pipeline, tokio::sync::broadcast::Receiver<PipelineEvent>) {
    in_memory.extend(pipeline.graph_jobs[&JOB].in_memory());
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let db = pipeline.db.clone();
    retire_pipeline_database(pipeline).await;
    drop(db);

    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(temp_dir, buffers(), 1).await;
    let heard = pipeline.event_tx.subscribe();
    scenario.prepare(&mut pipeline);
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
    (pipeline, heard)
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

pub(super) async fn restart_row(scenario: Scenario) {
    let whole = run(&scenario, Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
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

// Every event `pipeline` sent since `sent` subscribed, kept in order.
fn drain(
    sent: &mut tokio::sync::broadcast::Receiver<PipelineEvent>,
    into: &mut Vec<PipelineEvent>,
) {
    loop {
        match sent.try_recv() {
            Ok(event) => into.push(event),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(error) => panic!("the run's events were not all kept: {error}"),
        }
    }
}

// What a run restarted once after its repair came to.
struct RestartedDone {
    status: Option<JobStatus>,
    files: BTreeMap<String, Vec<u8>>,
    // Every event either pipeline sent from admission on, in order.
    events: Vec<PipelineEvent>,
}

// The job served as `run` serves it, and the pipeline taken down once, the
// first time the tree is done with its repair announced and the job's next
// completion check, which starts the move, has not run.
async fn run_restarted_once_done(scenario: &Scenario) -> RestartedDone {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp_dir, buffers(), 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    stamp_graph(&pipeline.db);
    pipeline.admit_graph_job(JOB, Engine::Graph);
    let mut sent = pipeline.event_tx.subscribe();
    let mut events = Vec::new();
    let mut in_memory = BTreeSet::new();
    let mut restarted = false;
    let mut quiet = 0;
    let mut asked = 0usize;
    loop {
        while let Some(work) = next_work(&mut pipeline, scenario) {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            asked += 1;
            serve(&mut pipeline, scenario, article).await;
        }
        if finished(&pipeline) {
            break;
        }
        // Down once the repair is announced and before the job's next
        // completion check starts its move.
        drain(&mut sent, &mut events);
        let announced = events.iter().any(
            |event| matches!(event, PipelineEvent::RepairComplete { job_id, .. } if *job_id == JOB),
        );
        let done = pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.phase() == RootPhase::Done);
        if !restarted && announced && done {
            (pipeline, sent) =
                restart_heard(&temp_dir, pipeline, scenario, &mut in_memory, &mut |_| {}).await;
            restarted = true;
            continue;
        }
        let before = asked;
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
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        if asked == before {
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
    assert!(restarted, "the tree was done before the job finished");
    let status = job_status_for_assert(&pipeline, JOB);
    pipeline.db.flush_write_queue().await.unwrap();
    drain(&mut sent, &mut events);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    RestartedDone {
        status,
        files,
        events,
    }
}

// A slice repaired, the repair announced, and the pipeline down before the
// job is through. The restored job is done as soon as it is restored, and
// what it announced before going down is not announced again: one
// `RepairComplete` for the one repair, then the job's completion.
#[tokio::test]
async fn a_repaired_job_restarted_after_its_repair_announces_the_repair_once() {
    let body = payload(9 * ARTICLE + 21, 71);
    let posts = with_par2(vec![Post::plain("ember.bin", body.clone())], 2);
    let restarted = run_restarted_once_done(&Scenario::new(posts).damaged(0, 3)).await;
    assert_eq!(restarted.status, Some(JobStatus::Complete));
    assert_eq!(
        restarted.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/ember.bin".to_string()]
    );
    assert!(restarted.files["lantern beacon/ember.bin"] == body);
    let said: Vec<String> = restarted
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RepairComplete {
                job_id,
                slices_repaired,
            } if *job_id == JOB => Some(format!("RepairComplete {slices_repaired}")),
            PipelineEvent::JobCompleted { job_id } if *job_id == JOB => {
                Some("JobCompleted".to_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(said, ["RepairComplete 1", "JobCompleted"]);
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
pub(in crate::pipeline::tests) fn stamp_graph(db: &crate::Database) {
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

// No row of the checkpoint, nor any object's facts, holds any of `secrets`.
fn assert_no_secret(rows: &crate::jobs::graph_checkpoint::GraphCheckpoint, secrets: &[Vec<u8>]) {
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
    for part in &rows.parts {
        blobs.push(part.bytes.to_vec());
    }
    for blob in &blobs {
        for secret in secrets {
            assert!(
                !contains(blob, secret),
                "a checkpoint row holds key material"
            );
        }
    }
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
        assert_no_secret(rows, &secrets);
    })
    .await;
    assert!(seen > 0);
    let whole = run(&scenario, Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert_restarted(&restarted, &whole);
}

// The RARLAB archive with a compressed member, cut fine enough to restart
// between its articles: the engine's run replays from the volume's start
// over the partials on disk, so a restart re-reads and never fetches.
#[tokio::test]
async fn a_compressed_rar5_volume_restarted_after_every_article_replays_the_engine_from_disk() {
    let bytes = crate::pipeline::tests::rar5_fixture_bytes("rar5_multifile_lz.rar");
    restart_row(Scenario::new(vec![Post::plain("mixed.rar", bytes)]).framed(64)).await;
}

// The five encrypted RARLAB volumes, offered their password again at every
// restore, in 64 KiB articles so a restart lands inside every volume.
#[tokio::test]
async fn the_encrypted_five_volume_rar5_set_restarted_after_every_article_delivers_its_member() {
    let set = (1..=5)
        .map(|part| {
            (
                format!("clip.part{part}.rar"),
                crate::pipeline::tests::rar5_fixture_bytes(&format!(
                    "rar5_enc_mv_video.part{part}.rar"
                )),
            )
        })
        .collect();
    restart_row(
        Scenario::new(volumes(set))
            .framed(64 * 1024)
            .password("testpass123"),
    )
    .await;
}

// Two members in one 7z coded by `method`, cut into `count` volumes.
fn sevenz(method: EncoderMethod, count: usize) -> Vec<Post> {
    let entries = [
        Entry::file("ember.bin", payload(7 * ARTICLE + 11, 78)),
        Entry::file("quill.bin", payload(4 * ARTICLE - 3, 79)),
    ];
    volumes(split_volumes(
        &build_7z_shaped(&entries, method, None, false),
        count,
    ))
}

#[tokio::test]
async fn a_stored_7z_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(sevenz(EncoderMethod::COPY, 1))).await;
}

#[tokio::test]
async fn a_three_volume_stored_7z_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(sevenz(EncoderMethod::COPY, 3))).await;
}

// The engine's run replays its folder from the start over the partials on
// disk, so a restart re-reads and never fetches.
#[tokio::test]
async fn an_lzma2_7z_restarted_after_every_article_replays_the_engine_from_disk() {
    restart_row(Scenario::new(sevenz(EncoderMethod::LZMA2, 1))).await;
}

#[tokio::test]
async fn a_tar_restarted_after_every_article_delivers_its_members() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.tar",
        tar(&members()),
    )]))
    .await;
}

#[tokio::test]
async fn a_gzipped_tar_restarted_after_every_article_replays_from_its_start() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.tar.gz",
        gzip(&tar(&members())),
    )]))
    .await;
}

// A stream object keeps how far its input was consumed; its input stays in
// its partial, so a restart replays the stream from its start off disk.
async fn stream_row(name: &str, compress: fn(&[u8]) -> Vec<u8>) {
    let body = compress(&payload(30 * ARTICLE + 7, 80));
    restart_row(Scenario::new(vec![Post::plain(name, body)])).await;
}

#[tokio::test]
async fn a_gzip_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.gz", gzip).await;
}

#[tokio::test]
async fn a_bzip2_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.bz2", bzip2).await;
}

#[tokio::test]
async fn a_zstd_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.zst", zstd).await;
}

#[tokio::test]
async fn a_brotli_stream_restarted_after_every_article_replays_from_its_start() {
    stream_row("ember.bin.br", brotli).await;
}

#[tokio::test]
async fn plain_files_beside_an_sfv_restarted_after_every_article_verify_with_no_refetch() {
    let lantern = payload(5 * ARTICLE, 81);
    let beacon = payload(3 * ARTICLE - 5, 82);
    restart_row(Scenario::new(vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
        listing(
            "lantern.sfv",
            &[("lantern.bin", crc(&lantern)), ("beacon.bin", crc(&beacon))],
        ),
    ]))
    .await;
}

// Each file posted under `names[i]`, then one PAR3 set describing them by
// their own names, posted under `set`.
fn with_par3(files: &[(&str, Vec<u8>)], names: &[&str], set: &str) -> Vec<Post> {
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

#[tokio::test]
async fn plain_files_under_par3_restarted_after_every_article_are_repaired_with_no_refetch() {
    let files = [
        ("lantern.bin", payload(6 * ARTICLE, 83)),
        ("beacon.bin", payload(3 * ARTICLE + 9, 84)),
    ];
    let posts = with_par3(&files, &["lantern.bin", "beacon.bin"], "lantern");
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

// The members are bound by their bytes, so the restored set must still
// know what it identified: a lost match would read them back again, and a
// lost rename would deliver them under their hex names.
#[tokio::test]
async fn obfuscated_members_under_par3_restarted_after_every_article_are_renamed_with_no_refetch() {
    let files = [
        ("lantern.bin", payload(5 * ARTICLE, 85)),
        ("beacon.bin", payload(2 * ARTICLE + 7, 86)),
    ];
    let posts = with_par3(
        &files,
        &["3f9a0c71e25b84d6", "a07d5e19c4b26f83"],
        "e81b6d3f0a4c9725",
    );
    restart_row(Scenario::new(posts).damaged(0, 1)).await;
}

// One stored member in a zip of its own.
fn one_member_zip() -> Vec<u8> {
    let mut writer = ::zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer.start_file("ember.bin", zip::stored()).unwrap();
    std::io::Write::write_all(&mut writer, &payload(9 * ARTICLE + 5, 87)).unwrap();
    writer.finish().unwrap().into_inner()
}

#[tokio::test]
async fn a_stored_zip_of_one_member_restarted_after_every_article_delivers_it() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.zip",
        one_member_zip(),
    )]))
    .await;
}

// The engine's inflate run replays the member's stream from its start over
// the partial on disk, so a restart inside the run re-reads and never fetches.
#[tokio::test]
async fn a_deflate_zip_restarted_after_every_article_replays_the_engine_from_disk() {
    restart_row(Scenario::new(vec![Post::plain(
        "lantern.zip",
        zip::deflated(),
    )]))
    .await;
}

// The store knows the set from the names it bound and each volume's length;
// a restored store that lost either would place no byte past the first volume.
#[tokio::test]
async fn a_stored_zip_spanned_over_z01_z02_zip_restarted_after_every_article_delivers_its_members()
{
    restart_row(Scenario::new(zip::spanned(&zip::archive(zip::stored()), 3))).await;
}

#[tokio::test]
async fn a_stored_zip_under_par2_restarted_after_every_article_is_repaired_with_no_refetch() {
    let posts = with_par2(
        vec![Post::plain("lantern.zip", zip::archive(zip::stored()))],
        2,
    );
    restart_row(Scenario::new(posts).damaged(0, 2)).await;
}

// The password is offered again at every restore, and the run that derives
// its key is the engine's; no checkpoint holds the password. The WinZip key
// itself is not grepped for: deriving it here needs PBKDF2 over SHA-1, a
// hash this crate does not depend on.
#[tokio::test]
async fn an_aes_zip_restarted_after_every_article_delivers_its_members_and_no_checkpoint_holds_the_password()
 {
    let enciphered = zip::stored().with_aes_encryption(::zip::AesMode::Aes256, SECRET);
    let scenario =
        Scenario::new(vec![Post::plain("lantern.zip", zip::archive(enciphered))]).password(SECRET);
    let secrets = vec![SECRET.as_bytes().to_vec()];
    let mut seen = 0;
    let restarted = run_restarting(&scenario, &mut |rows| {
        seen += 1;
        assert_no_secret(rows, &secrets);
    })
    .await;
    assert!(seen > 0);
    let whole = run(&scenario, Engine::Graph).await;
    assert!(whole.completed(), "{:?} {:?}", whole.status, whole.failure);
    assert_restarted(&restarted, &whole);
}

// A tar posted as `lantern.zip`, in 512-byte articles, refused at its first
// header, whose size field is no number: the tar is buried as soon as its
// first article is in. Posted after it are `.z01` and `.z02` of a set by the
// same stem, whose store would take `lantern.zip` as its last volume. A
// buried member is settled: no object bound later is offered it, a restored
// tree included, so once a checkpoint says it is buried no later one gives
// it an owner.
#[tokio::test]
async fn a_buried_refusal_restarted_after_every_article_is_offered_to_no_later_store() {
    let mut refused = tar(&[("ember.bin", payload(5 * ARTICLE + 13, 51))]);
    refused[124..136].copy_from_slice(b"no number!!\0");
    refused[148..156].copy_from_slice(b"        ");
    let sum: u32 = refused[..512].iter().map(|byte| u32::from(*byte)).sum();
    refused[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    let mut posts = vec![Post::plain("lantern.zip", refused)];
    posts.extend(
        zip::spanned(&zip::archive(zip::stored()), 3)
            .into_iter()
            .take(2),
    );
    let scenario = Scenario::new(posts).framed(512);
    let whole = run(&scenario, Engine::Graph).await;
    let mut buried = false;
    let restarted = run_restarting(&scenario, &mut |rows| {
        let Some(row) = rows
            .members
            .iter()
            .find(|row| row.node == 0 && row.name == "lantern.zip")
        else {
            return;
        };
        buried |= row.buried;
        assert!(
            !buried || (row.buried && row.owner.is_none()),
            "a buried member was given an owner: {row:?}"
        );
    })
    .await;
    assert!(buried, "a checkpoint says the tar is buried");
    assert_restarted(&restarted, &whole);
}

// A three-volume set the RAR engine reads, posted without `.r00`. The engine
// announces the member and never closes it, so its refusal leaves it buried
// under the engine's node, and that buried member leaves nothing whole, so
// the job fails. A refusal of that kind waits for the payload to be in, so it
// lands in the last settle, after the last checkpoint: restored after every
// article before it, the job still fails as the run that never restarted
// does. That a checkpoint carries a buried member back is the tree record's
// round trip.
#[tokio::test]
async fn a_set_missing_a_volume_restarted_after_every_article_still_fails() {
    let body = payload(7 * ARTICLE + 13, 51);
    let posts: Vec<Post> =
        header_encrypted_store_set("beacon.bin", &body, 3, SECRET, HeaderCheck::Absent)
            .into_iter()
            .zip(["ember.rar", "ember.r00", "ember.r01"])
            .filter(|(_, name)| *name != "ember.r00")
            .map(|((_, bytes), name)| Post::plain(name, bytes))
            .collect();
    let scenario = Scenario::new(posts).password(SECRET);
    let whole = run(&scenario, Engine::Graph).await;
    assert_eq!(
        whole.failure.as_deref(),
        Some("no file was delivered whole; 2 short: ember.rar, ember.r01")
    );
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
}
