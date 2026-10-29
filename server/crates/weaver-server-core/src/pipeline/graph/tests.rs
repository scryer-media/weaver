// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The joined path: a job's articles go from the decoder through the root,
// the tree and the namespace writer to its files, and the assembly writes
// nothing.

use std::path::{Path, PathBuf};

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Kind, MemberId, Provenance};

use super::*;
use crate::history::HistoryFailureKind;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::model::JobStatus;
use crate::pipeline::PipelineEvent;
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::graph::engine::echo::{Echoes, HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{Heard, Said};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::status::{Delivered, Delivery};
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Post, par2_set, par2_volumes, payload, spec,
};
use crate::pipeline::tests::{
    hear_graph_mail, insert_active_job, new_direct_pipeline, retire_pipeline_database, segment_id,
};
use crate::pipeline::{
    DecodeResult, DecodedChunk, Pipeline, SegmentEncoding, SegmentSource, SegmentTerminalState,
    YencLayoutAssertions,
};

// A file delivered under the name it wanted.
fn delivered(name: &str, depth: u32, whole: bool) -> Delivered {
    Delivered {
        name: name.to_string(),
        from: name.to_string(),
        depth,
        whole,
        crc32: None,
    }
}

// A graph job of `posts`, admitted the way the switch admits one.
async fn graph_job(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posts: &[Post],
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", posts, ARTICLE),
    )
    .await;
    assert!(pipeline.graph_jobs.contains_key(&job_id));
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    (pipeline, working_dir)
}

// Article `ordinal` of file `index` as the decoder hands it over, one
// checksummed segment over its bytes; `intact` false is a checksum mismatch.
async fn decode(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
    intact: bool,
) {
    let body = posts[index as usize]
        .articles(ARTICLE)
        .nth(ordinal as usize)
        .unwrap()
        .to_vec();
    decode_as(pipeline, job_id, posts, index, ordinal, body, intact).await;
}

// Article `ordinal` of file `index`, its bytes `body`.
async fn decode_as(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
    body: Vec<u8>,
    intact: bool,
) {
    let post = &posts[index as usize];
    let offset = ordinal as usize * ARTICLE;
    let crc = par2_rs::checksum::crc32(&body);
    let len = body.len() as u64;
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: segment_id(job_id, index, ordinal),
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                yenc_layout: YencLayoutAssertions {
                    file_size: post.bytes.len() as u64,
                    part: Some(ordinal + 1),
                    total: Some(post.articles(ARTICLE).count() as u32),
                    begin: Some(offset as u64 + 1),
                    end: Some(offset as u64 + len),
                },
                crc_valid: intact,
                part_crc_verified: intact,
                part_crc: crc,
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(body),
                yenc_name: post.name.clone(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: vec![weaver_yenc::Segment {
                    file_offset: offset as u64,
                    len,
                    crc32: crc,
                }],
            },
            SegmentSource {
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

async fn decode_all(pipeline: &mut Pipeline, job_id: JobId, posts: &[Post], index: u32) {
    let count = posts[index as usize].articles(ARTICLE).count() as u32;
    for ordinal in 0..count {
        decode(pipeline, job_id, posts, index, ordinal, true).await;
    }
}

fn runs(pipeline: &Pipeline, job_id: JobId, member: u32) -> Vec<(ByteRange, Provenance)> {
    pipeline.graph_jobs[&job_id]
        .namespace()
        .get(MemberId(member))
        .unwrap()
        .coverage
        .runs()
        .map(|run| (run.range, run.provenance))
        .collect()
}

fn member_file(pipeline: &Pipeline, job_id: JobId, member: u32) -> Option<PathBuf> {
    pipeline.graph_jobs[&job_id]
        .member_files()
        .into_iter()
        .find(|(id, _)| *id == MemberId(member))
        .map(|(_, path)| path)
}

// The assembly never saw a byte of the file.
fn assembly_wrote_nothing(pipeline: &Pipeline, job_id: JobId, index: u32) {
    let file = pipeline.jobs[&job_id]
        .articles
        .active()
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: index,
        })
        .unwrap();
    assert_eq!(file.received_bytes(), 0);
    assert!(!file.is_complete());
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

#[tokio::test]
async fn two_plain_files_are_written_at_their_names_and_the_assembly_writes_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_001);
    let posts = [
        Post::plain("lantern.bin", payload(3 * ARTICLE, 1)),
        Post::plain("lantern.dat", payload(2 * ARTICLE - 9, 2)),
    ];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;

    for (index, post) in posts.iter().enumerate() {
        let path = member_file(&pipeline, job_id, index as u32).unwrap();
        assert_eq!(path, working_dir.join(&post.name));
        assert_eq!(read(&path), post.bytes);
        assert_eq!(
            runs(&pipeline, job_id, index as u32),
            vec![(
                ByteRange::new(0, post.bytes.len() as u64),
                Provenance::Fresh
            )]
        );
        assembly_wrote_nothing(&pipeline, job_id, index as u32);
    }
    assert!(pipeline.graph_jobs[&job_id].namespace().is_clean());
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_damaged_article_is_written_as_damaged_and_its_range_reported() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_002);
    let posts = [Post::plain("lantern.bin", payload(2 * ARTICLE, 3))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    decode(&mut pipeline, job_id, &posts, 0, 1, false).await;

    let damaged = ByteRange::new(ARTICLE as u64, 2 * ARTICLE as u64);
    assert_eq!(
        runs(&pipeline, job_id, 0),
        vec![
            (ByteRange::new(0, ARTICLE as u64), Provenance::Fresh),
            (damaged, Provenance::Damaged),
        ]
    );
    let namespace = pipeline.graph_jobs[&job_id].namespace();
    assert!(!namespace.is_clean());
    assert_eq!(
        namespace.damaged_ranges().collect::<Vec<_>>(),
        vec![(MemberId(0), damaged)]
    );
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assembly_wrote_nothing(&pipeline, job_id, 0);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_par2_set_beside_a_plain_file_adjudicates_it_clean_without_reading_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_003);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    // The data first: its segments arrive before anything covers it.
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    // The volume's first article, which is what the sample fetches; the
    // rest stays parked.
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    hear_repair(&mut pipeline, job_id).await;

    let job = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    let summaries = job.recovery_summary();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].kind, Kind::Par2);
    assert_eq!(summaries[0].outcome, Some(RepairOutcome::Clean));
    assert_eq!(job.writer().read_backs(), 0, "the fed segments were enough");
    assert!(job.namespace().is_clean());
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_lost_article_under_a_par2_set_is_repaired_through_the_tree() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_004);
    let data = payload(4 * ARTICLE, 5);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    for ordinal in [0, 2, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    pipeline.book_terminal_segment(segment_id(job_id, 0, 1), SegmentTerminalState::Missing);
    hear_repair(&mut pipeline, job_id).await;

    // The repair waits for blocks, and its seek promoted the volume's
    // parked articles ahead of everything.
    let lost = ByteRange::new(ARTICLE as u64, 2 * ARTICLE as u64);
    assert!(runs(&pipeline, job_id, 0).contains(&(lost, Provenance::Damaged)));
    let job = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    assert_eq!(
        job.recovery_summary()[0].outcome,
        Some(RepairOutcome::Waiting)
    );
    let volume_articles = posts[2].articles(ARTICLE).count() as u32;
    assert!(volume_articles > 1);
    let state = &pipeline.jobs[&job_id];
    for ordinal in 1..volume_articles {
        let id = segment_id(job_id, 2, ordinal);
        assert_eq!(
            state
                .articles
                .active()
                .unwrap()
                .recovery_queue
                .count_matching(|work| work.segment_id == id),
            0
        );
        assert_eq!(
            state
                .articles
                .active()
                .unwrap()
                .download_queue
                .count_matching(|work| work.segment_id == id
                    && work.is_recovery
                    && work.completion_critical),
            1
        );
    }

    // The blocks arrive and the repair goes out on a thread: the step that
    // lent it returns with the slice still lost and the set not yet ruled.
    for ordinal in 1..volume_articles {
        decode(&mut pipeline, job_id, &posts, 2, ordinal, true).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    assert!(runs(&pipeline, job_id, 0).contains(&(lost, Provenance::Damaged)));
    let job = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    assert!(!matches!(
        job.recovery_summary()[0].outcome,
        Some(RepairOutcome::Repaired { .. })
    ));

    // What it came to returns through the run mail and is applied there.
    hear_repair(&mut pipeline, job_id).await;
    let job = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    assert_eq!(
        job.recovery_summary()[0].outcome,
        Some(RepairOutcome::Repaired { slices: 1 })
    );
    assert!(job.namespace().is_clean());
    assert_eq!(
        runs(&pipeline, job_id, 0),
        vec![
            (ByteRange::new(0, ARTICLE as u64), Provenance::Fresh),
            (lost, Provenance::Repaired),
            (
                ByteRange::new(2 * ARTICLE as u64, 4 * ARTICLE as u64),
                Provenance::Fresh
            ),
        ]
    );
    assert_eq!(read(&working_dir.join("lantern.bin")), data);
    assembly_wrote_nothing(&pipeline, job_id, 0);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_hex_named_member_bound_by_its_prefix_is_renamed_and_its_file_moves() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_005);
    let data = payload(3 * ARTICLE, 6);
    let mut posts = vec![Post::plain("8d1f0c2a9b7e4f6013a5c7e9", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    let hex = working_dir.join(&posts[0].name);
    assert_eq!(member_file(&pipeline, job_id, 0), Some(hex.clone()));
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    // The volume still to come is the set's own from the post, so with the
    // payload in, the set rules as its index is founded, before it is read;
    // the bind that names the member follows once that run is heard. The
    // file moves once the checkpoint that records its name has committed.
    hear_repair(&mut pipeline, job_id).await;
    assert!(pipeline.graph_jobs[&job_id].moves_waiting());
    assert!(hex.exists());
    hear_graph_mail(&mut pipeline, job_id).await;
    assert!(!pipeline.graph_jobs[&job_id].moves_waiting());

    let member = pipeline.graph_jobs[&job_id]
        .namespace()
        .get(MemberId(0))
        .unwrap();
    assert_eq!(member.name, "lantern.bin");
    let moved = working_dir.join("lantern.bin");
    assert_eq!(member_file(&pipeline, job_id, 0), Some(moved.clone()));
    assert_eq!(read(&moved), data);
    assert!(!hex.exists());
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn dropping_a_graph_job_gives_its_holds_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_006);
    let posts = [Post::plain("lantern.bin", payload(2 * ARTICLE, 7))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    // The second article before the first: nothing knows yet what the
    // member is, so its bytes are held.
    decode(&mut pipeline, job_id, &posts, 0, 1, true).await;
    assert_eq!(pipeline.graph_holds.usage().resident, ARTICLE as u64);
    pipeline.graph_jobs.remove(&job_id);
    assert_eq!(pipeline.graph_holds.usage().resident, 0);
    assert_eq!(pipeline.graph_holds.usage().namespaces, 0);
    retire_pipeline_database(pipeline).await;
}

// Volume `index` of an echo store: the first carries the store's signature,
// every one a header and then `body`.
fn echo_volume(index: u32, body: &[u8]) -> Post {
    let mut bytes = if index == 0 {
        SIGNATURE.to_vec()
    } else {
        vec![0x80; HEADER]
    };
    bytes.extend_from_slice(body);
    Post::plain(&format!("lantern.part{}.rar", index + 1), bytes)
}

// Read the job's archives as echo engines.
fn echo_engines(pipeline: &mut Pipeline, job_id: JobId, volumes: u32) {
    let echoes = Echoes {
        volumes,
        ..Echoes::default()
    };
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(echoes));
}

// Hear the job's runs until the current one returned and none is waiting.
async fn drain_runs(pipeline: &mut Pipeline, job_id: JobId) {
    loop {
        let said = pipeline.graph_runs.recv().await.unwrap();
        let runs = pipeline.graph_jobs[&job_id].runs();
        let ended = matches!(said.heard, Heard::Ended) && runs.current(said.node, said.generation);
        pipeline.graph_run_said(said);
        if ended && !pipeline.graph_jobs[&job_id].runs().live() {
            return;
        }
    }
}

// Hear the run mail until the job's repair is back, and no further: a run
// beside it goes on.
async fn hear_repair(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
}

// A turn hears everything the runs said since the last one, not one piece:
// a run speaks its output 64 KiB at a time, so one heard a turn runs as
// slowly as the pipeline's turns are busy with everything else.
#[tokio::test]
async fn a_turn_hears_all_the_run_mail_waiting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mail = pipeline.graph_runs.sender();
    for generation in 0..mail.max_capacity() as u64 {
        let said = Said {
            scope: 92_099,
            node: weaver_graph::NodeId(1),
            generation,
            heard: Heard::Ended,
        };
        mail.try_send(said).unwrap();
    }
    assert!(pipeline.hear_graph_runs());
    assert_eq!(
        mail.capacity(),
        mail.max_capacity(),
        "the mail is heard whole"
    );
    assert!(!pipeline.hear_graph_runs(), "nothing is heard twice");
    retire_pipeline_database(pipeline).await;
}

// A paused graph job with nothing out lets its articles and its graph go at
// once. Resumed while it holds its NZB slot, it is built again from its
// checkpoint and installed. Its graph starts no run while the slot is taken
// away, and once the slot is back the articles are written.
#[tokio::test]
async fn paused_graph_work_is_rebuilt_on_resume_and_runs_only_with_nzb_admission() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_100);
    let body = payload(2 * ARTICLE + 9, 8);
    let posts = vec![echo_volume(0, &body)];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.jobs[&job_id].articles.active().is_none());
    assert!(!pipeline.graph_jobs.contains_key(&job_id));

    assert!(pipeline.nzb_admitted.contains(&job_id));
    pipeline.resume_job_runtime(job_id).unwrap();
    crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    assert!(pipeline.graph_jobs.contains_key(&job_id));
    echo_engines(&mut pipeline, job_id, 1);
    pipeline.nzb_admitted.remove(&job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    assert!(!pipeline.graph_jobs[&job_id].awaits_mail());
    assert!(!working_dir.join("echo-0.bin").exists());
    pipeline.nzb_admitted.insert(job_id);
    pipeline.resume_graph_work(job_id);
    assert!(pipeline.graph_jobs[&job_id].awaits_mail());
    drain_runs(&mut pipeline, job_id).await;
    assert_eq!(read(&working_dir.join("echo-0.bin")), body);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_checkpoint_error_queued_before_resume_cannot_pause_the_resumed_job() {
    use super::engine::{Heard, Said};
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_103);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let control = pipeline.graph_jobs[&job_id].pending().control();
    let revision = control.revision();
    let error = |revision| Said {
        scope: job_id.0,
        node: weaver_graph::NodeId::ROOT,
        generation: 0,
        heard: Heard::CheckpointBlocked {
            control: control.clone(),
            revision,
            error: "fixture storage failure".into(),
        },
    };
    pipeline.graph_run_said(error(revision));
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Paused);
    pipeline.resume_job_runtime(job_id).unwrap();
    let resumed = pipeline.jobs[&job_id].status.clone();
    assert_ne!(resumed, JobStatus::Paused);
    assert_ne!(control.revision(), revision);
    pipeline.graph_run_said(error(revision));
    assert_eq!(pipeline.jobs[&job_id].status, resumed);
    pipeline.graph_run_said(error(control.revision()));
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Paused);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_closed_checkpoint_writer_pauses_with_an_actionable_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_105);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    pipeline.db.flush_write_queue().await.unwrap();
    hear_graph_mail(&mut pipeline, job_id).await;
    // Generic writes receive the writer's detached handle: SQL is available,
    // but its command queue is closed. Exercise that actual queue refusal.
    let (sent, received) = tokio::sync::oneshot::channel();
    pipeline
        .db
        .try_queue_write("capture_detached_writer", move |db| {
            assert!(sent.send(db.clone()).is_ok());
            Ok(())
        })
        .unwrap();
    let db = std::mem::replace(&mut pipeline.db, received.await.unwrap());
    pipeline.recover_cancelled_graph_checkpoint(job_id);
    while pipeline.jobs[&job_id].status != JobStatus::Paused {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert!(
        pipeline.jobs[&job_id]
            .failure_error
            .as_deref()
            .unwrap()
            .contains("Checkpoint could not be persisted")
    );
    assert!(working_dir.exists());
    pipeline.db = db;
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn terminal_graph_runtime_is_retained_until_its_worker_stops() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_101);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    assert!(pipeline.graph_jobs[&job_id].awaits_mail());
    pipeline.purge_terminal_job_runtime(job_id);
    assert!(!pipeline.graph_jobs.contains_key(&job_id));
    assert!(pipeline.retiring_graph_jobs[&job_id].awaits_mail());
    assert!(pipeline.nzb_admitted.contains(&job_id));
    while pipeline.retiring_graph_jobs.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn nonterminal_retirement_keeps_a_repair_only_worker_without_applying_its_output() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_109);
    let data = payload(4 * ARTICLE, 5);
    let mut posts = vec![Post::plain("lantern.bin", data)];
    posts.extend(par2_set("lantern", &[("lantern.bin", &posts[0].bytes)], 2));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    for ordinal in [0, 2, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    pipeline.book_terminal_segment(segment_id(job_id, 0, 1), SegmentTerminalState::Missing);
    hear_repair(&mut pipeline, job_id).await;
    let volume_articles = posts[2].articles(ARTICLE).count() as u32;
    for ordinal in 1..volume_articles {
        decode(&mut pipeline, job_id, &posts, 2, ordinal, true).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let before = read(&working_dir.join("lantern.bin"));
    pipeline.retire_graph_job(job_id);
    assert!(pipeline.retiring_graph_jobs[&job_id].repair_out());
    assert!(!pipeline.pending_graph_terminals.contains_key(&job_id));
    while pipeline.retiring_graph_jobs.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert_eq!(read(&working_dir.join("lantern.bin")), before);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn failure_keeps_a_repair_only_worker_and_its_slot_until_acknowledged() {
    repair_only_retirement(false).await;
}

#[tokio::test]
async fn cancellation_keeps_a_repair_only_worker_and_its_slot_until_acknowledged() {
    repair_only_retirement(true).await;
}

async fn repair_only_retirement(cancel: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_107);
    let data = payload(3 * ARTICLE, 6);
    let mut posts = vec![Post::plain("8d1f0c2a9b7e4f6013a5c7e9", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let waiter_job = JobId(job_id.0 + 1);
    insert_active_job(
        &mut pipeline,
        waiter_job,
        spec(
            "waiting repair",
            &[Post::plain("waiting.bin", payload(ARTICLE, 9))],
            ARTICLE,
        ),
    )
    .await;
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    assert!(!pipeline.graph_jobs[&job_id].runs().in_flight());
    let waiter = weaver_graph::slots::AskerId(waiter_job.0);
    assert!(pipeline.graph_status.repairs.admit(waiter, 1).is_none());
    let mut events = pipeline.event_tx.subscribe();
    if cancel {
        pipeline.report_all_for_test();
        let handle =
            crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        handle.cancel_job_keeping_files(job_id).await.unwrap();
        pipeline.apply_queue_intents().await;
        match pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            crate::pipeline::TerminalPostProcessingEvent::CancelJobsDone(done) => {
                pipeline.handle_cancel_jobs_done(done);
            }
            _ => panic!("expected cancellation archive reply"),
        }
    } else {
        pipeline.fail_job(job_id, "namespace move collision".into());
    }
    assert!(
        pipeline.retiring_graph_jobs.contains_key(&job_id),
        "a repair reply remains owned even when no decoder run is active"
    );
    assert!(pipeline.retiring_graph_jobs[&job_id].repair_out());
    assert!(pipeline.pending_graph_terminals.contains_key(&job_id));
    assert!(!pipeline.graph_status.repairs.handed(waiter));
    assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
    assert!(working_dir.exists());
    pipeline.remove_pending_completion_check(waiter_job);
    while pipeline.retiring_graph_jobs.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert!(!pipeline.pending_graph_terminals.contains_key(&job_id));
    assert!(pipeline.graph_status.repairs.handed(waiter));
    assert!(pipeline.pending_completion_checks.contains(&waiter_job));
    pipeline.graph_status.repairs.forget(waiter);
    if cancel {
        assert!(!pipeline.jobs.contains_key(&job_id));
    } else {
        crate::pipeline::tests::settle_failed_releases(&mut pipeline).await;
    }
    pipeline.db.flush_write_queue().await.unwrap();
    let terminals = std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| {
            matches!(event,
            PipelineEvent::JobCancelled { job_id: id }
            | PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id)
        })
        .count();
    assert_eq!(terminals, 1);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn repeated_failure_keeps_worker_ownership_and_releases_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_102);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    assert!(pipeline.graph_jobs[&job_id].awaits_mail());
    pipeline.transition_postprocessing_status(job_id, JobStatus::Extracting, None);
    assert_eq!(
        pipeline
            .metrics
            .extract_active
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    pipeline.fail_job(job_id, "first failure".into());
    pipeline.fail_job(job_id, "late failure".into());
    assert_eq!(
        pipeline
            .metrics
            .extract_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(pipeline.pending_graph_terminals.contains_key(&job_id));
    assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
    assert!(working_dir.exists());
    assert_eq!(
        pipeline.jobs[&job_id].failure_error.as_deref(),
        Some("first failure")
    );
    let retired = &pipeline.retiring_graph_jobs[&job_id];
    let node = retired.tree().engines()[0];
    let generation = retired.runs().generation(node).unwrap();
    pipeline.graph_run_said(super::engine::Said {
        scope: job_id.0,
        node,
        generation: generation.wrapping_add(1),
        heard: super::engine::Heard::Ended,
    });
    assert!(pipeline.retiring_graph_jobs[&job_id].awaits_mail());
    while pipeline.retiring_graph_jobs.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert!(!pipeline.pending_graph_terminals.contains_key(&job_id));
    assert!(pipeline.releasing_failed_jobs.contains(&job_id));
    pipeline.fail_job(job_id, "cleanup raced".into());
    crate::pipeline::tests::settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
    assert!(pipeline.failed_release_rx.try_recv().is_err());
    assert_eq!(
        pipeline
            .metrics
            .extract_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(
        !working_dir.join("echo-0.bin").exists(),
        "late worker output must not be published"
    );
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn cancellation_keeps_worker_files_and_blocks_same_id_retry_until_ack() {
    cancel_held_failure(false).await;
}

#[tokio::test]
async fn shutdown_hears_cancelled_graph_workers_before_finishing_cancellation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_106);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let gate = std::sync::Arc::new(super::engine::echo::RunGate::default());
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            gate: Some(gate.clone()),
            ..Default::default()
        }));
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    gate.started().await;
    pipeline.report_all_for_test();
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let mut events = handle.subscribe_events();
    handle.cancel_job_keeping_files(job_id).await.unwrap();
    pipeline.apply_queue_intents().await;
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        crate::pipeline::TerminalPostProcessingEvent::CancelJobsDone(done) => {
            pipeline.handle_cancel_jobs_done(done)
        }
        _ => panic!("expected cancellation archive reply"),
    }
    assert!(pipeline.retiring_graph_jobs[&job_id].awaits_mail());
    assert!(pipeline.jobs.contains_key(&job_id));
    assert!(working_dir.exists());
    handle.shutdown().await.unwrap();
    gate.release();
    // The production shutdown branch must receive the final graph ACK, not
    // wait exclusively for an archive reply that it has already processed.
    pipeline.run().await;
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.retiring_graph_jobs.contains_key(&job_id));
    assert!(!pipeline.pending_graph_terminals.contains_key(&job_id));
    assert_eq!(
        pipeline
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    let mut cancelled = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, PipelineEvent::JobCancelled { job_id: id } if id == job_id) {
            cancelled += 1;
        }
    }
    assert_eq!(cancelled, 1);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn cancellation_wins_failure_when_worker_ack_precedes_archive_reply() {
    cancel_held_failure(true).await;
}

async fn cancel_held_failure(ack_before_archive: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_104);
    let posts = vec![echo_volume(0, &payload(4 * ARTICLE, 8))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let gate = std::sync::Arc::new(super::engine::echo::RunGate::default());
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            gate: Some(gate.clone()),
            ..Default::default()
        }));
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    gate.started().await;
    pipeline.fail_job(job_id, "failure before operator cancellation".into());
    pipeline.report_all_for_test();
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let mut events = handle.subscribe_events();
    handle.cancel_job(job_id).await.unwrap();
    pipeline.apply_queue_intents().await;
    if ack_before_archive {
        gate.release();
        while pipeline.retiring_graph_jobs.contains_key(&job_id) {
            hear_graph_mail(&mut pipeline, job_id).await;
        }
        assert!(pipeline.pending_graph_terminals.contains_key(&job_id));
        assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
    }
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        crate::pipeline::TerminalPostProcessingEvent::CancelJobsDone(done) => {
            pipeline.handle_cancel_jobs_done(done);
        }
        _ => panic!("expected cancellation archive reply"),
    }
    if !ack_before_archive {
        assert!(pipeline.retiring_graph_jobs[&job_id].awaits_mail());
        assert!(pipeline.jobs.contains_key(&job_id));
        assert!(working_dir.exists());
        assert!(
            !pipeline
                .shared_state
                .queue
                .read(|inner| inner.pending_cleanup.contains(&job_id))
        );
        assert!(matches!(
            handle.redownload_job(job_id).await,
            Err(crate::SchedulerError::Conflict(_))
        ));
        pipeline.apply_queue_intents().await;
        assert!(pipeline.jobs.contains_key(&job_id));
        assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
        gate.release();
        while pipeline.retiring_graph_jobs.contains_key(&job_id) {
            hear_graph_mail(&mut pipeline, job_id).await;
        }
    }
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(!pipeline.pending_graph_terminals.contains_key(&job_id));
    assert!(!pipeline.releasing_failed_jobs.contains(&job_id));
    let mut cancelled = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, PipelineEvent::JobCancelled { job_id: id } if id == job_id) {
            cancelled += 1;
        }
        assert!(!matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if id == job_id));
    }
    assert_eq!(cancelled, 1);
    assert_eq!(
        pipeline
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    handle.redownload_job(job_id).await.unwrap();
    pipeline.apply_queue_intents().await;
    crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    assert!(!matches!(
        pipeline.jobs[&job_id].status,
        JobStatus::Failed { .. }
    ));
    assert!(pipeline.jobs[&job_id].working_dir.exists());
    assert!(pipeline.failed_release_rx.try_recv().is_err());
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn an_archive_handed_to_its_engine_writes_what_the_engine_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_007);
    let bodies = [payload(2 * ARTICLE + 9, 8), payload(ARTICLE - 3, 9)];
    let posts: Vec<Post> = bodies
        .iter()
        .enumerate()
        .map(|(index, body)| echo_volume(index as u32, body))
        .collect();
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 2);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    drain_runs(&mut pipeline, job_id).await;

    let job = &pipeline.graph_jobs[&job_id];
    assert_eq!(job.tree().engines().len(), 1);
    for (index, body) in bodies.iter().enumerate() {
        assert_eq!(&read(&working_dir.join(format!("echo-{index}.bin"))), body);
        assembly_wrote_nothing(&pipeline, job_id, index as u32);
    }
    // The volumes are the engine's input, not files of the job.
    assert!(job.member_files().is_empty());
    assert_eq!(job.phase(), RootPhase::Done);
    retire_pipeline_database(pipeline).await;
}

// Two volumes extract whole and a plain file beside them arrives damaged:
// the job delivers everything it has and ends Partial, the extracted
// members listed whole, the damaged file short, the history row a failed
// unpack.
#[tokio::test]
async fn a_delivery_with_a_damaged_member_settles_to_partial() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_009);
    let bodies = [payload(2 * ARTICLE + 9, 11), payload(ARTICLE - 3, 12)];
    let mut posts: Vec<Post> = bodies
        .iter()
        .enumerate()
        .map(|(index, body)| echo_volume(index as u32, body))
        .collect();
    posts.push(Post::plain("lantern.dat", payload(2 * ARTICLE, 13)));
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let mut events = pipeline.event_tx.subscribe();
    echo_engines(&mut pipeline, job_id, 2);
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    decode(&mut pipeline, job_id, &posts, 2, 1, false).await;
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    drain_runs(&mut pipeline, job_id).await;

    assert_eq!(
        pipeline.graph_delivery(job_id),
        Some(Delivery {
            whole: vec!["echo-0.bin".to_string(), "echo-1.bin".to_string()],
            damaged: vec!["lantern.dat".to_string()],
            unrepaired: Vec::new(),
            withheld: Vec::new(),
            files: vec![
                delivered("echo-0.bin", 1, true),
                delivered("echo-1.bin", 1, true),
                delivered("lantern.dat", 0, false),
            ],
        })
    );
    assert_eq!(
        pipeline.graph_terminal_status(job_id),
        Some(JobStatus::Partial)
    );

    // The queues owe nothing more, so the completion gate hands the job to
    // its delivery.
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
    state.articles.active_mut().unwrap().recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    pipeline.check_job_completion(job_id).await;
    assert!(pipeline.inflight_moves.contains(&job_id));
    let done = pipeline.move_done_rx.recv().await.unwrap();
    pipeline.handle_move_to_complete_done(done).await;
    while !matches!(
        events.recv().await.expect("pipeline event channel closed"),
        PipelineEvent::JobCompleted { job_id: completed } if completed == job_id
    ) {}

    let finished = pipeline
        .finished_jobs()
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(finished.status, JobStatus::Partial);
    let row = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!((row.status.as_str(), row.error_message), ("partial", None));
    let states = pipeline.db.nzbget_history_states(&[job_id.0]).unwrap();
    assert_eq!(
        states[&job_id.0].failure_kind,
        Some(HistoryFailureKind::Unpack)
    );
    retire_pipeline_database(pipeline).await;
}

// A plain file arrives damaged beside the recovery the post carried for
// it, which never yielded a set: none of its packets came, so nothing
// ruled on the damage. What is short is short because that recovery could
// not be used, so the shortfall is the repair's, for PAR2 and PAR3 alike.
#[tokio::test]
async fn a_short_file_beside_recovery_that_never_ruled_is_the_repair_stage() {
    for (job_id, recovery) in [
        (JobId(92_031), "lantern.par2"),
        (JobId(92_032), "lantern.par3"),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let posts = vec![
            Post::plain("lantern.dat", payload(2 * ARTICLE, 13)),
            Post::plain(recovery, payload(ARTICLE, 14)),
        ];
        let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
        decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
        decode(&mut pipeline, job_id, &posts, 0, 1, false).await;

        let delivery = pipeline.graph_delivery(job_id).unwrap();
        assert_eq!(
            delivery.unrepaired,
            vec!["lantern.dat".to_string()],
            "{recovery}"
        );
        assert_eq!(
            pipeline.graph_shortfall(job_id).map(|(stage, _)| stage),
            Some(crate::post_processing::model::PipelineFailureStage::Repair),
            "{recovery}"
        );
        retire_pipeline_database(pipeline).await;
    }
}

// An echo volume beside a PAR2 set, its second article arriving as `wrong`
// with `intact` the checksum's verdict on it. The index and the volume's
// first article come first, so the set can reach its blocks; the rest of
// the set stays out until the test sends it.
async fn echo_volume_with_a_wrong_article(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    intact: bool,
) -> (Pipeline, PathBuf, Vec<u8>, Vec<Post>) {
    let body = payload(4 * ARTICLE - HEADER, 10);
    let mut posts = vec![echo_volume(0, &body)];
    let volume = posts[0].bytes.clone();
    posts.extend(par2_set("lantern", &[("lantern.part1.rar", &volume)], 2));
    let (mut pipeline, working_dir) = graph_job(temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    for ordinal in 0..4 {
        let mut article = posts[0].articles(ARTICLE).nth(ordinal).unwrap().to_vec();
        if ordinal == 1 {
            article.iter_mut().for_each(|byte| *byte ^= 0x5a);
        }
        let intact = intact || ordinal != 1;
        decode_as(
            &mut pipeline,
            job_id,
            &posts,
            0,
            ordinal as u32,
            article,
            intact,
        )
        .await;
    }
    (pipeline, working_dir.join("echo-0.bin"), body, posts)
}

// The recovery blocks arrive, and the set repairs the one wrong slice.
async fn repair_the_wrong_article(pipeline: &mut Pipeline, job_id: JobId, posts: &[Post]) {
    for ordinal in 1..posts[2].articles(ARTICLE).count() as u32 {
        decode(pipeline, job_id, posts, 2, ordinal, true).await;
    }
    hear_repair(pipeline, job_id).await;
    assert_eq!(
        pipeline.graph_jobs[&job_id].recovery_summary()[0].outcome,
        Some(RepairOutcome::Repaired { slices: 1 })
    );
}

// The generation the job's one engine run reads under.
fn run_generation(pipeline: &Pipeline, job_id: JobId) -> Option<u64> {
    let job = &pipeline.graph_jobs[&job_id];
    let node = *job.tree().engines().first()?;
    job.runs().generation(node)
}

// A run reads verified bytes only: it waits at the article that failed its
// checksum, the repair brings that range in as repaired, and the run reads
// on from where it stood, once, to the whole body.
#[tokio::test]
async fn an_engine_waits_at_a_damaged_article_until_its_repair_and_reads_it_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_008);
    let (mut pipeline, echoed, body, posts) =
        echo_volume_with_a_wrong_article(&temp_dir, job_id, false).await;
    assert!(pipeline.graph_jobs[&job_id].runs().live());
    assert!(std::fs::read(&echoed).unwrap_or_default().len() < body.len());
    let started = run_generation(&pipeline, job_id);
    assert!(started.is_some());

    repair_the_wrong_article(&mut pipeline, job_id, &posts).await;
    assert!(pipeline.graph_jobs[&job_id].runs().live());
    drain_runs(&mut pipeline, job_id).await;
    assert_eq!(read(&echoed), body);
    assert_eq!(
        run_generation(&pipeline, job_id),
        started,
        "never retracted"
    );
    assert_eq!(pipeline.graph_jobs[&job_id].phase(), RootPhase::Done);
    retire_pipeline_database(pipeline).await;
}

// An article whose own checksum passed is verified, so the run reads it; the
// PAR2 set proves its slice wrong and repairs it behind what the run read.
// The run's output is retracted and it reads the volume again.
#[tokio::test]
async fn a_repair_of_an_article_that_passed_its_checksum_retracts_what_the_engine_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_010);
    let (mut pipeline, echoed, body, posts) =
        echo_volume_with_a_wrong_article(&temp_dir, job_id, true).await;
    drain_runs(&mut pipeline, job_id).await;
    assert_eq!(read(&echoed).len(), body.len());
    assert_ne!(read(&echoed), body);
    let first = run_generation(&pipeline, job_id);

    repair_the_wrong_article(&mut pipeline, job_id, &posts).await;
    assert!(pipeline.graph_jobs[&job_id].runs().live());
    assert_ne!(run_generation(&pipeline, job_id), first, "read again");
    drain_runs(&mut pipeline, job_id).await;
    // The retracted member's file went with its bytes; the one read again
    // waits under its name for the checkpoint that frees it.
    let engine = pipeline.graph_jobs[&job_id].tree().engines()[0];
    let written = pipeline.graph_jobs[&job_id]
        .writer()
        .files(engine)
        .map(|(_, path)| path.to_path_buf())
        .next()
        .expect("the member read again has a file");
    assert_ne!(written, echoed);
    assert_eq!(read(&written), body);
    assert_eq!(pipeline.graph_jobs[&job_id].phase(), RootPhase::Done);
    retire_pipeline_database(pipeline).await;
}

mod refusals;

// The articles of a recovery volume's head: enough for its first recovery
// packet to pass, which is what tells the PAR2 object what the volume is.
const HEAD: u32 = 3;

fn assert_parked(pipeline: &Pipeline, job_id: JobId, index: u32, ordinal: u32) {
    let id = segment_id(job_id, index, ordinal);
    assert_eq!(
        pipeline.graph_jobs[&job_id].article_class(weaver_graph::VolumeIndex(index), ordinal),
        Some(root::source::Class::Parked),
        "file {index} article {ordinal}"
    );
    let state = &pipeline.jobs[&job_id];
    assert_eq!(
        state
            .articles
            .active()
            .unwrap()
            .recovery_queue
            .count_matching(|work| work.segment_id == id),
        1
    );
    assert_eq!(
        state
            .articles
            .active()
            .unwrap()
            .download_queue
            .count_matching(|work| work.segment_id == id),
        0
    );
}

fn assert_raised(pipeline: &Pipeline, job_id: JobId, index: u32, ordinal: u32) {
    let id = segment_id(job_id, index, ordinal);
    assert_eq!(
        pipeline.graph_jobs[&job_id].article_class(weaver_graph::VolumeIndex(index), ordinal),
        Some(root::source::Class::Recovery),
        "file {index} article {ordinal}"
    );
    let state = &pipeline.jobs[&job_id];
    assert_eq!(
        state
            .articles
            .active()
            .unwrap()
            .recovery_queue
            .count_matching(|work| work.segment_id == id),
        0
    );
    assert_eq!(
        state
            .articles
            .active()
            .unwrap()
            .download_queue
            .count_matching(|work| work.segment_id == id
                && work.is_recovery
                && work.completion_critical),
        1
    );
}

fn after_head(posts: &[Post], index: u32) -> std::ops::Range<u32> {
    HEAD..posts[index as usize].articles(ARTICLE).count() as u32
}

async fn decode_head(pipeline: &mut Pipeline, job_id: JobId, posts: &[Post], index: u32) {
    for ordinal in 0..HEAD {
        decode(pipeline, job_id, posts, index, ordinal, true).await;
    }
}

// A data file and a PAR2 set of three volumes of two blocks each.
fn three_volumes(seed: u8) -> Vec<Post> {
    let data = payload(8 * ARTICLE, seed);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 6, 3));
    assert_eq!(posts.len(), 5, "the data, an index and three volumes");
    posts
}

// The head of a recovery volume arrives before any damage. The PAR2 object
// knows from it what the volume is and defers the rest, so the root parks
// it: it leaves the download queue and nothing fetches it.
#[tokio::test]
async fn a_recovery_volume_whose_head_arrived_before_damage_is_parked_not_fetched() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_030);
    let posts = three_volumes(30);
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let state = &pipeline.jobs[&job_id];
    assert!(
        state.articles.active().unwrap().recovery_queue.is_empty(),
        "nothing parked by name"
    );
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    decode_head(&mut pipeline, job_id, &posts, 2).await;
    let rest = after_head(&posts, 2);
    assert!(!rest.is_empty());
    for ordinal in rest {
        assert_parked(&pipeline, job_id, 2, ordinal);
    }
    retire_pipeline_database(pipeline).await;
}

// Damage appears: four slices lost, three blocks in hand from the heads.
// The one block short is wanted from the first volume, whole, now; the
// other volumes stay parked.
#[tokio::test]
async fn damage_raises_the_volumes_the_shortfall_needs_and_the_others_stay_parked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_031);
    let posts = three_volumes(31);
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    for index in 2..5 {
        decode_head(&mut pipeline, job_id, &posts, index).await;
    }
    for index in 2..5 {
        for ordinal in after_head(&posts, index) {
            assert_parked(&pipeline, job_id, index, ordinal);
        }
    }
    for ordinal in [0, 2, 4, 6] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [1, 3, 5, 7] {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
    }
    hear_repair(&mut pipeline, job_id).await;
    let job = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    assert_eq!(
        job.recovery_summary()[0].outcome,
        Some(RepairOutcome::Waiting)
    );
    for ordinal in after_head(&posts, 2) {
        assert_raised(&pipeline, job_id, 2, ordinal);
    }
    for index in 3..5 {
        for ordinal in after_head(&posts, index) {
            assert_parked(&pipeline, job_id, index, ordinal);
        }
    }
    retire_pipeline_database(pipeline).await;
}

// A restart while a volume is parked: no class of it was kept, as a parked
// one has no row, and the restored PAR2 object defers it again on the first
// seek round.
#[tokio::test]
async fn a_restart_parks_the_volume_again_from_the_objects_wants() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_032);
    let posts = three_volumes(32);
    let working_dir = {
        let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
        decode_all(&mut pipeline, job_id, &posts, 1).await;
        decode_head(&mut pipeline, job_id, &posts, 2).await;
        for ordinal in after_head(&posts, 2) {
            assert_parked(&pipeline, job_id, 2, ordinal);
        }
        pipeline.db.flush_write_queue().await.unwrap();
        retire_pipeline_database(pipeline).await;
        working_dir
    };

    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: spec("lantern joined", &posts, ARTICLE),
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
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&job_id));
    for ordinal in after_head(&posts, 2) {
        assert_parked(&pipeline, job_id, 2, ordinal);
    }
    retire_pipeline_database(pipeline).await;
}
