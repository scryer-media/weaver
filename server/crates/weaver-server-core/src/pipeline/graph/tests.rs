// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The engine switch, and the joined path under it: a graph job's articles go
// from the decoder through the root, the tree and the namespace writer to
// its files, and the classic assembly writes nothing.

use std::path::{Path, PathBuf};

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Kind, MemberId, Provenance};
use weaver_model::files::FileRole;

use super::*;
use crate::history::HistoryFailureKind;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::JobStatus;
use crate::jobs::model::{FileSpec, JobSpec, SegmentSpec};
use crate::pipeline::PipelineEvent;
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::graph::engine::echo::{Echoes, HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{Heard, Said};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::status::{Delivered, Delivery};
use crate::pipeline::tests::{insert_active_job, new_direct_pipeline, retire_pipeline_database};
use crate::pipeline::{
    DecodeResult, DecodedChunk, Pipeline, SegmentEncoding, SegmentSource, SegmentTerminalState,
    YencLayoutAssertions,
};

// One article's decoded size, and the PAR2 slice size, so every article is
// exactly one slice and its checksum is a slice's.
const ARTICLE: usize = 64;

struct Post {
    name: String,
    role: FileRole,
    bytes: Vec<u8>,
}

impl Post {
    fn plain(name: &str, bytes: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            role: FileRole::Standalone,
            bytes,
        }
    }

    fn articles(&self) -> impl Iterator<Item = &[u8]> {
        self.bytes.chunks(ARTICLE)
    }
}

// Bytes no format sniffs as its own.
// A file delivered under the name it wanted.
fn delivered(name: &str, depth: u32, whole: bool) -> Delivered {
    Delivered {
        name: name.to_string(),
        from: name.to_string(),
        depth,
        whole,
    }
}

fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed) | 0x80)
        .collect()
}

// A PAR2 set over `data` posted as `name`: an index and one volume of two
// recovery blocks, sliced at the article size.
fn par2_set(name: &str, data: &[u8]) -> Vec<Post> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join(name), data).unwrap();
    let mut options = Par2CreatorOptions::with_output(
        source.path().join("lantern.par2"),
        Some(source.path().to_path_buf()),
        vec![source.path().join(name)],
    );
    options.block_sizing = BlockSizing::Bytes(ARTICLE as u64);
    options.recovery_amount = RecoveryAmount::Count(2);
    options.volume_count = Some(1);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let mut posts: Vec<Post> = created
        .output_paths
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap().to_string();
            let is_index = !name.contains(".vol");
            Post {
                role: FileRole::Par2 {
                    is_index,
                    recovery_block_count: if is_index { 0 } else { 2 },
                },
                bytes: std::fs::read(path).unwrap(),
                name,
            }
        })
        .collect();
    // The index first, then the volume.
    posts.sort_by_key(|post| !matches!(post.role, FileRole::Par2 { is_index: true, .. }));
    assert_eq!(posts.len(), 2, "an index and one volume");
    posts
}

fn spec(name: &str, posts: &[Post]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: posts.iter().map(|post| post.bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: posts
            .iter()
            .enumerate()
            .map(|(index, post)| FileSpec {
                filename: post.name.clone(),
                role: post.role.clone(),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: post
                    .articles()
                    .enumerate()
                    .map(|(ordinal, article)| SegmentSpec {
                        ordinal: ordinal as u32,
                        article_number: ordinal as u32,
                        bytes: article.len() as u32,
                        message_id: format!("lantern-{index}-{ordinal}@example.com"),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn segment_id(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

// A graph job of `posts`, admitted the way the switch admits one.
async fn graph_job(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posts: &[Post],
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", posts)).await;
    pipeline.admit_graph_job(job_id, Engine::Graph);
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
        .articles()
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
                    total: Some(post.articles().count() as u32),
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
    let count = posts[index as usize].articles().count() as u32;
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

// The classic assembly never saw a byte of the file.
fn classic_wrote_nothing(pipeline: &Pipeline, job_id: JobId, index: u32) {
    let file = pipeline.jobs[&job_id]
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
async fn two_plain_files_are_written_at_their_names_and_classic_writes_nothing() {
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
        classic_wrote_nothing(&pipeline, job_id, index as u32);
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
    classic_wrote_nothing(&pipeline, job_id, 0);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_par2_set_beside_a_plain_file_adjudicates_it_clean_without_reading_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_003);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern.bin", &data));
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
    assert_eq!(summaries[0].damaged_slices, 0);
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
    posts.extend(par2_set("lantern.bin", &data));
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
    let volume_articles = posts[2].articles().count() as u32;
    assert!(volume_articles > 1);
    let state = &pipeline.jobs[&job_id];
    for ordinal in 1..volume_articles {
        let id = segment_id(job_id, 2, ordinal);
        assert_eq!(
            state
                .recovery_queue
                .count_matching(|work| work.segment_id == id),
            0
        );
        assert_eq!(
            state
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
    classic_wrote_nothing(&pipeline, job_id, 0);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn a_hex_named_member_bound_by_its_prefix_is_renamed_and_its_file_moves() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_005);
    let data = payload(3 * ARTICLE, 6);
    let mut posts = vec![Post::plain("8d1f0c2a9b7e4f6013a5c7e9", data.clone())];
    posts.extend(par2_set("lantern.bin", &data));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    let hex = working_dir.join(&posts[0].name);
    assert_eq!(member_file(&pipeline, job_id, 0), Some(hex.clone()));
    decode_all(&mut pipeline, job_id, &posts, 1).await;

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
        classic_wrote_nothing(&pipeline, job_id, index as u32);
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
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
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
        .finished_jobs
        .iter()
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
    posts.extend(par2_set("lantern.part1.rar", &volume));
    let (mut pipeline, working_dir) = graph_job(temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    decode_all(&mut pipeline, job_id, &posts, 1).await;
    decode(&mut pipeline, job_id, &posts, 2, 0, true).await;
    for ordinal in 0..4 {
        let mut article = posts[0].articles().nth(ordinal).unwrap().to_vec();
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
    for ordinal in 1..posts[2].articles().count() as u32 {
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
    assert_eq!(read(&echoed), body);
    assert_eq!(pipeline.graph_jobs[&job_id].phase(), RootPhase::Done);
    retire_pipeline_database(pipeline).await;
}

#[test]
fn parse_defaults_to_graph_when_unset_or_blank() {
    assert_eq!(Engine::parse(None), Engine::Graph);
    assert_eq!(Engine::parse(Some("")), Engine::Graph);
    assert_eq!(Engine::parse(Some("  ")), Engine::Graph);
    assert_eq!(Engine::default(), Engine::Graph);
}

#[test]
fn parse_reads_both_engines() {
    assert_eq!(Engine::parse(Some("classic")), Engine::Classic);
    assert_eq!(Engine::parse(Some("graph")), Engine::Graph);
}

#[test]
fn parse_ignores_case() {
    assert_eq!(Engine::parse(Some("GRAPH")), Engine::Graph);
    assert_eq!(Engine::parse(Some("Graph")), Engine::Graph);
    assert_eq!(Engine::parse(Some("ClAsSiC")), Engine::Classic);
    assert_eq!(Engine::parse(Some(" graph ")), Engine::Graph);
}

#[test]
fn parse_falls_back_to_graph_on_an_unknown_value() {
    assert_eq!(Engine::parse(Some("turbo")), Engine::Graph);
    assert_eq!(Engine::parse(Some("graph2")), Engine::Graph);
}

#[test]
fn spelling_round_trips() {
    for engine in [Engine::Classic, Engine::Graph] {
        assert_eq!(engine.as_str().parse::<Engine>(), Ok(engine));
        assert_eq!(engine.to_string(), engine.as_str());
    }
    assert_eq!(
        "turbo".parse::<Engine>(),
        Err(UnknownEngine("turbo".to_string()))
    );
}

#[test]
fn classic_switches_apply_only_to_classic() {
    assert!(Engine::Classic.classic_switches_apply());
    assert!(!Engine::Graph.classic_switches_apply());
}

mod refusals;
