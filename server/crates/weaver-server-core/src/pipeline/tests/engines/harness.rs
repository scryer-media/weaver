// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The two-engine harness: the API is described in `mod.rs`. A run stands a
// fresh pipeline up, admits the scenario's job under one engine, and plays
// the server: every article the job's download queue hands out is answered
// from the posts, damaged or lost as the scenario says, and whenever the
// queue runs dry the completion gate runs the way the orchestrator runs it,
// with every detached ticket settled before the next look. The run ends when
// the job is complete or failed; a round that changes nothing ends it too,
// with the job's state in the panic.

use std::collections::BTreeMap;
use std::path::Path;

use weaver_graph::recovery::RepairOutcome;
use weaver_model::files::FileRole;

use super::super::{
    debug_job_state, insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retire_pipeline_database, settle_direct_demotion_work,
    settle_direct_verification_read,
};
use crate::history::HistoryFailureKind;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, JobStatus, SegmentSpec};
use crate::pipeline::graph::Engine;
use crate::pipeline::{
    DecodeResult, DecodedChunk, Pipeline, SegmentEncoding, SegmentSource, YencLayoutAssertions,
};

pub(super) const ARTICLE: usize = 64;

// Every run is its own pipeline, so one id serves every scenario.
const JOB: JobId = JobId(93_001);
const JOB_NAME: &str = "lantern beacon";

pub(super) struct Post {
    pub(super) name: String,
    pub(super) role: FileRole,
    pub(super) bytes: Vec<u8>,
}

impl Post {
    pub(super) fn plain(name: &str, bytes: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            role: FileRole::from_filename(name),
            bytes,
        }
    }

    fn articles(&self) -> impl Iterator<Item = &[u8]> {
        self.bytes.chunks(ARTICLE)
    }
}

pub(super) fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed) | 0x80)
        .collect()
}

// An index and one volume of `blocks` recovery blocks over `files`, the
// index first.
pub(super) fn par2_set(base: &str, files: &[(&str, &[u8])], blocks: u32) -> Vec<Post> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

    let source = tempfile::tempdir().unwrap();
    let inputs = files
        .iter()
        .map(|(name, bytes)| {
            let path = source.path().join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let mut options = Par2CreatorOptions::with_output(
        source.path().join(format!("{base}.par2")),
        Some(source.path().to_path_buf()),
        inputs,
    );
    options.block_sizing = BlockSizing::Bytes(ARTICLE as u64);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_count = Some(1);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let mut posts: Vec<Post> = created
        .output_paths
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect();
    posts.sort_by_key(|post| !matches!(post.role, FileRole::Par2 { is_index: true, .. }));
    assert_eq!(posts.len(), 2, "an index and one volume");
    posts
}

// For each recovery block of a PAR2 volume, the ordinals of the articles
// holding its bytes.
pub(super) fn recovery_blocks(post: &Post) -> Vec<Vec<u32>> {
    const HEADER: usize = 64;
    const RECOVERY: &[u8; 16] = b"PAR 2.0\0RecvSlic";
    let bytes = &post.bytes;
    let mut blocks = Vec::new();
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        assert_eq!(&bytes[at..at + 8], b"PAR2\0PKT", "a packet at {at}");
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] == RECOVERY {
            // The exponent leads the block.
            let (start, end) = (at + HEADER + 4, at + length);
            blocks.push(((start / ARTICLE) as u32..=((end - 1) / ARTICLE) as u32).collect());
        }
        at += length;
    }
    blocks
}

// An article of a PAR2 volume whose loss costs exactly one recovery block.
pub(super) fn one_block_article(post: &Post) -> u32 {
    let blocks = recovery_blocks(post);
    blocks
        .iter()
        .flatten()
        .copied()
        .find(|ordinal| {
            blocks
                .iter()
                .filter(|block| block.contains(ordinal))
                .count()
                == 1
        })
        .expect("an article inside one block alone")
}

pub(super) struct Scenario {
    pub(super) posts: Vec<Post>,
    pub(super) damage: Vec<(u32, u32)>,
    pub(super) lost: Vec<(u32, u32)>,
}

impl Scenario {
    pub(super) fn new(posts: Vec<Post>) -> Self {
        Self {
            posts,
            damage: Vec::new(),
            lost: Vec::new(),
        }
    }

    pub(super) fn damaged(mut self, file: u32, ordinal: u32) -> Self {
        self.damage.push((file, ordinal));
        self
    }

    pub(super) fn lost(mut self, file: u32, ordinal: u32) -> Self {
        self.lost.push((file, ordinal));
        self
    }

    fn spec(&self) -> JobSpec {
        JobSpec {
            name: JOB_NAME.to_string(),
            password: None,
            total_bytes: self.posts.iter().map(|post| post.bytes.len() as u64).sum(),
            category: None,
            metadata: vec![],
            files: self
                .posts
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Claim {
    // Delivered at its length, every byte as posted.
    Complete,
    // Delivered at its length, some bytes not as posted.
    Damaged,
    // Not delivered, or delivered short.
    Missing,
}

// The graph job as last seen before it finished.
#[derive(Clone, Debug, Default)]
pub(super) struct GraphView {
    // Payload members by output name, read off the namespace's marks.
    pub(super) claims: BTreeMap<String, Claim>,
    pub(super) blocks_reachable: u64,
    pub(super) outcomes: Vec<Option<RepairOutcome>>,
}

#[derive(Debug)]
pub(super) struct Outcome {
    pub(super) engine: Engine,
    pub(super) status: Option<JobStatus>,
    pub(super) failure: Option<String>,
    pub(super) failure_kind: Option<HistoryFailureKind>,
    pub(super) files: BTreeMap<String, Vec<u8>>,
    pub(super) claims: BTreeMap<String, Claim>,
    pub(super) requests: BTreeMap<(u32, u32), usize>,
    pub(super) reads: u64,
    pub(super) graph: Option<GraphView>,
    // The posts that are PAR2 recovery volumes, by file index.
    pub(super) volumes: Vec<u32>,
}

impl Outcome {
    pub(super) fn refetched(&self) -> Vec<(u32, u32)> {
        self.requests
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(article, _)| *article)
            .collect()
    }

    pub(super) fn completed(&self) -> bool {
        self.status == Some(JobStatus::Complete)
    }

    // The first article of a recovery volume, asked for when `other` never
    // asked for it: the graph samples each volume's head to learn what it
    // holds, which classic never needs while nothing is damaged.
    pub(super) fn volume_samples(&self, other: &Outcome) -> Vec<(u32, u32)> {
        self.requests
            .keys()
            .filter(|(file, ordinal)| *ordinal == 0 && self.volumes.contains(file))
            .filter(|article| !other.requests.contains_key(article))
            .copied()
            .collect()
    }

    pub(super) fn graph(&self) -> &GraphView {
        self.graph.as_ref().expect("a graph run keeps its view")
    }
}

pub(super) async fn run_both(scenario: &Scenario) -> (Outcome, Outcome) {
    (
        run(scenario, Engine::Classic).await,
        run(scenario, Engine::Graph).await,
    )
}

pub(super) fn assert_same(classic: &Outcome, graph: &Outcome) {
    for outcome in [classic, graph] {
        assert!(
            outcome.refetched().is_empty(),
            "{} asked for an article twice: {:?}",
            outcome.engine,
            outcome.refetched()
        );
    }
    assert_eq!(classic.status, graph.status, "status");
    assert_eq!(classic.failure_kind, graph.failure_kind, "failure kind");
    assert_eq!(
        classic.files.keys().collect::<Vec<_>>(),
        graph.files.keys().collect::<Vec<_>>(),
        "delivered names"
    );
    for (name, bytes) in &classic.files {
        assert!(
            graph.files[name] == *bytes,
            "{name}: delivered bytes differ"
        );
    }
    assert_eq!(classic.claims, graph.claims, "claims");
    let samples = graph.volume_samples(classic);
    let mut asked = graph.requests.clone();
    asked.retain(|article, _| !samples.contains(article));
    assert_eq!(
        classic.requests, asked,
        "article requests, volume samples aside"
    );
}

pub(super) async fn run(scenario: &Scenario, engine: Engine) -> Outcome {
    let temp_dir = tempfile::tempdir().unwrap();
    // One server: an article it serves damaged has nowhere else to come
    // from, so both engines give it up for repair at once.
    let buffers = crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    };
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_buffers(&temp_dir, buffers, 1).await;
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    pipeline.admit_graph_job(JOB, engine);
    assert_eq!(pipeline.is_graph_job(JOB), engine == Engine::Graph);

    let mut requests = BTreeMap::new();
    let mut graph = (engine == Engine::Graph).then(GraphView::default);
    let mut reads = 0;
    let mut stalled = None;
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
            serve(&mut pipeline, scenario, article).await;
            observe(&pipeline, &mut graph, &mut reads);
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline, &requests);
        // The idle tick: bytes held out of order behind an article that
        // will never commit are written out.
        if pipeline.write_buffered_bytes > 0 {
            pipeline.flush_quiescent_write_backlog().await;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
        }
        let after = fingerprint(&pipeline, &requests);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!(
                "{engine} job stalled at {after}: {}",
                debug_job_state(&pipeline, JOB)
            );
        }
    }

    if engine == Engine::Classic {
        reads = (pipeline.par2_authoritative_verify_calls + pipeline.par2_selective_verify_calls)
            as u64;
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let failure = match &status {
        Some(JobStatus::Failed { error }) => Some(error.clone()),
        _ => None,
    };
    pipeline.db.flush_write_queue().await.unwrap();
    let failure_kind = pipeline
        .db
        .nzbget_history_states(&[JOB.0])
        .unwrap()
        .get(&JOB.0)
        .and_then(|state| state.failure_kind);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    let claims = scenario
        .posts
        .iter()
        .filter(|post| post.role.counts_toward_health())
        .map(|post| (post.name.clone(), delivered_claim(&files, post)))
        .collect();
    retire_pipeline_database(pipeline).await;
    Outcome {
        engine,
        status,
        failure,
        failure_kind,
        files,
        claims,
        requests,
        reads,
        graph,
        volumes: scenario
            .posts
            .iter()
            .zip(0u32..)
            .filter(|(post, _)| {
                matches!(
                    post.role,
                    FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            })
            .map(|(_, index)| index)
            .collect(),
    }
}

// One article as the scenario's server answers it.
async fn serve(pipeline: &mut Pipeline, scenario: &Scenario, (file, ordinal): (u32, u32)) {
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    };
    if scenario.lost.contains(&(file, ordinal)) {
        pipeline.book_failed_segment(segment_id);
        return;
    }
    let intact = !scenario.damage.contains(&(file, ordinal));
    let post = &scenario.posts[file as usize];
    let offset = ordinal as usize * ARTICLE;
    let posted = post.articles().nth(ordinal as usize).unwrap();
    // The checksum the article declares is the posted one, so damaged bytes
    // disagree with it.
    let crc = par2_rs::checksum::crc32(posted);
    let mut body = posted.to_vec();
    if !intact {
        body.iter_mut().take(8).for_each(|byte| *byte ^= 0x5a);
    }
    let len = body.len() as u64;
    let checkpoint_plan = pipeline.par2_checkpoint_plan(JOB);
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id,
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
                checkpoint_plan,
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
    // What the select loop would pick up after the article: the read-back
    // a set's last article submits, and a demotion's handback.
    settle_direct_verification_read(pipeline, JOB).await;
    settle_direct_demotion_work(pipeline).await;
}

fn finished(pipeline: &Pipeline) -> bool {
    matches!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete | JobStatus::Failed { .. }) | None
    )
}

// What a gate round can change: when none of it moved, nothing will.
fn fingerprint(pipeline: &Pipeline, requests: &BTreeMap<(u32, u32), usize>) -> String {
    let queued = pipeline.jobs.get(&JOB).map(|state| {
        (
            state.download_queue.len(),
            state.recovery_queue.len(),
            state.status.clone(),
        )
    });
    format!(
        "{queued:?} {} {} {}",
        pipeline.inflight_moves.contains(&JOB),
        requests.values().sum::<usize>(),
        pipeline.pending_completion_checks.len()
    )
}

// The graph job as it stands; a finished job has left the pipeline, so the
// last look before that is what the outcome keeps.
fn observe(pipeline: &Pipeline, view: &mut Option<GraphView>, reads: &mut u64) {
    let (Some(view), Some(job), Some(state)) = (
        view.as_mut(),
        pipeline.graph_jobs.get(&JOB),
        pipeline.jobs.get(&JOB),
    ) else {
        return;
    };
    let namespace = job.namespace();
    view.claims = state
        .spec
        .files
        .iter()
        .zip(0u32..)
        .filter(|(file, _)| file.role.counts_toward_health())
        .filter_map(|(_, index)| namespace.get(weaver_graph::MemberId(index)))
        .map(|member| {
            let damaged = member.coverage.damaged_ranges().next().is_some();
            let claim = match member.length {
                Some(_) if member.is_complete() && !damaged => Claim::Complete,
                Some(_) if member.is_complete() => Claim::Damaged,
                _ => Claim::Missing,
            };
            (member.output_name.clone(), claim)
        })
        .collect();
    let summary = job.recovery_summary();
    view.blocks_reachable = summary.iter().map(|set| set.blocks_reachable).sum();
    view.outcomes = summary.iter().map(|set| set.outcome).collect();
    *reads = job.writer().read_backs();
}

fn delivered_claim(files: &BTreeMap<String, Vec<u8>>, post: &Post) -> Claim {
    let delivered = files.iter().find(|(path, _)| {
        Path::new(path).file_name().and_then(|name| name.to_str()) == Some(post.name.as_str())
    });
    match delivered {
        Some((_, bytes)) if *bytes == post.bytes => Claim::Complete,
        Some((_, bytes)) if bytes.len() == post.bytes.len() => Claim::Damaged,
        _ => Claim::Missing,
    }
}

// The delivered files, without the marker every output directory carries,
// whose bytes name the run's own paths.
fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.file_name() == Some(std::ffi::OsStr::new(".weaver-output-dir")) {
            continue;
        }
        if path.is_dir() {
            collect(root, &path, files);
        } else {
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            files.insert(name, std::fs::read(&path).unwrap());
        }
    }
}
