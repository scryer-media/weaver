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

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use weaver_graph::recovery::RepairOutcome;
use weaver_model::files::FileRole;

use super::super::{
    debug_job_state, insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retention_client, retire_pipeline_database,
    settle_direct_demotion_work, settle_direct_verification_read,
};
use crate::history::HistoryFailureKind;
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, JobStatus, SegmentSpec};
use crate::pipeline::download::DownloadWork;
use crate::pipeline::extraction::ExtractionLimits;
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::status::Delivery;
use crate::pipeline::{
    DecodeResult, DecodedChunk, Pipeline, PipelineEvent, SegmentEncoding, SegmentSource,
    YencLayoutAssertions,
};

pub(super) const ARTICLE: usize = 64;

// Every run is its own pipeline, so one id serves every scenario.
pub(in crate::pipeline::tests) const JOB: JobId = JobId(93_001);
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

    fn articles(&self, size: usize) -> impl Iterator<Item = &[u8]> {
        self.bytes.chunks(size)
    }
}

// `len` bytes of a seeded xorshift: no period runs through them, so no two
// blocks are alike and a compressing level cannot fold them, and the same
// seed gives the same bytes every run.
pub(super) fn payload(len: usize, seed: u8) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(seed);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 56) as u8
        })
        .collect()
}

// An index and one volume of `blocks` recovery blocks over `files`, the
// index first.
pub(super) fn par2_set(base: &str, files: &[(&str, &[u8])], blocks: u32) -> Vec<Post> {
    let posts = par2_volumes(base, files, blocks, 1);
    assert_eq!(posts.len(), 2, "an index and one volume");
    posts
}

// An index and `volumes` volumes sharing `blocks` recovery blocks, as the
// creator spreads them, the index first and the volumes in block order.
pub(super) fn par2_volumes(
    base: &str,
    files: &[(&str, &[u8])],
    blocks: u32,
    volumes: u32,
) -> Vec<Post> {
    par2_volumes_sliced(base, files, ARTICLE as u64, blocks, volumes)
}

// The same, sliced at `slice` bytes rather than ARTICLE.
pub(super) fn par2_volumes_sliced(
    base: &str,
    files: &[(&str, &[u8])],
    slice: u64,
    blocks: u32,
    volumes: u32,
) -> Vec<Post> {
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
    options.block_sizing = BlockSizing::Bytes(slice);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_count = Some(volumes);
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
    posts.sort_by(|left, right| {
        let index = |post: &Post| !matches!(post.role, FileRole::Par2 { is_index: true, .. });
        index(left)
            .cmp(&index(right))
            .then_with(|| left.name.cmp(&right.name))
    });
    posts
}

// A PAR3 index and its volumes over `files`, blocks of one article, named
// as the creator names them under `base`, the index first.
pub(super) fn par3_set(base: &str, files: &[(&str, &[u8])], blocks: u64) -> Vec<Post> {
    let source = tempfile::tempdir().unwrap();
    let names: Vec<std::path::PathBuf> = files
        .iter()
        .map(|(name, bytes)| {
            std::fs::write(source.path().join(name), bytes).unwrap();
            std::path::PathBuf::from(name)
        })
        .collect();
    let out = tempfile::tempdir().unwrap();
    let report = par3_rs::create(
        &par3_rs::InputSpec::new(source.path(), &names),
        &out.path().join(base),
        &par3_rs::CreateOptions::default()
            .with_block_size(ARTICLE as u64)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(blocks)),
    )
    .unwrap();
    let mut written = report.files_written.clone();
    written.sort_by_key(|path| path.to_string_lossy().contains(".vol"));
    written
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect()
}

// A PAR3 set over `files` built as `options` say (its codec, its recovery
// count, whether it carries its input as Data packets), blocks of one
// article, named as the creator names them under `base`, the index first.
pub(super) fn par3_set_with(
    base: &str,
    files: &[(&str, &[u8])],
    options: par3_rs::creation::CreationOptions,
) -> Vec<Post> {
    use par3_rs::creation::{CreationPlan, CreationSource};
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut access = MemorySourceAccess::default();
    let mut sources = Vec::new();
    for (index, (name, bytes)) in files.iter().enumerate() {
        let source = SourceId(index as u64 + 1);
        access.insert(source, 1, bytes.to_vec().into());
        sources.push(CreationSource {
            name: (*name).into(),
            source,
        });
    }
    let options = par3_rs::creation::CreationOptions {
        block_size: ARTICLE as u64,
        ..options
    };
    let plan = CreationPlan::build(std::sync::Arc::new(access), &sources, options).unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut written = plan.execute(&out.path().join(base), out.path()).unwrap();
    written.sort_by_key(|path| {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        (name != format!("{base}.par3"), name)
    });
    written
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect()
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
    // Articles the first ask is refused for, as a server without them
    // answers: the article goes back on the queue the way the worker
    // retries it after a 430, and the next ask is served.
    pub(super) refused: Vec<(u32, u32)>,
    // Articles the primary server serves damaged while a backup serves them
    // whole. A scenario with any has both servers, and an ask that excludes
    // the primary is the backup's.
    pub(super) corrupt_primary: Vec<(u32, u32)>,
    pub(super) password: Option<String>,
    pub(super) global_passwords: Vec<String>,
    // The decoded size of every article but each post's last; PAR2 slices
    // stay at ARTICLE whatever it is.
    pub(super) article: usize,
    // Every batch the download queue hands out is served last article first.
    pub(super) backwards: bool,
    // Every batch the download queue hands out is served as workers fetching
    // these posts at once: one article of each in turn, in this order.
    pub(super) together: Vec<u32>,
    // A restarted run serves these articles first, in this order, each once
    // the queue holds it.
    pub(super) first: Vec<(u32, u32)>,
    // Every article is uuencoded: it says nothing of where its bytes go,
    // so they are placed in order.
    pub(super) uuencoded: bool,
    // Every batch is leased as a download lane leases it, and its articles'
    // checksums are cut at the slice sizes known when it was leased.
    pub(super) leased: bool,
    // The extraction limits the pipeline holds, when not the environment's.
    pub(super) limits: Option<ExtractionLimits>,
    // The operator's unacceptable extensions, when not the stored ones.
    pub(super) unacceptable: Option<Vec<String>>,
}

impl Scenario {
    pub(super) fn new(posts: Vec<Post>) -> Self {
        Self {
            posts,
            damage: Vec::new(),
            lost: Vec::new(),
            refused: Vec::new(),
            corrupt_primary: Vec::new(),
            password: None,
            global_passwords: Vec::new(),
            article: ARTICLE,
            backwards: false,
            together: Vec::new(),
            first: Vec::new(),
            uuencoded: false,
            leased: false,
            limits: None,
            unacceptable: None,
        }
    }

    // The job runs under these extraction limits.
    pub(super) fn limits(mut self, limits: ExtractionLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    // The settings refuse these extensions.
    pub(super) fn unacceptable(mut self, extensions: &[&str]) -> Self {
        self.unacceptable = Some(extensions.iter().map(|ext| ext.to_string()).collect());
        self
    }

    // What the scenario sets on a pipeline before its job is admitted or
    // restored: the settings' passwords and policy, and the limits.
    pub(super) fn prepare(&self, pipeline: &mut Pipeline) {
        if !self.global_passwords.is_empty() {
            pipeline
                .db
                .save_archive_password_settings(Some(self.global_passwords.clone()), None)
                .unwrap();
        }
        if let Some(extensions) = &self.unacceptable {
            pipeline
                .db
                .save_unacceptable_extensions(extensions.clone())
                .unwrap();
            // A save forgets whether any script could run; the runtime works
            // it out at startup, so the job's scripts gate does not wait.
            crate::post_processing::events::refresh_admission_hint(&pipeline.db).unwrap();
        }
        if let Some(limits) = &self.limits {
            pipeline.extraction_limits = std::sync::Arc::new(limits.clone());
        }
    }

    // Whatever the download queue hands out at once arrives in reverse.
    pub(super) fn backwards(mut self) -> Self {
        self.backwards = true;
        self
    }

    // Whatever the download queue hands out at once arrives as workers
    // fetching `posts` side by side: the first article of each, in this
    // order, then the second of each, and so on.
    pub(super) fn together(mut self, posts: &[u32]) -> Self {
        self.together = posts.to_vec();
        self
    }

    // A restarted run serves these articles before any other, in this
    // order, so a restart lands between two of them.
    pub(super) fn first(mut self, articles: &[(u32, u32)]) -> Self {
        self.first = articles.to_vec();
        self
    }

    // Every article posted uuencoded rather than yEnc.
    pub(super) fn uuencoded(mut self) -> Self {
        self.uuencoded = true;
        self
    }

    // Every batch leased by the download scheduler, served last article
    // first, each cut at the slice sizes its lease was taken under.
    pub(super) fn leased(mut self) -> Self {
        self.leased = true;
        self
    }

    // Every post cut into articles of `size` bytes instead of ARTICLE.
    pub(super) fn framed(mut self, size: usize) -> Self {
        self.article = size;
        self
    }

    // The password the job is submitted with.
    pub(super) fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    // A password the settings carry, not the job: classic tries it once
    // the volumes are in, graph offers it when an object asks.
    pub(super) fn global_password(mut self, password: &str) -> Self {
        self.global_passwords.push(password.to_string());
        self
    }

    pub(super) fn damaged(mut self, file: u32, ordinal: u32) -> Self {
        self.damage.push((file, ordinal));
        self
    }

    pub(super) fn lost(mut self, file: u32, ordinal: u32) -> Self {
        self.lost.push((file, ordinal));
        self
    }

    pub(super) fn refused_once(mut self, file: u32, ordinal: u32) -> Self {
        self.refused.push((file, ordinal));
        self
    }

    pub(super) fn corrupt_on_primary(mut self, file: u32, ordinal: u32) -> Self {
        self.corrupt_primary.push((file, ordinal));
        self
    }

    pub(super) fn spec(&self) -> JobSpec {
        JobSpec {
            name: JOB_NAME.to_string(),
            password: self.password.clone(),
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
                        .articles(self.article)
                        .enumerate()
                        .map(|(ordinal, article)| SegmentSpec {
                            ordinal: ordinal as u32,
                            article_number: ordinal as u32,
                            bytes: article.len() as u32,
                            message_ids: crate::jobs::MessageIds::new(vec![format!(
                                "lantern-{index}-{ordinal}@example.com"
                            )]),
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
    // The kind of every object the tree held at any look, by its name.
    pub(super) kinds: BTreeSet<String>,
    // How many password candidates each node was offered, at the last look.
    pub(super) offered: BTreeMap<weaver_graph::NodeId, usize>,
    // The files the job hands over at every depth, by output name.
    pub(super) delivery: Delivery,
    // What the job's checkpoints asked the database to write, at the last
    // look.
    pub(super) checkpoints: crate::pipeline::graph::checkpoint::Tally,
    // The most the live namespaces objects produced kept resident for their
    // members at any look, summed from the tree.
    pub(super) resident: u64,
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
    // Every article in the order it was asked for.
    pub(super) order: Vec<(u32, u32)>,
    pub(super) reads: u64,
    pub(super) graph: Option<GraphView>,
    // The posts that are PAR2 or PAR3 recovery volumes, by file index.
    pub(super) volumes: Vec<u32>,
    // Every pipeline event the job's run sent, in order.
    pub(super) events: Vec<PipelineEvent>,
}

// The kind of every object a graph run's tree held, by its name, sorted.
pub(super) fn kinds(graph: &Outcome) -> Vec<&str> {
    graph.graph().kinds.iter().map(String::as_str).collect()
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

// The repair and terminal events the run sent, in order, as words a row
// can compare: `RepairComplete <slices>`, `JobCompleted`, `JobFailed`.
pub(super) fn repair_and_terminal_events(outcome: &Outcome) -> Vec<String> {
    outcome
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
            PipelineEvent::JobFailed { job_id, .. } if *job_id == JOB => {
                Some("JobFailed".to_string())
            }
            _ => None,
        })
        .collect()
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

// A run whose gate rounds stopped changing anything before the job
// finished: the status it sat at, and the pipeline's account of the job.
#[derive(Debug)]
pub(super) struct Stall {
    pub(super) engine: Engine,
    pub(super) status: Option<JobStatus>,
    pub(super) state: String,
}

pub(super) async fn run(scenario: &Scenario, engine: Engine) -> Outcome {
    try_run(scenario, engine).await.unwrap_or_else(|stall| {
        panic!(
            "{} job stalled at {:?}: {}",
            stall.engine, stall.status, stall.state
        )
    })
}

// A run that may stall, for a row that asserts an engine does.
// A fresh pipeline with the scenario's job admitted under `engine`, nothing
// asked yet.
pub(super) async fn admit(
    scenario: &Scenario,
    engine: Engine,
) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
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
    if !scenario.corrupt_primary.is_empty() {
        pipeline.nntp = std::sync::Arc::new(retention_client(&[0, 0]));
    }
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    pipeline.admit_graph_job(JOB, engine);
    assert_eq!(pipeline.is_graph_job(JOB), engine == Engine::Graph);
    (temp_dir, pipeline, complete_dir)
}

pub(super) async fn try_run(scenario: &Scenario, engine: Engine) -> Result<Outcome, Stall> {
    let (_temp_dir, mut pipeline, complete_dir) = admit(scenario, engine).await;
    // Events the run sends are heard from admission on; nothing sent before
    // the job was admitted belongs to it.
    let mut sent = pipeline.event_tx.subscribe();
    let mut refused = BTreeSet::new();
    let mut requests = BTreeMap::new();
    let mut order = Vec::new();
    let mut graph = (engine == Engine::Graph).then(GraphView::default);
    let mut reads = 0;
    let mut stalled = None;
    loop {
        while let Some((batch, plan)) = handed_out(&mut pipeline, scenario) {
            for work in batch {
                let article = (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                );
                *requests.entry(article).or_insert(0usize) += 1;
                order.push(article);
                if scenario.refused.contains(&article) && refused.insert(article) {
                    // Rebuilt as the worker's retry rebuilds it: the retry
                    // carries no promotion of its own, so where it is queued is
                    // the job's answer to whether anything waits on it.
                    let mut retry = work;
                    retry.retry_count += 1;
                    retry.completion_critical = false;
                    pipeline.enqueue_download_work(retry);
                    continue;
                }
                serve_from(
                    &mut pipeline,
                    JOB,
                    scenario,
                    article,
                    &work.exclude_servers,
                    plan.as_ref(),
                )
                .await;
                observe(&pipeline, &mut graph, &mut reads);
            }
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
        // A run or a repair out on a thread speaks through the pipeline's mail, as the
        // orchestrator hears it: what it says is the job's next move.
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
        // A body a server spoiled is asked of the next server once the
        // worker's backoff ends: the retry it scheduled is the job's next
        // move.
        if pipeline
            .pending_retries_by_job
            .get(&JOB)
            .is_some_and(|pending| *pending > 0)
        {
            let retry = pipeline
                .retry_rx
                .recv()
                .await
                .expect("a scheduled retry is sent");
            pipeline.receive_retry_work(retry);
            continue;
        }
        // Classic's extraction runs out on a task too, and says it is done
        // the same way.
        if pipeline
            .inflight_extractions
            .get(&JOB)
            .is_some_and(|sets| !sets.is_empty())
        {
            let done = pipeline
                .extract_done_rx
                .recv()
                .await
                .expect("an extraction in flight reports");
            pipeline.handle_extraction_done(done).await;
            continue;
        }
        let after = fingerprint(&pipeline, &requests);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            let status = job_status_for_assert(&pipeline, JOB);
            let state = format!("{after}: {}", debug_job_state(&pipeline, JOB));
            retire_pipeline_database(pipeline).await;
            return Err(Stall {
                engine,
                status,
                state,
            });
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
    let mut events = Vec::new();
    loop {
        match sent.try_recv() {
            Ok(event) => events.push(event),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(error) => panic!("the run's events were not all kept: {error}"),
        }
    }
    retire_pipeline_database(pipeline).await;
    Ok(Outcome {
        engine,
        status,
        failure,
        failure_kind,
        files,
        claims,
        requests,
        order,
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
                    } | FileRole::Par3 { is_index: false }
                )
            })
            .map(|(_, index)| index)
            .collect(),
        events,
    })
}

// A graph run whose completion gate runs only when the pipeline itself
// scheduled it, as the orchestrator's loop runs it: no idle look is taken.
// Every article is answered one at a time and ends as a decode ends, each
// check a step scheduled is run, and a live run is heard. Returns where the
// job stands once nothing is queued, scheduled or running: the recovery
// outcomes as last seen, and whether its delivery move is out.
pub(super) async fn run_unprompted(scenario: &Scenario) -> (Vec<Option<RepairOutcome>>, bool) {
    fn look(pipeline: &Pipeline, outcomes: &mut Vec<Option<RepairOutcome>>) {
        if let Some(job) = pipeline.graph_jobs.get(&JOB) {
            *outcomes = job
                .recovery_summary()
                .iter()
                .map(|set| set.outcome)
                .collect();
        }
    }
    let (_temp_dir, mut pipeline, _) = admit(scenario, Engine::Graph).await;
    let mut outcomes = Vec::new();
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
            serve(&mut pipeline, scenario, article).await;
            // The tail of every decode: the last one ends the download pass.
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            look(&pipeline, &mut outcomes);
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            look(&pipeline, &mut outcomes);
            pipeline.check_job_completion(queued).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            look(&pipeline, &mut outcomes);
            continue;
        }
        break;
    }
    look(&pipeline, &mut outcomes);
    let moved = pipeline.inflight_moves.contains(&JOB) || finished(&pipeline);
    retire_pipeline_database(pipeline).await;
    (outcomes, moved)
}

// The next articles the download queue hands out: one, or with `backwards`
// every one it holds, last first, or with `together` every one it holds, a
// turn of each named post at a time, or with `leased` the batch a lane
// leases, last first, with the slice sizes it was leased under. `None` once
// it is dry.
fn handed_out(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
) -> Option<(Vec<DownloadWork>, Option<weaver_yenc::CheckpointPlan>)> {
    if scenario.leased {
        let lease = pipeline.lease_for_server_for_test(0)?;
        let mut works = lease.works;
        works.reverse();
        return Some((works, Some(lease.checkpoint_plan)));
    }
    let queue = &mut pipeline.jobs.get_mut(&JOB)?.download_queue;
    let all = scenario.backwards || !scenario.together.is_empty();
    let mut batch = Vec::new();
    while let Some(work) = queue.pop() {
        batch.push(work);
        if !all {
            break;
        }
    }
    if scenario.together.is_empty() {
        batch.reverse();
    } else {
        // An article's turn is its number in its post, then its post's
        // place in the order; a post not named comes after.
        batch.sort_by_key(|work| {
            let file = work.segment_id.file_id.file_index;
            let at = scenario.together.iter().position(|named| *named == file);
            (
                work.segment_id.segment_number,
                at.unwrap_or(scenario.together.len()),
            )
        });
    }
    (!batch.is_empty()).then_some((batch, None))
}

// One article as the scenario's server answers it.
pub(super) async fn serve(pipeline: &mut Pipeline, scenario: &Scenario, article: (u32, u32)) {
    serve_job(pipeline, JOB, scenario, article).await;
}

// [`serve`] for a job other than the run's own.
pub(super) async fn serve_job(
    pipeline: &mut Pipeline,
    job: JobId,
    scenario: &Scenario,
    article: (u32, u32),
) {
    serve_from(pipeline, job, scenario, article, &[], None).await;
}

// [`serve_job`] for an ask that excludes `exclude`: the first server it
// leaves answers.
async fn serve_from(
    pipeline: &mut Pipeline,
    job: JobId,
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
    exclude: &[usize],
    leased: Option<&weaver_yenc::CheckpointPlan>,
) {
    let server = (0..).find(|server| !exclude.contains(server)).unwrap();
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: ordinal,
    };
    if scenario.lost.contains(&(file, ordinal)) {
        pipeline.book_failed_segment(segment_id);
        return;
    }
    let intact = !scenario.damage.contains(&(file, ordinal))
        && !(server == 0 && scenario.corrupt_primary.contains(&(file, ordinal)));
    let post = &scenario.posts[file as usize];
    let offset = ordinal as usize * scenario.article;
    let posted = post
        .articles(scenario.article)
        .nth(ordinal as usize)
        .unwrap();
    // The checksum the article declares is the posted one, so damaged bytes
    // disagree with it.
    let crc = par2_rs::checksum::crc32(posted);
    let mut body = posted.to_vec();
    if !intact {
        body.iter_mut().take(8).for_each(|byte| *byte ^= 0x5a);
    }
    if scenario.uuencoded {
        // The body carries no part number, range or checksum; the last
        // article carries the `end` line.
        let ended = ordinal as usize + 1 == post.articles(scenario.article).count();
        let encoding = SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts {
            damaged: !intact,
            ended,
        });
        pipeline
            .handle_decode_success(
                DecodeResult {
                    segment_id,
                    raw_size: body.len() as u64,
                    encoding,
                    yenc_layout: YencLayoutAssertions {
                        file_size: 0,
                        part: None,
                        total: None,
                        begin: None,
                        end: None,
                    },
                    crc_valid: true,
                    part_crc_verified: false,
                    part_crc: 0,
                    truncation_suspected: false,
                    expected_file_crc: None,
                    data: DecodedChunk::from(body),
                    yenc_name: if ordinal == 0 {
                        post.name.clone()
                    } else {
                        String::new()
                    },
                    checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                    segments: Vec::new(),
                },
                SegmentSource {
                    source_server_idx: Some(server),
                    exclude_servers: exclude.to_vec(),
                },
            )
            .await;
        return;
    }
    let len = body.len() as u64;
    // The decoder checksums the bytes it decoded, cut at the block
    // boundaries of the plan the article was leased under: its lease's, or
    // what the job knows now when it was handed out alone.
    let checkpoint_plan = leased
        .cloned()
        .unwrap_or_else(|| pipeline.par2_checkpoint_plan(job));
    let mut checksums = weaver_yenc::SegmentedCrc32::new(offset as u64, checkpoint_plan.clone());
    checksums.update(&body);
    let (_, segments) = checksums.finish_article();
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id,
                raw_size: len,
                encoding: SegmentEncoding::Yenc,
                yenc_layout: YencLayoutAssertions {
                    file_size: post.bytes.len() as u64,
                    part: Some(ordinal + 1),
                    total: Some(post.articles(scenario.article).count() as u32),
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
                segments,
            },
            SegmentSource {
                source_server_idx: Some(server),
                exclude_servers: exclude.to_vec(),
            },
        )
        .await;
    // What the select loop would pick up after the article: the read-back
    // a set's last article submits, and a demotion's handback.
    settle_direct_verification_read(pipeline, job).await;
    settle_direct_demotion_work(pipeline).await;
}

pub(in crate::pipeline::tests) fn finished(pipeline: &Pipeline) -> bool {
    matches!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }) | None
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
    let tree = job.tree();
    view.kinds.extend(
        tree.live()
            .into_iter()
            .filter_map(|node| tree.kind(node))
            .map(|kind| kind.to_string()),
    );
    view.offered = job.passwords_offered();
    view.checkpoints = job.pending().tally();
    view.delivery = pipeline.graph_delivery(JOB).unwrap_or_default();
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
    view.blocks_reachable = summary
        .iter()
        .filter_map(|set| tree.recovery(set.node))
        .map(|recovery| {
            let mut sets = Vec::new();
            recovery.sets(&mut sets);
            sets.iter().map(|set| set.blocks_reachable).sum::<u64>()
        })
        .sum();
    view.outcomes = summary.iter().map(|set| set.outcome).collect();
    let resident = tree
        .live()
        .into_iter()
        .filter(|node| tree.volumes(*node).is_some())
        .filter_map(|node| tree.namespace(node))
        .flat_map(|namespace| {
            namespace
                .records()
                .map(|record| namespace.resident(record.id))
        })
        .sum();
    view.resident = view.resident.max(resident);
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
pub(super) fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
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
