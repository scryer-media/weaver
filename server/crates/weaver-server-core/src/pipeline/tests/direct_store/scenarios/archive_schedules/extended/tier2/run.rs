// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tier-two runner: one post, one schedule, one graph job.
//
// A schedule names four slots. Tier two posts more articles than that, so
// the data articles, in file order, are cut into four contiguous groups and
// slot `file * 2 + article` names group `file * 2 + article`: an arrival
// delivers the whole group in article order, a duplicate delivers it again,
// and boundaries fall between groups. A lost slot loses the middle article
// of its group rather than the whole group, so a loss stays inside what a
// recovery set sized for the cell can mend; a starved schedule withholds the
// recovery volumes instead.
//
// Every article reaches the pipeline as wire bytes through the production
// fused decoder, from a provider of one server: an article that fails to
// decode has nowhere else to come from and is given up for repair at once.
// After each article the pipeline moves as an orchestrator turn moves it,
// through the archive matrix's graph driver.
use super::super::super::graph::{fingerprint, restore, turn};
use super::post::{Post, Role};
use super::*;
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::engines::harness::{JOB, finished};

// What one run left behind.
pub(in super::super) struct Ran {
    pub status: Option<JobStatus>,
    // What the history row says failed the job, when it failed.
    pub failure_kind: Option<HistoryFailureKind>,
    // Each expected path, as the output directory holds it.
    pub files: BTreeMap<String, Option<Vec<u8>>>,
    pub published: BTreeSet<String>,
    pub leftovers: BTreeSet<String>,
    pub trace: Vec<String>,
}

// A provider of one server, so a decode failure books the article as
// exhausted instead of scheduling a timed retry. The pipeline is given no
// connections, so nothing ever dials it.
fn one_server(pipeline: &mut Pipeline) {
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: refusing_harness_server_port(),
                tls: false,
                ..Default::default()
            },
            max_connections: 1,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        }],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    }));
}

// The data articles cut into the four groups a schedule's slots name.
pub(in super::super) fn groups(post: &Post) -> Vec<Vec<(u32, u32)>> {
    let articles: Vec<(u32, u32)> = post
        .index_of(Role::Data)
        .into_iter()
        .flat_map(|file| {
            (0..post.files[file].articles()).map(move |article| (file as u32, article))
        })
        .collect();
    let per = articles.len().div_ceil(4).max(1);
    let mut groups: Vec<Vec<(u32, u32)>> = articles.chunks(per).map(<[_]>::to_vec).collect();
    groups.resize(4, Vec::new());
    groups
}

// The articles a schedule's loss mask takes: the middle article of each
// lost group, by file.
pub(in super::super) fn lost_articles(
    post: &Post,
    interruption: Interruption,
) -> BTreeMap<usize, BTreeSet<u32>> {
    let mut lost: BTreeMap<usize, BTreeSet<u32>> = BTreeMap::new();
    let Some((mask, _)) = interruption.loss() else {
        return lost;
    };
    for (slot, group) in groups(post).into_iter().enumerate() {
        if mask & (1 << slot) != 0
            && let Some(&(file, article)) = group.get(group.len() / 2)
        {
            lost.entry(file as usize).or_default().insert(article);
        }
    }
    lost
}

// Answers one request for an article with what the post has for it.
async fn deliver(pipeline: &mut Pipeline, post: &Post, file: u32, article: u32) {
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
    let job = JOB;
    retire_schedule_article(pipeline, job, file, article);
    for body in post.files[file as usize].wire_copies(article) {
        if !pipeline.jobs.contains_key(&job) || finished(pipeline) {
            return;
        }
        let segment = SegmentId {
            file_id: NzbFileId {
                job_id: job,
                file_index: file,
            },
            segment_number: article,
        };
        let raw = super::post::on_the_wire(&body);
        let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
        decoder.set_checkpoint_plan(checkpoint_plan(pipeline, job));
        let mut input = bytes::BytesMut::from(raw.as_slice());
        let source = || SegmentSource {
            source_server_idx: Some(0),
            exclude_servers: Vec::new(),
        };
        // A body that does not decode reaches the decode stage as a failed
        // decode, as a lane hands it over.
        let failed = |error: String| DecodeDone::Failed {
            segment_id: segment,
            raw_size: raw.len() as u64,
            error,
            source_server_idx: Some(0),
            exclude_servers: Vec::new(),
        };
        let done = match decoder.decode_available(&mut input) {
            Ok(Some(decoded)) => {
                let trace = DecodedBodyTrace {
                    attempts: Vec::new(),
                    result: Ok(DecodedBody {
                        raw_size: raw.len() as u32,
                        decoded: decoded.chunks,
                        body: decoded.body,
                        cpu: Default::default(),
                        io: Default::default(),
                    }),
                };
                let (payload, _, _) = Pipeline::download_data_from_decoded_trace(segment, trace);
                match payload {
                    Ok(DownloadPayload::Decoded(result)) => DecodeDone::Success {
                        result,
                        source: source(),
                    },
                    Err(DownloadError::Decode { error, .. }) => failed(error),
                    _ => panic!("a fused decode yields a decoded payload or a decode error"),
                }
            }
            Ok(None) => failed("article ended before its terminator".to_string()),
            Err(error) => failed(error.to_string()),
        };
        // The decode stage settles the article and, when it was the job's
        // last, ends the download pass.
        pipeline.note_decode_started(segment, raw.len() as u64);
        pipeline.handle_decode_done(done).await;
        turn(pipeline).await;
    }
}

// Books an article the one server does not have, as the downloader does
// once every server has answered 430.
async fn unavailable(pipeline: &mut Pipeline, file: u32, article: u32) {
    retire_schedule_article(pipeline, JOB, file, article);
    if !pipeline.jobs.contains_key(&JOB) {
        return;
    }
    pipeline.book_failed_segment(SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: article,
    });
    pipeline.maybe_finish_download_pass(JOB);
    turn(pipeline).await;
}

// Runs `post` under one schedule.
pub(in super::super) async fn run(
    post: &Post,
    order: &[(u32, u32)],
    interruption: Interruption,
) -> Ran {
    Box::pin(drive(post, order, interruption)).await
}

async fn drive(post: &Post, order: &[(u32, u32)], interruption: Interruption) -> Ran {
    enable_schedule_trace();
    let spec = post.spec();
    let groups = groups(post);
    let lost = lost_articles(post, interruption);
    let starved = interruption.fails();
    let is_lost = |file: u32, article: u32| {
        lost.get(&(file as usize))
            .is_some_and(|set| set.contains(&article))
    };
    // Whether a request for this article can be answered at all.
    let available = |file: u32, article: u32| {
        let Some(posted) = post.files.get(file as usize) else {
            return false;
        };
        !(posted.is_absent(article)
            || is_lost(file, article)
            || (starved && posted.role == Role::Recovery))
    };
    // One article as the one server answers it.
    let serve = async |pipeline: &mut Pipeline, file: u32, article: u32| {
        if available(file, article) {
            deliver(pipeline, post, file, article).await;
        } else {
            unavailable(pipeline, file, article).await;
        }
    };
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    one_server(&mut pipeline);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let index_first = interruption.loss().is_some_and(|(_, first)| first);
    let indices = post.index_of(Role::Index);
    let mut retired = None;
    let mut trace = vec![];
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    // The schedule delivers what the server has; what it lacks is answered
    // when the job asks for it.
    let deliver_index = async |pipeline: &mut Pipeline| {
        for &file in &indices {
            for article in 0..post.files[file].articles() {
                if available(file as u32, article) {
                    deliver(pipeline, post, file as u32, article).await;
                }
            }
        }
    };
    if index_first {
        deliver_index(&mut pipeline).await;
    }
    for (step, action, arrives) in interruption.boundaries(order.len()) {
        match action {
            BoundaryAction::None => {}
            action @ (BoundaryAction::Restart | BoundaryAction::Crash) => {
                if action == BoundaryAction::Restart {
                    pipeline.drain().await;
                }
                let status = job_status_for_assert(&pipeline, JOB);
                trace.push(format!("{action:?} at {step}: status={status:?}"));
                retire_pipeline_database(pipeline).await;
                let restored;
                (pipeline, restored) = restore(&root, &spec, &mut trace).await;
                one_server(&mut pipeline);
                if !restored {
                    retired = Some(status);
                    break;
                }
            }
        }
        if !arrives {
            continue;
        }
        let Some(&(file, article)) = order.get(step) else {
            break;
        };
        let slot = (file * 2 + article) as usize;
        for &(file, article) in &groups[slot] {
            if finished(&pipeline) {
                break;
            }
            if available(file, article) {
                deliver(&mut pipeline, post, file, article).await;
            }
        }
        trace.push(format!("arrive slot {slot}"));
    }
    if !index_first && retired.is_none() {
        deliver_index(&mut pipeline).await;
    }
    // Once the schedule is spent, every article the job asks for is served
    // until nothing is queued, scheduled or running. A round in which
    // nothing moved twice over is a stall, never a wait for time.
    let mut stalled = None;
    while retired.is_none() {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let (file, article) = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            trace.push(format!("requested {file}:{article}"));
            serve(&mut pipeline, file, article).await;
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if finished(&pipeline) {
            break;
        }
        // A run or a repair out on a thread speaks through the run mail:
        // what it says is the job's next move.
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        // A retry the worker scheduled is sent once its backoff ends.
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
        let after = fingerprint(&pipeline);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            // What the job's graph holds when nothing moves: its phase and
            // what each recovery set has ruled.
            let graph = pipeline
                .graph_jobs
                .get(&JOB)
                .map(|job| (job.phase(), job.recovery_summary()));
            panic!("tier-two run stalled at {after}: graph={graph:?} trace={trace:?}");
        }
    }
    let status = retired.unwrap_or_else(|| job_status_for_assert(&pipeline, JOB));
    let files = post
        .expected
        .iter()
        .map(|(name, _)| name)
        .chain(post.allowed.iter().map(|allowed| &allowed.name))
        .map(|name| (name.clone(), std::fs::read(output.join(name)).ok()))
        .collect();
    let mut published = files_under(&output);
    published.remove(crate::jobs::working_dir::OUTPUT_DIR_MARKER);
    let mut leftovers: BTreeSet<String> = files_under(&pipeline.intermediate_dir)
        .into_iter()
        .map(|path| format!("intermediate/{path}"))
        .collect();
    leftovers.extend(
        files_under(&complete.join(".weaver-staging"))
            .into_iter()
            .map(|path| format!("staging/{path}")),
    );
    let sizes: Vec<(String, Option<u64>, Option<usize>)> = published
        .iter()
        .map(|name| {
            (
                name.clone(),
                std::fs::metadata(output.join(name))
                    .ok()
                    .map(|meta| meta.len()),
                post.files
                    .iter()
                    .find(|posted| posted.name == *name)
                    .map(|posted| posted.bytes.len()),
            )
        })
        .collect();
    trace.push(format!(
        "terminal {status:?}; published (name, size, posted size)={sizes:?}"
    ));
    pipeline.db.flush_write_queue().await.unwrap();
    let failure_kind = pipeline
        .db
        .nzbget_history_states(&[JOB.0])
        .unwrap()
        .get(&JOB.0)
        .and_then(|state| state.failure_kind);
    retire_pipeline_database(pipeline).await;
    Ran {
        status,
        failure_kind,
        files,
        published,
        leftovers,
        trace,
    }
}

// What a run must end in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Verdict {
    // Every expected file published, byte for byte.
    Completes,
    // The recovery cannot mend what the post lost: the job does not
    // complete. It fails, named, publishing none of the expected files, or
    // it ends partial with the files it could deliver whole.
    Fails,
    // Any of the above; the oracle cannot tell which the bytes allow.
    Either,
}

impl Ran {
    // Holds the run to its verdict and to the invariants every run keeps:
    // it settles; a complete job published exactly the source bytes; a
    // partial job delivered at least one expected file whole; a failed job
    // is named and published none of them; a job that delivered anything
    // left nothing outside its output directory and published only the
    // expected files, the files it may publish, and the post's own files;
    // and a starved recovery set that leaves nothing delivered fails the
    // job as a repair.
    pub(in super::super) fn assert(
        &self,
        post: &Post,
        verdict: Verdict,
        starved_recovery: bool,
        context: &str,
    ) {
        let trace = &self.trace;
        let whole: Vec<&String> = post
            .expected
            .iter()
            .filter(|(name, bytes)| self.files[name].as_deref() == Some(bytes.as_slice()))
            .map(|(name, _)| name)
            .collect();
        let delivered = || {
            let allowed: BTreeSet<String> = post
                .expected
                .iter()
                .map(|(name, _)| name.clone())
                .chain(post.allowed.iter().map(|allowed| allowed.name.clone()))
                .chain(post.files.iter().map(|posted| posted.name.clone()))
                .collect();
            let unexpected: Vec<_> = self.published.difference(&allowed).collect();
            assert!(
                unexpected.is_empty(),
                "{context}: published {unexpected:?} besides the source: {trace:?}"
            );
            assert!(
                self.leftovers.is_empty(),
                "{context}: left {:?} behind: {trace:?}",
                self.leftovers
            );
        };
        match &self.status {
            Some(JobStatus::Complete) => {
                for (name, bytes) in &post.expected {
                    assert!(
                        self.files[name].as_deref() == Some(bytes.as_slice()),
                        "{context}: {name} is not the source bytes ({:?} of {}): {trace:?}",
                        self.files[name].as_ref().map(Vec::len),
                        bytes.len()
                    );
                }
                delivered();
                assert_ne!(
                    verdict,
                    Verdict::Fails,
                    "{context}: completed with source bytes where it must fail: {trace:?}"
                );
            }
            Some(JobStatus::Partial) => {
                // An allowed file counts as usable only when it is one a user
                // can open and holds its bytes whole; a container or
                // furniture never does, whatever is on disk.
                let allowed_whole = post.allowed.iter().any(|allowed| {
                    allowed
                        .usable
                        .as_deref()
                        .is_some_and(|bytes| self.files[&allowed.name].as_deref() == Some(bytes))
                });
                assert!(
                    !whole.is_empty() || allowed_whole,
                    "{context}: partial with no expected or allowed file whole: {trace:?}"
                );
                delivered();
                assert_ne!(
                    verdict,
                    Verdict::Completes,
                    "{context}: partial where it must complete, whole {whole:?}: {trace:?}"
                );
            }
            Some(JobStatus::Failed { error }) => {
                assert_ne!(
                    verdict,
                    Verdict::Completes,
                    "{context}: failed where it must complete: {error}: {trace:?}"
                );
                assert!(
                    !error.trim().is_empty(),
                    "{context}: an unnamed failure: {trace:?}"
                );
                let leaked: Vec<_> = post
                    .expected
                    .iter()
                    .filter(|(name, _)| self.published.contains(name) || self.files[name].is_some())
                    .map(|(name, _)| name)
                    .collect();
                assert!(
                    leaked.is_empty(),
                    "{context}: a failed job published {leaked:?}: {trace:?}"
                );
                if starved_recovery {
                    assert_eq!(
                        self.failure_kind,
                        Some(HistoryFailureKind::Repair),
                        "{context}: the starved set fails the job as a repair: {trace:?}"
                    );
                }
            }
            other => panic!("{context}: never settled: {other:?}: {trace:?}"),
        }
    }
}
