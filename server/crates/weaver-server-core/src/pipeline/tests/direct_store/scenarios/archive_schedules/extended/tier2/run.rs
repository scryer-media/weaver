//! The tier-two runner: one post, one profile, one schedule.
//!
//! A schedule names four slots. Tier two posts more articles than that, so
//! the data articles, in file order, are cut into four contiguous groups and
//! slot `file * 2 + article` names group `file * 2 + article`: an arrival
//! delivers the whole group in article order, a duplicate delivers it again,
//! and boundaries fall between groups. A lost slot loses the middle article
//! of its group rather than the whole group, so a loss stays inside what a
//! recovery set sized for the cell can mend; a starved schedule withholds the
//! recovery volumes instead.
//!
//! Every article reaches the pipeline as wire bytes through the production
//! fused decoder, from a provider of one server: an article that fails to
//! decode has nowhere else to come from and is given up for repair at once.
use super::post::{Post, Role};
use super::*;

/// What one run left behind.
pub(in super::super) struct Ran {
    pub status: Option<JobStatus>,
    /// Each expected path, as the output directory holds it.
    pub files: BTreeMap<String, Option<Vec<u8>>>,
    pub published: BTreeSet<String>,
    pub leftovers: BTreeSet<String>,
    pub finalized: usize,
    pub chase_armed: u64,
    pub chase_consumed: u64,
    pub trace: Vec<String>,
}

/// A provider of one server, so a decode failure books the article as
/// exhausted instead of scheduling a timed retry. The pipeline is given no
/// connections, so nothing ever dials it.
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
    pin_disk_reserve(pipeline);
}

/// The disk reserve is otherwise a share of the host's filesystem, so a
/// nearly full disk would refuse a fixture's few kilobytes and the outcome
/// would depend on the machine. Fixtures are tiny: reserve nothing.
fn pin_disk_reserve(pipeline: &mut Pipeline) {
    use crate::pipeline::direct_store::{DirectStoreSettings, wiring::DirectStoreRuntime};
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        holds_disk_reserve_bytes: 0,
        ..pipeline.direct_store.settings()
    });
}

/// The data articles cut into the four groups a schedule's slots name.
pub(in super::super) fn groups(post: &Post) -> Vec<Vec<(u32, u32)>> {
    let articles: Vec<(u32, u32)> = post
        .index_of(Role::Data)
        .into_iter()
        .flat_map(|file| (0..post.files[file].articles()).map(move |article| (file as u32, article)))
        .collect();
    let per = articles.len().div_ceil(4).max(1);
    let mut groups: Vec<Vec<(u32, u32)>> = articles.chunks(per).map(<[_]>::to_vec).collect();
    groups.resize(4, Vec::new());
    groups
}

/// The articles a schedule's loss mask takes: the middle article of each
/// lost group, by file.
pub(in super::super) fn lost_articles(post: &Post, interruption: Interruption) -> BTreeMap<usize, BTreeSet<u32>> {
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

/// Whether the downloader would start a fetch for the job: the statuses its
/// dispatch admits.
fn dispatchable(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline.jobs.get(&job).is_some_and(|state| {
        matches!(
            state.status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    })
}

fn terminal(pipeline: &Pipeline, job: JobId) -> bool {
    matches!(
        job_status_for_assert(pipeline, job),
        Some(JobStatus::Complete | JobStatus::Failed { .. })
    )
}

/// Answers one request for an article with what the post has for it.
async fn deliver(pipeline: &mut Pipeline, job: JobId, post: &Post, file: u32, article: u32) {
    use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};
    retire_schedule_article(pipeline, job, file, article);
    for body in post.files[file as usize].wire_copies(article) {
        if pipeline.jobs.get(&job).is_none() || terminal(pipeline, job) {
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
        decoder.set_checkpoint_plan(pipeline.par2_checkpoint_plan(job));
        let mut input = bytes::BytesMut::from(raw.as_slice());
        let source = || SegmentSource {
            source_server_idx: Some(0),
            exclude_servers: Vec::new(),
        };
        match decoder.decode_available(&mut input) {
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
                    Ok(DownloadPayload::Decoded(result)) => {
                        pipeline.handle_decode_success(result, source()).await;
                    }
                    Err(DownloadError::Decode { error, .. }) => {
                        pipeline.handle_decode_failure(segment, &error, &[], Some(0));
                    }
                    _ => panic!("a fused decode yields a decoded payload or a decode error"),
                }
            }
            Ok(None) => pipeline.handle_decode_failure(
                segment,
                "article ended before its terminator",
                &[],
                Some(0),
            ),
            Err(error) => pipeline.handle_decode_failure(segment, &error.to_string(), &[], Some(0)),
        }
        settle_direct_verification_read(pipeline, job).await;
        settle_direct_demotion_work(pipeline).await;
    }
}

/// Every article the job has queued, from both queues, left queued.
fn queued(pipeline: &mut Pipeline, job: JobId) -> Vec<(u32, u32)> {
    let Some(state) = pipeline.jobs.get_mut(&job) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for queue in [&mut state.download_queue, &mut state.recovery_queue] {
        let work = queue.drain_all();
        for item in work {
            out.push((
                item.segment_id.file_id.file_index,
                item.segment_id.segment_number,
            ));
            queue.push(item);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Books a queued article the one server does not have, as the downloader
/// does once every server has answered 430.
fn unavailable(pipeline: &mut Pipeline, job: JobId, file: u32, article: u32) {
    retire_schedule_article(pipeline, job, file, article);
    pipeline.book_failed_segment(SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: article,
    });
    pipeline.maybe_finish_download_pass(job);
}

/// Runs `post` under one schedule.
pub(in super::super) async fn run(
    post: &Post,
    profile: ExtractionProfile,
    order: &[(u32, u32)],
    interruption: Interruption,
) -> Ran {
    enable_schedule_trace();
    let spec = post.spec();
    let groups = groups(post);
    let lost = lost_articles(post, interruption);
    let starved = interruption.fails();
    let is_lost = |file: u32, article: u32| {
        lost.get(&(file as usize)).is_some_and(|set| set.contains(&article))
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
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    one_server(&mut pipeline);
    profile.configure(&mut pipeline);
    let job = JobId(42300);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let index_first = interruption.loss().is_some_and(|(_, first)| first);
    let indices = post.index_of(Role::Index);
    let mut chase_armed = 0;
    let mut chase_consumed = 0;
    let mut finalized = 0;
    let mut retired = None;
    let mut trace = vec![];
    insert_active_job(&mut pipeline, job, spec.clone()).await;
    let deliver_index = async |pipeline: &mut Pipeline| {
        for &file in &indices {
            for article in 0..post.files[file].articles() {
                if available(file as u32, article) {
                    deliver(pipeline, job, post, file as u32, article).await;
                }
            }
        }
    };
    if index_first {
        deliver_index(&mut pipeline).await;
    }
    for (step, action, arrives) in interruption.boundaries(order.len()) {
        match action {
            BoundaryAction::Demote => {
                if profile == ExtractionProfile::DirectStore {
                    let sets = pipeline.direct_store.sets_for(job).len();
                    let (set, reason) = scheduled_demotion(order, step, sets);
                    pipeline.demote_direct_set(job, set, reason).await;
                }
                pipeline.direct_unpack_abort_job(
                    job,
                    "schedule withdraws speculative extraction",
                    AbortLatch::Permanent,
                    ChaseDemotion::MemoryYielded,
                );
                settle_direct_post_repair_work(&mut pipeline).await;
                trace.push(format!("demote at {step}"));
            }
            action @ (BoundaryAction::Restart | BoundaryAction::Crash) => {
                if action == BoundaryAction::Restart {
                    pipeline
                        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
                        .await;
                }
                let counters = pipeline.direct_unpack.counters();
                chase_armed += counters.armed;
                chase_consumed += counters.consumed;
                finalized += pipeline.direct_store.finalized_sets;
                let status = job_status_for_assert(&pipeline, job);
                pipeline.direct_unpack_shutdown("schedule restart").await;
                retire_pipeline_database(pipeline).await;
                settle_direct_output_removals(root.path()).await;
                (pipeline, _, _) = new_direct_pipeline(&root).await;
                one_server(&mut pipeline);
                profile.configure(&mut pipeline);
                let recovered = pipeline.db.load_active_jobs().unwrap().remove(&job);
                let recovered = recovered.filter(|recovered| {
                    !matches!(
                        recovered.status.as_str(),
                        "complete" | "failed" | "cancelled"
                    )
                });
                trace.push(format!("{action:?} at {step}: status={status:?}"));
                let Some(recovered) = recovered else {
                    retired = Some(status);
                    break;
                };
                use crate::jobs::model::{DownloadState, PostState, RunState};
                pipeline
                    .restore_job(RestoreJobRequest {
                        job_id: job,
                        job_hash: recovered.nzb_hash,
                        spec: spec.clone(),
                        complete_files: recovered.complete_files,
                        file_progress: recovered.file_progress,
                        detected_archives: recovered.detected_archives,
                        file_identities: recovered.file_identities,
                        extracted_members: recovered.extracted_members,
                        status: crate::jobs::model::job_status_from_persisted_str(
                            &recovered.status,
                            recovered.error.as_deref(),
                        ),
                        download_state: recovered
                            .download_state
                            .as_deref()
                            .and_then(DownloadState::parse),
                        post_state: recovered.post_state.as_deref().and_then(PostState::parse),
                        run_state: recovered.run_state.as_deref().and_then(RunState::parse),
                        queued_repair_at_epoch_ms: recovered.queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms: recovered.queued_extract_at_epoch_ms,
                        paused_resume_status: None,
                        paused_resume_download_state: None,
                        paused_resume_post_state: None,
                        working_dir: recovered.output_dir,
                    })
                    .await
                    .unwrap();
            }
            BoundaryAction::None => {}
        }
        if !arrives {
            continue;
        }
        let Some(&(file, article)) = order.get(step) else {
            break;
        };
        let slot = (file * 2 + article) as usize;
        for &(file, article) in &groups[slot] {
            if !dispatchable(&pipeline, job) {
                break;
            }
            if available(file, article) {
                deliver(&mut pipeline, job, post, file, article).await;
            }
        }
        trace.push(format!("arrive slot {slot}: {:?}", pipeline.direct_store.sets_for(job)));
    }
    if !index_first && retired.is_none() {
        deliver_index(&mut pipeline).await;
    }
    // Every wait below is for a registered operation; the bound is a count of
    // rounds, never a time.
    let mut idle = 0;
    for _ in 0..256 {
        if retired.is_some() || pipeline.jobs.get(&job).is_none() {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if terminal(&pipeline, job) {
            break;
        }
        let requested = queued(&mut pipeline, job);
        if !requested.is_empty() && dispatchable(&pipeline, job) {
            let (answer, refuse): (Vec<_>, Vec<_>) = requested
                .into_iter()
                .partition(|&(file, article)| available(file, article));
            if !refuse.is_empty() {
                trace.push(format!("unavailable {refuse:?}"));
            }
            for (file, article) in refuse {
                unavailable(&mut pipeline, job, file, article);
            }
            if !answer.is_empty() {
                trace.push(format!("requested {answer:?}"));
            }
            // One connection: each fetch is of an article the job still
            // wants when the fetch starts.
            for (file, article) in answer {
                if !dispatchable(&pipeline, job) {
                    break;
                }
                if !queued(&mut pipeline, job).contains(&(file, article)) {
                    continue;
                }
                deliver(&mut pipeline, job, post, file, article).await;
            }
            continue;
        }
        pipeline.flush_quiescent_write_backlog().await;
        pipeline.check_job_completion(job).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if terminal(&pipeline, job) || pipeline.jobs.get(&job).is_none() {
            break;
        }
        if dispatchable(&pipeline, job) && !queued(&mut pipeline, job).is_empty() {
            continue;
        }
        if pipeline
            .inflight_extractions
            .get(&job)
            .is_some_and(|sets| !sets.is_empty())
            || pipeline.has_active_rar_workers(job)
        {
            let done = pipeline
                .extract_done_rx
                .recv()
                .await
                .expect("registered extraction receipt");
            pipeline.handle_extraction_done(done).await;
        } else if pipeline.job_has_pending_rar_refresh_for_current_sets(job) {
            drain_rar_refreshes(&mut pipeline).await;
        } else if pipeline
            .par3_runtime
            .as_ref()
            .is_some_and(|runtime| runtime.has_worker_in_flight(job))
        {
            settle_par3_work(&mut pipeline, job).await;
        } else {
            idle += 1;
            if idle > 8 {
                break;
            }
        }
    }
    let status = retired.unwrap_or_else(|| job_status_for_assert(&pipeline, job));
    let still_queued = if pipeline.jobs.contains_key(&job) {
        queued(&mut pipeline, job)
    } else {
        Vec::new()
    };
    trace.push(format!(
        "terminal: {}; analyses={} repairs={}; par2={:?}; queued={:?}",
        if pipeline.jobs.contains_key(&job) {
            debug_job_state(&pipeline, job)
        } else {
            "retired".to_string()
        },
        pipeline.par2_repairer_analyze_calls,
        pipeline.par2_repairer_execute_calls,
        pipeline
            .par2_set(job)
            .map(|set| (set.files.len(), set.recovery_slices.len())),
        still_queued,
    ));
    let files = post
        .expected
        .iter()
        .map(|(name, _)| (name.clone(), std::fs::read(output.join(name)).ok()))
        .collect();
    settle_direct_output_removals(root.path()).await;
    let counters = pipeline.direct_unpack.counters();
    chase_armed += counters.armed;
    chase_consumed += counters.consumed;
    finalized += pipeline.direct_store.finalized_sets;
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
                std::fs::metadata(output.join(name)).ok().map(|meta| meta.len()),
                post.files
                    .iter()
                    .find(|posted| posted.name == *name)
                    .map(|posted| posted.bytes.len()),
            )
        })
        .collect();
    trace.push(format!(
        "profile={profile:?}; chase_armed={chase_armed}; chase_consumed={chase_consumed}; published (name, size, posted size)={sizes:?}"
    ));
    Ran {
        status,
        files,
        published,
        leftovers,
        finalized,
        chase_armed,
        chase_consumed,
        trace,
    }
}

/// What a run must end in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Verdict {
    /// Every expected file published, byte for byte.
    Completes,
    /// A named failure that publishes none of the expected files.
    Fails,
    /// Either of the above; the oracle cannot tell which the bytes allow.
    Either,
}

impl Ran {
    /// Holds the run to its verdict and to the invariants every run keeps:
    /// it settles, a complete job published exactly the source bytes and
    /// nothing unexpected, a failed job is named and published none of them.
    pub(in super::super) fn assert(&self, post: &Post, profile: ExtractionProfile, verdict: Verdict, context: &str) {
        let trace = &self.trace;
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
                let allowed: BTreeSet<String> = post
                    .expected
                    .iter()
                    .map(|(name, _)| name.clone())
                    .chain(post.allowed.iter().cloned())
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
                assert_ne!(verdict, Verdict::Fails, "{context}: completed with source bytes where it must fail: {trace:?}");
            }
            Some(JobStatus::Failed { error }) => {
                assert_ne!(
                    verdict,
                    Verdict::Completes,
                    "{context}: failed where it must complete: {error}: {trace:?}"
                );
                assert!(!error.trim().is_empty(), "{context}: an unnamed failure: {trace:?}");
                assert_eq!(self.finalized, 0, "{context}: a failed job finalized a set: {trace:?}");
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
            }
            other => panic!("{context}: never settled: {other:?}: {trace:?}"),
        }
        if profile != ExtractionProfile::DirectStore {
            assert_eq!(self.finalized, 0, "{context}: {trace:?}");
        }
        if profile == ExtractionProfile::Conventional {
            assert_eq!(self.chase_armed + self.chase_consumed, 0, "{context}: {trace:?}");
        }
    }
}
