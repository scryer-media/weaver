// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The archive matrix under the graph engine: the matrix's fixtures and
// schedules, each admitted as a graph job, served article by article as the
// schedule orders, moved as the orchestrator moves it, and judged by one
// rule set read off the schedule and the post.

use weaver_graph::recovery::RepairOutcome;

use super::*;
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::engines::harness::{JOB, finished};

pub(in crate::pipeline::tests) mod grouping;
mod two_sets;

// What a schedule came to under the graph.
pub(in super::super) struct GraphOutcome {
    pub(in super::super) status: Option<JobStatus>,
    // What the history row says failed the job, when it failed.
    pub(in super::super) failure_kind: Option<HistoryFailureKind>,
    // Each wanted member, read from the job's output directory.
    pub(in super::super) files: BTreeMap<String, Option<Vec<u8>>>,
    // Every file under the job's output directory, by relative path.
    pub(in super::super) published: BTreeSet<String>,
    // Everything the job left outside its output directory.
    pub(in super::super) leftovers: BTreeSet<String>,
    // The names the post carries its files under, and the names its
    // recovery set describes the volumes by.
    pub(in super::super) own: BTreeSet<String>,
    // A joined posting whose output was compared with every source byte.
    pub(in super::super) joined: Option<String>,
    // Articles the job asked for while it held them on disk by its own
    // account.
    pub(in super::super) rerequested: BTreeSet<(u32, u32)>,
    // Articles the job held on disk by its own account when it settled:
    // every one handed over, less those a pipeline went down holding only
    // in memory and that were not handed over again.
    pub(in super::super) durable: BTreeSet<(u32, u32)>,
    pub(in super::super) trace: Vec<String>,
    // Every object kind the job's tree held live when the driver looked
    // between steps, over every incarnation. An object bound and retired
    // inside one step is never seen.
    pub(in super::super) bound: BTreeSet<String>,
    // Every recovery outcome a set of the job showed when the driver looked
    // between steps. The completion gate rules and ends a job inside one
    // step, so the ruling a job ends on may be missing; `failure_kind`
    // carries it.
    pub(in super::super) recovery: Vec<RepairOutcome>,
}

// What a post leaves a job under one schedule: the members it can deliver
// whole, whether every posted file the schedule damaged can be rebuilt,
// and the pieces the job holds as plain files when nothing gives it reason
// to read the post as an archive: no name says so, and no piece whose
// bytes say so reaches it.
pub(in super::super) struct Leaves {
    pub(in super::super) deliverable: BTreeSet<String>,
    pub(in super::super) mended: bool,
    pub(in super::super) files: Option<Files>,
}

// Pieces a job holds as plain files: how many the post carries to it, and
// how many of those come whole.
pub(in super::super) struct Files {
    pub(in super::super) posted: usize,
    pub(in super::super) whole: usize,
    pub(in super::super) joined: Option<String>,
}

// What a post is under one schedule: the members its archive holds, and
// what the post leaves the job under that schedule and the job's password.
pub(in super::super) struct Post<'a> {
    pub(in super::super) members: &'a BTreeMap<String, Vec<u8>>,
    pub(in super::super) leaves: Leaves,
}

// What a post leaves a job under a schedule, given whether the job's
// password opens the archive. The campaign answers it from what it knows
// of its own layout.
pub(in super::super) type Deliverable = Box<dyn Fn(bool, Interruption) -> Leaves>;

// A RAR post of one set. A loss its recovery set mends leaves every member
// deliverable; one it cannot mend leaves the members it never touched. A
// password that does not open the archive leaves nothing.
pub(in super::super) fn rar_post(
    volumes: &[(String, Vec<u8>)],
    spec: &JobSpec,
    members: &BTreeMap<String, Vec<u8>>,
    options: ScheduleOptions,
) -> Deliverable {
    let volumes = volumes.to_vec();
    let layout: Vec<usize> = spec.files[..volumes.len()]
        .iter()
        .map(|file| file.segments.len())
        .collect();
    let password = spec.password.clone();
    let members = members.clone();
    Box::new(move |opens, interruption| {
        let mended = !interruption.fails()
            && !(loses(interruption) && options.recovery == RecoveryFormat::Absent);
        let deliverable = if !opens {
            BTreeSet::new()
        } else if mended {
            members.keys().cloned().collect()
        } else {
            rar_recovers(
                &volumes,
                &layout,
                password.as_deref(),
                &members,
                interruption,
            )
        };
        Leaves {
            deliverable,
            mended,
            files: None,
        }
    })
}

// The members a RAR post still holds whole as a schedule leaves it on the
// server. A reader reaches a piece of a member through every header before
// it in its volume, so a piece needs those headers, its own, and its data;
// a member needs all of its pieces, and a solid member the member before it
// in the stream as well. A lost article loses its bytes; a withheld volume
// loses all of its own.
fn rar_recovers(
    volumes: &[(String, Vec<u8>)],
    layout: &[usize],
    password: Option<&str>,
    members: &BTreeMap<String, Vec<u8>>,
    interruption: Interruption,
) -> BTreeSet<String> {
    let mask = interruption.loss().map_or(0, |(mask, _)| mask);
    let withheld = interruption.withheld().map(|(volume, _)| volume as usize);
    let lost = |volume: usize, start: usize, end: usize| {
        let len = volumes[volume].1.len();
        Some(volume) == withheld
            || (0..layout[volume] as u32).any(|article| {
                let (from, to) = article_extent(len, article, layout[volume]);
                mask & (1 << article_slot(layout, volume as u32, article)) != 0
                    && from < end
                    && start < to
            })
    };
    // Each member in stream order: whether it is solid, and whether every
    // piece seen so far is whole.
    let mut stream: Vec<(String, bool, bool)> = Vec::new();
    for (volume, (_, bytes)) in volumes.iter().enumerate() {
        let facts =
            unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(bytes.clone()), password)
                .expect("an intact volume of the post parses");
        let mut headers_whole = true;
        let mut end = 0;
        for member in &facts.members {
            let data = member.data_offset as usize;
            headers_whole &= !lost(volume, end, data);
            end = data + member.data_size as usize;
            let piece = headers_whole && !lost(volume, data, end);
            if !member.is_directory {
                let name = member.name.replace('\\', "/");
                match stream.iter_mut().find(|(known, _, _)| *known == name) {
                    Some((_, _, whole)) => *whole &= piece,
                    None => stream.push((name, member.compression_solid, piece)),
                }
            }
        }
    }
    let mut recovered = BTreeSet::new();
    let mut before_whole = true;
    for (name, solid, pieces_whole) in stream {
        let whole = pieces_whole && (!solid || before_whole);
        before_whole = whole;
        if whole && members.contains_key(&name) {
            recovered.insert(name);
        }
    }
    recovered
}

// What a schedule loses: an article of a volume, or a volume the post never
// carried.
fn loses(interruption: Interruption) -> bool {
    interruption.loss().is_some_and(|(mask, _)| mask != 0) || interruption.withheld().is_some()
}

// The one rule set, as the ways a run disagrees with it. The members
// delivered whole include the reference reader's deliverable ones, and
// every published member is checked against its fixture bytes. The job is complete
// when every member is whole and the post mended all the schedule
// damaged, failed when nothing is whole, and partial otherwise. A post the
// job has no reason to read as an archive is files as posted, held to the
// same rule: complete when every piece came whole, failed when none did,
// partial otherwise. A job that
// delivered nothing leaves nothing in its output directory; one that
// delivered anything moved all of itself there. Only the members and the
// post's own files are published. Nothing on disk is asked for again. A
// starved recovery set that leaves nothing deliverable fails the job as a
// repair.
pub(in super::super) fn graph_disagreements(
    outcome: &GraphOutcome,
    post: &Post<'_>,
    interruption: Interruption,
    options: ScheduleOptions,
) -> Vec<String> {
    let mut said = Vec::new();
    let Leaves {
        deliverable,
        mended,
        files,
    } = &post.leaves;
    let whole: BTreeSet<String> = post
        .members
        .iter()
        .filter(|(name, bytes)| outcome.files[*name].as_deref() == Some(bytes.as_slice()))
        .map(|(name, _)| name.clone())
        .collect();
    let status = if let Some(files) = files {
        if files.joined.is_some() && files.joined != outcome.joined {
            said.push(format!(
                "joined posting {:?} was not delivered byte-exact",
                files.joined
            ));
        }
        if files.whole == 0 {
            matches!(outcome.status, Some(JobStatus::Failed { .. }))
        } else if files.whole == files.posted {
            outcome.status == Some(JobStatus::Complete)
        } else {
            outcome.status == Some(JobStatus::Partial)
        }
    } else if whole.is_empty() {
        matches!(outcome.status, Some(JobStatus::Failed { .. }))
    } else if whole.len() == post.members.len() && *mended {
        outcome.status == Some(JobStatus::Complete)
    } else {
        outcome.status == Some(JobStatus::Partial)
    };
    if !status {
        said.push(format!(
            "status {:?} with {} of {} members whole, mended={mended}, files={}",
            outcome.status,
            whole.len(),
            post.members.len(),
            files.as_ref().map_or("none".to_string(), |files| format!(
                "{} of {} whole",
                files.whole, files.posted
            ))
        ));
    }
    let present = outcome
        .files
        .iter()
        .filter(|(_, bytes)| bytes.is_some())
        .map(|(name, _)| name.clone())
        .collect();
    said.extend(
        crate::pipeline::tests::archive_output_contract::disagreements(
            deliverable,
            &whole,
            &present,
        ),
    );
    if !whole.is_empty() && !outcome.leftovers.is_empty() {
        said.push(format!(
            "left outside the output directory: {:?}",
            outcome.leftovers
        ));
    }
    let foreign: Vec<_> = outcome
        .published
        .iter()
        .filter(|path| !post.members.contains_key(*path) && !outcome.own.contains(*path))
        .collect();
    if !foreign.is_empty() {
        said.push(format!(
            "published beside the members and the post's own files: {foreign:?}"
        ));
    }
    if !outcome.rerequested.is_empty() {
        said.push(format!(
            "asked again though on disk: {:?} of {:?}",
            outcome.rerequested, outcome.durable
        ));
    }
    if interruption.fails()
        && options.recovery != RecoveryFormat::Absent
        && whole.is_empty()
        && outcome.failure_kind != Some(HistoryFailureKind::Repair)
    {
        said.push(format!(
            "the starved set fails the job as {:?}, not as a repair; recovery seen {:?}",
            outcome.failure_kind, outcome.recovery
        ));
    }
    if !said.is_empty() {
        for (name, expected) in post.members {
            if let Some(Some(actual)) = outcome.files.get(name)
                && let Some(difference) =
                    crate::pipeline::tests::archive_output_contract::output_difference(
                        name, expected, actual,
                    )
            {
                said.push(difference);
            }
        }
    }
    said
}

// A schedule driven through the graph. The schedule owns delivery: each of
// its arrivals is served in order, a lost one as a server answers it, and
// after each the pipeline moves as an orchestrator turn moves it. A restart
// drains the pipeline as shutdown does and a crash drops it as it stands;
// either way a new pipeline over the same root restores the job from its
// rows and its checkpoint, and the schedule continues. Once the schedule is
// spent, every article the job asks for is served until nothing is queued,
// scheduled or running.
//
// The driver is one future holding every step of a schedule; boxed, it lives
// on the heap however large it gets.
#[allow(clippy::too_many_arguments)]
pub(in super::super) async fn run_graph_schedule(
    options: ScheduleOptions,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    described: Option<&[String]>,
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> GraphOutcome {
    Box::pin(drive_graph_schedule(
        options,
        spec,
        volumes,
        described,
        order,
        wanted,
        interruption,
    ))
    .await
}

// The schedule's bookkeeping across the pipelines that serve it.
struct Run<'a> {
    volumes: &'a [(String, Vec<u8>)],
    layout: Vec<usize>,
    lost: u8,
    recovery: Vec<ScheduleRecovery>,
    // Articles the job holds on disk by its own account: every one handed
    // over, less those a pipeline went down holding only in memory.
    delivered: BTreeSet<(u32, u32)>,
    rerequested: BTreeSet<(u32, u32)>,
    bound: BTreeSet<String>,
    outcomes: Vec<RepairOutcome>,
    trace: Vec<String>,
}

impl Run<'_> {
    fn is_lost(&self, (file, article): (u32, u32)) -> bool {
        file < self.volumes.len() as u32
            && self.lost & (1 << article_slot(&self.layout, file, article)) != 0
    }

    // One article as the schedule's server answers it: a lost one is
    // missing everywhere, a recovery file's comes from the recovery set's
    // bytes, a volume's from the volume's.
    async fn serve(&mut self, pipeline: &mut Pipeline, article: (u32, u32)) {
        let (file, ordinal) = article;
        if !pipeline.jobs.contains_key(&JOB) {
            self.trace
                .push(format!("{file}:{ordinal} after the job left"));
            return;
        }
        retire_schedule_article(pipeline, JOB, file, ordinal);
        if self.is_lost(article) {
            pipeline.book_failed_segment(SegmentId {
                file_id: NzbFileId {
                    job_id: JOB,
                    file_index: file,
                },
                segment_number: ordinal,
            });
            self.trace.push(format!("lost {file}:{ordinal}"));
            return;
        }
        if let Some(carrier) = self.recovery.iter().find(|carrier| carrier.index == file) {
            submit_schedule_recovery(pipeline, JOB, carrier, ordinal).await;
        } else {
            let articles = self.layout[file as usize];
            submit_volume_article_of(pipeline, JOB, self.volumes, file, ordinal, articles).await;
        }
        self.delivered.insert(article);
        self.trace.push(format!("arrive {file}:{ordinal}"));
    }

    // Articles the job has queued while it holds them on disk.
    fn note_asks(&mut self, pipeline: &mut Pipeline) {
        if !pipeline.jobs.contains_key(&JOB) {
            return;
        }
        let asked: Vec<_> = peek_queued_segments(pipeline, JOB)
            .into_iter()
            .filter(|article| self.delivered.contains(article))
            .collect();
        self.rerequested.extend(asked);
    }

    // The graph job as it stands: what its tree bound and what its sets
    // came to.
    fn look(&mut self, pipeline: &Pipeline) {
        let Some(job) = pipeline.graph_jobs.get(&JOB) else {
            return;
        };
        let tree = job.tree();
        self.bound.extend(
            tree.live()
                .into_iter()
                .filter_map(|node| tree.kind(node))
                .map(|kind| kind.to_string()),
        );
        for set in job.recovery_summary() {
            if let Some(outcome) = set.outcome
                && !self.outcomes.contains(&outcome)
            {
                self.outcomes.push(outcome);
            }
        }
    }
}

// What an orchestrator turn does after an article: the runs' mail is heard,
// every completion check a step scheduled runs, and the runtime queues
// settle.
pub(super) async fn turn(pipeline: &mut Pipeline) {
    pipeline.hear_graph_runs();
    while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued).await;
    }
    pump_pipeline_runtime_queues(pipeline).await;
}

// What a gate round can change: when none of it moved, nothing will.
pub(super) fn fingerprint(pipeline: &Pipeline) -> String {
    let queued = pipeline.jobs.get(&JOB).map(|state| {
        (
            state.articles.active().unwrap().download_queue.len(),
            state.articles.active().unwrap().recovery_queue.len(),
            state.status.clone(),
        )
    });
    format!(
        "{queued:?} {} {}",
        pipeline.inflight_moves.contains(&JOB),
        pipeline.pending_completion_checks.len()
    )
}

// A new pipeline over `root` with the job restored from the rows the last
// one left, exactly as startup recovery restores it, and whether it was: a
// job the last one had finished is not.
pub(super) async fn restore(
    root: &tempfile::TempDir,
    spec: &JobSpec,
    trace: &mut Vec<String>,
) -> (Pipeline, bool) {
    use crate::jobs::model::{DownloadState, PostState, RunState};
    let (mut pipeline, _, _) = new_direct_pipeline(root).await;
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&JOB)
        .filter(|recovered| {
            !matches!(
                recovered.status.as_str(),
                "complete" | "failed" | "cancelled"
            )
        });
    trace.push(format!(
        "recovered={:?}",
        recovered.as_ref().map(|recovered| &recovered.status)
    ));
    let Some(recovered) = recovered else {
        return (pipeline, false);
    };
    pipeline
        .restore_job(RestoreJobRequest {
            job_id: JOB,
            job_hash: recovered.nzb_hash,
            spec: spec.clone(),
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
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
    assert!(
        !pipeline.blocked_restores.contains_key(&JOB) && pipeline.graph_jobs.contains_key(&JOB),
        "a graph job restores from its checkpoint: {trace:?}"
    );
    (pipeline, true)
}

#[allow(clippy::too_many_arguments)]
async fn drive_graph_schedule(
    options: ScheduleOptions,
    mut spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    described: Option<&[String]>,
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> GraphOutcome {
    enable_schedule_trace();
    // Every volume the set is written as: what a recovery set or a listing
    // describes. The run serves the ones the post carries.
    let written = volumes;
    let (posted, layout, order) = posted_schedule(&mut spec, written, order, interruption);
    let (recovery, index_first) =
        schedule_recovery(options, &mut spec, written, described, interruption);
    let mut own: BTreeSet<String> = spec
        .files
        .iter()
        .map(|file| file.filename.clone())
        .collect();
    own.extend(described.into_iter().flatten().cloned());
    let mut run = Run {
        volumes: &posted,
        layout,
        lost: interruption.loss().map_or(0, |(mask, _)| mask),
        recovery,
        delivered: BTreeSet::new(),
        rerequested: BTreeSet::new(),
        bound: BTreeSet::new(),
        outcomes: Vec::new(),
        trace: Vec::new(),
    };
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    let index = run.recovery.first().map(|carrier| (carrier.index, 0));
    if let Some(index) = index
        && index_first
    {
        run.serve(&mut pipeline, index).await;
        turn(&mut pipeline).await;
        run.look(&pipeline);
    }
    let mut retired = None;
    for (step, action, arrives) in interruption.boundaries(order.len()) {
        match action {
            BoundaryAction::None => {}
            action @ (BoundaryAction::Restart | BoundaryAction::Crash) => {
                if action == BoundaryAction::Restart {
                    pipeline.drain().await;
                }
                let status = job_status_for_assert(&pipeline, JOB);
                // What the job held only in memory goes down with it, and a
                // later pipeline may ask for it again.
                if let Some(job) = pipeline.graph_jobs.get(&JOB) {
                    for article in job.in_memory() {
                        run.delivered.remove(&article);
                    }
                }
                run.trace
                    .push(format!("{action:?} at {step}: status={status:?}"));
                retire_pipeline_database(pipeline).await;
                let restored;
                (pipeline, restored) = restore(&root, &spec, &mut run.trace).await;
                if !restored {
                    retired = Some(status);
                    break;
                }
                run.note_asks(&mut pipeline);
                run.look(&pipeline);
            }
        }
        if !arrives {
            continue;
        }
        let Some(&article) = order.get(step) else {
            break;
        };
        run.serve(&mut pipeline, article).await;
        turn(&mut pipeline).await;
        run.look(&pipeline);
        run.note_asks(&mut pipeline);
    }
    if let Some(index) = index
        && !index_first
        && retired.is_none()
    {
        run.serve(&mut pipeline, index).await;
        turn(&mut pipeline).await;
        run.look(&pipeline);
    }
    let mut stalled = None;
    while retired.is_none() {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            if run.delivered.contains(&article) {
                run.rerequested.insert(article);
            }
            run.serve(&mut pipeline, article).await;
            turn(&mut pipeline).await;
            run.look(&pipeline);
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        run.look(&pipeline);
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
            run.look(&pipeline);
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
            panic!("graph schedule stalled at {after}: trace={:?}", run.trace);
        }
    }
    let status = retired.unwrap_or_else(|| job_status_for_assert(&pipeline, JOB));
    run.trace.push(format!(
        "terminal {status:?}; bound={:?}; recovery={:?}; rerequested={:?}",
        run.bound, run.outcomes, run.rerequested
    ));
    let files = wanted
        .iter()
        .map(|name| ((*name).to_string(), std::fs::read(output.join(name)).ok()))
        .collect();
    let mut published = files_under(&output);
    published.remove(crate::jobs::working_dir::OUTPUT_DIR_MARKER);
    // A numbered posting may be delivered under its joined name. The
    // alias belongs to this post only if every output byte is the posted
    // sequence; a name alone never makes an unexpected file acceptable.
    let mut joined = None;
    if let Some((name, bytes)) = crate::pipeline::tests::archive_output_contract::joined_posting(
        posted
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice())),
    ) && std::fs::read(output.join(&name)).ok().as_deref() == Some(bytes.as_slice())
    {
        own.insert(name.clone());
        joined = Some(name);
    }
    let mut leftovers: BTreeSet<String> = files_under(&pipeline.intermediate_dir)
        .into_iter()
        .map(|path| format!("intermediate/{path}"))
        .collect();
    leftovers.extend(
        files_under(&complete.join(".weaver-staging"))
            .into_iter()
            .map(|path| format!("staging/{path}")),
    );
    pipeline.db.flush_write_queue().await.unwrap();
    let failure_kind = pipeline
        .db
        .nzbget_history_states(&[JOB.0])
        .unwrap()
        .get(&JOB.0)
        .and_then(|state| state.failure_kind);
    retire_pipeline_database(pipeline).await;
    GraphOutcome {
        status,
        failure_kind,
        files,
        published,
        leftovers,
        own,
        joined,
        rerequested: run.rerequested,
        durable: run.delivered,
        trace: run.trace,
        bound: run.bound,
        recovery: run.outcomes,
    }
}

// A campaign's cases under the graph, by their replay indices in the pool
// they were drawn from. The bounded sample is every arrival schedule a
// layout of `volumes` admits.
pub(in super::super) fn graph_schedules(
    selection: Selection,
    volumes: usize,
) -> Vec<(usize, Schedule)> {
    let cases = match selection {
        Selection::Smoke => schedules().into_iter().enumerate().collect(),
        _ => selected_schedules(selection),
    };
    cases
        .into_iter()
        .filter(|(_, (_, interruption))| interruption.fits(volumes))
        .collect()
}

// A layout as the graph campaigns run it.
pub(in super::super) struct GraphFixture {
    pub(in super::super) label: String,
    pub(in super::super) options: ScheduleOptions,
    pub(in super::super) spec: JobSpec,
    pub(in super::super) volumes: Vec<(String, Vec<u8>)>,
    pub(in super::super) described: Option<Vec<String>>,
    pub(in super::super) members: BTreeMap<String, Vec<u8>>,
    pub(in super::super) needs_password: bool,
    pub(in super::super) deliverable: Deliverable,
}

// The single-member store layout of `format`, cut into `slots` article
// slots.
pub(in super::super) fn graph_fixture(format: Format, slots: usize) -> GraphFixture {
    let SlotFixture {
        name,
        payload,
        volumes,
        described,
        encrypted,
        spec,
    } = slot_fixture(format, slots);
    let members = BTreeMap::from([(name.to_string(), payload)]);
    // A repair makes a volume whole, and every RAR set here with middle
    // volumes states each volume's number in its headers, so a repaired
    // middle volume is placed by its own number.
    GraphFixture {
        label: format!("{format:?}"),
        options: format.options(),
        deliverable: rar_post(&volumes, &spec, &members, format.options()),
        spec,
        volumes,
        described,
        members,
        needs_password: encrypted,
    }
}

// The packed layout of `format`.
pub(in super::super) fn graph_compressed_fixture(format: CompressedFormat) -> GraphFixture {
    let (volumes, spec) = compressed_job(format);
    let members = compressed_members(format);
    GraphFixture {
        label: format!("{format:?}"),
        options: ScheduleOptions::MATRIX,
        needs_password: spec.password.is_some(),
        deliverable: rar_post(&volumes, &spec, &members, ScheduleOptions::MATRIX),
        spec,
        volumes,
        described: None,
        members,
    }
}

// One cell: the schedule driven under `spec`'s password and judged. A cell
// the rule set disagrees with comes back as its report.
async fn graph_cell(
    fixture: &GraphFixture,
    spec: JobSpec,
    opens: bool,
    cell: String,
    (order, interruption): Schedule,
) -> Option<String> {
    eprintln!("{cell} order={order:?} interruption={interruption:?}");
    let wanted: Vec<&str> = fixture.members.keys().map(String::as_str).collect();
    let outcome = run_graph_schedule(
        fixture.options,
        spec,
        &fixture.volumes,
        fixture.described.as_deref(),
        &order,
        &wanted,
        interruption,
    )
    .await;
    let post = Post {
        members: &fixture.members,
        leaves: (fixture.deliverable)(opens, interruption),
    };
    let said = graph_disagreements(&outcome, &post, interruption, fixture.options);
    (!said.is_empty()).then(|| {
        format!(
            "{cell} order={order:?} interruption={interruption:?}: {}; the graph: status={:?} bound={:?} recovery={:?} trace={:?}",
            said.join("; "),
            outcome.status,
            outcome.bound,
            outcome.recovery,
            outcome.trace
        )
    })
}

// The fixture's cases under its own password, by replay index; the reports
// of the cells the rule set disagrees with.
pub(in super::super) async fn graph_fixture_cases(
    fixture: &GraphFixture,
    cases: impl IntoIterator<Item = (usize, Schedule)>,
) -> Vec<String> {
    let mut reports = Vec::new();
    for (case, schedule) in cases {
        let cell = format!("{} case={case}", fixture.label);
        reports.extend(graph_cell(fixture, fixture.spec.clone(), true, cell, schedule).await);
    }
    reports
}

// A password the archive does not open with, across the wrong password's
// schedules. An archive that needs none must not notice it.
async fn graph_wrong_password(fixture: &GraphFixture, selection: Selection) -> Vec<String> {
    let mut reports = Vec::new();
    for (case, schedule) in wrong_password_schedules(selection).into_iter().enumerate() {
        let mut wrong = fixture.spec.clone();
        wrong.password = Some("incorrect-key".to_string());
        let cell = format!("{} wrong password case={case}", fixture.label);
        reports.extend(graph_cell(fixture, wrong, !fixture.needs_password, cell, schedule).await);
    }
    reports
}

// Every cell of a campaign agrees with the rule set.
pub(in super::super) fn assert_cells(reports: &[String]) {
    assert!(
        reports.is_empty(),
        "{} cells disagree:\n{}",
        reports.len(),
        reports.join("\n")
    );
}

// A single-set layout's campaign: the wrong password, then the selection.
pub(in super::super) async fn graph_fixture_campaign(fixture: GraphFixture, selection: Selection) {
    let mut reports = graph_wrong_password(&fixture, selection).await;
    let cases = graph_schedules(selection, fixture.volumes.len());
    reports.extend(graph_fixture_cases(&fixture, cases).await);
    assert_cells(&reports);
}

pub(in super::super) async fn graph_campaign(format: Format, selection: Selection) {
    graph_fixture_campaign(graph_fixture(format, 4), selection).await;
}

pub(in super::super) async fn graph_compressed_campaign(
    format: CompressedFormat,
    selection: Selection,
) {
    graph_fixture_campaign(graph_compressed_fixture(format), selection).await;
}

// The smokes run in the archive-matrix profile, which alone selects this
// module.
macro_rules! graph_smokes {
    ($run:ident; $($name:ident $variant:expr),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                $run($variant, Selection::Smoke).await;
            }
        )+
    };
}

graph_smokes!(graph_campaign;
    rar4_arrival_schedules Format::Rar4,
    rar5_arrival_schedules Format::Rar5,
    rar4_encrypted_arrival_schedules Format::Rar4Encrypted,
    rar4_unsalted_arrival_schedules Format::Rar4Unsalted,
    rar5_encrypted_arrival_schedules Format::Rar5Encrypted,
    rar5_keyed_checksum_arrival_schedules Format::Rar5KeyedChecksum,
    rar5_header_encrypted_arrival_schedules Format::Rar5EncryptedHeaders,
    rar5_unchecked_header_arrival_schedules Format::Rar5UncheckedHeaders,
    quick_open_arrival_schedules Format::QuickOpen,
    blake2_arrival_schedules Format::Blake2,
    rar4_four_volume_arrival_schedules Format::Rar4FourVolumes,
    rar5_four_volume_arrival_schedules Format::Rar5FourVolumes,
    rar4_encrypted_four_volume_arrival_schedules Format::Rar4EncryptedFourVolumes,
    rar5_obfuscated_arrival_schedules Format::Rar5Obfuscated,
    rar5_hex_bare_arrival_schedules Format::Rar5HexBare,
    rar4_hex_bare_arrival_schedules Format::Rar4HexBare,
    rar5_hex_self_described_arrival_schedules Format::Rar5HexSelfDescribed,
    rar4_hex_self_described_arrival_schedules Format::Rar4HexSelfDescribed,
    rar5_hex_late_index_arrival_schedules Format::Rar5HexLateIndex,
    rar4_hex_late_index_arrival_schedules Format::Rar4HexLateIndex,
    rar5_hex_misnumbered_arrival_schedules Format::Rar5HexMisnumbered,
    rar4_hex_misnumbered_arrival_schedules Format::Rar4HexMisnumbered,
    rar5_hex_swapped_rar_arrival_schedules Format::Rar5HexSwappedRar,
    rar4_hex_swapped_rar_arrival_schedules Format::Rar4HexSwappedRar,
    rar5_hex_scattered_arrival_schedules Format::Rar5HexScattered,
    rar4_hex_scattered_arrival_schedules Format::Rar4HexScattered,
    rar5_hex_single_arrival_schedules Format::Rar5HexSingle,
    rar4_hex_single_arrival_schedules Format::Rar4HexSingle,
);

graph_smokes!(graph_compressed_campaign;
    compressed_rar4_mixed_arrival_schedules CompressedFormat::Rar4Mixed,
    compressed_rar4_lz_arrival_schedules CompressedFormat::Rar4Lz,
    compressed_rar4_solid_arrival_schedules CompressedFormat::Rar4Solid,
    compressed_rar4_solid_encrypted_arrival_schedules CompressedFormat::Rar4SolidEncrypted,
    compressed_rar4_solid_headers_arrival_schedules CompressedFormat::Rar4SolidHeaders,
    compressed_rar4_ppmd_arrival_schedules CompressedFormat::Rar4Ppmd,
    compressed_rar4_ppmd_encrypted_arrival_schedules CompressedFormat::Rar4PpmdEncrypted,
    compressed_rar4_ppmd_headers_arrival_schedules CompressedFormat::Rar4PpmdHeaders,
    compressed_rar4_encrypted_arrival_schedules CompressedFormat::Rar4Encrypted,
    compressed_rar4_headers_arrival_schedules CompressedFormat::Rar4Headers,
    compressed_rar5_mixed_arrival_schedules CompressedFormat::Rar5Mixed,
    compressed_rar5_lz_arrival_schedules CompressedFormat::Rar5Lz,
    compressed_rar5_encrypted_arrival_schedules CompressedFormat::Rar5Encrypted,
    compressed_rar5_headers_arrival_schedules CompressedFormat::Rar5Headers,
    compressed_rar5_solid_arrival_schedules CompressedFormat::Rar5Solid,
    compressed_rar5_solid_encrypted_arrival_schedules CompressedFormat::Rar5SolidEncrypted,
    compressed_rar5_solid_headers_arrival_schedules CompressedFormat::Rar5SolidHeaders,
    compressed_rar4_lz_two_volume_arrival_schedules CompressedFormat::Rar4LzTwoVolumes,
    compressed_rar5_lz_two_volume_arrival_schedules CompressedFormat::Rar5LzTwoVolumes,
    compressed_rar4_lz_four_volume_arrival_schedules CompressedFormat::Rar4LzFourVolumes,
    compressed_rar5_lz_four_volume_arrival_schedules CompressedFormat::Rar5LzFourVolumes,
);

// The combined matrix under the graph: every case in shards, and the wrong
// password's schedules as a test of their own.
combined_campaign!(combined_rar4, Format::Rar4, graph_campaign);
combined_campaign!(combined_rar5, Format::Rar5, graph_campaign);
combined_campaign!(
    combined_rar4_encrypted,
    Format::Rar4Encrypted,
    graph_campaign
);
combined_campaign!(combined_rar4_unsalted, Format::Rar4Unsalted, graph_campaign);
combined_campaign!(
    combined_rar5_encrypted,
    Format::Rar5Encrypted,
    graph_campaign
);
combined_campaign!(
    combined_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    graph_campaign
);
combined_campaign!(
    combined_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    graph_campaign
);
combined_campaign!(
    combined_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    graph_campaign
);
combined_campaign!(combined_quick_open, Format::QuickOpen, graph_campaign);
combined_campaign!(combined_blake2, Format::Blake2, graph_campaign);
combined_campaign!(
    combined_rar4_four_volume,
    Format::Rar4FourVolumes,
    graph_campaign
);
combined_campaign!(
    combined_rar5_four_volume,
    Format::Rar5FourVolumes,
    graph_campaign
);
combined_campaign!(
    combined_rar4_encrypted_four_volume,
    Format::Rar4EncryptedFourVolumes,
    graph_campaign
);
combined_campaign!(
    combined_rar5_obfuscated,
    Format::Rar5Obfuscated,
    graph_campaign
);
combined_campaign!(combined_rar5_hex_bare, Format::Rar5HexBare, graph_campaign);
combined_campaign!(combined_rar4_hex_bare, Format::Rar4HexBare, graph_campaign);
combined_campaign!(
    combined_rar5_hex_self_described,
    Format::Rar5HexSelfDescribed,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_self_described,
    Format::Rar4HexSelfDescribed,
    graph_campaign
);
combined_campaign!(
    combined_rar5_hex_late_index,
    Format::Rar5HexLateIndex,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_late_index,
    Format::Rar4HexLateIndex,
    graph_campaign
);
combined_campaign!(
    combined_rar5_hex_misnumbered,
    Format::Rar5HexMisnumbered,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_misnumbered,
    Format::Rar4HexMisnumbered,
    graph_campaign
);
combined_campaign!(
    combined_rar5_hex_swapped_rar,
    Format::Rar5HexSwappedRar,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_swapped_rar,
    Format::Rar4HexSwappedRar,
    graph_campaign
);
combined_campaign!(
    combined_rar5_hex_scattered,
    Format::Rar5HexScattered,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_scattered,
    Format::Rar4HexScattered,
    graph_campaign
);
combined_campaign!(
    combined_rar5_hex_single,
    Format::Rar5HexSingle,
    graph_campaign
);
combined_campaign!(
    combined_rar4_hex_single,
    Format::Rar4HexSingle,
    graph_campaign
);
combined_campaign!(
    combined_compressed_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_lz,
    CompressedFormat::Rar4Lz,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_headers,
    CompressedFormat::Rar4Headers,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_lz,
    CompressedFormat::Rar5Lz,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_headers,
    CompressedFormat::Rar5Headers,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_solid,
    CompressedFormat::Rar5Solid,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_solid,
    CompressedFormat::Rar4Solid,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_lz_two_volume,
    CompressedFormat::Rar4LzTwoVolumes,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_lz_two_volume,
    CompressedFormat::Rar5LzTwoVolumes,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar4_lz_four_volume,
    CompressedFormat::Rar4LzFourVolumes,
    graph_compressed_campaign
);
combined_campaign!(
    combined_compressed_rar5_lz_four_volume,
    CompressedFormat::Rar5LzFourVolumes,
    graph_compressed_campaign
);
