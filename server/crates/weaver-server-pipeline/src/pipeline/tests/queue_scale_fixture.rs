// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub const QUEUED: u64 = 200;

pub async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 4]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("queued-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// A pipeline holding `QUEUED` jobs with `slots` of them admitted.
pub async fn long_queue(temp: &tempfile::TempDir, slots: u32) -> (Pipeline, SchedulerHandle) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), slots as usize);
    (pipeline, handle)
}

pub fn row_walks() -> u64 {
    crate::pipeline::queue::ROW_WALKS.load(Ordering::Relaxed)
}

pub fn row_run_state(pipeline: &Pipeline, id: JobId) -> crate::RunState {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(id)
            .map(|row| row.info(&inner.globals).run_state)
            .unwrap()
    })
}

pub const NZB_SLOT_WAIT_REASON: &str = crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON;

pub struct WaitReason {
    pub id: JobId,
    pub stored: Option<String>,
    pub computed: Option<String>,
    pub admitted: bool,
}

pub fn wait_reasons(pipeline: &Pipeline) -> Vec<WaitReason> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .map(|row| WaitReason {
                id: row.id,
                stored: row.report.job.download_wait_reason.clone(),
                computed: pipeline
                    .stored_wait_reason(row, pipeline.login_refusal_view())
                    .map(std::borrow::Cow::into_owned),
                admitted: pipeline.nzb_admitted.contains(&row.id),
            })
            .collect()
    })
}

pub fn report_changed_dispatch_holds(pipeline: &mut Pipeline) {
    pipeline.report_changed_dispatch_holds();
}

pub struct QueueProbe(Arc<crate::pipeline::queue::Queue>);

impl QueueProbe {
    pub fn observe(pipeline: &Pipeline) -> Self {
        Self(pipeline.shared_state.queue.clone())
    }

    pub fn revision(&self) -> u64 {
        self.0.revision()
    }

    pub fn intent_epoch(&self) -> u64 {
        self.0.read(|inner| inner.intent_epoch())
    }

    pub fn ids(&self) -> Vec<JobId> {
        self.0
            .read(|inner| inner.jobs.iter().map(|row| row.id).collect())
    }

    pub fn positions(&self) -> Vec<(JobId, Option<usize>)> {
        self.0.read(|inner| {
            inner
                .jobs
                .iter()
                .map(|row| (row.id, inner.job_position(row.id)))
                .collect()
        })
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.subscribe()
    }

    pub fn subscribe_drive(&self) -> tokio::sync::watch::Receiver<bool> {
        self.0.subscribe_drive()
    }

    pub fn driving(&self) -> bool {
        self.0.driving()
    }

    pub fn admitted(&self, id: JobId) -> bool {
        self.0
            .read(|inner| inner.job(id).is_some_and(|row| row.report.nzb_admitted))
    }

    pub fn has_phased_job(&self) -> bool {
        self.0.read(|inner| inner.phased_jobs().next().is_some())
    }
}

pub fn installed_order(pipeline: &Pipeline) -> &[JobId] {
    &pipeline.job_order
}

pub fn admitted_ids(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.nzb_admitted.iter().copied().collect()
}

pub fn admitting_ids(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.nzb_admitting.iter().copied().collect()
}

pub fn releasing(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.nzb_releasing.contains(&id)
}

pub fn installing_ids(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.installing.iter().copied().collect()
}

pub fn positions_lost(pipeline: &Pipeline) -> bool {
    pipeline.queue_positions_lost.load(Ordering::Acquire)
}

pub fn mark_positions_lost(pipeline: &Pipeline) {
    pipeline.queue_positions_lost.store(true, Ordering::Release);
}

pub fn pipeline_live(pipeline: &Pipeline) -> bool {
    pipeline.pipeline_is_live()
}

pub fn has_work_to_drive(pipeline: &Pipeline) -> bool {
    pipeline.has_work_to_drive()
}

pub fn add_expired_hold(pipeline: &mut Pipeline, id: JobId) {
    pipeline
        .propagation_ready_at
        .insert(id, (std::time::Instant::now(), 0));
}

pub fn has_propagation_hold(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.propagation_ready_at.contains_key(&id)
}

pub fn phase_downloading(pipeline: &mut Pipeline, id: JobId, bytes: u64) {
    pipeline.active_download_passes.insert(id);
    pipeline.phase_begin(id, crate::JobPhase::Downloading, Some(bytes));
}

pub const STALL_CHECK_INTERVAL: Duration = STALLED_DOWNLOAD_CHECK_INTERVAL;

pub fn periodic_ticks() -> u64 {
    crate::pipeline::orchestrator::PERIODIC_TICKS.load(Ordering::Relaxed)
}

pub fn counter_samples() -> u64 {
    crate::pipeline::queue::COUNTER_SAMPLES.load(Ordering::Relaxed)
}

pub async fn sql(db: &crate::Database, statement: &'static str) {
    let datastore = db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_sql", |tx| {
        Box::pin(async move {
            tx.execute(statement, &[]).await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

// The database's one writer held busy by a write that waits for the test,
// as a slow disk holds it. Sending on the returned channel lets it go.
pub async fn hold_writer(db: &crate::Database) -> std::sync::mpsc::Sender<()> {
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let (held, holding) = tokio::sync::oneshot::channel();
    db.try_queue_write("test_hold_writer", move |_| {
        let _ = held.send(());
        let _ = gate.recv();
        Ok(())
    })
    .unwrap();
    holding.await.unwrap();
    release
}

// Whether one turn of the queue's intents runs to its end on its first
// poll: a turn that waited on the database would not.
pub fn turn_completes(pipeline: &mut Pipeline) -> bool {
    use std::future::Future;
    let mut turn = std::pin::pin!(pipeline.run_queue_intents());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    turn.as_mut().poll(&mut cx).is_ready()
}

pub const RESTORE_BATCH: usize = crate::pipeline::orchestrator::RESTORE_BATCH;
pub const NEXT_UP_DEPTH: usize = crate::pipeline::next_up::NEXT_UP_DEPTH;

pub fn nzb_parses() -> u64 {
    crate::ingest::NZB_PARSES.load(Ordering::Relaxed)
}

pub fn runtime_writes() -> u64 {
    crate::pipeline::completion::finalize::RUNTIME_WRITES.load(Ordering::Relaxed)
}

pub fn decodes_heard_while_restoring() -> u64 {
    crate::pipeline::orchestrator::DECODES_HEARD_RESTORING.load(Ordering::Relaxed)
}

pub fn outline_only_reads() -> u64 {
    crate::jobs::service::OUTLINE_ONLY_READS.load(Ordering::Relaxed)
}

pub fn restore_waiting(pipeline: &Pipeline) -> bool {
    pipeline.restores_waiting
}

pub fn restore_pass_active(pipeline: &Pipeline) -> bool {
    pipeline.restore_pass.is_some()
}

pub fn installed_summaries(
    pipeline: &Pipeline,
) -> HashMap<JobId, Vec<crate::jobs::model::FileSummary>> {
    use crate::jobs::model::{FileSummary, JobArticles};
    pipeline
        .jobs
        .iter()
        .map(|(id, state)| {
            let summaries = match &state.articles {
                JobArticles::Queued(summaries) => summaries.0.clone(),
                _ => state.spec.files.iter().map(FileSummary::of).collect(),
            };
            (*id, summaries)
        })
        .collect()
}

pub fn queued_summaries(
    pipeline: &Pipeline,
    id: JobId,
) -> Option<&[crate::jobs::model::FileSummary]> {
    match &pipeline.jobs[&id].articles {
        crate::jobs::model::JobArticles::Queued(summaries) => Some(&summaries.0),
        _ => None,
    }
}

pub fn file_identity(
    pipeline: &Pipeline,
    id: JobId,
    file: u32,
) -> Option<&crate::jobs::ActiveFileIdentity> {
    pipeline.jobs[&id].file_identities.get(&file)
}

pub fn sql_blocking(db: &Database, statement: &'static str) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(datastore.write_exec(), statement, &[]).await?;
        Ok(())
    })
    .unwrap();
}

pub fn restore_blocked(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.blocked_restores.contains_key(&id)
}

pub async fn resume_restored_job(pipeline: &mut Pipeline, id: JobId) -> Result<(), SchedulerError> {
    pipeline.resume_restored_job(id).await
}

pub async fn enqueue_failed_decode(pipeline: &Pipeline, id: JobId) {
    pipeline
        .decode_done_tx
        .send(DecodeDone::Failed {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            raw_size: 0,
            error: "synthetic decode failure".to_string(),
            servers: PoolServers::stamped(
                pipeline.pool_generation,
                None,
                weaver_nntp::ServerSet::EMPTY,
            ),
        })
        .await
        .unwrap();
}

impl QueueProbe {
    pub fn restored(&self) -> bool {
        self.0.read(|inner| {
            inner
                .jobs
                .iter()
                .all(|row| row.intents.restore.is_none() && !row.report.restoring)
        })
    }

    pub fn contains(&self, id: JobId) -> bool {
        self.0.read(|inner| inner.contains_job(id))
    }
}

pub fn disable_read_ahead(pipeline: &mut Pipeline) {
    pipeline.next_up.depth = 0;
}

pub async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

pub fn held_ahead(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.next_up.held()
}

pub fn stored_spec(pipeline: &Pipeline, id: JobId) -> JobSpec {
    let spec = &pipeline.jobs[&id].spec;
    activation::read_stored_spec(
        &pipeline.db,
        id,
        spec.category.clone(),
        spec.metadata.clone(),
    )
    .unwrap()
}

pub fn outlines(db: &Database) -> HashMap<JobId, crate::ingest::outline::NzbOutline> {
    use crate::jobs::outline_persistence::JobsOutlinePersistenceInternalQueries;
    db.load_job_outlines(None).unwrap()
}

pub fn persisted_admissions(db: &Database) -> HashSet<JobId> {
    use crate::jobs::queries::JobsInternalQueries;
    db.load_nzb_admissions().unwrap()
}

pub fn failure_error(pipeline: &Pipeline, id: JobId) -> Option<&str> {
    pipeline.jobs[&id].failure_error.as_deref()
}

pub fn hold_first_lane_work(pipeline: &mut Pipeline, job_id: JobId) -> SegmentId {
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let work = work.dispatched(&pipeline.jobs[&job_id].spec);
    let segment_id = work.segment_id;
    pipeline.hold_test_lane_works(vec![work], false);
    segment_id
}

pub async fn hear_closed_lane_and_refill(
    pipeline: &mut Pipeline,
    job_id: JobId,
    segment_id: SegmentId,
) {
    use weaver_server_db::persistence::executor::actor_thread;
    let (response_tx, _response_rx) = tokio::sync::oneshot::channel();
    actor_thread::OnActor(Box::pin(async {
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::Fetch(
                    DownloadFailure::from_lane_acquire_failure(Some(
                        &weaver_nntp::NntpError::ConnectionClosed,
                    )),
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
        pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
            lane_id: 0,
            runtime_generation: 0,
            server_idx: 0,
            supports_pipelining: false,
            current_mode: DownloadLaneMode::Sequential,
            response_tx,
        });
    }))
    .await;
}

pub fn actor_awaited_db_calls() -> u64 {
    crate::pipeline::orchestrator::actor_db::total()
}

pub fn actor_thread_db_calls() -> u64 {
    weaver_server_db::persistence::executor::actor_thread::total()
}

pub fn admission_visits() -> u64 {
    crate::pipeline::nzb_admission::ADMISSION_VISITS.with(std::cell::Cell::get)
}

pub fn admission_disk_held(pipeline: &Pipeline) -> bool {
    pipeline.admission_disk_held
}

pub fn disk_unadmitted(pipeline: &Pipeline) -> usize {
    pipeline.disk_unadmitted.len()
}

pub fn apply_disk_reading(pipeline: &mut Pipeline) {
    pipeline.disk_reading();
}

pub const DISK_RESERVE: u64 = 1 << 30;
const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

// The disk under every root, read by its sampler alone: `room` over the
// reserve, and the probes made on the thread the pipeline runs on.
pub struct ScaleDisk {
    room: Arc<std::sync::atomic::AtomicU64>,
    on_actor: Arc<std::sync::atomic::AtomicU64>,
}

impl ScaleDisk {
    pub fn under(pipeline: &mut Pipeline) -> Self {
        use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
        let room = Arc::new(std::sync::atomic::AtomicU64::new(DISK_RESERVE));
        let on_actor = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let actor = std::thread::current().id();
        let probe = {
            let (room, on_actor) = (Arc::clone(&room), Arc::clone(&on_actor));
            Arc::new(move |_: &std::path::Path| {
                if std::thread::current().id() == actor {
                    on_actor.fetch_add(1, Ordering::Relaxed);
                }
                Ok(DiskSpace {
                    total_bytes: DISK_RESERVE * 64,
                    available_bytes: DISK_RESERVE + room.load(Ordering::Acquire),
                })
            })
        };
        let roots = [
            pipeline.script_data_dir.clone(),
            pipeline.intermediate_dir.clone(),
            pipeline.complete_dir.clone(),
        ];
        let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
        for root in StorageRoot::ALL {
            capacity.wait_for_probes(root, 1);
        }
        pipeline.storage_capacity = Arc::new(capacity);
        let mut limits = (*pipeline.extraction_limits).clone();
        limits.min_free_bytes = DISK_RESERVE;
        pipeline.extraction_limits = Arc::new(limits);
        Self { room, on_actor }
    }

    // The working disk reads `room` over the reserve: the test waits
    // for the reading that says so, whatever other probes ran first.
    // `room` differs from the room the disk read last.
    pub fn reads(&self, pipeline: &Pipeline, room: u64) {
        use crate::operations::StorageRoot;
        self.room.store(room, Ordering::Release);
        let capacity = &pipeline.storage_capacity;
        capacity.request_refresh(StorageRoot::Intermediate);
        capacity.wait_for_reading(StorageRoot::Intermediate, |reading| {
            reading.available_bytes == DISK_RESERVE + room
        });
    }
    pub fn actor_probes(&self) -> u64 {
        self.on_actor.load(Ordering::Relaxed)
    }
}

// Hears each admitted job's article read come back, so what the read
// does is done before the test counts.
pub async fn activations_heard(pipeline: &mut Pipeline) {
    while pipeline.jobs.values().any(|state| {
        matches!(
            state.articles,
            crate::jobs::model::JobArticles::Activating(..)
        )
    }) {
        let done = pipeline.job_activated_rx.recv().await.unwrap();
        pipeline.handle_job_activated(done);
    }
}

pub fn reported_wait_reasons(pipeline: &Pipeline) -> Vec<Option<String>> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .map(|row| row.report.job.download_wait_reason.clone())
            .collect()
    })
}
