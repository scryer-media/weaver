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
