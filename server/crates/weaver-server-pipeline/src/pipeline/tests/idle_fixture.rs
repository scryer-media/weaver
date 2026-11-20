// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Exercise idle control-plane behavior through published events and metrics.

use super::*;

pub struct IdlePipeline {
    _temp_dir: TempDir,
    pub handle: SchedulerHandle,
    pub shared_state: SharedPipelineState,
    task: tokio::task::JoinHandle<()>,
}

// A pipeline holding `paused_jobs` paused jobs and `history` finished rows,
// with `prepare` applied before it starts running.
pub async fn idle_pipeline(
    paused_jobs: u64,
    history: Vec<JobInfo>,
    prepare: impl FnOnce(&mut Pipeline),
) -> IdlePipeline {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().join("data");
    let intermediate_dir = temp_dir.path().join("intermediate");
    let complete_dir = temp_dir.path().join("complete");
    let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
    let config: SharedConfig = Arc::new(RwLock::new(Config {
        upload_cap: None,
        seeding: None,
        torrent: Default::default(),
        data_dir: data_dir.display().to_string(),
        hardware_profile: None,
        intermediate_dir: Some(intermediate_dir.display().to_string()),
        complete_dir: Some(complete_dir.display().to_string()),
        servers: vec![],
        categories: vec![],
        retry: None,
        bandwidth: Default::default(),
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        cleanup_after_extract: Some(true),
        delete_failed_job_files: None,
        history_retention_days: None,
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));

    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let handle = SchedulerHandle::new(event_tx.clone(), shared_state.clone());
    let profile = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 4,
            logical_cores: 4,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 8 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let nntp = NntpClient::new(NntpClientConfig {
        servers: vec![],
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    let mut pipeline = Pipeline::new(
        event_tx,
        nntp,
        16 * 1024 * 1024,
        profile,
        data_dir,
        intermediate_dir.clone(),
        complete_dir,
        0,
        4,
        history,
        crate::jobs::HoldSet::default(),
        shared_state.clone(),
        db,
        config,
        crate::runtime::StartupDiskGate::opened(),
        crate::pipeline::graph::disk::threads::writers(),
        std::sync::Arc::new(crate::operations::VolumeBuffers::new()),
    )
    .await
    .unwrap();
    for id in 1..=paused_jobs {
        let job_id = JobId(id);
        let mut state = minimal_job_state(
            job_id,
            &format!("paused-{id}"),
            intermediate_dir.join(format!("paused-{id}")),
        );
        state.status = JobStatus::Paused;
        pipeline.jobs.insert(job_id, state);
        pipeline.job_order.push(job_id);
    }
    pipeline.report_all_for_test();
    prepare(&mut pipeline);
    let task = tokio::spawn(async move { pipeline.run().await });
    IdlePipeline {
        _temp_dir: temp_dir,
        handle,
        shared_state,
        task,
    }
}

impl IdlePipeline {
    pub async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        self.task.await.unwrap();
    }
}

pub fn long_history() -> Vec<JobInfo> {
    let rows = crate::jobs::FINISHED_JOBS_RUNTIME_CAP.min(2_000) as u64;
    (10_000..10_000 + rows)
        .map(|id| finished_job_info(JobId(id)))
        .collect()
}

fn queued_work(job_id: JobId) -> DownloadWork {
    DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        message_id: weaver_server_base::jobs::ids::MessageId::new(&format!(
            "queued-{}@example.com",
            job_id.0
        )),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 1000,
        byte_estimate: 64,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        avoid_server: None,
    }
}

pub fn seed_reconcile(pipeline: &mut Pipeline, intermediate_dir: &std::path::Path) -> JobId {
    for id in 1..=200 {
        let job_id = JobId(id);
        let mut state = minimal_job_state(
            job_id,
            &format!("queued-{id}"),
            intermediate_dir.join(format!("queued-{id}")),
        );
        state.status = JobStatus::Queued;
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(queued_work(job_id));
        pipeline.jobs.insert(job_id, state);
        pipeline.job_order.push(job_id);
    }
    let drained = JobId(500);
    pipeline.jobs.insert(
        drained,
        minimal_job_state(drained, "drained", intermediate_dir.join("drained")),
    );
    pipeline.job_order.push(drained);

    drained
}

use crate::pipeline::graph::engine::{Heard, Said};
pub struct GraphMail(tokio::sync::mpsc::Sender<Said>);
impl GraphMail {
    pub async fn ended(&self, scope: u64, node: weaver_graph::NodeId, generation: u64) {
        self.0
            .send(Said {
                scope,
                node,
                generation,
                heard: Heard::Ended,
            })
            .await
            .unwrap();
    }
}
pub async fn with_mail() -> (
    IdlePipeline,
    GraphMail,
    tokio::sync::mpsc::UnboundedReceiver<
        weaver_server_base::operations::instrumentation::PipelineTurnReading,
    >,
) {
    let (stamps_tx, stamps) = tokio::sync::mpsc::unbounded_channel();
    let mut mail = None;
    let pipeline = idle_pipeline(0, Vec::new(), |pipeline| {
        pipeline.graph_mail_stamps = Some(stamps_tx);
        mail = Some(pipeline.graph_runs.sender());
    })
    .await;
    let mail = mail.unwrap();
    (pipeline, GraphMail(mail), stamps)
}

pub fn clear_checks(pipeline: &mut Pipeline) {
    pipeline.pending_completion_checks.clear();
}
pub async fn reconcile(pipeline: &mut Pipeline) {
    pipeline.reconcile_candidate_jobs().await;
}
pub fn take_checks(pipeline: &mut Pipeline) -> Vec<JobId> {
    pipeline.pending_completion_checks.drain(..).collect()
}
pub fn checks_empty(pipeline: &Pipeline) -> bool {
    pipeline.pending_completion_checks.is_empty()
}
pub fn invalidate_reconciliation(pipeline: &mut Pipeline) {
    pipeline.reconcile_clean = false;
}
pub fn advance_downloaded(pipeline: &mut Pipeline, id: JobId) {
    pipeline.jobs.get_mut(&id).unwrap().downloaded_bytes += 1;
}
