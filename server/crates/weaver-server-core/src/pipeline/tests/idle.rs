//! What an idle pipeline costs: no job snapshot publishes, a slow metrics
//! tick, and no completion checks for jobs that cannot have changed.
//!
//! Every test runs on the paused clock. Sleeping on it advances virtual time
//! only once every task is parked, so each periodic tick runs to completion
//! before the next one fires.

use super::*;

struct IdlePipeline {
    _temp_dir: TempDir,
    handle: SchedulerHandle,
    shared_state: SharedPipelineState,
    task: tokio::task::JoinHandle<()>,
}

/// A pipeline holding `paused_jobs` paused jobs and `history` finished rows,
/// with `prepare` applied before it starts running.
async fn idle_pipeline(
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
        data_dir: data_dir.display().to_string(),
        hardware_profile: None,
        intermediate_dir: Some(intermediate_dir.display().to_string()),
        complete_dir: Some(complete_dir.display().to_string()),
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        isp_bandwidth_cap: None,
        propagation_delay_secs: None,
        cleanup_after_extract: Some(true),
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        direct_unpack: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());
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
    let buffers = BufferPool::new(BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    });
    let mut pipeline = Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir,
        intermediate_dir.clone(),
        complete_dir,
        0,
        4,
        history,
        false,
        shared_state.clone(),
        db,
        config,
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
    /// Wait until the startup publish has happened, so later revisions
    /// count only what the test causes.
    async fn settled_revisions(&self) -> tokio::sync::watch::Receiver<u64> {
        let mut revisions = self.handle.subscribe_job_changes();
        revisions.wait_for(|revision| *revision > 0).await.unwrap();
        revisions.mark_unchanged();
        // One idle tick after startup, so the first turn's own work is done.
        let refreshes = self.shared_state.metrics_refresh_count();
        while self.shared_state.metrics_refresh_count() == refreshes {
            tokio::time::sleep(Pipeline::IDLE_SNAPSHOT_INTERVAL).await;
        }
        revisions.mark_unchanged();
        revisions
    }

    /// Metrics refreshes over `span` of virtual time.
    async fn refreshes_over(&self, span: Duration) -> u64 {
        let before = self.shared_state.metrics_refresh_count();
        tokio::time::sleep(span).await;
        self.shared_state.metrics_refresh_count() - before
    }

    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        self.task.await.unwrap();
    }
}

fn long_history() -> Vec<JobInfo> {
    let rows = crate::jobs::FINISHED_JOBS_RUNTIME_CAP.min(2_000) as u64;
    (10_000..10_000 + rows)
        .map(|id| finished_job_info(JobId(id)))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn idle_pipeline_with_paused_jobs_and_history_publishes_nothing() {
    let pipeline = idle_pipeline(64, long_history(), |_| {}).await;
    let revisions = pipeline.settled_revisions().await;
    let published = *revisions.borrow();

    let refreshes = pipeline.refreshes_over(Duration::from_secs(60)).await;

    assert!(
        !revisions.has_changed().unwrap(),
        "an idle pipeline republished its job snapshot"
    );
    assert_eq!(*revisions.borrow(), published);
    // The gauges are still sampled, at the idle cadence.
    assert!(refreshes > 0);
    assert!(
        refreshes <= 61,
        "idle metrics tick ran {refreshes} times in 60 s"
    );
    pipeline.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn idle_pipeline_publishes_a_change_in_the_turn_that_makes_it() {
    let pipeline = idle_pipeline(8, long_history(), |_| {}).await;
    let mut revisions = pipeline.settled_revisions().await;

    // The command's reply is sent from the turn that handles it, and that
    // turn ends with the publish: no periodic tick runs between the reply
    // and the new revision. (Ticks may run while the command persists its
    // setting, before the reply, so the count is taken after it.)
    pipeline.handle.pause_all().await.unwrap();
    let refreshes = pipeline.shared_state.metrics_refresh_count();
    revisions.changed().await.unwrap();
    assert_eq!(pipeline.shared_state.metrics_refresh_count(), refreshes);
    assert!(pipeline.handle.list_jobs().len() >= long_history().len());
    pipeline.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn metrics_tick_is_fast_while_live_and_slow_while_idle() {
    let idle = idle_pipeline(8, Vec::new(), |_| {}).await;
    idle.settled_revisions().await;
    let idle_refreshes = idle.refreshes_over(Duration::from_secs(10)).await;
    idle.shutdown().await;

    // A phase past the download in flight keeps the pipeline live.
    let live = idle_pipeline(8, Vec::new(), |pipeline| {
        pipeline.phase_begin(JobId(1), JobPhase::Repairing, Some(1_000));
    })
    .await;
    live.handle
        .subscribe_job_changes()
        .wait_for(|revision| *revision > 0)
        .await
        .unwrap();
    let live_refreshes = live.refreshes_over(Duration::from_secs(10)).await;
    live.shutdown().await;

    assert!(
        idle_refreshes <= 11,
        "idle pipeline refreshed metrics {idle_refreshes} times in 10 s"
    );
    assert!(
        live_refreshes >= 90,
        "live pipeline refreshed metrics only {live_refreshes} times in 10 s"
    );
}

#[tokio::test(start_paused = true)]
async fn moving_rates_hold_the_fast_tick_until_a_refresh_reads_zero() {
    // The pipeline starts as if the last refresh had seen bytes moving. The
    // rate estimator runs on the wall clock, which the paused clock does not
    // drive, so the test starts from the flag rather than from real bytes.
    let pipeline = idle_pipeline(4, Vec::new(), |pipeline| {
        pipeline.metrics_rates_moving = true;
    })
    .await;
    pipeline.settled_revisions().await;
    let settled = pipeline.shared_state.metrics_snapshot();
    assert_eq!(settled.current_download_speed, 0);
    assert_eq!(settled.articles_per_sec, 0.0);
    assert_eq!(settled.decode_rate_mbps, 0.0);

    // The refresh that read zero rates cleared the flag; the tick is slow.
    let refreshes = pipeline.refreshes_over(Duration::from_secs(10)).await;
    assert!(
        refreshes <= 11,
        "settled gauges kept the fast tick: {refreshes} refreshes in 10 s"
    );
    pipeline.shutdown().await;
}

fn queued_work(job_id: JobId) -> DownloadWork {
    DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        message_id: MessageId::new(&format!("queued-{}@example.com", job_id.0)),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 1000,
        byte_estimate: 64,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    }
}

#[tokio::test]
async fn reconcile_checks_only_jobs_that_could_be_waiting_on_a_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    for id in 1..=200 {
        let job_id = JobId(id);
        let mut state = minimal_job_state(
            job_id,
            &format!("queued-{id}"),
            intermediate_dir.join(format!("queued-{id}")),
        );
        state.status = JobStatus::Queued;
        state.download_queue.push(queued_work(job_id));
        pipeline.jobs.insert(job_id, state);
        pipeline.job_order.push(job_id);
    }
    let drained = JobId(500);
    pipeline.jobs.insert(
        drained,
        minimal_job_state(drained, "drained", intermediate_dir.join("drained")),
    );
    pipeline.job_order.push(drained);

    // The first pass checks the drained download and nothing queued.
    pipeline.reconcile_candidate_jobs().await;
    assert_eq!(
        pipeline
            .pending_completion_checks
            .drain(..)
            .collect::<Vec<_>>(),
        vec![drained]
    );

    // Later passes with nothing changed check nothing: first because nothing
    // happened at all, then because the drained job looks as it did.
    pipeline.reconcile_candidate_jobs().await;
    assert!(pipeline.pending_completion_checks.is_empty());
    pipeline.reconcile_clean = false;
    pipeline.reconcile_candidate_jobs().await;
    assert!(pipeline.pending_completion_checks.is_empty());

    // Progress on the drained job makes it worth checking again.
    pipeline.jobs.get_mut(&drained).unwrap().downloaded_bytes += 1;
    pipeline.reconcile_clean = false;
    pipeline.reconcile_candidate_jobs().await;
    assert_eq!(
        pipeline
            .pending_completion_checks
            .drain(..)
            .collect::<Vec<_>>(),
        vec![drained]
    );
}
