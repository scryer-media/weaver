// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Exercise idle control-plane behavior through published events and metrics.

use super::*;

struct IdlePipeline {
    _temp_dir: TempDir,
    handle: SchedulerHandle,
    shared_state: SharedPipelineState,
    task: tokio::task::JoinHandle<()>,
}

// A pipeline holding `paused_jobs` paused jobs and `history` finished rows,
// with `prepare` applied before it starts running.
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
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
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
        false,
        shared_state.clone(),
        db,
        config,
        crate::runtime::StartupDiskGate::opened(),
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
async fn idle_pipeline_preserves_paused_jobs_and_history_when_global_intents_change() {
    let pipeline = idle_pipeline(64, long_history(), |_| {}).await;
    let before = pipeline.handle.list_jobs();
    let mut events = pipeline.handle.subscribe_events();
    let mut revisions = pipeline.handle.subscribe_job_changes();
    revisions.mark_unchanged();
    pipeline.handle.pause_all().await.unwrap();
    revisions.changed().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    let after = pipeline.handle.list_jobs();
    assert_eq!(after.len(), before.len());
    for id in 1..=64 {
        assert_eq!(
            pipeline.shared_state.get_job(JobId(id)).unwrap().status,
            JobStatus::Paused
        );
    }
    pipeline.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn idle_pipeline_records_production_turn_histogram() {
    let pipeline = idle_pipeline(0, Vec::new(), |_| {}).await;
    let shared = pipeline.shared_state.clone();
    let mut events = pipeline.handle.subscribe_events();
    pipeline.handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    pipeline.shutdown().await;
    assert!(
        shared
            .metrics()
            .pipeline_histograms
            .turn_duration
            .snapshot()
            .count
            > 0
    );
}

// The loop stamps its turns where the handle reads them: once the pipeline
// has stopped, turns have completed, none is in progress, and the last one
// to close counted the jobs it had installed.
#[tokio::test(start_paused = true)]
async fn idle_pipeline_stamps_its_turns_where_the_handle_reads_them() {
    use crate::operations::instrumentation::PipelinePhase;

    let pipeline = idle_pipeline(2, Vec::new(), |_| {}).await;
    let handle = pipeline.handle.clone();
    let mut events = handle.subscribe_events();
    handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    pipeline.shutdown().await;

    let (queue, turn) = handle.pipeline_unanswered_state(std::time::Instant::now());
    assert_eq!(turn.phase, PipelinePhase::BetweenTurns);
    assert_eq!(turn.busy_for, None);
    assert!(turn.turns_completed > 0);
    assert!(turn.since_last_turn_ended.is_some());
    assert_eq!(turn.installed_jobs, Some(2));
    assert_eq!(queue.jobs, 2);
    assert_eq!(queue.jobs_by_status.count("paused"), 2.0);
    assert!(queue.global_paused);
}

// Graph mail that ends the wait (a run or repair reporting back) makes the
// turn busy under its own phase, as every other event does.
#[tokio::test(start_paused = true)]
async fn a_turn_woken_by_graph_mail_is_stamped_busy_with_graph_run() {
    use crate::operations::instrumentation::PipelinePhase;
    use crate::pipeline::graph::engine::{Heard, Said};

    let (stamps_tx, mut stamps) = tokio::sync::mpsc::unbounded_channel();
    let mut mail = None;
    let pipeline = idle_pipeline(0, Vec::new(), |pipeline| {
        pipeline.graph_mail_stamps = Some(stamps_tx);
        mail = Some(pipeline.graph_runs.sender());
    })
    .await;
    let mail = mail.unwrap();
    let mut events = pipeline.handle.subscribe_events();
    pipeline.handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}

    // Mail that is already waiting when a turn opens is heard before the
    // wait; only mail arriving during the wait reaches the select arm.
    let turn = &pipeline.shared_state.metrics().pipeline_turn;
    let mut yields = 0;
    while turn.read(std::time::Instant::now()).phase != PipelinePhase::WaitingForWork {
        yields += 1;
        assert!(yields < 100_000, "the pipeline never reached its wait");
        tokio::task::yield_now().await;
    }
    mail.send(Said {
        scope: 92_099,
        node: weaver_graph::NodeId(1),
        generation: 0,
        heard: Heard::Ended,
    })
    .await
    .unwrap();

    let reading = stamps.recv().await.expect("the graph mail arm ran");
    assert_eq!(reading.phase, PipelinePhase::GraphRun);
    assert!(
        reading.busy_for.is_some(),
        "the turn is busy while it handles the mail"
    );
    pipeline.shutdown().await;
}

#[tokio::test]
async fn idle_footprint_tick_tracks_queue_edits_while_globally_paused() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.pause_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.refresh_periodic_snapshot();
    assert!(!pipeline.pipeline_is_live());
    assert_eq!(
        pipeline
            .metrics
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        0
    );

    let id = JobId(31020);
    insert_active_job(
        &mut pipeline,
        id,
        standalone_job_spec("idle queue edit", &[("payload.bin".into(), 512)]),
    )
    .await;
    pipeline.report_all_for_test();
    pipeline.pending_completion_checks.clear();
    assert!(!pipeline.pipeline_is_live());
    pipeline.refresh_periodic_snapshot();
    assert_eq!(
        pipeline
            .metrics
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        pipeline.metrics.download_jobs_hot.load(Ordering::Relaxed),
        1
    );

    handle.cancel_job(id).await.unwrap();
    assert!(!pipeline.pipeline_is_live());
    pipeline.refresh_periodic_snapshot();
    assert_eq!(
        pipeline
            .metrics
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline.metrics.download_jobs_hot.load(Ordering::Relaxed),
        0
    );
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
