use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::sync::{RwLock, broadcast, mpsc};
use weaver_model::files::FileRole;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_core::ingest::{
    SubmitNzbError, compress_nzb_bytes, materialize_semantic_promotion,
};
use weaver_server_core::jobs::working_dir::compute_working_dir;
use weaver_server_core::runtime::buffers::{BufferPool, BufferPoolConfig};
use weaver_server_core::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::watch_folder::WatchFolderConfig;
use weaver_server_core::{
    Database, DuplicateAdmission, DuplicateAdmissionRequest, DuplicateMode, DuplicatePolicy,
    FileSpec, FingerprintEvidence, JobId, JobSpec, Pipeline, PipelineMetrics, SchedulerCommand,
    SchedulerError, SchedulerHandle, SegmentSpec, SemanticCandidateSource, SemanticCandidateState,
    SemanticDuplicate, SemanticPromotionState, SharedPipelineState, SubmissionOrigin,
    semantic_duplicate_lifecycle_metrics_snapshot,
};

struct Harness {
    temp_dir: TempDir,
    db_path: PathBuf,
    handle: SchedulerHandle,
    db: Database,
    pipeline_task: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("weaver.db");
        let data_dir = temp_dir.path().join("data");
        let intermediate_dir = temp_dir.path().join("intermediate");
        let complete_dir = temp_dir.path().join("complete");
        let db = Database::open(&db_path).unwrap();
        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: data_dir.display().to_string(),
            intermediate_dir: Some(intermediate_dir.display().to_string()),
            complete_dir: Some(complete_dir.display().to_string()),
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            cleanup_after_extract: Some(true),
            watch_folder: WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        }));
        let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
        let (event_tx, _) = broadcast::channel(1024);
        let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), state.clone());
        let nntp = NntpClient::new(NntpClientConfig {
            servers: vec![],
            max_idle_age: Duration::from_secs(1),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        });
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
            intermediate_dir,
            complete_dir,
            0,
            4,
            vec![],
            false,
            state,
            db.clone(),
            config,
        )
        .await
        .unwrap();
        let pipeline_task = tokio::spawn(async move { pipeline.run().await });
        Self {
            temp_dir,
            db_path,
            handle,
            db,
            pipeline_task,
        }
    }

    async fn expire_promotion_lease(&self, job_id: JobId, generation: i64) {
        let options = SqliteConnectOptions::new()
            .filename(&self.db_path)
            .create_if_missing(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        let changed = sqlx::query(
            "UPDATE semantic_duplicate_candidates
             SET promotion_lease_expires_at = 0
             WHERE job_id = ? AND promotion_state = 'claimed' AND promotion_generation = ?",
        )
        .bind(job_id.0 as i64)
        .bind(generation)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();
        pool.close().await;
        assert_eq!(changed, 1);
    }

    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        self.pipeline_task.await.unwrap();
    }
}

fn source_nzb_zstd() -> Vec<u8> {
    compress_nzb_bytes(
        br#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="test" date="1700000000" subject="fallback-source.bin">
                <groups><group>alt.binaries.test</group></groups>
                <segments><segment bytes="10" number="1">fallback-source@example.com</segment></segments>
              </file>
            </nzb>"#,
    )
    .unwrap()
}

fn score_spec(name: &str, message_id: &str) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: 10,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: format!("{name}.bin"),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![SegmentSpec {
                ordinal: 0,
                article_number: 1,
                bytes: 10,
                message_id: message_id.to_string(),
            }],
        }],
    }
}

fn score_request(spec: &JobSpec, raw_hash_byte: u8, score: i64) -> DuplicateAdmissionRequest {
    DuplicateAdmissionRequest {
        evidence: FingerprintEvidence::from_validated_spec(spec, [raw_hash_byte; 32]),
        mode: DuplicateMode::Score,
        semantic: SemanticDuplicate::from_source("lease-owner-race", score),
        semantic_source: Some(SemanticCandidateSource {
            nzb_zstd: source_nzb_zstd(),
            filename: Some("fallback-source.nzb".to_string()),
            password: None,
            category: None,
            metadata: vec![("test".to_string(), "semantic-promotion".to_string())],
        }),
        origin: SubmissionOrigin::Api,
        idempotency: None,
        policy: DuplicatePolicy::default(),
    }
}

fn admission_job_id(admission: DuplicateAdmission) -> JobId {
    match admission {
        DuplicateAdmission::Accepted { job_id, .. } | DuplicateAdmission::Parked { job_id, .. } => {
            job_id
        }
        outcome => panic!("expected semantic admission, got {outcome:?}"),
    }
}

#[tokio::test]
async fn expired_promotion_owner_cannot_materialize_after_reclaim() {
    let harness = Harness::new().await;
    let metric_count = |event| {
        semantic_duplicate_lifecycle_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.event == event)
            .map(|metric| metric.count)
            .unwrap_or_default()
    };
    let before_recovery = metric_count("promotion_recovery");
    let before_failure = metric_count("promotion_failure");
    let before_promote = metric_count("promote");
    let incumbent = score_spec("lease-incumbent", "lease-incumbent@example.com");
    let fallback = score_spec("lease-fallback", "lease-fallback@example.com");
    let incumbent_job = admission_job_id(
        harness
            .db
            .admit_duplicate_submission(&score_request(&incumbent, 1, 100))
            .unwrap(),
    );
    let fallback_job = admission_job_id(
        harness
            .db
            .admit_duplicate_submission(&score_request(&fallback, 2, 90))
            .unwrap(),
    );
    assert_eq!(
        harness
            .db
            .semantic_candidate_snapshot(fallback_job)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Parked
    );

    assert!(
        harness
            .db
            .mark_semantic_candidate_bad(incumbent_job)
            .unwrap()
    );
    let stale_owner = harness
        .db
        .claim_semantic_promotion(incumbent_job)
        .unwrap()
        .expect("fallback should be claimed");
    assert_eq!(stale_owner.job_id, fallback_job);
    assert_eq!(
        stale_owner.source.filename.as_deref(),
        Some("fallback-source.nzb")
    );
    assert_eq!(stale_owner.source.nzb_zstd, source_nzb_zstd());
    harness
        .expire_promotion_lease(fallback_job, stale_owner.generation)
        .await;

    let mut recovered = harness.db.reconcile_semantic_promotion_claims(8).unwrap();
    assert_eq!(recovered.len(), 1);
    let current_owner = recovered.pop().unwrap();
    assert_eq!(current_owner.job_id, fallback_job);
    assert_eq!(current_owner.generation, stale_owner.generation + 1);
    assert_eq!(
        current_owner.source.filename.as_deref(),
        Some("fallback-source.nzb")
    );
    assert_eq!(current_owner.source.nzb_zstd, source_nzb_zstd());

    let work_dir = compute_working_dir(
        &harness.temp_dir.path().join("intermediate"),
        fallback_job,
        "fallback-source",
    );
    assert!(!work_dir.exists());

    let stale = materialize_semantic_promotion(&harness.db, &harness.handle, stale_owner)
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        SubmitNzbError::Scheduler(SchedulerError::SemanticSuperseded)
    ));
    assert!(
        !work_dir.exists(),
        "stale owner created a persistent workdir"
    );
    assert!(harness.handle.get_job(fallback_job).is_err());
    assert!(harness.db.load_active_jobs().unwrap().is_empty());
    let claimed = harness
        .db
        .semantic_candidate_snapshot(fallback_job)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.state, SemanticCandidateState::Active);
    assert_eq!(claimed.promotion_state, SemanticPromotionState::Claimed);
    assert!(claimed.source_stored);

    assert_eq!(
        materialize_semantic_promotion(&harness.db, &harness.handle, current_owner)
            .await
            .unwrap(),
        fallback_job
    );
    assert!(harness.handle.get_job(fallback_job).is_ok());
    let active_jobs = harness.db.load_active_jobs().unwrap();
    assert_eq!(active_jobs.len(), 1);
    assert!(active_jobs.contains_key(&fallback_job));
    assert_eq!(
        harness
            .db
            .semantic_candidate_snapshot(fallback_job)
            .unwrap()
            .unwrap()
            .promotion_state,
        SemanticPromotionState::None
    );
    assert!(metric_count("promotion_recovery") > before_recovery);
    assert!(metric_count("promotion_failure") > before_failure);
    assert!(metric_count("promote") > before_promote);
    harness.shutdown().await;
}
