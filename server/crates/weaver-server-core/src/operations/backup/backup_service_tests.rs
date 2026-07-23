use super::*;
use std::path::Path;
use std::sync::{Arc as StdArc, Mutex as StdMutex};

use crate::JobHistoryRow;
use crate::categories::CategoryConfig;
use crate::operations::backup::manifest::BackupInstanceSecrets;
use crate::persistence::sql_runtime::StoreDatastore;
use crate::post_processing::discovery::{
    DiscoveryOptions, approve_discovered_extension, discover_and_record_extensions, hash_package,
};
use crate::post_processing::model::{
    ApprovedFilesystemRoot, ApprovedFilesystemRoots, AttemptStatus, ExtensionSelection, OnFailure,
    OrderedStep, OutcomeImpact, PipelineOutcome, PostProcessingSettings, PostProcessingSummary,
    Profile, ProfileId, RunStatus, RunWhen, TimeoutPolicy,
};
use crate::post_processing::persistence::{LogStream, TerminalIntent};
use crate::settings::Config;
use crate::{PipelineMetrics, SchedulerCommand, SharedPipelineState};
use tokio::sync::{RwLock, broadcast, mpsc};

#[derive(Default, Clone)]
struct RuntimeCapture {
    updated_paths: StdArc<StdMutex<Option<(PathBuf, PathBuf, PathBuf)>>>,
    speed_limits: StdArc<StdMutex<Vec<u64>>>,
}

fn open_temp_db() -> Database {
    Database::open_in_memory().unwrap()
}

fn execute_sql(db: &Database, statements: Vec<&'static str>) {
    let StoreDatastore::Sqlite { pool, .. } = db.datastore() else {
        panic!("test database is SQLite");
    };
    db.run_sql_blocking_local(move || async move {
        let mut connection = pool
            .acquire()
            .await
            .map_err(|error| crate::StateError::Database(error.to_string()))?;
        for statement in statements {
            sqlx::query(statement)
                .execute(&mut *connection)
                .await
                .map_err(|error| crate::StateError::Database(error.to_string()))?;
        }
        Ok(())
    })
    .unwrap();
}

fn query_count(db: &Database, table: &'static str) -> i64 {
    let StoreDatastore::Sqlite { pool, .. } = db.datastore() else {
        panic!("test database is SQLite");
    };
    db.run_sql_blocking_local(move || async move {
        let mut connection = pool
            .acquire()
            .await
            .map_err(|error| crate::StateError::Database(error.to_string()))?;
        let query = format!("SELECT COUNT(*) FROM {table}");
        sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_one(&mut *connection)
            .await
            .map_err(|error| crate::StateError::Database(error.to_string()))
    })
    .unwrap()
}

fn build_service(
    mut db: Database,
    config: Config,
) -> (
    BackupService,
    RuntimeCapture,
    StdArc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
) {
    if db.encryption_key().is_none() {
        db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    }
    let capture = RuntimeCapture::default();
    let transfer_policy = StdArc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            db.clone(),
            &config.servers,
        )
        .unwrap(),
    );
    let restore_locator_dir = db
        .database_target()
        .sqlite_path()
        .ok()
        .flatten()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from(&config.data_dir));
    let shared_config = StdArc::new(RwLock::new(config));
    let handle = test_scheduler_handle(capture.clone());
    handle.set_server_transfer_policy(StdArc::clone(&transfer_policy));
    let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
    (
        BackupService::new(handle, shared_config, db, rss, restore_locator_dir),
        capture,
        transfer_policy,
    )
}

fn test_scheduler_handle(capture: RuntimeCapture) -> SchedulerHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
    let (event_tx, _) = broadcast::channel(16);
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let task_state = state.clone();
    tokio::spawn(async move {
        while let Some(command) = cmd_rx.recv().await {
            match command {
                SchedulerCommand::SetSpeedLimit {
                    bytes_per_sec,
                    reply,
                } => {
                    capture.speed_limits.lock().unwrap().push(bytes_per_sec);
                    let _ = reply.send(());
                }
                SchedulerCommand::SetBandwidthCapPolicy { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::RebuildNntp { reply, .. } => {
                    let _ = reply.send(Ok(crate::NntpRuntimeActivation {
                        generation: 1,
                        configured_connections: 0,
                        effective_connections: 0,
                    }));
                }
                SchedulerCommand::UpdateRuntimePaths {
                    data_dir,
                    intermediate_dir,
                    complete_dir,
                    reply,
                } => {
                    *capture.updated_paths.lock().unwrap() =
                        Some((data_dir, intermediate_dir, complete_dir));
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::PauseAll { reply } => {
                    task_state.set_paused(true);
                    let _ = reply.send(());
                }
                SchedulerCommand::ResumeAll { reply } => {
                    task_state.set_paused(false);
                    let _ = reply.send(());
                }
                _ => {}
            }
        }
    });
    SchedulerHandle::new(cmd_tx, event_tx, state)
}

fn sample_server(id: u32) -> crate::servers::ServerConfig {
    crate::servers::ServerConfig {
        id,
        host: format!("news-{id}.example.com"),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 2,
        active: true,
        supports_pipelining: true,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: crate::servers::ServerDownloadQuotaConfig::default(),
        tls_ca_cert: None,
    }
}

fn sample_config() -> Config {
    Config {
        data_dir: "/old/data".into(),
        intermediate_dir: Some("/old/data/intermediate".into()),
        complete_dir: Some("/old/data/complete".into()),
        buffer_pool: None,
        tuner: None,
        servers: vec![],
        categories: vec![
            CategoryConfig {
                id: 1,
                name: "tv".into(),
                dest_dir: Some("/old/data/complete/tv".into()),
                aliases: String::new(),
            },
            CategoryConfig {
                id: 2,
                name: "movies".into(),
                dest_dir: Some("/mnt/media/movies".into()),
                aliases: String::new(),
            },
        ],
        retry: None,
        max_download_speed: Some(1234),
        isp_bandwidth_cap: None,
        ip_replacement_trial_extra_connections: None,
        cleanup_after_extract: Some(true),
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        config_path: None,
    }
}

fn populate_source_db(db: &Database) {
    db.save_config(&sample_config()).unwrap();
    db.insert_job_history(&JobHistoryRow {
        job_id: 41,
        job_hash: None,
        name: "Backup Fixture".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 10,
        downloaded_bytes: 10,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: Some("tv".into()),
        output_dir: Some("/old/data/complete/tv/fixture".into()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
    })
    .unwrap();
}

fn add_managed_extension(db: &Database, data_dir: &Path) -> PathBuf {
    let package = data_dir.join("scripts/email");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("manifest.json"),
        include_str!("../../post_processing/fixtures/nzbget-v2-post-processing-manifest.json"),
    )
    .unwrap();
    std::fs::write(package.join("email.py"), "#!/usr/bin/env python3\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(
            package.join("email.py"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }
    let discovered = discover_and_record_extensions(
        db,
        data_dir,
        DiscoveryOptions {
            enabled: true,
            bare_script_adapter: None,
        },
        10,
    )
    .unwrap();
    let revision = discovered[0].manifest.revision();
    approve_discovered_extension(
        db,
        data_dir,
        revision.extension_id(),
        revision.revision_id(),
        20,
    )
    .unwrap()
}

#[test]
fn external_category_paths_require_manual_mapping() {
    let config = sample_config();
    let remaps = required_category_remaps(&config);
    assert_eq!(remaps.len(), 1);
    assert_eq!(remaps[0].category_name, "movies");
}

#[tokio::test]
async fn password_protected_backup_roundtrip_inspects() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (service, _, _) = build_service(source_db, sample_config());

    let artifact = service
        .create_backup(Some("secret-pass".into()))
        .await
        .unwrap();
    assert!(artifact.filename.ends_with(".enc"));

    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, temp.path()).unwrap();

    let inspect = service
        .inspect_backup(temp.path(), Some("secret-pass".into()))
        .await
        .unwrap();
    assert_eq!(inspect.manifest.scope, BACKUP_SCOPE);
    assert_eq!(inspect.required_category_remaps.len(), 1);
    assert_eq!(inspect.required_category_remaps[0].category_name, "movies");

    let error = service
        .inspect_backup(temp.path(), Some("wrong-pass".into()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, BackupServiceError::InvalidPassword),
        "{error:?}"
    );
}

#[tokio::test]
async fn logical_backup_rejects_a_master_key_that_cannot_decrypt_its_data() {
    let mut source_db = open_temp_db();
    source_db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    let mut config = sample_config();
    let mut server = sample_server(1);
    server.username = Some("backup-user".into());
    server.password = Some("backup-password".into());
    config.servers = vec![server];
    source_db.save_config(&config).unwrap();
    let (service, _, _) = build_service(source_db, config);
    let artifact = service
        .create_backup(Some("secret-pass".into()))
        .await
        .unwrap();

    let unpacked = tempfile::tempdir().unwrap();
    let mut manifest =
        archive::unpack_bundle_archive(&artifact.path, unpacked.path(), Some("secret-pass".into()))
            .unwrap();
    let secrets = BackupInstanceSecrets {
        encryption_master_key: crate::persistence::encryption::EncryptionKey::generate()
            .to_base64(),
        key_source: "test".into(),
    };
    let secrets_bytes = serde_json::to_vec_pretty(&secrets).unwrap();
    std::fs::write(
        unpacked.path().join("instance-secrets.json"),
        &secrets_bytes,
    )
    .unwrap();
    manifest.part_checksums.insert(
        "instance-secrets.json".into(),
        blake3::hash(&secrets_bytes).to_hex().to_string(),
    );
    std::fs::write(
        unpacked.path().join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let tampered = tempfile::NamedTempFile::new().unwrap();
    archive::write_bundle_archive(tampered.path(), "secret-pass", unpacked.path(), &[]).unwrap();

    let error = service
        .inspect_backup(tampered.path(), Some("secret-pass".into()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, BackupServiceError::Validation(ref message)
            if message.contains("encryption master key")),
        "{error:?}"
    );
}

#[tokio::test]
async fn logical_backup_preserves_only_terminal_duplicate_identity_graphs() {
    let source_db = open_temp_db();
    source_db.save_config(&sample_config()).unwrap();
    execute_sql(
        &source_db,
        vec![
            "INSERT INTO duplicate_job_snapshots
                (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at)
             VALUES (20001, 'succeeded', 'terminal', 'accept', 'api', 1, 1)",
            "INSERT INTO duplicate_job_snapshots
                (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at)
             VALUES (20002, 'active', 'active', 'accept', 'api', 1, 1)",
            "INSERT INTO job_fingerprints
                (job_id, fingerprint_kind, fingerprint_version, fingerprint_digest, created_at)
             VALUES (20001, 'name', 1, X'01', 1)",
            "INSERT INTO job_fingerprints
                (job_id, fingerprint_kind, fingerprint_version, fingerprint_digest, created_at)
             VALUES (20002, 'name', 1, X'02', 1)",
            "INSERT INTO duplicate_admission_claims
                (fingerprint_kind, fingerprint_version, fingerprint_digest, job_id, claimed_at)
             VALUES ('name', 1, X'01', 20001, 1)",
            "INSERT INTO duplicate_admission_claims
                (fingerprint_kind, fingerprint_version, fingerprint_digest, job_id, claimed_at)
             VALUES ('name', 1, X'02', 20002, 1)",
            "INSERT INTO submission_idempotency
                (caller_scope, idempotency_key, request_hash, job_id, created_at)
             VALUES ('admin', 'terminal', X'01', 20001, 1)",
            "INSERT INTO submission_idempotency
                (caller_scope, idempotency_key, request_hash, job_id, created_at)
             VALUES ('admin', 'active', X'02', 20002, 1)",
            "INSERT INTO semantic_duplicate_groups
                (group_id, normalized_key, created_at, updated_at)
             VALUES (1, 'group', 1, 1)",
            "INSERT INTO semantic_duplicate_candidates
                (job_id, group_id, score, candidate_state, created_at, updated_at)
             VALUES (20001, 1, 100, 'nonblocking', 1, 1)",
            "INSERT INTO semantic_duplicate_candidates
                (job_id, group_id, score, candidate_state, created_at, updated_at)
             VALUES (20002, 1, 90, 'active', 1, 1)",
            "INSERT INTO forgotten_duplicate_identities (job_id, forgotten_at)
             VALUES (19999, 1)",
        ],
    );
    let (source_service, _, _) = build_service(source_db, sample_config());
    let artifact = source_service
        .create_backup(Some("duplicate-secret".into()))
        .await
        .unwrap();

    let target_root = tempfile::tempdir().unwrap();
    let target_db = open_temp_db();
    let mut target_config = sample_config();
    target_config.data_dir = target_root.path().to_string_lossy().into_owned();
    let (target_service, _, _) = build_service(target_db.clone(), target_config);
    target_service
        .restore_backup(
            &artifact.path,
            Some("duplicate-secret".into()),
            RestoreOptions {
                data_dir: target_root.path().to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: target_root
                        .path()
                        .join("movies")
                        .to_string_lossy()
                        .into_owned(),
                }],
            },
        )
        .await
        .unwrap();
    drop(target_service);
    let (restored, outcome) = apply_pending_restore(target_db, target_root.path()).unwrap();
    assert!(outcome.is_some());

    for table in [
        "duplicate_job_snapshots",
        "job_fingerprints",
        "duplicate_admission_claims",
        "submission_idempotency",
        "semantic_duplicate_groups",
        "semantic_duplicate_candidates",
        "forgotten_duplicate_identities",
    ] {
        assert_eq!(query_count(&restored, table), 1, "{table}");
    }
    assert!(restored.initialize_next_job_id_counter().unwrap() > 20001);
}

#[tokio::test]
async fn encrypted_v2_backup_restores_managed_packages_under_the_target_data_dir() {
    let source_root = tempfile::tempdir().unwrap();
    let mut source_config = sample_config();
    source_config.intermediate_dir = Some(
        source_root
            .path()
            .join("intermediate")
            .to_string_lossy()
            .into_owned(),
    );
    source_config.complete_dir = Some(
        source_root
            .path()
            .join("complete")
            .to_string_lossy()
            .into_owned(),
    );
    source_config.categories[0].dest_dir = Some(
        source_root
            .path()
            .join("complete/tv")
            .to_string_lossy()
            .into_owned(),
    );
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    source_db.save_config(&source_config).unwrap();
    source_db
        .set_setting("watch_folder.mode", "polling")
        .unwrap();
    source_db
        .set_setting("watch_folder.path", "/host-only/incoming")
        .unwrap();
    let source_managed = add_managed_extension(&source_db, source_root.path());
    let source_digest = hash_package(&source_managed).unwrap();
    let revision_record = source_db.list_extension_revisions().unwrap().remove(0);
    let revision = revision_record.manifest.revision();
    let approved_roots = vec![
        ApprovedFilesystemRoot::new("/old/data/approved").unwrap(),
        ApprovedFilesystemRoot::new("/host-only/approved").unwrap(),
    ];
    source_db
        .save_post_processing_settings(&PostProcessingSettings {
            allowed_roots: approved_roots
                .iter()
                .map(|root| root.as_str().to_string())
                .collect(),
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let profile_id = ProfileId::new("backup-profile").unwrap();
    let profile = Profile::new(
        profile_id.clone(),
        "Backup Profile".into(),
        vec![
            OrderedStep::new(
                0,
                ExtensionSelection::pinned(
                    revision.extension_id().clone(),
                    revision.revision_id().clone(),
                ),
                RunWhen::Always,
                OnFailure::Stop,
                OutcomeImpact::FailJob,
                TimeoutPolicy::Default24Hours,
                ApprovedFilesystemRoots::new(approved_roots),
                vec![],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    source_db
        .save_post_processing_profile(&profile, true, 30)
        .unwrap();
    source_db
        .assign_category_post_processing_profile("tv", Some(&profile_id))
        .unwrap();
    let frozen = source_db
        .resolve_post_processing_plan(None, Some("tv"))
        .unwrap();
    source_db
        .save_frozen_post_processing_plan(41, &frozen, 31)
        .unwrap();
    let run_id = source_db
        .create_post_processing_run(
            41,
            &frozen,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            32,
        )
        .unwrap();
    let attempt_id = source_db
        .enqueue_post_processing_attempt(
            &run_id,
            &frozen.steps()[0],
            revision_record.manifest.adapter(),
            None,
            33,
        )
        .unwrap();
    source_db
        .append_post_processing_log(&attempt_id, LogStream::Stdout, b"restored log", 34)
        .unwrap();
    source_db
        .finish_post_processing_attempt(
            &attempt_id,
            AttemptStatus::Succeeded,
            Some(0),
            None,
            None,
            35,
        )
        .unwrap();
    source_db
        .finish_post_processing_run(
            &run_id,
            RunStatus::Succeeded,
            PostProcessingSummary::Succeeded,
            36,
        )
        .unwrap();
    let queued_run_id = source_db
        .create_post_processing_run(
            41,
            &frozen,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            Some(&run_id),
            37,
        )
        .unwrap();
    let (source_service, _, _) = build_service(source_db, source_config.clone());

    let artifact = source_service
        .create_backup(Some("package-secret".into()))
        .await
        .unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, archive.path()).unwrap();
    let inspect = source_service
        .inspect_backup(archive.path(), Some("package-secret".into()))
        .await
        .unwrap();
    assert!(inspect.manifest.format_version.is_bundle_v2());
    assert_eq!(inspect.manifest.managed_packages.len(), 1);

    let target_root = tempfile::tempdir().unwrap();
    let mut target_db = open_temp_db();
    let mut target_config = source_config;
    target_config.data_dir = target_root.path().to_string_lossy().into_owned();
    target_config.intermediate_dir = None;
    target_config.complete_dir = None;
    let (target_service, _, _) = build_service(target_db.clone(), target_config);
    let report = target_service
        .restore_backup(
            archive.path(),
            Some("package-secret".into()),
            RestoreOptions {
                data_dir: target_root.path().to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: target_root
                        .path()
                        .join("complete/movies")
                        .to_string_lossy()
                        .into_owned(),
                }],
            },
        )
        .await
        .unwrap();

    assert!(report.staged);
    assert!(report.restart_required);
    assert!(!report.restored);
    assert!(report.warnings.is_empty());
    let (restored_db, outcome) = apply_pending_restore(target_db, target_root.path()).unwrap();
    target_db = restored_db;
    let outcome = outcome.unwrap();
    assert_eq!(outcome.managed_packages_restored, 1);
    let restored = target_db.list_extension_revisions().unwrap();
    let restored_path = PathBuf::from(restored[0].managed_path.as_deref().unwrap());
    assert!(restored_path.starts_with(target_root.path()));
    assert_eq!(hash_package(&restored_path).unwrap(), source_digest);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_ne!(
            std::fs::metadata(restored_path.join("email.py"))
                .unwrap()
                .permissions()
                .mode()
                & 0o100,
            0
        );
    }
    let restored_config = target_db.load_config().unwrap();
    assert!(!restored_config.watch_folder.automatic_scanning_enabled());
    assert!(restored_config.watch_folder.normalized_path().is_none());
    let expected_approved_root = target_root.path().join("approved").display().to_string();
    assert_eq!(
        target_db.post_processing_settings().unwrap().allowed_roots,
        vec![expected_approved_root.clone()]
    );
    let restored_profile = target_db
        .post_processing_profile(&profile_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        restored_profile.profile.steps()[0]
            .approved_roots()
            .as_slice()[0]
            .as_str(),
        expected_approved_root
    );
    assert_eq!(
        target_db
            .resolve_post_processing_plan(None, Some("tv"))
            .unwrap()
            .steps()
            .len(),
        1
    );
    let restored_plan = target_db.frozen_post_processing_plan(41).unwrap().unwrap();
    assert_eq!(
        restored_plan.steps()[0].approved_roots().as_slice()[0].as_str(),
        expected_approved_root
    );
    assert!(target_db.post_processing_run(&run_id).unwrap().is_some());
    assert!(
        target_db
            .post_processing_run(&queued_run_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        target_db.post_processing_attempts(&run_id).unwrap().len(),
        1
    );
    assert_eq!(
        target_db
            .post_processing_logs(&attempt_id, None, 10)
            .unwrap()
            .chunks
            .len(),
        1
    );

    let reuse_db = open_temp_db();
    let mut reuse_config = sample_config();
    reuse_config.data_dir = target_root.path().to_string_lossy().into_owned();
    let (reuse_service, _, _) = build_service(reuse_db.clone(), reuse_config);
    reuse_service
        .restore_backup(
            archive.path(),
            Some("package-secret".into()),
            RestoreOptions {
                data_dir: target_root.path().to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: target_root
                        .path()
                        .join("complete/movies")
                        .to_string_lossy()
                        .into_owned(),
                }],
            },
        )
        .await
        .unwrap();
    let (_restored_reuse_db, reuse_outcome) =
        apply_pending_restore(reuse_db, target_root.path()).unwrap();
    let reuse_outcome = reuse_outcome.unwrap();
    assert_eq!(reuse_outcome.managed_packages_restored, 0);
    assert_eq!(hash_package(&restored_path).unwrap(), source_digest);

    if let Some((mut postgres_db, admin, schema)) =
        open_postgres_backup_test_db("managed_packages").await
    {
        let postgres_root = tempfile::tempdir().unwrap();
        let mut postgres_config = sample_config();
        postgres_config.data_dir = postgres_root.path().to_string_lossy().into_owned();
        let (postgres_service, _, _) = build_service(postgres_db.clone(), postgres_config);
        postgres_service
            .restore_backup(
                archive.path(),
                Some("package-secret".into()),
                RestoreOptions {
                    data_dir: postgres_root.path().to_string_lossy().into_owned(),
                    intermediate_dir: None,
                    complete_dir: None,
                    category_remaps: vec![CategoryRemapInput {
                        category_name: "movies".into(),
                        new_dest_dir: postgres_root
                            .path()
                            .join("complete/movies")
                            .to_string_lossy()
                            .into_owned(),
                    }],
                },
            )
            .await
            .unwrap();
        let (restored_postgres_db, outcome) =
            apply_pending_restore(postgres_db, postgres_root.path()).unwrap();
        postgres_db = restored_postgres_db;
        outcome.unwrap();
        assert!(
            postgres_db
                .post_processing_profile(&profile_id)
                .unwrap()
                .is_some()
        );
        assert!(
            postgres_db
                .frozen_post_processing_plan(41)
                .unwrap()
                .is_some()
        );
        assert!(postgres_db.post_processing_run(&run_id).unwrap().is_some());
        assert!(
            postgres_db
                .post_processing_run(&queued_run_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            postgres_db.post_processing_attempts(&run_id).unwrap().len(),
            1
        );
        assert_eq!(
            postgres_db
                .post_processing_logs(&attempt_id, None, 10)
                .unwrap()
                .chunks
                .len(),
            1
        );
        drop(postgres_service);
        drop(postgres_db);
        drop_postgres_backup_test_schema(admin, schema).await;
    }
}

#[tokio::test]
async fn backup_rejects_a_managed_package_changed_after_approval() {
    let source_root = tempfile::tempdir().unwrap();
    let mut config = sample_config();
    config.data_dir = source_root.path().to_string_lossy().into_owned();
    let source_db = open_temp_db();
    source_db.save_config(&config).unwrap();
    let managed = add_managed_extension(&source_db, source_root.path());
    std::fs::write(managed.join("email.py"), "tampered\n").unwrap();
    let (service, _, _) = build_service(source_db, config);

    assert!(matches!(
        service.create_backup(Some("backup-secret".into())).await,
        Err(BackupServiceError::Validation(_))
    ));
}

#[tokio::test]
async fn restore_rejects_manifest_inventory_that_omits_database_references() {
    let source_root = tempfile::tempdir().unwrap();
    let mut config = sample_config();
    config.data_dir = source_root.path().to_string_lossy().into_owned();
    let source_db = open_temp_db();
    source_db.save_config(&config).unwrap();
    add_managed_extension(&source_db, source_root.path());
    let (service, _, _) = build_service(source_db, config);
    let artifact = service
        .create_backup(Some("backup-secret".into()))
        .await
        .unwrap();

    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, original.path()).unwrap();
    let unpacked = tempfile::tempdir().unwrap();
    let mut manifest = archive::unpack_bundle_archive(
        original.path(),
        unpacked.path(),
        Some("backup-secret".into()),
    )
    .unwrap();
    manifest.managed_packages.clear();
    std::fs::write(
        unpacked.path().join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let tampered = tempfile::NamedTempFile::new().unwrap();
    archive::write_bundle_archive(tampered.path(), "backup-secret", unpacked.path(), &[]).unwrap();

    assert!(matches!(
        service
            .inspect_backup(tampered.path(), Some("backup-secret".into()))
            .await,
        Err(BackupServiceError::Validation(message))
            if message.contains("inventory")
    ));
}

#[tokio::test]
async fn restore_removes_new_packages_when_database_import_fails() {
    let source_root = tempfile::tempdir().unwrap();
    let mut source_config = sample_config();
    source_config.data_dir = source_root.path().to_string_lossy().into_owned();
    for category in &mut source_config.categories {
        category.dest_dir = Some(
            source_root
                .path()
                .join("complete")
                .join(&category.name)
                .to_string_lossy()
                .into_owned(),
        );
    }
    let source_db = open_temp_db();
    source_db.save_config(&source_config).unwrap();
    let package = add_managed_extension(&source_db, source_root.path());
    let digest = hash_package(&package).unwrap();
    let (source_service, _, _) = build_service(source_db, source_config.clone());
    let artifact = source_service
        .create_backup(Some("backup-secret".into()))
        .await
        .unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, archive.path()).unwrap();

    let target_root = tempfile::tempdir().unwrap();
    let target_db_path = target_root.path().join("target.db");
    let target_db = Database::open(&target_db_path).unwrap();

    let mut target_config = source_config;
    target_config.data_dir = target_root.path().to_string_lossy().into_owned();
    let (target_service, _, _) = build_service(target_db.clone(), target_config);
    target_service
        .restore_backup(
            archive.path(),
            Some("backup-secret".into()),
            RestoreOptions {
                data_dir: target_root.path().to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: ["tv", "movies"]
                    .into_iter()
                    .map(|category_name| CategoryRemapInput {
                        category_name: category_name.into(),
                        new_dest_dir: target_root
                            .path()
                            .join("complete")
                            .join(category_name)
                            .to_string_lossy()
                            .into_owned(),
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
    super::pending::set_restore_test_fail_database_import(true);
    assert!(apply_pending_restore(target_db, target_root.path()).is_err());
    let pending = super::pending::pending_restore_status(target_root.path()).unwrap();
    assert!(pending.error.is_some());

    let package_path = target_root
        .path()
        .join("managed-extensions/blake3")
        .join(digest.as_str().trim_start_matches("blake3:"));
    assert!(!package_path.exists());
}

#[tokio::test]
async fn legacy_v1_restore_skips_post_processing_state_and_reports_a_warning() {
    use sqlx::Connection as _;

    let source_root = tempfile::tempdir().unwrap();
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let source_config = sample_config();
    source_db.save_config(&source_config).unwrap();
    add_managed_extension(&source_db, source_root.path());
    let exported_db = source_root.path().join("backup.db");
    source_db.export_stable_state(&exported_db).unwrap();
    let mut exported_connection = sqlx::sqlite::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&exported_db)
            .create_if_missing(false),
    )
    .await
    .unwrap();
    let exported_history: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_history")
        .fetch_one(&mut exported_connection)
        .await
        .unwrap();
    assert_eq!(exported_history, 1);
    exported_connection.close().await.unwrap();
    let manifest: BackupManifest = serde_json::from_slice(&plain_test_manifest()).unwrap();
    let archive_path = source_root.path().join("legacy.tar.zst");
    archive::write_plain_archive(&archive_path, &manifest, &exported_db, &[]).unwrap();
    let encrypted_archive_path = source_root.path().join("legacy.enc");
    archive::encrypt_archive(&archive_path, &encrypted_archive_path, "legacy-secret").unwrap();

    let target_root = tempfile::tempdir().unwrap();
    let mut target_db = open_temp_db();
    let mut target_config = source_config;
    target_config.data_dir = target_root.path().to_string_lossy().into_owned();
    target_config.intermediate_dir = None;
    target_config.complete_dir = None;
    let (target_service, _, _) = build_service(target_db.clone(), target_config);
    let report = target_service
        .restore_backup(
            &encrypted_archive_path,
            Some("legacy-secret".into()),
            RestoreOptions {
                data_dir: target_root.path().to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: target_root
                        .path()
                        .join("complete/movies")
                        .to_string_lossy()
                        .into_owned(),
                }],
            },
        )
        .await
        .unwrap();

    assert_eq!(report.managed_packages_restored, 0);
    assert_eq!(report.warnings.len(), 1);
    let (restored_target_db, outcome) =
        apply_pending_restore(target_db, target_root.path()).unwrap();
    target_db = restored_target_db;
    outcome.unwrap();
    assert!(target_db.list_extension_revisions().unwrap().is_empty());
    let restored_history = target_db
        .list_job_history(&HistoryFilter::default())
        .unwrap();
    assert_eq!(
        restored_history[0].output_dir.as_deref(),
        Some(
            target_root
                .path()
                .join("complete/tv/fixture")
                .to_string_lossy()
                .as_ref()
        )
    );
}

#[tokio::test]
async fn plaintext_archive_cannot_claim_the_bundle_v2_format() {
    let root = tempfile::tempdir().unwrap();
    let source_db = Database::open(&root.path().join("source.db")).unwrap();
    source_db.save_config(&sample_config()).unwrap();
    let exported_db = root.path().join("backup.db");
    source_db.export_stable_state(&exported_db).unwrap();
    let mut manifest: BackupManifest = serde_json::from_slice(&plain_test_manifest()).unwrap();
    manifest.format_version =
        super::manifest::BackupFormatVersion::Named(super::manifest::BACKUP_FORMAT_VERSION.into());
    let archive_path = root.path().join("ambiguous-v2.tar.zst");
    archive::write_plain_archive(&archive_path, &manifest, &exported_db, &[]).unwrap();

    let target_db = open_temp_db();
    let (service, _, _) = build_service(target_db, sample_config());
    let error = service
        .inspect_backup(&archive_path, None)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BackupServiceError::UnsupportedFormat(ref format)
            if format == super::manifest::BACKUP_FORMAT_VERSION
    ));
}

#[tokio::test]
async fn new_backup_requires_a_nonblank_password() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (service, _, _) = build_service(source_db, sample_config());

    assert!(matches!(
        service.create_backup(None).await,
        Err(BackupServiceError::PasswordRequired)
    ));
    assert!(matches!(
        service.create_backup(Some("   ".into())).await,
        Err(BackupServiceError::PasswordRequired)
    ));
}

#[tokio::test]
async fn backup_flushes_live_server_download_usage_before_export() {
    let source_db = open_temp_db();
    let mut config = sample_config();
    config.servers = vec![sample_server(7)];
    source_db.save_config(&config).unwrap();
    let (service, _, transfer_policy) = build_service(source_db.clone(), config);

    let control = transfer_policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(7));
    let mut permit = control.try_reserve(0).unwrap();
    permit.record_blocking(4_321);
    drop(permit);
    assert!(
        source_db
            .server_download_usage(7)
            .unwrap()
            .is_none_or(|usage| usage.lifetime_bytes == 0)
    );

    let artifact = service
        .create_backup(Some("backup-secret".into()))
        .await
        .unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, archive.path()).unwrap();
    let unpacked = tempfile::tempdir().unwrap();
    archive::unpack_bundle_archive(
        archive.path(),
        unpacked.path(),
        Some("backup-secret".into()),
    )
    .unwrap();
    let rows = logical::read_table_objects(unpacked.path(), "server_download_usage").unwrap();
    let usage = rows
        .iter()
        .find(|row| row.get("server_id").and_then(serde_json::Value::as_i64) == Some(7))
        .unwrap();
    assert_eq!(
        usage
            .get("lifetime_bytes")
            .and_then(serde_json::Value::as_i64),
        Some(4_321)
    );
}

fn write_plain_test_archive(path: &Path, entries: &[(&str, &[u8])]) {
    let file = File::create(path).unwrap();
    let encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
    let mut tar = tar::Builder::new(encoder);
    for (name, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, *name, *bytes).unwrap();
    }
    let encoder = tar.into_inner().unwrap();
    encoder.finish().unwrap();
}

fn write_plain_test_archive_with_extra_header(path: &Path, header: &tar::Header) {
    let file = File::create(path).unwrap();
    let encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
    let mut tar = tar::Builder::new(encoder);
    for (name, bytes) in [
        ("manifest.json", plain_test_manifest()),
        ("backup.db", b"sqlite".to_vec()),
    ] {
        let mut regular = tar::Header::new_gnu();
        regular.set_size(bytes.len() as u64);
        regular.set_mode(0o644);
        regular.set_cksum();
        tar.append_data(&mut regular, name, bytes.as_slice())
            .unwrap();
    }
    tar.append(header, std::io::empty()).unwrap();
    let encoder = tar.into_inner().unwrap();
    encoder.finish().unwrap();
}

fn plain_test_manifest() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "format_version": 1,
        "scope": BACKUP_SCOPE,
        "created_at_epoch_ms": 0,
        "weaver_schema_version": 0,
        "included_tables": [],
        "source_paths": {
            "data_dir": "/data",
            "intermediate_dir": "/data/intermediate",
            "complete_dir": "/data/complete"
        },
        "encrypted": false,
        "managed_packages": [],
        "notes": []
    }))
    .unwrap()
}

#[test]
fn unpack_plain_archive_rejects_unexpected_entries() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("bad.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();

    let manifest = plain_test_manifest();
    write_plain_test_archive(
        &archive_path,
        &[
            ("manifest.json", manifest.as_slice()),
            ("backup.db", b"sqlite".as_slice()),
            ("extra.txt", b"nope".as_slice()),
        ],
    );

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();
    assert!(error.to_string().contains("unexpected entry extra.txt"));
    assert!(!output_dir.join("extra.txt").exists());
}

#[test]
fn unpack_plain_archive_rejects_duplicate_entries() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("duplicate.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();
    let manifest = plain_test_manifest();
    write_plain_test_archive(
        &archive_path,
        &[
            ("manifest.json", manifest.as_slice()),
            ("backup.db", b"first".as_slice()),
            ("backup.db", b"second".as_slice()),
        ],
    );

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();
    assert!(error.to_string().contains("duplicate path"));
}

#[test]
fn unpack_plain_archive_rejects_links() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("link.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_path("managed-link").unwrap();
    header.set_link_name("backup.db").unwrap();
    header.set_size(0);
    header.set_mode(0o777);
    header.set_cksum();
    write_plain_test_archive_with_extra_header(&archive_path, &header);

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();
    assert!(error.to_string().contains("unsupported entry"));
    assert!(!output_dir.join("managed-link").exists());
}

#[test]
fn unpack_plain_archive_rejects_traversal() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("traversal.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(0);
    header.set_mode(0o644);
    header.as_mut_bytes()[..9].copy_from_slice(b"../escape");
    header.set_cksum();
    write_plain_test_archive_with_extra_header(&archive_path, &header);

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();
    assert!(error.to_string().contains("unsafe or duplicate path"));
    assert!(!tempdir.path().join("escape").exists());
}

#[test]
fn unpack_plain_archive_requires_manifest() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("missing-manifest.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();
    write_plain_test_archive(&archive_path, &[("backup.db", b"sqlite".as_slice())]);

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();

    assert!(error.to_string().contains("missing manifest.json"));
}

#[test]
fn unpack_plain_archive_requires_backup_db() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("missing-db.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();
    let manifest = plain_test_manifest();
    write_plain_test_archive(&archive_path, &[("manifest.json", manifest.as_slice())]);

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();

    assert!(error.to_string().contains("missing backup.db"));
}

#[tokio::test]
async fn restore_requires_category_remap_for_external_paths() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (source_service, _, _) = build_service(source_db, sample_config());

    let artifact = source_service
        .create_backup(Some("backup-secret".into()))
        .await
        .unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, archive.path()).unwrap();

    let target_db = open_temp_db();
    target_db
        .save_config(&Config {
            data_dir: "/bootstrap".into(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            cleanup_after_extract: Some(true),
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        })
        .unwrap();

    let (target_service, _, _) = build_service(
        target_db.clone(),
        Config {
            data_dir: "/bootstrap".into(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            cleanup_after_extract: Some(true),
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        },
    );

    let err = target_service
        .restore_backup(
            archive.path(),
            Some("backup-secret".into()),
            RestoreOptions {
                data_dir: "/new/data".into(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![],
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        BackupServiceError::MissingCategoryRemaps(ref names) if names == "movies"
    ));
    assert_eq!(target_db.load_config().unwrap().data_dir, "/bootstrap");
    assert!(
        target_db
            .list_job_history(&HistoryFilter::default())
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn logical_restore_persists_selected_paths_when_source_used_defaults() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let mut source_config = sample_config();
    source_config.intermediate_dir = None;
    source_config.complete_dir = None;
    source_db.save_config(&source_config).unwrap();
    let (source_service, _, _) = build_service(source_db, source_config);
    let artifact = source_service
        .create_backup(Some("path-secret".into()))
        .await
        .unwrap();

    let target_root = tempfile::tempdir().unwrap();
    let restored_root = target_root.path().join("restored");
    let intermediate = target_root.path().join("custom-work");
    let complete = target_root.path().join("custom-complete");
    let target_db = open_temp_db();
    let mut target_config = sample_config();
    target_config.data_dir = target_root.path().to_string_lossy().into_owned();
    let (target_service, _, _) = build_service(target_db.clone(), target_config);
    target_service
        .restore_backup(
            &artifact.path,
            Some("path-secret".into()),
            RestoreOptions {
                data_dir: restored_root.to_string_lossy().into_owned(),
                intermediate_dir: Some(intermediate.to_string_lossy().into_owned()),
                complete_dir: Some(complete.to_string_lossy().into_owned()),
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: target_root
                        .path()
                        .join("movies")
                        .to_string_lossy()
                        .into_owned(),
                }],
            },
        )
        .await
        .unwrap();
    drop(target_service);

    let (restored_db, outcome) = apply_pending_restore(target_db, target_root.path()).unwrap();
    assert!(outcome.is_some());
    let restored = restored_db.load_config().unwrap();
    assert_eq!(restored.intermediate_dir.as_deref(), intermediate.to_str());
    assert_eq!(restored.complete_dir.as_deref(), complete.to_str());
    assert_eq!(
        restored
            .categories
            .iter()
            .find(|category| category.name == "tv")
            .and_then(|category| category.dest_dir.as_deref()),
        complete.join("tv").to_str()
    );
}

#[tokio::test]
async fn staged_restore_rewrites_paths_without_mutating_the_live_runtime() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let mut source_config = sample_config();
    source_config.servers = vec![sample_server(7), sample_server(8)];
    source_db.save_config(&source_config).unwrap();
    let (source_service, _, source_policy) = build_service(source_db, source_config);
    let source_control = source_policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(7));
    let mut source_permit = source_control.try_reserve(0).unwrap();
    source_permit.record_blocking(9_000);
    drop(source_permit);
    let stray_source_control = source_policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(8));
    let mut stray_source_permit = stray_source_control.try_reserve(0).unwrap();
    stray_source_permit.record_blocking(1_000);
    drop(stray_source_permit);

    let artifact = source_service
        .create_backup(Some("backup-secret".into()))
        .await
        .unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(&artifact.path, archive.path()).unwrap();

    let target_root = tempfile::tempdir().unwrap();
    let restored_root = target_root.path().join("restored");
    let movies_root = target_root.path().join("media/movies");
    let mut bootstrap = sample_config();
    bootstrap.data_dir = target_root.path().to_string_lossy().into_owned();
    bootstrap.intermediate_dir = None;
    bootstrap.complete_dir = None;
    bootstrap.servers = vec![sample_server(7)];
    bootstrap.categories.clear();
    bootstrap.max_download_speed = None;
    let mut target_db = open_temp_db();
    target_db.save_config(&bootstrap).unwrap();

    let (target_service, capture, target_policy) = build_service(target_db.clone(), bootstrap);
    let stale_control = target_policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(7));
    let mut stale_permit = stale_control.try_reserve(0).unwrap();
    stale_permit.record_blocking(50_000);
    drop(stale_permit);
    let stray_control = target_policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(8));
    let mut stray_permit = stray_control.try_reserve(0).unwrap();
    stray_permit.record_blocking(75_000);
    drop(stray_permit);

    let report = target_service
        .restore_backup(
            archive.path(),
            Some("backup-secret".into()),
            RestoreOptions {
                data_dir: restored_root.to_string_lossy().into_owned(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: movies_root.to_string_lossy().into_owned(),
                }],
            },
        )
        .await
        .unwrap();

    assert!(report.staged);
    assert!(!report.restored);
    assert_eq!(
        target_db.load_config().unwrap().data_dir,
        target_root.path().to_string_lossy()
    );
    assert!(capture.updated_paths.lock().unwrap().is_none());
    assert!(capture.speed_limits.lock().unwrap().is_empty());
    assert_eq!(target_policy.snapshot(7).unwrap().lifetime_bytes, 50_000);

    let (restored_target_db, outcome) =
        apply_pending_restore(target_db, target_root.path()).unwrap();
    target_db = restored_target_db;
    outcome.unwrap();
    let restored = target_db.load_config().unwrap();
    assert_eq!(restored.data_dir, restored_root.to_string_lossy());
    assert_eq!(restored.complete_dir(), restored_root.join("complete"));
    let tv = restored
        .categories
        .iter()
        .find(|category| category.name == "tv")
        .unwrap();
    assert_eq!(
        tv.dest_dir.as_deref(),
        Some(restored_root.join("complete/tv").to_string_lossy().as_ref())
    );
    let movies = restored
        .categories
        .iter()
        .find(|category| category.name == "movies")
        .unwrap();
    assert_eq!(
        movies.dest_dir.as_deref(),
        Some(movies_root.to_string_lossy().as_ref())
    );
    assert_eq!(
        target_db
            .list_job_history(&HistoryFilter::default())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        target_db
            .server_download_usage(7)
            .unwrap()
            .unwrap()
            .lifetime_bytes,
        9_000
    );
    assert_eq!(
        target_db
            .server_download_usage(8)
            .unwrap()
            .unwrap()
            .lifetime_bytes,
        1_000
    );
}

#[tokio::test]
async fn backup_catalog_rejects_an_unclassified_application_table() {
    use sqlx::Connection as _;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("catalog.db");
    let db = Database::open(&path).unwrap();
    db.validate_backup_catalog().unwrap();

    let mut connection = sqlx::sqlite::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false),
    )
    .await
    .unwrap();
    sqlx::query("CREATE TABLE unclassified_test_table (id INTEGER PRIMARY KEY)")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let error = db.validate_backup_catalog().unwrap_err();
    assert!(error.to_string().contains("unclassified_test_table"));
}

#[tokio::test]
async fn sqlite_restore_resumes_after_every_promotion_phase() {
    use super::pending::{PromotionPhase, set_restore_test_fail_after};

    let source_packages = tempfile::tempdir().unwrap();
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let source_package = add_managed_extension(&source_db, source_packages.path());
    let source_package_digest = hash_package(&source_package).unwrap();
    let (source_service, _, _) = build_service(source_db, sample_config());
    let artifact = source_service
        .create_backup(Some("crash-secret".into()))
        .await
        .unwrap();

    for phase in [
        PromotionPhase::Validated,
        PromotionPhase::DatabasePrepared,
        PromotionPhase::DatabaseInstalled,
        PromotionPhase::KeyPromoted,
    ] {
        let current_root = tempfile::tempdir().unwrap();
        let restored_root = tempfile::tempdir().unwrap();
        let target_path = current_root.path().join("weaver.db");
        let target_db = Database::open(&target_path).unwrap();
        let mut target_config = sample_config();
        let current_data_dir = current_root.path().join("custom-data");
        target_config.data_dir = current_data_dir.to_string_lossy().into_owned();
        target_db.save_config(&target_config).unwrap();
        let (target_service, _, _) = build_service(target_db.clone(), target_config);
        target_service
            .restore_backup(
                &artifact.path,
                Some("crash-secret".into()),
                RestoreOptions {
                    data_dir: restored_root.path().to_string_lossy().into_owned(),
                    intermediate_dir: None,
                    complete_dir: None,
                    category_remaps: vec![CategoryRemapInput {
                        category_name: "movies".into(),
                        new_dest_dir: restored_root
                            .path()
                            .join("complete/movies")
                            .to_string_lossy()
                            .into_owned(),
                    }],
                },
            )
            .await
            .unwrap();
        drop(target_service);
        assert!(
            current_root
                .path()
                .join("restore-pending-location")
                .is_file()
        );

        set_restore_test_fail_after(Some(phase));
        let error = apply_pending_restore(target_db, current_root.path())
            .err()
            .unwrap_or_else(|| panic!("restore failpoint should interrupt after {phase:?}"));
        assert!(error.to_string().contains("injected restore interruption"));
        let pending = super::pending::pending_restore_status(current_root.path()).unwrap();
        assert!(pending.error.is_some());

        let reopened = Database::open(&target_path).unwrap();
        let (restored, outcome) = apply_pending_restore(reopened, current_root.path()).unwrap();
        assert_eq!(outcome.unwrap().managed_packages_restored, 1);
        assert_eq!(
            restored.load_config().unwrap().data_dir,
            restored_root.path().to_string_lossy()
        );
        assert_eq!(
            restored
                .list_job_history(&HistoryFilter::default())
                .unwrap()
                .len(),
            1,
            "restore did not recover after {phase:?}"
        );
        let restored_package = restored
            .list_extension_revisions()
            .unwrap()
            .into_iter()
            .find_map(|revision| revision.managed_path.map(PathBuf::from))
            .expect("managed revision should survive crash recovery");
        assert_eq!(
            hash_package(&restored_package).unwrap(),
            source_package_digest
        );
        assert!(!current_data_dir.join("restore-pending").exists());
        assert!(
            !current_root
                .path()
                .join("restore-pending-location")
                .exists()
        );
        assert!(
            !restored_root
                .path()
                .join("restore-pending-location")
                .exists()
        );
    }
}

#[tokio::test]
async fn sqlite_restore_distinguishes_an_installed_database_from_a_lost_prepared_file() {
    use super::pending::{PromotionPhase, set_restore_test_fail_after};

    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (source_service, _, _) = build_service(source_db, sample_config());
    let artifact = source_service
        .create_backup(Some("marker-secret".into()))
        .await
        .unwrap();

    for installed_before_journal in [false, true] {
        let current_root = tempfile::tempdir().unwrap();
        let restored_root = tempfile::tempdir().unwrap();
        let target_path = current_root.path().join("weaver.db");
        let target_db = Database::open(&target_path).unwrap();
        let mut target_config = sample_config();
        let current_data_dir = current_root.path().join("custom-data");
        target_config.data_dir = current_data_dir.to_string_lossy().into_owned();
        target_db.save_config(&target_config).unwrap();
        let (target_service, _, _) = build_service(target_db.clone(), target_config);
        target_service
            .restore_backup(
                &artifact.path,
                Some("marker-secret".into()),
                RestoreOptions {
                    data_dir: restored_root.path().to_string_lossy().into_owned(),
                    intermediate_dir: None,
                    complete_dir: None,
                    category_remaps: vec![CategoryRemapInput {
                        category_name: "movies".into(),
                        new_dest_dir: restored_root
                            .path()
                            .join("complete/movies")
                            .to_string_lossy()
                            .into_owned(),
                    }],
                },
            )
            .await
            .unwrap();
        drop(target_service);

        set_restore_test_fail_after(Some(PromotionPhase::DatabasePrepared));
        assert!(apply_pending_restore(target_db, current_root.path()).is_err());
        let pending = super::pending::pending_restore_status(current_root.path()).unwrap();
        let prepared = current_root.path().join(format!(
            ".weaver-restore-{}.prepared.db",
            pending.restore_id
        ));
        assert!(prepared.is_file());

        if installed_before_journal {
            for suffix in ["-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", target_path.display()));
            }
            std::fs::remove_file(&target_path).unwrap();
            std::fs::rename(&prepared, &target_path).unwrap();

            let reopened = Database::open(&target_path).unwrap();
            let (restored, outcome) = apply_pending_restore(reopened, current_root.path()).unwrap();
            assert!(outcome.is_some());
            assert_eq!(
                restored.load_config().unwrap().data_dir,
                restored_root.path().to_string_lossy()
            );
        } else {
            std::fs::remove_file(&prepared).unwrap();
            let reopened = Database::open(&target_path).unwrap();
            let error = match apply_pending_restore(reopened, current_root.path()) {
                Ok(_) => panic!("lost prepared database must not be treated as installed"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("disappeared before it was installed"),
                "{error:?}"
            );
            let original = Database::open(&target_path).unwrap();
            assert_eq!(
                original.load_config().unwrap().data_dir,
                current_data_dir.to_string_lossy()
            );
            assert!(!restored_root.path().join("encryption.key").exists());
        }
    }
}
