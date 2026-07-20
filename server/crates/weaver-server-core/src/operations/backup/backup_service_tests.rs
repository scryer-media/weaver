use super::*;
use std::path::Path;
use std::sync::{Arc as StdArc, Mutex as StdMutex};

use crate::JobHistoryRow;
use crate::categories::CategoryConfig;
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

fn build_service(
    db: Database,
    config: Config,
) -> (
    BackupService,
    RuntimeCapture,
    StdArc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
) {
    let capture = RuntimeCapture::default();
    let transfer_policy = StdArc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            db.clone(),
            &config.servers,
        )
        .unwrap(),
    );
    let shared_config = StdArc::new(RwLock::new(config));
    let handle = test_scheduler_handle(capture.clone());
    handle.set_server_transfer_policy(StdArc::clone(&transfer_policy));
    let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
    (
        BackupService::new(handle, shared_config, db, rss),
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
    assert!(artifact.filename.ends_with(".age"));

    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), artifact.bytes).unwrap();

    let inspect = service
        .inspect_backup(temp.path(), Some("secret-pass".into()))
        .await
        .unwrap();
    assert_eq!(inspect.manifest.scope, BACKUP_SCOPE);
    assert_eq!(inspect.required_category_remaps.len(), 1);
    assert_eq!(inspect.required_category_remaps[0].category_name, "movies");
}

#[tokio::test]
async fn plain_backup_contains_manifest_and_database() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (service, _, _) = build_service(source_db, sample_config());

    let artifact = service.create_backup(None).await.unwrap();
    assert!(artifact.filename.ends_with(".tar.zst"));

    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(archive.path(), artifact.bytes).unwrap();

    let mut entries = Vec::new();
    let file = File::open(archive.path()).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut tar = tar::Archive::new(decoder);
    for entry in tar.entries().unwrap() {
        let entry = entry.unwrap();
        entries.push(entry.path().unwrap().to_string_lossy().to_string());
    }
    entries.sort();

    assert_eq!(
        entries,
        vec!["backup.db".to_string(), "manifest.json".to_string()]
    );
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

    let artifact = service.create_backup(None).await.unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(archive.path(), artifact.bytes).unwrap();
    let unpacked = tempfile::tempdir().unwrap();
    archive::unpack_plain_archive(archive.path(), unpacked.path()).unwrap();

    let exported = Database::open(&unpacked.path().join("backup.db")).unwrap();
    let usage = exported.server_download_usage(7).unwrap().unwrap();
    assert_eq!(usage.lifetime_bytes, 4_321);
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

#[test]
fn unpack_plain_archive_rejects_unexpected_entries() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive_path = tempdir.path().join("bad.tar.zst");
    let output_dir = tempdir.path().join("out");
    std::fs::create_dir(&output_dir).unwrap();

    write_plain_test_archive(
        &archive_path,
        &[
            ("manifest.json", b"{}".as_slice()),
            ("backup.db", b"sqlite".as_slice()),
            ("extra.txt", b"nope".as_slice()),
        ],
    );

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();
    assert!(error.to_string().contains("unexpected entry extra.txt"));
    assert!(!output_dir.join("extra.txt").exists());
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
    write_plain_test_archive(&archive_path, &[("manifest.json", b"{}".as_slice())]);

    let error = archive::unpack_plain_archive(&archive_path, &output_dir).unwrap_err();

    assert!(error.to_string().contains("missing backup.db"));
}

#[tokio::test]
async fn restore_requires_category_remap_for_external_paths() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (source_service, _, _) = build_service(source_db, sample_config());

    let artifact = source_service.create_backup(None).await.unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(archive.path(), artifact.bytes).unwrap();

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
            None,
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
async fn restore_rewrites_paths_and_refreshes_runtime() {
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

    let artifact = source_service.create_backup(None).await.unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(archive.path(), artifact.bytes).unwrap();

    let target_db = open_temp_db();
    target_db
        .save_config(&Config {
            data_dir: "/bootstrap".into(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![sample_server(7)],
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

    let (target_service, capture, target_policy) = build_service(
        target_db.clone(),
        Config {
            data_dir: "/bootstrap".into(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![sample_server(7)],
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
            None,
            RestoreOptions {
                data_dir: "/new/data".into(),
                intermediate_dir: None,
                complete_dir: None,
                category_remaps: vec![CategoryRemapInput {
                    category_name: "movies".into(),
                    new_dest_dir: "/srv/media/movies".into(),
                }],
            },
        )
        .await
        .unwrap();

    assert!(report.restored);
    let restored = target_db.load_config().unwrap();
    assert_eq!(restored.data_dir, "/new/data");
    assert_eq!(restored.complete_dir(), "/new/data/complete");
    let tv = restored
        .categories
        .iter()
        .find(|category| category.name == "tv")
        .unwrap();
    assert_eq!(tv.dest_dir.as_deref(), Some("/new/data/complete/tv"));
    let movies = restored
        .categories
        .iter()
        .find(|category| category.name == "movies")
        .unwrap();
    assert_eq!(movies.dest_dir.as_deref(), Some("/srv/media/movies"));
    assert_eq!(
        target_db
            .list_job_history(&HistoryFilter::default())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        target_policy.snapshot(7).unwrap().lifetime_bytes,
        9_000,
        "imported usage must replace stale live counters for the same server ID"
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
        target_policy.snapshot(8).unwrap().lifetime_bytes,
        1_000,
        "restore must clear stray controls that were absent from the old policy map"
    );
    assert_eq!(
        target_db
            .server_download_usage(8)
            .unwrap()
            .unwrap()
            .lifetime_bytes,
        1_000
    );

    let updated = capture.updated_paths.lock().unwrap().clone().unwrap();
    assert_eq!(updated.0, Path::new("/new/data"));
    assert_eq!(updated.1, Path::new("/new/data/intermediate"));
    assert_eq!(updated.2, Path::new("/new/data/complete"));
    assert_eq!(
        capture.speed_limits.lock().unwrap().last().copied(),
        Some(1234)
    );
}
