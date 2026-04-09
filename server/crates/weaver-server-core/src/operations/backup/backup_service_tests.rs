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
    let dir = tempfile::tempdir().unwrap();
    Database::open(&dir.path().join("weaver.db")).unwrap()
}

fn build_service(db: Database, config: Config) -> (BackupService, RuntimeCapture) {
    let capture = RuntimeCapture::default();
    let shared_config = StdArc::new(RwLock::new(config));
    let handle = test_scheduler_handle(capture.clone());
    let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
    (BackupService::new(handle, shared_config, db, rss), capture)
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
                    let _ = reply.send(());
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
        cleanup_after_extract: Some(true),
        config_path: None,
    }
}

fn populate_source_db(db: &Database) {
    db.save_config(&sample_config()).unwrap();
    db.insert_job_history(&JobHistoryRow {
        job_id: 41,
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
    let (service, _) = build_service(source_db, sample_config());

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
    let (service, _) = build_service(source_db, sample_config());

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
async fn restore_requires_category_remap_for_external_paths() {
    let source_db = open_temp_db();
    populate_source_db(&source_db);
    let (source_service, _) = build_service(source_db, sample_config());

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
            cleanup_after_extract: Some(true),
            config_path: None,
        })
        .unwrap();

    let (target_service, _) = build_service(
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
            cleanup_after_extract: Some(true),
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
    let (source_service, _) = build_service(source_db, sample_config());

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
            cleanup_after_extract: Some(true),
            config_path: None,
        })
        .unwrap();

    let (target_service, capture) = build_service(
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
            cleanup_after_extract: Some(true),
            config_path: None,
        },
    );

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

    let updated = capture.updated_paths.lock().unwrap().clone().unwrap();
    assert_eq!(updated.0, Path::new("/new/data"));
    assert_eq!(updated.1, Path::new("/new/data/intermediate"));
    assert_eq!(updated.2, Path::new("/new/data/complete"));
    assert_eq!(
        capture.speed_limits.lock().unwrap().last().copied(),
        Some(1234)
    );
}
