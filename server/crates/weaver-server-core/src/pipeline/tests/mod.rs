use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::Database;
use crate::ingest::submit_nzb_bytes;
use crate::jobs::FileIdentitySource;
use crate::jobs::ids::MessageId;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::runtime::buffers::{BufferPool, BufferPoolConfig};
use crate::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use crate::settings::{Config, SharedConfig};
use crate::{
    FileSpec, JobInfo, PipelineMetrics, SchedulerHandle, SegmentSpec, SharedPipelineState,
};
use chrono::Timelike;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::sync::{RwLock, oneshot};
use weaver_model::files::FileRole;
use weaver_nntp::client::{NntpClient, NntpClientConfig};

macro_rules! segment_spec {
    (number: $number:expr, bytes: $bytes:expr, message_id: $message_id:expr $(,)?) => {
        SegmentSpec {
            ordinal: $number,
            article_number: $number,
            bytes: $bytes,
            message_id: $message_id,
        }
    };
    (ordinal: $ordinal:expr, article_number: $article_number:expr, bytes: $bytes:expr, message_id: $message_id:expr $(,)?) => {
        SegmentSpec {
            ordinal: $ordinal,
            article_number: $article_number,
            bytes: $bytes,
            message_id: $message_id,
        }
    };
}

mod archive_topology;
mod core;
mod decode_and_files;
mod download_dispatch;
mod health_probe;
mod par2_completion;
mod rar_extraction;
mod restore_history;

struct TestHarness {
    _temp_dir: TempDir,
    _data_dir: PathBuf,
    handle: SchedulerHandle,
    config: SharedConfig,
    db: Database,
    pipeline_task: tokio::task::JoinHandle<()>,
}

impl TestHarness {
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let intermediate_dir = temp_dir.path().join("intermediate");
        let complete_dir = temp_dir.path().join("complete");
        let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
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
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        }));

        let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
        let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

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
            data_dir.clone(),
            intermediate_dir,
            complete_dir,
            0,
            4,
            vec![],
            false,
            shared_state,
            db.clone(),
            config.clone(),
        )
        .await
        .unwrap();
        let pipeline_task = tokio::spawn(async move {
            pipeline.run().await;
        });

        Self {
            _temp_dir: temp_dir,
            _data_dir: data_dir,
            handle,
            config,
            db,
            pipeline_task,
        }
    }

    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        self.pipeline_task.await.unwrap();
    }
}

fn sample_nzb_bytes() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Silver Horizon.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
        .to_vec()
}

fn sample_nzb_zstd() -> Vec<u8> {
    crate::ingest::compress_nzb_bytes(&sample_nzb_bytes()).unwrap()
}

fn sample_nzb_zstd_with_password(password: &str) -> Vec<u8> {
    let nzb = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <head><meta type="password">{password}</meta></head>
          <file poster="poster" date="1700000000" subject="Silver Horizon.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
    );
    crate::ingest::compress_nzb_bytes(nzb.as_bytes()).unwrap()
}

fn minimal_job_state(job_id: JobId, name: &str, working_dir: PathBuf) -> JobState {
    JobState {
        job_id,
        job_hash: [0; 32],
        spec: JobSpec {
            name: name.to_string(),
            password: None,
            files: vec![],
            total_bytes: 0,
            category: None,
            metadata: vec![],
        },
        status: JobStatus::Downloading,
        download_state: crate::jobs::model::DownloadState::Downloading,
        post_state: crate::jobs::model::PostState::Idle,
        run_state: crate::jobs::model::RunState::Active,
        assembly: JobAssembly::new(job_id),
        extraction_depth: 0,
        created_at: std::time::Instant::now(),
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
        queued_repair_at_epoch_ms: None,
        queued_extract_at_epoch_ms: None,
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        failure_error: None,
        working_dir,
        downloaded_bytes: 0,
        restored_download_floor_bytes: 0,
        failed_bytes: 0,
        par2_bytes: 0,
        health_probing: false,
        health_probe_round: 0,
        last_health_probe_failed_bytes: 0,
        next_health_probe_failed_bytes: 1,
        detected_archives: HashMap::new(),
        file_identities: HashMap::new(),
        held_segments: Vec::new(),
        download_queue: DownloadQueue::new(),
        recovery_queue: DownloadQueue::new(),
        staging_dir: None,
    }
}

fn finished_job_info(job_id: JobId) -> JobInfo {
    JobInfo {
        job_id,
        job_hash: Some([0; 32]),
        name: format!("finished-{}", job_id.0),
        error: None,
        status: JobStatus::Complete,
        download_state: crate::jobs::model::DownloadState::Complete,
        post_state: crate::jobs::model::PostState::Idle,
        run_state: crate::jobs::model::RunState::Active,
        progress: 1.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        password: None,
        category: None,
        metadata: vec![],
        output_dir: None,
        created_at_epoch_ms: job_id.0 as f64,
    }
}

fn history_row_with_output_dir(
    job_id: JobId,
    name: &str,
    status: &str,
    output_dir: PathBuf,
) -> crate::JobHistoryRow {
    crate::JobHistoryRow {
        job_id: job_id.0,
        job_hash: None,
        name: name.to_string(),
        status: status.to_string(),
        error_message: None,
        total_bytes: 1024,
        downloaded_bytes: 1024,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some(output_dir.display().to_string()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
    }
}

fn insert_history_row_with_nzb_zstd(db: &Database, row: &crate::JobHistoryRow, nzb_zstd: &[u8]) {
    db.insert_job_history(row).unwrap();
    let datastore = db.datastore();
    let job_id = row.job_id as i64;
    let nzb_zstd = nzb_zstd.to_vec();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE job_history SET nzb_zstd = {} WHERE job_id = {}",
            &[SqlArg::Bytes(nzb_zstd), SqlArg::I64(job_id)],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

async fn new_direct_pipeline_with_buffers(
    temp_dir: &TempDir,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
) -> (Pipeline, PathBuf, PathBuf) {
    let data_dir = temp_dir.path().join("data");
    let intermediate_dir = temp_dir.path().join("intermediate");
    let complete_dir = temp_dir.path().join("complete");
    let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
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
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        config_path: None,
    }));

    let (_cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
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
    let buffers = BufferPool::new(buffer_config);

    let pipeline = Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir,
        intermediate_dir.clone(),
        complete_dir.clone(),
        total_connections,
        4,
        vec![],
        false,
        shared_state,
        db,
        config,
    )
    .await
    .unwrap();

    (pipeline, intermediate_dir, complete_dir)
}

async fn new_direct_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
    new_direct_pipeline_with_buffers(
        temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        0,
    )
    .await
}

fn encode_article_part(
    filename: &str,
    payload: &[u8],
    part: u32,
    total: u32,
    begin: u64,
    file_size: u64,
) -> Bytes {
    let mut encoded = Vec::new();
    weaver_yenc::encode_part(
        payload,
        &mut encoded,
        128,
        filename,
        part,
        total,
        begin,
        begin + payload.len() as u64 - 1,
        file_size,
    )
    .unwrap();
    Bytes::from(encoded)
}

const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

fn build_test_rar_header(
    header_type: u64,
    common_flags: u64,
    type_body: &[u8],
    extra: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(header_type));

    let mut flags = common_flags;
    if !extra.is_empty() {
        flags |= 0x0001;
    }
    body.extend_from_slice(&encode_test_rar_vint(flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
    }
    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
    if let Some(volume_number) = volume_number {
        type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
    }
    build_test_rar_header(1, 0, &type_body, &[])
}

fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
    let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
    build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
}

fn build_test_rar_file_header(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
) -> Vec<u8> {
    let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
    type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
    type_body.extend_from_slice(&encode_test_rar_vint(0o644));
    if let Some(data_crc) = data_crc {
        type_body.extend_from_slice(&data_crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_test_rar_vint(0));
    type_body.extend_from_slice(&encode_test_rar_vint(1));
    type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(2));
    body.extend_from_slice(&encode_test_rar_vint(0x0002 | common_flags_extra));
    body.extend_from_slice(&encode_test_rar_vint(data_size));
    body.extend_from_slice(&type_body);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

fn build_multifile_multivolume_rar_set() -> Vec<(String, Vec<u8>)> {
    let episode_a = b"episode-a-payload";
    let episode_b = b"episode-b-payload";
    let episode_a_crc = checksum::crc32(episode_a);
    let episode_b_crc = checksum::crc32(episode_b);

    let a_part1 = &episode_a[..8];
    let a_part2 = &episode_a[8..];
    let b_part1 = &episode_b[..8];
    let b_part2 = &episode_b[8..];

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0010,
        a_part1.len() as u64,
        episode_a.len() as u64,
        None,
    ));
    part01.extend_from_slice(a_part1);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0008,
        a_part2.len() as u64,
        episode_a.len() as u64,
        Some(episode_a_crc),
    ));
    part02.extend_from_slice(a_part2);
    part02.extend_from_slice(&build_test_rar_end_header(true));

    let mut part03 = Vec::new();
    part03.extend_from_slice(&TEST_RAR5_SIG);
    part03.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
    part03.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0010,
        b_part1.len() as u64,
        episode_b.len() as u64,
        None,
    ));
    part03.extend_from_slice(b_part1);
    part03.extend_from_slice(&build_test_rar_end_header(true));

    let mut part04 = Vec::new();
    part04.extend_from_slice(&TEST_RAR5_SIG);
    part04.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(3)));
    part04.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0008,
        b_part2.len() as u64,
        episode_b.len() as u64,
        Some(episode_b_crc),
    ));
    part04.extend_from_slice(b_part2);
    part04.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("show.part01.rar".to_string(), part01),
        ("show.part02.rar".to_string(), part02),
        ("show.part03.rar".to_string(), part03),
        ("show.part04.rar".to_string(), part04),
    ]
}

fn dummy_rar_volume_facts(volume_number: u32) -> weaver_unrar::RarVolumeFacts {
    weaver_unrar::RarVolumeFacts {
        format: 5,
        volume_number,
        more_volumes: true,
        is_solid: false,
        is_encrypted: false,
        is_volume: true,
        has_recovery_record: false,
        is_locked: false,
        has_authenticity_verification: false,
        has_locator: false,
        quick_open_offset: None,
        recovery_record_offset: None,
        original_name: None,
        original_name_raw: None,
        original_creation_time_ns: None,
        members: Vec::new(),
        services: Vec::new(),
    }
}

fn dummy_named_rar_volume_facts(
    volume_number: u32,
    member_name: &str,
) -> weaver_unrar::RarVolumeFacts {
    weaver_unrar::RarVolumeFacts {
        members: vec![weaver_unrar::RarVolumeMemberFacts {
            order: 0,
            name: member_name.to_string(),
            name_raw: Some(member_name.as_bytes().to_vec()),
            unpacked_size: Some(1024),
            data_crc32: None,
            data_blake2_hash: None,
            version: None,
            packed_crc32: None,
            packed_blake2_hash: None,
            packed_hash_uses_mac: false,
            split_before: volume_number > 0,
            split_after: true,
            is_directory: false,
            is_encrypted: false,
            host_os: Some(weaver_unrar::RarVolumeHostOs::Unix),
            attributes: Some(0o644),
            owner: None,
            mtime_ns: None,
            ctime_ns: None,
            atime_ns: None,
            data_offset: 0,
            data_size: 128,
            compression_method: 0x30,
            compression_version: 29,
            compression_solid: false,
            dict_size: 0,
            use_hash_mac: false,
            redirection_type: None,
            redirection_target: None,
            redirection_target_raw: None,
            redirection_target_is_directory: false,
        }],
        ..dummy_rar_volume_facts(volume_number)
    }
}

fn rar_job_spec(name: &str, files: &[(String, Vec<u8>)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("rar-{index}@example.com"),
                }],
            })
            .collect(),
    }
}

fn standalone_job_spec(name: &str, files: &[(String, u32)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| *bytes as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: *bytes,
                    message_id: format!("segment-{index}@example.com"),
                }],
            })
            .collect(),
    }
}

fn many_standalone_files(prefix: &str, count: usize) -> Vec<(String, u32)> {
    (0..count)
        .map(|idx| (format!("{prefix}-{idx}.bin"), 512u32))
        .collect()
}

const TEST_HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT: usize = 64;
const TEST_HOT_LEASE_WARMUP_WORK_LIMIT: usize = 16;

fn standalone_with_par2_job_spec(name: &str, payload_bytes: u32, recovery_bytes: u32) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (payload_bytes + recovery_bytes + 16) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload_bytes,
                    message_id: "payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 16,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes,
                    message_id: "repair-volume@example.com".to_string(),
                }],
            },
        ],
    }
}

fn minimal_par2_file_set() -> Par2FileSet {
    Par2FileSet {
        recovery_set_id: weaver_par2::RecoverySetId::from_bytes([0; 16]),
        slice_size: 1,
        recovery_file_ids: Vec::new(),
        non_recovery_file_ids: Vec::new(),
        files: HashMap::new(),
        slice_checksums: HashMap::new(),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

fn placement_par2_file_set(files: &[(String, Vec<u8>)]) -> Par2FileSet {
    let slice_size = files
        .iter()
        .map(|(_, bytes)| bytes.len() as u64)
        .max()
        .unwrap_or(1)
        .max(1);
    let mut recovery_file_ids = Vec::new();
    let mut descriptions = HashMap::new();

    for (index, (filename, bytes)) in files.iter().enumerate() {
        let mut raw_id = [0u8; 16];
        raw_id[12..].copy_from_slice(&((index as u32) + 1).to_be_bytes());
        let file_id = weaver_par2::FileId::from_bytes(raw_id);
        let hash_full = weaver_par2::checksum::md5(bytes);
        let hash_16k = weaver_par2::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]);
        recovery_file_ids.push(file_id);
        descriptions.insert(
            file_id,
            weaver_par2::FileDescription {
                file_id,
                hash_full,
                hash_16k,
                length: bytes.len() as u64,
                par2_name: filename.clone(),
                filename: filename.clone(),
            },
        );
    }

    Par2FileSet {
        recovery_set_id: weaver_par2::RecoverySetId::from_bytes([9; 16]),
        slice_size,
        recovery_file_ids,
        non_recovery_file_ids: Vec::new(),
        files: descriptions,
        slice_checksums: HashMap::new(),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

fn install_test_par2_runtime(
    pipeline: &mut Pipeline,
    job_id: JobId,
    par2_set: Par2FileSet,
    files: &[(u32, &str, u32, bool)],
) {
    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.set = Some(Arc::new(par2_set));
    runtime.files.clear();
    for (file_index, filename, recovery_blocks, promoted) in files {
        runtime.files.insert(
            *file_index,
            Par2FileRuntime {
                filename: (*filename).to_string(),
                recovery_blocks: *recovery_blocks,
                promoted: *promoted,
            },
        );
    }
}

fn build_test_par2_packet(
    packet_type: &[u8; 16],
    body: &[u8],
    recovery_set_id: [u8; 16],
) -> Vec<u8> {
    let length = (weaver_par2::packet::header::HEADER_SIZE + body.len()) as u64;
    let mut hash_input = Vec::new();
    hash_input.extend_from_slice(&recovery_set_id);
    hash_input.extend_from_slice(packet_type);
    hash_input.extend_from_slice(body);
    let packet_hash = checksum::md5(&hash_input);

    let mut data = Vec::new();
    data.extend_from_slice(weaver_par2::packet::header::MAGIC);
    data.extend_from_slice(&length.to_le_bytes());
    data.extend_from_slice(&packet_hash);
    data.extend_from_slice(&recovery_set_id);
    data.extend_from_slice(packet_type);
    data.extend_from_slice(body);
    data
}

fn build_test_par2_index(filename: &str, file_data: &[u8], slice_size: u64) -> Vec<u8> {
    let file_length = file_data.len() as u64;
    let hash_full = checksum::md5(file_data);
    let hash_16k = checksum::md5(&file_data[..file_data.len().min(16 * 1024)]);

    let mut file_id_input = Vec::new();
    file_id_input.extend_from_slice(&hash_16k);
    file_id_input.extend_from_slice(&file_length.to_le_bytes());
    file_id_input.extend_from_slice(filename.as_bytes());
    let file_id_bytes = checksum::md5(&file_id_input);

    let num_slices = if file_length == 0 {
        0
    } else {
        file_length.div_ceil(slice_size) as usize
    };

    let mut checksums = Vec::new();
    for slice_index in 0..num_slices {
        let start = slice_index as u64 * slice_size;
        let end = ((start + slice_size) as usize).min(file_data.len());
        let slice_data = &file_data[start as usize..end];
        let mut state = weaver_par2::SliceChecksumState::new();
        state.update(slice_data);
        let (crc32, md5) =
            state.finalize(((slice_data.len() as u64) < slice_size).then_some(slice_size));
        checksums.push(weaver_par2::SliceChecksum { crc32, md5 });
    }

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&1u32.to_le_bytes());
    main_body.extend_from_slice(&file_id_bytes);
    let recovery_set_id = checksum::md5(&main_body);

    let mut file_desc_body = Vec::new();
    file_desc_body.extend_from_slice(&file_id_bytes);
    file_desc_body.extend_from_slice(&hash_full);
    file_desc_body.extend_from_slice(&hash_16k);
    file_desc_body.extend_from_slice(&file_length.to_le_bytes());
    file_desc_body.extend_from_slice(filename.as_bytes());
    while file_desc_body.len() % 4 != 0 {
        file_desc_body.push(0);
    }

    let mut ifsc_body = Vec::new();
    ifsc_body.extend_from_slice(&file_id_bytes);
    for checksum in checksums {
        ifsc_body.extend_from_slice(&checksum.md5);
        ifsc_body.extend_from_slice(&checksum.crc32.to_le_bytes());
    }

    let mut stream = Vec::new();
    stream.extend_from_slice(&build_test_par2_packet(
        weaver_par2::packet::header::TYPE_MAIN,
        &main_body,
        recovery_set_id,
    ));
    stream.extend_from_slice(&build_test_par2_packet(
        weaver_par2::packet::header::TYPE_FILE_DESC,
        &file_desc_body,
        recovery_set_id,
    ));
    stream.extend_from_slice(&build_test_par2_packet(
        weaver_par2::packet::header::TYPE_IFSC,
        &ifsc_body,
        recovery_set_id,
    ));
    stream
}

fn build_repairable_par2_set(
    filename: &str,
    file_data: &[u8],
    slice_size: u64,
    recovery_block_count: usize,
) -> Par2FileSet {
    let file_length = file_data.len() as u64;
    let hash_full = checksum::md5(file_data);
    let hash_16k = checksum::md5(&file_data[..file_data.len().min(16 * 1024)]);

    let mut file_id_input = Vec::new();
    file_id_input.extend_from_slice(&hash_16k);
    file_id_input.extend_from_slice(&file_length.to_le_bytes());
    file_id_input.extend_from_slice(filename.as_bytes());
    let file_id = weaver_par2::FileId::from_bytes(checksum::md5(&file_id_input));

    let mut slice_checksums = Vec::new();
    let slice_count = if file_length == 0 {
        0usize
    } else {
        file_length.div_ceil(slice_size) as usize
    };
    for slice_index in 0..slice_count {
        let start = slice_index as u64 * slice_size;
        let end = ((start + slice_size) as usize).min(file_data.len());
        let slice_data = &file_data[start as usize..end];
        let mut checksum_state = weaver_par2::SliceChecksumState::new();
        checksum_state.update(slice_data);
        let pad_to = ((slice_data.len() as u64) < slice_size).then_some(slice_size);
        let (crc32, md5) = checksum_state.finalize(pad_to);
        slice_checksums.push(weaver_par2::SliceChecksum { crc32, md5 });
    }

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&1u32.to_le_bytes());
    main_body.extend_from_slice(file_id.as_bytes());

    let mut par2_set = Par2FileSet {
        recovery_set_id: weaver_par2::RecoverySetId::from_bytes(checksum::md5(&main_body)),
        slice_size,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files: HashMap::from([(
            file_id,
            weaver_par2::FileDescription {
                file_id,
                hash_full,
                hash_16k,
                length: file_length,
                par2_name: filename.to_string(),
                filename: filename.to_string(),
            },
        )]),
        slice_checksums: HashMap::from([(file_id, slice_checksums)]),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    };

    let slice_size_bytes = slice_size as usize;
    let word_count = (slice_size_bytes / 2).max(1);
    let constants = weaver_par2::input_slice_constants(slice_count);
    let mut padded = file_data.to_vec();
    padded.resize(slice_count * slice_size_bytes, 0);

    for exponent in 0..recovery_block_count {
        let exponent = exponent as u32;
        let mut recovery = vec![0u8; slice_size_bytes];

        for (input_index, &constant) in constants.iter().enumerate() {
            let factor = weaver_par2::gf_pow(constant, exponent);
            for word_index in 0..word_count {
                let input_word = u16::from_le_bytes([
                    padded[input_index * slice_size_bytes + word_index * 2],
                    padded[input_index * slice_size_bytes + word_index * 2 + 1],
                ]);
                let contribution = weaver_par2::gf_mul(input_word, factor);
                let current =
                    u16::from_le_bytes([recovery[word_index * 2], recovery[word_index * 2 + 1]]);
                let updated = weaver_par2::gf_add(current, contribution).to_le_bytes();
                recovery[word_index * 2] = updated[0];
                recovery[word_index * 2 + 1] = updated[1];
            }
        }

        par2_set.recovery_slices.insert(
            exponent,
            weaver_par2::RecoverySlice {
                exponent,
                data: bytes::Bytes::from(recovery).into(),
            },
        );
    }

    par2_set
}

fn par2_only_job_spec(name: &str, filename: &str, bytes: u32) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: bytes as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: bytes,
                message_id: "par2-0@example.com".to_string(),
            }],
        }],
    }
}

fn build_empty_rar_volume() -> Vec<u8> {
    let mut volume = Vec::new();
    volume.extend_from_slice(&TEST_RAR5_SIG);
    volume.extend_from_slice(&build_test_rar_main_header(0, None));
    volume.extend_from_slice(&build_test_rar_end_header(false));
    volume
}

fn segmented_job_spec(name: &str, filename: &str, segment_sizes: &[u32]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: segment_sizes.iter().map(|size| *size as u64).sum(),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: segment_sizes
                .iter()
                .enumerate()
                .map(|(index, bytes)| {
                    segment_spec! {
                        number: index as u32,
                        bytes: *bytes,
                        message_id: format!("segment-{index}@example.com"),
                    }
                })
                .collect(),
        }],
    }
}

fn with_priority(mut spec: JobSpec, priority: &str) -> JobSpec {
    spec.metadata = vec![("priority".to_string(), priority.to_string())];
    spec
}

async fn insert_active_job(pipeline: &mut Pipeline, job_id: JobId, spec: JobSpec) -> PathBuf {
    insert_active_job_with_persisted_nzb(pipeline, job_id, spec, sample_nzb_zstd()).await
}

async fn insert_active_job_with_persisted_nzb(
    pipeline: &mut Pipeline,
    job_id: JobId,
    spec: JobSpec,
    nzb_zstd: Vec<u8>,
) -> PathBuf {
    let dir_name = crate::jobs::working_dir::sanitize_dirname(&spec.name);
    let candidate = pipeline.intermediate_dir.join(&dir_name);
    let working_dir = if candidate.exists() {
        pipeline
            .intermediate_dir
            .join(format!("{}.#{}", dir_name, job_id.0))
    } else {
        candidate
    };
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&working_dir),
        [],
    )
    .await
    .unwrap();
    pipeline
        .db
        .create_active_job(&crate::ActiveJob {
            job_id,
            nzb_hash: [0; 32],
            nzb_path: working_dir.join(format!("{}.nzb", job_id.0)),
            nzb_zstd,
            output_dir: working_dir.clone(),
            created_at: 0,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
            status: "downloading",
            download_state: "downloading",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
        })
        .unwrap();
    let (assembly, download_queue, recovery_queue) =
        Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());
    let par2_bytes = spec.par2_bytes();
    pipeline.jobs.insert(
        job_id,
        JobState {
            job_id,
            job_hash: [0; 32],
            spec,
            status: JobStatus::Downloading,
            download_state: crate::jobs::model::DownloadState::Downloading,
            post_state: crate::jobs::model::PostState::Idle,
            run_state: crate::jobs::model::RunState::Active,
            assembly,
            extraction_depth: 0,
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            failure_error: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: 0,
            restored_download_floor_bytes: 0,
            failed_bytes: 0,
            par2_bytes,
            health_probing: false,
            health_probe_round: 0,
            last_health_probe_failed_bytes: 0,
            next_health_probe_failed_bytes: 1,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            held_segments: Vec::new(),
            download_queue,
            recovery_queue,
            staging_dir: None,
        },
    );
    pipeline.job_order.push(job_id);
    working_dir
}

fn rar5_fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar5")
            .join(name),
    )
    .unwrap()
}

fn rar_original_fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/originals")
            .join(name),
    )
    .unwrap()
}

fn sevenz_fixture_bytes(prefix: &str) -> Vec<(String, Vec<u8>)> {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sevenz");
    let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(&fixture_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let filename = path.file_name()?.to_str()?.to_string();
            (filename.starts_with(prefix)).then(|| (filename, std::fs::read(path).unwrap()))
        })
        .collect();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

async fn drive_extractions_to_terminal(pipeline: &mut Pipeline, job_id: JobId, max_rounds: usize) {
    for _ in 0..max_rounds {
        pump_pipeline_runtime_queues(pipeline).await;
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            return;
        }

        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Moving)
        ) {
            let done = tokio::time::timeout(Duration::from_secs(180), pipeline.move_done_rx.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "timed out waiting for final move completion\n{}",
                        debug_job_state(pipeline, job_id)
                    )
                })
                .expect("move channel should stay open");
            pipeline.handle_move_to_complete_done(done);
            pump_pipeline_runtime_queues(pipeline).await;
            continue;
        }

        let done = tokio::time::timeout(Duration::from_secs(180), pipeline.extract_done_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for extraction completion\n{}",
                    debug_job_state(pipeline, job_id)
                )
            })
            .expect("extraction channel should stay open");
        pipeline.handle_extraction_done(done).await;
        pump_pipeline_runtime_queues(pipeline).await;
    }

    panic!("job {job_id} did not reach a terminal state after {max_rounds} extraction rounds");
}

async fn settle_inflight_moves(pipeline: &mut Pipeline) {
    while !pipeline.inflight_moves.is_empty() {
        let done = tokio::time::timeout(Duration::from_secs(5), pipeline.move_done_rx.recv())
            .await
            .expect("final move result should arrive")
            .expect("move channel should stay open");
        pipeline.handle_move_to_complete_done(done);
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }

    while let Ok(done) = pipeline.move_done_rx.try_recv() {
        pipeline.handle_move_to_complete_done(done);
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }
}

async fn drain_rar_refreshes(pipeline: &mut Pipeline) {
    for _ in 0..32 {
        while let Ok(done) = pipeline.rar_refresh_done_rx.try_recv() {
            pipeline.handle_rar_refresh_done(done).await;
        }
        if pipeline
            .rar_refresh_state
            .values()
            .all(|state| state.in_flight.is_none() && state.queued.is_none())
        {
            return;
        }
        let done =
            tokio::time::timeout(Duration::from_secs(5), pipeline.rar_refresh_done_rx.recv())
                .await
                .expect("RAR refresh result should arrive")
                .expect("RAR refresh channel should stay open");
        pipeline.handle_rar_refresh_done(done).await;
    }
    panic!("RAR refresh queue did not drain");
}

async fn drain_verified_suspect_persists(pipeline: &mut Pipeline) {
    for _ in 0..32 {
        while let Ok(done) = pipeline.verified_suspect_persist_done_rx.try_recv() {
            pipeline.handle_verified_suspect_persist_done(done);
        }
        if pipeline
            .verified_suspect_persist_state
            .values()
            .all(|state| state.in_flight_version.is_none())
        {
            return;
        }
        let done = tokio::time::timeout(
            Duration::from_secs(5),
            pipeline.verified_suspect_persist_done_rx.recv(),
        )
        .await
        .expect("verified suspect persistence result should arrive")
        .expect("verified suspect persistence channel should stay open");
        pipeline.handle_verified_suspect_persist_done(done);
    }
    panic!("verified suspect persistence queue did not drain");
}

fn set_job_status_for_test(pipeline: &mut Pipeline, job_id: JobId, status: JobStatus) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.status = status;
    state.refresh_runtime_lanes_from_status();
}

fn pause_job_for_rar_fixture_setup(pipeline: &mut Pipeline, job_id: JobId) {
    // UnRAR owns the active volume chain inside ExtractCurrentFile/MergeArchive.
    // Tests that swap, delete, or surgically rewrite RAR state must not leave
    // eager extraction workers holding those readers while they mutate fixtures.
    set_job_status_for_test(pipeline, job_id, JobStatus::Paused);
}

fn resume_job_downloading_for_test(pipeline: &mut Pipeline, job_id: JobId) {
    set_job_status_for_test(pipeline, job_id, JobStatus::Downloading);
}

async fn write_and_complete_rar_volume(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    write_and_complete_rar_volume_without_drain(pipeline, job_id, file_index, filename, bytes)
        .await;
    drain_rar_refreshes(pipeline).await;
}

async fn write_and_complete_rar_volume_without_drain(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    tokio::fs::write(working_dir.join(filename), bytes)
        .await
        .unwrap();

    let file_id = NzbFileId { job_id, file_index };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, bytes.len() as u32)
            .unwrap();
    }

    pipeline
        .refresh_archive_state_for_completed_file(job_id, file_id, true)
        .await;
}

async fn write_and_complete_file(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    tokio::fs::write(working_dir.join(filename), bytes)
        .await
        .unwrap();

    let file_id = NzbFileId { job_id, file_index };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, bytes.len() as u32)
            .unwrap();
    }

    pipeline
        .refresh_archive_state_for_completed_file(job_id, file_id, true)
        .await;
}

fn two_segment_standalone_job_spec(
    name: &str,
    filename: &str,
    first_bytes: u32,
    second_bytes: u32,
) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (first_bytes + second_bytes) as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    number: 0,
                    bytes: first_bytes,
                    message_id: "segment-0@example.com".to_string(),
                },
                segment_spec! {
                    number: 1,
                    bytes: second_bytes,
                    message_id: "segment-1@example.com".to_string(),
                },
            ],
        }],
    }
}

async fn submit_decoded_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
) {
    submit_decoded_segment_with_part_crc_verified(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        expected_file_crc,
        true,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn submit_decoded_segment_with_part_crc_verified(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
    part_crc_verified: bool,
) {
    pipeline
        .handle_decode_success(DecodeResult {
            segment_id: SegmentId {
                file_id,
                segment_number,
            },
            raw_size: data.len() as u64,
            file_offset,
            decoded_size: data.len() as u32,
            crc_valid: true,
            part_crc_verified,
            part_crc: weaver_par2::checksum::crc32(data),
            expected_file_crc,
            data: DecodedChunk::from(data.to_vec()),
            yenc_name: filename.to_string(),
        })
        .await;
}

async fn persist_completed_file_hash(
    pipeline: &Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    let filename = filename.to_string();
    let hash = weaver_par2::checksum::md5(bytes);
    pipeline
        .db_blocking(move |db| db.complete_file(job_id, file_index, &filename, &hash))
        .await
        .unwrap();
}

async fn write_and_complete_file_like_decode_worker(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    write_and_complete_file(pipeline, job_id, file_index, filename, bytes).await;
    pipeline.retry_par2_authoritative_identity(job_id).await;
    pipeline.try_rar_extraction(job_id).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(pipeline).await;
}

fn member_span(
    pipeline: &Pipeline,
    job_id: JobId,
    set_name: &str,
    member_name: &str,
) -> Option<(u32, u32)> {
    pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .and_then(|topology| {
            topology
                .members
                .iter()
                .find(|member| member.name == member_name)
                .map(|member| (member.first_volume, member.last_volume))
        })
}

fn unresolved_spans(pipeline: &Pipeline, job_id: JobId, set_name: &str) -> Vec<(u32, u32)> {
    let mut spans: Vec<(u32, u32)> = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .map(|topology| {
            topology
                .unresolved_spans
                .iter()
                .map(|span| (span.first_volume, span.last_volume))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    spans.sort_unstable();
    spans
}

fn job_status_for_assert(pipeline: &Pipeline, job_id: JobId) -> Option<JobStatus> {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.status.clone())
        .or_else(|| {
            pipeline
                .finished_jobs
                .iter()
                .find(|job| job.job_id == job_id)
                .map(|job| job.status.clone())
        })
}

fn drain_job_verification_started(
    events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>,
    job_id: JobId,
) -> usize {
    let mut count = 0;
    loop {
        match events.try_recv() {
            Ok(PipelineEvent::JobVerificationStarted {
                job_id: event_job_id,
            }) if event_job_id == job_id => {
                count += 1;
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
    count
}

fn drain_job_repair_complete(
    events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>,
    job_id: JobId,
) -> usize {
    let mut count = 0usize;
    while let Ok(event) = events.try_recv() {
        if matches!(event, PipelineEvent::RepairComplete { job_id: event_job_id, .. } if event_job_id == job_id)
        {
            count += 1;
        }
    }
    count
}

fn drain_job_events(
    events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>,
    job_id: JobId,
) -> Vec<PipelineEvent> {
    let mut drained = Vec::new();
    while let Ok(event) = events.try_recv() {
        if crate::events::publish::pipeline_job_id(&event) == Some(job_id.0) {
            drained.push(event);
        }
    }
    drained
}

fn debug_job_state(pipeline: &Pipeline, job_id: JobId) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "status={:?}",
        job_status_for_assert(pipeline, job_id)
    ));
    lines.push(format!(
        "pending_completion_checks={:?}",
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>()
    ));
    lines.push(format!(
        "inflight_extractions={:?}",
        pipeline
            .inflight_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default()
    ));
    lines.push(format!(
        "extracted_archives={:?}",
        pipeline
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default()
    ));
    lines.push(format!(
        "failed_extractions={:?}",
        pipeline
            .failed_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default()
    ));

    if let Some(state) = pipeline.jobs.get(&job_id) {
        let topologies: Vec<String> = state
            .assembly
            .archive_topologies()
            .iter()
            .map(|(set_name, topology)| {
                format!(
                    "{}:{:?}:complete={:?}:expected={:?}:members={}:unresolved={:?}",
                    set_name,
                    topology.archive_type,
                    topology.complete_volumes,
                    topology.expected_volume_count,
                    topology.members.len(),
                    topology.unresolved_spans
                )
            })
            .collect();
        lines.push(format!("topologies={topologies:?}"));
    }

    let rar_sets: Vec<String> = pipeline
        .rar_sets
        .iter()
        .filter(|((jid, _), _)| *jid == job_id)
        .map(|((_, set_name), state)| {
            let plan = state.plan.as_ref().map(|plan| {
                format!(
                    "phase={:?},ready={:?},waiting={:?},members={:?},solid={}",
                    plan.phase,
                    plan.ready_members
                        .iter()
                        .map(|member| member.name.clone())
                        .collect::<Vec<_>>(),
                    plan.waiting_on_volumes,
                    plan.member_names,
                    plan.is_solid
                )
            });
            format!(
                "{}:active_workers={}:in_flight={:?}:facts={:?}:volume_files={:?}:plan={:?}",
                set_name,
                state.active_workers,
                state.in_flight_members,
                state.facts.keys().copied().collect::<Vec<_>>(),
                state.volume_files,
                plan
            )
        })
        .collect();
    lines.push(format!("rar_sets={rar_sets:?}"));

    lines.join("\n")
}

async fn pump_pipeline_runtime_queues(pipeline: &mut Pipeline) {
    pipeline.pump_decode_queue();
    while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued_job).await;
        pipeline.pump_decode_queue();
    }

    settle_inflight_moves(pipeline).await;

    while let Ok(done) = pipeline.extract_done_rx.try_recv() {
        pipeline.handle_extraction_done(done).await;
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }

    settle_inflight_moves(pipeline).await;
}

async fn next_extraction_done(pipeline: &mut Pipeline) -> ExtractionDone {
    tokio::time::timeout(Duration::from_secs(2), pipeline.extract_done_rx.recv())
        .await
        .expect("extraction result should arrive")
        .expect("extraction channel should stay open")
}

async fn wait_until(
    timeout_duration: Duration,
    mut predicate: impl FnMut() -> bool,
) -> Result<(), &'static str> {
    let deadline = tokio::time::Instant::now() + timeout_duration;
    loop {
        if predicate() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("condition timed out");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn drain_decode_results(pipeline: &mut Pipeline, expected: usize) {
    for index in 0..expected {
        let done =
            match tokio::time::timeout(Duration::from_secs(20), pipeline.decode_done_rx.recv())
                .await
            {
                Ok(Some(done)) => done,
                Ok(None) => panic!("decode channel should stay open"),
                Err(_) => panic!("decode result {}/{} should arrive", index + 1, expected),
            };
        pipeline.handle_decode_done(done).await;
        settle_inflight_moves(pipeline).await;
    }
    settle_inflight_moves(pipeline).await;
}

/// Fill servers with the given retention windows (days, 0 = unlimited). No
/// connections are opened — only the config matters.
fn retention_client(retention_days: &[u32]) -> weaver_nntp::client::NntpClient {
    let servers = retention_days
        .iter()
        .enumerate()
        .map(|(idx, days)| weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: format!("retention-{idx}.example.com"),
                ..Default::default()
            },
            max_connections: 2,
            retention_days: *days,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        })
        .collect();
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    })
}

/// Two fill servers: index 0 holds 5 days of retention, index 1 is unlimited.
fn two_server_retention_client() -> weaver_nntp::client::NntpClient {
    retention_client(&[5, 0])
}

fn age_job_days(pipeline: &mut Pipeline, job_id: JobId, days: u64) {
    let posted = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - days * 86_400;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    for file in &mut state.spec.files {
        file.posted_at_epoch = Some(posted);
    }
}

async fn setup_extracting_rar_full_set(
    pipeline: &mut Pipeline,
    job_id: JobId,
    title: &str,
) -> String {
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec(title, &files);
    insert_active_job(pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let set_name = "show".to_string();
    let key = (job_id, set_name.clone());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist after RAR volumes complete");
        set_state.active_workers = 1;
        set_state.in_flight_members.clear();
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        let plan = set_state
            .plan
            .as_mut()
            .expect("RAR plan should exist after volume facts are built");
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert(set_name.clone());
    set_name
}

fn rar_unlock_work(job_id: JobId, file_index: u32, priority: u32) -> DownloadWork {
    DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId { job_id, file_index },
            segment_number: 0,
        },
        message_id: MessageId::new(&format!("rar-unlock-{job_id:?}-{file_index}@example.com")),
        groups: vec!["alt.binaries.test".to_string()],
        priority,
        byte_estimate: 1024,
        retry_count: 0,
        is_recovery: false,
        exclude_servers: Vec::new(),
    }
}

fn install_rar_unlock_plan(
    pipeline: &mut Pipeline,
    job_id: JobId,
    set_name: &str,
    is_solid: bool,
    volume_count: u32,
    complete_volumes: HashSet<u32>,
    members: Vec<crate::jobs::assembly::ArchiveMember>,
) {
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: (0..volume_count)
            .map(|volume| (format!("{set_name}.part{:02}.rar", volume + 1), volume))
            .collect(),
        complete_volumes,
        expected_volume_count: Some(volume_count),
        members: members.clone(),
        unresolved_spans: Vec::new(),
    };
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state
            .assembly
            .set_archive_topology(set_name.to_string(), topology.clone());
    }
    pipeline.rar_sets.insert(
        (job_id, set_name.to_string()),
        rar_state::RarSetState {
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid,
                ready_members: Vec::new(),
                member_names: members.iter().map(|member| member.name.clone()).collect(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );
}

fn reset_rar_unlock_queue(pipeline: &mut Pipeline, job_id: JobId, indices: &[u32]) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    for &file_index in indices {
        let priority = FileRole::RarVolume {
            volume_number: file_index,
        }
        .download_priority();
        state
            .download_queue
            .push(rar_unlock_work(job_id, file_index, priority));
    }
}
