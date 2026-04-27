use super::*;

use std::io::Read;
use std::time::Duration;

use crate::Database;
use crate::ingest::submit_nzb_bytes;
use crate::jobs::FileIdentitySource;
use crate::jobs::ids::MessageId;
use crate::runtime::buffers::{BufferPool, BufferPoolConfig};
use crate::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use crate::settings::{Config, SharedConfig};
use crate::{FileSpec, PipelineMetrics, SchedulerHandle, SegmentSpec, SharedPipelineState};
use chrono::Timelike;
use tempfile::TempDir;
use tokio::sync::{RwLock, oneshot};
use weaver_model::files::FileRole;
use weaver_nntp::client::{NntpClient, NntpClientConfig};

struct TestHarness {
    _temp_dir: TempDir,
    data_dir: PathBuf,
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
            cleanup_after_extract: Some(true),
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
            data_dir,
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
          <file poster="poster" date="1700000000" subject="Frieren.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
        .to_vec()
}

fn minimal_job_state(job_id: JobId, name: &str, working_dir: PathBuf) -> JobState {
    JobState {
        job_id,
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
        last_health_probe_failed_bytes: 0,
        detected_archives: HashMap::new(),
        file_identities: HashMap::new(),
        held_segments: Vec::new(),
        download_queue: DownloadQueue::new(),
        recovery_queue: DownloadQueue::new(),
        staging_dir: None,
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
        cleanup_after_extract: Some(true),
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

#[tokio::test]
async fn extraction_refreshes_stale_cached_headers_for_touched_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();

    let mut stale_archive = weaver_rar::RarArchive::open(std::io::Cursor::new(files[0].1.clone()))
        .unwrap();
    stale_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    stale_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();
    stale_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    let stale_headers = stale_archive.serialize_headers();

    let volume_paths = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, bytes))| {
            let path = temp_dir.path().join(filename);
            std::fs::write(&path, bytes).unwrap();
            (volume as u32, path)
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let options = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };

    let output_dir_without_refresh = temp_dir.path().join("without-refresh");
    std::fs::create_dir_all(&output_dir_without_refresh).unwrap();
    let mut archive_without_refresh = Pipeline::open_rar_archive_from_snapshot_or_disk(
        "show",
        volume_paths.clone(),
        None,
        Some(stale_headers.clone()),
        false,
    )
    .unwrap();
    let idx_without_refresh = archive_without_refresh.find_member_sanitized("E02.mkv").unwrap();
    let error = Pipeline::extract_rar_member_to_output(
        &mut archive_without_refresh,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.db,
            &pipeline.event_tx,
            JobId(40100),
            "show",
            &output_dir_without_refresh,
            &options,
        ),
        idx_without_refresh,
    )
    .unwrap_err();
    assert!(
        error.contains("CRC mismatch")
            || error.to_ascii_lowercase().contains("checksum")
            || error.contains("not registered")
            || error.contains("unavailable"),
        "unexpected stale-header extraction error: {error}"
    );

    let output_dir_with_refresh = temp_dir.path().join("with-refresh");
    std::fs::create_dir_all(&output_dir_with_refresh).unwrap();
    let mut archive_with_refresh = Pipeline::open_rar_archive_from_snapshot_or_disk(
        "show",
        volume_paths.clone(),
        None,
        Some(stale_headers),
        true,
    )
    .unwrap();
    let idx_with_refresh = archive_with_refresh.find_member_sanitized("E02.mkv").unwrap();
    let (member_name, _, _) = Pipeline::extract_rar_member_to_output(
        &mut archive_with_refresh,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.db,
            &pipeline.event_tx,
            JobId(40101),
            "show",
            &output_dir_with_refresh,
            &options,
        ),
        idx_with_refresh,
    )
    .unwrap();

    assert_eq!(member_name, "E02.mkv");
    assert_eq!(
        std::fs::read(output_dir_with_refresh.join("E02.mkv")).unwrap(),
        b"episode-b-payload"
    );
}

#[tokio::test]
async fn recompute_rar_set_state_refreshes_cached_span_when_facts_contradict_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40102);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Facts Heal Stale Cached Span", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let mut good_archive = weaver_rar::RarArchive::open(std::io::Cursor::new(files[0].1.clone()))
        .unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    let members = cached["members"].as_array_mut().unwrap();
    let e01 = members
        .iter_mut()
        .find(|member| member["name"] == "E01.mkv")
        .expect("cached snapshot should contain E01");
    let first_segment = e01["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("cached snapshot should include the first E01 segment");
    e01["segments"] = serde_json::json!([first_segment]);
    e01["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<weaver_rar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after all volumes complete")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();

    pipeline.recompute_rar_set_state(job_id, "show").await.unwrap();

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after recompute");
    let e01 = topology
        .members
        .iter()
        .find(|member| member.name == "E01.mkv")
        .expect("recomputed topology should contain E01");
    assert_eq!(e01.first_volume, 0);
    assert_eq!(e01.last_volume, 1);

    let healed_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("recompute should persist a healed snapshot");
    let healed_archive = weaver_rar::RarArchive::deserialize_headers_with_password(
        &healed_headers,
        None::<String>,
    )
    .unwrap();
    let healed_e01 = healed_archive
        .metadata()
        .members
        .into_iter()
        .find(|member| member.name == "E01.mkv")
        .expect("healed snapshot should contain E01");
    assert_eq!(healed_e01.volumes.first_volume, 0);
    assert_eq!(healed_e01.volumes.last_volume, 1);
}

#[tokio::test]
async fn final_volume_refresh_heals_encrypted_multivolume_member_span_before_scheduling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40103);
    let set_name = "video";
    let member_name = "test_clip.mkv";
    let files = vec![
        (
            "video.part001.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part1.rar"),
        ),
        (
            "video.part002.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part2.rar"),
        ),
        (
            "video.part003.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part3.rar"),
        ),
        (
            "video.part004.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part4.rar"),
        ),
        (
            "video.part005.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part5.rar"),
        ),
    ];
    let mut spec = rar_job_spec("RAR Facts Heal Encrypted Fixture Span", &files);
    spec.password = Some("testpass123".to_string());

    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate().take(4) {
        write_and_complete_rar_volume(
            &mut pipeline,
            job_id,
            file_index as u32,
            filename,
            bytes,
        )
        .await;
    }

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, set_name)
        .expect("partial encrypted snapshot should exist after four volumes");
    let mut cached = serde_json::to_value(
        rmp_serde::from_slice::<weaver_rar::CachedArchiveHeaders>(&cached_headers).unwrap(),
    )
    .unwrap();
    let members = cached["members"].as_array_mut().unwrap();
    let clip = members
        .iter_mut()
        .find(|member| member["name"] == member_name)
        .expect("cached snapshot should contain the encrypted clip member");
    let first_segment = clip["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("encrypted clip should retain its first segment in the snapshot");
    clip["segments"] = serde_json::json!([first_segment]);
    clip["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<weaver_rar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    pipeline
        .rar_sets
        .get_mut(&(job_id, set_name.to_string()))
        .expect("RAR set state should exist after partial arrival")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, set_name, &stale_headers)
        .unwrap();

    write_and_complete_rar_volume(
        &mut pipeline,
        job_id,
        4,
        &files[4].0,
        &files[4].1,
    )
    .await;

    assert_eq!(member_span(&pipeline, job_id, set_name, member_name), Some((0, 4)));

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let selected = pipeline.volume_paths_for_rar_members(
        job_id,
        set_name,
        &[member_name.to_string()],
        &volume_paths,
        true,
        false,
    );
    assert_eq!(selected.keys().copied().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
}

fn dummy_rar_volume_facts(volume_number: u32) -> weaver_rar::RarVolumeFacts {
    weaver_rar::RarVolumeFacts {
        format: 5,
        volume_number,
        more_volumes: true,
        is_solid: false,
        is_encrypted: false,
        members: Vec::new(),
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
                segments: vec![SegmentSpec {
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
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: *bytes,
                    message_id: format!("segment-{index}@example.com"),
                }],
            })
            .collect(),
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
            segments: vec![SegmentSpec {
                number: 0,
                bytes,
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
            segments: segment_sizes
                .iter()
                .enumerate()
                .map(|(index, bytes)| SegmentSpec {
                    number: index as u32,
                    bytes: *bytes,
                    message_id: format!("segment-{index}@example.com"),
                })
                .collect(),
        }],
    }
}

async fn insert_active_job(pipeline: &mut Pipeline, job_id: JobId, spec: JobSpec) -> PathBuf {
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
            output_dir: working_dir.clone(),
            created_at: 0,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
        })
        .unwrap();
    let (assembly, download_queue, recovery_queue) =
        Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());
    let par2_bytes = spec.par2_bytes();
    pipeline.jobs.insert(
        job_id,
        JobState {
            job_id,
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
            last_health_probe_failed_bytes: 0,
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
            .join("../../../engines/weaver-rar/tests/fixtures/rar5")
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

async fn write_and_complete_rar_volume(
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

    while let Ok(done) = pipeline.extract_done_rx.try_recv() {
        pipeline.handle_extraction_done(done).await;
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }
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
    for _ in 0..expected {
        let done = tokio::time::timeout(Duration::from_secs(5), pipeline.decode_done_rx.recv())
            .await
            .expect("decode result should arrive")
            .expect("decode channel should stay open");
        pipeline.handle_decode_done(done).await;
    }
}

#[tokio::test]
async fn submit_nzb_persists_zstd_and_creates_active_job() {
    let harness = TestHarness::new().await;
    let nzb_bytes = sample_nzb_bytes();

    let submitted = tokio::time::timeout(
        Duration::from_secs(2),
        submit_nzb_bytes(
            &harness.handle,
            &harness.config,
            &nzb_bytes,
            Some("Frieren.Sample.nzb".to_string()),
            None,
            None,
            vec![("source".to_string(), "test".to_string())],
        ),
    )
    .await
    .unwrap()
    .unwrap();

    wait_until(Duration::from_secs(2), || {
        harness
            .db
            .load_active_jobs()
            .map(|jobs| jobs.contains_key(&submitted.job_id))
            .unwrap_or(false)
    })
    .await
    .unwrap();

    let info = harness.handle.get_job(submitted.job_id).unwrap();
    assert_eq!(info.status, JobStatus::Queued);
    assert!(
        info.metadata
            .contains(&("source".to_string(), "test".to_string()))
    );
    assert!(
        info.metadata
            .iter()
            .any(|(key, value)| { key == "weaver.original_title" && value == "Frieren.Sample" })
    );

    let stored_path = harness
        .data_dir
        .join(".weaver-nzbs")
        .join(format!("{}.nzb", submitted.job_id.0));
    let stored_nzb = tokio::fs::read(&stored_path).await.unwrap();
    assert!(stored_nzb.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));

    let mut reader = crate::ingest::open_persisted_nzb_reader(&stored_path).unwrap();
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, nzb_bytes);

    harness.shutdown().await;
}

#[tokio::test]
async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let job_name = "Frieren Beyond Journeys End";
    let payload_dir = "Frieren.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
    let episode_name = "Frieren.Beyond.Journeys.End.S01E01.mkv";

    let existing_dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    tokio::fs::create_dir_all(existing_dest.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(
        existing_dest.join(payload_dir).join(episode_name),
        b"existing",
    )
    .await
    .unwrap();

    let working_dir = intermediate_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(working_dir.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(payload_dir).join(episode_name), b"new")
        .await
        .unwrap();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );

    let dest = pipeline.move_to_complete(job_id).await.unwrap();
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().working_dir,
        expected_dest
    );
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"new"
    );
    assert_eq!(
        tokio::fs::read(existing_dest.join(payload_dir).join(episode_name))
            .await
            .unwrap(),
        b"existing"
    );
}

#[tokio::test]
async fn failed_final_move_marks_job_failed_instead_of_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10068);
    let job_name = "Frieren Broken Final Move";
    let missing_working_dir = intermediate_dir.join("missing");

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, missing_working_dir),
    );

    pipeline.check_job_completion(job_id).await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(status, JobStatus::Failed { .. }));
    let JobStatus::Failed { error } = &status else {
        unreachable!();
    };
    assert!(error.contains("failed to read working directory"));
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
}

#[tokio::test]
async fn nested_rar_two_deep_extracts_final_media() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10069);
    let fixture_name = "rar5_nested_2deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Two Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Two Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("sample.mkv").exists());
    assert!(!dest.join("inner.rar").exists());
}

#[tokio::test]
async fn nested_rar_three_deep_extracts_through_inner_7z() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10070);
    let fixture_name = "rar5_nested_3deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Three Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 6).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Three Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("sample.mkv").exists());
    assert!(!dest.join("middle.rar").exists());
    assert!(!dest.join("inner.7z").exists());
}

#[tokio::test]
async fn nested_rar_five_deep_stops_at_depth_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10071);
    let fixture_name = "rar5_nested_5deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Five Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 8).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Five Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("level2.rar").exists());
    assert!(!dest.join("sample.mkv").exists());
}

#[tokio::test]
async fn nested_scan_detects_obfuscated_rar_archives_from_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100_711);
    let staging_dir = temp_dir.path().join("nested-obfuscated-rar");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    for (filename, bytes) in &obfuscated_files {
        tokio::fs::write(staging_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let mut state = minimal_job_state(job_id, "Nested Obfuscated RAR", temp_dir.path().join("wd"));
    state.staging_dir = Some(staging_dir);
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);

    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::NestedExtractionDecision::Started
    ));

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("51273aad56a8b904e96928935278a627")
        .unwrap();
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Rar
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    for file_index in 0..obfuscated_files.len() {
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: file_index as u32,
            })
            .unwrap();
        assert!(matches!(
            file.role(),
            weaver_model::files::FileRole::Unknown
        ));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some("51273aad56a8b904e96928935278a627")
        );
    }
}

#[tokio::test]
async fn nested_scan_detects_obfuscated_split_7z_archives_from_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100_712);
    let staging_dir = temp_dir.path().join("nested-obfuscated-7z");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    for (filename, bytes) in &obfuscated_files {
        tokio::fs::write(staging_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let mut state = minimal_job_state(job_id, "Nested Obfuscated 7z", temp_dir.path().join("wd"));
    state.staging_dir = Some(staging_dir);
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);

    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::NestedExtractionDecision::Started
    ));

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("51273aad56a8b904e96928935278a627")
        .unwrap();
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    for file_index in 0..obfuscated_files.len() {
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: file_index as u32,
            })
            .unwrap();
        assert!(matches!(
            file.role(),
            weaver_model::files::FileRole::Unknown
        ));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::SevenZipSplit { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some("51273aad56a8b904e96928935278a627")
        );
    }
}

#[tokio::test]
async fn upstream_probe_registers_obfuscated_unknown_rar_volumes_before_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10072);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Unknown RAR Recovery", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
        pipeline.try_rar_extraction(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }

    let mut rar_topologies: Vec<String> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .archive_topologies()
        .iter()
        .filter_map(|(set_name, topology)| {
            (topology.archive_type == crate::jobs::assembly::ArchiveType::Rar)
                .then_some(set_name.clone())
        })
        .collect();
    rar_topologies.sort();
    assert_eq!(rar_topologies, vec!["51273aad56a8b904e96928935278a627"]);
    let detected_archives = pipeline
        .db
        .load_detected_archive_identities(job_id)
        .unwrap();
    assert_eq!(detected_archives.len(), obfuscated_files.len());
    for file_index in 0..obfuscated_files.len() {
        let file = pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: file_index as u32,
            })
            .unwrap();
        assert!(matches!(
            file.role(),
            weaver_model::files::FileRole::Unknown
        ));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some("51273aad56a8b904e96928935278a627")
        );
        assert!(matches!(
            detected_archives
                .get(&(file_index as u32))
                .map(|detected| &detected.kind),
            Some(crate::jobs::assembly::DetectedArchiveKind::Rar)
        ));
    }

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Unknown RAR Recovery",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("E01.mkv").exists());
    assert!(dest.join("E02.mkv").exists());
}

#[tokio::test]
async fn upstream_probe_registers_obfuscated_split_topology_rar_volumes_before_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10073);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.10{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Split RAR Recovery", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
        pipeline.try_rar_extraction(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }

    let mut rar_topologies: Vec<String> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .archive_topologies()
        .iter()
        .filter_map(|(set_name, topology)| {
            (topology.archive_type == crate::jobs::assembly::ArchiveType::Rar)
                .then_some(set_name.clone())
        })
        .collect();
    rar_topologies.sort();
    assert_eq!(rar_topologies, vec!["51273aad56a8b904e96928935278a627"]);
    let detected_archives = pipeline
        .db
        .load_detected_archive_identities(job_id)
        .unwrap();
    assert_eq!(detected_archives.len(), obfuscated_files.len());

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Split RAR Recovery",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("E01.mkv").exists());
    assert!(dest.join("E02.mkv").exists());
}

#[tokio::test]
async fn upstream_probe_falls_back_from_rar_to_7z_for_obfuscated_split_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10074);
    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 10),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Split 7z Detection", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let set_name = "51273aad56a8b904e96928935278a627";
    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file_like_decode_worker(
            &mut pipeline,
            job_id,
            file_index as u32,
            filename,
            bytes,
        )
        .await;

        let topology = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for(set_name))
            .cloned()
            .expect("detected 7z topology should exist");
        assert_eq!(
            topology.archive_type,
            crate::jobs::assembly::ArchiveType::SevenZip
        );
        assert_eq!(
            topology.expected_volume_count,
            Some(obfuscated_files.len() as u32)
        );

        let expected_complete_volumes: std::collections::HashSet<u32> =
            (0..=file_index as u32).collect();
        assert_eq!(topology.complete_volumes, expected_complete_volumes);
    }

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 2).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Split 7z Detection",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("generated_split_clip.mkv").exists());
}

#[tokio::test]
async fn restore_job_rehydrates_detected_obfuscated_rar_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10076);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Restore Obfuscated RAR", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);
    let committed_segments = Pipeline::all_segment_ids(job_id, &spec);

    let (mut restored, _intermediate_dir, complete_dir_restored) =
        new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress: recovered.file_progress,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let restored_file = restored
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    assert!(matches!(
        restored_file.role(),
        weaver_model::files::FileRole::Unknown
    ));
    assert!(matches!(
        restored.classified_role_for_file(job_id, restored_file),
        weaver_model::files::FileRole::RarVolume { .. }
    ));
    assert_eq!(
        restored
            .detected_archive_identity(job_id, restored_file.file_id())
            .map(|detected| detected.set_name.as_str()),
        Some("51273aad56a8b904e96928935278a627")
    );

    let set_name = "51273aad56a8b904e96928935278a627";
    let topology = restored
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .cloned()
        .expect("restored RAR topology should exist");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Rar
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    assert!(!topology.members.is_empty());
    let _ = complete_dir;
    let _ = complete_dir_restored;
}

#[tokio::test]
async fn restore_job_rehydrates_detected_obfuscated_split_7z_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10077);
    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 10),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Restore Obfuscated Split 7z", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);
    let committed_segments = Pipeline::all_segment_ids(job_id, &spec);

    let (mut restored, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress: recovered.file_progress,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let restored_file = restored
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 1,
        })
        .unwrap();
    assert!(matches!(
        restored_file.role(),
        weaver_model::files::FileRole::Unknown | weaver_model::files::FileRole::SplitFile { .. }
    ));
    assert!(matches!(
        restored.classified_role_for_file(job_id, restored_file),
        weaver_model::files::FileRole::SevenZipSplit { .. }
    ));
    assert_eq!(
        restored
            .detected_archive_identity(job_id, restored_file.file_id())
            .map(|detected| detected.set_name.as_str()),
        Some("51273aad56a8b904e96928935278a627")
    );

    let set_name = "51273aad56a8b904e96928935278a627";
    let topology = restored
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .cloned()
        .expect("restored 7z topology should exist");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert_eq!(
        topology.expected_volume_count,
        Some(obfuscated_files.len() as u32)
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    let _ = complete_dir;
}

#[tokio::test]
async fn upstream_probe_does_not_misclassify_unknown_numeric_plain_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10075);
    let files = vec![
        (
            "51273aad56a8b904e96928935278a627.10".to_string(),
            b"plain-a".to_vec(),
        ),
        (
            "51273aad56a8b904e96928935278a627.11".to_string(),
            b"plain-b".to_vec(),
        ),
    ];
    let spec = rar_job_spec("Unknown Numeric Plain Files", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topologies()
            .is_empty()
    );

    pipeline.check_job_completion(job_id).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Unknown Numeric Plain Files",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("51273aad56a8b904e96928935278a627.10").exists());
    assert!(dest.join("51273aad56a8b904e96928935278a627.11").exists());
}

#[tokio::test]
async fn tiny_write_budget_evicts_out_of_order_segments_and_job_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20001);
    let filename = "episode.bin";
    let payload_size =
        (crate::runtime::buffers::BufferTier::Small.size_bytes() + 256 * 1024) as u32;
    let spec = segmented_job_spec(
        "Write Backlog Budget",
        filename,
        &[payload_size, payload_size, payload_size],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.write_backlog_budget_bytes = payload_size as usize;

    let total_size = payload_size as u64 * 3;
    for segment_number in [1u32, 2u32] {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            3,
            segment_number as u64 * payload_size as u64 + 1,
            total_size,
        );
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };

        pipeline.active_downloads += 1;
        tokio::time::timeout(
            Duration::from_secs(1),
            pipeline.handle_download_done(DownloadResult {
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                is_recovery: false,
                retry_count: 0,
                exclude_servers: Vec::new(),
            }),
        )
        .await
        .expect("download completion should not block");
    }

    drain_decode_results(&mut pipeline, 2).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    wait_until(Duration::from_secs(2), || {
        pipeline
            .buffers
            .available(crate::runtime::buffers::BufferTier::Medium)
            == 1
    })
    .await
    .expect("decode scratch buffer should be returned after backlog relief");
    assert_eq!(
        pipeline
            .buffers
            .available(crate::runtime::buffers::BufferTier::Medium),
        1
    );
    assert!(
        pipeline
            .metrics
            .direct_write_evictions
            .load(Ordering::Relaxed)
            >= 1,
        "tiny write budget should degrade to direct writes"
    );
    assert!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed)
            <= payload_size as u64
    );

    let payload = vec![1u8; payload_size as usize];
    let raw = encode_article_part(filename, &payload, 1, 3, 1, total_size);
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Ok(DownloadPayload::Raw(raw)),
            is_recovery: false,
            retry_count: 0,
            exclude_servers: Vec::new(),
        })
        .await;
    drain_decode_results(&mut pipeline, 1).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn in_order_segments_keep_write_cursor_until_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20007);
    let segment_count = 6u32;
    let payload_size = 128u32;
    let total_size = segment_count * payload_size;
    let filename = "cursor.bin";
    let spec = JobSpec {
        name: "Write Cursor".to_string(),
        password: None,
        total_bytes: total_size as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            segments: (0..segment_count)
                .map(|number| SegmentSpec {
                    number,
                    bytes: payload_size,
                    message_id: format!("cursor-{number}@example.com"),
                })
                .collect(),
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for segment_number in 0..segment_count {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            segment_count,
            segment_number as u64 * payload_size as u64 + 1,
            total_size as u64,
        );
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        };

        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                is_recovery: false,
                retry_count: 0,
                exclude_servers: Vec::new(),
            })
            .await;
    }

    drain_decode_results(&mut pipeline, segment_count as usize).await;

    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert!(!pipeline.write_buffers.contains_key(&NzbFileId {
        job_id,
        file_index: 0,
    }));
    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        segment_count as u64
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn transient_retry_backoff_does_not_fail_job_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20008);
    let spec = segmented_job_spec("Retry Backoff Guard", "retry.bin", &[128, 128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::Fetch("connection reset by peer".to_string())),
            is_recovery: false,
            retry_count: 0,
            exclude_servers: Vec::new(),
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn excluded_source_not_found_retries_without_marking_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20011);
    let spec = segmented_job_spec("Excluded Source Not Found", "retry.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Err(DownloadError::Fetch("article not found".to_string())),
            is_recovery: false,
            retry_count: 0,
            exclude_servers: vec![0],
        })
        .await;

    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.failed_bytes),
        Some(0)
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.pending_completion_checks.is_empty());

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let work = pipeline
        .retry_rx
        .try_recv()
        .expect("excluded-source miss should requeue the segment");
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(work.retry_count, 1);
}

#[tokio::test]
async fn exhausted_incomplete_download_fails_instead_of_hanging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "stalled.bin";
    let first_segment_size = 6400u32;
    let second_segment_size = 128u32;
    let total_size = (first_segment_size + second_segment_size) as u64;
    let spec = segmented_job_spec(
        "Incomplete Exhausted",
        filename,
        &[first_segment_size, second_segment_size],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Ok(DownloadPayload::Raw(encode_article_part(
                filename,
                &vec![1u8; first_segment_size as usize],
                1,
                2,
                1,
                total_size,
            ))),
            is_recovery: false,
            retry_count: 0,
            exclude_servers: Vec::new(),
        })
        .await;
    drain_decode_results(&mut pipeline, 1).await;

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .complete_data_file_count(),
        0
    );

    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline
        .handle_download_done(DownloadResult {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            data: Err(DownloadError::Fetch("connection reset by peer".to_string())),
            is_recovery: false,
            retry_count: MAX_SEGMENT_RETRIES,
            exclude_servers: Vec::new(),
        })
        .await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued_job).await;
    }

    let status = job_status_for_assert(&pipeline, job_id).expect("job should be archived");
    assert!(
        matches!(status, JobStatus::Failed { .. }),
        "status: {status:?}"
    );
}

#[tokio::test]
async fn download_pass_finishes_when_only_optional_recovery_queue_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20010);
    let spec = segmented_job_spec("Optional Recovery Only", "payload.bin", &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Downloading;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("recovery-0@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 0);

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));

    pipeline.maybe_finish_download_pass(job_id);

    assert!(!pipeline.active_download_passes.contains(&job_id));
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().recovery_queue.len(),
        1,
        "optional recovery files should stay parked until promoted"
    );
}

#[tokio::test]
async fn dispatch_downloads_respects_decode_backpressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20002);
    let files = vec![("queued.bin".to_string(), 512u32)];
    let spec = standalone_job_spec("Decode Queue Limit", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline
        .metrics
        .decode_pending
        .store(pipeline.tuner.params().max_decode_queue, Ordering::Relaxed);
    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        files.len()
    );
}

#[tokio::test]
async fn dispatch_downloads_ignores_write_backlog_when_raw_decode_queue_is_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20004);
    let spec = standalone_job_spec(
        "Write Backlog Does Not Gate Dispatch",
        &[("queued.bin".to_string(), 512u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.connection_ramp = 1;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 99,
        },
        decoded_size: 4096,
        crc32: 0,
        data: DecodedChunk::from(vec![7u8; 4096]),
        yenc_name: "queued.bin".to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(4096, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 1);
    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_segments
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn dispatch_downloads_allows_postprocessing_status_with_remaining_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    pipeline.connection_ramp = 2;
    let cases = [
        (JobId(20006), JobStatus::Extracting),
        (JobId(20007), JobStatus::QueuedExtract),
    ];

    for (job_id, status) in &cases {
        insert_active_job(
            &mut pipeline,
            *job_id,
            standalone_job_spec(
                &format!("Postprocessing Dispatch {}", job_id.0),
                &[(format!("queued-{}.bin", job_id.0), 512u32)],
            ),
        )
        .await;
        let state = pipeline.jobs.get_mut(job_id).unwrap();
        state.status = status.clone();
        state.refresh_runtime_lanes_from_status();
        assert_eq!(
            state.download_state,
            crate::jobs::model::DownloadState::Complete
        );
        assert_eq!(state.download_queue.len(), 1);
    }

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 2);
    for (job_id, expected_status) in &cases {
        let state = pipeline.jobs.get(job_id).unwrap();
        assert_eq!(&state.status, expected_status);
        assert_eq!(state.download_queue.len(), 0);
        assert!(pipeline.active_download_passes.contains(job_id));
    }
}

#[tokio::test]
async fn list_jobs_projects_downloading_while_extracting_with_remaining_download_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 2,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20008);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Extracting Still Downloading",
            &[("queued.bin".to_string(), 512)],
        ),
    )
    .await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        assert_eq!(
            state.download_state,
            crate::jobs::model::DownloadState::Complete
        );
        assert_eq!(state.download_queue.len(), 1);
    }

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    assert_eq!(info.status, JobStatus::Extracting);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(info.post_state, crate::jobs::model::PostState::Extracting);
}

#[tokio::test]
async fn list_jobs_projects_waiting_for_volumes_from_rar_phase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("RAR Waiting", &[("show.part01.rar".to_string(), 512)]),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Downloading;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            ..Default::default()
        },
    );

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    assert_eq!(info.status, JobStatus::Downloading);
    assert_eq!(info.download_state, crate::jobs::model::DownloadState::Complete);
    assert_eq!(
        info.post_state,
        crate::jobs::model::PostState::WaitingForVolumes
    );
}

#[tokio::test]
async fn dispatch_downloads_blocks_when_isp_bandwidth_cap_is_hit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        2,
    )
    .await;
    let job_id = JobId(20005);
    let spec = standalone_job_spec("ISP Cap Gate", &[("queued.bin".to_string(), 512u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.connection_ramp = 1;

    let now = chrono::Local::now();
    let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
    pipeline
        .db
        .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 1024)
        .unwrap();
    pipeline
        .apply_bandwidth_cap_policy(Some(crate::bandwidth::IspBandwidthCapConfig {
            enabled: true,
            period: crate::bandwidth::IspBandwidthCapPeriod::Daily,
            limit_bytes: 512,
            reset_time_minutes_local: reset_minutes,
            weekly_reset_weekday: crate::bandwidth::IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }))
        .unwrap();

    pipeline.dispatch_downloads();

    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 1);
    assert_eq!(
        pipeline.shared_state.download_block().kind,
        crate::jobs::handle::DownloadBlockKind::IspCap
    );
}

#[tokio::test]
async fn set_bandwidth_cap_policy_recomputes_current_window_usage_from_ledger() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        1,
    )
    .await;

    let now = chrono::Local::now();
    let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
    pipeline
        .db
        .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 4096)
        .unwrap();

    pipeline
        .apply_bandwidth_cap_policy(Some(crate::bandwidth::IspBandwidthCapConfig {
            enabled: false,
            period: crate::bandwidth::IspBandwidthCapPeriod::Daily,
            limit_bytes: 10_000,
            reset_time_minutes_local: reset_minutes,
            weekly_reset_weekday: crate::bandwidth::IspBandwidthCapWeekday::Mon,
            monthly_reset_day: 1,
        }))
        .unwrap();

    let block = pipeline.shared_state.download_block();
    assert_eq!(block.used_bytes, 4096);
    assert_eq!(block.remaining_bytes, 10_000 - 4096);
    assert!(!block.cap_enabled);
}

#[tokio::test]
async fn decode_failure_drains_backlog_and_keeps_commands_responsive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        },
        4,
    )
    .await;
    let job_id = JobId(20003);
    let files = vec![("broken.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Decode Failure", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    pipeline.active_downloads += 1;
    pipeline
        .handle_download_done(DownloadResult {
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from_static(
                b"not a yenc article",
            ))),
            is_recovery: false,
            retry_count: 0,
            exclude_servers: Vec::new(),
        })
        .await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 1);

    let done = tokio::time::timeout(Duration::from_secs(2), pipeline.decode_done_rx.recv())
        .await
        .expect("decode failure should arrive")
        .expect("decode channel should stay open");
    let DecodeDone::Failed {
        segment_id: failed_segment,
        ..
    } = &done
    else {
        panic!("expected decode failure");
    };
    assert_eq!(*failed_segment, segment_id);

    pipeline.handle_decode_done(done).await;

    assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PauseAll { reply })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("pause reply should arrive")
        .unwrap();
    assert!(pipeline.global_paused);
    assert_eq!(
        pipeline.db.get_setting("global_paused").unwrap().as_deref(),
        Some("true")
    );

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ResumeAll { reply })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("resume reply should arrive")
        .unwrap();
    assert!(!pipeline.global_paused);
    assert_eq!(
        pipeline.db.get_setting("global_paused").unwrap().as_deref(),
        Some("false")
    );
}

#[tokio::test]
async fn delete_history_removes_intermediate_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let output_dir = intermediate_dir.join("history-cleanup-job");
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
        .await
        .unwrap();

    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            job_id,
            "History Cleanup",
            "failed",
            output_dir.clone(),
        ))
        .unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteHistory {
            job_id,
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(!output_dir.exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_history_removes_retained_failed_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let output_dir = intermediate_dir.join("history-retained-nzb-job");
    let nzb_path = pipeline.nzb_dir.join(format!("{}.nzb", job_id.0));
    tokio::fs::create_dir_all(&output_dir).await.unwrap();
    tokio::fs::create_dir_all(&pipeline.nzb_dir).await.unwrap();
    tokio::fs::write(&nzb_path, b"persisted").await.unwrap();

    let mut row = history_row_with_output_dir(job_id, "History Retained NZB", "failed", output_dir);
    row.nzb_path = Some(nzb_path.display().to_string());
    pipeline.db.insert_job_history(&row).unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteHistory {
            job_id,
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(!nzb_path.exists());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn delete_all_history_keeps_complete_output_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let failed_job_id = JobId(30021);
    let complete_job_id = JobId(30022);
    let failed_output_dir = intermediate_dir.join("failed-history-job");
    let complete_output_dir = complete_dir.join("complete-history-job");

    tokio::fs::create_dir_all(&failed_output_dir).await.unwrap();
    tokio::fs::create_dir_all(&complete_output_dir)
        .await
        .unwrap();
    tokio::fs::write(failed_output_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(complete_output_dir.join("episode.mkv"), b"complete")
        .await
        .unwrap();

    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            failed_job_id,
            "Failed History Cleanup",
            "failed",
            failed_output_dir.clone(),
        ))
        .unwrap();
    pipeline
        .db
        .insert_job_history(&history_row_with_output_dir(
            complete_job_id,
            "Complete History Cleanup",
            "complete",
            complete_output_dir.clone(),
        ))
        .unwrap();

    let (reply, recv) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::DeleteAllHistory {
            delete_files: false,
            reply,
        })
        .await;
    tokio::time::timeout(Duration::from_secs(1), recv)
        .await
        .expect("delete all history reply should arrive")
        .unwrap()
        .unwrap();

    assert!(!failed_output_dir.exists());
    assert!(complete_output_dir.exists());
    assert!(
        pipeline
            .db
            .list_job_history(&crate::HistoryFilter::default())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn hdd_profile_allocates_more_write_backlog_than_ssd() {
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
    let mut hdd_profile = profile.clone();
    hdd_profile.disk.storage_class = StorageClass::Hdd;
    let buffers = BufferPool::new(BufferPoolConfig {
        small_count: 64,
        medium_count: 8,
        large_count: 2,
    });

    let ssd_budget = compute_write_backlog_budget_bytes(&profile, &buffers);
    let hdd_budget = compute_write_backlog_budget_bytes(&hdd_profile, &buffers);

    assert!(hdd_budget > ssd_budget);
}

#[tokio::test]
async fn fail_job_clears_write_backlog_accounting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20005);
    let spec = standalone_job_spec("Fail Clears Backlog", &[("stalled.bin".to_string(), 64u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 4096,
        crc32: 0,
        data: DecodedChunk::from(vec![3u8; 4096]),
        yenc_name: "stalled.bin".to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(8192, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    pipeline.fail_job(job_id, "forced failure".to_string());

    assert!(!pipeline.write_buffers.contains_key(&file_id));
    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .write_buffered_segments
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn quiescent_tail_flush_completes_data_file_with_only_recovery_left() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20006);
    let spec = JobSpec {
        name: "Tail Flush".to_string(),
        password: None,
        total_bytes: 112,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "episode.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 64,
                    message_id: "data@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 16,
                    message_id: "index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 32,
                    message_id: "repair@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let buffered = BufferedDecodedSegment {
        segment_id: SegmentId {
            file_id,
            segment_number: 0,
        },
        decoded_size: 64,
        crc32: 0,
        data: DecodedChunk::from(vec![9u8; 64]),
        yenc_name: "episode.bin".to_string(),
    };
    let buffered_len = buffered.len_bytes();
    pipeline
        .write_buffers
        .entry(file_id)
        .or_insert_with(|| WriteReorderBuffer::new(4))
        .insert(0, buffered);
    pipeline.note_write_buffered(buffered_len, 1);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state
        .assembly
        .file_mut(NzbFileId {
            job_id,
            file_index: 1,
        })
        .unwrap()
        .commit_segment(0, 16)
        .unwrap();
    state.download_queue = DownloadQueue::new();
    assert_eq!(state.recovery_queue.len(), 1);

    pipeline.flush_quiescent_write_backlog().await;

    assert_eq!(pipeline.write_buffered_bytes, 0);
    assert_eq!(pipeline.write_buffered_segments, 0);
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn out_of_order_rar_completion_keeps_pending_continuation_until_start_arrives() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30001);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Snapshot Topology", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;

    assert_eq!(member_span(&pipeline, job_id, "show", "E02.mkv"), None);
    assert_eq!(
        unresolved_spans(&pipeline, job_id, "show"),
        vec![(1, 1), (3, 3)]
    );
    let fact_volumes: Vec<u32> = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .unwrap()
        .iter()
        .map(|(volume, _)| *volume)
        .collect();
    assert_eq!(fact_volumes, vec![0, 1, 3]);

    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;

    assert_eq!(
        member_span(&pipeline, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert!(unresolved_spans(&pipeline, job_id, "show").is_empty());
    let fact_volumes: Vec<u32> = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .unwrap()
        .iter()
        .map(|(volume, _)| *volume)
        .collect();
    assert_eq!(fact_volumes, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn eager_delete_preserves_later_member_volumes_after_out_of_order_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30002);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Eager Delete", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in [
        (0usize, &files[0]),
        (1, &files[1]),
        (3, &files[3]),
        (2, &files[2]),
    ] {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    pipeline.try_delete_volumes(job_id, "show");

    let deletion_eligible = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .map(|plan| plan.deletion_eligible.clone())
        .unwrap();
    assert!(deletion_eligible.contains(&0));
    assert!(deletion_eligible.contains(&1));
    assert!(!deletion_eligible.contains(&2));
    assert!(!deletion_eligible.contains(&3));
    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
    assert!(working_dir.join("show.part03.rar").exists());
    assert!(working_dir.join("show.part04.rar").exists());
    // Wait for fire-and-forget spawn_blocking db writes to complete.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let deleted_rows = pipeline.db.load_deleted_volume_statuses(job_id).unwrap();
    assert_eq!(
        deleted_rows,
        vec![("show".to_string(), 0), ("show".to_string(), 1)]
    );
}

#[tokio::test]
async fn eager_delete_waits_for_par2_verification_before_removing_rar_sources() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let files = build_multifile_multivolume_rar_set();
    let mut spec = rar_job_spec("RAR Eager Delete Waits For PAR2", &files);
    let par2_bytes = [0x50, 0x41, 0x52, 0x32];
    spec.total_bytes = spec.total_bytes.saturating_add(par2_bytes.len() as u64);
    spec.files.push(FileSpec {
        filename: "repair.par2".to_string(),
        role: FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0,
        },
        groups: vec!["alt.binaries.test".to_string()],
        segments: vec![SegmentSpec {
            number: 0,
            bytes: par2_bytes.len() as u32,
            message_id: "rar-par2-index@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part01.rar").exists());
    assert!(working_dir.join("show.part02.rar").exists());
    assert!(
        pipeline
            .eagerly_deleted
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );

    pipeline.par2_verified.insert(job_id);
    pipeline.try_delete_volumes(job_id, "show");

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
}

#[tokio::test]
async fn restore_job_reuses_persisted_rar_volume_facts_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Snapshot Restore", &files);
    let job_id = JobId(30003);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

        for (file_index, (filename, bytes)) in [
            (0usize, &files[0]),
            (1, &files[1]),
            (3, &files[3]),
            (2, &files[2]),
        ] {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .db
            .add_extracted_member(job_id, "E01.mkv", &working_dir.join("E01.mkv"))
            .unwrap();
        pipeline.try_delete_volumes(job_id, "show");
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    let committed_segments = Pipeline::all_segment_ids(job_id, &spec);
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: ["E01.mkv".to_string()].into_iter().collect(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    assert_eq!(restored.try_update_archive_topology_calls, 0);

    assert_eq!(
        member_span(&restored, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&restored, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert_eq!(
        restored
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .map(|facts| facts.len()),
        Some(4)
    );
    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
    assert!(working_dir.join("show.part03.rar").exists());
    assert!(working_dir.join("show.part04.rar").exists());
}

#[tokio::test]
async fn add_job_records_streamed_nzb_hash_in_active_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30036);
    let spec = standalone_job_spec("Streamed Hash", &[("episode.mkv".to_string(), 123)]);
    let xml = sample_nzb_bytes();
    tokio::fs::create_dir_all(&pipeline.nzb_dir).await.unwrap();
    let nzb_path = pipeline.nzb_dir.join(format!("{}.nzb", job_id.0));
    let compressed = zstd::bulk::compress(&xml, 3).unwrap();
    tokio::fs::write(&nzb_path, &compressed).await.unwrap();

    let expected_hash = crate::ingest::hash_persisted_nzb(&nzb_path).unwrap();

    pipeline.add_job(job_id, spec, nzb_path).await.unwrap();

    let conn = rusqlite::Connection::open(temp_dir.path().join("weaver.db")).unwrap();
    let stored_hash: Vec<u8> = conn
        .query_row(
            "SELECT nzb_hash FROM active_jobs WHERE job_id = ?1",
            rusqlite::params![job_id.0 as i64],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_hash, expected_hash);
}

#[tokio::test]
async fn record_job_history_purges_terminal_job_runtime_and_queue_metrics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Terminal Runtime Cleanup", &files);
    let job_id = JobId(30033);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.update_queue_metrics();
    assert!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed)
            > 0
    );

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
    pipeline.record_job_history(job_id);

    assert!(pipeline.db.load_active_jobs().unwrap().is_empty());
    let history = pipeline.db.get_job_history(job_id.0).unwrap();
    assert!(history.is_some());
    assert_eq!(history.unwrap().status, "complete");
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(
        pipeline
            .metrics
            .download_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .recovery_queue_depth
            .load(Ordering::Relaxed),
        0
    );
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .any(|job| job.job_id == job_id)
    );
}

#[tokio::test]
async fn record_job_history_retains_failed_job_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30034);
    let spec = standalone_job_spec(
        "Failed History Retention",
        &[("episode.mkv".to_string(), 123)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::create_dir_all(&pipeline.nzb_dir).await.unwrap();
    let nzb_path = pipeline.nzb_dir.join(format!("{}.nzb", job_id.0));
    tokio::fs::write(&nzb_path, b"persisted").await.unwrap();

    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Failed {
        error: "boom".to_string(),
    };
    pipeline.record_job_history(job_id);

    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(
        history.nzb_path.as_deref(),
        Some(nzb_path.to_str().unwrap())
    );
    assert!(nzb_path.exists());
}

#[tokio::test]
async fn swapped_rar_volume_arrival_uses_parsed_volume_identity_for_claims() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30032);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swapped Live Mapping", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    // `show.part04.rar` arrives first but actually contains logical volume 2.
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

    let key = (job_id, "show".to_string());
    let plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .expect("RAR plan should exist after swapped volume arrival");
    assert!(
        plan.delete_decisions
            .values()
            .all(|decision| !decision.owners.is_empty())
    );
    assert!(plan.waiting_on_volumes.contains(&3));
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part04.rar")
    );

    // The counterpart arrives under the opposite filename and should complete the mapping.
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;

    let plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .expect("RAR plan should exist after swapped pair arrival");
    assert!(
        plan.delete_decisions
            .values()
            .all(|decision| !decision.owners.is_empty())
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part04.rar")
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
        Some("show.part03.rar")
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
}

#[tokio::test]
async fn restore_job_reloads_par2_metadata_from_disk_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let par2_filename = "repair.par2";
    let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
    let spec = par2_only_job_spec("PAR2 Restore", par2_filename, par2_bytes.len() as u32);
    let job_id = JobId(30030);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(par2_filename), &par2_bytes)
            .await
            .unwrap();
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: Pipeline::all_segment_ids(
                job_id,
                &JobSpec {
                    name: "PAR2 Restore".to_string(),
                    password: None,
                    total_bytes: par2_bytes.len() as u64,
                    category: None,
                    metadata: vec![],
                    files: vec![FileSpec {
                        filename: par2_filename.to_string(),
                        role: FileRole::from_filename(par2_filename),
                        groups: vec!["alt.binaries.test".to_string()],
                        segments: vec![SegmentSpec {
                            number: 0,
                            bytes: par2_bytes.len() as u32,
                            message_id: "par2-0@example.com".to_string(),
                        }],
                    }],
                },
            ),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert!(restored.par2_set(job_id).is_some());
    let par2_set = restored.par2_set(job_id).unwrap();
    assert_eq!(par2_set.files.len(), 1);
    assert_eq!(par2_set.recovery_block_count(), 0);
}

#[tokio::test]
async fn par2_metadata_immediately_rebinds_obfuscated_rar_file_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30111);
    let canonical_filename = "show.part001.rar";
    let obfuscated_filename = "51273aad56a8b904e96928935278a627.101";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let par2_filename = "repair.par2";
    let par2_bytes = build_test_par2_index(canonical_filename, &rar_bytes, 8);
    let spec = JobSpec {
        name: "PAR2 Canonical Rebind".to_string(),
        password: None,
        total_bytes: (rar_bytes.len() + par2_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: obfuscated_filename.to_string(),
                role: FileRole::from_filename(obfuscated_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: rar_bytes.len() as u32,
                    message_id: "rar-obfuscated@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: par2_filename.to_string(),
                role: FileRole::from_filename(par2_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 0, obfuscated_filename, &rar_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 1, par2_filename, &par2_bytes).await;
    pipeline
        .try_load_par2_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 1,
            },
        )
        .await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("data file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(matches!(
        identity
            .classification
            .as_ref()
            .map(|classification| &classification.kind),
        Some(crate::jobs::assembly::DetectedArchiveKind::Rar)
    ));
    assert!(!working_dir.join(obfuscated_filename).exists());
    assert!(working_dir.join(canonical_filename).exists());

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("PAR2 rebinding should rebuild RAR topology");
    assert!(topology.volume_map.contains_key(canonical_filename));
    assert!(!topology.volume_map.contains_key(obfuscated_filename));
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn par2_metadata_records_canonical_name_without_phantom_current_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30113);
    let canonical_filename = "show.part001.rar";
    let source_filename = "incoming.part001.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = JobSpec {
        name: "PAR2 Canonical Before File Completion".to_string(),
        password: None,
        total_bytes: rar_bytes.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: source_filename.to_string(),
            role: FileRole::from_filename(source_filename),
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![SegmentSpec {
                number: 0,
                bytes: rar_bytes.len() as u32,
                message_id: "rar-before-complete@example.com".to_string(),
            }],
        }],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes.clone())]),
        &[],
    );

    pipeline.retry_par2_authoritative_identity(job_id).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("PAR2 should still bind identity by RAR volume number");
    assert_eq!(identity.current_filename, source_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(!working_dir.join(canonical_filename).exists());

    write_and_complete_file(&mut pipeline, job_id, 0, source_filename, &rar_bytes).await;
    pipeline.retry_par2_authoritative_identity(job_id).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("data file identity should remain persisted");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert!(!working_dir.join(source_filename).exists());
    assert!(working_dir.join(canonical_filename).exists());
}

#[tokio::test]
async fn authoritative_par2_identity_clears_preexisting_stale_rar_set_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30131);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Authoritative PAR2 Identity Stale Set",
            &[(old_filename.to_string(), rar_bytes.clone())],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, old_set_name.to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(old_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: vec![crate::jobs::assembly::ArchiveMember {
                        name: "E01.mkv".to_string(),
                        first_volume: 0,
                        last_volume: 1,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes)]),
        &[],
    );

    pipeline.retry_par2_authoritative_identity(job_id).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.classification.as_ref().map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
    assert!(!pipeline.rar_sets.contains_key(&(job_id, old_set_name.to_string())));
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
}

#[tokio::test]
async fn restore_job_scrubs_stale_par2_rar_set_state_before_rar_runtime_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30132);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = rar_job_spec(
        "Restore Stale PAR2 RAR Set",
        &[(old_filename.to_string(), rar_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    write_and_complete_file(&mut pipeline, job_id, 0, old_filename, &rar_bytes).await;
    std::fs::rename(
        working_dir.join(old_filename),
        working_dir.join(canonical_filename),
    )
    .unwrap();

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    let encoded_facts = rmp_serde::to_vec_named(&dummy_rar_volume_facts(0)).unwrap();
    pipeline
        .db
        .save_rar_volume_facts(job_id, old_set_name, 0, &encoded_facts)
        .unwrap();

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec: spec.clone(),
            committed_segments: Pipeline::all_segment_ids(job_id, &spec),
            file_progress: recovered.file_progress,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let identity = restored
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist after restore");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.classification.as_ref().map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(!restored.rar_sets.contains_key(&(job_id, old_set_name.to_string())));
    assert!(restored.rar_sets.contains_key(&(job_id, "show".to_string())));
    assert!(!restored
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .contains_key(old_set_name));
}

#[tokio::test]
async fn yenc_source_name_is_treated_as_expected_after_par2_rebind() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30128);
    let canonical_filename = "show.part001.rar";
    let source_filename = "incoming.part001.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = JobSpec {
        name: "PAR2 Rebind Preserves Source Filename".to_string(),
        password: None,
        total_bytes: rar_bytes.len() as u64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: source_filename.to_string(),
            role: FileRole::from_filename(source_filename),
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![SegmentSpec {
                number: 0,
                bytes: rar_bytes.len() as u32,
                message_id: "rar-source-name@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes.clone())]),
        &[],
    );

    write_and_complete_file(&mut pipeline, job_id, 0, source_filename, &rar_bytes).await;
    pipeline.retry_par2_authoritative_identity(job_id).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert!(pipeline.yenc_name_matches_rewritten_source(
        job_id,
        file_id,
        source_filename,
        canonical_filename,
    ));
}

#[tokio::test]
async fn par2_metadata_rebinds_obfuscated_rar_after_late_content_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30112);
    let canonical_files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = canonical_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 101),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("PAR2 Late Canonical Rebind", &obfuscated_files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&canonical_files),
        &[],
    );

    for (index, ((_, _), (obfuscated_filename, bytes))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        write_and_complete_file(
            &mut pipeline,
            job_id,
            index as u32,
            obfuscated_filename,
            bytes,
        )
        .await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }

    for (index, ((canonical_filename, _), (obfuscated_filename, _))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        let identity = pipeline
            .file_identity(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: index as u32,
                },
            )
            .cloned()
            .expect("data file identity should exist");
        assert_eq!(identity.current_filename, *canonical_filename);
        assert_eq!(
            identity.canonical_filename.as_deref(),
            Some(canonical_filename.as_str())
        );
        assert_eq!(identity.classification_source, FileIdentitySource::Par2);
        assert!(!working_dir.join(obfuscated_filename).exists());
        assert!(working_dir.join(canonical_filename).exists());
    }

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("late PAR2 rebinding should rebuild RAR topology");
    for (canonical_filename, _) in &canonical_files {
        assert!(topology.volume_map.contains_key(canonical_filename));
    }
    for (obfuscated_filename, _) in &obfuscated_files {
        assert!(!topology.volume_map.contains_key(obfuscated_filename));
    }
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive late canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn par2_set_name_rebind_keeps_encrypted_multivolume_member_span_ready() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30133);
    let source_set_name = "incoming";
    let canonical_set_name = "video";
    let member_name = "test_clip.mkv";
    let canonical_files = vec![
        (
            "video.part001.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part1.rar"),
        ),
        (
            "video.part002.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part2.rar"),
        ),
        (
            "video.part003.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part3.rar"),
        ),
        (
            "video.part004.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part4.rar"),
        ),
        (
            "video.part005.rar".to_string(),
            rar5_fixture_bytes("rar5_enc_mv_video.part5.rar"),
        ),
    ];
    let source_files: Vec<(String, Vec<u8>)> = canonical_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("incoming.part{:03}.rar", index + 1),
                bytes.clone(),
            )
        })
        .collect();
    let mut spec = rar_job_spec("PAR2 Rebind Encrypted Boundary", &source_files);
    spec.password = Some("testpass123".to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&canonical_files),
        &[],
    );

    for (index, (source_filename, bytes)) in source_files.iter().enumerate().take(4) {
        write_and_complete_rar_volume(
            &mut pipeline,
            job_id,
            index as u32,
            source_filename,
            bytes,
        )
        .await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, canonical_set_name)
        .expect("canonical encrypted snapshot should exist after four rebound volumes");
    let mut cached = serde_json::to_value(
        rmp_serde::from_slice::<weaver_rar::CachedArchiveHeaders>(&cached_headers).unwrap(),
    )
    .unwrap();
    let clip = cached["members"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|member| member["name"] == member_name)
        .expect("cached snapshot should contain the encrypted clip member");
    let first_segment = clip["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("encrypted clip should keep its first segment");
    clip["segments"] = serde_json::json!([first_segment]);
    clip["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<weaver_rar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    pipeline
        .rar_sets
        .get_mut(&(job_id, canonical_set_name.to_string()))
        .expect("canonical set should exist after PAR2 rebind")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, canonical_set_name, &stale_headers)
        .unwrap();

    write_and_complete_rar_volume(
        &mut pipeline,
        job_id,
        4,
        &source_files[4].0,
        &source_files[4].1,
    )
    .await;
    pipeline.retry_par2_authoritative_identity(job_id).await;

    assert_eq!(
        member_span(&pipeline, job_id, canonical_set_name, member_name),
        Some((0, 4))
    );
    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, canonical_set_name);
    let selected = pipeline.volume_paths_for_rar_members(
        job_id,
        canonical_set_name,
        &[member_name.to_string()],
        &volume_paths,
        true,
        false,
    );
    assert_eq!(selected.keys().copied().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, source_set_name.to_string())),
        "the pre-rebind RAR set should not survive after canonical migration"
    );
}

#[tokio::test]
async fn waiting_for_missing_volumes_ignores_stale_noncurrent_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30129);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Ignore Stale RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xAB; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "51273aad56a8b904e96928935278a627.101".to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "51273aad56a8b904e96928935278a627".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(
                        "51273aad56a8b904e96928935278a627.101".to_string(),
                        0,
                    )]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn waiting_for_missing_volumes_still_tracks_current_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30130);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Track Current RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xCD; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: canonical_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(canonical_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_rar_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30113);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Clean PAR2 RAR Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(done_job_id, job_id);
            assert!(!attempted.is_empty());
            assert!(result.is_ok());
        }
        _ => panic!("expected RAR extraction batch"),
    }
}

#[tokio::test]
async fn clean_par2_quick_verification_completes_direct_payload_without_authoritative_verify() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30114);
    let payload_filename = "payload.mkv";
    let payload: Vec<u8> = (0..32u32).map(|value| (value % 251) as u8).collect();
    let spec = standalone_job_spec(
        "Clean Direct Payload Quick Verify",
        &[(payload_filename.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));

    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(job_status_for_assert(&pipeline, job_id), Some(JobStatus::Complete));
}

#[tokio::test]
async fn clean_par2_quick_verification_exits_verifying_for_split_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30115);
    let files = vec![
        ("archive.001".to_string(), b"hello ".to_vec()),
        ("archive.002".to_string(), b"world".to_vec()),
    ];
    let spec = rar_job_spec("Clean PAR2 Split Verify Starts Join", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(done_job_id, job_id);
            assert_eq!(set_name, "archive");
            assert!(result.is_ok());
        }
        _ => panic!("expected split join extraction result"),
    }
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30116);
    let files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let spec = rar_job_spec("Clean PAR2 7z Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            result,
            ..
        } => {
            assert_eq!(done_job_id, job_id);
            assert!(result.is_ok());
        }
        _ => panic!("expected 7z extraction result"),
    }
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_single_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30117);
    let archive_filename = "archive.7z";
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = standalone_job_spec(
        "Clean PAR2 Single 7z Verify Starts Extraction",
        &[(archive_filename.to_string(), seven_zip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), seven_zip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    persist_completed_file_hash(&pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_err());
        }
        _ => panic!("expected single 7z extraction result"),
    }
}

#[tokio::test]
async fn corrupt_single_sevenz_enters_authoritative_par2_verification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30127);
    let archive_filename = "archive.7z";
    let original_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let mut damaged_bytes = original_bytes.clone();
    damaged_bytes[7] ^= 0xFF;
    let spec = standalone_job_spec(
        "Corrupt PAR2 Single 7z Requires Verify",
        &[(archive_filename.to_string(), damaged_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), original_bytes)]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &damaged_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(!pipeline.inflight_extractions.contains_key(&job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_gzip_extraction() {
    use std::io::Write;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30118);
    let archive_filename = "payload.gz";
    let payload = b"gzip finalize payload";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).unwrap();
    let gzip_bytes = encoder.finish().unwrap();
    let spec = standalone_job_spec(
        "Clean PAR2 Gzip Verify Starts Extraction",
        &[(archive_filename.to_string(), gzip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), gzip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &gzip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Gz,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "payload".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: payload.len() as u64,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_ok());
        }
        _ => panic!("expected gzip extraction result"),
    }
}

#[tokio::test]
async fn clean_par2_tar_requires_authoritative_verify_without_hash_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30119);
    let archive_filename = "payload.tar";
    let payload = b"plain tar payload";

    let tar_bytes = {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(payload.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, "payload.bin", &payload[..])
            .unwrap();
        builder.into_inner().unwrap()
    };

    let spec = standalone_job_spec(
        "Clean PAR2 Tar Requires Verify",
        &[(archive_filename.to_string(), tar_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), tar_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &tar_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Tar,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "payload.bin".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: payload.len() as u64,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
}

#[tokio::test]
async fn finalize_completed_file_hash_falls_back_to_disk_after_out_of_order_stream() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30117);
    let payload_filename = "payload.bin";
    let payload = b"abcdefgh";
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let file_path = temp_dir.path().join(payload_filename);
    tokio::fs::write(&file_path, payload).await.unwrap();

    pipeline.note_file_hash_chunk(file_id, 4, &payload[4..8]);
    pipeline.note_file_hash_chunk(file_id, 0, &payload[0..4]);

    let hash = pipeline
        .finalize_completed_file_hash(file_id, file_path, payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(hash, weaver_par2::checksum::md5(payload));
}

#[tokio::test]
async fn reprocess_job_rebuilds_failed_history_from_streamed_persisted_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30037);
    let nzb_path = pipeline.nzb_dir.join(format!("{}.nzb", job_id.0));
    tokio::fs::create_dir_all(&pipeline.nzb_dir).await.unwrap();
    tokio::fs::write(
        &nzb_path,
        zstd::bulk::compress(&sample_nzb_bytes(), 3).unwrap(),
    )
    .await
    .unwrap();
    pipeline.finished_jobs.push(JobInfo {
        job_id,
        name: "Failed History Job".to_string(),
        status: JobStatus::Failed {
            error: "boom".to_string(),
        },
        download_state: crate::jobs::model::DownloadState::Failed,
        post_state: crate::jobs::model::PostState::Failed,
        run_state: crate::jobs::model::RunState::Active,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 0,
        password: None,
        category: Some("tv".to_string()),
        metadata: vec![("source".to_string(), "history".to_string())],
        output_dir: None,
        error: Some("boom".to_string()),
        created_at_epoch_ms: 0.0,
    });

    pipeline.reprocess_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Extracting);
    assert_eq!(state.spec.files.len(), 1);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert_eq!(
        state.spec.metadata,
        vec![
            ("source".to_string(), "history".to_string()),
            ("weaver.original_title".to_string(), job_id.0.to_string()),
        ]
    );
    assert!(state.download_queue.is_empty());
}

#[tokio::test]
async fn redownload_job_rebuilds_failed_history_as_queued_download() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30038);
    let nzb_path = pipeline.nzb_dir.join(format!("{}.nzb", job_id.0));
    let working_dir = intermediate_dir.join("failed-redownload-job");
    let staging_dir = pipeline
        .complete_dir
        .join(".weaver-staging")
        .join(job_id.0.to_string());

    tokio::fs::create_dir_all(&pipeline.nzb_dir).await.unwrap();
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();
    tokio::fs::write(working_dir.join("partial.mkv"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(staging_dir.join("partial.srt"), b"partial")
        .await
        .unwrap();
    tokio::fs::write(
        &nzb_path,
        zstd::bulk::compress(&sample_nzb_bytes(), 3).unwrap(),
    )
    .await
    .unwrap();

    let mut row =
        history_row_with_output_dir(job_id, "Failed History Job", "failed", working_dir.clone());
    row.category = Some("tv".to_string());
    row.nzb_path = Some(nzb_path.display().to_string());
    row.metadata = Some(serde_json::to_string(&vec![("source", "history")]).unwrap());
    pipeline.db.insert_job_history(&row).unwrap();
    pipeline.finished_jobs.push(JobInfo {
        job_id,
        name: "Failed History Job".to_string(),
        status: JobStatus::Failed {
            error: "boom".to_string(),
        },
        download_state: crate::jobs::model::DownloadState::Failed,
        post_state: crate::jobs::model::PostState::Failed,
        run_state: crate::jobs::model::RunState::Active,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 0,
        password: None,
        category: Some("tv".to_string()),
        metadata: vec![("source".to_string(), "history".to_string())],
        output_dir: Some(working_dir.display().to_string()),
        error: Some("boom".to_string()),
        created_at_epoch_ms: 0.0,
    });

    pipeline.redownload_job(job_id).await.unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Queued);
    assert!(!state.download_queue.is_empty());
    assert_eq!(state.downloaded_bytes, 0);
    assert_eq!(state.spec.category.as_deref(), Some("tv"));
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
    assert!(
        pipeline
            .finished_jobs
            .iter()
            .all(|job| job.job_id != job_id)
    );
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
}

#[tokio::test]
async fn restore_job_uses_per_file_progress_floor_for_reporting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30035);
    let spec = standalone_job_spec(
        "Restore Progress Floor",
        &[("a.bin".to_string(), 100), ("b.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-progress-floor");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    let committed_segments = HashSet::from([SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    }]);
    let file_progress = HashMap::from([(0u32, 40u64), (1u32, 80u64)]);

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress,
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.downloaded_bytes, 100);
    assert_eq!(state.restored_download_floor_bytes, 180);
    assert_eq!(Pipeline::effective_downloaded_bytes(state), 180);
    assert!((Pipeline::effective_progress(state) - 0.9).abs() < f64::EPSILON);
}

#[tokio::test]
async fn restore_job_reapplies_only_promoted_recovery_segments() {
    let temp_dir = tempfile::tempdir().unwrap();
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
    let spec = JobSpec {
        name: "PAR2 Promote Restore".to_string(),
        password: None,
        total_bytes: par2_bytes.len() as u64 + 64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "par2-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 64,
                    message_id: "par2-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let job_id = JobId(30032);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        tokio::fs::write(working_dir.join(index_filename), &par2_bytes)
            .await
            .unwrap();
        pipeline
            .db
            .upsert_par2_file(job_id, 1, recovery_filename, 1, true)
            .unwrap();
        working_dir
    };

    let committed_segments = [SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    }]
    .into_iter()
    .collect();

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert!(restored.par2_set(job_id).is_some());
    assert_eq!(
        restored
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&1))
            .map(|file| (file.recovery_blocks, file.promoted)),
        Some((1, true))
    );

    let state = restored.jobs.get_mut(&job_id).unwrap();
    let mut queued = state.download_queue.drain_all();
    queued.sort_by_key(|work| work.segment_id.file_id.file_index);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].segment_id.file_id.file_index, 1);
    assert!(state.recovery_queue.is_empty());
}

#[tokio::test]
async fn restore_job_rehydrates_failed_members_and_verified_suspect_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Restart Runtime Restore", &files);
    let job_id = JobId(30034);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .db
            .add_failed_extraction(job_id, "E10.mkv")
            .unwrap();
        pipeline
            .db
            .add_failed_extraction(job_id, "E15.mkv")
            .unwrap();
        pipeline
            .db
            .set_active_job_normalization_retried(job_id, true)
            .unwrap();
        pipeline
            .db
            .replace_verified_suspect_volumes(
                job_id,
                "show",
                &std::collections::HashSet::from([1u32, 2u32]),
            )
            .unwrap();
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: Pipeline::all_segment_ids(
                job_id,
                &rar_job_spec("RAR Restart Runtime Restore", &files),
            ),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        restored.failed_extractions.get(&job_id).cloned(),
        Some(HashSet::from([
            "E10.mkv".to_string(),
            "E15.mkv".to_string(),
        ]))
    );
    assert!(restored.normalization_retried.contains(&job_id));
    assert_eq!(
        restored
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.verified_suspect_volumes.clone()),
        Some(HashSet::from([1u32, 2u32]))
    );
}

#[tokio::test]
async fn restore_job_skips_eager_delete_for_ownerless_restored_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let empty_rar = build_empty_rar_volume();
    let files = vec![("ownerless.part01.rar".to_string(), empty_rar)];
    let spec = rar_job_spec("RAR Ownerless Restore", &files);
    let job_id = JobId(30031);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: Pipeline::all_segment_ids(
                job_id,
                &rar_job_spec("RAR Ownerless Restore", &files),
            ),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    assert!(working_dir.join("ownerless.part01.rar").exists());
    assert!(
        !restored
            .eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains("ownerless.part01.rar"))
    );
    let plan = restored
        .rar_sets
        .get(&(job_id, "ownerless".to_string()))
        .and_then(|state| state.plan.as_ref())
        .expect("ownerless RAR restore should produce a plan");
    let decision = plan
        .delete_decisions
        .get(&0)
        .expect("ownerless restore should keep volume 0 audited");
    assert!(decision.owners.is_empty());
    assert!(!decision.ownership_eligible);
}

#[tokio::test]
async fn normalization_refresh_rebuilds_rar_snapshot_from_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30004);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalize Refresh", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in [
        (0usize, &files[0]),
        (1, &files[1]),
        (3, &files[3]),
        (2, &files[2]),
    ] {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::write(working_dir.join("show.part03.rar"), &files[2].1)
        .await
        .unwrap();
    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string()].into_iter().collect(),
        )
        .await
        .unwrap();

    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .contains_key("show")
    );
}

#[tokio::test]
async fn live_rebuild_failure_retains_previous_rar_volume_facts_and_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30005);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebuild Failure", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let original_facts = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .cloned()
        .expect("good facts should be persisted");

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part01.rar").exists());
    assert!(working_dir.join("show.part02.rar").exists());

    let corrupt_part04 = vec![0u8; files[3].1.len()];
    tokio::fs::write(working_dir.join(&files[3].0), &corrupt_part04)
        .await
        .unwrap();

    pipeline
        .try_update_archive_topology(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .await;

    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .cloned(),
        Some(original_facts)
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
}

#[tokio::test]
async fn incremental_rar_batches_survive_eager_delete_of_earlier_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30006);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Batches", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let first_done = next_extraction_done(&mut pipeline).await;
    match &first_done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(first_done).await;

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;
    pipeline.try_rar_extraction(job_id).await;

    let second_done = next_extraction_done(&mut pipeline).await;
    match &second_done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(second_done).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn non_solid_incremental_rar_batches_cleanup_chunks_after_finalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30007);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Chunks", &files);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let staging_dir = pipeline.extraction_staging_dir(job_id);

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    let chunks = pipeline.db.get_extraction_chunks(job_id, "show").unwrap();
    assert!(chunks.iter().all(|chunk| chunk.member_name != "E01.mkv"));
    assert!(staging_dir.join("E01.mkv").exists());
    assert!(
        !staging_dir
            .join(".weaver-chunks")
            .join("show")
            .join("E01.mkv")
            .exists()
    );
}

#[tokio::test]
async fn non_solid_rar_set_dispatches_two_members_concurrently() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30008);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Concurrent Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(
        attempted_members,
        vec!["E01.mkv".to_string(), "E02.mkv".to_string()]
    );

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn rar_eager_delete_waits_for_all_active_workers_in_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30009);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Delete Waits For Workers", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(2)
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;

    pipeline.handle_extraction_done(first_done).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(1)
    );
    for (filename, _) in &files {
        assert!(
            working_dir.join(filename).exists(),
            "{filename} should not be eagerly deleted while another set worker is active"
        );
    }

    pipeline.handle_extraction_done(second_done).await;
    for (filename, _) in &files {
        assert!(
            !working_dir.join(filename).exists(),
            "{filename} should be eagerly deleted after all set workers finish"
        );
    }
}

#[tokio::test]
async fn non_solid_rar_scheduler_skips_duplicate_ready_members() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30010);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Duplicate Ready Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("set state should exist");
    let plan = set_state
        .plan
        .as_mut()
        .expect("ready plan should exist after all volumes complete");
    let first = plan.ready_members[0].name.clone();
    let second = plan.ready_members[1].name.clone();
    plan.ready_members = vec![
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: second.clone(),
        },
    ];

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        [first.clone(), second.clone()].into_iter().collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(attempted_members, vec![first, second]);

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn rar_identity_rebind_preserves_in_flight_workers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30012);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Preserves Workers", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let expected_in_flight: std::collections::HashSet<String> =
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect();
    let set_key = (job_id, "show".to_string());
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);

    let touched_filenames = [files[0].0.clone()].into_iter().collect();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive identity rebind");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);
    assert!(set_state.plan.is_none());

    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist after completions");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());
}

#[tokio::test]
async fn rar_identity_rebind_removes_empty_set_after_in_flight_workers_finish() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Removes Empty Set", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_key = (job_id, "show".to_string());
    let touched_filenames = files
        .iter()
        .map(|(filename, _)| filename.clone())
        .collect::<HashSet<_>>();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive rebind while extraction workers are still running");
    assert!(set_state.volume_files.is_empty());
    assert!(set_state.active_workers > 0);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;

    assert!(
        !pipeline.rar_sets.contains_key(&set_key),
        "fully invalidated RAR set should be purged once in-flight workers finish"
    );
}

#[tokio::test]
async fn non_solid_rar_incremental_requires_member_chain_not_download_activity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Readiness", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    assert!(!pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after first RAR volume");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());

    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after second RAR volume");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string()].into_iter().collect()
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn non_solid_rar_incremental_uses_ready_plan_even_if_complete_volumes_lag() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Ready Plan Wins", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("set state should exist after all RAR volumes complete");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all RAR volumes complete");
        plan.ready_members = vec![crate::pipeline::rar_state::RarReadyMember {
            name: "E02.mkv".to_string(),
        }];
        plan.topology.complete_volumes = [2u32].into_iter().collect();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut topology = state
            .assembly
            .archive_topology_for("show")
            .cloned()
            .expect("assembly topology should exist for completed RAR set");
        topology.complete_volumes = [2u32].into_iter().collect();
        state.assembly.set_archive_topology("show".to_string(), topology);
    }

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E02.mkv"));

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist while retry is in flight");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(set_state.in_flight_members, ["E02.mkv".to_string()].into_iter().collect());

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn solid_rar_keeps_later_members_ready_after_earlier_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30009);
    let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../engines/weaver-rar/tests/fixtures/rar5/rar5_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();
    let archive =
        weaver_rar::RarArchive::open(std::fs::File::open(&fixture_path).unwrap()).unwrap();
    let member_names = archive.member_names();
    assert!(member_names.len() >= 3);

    let spec = rar_job_spec(
        "Solid Failure Continuation",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    // Rebuild with extracted + failed state that mirrors a solid archive
    // where earlier members were attempted before later members.
    pipeline
        .extracted_members
        .insert(job_id, [member_names[0].to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, [member_names[1].to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "solid")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "solid".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("solid set plan should exist");

    assert!(plan.is_solid);
    assert_eq!(plan.phase, crate::pipeline::rar_state::RarSetPhase::Ready);
    let ready_members: Vec<String> = plan
        .ready_members
        .into_iter()
        .map(|member| member.name)
        .collect();
    assert_eq!(
        ready_members,
        member_names[2..]
            .iter()
            .map(|member| member.to_string())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn solid_rar4_pipeline_extracts_large_fixture_end_to_end() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30010);
    let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../engines/weaver-rar/tests/fixtures/rar4/rar4_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();

    let spec = rar_job_spec(
        "RAR4 Solid End-to-End",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR4 Solid End-to-End",
    ));
    let dest_entries = std::fs::read_dir(&dest)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let working_entries = std::fs::read_dir(&working_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "job status: {:?}, dest entries: {:?}, working entries: {:?}",
        job_status_for_assert(&pipeline, job_id),
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("sample.mkv").exists(),
        "dest entries: {:?}, working entries: {:?}",
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("file1.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(
        dest.join("file2.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(!working_dir.join("solid.rar").exists());
}

#[tokio::test]
async fn check_job_completion_retains_par2_until_rar_extraction_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Keeps PAR2 During Extraction", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(4).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after volume facts are built");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.recovery_blocks),
        Some(1)
    );
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn check_job_completion_defers_verify_while_rar_workers_are_active() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Barrier", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }

    // Set status to Extracting to match what the real pipeline would have
    // done when extraction workers were spawned.  The bounded workload
    // queue may gate through QueuedExtract first, but by the time workers
    // are active the status is always Extracting.
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Extracting;
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!pipeline.normalization_retried.contains(&job_id));

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 0;
    set_state.in_flight_members.insert("E02.mkv".to_string());
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn partial_rar_extraction_does_not_bypass_par2_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30012);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Partial Keeps PAR2", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    assert!(!pipeline.par2_bypassed.contains(&job_id));
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn rar_completion_prefers_incremental_batches_over_full_set_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Completion Uses Batch", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();
    pipeline.try_delete_volumes(job_id, "show");

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    pipeline.check_job_completion(job_id).await;

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        ExtractionDone::FullSet { .. } => {
            panic!("RAR completion should not fall back to full-set extraction here")
        }
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn eager_delete_exclusions_do_not_hide_suspect_deleted_rar_damage() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Keeps Suspect Deleted Volumes", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part02.rar".to_string(), "show.part04.rar".to_string()]
            .into_iter()
            .collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let mut verification = weaver_par2::VerificationResult {
        files: vec![
            weaver_par2::verify::FileVerification {
                file_id: weaver_par2::FileId::from_bytes([1; 16]),
                filename: "show.part02.rar".to_string(),
                status: weaver_par2::verify::FileStatus::Missing,
                valid_slices: vec![false; 3],
                missing_slice_count: 3,
            },
            weaver_par2::verify::FileVerification {
                file_id: weaver_par2::FileId::from_bytes([2; 16]),
                filename: "show.part04.rar".to_string(),
                status: weaver_par2::verify::FileStatus::Missing,
                valid_slices: vec![false; 2],
                missing_slice_count: 2,
            },
        ],
        recovery_blocks_available: 3,
        total_missing_blocks: 5,
        repairable: weaver_par2::verify::Repairability::Insufficient {
            blocks_needed: 5,
            blocks_available: 3,
            deficit: 2,
        },
    };

    let (skipped_blocks, retained_suspect_blocks) =
        pipeline.apply_eager_delete_exclusions(job_id, &mut verification);

    assert_eq!(skipped_blocks, 2);
    assert_eq!(retained_suspect_blocks, 3);
    assert_eq!(verification.total_missing_blocks, 3);
    assert!(matches!(
        verification.files[0].status,
        weaver_par2::verify::FileStatus::Missing
    ));
    assert_eq!(verification.files[0].missing_slice_count, 3);
    assert!(matches!(
        verification.files[1].status,
        weaver_par2::verify::FileStatus::Complete
    ));
    assert_eq!(verification.files[1].missing_slice_count, 0);
    assert!(matches!(
        verification.repairable,
        weaver_par2::verify::Repairability::Repairable {
            blocks_needed: 3,
            blocks_available: 3
        }
    ));

    pipeline.recompute_volume_safety_from_verification(job_id, &verification);

    let verified_suspect = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .unwrap_or_default();
    assert!(verified_suspect.contains(&1));
    assert!(!verified_suspect.contains(&3));
}

#[tokio::test]
async fn recoverable_full_set_extraction_error_defers_to_repair_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = standalone_job_spec(
        "Recoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to extract sample.mkv: Invalid checksum".to_string()),
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.failed_extractions.get(&job_id),
        Some(&HashSet::from(["archive.zip".to_string()]))
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );
}

#[tokio::test]
async fn nonrecoverable_full_set_extraction_error_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let spec = standalone_job_spec(
        "Nonrecoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to parse zip central directory".to_string()),
        })
        .await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn clean_verify_retries_non_rar_full_set_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec(
        "Non-RAR Extraction Retry After Verify",
        &[("archive.zip".to_string(), 16)],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join("archive.zip"), b"not-a-real-zip")
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 14)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.zip".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([("archive.zip".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    install_test_par2_runtime(&mut pipeline, job_id, minimal_par2_file_set(), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["archive.zip".to_string()].into_iter().collect());

    pipeline.check_job_completion(job_id).await;

    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet { set_name, .. } => {
            assert_eq!(set_name, "archive.zip");
        }
        _ => panic!("expected full-set extraction retry"),
    }
}

#[tokio::test]
async fn rar_state_recompute_supplements_stale_volume_registry_from_assembly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Registry Merge", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .volume_files = std::collections::BTreeMap::from([(0u32, "show.part01.rar".to_string())]);

    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(volume_paths.len(), 4);
    assert!(volume_paths.contains_key(&0));
    assert!(volume_paths.contains_key(&1));
    assert!(volume_paths.contains_key(&2));
    assert!(volume_paths.contains_key(&3));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert_eq!(plan.topology.complete_volumes.len(), 4);
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 1));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 2));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 3));
}

#[tokio::test]
async fn incremental_rar_member_extraction_uses_member_span_volume_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Member Span Window", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let all_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(all_paths.len(), 4);

    let member_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        false,
    );
    assert_eq!(member_paths.keys().copied().collect::<Vec<_>>(), vec![2, 3]);

    let without_cached_headers = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        false,
        false,
    );
    assert_eq!(without_cached_headers.len(), all_paths.len());

    let solid_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        true,
    );
    assert_eq!(solid_paths.len(), all_paths.len());
}

#[tokio::test]
async fn no_par2_full_set_failure_requeues_archive_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30026);
    let spec = JobSpec {
        name: "No PAR2 ZIP Retry".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "archive.zip".to_string(),
            role: FileRole::from_filename("archive.zip"),
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![SegmentSpec {
                number: 0,
                bytes: 128,
                message_id: "zip-0@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.zip".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([("archive.zip".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["archive.zip".to_string()]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn no_par2_single_file_retry_marks_zip_volume_complete_after_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30028);
    let spec = JobSpec {
        name: "No PAR2 ZIP Retry Refresh".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "archive.zip".to_string(),
            role: FileRole::from_filename("archive.zip"),
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![SegmentSpec {
                number: 0,
                bytes: 128,
                message_id: "zip-refresh-0@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.zip".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([("archive.zip".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["archive.zip".to_string()]));

    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.assembly.archive_topology_for("archive.zip").is_none());
    }

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
    }

    pipeline.try_update_7z_topology(job_id, file_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive.zip").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive.zip"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_single_file_retry_marks_7z_volume_complete_after_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30029);
    let spec = JobSpec {
        name: "No PAR2 7z Retry Refresh".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "archive.7z".to_string(),
            role: FileRole::from_filename("archive.7z"),
            groups: vec!["alt.binaries.test".to_string()],
            segments: vec![SegmentSpec {
                number: 0,
                bytes: 128,
                message_id: "7z-refresh-0@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.7z".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([("archive.7z".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["archive.7z".to_string()]));

    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.assembly.archive_topology_for("archive.7z").is_none());
    }

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
    }

    pipeline.try_update_7z_topology(job_id, file_id);

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive.7z").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive.7z"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn direct_payload_par2_repair_completes_after_download_exhaustion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30080);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Direct Payload PAR2 Repair".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![
                    SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "payload-0@example.com".to_string(),
                    },
                    SegmentSpec {
                        number: 1,
                        bytes: 64,
                        message_id: "payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "payload-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "payload-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Direct Payload PAR2 Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[tokio::test]
async fn direct_payload_par2_repair_verifies_complete_corrupt_payload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30087);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Complete Direct Payload PAR2 Repair".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![
                    SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "complete-payload-0@example.com".to_string(),
                    },
                    SegmentSpec {
                        number: 1,
                        bytes: 64,
                        message_id: "complete-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "complete-payload-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "complete-payload-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(drain_job_repair_complete(&mut events, job_id), 1);
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(
            "Complete Direct Payload PAR2 Repair",
        ));
    let completed_payload = tokio::fs::read(output_dir.join(payload_filename))
        .await
        .unwrap();
    assert_eq!(completed_payload, original_payload);
}

#[tokio::test]
async fn complete_payload_does_not_finalize_while_promoted_recovery_is_pending() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30088);
    let payload_filename = "payload.mkv";
    let payload = vec![0x42; 128];
    let spec = segmented_job_spec(
        "Pending Recovery Completion Guard",
        payload_filename,
        &[128],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("promoted-recovery@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.jobs.contains_key(&job_id));
    assert!(
        !pipeline
            .complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(
                "Pending Recovery Completion Guard",
            ))
            .exists()
    );
}

#[tokio::test]
async fn complete_payload_finalizes_while_optional_recovery_is_parked() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let payload_filename = "payload.mkv";
    let payload = vec![0x42; 128];
    let spec = segmented_job_spec("Optional Recovery Completion Guard", payload_filename, &[128]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("optional-recovery-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(job_status_for_assert(&pipeline, job_id), Some(JobStatus::Complete));
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Optional Recovery Completion Guard",
    ));
    assert!(output_dir.join(payload_filename).exists());
}

#[tokio::test]
async fn archive_payload_does_not_extract_while_promoted_recovery_is_pending() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let archive_filename = "archive.7z";
    let recovery_filename = "archive.7z.vol00+01.par2";
    let spec = JobSpec {
        name: "Pending Recovery Archive Extraction Guard".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: archive_filename.to_string(),
                role: FileRole::from_filename(archive_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 128,
                    message_id: "pending-recovery-archive@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 64,
                    message_id: "pending-recovery-archive-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("pending-recovery-archive-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 2,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
    assert!(!pipeline.inflight_extractions.contains_key(&job_id));
}

#[tokio::test]
async fn promoted_recovery_wait_does_not_reverify_until_recovery_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30089);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let spec = JobSpec {
        name: "Pending Targeted Recovery Verify Guard".to_string(),
        password: None,
        total_bytes: original_payload.len() as u64 + par2_bytes.len() as u64 + 64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![
                    SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "pending-recovery-payload-0@example.com".to_string(),
                    },
                    SegmentSpec {
                        number: 1,
                        bytes: 64,
                        message_id: "pending-recovery-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "pending-recovery-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 64,
                    message_id: "pending-recovery-volume@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &damaged_payload).await;
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.recovery_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("pending-recovery-volume@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            exclude_servers: Vec::new(),
        });
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, false),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    assert_eq!(drain_job_verification_started(&mut events, job_id), 1);
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.download_queue.has_recovery_work())
    );

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn direct_payload_par2_repair_fails_when_recovery_is_insufficient() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30081);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| ((value * 7) % 251) as u8).collect();
    let damaged_payload = vec![0u8; original_payload.len()];
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0x55; 64];
    let spec = JobSpec {
        name: "Direct Payload PAR2 Failure".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![
                    SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "payload-fail-0@example.com".to_string(),
                    },
                    SegmentSpec {
                        number: 1,
                        bytes: 64,
                        message_id: "payload-fail-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "payload-fail-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "payload-fail-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed when recovery blocks are insufficient");
    };
    assert!(error.contains("not repairable"));
}

#[tokio::test]
async fn generic_par2_repair_requeues_extraction_for_7z_and_gzip_payloads() {
    for (job_id, payload_filename, original_payload) in [
        (
            JobId(30082),
            "archive.7z",
            vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04],
        ),
        (
            JobId(30083),
            "payload.gz",
            vec![0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03],
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let index_filename = "repair.par2";
        let recovery_filename = "repair.vol00+01.par2";
        let damaged_payload = vec![0u8; original_payload.len()];
        let par2_bytes = build_test_par2_index(
            payload_filename,
            &original_payload,
            original_payload.len() as u64,
        );
        let recovery_bytes = vec![0xCC; original_payload.len()];
        let spec = JobSpec {
            name: format!("Archive Repair {}", job_id.0),
            password: None,
            total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: payload_filename.to_string(),
                    role: FileRole::from_filename(payload_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: original_payload.len() as u32,
                        message_id: format!("archive-{}-0@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: index_filename.to_string(),
                    role: FileRole::from_filename(index_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: par2_bytes.len() as u32,
                        message_id: format!("archive-{}-index@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: recovery_filename.to_string(),
                    role: FileRole::from_filename(recovery_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: recovery_bytes.len() as u32,
                        message_id: format!("archive-{}-recovery@example.com", job_id.0),
                    }],
                },
            ],
        };
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
            .await
            .unwrap();
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }
        write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
        write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set(
                payload_filename,
                &original_payload,
                original_payload.len() as u64,
                1,
            ),
            &[
                (1, index_filename, 0, false),
                (2, recovery_filename, 1, true),
            ],
        );

        pipeline.check_job_completion(job_id).await;

        let queued_job = pipeline
            .pending_completion_checks
            .pop_front()
            .expect("post-repair completion should be scheduled");
        assert_eq!(queued_job, job_id);
        pipeline.check_job_completion(queued_job).await;

        assert!(matches!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting | JobStatus::QueuedExtract)
        ));
        let done = next_extraction_done(&mut pipeline).await;
        match done {
            ExtractionDone::FullSet { set_name, .. } => {
                assert_eq!(set_name, payload_filename);
            }
            _ => panic!("expected full-set extraction after generic PAR2 repair"),
        }
    }
}

#[tokio::test]
async fn extracted_archive_job_finalizes_without_reverifying_missing_par2_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30084);
    let files = build_multifile_multivolume_rar_set();
    let mut spec = rar_job_spec("RAR Finalize Skips Missing PAR2 Index", &files);
    spec.total_bytes += 128;
    spec.files.push(FileSpec {
        filename: "repair.par2".to_string(),
        role: FileRole::from_filename("repair.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        segments: vec![SegmentSpec {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-index@example.com".to_string(),
        }],
    });
    spec.files.push(FileSpec {
        filename: "repair.vol00+01.par2".to_string(),
        role: FileRole::from_filename("repair.vol00+01.par2"),
        groups: vec!["alt.binaries.test".to_string()],
        segments: vec![SegmentSpec {
            number: 0,
            bytes: 64,
            message_id: "rar-finalize-par2-recovery@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(&files[0].0, &files[0].1, 64, 0),
        &[],
    );

    for (member_name, bytes) in [
        ("E01.mkv", b"episode-a-payload".as_slice()),
        ("E02.mkv", b"episode-b-payload".as_slice()),
    ] {
        let output_path = working_dir.join(member_name);
        tokio::fs::write(&output_path, bytes).await.unwrap();
        pipeline
            .db
            .add_extracted_member(job_id, member_name, &output_path)
            .unwrap();
        pipeline
            .extracted_members
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    for (filename, _) in &files {
        tokio::fs::remove_file(working_dir.join(filename))
            .await
            .unwrap();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR Finalize Skips Missing PAR2 Index",
    ));
    assert!(output_dir.join("E01.mkv").exists());
    assert!(output_dir.join("E02.mkv").exists());
}

#[tokio::test]
async fn stale_extracted_member_checkpoint_is_recovered_before_rar_completion_reconcile() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30085);
    let files = build_multifile_multivolume_rar_set();
    let _working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Recover Stale Extracted Member", &files),
    )
    .await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let member_name = "E01.mkv".to_string();
    let staging_dir = pipeline.extraction_staging_dir(job_id);
    let (out_path, partial_path) = Pipeline::member_output_paths(&staging_dir, &member_name);
    let chunk_dir = Pipeline::member_chunk_dir(&staging_dir, "show", &member_name);
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(&partial_path, b"episode-a-payload").unwrap();

    pipeline
        .db
        .add_extracted_member(job_id, &member_name, &out_path)
        .unwrap();
    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert(member_name.clone());

    pipeline
        .db
        .replace_member_chunks(
            job_id,
            "show",
            &member_name,
            &[crate::ExtractionChunk {
                member_name: member_name.clone(),
                volume_index: 0,
                bytes_written: b"episode-a-payload".len() as u64,
                temp_path: partial_path.to_string_lossy().to_string(),
                start_offset: 0,
                end_offset: b"episode-a-payload".len() as u64,
                verified: true,
                appended: false,
            }],
        )
        .unwrap();

    let reconciled = pipeline
        .reconcile_extracted_outputs_for_completion(job_id)
        .await;

    assert!(!reconciled);
    assert!(out_path.exists());
    assert!(!partial_path.exists());
    assert_eq!(std::fs::read(&out_path).unwrap(), b"episode-a-payload");
    assert!(
        pipeline
            .db
            .get_extraction_chunks(job_id, "show")
            .unwrap()
            .into_iter()
            .all(|chunk| chunk.member_name != member_name)
    );
    assert!(
        pipeline
            .extracted_members
            .get(&job_id)
            .is_some_and(|members| members.contains(&member_name))
    );
}

#[tokio::test]
async fn restore_job_rehydrates_existing_deterministic_staging_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31207);
    let spec = standalone_job_spec(
        "Restore staged extraction",
        &[("sample.bin".to_string(), 100)],
    );
    let working_dir = temp_dir.path().join("restore-staged-extraction");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    let staging_dir = pipeline.deterministic_extraction_staging_dir(job_id);
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.staging_dir.clone()),
        Some(staging_dir)
    );
}

#[test]
fn runtime_transition_failpoint_mapping_uses_lane_transitions() {
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Verifying,
            crate::jobs::model::RunState::Active,
        ),
        Some("status.enter_verifying")
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::QueuedRepair,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Repairing,
            crate::jobs::model::RunState::Active,
        ),
        Some("status.enter_repairing")
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::Idle,
            crate::jobs::model::RunState::Paused,
        ),
        Some("status.enter_paused")
    );
    assert_eq!(
        Pipeline::status_enter_failpoint_for_transition(
            crate::jobs::model::PostState::Extracting,
            crate::jobs::model::RunState::Active,
            crate::jobs::model::PostState::WaitingForVolumes,
            crate::jobs::model::RunState::Active,
        ),
        None
    );
}

#[tokio::test]
async fn split_files_register_topology_when_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30077);
    let files = [
        ("archive.001".to_string(), b"split-a".to_vec()),
        ("archive.002".to_string(), b"split-b".to_vec()),
    ];
    let spec = JobSpec {
        name: "Split Topology Registration".to_string(),
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
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("split-register-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("archive")
        .expect("split topology should be registered");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Split
    );
    assert_eq!(topology.expected_volume_count, Some(files.len() as u32));
    assert_eq!(topology.complete_volumes.len(), files.len());
    assert!(topology.volume_map.contains_key("archive.001"));
    assert!(topology.volume_map.contains_key("archive.002"));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_retry_clears_detected_archive_identity_before_redownload() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    let filename = "51273aad56a8b904e96928935278a627";
    let fixture_bytes = rar5_fixture_bytes("rar5_store.rar");
    let spec = rar_job_spec(
        "Obfuscated RAR Retry Clears Detection",
        &[(filename.to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(file.role(), FileRole::Unknown));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some(filename)
        );
    }
    assert_eq!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .len(),
        1
    );

    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([filename.to_string()]));

    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(state.status, JobStatus::Downloading));
        assert_eq!(state.download_queue.len(), 1);
        assert!(!file.is_complete());
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            FileRole::Unknown
        ));
        assert!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .is_none()
        );
        assert!(state.assembly.archive_topology_for(filename).is_none());
    }
    assert!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_archive_headers(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state.assembly.file(file_id).unwrap();
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some(filename)
        );
    }
    assert_eq!(
        pipeline
            .db
            .load_detected_archive_identities(job_id)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn no_par2_retry_reclassifies_obfuscated_rar_redownload_as_7z() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let filename = "51273aad56a8b904e96928935278a627";
    let rar_bytes = rar5_fixture_bytes("rar5_store.rar");
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = rar_job_spec(
        "Obfuscated RAR Retry Reclassifies As 7z",
        &[(filename.to_string(), rar_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &rar_bytes).await;
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([filename.to_string()]));

    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap();
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            FileRole::Unknown
        ));
        assert!(state.assembly.archive_topology_for(filename).is_none());
    }
    assert!(pipeline.rar_sets.is_empty());
    assert!(
        pipeline
            .db
            .load_all_archive_headers(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .is_empty()
    );

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &seven_zip_bytes).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let file = state
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    assert!(matches!(
        pipeline.classified_role_for_file(job_id, file),
        weaver_model::files::FileRole::SevenZipArchive
    ));
    let topology = state
        .assembly
        .archive_topology_for(filename)
        .expect("replacement payload should create a fresh 7z topology");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert!(topology.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness(filename),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn re_registering_identical_rar_facts_still_recomputes_readiness() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30030);
    let filename = "archive.rar";
    let fixture_bytes = rar5_fixture_bytes("rar5_store.rar");
    let spec = rar_job_spec(
        "RAR Recompute On Same Facts",
        &[(filename.to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let topo = state.assembly.archive_topology_for_mut("archive").unwrap();
        topo.complete_volumes.clear();
    }

    pipeline
        .refresh_archive_state_for_completed_file(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            true,
        )
        .await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_rar_failure_requeues_member_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30027);
    let files = vec![("archive.rar".to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "work/sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["work/sample.mkv".to_string()]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn no_par2_runtime_rar_failure_requeues_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let filename = "51273aad56a8b904e96928935278a627";
    let set_name = filename.to_string();
    let member_name = "sample.mkv".to_string();
    let files = vec![(filename.to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 Runtime RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([(filename.to_string(), 0)]),
        complete_volumes: [0u32].into_iter().collect(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            volume_files: std::collections::BTreeMap::from([(0, filename.to_string())]),
            phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec![member_name.clone()],
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([member_name]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.rar_sets.contains_key(&(job_id, set_name)));
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn rar_waiting_for_missing_volumes_without_par2_fails_after_download_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30077);
    let working_dir = intermediate_dir.join("rar-missing-tail");
    let mut state = minimal_job_state(job_id, "RAR Missing Tail", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("no PAR2 metadata is available for repair"));
}

#[tokio::test]
async fn legacy_reconcile_schedules_waiting_rar_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    let working_dir = intermediate_dir.join("rar-waiting-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting No Spin", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.reconcile_job_progress(job_id).await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn rar_completion_waiting_for_volumes_does_not_requeue_itself() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let working_dir = intermediate_dir.join("rar-waiting-check-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting Check No Spin", working_dir);
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn clean_member_keeps_failed_neighbor_boundary_volume_suspect() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Boundary Suspect Claims", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
                (2u32, dummy_rar_volume_facts(2)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: std::collections::HashSet::from([1u32]),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    let suspect = pipeline.suspect_rar_volumes_for_job(job_id);
    let decision = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .and_then(|plan| plan.delete_decisions.get(&1))
        .unwrap();

    assert!(suspect.contains(&1));
    assert!(!Pipeline::claim_clean_rar_volume(decision));
}

#[tokio::test]
async fn normalization_refresh_preserves_deleted_untouched_rar_facts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30016);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalization Keeps Facts", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string(), "show.part04.rar".to_string()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

    let facts: Vec<u32> = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("RAR set should exist")
        .facts
        .keys()
        .copied()
        .collect();
    assert_eq!(facts, vec![0, 1, 2, 3]);
    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .map(|rows| rows.len()),
        Some(4)
    );
}

#[tokio::test]
async fn clean_verify_after_swap_correction_preserves_retry_frontier_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30017);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Retry Frontier", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let part03 = working_dir.join("show.part03.rar");
    let part04 = working_dir.join("show.part04.rar");
    let swap_tmp = working_dir.join("show.swap.tmp");
    tokio::fs::rename(&part03, &swap_tmp).await.unwrap();
    tokio::fs::rename(&part04, &part03).await.unwrap();
    tokio::fs::rename(&swap_tmp, &part04).await.unwrap();

    for (file_index, (filename, _)) in files.iter().enumerate() {
        let current_bytes = tokio::fs::read(working_dir.join(filename)).await.unwrap();
        persist_completed_file_hash(
            &pipeline,
            job_id,
            file_index as u32,
            filename,
            &current_bytes,
        )
        .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part01.rar".to_string(), "show.part02.rar".to_string()]
            .into_iter()
            .collect(),
    );
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should still exist after normalization retry");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert!(!plan.waiting_on_volumes.contains(&0));
    assert!(plan.waiting_on_volumes.is_disjoint(&plan.deletion_eligible));
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv")
    );
    assert_eq!(
        plan.delete_decisions
            .get(&2)
            .expect("volume 2 decision should exist")
            .owners,
        vec!["E02.mkv".to_string()]
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental retry batch"),
    }
}

#[tokio::test]
async fn clean_verify_after_file_swap_refreshes_stale_rar_snapshot_without_volume_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30021);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Refreshes Snapshot", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

    for (file_index, (filename, _)) in files.iter().enumerate() {
        let current_bytes = tokio::fs::read(working_dir.join(filename)).await.unwrap();
        persist_completed_file_hash(
            &pipeline,
            job_id,
            file_index as u32,
            filename,
            &current_bytes,
        )
        .await;
    }

    tokio::fs::remove_file(working_dir.join(&files[0].0))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join(&files[1].0))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        [files[0].0.clone(), files[1].0.clone()]
            .into_iter()
            .collect(),
    );

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after placement correction");
    let suspect_volumes = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .expect("RAR state should exist after placement correction");
    let part03_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .cloned()
        .expect("swapped volume identity should persist after placement correction");
    let part04_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .cloned()
        .expect("counterpart swapped volume identity should persist after placement correction");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert_eq!(suspect_volumes, [2, 3].into_iter().collect());
    assert_eq!(part03_identity.current_filename, "show.part04.rar");
    assert_eq!(part03_identity.canonical_filename.as_deref(), Some("show.part04.rar"));
    assert_eq!(part03_identity.classification_source, FileIdentitySource::Par2);
    assert_eq!(part04_identity.current_filename, "show.part03.rar");
    assert_eq!(part04_identity.canonical_filename.as_deref(), Some("show.part03.rar"));
    assert_eq!(part04_identity.classification_source, FileIdentitySource::Par2);
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part03.rar")
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
        Some("show.part04.rar")
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv"),
        "stale cached headers must be refreshed so the swapped member can retry: {:?}",
        plan.waiting_on_volumes
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental retry batch"),
    }
}

#[tokio::test]
async fn rar_retry_frontier_rejects_waiting_on_deleted_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30018);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Retry Frontier Rejects Deleted Waiting Volume", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part04.rar"))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part04.rar".to_string()].into_iter().collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert!(plan.waiting_on_volumes.contains(&3));
    assert!(!plan.deletion_eligible.contains(&3));
    assert_eq!(
        pipeline.invalid_rar_retry_frontier_reason(job_id),
        Some("set 'show' waiting volumes already deleted: [3]".to_string())
    );
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_some());
}

#[tokio::test]
async fn eager_delete_retains_volume_with_failed_member_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30019);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Failed Claim Delete Guard", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (filename, bytes) in &files {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline
        .failed_extractions
        .insert(job_id, ["E10.mkv".to_string()].into_iter().collect());
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(1u32, dummy_rar_volume_facts(1))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: [1u32].into_iter().collect(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part02.rar").exists());
    assert!(
        !pipeline
            .eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains("show.part02.rar"))
    );
}

#[tokio::test]
async fn probe_completion_clears_health_probing_and_restores_queues() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let spec = standalone_job_spec(
        "Probe Reset",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.activate_health_probes(job_id);

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        assert!(matches!(state.status, JobStatus::Checking));
        assert_eq!(state.download_queue.len(), 0);
        assert_eq!(state.recovery_queue.len(), 0);
        assert_eq!(state.held_segments.len(), 3);
    }

    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        total: 1,
        missed: 0,
        done: true,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert!(state.held_segments.is_empty());
    assert_eq!(state.download_queue.len(), 3);
    assert_eq!(state.recovery_queue.len(), 0);
}

#[tokio::test]
async fn reconcile_job_progress_does_not_rewrite_status_from_lanes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30088);
    let spec = standalone_job_spec(
        "Restored Checking",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.health_probing = false;
        state.download_state = crate::jobs::model::DownloadState::Checking;
        state.refresh_legacy_status();
    }

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Checking
    ));
    assert!(matches!(state.status, JobStatus::Checking));
}

#[tokio::test]
async fn reconcile_job_progress_leaves_terminal_recovery_to_restore_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let spec = standalone_job_spec(
        "Restored Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.health_probing = false;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.download_state = crate::jobs::model::DownloadState::Checking;
        state.refresh_legacy_status();
    }

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Checking
    ));
    assert!(matches!(state.status, JobStatus::Checking));
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_queued_when_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    let spec = standalone_job_spec(
        "Restore Checking Pending",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    let working_dir = temp_dir.path().join("restore-checking-pending");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Checking,
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Queued
    ));
}

#[tokio::test]
async fn restore_job_normalizes_persisted_checking_to_complete_when_no_work_remains() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30093);
    let spec = standalone_job_spec(
        "Restore Checking Complete",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
        ],
    );
    let committed_segments = Pipeline::all_segment_ids(job_id, &spec);
    let working_dir = temp_dir.path().join("restore-checking-complete");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments,
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Checking,
            download_state: Some(crate::jobs::model::DownloadState::Checking),
            post_state: Some(crate::jobs::model::PostState::Idle),
            run_state: Some(crate::jobs::model::RunState::Active),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(
        state.download_state,
        crate::jobs::model::DownloadState::Complete
    ));
}

#[tokio::test]
async fn paused_queued_extraction_is_not_promoted_when_capacity_frees() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30089);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Paused Queued Extract", &[("archive.7z".to_string(), 100)]),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Paused;
        state.paused_resume_status = Some(JobStatus::Downloading);
        state.queued_extract_at_epoch_ms = Some(crate::jobs::model::epoch_ms_now());
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.promote_queued_extractions();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Paused));
    assert!(matches!(
        state.paused_resume_status,
        Some(JobStatus::Downloading)
    ));
}

#[tokio::test]
async fn queued_repair_blocks_extraction_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Awaiting Repair Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::QueuedRepair;
        state.refresh_runtime_lanes_from_status();
    }

    assert!(!pipeline.maybe_start_extraction(job_id).await);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::QueuedRepair));
}

#[tokio::test]
async fn exhausted_incomplete_rar_member_is_not_scheduled_for_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Incomplete RAR Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    let set_name = "archive".to_string();
    let member_name = "work/sample.mkv".to_string();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
        complete_volumes: HashSet::new(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 100,
        }],
        unresolved_spans: Vec::new(),
    };

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state
            .assembly
            .set_archive_topology(set_name.clone(), topology.clone());
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: vec![crate::pipeline::archive::rar_state::RarReadyMember {
                    name: member_name,
                }],
                member_names: vec!["work/sample.mkv".to_string()],
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    pipeline.try_rar_extraction(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!matches!(state.status, JobStatus::Extracting));
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, set_name))
            .map(|set| set.active_workers),
        Some(0)
    );
}

#[tokio::test]
async fn incomplete_download_with_active_extraction_defers_instead_of_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let spec = JobSpec {
        name: "Incomplete Download Active Extraction".to_string(),
        password: None,
        total_bytes: 320,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![
                    SegmentSpec {
                        number: 0,
                        bytes: 128,
                        message_id: "active-extract-payload-0@example.com".to_string(),
                    },
                    SegmentSpec {
                        number: 1,
                        bytes: 128,
                        message_id: "active-extract-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            message_id: MessageId::new("active-extract-payload-1@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 0,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: false,
            exclude_servers: Vec::new(),
        });
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(1, index_filename, 0, false)],
    );
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("payload".to_string());

    pipeline.check_job_completion(job_id).await;

    let status = job_status_for_assert(&pipeline, job_id);
    assert!(!matches!(status, Some(JobStatus::Failed { .. })));
    assert!(pipeline.jobs.contains_key(&job_id));
}

#[tokio::test]
async fn impossible_rar_state_fails_loudly_after_forced_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30076);
    let working_dir = intermediate_dir.join("impossible-rar-state");
    let mut state = minimal_job_state(job_id, "Impossible RAR State", working_dir);
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::new(),
        complete_volumes: HashSet::new(),
        expected_volume_count: None,
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "E10.mkv".to_string(),
            first_volume: 0,
            last_volume: 1,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(0u32, dummy_rar_volume_facts(0))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string()],
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("invalid RAR state after recompute"));
}

#[tokio::test]
async fn probe_completion_does_not_immediately_reenter_checking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30021);
    let spec = standalone_job_spec(
        "Probe Reentry Guard",
        &[
            ("probe-a.bin".to_string(), 100),
            ("probe-b.bin".to_string(), 100),
            ("probe-c.bin".to_string(), 100),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.failed_bytes = 10;
    }

    pipeline.activate_health_probes(job_id);
    pipeline.handle_probe_update(ProbeUpdate {
        job_id,
        total: 1,
        missed: 0,
        done: true,
    });

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!state.health_probing);
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.last_health_probe_failed_bytes, 10);
}

#[tokio::test]
async fn repair_queue_limits_to_one_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31001);
    let job_b = JobId(31002);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert_eq!(
        pipeline.jobs.get(&job_a).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );
}

#[tokio::test]
async fn extraction_queue_limits_to_tuner_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let extraction_limit = pipeline.tuner.max_concurrent_extractions();
    let jobs: Vec<JobId> = (0..=extraction_limit)
        .map(|idx| JobId(31101 + idx as u64))
        .collect();
    let queued_job = jobs[extraction_limit];

    for (idx, job_id) in jobs.iter().enumerate() {
        pipeline.jobs.insert(
            *job_id,
            minimal_job_state(
                *job_id,
                &format!("extract-{idx}"),
                temp_dir.path().join(format!("extract-{idx}")),
            ),
        );
    }

    for job_id in jobs.iter().take(extraction_limit) {
        assert!(pipeline.maybe_start_extraction(*job_id).await);
        assert_eq!(
            pipeline.jobs.get(job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting)
        );
    }
    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );

    assert!(!pipeline.maybe_start_extraction(queued_job).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::QueuedExtract)
    );

    pipeline.transition_postprocessing_status(jobs[0], JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![queued_job]
    );
}

#[tokio::test]
async fn restore_queued_postprocessing_schedules_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31201);
    let spec = standalone_job_spec("Restore queued repair", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-queued-repair");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::QueuedRepair,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn restore_repairing_preserves_status_and_slot_ownership() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31205);
    let spec = standalone_job_spec("Restore repairing", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-repairing");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Repairing,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: Some(42_000.0),
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(42_000.0)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 1);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
}

#[tokio::test]
async fn restore_paused_postprocessing_target_normalizes_to_downloading() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31206);
    let spec = standalone_job_spec("Restore paused", &[("sample.bin".to_string(), 100)]);
    let working_dir = temp_dir.path().join("restore-paused");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            spec,
            committed_segments: HashSet::new(),
            file_progress: HashMap::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Paused,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: Some(84_000.0),
            paused_resume_status: Some(JobStatus::QueuedExtract),
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.queued_extract_at_epoch_ms),
        Some(84_000.0)
    );

    pipeline.resume_job_runtime(job_id).unwrap();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn repair_queue_promotion_reserves_slot_and_keeps_queue_age() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31211);
    let job_b = JobId(31212);
    let job_c = JobId(31213);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );
    pipeline.jobs.insert(
        job_c,
        minimal_job_state(job_c, "repair-c", temp_dir.path().join("repair-c")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    assert!(!pipeline.maybe_start_repair(job_b).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );

    pipeline.transition_postprocessing_status(job_a, JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::Repairing)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_b]
    );

    assert!(!pipeline.maybe_start_repair(job_c).await);
    assert_eq!(
        pipeline.jobs.get(&job_c).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
}

#[tokio::test]
async fn pause_rejects_queued_repair_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_a = JobId(31221);
    let job_b = JobId(31222);

    pipeline.jobs.insert(
        job_a,
        minimal_job_state(job_a, "repair-a", temp_dir.path().join("repair-a")),
    );
    pipeline.jobs.insert(
        job_b,
        minimal_job_state(job_b, "repair-b", temp_dir.path().join("repair-b")),
    );

    assert!(pipeline.maybe_start_repair(job_a).await);
    assert!(!pipeline.maybe_start_repair(job_b).await);
    let queued_at = pipeline
        .jobs
        .get(&job_b)
        .and_then(|state| state.queued_repair_at_epoch_ms)
        .unwrap();

    let error = pipeline.pause_job_runtime(job_b).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
    assert_eq!(
        pipeline.jobs.get(&job_b).map(|state| state.status.clone()),
        Some(JobStatus::QueuedRepair)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_b)
            .and_then(|state| state.queued_repair_at_epoch_ms),
        Some(queued_at)
    );
}

#[tokio::test]
async fn pause_clears_stale_completion_rechecks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31231);

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "paused-job", temp_dir.path().join("paused-job")),
    );
    pipeline.schedule_job_completion_check(job_id);
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );

    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.pending_completion_checks.is_empty());

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
}

#[tokio::test]
async fn auto_pause_stalled_download_releases_blocking_runtime() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31232);
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "stalled-job", temp_dir.path().join("stalled-job")),
    );
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);
    pipeline.bandwidth_cap.reserve(256);
    pipeline.bandwidth_reservations.insert(segment_id, 256);
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );

    pipeline.auto_pause_stalled_downloads();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Paused)
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.paused_resume_status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(pipeline.active_downloads, 0);
    assert!(!pipeline.active_download_passes.contains(&job_id));
    assert!(!pipeline.active_downloads_by_job.contains_key(&job_id));
    assert!(pipeline.bandwidth_reservations.is_empty());
}

#[tokio::test]
async fn reconcile_job_progress_marks_waiting_for_rar_volumes_without_clobbering_download_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31233);
    let set_name = "show".to_string();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "waiting-rar", temp_dir.path().join("waiting-rar")),
    );
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            set_name.clone(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: std::collections::HashMap::from([
                    ("show.part01.rar".to_string(), 0u32),
                    ("show.part02.rar".to_string(), 1u32),
                    ("show.part03.rar".to_string(), 2u32),
                ]),
                complete_volumes: std::collections::HashSet::from([0u32, 1u32]),
                expected_volume_count: Some(3),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "E01.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 2,
                    unpacked_size: 100,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                waiting_on_volumes: std::collections::HashSet::from([2u32]),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: pipeline
                    .jobs
                    .get(&job_id)
                    .unwrap()
                    .assembly
                    .archive_topology_for(&set_name)
                    .unwrap()
                    .clone(),
                fallback_reason: None,
            }),
        },
    );

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        state.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(state.post_state, crate::jobs::model::PostState::Idle);
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn completion_reconciliation_clears_missing_extracted_outputs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31234);
    let spec = JobSpec {
        name: "missing-output".to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline
        .extracted_members
        .insert(job_id, ["missing.mkv".to_string()].into_iter().collect());
    pipeline
        .db
        .add_extracted_member(
            job_id,
            "missing.mkv",
            std::path::Path::new("/nonexistent/missing.mkv"),
        )
        .unwrap();

    assert!(
        pipeline
            .reconcile_extracted_outputs_for_completion(job_id)
            .await
    );

    assert!(!pipeline.extracted_members.contains_key(&job_id));
    let recovered = pipeline.db.load_active_jobs().unwrap();
    assert!(
        recovered
            .get(&job_id)
            .is_some_and(|job| job.extracted_members.is_empty())
    );
}

#[tokio::test]
async fn auto_pause_ignores_stale_jobs_without_inflight_downloads() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31233);

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "idle-job", temp_dir.path().join("idle-job")),
    );
    pipeline.active_download_passes.insert(job_id);
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );

    pipeline.auto_pause_stalled_downloads();

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert!(pipeline.active_download_passes.contains(&job_id));
}

#[tokio::test]
async fn pause_rejects_extracting_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31241);

    let mut state = minimal_job_state(
        job_id,
        "extracting-job",
        temp_dir.path().join("extracting-job"),
    );
    state.status = JobStatus::Extracting;
    pipeline.jobs.insert(job_id, state);
    let error = pipeline.pause_job_runtime(job_id).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("pause is only supported in queued or downloading states")
    );
}
