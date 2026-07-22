use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

fn capacity_test_client(port: u16, connections: usize) -> NntpClient {
    NntpClient::new(NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            tls: false,
            connect_timeout: Duration::from_secs(2),
            command_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        connections,
    ))
}

async fn spawn_capacity_limited_body_server(
    connection_limit: usize,
    payload: Vec<u8>,
    total_segments: usize,
) -> (
    u16,
    Arc<AtomicBool>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let fast_bodies = Arc::new(AtomicBool::new(false));
    let active_connections = Arc::new(AtomicUsize::new(0));
    let mut encoded = Vec::new();
    weaver_yenc::encode(&payload, &mut encoded, 128, "capacity-recovery.bin").unwrap();
    let header_end = encoded
        .windows(2)
        .position(|bytes| bytes == b"\r\n")
        .unwrap()
        + 2;
    let trailer_start = encoded
        .windows(b"\r\n=yend".len())
        .rposition(|bytes| bytes == b"\r\n=yend")
        .unwrap();
    let crc_start = encoded
        .windows(b"crc32=".len())
        .position(|bytes| bytes == b"crc32=")
        .unwrap()
        + b"crc32=".len();
    let encoded_body = Arc::new(encoded[header_end..trailer_start].to_vec());
    let part_crc = Arc::new(String::from_utf8(encoded[crc_start..crc_start + 8].to_vec()).unwrap());
    let part_size = payload.len();
    let total_size = part_size * total_segments;

    let fast_bodies_for_server = Arc::clone(&fast_bodies);
    let active_for_server = Arc::clone(&active_connections);
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let active = active_for_server.fetch_add(1, Ordering::SeqCst) + 1;
            let active_for_connection = Arc::clone(&active_for_server);
            let fast_bodies = Arc::clone(&fast_bodies_for_server);
            let encoded_body = Arc::clone(&encoded_body);
            let part_crc = Arc::clone(&part_crc);
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                if active > connection_limit {
                    let _ = writer.write_all(b"502 Too Many Connections\r\n").await;
                    active_for_connection.fetch_sub(1, Ordering::SeqCst);
                    return;
                }

                if writer.write_all(b"200 test server ready\r\n").await.is_ok() {
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if line == "CAPABILITIES" {
                            if writer
                                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                                .await
                                .is_err()
                            {
                                break;
                            }
                        } else if line.starts_with("GROUP ") {
                            if writer
                                .write_all(b"211 1000 1 1000 alt.binaries.test\r\n")
                                .await
                                .is_err()
                            {
                                break;
                            }
                        } else if line.starts_with("BODY ") {
                            if !fast_bodies.load(Ordering::Acquire) {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                            let part_index = line
                                .split("segment-")
                                .nth(1)
                                .and_then(|suffix| suffix.split('@').next())
                                .and_then(|index| index.parse::<usize>().ok())
                                .unwrap();
                            let part_number = part_index + 1;
                            let part_begin = part_index * part_size + 1;
                            let part_end = part_begin + part_size - 1;
                            let header = format!(
                                "222 0 <test> body follows\r\n=ybegin part={part_number} total={total_segments} line=128 size={total_size} name=capacity-recovery.bin\r\n=ypart begin={part_begin} end={part_end}\r\n"
                            );
                            let trailer = format!(
                                "\r\n=yend size={part_size} part={part_number} pcrc32={}\r\n.\r\n",
                                part_crc
                            );
                            if writer.write_all(header.as_bytes()).await.is_err()
                                || writer.write_all(&encoded_body).await.is_err()
                                || writer.write_all(trailer.as_bytes()).await.is_err()
                            {
                                break;
                            }
                        } else if line == "QUIT" {
                            let _ = writer.write_all(b"205 closing\r\n").await;
                            break;
                        } else if writer.write_all(b"500 unsupported\r\n").await.is_err() {
                            break;
                        }
                    }
                }
                active_for_connection.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });

    (port, fast_bodies, active_connections, server)
}

#[tokio::test]
async fn phase_end_removes_only_the_ended_phase_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(99);

    let repairing = pipeline.phase_begin(job_id, JobPhase::Repairing, Some(100));
    repairing.completed_bytes.store(25, Ordering::Relaxed);
    let extracting = pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    extracting.completed_bytes.store(50, Ordering::Relaxed);

    pipeline.sample_phase_progress();
    assert_eq!(
        pipeline.phase_progress_snapshots.get(&job_id).map(Vec::len),
        Some(2)
    );

    pipeline.phase_end(job_id, JobPhase::Extracting);

    let remaining = pipeline.phase_progress_snapshots.get(&job_id).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].phase, JobPhase::Repairing);
}

#[tokio::test]
async fn phase_end_extracting_if_idle_preserves_sibling_inflight_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100);

    pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("sibling-set".to_string());

    pipeline.phase_end_extracting_if_idle(job_id);

    assert!(
        pipeline
            .phase_progress
            .contains_key(&(job_id, JobPhase::Extracting)),
        "job-level extracting phase must stay live while any set is inflight"
    );

    pipeline.inflight_extractions.remove(&job_id);
    pipeline.phase_end_extracting_if_idle(job_id);

    assert!(
        !pipeline
            .phase_progress
            .contains_key(&(job_id, JobPhase::Extracting)),
        "extracting phase should end once all set and member work is idle"
    );
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
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, set_name)
        .expect("partial encrypted snapshot should exist after four volumes");
    let mut cached = serde_json::to_value(
        rmp_serde::from_slice::<weaver_unrar::CachedArchiveHeaders>(&cached_headers).unwrap(),
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
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

    write_and_complete_rar_volume(&mut pipeline, job_id, 4, &files[4].0, &files[4].1).await;

    assert_eq!(
        member_span(&pipeline, job_id, set_name, member_name),
        Some((0, 4))
    );

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let selected = pipeline.volume_paths_for_rar_members(
        job_id,
        set_name,
        &[member_name.to_string()],
        &volume_paths,
        true,
        false,
    );
    assert_eq!(
        selected.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
}

#[tokio::test]
async fn verified_suspect_persist_serializes_latest_value_per_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40123);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Suspect Persist Queue", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    pipeline.persist_verified_suspect_volumes(job_id, "show", &HashSet::from([1u32]));
    pipeline.persist_verified_suspect_volumes(job_id, "show", &HashSet::from([2u32, 3u32]));

    let key = (job_id, "show".to_string());
    let persist_state = pipeline
        .verified_suspect_persist_state
        .get(&key)
        .expect("verified suspect persistence state should exist");
    assert!(persist_state.in_flight_version.is_some());
    assert!(persist_state.queued);
    assert_eq!(persist_state.desired, HashSet::from([2u32, 3u32]));

    drain_verified_suspect_persists(&mut pipeline).await;

    assert!(
        pipeline
            .db
            .load_verified_suspect_volumes(job_id)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn submit_nzb_persists_zstd_and_creates_active_job() {
    let harness = TestHarness::new().await;
    let nzb_bytes = sample_nzb_bytes();

    let submitted = tokio::time::timeout(
        Duration::from_secs(2),
        submit_nzb_bytes(
            &harness.db,
            &harness.handle,
            &harness.config,
            &nzb_bytes,
            Some("Silver Horizon.Sample.nzb".to_string()),
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
    assert!(info.metadata.iter().any(|(key, value)| {
        key == "weaver.original_title" && value == "Silver Horizon.Sample"
    }));

    let (stored_path, stored_nzb) = harness
        .db
        .load_active_job_persisted_nzb(submitted.job_id)
        .unwrap()
        .and_then(|(path, nzb_zstd)| nzb_zstd.map(|nzb_zstd| (path, nzb_zstd)))
        .unwrap();
    assert_eq!(stored_path, PathBuf::from("Silver Horizon.Sample.nzb"));
    assert!(stored_nzb.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));

    let decoded = crate::ingest::decode_persisted_nzb_bytes(&stored_nzb).unwrap();
    assert_eq!(decoded, nzb_bytes);

    harness.shutdown().await;
}

#[tokio::test]
async fn active_job_recovers_from_provider_cap_and_live_80_to_20_generation_change() {
    const TOTAL_SEGMENTS: usize = 1_000;
    let payload = vec![b'A'; 1024];
    let (port, fast_bodies, active_connections, server) =
        spawn_capacity_limited_body_server(20, payload.clone(), TOTAL_SEGMENTS).await;
    let initial_client = capacity_test_client(port, 80);
    let old_pool = Arc::clone(initial_client.pool());
    let harness = TestHarness::new_with_nntp(initial_client, 80).await;
    let job_id = JobId(80_020);
    let segment_sizes = vec![payload.len() as u32; TOTAL_SEGMENTS];
    let spec = segmented_job_spec(
        "active 80 to 20 capacity recovery",
        "capacity-recovery.bin",
        &segment_sizes,
    );

    harness
        .handle
        .add_job(
            job_id,
            spec,
            PathBuf::from("capacity-recovery.nzb"),
            Vec::new(),
        )
        .await
        .unwrap();

    wait_until(Duration::from_secs(15), || {
        harness.handle.get_job(job_id).is_ok_and(|job| {
            job.downloaded_bytes > 0
                && !matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. })
        })
    })
    .await
    .expect("job should begin downloading before the generation correction");

    wait_until(Duration::from_secs(20), || {
        old_pool.effective_connection_capacity() == 20
    })
    .await
    .expect("configured 80/provider 20 should converge to effective 20");
    assert_eq!(
        old_pool.capacity_reductions(weaver_nntp::ServerId(0)),
        Some(60)
    );
    assert!(active_connections.load(Ordering::Acquire) <= 20);
    let progress_before_rebuild = harness.handle.get_job(job_id).unwrap().downloaded_bytes;

    let replacement_client = capacity_test_client(port, 20);
    let replacement_pool = Arc::clone(replacement_client.pool());
    let activation = harness
        .handle
        .rebuild_nntp(replacement_client, 20)
        .await
        .unwrap();
    assert_eq!(activation.generation, 1);
    assert_eq!(activation.configured_connections, 20);
    assert_eq!(activation.effective_connections, 20);
    fast_bodies.store(true, Ordering::Release);

    wait_until(Duration::from_secs(15), || {
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| job.downloaded_bytes > progress_before_rebuild)
    })
    .await
    .expect("the same active job should resume within the generation handoff bound");
    assert_eq!(replacement_pool.effective_connection_capacity(), 20);
    assert_eq!(
        replacement_pool.capacity_reductions(weaver_nntp::ServerId(0)),
        Some(0)
    );
    assert_eq!(
        replacement_pool.capacity_penalty_until_epoch_ms(weaver_nntp::ServerId(0)),
        None
    );

    let completed = wait_until(Duration::from_secs(30), || {
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| matches!(job.status, JobStatus::Complete))
    })
    .await;
    assert!(
        completed.is_ok(),
        "same no-PAR2 job did not complete: job={:?} metrics={:?}",
        harness.handle.get_job(job_id),
        harness.handle.get_live_metrics()
    );

    let job = harness.handle.get_job(job_id).unwrap();
    assert_eq!(job.failed_bytes, 0);
    assert_eq!(job.health, 1000);
    assert_eq!(job.remaining_par_files, 0);
    let metrics = harness.handle.get_live_metrics();
    assert_eq!(metrics.segments_failed_permanent, 0);
    assert_eq!(metrics.download_failures_article_not_found, 0);

    harness.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn stalled_capacity_probe_does_not_block_rar_scheduler_events() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        std::future::pending::<()>().await;
    });

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let client = capacity_test_client(port, 2);
    assert!(
        !client
            .pool()
            .record_provider_capacity_rejection(weaver_nntp::ServerId(0))
    );
    pipeline.nntp = Arc::new(client);
    pipeline.drive_capacity_probes_at(tokio::time::Instant::now() + Duration::from_secs(10 * 60));
    accepted_rx.await.unwrap();

    for kind in [
        RarCapacityRetryKind::Refresh,
        RarCapacityRetryKind::Extraction,
        RarCapacityRetryKind::FullSetExtraction,
    ] {
        pipeline
            .rar_capacity_retry_tx
            .send(RarCapacityRetry {
                job_id: JobId(1),
                set_name: "stalled-probe.rar".to_string(),
                kind,
            })
            .await
            .unwrap();
        let received = pipeline.rar_capacity_retry_rx.recv().await.unwrap();
        assert_eq!(received.kind, kind);
    }

    pipeline.nntp.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let job_name = "Silver Horizon Beyond the Vale";
    let payload_dir =
        "Silver Horizon.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
    let episode_name = "Silver Horizon.Beyond.Journeys.End.S01E01.mkv";

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

    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    assert!(done.result.is_ok());
    pipeline.handle_move_to_complete_done(done);
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(pipeline.finished_jobs.iter().any(|job| job.job_id == job_id
        && job.output_dir.as_deref() == Some(expected_dest.to_str().unwrap())));
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
    let job_name = "Silver Horizon Broken Final Move";
    let missing_working_dir = intermediate_dir.join("missing");

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, missing_working_dir),
    );

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;

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
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
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
            runtime_generation: 0,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 0,
            },
            data: Ok(DownloadPayload::Raw(raw)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: None,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
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
            posted_at_epoch: None,
            segments: (0..segment_count)
                .map(|number| {
                    segment_spec! {
                        number: number,
                        bytes: payload_size,
                        message_id: format!("cursor-{number}@example.com"),
                    }
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
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
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
async fn sparse_article_numbers_commit_cleanly_with_dense_ordinals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    let filename = "sparse.bin";
    let payload_size = 128u32;
    let total_size = payload_size as u64 * 3;
    let spec = JobSpec {
        name: "Sparse Segment Ordinals".to_string(),
        password: None,
        total_bytes: total_size,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                segment_spec! {
                    ordinal: 0,
                    article_number: 1,
                    bytes: payload_size,
                    message_id: "sparse-0@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 1,
                    article_number: 4,
                    bytes: payload_size,
                    message_id: "sparse-1@example.com".to_string(),
                },
                segment_spec! {
                    ordinal: 2,
                    article_number: 6,
                    bytes: payload_size,
                    message_id: "sparse-2@example.com".to_string(),
                },
            ],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for segment_number in 0..3u32 {
        let payload = vec![segment_number as u8 + 1; payload_size as usize];
        let raw = encode_article_part(
            filename,
            &payload,
            segment_number + 1,
            3,
            segment_number as u64 * payload_size as u64 + 1,
            total_size,
        );

        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                runtime_generation: 0,
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                    segment_number,
                },
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
            })
            .await;
    }

    drain_decode_results(&mut pipeline, 3).await;

    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        3
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn hard_write_pressure_latches_until_soft_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1000;

    pipeline
        .metrics
        .write_buffered_bytes
        .store(1000, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Write.as_code()
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(701, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_stalls_total
            .load(Ordering::Relaxed),
        1
    );

    pipeline
        .metrics
        .write_buffered_bytes
        .store(699, Ordering::Relaxed);
    pipeline.refresh_download_pressure();
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert_eq!(
        pipeline
            .metrics
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::None.as_code()
    );
}

#[tokio::test]
async fn delayed_retry_drops_stale_exclusions_after_pool_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40041);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let segment_id = SegmentId {
        file_id,
        segment_number: 0,
    };
    let spec = JobSpec {
        name: "Stale Exclusion Rebuild".to_string(),
        password: None,
        total_bytes: 128,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "payload.bin".to_string(),
            role: FileRole::from_filename("payload.bin"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 128,
                message_id: "stale-exclusion@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
    }

    let make_work = || DownloadWork {
        segment_id,
        message_id: MessageId::new("stale-exclusion@example.com"),
        groups: vec!["alt.binaries.test".to_string()],
        priority: 100,
        byte_estimate: 128,
        retry_count: 1,
        is_recovery: false,
        exclude_servers: vec![0],
    };

    // A rebuild bumped the generation after this retry was scheduled under gen 0:
    // its exclude index no longer refers to the server it was computed against,
    // so the guard must drop it (the retry re-tries every server cleanly).
    pipeline.pool_generation = 1;
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 0,
        infrastructure_retry: false,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("stale retry should still requeue");
    assert_eq!(queued.segment_id, segment_id);
    assert!(
        queued.exclude_servers.is_empty(),
        "stale server exclusions must be dropped after a pool rebuild"
    );

    // No rebuild since scheduling: exclusions are still valid and preserved.
    pipeline.note_retry_scheduled(segment_id);
    pipeline.receive_retry_work(super::RetryWork {
        scheduled_pool_generation: 1,
        infrastructure_retry: false,
        work: make_work(),
    });
    let queued = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .expect("same-generation retry should requeue");
    assert_eq!(
        queued.exclude_servers,
        vec![0],
        "current-generation exclusions must be preserved"
    );
}

#[tokio::test]
async fn stale_extracted_member_checkpoint_is_discarded_during_completion_reconcile() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30085);
    let files = build_multifile_multivolume_rar_set();
    let _working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Discard Stale Extracted Member", &files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

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

    assert!(reconciled);
    assert!(!out_path.exists());
    assert!(partial_path.exists());
    assert!(
        pipeline
            .db
            .get_extraction_chunks(job_id, "show")
            .unwrap()
            .into_iter()
            .all(|chunk| chunk.member_name != member_name)
    );
    assert!(!pipeline.extracted_members.contains_key(&job_id));
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
