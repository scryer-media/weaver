// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::atomic::AtomicUsize;

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

// A fake provider that is deliberately **strict** about the BODY argument.
//
// Per RFC 3977 §6.2 a message-id argument must be enclosed in angle brackets;
// an unbracketed argument is an article-number reference, which real providers
// answer with 430 for every article. A lenient harness hid exactly that
// regression, so this one answers 430 for anything unbracketed and records the
// raw command lines so tests can assert on the wire frame directly.
async fn spawn_capacity_limited_body_server(
    connection_limit: usize,
    payload: Vec<u8>,
    total_segments: usize,
) -> (
    u16,
    tokio::sync::watch::Sender<bool>,
    Arc<AtomicUsize>,
    Arc<std::sync::Mutex<Vec<String>>>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (release_final_body, final_body_released) = tokio::sync::watch::channel(false);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let body_commands: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
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

    let active_for_server = Arc::clone(&active_connections);
    let body_commands_for_server = Arc::clone(&body_commands);
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            // Claim a provider slot only when one is free, so a refused
            // attempt never counts as a held session, even while its 502 is
            // still being written.
            let admitted = active_for_server
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    (active < connection_limit).then_some(active + 1)
                })
                .is_ok();
            let active_for_connection = Arc::clone(&active_for_server);
            let mut final_body_released = final_body_released.clone();
            let encoded_body = Arc::clone(&encoded_body);
            let part_crc = Arc::clone(&part_crc);
            let body_commands = Arc::clone(&body_commands_for_server);
            tokio::spawn(async move {
                let (reader, mut writer) = socket.into_split();
                if !admitted {
                    let _ = writer.write_all(b"502 Too Many Connections\r\n").await;
                    return;
                }

                if writer.write_all(b"200 test server ready\r\n").await.is_ok() {
                    let mut lines = BufReader::new(reader).lines();
                    'commands: while let Ok(Some(line)) = lines.next_line().await {
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
                        } else if let Some(argument) = line.strip_prefix("BODY ") {
                            body_commands
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .push(line.clone());
                            // Strict, like a real provider: only the bracketed
                            // message-id form names an article. An unbracketed
                            // argument is an article-number reference into the
                            // selected group, which answers 430.
                            let Some(message_id) = argument
                                .trim()
                                .strip_prefix('<')
                                .and_then(|rest| rest.strip_suffix('>'))
                            else {
                                if writer.write_all(b"430 No such article\r\n").await.is_err() {
                                    break;
                                }
                                continue;
                            };
                            let part_index = message_id
                                .split("segment-")
                                .nth(1)
                                .and_then(|suffix| suffix.split('@').next())
                                .and_then(|index| index.parse::<usize>().ok())
                                .unwrap();
                            let part_number = part_index + 1;
                            // Keep the job active until the test changes its
                            // generation, without slowing every BODY into a
                            // transport timeout. This server is sequential;
                            // a closed old-generation socket must release its
                            // provider slot even while the final body is held.
                            while part_number == total_segments && !*final_body_released.borrow() {
                                tokio::select! {
                                    changed = final_body_released.changed() => {
                                        if changed.is_err() {
                                            break 'commands;
                                        }
                                    }
                                    _ = lines.next_line() => break 'commands,
                                }
                            }
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

    (
        port,
        release_final_body,
        active_connections,
        body_commands,
        server,
    )
}

#[tokio::test]
async fn metrics_refresh_advances_without_pipeline_turns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    tokio::time::pause();
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let mut changes = handle.subscribe_job_changes();
    for downloaded in [1, 2, 99] {
        let watermark = *changes.borrow_and_update();
        pipeline
            .metrics
            .bytes_downloaded
            .store(downloaded, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(250)).await;
        while pipeline.shared_state.metrics_snapshot().bytes_downloaded != downloaded {
            changes.changed().await.unwrap();
        }
        assert!(*changes.borrow() > watermark);
    }
}

#[tokio::test]
async fn phase_end_removes_only_the_ended_phase_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(99);

    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(crate::pipeline::queue::JobRow::new(finished_job_info(
            job_id,
        )));
        crate::pipeline::queue::Change::changed(())
    });
    let repairing = pipeline.phase_begin(job_id, JobPhase::Repairing, Some(100));
    repairing.completed_bytes.store(25, Ordering::Relaxed);
    let extracting = pipeline.phase_begin(job_id, JobPhase::Extracting, Some(200));
    extracting.completed_bytes.store(50, Ordering::Relaxed);
    pipeline.shared_state.queue.sample_counters(
        &pipeline.metrics,
        &pipeline.event_tx,
        std::time::Instant::now(),
    );
    assert_eq!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .phase_progress
            .len(),
        2
    );
    pipeline.phase_end(job_id, JobPhase::Extracting);
    let remaining = pipeline
        .shared_state
        .get_job(job_id)
        .unwrap()
        .phase_progress;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].phase, JobPhase::Repairing);
    pipeline.clear_job_phase_progress_runtime(job_id);
    assert!(
        pipeline
            .shared_state
            .get_job(job_id)
            .unwrap()
            .phase_progress
            .is_empty()
    );
}

#[tokio::test]
async fn submit_nzb_persists_zstd_and_creates_active_job() {
    let harness = TestHarness::new().await;
    let nzb_bytes = sample_nzb_bytes();

    let submitted = submit_nzb_bytes(
        &harness.db,
        &harness.handle,
        &harness.config,
        &nzb_bytes,
        Some("Silver Horizon.Sample.nzb".to_string()),
        None,
        None,
        vec![("source".to_string(), "test".to_string())],
    )
    .await
    .unwrap();

    wait_until(|| {
        harness
            .db
            .load_active_jobs()
            .map(|jobs| jobs.contains_key(&submitted.job_id))
            .unwrap_or(false)
    })
    .await;

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

// The provider must keep serving while the pipeline handles an 80-lane
// connection burst; sharing one executor thread turns scheduler contention
// into fake provider timeouts and can park every article before any progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_job_recovers_from_provider_cap_and_live_80_to_20_generation_change() {
    const TOTAL_SEGMENTS: usize = 1_000;
    let payload = vec![b'A'; 1024];
    let (port, release_final_body, active_connections, _body_commands, server) =
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
            spec.clone(),
            PathBuf::from("capacity-recovery.nzb"),
            nzb_zstd_for(&spec),
        )
        .await
        .unwrap();

    wait_until(|| {
        harness.handle.get_job(job_id).is_ok_and(|job| {
            job.downloaded_bytes > 0
                && !matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. })
        })
    })
    .await;

    wait_until(|| old_pool.is_over_limit(weaver_nntp::ServerId(0))).await;
    // The holdoff never edits the operator's configuration.
    assert_eq!(
        old_pool.configured_connections(weaver_nntp::ServerId(0)),
        Some(80)
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
    release_final_body.send_replace(true);

    wait_until(|| {
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| job.downloaded_bytes > progress_before_rebuild)
    })
    .await;
    assert_eq!(replacement_pool.fill_connection_capacity(), 20);
    assert_eq!(
        replacement_pool.over_limit_until_epoch_ms(weaver_nntp::ServerId(0)),
        None
    );

    wait_until(|| {
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. }))
    })
    .await;
    assert!(
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| matches!(job.status, JobStatus::Complete)),
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
async fn held_off_server_stops_reconnecting_inside_the_holdoff_window() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_by_server = Arc::clone(&accepted);
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            accepted_by_server.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (_reader, mut writer) = socket.into_split();
                let _ = writer.write_all(b"502 Too Many Connections\r\n").await;
            });
        }
    });

    const CONNECTIONS: usize = 8;
    let client = capacity_test_client(port, CONNECTIONS);
    let pool = Arc::clone(client.pool());
    // Finish one real rejection before dispatching the concurrent workload.
    // Sockets admitted before holdoff may reach accept() after it is armed;
    // counting those as reconnects made this assertion depend on scheduling.
    assert!(matches!(
        pool.acquire(weaver_nntp::ServerId(0)).await,
        Err(weaver_nntp::NntpError::TooManyConnections)
    ));
    assert!(pool.is_over_limit(weaver_nntp::ServerId(0)));
    assert_eq!(pool.active_connections(0), 0);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    // Exercise both acquisition paths after the admission boundary is known.
    for _ in 0..CONNECTIONS {
        assert!(matches!(
            pool.acquire(weaver_nntp::ServerId(0)).await,
            Err(weaver_nntp::NntpError::ServerOverLimit { .. })
        ));
    }
    let client = tokio::task::spawn_blocking(move || {
        for _ in 0..CONNECTIONS {
            assert!(matches!(
                client.try_acquire_blocking_body_lane(&[], &[]),
                Err(
                    weaver_nntp::client::BlockingBodyLaneAcquireError::ProviderCapacity(
                        weaver_nntp::NntpError::ServerOverLimit { .. }
                    )
                )
            ));
        }
        client
    })
    .await
    .unwrap();
    let harness = TestHarness::new_with_nntp(client, CONNECTIONS).await;
    let job_id = JobId(80_021);
    const SEGMENTS: usize = 64;
    let spec = segmented_job_spec(
        "held off provider",
        "held-off.bin",
        &vec![1024_u32; SEGMENTS],
    );
    let leased_before = harness
        .handle
        .get_live_metrics()
        .download_lane_lease_items_total;

    harness
        .handle
        .add_job(
            job_id,
            spec.clone(),
            PathBuf::from("held-off.nzb"),
            nzb_zstd_for(&spec),
        )
        .await
        .unwrap();

    // The scheduler also keeps the queued workload off the held-off provider.
    // Leasing more articles than the job holds means at least one lease came
    // back unserved and its articles were leased again, so a lane has asked
    // the held-off server and been answered.
    wait_until(|| {
        harness
            .handle
            .get_live_metrics()
            .download_lane_lease_items_total
            > leased_before + SEGMENTS as u64
    })
    .await;
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "a held-off server must not be reconnected inside its window"
    );

    // Work waits for the deadline instead of failing the job.
    let job = harness.handle.get_job(job_id).unwrap();
    assert!(!matches!(job.status, JobStatus::Failed { .. }));
    assert_eq!(
        pool.configured_connections(weaver_nntp::ServerId(0)),
        Some(CONNECTIONS)
    );

    harness.shutdown().await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn rename_preserves_an_existing_working_directory_collision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let old_dir = intermediate_dir.join("original");
    std::fs::create_dir(&old_dir).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_working_dir(&intermediate_dir, &old_dir, job_id)
        .unwrap();
    let mut state = minimal_job_state(job_id, "renamed", old_dir.clone());
    state.download_state = crate::jobs::model::DownloadState::Queued;
    pipeline.jobs.insert(job_id, state);

    std::fs::create_dir(intermediate_dir.join("renamed")).unwrap();
    let collision = intermediate_dir.join("renamed.#10067");
    std::fs::create_dir(&collision).unwrap();
    std::fs::write(collision.join("keep.bin"), b"another job's data").unwrap();

    assert!(
        !pipeline
            .rename_working_dir_when_quiet(job_id, "renamed")
            .await
    );

    assert_eq!(
        std::fs::read(collision.join("keep.bin")).unwrap(),
        b"another job's data"
    );
    assert_eq!(pipeline.jobs[&job_id].working_dir, old_dir);
    assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(
        &old_dir
    ));
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
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    assert!(done.result.is_ok());
    pipeline.handle_move_to_complete_done(done).await;
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .any(|job| job.job_id == job_id
                && job.output_dir.as_deref() == Some(expected_dest.to_str().unwrap()))
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
async fn completion_check_during_post_processing_does_not_finalize_again() {
    for status in [JobStatus::QueuedPostProcessing, JobStatus::PostProcessing] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(10070);
        let job_name = "Copper Lantern Late Completion Check";
        let working_dir = intermediate_dir.join("copper-lantern");
        tokio::fs::create_dir_all(&working_dir).await.unwrap();
        tokio::fs::write(working_dir.join("payload.mkv"), b"payload")
            .await
            .unwrap();
        let mut state = minimal_job_state(job_id, job_name, working_dir.clone());
        state.status = status.clone();
        pipeline.jobs.insert(job_id, state);

        // A late completion check landing while scripts run must not run the
        // final move a second time: that claims `<name>.#<id>` and repoints the
        // job's output directory out from under the running script.
        pipeline.check_job_completion(job_id).await;
        settle_inflight_moves(&mut pipeline).await;

        assert_eq!(job_status_for_assert(&pipeline, job_id).unwrap(), status);
        assert!(working_dir.join("payload.mkv").exists());
        let mut entries = tokio::fs::read_dir(&complete_dir).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "{status:?}: completion check published into {}",
            complete_dir.display()
        );
    }
}

#[tokio::test]
async fn unacceptable_extension_rejection_never_starts_a_final_move_or_scripts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10069);
    let job_name = "Rejected Before Publication";
    pipeline
        .db
        .save_post_processing_settings(&crate::post_processing::model::PostProcessingSettings {
            execution_enabled: true,
            unacceptable_extensions: vec!["exe".into()],
            ..Default::default()
        })
        .unwrap();
    pipeline
        .db
        .create_script_instance(crate::post_processing::instances::ScriptInstanceDraft::new(
            crate::post_processing::model::ScriptName::new("sentinel.sh").unwrap(),
            crate::post_processing::instances::InstanceTrigger::PostProcessing,
        ))
        .unwrap();

    let working_dir = intermediate_dir.join("rejected-before-publication");
    tokio::fs::create_dir_all(working_dir.join("nested"))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("nested/payload.EXE"), b"rejected")
        .await
        .unwrap();
    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
    pipeline.start_move_to_complete(job_id).await.unwrap();

    settle_inflight_moves(&mut pipeline).await;
    assert!(pipeline.inflight_terminal_post_processing.is_empty());
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    let JobStatus::Failed { error } = status else {
        panic!("unacceptable extension must fail the job");
    };
    assert_eq!(error, "unwanted extension '.exe' in 'nested/payload.EXE'");
    assert!(
        pipeline
            .db
            .job_post_processing_summary(job_id.0)
            .unwrap()
            .is_none()
    );
    assert!(
        working_dir.exists(),
        "the rejected source was never published"
    );
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
        state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
    }

    let make_work = || DownloadWork {
        counters: None,
        segment_id,
        message_id: MessageId::new("stale-exclusion@example.com"),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 100,
        byte_estimate: 128,
        retry_count: 1,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: vec![0],
        avoid_server: None,
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
        .articles
        .active_mut()
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
        .articles
        .active_mut()
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
async fn restored_post_processing_is_interrupted_without_rerunning_scripts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31237);
    let spec = JobSpec {
        name: "interrupted-post-processing".to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::PostProcessing;
    // The executor stamps this marker before the first script runs.
    pipeline
        .db
        .mark_job_post_processing_running(job_id.0)
        .unwrap();
    // The startup scan is the whole recovery: a `running` marker can only have
    // been left behind by a process that died mid-pass.
    assert_eq!(
        pipeline.db.recover_interrupted_post_processing().unwrap(),
        1
    );
    pipeline.schedule_job_completion_check(job_id);

    assert!(pipeline.recover_restored_terminal_post_processing(job_id));
    settle_failed_releases(&mut pipeline).await;

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(matches!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .find(|job| job.job_id == job_id)
            .map(|job| job.status),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn restored_post_processing_that_already_finished_archives_as_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31238);
    let spec = JobSpec {
        name: "completed-post-processing".to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::PostProcessing;
    pipeline
        .db
        .save_job_post_processing_results(
            job_id.0,
            crate::post_processing::model::PostProcessingSummary::Succeeded,
            &[],
        )
        .unwrap();
    // A job whose summary is already terminal is not swept by the scan.
    assert_eq!(
        pipeline.db.recover_interrupted_post_processing().unwrap(),
        0
    );
    pipeline.schedule_job_completion_check(job_id);

    assert!(pipeline.recover_restored_terminal_post_processing(job_id));

    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert!(matches!(
        pipeline
            .finished_jobs()
            .await
            .into_iter()
            .find(|job| job.job_id == job_id)
            .map(|job| job.status),
        Some(JobStatus::Complete)
    ));
}

// The wire frame regression guard: BODY must carry the **bracketed**
// message-id.
//
// `DownloadWork::message_id` stores the bare id (the NZB parser strips the
// brackets), so a lane that borrows it directly emits `BODY segment-0@…`.
// That is a legal article-*number* reference, and every real provider answers
// 430 to it — a total, silent download failure. The fake provider here is
// strict about brackets, so this test fails outright on a regression.
#[tokio::test]
async fn download_lanes_send_bracketed_message_ids_on_the_wire() {
    const TOTAL_SEGMENTS: usize = 4;
    let payload = vec![b'Z'; 1024];
    let (port, release_final_body, _active_connections, body_commands, server) =
        spawn_capacity_limited_body_server(8, payload.clone(), TOTAL_SEGMENTS).await;
    release_final_body.send_replace(true);

    let harness = TestHarness::new_with_nntp(capacity_test_client(port, 4), 4).await;
    let job_id = JobId(80_021);
    let spec = segmented_job_spec(
        "Silver Horizon Wire Frame",
        "wire-frame.bin",
        &[payload.len() as u32; TOTAL_SEGMENTS],
    );

    harness
        .handle
        .add_job(
            job_id,
            spec.clone(),
            PathBuf::from("wire-frame.nzb"),
            nzb_zstd_for(&spec),
        )
        .await
        .unwrap();

    wait_until(|| {
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. }))
    })
    .await;
    assert!(
        harness
            .handle
            .get_job(job_id)
            .is_ok_and(|job| matches!(job.status, JobStatus::Complete)),
        "job did not complete against a bracket-strict provider: job={:?}",
        harness.handle.get_job(job_id)
    );

    let commands = body_commands
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert!(
        !commands.is_empty(),
        "provider recorded no BODY commands at all"
    );
    for command in &commands {
        let argument = command.strip_prefix("BODY ").unwrap();
        assert!(
            argument.starts_with('<') && argument.ends_with('>'),
            "BODY argument must be a bracketed message-id, got {command:?}"
        );
        assert!(
            argument.contains("segment-") && argument.contains("@example.com"),
            "unexpected BODY argument {command:?}"
        );
    }

    let metrics = harness.handle.get_live_metrics();
    assert_eq!(metrics.download_failures_article_not_found, 0);

    harness.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn repeated_articles_fetch_once_and_preserve_every_admitted_file() {
    repeated_article_case(false).await;
}

#[tokio::test]
async fn repeated_uuencode_articles_preserve_each_file_and_wire_accounting() {
    repeated_article_case(true).await;
}

#[tokio::test]
async fn cancel_mid_download_archives_then_cleans_up() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        write.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(read).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.starts_with("BODY ") {
                write.write_all(b"222 0 <held@example.com> body\r\n=ybegin line=128 size=100 name=payload.bin\r\n").await.unwrap();
                started_tx.send(()).unwrap();
                let _ = release_rx.await;
                break;
            }
            let response: &[u8] = if line == "CAPABILITIES" {
                b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
            } else if line.starts_with("GROUP ") {
                b"211 1 1 1 alt.binaries.test\r\n"
            } else {
                b"500 unsupported\r\n"
            };
            write.write_all(response).await.unwrap();
        }
    });
    let harness = TestHarness::new_with_nntp(capacity_test_client(port, 1), 1).await;
    let id = JobId(80032);
    let script =
        crate::post_processing::events::hold_test_event_run(&harness.db, id.0, "cancel-cleanup");
    let mut events = harness.handle.subscribe_events();
    let mut revisions = harness.handle.subscribe_job_changes();
    harness
        .handle
        .add_job(
            id,
            segmented_job_spec("Held download", "payload.bin", &[100]),
            PathBuf::from("held.nzb"),
            nzb_zstd_for(&(segmented_job_spec("Held download", "payload.bin", &[100]))),
        )
        .await
        .unwrap();
    started_rx.await.unwrap();
    let path = PathBuf::from(harness.handle.get_job(id).unwrap().output_dir.unwrap());
    let watermark = harness.handle.job_revision();
    harness.handle.cancel_job(id).await.unwrap();
    assert!(harness.handle.job_revision() > watermark);
    assert!(path.exists());
    release_tx.send(()).unwrap();
    server.await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert_eq!(
        harness.db.get_job_history(id.0).unwrap().unwrap().status,
        "cancelled"
    );
    assert!(path.exists());
    drop(script);
    loop {
        revisions.mark_unchanged();
        if !path.exists() {
            break;
        }
        revisions.changed().await.unwrap();
    }
    assert!(
        !harness
            .handle
            .list_jobs()
            .iter()
            .any(|job| job.job_id == id)
    );
    harness.shutdown().await;
}

async fn repeated_article_case(uuencode: bool) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload: &[u8] = if uuencode {
        b"abc"
    } else {
        b"one article, several independent file placements"
    };
    let mut body = Vec::new();
    if uuencode {
        body.extend_from_slice(b"begin 644 shared.bin\r\n#86)C\r\n`\r\nend\r\n");
    } else {
        weaver_yenc::encode(payload, &mut body, 128, "shared.bin").unwrap();
    }
    let expected_wire = body.len() as u64;
    let body = Arc::new(body);
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let body = body.clone(); let counted = counted.clone();
                    connections.spawn(async move {
                        let (read, mut write) = stream.into_split();
                        write.write_all(b"200 fixture ready\r\n").await.unwrap();
                        let mut lines = BufReader::new(read).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let response: &[u8] = if line == "CAPABILITIES" {
                                b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
                            } else if line.starts_with("GROUP ") {
                                b"211 1 1 1 alt.binaries.test\r\n"
                            } else if line == "BODY <shared@example.com>" {
                                counted.fetch_add(1, Ordering::SeqCst);
                                if write.write_all(b"222 0 <shared@example.com> body\r\n").await.is_err() { break; }
                                if write.write_all(&body).await.is_err() { break; }
                                b".\r\n"
                            } else if line == "QUIT" { break; }
                            else { b"500 unsupported\r\n" };
                            if write.write_all(response).await.is_err() { break; }
                        }
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    let harness = TestHarness::new_with_nntp(capacity_test_client(port, 4), 4).await;
    let id = JobId(80031);
    let mut spec = standalone_job_spec(
        "Repeated Article",
        &(0..4)
            .map(|n| (format!("copy-{n}.bin"), payload.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.segments[0].message_ids = "shared@example.com".into();
    }
    harness
        .handle
        .add_job(
            id,
            spec.clone(),
            PathBuf::from("repeat.nzb"),
            nzb_zstd_for(&spec),
        )
        .await
        .unwrap();
    let status = loop {
        let job = harness.handle.get_job(id).unwrap();
        if matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. }) {
            break job.status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let output = harness._temp_dir.path().join("complete/Repeated Article");
    let files: Vec<_> = std::fs::read_dir(&output)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| std::fs::read(entry.path()).unwrap())
        .collect();
    let wire_bytes = harness.handle.get_live_metrics().bytes_downloaded;
    harness.shutdown().await;
    server.abort();
    let _ = server.await;
    assert_eq!(status, JobStatus::Complete);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        files
            .iter()
            .filter(|bytes| bytes.as_slice() == payload)
            .count(),
        4
    );
    assert_eq!(
        wire_bytes, expected_wire,
        "cached placements must not count as new network transfers"
    );
}

#[tokio::test]
async fn failed_final_move_marks_job_failed_instead_of_complete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10068);
    let job_name = "Silver Horizon Broken Final Move";
    let spec = JobSpec {
        name: job_name.to_string(),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: None,
        metadata: vec![],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // Nothing is left to deliver from when the move runs.
    std::fs::remove_dir_all(&working_dir).unwrap();
    hear_graph_mail(&mut pipeline, job_id).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(matches!(status, JobStatus::Failed { .. }), "{status:?}");
    let JobStatus::Failed { error } = &status else {
        unreachable!();
    };
    assert!(error.contains("no delivery source exists"), "{error}");
    assert!(
        !complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname(job_name))
            .exists()
    );
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

    for _ in 0..3u32 {
        // Each article is fetched as dispatch hands it out.
        let work = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        let segment_number = work.segment_id.segment_number;
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
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id: work.segment_id,
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
    // The graph job's runs and its completion check settle it.
    hear_graph_mail(&mut pipeline, job_id).await;
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        pipeline.metrics.segments_committed.load(Ordering::Relaxed),
        3
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}
