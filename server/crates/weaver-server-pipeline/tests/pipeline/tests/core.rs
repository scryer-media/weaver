// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::pipeline::tests::{
    TestHarness, nzb_zstd_for, segmented_job_spec, standalone_job_spec, wait_until,
};
use weaver_server_pipeline::pipeline::tests::{
    delivery_fixture::{finish_delivery, install_delivery_job},
    history_fixture::finished_jobs,
    new_direct_pipeline,
};
use weaver_server_pipeline::{JobId, JobStatus};

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
    let client = tokio::task::spawn_blocking(move || {
        assert!(matches!(
            client.try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY),
            Err(
                weaver_nntp::client::BlockingBodyLaneAcquireError::ProviderCapacity(
                    weaver_nntp::NntpError::TooManyConnections
                )
            )
        ));
        client
    })
    .await
    .unwrap();
    assert!(pool.is_over_limit(weaver_nntp::ServerId(0)));
    assert_eq!(pool.active_connections(0), 0);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    // Every later lane meets the holdoff without a socket.
    let client = tokio::task::spawn_blocking(move || {
        for _ in 0..CONNECTIONS {
            assert!(matches!(
                client.try_acquire_blocking_body_lane(&[], weaver_nntp::ServerSet::EMPTY),
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

    // Wait for this job's activated queue, then cross another actor turn.
    // A held provider offers no fresh seats, so there must be no refused
    // handouts to wait for: those were the dispatch spin this test guards.
    loop {
        let snapshot = harness.handle.pipeline_diagnostics().await.unwrap();
        if snapshot
            .jobs
            .iter()
            .any(|job| job.job_id == job_id.0 && job.download_queue_len == SEGMENTS)
        {
            break;
        }
    }
    harness.handle.pipeline_diagnostics().await.unwrap();
    assert_eq!(
        harness
            .handle
            .get_live_metrics()
            .download_lane_lease_items_total,
        leased_before,
        "a held-off provider must not receive and return leases"
    );
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
    let script = weaver_server_pipeline::post_processing::events::hold_test_event_run(
        &harness.db,
        id.0,
        "cancel-cleanup",
    );
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
    let mut revisions = harness.handle.subscribe_job_changes();
    let status = loop {
        revisions.mark_unchanged();
        let job = harness.handle.get_job(id).unwrap();
        if matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. }) {
            break job.status;
        }
        revisions.changed().await.unwrap();
    };
    let output = harness.root().join("complete/Repeated Article");
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
async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10067);
    let job_name = "Silver Horizon Beyond the Vale";
    let payload_dir =
        "Silver Horizon.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
    let episode_name = "Silver Horizon.Beyond.Journeys.End.S01E01.mkv";

    let existing_dest = complete_dir.join(
        weaver_server_pipeline::jobs::working_dir::sanitize_dirname(job_name),
    );
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
        weaver_server_pipeline::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(working_dir.join(payload_dir))
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(payload_dir).join(episode_name), b"new")
        .await
        .unwrap();

    install_delivery_job(&mut pipeline, job_id, job_name, working_dir.clone());
    let (dest, succeeded) = finish_delivery(&mut pipeline, job_id).await;
    assert!(succeeded);
    let expected_dest = complete_dir.join(format!(
        "{}.#{}",
        weaver_server_pipeline::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));

    assert_eq!(dest, expected_dest);
    assert!(!weaver_server_pipeline::pipeline::tests::uu_fixture::has_job(&pipeline, job_id));
    assert!(
        finished_jobs(&mut pipeline)
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
