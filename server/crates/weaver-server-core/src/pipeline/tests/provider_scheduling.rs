// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// Hold each socket at its first BODY. Accepted sockets and observed commands
// provide the barrier; elapsed time and scheduler speed play no part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_four_providers_and_124_nzbs_dial_only_reserved_primary_work() {
    for format in ["par2", "par3", "mixed", "indexless"] {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let mut servers = tokio::task::JoinSet::new();
        let mut configs = Vec::new();
        let accepted = Arc::new((0..24).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (commands, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let (release, held) = tokio::sync::watch::channel(false);
        let (closed, mut closures) = tokio::sync::mpsc::unbounded_channel();
        for server in 0..24 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
                tls: false,
                ..Default::default()
            });
            config.group = (server / 2) as u32;
            config.max_connections = 2;
            config.backfill = server >= 22;
            configs.push(config);
            let accepted = Arc::clone(&accepted);
            let commands = commands.clone();
            let held = held.clone();
            let closed = closed.clone();
            servers.spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    accepted[server].fetch_add(1, Ordering::SeqCst);
                    let commands = commands.clone();
                    let mut held = held.clone();
                    let closed = closed.clone();
                    connections.spawn(async move {
                        let (read, mut write) = socket.into_split();
                        write.write_all(b"200 fixture ready\r\n").await.unwrap();
                        let mut lines = BufReader::new(read).lines();
                        loop {
                            let command = match lines.next_line().await {
                                Ok(Some(command)) => command,
                                Ok(None) => break,
                                Err(error)
                                    if error.kind() == std::io::ErrorKind::ConnectionReset =>
                                {
                                    break;
                                }
                                Err(error) => panic!("fixture command read failed: {error}"),
                            };
                            if command == "CAPABILITIES" {
                                write
                                    .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                                    .await
                                    .unwrap();
                            } else if command.starts_with("GROUP ") {
                                write
                                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                                    .await
                                    .unwrap();
                            } else if command.starts_with("BODY ") {
                                commands.send((server, command)).unwrap();
                                held.wait_for(|released| *released).await.unwrap();
                                write.write_all(b"430 fixture absent\r\n").await.unwrap();
                            } else if command == "QUIT" {
                                break;
                            } else {
                                write.write_all(b"500 unsupported\r\n").await.unwrap();
                            }
                        }
                        closed.send(server).unwrap();
                    });
                }
            });
        }
        pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
            weaver_nntp::client::NntpClientConfig {
                servers: configs,
                max_idle_age: Duration::from_mins(5),
                max_retries_per_server: 0,
                soft_timeout: Duration::from_secs(3600),
            },
        ));
        pipeline.tuner.set_connection_limit(48);
        pipeline.owned_download_lane_pool.resize(48);
        for id in 1..=124 {
            let mut spec = segmented_job_spec(&format!("scheduled-{id}"), "payload.bin", &[512; 8]);
            let extensions: &[&str] = match format {
                "par2" => &["par2"],
                "par3" => &["par3"],
                "mixed" => &["par2", "par3"],
                _ => &[],
            };
            for extension in extensions {
                let role = if *extension == "par2" {
                    FileRole::Par2 {
                        is_index: true,
                        recovery_block_count: 0,
                    }
                } else {
                    FileRole::Par3 { is_index: true }
                };
                spec.files.insert(
                    0,
                    FileSpec {
                        filename: format!("index.{extension}"),
                        role,
                        groups: vec!["alt.binaries.test".into()],
                        posted_at_epoch: None,
                        segments: vec![segment_spec! { number: 0, bytes: 512,
                        message_id: format!("metadata-{id}-{extension}@example.invalid") }],
                    },
                );
                spec.total_bytes += 512;
            }
            pipeline
                .add_job(
                    JobId(id),
                    spec.clone(),
                    PathBuf::from(format!("scheduled-{id}.nzb")),
                    nzb_zstd_for(&spec),
                    crate::jobs::AddJobOptions::default(),
                )
                .await
                .unwrap();
        }
        assert_eq!(pipeline.nzb_admitted.len(), 10);
        assert_eq!(
            pipeline.shared_state.queue.read(|inner| inner.jobs.len()),
            124
        );
        pipeline.shared_state.queue.read(|inner| {
            assert_eq!(inner.globals.nntp_generation, pipeline.pool_generation);
            for row in &inner.jobs {
                if pipeline.nzb_admitted.contains(&row.id) {
                    assert!(
                        row.report.nzb_admitted,
                        "admission was not published for {:?}",
                        row.id
                    );
                    assert!(
                        !row.report.retiring && !row.report.restoring && row.admission.is_none()
                    );
                    assert_ne!(row.report.job.run_state, crate::RunState::Paused);
                }
            }
        });
        // The admitted jobs' articles are built as their first handout
        // builds them, so the dispatch below hands them out.
        let mut admitted: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
        admitted.sort_unstable();
        for job_id in admitted {
            activate_job(&mut pipeline, job_id).await;
        }
        pipeline.dispatch_downloads();
        assert_eq!(
            pipeline.download_lane_owners.len(),
            4,
            "{format}: initial dispatch reservations"
        );
        let mut issued = Vec::new();
        while issued.len() < 4 {
            let (server, command) = tokio::select! {
                command = observed.recv() => command.unwrap(),
                event = pipeline.owned_download_lane_event_rx.recv() => {
                    match event.unwrap() {
                        event @ OwnedDownloadLaneEvent::AcquireFailed { .. } => {
                            // Health-lock contention returns a reservation to
                            // the actor. Drive that event exactly as runtime
                            // does, rather than assuming every dial wins a
                            // concurrently held selection lock on its first try.
                            let mut results = VecDeque::new();
                            pipeline.handle_owned_download_lane_event(event, &mut results);
                            assert!(results.is_empty(), "local admission must not become article failure");
                            pipeline.dispatch_downloads();
                            continue;
                        },
                        OwnedDownloadLaneEvent::LeaseObsolete { lease } => panic!("{format}: obsolete lease for {:?} generation {}", lease.job_id, lease.runtime_generation),
                        OwnedDownloadLaneEvent::BatchComplete { .. } => panic!("{format}: premature completion"),
                    }
                }
            };
            assert!(
                server < 2,
                "{format}: lower provider {server} received {command}"
            );
            if format != "indexless" {
                assert!(command.contains("metadata-"), "{command}");
            }
            issued.push(server);
        }
        issued.sort_unstable();
        assert_eq!(
            issued,
            vec![0, 0, 1, 1],
            "equal-priority providers share all four connections"
        );
        pipeline.dispatch_downloads();
        assert_eq!(pipeline.download_lane_owners.len(), 4);
        assert_eq!(pipeline.nzb_admitted.len(), 10);
        for (server, count) in accepted.iter().enumerate() {
            assert_eq!(
                count.load(Ordering::SeqCst),
                if server < 2 { 2 } else { 0 },
                "{format}: provider {server}"
            );
        }
        // Stop admission before releasing the held replies. Drain every
        // lease and close its cached socket while these listeners still own
        // their ports; a later format must never inherit a recycled endpoint
        // that an earlier fixture's workers can still contact.
        pipeline.shared_state.queue.write(|inner| {
            inner.globals.paused = true;
            crate::pipeline::queue::Change::changed(())
        });
        release.send(true).unwrap();
        let mut parked = 0;
        while parked < 4 {
            tokio::select! {
                Some(request) = pipeline.download_refill_rx.recv() => {
                    assert!(request.response_tx.send(DownloadLaneRefillResponse {
                        lease: None,
                        park_reason: LaneParkReason::NoWork,
                    }).is_ok());
                }
                Some(event) = pipeline.owned_download_lane_event_rx.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { ack, .. } = event else {
                        panic!("only issued leases remain during fixture teardown");
                    };
                    if let Some(ack) = ack { ack.send(()).unwrap(); }
                }
                Some(_) = pipeline.download_lane_parked_rx.recv() => parked += 1,
            }
        }
        assert!(pipeline.owned_download_lane_pool.reset().leases.is_empty());
        for _ in 0..4 {
            assert!(closures.recv().await.unwrap() < 2);
        }
        pipeline.nntp.shutdown().await;
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }
}

// A fill server whose quota cannot take one more article turns the article
// away at dispatch, and the lower-priority server that takes it instead does
// not clear the refusal: the refusing server reads blocked for as long as its
// allowance stays short of an article.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_out_of_quota_reads_blocked_while_a_lower_priority_server_takes_its_work() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let primary = transfers.configure(
        weaver_nntp::transfer::StableServerId(1),
        weaver_nntp::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 4096,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    let (commands, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let mut servers = tokio::task::JoinSet::new();
    let mut configs = Vec::new();
    for server in 0..2usize {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
            tls: false,
            ..Default::default()
        });
        config.group = server as u32;
        config.max_connections = 1;
        config.stable_id = weaver_nntp::transfer::StableServerId(server as u32 + 1);
        if server == 0 {
            config.transfer_control = Some(Arc::clone(&primary));
        }
        configs.push(config);
        let commands = commands.clone();
        servers.spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let commands = commands.clone();
                connections.spawn(async move {
                    let (read, mut write) = socket.into_split();
                    write.write_all(b"200 fixture ready\r\n").await.unwrap();
                    let mut lines = BufReader::new(read).lines();
                    while let Ok(Some(command)) = lines.next_line().await {
                        if command == "CAPABILITIES" {
                            write
                                .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                                .await
                                .unwrap();
                        } else if command.starts_with("GROUP ") {
                            write
                                .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                                .await
                                .unwrap();
                        } else if command.starts_with("BODY ") {
                            let _ = commands.send((server, command));
                            write.write_all(b"430 fixture absent\r\n").await.unwrap();
                        } else if command == "QUIT" {
                            break;
                        } else {
                            write.write_all(b"500 unsupported\r\n").await.unwrap();
                        }
                    }
                });
            }
        });
    }
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: configs,
            max_idle_age: Duration::from_mins(5),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(3600),
        },
    ));
    pipeline.tuner.set_connection_limit(2);
    pipeline.owned_download_lane_pool.resize(2);
    let spec = segmented_job_spec("quota-fallback", "payload.bin", &[65536; 3]);
    pipeline
        .add_job(
            JobId(1),
            spec.clone(),
            PathBuf::from("quota-fallback.nzb"),
            nzb_zstd_for(&spec),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    assert!(!primary.snapshot().quota_blocked);
    activate_job(&mut pipeline, JobId(1)).await;

    pipeline.dispatch_downloads();
    let (server, command) = loop {
        tokio::select! {
            command = observed.recv() => break command.unwrap(),
            event = pipeline.owned_download_lane_event_rx.recv() => {
                let event = event.unwrap();
                let mut results = VecDeque::new();
                pipeline.handle_owned_download_lane_event(event, &mut results);
                pipeline.dispatch_downloads();
            }
            Some(request) = pipeline.download_refill_rx.recv() => {
                let _ = request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                });
            }
        }
    };
    assert_eq!(server, 1, "the out-of-quota server sent {command}");
    assert!(
        primary.snapshot().quota_blocked,
        "the server that turned the article away must read blocked"
    );
    servers.abort_all();
}
