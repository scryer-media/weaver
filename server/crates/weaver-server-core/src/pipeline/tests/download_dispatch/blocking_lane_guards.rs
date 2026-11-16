// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a download lane does with work it may no longer send, and with the
// recovery probe it is carrying. Every wait here is on the fixture server's
// own report or on a lane event: nothing waits on a clock.

use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// What the fixture server tells the test, in the order it happened.
#[derive(Debug)]
enum Seen {
    Accepted,
    Command(String),
    Closed,
}

// A one-connection fixture. It greets once `greet` reads true, answers
// CAPABILITIES and GROUP, and holds every BODY until `answer` reads true,
// then says the article is absent. Every step goes to `seen`.
async fn spawn_fixture(
    greet: tokio::sync::watch::Receiver<bool>,
    answer: tokio::sync::watch::Receiver<bool>,
) -> (
    u16,
    tokio::sync::mpsc::UnboundedReceiver<Seen>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (seen, observed) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        seen.send(Seen::Accepted).unwrap();
        let mut greet = greet;
        greet.wait_for(|go| *go).await.unwrap();
        let (read, mut write) = socket.into_split();
        write.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(command)) = lines.next_line().await {
            seen.send(Seen::Command(command.clone())).unwrap();
            let reply: &[u8] = if command == "CAPABILITIES" {
                b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
            } else if command.starts_with("GROUP ") {
                b"211 1 1 1 alt.binaries.test\r\n"
            } else if command.starts_with("BODY ") {
                let mut answer = answer.clone();
                answer.wait_for(|go| *go).await.unwrap();
                b"430 fixture absent\r\n"
            } else if command == "QUIT" {
                break;
            } else {
                b"500 unsupported\r\n"
            };
            if write.write_all(reply).await.is_err() {
                break;
            }
        }
        seen.send(Seen::Closed).unwrap();
    });
    (port, observed, server)
}

// A pipeline on the fixture alone, with one active job of `segments`
// articles, and the lease a dispatch would cut for all of them.
async fn pipeline_on_fixture(
    temp_dir: &TempDir,
    port: u16,
    job_id: JobId,
    segments: u32,
) -> (Pipeline, DownloadBatchLease) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    let mut server = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        ..Default::default()
    });
    server.max_connections = 1;
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server],
            max_idle_age: Duration::from_mins(5),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(3600),
        },
    ));
    pipeline.tuner.set_connection_limit(1);
    pipeline.owned_download_lane_pool.resize(1);
    let spec = segmented_job_spec(
        "lane-guards",
        "payload.bin",
        &vec![512; segments as usize],
    );
    pipeline
        .add_job(
            job_id,
            spec.clone(),
            PathBuf::from("lane-guards.nzb"),
            nzb_zstd_for(&spec),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    activate_job(&mut pipeline, job_id).await;
    let works = (0..segments)
        .map(|segment_number| DownloadWork {
            counters: None,
            segment_id: held_segment(job_id, 0, segment_number),
            message_id: MessageId::new(&format!("guard-{segment_number}@example.invalid")),
            groups: Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 0,
            byte_estimate: 512,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            avoid_server: None,
        })
        .collect();
    let lease = DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: 0,
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: Vec::new(),
        completion_critical: false,
        effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
        works,
    };
    (pipeline, lease)
}

// Close the lane's cached connection and wait for the fixture to see it
// close. Everything the lane wrote before then has been reported.
async fn close_lane_and_collect(
    pipeline: &Pipeline,
    observed: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>,
    mut commands: Vec<String>,
) -> Vec<String> {
    assert!(pipeline.owned_download_lane_pool.reset().leases.is_empty());
    loop {
        match observed.recv().await.unwrap() {
            Seen::Command(command) => commands.push(command),
            Seen::Closed => return commands,
            Seen::Accepted => panic!("the lane dialled a second connection"),
        }
    }
}

fn bodies(commands: &[String]) -> usize {
    commands
        .iter()
        .filter(|command| command.starts_with("BODY "))
        .count()
}

// Work that stops being current while its lane dials is never sent: the lane
// reports the lease obsolete with no result and no fetch attempt, the server
// is asked for no article, and its health records nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obsolete_work_on_a_lane_leaves_no_attempt_and_no_health_mark() {
    let (greet, greet_rx) = tokio::sync::watch::channel(false);
    let (_answer, answer_rx) = tokio::sync::watch::channel(false);
    let (port, mut observed, server) = spawn_fixture(greet_rx, answer_rx).await;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(81_001);
    let (mut pipeline, lease) = pipeline_on_fixture(&temp_dir, port, job_id, 2).await;

    pipeline.spawn_download_batch(lease);
    // The lane passed its first check and is inside the dial, waiting for
    // the greeting. Downloads stop now, before the lane holds a connection.
    assert!(matches!(observed.recv().await.unwrap(), Seen::Accepted));
    pipeline.shared_state.queue.write(|inner| {
        inner.globals.paused = true;
        crate::pipeline::queue::Change::changed(())
    });
    greet.send(true).unwrap();

    match pipeline.owned_download_lane_event_rx.recv().await.unwrap() {
        OwnedDownloadLaneEvent::LeaseObsolete { lease } => {
            assert_eq!(lease.job_id, job_id);
            assert_eq!(lease.works.len(), 2, "every work comes back unsent");
        }
        OwnedDownloadLaneEvent::AcquireFailed { error, .. } => {
            panic!("the dial succeeded, yet the lane reported {error:?}")
        }
        OwnedDownloadLaneEvent::BatchComplete { results, .. } => {
            panic!("obsolete work produced {} results", results.len())
        }
    }

    let commands = close_lane_and_collect(&pipeline, &mut observed, Vec::new()).await;
    assert_eq!(bodies(&commands), 0, "no article was asked for: {commands:?}");
    {
        let health = pipeline.nntp.pool().health().lock().await;
        assert_eq!(health.server(0).failure_count, 0);
        assert_eq!(health.server(0).consecutive_failures, 0);
    }
    assert!(!pipeline.nntp.pool().requires_recovery(0));
    assert!(!pipeline.nntp.pool().is_over_limit(weaver_nntp::ServerId(0)));
    server.await.unwrap();
}

// A lane whose dial became its server's recovery probe hands every article
// but the first back before it sends anything, and sends that one alone:
// a slow recovery must not hold a multi-article lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lane_carrying_the_recovery_probe_sends_one_article_until_the_probe_is_answered() {
    let (_greet, greet_rx) = tokio::sync::watch::channel(true);
    let (answer, answer_rx) = tokio::sync::watch::channel(false);
    let (port, mut observed, server) = spawn_fixture(greet_rx, answer_rx).await;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(81_002);
    let (mut pipeline, lease) = pipeline_on_fixture(&temp_dir, port, job_id, 3).await;
    pipeline.nntp.pool().quarantine_for_transport_for_test(0);
    pipeline.nntp.pool().expire_quarantine_for_test(0);
    assert!(pipeline.nntp.pool().requires_recovery(0));

    pipeline.spawn_download_batch(lease);
    // The hand-back waits for its acknowledgement before any BODY goes out.
    match pipeline.owned_download_lane_event_rx.recv().await.unwrap() {
        OwnedDownloadLaneEvent::BatchComplete {
            results,
            unrequested_works,
            ack,
            ..
        } => {
            assert!(results.is_empty(), "nothing was fetched yet");
            let returned: Vec<_> = unrequested_works
                .iter()
                .map(|work| work.segment_id.segment_number)
                .collect();
            assert_eq!(returned, vec![1, 2], "every article but the probe's");
            ack.expect("the hand-back is acknowledged").send(()).unwrap();
        }
        OwnedDownloadLaneEvent::LeaseObsolete { .. } => {
            panic!("the probe's own lane must carry its lease")
        }
        OwnedDownloadLaneEvent::AcquireFailed { error, .. } => {
            panic!("the probe dial failed: {error:?}")
        }
    }

    // The probe's one article reaches the server, which holds its answer.
    let mut commands = Vec::new();
    loop {
        match observed.recv().await.unwrap() {
            Seen::Accepted => {}
            Seen::Command(command) => {
                let body = command.starts_with("BODY ");
                commands.push(command);
                if body {
                    break;
                }
            }
            Seen::Closed => panic!("the fixture closed before any BODY"),
        }
    }
    assert!(commands.last().unwrap().contains("guard-0@"), "{commands:?}");
    assert!(pipeline.nntp.pool().requires_recovery(0), "the probe is out");
    answer.send(true).unwrap();

    // The answer comes back, the lane asks for more and is told there is
    // none, and parks.
    let mut fetched = Vec::new();
    loop {
        tokio::select! {
            Some(event) = pipeline.owned_download_lane_event_rx.recv() => {
                let OwnedDownloadLaneEvent::BatchComplete { results, unrequested_works, ack, .. } = event else {
                    panic!("only the probe's answer is still to come");
                };
                assert!(unrequested_works.is_empty());
                fetched.extend(results.iter().map(|result| result.segment_id.segment_number));
                if let Some(ack) = ack {
                    ack.send(()).unwrap();
                }
            }
            Some(request) = pipeline.download_refill_rx.recv() => {
                assert!(request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                }).is_ok());
            }
            Some(_) = pipeline.download_lane_parked_rx.recv() => break,
        }
    }
    assert_eq!(fetched, vec![0], "only the probe's article was fetched");
    assert!(
        !pipeline.nntp.pool().requires_recovery(0),
        "the 430 answered the probe"
    );

    let commands = close_lane_and_collect(&pipeline, &mut observed, commands).await;
    assert_eq!(bodies(&commands), 1, "one article while the probe was out: {commands:?}");
    server.await.unwrap();
}
