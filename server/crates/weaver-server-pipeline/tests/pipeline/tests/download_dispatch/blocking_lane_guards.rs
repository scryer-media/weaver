// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a download lane does with work it may no longer send, and with the
// recovery probe it is carrying. Every wait here is on the fixture server's
// own report or on a lane event: nothing waits on a clock.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use weaver_server_pipeline::JobId;
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::blocking_lane::{
    LaneEvent, LaneGuard, LaneProgress, lane_guard,
};

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

// Close the lane's cached connection and wait for the fixture to see it
// close. Everything the lane wrote before then has been reported.
async fn close_lane_and_collect(
    pipeline: &LaneGuard,
    observed: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>,
    mut commands: Vec<String>,
) -> Vec<String> {
    assert!(pipeline.no_leases_after_reset());
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
    let mut pipeline = lane_guard(&temp_dir, port, job_id, 2).await;

    pipeline.spawn();
    // The lane passed its first check and is inside the dial, waiting for
    // the greeting. Downloads stop now, before the lane holds a connection.
    assert!(matches!(observed.recv().await.unwrap(), Seen::Accepted));
    pipeline.stop_downloads();
    greet.send(true).unwrap();

    match pipeline.next_event().await {
        LaneEvent::Obsolete { job, work_count } => {
            assert_eq!(job, job_id);
            assert_eq!(work_count, 2, "every work comes back unsent");
        }
        LaneEvent::Refused(error) => {
            panic!("the dial succeeded, yet the lane reported {error:?}")
        }
        LaneEvent::Complete { fetched, .. } => {
            panic!("obsolete work produced {} results", fetched.len())
        }
    }

    let commands = close_lane_and_collect(&pipeline, &mut observed, Vec::new()).await;
    assert_eq!(
        bodies(&commands),
        0,
        "no article was asked for: {commands:?}"
    );
    {
        let health = pipeline.client().pool().health().lock().await;
        assert_eq!(health.server(0).failure_count, 0);
        assert_eq!(health.server(0).consecutive_failures, 0);
    }
    assert!(!pipeline.client().pool().requires_recovery(0));
    assert!(
        !pipeline
            .client()
            .pool()
            .is_over_limit(weaver_nntp::ServerId(0))
    );
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
    let mut pipeline = lane_guard(&temp_dir, port, job_id, 3).await;
    pipeline
        .client()
        .pool()
        .quarantine_for_transport_for_test(0);
    pipeline.client().pool().expire_quarantine_for_test(0);
    assert!(pipeline.client().pool().requires_recovery(0));

    pipeline.spawn();
    // The hand-back waits for its acknowledgement before any BODY goes out.
    match pipeline.next_event().await {
        LaneEvent::Complete {
            fetched,
            unrequested,
            ack,
        } => {
            assert!(fetched.is_empty(), "nothing was fetched yet");
            assert_eq!(unrequested, vec![1, 2], "every article but the probe's");
            ack.expect("the hand-back is acknowledged")
                .send(())
                .unwrap();
        }
        LaneEvent::Obsolete { .. } => {
            panic!("the probe's own lane must carry its lease")
        }
        LaneEvent::Refused(error) => {
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
    assert!(
        commands.last().unwrap().contains("guard-0@"),
        "{commands:?}"
    );
    assert!(
        pipeline.client().pool().requires_recovery(0),
        "the probe is out"
    );
    answer.send(true).unwrap();

    // The answer comes back, the lane asks for more and is told there is
    // none, and parks.
    let mut fetched = Vec::new();
    loop {
        match pipeline.decline_refill_or_hear_lane().await {
            LaneProgress::Event(event) => {
                let LaneEvent::Complete {
                    fetched: results,
                    unrequested,
                    ack,
                } = event
                else {
                    panic!("only the probe's answer is still to come");
                };
                assert!(unrequested.is_empty());
                fetched.extend(results);
                if let Some(ack) = ack {
                    ack.send(()).unwrap();
                }
            }
            LaneProgress::RefillDeclined(sent) => assert!(sent),
            LaneProgress::Parked => break,
        }
    }
    assert_eq!(fetched, vec![0], "only the probe's article was fetched");
    assert!(
        !pipeline.client().pool().requires_recovery(0),
        "the 430 answered the probe"
    );

    let commands = close_lane_and_collect(&pipeline, &mut observed, commands).await;
    assert_eq!(
        bodies(&commands),
        1,
        "one article while the probe was out: {commands:?}"
    );
    server.await.unwrap();
}
