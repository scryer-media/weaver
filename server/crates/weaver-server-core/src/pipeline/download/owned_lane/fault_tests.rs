// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_tail_and_granted_refill_are_returned_when_the_job_becomes_ineligible() {
    for transition in ["pause", "generation", "admission", "global_pause"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (read, mut write) = socket.into_split();
            write.write_all(b"200 ready\r\n").await.unwrap();
            let mut lines = BufReader::new(read).lines();
            let mut release = Some(release_rx);
            let mut bodies = 0;
            while let Some(line) = lines.next_line().await.unwrap() {
                if line == "CAPABILITIES" {
                    write
                        .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                        .await
                        .unwrap();
                } else if line.starts_with("BODY ") {
                    bodies += 1;
                    if let Some(release) = release.take() {
                        release.await.unwrap();
                    }
                    write.write_all(b"430 absent\r\n").await.unwrap();
                } else if line == "QUIT" {
                    break;
                } else {
                    write.write_all(b"500 unsupported\r\n").await.unwrap();
                }
            }
            bodies
        });
        let nntp = Arc::new(weaver_nntp::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".into(),
                    port,
                    tls: false,
                    ..Default::default()
                },
                1,
            ),
        ));
        let state = test_admitted_state(JobId(42));
        let worker_state = state.clone();
        let (event_tx, mut events) = mpsc::channel(16);
        let (refill_tx, mut refills) = mpsc::channel(16);
        let (parked_tx, mut parks) = mpsc::channel(16);
        let worker = tokio::task::spawn_blocking(move || {
            let mut cached = None;
            run_owned_blocking_download_lane(
                &mut cached,
                OwnedLaneRun {
                    nntp,
                    state: worker_state,
                    event_tx,
                    refill_tx,
                    parked_tx,
                    initial_lease: test_lease(
                        JobId(42),
                        0,
                        vec![],
                        (0..3).map(|i| tail_work(i, 0)).collect(),
                    ),
                },
            );
        });
        let request = refills
            .recv()
            .await
            .expect("issued BODY requests its next lease");
        let mut next = test_lease(JobId(42), 0, vec![], vec![tail_work(3, 0)]);
        next.completion_critical = true;
        assert!(
            request
                .response_tx
                .send(DownloadLaneRefillResponse {
                    lease: Some(next),
                    park_reason: LaneParkReason::NoWork,
                })
                .is_ok()
        );
        state.queue.write(|inner| {
            match transition {
                "pause" => inner.jobs[0].intents.paused = Some(true),
                "generation" => inner.globals.nntp_generation += 1,
                "admission" => inner.jobs[0].report.nzb_admitted = false,
                "global_pause" => inner.globals.paused = true,
                _ => unreachable!(),
            }
            crate::pipeline::queue::Change::changed(())
        });
        release_tx.send(()).unwrap();
        let mut returned = Vec::new();
        let mut completed = 0;
        loop {
            tokio::select! {
                Some(event) = events.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { results, unrequested_works, ack, .. } = event else {
                        panic!("the initial lease has already issued its BODY");
                    };
                    completed += results.len();
                    returned.extend(unrequested_works.into_iter().map(|work| work.segment_id.segment_number));
                    if let Some(ack) = ack { ack.send(()).unwrap(); }
                }
                Some(park) = parks.recv() => {
                    assert_eq!(park.reason, LaneParkReason::Capacity);
                    assert!(park.completion_critical, "the refill booking must be released");
                    break;
                }
            }
        }
        worker.await.unwrap();
        assert_eq!(
            server.await.unwrap(),
            1,
            "{transition} must prevent new BODY commands"
        );
        returned.sort_unstable();
        assert_eq!(
            returned,
            vec![1, 2, 3],
            "{transition} returns reservations once"
        );
        assert_eq!(completed, 1, "the already-issued BODY still completes");
    }
}

// A refill has been booked by the actor, but the socket dies before the
// worker can adopt its answer. The final park must release the new booking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_fault_after_prefetch_releases_the_granted_connection_class() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (close_tx, close_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = socket.into_split();
        writer.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == "CAPABILITIES" {
                writer
                    .write_all(b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("GROUP ") {
                writer
                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("BODY ") {
                close_rx.await.unwrap();
                break;
            } else {
                writer.write_all(b"500 unsupported\r\n").await.unwrap();
            }
        }
    });
    let nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port,
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    let (event_tx, mut events) = mpsc::channel(16);
    let (refill_tx, mut refills) = mpsc::channel(16);
    let (parked_tx, mut parks) = mpsc::channel(16);
    let client = Arc::clone(&nntp);
    let worker = tokio::task::spawn_blocking(move || {
        let mut cached = None;
        run_owned_blocking_download_lane(
            &mut cached,
            OwnedLaneRun {
                nntp: client,
                state: test_admitted_state(JobId(42)),
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease: test_lease(JobId(42), 0, vec![], vec![tail_work(0, 0)]),
            },
        );
    });
    async {
        let request = refills.recv().await.expect("the first article prefetches its successor");
        close_tx.send(()).unwrap();
        // Let the socket failure reach the worker while the refill remains
        // unanswered, so it necessarily takes the drain path, not adoption.
        let first = events.recv().await.unwrap();
        let OwnedDownloadLaneEvent::BatchComplete { results, .. } = first else {
            panic!("the established connection must reach a BODY failure");
        };
        assert!(matches!(&results[0].data, Err(DownloadError::Fetch(_))));
        let mut work = tail_work(1, 0);
        work.is_recovery = true;
        work.completion_critical = true;
        let mut next = test_lease(JobId(42), 0, vec![], vec![work]);
        next.completion_critical = true;
        assert!(request.response_tx.send(DownloadLaneRefillResponse { lease: Some(next), park_reason: LaneParkReason::NoWork }).is_ok());
        let mut returned = 0;
        loop {
            tokio::select! {
                Some(event) = events.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { unrequested_works, ack, .. } = event else { panic!("unexpected acquisition failure"); };
                    returned += unrequested_works.len();
                    if let Some(ack) = ack { ack.send(()).unwrap(); }
                }
                Some(park) = parks.recv() => {
                    assert_eq!(returned, 1, "the prefetched recovery article is returned exactly once");
                    assert!(park.completion_critical, "the actor already moved this connection into the critical class");
                    assert!(park.release_connection_slot);
                    break;
                }
            }
        }
    }
    .await;
    worker.await.unwrap();
    server.await.unwrap();
    assert_eq!(
        nntp.pool().active_connections(0),
        0,
        "the failed socket returns its permit"
    );
}

// A lane that parks with articles still in hand returns them before it
// waits on its outstanding refill.
//
// The actor holds a saturated lane's refill until the lane's holdings fall
// below its share, and a lane parking on a transport fault with a full
// pending tail only gets there by returning that tail. Waiting first would
// leave the two sides each waiting on the other: the refill never answered,
// the articles never requeued, the connection slot never released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parking_lane_returns_its_tail_before_waiting_on_its_refill() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (close_tx, close_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = socket.into_split();
        writer.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == "CAPABILITIES" {
                writer
                    .write_all(b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("GROUP ") {
                writer
                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("BODY ") {
                close_rx.await.unwrap();
                break;
            } else {
                writer.write_all(b"500 unsupported\r\n").await.unwrap();
            }
        }
    });
    let nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port,
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    let (event_tx, mut events) = mpsc::channel(16);
    let (refill_tx, mut refills) = mpsc::channel(16);
    let (parked_tx, mut parks) = mpsc::channel(16);
    let client = Arc::clone(&nntp);
    // Three articles on a sequential lane: one goes on the wire and two stay
    // in hand, and three is within the deadline that asks ahead.
    let works = (0..3).map(|segment| tail_work(segment, 0)).collect();
    let worker = tokio::task::spawn_blocking(move || {
        let mut cached = None;
        run_owned_blocking_download_lane(
            &mut cached,
            OwnedLaneRun {
                nntp: client,
                state: test_admitted_state(JobId(42)),
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease: test_lease(JobId(42), 0, vec![], works),
            },
        );
    });
    async {
        let request = refills.recv().await.expect("the lane asks ahead for its next batch");
        close_tx.send(()).unwrap();
        // The refill stays unanswered, as a saturated lane's does, until the
        // lane has handed back the two articles it never issued.
        let mut returned = 0;
        let mut faulted = 0;
        while returned < 2 {
            let OwnedDownloadLaneEvent::BatchComplete { results, unrequested_works, ack, .. } =
                events.recv().await.unwrap()
            else {
                panic!("unexpected acquisition failure");
            };
            faulted += results.len();
            returned += unrequested_works.len();
            if let Some(ack) = ack {
                ack.send(()).unwrap();
            }
        }
        assert_eq!(returned, 2, "the unissued tail comes back whole");
        assert_eq!(faulted, 1, "the issued article faults before the tail is returned");
        assert!(parks.try_recv().is_err(), "the lane has not parked yet");
        assert!(
            request
                .response_tx
                .send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                })
                .is_ok(),
            "the lane is still waiting on its refill"
        );
        let park = loop {
            tokio::select! {
                Some(event) = events.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { unrequested_works, ack, .. } = event else {
                        panic!("unexpected acquisition failure");
                    };
                    assert!(unrequested_works.is_empty(), "nothing is returned twice");
                    if let Some(ack) = ack {
                        ack.send(()).unwrap();
                    }
                }
                Some(park) = parks.recv() => break park,
            }
        };
        assert!(park.release_connection_slot);
    }
    .await;
    worker.await.unwrap();
    server.await.unwrap();
}

// A parking lane asks the allocator to release its idle memory, exactly once.
//
// The lane thread allocates the article buffers that the decode and writer
// threads free, so with a per-thread-heap allocator the freed pages pile up
// on a lane that has gone idle. The release is the lane's own last act before
// it reports itself parked, and it must not fire more than once per park:
// doing it per article or per refill would put a full heap collection on the
// hot path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parking_lane_releases_its_idle_thread_memory_once() {
    static RELEASES: AtomicUsize = AtomicUsize::new(0);

    fn count_release() {
        RELEASES.fetch_add(1, Ordering::SeqCst);
    }

    // The hook is process-wide and installed once, so this test owns it for
    // the whole binary; each test runs in its own process.
    crate::runtime::thread_release::install_idle_thread_release(count_release);
    let before = RELEASES.load(Ordering::SeqCst);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = socket.into_split();
        writer.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == "CAPABILITIES" {
                writer
                    .write_all(b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("GROUP ") {
                writer
                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("BODY ") {
                writer.write_all(b"430 no such article\r\n").await.unwrap();
            } else if line == "QUIT" {
                writer.write_all(b"205 bye\r\n").await.unwrap();
                break;
            } else {
                writer.write_all(b"500 unsupported\r\n").await.unwrap();
            }
        }
    });
    let nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".into(),
                port,
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    let (event_tx, mut events) = mpsc::channel(16);
    let (refill_tx, mut refills) = mpsc::channel(16);
    let (parked_tx, mut parks) = mpsc::channel(16);
    let client = Arc::clone(&nntp);
    let worker = tokio::task::spawn_blocking(move || {
        let mut cached = None;
        run_owned_blocking_download_lane(
            &mut cached,
            OwnedLaneRun {
                nntp: client,
                state: test_admitted_state(JobId(42)),
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease: test_lease(JobId(42), 0, vec![], vec![tail_work(0, 0)]),
            },
        );
    });
    async {
        let park = loop {
            tokio::select! {
                Some(request) = refills.recv() => {
                    // No successor: the lease is done and the lane parks.
                    let _ = request.response_tx.send(DownloadLaneRefillResponse {
                        lease: None,
                        park_reason: LaneParkReason::NoWork,
                    });
                }
                Some(event) = events.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { ack, .. } = event else {
                        panic!("unexpected acquisition failure");
                    };
                    if let Some(ack) = ack {
                        assert_eq!(
                            RELEASES.load(Ordering::SeqCst),
                            before,
                            "the release happens after the closing batch event, not before it"
                        );
                        ack.send(()).unwrap();
                    }
                }
                Some(park) = parks.recv() => break park,
            }
        };
        assert_eq!(park.reason, LaneParkReason::NoWork);
    }
    .await;
    worker.await.unwrap();
    server.await.unwrap();

    assert_eq!(
        RELEASES.load(Ordering::SeqCst) - before,
        1,
        "one park, one release"
    );
}
