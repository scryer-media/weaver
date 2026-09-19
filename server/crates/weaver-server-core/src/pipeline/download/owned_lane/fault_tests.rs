use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// A refill has been booked by the actor, but the socket dies before the
/// worker can adopt its answer. The final park must release the new booking.
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
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease: test_lease(JobId(42), 0, vec![], vec![tail_work(0, 0)]),
            },
            None,
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

/// A probe reaching a lane mid-lease is answered once the ring drains.
///
/// The lane has three BODY responses outstanding when the probe arrives. The
/// worker takes the probe up, stops issuing, reads the three responses, and
/// only then writes the STAT: on the wire every BODY precedes it, so the STAT
/// never reads an article payload as its status line. The answer is the
/// server's, not an inconclusive shrug, and the lease still parks normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_probe_on_a_busy_lane_is_answered_after_the_ring_drains() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let order: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (bodies_seen_tx, bodies_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let server_order = Arc::clone(&order);
    let server = tokio::spawn(async move {
        let mut bodies_seen_tx = Some(bodies_seen_tx);
        let mut release_rx = Some(release_rx);
        let mut held: Vec<String> = Vec::new();
        let (socket, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = socket.into_split();
        writer.write_all(b"200 fixture ready\r\n").await.unwrap();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            if line == "CAPABILITIES" {
                writer
                    .write_all(b"101 Capabilities\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n")
                    .await
                    .unwrap();
            } else if line.starts_with("BODY ") {
                server_order.lock().unwrap().push("BODY".into());
                held.push(line);
                if held.len() == 3 {
                    bodies_seen_tx.take().unwrap().send(()).unwrap();
                    release_rx.take().unwrap().await.unwrap();
                    for _ in held.drain(..) {
                        writer.write_all(b"430 no such article\r\n").await.unwrap();
                    }
                }
            } else if let Some(id) = line.strip_prefix("STAT ") {
                server_order.lock().unwrap().push("STAT".into());
                writer
                    .write_all(format!("223 0 {id}\r\n").as_bytes())
                    .await
                    .unwrap();
            } else if line.starts_with("HEAD ") {
                server_order.lock().unwrap().push("HEAD".into());
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
    let (commands_tx, commands_rx) = std_mpsc::channel();
    let shared = Arc::new(std::sync::Mutex::new(OwnedLanePoolShared {
        workers: vec![OwnedLaneWorkerSlot {
            sender: commands_tx.clone(),
            idle: None,
            busy_server: None,
        }],
        queued_runs: VecDeque::new(),
    }));
    let client = Arc::clone(&nntp);
    let worker_shared = Arc::clone(&shared);
    let worker = tokio::task::spawn_blocking(move || {
        let mut cached = None;
        run_owned_blocking_download_lane(
            &mut cached,
            OwnedLaneRun {
                nntp: client,
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease: test_lease(
                    JobId(42),
                    0,
                    vec![],
                    vec![tail_work(0, 0), tail_work(1, 0), tail_work(2, 0)],
                ),
            },
            Some(WorkerLink {
                index: 0,
                shared: &worker_shared,
                commands: &commands_rx,
            }),
        )
    });
    let (picked_up_tx, picked_up_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    async {
        bodies_seen_rx.await.expect("three BODYs pipelined before any answer");
        assert_eq!(
            lock_pool(&shared).workers[0].busy_server,
            Some(0),
            "a lane on a connection publishes its server for the probe router"
        );
        commands_tx
            .send(OwnedLanePoolCommand::Probe {
                message_ids: Arc::from(["<probe@silver.horizon>".to_string()]),
                picked_up: picked_up_tx,
                reply: reply_tx,
            })
            .unwrap();
        release_tx.send(()).unwrap();
        picked_up_rx.await.expect("the busy worker takes the probe up");
        let answer = reply_rx
            .await
            .expect("the worker answers rather than dropping the probe")
            .expect("the answer comes from the lane, not a decline");
        assert_eq!(answer.exists, vec![true]);
        assert!(!answer.inconclusive);

        let mut results = 0;
        loop {
            tokio::select! {
                Some(request) = refills.recv() => {
                    assert!(request.response_tx.send(DownloadLaneRefillResponse { lease: None, park_reason: LaneParkReason::NoWork }).is_ok());
                }
                Some(event) = events.recv() => {
                    let OwnedDownloadLaneEvent::BatchComplete { results: batch, ack, .. } = event else { panic!("unexpected acquisition failure"); };
                    results += batch.len();
                    if let Some(ack) = ack { ack.send(()).unwrap(); }
                }
                Some(park) = parks.recv() => {
                    assert!(matches!(park.reason, LaneParkReason::NoWork), "the lease parks normally after the probe");
                    break;
                }
            }
        }
        assert_eq!(results, 3, "every BODY on the ring is still reported");
    }
    .await;
    let deferred = worker.await.unwrap();
    assert!(
        deferred.is_empty(),
        "nothing but the probe reached the worker"
    );
    drop(nntp);
    server.await.unwrap();

    let order = order.lock().unwrap();
    let last_body = order.iter().rposition(|command| command == "BODY").unwrap();
    let first_stat = order
        .iter()
        .position(|command| command == "STAT")
        .expect("the probe reached the wire");
    assert_eq!(order.iter().filter(|command| *command == "BODY").count(), 3);
    assert!(
        last_body < first_stat,
        "every BODY precedes the STAT on the wire: {order:?}"
    );
    assert!(
        !order.iter().any(|command| command == "HEAD"),
        "STAT settled the batch"
    );
}
