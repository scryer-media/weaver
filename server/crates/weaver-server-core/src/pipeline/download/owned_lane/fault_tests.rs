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
                hot_share_yield_signal: Arc::new(HotShareYieldSignal::default()),
                initial_lease: test_lease(JobId(42), 0, vec![], vec![tail_work(0, 0)]),
            },
        );
    });
    tokio::time::timeout(Duration::from_secs(10), async {
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
        next.compatibility.is_recovery = true;
        next.compatibility.completion_critical = true;
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
    }).await.expect("faulted worker must return work and park");
    worker.await.unwrap();
    server.await.unwrap();
    assert_eq!(
        nntp.pool().active_connections(0),
        0,
        "the failed socket returns its permit"
    );
}
