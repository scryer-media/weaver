// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use weaver_tunnel::pipe::{
    SessionSockets, SocketBudget, SocketClass, overhead_bytes, stream_bytes,
};

// A tunnel whose torrents hold every socket but usenet's probe, which usenet
// holds: every usenet dial over it is refused, and presses.
fn full_tunnel() -> (Arc<SessionSockets>, Vec<weaver_tunnel::pipe::SocketPermit>) {
    let peer = stream_bytes(None);
    let tunnel = Arc::new(SessionSockets::new(
        SocketBudget {
            bytes: overhead_bytes(1420) + 4 * peer + 20 * peer,
            reserve: 4 * peer,
            torrent_kept: 0,
            mtu: 1420,
        },
        Arc::new(crate::proxies::SplitDivider::Fixed(None)),
    ));
    let mut held: Vec<_> = std::iter::from_fn(|| {
        tunnel
            .has_room(SocketClass::Torrent, peer)
            .then(|| tunnel.take(SocketClass::Torrent, peer).ok().flatten())
            .flatten()
    })
    .collect();
    loop {
        match tunnel.take(SocketClass::Nntp, peer) {
            Ok(permit) => held.extend(permit),
            Err(refused) => {
                drop(refused);
                break;
            }
        }
    }
    assert!(!tunnel.pressing(SocketClass::Nntp));
    (tunnel, held)
}

// A tunnel route that refuses every dial for room while `full`, with the
// tunnel's own refusal, and dials straight through otherwise.
struct GatedTunnel {
    tunnel: Arc<SessionSockets>,
    full: AtomicBool,
    inner: Arc<dyn weaver_tunnel::pipe::Dialer>,
}

#[async_trait::async_trait]
impl weaver_tunnel::pipe::Dialer for GatedTunnel {
    async fn dial(
        &self,
        target: &weaver_tunnel::pipe::Target,
    ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
        if self.full.load(Ordering::SeqCst) {
            return match self.tunnel.take(SocketClass::Nntp, stream_bytes(None)) {
                Err(refused) => Err(weaver_tunnel::pipe::DialError::AtCapacity(
                    weaver_tunnel::pipe::Capacity::from(refused),
                )),
                Ok(_) => panic!("the full tunnel had room"),
            };
        }
        self.inner.dial(target).await
    }
    fn budget(&self) -> Duration {
        self.inner.budget()
    }
    fn describe(&self) -> String {
        "gated tunnel fixture".into()
    }
}

// Server A behind `route` on `port`, and server B at its provider's limit.
fn client_with_a_behind(route: Arc<GatedTunnel>, port: u16) -> Arc<weaver_nntp::NntpClient> {
    let direct = weaver_nntp::route_dialer::RouteDialer::direct_for_tests();
    let a = weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        dialer: Arc::new(weaver_nntp::route_dialer::RouteDialer {
            inner: route,
            plan: None,
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
            runtime: direct.runtime.clone(),
            server: 1,
        }),
        ..Default::default()
    };
    let b = weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port: 1,
        tls: false,
        ..Default::default()
    };
    let nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![
                weaver_nntp::pool::ServerPoolConfig {
                    max_connections: 1,
                    stable_id: weaver_nntp::transfer::StableServerId(1),
                    ..weaver_nntp::pool::ServerPoolConfig::new(a)
                },
                weaver_nntp::pool::ServerPoolConfig {
                    max_connections: 1,
                    stable_id: weaver_nntp::transfer::StableServerId(2),
                    ..weaver_nntp::pool::ServerPoolConfig::new(b)
                },
            ],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        },
    ));
    nntp.pool()
        .note_provider_over_limit(weaver_nntp::pool::ServerId(1));
    nntp
}

struct LaneChannels {
    events: mpsc::Receiver<OwnedDownloadLaneEvent>,
    refills: mpsc::Receiver<DownloadLaneRefillRequest>,
    parks: mpsc::Receiver<DownloadLaneParked>,
}

// A worker's run on a blocking thread, giving back its connection and press.
type WorkerRun = tokio::task::JoinHandle<(Option<CachedOwnedLane>, Option<RoutePress>)>;

// One run of `job`'s single article on a worker holding `press`, on a
// blocking thread; the worker's connection and press come back with it.
fn run_on_worker(
    nntp: &Arc<weaver_nntp::NntpClient>,
    state: &crate::jobs::handle::SharedPipelineState,
    cached: Option<CachedOwnedLane>,
    press: Option<RoutePress>,
) -> (WorkerRun, LaneChannels) {
    let (event_tx, events) = mpsc::channel(16);
    let (refill_tx, refills) = mpsc::channel(16);
    let (parked_tx, parks) = mpsc::channel(16);
    let run = OwnedLaneRun {
        nntp: Arc::clone(nntp),
        state: state.clone(),
        event_tx,
        refill_tx,
        parked_tx,
        initial_lease: test_lease(
            JobId(42),
            0,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        ),
    };
    let worker = tokio::task::spawn_blocking(move || {
        let (mut cached, mut press) = (cached, press);
        run_owned_lane(&mut cached, &mut press, run);
        (cached, press)
    });
    (
        worker,
        LaneChannels {
            events,
            refills,
            parks,
        },
    )
}

// Server A's tunnel is full and server B is at its provider's limit, so B's
// refusal is the error the lane reports. The lane holds A's press anyway,
// past the event's handling, which keeps no demand: the press stands until
// the lane's next dial resolves. That dial opens a connection, and the press
// is gone before its first BODY is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lane_refused_for_a_full_tunnel_presses_until_its_next_dial_resolves() {
    let (tunnel, _torrents) = full_tunnel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let route = Arc::new(GatedTunnel {
        tunnel: Arc::clone(&tunnel),
        full: AtomicBool::new(true),
        inner: weaver_nntp::route_dialer::RouteDialer::direct_for_tests()
            .inner
            .clone(),
    });
    let nntp = client_with_a_behind(Arc::clone(&route), port);
    let state = test_admitted_state(JobId(42));

    let (worker, mut lane) = run_on_worker(&nntp, &state, None, None);
    let (cached, press) = worker.await.unwrap();
    let Some(OwnedDownloadLaneEvent::AcquireFailed { error, .. }) = lane.events.recv().await else {
        panic!("the dial was not refused");
    };
    assert!(
        matches!(
            error,
            weaver_nntp::client::BlockingBodyLaneAcquireError::ProviderCapacity(_)
        ),
        "{error:?}"
    );
    drop((error, lane));
    assert!(cached.is_none());
    assert!(press.is_some(), "the lane holds the press");
    assert!(
        tunnel.pressing(SocketClass::Nntp),
        "A's tunnel is pressed while B's refusal answered"
    );

    // A's tunnel has room again; the lane's next dial opens a connection.
    route.full.store(false, Ordering::SeqCst);
    let (pressed_at_body_tx, pressed_at_body) = oneshot::channel();
    let server = {
        let tunnel = Arc::clone(&tunnel);
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (read, mut write) = socket.into_split();
            write.write_all(b"200 ready\r\n").await.unwrap();
            let mut lines = BufReader::new(read).lines();
            let mut pressed_at_body_tx = Some(pressed_at_body_tx);
            while let Some(line) = lines.next_line().await.unwrap() {
                if line == "CAPABILITIES" {
                    write
                        .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                        .await
                        .unwrap();
                } else if line.starts_with("BODY ") {
                    if let Some(tx) = pressed_at_body_tx.take() {
                        let _ = tx.send(tunnel.pressing(SocketClass::Nntp));
                    }
                    write.write_all(b"430 absent\r\n").await.unwrap();
                } else if line == "QUIT" {
                    break;
                } else {
                    write.write_all(b"500 unsupported\r\n").await.unwrap();
                }
            }
        })
    };
    let (worker, mut lane) = run_on_worker(&nntp, &state, cached, press);
    loop {
        tokio::select! {
            Some(event) = lane.events.recv() => {
                if let OwnedDownloadLaneEvent::BatchComplete { ack: Some(ack), .. } = event {
                    let _ = ack.send(());
                }
            }
            Some(request) = lane.refills.recv() => {
                let _ = request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                });
            }
            Some(_) = lane.parks.recv() => break,
        }
    }
    let (mut cached, press) = worker.await.unwrap();
    assert!(
        !pressed_at_body.await.unwrap(),
        "the press outlived the dial that opened a connection"
    );
    assert!(press.is_none());
    assert!(!tunnel.pressing(SocketClass::Nntp));
    park_cached_lane(&mut cached);
    server.await.unwrap();
}

// A worker holding a press publishes it, and the next run with no
// connection to reuse is routed to it first, so its own dial settles the
// press. The press goes when the job it was for stops wanting downloads.
#[test]
fn a_pressing_worker_takes_the_next_dial_and_lets_its_press_go_with_its_job() {
    let (tunnel, _torrents) = full_tunnel();
    let Err(refused) = tunnel.take(SocketClass::Nntp, stream_bytes(None)) else {
        panic!("the tunnel had room");
    };
    let state = test_admitted_state(JobId(42));
    let mut press = Some(RoutePress {
        state: state.clone(),
        job_id: JobId(42),
        generation: 0,
        report: None,
        _demand: refused.demand.clone().into_iter().collect(),
    });
    drop(refused);
    assert!(tunnel.pressing(SocketClass::Nntp));

    let idle = || Some(IdleOwnedLaneWorker { lane: None });
    let shared = Arc::new(std::sync::Mutex::new(tests_shared_with(vec![
        idle(),
        idle(),
    ])));
    let seat = OwnedLaneSeat { index: 1, id: 1 };
    let mut published = false;
    settle_route_press(seat, &shared, &None, &mut press, &mut published);
    assert!(published && press.is_some());
    assert_eq!(
        lock_pool(&shared).claim_worker_where(|_| false),
        Some(1),
        "the pressing worker dials next"
    );

    state.queue.write(|inner| {
        inner.jobs[0].intents.paused = Some(true);
        crate::pipeline::queue::Change::changed(())
    });
    settle_route_press(seat, &shared, &None, &mut press, &mut published);
    assert!(press.is_none() && !published);
    assert!(!lock_pool(&shared).workers[1].as_ref().unwrap().pressing);
    assert!(
        !tunnel.pressing(SocketClass::Nntp),
        "a stopped job's lane presses no more"
    );
}

// Two idle worker slots, as the pool's own routing tests build them.
fn tests_shared_with(idle: Vec<Option<IdleOwnedLaneWorker>>) -> OwnedLanePoolShared {
    let workers: Vec<_> = idle
        .into_iter()
        .enumerate()
        .map(|(index, idle)| {
            Some(OwnedLaneWorkerSlot {
                id: index as u64,
                sender: std_mpsc::channel().0,
                idle,
                pressing: false,
            })
        })
        .collect();
    OwnedLanePoolShared {
        cap: workers.len(),
        next_worker_id: workers.len() as u64,
        workers,
        queued_runs: VecDeque::new(),
        intervals: OwnedLaneIntervals::default(),
        observer: None,
    }
}
