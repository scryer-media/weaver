// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use weaver_server_base::jobs::ids::JobId;

fn test_client() -> Arc<weaver_nntp::NntpClient> {
    Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 1,
                tls: false,
                ..Default::default()
            },
            4,
        ),
    ))
}

#[test]
fn queued_lease_requires_current_admission_and_runtime() {
    let nntp = test_client();
    let state = test_admitted_state(JobId(42));
    let mut lease = test_lease(
        JobId(42),
        0,
        weaver_nntp::ServerSet::EMPTY,
        vec![tail_work(1, 0)],
    );
    assert!(lease_is_current(&state, &nntp, &lease, None));
    state.queue.write(|inner| {
        inner.jobs[0].report.nzb_admitted = false;
        crate::pipeline::queue::Change::changed(())
    });
    assert!(!lease_is_current(&state, &nntp, &lease, None));
    state.queue.write(|inner| {
        inner.jobs[0].report.nzb_admitted = true;
        inner.jobs[0].intents.paused = Some(true);
        crate::pipeline::queue::Change::changed(())
    });
    assert!(!lease_is_current(&state, &nntp, &lease, None));
    state.queue.write(|inner| {
        inner.jobs[0].intents.paused = Some(false);
        inner.globals.nntp_generation = 1;
        crate::pipeline::queue::Change::changed(())
    });
    assert!(!lease_is_current(&state, &nntp, &lease, None));
    lease.runtime_generation = 1;
    assert!(lease_is_current(&state, &nntp, &lease, None));
    state.queue.write(|inner| {
        inner
            .globals
            .holds
            .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
        crate::pipeline::queue::Change::changed(())
    });
    assert!(!lease_is_current(&state, &nntp, &lease, None));
}

// A lease is cut for one server and pinned to it, so two works whose own
// exclusions differ stay current while that server may serve both.
#[test]
fn a_lease_of_works_with_differing_exclusions_stays_current_on_its_server() {
    let server = |port| weaver_nntp::pool::ServerPoolConfig {
        max_connections: 4,
        ..weaver_nntp::pool::ServerPoolConfig::new(weaver_nntp::ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            tls: false,
            ..Default::default()
        })
    };
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server(1), server(2)],
            ..weaver_nntp::client::NntpClientConfig::single(weaver_nntp::ServerConfig::default(), 1)
        },
    ));
    let state = test_admitted_state(JobId(42));
    let mut not_found_on_zero = tail_work(1, 1);
    not_found_on_zero.exclude_servers = weaver_nntp::ServerSet::from([0]);
    let lease = test_lease(
        JobId(42),
        0,
        weaver_nntp::ServerSet::from([0]),
        vec![not_found_on_zero, tail_work(2, 0)],
    );
    assert!(lease_is_current(&state, &nntp, &lease, None));
}

// A lease cut for a server whose quarantine has lapsed is carried by the
// dial that becomes the probe. While that probe is out the server takes
// no new connection, so a lane with no connection finds the lease
// obsolete; the probe's own connection does not, or it would never issue
// the article it was admitted for, the probe would never answer, and the
// server would never come back.
#[test]
fn the_probe_connection_still_carries_its_lease_while_the_probe_is_out() {
    let nntp = test_client();
    let state = test_admitted_state(JobId(42));
    let lease = test_lease(
        JobId(42),
        0,
        weaver_nntp::ServerSet::EMPTY,
        vec![tail_work(1, 0)],
    );
    nntp.pool().quarantine_for_transport_for_test(0);
    nntp.pool().expire_quarantine_for_test(0);
    assert!(
        lease_is_current(&state, &nntp, &lease, None),
        "the probe is due, so the lease has a server to dial"
    );
    let probe = nntp.pool().take_recovery_probe_for_test(0);
    assert!(!lease_is_current(&state, &nntp, &lease, None));
    let on = |holds_probe| {
        Some(HeldConnection {
            server: 0,
            holds_probe,
        })
    };
    assert!(lease_is_current(&state, &nntp, &lease, on(true)));
    assert!(
        !lease_is_current(&state, &nntp, &lease, on(false)),
        "a connection that is not the probe is held with the server"
    );
    drop(probe);
    assert!(lease_is_current(&state, &nntp, &lease, None));
}

fn test_run(
    nntp: &Arc<weaver_nntp::NntpClient>,
    exclude_servers: weaver_nntp::ServerSet,
) -> Box<OwnedLaneRun> {
    let (event_tx, _event_rx) = mpsc::channel(4);
    let (refill_tx, _refill_rx) = mpsc::channel(4);
    let (parked_tx, _parked_rx) = mpsc::channel(4);
    // The receivers are dropped with the run; routing never sends on them.
    let mut lease = test_lease(
        JobId(7),
        1,
        weaver_nntp::ServerSet::EMPTY,
        vec![tail_work(1, 0)],
    );
    lease.dial_exclude_servers = exclude_servers;
    Box::new(OwnedLaneRun {
        nntp: Arc::clone(nntp),
        state: crate::jobs::handle::SharedPipelineState::new(
            weaver_server_base::operations::metrics::PipelineMetrics::new(),
            Vec::new(),
        ),
        event_tx,
        refill_tx,
        parked_tx,
        initial_lease: lease,
    })
}

fn idle_lane(nntp: &Arc<weaver_nntp::NntpClient>, server: usize) -> IdleOwnedLane {
    IdleOwnedLane {
        nntp: Arc::downgrade(nntp),
        server: weaver_nntp::pool::ServerId(server),
    }
}

fn shared_with(idle: Vec<Option<IdleOwnedLaneWorker>>) -> OwnedLanePoolShared {
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

// The worker the test fixtures put in slot `index`.
fn seat(index: usize) -> OwnedLaneSeat {
    OwnedLaneSeat {
        index,
        id: index as u64,
    }
}

fn slot(shared: &OwnedLanePoolShared, index: usize) -> &OwnedLaneWorkerSlot {
    shared.workers[index].as_ref().expect("an occupied slot")
}

fn idle_with(lane: Option<IdleOwnedLane>) -> Option<IdleOwnedLaneWorker> {
    Some(IdleOwnedLaneWorker { lane })
}

// Routing, not rotation.
//
// After a wave parks, every worker sits idle holding a connection — and
// with it every server permit. Handing the next lease to whichever worker
// the counter lands on makes a warm connection useless: the chosen worker
// parks a connection it cannot use and then tries to dial one the idle
// lanes are holding the permits for. The order here is the cost of
// starting: no dial, one dial, a park and a dial.
#[test]
fn a_lease_goes_to_the_worker_that_can_start_it_soonest() {
    let nntp = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::from([3]));
    let mut shared = shared_with(vec![
        // 0: busy on a lease.
        None,
        // 1: idle holding a connection on a server this lease excludes.
        idle_with(Some(idle_lane(&nntp, 3))),
        // 2: idle with no connection at all.
        idle_with(None),
        // 3: idle holding a connection this lease can use.
        idle_with(Some(idle_lane(&nntp, 1))),
    ]);

    assert_eq!(
        shared.claim_worker_for(&run),
        Some(3),
        "the warm, servable connection comes first"
    );
    assert_eq!(
        shared.claim_worker_for(&run),
        Some(2),
        "then the worker that only has to dial"
    );
    assert_eq!(
        shared.claim_worker_for(&run),
        Some(1),
        "then the one that must park a connection before dialling"
    );
    assert_eq!(
        shared.claim_worker_for(&run),
        None,
        "the busy worker is never offered a second lease"
    );
}

// A claim is what stops two submits choosing the same idle worker: the
// marker is taken at selection time, not when the worker starts running.
#[test]
fn claiming_a_worker_takes_it_out_of_the_running() {
    let nntp = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
    let mut shared = shared_with(vec![idle_with(Some(idle_lane(&nntp, 0)))]);

    assert_eq!(shared.claim_worker_for(&run), Some(0));
    assert_eq!(shared.claim_worker_for(&run), None);
}

// An excluded server is not a connection this lease may use, however warm
// it is.
#[test]
fn an_excluded_server_is_not_a_match() {
    let nntp = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::from([1]));
    let lane = idle_lane(&nntp, 1);

    assert!(!lane.serves(&run));
    assert!(idle_lane(&nntp, 0).serves(&run));
}

// The newsgroups a connection was opened for are no reason to redial:
// the worker re-points the socket at the next lease's groups, so a job
// boundary keeps every warm connection in play.
#[test]
fn a_connection_opened_for_other_newsgroups_still_serves() {
    let nntp = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

    assert!(idle_lane(&nntp, 0).serves(&run));
    let mut shared = shared_with(vec![idle_with(None), idle_with(Some(idle_lane(&nntp, 0)))]);
    assert_eq!(
        shared.claim_worker_for(&run),
        Some(1),
        "the warm connection is preferred over a fresh dial whatever its groups"
    );
}

// Whatever connection a lease lands on is pointed at that lease's own
// newsgroups. An empty list is not the same thing: a server that answers
// a message-id fetch only after a GROUP prologue is sent none, and a
// fresh dial has no candidate for its initial group probe.
#[test]
fn a_lease_carries_the_newsgroups_its_work_was_posted_to() {
    let lease = test_lease(
        JobId(7),
        1,
        weaver_nntp::ServerSet::EMPTY,
        vec![tail_work(1, 0)],
    );
    assert_eq!(
        lease_groups(&lease),
        ["alt.binaries.test".to_string()],
        "the lane is opened for the lease's groups"
    );

    let run = test_run(&test_client(), weaver_nntp::ServerSet::EMPTY);
    assert_eq!(
        lease_groups(&run.initial_lease),
        ["alt.binaries.test".to_string()],
        "a run carries them through to the worker"
    );
}

// A connection belonging to a client that has been retired matches
// nothing: the marker is weak precisely so it cannot keep one alive.
#[test]
fn a_connection_from_another_client_is_not_a_match() {
    let nntp = test_client();
    let other = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

    assert!(!idle_lane(&other, 0).serves(&run));
    assert!(!test_idle_lane(0).serves(&run));
}

// With every worker mid-lease the run waits for the pool, not for one
// worker's private channel: whichever finishes first takes it, and it
// does so before publishing itself idle so the run cannot be missed.
#[test]
fn a_run_with_no_idle_worker_waits_for_whichever_finishes_first() {
    let nntp = test_client();
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
    let mut shared = shared_with(vec![None, None]);

    assert!(shared.claim_worker_for(&run).is_none());
    shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));

    // Worker 1 finishes first and takes the queued run instead of going
    // idle; worker 0 finishes into an empty queue and publishes itself.
    assert!(
        shared
            .take_queued_run_or_publish_idle(seat(1), None)
            .is_some_and(|work| matches!(
                work,
                QueuedOwnedLaneWork::Run(run) if run.initial_lease.job_id == JobId(7)
            ))
    );
    assert!(slot(&shared, 1).idle.is_none(), "worker 1 is running it");
    assert!(
        shared
            .take_queued_run_or_publish_idle(seat(0), None)
            .is_none()
    );
    assert!(slot(&shared, 0).idle.is_some());
}

#[test]
fn reset_returns_queued_leases_before_a_worker_can_take_old_work() {
    let nntp = test_client();
    let (sender, commands) = std_mpsc::channel();
    let mut shared = shared_with(vec![None]);
    shared.workers[0].as_mut().unwrap().sender = sender;
    for segment_number in 0..3 {
        let mut run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
        run.initial_lease.works[0].segment_id.segment_number = segment_number;
        shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));
    }
    let pool = OwnedDownloadLanePool {
        shared: Arc::new(std::sync::Mutex::new(shared)),
        reset_calls: AtomicUsize::new(0),
    };

    let returned = pool.reset().leases;

    assert_eq!(returned.len(), 3);
    for (index, lease) in returned.iter().enumerate() {
        assert_eq!(lease.job_id, JobId(7));
        assert_eq!(lease.works.len(), 1);
        assert_eq!(lease.works[0].segment_id.segment_number, index as u32);
    }
    assert_eq!(
        Arc::strong_count(&nntp),
        1,
        "queued runs release the old client"
    );
    assert!(
        lock_pool(&pool.shared)
            .take_queued_run_or_publish_idle(seat(0), None)
            .is_none(),
        "finishing a lease must reach the pending reset without another old run"
    );
    assert!(matches!(
        commands.try_recv(),
        Ok(OwnedLanePoolCommand::Reset)
    ));
    assert!(
        pool.reset().leases.is_empty(),
        "leases are returned only once"
    );
}

// Only workers with nothing to lose are warmed: a worker already holding
// a connection has nothing to gain, and a busy one is not there to ask.
#[test]
fn warming_targets_idle_workers_without_a_connection() {
    let nntp = test_client();
    let shared = shared_with(vec![
        None,
        idle_with(Some(idle_lane(&nntp, 0))),
        idle_with(None),
        idle_with(None),
    ]);

    assert_eq!(shared.idle_workers_without_a_lane(8).len(), 2);
    assert_eq!(
        shared.idle_workers_without_a_lane(1).len(),
        1,
        "never more than the job's share of the free connections"
    );
    assert!(shared.idle_workers_without_a_lane(0).is_empty());
}

// A server that greets, answers CAPABILITIES with nothing beyond a
// reader, and refuses everything else, until its client hangs up.
fn greeting_server() -> (u16, std::thread::JoinHandle<()>) {
    use std::io::{BufRead, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut writer = socket.try_clone().unwrap();
        writer.write_all(b"200 fixture ready\r\n").unwrap();
        for line in std::io::BufReader::new(socket).lines() {
            let Ok(line) = line else { break };
            let reply: &[u8] = if line == "CAPABILITIES" {
                b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
            } else {
                b"500 unsupported\r\n"
            };
            if writer.write_all(reply).is_err() {
                break;
            }
        }
    });
    (port, server)
}

fn warm_for(nntp: &Arc<weaver_nntp::NntpClient>, seat: usize) -> OwnedLaneWarm {
    OwnedLaneWarm {
        nntp: Arc::clone(nntp),
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        byte_estimate: 750_000,
        seat: weaver_nntp::pool::ServerId(seat),
    }
}

// A warm lane is an idle lane: it is published exactly like one a park
// kept, so the next lease for that job is routed straight to it.
#[test]
fn a_warm_connection_is_published_as_an_idle_lane() {
    let (port, server) = greeting_server();
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
                ..Default::default()
            },
            4,
        ),
    ));
    let shared = Arc::new(std::sync::Mutex::new(shared_with(vec![idle_with(None)])));
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
    let mut cached = None;

    let started = answer_idle_command(
        seat(0),
        &shared,
        &mut cached,
        OwnedLanePoolCommand::Warm(Box::new(warm_for(&nntp, 0))),
    );

    assert!(started.is_none(), "a warm starts no run");
    assert!(
        cached.is_some(),
        "the worker now holds the warmed connection"
    );
    assert!(
        slot(&lock_pool(&shared), 0)
            .idle
            .as_ref()
            .and_then(|idle| idle.lane.as_ref())
            .is_some_and(|lane| lane.server.0 == 0),
        "the warmed connection is published on the worker's idle slot"
    );
    assert_eq!(
        lock_pool(&shared).claim_worker_for(&run),
        Some(0),
        "the warmed connection is the one this lease is routed to"
    );

    drop(cached);
    server.join().unwrap();
}

// A warm makes the server choice a lease would make. A fill server that
// cannot be reached is not a reason to open the backfill tier, so a warm
// with only a backfill server answering stays without a connection, even
// when the seat it is handed is on the backfill server.
#[test]
fn a_warm_never_dials_a_backfill_server() {
    let backfill = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    backfill.set_nonblocking(true).unwrap();
    let server = |port: u16, backfill: bool| weaver_nntp::pool::ServerPoolConfig {
        max_connections: 4,
        backfill,
        ..weaver_nntp::pool::ServerPoolConfig::new(weaver_nntp::ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            tls: false,
            ..Default::default()
        })
    };
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![
                // Nothing listens on port 1: every dial is refused.
                server(1, false),
                server(backfill.local_addr().unwrap().port(), true),
            ],
            ..weaver_nntp::client::NntpClientConfig::single(weaver_nntp::ServerConfig::default(), 1)
        },
    ));
    let mut cached = None;

    warm_cached_lane(&mut cached, warm_for(&nntp, 0));
    warm_cached_lane(&mut cached, warm_for(&nntp, 1));

    assert!(cached.is_none(), "no fill server answered the warm");
    // A dial completes its TCP handshake into the listener's backlog
    // before it returns, so a dial to the backfill server would be
    // waiting here.
    assert_eq!(
        backfill.accept().map(|_| ()).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "the warm never dialed the backfill server"
    );
}

// A server that holds every article whose id ends in `-0` and nothing
// else, answering STAT and HEAD, until its one client hangs up.
fn stat_fixture() -> (u16, std::thread::JoinHandle<()>) {
    use std::io::{BufRead, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut writer = socket.try_clone().unwrap();
        writer.write_all(b"200 fixture ready\r\n").unwrap();
        for line in std::io::BufReader::new(socket).lines() {
            let Ok(line) = line else { break };
            let upper = line.to_ascii_uppercase();
            let id = line.get(5..).unwrap_or_default().trim();
            let held = id
                .trim_matches(['<', '>'])
                .split('@')
                .next()
                .unwrap_or_default()
                .ends_with("-0");
            let reply = if upper == "CAPABILITIES" {
                "101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n".to_string()
            } else if upper.starts_with("STAT ") && held {
                format!("223 0 {id}\r\n")
            } else if upper.starts_with("HEAD ") && held {
                format!("221 0 {id}\r\nSubject: fixture\r\n.\r\n")
            } else if upper.starts_with("STAT ") || upper.starts_with("HEAD ") {
                "430 no such article\r\n".to_string()
            } else if upper.starts_with("QUIT") {
                let _ = writer.write_all(b"205 closing\r\n");
                break;
            } else {
                "500 unsupported\r\n".to_string()
            };
            if writer.write_all(reply.as_bytes()).is_err() {
                break;
            }
        }
    });
    (port, server)
}

// A probe is lane work and takes its lane as a lease does. A dial that
// finds the server health state busy through every retry is no answer
// from the server: the batch comes back unasked, and the seating after
// that asks it and gets the server's word.
#[test]
fn a_probe_that_meets_a_busy_health_state_is_handed_back_unasked_and_asked_next_time() {
    let (port, server) = stat_fixture();
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    let (mail, _heard) = mpsc::channel(4);
    let probe = |batch: crate::pipeline::post_health::ProbeBatch| OwnedLaneProbe {
        nntp: Arc::clone(&nntp),
        seat: weaver_nntp::pool::ServerId(0),
        ids: batch.unfound(),
        batch,
        mail: mail.clone(),
    };
    let batch = crate::pipeline::post_health::ProbeBatch::new(
        JobId(1),
        1,
        vec![
            "<lantern-0@example.com>".into(),
            "<lantern-1@example.com>".into(),
        ],
    );
    let mut cached = None;

    let health = Arc::clone(nntp.pool().health());
    let busy = health
        .try_lock()
        .expect("nothing else holds the health state");
    let first = ask_owned_lane_probe(&mut cached, probe(batch));
    drop(busy);
    let crate::pipeline::post_health::ProbeMail::Unseated(batch) = first.word else {
        panic!(
            "a busy health state is not the server's answer: {:?}",
            first.word
        );
    };
    assert!(batch.asked().is_empty(), "the batch comes back unasked");
    assert_eq!(batch.unfound().len(), 2, "nothing was folded into it");

    let second = ask_owned_lane_probe(&mut cached, probe(batch));
    let crate::pipeline::post_health::ProbeMail::Answered(answer) = second.word else {
        panic!("the next seating asks the batch: {:?}", second.word);
    };
    assert_eq!(answer.server, 0);
    assert!(!answer.result.inconclusive, "the server settled the batch");
    assert_eq!(answer.result.exists, vec![true, false]);
    assert!(
        cached
            .as_ref()
            .is_some_and(|cached| cached.lane.server_id().0 == 0),
        "the lane that answered stays with the worker"
    );

    park_cached_lane(&mut cached);
    server.join().unwrap();
}

// A route that refuses every dial for room, and the dials it refused.
struct FullRoute(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl weaver_tunnel::pipe::Dialer for FullRoute {
    async fn dial(
        &self,
        _: &weaver_tunnel::pipe::Target,
    ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(weaver_tunnel::pipe::DialError::AtCapacity(
            Default::default(),
        ))
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(3600)
    }
    fn describe(&self) -> String {
        "full route fixture".into()
    }
}

// A probe batch whose seat sits behind a full route is answered
// inconclusive on that server, so the health round moves on. Handing it
// back unasked would seat it on the same server at once and be refused
// again: nothing holds a server off while its route is full.
#[test]
fn a_probe_behind_a_full_route_is_inconclusive_there_and_seated_once() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dialer = Arc::new(weaver_nntp::route_dialer::RouteDialer {
        inner: Arc::new(FullRoute(Arc::clone(&dials))),
        plan: None,
        egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
        runtime: runtime.handle().clone(),
        server: 0,
    });
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "nntp-full-route.invalid".to_string(),
                port: 119,
                tls: false,
                dialer,
                ..Default::default()
            },
            1,
        ),
    ));
    let (mail, _heard) = mpsc::channel(4);
    let batch = crate::pipeline::post_health::ProbeBatch::new(
        JobId(1),
        1,
        vec!["<lantern-0@example.com>".into()],
    );
    let probe = OwnedLaneProbe {
        nntp: Arc::clone(&nntp),
        seat: weaver_nntp::pool::ServerId(0),
        ids: batch.unfound(),
        batch,
        mail,
    };
    let mut cached = None;

    let owed = ask_owned_lane_probe(&mut cached, probe);

    let crate::pipeline::post_health::ProbeMail::Answered(answer) = owed.word else {
        panic!(
            "a full route must not hand the batch back to be seated again: {:?}",
            owed.word
        );
    };
    assert_eq!(answer.server, 0);
    assert!(answer.result.inconclusive, "the server could not be asked");
    assert_eq!(answer.result.exists, vec![false]);
    assert_eq!(
        dials.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the seat is dialled once"
    );
    assert!(cached.is_none());
    let health = nntp.pool().health().blocking_lock();
    assert_eq!(
        health.server(0).failure_count,
        0,
        "a full route is no fault"
    );
}

// A route with `room` sockets free, which refuses a dial for room with
// the slot that rings as one frees, and the dials made on it. A dial it
// has room for takes the room and fails on the wire.
struct MeteredRoute {
    room: std::sync::atomic::AtomicUsize,
    freed: Arc<tokio::sync::Notify>,
    dials: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl weaver_tunnel::pipe::Dialer for MeteredRoute {
    async fn dial(
        &self,
        _: &weaver_tunnel::pipe::Target,
    ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
        use std::sync::atomic::Ordering::SeqCst;
        self.dials.fetch_add(1, SeqCst);
        if self
            .room
            .fetch_update(SeqCst, SeqCst, |room| room.checked_sub(1))
            .is_err()
        {
            return Err(weaver_tunnel::pipe::DialError::AtCapacity(
                weaver_tunnel::pipe::Capacity {
                    waits: vec![Arc::clone(&self.freed)],
                    demand: Vec::new(),
                },
            ));
        }
        Err(weaver_tunnel::pipe::DialError::Egress(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        ))
    }
    fn room(
        &self,
        _: &weaver_tunnel::pipe::Purpose,
    ) -> std::result::Result<(), Vec<Arc<tokio::sync::Notify>>> {
        if self.room.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            Err(vec![Arc::clone(&self.freed)])
        } else {
            Ok(())
        }
    }
    fn budget(&self) -> std::time::Duration {
        std::time::Duration::from_secs(3600)
    }
    fn describe(&self) -> String {
        "metered route fixture".into()
    }
}

// Lanes refused by a full route wait in its line and dial nothing until
// a socket frees; one freed socket lets exactly one of them dial, and the
// rest wait on. A shutdown sends every one of them back.
#[test]
fn lanes_behind_a_full_route_wait_for_a_freed_socket_and_one_dials_per_socket() {
    use std::sync::atomic::Ordering::SeqCst;
    const LANES: usize = 3;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let route = Arc::new(MeteredRoute {
        room: std::sync::atomic::AtomicUsize::new(0),
        freed: Arc::new(tokio::sync::Notify::new()),
        dials: std::sync::atomic::AtomicUsize::new(0),
    });
    let dialer = Arc::new(weaver_nntp::route_dialer::RouteDialer {
        inner: Arc::clone(&route) as Arc<dyn weaver_tunnel::pipe::Dialer>,
        plan: None,
        egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
        runtime: runtime.handle().clone(),
        server: 0,
    });
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "nntp-metered-route.invalid".to_string(),
                port: 119,
                tls: false,
                dialer,
                ..Default::default()
            },
            8,
        ),
    ));
    let state = test_admitted_state(JobId(42));
    let (event_tx, mut events) = mpsc::channel(LANES * 2);
    let (refill_tx, _refill_rx) = mpsc::channel(4);
    let (parked_tx, _parked_rx) = mpsc::channel(4);
    let mut waiting = nntp.pool().route_line_waiting(weaver_nntp::ServerId(0));
    // One lane at a time, each in line before the next starts: lanes dialling
    // at once contend for the pool's health lock, and one that loses its
    // tries gives its work back instead of joining the line.
    let lanes: Vec<_> = (0..LANES)
        .map(|segment| {
            let run = OwnedLaneRun {
                nntp: Arc::clone(&nntp),
                state: state.clone(),
                event_tx: event_tx.clone(),
                refill_tx: refill_tx.clone(),
                parked_tx: parked_tx.clone(),
                initial_lease: test_lease(
                    JobId(42),
                    0,
                    weaver_nntp::ServerSet::EMPTY,
                    vec![tail_work(u32::try_from(segment).unwrap(), 0)],
                ),
            };
            let lane = std::thread::spawn(move || run_owned_blocking_download_lane(&mut None, run));
            runtime.block_on(async {
                waiting
                    .wait_for(|waiting| *waiting == segment + 1)
                    .await
                    .unwrap();
            });
            lane
        })
        .collect();
    drop(event_tx);
    assert_eq!(route.dials.load(SeqCst), LANES, "each lane dialled once");

    route.room.store(1, SeqCst);
    route.freed.notify_waiters();
    let first = events.blocking_recv().expect("the lane let go reports");
    assert!(
        matches!(first, OwnedDownloadLaneEvent::AcquireFailed { .. }),
        "the lane let go dialled and met the wire"
    );
    assert_eq!(
        route.dials.load(SeqCst),
        LANES + 1,
        "one freed socket, one dial"
    );
    runtime.block_on(async {
        waiting
            .wait_for(|waiting| *waiting == LANES - 1)
            .await
            .unwrap();
    });
    assert_eq!(
        route.dials.load(SeqCst),
        LANES + 1,
        "the full route is not redialled"
    );

    state.queue.write(|inner| {
        inner.globals.shutting_down = true;
        crate::pipeline::queue::Change::changed(())
    });
    for lane in lanes {
        lane.join().unwrap();
    }
    let mut obsolete = 0;
    while let Some(event) = events.blocking_recv() {
        assert!(
            matches!(event, OwnedDownloadLaneEvent::LeaseObsolete { .. }),
            "a waiting lane gives its lease back"
        );
        obsolete += 1;
    }
    assert_eq!(obsolete, LANES - 1);
    assert_eq!(route.dials.load(SeqCst), LANES + 1);
}

// The check just before a BODY is written reads the policy as the lane's
// connection does: the server's recovery probe still issues the work it
// was admitted for while its probe is out, and any other connection to
// the server is held with it.
#[test]
fn the_probe_connection_still_issues_its_work_while_the_probe_is_out() {
    let nntp = test_client();
    let state = test_admitted_state(JobId(42));
    let mut lease = test_lease(
        JobId(42),
        0,
        weaver_nntp::ServerSet::EMPTY,
        vec![tail_work(1, 0)],
    );
    let context = LaneLeaseContext::from_lease(&mut lease, 0, false);
    let work = &lease.works[0];
    let on = |holds_probe| HeldConnection {
        server: 0,
        holds_probe,
    };
    nntp.pool().quarantine_for_transport_for_test(0);
    nntp.pool().expire_quarantine_for_test(0);
    let probe = nntp.pool().take_recovery_probe_for_test(0);
    assert!(issue_is_current(&state, &nntp, on(true), work, &context));
    assert!(!issue_is_current(&state, &nntp, on(false), work, &context));
    drop(probe);
    assert!(issue_is_current(&state, &nntp, on(false), work, &context));
}

// A server whose quarantine has lapsed, and the probe batch a worker asks
// on its seat there: the worker's dial is the server's recovery probe.
fn probe_due_on_stat_fixture(
    ids: &[&str],
) -> (
    Arc<weaver_nntp::NntpClient>,
    OwnedLaneProbe,
    std::thread::JoinHandle<()>,
) {
    let (port, server) = stat_fixture();
    let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig::single(
            weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
                ..Default::default()
            },
            1,
        ),
    ));
    nntp.pool().quarantine_for_transport_for_test(0);
    nntp.pool().expire_quarantine_for_test(0);
    // The answer is handed back unsent, so nothing reads the mail.
    let (mail, _) = mpsc::channel(4);
    let batch = crate::pipeline::post_health::ProbeBatch::new(
        JobId(1),
        1,
        ids.iter().map(|id| (*id).into()).collect(),
    );
    let probe = OwnedLaneProbe {
        nntp: Arc::clone(&nntp),
        seat: weaver_nntp::pool::ServerId(0),
        ids: batch.unfound(),
        batch,
        mail,
    };
    (nntp, probe, server)
}

fn server_takes_new_work(nntp: &weaver_nntp::NntpClient) -> bool {
    matches!(
        nntp.pool().provider_policy().eligibility(
            0,
            weaver_nntp::ServerSet::EMPTY,
            weaver_nntp::ServerSet::EMPTY,
            1
        ),
        weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
    )
}

// An existence probe asks no BODY. A STAT the server answers 223 is its
// answer, so a probe dialled for it recovers the server, and the lane is
// then kept like any other.
#[test]
fn an_existence_probe_answered_found_recovers_its_server() {
    let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-0@example.com>"]);
    let mut cached = None;

    let owed = run_owned_lane_work(
        &mut cached,
        &mut None,
        QueuedOwnedLaneWork::Probe(Box::new(probe)),
    );

    assert!(owed.is_some());
    assert!(
        !nntp.pool().requires_recovery(0),
        "the 223 answered the probe"
    );
    assert!(cached.is_some(), "an answered lane is kept");
    park_cached_lane(&mut cached);
    server.join().unwrap();
}

// A probe batch the server answers with nothing found leaves the probe
// unanswered. The lane is not kept idle holding it: it is parked, the
// probe is given back, and the server takes new work again.
#[test]
fn an_existence_probe_that_finds_nothing_gives_the_probe_back() {
    let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-1@example.com>"]);
    let mut cached = None;

    let owed = run_owned_lane_work(
        &mut cached,
        &mut None,
        QueuedOwnedLaneWork::Probe(Box::new(probe)),
    );

    assert!(owed.is_some());
    assert!(nntp.pool().requires_recovery(0), "a 430 is no answer");
    assert!(cached.is_none(), "the unanswered probe is not kept idle");
    assert!(server_takes_new_work(&nntp), "the probe was given back");
    server.join().unwrap();
}

// The one rule for a resting lane: an unanswered probe is parked, which
// gives the probe back; a lane that is not a probe is kept.
#[test]
fn a_resting_lane_gives_back_an_unanswered_probe_and_keeps_anything_else() {
    let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-1@example.com>"]);
    let mut cached = None;
    let owed = ask_owned_lane_probe(&mut cached, probe);
    drop(owed);
    assert!(
        cached.is_some(),
        "the probe's lane came back from its batch"
    );
    assert!(!server_takes_new_work(&nntp), "its probe is out");

    give_back_unanswered_probe(&mut cached);

    assert!(cached.is_none());
    assert!(server_takes_new_work(&nntp));
    server.join().unwrap();

    let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-0@example.com>"]);
    let mut cached = None;
    drop(ask_owned_lane_probe(&mut cached, probe));
    assert!(!nntp.pool().requires_recovery(0));
    give_back_unanswered_probe(&mut cached);
    assert!(cached.is_some(), "a lane that is no probe is kept");
    park_cached_lane(&mut cached);
    server.join().unwrap();
}

// A worker that has already been claimed must not be republished as idle
// by a command it answers on the way to its next run.
#[test]
fn a_claimed_worker_is_not_republished_as_idle() {
    let nntp = test_client();
    let mut shared = shared_with(vec![idle_with(None)]);
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

    assert_eq!(shared.claim_worker_for(&run), Some(0));
    shared.note_idle_lane(seat(0), Some(idle_lane(&nntp, 0)));

    assert!(
        slot(&shared, 0).idle.is_none(),
        "its next lease is already on the way"
    );
}

// Submit a run whose lease is stale: its worker reports it obsolete on
// `events` and is done. While `events` is full the report waits for
// room, so the test decides how long the worker stays busy.
fn submit_obsolete_run(
    pool: &OwnedDownloadLanePool,
    nntp: &Arc<weaver_nntp::NntpClient>,
    events: &mpsc::Sender<OwnedDownloadLaneEvent>,
) {
    let (refill_tx, _refill_rx) = mpsc::channel(1);
    let (parked_tx, _parked_rx) = mpsc::channel(1);
    pool.submit(
        Arc::clone(nntp),
        crate::jobs::handle::SharedPipelineState::new(
            weaver_server_base::operations::metrics::PipelineMetrics::new(),
            Vec::new(),
        ),
        events.clone(),
        refill_tx,
        parked_tx,
        test_lease(
            JobId(7),
            1,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        ),
    )
    .unwrap_or_else(|_| panic!("a pool with room takes the run"));
}

fn obsolete_report() -> OwnedDownloadLaneEvent {
    OwnedDownloadLaneEvent::LeaseObsolete {
        lease: test_lease(
            JobId(9),
            1,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        ),
    }
}

// Wait for `count` idle publications, whichever workers they come from.
fn await_idle(observed: &std_mpsc::Receiver<OwnedLaneObservation>, count: usize) {
    let mut seen = 0;
    while seen < count {
        if let OwnedLaneObservation::Idle(_) = observed.recv().unwrap() {
            seen += 1;
        }
    }
}

// A pool sized for many connections starts no thread until work comes,
// and then only as many as the work needs at once.
#[test]
fn lane_threads_start_on_demand_up_to_the_cap() {
    let nntp = test_client();
    let pool = OwnedDownloadLanePool::new(48);
    let observed = pool.observe();
    assert_eq!(pool.worker_count(), 48, "the cap is the configured count");
    assert_eq!(pool.live_worker_count(), 0, "no thread before any work");

    let (events, mut reports) = mpsc::channel(1);
    events.try_send(obsolete_report()).unwrap();
    submit_obsolete_run(&pool, &nntp, &events);
    assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Spawned(0));
    assert_eq!(pool.live_worker_count(), 1, "one run, one thread");

    // The first worker cannot finish while `events` is full.
    submit_obsolete_run(&pool, &nntp, &events);
    assert_eq!(
        observed.recv().unwrap(),
        OwnedLaneObservation::Spawned(1),
        "a run that finds every live worker busy starts another"
    );
    assert_eq!(pool.live_worker_count(), 2);

    for _ in 0..3 {
        reports.blocking_recv().unwrap();
    }
    await_idle(&observed, 2);
    submit_obsolete_run(&pool, &nntp, &events);
    assert_eq!(
        pool.live_worker_count(),
        2,
        "an idle worker takes the next run without a new thread"
    );
    reports.blocking_recv().unwrap();
}

// A worker idle past the release interval gives up its slot, and the
// next run starts a thread in that same slot.
#[test]
fn an_idle_worker_leaves_and_its_slot_is_reused() {
    let nntp = test_client();
    let pool =
        OwnedDownloadLanePool::with_test_intervals(4, Duration::from_millis(1), Duration::ZERO);
    let observed = pool.observe();
    let (events, mut reports) = mpsc::channel(4);

    submit_obsolete_run(&pool, &nntp, &events);
    assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Spawned(0));
    reports.blocking_recv().unwrap();
    assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Idle(0));
    assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Vacated(0));
    assert_eq!(pool.live_worker_count(), 0, "the thread is gone");

    submit_obsolete_run(&pool, &nntp, &events);
    assert_eq!(
        observed.recv().unwrap(),
        OwnedLaneObservation::Spawned(0),
        "the vacated slot is the one reused"
    );
    reports.blocking_recv().unwrap();
}

// A leaving worker counts against the cap until its slot is empty, so a
// run queued while it let its connection go may have been queued for
// it. It takes that run instead of leaving; with nothing queued it goes.
#[test]
fn a_leaving_worker_takes_work_queued_on_its_way_out() {
    let nntp = test_client();
    let mut shared = shared_with(vec![idle_with(None)]);

    assert!(matches!(
        shared.begin_release(seat(0)),
        OwnedLaneRelease::Withdrawn
    ));
    let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
    assert!(
        shared.claim_worker_for(&run).is_none(),
        "a withdrawn worker is offered nothing"
    );
    shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));
    assert!(matches!(
        shared.finish_release(seat(0)),
        OwnedLaneRelease::Work(QueuedOwnedLaneWork::Run(_))
    ));
    assert!(shared.workers[0].is_some(), "it keeps its slot to run it");

    assert!(
        shared
            .take_queued_run_or_publish_idle(seat(0), None)
            .is_none()
    );
    assert!(matches!(
        shared.begin_release(seat(0)),
        OwnedLaneRelease::Withdrawn
    ));
    assert!(matches!(
        shared.finish_release(seat(0)),
        OwnedLaneRelease::Left
    ));
    assert!(shared.workers.is_empty(), "the slot is vacated");
}

// Queued work found as a worker starts to leave is taken at once.
#[test]
fn a_worker_about_to_leave_takes_queued_work_first() {
    let nntp = test_client();
    let mut shared = shared_with(vec![idle_with(None)]);
    shared
        .queued_runs
        .push_back(QueuedOwnedLaneWork::Run(test_run(
            &nntp,
            weaver_nntp::ServerSet::EMPTY,
        )));

    assert!(matches!(
        shared.begin_release(seat(0)),
        OwnedLaneRelease::Work(QueuedOwnedLaneWork::Run(_))
    ));
    assert!(slot(&shared, 0).idle.is_none(), "it is busy with the run");
}

// A worker claimed for a run does not leave: the run is already in its
// channel.
#[test]
fn a_claimed_worker_stays_for_its_run() {
    let nntp = test_client();
    let mut shared = shared_with(vec![idle_with(None)]);

    assert_eq!(
        shared.claim_worker_for(&test_run(&nntp, weaver_nntp::ServerSet::EMPTY)),
        Some(0)
    );
    assert!(matches!(
        shared.begin_release(seat(0)),
        OwnedLaneRelease::Stay
    ));
    assert!(shared.workers[0].is_some());
}

// A worker a resize sent away must not publish itself into a slot a
// later worker has taken.
#[test]
fn a_departed_worker_never_publishes_into_its_successor_s_slot() {
    let mut shared = shared_with(vec![None]);
    shared.workers[0].as_mut().unwrap().id = 5;

    assert!(
        shared
            .take_queued_run_or_publish_idle(seat(0), None)
            .is_none()
    );
    assert!(slot(&shared, 0).idle.is_none(), "the successor stays busy");
    shared.vacate(seat(0));
    assert!(shared.workers[0].is_some(), "nor can it vacate the slot");
}
