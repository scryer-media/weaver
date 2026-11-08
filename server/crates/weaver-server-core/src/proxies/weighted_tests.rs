// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::{Failover, LegPath, RouteLeg};
use super::*;
use std::sync::atomic::{AtomicU8, Ordering};
use weaver_tunnel::{
    bridge::ConnectionOutcome,
    pipe::{DialPath, DialedStream, Purpose},
};

struct Scripted {
    mode: AtomicU8,
    started: Notify,
    dialed: std::sync::Mutex<Vec<Purpose>>,
    // The signal of a full tunnel share, while the leg has no room.
    full: std::sync::Mutex<Option<Arc<Notify>>>,
}
#[async_trait::async_trait]
impl Dialer for Scripted {
    fn room(&self, _: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        match &*self.full.lock().unwrap() {
            Some(freed) => Err(vec![freed.clone()]),
            None => Ok(()),
        }
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialed.lock().unwrap().push(target.purpose.clone());
        self.started.notify_one();
        match self.mode.load(Ordering::SeqCst) {
            1 => {
                return Err(DialError::Bind(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "offline",
                )));
            }
            2 => return Err(DialError::Skipped("cooling".into())),
            3 => std::future::pending::<()>().await,
            _ => {}
        }
        let (stream, _) = tokio::io::duplex(64);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(stream)),
            outcome: Arc::new(ConnectionOutcome::default()),
            path: DialPath::default(),
            peer: Some("127.0.0.1:119".parse().unwrap()),
            source: None,
            setup: None,
        })
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "scripted".into()
    }
}
fn target() -> Target {
    Target {
        host: "news.invalid".into(),
        port: 119,
        purpose: Purpose::Nntp {
            server: 1,
            leg: 0,
            probing: false,
        },
        addresses: Vec::new(),
    }
}
fn create(cap: u16) -> (Arc<Weighted>, Vec<Arc<Scripted>>) {
    let route = Route {
        legs: vec![
            RouteLeg {
                egress_id: 0,
                weight: 60,
                path: LegPath::Direct,
            },
            RouteLeg {
                egress_id: 1,
                weight: 40,
                path: LegPath::Direct,
            },
        ],
        failover: Failover::Redistribute,
    };
    let stages: Vec<_> = (0..2)
        .map(|_| {
            Arc::new(Scripted {
                mode: AtomicU8::new(0),
                started: Notify::new(),
                dialed: Default::default(),
                full: Default::default(),
            })
        })
        .collect();
    let weighted = Weighted::new(
        route,
        stages
            .iter()
            .map(|s| s.clone() as Arc<dyn Dialer>)
            .collect(),
        cap,
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    (weighted, stages)
}

#[tokio::test]
async fn deficits_admit_exactly_the_cap_and_close_returns_capacity() {
    let (route, _) = create(5);
    let mut streams = Vec::new();
    for _ in 0..5 {
        streams.push(route.dial(&target()).await.unwrap());
    }
    assert_eq!(
        streams
            .iter()
            .map(|s| s.path.leg.unwrap())
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 0, 1]
    );
    assert!(matches!(
        route.dial(&target()).await,
        Err(DialError::AtCapacity(_))
    ));
    streams.pop().unwrap().outcome.closed();
    assert_eq!(route.dial(&target()).await.unwrap().path.leg, Some(1));
}

// A leg whose tunnel share is full is passed over for one with room, and
// with no room on any leg the dial waits on every leg's share and the route.
#[tokio::test]
async fn a_full_share_sends_dials_to_the_leg_with_room() {
    let (route, stages) = create(5);
    let freed = Arc::new(Notify::new());
    *stages[0].full.lock().unwrap() = Some(freed.clone());
    let mut streams = Vec::new();
    let waits = loop {
        match route.dial(&target()).await {
            Ok(dialed) => streams.push(dialed),
            Err(DialError::AtCapacity(waits)) => break waits,
            Err(error) => panic!("{error}"),
        }
    };
    assert!(!streams.is_empty());
    assert!(streams.iter().all(|s| s.path.leg == Some(1)));
    assert!(stages[0].dialed.lock().unwrap().is_empty());
    assert!(!route.has_room(&target().purpose));
    assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, &freed)));
    assert!(
        waits
            .iter()
            .any(|wait| Arc::ptr_eq(wait, &route.budget_changed()))
    );

    *stages[0].full.lock().unwrap() = None;
    assert!(route.has_room(&target().purpose));
    assert_eq!(route.dial(&target()).await.unwrap().path.leg, Some(0));
    assert_eq!(stages[0].dialed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancelling_an_opening_releases_it_without_health_evidence() {
    let (route, stages) = create(5);
    stages[0].mode.store(3, Ordering::SeqCst);
    let target = target();
    let mut dial = Box::pin(route.dial(&target));
    tokio::select! { biased; _ = &mut dial => panic!("scripted pending dial returned"), _ = stages[0].started.notified() => {} }
    assert_eq!(route.allocations()[0].opening, 1);
    drop(dial);
    let state = route.allocations();
    assert_eq!(state[0].opening, 0);
    assert_eq!(state[0].health, LegHealthState::Up);
}

#[tokio::test(start_paused = true)]
async fn two_failures_redistribute_then_offer_one_probe_and_restore() {
    let (route, stages) = create(5);
    stages[0].mode.store(1, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(route.dial(&target()).await.is_err());
    }
    assert_eq!(
        route
            .allocations()
            .iter()
            .map(|s| s.target)
            .collect::<Vec<_>>(),
        vec![0, 5]
    );
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(route.allocations()[0].health, LegHealthState::Probing);
    assert_eq!(route.allocations()[0].target, 1);
    assert!(route.needs_probe());
    stages[0].mode.store(0, Ordering::SeqCst);
    let recovered = route.dial(&target()).await.unwrap();
    assert_eq!(recovered.path.leg, Some(0));
    // The probe is the leg's first pooled connection: it tries rungs still
    // cooling and takes a usenet socket, not one of the reserve.
    let probe = stages[0].dialed.lock().unwrap().last().cloned().unwrap();
    assert!(
        matches!(
            probe,
            Purpose::Nntp {
                leg: 0,
                probing: true,
                ..
            }
        ),
        "{probe:?}"
    );
    assert_eq!(
        weaver_tunnel::pipe::SocketClass::of(&probe),
        weaver_tunnel::pipe::SocketClass::Nntp
    );
    assert_eq!(route.allocations()[0].health, LegHealthState::Up);
    assert_eq!(
        route
            .allocations()
            .iter()
            .map(|s| s.target)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
}

// A probing leg whose share is full gives way: the dial goes through the
// healthy leg as that leg's own, and the probing leg stays probing. Once
// its share frees, the next dial probes it.
#[tokio::test(start_paused = true)]
async fn a_probing_leg_with_a_full_share_gives_way_to_a_leg_with_room() {
    let (route, stages) = create(5);
    stages[0].mode.store(1, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(route.dial(&target()).await.is_err());
    }
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(route.allocations()[0].health, LegHealthState::Probing);
    stages[0].mode.store(0, Ordering::SeqCst);
    let freed = Arc::new(Notify::new());
    *stages[0].full.lock().unwrap() = Some(freed.clone());
    let probed = stages[0].dialed.lock().unwrap().len();

    assert!(route.has_room(&target().purpose));
    let normal = route.dial(&target()).await.unwrap();
    assert_eq!(normal.path.leg, Some(1));
    assert!(matches!(
        stages[1].dialed.lock().unwrap().last(),
        Some(Purpose::Nntp {
            leg: 1,
            probing: false,
            ..
        })
    ));
    assert_eq!(stages[0].dialed.lock().unwrap().len(), probed);
    assert_eq!(route.allocations()[0].health, LegHealthState::Probing);
    assert_eq!(route.allocations()[1].health, LegHealthState::Up);

    *stages[0].full.lock().unwrap() = None;
    let probe = route.dial(&target()).await.unwrap();
    assert_eq!(probe.path.leg, Some(0));
    assert!(matches!(
        stages[0].dialed.lock().unwrap().last(),
        Some(Purpose::Nntp {
            leg: 0,
            probing: true,
            ..
        })
    ));
    assert_eq!(route.allocations()[0].health, LegHealthState::Up);
}

#[tokio::test(start_paused = true)]
async fn every_dial_in_flight_during_one_outage_cools_the_leg_once() {
    let (route, stages) = create(5);
    for stage in &stages {
        stage.mode.store(3, Ordering::SeqCst);
    }
    // Four lanes open at once and hang against the same outage, so their
    // connects time out in the same instant. The first two verdicts take
    // the leg down; the rest land while it is cooling and must not stretch
    // that cooldown.
    let dials: Vec<_> = (0..4)
        .map(|_| {
            let route = route.clone();
            tokio::spawn(async move { route.dial(&target()).await.is_err() })
        })
        .collect();
    tokio::task::yield_now().await;
    assert!(route.allocations()[0].opening >= 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    for dial in dials {
        assert!(dial.await.unwrap());
    }
    assert!(matches!(
        route.allocations()[0].health,
        LegHealthState::Down(_)
    ));
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(route.allocations()[0].health, LegHealthState::Probing);
    assert!(route.needs_probe());
    // A probe that fails after the cooldown lapsed is the next verdict and
    // does lengthen it.
    stages[0].mode.store(1, Ordering::SeqCst);
    let probe = route.dial(&target()).await;
    assert!(probe.is_err());
    assert!(matches!(
        route.allocations()[0].health,
        LegHealthState::Down(_)
    ));
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(matches!(
        route.allocations()[0].health,
        LegHealthState::Down(_)
    ));
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(route.allocations()[0].health, LegHealthState::Probing);
}

#[tokio::test(start_paused = true)]
async fn a_leg_that_swallows_connects_is_booked_and_cooled() {
    let (route, stages) = create(5);
    stages[0].mode.store(3, Ordering::SeqCst);
    // Each dial lands on leg 0 until it cools; its hung connect times out
    // on the leg's own budget instead of being dropped unbooked.
    for _ in 0..2 {
        assert!(matches!(
            route.dial(&target()).await,
            Err(DialError::Timeout { .. })
        ));
    }
    let state = route.allocations();
    assert_eq!(state[0].opening, 0);
    assert!(matches!(state[0].health, LegHealthState::Down(_)));
    assert_eq!(
        state.iter().map(|s| s.target).collect::<Vec<_>>(),
        vec![0, 5]
    );
}

#[tokio::test]
async fn skipped_rungs_are_not_evidence_and_reweight_keeps_connections() {
    let (route, stages) = create(5);
    stages[0].mode.store(2, Ordering::SeqCst);
    for _ in 0..3 {
        assert!(route.dial(&target()).await.is_err());
    }
    assert_eq!(route.allocations()[0].health, LegHealthState::Up);
    stages[0].mode.store(0, Ordering::SeqCst);
    let stream = route.dial(&target()).await.unwrap();
    let mut changed = route.shared.state.lock().unwrap().route.clone();
    changed.legs[0].weight = 20;
    changed.legs[1].weight = 80;
    route.reweight(changed, 5).unwrap();
    assert_eq!(route.allocations()[0].open, 1);
    assert_eq!(route.allocations()[0].target, 1);
    stream.outcome.closed();
    assert_eq!(route.allocations()[0].open, 0);
}

#[tokio::test]
async fn replaced_leg_ignores_old_close_failure_and_cancel_callbacks() {
    let (route, stages) = create(5);
    let old = route.dial(&target()).await.unwrap();
    stages[0].mode.store(3, Ordering::SeqCst);
    let target = target();
    let mut pending = Box::pin(route.dial(&target));
    // Drain the earlier successful dial's notification before observing this opening.
    stages[0].started.notified().await;
    tokio::select! { biased; _ = &mut pending => panic!("pending dial completed"), _ = stages[0].started.notified() => {} }
    let definition = route.shared.state.lock().unwrap().route.clone();
    let replacement = Arc::new(Scripted {
        mode: AtomicU8::new(0),
        started: Notify::new(),
        dialed: Default::default(),
        full: Default::default(),
    });
    route
        .update(definition, vec![replacement, stages[1].clone()], 5)
        .unwrap();
    let current = route.dial(&target).await.unwrap();
    old.outcome.failed();
    old.outcome.closed();
    drop(pending);
    let allocations = route.allocations();
    assert_eq!(allocations[0].open, 1);
    assert_eq!(allocations[0].opening, 0);
    assert_eq!(allocations[0].health, LegHealthState::Up);
    current.outcome.closed();
    assert_eq!(route.allocations()[0].open, 0);
}

#[tokio::test]
async fn destination_errors_and_untyped_stream_failures_leave_legs_up() {
    let (route, _) = create(10);
    let stages = route.legs.read().unwrap().clone();
    for _ in 0..5 {
        route.report_external(
            0,
            &stages[0],
            Some(&DialError::Destination(std::io::Error::other(
                "origin failed",
            ))),
        );
    }
    let dialed = route.dial(&target()).await.unwrap();
    dialed.outcome.failed();
    assert!(
        route
            .allocations()
            .iter()
            .all(|leg| leg.health == LegHealthState::Up)
    );
}

#[tokio::test(start_paused = true)]
async fn throughput_publish_does_not_wake_capacity_waiters() {
    use std::future::Future;
    let (weighted, _) = create(5);
    let dialed = weighted.dial(&target()).await.unwrap();
    let changed = weighted.budget_changed();
    let waiting = changed.notified();
    tokio::pin!(waiting);
    waiting.as_mut().enable();
    let mut updates = weighted.subscribe();
    dialed.outcome.read(1024);
    tokio::time::advance(Duration::from_secs(1)).await;
    while updates
        .borrow_and_update()
        .iter()
        .all(|leg| leg.bytes_per_second == 0)
    {
        updates.changed().await.unwrap();
    }
    assert!(
        waiting
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(dialed);
    waiting.await;
}

async fn settle_rate(updates: &mut watch::Receiver<Vec<LegAllocation>>, expected: u64) {
    loop {
        let rate = updates
            .borrow_and_update()
            .iter()
            .map(|leg| leg.bytes_per_second)
            .max()
            .unwrap_or(0);
        if rate == expected {
            return;
        }
        updates.changed().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn leg_rate_is_the_sustained_rate_over_recent_windows() {
    let (weighted, _) = create(5);
    let dialed = weighted.dial(&target()).await.unwrap();
    let mut updates = weighted.subscribe();
    dialed.outcome.read(1024);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle_rate(&mut updates, 1024).await;
    // A burst twice the size lands in the next second: the rate averages
    // both windows instead of jumping to the burst.
    dialed.outcome.read(3072);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle_rate(&mut updates, 2048).await;
    // Idle seconds age the carried bytes out of the window set one at a time.
    for expected in [1365, 1024, 768, 0] {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle_rate(&mut updates, expected).await;
    }
}

fn timer_snapshots(weighted: &Weighted) -> u64 {
    weighted.shared.timer_snapshots.load(Ordering::Relaxed)
}

async fn assert_timer_parked(weighted: &Weighted) {
    while !weighted.shared.timer_parked.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let mut state = weighted.shared.state.lock().unwrap();
    assert_eq!(weighted.shared.sample(&mut state, Instant::now()), None);
}

#[tokio::test(start_paused = true)]
async fn idle_route_timer_builds_no_snapshot_and_publishes_nothing() {
    let (weighted, _) = create(5);
    let _open = weighted.dial(&target()).await.unwrap();
    let mut updates = weighted.subscribe();
    updates.borrow_and_update();
    let before = timer_snapshots(&weighted);
    assert_timer_parked(&weighted).await;
    assert_eq!(timer_snapshots(&weighted), before);
    assert!(!updates.has_changed().unwrap());
}

#[tokio::test]
async fn restating_unchanged_egress_health_publishes_nothing() {
    let (weighted, _) = create(5);
    let mut updates = weighted.subscribe();
    updates.borrow_and_update();
    weighted.set_egress_health(0, EgressHealth::Up);
    assert!(!updates.has_changed().unwrap());
    weighted.set_egress_health(0, EgressHealth::Down("link down".into()));
    assert!(updates.has_changed().unwrap());
    assert!(matches!(
        updates.borrow_and_update()[0].health,
        LegHealthState::Down(_)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_decayed_rate_is_published_at_zero_once_then_the_timer_sleeps() {
    let (weighted, _) = create(5);
    let dialed = weighted.dial(&target()).await.unwrap();
    let mut updates = weighted.subscribe();
    dialed.outcome.read(1024);
    settle_rate(&mut updates, 1024).await;
    settle_rate(&mut updates, 0).await;
    let settled = timer_snapshots(&weighted);
    assert_timer_parked(&weighted).await;
    assert_eq!(timer_snapshots(&weighted), settled);
    assert!(!updates.has_changed().unwrap());

    // The first read after parking wakes the timer. Alongside the three
    // empty windows left from the decay it reads 2048 / 4.
    dialed.outcome.read(2048);
    settle_rate(&mut updates, 512).await;
}

#[tokio::test(start_paused = true)]
async fn a_cooling_leg_is_published_as_probing_when_its_cooldown_lapses() {
    let (route, stages) = create(5);
    stages[0].mode.store(1, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(route.dial(&target()).await.is_err());
    }
    let cooled_at = Instant::now();
    let mut updates = route.subscribe();
    updates.borrow_and_update();
    let before = timer_snapshots(&route);
    while updates.borrow_and_update()[0].health != LegHealthState::Probing {
        updates.changed().await.unwrap();
    }
    // The timer slept through the cooldown and woke once, at its end.
    assert_eq!(Instant::now() - cooled_at, Duration::from_secs(30));
    assert_eq!(timer_snapshots(&route), before + 1);
    assert_eq!(updates.borrow()[0].target, 1);
}

// A leg that answers every name with `answer` and records the addresses it
// is asked to dial.
struct OwnNaming {
    answer: Vec<std::net::IpAddr>,
    lookups: std::sync::atomic::AtomicUsize,
    dialled: std::sync::Mutex<Vec<Vec<std::net::IpAddr>>>,
}
impl OwnNaming {
    fn new(answer: &[[u8; 4]]) -> Arc<Self> {
        Arc::new(Self {
            answer: answer.iter().copied().map(std::net::IpAddr::from).collect(),
            lookups: Default::default(),
            dialled: Default::default(),
        })
    }
    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}
#[async_trait::async_trait]
impl Dialer for OwnNaming {
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dialled.lock().unwrap().push(target.addresses.clone());
        let (stream, _) = tokio::io::duplex(64);
        Ok(Dialed {
            stream: DialedStream::Tunnel(Box::new(stream)),
            outcome: Arc::new(ConnectionOutcome::default()),
            path: DialPath::default(),
            peer: None,
            source: None,
            setup: None,
        })
    }
    async fn resolve(&self, _host: &str) -> Result<Resolution, DialError> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        Ok(Resolution::Addresses(self.answer.clone()))
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "own naming".into()
    }
}

// Two legs of a route whose namings answer `first` and `second`, and a
// target for a name resolved through the route, pinned to its answer.
async fn named_legs(
    first: &[[u8; 4]],
    second: &[[u8; 4]],
) -> (Arc<Weighted>, [Arc<OwnNaming>; 2], Target) {
    let legs = [OwnNaming::new(first), OwnNaming::new(second)];
    let route = Route {
        legs: (0..2)
            .map(|egress_id| RouteLeg {
                egress_id,
                weight: 50,
                path: LegPath::Direct,
            })
            .collect(),
        failover: Failover::Redistribute,
    };
    let weighted = Weighted::new(
        route,
        legs.iter().map(|l| l.clone() as Arc<dyn Dialer>).collect(),
        2,
        &tokio::runtime::Handle::current(),
    )
    .unwrap();
    let Resolution::Addresses(addresses) = weighted.resolve("news.invalid").await.unwrap() else {
        panic!("the first leg resolves names");
    };
    let target = Target {
        addresses,
        ..target()
    };
    (weighted, legs, target)
}

// A name resolved under the first leg's naming is dialled by the second leg
// only at the checked addresses its own naming also gives.
#[tokio::test]
async fn a_leg_dials_a_checked_name_under_its_own_naming() {
    let (weighted, [first, second], target) = named_legs(
        &[[192, 0, 2, 1], [192, 0, 2, 2]],
        &[[192, 0, 2, 2], [192, 0, 2, 3]],
    )
    .await;
    let opened = [
        weighted.dial(&target).await.unwrap(),
        weighted.dial(&target).await.unwrap(),
    ];
    assert_eq!(
        opened
            .iter()
            .map(|d| d.path.leg.unwrap())
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(first.lookups(), 1, "the answering leg dials what it gave");
    assert_eq!(
        *first.dialled.lock().unwrap(),
        std::slice::from_ref(&target.addresses)
    );
    assert_eq!(second.lookups(), 1, "the other leg asks its own naming");
    assert_eq!(
        *second.dialled.lock().unwrap(),
        [vec![std::net::IpAddr::from([192, 0, 2, 2])]]
    );
}

// A leg whose naming gives none of the checked addresses carries nothing for
// the name and books no failure against itself.
#[tokio::test]
async fn a_leg_whose_naming_disagrees_carries_nothing_and_stays_up() {
    let (weighted, [_, second], target) = named_legs(&[[192, 0, 2, 1]], &[[192, 0, 2, 3]]).await;
    let _first = weighted.dial(&target).await.unwrap();
    let health = weighted.allocations()[1].health.clone();
    assert!(matches!(
        weighted.dial(&target).await,
        Err(DialError::Skipped(_))
    ));
    assert!(second.dialled.lock().unwrap().is_empty());
    assert_eq!(weighted.allocations()[1].health, health);
    assert_eq!(weighted.allocations()[1].opening, 0);
}
