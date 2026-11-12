// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::sync::{Semaphore, mpsc};
use weaver_tunnel::{
    bridge::ConnectionOutcome,
    pipe::{DialPath, DialedStream, Purpose},
};

struct Fixture {
    id: u32,
    opened: mpsc::UnboundedSender<u32>,
    gate: Semaphore,
    prepared: AtomicU64,
    retired: AtomicU64,
    // The signal of the member's full tunnel share, while it has no room.
    full: std::sync::Mutex<Option<Arc<Notify>>>,
    // The pool whose members lock `room` checks is free, and how often it
    // was when asked.
    pool: std::sync::Mutex<Option<std::sync::Weak<PoolState>>>,
    asked_unlocked: AtomicU64,
    // Preparing the member's session fails fatally while set.
    fatal: std::sync::atomic::AtomicBool,
    // Told the member's dial count each time it is asked for room while
    // full.
    asked_full: std::sync::Mutex<Option<mpsc::UnboundedSender<u64>>>,
    dials: AtomicU64,
    // The member's share frees as soon as a dial is turned away full.
    frees_after_dial: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl Dialer for Fixture {
    fn room(&self, _: &Purpose) -> Result<(), Vec<Arc<Notify>>> {
        let pool = self
            .pool
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|pool| pool.upgrade());
        if pool.is_some_and(|pool| pool.members.try_write().is_ok()) {
            self.asked_unlocked.fetch_add(1, Ordering::SeqCst);
        }
        match &*self.full.lock().unwrap() {
            Some(freed) => {
                if let Some(asked) = &*self.asked_full.lock().unwrap() {
                    asked.send(self.dials.load(Ordering::SeqCst)).ok();
                }
                Err(vec![freed.clone()])
            }
            None => Ok(()),
        }
    }
    async fn prepare(&self) -> Result<(), DialError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        if self.fatal.load(Ordering::SeqCst) {
            return Err(DialError::Fatal(weaver_tunnel::TunnelError::Engine(
                "refused".into(),
            )));
        }
        Ok(())
    }
    async fn retire(&self) {
        self.retired.fetch_add(1, Ordering::SeqCst);
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        self.dials.fetch_add(1, Ordering::SeqCst);
        self.opened.send(self.id).unwrap();
        let full = if self.frees_after_dial.load(Ordering::SeqCst) {
            self.full.lock().unwrap().take()
        } else {
            self.full.lock().unwrap().clone()
        };
        if let Some(freed) = full {
            return Err(DialError::AtCapacity(vec![freed]));
        }
        self.gate.acquire().await.unwrap().forget();
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
        Duration::from_secs(15)
    }
    fn describe(&self) -> String {
        format!("fixture {}", self.id)
    }
}
fn fixtures() -> (
    Arc<PoolStage>,
    Vec<Arc<Fixture>>,
    mpsc::UnboundedReceiver<u32>,
) {
    let (opened, recv) = mpsc::unbounded_channel();
    let members: Vec<_> = (1..=2)
        .map(|id| {
            Arc::new(Fixture {
                id,
                opened: opened.clone(),
                gate: Semaphore::new(0),
                prepared: AtomicU64::new(0),
                retired: AtomicU64::new(0),
                full: Default::default(),
                pool: Default::default(),
                asked_unlocked: AtomicU64::new(0),
                fatal: Default::default(),
                asked_full: Default::default(),
                dials: AtomicU64::new(0),
                frees_after_dial: Default::default(),
            })
        })
        .collect();
    let pool = PoolStage::new(
        3,
        members
            .iter()
            .map(|m| (m.id, m.clone() as Arc<dyn Dialer>))
            .collect(),
        tokio::runtime::Handle::current(),
    );
    (pool, members, recv)
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

// A pool whose members' tunnel shares are all full has no room, and says
// which signals free it: each member's and the pool's own. A route over it
// therefore waits instead of dialling; room on one member is room again.
#[tokio::test]
async fn a_pool_whose_members_are_all_full_has_no_room_until_one_frees() {
    let (pool, members, mut opened) = fixtures();
    let freed: Vec<_> = members
        .iter()
        .map(|member| {
            let freed = Arc::new(Notify::new());
            *member.full.lock().unwrap() = Some(freed.clone());
            freed
        })
        .collect();
    let waits = pool.room(&target().purpose).unwrap_err();
    for freed in &freed {
        assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, freed)));
    }
    assert!(
        waits
            .iter()
            .any(|wait| Arc::ptr_eq(wait, &pool.state.changed))
    );
    assert!(opened.try_recv().is_err());

    *members[1].full.lock().unwrap() = None;
    assert!(pool.room(&target().purpose).is_ok());
}

// A pool asks its members for room with its members lock released, so a
// member edit, which holds that lock while it closes connections, never
// waits on a caller that asks for room under a route's lock.
#[tokio::test]
async fn a_pool_asks_its_members_for_room_without_its_members_lock() {
    let (pool, members, _opened) = fixtures();
    for member in &members {
        *member.full.lock().unwrap() = Some(Arc::new(Notify::new()));
        *member.pool.lock().unwrap() = Some(Arc::downgrade(&pool.state));
    }
    assert!(pool.room(&target().purpose).is_err());
    for member in &members {
        assert_eq!(member.asked_unlocked.load(Ordering::SeqCst), 1);
    }
}

// A dial waiting for room on a pool's full members wakes when the members
// become blocked, and then fails at once: no member is left to wait for.
#[tokio::test]
async fn a_wait_for_room_wakes_when_the_last_member_blocks() {
    let (pool, members, _opened) = fixtures();
    for member in &members {
        *member.full.lock().unwrap() = Some(Arc::new(Notify::new()));
        member.fatal.store(true, Ordering::SeqCst);
    }
    let waits = pool.room(&target().purpose).unwrap_err();
    let changed = waits
        .into_iter()
        .find(|wait| Arc::ptr_eq(wait, &pool.state.changed))
        .unwrap();
    let mut prewarmed = false;
    loop {
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if pool.room(&target().purpose).is_ok() {
            break;
        }
        if !prewarmed {
            pool.serve_whole();
            pool.prewarm();
            prewarmed = true;
        }
        notified.await;
    }
    assert!(pool.all_blocked());
    let Err(error) = pool.dial(&target()).await else {
        panic!("a pool with every member blocked dialled");
    };
    assert!(matches!(error, DialError::Fatal(_)), "{error}");
}

// A usenet connect through a pool whose members' shares are all full waits
// for a socket to free instead of dialling again, and dials once one does.
#[tokio::test]
async fn a_usenet_connect_over_a_full_pool_waits_and_redials_once_a_member_frees() {
    let (pool, members, mut opened) = fixtures();
    let (asked, mut asks) = mpsc::unbounded_channel();
    let freed: Vec<_> = members
        .iter()
        .map(|member| {
            let freed = Arc::new(Notify::new());
            *member.full.lock().unwrap() = Some(freed.clone());
            *member.asked_full.lock().unwrap() = Some(asked.clone());
            freed
        })
        .collect();
    let mut server = weaver_nntp::connection::ServerConfig::new(Arc::new(
        weaver_nntp::route_dialer::RouteDialer {
            inner: pool.clone(),
            plan: None,
            runtime: tokio::runtime::Handle::current(),
            server: 1,
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
        },
    ));
    server.host = "news.invalid".into();
    server.port = 119;
    let usenet = Arc::new(weaver_nntp::NntpPool::new(weaver_nntp::PoolConfig {
        servers: vec![weaver_nntp::ServerPoolConfig {
            server,
            max_connections: 1,
            ..Default::default()
        }],
        ..Default::default()
    }));
    let acquiring = tokio::spawn({
        let usenet = usenet.clone();
        async move { usenet.acquire(weaver_nntp::ServerId(0)).await.map(|_| ()) }
    });

    // The connect dialled each member once and was told it is at capacity,
    // with room asked of both members: once by the pool for the signals it
    // hands back and once by the connect before it parks.
    let mut after_dial = 0;
    while after_dial < 4 {
        if asks.recv().await.unwrap() > 0 {
            after_dial += 1;
        }
    }
    let dials = |members: &[Arc<Fixture>]| -> Vec<u64> {
        members
            .iter()
            .map(|member| member.dials.load(Ordering::SeqCst))
            .collect()
    };
    assert_eq!(dials(&members), [1, 1]);
    let mut first = Vec::new();
    while let Ok(id) = opened.try_recv() {
        first.push(id);
    }
    first.sort();
    assert_eq!(first, [1, 2]);

    *members[0].full.lock().unwrap() = None;
    freed[0].notify_waiters();
    while opened.recv().await.unwrap() != 1 {}
    assert_eq!(members[0].dials.load(Ordering::SeqCst), 2);
    assert!(members[1].dials.load(Ordering::SeqCst) <= 2);

    acquiring.abort();
    usenet.shutdown().await;
}

// A pool dial turned away by every member carries the pool's signal and
// the signal of each member still full, even when another member freed a
// socket after turning the dial away.
#[tokio::test]
async fn a_pool_dial_at_capacity_carries_every_full_members_signal() {
    let (pool, members, _opened) = fixtures();
    let freed: Vec<_> = members
        .iter()
        .map(|member| {
            let freed = Arc::new(Notify::new());
            *member.full.lock().unwrap() = Some(freed.clone());
            freed
        })
        .collect();
    members[1].frees_after_dial.store(true, Ordering::SeqCst);
    let Err(DialError::AtCapacity(waits)) = pool.dial(&target()).await else {
        panic!("a pool whose members are all full is at capacity");
    };
    assert!(waits.iter().any(|wait| Arc::ptr_eq(wait, &freed[0])));
    assert!(
        waits
            .iter()
            .any(|wait| Arc::ptr_eq(wait, &pool.state.changed))
    );
    assert!(pool.room(&target().purpose).is_ok());
}

#[tokio::test]
async fn cancelling_the_caller_does_not_cancel_or_fail_the_owned_race() {
    let (pool, members, mut started) = fixtures();
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dial(&target()).await }
    });
    let mut seen = vec![started.recv().await.unwrap(), started.recv().await.unwrap()];
    seen.sort();
    assert_eq!(seen, vec![1, 2]);
    assert!(
        members
            .iter()
            .all(|m| m.prepared.load(Ordering::SeqCst) == 1)
    );
    caller.abort();
    assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    let finished = pool.state.changed.notified();
    tokio::pin!(finished);
    finished.as_mut().enable();
    for member in members {
        member.gate.add_permits(1);
    }
    finished.await;
    let (plan, status) = pool.snapshot();
    assert_eq!((plan.races_won, plan.races_failed), (1, 0));
    assert!(plan.pinned.is_some());
    assert!(plan.candidates.iter().all(|c| c.connect_time.is_some()));
    assert!(status.iter().all(|m| m.open == 0 && m.opening == 0));
}

#[tokio::test]
async fn winner_returns_early_and_late_loser_still_books_connect_evidence() {
    let (pool, members, mut started) = fixtures();
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dial(&target()).await }
    });
    started.recv().await.unwrap();
    started.recv().await.unwrap();
    members[0].gate.add_permits(1);
    let winner = caller.await.unwrap().unwrap();
    assert_eq!(winner.path.member, Some(1));
    assert_eq!(pool.snapshot().1[1].opening, 1);
    let finished = pool.state.changed.notified();
    tokio::pin!(finished);
    finished.as_mut().enable();
    members[1].gate.add_permits(1);
    finished.await;
    let (plan, status) = pool.snapshot();
    assert_eq!(plan.pinned, Some(1));
    assert!(plan.candidates.iter().all(|c| c.connect_time.is_some()));
    assert_eq!((status[0].open, status[1].open), (1, 0));
    drop(winner);
    assert!(pool.snapshot().1.iter().all(|m| m.open == 0));
}

#[tokio::test]
async fn replaced_member_ignores_old_setup_and_transport_failure() {
    let (pool, members, _started) = fixtures();
    members[0].gate.add_permits(1);
    let lease = pool.lease().unwrap();
    let mut old = lease.dial(&target()).await.unwrap();
    let (opened, _) = mpsc::unbounded_channel();
    let replacement = Arc::new(Fixture {
        id: 1,
        opened,
        gate: Semaphore::new(0),
        prepared: AtomicU64::new(0),
        retired: AtomicU64::new(0),
        full: Default::default(),
        pool: Default::default(),
        asked_unlocked: AtomicU64::new(0),
        fatal: Default::default(),
        asked_full: Default::default(),
        dials: AtomicU64::new(0),
        frees_after_dial: Default::default(),
    });
    pool.set_members(vec![(1, replacement), (2, members[1].clone())]);
    old.setup.take().unwrap().complete(false);
    old.outcome.failed();
    assert!(pool.snapshot().0.candidates.iter().all(|c| c.failures == 0));
    assert!(matches!(
        lease.dial(&target()).await,
        Err(DialError::Skipped(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn feed_lease_prevents_idle_retirement_until_released() {
    let (pool, members, _) = fixtures();
    let lease = pool.lease().unwrap();
    let member = pool.state.members.read().unwrap()[&1].clone();
    member.warm().await.unwrap();
    assert_eq!(pool.snapshot().1[0].opening, 0);
    lease.resolve("news.invalid").await.unwrap();
    assert_eq!(pool.snapshot().1[0].opening, 1);
    assert_eq!(pool.snapshot().1[1].opening, 0);
    tokio::time::advance(weaver_nntp::plan_timing::timing().pool_idle_retire).await;
    member.retire_if_idle().await;
    assert_eq!(members[0].retired.load(Ordering::SeqCst), 0);
    drop(lease);
    member.retire_if_idle().await;
    assert_eq!(members[0].retired.load(Ordering::SeqCst), 1);
    member.warm().await.unwrap();
    assert_eq!(members[0].prepared.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn membership_edit_retires_only_removed_members_at_work_boundary() {
    let (pool, members, _started) = fixtures();
    let selected: Vec<_> = {
        let current = pool.state.members.read().unwrap();
        [current[&1].clone(), current[&2].clone()]
    }
    .into();
    let mut streams = Vec::new();
    let mut closed = Vec::new();
    let mut retiring = Vec::new();
    for (fixture, member) in members.iter().zip(selected) {
        fixture.gate.add_permits(1);
        let (stream, _) = member.dial(&target()).await.unwrap();
        let count = Arc::new(AtomicU64::new(0));
        let report = count.clone();
        stream.outcome.on_close(move || {
            report.fetch_add(1, Ordering::SeqCst);
        });
        closed.push(count);
        let count = Arc::new(AtomicU64::new(0));
        let report = count.clone();
        stream.outcome.on_retire(move || {
            report.fetch_add(1, Ordering::SeqCst);
        });
        retiring.push(count);
        streams.push(stream);
    }
    assert_eq!(pool.set_members(vec![(2, members[1].clone())]), [1]);
    assert_eq!(retiring[0].load(Ordering::SeqCst), 1);
    assert_eq!(retiring[1].load(Ordering::SeqCst), 0);
    assert_eq!(closed[0].load(Ordering::SeqCst), 0);
    assert_eq!(closed[1].load(Ordering::SeqCst), 0);
    assert_eq!(pool.snapshot().1[0].open, 1);
    drop(streams);
    assert_eq!(closed[0].load(Ordering::SeqCst), 1);
    assert_eq!(closed[1].load(Ordering::SeqCst), 1);
}

// Removing a member closes its idle connections, and a connection's close
// can record on the pool, as a shadow connection's does, or take the lock of
// the route over the pool. The edit closes them with the members lock
// released, so neither waits on that lock.
#[tokio::test]
async fn removing_a_member_closes_its_connections_with_the_members_lock_released() {
    let (pool, members, _started) = fixtures();
    let member = pool.state.members.read().unwrap()[&1].clone();
    members[0].gate.add_permits(1);
    let (stream, _) = member.dial(&target()).await.unwrap();
    let recorded = Arc::new(AtomicU64::new(0));
    stream.outcome.on_close({
        let state = Arc::downgrade(&pool.state);
        let member = member.clone();
        let recorded = recorded.clone();
        move || {
            let state = state.upgrade().unwrap();
            if state.members.try_write().is_ok() {
                state.record(&member, |_| {});
                recorded.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    // An idle connection is dropped by whoever holds it once it is recalled.
    let idle = Arc::new(std::sync::Mutex::new(Some(stream)));
    let outcome = idle.lock().unwrap().as_ref().unwrap().outcome.clone();
    outcome.on_retire({
        let idle = idle.clone();
        move || drop(idle.lock().unwrap().take())
    });
    drop(outcome);

    assert_eq!(pool.set_members(vec![(2, members[1].clone())]), [1]);
    assert!(idle.lock().unwrap().is_none());
    assert_eq!(recorded.load(Ordering::SeqCst), 1);
}

struct FatalMember;
#[async_trait::async_trait]
impl Dialer for FatalMember {
    async fn prepare(&self) -> Result<(), DialError> {
        Err(DialError::Fatal(weaver_tunnel::TunnelError::Engine(
            "trust rejected".into(),
        )))
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        panic!("fatal preflight must stop channel opens")
    }
    fn budget(&self) -> Duration {
        Duration::from_secs(1)
    }
    fn describe(&self) -> String {
        "fatal fixture".into()
    }
}
#[tokio::test]
async fn fatal_member_is_isolated_and_profile_replacement_clears_its_block() {
    let (_, members, mut started) = fixtures();
    let pool = PoolStage::new(
        7,
        vec![(1, Arc::new(FatalMember)), (2, members[1].clone())],
        tokio::runtime::Handle::current(),
    );
    let feed_pool = PoolStage::new(
        8,
        vec![(1, Arc::new(FatalMember)), (2, members[1].clone())],
        tokio::runtime::Handle::current(),
    );
    let failed_feed_member = feed_pool
        .lease_members()
        .into_iter()
        .find(|(id, _)| *id == 1)
        .unwrap()
        .1;
    assert!(matches!(
        failed_feed_member.dial(&target()).await,
        Err(DialError::Egress(_))
    ));
    for _ in 0..2 {
        members[1].gate.add_permits(1);
        let dialed = pool.dial(&target()).await.unwrap();
        assert_eq!(dialed.path.member, Some(2));
        assert_eq!(started.recv().await.unwrap(), 2);
    }
    assert!(pool.snapshot().1[0].blocked.is_some());
    assert!(pool.lease_members().iter().all(|(id, _)| *id == 2));
    pool.set_members(vec![(1, members[0].clone()), (2, members[1].clone())]);
    assert!(
        pool.snapshot()
            .1
            .iter()
            .all(|member| member.blocked.is_none())
    );
    let lease = pool
        .lease_members()
        .into_iter()
        .find(|(id, _)| *id == 1)
        .unwrap()
        .1;
    members[0].gate.add_permits(1);
    assert_eq!(lease.dial(&target()).await.unwrap().path.member, Some(1));
}

#[tokio::test]
async fn all_blocked_members_preserve_fatal_trust_failure() {
    let pool = PoolStage::new(
        7,
        vec![(1, Arc::new(FatalMember))],
        tokio::runtime::Handle::current(),
    );
    for _ in 0..2 {
        assert!(matches!(
            pool.dial(&target()).await,
            Err(DialError::Fatal(_))
        ));
    }
}

#[tokio::test]
async fn feed_members_fail_over_and_pin_only_after_request_success() {
    let (pool, _, _) = fixtures();
    let leases = pool.lease_members();
    assert_eq!(leases.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [1, 2]);
    leases[0]
        .1
        .report_request(Some(&DialError::Egress(std::io::Error::other(
            "proxy unavailable",
        ))));
    assert_eq!(pool.snapshot().0.pinned, None);
    assert_eq!(pool.lease_members()[0].0, 2);
    leases[1].1.report_request(None);
    assert_eq!(pool.snapshot().0.pinned, Some(2));
    assert_eq!(pool.lease_members()[0].0, 2);
    leases[1]
        .1
        .report_request(Some(&DialError::Destination(std::io::Error::other(
            "origin unavailable",
        ))));
    assert_eq!(
        pool.snapshot()
            .0
            .candidates
            .iter()
            .find(|c| c.candidate == 2)
            .unwrap()
            .failures,
        0
    );
}

#[tokio::test(start_paused = true)]
async fn hung_preparation_is_bounded_and_other_member_can_win() {
    struct Hung(tokio::sync::mpsc::UnboundedSender<()>);
    #[async_trait::async_trait]
    impl Dialer for Hung {
        async fn prepare(&self) -> Result<(), DialError> {
            self.0.send(()).unwrap();
            std::future::pending().await
        }
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            panic!("a failed preparation must not open a channel")
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "hung fixture".into()
        }
    }
    let (_, members, mut started) = fixtures();
    let (preparing, mut prepared) = mpsc::unbounded_channel();
    let pool = PoolStage::new(
        7,
        vec![(1, Arc::new(Hung(preparing))), (2, members[1].clone())],
        tokio::runtime::Handle::current(),
    );
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dial(&target()).await }
    });
    prepared.recv().await.unwrap();
    assert_eq!(started.recv().await.unwrap(), 2);
    members[1].gate.add_permits(1);
    let winner = caller.await.unwrap().unwrap();
    assert_eq!(winner.path.member, Some(2));
    assert_eq!(pool.snapshot().0.races_won, 0);
    let finished = pool.state.changed.notified();
    tokio::pin!(finished);
    finished.as_mut().enable();
    tokio::time::advance(Duration::from_secs(1)).await;
    finished.await;
    assert_eq!(
        pool.snapshot()
            .0
            .candidates
            .iter()
            .find(|c| c.candidate == 1)
            .unwrap()
            .failures,
        1
    );
}

#[tokio::test]
async fn connections_that_fail_after_setup_do_not_make_the_pin_suspect() {
    let (pool, members, mut started) = fixtures();
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dial(&target()).await }
    });
    started.recv().await.unwrap();
    started.recv().await.unwrap();
    let finished = pool.state.changed.notified();
    tokio::pin!(finished);
    finished.as_mut().enable();
    for member in &members {
        member.gate.add_permits(1);
    }
    let first = caller.await.unwrap().unwrap();
    finished.await;
    let pin = pool.snapshot().0.pinned.unwrap();
    assert_eq!(first.path.member, Some(pin));
    // The pinned connection dies twice over, as a server dropping sessions
    // mid-transfer would have it; the pool must not treat that as the pin
    // refusing connects and race again.
    first.outcome.failed();
    first.outcome.failed();
    let (plan, _) = pool.snapshot();
    assert_eq!(plan.pinned, Some(pin));
    assert!(
        plan.candidates.iter().all(|c| c.failures == 0),
        "{:?}",
        plan.candidates
    );
    members[pin as usize - 1].gate.add_permits(1);
    let next = pool.dial(&target()).await.unwrap();
    assert_eq!(
        next.path.member,
        Some(pin),
        "the reconnect dials the pin, not a race"
    );
    assert_eq!(started.recv().await.unwrap(), pin);
    assert!(started.try_recv().is_err(), "no other member was dialled");
    assert_eq!(pool.snapshot().0.races_won, 1);
}

#[tokio::test(start_paused = true)]
async fn the_member_carrying_traffic_reports_its_connections_and_rate() {
    let (pool, members, _started) = fixtures();
    members[1].gate.add_permits(1);
    let (_, lease) = pool
        .lease_members()
        .into_iter()
        .find(|(id, _)| *id == 2)
        .unwrap();
    let dialed = lease.dial(&target()).await.unwrap();
    assert_eq!(dialed.path.member, Some(2));
    let status = pool.snapshot().1;
    assert_eq!((status[0].open, status[1].open), (0, 1));
    assert_eq!(
        (status[0].bytes_per_second, status[1].bytes_per_second),
        (0, 0)
    );
    // Read once a second, the rate spans the last four.
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(1)).await;
        dialed.outcome.read(1 << 20);
        pool.snapshot();
    }
    let status = pool.snapshot().1;
    assert_eq!((status[0].open, status[1].open), (0, 1));
    assert_eq!(
        (status[0].bytes_per_second, status[1].bytes_per_second),
        (0, 1 << 20)
    );
    // Once the connection closes and the window passes, nothing is carried.
    drop(dialed);
    tokio::time::advance(MEMBER_RATE_WINDOW + Duration::from_secs(1)).await;
    let status = pool.snapshot().1;
    assert_eq!((status[1].open, status[1].bytes_per_second), (0, 0));
}

// A member no session opens on is passed over once that has happened as
// often in a row as the plan takes to call a member suspect, and only while
// another member is up; its plan evidence says so. One failed open passes
// nothing over, and one that comes up ends the run.
#[tokio::test]
async fn a_member_no_session_opens_on_is_passed_over_while_another_is_up() {
    let (pool, _members, _opened) = fixtures();
    let one = pool.pin_member(1).unwrap();
    let two = pool.pin_member(2).unwrap();
    let failures = |pool: &PoolStage, id: u32| {
        pool.snapshot()
            .0
            .candidates
            .into_iter()
            .find(|candidate| candidate.candidate == id)
            .map(|candidate| (candidate.failures, candidate.state))
            .unwrap()
    };

    one.opened(false);
    assert_eq!(pool.member_down(1), None);
    assert!(pool.members_down().is_empty());
    assert_eq!(failures(&pool, 1).0, 1);

    one.opened(false);
    assert_eq!(
        pool.member_down(1).as_deref(),
        Some("proxy 1 opened no session 2 times in a row")
    );
    assert_eq!(failures(&pool, 1), (SUSPECT_AFTER_FAILURES, "SUSPECT"));
    assert_eq!(pool.member_down(2), None);

    // With no other member to go to, each stays up.
    two.opened(false);
    two.opened(false);
    assert!(pool.members_down().is_empty());

    // A session that comes up on the other ends its run, and the first is
    // passed over again.
    two.opened(true);
    assert_eq!(failures(&pool, 2).0, 0);
    assert_eq!(pool.members_down().keys().copied().collect::<Vec<_>>(), [1]);
    one.opened(true);
    assert!(pool.members_down().is_empty());
}
