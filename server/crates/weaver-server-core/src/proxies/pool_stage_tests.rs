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
}
#[async_trait::async_trait]
impl Dialer for Fixture {
    async fn prepare(&self) -> Result<(), DialError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn retire(&self) {
        self.retired.fetch_add(1, Ordering::SeqCst);
    }
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
        self.opened.send(self.id).unwrap();
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
        purpose: Purpose::Nntp { server: 1, leg: 0 },
        addresses: Vec::new(),
    }
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
    tokio::time::advance(POOL_IDLE_RETIRE).await;
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
