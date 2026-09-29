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
}
#[async_trait::async_trait]
impl Dialer for Scripted {
    async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
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
        purpose: Purpose::Nntp { server: 1, leg: 0 },
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
