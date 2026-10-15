use super::*;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use tokio::sync::oneshot;

fn address() -> IpAddr {
    "192.0.2.7".parse().unwrap()
}

fn answer(ttl: Ttl) -> Lookup {
    Lookup {
        addresses: vec![address()],
        ttl,
        nonexistent: false,
    }
}

fn negative(ttl: Ttl) -> Lookup {
    Lookup {
        addresses: Vec::new(),
        ttl,
        nonexistent: false,
    }
}

// A cache whose lookups are counted.
#[derive(Default)]
struct Counted {
    cache: ResolutionCache<io::Error>,
    asked: AtomicUsize,
}

impl Counted {
    fn with(policy: Policy) -> Self {
        Self {
            cache: ResolutionCache::new(policy),
            asked: AtomicUsize::new(0),
        }
    }
    async fn resolve(&self, name: &str, found: io::Result<Lookup>) -> io::Result<Vec<IpAddr>> {
        self.cache
            .resolve(Resolver::Routed, name, || async move {
                self.asked.fetch_add(1, SeqCst);
                found
            })
            .await
    }
    fn asked(&self) -> usize {
        self.asked.load(SeqCst)
    }
}

#[tokio::test(start_paused = true)]
async fn an_answer_is_held_for_its_ttl_and_asked_for_again_after() {
    let cache = Counted::default();
    let found = cache.resolve("a.test", Ok(answer(Ttl::Seconds(30))));
    assert_eq!(found.await.unwrap(), [address()]);
    tokio::time::advance(Duration::from_secs(29)).await;
    let held = cache.resolve("a.test", Ok(answer(Ttl::Seconds(30))));
    assert_eq!(held.await.unwrap(), [address()]);
    assert_eq!(cache.asked(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    cache
        .resolve("a.test", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn an_answer_with_a_zero_ttl_is_never_held() {
    let cache = Counted::default();
    for _ in 0..2 {
        cache
            .resolve("zero.test", Ok(answer(Ttl::Seconds(0))))
            .await
            .unwrap();
    }
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn no_answer_is_held_past_the_ceiling() {
    let cache = Counted::default();
    let day = Ttl::Seconds(24 * 60 * 60);
    cache.resolve("long.test", Ok(answer(day))).await.unwrap();
    tokio::time::advance(Duration::from_secs(60 * 60 - 1)).await;
    cache.resolve("long.test", Ok(answer(day))).await.unwrap();
    assert_eq!(cache.asked(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    cache.resolve("long.test", Ok(answer(day))).await.unwrap();
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn an_answer_without_a_ttl_is_held_for_a_minute() {
    let cache = Counted::default();
    cache
        .resolve("untimed.test", Ok(answer(Ttl::Unknown)))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    cache
        .resolve("untimed.test", Ok(answer(Ttl::Unknown)))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    cache
        .resolve("untimed.test", Ok(answer(Ttl::Unknown)))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_negative_answer_is_held_for_its_ttl_and_at_most_a_minute() {
    let cache = Counted::default();
    let short = negative(Ttl::Seconds(10));
    assert!(
        cache
            .resolve("short.test", Ok(short.clone()))
            .await
            .unwrap()
            .is_empty()
    );
    let long = negative(Ttl::Seconds(300));
    cache.resolve("long.test", Ok(long.clone())).await.unwrap();
    tokio::time::advance(Duration::from_secs(9)).await;
    cache
        .resolve("short.test", Ok(short.clone()))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 2);
    tokio::time::advance(Duration::from_secs(1)).await;
    cache.resolve("short.test", Ok(short)).await.unwrap();
    assert_eq!(cache.asked(), 3);
    tokio::time::advance(Duration::from_secs(49)).await;
    let held = cache.resolve("long.test", Ok(long.clone()));
    assert!(held.await.unwrap().is_empty());
    assert_eq!(cache.asked(), 3);
    tokio::time::advance(Duration::from_secs(1)).await;
    cache.resolve("long.test", Ok(long)).await.unwrap();
    assert_eq!(cache.asked(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_negative_answer_that_does_not_say_for_how_long_is_not_held() {
    let cache = Counted::default();
    for _ in 0..2 {
        let found = cache.resolve("unknown.test", Ok(negative(Ttl::Unknown)));
        assert!(found.await.unwrap().is_empty());
    }
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_failed_lookup_is_never_held() {
    let cache = Counted::default();
    for _ in 0..2 {
        let failed = cache.resolve("down.test", Err(io::Error::other("unreachable")));
        assert_eq!(failed.await.unwrap_err().to_string(), "unreachable");
    }
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn names_differing_in_case_or_a_trailing_dot_share_an_answer() {
    let cache = Counted::default();
    cache
        .resolve("Mixed.TEST.", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    cache
        .resolve("mixed.test", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_address_literal_is_never_looked_up() {
    let cache = Counted::default();
    for literal in ["192.0.2.1", "2001:db8::1"] {
        let found = cache.resolve(literal, Ok(answer(Ttl::Seconds(30))));
        assert_eq!(found.await.unwrap(), [literal.parse::<IpAddr>().unwrap()]);
    }
    assert_eq!(cache.asked(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_forgotten_answer_is_asked_for_again() {
    let cache = Counted::default();
    cache
        .resolve("stale.test", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    cache.cache.forget("stale.test");
    cache
        .resolve("stale.test", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_full_cache_drops_the_answer_closest_to_running_out() {
    let cache = Counted::with(Policy {
        capacity: 2,
        ..Policy::default()
    });
    for (name, ttl) in [("soon.test", 10), ("late.test", 100), ("middle.test", 50)] {
        cache
            .resolve(name, Ok(answer(Ttl::Seconds(ttl))))
            .await
            .unwrap();
    }
    for name in ["late.test", "middle.test"] {
        cache
            .resolve(name, Ok(answer(Ttl::Seconds(30))))
            .await
            .unwrap();
    }
    assert_eq!(cache.asked(), 3);
    cache
        .resolve("soon.test", Ok(answer(Ttl::Seconds(30))))
        .await
        .unwrap();
    assert_eq!(cache.asked(), 4);
}

#[tokio::test]
async fn callers_asking_together_share_one_lookup() {
    let cache = Arc::new(ResolutionCache::<io::Error>::default());
    let asked = Arc::new(AtomicUsize::new(0));
    let (open, gate) = oneshot::channel::<()>();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let (cache, asked, gate) = (cache.clone(), asked.clone(), gate.clone());
        callers.spawn(async move {
            cache
                .resolve(Resolver::Routed, "shared.test", || async move {
                    asked.fetch_add(1, SeqCst);
                    let gate = gate.lock().unwrap().take().expect("one lookup");
                    gate.await.unwrap();
                    // Not held, so every caller's answer is the one lookup's.
                    Ok(answer(Ttl::Seconds(0)))
                })
                .await
        });
    }
    while cache.waiting("shared.test") != Some(3) {
        tokio::task::yield_now().await;
    }
    open.send(()).unwrap();
    while let Some(found) = callers.join_next().await {
        assert_eq!(found.unwrap().unwrap(), [address()]);
    }
    assert_eq!(asked.load(SeqCst), 1);
}

#[tokio::test]
async fn a_failure_reaches_every_waiting_caller_and_is_not_held() {
    let cache = Arc::new(ResolutionCache::<io::Error>::default());
    let (open, gate) = oneshot::channel::<()>();
    let gate = Arc::new(Mutex::new(Some(gate)));
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..3 {
        let (cache, gate) = (cache.clone(), gate.clone());
        callers.spawn(async move {
            cache
                .resolve(Resolver::Routed, "refused.test", || async move {
                    let gate = gate.lock().unwrap().take().expect("one lookup");
                    gate.await.unwrap();
                    Err(io::Error::new(io::ErrorKind::ConnectionRefused, "refused"))
                })
                .await
        });
    }
    while cache.waiting("refused.test") != Some(2) {
        tokio::task::yield_now().await;
    }
    open.send(()).unwrap();
    while let Some(found) = callers.join_next().await {
        let error = found.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(error.to_string(), "refused");
    }
    let again = cache.resolve(Resolver::Routed, "refused.test", || async {
        Ok(answer(Ttl::Seconds(30)))
    });
    assert_eq!(again.await.unwrap(), [address()]);
}

#[tokio::test]
async fn a_lookup_dropped_before_it_lands_lets_a_waiting_caller_ask() {
    let cache = Arc::new(ResolutionCache::<io::Error>::default());
    let leader = {
        let cache = cache.clone();
        tokio::spawn(async move {
            cache
                .resolve(Resolver::Routed, "dropped.test", || {
                    std::future::pending::<io::Result<Lookup>>()
                })
                .await
        })
    };
    while cache.waiting("dropped.test").is_none() {
        tokio::task::yield_now().await;
    }
    let follower = {
        let cache = cache.clone();
        tokio::spawn(async move {
            cache
                .resolve(Resolver::Routed, "dropped.test", || async {
                    Ok(answer(Ttl::Seconds(30)))
                })
                .await
        })
    };
    while cache.waiting("dropped.test") != Some(1) {
        tokio::task::yield_now().await;
    }
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    assert_eq!(follower.await.unwrap().unwrap(), [address()]);
    let held = cache.resolve(Resolver::Routed, "dropped.test", || async {
        unreachable!("the follower's answer is held")
    });
    assert_eq!(held.await.unwrap(), [address()]);
}

#[tokio::test(start_paused = true)]
async fn a_blocking_caller_shares_the_held_answers() {
    let cache = ResolutionCache::<io::Error>::default();
    let asked = AtomicUsize::new(0);
    let lookup = || {
        asked.fetch_add(1, SeqCst);
        Ok(answer(Ttl::Seconds(30)))
    };
    for _ in 0..2 {
        let found = cache.resolve_blocking(Resolver::Host, "blocking.test", lookup);
        assert_eq!(found.unwrap(), [address()]);
    }
    assert_eq!(asked.load(SeqCst), 1);
    let held = cache.resolve(Resolver::Host, "blocking.test", || async {
        unreachable!("the blocking caller's answer is held")
    });
    assert_eq!(held.await.unwrap(), [address()]);
    tokio::time::advance(Duration::from_secs(30)).await;
    cache
        .resolve_blocking(Resolver::Host, "blocking.test", lookup)
        .unwrap();
    assert_eq!(asked.load(SeqCst), 2);
}

#[test]
fn a_replicated_failure_keeps_its_kind_message_and_source() {
    let refused = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
    let copy = refused.replicate();
    assert_eq!(
        (copy.kind(), copy.to_string()),
        (refused.kind(), refused.to_string())
    );
    let unresolved = io::Error::other(ResolutionFailed).replicate();
    assert!(unresolved.get_ref().unwrap().is::<ResolutionFailed>());
    let tunnel = TunnelError::Engine("routed DNS could not resolve the destination".into());
    let copy = io::Error::other(tunnel.clone()).replicate();
    assert_eq!(
        copy.get_ref().unwrap().downcast_ref::<TunnelError>(),
        Some(&tunnel)
    );
}

#[test]
fn the_host_order_is_kept_without_repeats() {
    let [a, b]: [IpAddr; 2] = ["2001:db8::1".parse().unwrap(), "192.0.2.1".parse().unwrap()];
    assert_eq!(untimed([a, b, a].into_iter()).addresses, [a, b]);
}

#[tokio::test]
async fn a_host_that_answers_on_no_address_is_asked_for_again() {
    host_lookup("localhost").await.unwrap();
    assert!(HOST.held("localhost"));
    // Nothing listens on port zero.
    connect_host("localhost", 0).await.unwrap_err();
    assert!(!HOST.held("localhost"));
}
