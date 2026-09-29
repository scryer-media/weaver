use super::*;

fn pinned<C: Candidate>(candidates: &[C], now: Instant) -> CandidatePlan<C> {
    let mut plan = CandidatePlan::default();
    plan.set_candidates(candidates.to_vec(), now);
    assert!(matches!(
        plan.next_attempt(now),
        Attempt::Race {
            reason: RaceReason::Initial,
            ..
        }
    ));
    assert!(matches!(plan.next_attempt(now), Attempt::Wait(0)));
    for (i, &candidate) in candidates.iter().enumerate() {
        plan.connected(candidate, Duration::from_millis(10 + i as u64), now);
    }
    plan.race_finished(Ok(candidates[0]), RaceReason::Initial, now);
    plan
}
fn deliver<C: Candidate>(
    plan: &mut CandidatePlan<C>,
    candidate: C,
    samples: u32,
    wire: Duration,
    now: Instant,
) {
    for _ in 0..samples {
        plan.delivered(candidate, 1_000_000, wire, now);
    }
}
fn dial<C: Candidate>(plan: &mut CandidatePlan<C>, now: Instant) -> Vec<C> {
    match plan.next_attempt(now) {
        Attempt::Dial(order) => order,
        _ => panic!("expected sequential dial"),
    }
}

// Every scenario runs unchanged against socket addresses and pool member IDs.
fn scenarios<C: Candidate>([a, b, c, d]: [C; 4]) {
    let now = Instant::now();
    let mut plan = pinned(&[a, b, c], now);
    assert_eq!(dial(&mut plan, now), [a, b, c]);
    plan.failed(a, now);
    assert_eq!(dial(&mut plan, now), [a, b, c]);
    plan.failed(a, now);
    assert!(matches!(
        plan.next_attempt(now),
        Attempt::Race {
            reason: RaceReason::Suspect,
            ..
        }
    ));
    plan.race_finished(Ok(b), RaceReason::Suspect, now);
    assert_eq!(dial(&mut plan, now), [b, c, a]);
    plan.over_limit_cleared(now);
    assert!(matches!(
        plan.next_attempt(now),
        Attempt::Race {
            reason: RaceReason::OverLimitCleared,
            ..
        }
    ));

    let mut plan = pinned(&[a, b], now);
    plan.setup(a, false, now);
    plan.connected(a, Duration::from_millis(1), now);
    plan.setup(a, false, now);
    assert!(matches!(
        plan.next_attempt(now),
        Attempt::Race {
            reason: RaceReason::Suspect,
            ..
        }
    ));
    plan.race_finished(Ok(a), RaceReason::Suspect, now);
    plan.setup(a, true, now);
    assert_eq!(plan.stats(a).setup_failures, 0);
    plan.body_latency(a, Duration::from_millis(40), now);
    plan.body_latency(d, Duration::from_millis(40), now);
    plan.delivered(a, 500_000, Duration::from_millis(50), now);
    plan.delivered(a, 0, Duration::from_millis(50), now);
    plan.delivered(a, 500_000, Duration::ZERO, now);
    plan.delivered(d, 500_000, Duration::from_millis(50), now);
    assert_eq!(
        plan.stats(a).body_latency_ewma,
        Some(Duration::from_millis(40))
    );
    assert_eq!(plan.stats(a).delivery.samples, 1);
    assert_eq!(
        plan.stats(a).delivery.bytes_per_second(),
        Some(10_000_000.0)
    );
    assert!(!plan.per_addr.contains_key(&d.key()));
    plan.set_candidates(vec![a, c, a], now);
    assert_eq!(plan.candidates, [a, c]);
    assert_eq!(plan.pinned, Some(a));
    assert_eq!(plan.stats(a).delivery.samples, 1);
    assert!(!plan.per_addr.contains_key(&b.key()));

    let mut plan = CandidatePlan::default();
    plan.set_candidates(vec![a, b], now);
    assert!(matches!(plan.next_attempt(now), Attempt::Race { .. }));
    plan.race_finished(
        Err((io::ErrorKind::ConnectionRefused, "refused".into())),
        RaceReason::Initial,
        now,
    );
    assert_eq!(dial(&mut plan, now), [a, b]);
    assert_eq!(
        dial(
            &mut plan,
            now + FAILED_RACE_HOLDOFF - Duration::from_nanos(1)
        ),
        [a, b]
    );
    assert!(matches!(
        plan.next_attempt(now + FAILED_RACE_HOLDOFF),
        Attempt::Race {
            reason: RaceReason::Initial,
            ..
        }
    ));

    for (wire, repins) in [
        (Duration::from_millis(800), true),
        (Duration::from_millis(900), false),
    ] {
        let mut plan = pinned(&[a, b], now);
        for verdict in 1..=2 {
            let at = now + DELIVERY_VERDICT_INTERVAL * verdict;
            deliver(
                &mut plan,
                a,
                DELIVERY_MIN_SAMPLES,
                Duration::from_secs(1),
                at,
            );
            deliver(&mut plan, b, DELIVERY_MIN_SAMPLES, wire, at);
            let expected = if verdict == 2 && repins { b } else { a };
            assert_eq!(dial(&mut plan, at)[0], expected);
            assert_eq!(plan.stats(a).delivery.samples, 0);
            assert_eq!(plan.stats(b).delivery.samples, 0);
            assert_eq!(plan.races_won, 1);
        }
        assert_eq!(plan.repins[RaceReason::Delivery.index()], u64::from(repins));
        assert!(matches!(
            plan.next_attempt(now + ADDRESS_REPLAN_INTERVAL),
            Attempt::Race {
                reason: RaceReason::Interval,
                ..
            }
        ));
    }

    let mut plan = pinned(&[a, b, c], now);
    plan.failed(c, now);
    deliver(
        &mut plan,
        a,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        now,
    );
    for _ in 0..SHADOW_EVERY_CONNECTS {
        assert_eq!(dial(&mut plan, now)[0], a);
    }
    assert_eq!(dial(&mut plan, now + SHADOW_MIN_PIN_AGE)[0], b);
    assert_eq!(plan.pinned, Some(a));
    for _ in 0..SHADOW_EVERY_CONNECTS + 1 {
        assert_eq!(dial(&mut plan, now + SHADOW_MIN_PIN_AGE)[0], a);
    }
    assert_eq!(
        dial(&mut plan, now + SHADOW_MIN_PIN_AGE + SHADOW_INTERVAL)[0],
        b
    );

    let mut plan = pinned(&[a, b], now);
    for verdict in 1..=2 {
        let at = now + DELIVERY_VERDICT_INTERVAL * verdict;
        deliver(
            &mut plan,
            a,
            DELIVERY_MIN_SAMPLES,
            Duration::from_secs(1),
            at,
        );
        deliver(
            &mut plan,
            b,
            DELIVERY_MIN_SAMPLES,
            Duration::from_millis(800),
            at,
        );
        plan.failed(b, at);
        assert_eq!(dial(&mut plan, at)[0], a);
        assert_eq!(plan.repins[RaceReason::Delivery.index()], 0);
    }
}

#[test]
fn socket_address_scenarios() {
    scenarios([1, 2, 3, 4].map(|last| SocketAddr::from(([192, 0, 2, last], 563))));
}
#[test]
fn pool_member_scenarios() {
    scenarios([1_u32, 2, 3, 4]);
}

// Selection cases from the address driver also run directly against both candidate keys.
fn selection_edges<C: Candidate>([a, b, c]: [C; 3]) {
    let now = Instant::now();
    let mut empty = CandidatePlan::<C>::default();
    assert!(
        matches!(empty.next_attempt(now), Attempt::Race { candidates, .. } if candidates.is_empty())
    );
    empty.race_finished(
        Err((io::ErrorKind::AddrNotAvailable, "no candidates".into())),
        RaceReason::Initial,
        now,
    );
    assert!(matches!(
        empty.next_attempt(now),
        Attempt::Fail(io::ErrorKind::AddrNotAvailable, _)
    ));

    let mut plan = pinned(&[a, b], now);
    plan.race_finished(
        Err((io::ErrorKind::TimedOut, "none answered".into())),
        RaceReason::Interval,
        now + ADDRESS_REPLAN_INTERVAL,
    );
    assert_eq!(plan.snapshot().pinned, Some(a));
    assert_eq!(dial(&mut plan, now + ADDRESS_REPLAN_INTERVAL)[0], a);
    assert!(matches!(
        plan.next_attempt(now + ADDRESS_REPLAN_INTERVAL * 2),
        Attempt::Race {
            reason: RaceReason::Interval,
            ..
        }
    ));

    // Many fast samples still need the minimum total wire time.
    let mut plan = pinned(&[a, b], now);
    let at = now + DELIVERY_VERDICT_INTERVAL;
    deliver(
        &mut plan,
        a,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        at,
    );
    deliver(
        &mut plan,
        b,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(1),
        at,
    );
    assert_eq!(dial(&mut plan, at)[0], a);
    assert!(plan.last_verdict_at.is_none());
    assert_eq!(plan.stats(b).delivery.samples, DELIVERY_MIN_SAMPLES);

    // A different challenger resets the two-verdict confirmation.
    let mut plan = pinned(&[a, b, c], now);
    for (i, challenger) in [b, c, c].into_iter().enumerate() {
        let at = now + DELIVERY_VERDICT_INTERVAL * (i as u32 + 1);
        deliver(
            &mut plan,
            a,
            DELIVERY_MIN_SAMPLES,
            Duration::from_secs(1),
            at,
        );
        deliver(
            &mut plan,
            challenger,
            DELIVERY_MIN_SAMPLES,
            Duration::from_millis(800),
            at,
        );
        assert_eq!(dial(&mut plan, at)[0], if i == 2 { c } else { a });
        assert_eq!(plan.chosen_at, Some(now));
    }
    assert_eq!(plan.repins[RaceReason::Delivery.index()], 1);

    // Evidence alone cannot bypass the verdict interval or delay the age race.
    let mut plan = pinned(&[a, b], now);
    deliver(
        &mut plan,
        a,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        now,
    );
    deliver(
        &mut plan,
        b,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(800),
        now,
    );
    assert_eq!(
        dial(
            &mut plan,
            now + DELIVERY_VERDICT_INTERVAL - Duration::from_nanos(1)
        )[0],
        a
    );
    assert!(plan.delivery_leader.is_none());
    assert_eq!(dial(&mut plan, now + DELIVERY_VERDICT_INTERVAL)[0], a);
    assert_eq!(plan.delivery_leader, Some(b));
    let at = now + ADDRESS_REPLAN_INTERVAL;
    deliver(
        &mut plan,
        a,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        at,
    );
    deliver(
        &mut plan,
        b,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(800),
        at,
    );
    assert!(matches!(
        plan.next_attempt(at),
        Attempt::Race {
            reason: RaceReason::Interval,
            ..
        }
    ));

    // Measured challengers are no longer shadowed; stale evidence cannot repin.
    let mut plan = pinned(&[a, b], now);
    deliver(
        &mut plan,
        a,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        now,
    );
    deliver(
        &mut plan,
        b,
        DELIVERY_MIN_SAMPLES,
        Duration::from_secs(1),
        now,
    );
    plan.connects_since_shadow = SHADOW_EVERY_CONNECTS;
    assert!(plan.shadow_candidate(now + SHADOW_MIN_PIN_AGE).is_none());
    assert!(matches!(
        plan.delivery_verdict(now + DELIVERY_EVIDENCE_AGE, a),
        DeliveryVerdict::Unmeasured
    ));
    plan.set_candidates(vec![b, c], now);
    assert_eq!(plan.snapshot().pinned, None);
    assert!(!plan.per_addr.contains_key(&a.key()));
}
#[test]
fn socket_selection_edge_scenarios() {
    selection_edges([1, 2, 3].map(|last| SocketAddr::from(([192, 0, 2, last], 563))));
}
#[test]
fn member_selection_edge_scenarios() {
    selection_edges([1_u32, 2, 3]);
}
