use super::*;

#[test]
fn leg_rebalance_recalls_idle_excess_without_interrupting_active_work() {
    let budget = SocketBudget::new(3);
    let busy = budget.try_acquire().unwrap();
    let idle = budget.try_acquire().unwrap();
    let healthy = budget.try_acquire().unwrap();
    for (slot, leg) in [(&busy, 0), (&idle, 0), (&healthy, 1)] {
        slot.set_path(Some(&weaver_tunnel::pipe::DialPath {
            leg: Some(leg),
            ..Default::default()
        }));
        slot.active();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    idle.idle(
        SocketPhase::OwnedIdle,
        Arc::new(move |id| tx.send(id).unwrap()),
    );
    budget.configure_legs(&[1, 2]);
    assert!(
        rx.try_recv().is_err(),
        "weight changes preserve idle sockets until demand"
    );
    assert!(budget.recall_idle());
    assert_eq!(rx.try_recv().unwrap(), idle.id());
    assert!(!busy.retiring());
    assert!(busy.claim_reuse());
    assert!(healthy.claim_reuse());
    assert!(
        budget.try_acquire().is_none(),
        "recall does not refund a physical slot"
    );
    drop(idle);
    let moved = budget.try_acquire().unwrap();
    moved.set_path(Some(&weaver_tunnel::pipe::DialPath {
        leg: Some(1),
        ..Default::default()
    }));
    assert_eq!(budget.snapshot().physical, 3);
    budget.configure_legs(&[0, 2]);
    assert_eq!(budget.snapshot().limit, 2);
    assert!(!busy.retiring(), "an active article must finish");
    assert!(
        !busy.claim_reuse(),
        "a down leg cannot take another article"
    );
    assert!(budget.try_acquire().is_none());
    drop(busy);
    assert_eq!(budget.snapshot().physical, 2);
}

#[test]
fn recall_filter_preserves_other_legs_and_path_metadata() {
    let budget = SocketBudget::new(2);
    let slots: Vec<_> = (0..2)
        .map(|leg| {
            let slot = budget.try_acquire().unwrap();
            slot.set_path(Some(&weaver_tunnel::pipe::DialPath {
                egress: 7,
                leg: Some(leg),
                rung: Some(2),
                pool: Some(4),
                member: Some(8),
                proxies: vec![8],
            }));
            slot
        })
        .collect();
    let (tx, rx) = std::sync::mpsc::channel();
    for slot in &slots {
        let tx = tx.clone();
        slot.idle(
            SocketPhase::AsyncIdle,
            Arc::new(move |id| tx.send(id).unwrap()),
        );
    }
    assert!(budget.recall_idle_for_leg(Some(1)));
    assert_eq!(rx.try_recv().unwrap(), slots[1].id());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        budget.state.lock().unwrap().entries[&slots[0].id()]
            .path
            .as_ref()
            .unwrap()
            .member,
        Some(8)
    );
    assert!(!budget.recall_idle_for_leg(Some(1)));
    budget.configure_legs(&[0, 0]);
    assert_eq!(budget.snapshot().limit, 0);
    assert_eq!(rx.try_recv().unwrap(), slots[0].id());
    assert!(budget.try_acquire().is_none());
}

#[test]
fn recall_is_not_a_refund_and_only_targets_one_socket() {
    let budget = SocketBudget::new(2);
    let first = budget.try_acquire().unwrap();
    let second = budget.try_acquire().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    for slot in [&first, &second] {
        let send = send.clone();
        slot.idle(
            SocketPhase::OwnedIdle,
            Arc::new(move |id| {
                send.send(id).unwrap();
            }),
        );
    }
    assert!(budget.recall_idle());
    assert_eq!(recv.try_recv().unwrap(), first.id());
    assert!(recv.try_recv().is_err());
    assert_eq!(budget.snapshot().closing, 1);
    assert!(budget.try_acquire().is_none());
    drop(first);
    let replacement = budget.try_acquire().unwrap();
    assert_eq!(budget.snapshot().physical, 2);
    drop((second, replacement));
    assert_eq!(budget.snapshot().physical, 0);
}

#[test]
fn cap_reduction_drains_without_replacement_growth() {
    let budget = SocketBudget::new(3);
    let slots: Vec<_> = (0..3).map(|_| budget.try_acquire().unwrap()).collect();
    budget.configure(1);
    assert!(!slots[0].retiring());
    assert!(slots[1].retiring());
    assert!(slots[2].retiring());
    let mut slots = slots.into_iter();
    drop(slots.next());
    assert!(budget.try_acquire().is_none());
    drop(slots.next());
    assert!(budget.try_acquire().is_none());
    drop(slots.next());
    assert!(budget.try_acquire().is_some());
}

#[tokio::test]
async fn subscribe_before_inspection_does_not_lose_return_or_release() {
    let budget = SocketBudget::new(1);
    let slot = budget.try_acquire().unwrap();
    let mut changed = budget.subscribe();
    slot.idle(SocketPhase::AsyncIdle, Arc::new(|_| {}));
    changed.changed().await.unwrap();
    drop(slot);
    changed.changed().await.unwrap();
    assert!(budget.try_acquire().is_some());
}

#[test]
fn simultaneous_dials_cannot_overbook() {
    let budget = SocketBudget::new(8);
    let gate = Arc::new(std::sync::Barrier::new(33));
    let threads: Vec<_> = (0..32)
        .map(|_| {
            let budget = Arc::clone(&budget);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let slot = budget.try_acquire();
                gate.wait();
                gate.wait();
                drop(slot);
            })
        })
        .collect();
    gate.wait();
    assert_eq!(budget.snapshot().physical, 8);
    gate.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(budget.snapshot().physical, 0);
}

#[test]
fn explicit_replacement_is_separate_and_bounded() {
    let budget = SocketBudget::new(1);
    let ordinary = budget.try_acquire().unwrap();
    assert!(budget.try_acquire().is_none());
    let replacement = budget.try_acquire_replacement().unwrap();
    assert_eq!(budget.snapshot().physical, 2);
    assert_eq!(budget.snapshot().replacement, 1);
    assert!(budget.try_acquire_replacement().is_none());
    drop(ordinary);
    assert!(budget.try_acquire().is_none());
    drop(replacement);
    assert!(budget.try_acquire().is_some());
}

#[test]
fn retirement_is_visible_without_waiting_for_the_registry() {
    let budget = SocketBudget::new(2);
    let first = budget.try_acquire().unwrap();
    let second = budget.try_acquire().unwrap();
    budget.configure(1);
    let guard = budget.state.lock().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send((first.retiring(), second.retiring())).unwrap();
        (first, second)
    });
    let result = rx.recv();
    drop(guard);
    drop(worker.join().unwrap());
    assert_eq!(result.unwrap(), (false, true));
    assert_eq!(budget.snapshot().physical, 0);
}

#[test]
fn member_retirement_recalls_idle_and_drains_busy_at_boundary() {
    for phase in [SocketPhase::AsyncIdle, SocketPhase::OwnedIdle] {
        let budget = SocketBudget::new(2);
        let idle = budget.try_acquire().unwrap();
        let busy = budget.try_acquire().unwrap();
        let outcome = Arc::new(weaver_tunnel::bridge::ConnectionOutcome::default());
        idle.observe_outcome(Some(&outcome));
        busy.observe_outcome(Some(&outcome));
        busy.active();
        let (tx, rx) = std::sync::mpsc::channel();
        let report = tx.clone();
        idle.idle(phase, Arc::new(move |id| report.send(id).unwrap()));
        outcome.retire();
        assert_eq!(rx.try_recv().unwrap(), idle.id());
        assert!(rx.try_recv().is_err());
        assert!(!busy.claim_reuse());
        assert_eq!(budget.snapshot().physical, 2);
        busy.idle(phase, Arc::new(move |id| tx.send(id).unwrap()));
        assert_eq!(rx.try_recv().unwrap(), busy.id());
        assert_eq!(budget.snapshot().closing, 2);
        outcome.retire();
        assert!(rx.try_recv().is_err(), "retirement must be idempotent");
        drop((idle, busy));
        assert_eq!(budget.snapshot().physical, 0);
    }
}

#[test]
fn a_leg_over_its_share_gives_up_exactly_the_excess_sockets() {
    let budget = SocketBudget::new(4);
    let slots: Vec<_> = (0..4).map(|_| budget.try_acquire().unwrap()).collect();
    for (slot, leg) in slots.iter().zip([0, 0, 1, 1]) {
        slot.set_path(Some(&weaver_tunnel::pipe::DialPath {
            leg: Some(leg),
            ..Default::default()
        }));
        slot.active();
    }
    budget.configure_legs(&[2, 2]);
    assert!(slots.iter().all(|slot| slot.claim_reuse()));
    // 2/2 becomes 1/3: the two lanes on leg 0 ask in the same instant and
    // only the first is the excess; the second keeps its socket.
    budget.configure_legs(&[1, 3]);
    assert!(!slots[0].claim_reuse());
    assert!(slots[1].claim_reuse());
    assert!(
        !slots[0].claim_reuse(),
        "the claim holds until the socket goes"
    );
    assert_eq!(budget.snapshot().closing, 1);
    assert!(slots[2].claim_reuse());
    assert!(slots[3].claim_reuse());
    drop(slots);
    assert_eq!(budget.snapshot().physical, 0);
}

#[test]
fn retirement_before_socket_registration_is_not_lost() {
    let budget = SocketBudget::new(1);
    let slot = budget.try_acquire().unwrap();
    let outcome = Arc::new(weaver_tunnel::bridge::ConnectionOutcome::default());
    outcome.retire();
    slot.observe_outcome(Some(&outcome));
    slot.active();
    assert!(!slot.claim_reuse());
    let (tx, rx) = std::sync::mpsc::channel();
    slot.idle(
        SocketPhase::AsyncIdle,
        Arc::new(move |id| tx.send(id).unwrap()),
    );
    assert_eq!(rx.try_recv().unwrap(), slot.id());
}
