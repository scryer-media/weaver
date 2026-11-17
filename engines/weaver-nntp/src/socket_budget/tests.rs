// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
            SocketPhase::OwnedIdle,
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
    slot.idle(SocketPhase::OwnedIdle, Arc::new(|_| {}));
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
    let phase = SocketPhase::OwnedIdle;
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
        SocketPhase::OwnedIdle,
        Arc::new(move |id| tx.send(id).unwrap()),
    );
    assert_eq!(rx.try_recv().unwrap(), slot.id());
}

#[tokio::test]
async fn a_loan_recalls_idle_sockets_and_gives_them_back_when_dropped() {
    let budget = SocketBudget::new(3);
    let (tx, rx) = std::sync::mpsc::channel();
    let mut slots: Vec<_> = (0..3).map(|_| budget.try_acquire().unwrap()).collect();
    for slot in &slots {
        slot.active();
        let tx = tx.clone();
        slot.idle(
            SocketPhase::OwnedIdle,
            Arc::new(move |id| tx.send(id).unwrap()),
        );
    }
    budget.lend(2);
    let loan = ConnectionLoan::new(Arc::clone(&budget), 2);
    assert_eq!(loan.count(), 2);
    let recalled: Vec<u64> = rx.try_iter().collect();
    assert_eq!(
        recalled.len(),
        2,
        "idle sockets above the lowered limit go at once"
    );
    assert_eq!(budget.snapshot().limit, 1);
    assert_eq!(loan.open(), 3, "closing sockets still count as open");

    let waiting = tokio::spawn({
        let loan = Arc::new(loan);
        let waiter = Arc::clone(&loan);
        async move {
            waiter.wait_open_at_most(1).await;
            loan
        }
    });
    slots.retain(|slot| !recalled.contains(&slot.id()));
    let loan = waiting.await.unwrap();
    assert_eq!(loan.open(), 1);
    assert!(
        budget.try_acquire().is_none(),
        "the lent connections stay lent"
    );

    budget.configure(4);
    assert_eq!(budget.snapshot().limit, 2, "a resize keeps the loan out");
    drop(loan);
    assert_eq!(budget.snapshot().limit, 4);
    assert!(budget.try_acquire().is_some());
}

// Bytes a tunnel asks back are given back by connections dialled over it,
// up to the number asked: idle ones close now, and for the rest busy ones
// close once their article is done, never during it. One over another path
// stays, and so does the server's last one on the tunnel.
#[test]
fn a_give_back_retires_connections_on_the_tunnel_idle_first() {
    let budget = SocketBudget::new(6);
    let (tx, rx) = std::sync::mpsc::channel();
    let busy = path_slot(&budget, &on_tunnel(), None);
    let idle: Vec<_> = (0..3)
        .map(|_| path_slot(&budget, &on_tunnel(), Some(&tx)))
        .collect();
    let other = path_slot(&budget, &elsewhere(), Some(&tx));
    let last = path_slot(&budget, &on_tunnel(), None);
    let stands = standing(true);

    assert_eq!(budget.recall_idle_on(2, &on, &stands), 2);
    let recalled: Vec<u64> = rx.try_iter().collect();
    assert_eq!(recalled.len(), 2);
    assert!(recalled.iter().all(|id| idle.iter().any(|s| s.id() == *id)));
    assert!(busy.claim_reuse(), "idle connections covered the ask");

    // Asked for more than is idle: the idle one left closes now, the busy
    // one is marked and keeps its article.
    assert_eq!(budget.recall_idle_on(5, &on, &stands), 2);
    assert_eq!(rx.try_iter().count(), 1);
    assert_eq!(budget.recall_idle_on(5, &on, &stands), 0, "asked twice");
    // Its article done, it takes no other, and closes as it goes idle.
    assert!(!busy.claim_reuse());
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    busy.idle(
        SocketPhase::OwnedIdle,
        Arc::new(move |id| busy_tx.send(id).unwrap()),
    );
    assert_eq!(busy_rx.try_recv(), Ok(busy.id()));
    assert!(other.claim_reuse(), "a connection over another path stays");
    assert!(last.claim_reuse(), "the server's last connection was given");
}

// A busy connection marked for an ask that is withdrawn before its article
// ends, as when torrents stop pressing, stays open: it takes its next
// article, and going idle does not close it.
#[test]
fn a_withdrawn_give_back_leaves_its_busy_connections_open() {
    let budget = SocketBudget::new(2);
    let busy = path_slot(&budget, &on_tunnel(), None);
    let _last = path_slot(&budget, &on_tunnel(), None);
    let held = Arc::new(AtomicBool::new(true));
    let stands: GiveBackStands = {
        let held = Arc::clone(&held);
        Arc::new(move || held.load(Ordering::Acquire))
    };
    assert_eq!(budget.recall_idle_on(1, &on, &stands), 1);
    held.store(false, Ordering::Release);
    assert!(busy.claim_reuse(), "the ask stands no more");

    // Marked again and withdrawn while it is busy, going idle keeps it.
    held.store(true, Ordering::Release);
    assert_eq!(budget.recall_idle_on(1, &on, &stands), 1);
    held.store(false, Ordering::Release);
    let (tx, rx) = std::sync::mpsc::channel();
    busy.idle(
        SocketPhase::OwnedIdle,
        Arc::new(move |id| tx.send(id).unwrap()),
    );
    assert!(
        rx.try_recv().is_err(),
        "an idle connection nobody asked for"
    );
    assert_eq!(budget.snapshot().owned_idle, 1);
}

// An ask withdrawn before usenet acts on it, as when the waiter it was made
// for is served elsewhere first, closes nothing: no idle connection closes
// and no busy one is marked.
#[test]
fn a_give_back_withdrawn_before_usenet_acts_closes_nothing() {
    let budget = SocketBudget::new(3);
    let (tx, rx) = std::sync::mpsc::channel();
    let busy = path_slot(&budget, &on_tunnel(), None);
    let _idle = path_slot(&budget, &on_tunnel(), Some(&tx));
    assert_eq!(budget.recall_idle_on(2, &on, &standing(false)), 0);
    assert!(rx.try_recv().is_err(), "an idle connection closed");
    assert!(busy.claim_reuse(), "a busy connection was marked");
    assert_eq!(budget.snapshot().physical, 2);
}

// A lowered limit and a give-back can both mark one busy connection, and
// neither clears the other: withdrawing the ask leaves the lend's retire in
// force, and taking the loan back leaves a standing ask in force.
#[test]
fn a_give_back_and_a_lowered_limit_do_not_clear_each_other() {
    // Withdrawn ask, lend still in force: the connection still closes.
    let budget = SocketBudget::new(1);
    let busy = path_slot(&budget, &on_tunnel(), None);
    let held = Arc::new(AtomicBool::new(true));
    let stands: GiveBackStands = {
        let held = Arc::clone(&held);
        Arc::new(move || held.load(Ordering::Acquire))
    };
    budget.lend(1);
    assert!(busy.retiring());
    assert_eq!(
        budget.recall_idle_on(1, &on, &stands),
        0,
        "a retiring connection is not asked twice"
    );
    held.store(false, Ordering::Release);
    assert!(!busy.claim_reuse(), "the lend's retire stands");
    drop(busy);

    // Standing ask, a loan lent and taken back meanwhile: the connection
    // still closes, for the ask.
    let budget = SocketBudget::new(3);
    let busy = path_slot(&budget, &on_tunnel(), None);
    let _last = path_slot(&budget, &on_tunnel(), None);
    let _other = path_slot(&budget, &elsewhere(), None);
    assert_eq!(budget.recall_idle_on(1, &on, &standing(true)), 1);
    budget.lend(1);
    budget.lend(0);
    assert!(!busy.retiring(), "the loan retired the other connection");
    assert!(!busy.claim_reuse(), "the give-back stands");
}

// A server whose one connection on the tunnel is all it holds there keeps
// it, idle or busy: a give-back never takes a server's last one.
#[test]
fn a_give_back_leaves_a_server_its_last_connection_on_the_tunnel() {
    let (tx, rx) = std::sync::mpsc::channel();
    let idle_budget = SocketBudget::new(2);
    let idle = path_slot(&idle_budget, &on_tunnel(), Some(&tx));
    assert_eq!(idle_budget.recall_idle_on(1, &on, &standing(true)), 0);
    assert!(rx.try_recv().is_err(), "the last idle connection closed");
    let busy_budget = SocketBudget::new(2);
    let busy = path_slot(&busy_budget, &on_tunnel(), None);
    assert_eq!(busy_budget.recall_idle_on(1, &on, &standing(true)), 0);
    assert!(busy.claim_reuse(), "the last busy connection was marked");
    drop(idle);
}

// A dial still under way is not a connection the server holds: one
// connected socket beside it is the server's last on the tunnel and is kept,
// so a dial that then fails leaves the server one, not none.
#[test]
fn a_dialing_socket_does_not_count_toward_a_servers_last_connection() {
    let (tx, rx) = std::sync::mpsc::channel();
    let budget = SocketBudget::new(2);
    let connected = path_slot(&budget, &on_tunnel(), Some(&tx));
    let dialing = budget.try_acquire().unwrap();
    dialing.set_path(Some(&on_tunnel()));
    assert_eq!(
        budget.givable_on(&on),
        1,
        "only the connected socket is held"
    );
    assert_eq!(budget.recall_idle_on(1, &on, &standing(true)), 0);
    assert!(
        rx.try_recv().is_err(),
        "the server's last connection closed"
    );
    drop(dialing);
    assert_eq!(budget.givable_on(&on), 1);
    drop(connected);
}

fn on_tunnel() -> weaver_tunnel::pipe::DialPath {
    weaver_tunnel::pipe::DialPath {
        egress: 1,
        proxies: vec![3],
        ..Default::default()
    }
}

fn elsewhere() -> weaver_tunnel::pipe::DialPath {
    weaver_tunnel::pipe::DialPath {
        egress: 1,
        proxies: vec![4],
        ..Default::default()
    }
}

fn on(path: &weaver_tunnel::pipe::DialPath) -> bool {
    path.egress == 1 && path.proxies == [3]
}

fn standing(stands: bool) -> GiveBackStands {
    Arc::new(move || stands)
}

// A connection over `path`, busy, or idle with a recall that reports to `tx`.
fn path_slot(
    budget: &Arc<SocketBudget>,
    path: &weaver_tunnel::pipe::DialPath,
    tx: Option<&std::sync::mpsc::Sender<u64>>,
) -> SocketSlot {
    let slot = budget.try_acquire().unwrap();
    slot.set_path(Some(path));
    slot.active();
    if let Some(tx) = tx {
        let tx = tx.clone();
        slot.idle(
            SocketPhase::OwnedIdle,
            Arc::new(move |id| tx.send(id).unwrap()),
        );
    }
    slot
}
