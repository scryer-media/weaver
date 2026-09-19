use super::*;

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
