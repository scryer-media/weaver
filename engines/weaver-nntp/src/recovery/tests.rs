use super::*;

#[test]
fn canceled_probes_remain_stale_after_a_later_probe_recovers() {
    let gate = Arc::new(RecoveryGate::default());
    let old = gate.admit(true).unwrap();
    gate.quarantine(Duration::ZERO, Some(old.0.event_key()))
        .unwrap();
    assert!(!old.0.current());
    let canceled = gate.admit(true).unwrap();
    let late = Arc::clone(&canceled.0);
    drop(canceled);
    assert!(!late.current());
    let probe = gate.admit(true).unwrap();
    assert!(probe.0.current() && probe.0.probing());
    assert!(probe.0.complete_recovery());
    assert!(!late.current() && !old.0.current());
    assert!(
        gate.quarantine(Duration::ZERO, Some(late.event_key()))
            .is_none()
    );
    assert!(probe.0.current() && !probe.0.probing());
    let fresh = gate.admit(true).unwrap();
    assert!(fresh.0.current());
    gate.quarantine(Duration::ZERO, Some(fresh.0.event_key()))
        .unwrap();
    assert!(!probe.0.current() && !fresh.0.current());
}

#[test]
fn healthy_and_recovered_connections_do_not_wait_for_the_policy_mutex() {
    let gate = Arc::new(RecoveryGate::default());
    let old = gate.admit(true).unwrap();
    gate.quarantine(Duration::ZERO, Some(old.0.event_key()))
        .unwrap();
    let recovered = gate.admit(true).unwrap();
    assert!(recovered.0.complete_recovery());
    let healthy = gate.admit(true).unwrap();
    let guard = gate.state.lock().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        for lease in [&healthy, &recovered] {
            assert!(lease.0.current());
            assert!(!lease.0.probing());
            assert!(!lease.0.complete_recovery());
        }
        assert!(!healthy.0.gate.quarantined());
        tx.send(()).unwrap();
        (healthy, recovered)
    });
    let result = rx.recv_timeout(Duration::from_secs(1));
    drop(guard);
    drop(worker.join().unwrap());
    result.expect("healthy article checks must not wait for policy updates");
}
