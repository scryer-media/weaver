use super::*;

fn ready(health: &mut ServerHealth) {
    let ServerState::Disabled { until, .. } = &mut health.state else {
        panic!("must be quarantined");
    };
    *until = Instant::now();
    health.recovery.expire_now();
    health.check_reenable();
}

fn deadline(health: &ServerHealth) -> Instant {
    let ServerState::Disabled { until, .. } = health.state else {
        panic!("must be quarantined");
    };
    until
}

#[test]
fn hundred_socket_failure_wave_has_one_episode_and_one_fresh_probe() {
    let mut health = ServerHealth::new(HealthConfig::default());
    let gate = health.recovery.clone();
    let old: Vec<_> = (0..101).map(|_| gate.admit(true).unwrap()).collect();
    for lease in &old[..10] {
        health.record_connection_outcome(&lease.0, false, false, false);
    }
    let until = deadline(&health);
    assert!(until.duration_since(Instant::now()) <= Duration::from_secs(30));
    for lease in &old[10..100] {
        health.record_connection_outcome(&lease.0, false, false, false);
    }
    assert_eq!(health.failure_count, 100);
    assert_eq!(health.disable_count, 1);
    assert_eq!(deadline(&health), until);
    assert!(gate.admit(true).is_none());
    ready(&mut health);
    assert!(
        gate.admit(false).is_none(),
        "warmers and metadata cannot own recovery"
    );
    let probe = gate.admit(true).unwrap();
    assert!(gate.admit(true).is_none());
    // An old successful or failed socket cannot settle or restart this probe.
    health.record_connection_outcome(&old[100].0, true, false, false);
    health.record_connection_outcome(&old[100].0, false, false, false);
    assert!(probe.0.probing());
    health.record_connection_outcome(&probe.0, true, false, false);
    assert_eq!(health.state(), &ServerState::Healthy);
    assert_eq!(health.recovery_attempts, 0);
    assert!(gate.admit(true).is_some());
}

#[test]
fn pipeline_replay_counts_one_terminal_socket_failure() {
    let mut health = ServerHealth::new(HealthConfig::default());
    let lease = health.recovery.admit(true).unwrap();
    for _ in 0..100 {
        health.record_connection_outcome(&lease.0, false, false, false);
    }
    assert_eq!(health.failure_count, 1);
    assert_eq!(health.consecutive_failures, 1);
}

#[test]
fn failed_probe_is_bounded_and_cancellation_is_not_failure() {
    let mut health = ServerHealth::new(HealthConfig {
        disable_threshold: 1,
        ..HealthConfig::default()
    });
    let gate = health.recovery.clone();
    let old = gate.admit(true).unwrap();
    health.record_connection_outcome(&old.0, false, false, false);
    ready(&mut health);
    let canceled = gate.admit(true).unwrap();
    let late = canceled.0.clone();
    drop(canceled);
    let probe = gate.admit(true).unwrap();
    health.record_connection_outcome(&late, true, false, false);
    assert!(probe.0.probing());
    assert_eq!(health.failure_count, 1);
    health.record_connection_outcome(&probe.0, false, false, false);
    let wait = deadline(&health).duration_since(Instant::now());
    assert!(wait <= Duration::from_secs(60) && wait > Duration::from_secs(59));
    for _ in 0..8 {
        ready(&mut health);
        let probe = gate.admit(true).unwrap();
        health.record_connection_outcome(&probe.0, false, false, false);
        assert!(deadline(&health).duration_since(Instant::now()) <= Duration::from_secs(60));
    }
    ready(&mut health);
    let probe = gate.admit(true).unwrap();
    health.record_connection_outcome(&probe.0, true, false, false);
    let fresh = gate.admit(true).unwrap();
    health.record_connection_outcome(&fresh.0, false, false, false);
    assert!(deadline(&health).duration_since(Instant::now()) <= Duration::from_secs(30));
}

#[test]
fn a_failed_probe_does_not_depend_on_a_health_reenable_poll() {
    let mut health = ServerHealth::new(HealthConfig {
        disable_threshold: 1,
        ..HealthConfig::default()
    });
    let gate = health.recovery.clone();
    let old = gate.admit(true).unwrap();
    health.record_connection_outcome(&old.0, false, false, false);
    gate.expire_now();
    let probe = gate.admit(true).unwrap();
    assert!(matches!(health.state(), ServerState::Disabled { .. }));
    let snapshot = gate.snapshot();
    assert!(
        gate.quarantine(Duration::from_secs(60), Some(old.0.event_key()))
            .is_none()
    );
    assert_eq!(gate.snapshot().epoch, snapshot.epoch);
    assert!(!old.0.complete_recovery());
    health.record_connection_outcome(&probe.0, false, false, false);
    assert_eq!(health.disable_count, 2);
    assert!(deadline(&health).duration_since(Instant::now()) > Duration::from_secs(59));
    assert!(gate.admit(true).is_none());
}
