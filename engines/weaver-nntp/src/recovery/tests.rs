// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
    let result = rx.recv();
    drop(guard);
    drop(worker.join().unwrap());
    result.expect("healthy article checks must not wait for policy updates");
}

const GIVE_UP: Duration = Duration::from_secs(3600);
const DISABLE: Duration = Duration::from_secs(300);

// The server refuses the login at the probe `gate` admits now, read at `at`.
fn refused_probe(gate: &Arc<RecoveryGate>, at: Instant) -> LoginRefusalRun {
    gate.set_refusal_clock(at);
    gate.expire_now();
    let probe = gate
        .admit(true)
        .expect("the lapsed quarantine admits its probe");
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, Some(probe.0.event_key()))
        .expect("the probe's refusal is current")
        .1
}

// A refused login waits for the give-up time: refused again at each probe the
// run goes on, and only once it is that old does the gate answer that the
// server refuses the credentials.
#[test]
fn a_refused_login_is_a_verdict_only_once_the_run_is_its_give_up_time_old() {
    let gate = Arc::new(RecoveryGate::default());
    let start = Instant::now();
    gate.set_refusal_clock(start);
    let (_, run) = gate
        .quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    assert_eq!(run, LoginRefusalRun::Began);
    assert!(!gate.refuses_credentials(false), "a fresh refusal waits");
    assert!(
        gate.refuses_credentials(true),
        "unless it is answered at once"
    );
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP / 2),
        LoginRefusalRun::Waiting
    );
    assert!(!gate.refuses_credentials(false));
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP - Duration::from_secs(1)),
        LoginRefusalRun::Waiting
    );
    assert!(
        !gate.refuses_credentials(false),
        "the run is one second short"
    );
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP),
        LoginRefusalRun::Waiting
    );
    assert!(gate.refuses_credentials(false));
    // Once given up, later refusals stay the verdict.
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP + DISABLE),
        LoginRefusalRun::Waiting
    );
    assert!(gate.refuses_credentials(false));
    // Past the quarantine's deadline the probe is due, and waits.
    gate.expire_now();
    assert!(!gate.refuses_credentials(false));
}

// A login that succeeds ends the run: a later refusal starts a new one, and
// waits its whole give-up time again.
#[test]
fn a_successful_login_starts_the_next_refusal_run_afresh() {
    let gate = Arc::new(RecoveryGate::default());
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    gate.set_refusal_clock(start + GIVE_UP / 2);
    gate.expire_now();
    let probe = gate.admit(true).unwrap();
    assert!(probe.0.complete_recovery());
    drop(probe);
    let (_, run) = gate
        .quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    assert_eq!(run, LoginRefusalRun::Began);
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP),
        LoginRefusalRun::Waiting
    );
    assert!(
        !gate.refuses_credentials(false),
        "the new run is half its give-up time old"
    );
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP / 2 + GIVE_UP),
        LoginRefusalRun::Waiting
    );
    assert!(gate.refuses_credentials(false));
}

// A quarantine for another cause in between is a wait, not a refusal, and
// does not end the run: the refusal after it carries the same run on, which
// still gives up at its first refusal's give-up time. The work the server
// alone could serve keeps waiting through it, to the same instant.
#[test]
fn a_transport_quarantine_between_refusals_leaves_the_run_as_it_is() {
    let gate = Arc::new(RecoveryGate::default());
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    gate.set_refusal_clock(start + GIVE_UP / 2);
    gate.expire_now();
    let probe = gate.admit(true).unwrap();
    gate.quarantine(DISABLE, Some(probe.0.event_key())).unwrap();
    drop(probe);
    assert_eq!(gate.refusal_verdict_at(), Some(start + GIVE_UP));
    assert!(
        !gate.refuses_credentials(true),
        "an outage is never a refusal"
    );
    assert_eq!(
        gate.login_refusal_wait_ends(),
        Some(start + GIVE_UP),
        "an outage inside the run is waited through to the run's verdict"
    );
    // Past the run's give-up time, a quarantine of another kind is still a
    // wait.
    gate.set_refusal_clock(start + GIVE_UP);
    assert!(!gate.refuses_credentials(false));
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP),
        LoginRefusalRun::Waiting
    );
    assert!(gate.refuses_credentials(false));
}

// While a refusal is a wait the gate says when it becomes the verdict; once
// it is the verdict, it says nothing.
#[test]
fn a_waiting_refusal_says_when_it_becomes_the_verdict() {
    let gate = Arc::new(RecoveryGate::default());
    assert_eq!(gate.login_refusal_wait_ends(), None);
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    assert_eq!(gate.login_refusal_wait_ends(), Some(start + GIVE_UP));
    gate.set_refusal_clock(start + GIVE_UP);
    assert_eq!(gate.login_refusal_wait_ends(), None);
}

// A pool built from another login identity changes nothing until it is the
// pool in use: the pool still in use holds the old password and must not be
// handed a probe meant for the new one. Put in use, it ends the run and its
// probe is due at once. A pool rebuilt from the same identity changes
// nothing either way.
#[test]
fn only_a_pool_put_in_use_with_another_login_identity_ends_the_refusal_run() {
    let gate = Arc::new(RecoveryGate::default());
    gate.built(1, false);
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    let mut heard = gate.subscribe();
    heard.mark_unchanged();
    gate.built(1, false);
    assert!(!gate.activated(1, false));
    assert!(
        !heard.has_changed().unwrap(),
        "a pool rebuilt from the same identity changes nothing"
    );
    assert_eq!(gate.refusal_verdict_at(), Some(start + GIVE_UP));

    gate.built(2, false);
    assert!(!heard.has_changed().unwrap());
    assert_eq!(
        gate.refusal_verdict_at(),
        Some(start + GIVE_UP),
        "building a pool ends nothing"
    );
    assert!(
        matches!(gate.hold(true), Some(ConnectionHold::Until(_))),
        "the pool still in use is not handed the probe"
    );
    assert!(gate.admit(true).is_none());

    assert!(gate.activated(2, false));
    assert!(heard.has_changed().unwrap(), "a waiting lane hears it");
    assert_eq!(gate.refusal_verdict_at(), None);
    assert_eq!(gate.hold(true), Some(ConnectionHold::Probe));
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP),
        LoginRefusalRun::Began
    );
    assert!(!gate.refuses_credentials(false));
}

// The pool of the old identity had the probe out when the pool of the new
// one came into use. Whatever that probe answers is about the old password:
// its refusal neither begins a run nor quarantines the server afresh, and
// its success does not lift the quarantine. The new pool's probe is due.
#[test]
fn a_probe_of_the_old_login_identity_says_nothing_once_the_new_pool_is_in_use() {
    let gate = Arc::new(RecoveryGate::default());
    gate.built(1, false);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    gate.expire_now();
    let old_probe = gate.admit(true).expect("the old pool takes the probe");
    gate.built(2, false);
    assert!(gate.activated(2, false));
    assert!(
        gate.quarantine_for_credentials(DISABLE, GIVE_UP, Some(old_probe.0.event_key()))
            .is_none(),
        "the old password's refusal is dropped"
    );
    assert_eq!(gate.refusal_verdict_at(), None, "no run began");
    assert!(!old_probe.0.complete_recovery());
    assert!(!old_probe.0.current());
    assert!(gate.quarantined());
    assert_eq!(gate.hold(true), Some(ConnectionHold::Probe));
    drop(old_probe);
    assert_eq!(
        gate.hold(true),
        Some(ConnectionHold::Probe),
        "letting the old probe go takes nothing from the new pool"
    );
}

// A server whose configured role changes between backfill and fill ends its
// run of login refusals and is asked again at once; a pool put in use with
// the server in the same role keeps the run and the quarantine's deadline.
#[test]
fn a_change_of_the_configured_role_ends_the_refusal_run_and_makes_the_probe_due() {
    let gate = Arc::new(RecoveryGate::default());
    gate.built(1, true);
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    assert!(!gate.activated(1, true));
    assert_eq!(gate.refusal_verdict_at(), Some(start + GIVE_UP));
    assert!(matches!(gate.hold(true), Some(ConnectionHold::Until(_))));
    assert!(gate.activated(1, false));
    assert_eq!(gate.refusal_verdict_at(), None);
    assert_eq!(
        gate.hold(true),
        Some(ConnectionHold::Probe),
        "the server is asked again at once in its new role"
    );
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP),
        LoginRefusalRun::Began,
        "the fill server's first refusal begins a run of its own"
    );
    assert!(gate.activated(1, true));
    assert_eq!(gate.refusal_verdict_at(), None, "and back again");
    assert_eq!(gate.hold(true), Some(ConnectionHold::Probe));
}

// The run's give-up time passes with no probe out. The verdict is due at
// that instant, is found untold while the server refuses, is marked told
// once and heard by the gate's watchers, and a probe refused afterwards
// does not make it untold again.
#[test]
fn the_verdict_instant_is_told_once_with_no_probe_to_tell_it() {
    let gate = Arc::new(RecoveryGate::default());
    assert_eq!(gate.login_refusal_verdict_due(), None);
    assert_eq!(gate.login_refusal_verdict_untold(), None);
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    assert_eq!(gate.login_refusal_verdict_due(), Some(start + GIVE_UP));
    assert_eq!(
        gate.login_refusal_verdict_untold(),
        None,
        "the run still waits"
    );
    let mut heard = gate.subscribe();
    heard.mark_unchanged();
    gate.set_refusal_clock(start + GIVE_UP);
    assert!(gate.refuses_credentials(false));
    assert_eq!(gate.login_refusal_verdict_due(), Some(start + GIVE_UP));
    let untold = gate.login_refusal_verdict_untold().expect("untold");
    assert!(
        !heard.has_changed().unwrap(),
        "finding the verdict tells nothing"
    );
    assert!(gate.mark_login_refusal_verdict_told(untold));
    assert!(heard.has_changed().unwrap());
    assert!(!gate.mark_login_refusal_verdict_told(untold), "told once");
    assert_eq!(gate.login_refusal_verdict_untold(), None);
    assert_eq!(gate.login_refusal_verdict_due(), None);
    assert_eq!(
        refused_probe(&gate, start + GIVE_UP + DISABLE),
        LoginRefusalRun::Waiting
    );
    assert_eq!(
        gate.login_refusal_verdict_untold(),
        None,
        "a later refused probe does not make it untold again"
    );
    assert_eq!(gate.login_refusal_verdict_due(), None);
}

// The give-up time passes while a transport quarantine holds the server:
// the server is waited for as through an outage, so no verdict is due or
// found then. The next refused login leaves it due at once and untold.
#[test]
fn a_verdict_instant_passed_under_a_transport_quarantine_waits_for_a_refused_login() {
    let gate = Arc::new(RecoveryGate::default());
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    gate.quarantine(DISABLE, None).unwrap();
    gate.set_refusal_clock(start + GIVE_UP);
    assert_eq!(gate.login_refusal_verdict_due(), None);
    assert_eq!(gate.login_refusal_verdict_untold(), None);
    let mut heard = gate.subscribe();
    heard.mark_unchanged();
    refused_probe(&gate, start + GIVE_UP + Duration::from_secs(180));
    assert!(heard.has_changed().unwrap(), "the refusal is heard");
    assert_eq!(
        gate.login_refusal_verdict_due(),
        Some(start + GIVE_UP),
        "due since the instant already passed"
    );
    let untold = gate.login_refusal_verdict_untold().expect("untold");
    assert!(gate.mark_login_refusal_verdict_told(untold));
}

// The give-up time passes when the quarantine's deadline already has: the
// probe is due or out, the server does not refuse just then, and nothing
// is due, found or marked. The probe's refusal leaves the verdict untold.
#[test]
fn a_verdict_instant_passed_with_the_probe_due_is_not_told_until_the_server_refuses() {
    let gate = Arc::new(RecoveryGate::default());
    let start = Instant::now();
    gate.set_refusal_clock(start);
    gate.quarantine_for_credentials(DISABLE, GIVE_UP, None)
        .unwrap();
    gate.set_refusal_clock(start + GIVE_UP);
    let untold = gate.login_refusal_verdict_untold().expect("untold");
    gate.expire_now();
    assert!(!gate.refuses_credentials(false));
    assert_eq!(gate.login_refusal_verdict_due(), None);
    assert_eq!(gate.login_refusal_verdict_untold(), None);
    assert!(
        !gate.mark_login_refusal_verdict_told(untold),
        "a verdict found before the deadline passed is not marked after it"
    );
    refused_probe(&gate, start + GIVE_UP);
    assert_eq!(gate.login_refusal_verdict_due(), Some(start + GIVE_UP));
    assert!(
        !gate.mark_login_refusal_verdict_told(untold),
        "nor is one found before the server was quarantined afresh"
    );
    let untold = gate.login_refusal_verdict_untold().expect("still untold");
    assert!(gate.mark_login_refusal_verdict_told(untold));
}

// The login identity's fingerprint never reaches a Debug dump of the gate.
#[test]
fn the_login_fingerprint_stays_out_of_debug_output() {
    let gate = RecoveryGate::default();
    gate.built(0x5eed_cafe_f00d_beef, false);
    let dump = format!("{gate:?}");
    assert!(dump.contains("LoginFingerprint(..)"), "{dump}");
    for shown in [
        0x5eed_cafe_f00d_beef_u64.to_string(),
        format!("{:x}", 0x5eed_cafe_f00d_beef_u64),
    ] {
        assert!(!dump.contains(&shown), "{dump}");
    }
}
