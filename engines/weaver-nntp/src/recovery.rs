// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Connection-scoped recovery admission and stale-outcome fencing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    next_id: u64,
    quarantine: bool,
    deadline: Option<Instant>,
    probe: Option<u64>,
    // The quarantine is the server refusing this pool's credentials.
    credentials: bool,
    // When the present run of login refusals becomes a verdict: set by the
    // first refusal of a run and kept across the probes refused again and
    // any quarantine of another kind between them. Only a login that
    // succeeds, an edit of the server's login identity or a change of its
    // configured role ends the run.
    refusal_verdict_at: Option<Instant>,
    // The run's verdict has been told: set in one place only,
    // [`RecoveryGate::mark_login_refusal_verdict_told`].
    refusal_verdict_reported: bool,
    // The server as the pool in use was built: a fingerprint of its login
    // identity (host, port, username, password) and whether it is
    // configured as backfill. See [`RecoveryGate::activated`].
    setup: Option<(LoginFingerprint, bool)>,
    // Stands in for the clock the refusal run is read against, in tests.
    #[cfg(any(test, feature = "test-support"))]
    refusal_clock: Option<Instant>,
}

// A fingerprint of a server's login identity. Its Debug prints nothing of
// it, so no dump of the gate carries anything derived from the password.
#[derive(Clone, Copy, PartialEq, Eq)]
struct LoginFingerprint(u64);

impl std::fmt::Debug for LoginFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LoginFingerprint(..)")
    }
}

impl State {
    // The quarantine's deadline has passed: its one probe is due now.
    fn expire_deadline(&mut self) {
        self.deadline = Some(Instant::now());
    }

    // The instant a login refusal run is measured at.
    fn refusal_now(&self) -> Instant {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(now) = self.refusal_clock {
            return now;
        }
        Instant::now()
    }

    // Whether the present run of login refusals has lasted its give-up time.
    fn refusal_settled(&self) -> bool {
        self.refusal_verdict_at
            .is_some_and(|at| at <= self.refusal_now())
    }

    // The quarantine is a refusal of the credentials and still runs.
    fn refuses_now(&self) -> bool {
        self.quarantine && self.credentials && self.deadline.is_some_and(|at| at > Instant::now())
    }

    // What holds a new connection back, if anything. The one answer both the
    // admission below and a dispatcher's choice of server read: a quarantine
    // holds every connection until its deadline, then admits one demanded
    // probe, and holds everything else until that probe answers or is given
    // back.
    fn hold(&self, demanded: bool, now: Instant) -> Option<ConnectionHold> {
        if !self.quarantine {
            return None;
        }
        if !demanded || self.probe.is_some() {
            return Some(ConnectionHold::Recovery);
        }
        match self.deadline {
            Some(at) if at > now => Some(ConnectionHold::Until(at)),
            _ => Some(ConnectionHold::Probe),
        }
    }
}

// Where a refusal of this pool's credentials leaves the server's run of
// refusals: the run began with it, or goes on. The run's verdict is not
// told from here; see [`RecoveryGate::login_refusal_verdict_untold`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoginRefusalRun {
    Began,
    Waiting,
}

// A server's run of login refusals found to be an untold verdict, to be
// marked told after the work that waited on it is weighed; see
// [`RecoveryGate::login_refusal_verdict_untold`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UntoldVerdict {
    epoch: u64,
}

// Why a new connection to a server is held back, and what ends the hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionHold {
    // The hold lifts by itself at this instant.
    Until(Instant),
    // The quarantine's deadline has passed and its one probe is due: exactly
    // one new demanded connection is admitted, and it is the probe.
    Probe,
    // The hold lifts only when the server's recovery changes: its probe
    // answers, or the probe is given back unanswered.
    Recovery,
}

#[derive(Debug)]
pub(crate) struct RecoveryGate {
    state: Mutex<State>,
    // Only admission/retirement changes this frontier. Live connections at or
    // above it may issue work without locking the recovery policy.
    first_current_id: AtomicU64,
    quarantined: AtomicBool,
    changed: tokio::sync::watch::Sender<u64>,
    // Pool-wide watches that also hear this gate's changes. A gate can
    // outlive the pool that registered one, so they are held weakly and a
    // dropped one is let go on the next change.
    listeners: Mutex<Vec<Weak<tokio::sync::watch::Sender<u64>>>>,
}

impl Default for RecoveryGate {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            first_current_id: AtomicU64::default(),
            quarantined: AtomicBool::default(),
            changed: tokio::sync::watch::channel(0).0,
            listeners: Mutex::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecoverySnapshot {
    pub epoch: u64,
    pub probe_id: Option<u64>,
    pub remaining: Option<Duration>,
    pub quarantined: bool,
}

impl RecoveryGate {
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }
    // Have `listener` hear every later change to this gate as well.
    pub(crate) fn add_listener(&self, listener: Weak<tokio::sync::watch::Sender<u64>>) {
        self.listeners
            .lock()
            .expect("recovery gate listeners poisoned")
            .push(listener);
    }

    fn publish(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        self.listeners
            .lock()
            .expect("recovery gate listeners poisoned")
            .retain(|listener| {
                let Some(listener) = listener.upgrade() else {
                    return false;
                };
                listener.send_modify(|revision| *revision = revision.wrapping_add(1));
                true
            });
    }
    pub(crate) fn snapshot(&self) -> RecoverySnapshot {
        let state = self.state.lock().expect("recovery gate poisoned");
        RecoverySnapshot {
            epoch: state.epoch,
            probe_id: state.probe,
            remaining: state
                .deadline
                .map(|at| at.saturating_duration_since(Instant::now())),
            quarantined: state.quarantine,
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn expire_now(&self) {
        self.state
            .lock()
            .expect("recovery gate poisoned")
            .expire_deadline();
    }

    pub(crate) fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Acquire)
    }

    pub(crate) fn quarantine(
        &self,
        duration: Duration,
        event: Option<(u64, u64)>,
    ) -> Option<Instant> {
        self.quarantine_as(duration, event, None)
            .map(|(until, _)| until)
    }

    // A quarantine for a server that refused this pool's credentials. The
    // refusal is a wait like any other quarantine until the server has
    // refused the login for `give_up` without a break; from then on it is
    // an answer for the quarantine's whole length: see
    // [`Self::refuses_credentials`]. Says where the refusal leaves the run.
    pub(crate) fn quarantine_for_credentials(
        &self,
        duration: Duration,
        give_up: Duration,
        event: Option<(u64, u64)>,
    ) -> Option<(Instant, LoginRefusalRun)> {
        self.quarantine_as(duration, event, Some(give_up))
            .map(|(until, run)| (until, run.unwrap_or(LoginRefusalRun::Waiting)))
    }

    fn quarantine_as(
        &self,
        duration: Duration,
        event: Option<(u64, u64)>,
        credentials_give_up: Option<Duration>,
    ) -> Option<(Instant, Option<LoginRefusalRun>)> {
        let mut state = self.state.lock().expect("recovery gate poisoned");
        if event.is_some_and(|(epoch, id)| {
            epoch != state.epoch
                || id < self.first_current_id.load(Ordering::Relaxed)
                || (state.quarantine && state.probe != Some(id))
        }) {
            return None;
        }
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("recovery epoch exhausted");
        state.quarantine = true;
        state.credentials = credentials_give_up.is_some();
        let run = match credentials_give_up {
            // A failure of another kind leaves the run where it is: while it
            // holds, `credentials` is false and the server is waited for, and
            // the next refused login carries the same run on.
            None => None,
            Some(give_up) => {
                let began = state.refusal_verdict_at.is_none();
                if began {
                    let now = state.refusal_now();
                    state.refusal_verdict_at = Some(now.checked_add(give_up).unwrap_or(now));
                    state.refusal_verdict_reported = false;
                }
                Some(if began {
                    LoginRefusalRun::Began
                } else {
                    LoginRefusalRun::Waiting
                })
            }
        };
        let until = Instant::now() + duration;
        state.deadline = Some(until);
        state.probe = None;
        self.first_current_id.store(
            state
                .next_id
                .checked_add(1)
                .expect("recovery identity exhausted"),
            Ordering::Release,
        );
        self.quarantined.store(true, Ordering::Release);
        self.publish();
        Some((until, run))
    }

    // Whether the server refused this pool's credentials, has refused them
    // without a break for the run's give-up time (at once when `at_once`),
    // and its quarantine still runs. Until the give-up time the refusal is a
    // wait like any other quarantine: the server may yet take the login.
    // Past it, until the deadline nothing it could be asked would get a
    // different answer, so the work it would serve is not waiting for it.
    // Past the deadline the one probe is due and this answers no again: a
    // probe out on a refusing server is a wait like any other.
    pub(crate) fn refuses_credentials(&self, at_once: bool) -> bool {
        if !self.quarantined() {
            return false;
        }
        let state = self.state.lock().expect("recovery gate poisoned");
        state.refuses_now() && (at_once || state.refusal_settled())
    }

    // A pool was built over this server with this login identity, as a
    // fingerprint, and this configured role. The first pool ever built is
    // the one in use and is what later pools are compared with; a later
    // one changes nothing until it is the pool in use
    // ([`Self::activated`]), so the pool still in use cannot spend a probe
    // meant for a login identity it was not built from.
    pub(crate) fn built(&self, identity: u64, backfill: bool) {
        self.state
            .lock()
            .expect("recovery gate poisoned")
            .setup
            .get_or_insert((LoginFingerprint(identity), backfill));
    }

    // The pool built over this server with this login identity and this
    // configured role is now the one in use. Says whether a run of login
    // refusals was ended.
    //
    // A login identity other than the last pool's ends the run: the next
    // refusal is of different credentials and starts a run of its own.
    // Nothing the last pool has out may speak for the new identity, so the
    // epoch moves on and its probe is let go: an outcome of a connection it
    // admitted is dropped, a refusal of the old password among them.
    //
    // A server whose configured role changed between backfill and fill
    // ends its run too: a backfill server's refusal is answered at once and
    // a fill server's is waited for, so the run of one says nothing of the
    // other. Only the configured role counts. Whether the server is held as
    // backfill in effect also follows which other servers are switched on,
    // and a run that ended whenever those changed might never reach its
    // verdict.
    //
    // Either change makes a running quarantine's probe due now, so the
    // server is asked again at once and not when a backoff earned under
    // the old setup ends. A pool rebuilt over the same server with the same
    // login identity and role changes nothing.
    pub(crate) fn activated(&self, identity: u64, backfill: bool) -> bool {
        let setup = (LoginFingerprint(identity), backfill);
        let mut state = self.state.lock().expect("recovery gate poisoned");
        let Some(previous) = state.setup.replace(setup) else {
            return false;
        };
        if previous == setup {
            return false;
        }
        let ended = state.refusal_verdict_at.take().is_some();
        state.refusal_verdict_reported = false;
        if previous.0 != setup.0 {
            state.epoch = state
                .epoch
                .checked_add(1)
                .expect("recovery epoch exhausted");
            state.probe = None;
            self.first_current_id.store(
                state
                    .next_id
                    .checked_add(1)
                    .expect("recovery identity exhausted"),
                Ordering::Release,
            );
        }
        if state.quarantine {
            state.expire_deadline();
        }
        self.publish();
        ended
    }

    // The instant the loop that weighs waiting work is to look at this
    // server's run of login refusals: the instant the run becomes a
    // verdict, which no connection outcome announces, or, already past,
    // the instant it became one nobody has been told of while the server
    // refuses now. None once it is told. None too while the run is past
    // its give-up time and the server does not refuse now (a quarantine of
    // another kind holds it, or its probe is due or out): the next refused
    // login makes it refuse again, and every watcher of the gate hears
    // that.
    pub(crate) fn login_refusal_verdict_due(&self) -> Option<Instant> {
        if !self.quarantined() {
            return None;
        }
        let state = self.state.lock().expect("recovery gate poisoned");
        if !state.quarantine || state.refusal_verdict_reported {
            return None;
        }
        if state.refusal_settled() && !state.refuses_now() {
            return None;
        }
        state.refusal_verdict_at
    }

    // The server's run of login refusals is a verdict nobody has been told
    // of, and the server refuses now: the work that waited on it can be
    // weighed, and the verdict then marked told with what this returns.
    pub(crate) fn login_refusal_verdict_untold(&self) -> Option<UntoldVerdict> {
        if !self.quarantined() {
            return None;
        }
        let state = self.state.lock().expect("recovery gate poisoned");
        (state.quarantine
            && !state.refusal_verdict_reported
            && state.refusal_settled()
            && state.refuses_now())
        .then_some(UntoldVerdict { epoch: state.epoch })
    }

    // Marks the verdict found by [`Self::login_refusal_verdict_untold`]
    // told, once: the only place a run's verdict becomes told. True when
    // the server has refused without a break since the verdict was found,
    // so everything weighed in between was weighed against a server that
    // refused. False leaves the verdict untold, to be found again.
    pub(crate) fn mark_login_refusal_verdict_told(&self, untold: UntoldVerdict) -> bool {
        let mut state = self.state.lock().expect("recovery gate poisoned");
        if state.epoch != untold.epoch
            || !state.quarantine
            || state.refusal_verdict_reported
            || !state.refusal_settled()
            || !state.refuses_now()
        {
            return false;
        }
        state.refusal_verdict_reported = true;
        self.publish();
        true
    }

    // When the server's present run of login refusals becomes the verdict,
    // while the server is quarantined, of whatever kind, inside a run that
    // has not reached it: until then the work only it could serve waits for
    // it. A transport quarantine between two refusals keeps the wait, as it
    // keeps the run. Lock-free on a server that is not quarantined.
    pub(crate) fn login_refusal_wait_ends(&self) -> Option<Instant> {
        if !self.quarantined() {
            return None;
        }
        let state = self.state.lock().expect("recovery gate poisoned");
        if !state.quarantine || state.refusal_settled() {
            return None;
        }
        state.refusal_verdict_at
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn refusal_verdict_at(&self) -> Option<Instant> {
        self.state
            .lock()
            .expect("recovery gate poisoned")
            .refusal_verdict_at
    }

    // Read the login refusal run against `now` instead of the clock.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn set_refusal_clock(&self, now: Instant) {
        self.state
            .lock()
            .expect("recovery gate poisoned")
            .refusal_clock = Some(now);
    }

    // What holds a new connection to this server back. Lock-free on a server
    // that is not quarantined.
    pub(crate) fn hold(&self, demanded: bool) -> Option<ConnectionHold> {
        if !self.quarantined() {
            return None;
        }
        self.state
            .lock()
            .expect("recovery gate poisoned")
            .hold(demanded, Instant::now())
    }

    pub(crate) fn admit(self: &Arc<Self>, demanded: bool) -> Option<ConnectionHealthLease> {
        let mut state = self.state.lock().expect("recovery gate poisoned");
        if !matches!(
            state.hold(demanded, Instant::now()),
            None | Some(ConnectionHold::Probe)
        ) {
            return None;
        }
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("recovery identity exhausted");
        let id = state.next_id;
        let probe = state.quarantine;
        if probe {
            // Announced by the caller once the probe has what it dials with:
            // see [`ConnectionHealthLease::announce`].
            state.probe = Some(id);
        }
        Some(ConnectionHealthLease(Arc::new(ConnectionHealth {
            gate: Arc::clone(self),
            epoch: state.epoch,
            id,
            probe: AtomicBool::new(probe),
            failed: AtomicBool::new(false),
        })))
    }
}

#[derive(Debug)]
pub struct ConnectionHealth {
    gate: Arc<RecoveryGate>,
    epoch: u64,
    id: u64,
    probe: AtomicBool,
    failed: AtomicBool,
}

impl ConnectionHealth {
    pub(crate) fn event_key(&self) -> (u64, u64) {
        (self.epoch, self.id)
    }

    pub(crate) fn complete_recovery(&self) -> bool {
        if !self.probe.load(Ordering::Acquire) {
            return false;
        }
        let mut state = self.gate.state.lock().expect("recovery gate poisoned");
        if self.epoch != state.epoch || state.probe != Some(self.id) {
            return false;
        }
        state.quarantine = false;
        state.credentials = false;
        state.refusal_verdict_at = None;
        state.refusal_verdict_reported = false;
        state.deadline = None;
        state.probe = None;
        self.probe.store(false, Ordering::Release);
        self.gate.quarantined.store(false, Ordering::Release);
        self.gate.publish();
        true
    }

    // The connection's demanded command was answered with a 2xx status line:
    // a BODY's 222, or, on a connection that only asks whether articles
    // exist, a STAT's 223 or a HEAD's 221. A 430 is no such answer.
    // For the quarantine's probe that answer is the server's recovery: the
    // gate lifts here, not when the transfer behind the answer finishes, so a
    // long BODY on a slow server does not hold every other connection back.
    // The health tracker reads a lifted gate as the recovery when the
    // connection's outcome is recorded. Any other connection's answer changes
    // nothing.
    pub(crate) fn answered(&self) {
        self.complete_recovery();
    }

    pub(crate) fn current(&self) -> bool {
        self.id >= self.gate.first_current_id.load(Ordering::Acquire)
    }

    pub(crate) fn first_failure(&self) -> bool {
        !self.failed.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn probing(&self) -> bool {
        self.probe.load(Ordering::Acquire) && self.current()
    }

    // Whether the connection may be kept idle: the one rule for every place
    // a connection rests (the async idle list, an owned lane's cache, the
    // seat of an existence probe). The gate holds every other connection to
    // the server while its probe is out, so an unanswered probe kept idle
    // would hold them with nothing on the wire to answer for. Such a probe
    // is dropped instead, and dropping its lease gives the probe back.
    pub(crate) fn may_rest_idle(&self) -> bool {
        !self.probing()
    }
}

pub(crate) struct ConnectionHealthLease(pub(crate) Arc<ConnectionHealth>);

impl ConnectionHealthLease {
    // The probe is out: every other connection is held until it answers, so
    // the server takes no new work meanwhile. Say so, as the probe's answer
    // and its giving back do, so the work waiting on this server is offered
    // elsewhere at once. Asked once per admission, after the admission has
    // everything else it needs, so a probe given back at once for want of a
    // socket is heard of once, by its giving back. Nothing for a lease that
    // is not a probe.
    pub(crate) fn announce(&self) {
        if self.0.probe.load(Ordering::Acquire) {
            self.0.gate.publish();
        }
    }
}

impl Drop for ConnectionHealthLease {
    fn drop(&mut self) {
        let mut state = self.0.gate.state.lock().expect("recovery gate poisoned");
        if self.0.epoch == state.epoch && state.probe == Some(self.0.id) {
            state.probe = None;
            self.0.gate.first_current_id.store(
                state
                    .next_id
                    .checked_add(1)
                    .expect("recovery identity exhausted"),
                Ordering::Release,
            );
            self.0.gate.publish();
        }
    }
}

#[cfg(test)]
mod tests;
