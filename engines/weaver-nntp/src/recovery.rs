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
}

impl State {
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
    #[cfg(test)]
    pub(crate) fn expire_now(&self) {
        self.state.lock().expect("recovery gate poisoned").deadline = Some(Instant::now());
    }

    pub(crate) fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Acquire)
    }

    pub(crate) fn quarantine(
        &self,
        duration: Duration,
        event: Option<(u64, u64)>,
    ) -> Option<Instant> {
        self.quarantine_as(duration, event, false)
    }

    // A quarantine for a server that refused this pool's credentials. The
    // refusal is an answer for its whole length, not a wait: see
    // [`Self::refuses_credentials`].
    pub(crate) fn quarantine_for_credentials(
        &self,
        duration: Duration,
        event: Option<(u64, u64)>,
    ) -> Option<Instant> {
        self.quarantine_as(duration, event, true)
    }

    fn quarantine_as(
        &self,
        duration: Duration,
        event: Option<(u64, u64)>,
        credentials: bool,
    ) -> Option<Instant> {
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
        state.credentials = credentials;
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
        Some(until)
    }

    // Whether the server refused this pool's credentials and its quarantine
    // still runs. Until the deadline nothing it could be asked would get a
    // different answer, so the work it would serve is not waiting for it.
    // Past the deadline the one probe is due and this answers no again: a
    // probe out on a refusing server is a wait like any other.
    pub(crate) fn refuses_credentials(&self) -> bool {
        if !self.quarantined() {
            return false;
        }
        let state = self.state.lock().expect("recovery gate poisoned");
        state.quarantine
            && state.credentials
            && state.deadline.is_some_and(|at| at > Instant::now())
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
        state.deadline = None;
        state.probe = None;
        self.probe.store(false, Ordering::Release);
        self.gate.quarantined.store(false, Ordering::Release);
        self.gate.publish();
        true
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
}

pub(crate) struct ConnectionHealthLease(pub(crate) Arc<ConnectionHealth>);

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
