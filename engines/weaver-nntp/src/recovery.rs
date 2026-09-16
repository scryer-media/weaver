//! Connection-scoped recovery admission and stale-outcome fencing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    next_id: u64,
    quarantine: bool,
    deadline: Option<Instant>,
    probe: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct RecoveryGate {
    state: Mutex<State>,
    // Only admission/retirement changes this frontier. Live connections at or
    // above it may issue work without locking the recovery policy.
    first_current_id: AtomicU64,
    quarantined: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecoverySnapshot {
    pub epoch: u64,
    pub probe_id: Option<u64>,
    pub remaining: Option<Duration>,
    pub quarantined: bool,
}

impl RecoveryGate {
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
        Some(until)
    }

    pub(crate) fn admit(self: &Arc<Self>, demanded: bool) -> Option<ConnectionHealthLease> {
        let mut state = self.state.lock().expect("recovery gate poisoned");
        if state.quarantine
            && (!demanded
                || state.probe.is_some()
                || state.deadline.is_some_and(|at| at > Instant::now()))
        {
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
        state.deadline = None;
        state.probe = None;
        self.probe.store(false, Ordering::Release);
        self.gate.quarantined.store(false, Ordering::Release);
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
        }
    }
}

#[cfg(test)]
mod tests;
