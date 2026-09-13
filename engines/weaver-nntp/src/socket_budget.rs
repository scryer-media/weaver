//! Physical socket ownership, independent of checked-out work and client generations.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

type Recall = Arc<dyn Fn(u64) + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SocketPhase {
    Dialing,
    Active,
    AsyncIdle,
    OwnedIdle,
    Closing,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SocketBudgetSnapshot {
    pub limit: usize,
    pub physical: usize,
    pub dialing: usize,
    pub async_idle: usize,
    pub owned_idle: usize,
    pub closing: usize,
    pub replacement: usize,
    pub local_denials: u64,
    pub provider_refusals: u64,
}

struct Entry {
    phase: SocketPhase,
    recall: Option<Recall>,
    retiring: bool,
    replacement: bool,
}

#[derive(Default)]
struct State {
    limit: usize,
    next_id: u64,
    entries: BTreeMap<u64, Entry>,
    local_denials: u64,
    provider_refusals: u64,
}

pub(crate) struct SocketBudget {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl SocketBudget {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                limit,
                ..State::default()
            }),
            changed: watch::channel(0).0,
        })
    }

    fn publish(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) fn configure(&self, limit: usize) {
        let recalls = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            state.limit = limit;
            state
                .entries
                .iter_mut()
                .filter(|(_, entry)| !entry.replacement)
                .skip(limit)
                .filter_map(|(&id, entry)| {
                    entry.retiring = true;
                    entry.recall.take().map(|recall| {
                        entry.phase = SocketPhase::Closing;
                        (id, recall)
                    })
                })
                .collect::<Vec<_>>()
        };
        for (id, recall) in recalls {
            recall(id);
        }
        self.publish();
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<SocketSlot> {
        self.acquire(false)
    }

    /// Only the explicitly enabled IP-replacement path may use this extra
    /// slot; it is never poolable or available to ordinary dispatch.
    pub(crate) fn try_acquire_replacement(self: &Arc<Self>) -> Option<SocketSlot> {
        self.acquire(true)
    }

    pub(crate) fn note_provider_refusal(&self) {
        let mut state = self.state.lock().expect("socket budget poisoned");
        state.provider_refusals = state.provider_refusals.saturating_add(1);
    }

    fn acquire(self: &Arc<Self>, replacement: bool) -> Option<SocketSlot> {
        let mut state = self.state.lock().expect("socket budget poisoned");
        let denied = if replacement {
            state.limit == 0
                || state.entries.values().any(|entry| entry.replacement)
                || state.entries.len() > state.limit
        } else {
            state.entries.len() >= state.limit
        };
        if denied {
            state.local_denials = state.local_denials.saturating_add(1);
            return None;
        }
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("socket identity exhausted");
        let id = state.next_id;
        state.entries.insert(
            id,
            Entry {
                phase: SocketPhase::Dialing,
                recall: None,
                retiring: false,
                replacement,
            },
        );
        Some(SocketSlot {
            budget: Arc::clone(self),
            id,
        })
    }

    /// Ask one idle owner to close its exact socket. Removing the entry in
    /// `SocketSlot::drop` acknowledges closure; a recall never refunds a slot.
    pub(crate) fn recall_idle(&self) -> bool {
        let recalled = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            state.entries.iter_mut().find_map(|(&id, entry)| {
                let recall = entry.recall.take()?;
                entry.phase = SocketPhase::Closing;
                Some((id, recall))
            })
        };
        if let Some((id, recall)) = recalled {
            recall(id);
            true
        } else {
            false
        }
    }

    pub(crate) fn snapshot(&self) -> SocketBudgetSnapshot {
        let state = self.state.lock().expect("socket budget poisoned");
        let mut snapshot = SocketBudgetSnapshot {
            limit: state.limit,
            physical: state.entries.len(),
            local_denials: state.local_denials,
            provider_refusals: state.provider_refusals,
            ..SocketBudgetSnapshot::default()
        };
        for entry in state.entries.values() {
            snapshot.replacement += usize::from(entry.replacement);
            match entry.phase {
                SocketPhase::Dialing => snapshot.dialing += 1,
                SocketPhase::AsyncIdle => snapshot.async_idle += 1,
                SocketPhase::OwnedIdle => snapshot.owned_idle += 1,
                SocketPhase::Closing => snapshot.closing += 1,
                SocketPhase::Active => {}
            }
        }
        snapshot
    }
}

/// Must be stored after the transport so local closure precedes its refund.
pub(crate) struct SocketSlot {
    budget: Arc<SocketBudget>,
    id: u64,
}

impl SocketSlot {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn retiring(&self) -> bool {
        self.budget
            .state
            .lock()
            .expect("socket budget poisoned")
            .entries[&self.id]
            .retiring
    }

    pub(crate) fn active(&self) {
        self.set_phase(SocketPhase::Active, None);
    }

    pub(crate) fn idle(&self, phase: SocketPhase, recall: Recall) {
        self.set_phase(phase, Some(recall));
    }

    fn set_phase(&self, phase: SocketPhase, recall: Option<Recall>) {
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        let entry = state.entries.get_mut(&self.id).expect("live socket slot");
        entry.phase = phase;
        entry.recall = recall;
        drop(state);
        self.budget.publish();
    }
}

impl Drop for SocketSlot {
    fn drop(&mut self) {
        self.budget
            .state
            .lock()
            .expect("socket budget poisoned")
            .entries
            .remove(&self.id);
        self.budget.publish();
    }
}

#[cfg(test)]
#[path = "socket_budget/tests.rs"]
mod tests;
