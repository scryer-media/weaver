//! Physical socket ownership, independent of checked-out work and client generations.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
    path: Option<weaver_tunnel::pipe::DialPath>,
    phase: SocketPhase,
    recall: Option<Recall>,
    retiring: Arc<AtomicBool>,
    replacement: bool,
}

#[derive(Default)]
struct State {
    limit: usize,
    leg_targets: Option<Vec<u16>>,
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
                    entry.retiring.store(true, Ordering::Release);
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

    /// Withdraw unavailable idle paths immediately. Other weight changes drain
    /// on demand, preserving warm sockets while there is no replacement work.
    pub(crate) fn configure_legs(&self, targets: &[u16]) {
        let (changed, recalls) = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            let limit = targets.iter().map(|target| usize::from(*target)).sum();
            let changed = state.limit != limit || state.leg_targets.as_deref() != Some(targets);
            // Socket phase notifications need no entry scan while every leg is available.
            if !changed && targets.iter().all(|target| *target > 0) {
                return;
            }
            state.limit = limit;
            if changed {
                state.leg_targets = Some(targets.to_vec());
            }
            let mut counts = vec![0_usize; targets.len()];
            for entry in state
                .entries
                .values()
                .filter(|entry| entry.phase != SocketPhase::Closing)
            {
                if let Some(count) = entry
                    .path
                    .as_ref()
                    .and_then(|p| p.leg)
                    .and_then(|leg| counts.get_mut(leg))
                {
                    *count += 1;
                }
            }
            let mut recalls = Vec::new();
            for (&id, entry) in &mut state.entries {
                let Some(leg) = entry.path.as_ref().and_then(|p| p.leg) else {
                    continue;
                };
                let target = usize::from(targets.get(leg).copied().unwrap_or(0));
                if target > 0 {
                    continue;
                }
                if counts.get(leg).copied().unwrap_or(1) <= target {
                    continue;
                }
                if let Some(recall) = entry.recall.take() {
                    entry.phase = SocketPhase::Closing;
                    entry.retiring.store(true, Ordering::Release);
                    if let Some(count) = counts.get_mut(leg) {
                        *count -= 1;
                    }
                    recalls.push((id, recall));
                }
            }
            (changed, recalls)
        };
        let recalled = !recalls.is_empty();
        for (id, recall) in recalls {
            recall(id);
        }
        if changed || recalled {
            self.publish();
        }
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
        let retiring = Arc::new(AtomicBool::new(false));
        state.entries.insert(
            id,
            Entry {
                path: None,
                phase: SocketPhase::Dialing,
                recall: None,
                retiring: Arc::clone(&retiring),
                replacement,
            },
        );
        Some(SocketSlot {
            budget: Arc::clone(self),
            id,
            retiring,
        })
    }

    /// Ask one idle owner to close its exact socket. Removing the entry in
    /// `SocketSlot::drop` acknowledges closure; a recall never refunds a slot.
    pub(crate) fn recall_idle(&self) -> bool {
        let excess = {
            let state = self.state.lock().expect("socket budget poisoned");
            state.leg_targets.as_ref().and_then(|targets| {
                targets.iter().enumerate().find_map(|(leg, target)| {
                    let count = state
                        .entries
                        .values()
                        .filter(|entry| {
                            entry.phase != SocketPhase::Closing
                                && entry.path.as_ref().and_then(|path| path.leg) == Some(leg)
                        })
                        .count();
                    (count > usize::from(*target)
                        && state.entries.values().any(|entry| {
                            entry.recall.is_some()
                                && entry.path.as_ref().and_then(|path| path.leg) == Some(leg)
                        }))
                    .then_some(leg)
                })
            })
        };
        if let Some(leg) = excess {
            return self.recall_idle_for_leg(Some(leg));
        }
        self.recall_idle_for_leg(None)
    }

    pub(crate) fn recall_idle_for_leg(&self, leg: Option<usize>) -> bool {
        let recalled = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            state.entries.iter_mut().find_map(|(&id, entry)| {
                if leg.is_some() && entry.path.as_ref().and_then(|path| path.leg) != leg {
                    return None;
                }
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
    retiring: Arc<AtomicBool>,
}

impl SocketSlot {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn retiring(&self) -> bool {
        self.retiring.load(Ordering::Acquire)
    }

    pub(crate) fn active(&self) {
        self.set_phase(SocketPhase::Active, None);
    }

    pub(crate) fn set_path(&self, path: Option<&weaver_tunnel::pipe::DialPath>) {
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        state
            .entries
            .get_mut(&self.id)
            .expect("live socket slot")
            .path = path.cloned();
        drop(state);
        self.budget.publish();
    }

    pub(crate) fn observe_outcome(
        &self,
        outcome: Option<&Arc<weaver_tunnel::bridge::ConnectionOutcome>>,
    ) {
        let Some(outcome) = outcome else {
            return;
        };
        let budget = Arc::downgrade(&self.budget);
        let id = self.id;
        outcome.on_retire(move || {
            let Some(budget) = budget.upgrade() else {
                return;
            };
            let recall = {
                let mut state = budget.state.lock().expect("socket budget poisoned");
                let Some(entry) = state.entries.get_mut(&id) else {
                    return;
                };
                entry.retiring.store(true, Ordering::Release);
                if matches!(entry.phase, SocketPhase::AsyncIdle | SocketPhase::OwnedIdle) {
                    entry.phase = SocketPhase::Closing;
                    entry.recall.take()
                } else {
                    None
                }
            };
            budget.publish();
            if let Some(recall) = recall {
                recall(id);
            }
        });
    }

    pub(crate) fn reusable(&self) -> bool {
        if self.retiring() {
            return false;
        }
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        let Some(targets) = &state.leg_targets else {
            return true;
        };
        let Some(entry) = state.entries.get(&self.id) else {
            return true;
        };
        if entry.phase == SocketPhase::Closing {
            return false;
        }
        let Some(leg) = entry.path.as_ref().and_then(|path| path.leg) else {
            return true;
        };
        let target = usize::from(targets.get(leg).copied().unwrap_or(0));
        let count = state
            .entries
            .values()
            .filter(|entry| {
                entry.phase != SocketPhase::Closing
                    && entry.path.as_ref().and_then(|path| path.leg) == Some(leg)
            })
            .count();
        if target == 0 {
            return false;
        }
        if count > target {
            // This socket is the excess and goes: claim that here, under the
            // lock, so every other lane on the leg that asks in the same
            // instant counts it as gone and keeps its own.
            state
                .entries
                .get_mut(&self.id)
                .expect("live socket slot")
                .phase = SocketPhase::Closing;
            return false;
        }
        true
    }

    pub(crate) fn idle(&self, phase: SocketPhase, recall: Recall) {
        self.set_phase(phase, Some(recall));
    }

    fn set_phase(&self, phase: SocketPhase, recall: Option<Recall>) {
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        let entry = state.entries.get_mut(&self.id).expect("live socket slot");
        entry.phase = phase;
        entry.recall = recall;
        let recalled = if entry.retiring.load(Ordering::Acquire)
            && matches!(phase, SocketPhase::AsyncIdle | SocketPhase::OwnedIdle)
        {
            entry.phase = SocketPhase::Closing;
            entry.recall.take()
        } else {
            None
        };
        drop(state);
        self.budget.publish();
        if let Some(recall) = recalled {
            recall(self.id);
        }
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
