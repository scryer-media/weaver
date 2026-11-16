// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Physical socket ownership, independent of checked-out work and client generations.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

type Recall = Arc<dyn Fn(u64) + Send + Sync>;

/// Whether the tunnel still owes bytes: read when a busy connection marked
/// to give them back would close.
pub type GiveBackStands = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SocketPhase {
    Dialing,
    Active,
    OwnedIdle,
    Closing,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SocketBudgetSnapshot {
    pub limit: usize,
    pub physical: usize,
    pub dialing: usize,
    pub owned_idle: usize,
    pub closing: usize,
    pub local_denials: u64,
    pub provider_refusals: u64,
}

struct Entry {
    path: Option<weaver_tunnel::pipe::DialPath>,
    phase: SocketPhase,
    recall: Option<Recall>,
    retiring: Arc<AtomicBool>,
    // Marked to close once idle to give a tunnel's bytes back, while the
    // ask stands. Apart from `retiring`, which a lowered limit sets, so
    // neither clears the other.
    give_back: Option<GiveBackStands>,
    // The tunnel's outcome for a socket still dialling, told once the
    // socket leaves dialling for a connected phase: a give-back counts only
    // connected sockets, so the tunnel reads that as holdings moving.
    connects: Option<Arc<weaver_tunnel::bridge::ConnectionOutcome>>,
}

impl Entry {
    // Whether the entry is to close once idle for a give-back that still
    // stands; a mark whose ask no longer stands is cleared.
    fn gives_back(&mut self) -> bool {
        match &self.give_back {
            Some(stands) if stands() => true,
            Some(_) => {
                self.give_back = None;
                false
            }
            None => false,
        }
    }
}

// Whether a give-back may still ask for the entry's socket: dialled over a
// path `on` accepts, open, and not closing, retiring or marked already.
fn givable(entry: &Entry, on: &dyn Fn(&weaver_tunnel::pipe::DialPath) -> bool) -> bool {
    entry.phase != SocketPhase::Closing
        && entry.give_back.is_none()
        && !entry.retiring.load(Ordering::Acquire)
        && entry.path.as_ref().is_some_and(on)
}

// A givable socket that is connected, not still dialing: what a server holds
// on the path, and what its last connection is counted from. A dial may yet
// fail, so it never keeps a server's floor.
fn held_on(entry: &Entry, on: &dyn Fn(&weaver_tunnel::pipe::DialPath) -> bool) -> bool {
    entry.phase != SocketPhase::Dialing && givable(entry, on)
}

#[derive(Default)]
struct State {
    limit: usize,
    // The limit the pool asked for, before any connections are lent out.
    base: usize,
    // Connections lent to something outside the pool; the limit excludes them.
    lent: usize,
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
                base: limit,
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
            state.base = limit;
            let limit = limit.saturating_sub(state.lent);
            state.limit = limit;
            state
                .entries
                .iter_mut()
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

    // Withdraw unavailable idle paths immediately. Other weight changes drain
    // on demand, preserving warm sockets while no new work waits for them.
    pub(crate) fn configure_legs(&self, targets: &[u16]) {
        let (changed, recalls) = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            let base: usize = targets.iter().map(|target| usize::from(*target)).sum();
            state.base = base;
            let limit = base.saturating_sub(state.lent);
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

    // Lend `count` connections to something outside the pool, or with zero
    // take every lent connection back. Idle sockets above the lowered limit
    // are recalled at once; busy ones close when their work ends.
    pub(crate) fn lend(&self, count: usize) {
        let recalls = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            state.lent = count;
            let limit = state.base.saturating_sub(count);
            state.limit = limit;
            state
                .entries
                .iter_mut()
                .filter(|(_, entry)| entry.phase != SocketPhase::Closing)
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

    pub(crate) fn note_provider_refusal(&self) {
        let mut state = self.state.lock().expect("socket budget poisoned");
        state.provider_refusals = state.provider_refusals.saturating_add(1);
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<SocketSlot> {
        let mut state = self.state.lock().expect("socket budget poisoned");
        if state.entries.len() >= state.limit {
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
                give_back: None,
                connects: None,
            },
        );
        Some(SocketSlot {
            budget: Arc::clone(self),
            id,
            retiring,
        })
    }

    // Ask one idle owner to close its exact socket. Removing the entry in
    // `SocketSlot::drop` acknowledges closure; a recall never refunds a slot.
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

    // The connected sockets dialled over a path `on` accepts that a
    // give-back may still ask for: not dialing, closing, retiring or marked
    // already.
    #[cfg(test)]
    pub(crate) fn givable_on(&self, on: &dyn Fn(&weaver_tunnel::pipe::DialPath) -> bool) -> usize {
        self.give_back_holds(on).0
    }

    // On a path `on` accepts, the connected sockets a give-back may still
    // ask for, and those already marked to close for one and still open:
    // what an answer counts as covered.
    pub(crate) fn give_back_holds(
        &self,
        on: &dyn Fn(&weaver_tunnel::pipe::DialPath) -> bool,
    ) -> (usize, usize) {
        let state = self.state.lock().expect("socket budget poisoned");
        state
            .entries
            .values()
            .fold((0, 0), |(held, marked), entry| {
                let is_marked = entry.give_back.is_some()
                    && entry.phase != SocketPhase::Closing
                    && entry.path.as_ref().is_some_and(on);
                (
                    held + usize::from(held_on(entry, on)),
                    marked + usize::from(is_marked),
                )
            })
    }

    // Retire up to `max` sockets dialled over a path `on` accepts, and say
    // how many: idle owners are asked to close theirs now, and for what they
    // do not cover, busy ones are marked to close once their work ends while
    // the ask `stands`. A busy connection is never interrupted, and one whose
    // ask stands no more when its work ends stays open. The server keeps at
    // least one connected socket on the path: its last is never given back.
    pub(crate) fn recall_idle_on(
        &self,
        max: usize,
        on: &dyn Fn(&weaver_tunnel::pipe::DialPath) -> bool,
        stands: &GiveBackStands,
    ) -> usize {
        // An ask withdrawn before usenet acts on it closes nothing: read
        // just before any connection is closed or marked.
        if !stands() {
            return 0;
        }
        let (recalled, retiring) = {
            let mut state = self.state.lock().expect("socket budget poisoned");
            // Counted from connected sockets only: a dial that fails would
            // leave the server none.
            let held = state
                .entries
                .values()
                .filter(|entry| held_on(entry, on))
                .count();
            let max = max.min(held.saturating_sub(1));
            let recalled: Vec<(u64, Recall)> = state
                .entries
                .iter_mut()
                .filter(|(_, entry)| givable(entry, on) && entry.recall.is_some())
                .take(max)
                .filter_map(|(&id, entry)| {
                    let recall = entry.recall.take()?;
                    entry.phase = SocketPhase::Closing;
                    Some((id, recall))
                })
                .collect();
            let busy = max - recalled.len();
            let retiring = state
                .entries
                .values_mut()
                .filter(|entry| givable(entry, on) && entry.recall.is_none())
                .take(busy)
                .map(|entry| entry.give_back = Some(Arc::clone(stands)))
                .count();
            (recalled, retiring)
        };
        let asked = recalled.len() + retiring;
        for (id, recall) in recalled {
            recall(id);
        }
        if asked > 0 {
            self.publish();
        }
        asked
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
            match entry.phase {
                SocketPhase::Dialing => snapshot.dialing += 1,
                SocketPhase::OwnedIdle => snapshot.owned_idle += 1,
                SocketPhase::Closing => snapshot.closing += 1,
                SocketPhase::Active => {}
            }
        }
        snapshot
    }
}

// Connections a server's pool has lent out. Dropping it gives them back.
pub struct ConnectionLoan {
    budget: Arc<SocketBudget>,
    count: usize,
}

impl ConnectionLoan {
    pub(crate) fn new(budget: Arc<SocketBudget>, count: usize) -> Self {
        Self { budget, count }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    // Sockets the pool holds open, counting those still closing.
    pub fn open(&self) -> usize {
        self.budget.snapshot().physical
    }

    // Wait until the pool holds at most `open` sockets. It wakes on every
    // socket change, so nothing here depends on time.
    pub async fn wait_open_at_most(&self, open: usize) {
        let mut changed = self.budget.subscribe();
        while self.budget.snapshot().physical > open {
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for ConnectionLoan {
    fn drop(&mut self) {
        self.budget.lend(0);
    }
}

// Must be stored after the transport so local closure precedes its refund.
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
        {
            let mut state = self.budget.state.lock().expect("socket budget poisoned");
            if let Some(entry) = state.entries.get_mut(&self.id)
                && entry.phase == SocketPhase::Dialing
            {
                entry.connects = Some(Arc::clone(outcome));
            }
        }
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
                if entry.phase == SocketPhase::OwnedIdle {
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

    // Claim whether this socket can take another article. An excess socket
    // transitions to closing atomically with the decision, so other lanes
    // count it as gone before making their own claims.
    pub(crate) fn claim_reuse(&self) -> bool {
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        let gives_back = state
            .entries
            .get_mut(&self.id)
            .is_some_and(Entry::gives_back);
        if !gives_back && self.reusable_in(&state) {
            return true;
        }
        if let Some(entry) = state.entries.get_mut(&self.id) {
            entry.phase = SocketPhase::Closing;
        }
        false
    }

    fn reusable_in(&self, state: &State) -> bool {
        if self.retiring() {
            return false;
        }
        let Some(entry) = state.entries.get(&self.id) else {
            return true;
        };
        if entry.phase == SocketPhase::Closing {
            return false;
        }
        let Some(targets) = &state.leg_targets else {
            return true;
        };
        let Some(leg) = entry.path.as_ref().and_then(|path| path.leg) else {
            return true;
        };
        let target = usize::from(targets.get(leg).copied().unwrap_or(0));
        target != 0
            && state
                .entries
                .values()
                .filter(|entry| {
                    entry.phase != SocketPhase::Closing
                        && entry.path.as_ref().and_then(|path| path.leg) == Some(leg)
                })
                .count()
                <= target
    }

    pub(crate) fn idle(&self, phase: SocketPhase, recall: Recall) {
        self.set_phase(phase, Some(recall));
    }

    fn set_phase(&self, phase: SocketPhase, recall: Option<Recall>) {
        let mut state = self.budget.state.lock().expect("socket budget poisoned");
        let entry = state.entries.get_mut(&self.id).expect("live socket slot");
        // Leaving dialling for a connected phase; one closing tells nothing.
        let connected = (phase != SocketPhase::Closing)
            .then(|| entry.connects.take())
            .flatten();
        entry.phase = phase;
        entry.recall = recall;
        let closes = phase == SocketPhase::OwnedIdle
            && (entry.retiring.load(Ordering::Acquire) || entry.gives_back());
        let recalled = if closes {
            entry.phase = SocketPhase::Closing;
            entry.recall.take()
        } else {
            None
        };
        drop(state);
        self.budget.publish();
        // Told after the phase is set, so the answer it wakes counts it.
        if let Some(outcome) = connected {
            outcome.connected();
        }
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
