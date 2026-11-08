// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A slot is permission to run one decoder, one engine or one repair. The
// job's extraction limit is the number of slots.
//
// Slots are runtime state only and are never persisted: nothing here
// serializes, on purpose. A slot means "running in this process now", which
// no restart can carry; after one, every object asks again and the queue is
// rebuilt in the order they ask.
//
// `Slots` is a synchronous state machine. Every call that frees capacity
// returns the askers it admitted, and the caller wakes them; nothing is woken
// behind the caller's back. The invariant it keeps is that no asker waits
// while a slot is free beside it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

// Grants are numbered across the process, not per `Slots`, so a ticket from
// another job's slots can never pass for one of these. Only uniqueness
// matters, never the order, so the numbering cannot make anything here
// depend on what other jobs did.
static NEXT_GRANT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AskerId(pub u64);

// Proof of one admission, owned by whatever runs under it.
//
// It is a plain value, not a guard that releases on drop: a drop would admit
// the next waiter silently, and that admission would have no one to wake it.
// The owner releases explicitly and wakes who `release` returns. It is
// neither `Clone` nor `Copy`, so one admission cannot be released twice.
#[derive(Debug, PartialEq, Eq)]
pub struct Ticket {
    asker: AskerId,
    // Which admission this is, unique in the process, so a ticket from an
    // earlier admission or another `Slots` cannot release this one.
    grant: u64,
}

impl Ticket {
    pub fn asker(&self) -> AskerId {
        self.asker
    }
}

// The askers just admitted, in the order they asked. The caller wakes each.
pub type Admissions = Vec<(AskerId, Ticket)>;

#[derive(Debug, PartialEq, Eq)]
pub enum Asked {
    Admitted(Ticket),
    // Waiting. Asking again while waiting keeps the original place.
    Queued,
    // The asker already holds a slot; one asker runs one thing at a time.
    Holding,
}

// What became of a holder that yielded.
#[derive(Debug, PartialEq, Eq)]
pub enum Waiting {
    // At the back of the queue; its slot went to the askers admitted.
    Queued,
    // No one else was waiting, so the free slot came straight back rather
    // than sit empty beside a waiter.
    Readmitted(Ticket),
}

#[derive(Debug)]
pub struct Slots {
    limit: usize,
    // Holder to the grant of its ticket.
    holders: HashMap<AskerId, u64>,
    queue: VecDeque<AskerId>,
    waiting: HashSet<AskerId>,
}

impl Slots {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            holders: HashMap::new(),
            queue: VecDeque::new(),
            waiting: HashSet::new(),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn in_use(&self) -> usize {
        self.holders.len()
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    pub fn ask(&mut self, asker: AskerId) -> Asked {
        if self.holders.contains_key(&asker) {
            return Asked::Holding;
        }
        if self.waiting.contains(&asker) {
            return Asked::Queued;
        }
        // Waiters come first: a free slot with a queue beside it cannot
        // happen, but the queue's order must win even if it did.
        if self.queue.is_empty() && self.holders.len() < self.limit {
            return Asked::Admitted(self.grant(asker));
        }
        self.enqueue(asker);
        Asked::Queued
    }

    // Give a slot back. A ticket this instance did not grant changes nothing.
    pub fn release(&mut self, ticket: Ticket) -> Admissions {
        if !self.revoke(&ticket) {
            return Vec::new();
        }
        self.admit()
    }

    // Change the limit. A rise admits waiters at once. A fall takes nothing
    // away, since holders are never interrupted mid-block; releases simply
    // admit no one until the count is under the new limit.
    pub fn set_limit(&mut self, limit: usize) -> Admissions {
        self.limit = limit;
        self.admit()
    }

    // A holder that must wait for something other than a slot gives its slot
    // back and asks again at the back of the queue.
    pub fn yield_slot(&mut self, ticket: Ticket) -> (Waiting, Admissions) {
        let asker = ticket.asker;
        self.revoke(&ticket);
        self.enqueue(asker);
        let mut admissions = self.admit();
        let readmitted = admissions
            .iter()
            .position(|(admitted, _)| *admitted == asker)
            .map(|at| admissions.remove(at).1);
        let waiting = match readmitted {
            Some(ticket) => Waiting::Readmitted(ticket),
            None => Waiting::Queued,
        };
        (waiting, admissions)
    }

    // A waiter that no longer wants a slot, because its job or member went
    // away. Frees no capacity, so admits no one.
    pub fn withdraw(&mut self, asker: AskerId) -> bool {
        if !self.waiting.remove(&asker) {
            return false;
        }
        self.queue.retain(|queued| *queued != asker);
        true
    }

    fn revoke(&mut self, ticket: &Ticket) -> bool {
        if self.holders.get(&ticket.asker) != Some(&ticket.grant) {
            return false;
        }
        self.holders.remove(&ticket.asker);
        true
    }

    fn enqueue(&mut self, asker: AskerId) {
        if self.waiting.insert(asker) {
            self.queue.push_back(asker);
        }
    }

    fn grant(&mut self, asker: AskerId) -> Ticket {
        let grant = NEXT_GRANT.fetch_add(1, Ordering::Relaxed);
        self.holders.insert(asker, grant);
        Ticket { asker, grant }
    }

    fn admit(&mut self) -> Admissions {
        let mut admissions = Vec::new();
        while self.holders.len() < self.limit {
            let Some(asker) = self.queue.pop_front() else {
                break;
            };
            self.waiting.remove(&asker);
            let ticket = self.grant(asker);
            admissions.push((asker, ticket));
        }
        admissions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted(asked: Asked) -> Ticket {
        match asked {
            Asked::Admitted(ticket) => ticket,
            other => panic!("expected an admission, got {other:?}"),
        }
    }

    fn askers(admissions: &Admissions) -> Vec<u64> {
        admissions.iter().map(|(asker, _)| asker.0).collect()
    }

    #[test]
    fn askers_are_admitted_up_to_the_limit_then_queue() {
        let mut slots = Slots::new(2);
        admitted(slots.ask(AskerId(1)));
        admitted(slots.ask(AskerId(2)));
        assert_eq!(slots.ask(AskerId(3)), Asked::Queued);
        assert_eq!((slots.in_use(), slots.queued()), (2, 1));
    }

    #[test]
    fn a_release_admits_the_oldest_waiter() {
        let mut slots = Slots::new(1);
        let first = admitted(slots.ask(AskerId(1)));
        slots.ask(AskerId(2));
        slots.ask(AskerId(3));
        let admissions = slots.release(first);
        assert_eq!(askers(&admissions), vec![2]);
        assert_eq!(admissions[0].1.asker(), AskerId(2));
        assert_eq!((slots.in_use(), slots.queued()), (1, 1));
    }

    #[test]
    fn raising_the_limit_admits_waiters_in_ask_order() {
        let mut slots = Slots::new(1);
        admitted(slots.ask(AskerId(1)));
        for asker in 2..=5 {
            slots.ask(AskerId(asker));
        }
        let admissions = slots.set_limit(3);
        assert_eq!(askers(&admissions), vec![2, 3]);
        assert_eq!((slots.in_use(), slots.queued()), (3, 2));
    }

    #[test]
    fn lowering_the_limit_revokes_nothing_and_releases_admit_no_one_until_under_it() {
        let mut slots = Slots::new(3);
        let tickets: Vec<_> = (1..=3)
            .map(|asker| admitted(slots.ask(AskerId(asker))))
            .collect();
        slots.ask(AskerId(4));
        assert!(slots.set_limit(1).is_empty());
        assert_eq!(slots.in_use(), 3);

        let mut tickets = tickets.into_iter();
        let (Some(first), Some(second), Some(third)) =
            (tickets.next(), tickets.next(), tickets.next())
        else {
            unreachable!();
        };
        assert!(slots.release(first).is_empty());
        assert!(slots.release(second).is_empty());
        assert_eq!(slots.in_use(), 1);
        assert_eq!(askers(&slots.release(third)), vec![4]);
        assert_eq!(slots.in_use(), 1);
    }

    #[test]
    fn a_yield_requeues_at_the_back_and_hands_its_slot_to_the_next_waiter() {
        let mut slots = Slots::new(1);
        let repair = admitted(slots.ask(AskerId(1)));
        slots.ask(AskerId(2));
        slots.ask(AskerId(3));
        let (waiting, admissions) = slots.yield_slot(repair);
        assert_eq!(waiting, Waiting::Queued);
        assert_eq!(askers(&admissions), vec![2]);

        let (_, second) = admissions.into_iter().next().expect("asker 2 admitted");
        assert_eq!(askers(&slots.release(second)), vec![3]);
        assert_eq!(slots.queued(), 1);
    }

    #[test]
    fn a_lone_yielder_is_readmitted_rather_than_wait_beside_a_free_slot() {
        let mut slots = Slots::new(1);
        let repair = admitted(slots.ask(AskerId(1)));
        let (waiting, admissions) = slots.yield_slot(repair);
        assert!(admissions.is_empty());
        assert!(matches!(waiting, Waiting::Readmitted(ref ticket) if ticket.asker() == AskerId(1)));
        assert_eq!((slots.in_use(), slots.queued()), (1, 0));
    }

    #[test]
    fn asking_twice_never_admits_twice() {
        let mut slots = Slots::new(1);
        let ticket = admitted(slots.ask(AskerId(1)));
        assert_eq!(slots.ask(AskerId(1)), Asked::Holding);
        assert_eq!(slots.ask(AskerId(2)), Asked::Queued);
        assert_eq!(slots.ask(AskerId(2)), Asked::Queued);
        assert_eq!(slots.queued(), 1);
        assert_eq!(askers(&slots.release(ticket)), vec![2]);
        assert_eq!(slots.queued(), 0);
    }

    #[test]
    fn an_old_ticket_cannot_release_a_later_admission() {
        let mut slots = Slots::new(1);
        let mut other = Slots::new(1);
        let foreign = admitted(other.ask(AskerId(1)));
        admitted(slots.ask(AskerId(1)));
        slots.ask(AskerId(2));
        assert!(slots.release(foreign).is_empty());
        assert_eq!(slots.in_use(), 1);
    }

    #[test]
    fn a_withdrawn_waiter_is_never_admitted() {
        let mut slots = Slots::new(1);
        let ticket = admitted(slots.ask(AskerId(1)));
        slots.ask(AskerId(2));
        slots.ask(AskerId(3));
        assert!(slots.withdraw(AskerId(2)));
        assert!(!slots.withdraw(AskerId(2)));
        assert_eq!(askers(&slots.release(ticket)), vec![3]);
    }

    // A scripted run of every operation, from a fixed seed, checking the
    // invariants after each step.
    #[test]
    fn a_scripted_run_keeps_every_invariant() {
        let mut slots = Slots::new(2);
        let mut held: Vec<Ticket> = Vec::new();
        let mut seed: u64 = 0x5eed;
        let mut next = move || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            seed >> 33
        };
        let take_admissions = |held: &mut Vec<Ticket>, admissions: Admissions| {
            for (asker, ticket) in admissions {
                assert_eq!(asker, ticket.asker());
                assert!(
                    held.iter().all(|holding| holding.asker() != asker),
                    "asker {asker:?} admitted twice"
                );
                held.push(ticket);
            }
        };
        for _ in 0..2_000 {
            match next() % 5 {
                0 | 1 => {
                    let asker = AskerId(next() % 12);
                    if let Asked::Admitted(ticket) = slots.ask(asker) {
                        take_admissions(&mut held, vec![(asker, ticket)]);
                    }
                }
                2 if !held.is_empty() => {
                    let at = usize::try_from(next()).unwrap_or(0) % held.len();
                    let admissions = slots.release(held.swap_remove(at));
                    take_admissions(&mut held, admissions);
                }
                3 if !held.is_empty() => {
                    let at = usize::try_from(next()).unwrap_or(0) % held.len();
                    let (waiting, admissions) = slots.yield_slot(held.swap_remove(at));
                    take_admissions(&mut held, admissions);
                    if let Waiting::Readmitted(ticket) = waiting {
                        let asker = ticket.asker();
                        take_admissions(&mut held, vec![(asker, ticket)]);
                    }
                }
                _ => {
                    let limit = usize::try_from(next() % 4).unwrap_or(0);
                    let admissions = slots.set_limit(limit);
                    take_admissions(&mut held, admissions);
                }
            }
            assert_eq!(slots.in_use(), held.len());
            if slots.queued() > 0 {
                assert!(
                    slots.in_use() >= slots.limit(),
                    "a waiter sits beside a free slot"
                );
            }
        }
        // Drain: once every holder releases, the count settles under the
        // limit and stays there.
        slots.set_limit(2);
        while let Some(ticket) = held.pop() {
            let admissions = slots.release(ticket);
            take_admissions(&mut held, admissions);
            if held.len() <= slots.limit() {
                assert!(slots.in_use() <= slots.limit());
            }
        }
        assert_eq!(slots.in_use(), 0);
        assert_eq!(slots.queued(), 0);
    }
}
