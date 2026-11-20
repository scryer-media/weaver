// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The repair slots every graph job shares, and the tickets of the jobs that
// hold one. A ticket is kept here between advances, so an admission handed
// out by someone else's release is found by the job it names the next time
// that job advances; the jobs admitted are queued to be woken.

use std::collections::HashMap;

use weaver_graph::slots::{Admissions, Asked, AskerId, Slots, Ticket, Waiting};

#[derive(Debug)]
pub(crate) struct RepairSlots {
    slots: Slots,
    tickets: HashMap<AskerId, Ticket>,
    woken: Vec<AskerId>,
}

impl Default for RepairSlots {
    // No slot until the first ask sets the limit from the settings.
    fn default() -> Self {
        Self {
            slots: Slots::new(0),
            tickets: HashMap::new(),
            woken: Vec::new(),
        }
    }
}

impl RepairSlots {
    // The asker's slot, if it has or gets one. The limit is set on every
    // ask so a tuning change applies to the next admission.
    pub(crate) fn admit(&mut self, asker: AskerId, limit: usize) -> Option<Ticket> {
        let admissions = self.slots.set_limit(limit);
        self.note(admissions);
        if let Some(ticket) = self.tickets.remove(&asker) {
            return Some(ticket);
        }
        match self.slots.ask(asker) {
            Asked::Admitted(ticket) => Some(ticket),
            Asked::Queued | Asked::Holding => None,
        }
    }

    // Whether a slot was handed to the asker while it waited, and is kept
    // here until it asks again.
    pub(crate) fn handed(&self, asker: AskerId) -> bool {
        self.tickets.contains_key(&asker)
    }

    pub(crate) fn release(&mut self, ticket: Ticket) {
        let admissions = self.slots.release(ticket);
        self.note(admissions);
    }

    // A repair waiting on recovery bytes gives its slot to whoever waits.
    pub(crate) fn yield_slot(&mut self, ticket: Ticket) {
        let (waiting, admissions) = self.slots.yield_slot(ticket);
        if let Waiting::Readmitted(ticket) = waiting {
            self.tickets.insert(ticket.asker(), ticket);
        }
        self.note(admissions);
    }

    // A job that went away: its slot or its place in the queue.
    pub(crate) fn forget(&mut self, asker: AskerId) {
        match self.tickets.remove(&asker) {
            Some(ticket) => self.release(ticket),
            None => {
                self.slots.withdraw(asker);
            }
        }
    }

    pub(crate) fn take_woken(&mut self) -> Vec<AskerId> {
        std::mem::take(&mut self.woken)
    }

    fn note(&mut self, admissions: Admissions) {
        for (asker, ticket) in admissions {
            self.tickets.insert(asker, ticket);
            self.woken.push(asker);
        }
    }
}
