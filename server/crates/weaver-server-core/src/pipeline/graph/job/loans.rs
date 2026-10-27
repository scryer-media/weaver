// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the lender of its recovery sets' repairs. A repair holds one of
// the repair slots every graph job shares and runs on a blocking thread, so
// the loop goes on with every other job while it reads and rebuilds. One is
// out at a time per job, and the slot it took is kept for the next repair
// the job lends in the same step, and given back once none is due. What the
// repair came to returns through the run mail, numbered by the loan, and is
// applied by the namespace as the set's ruling.

use tokio::sync::mpsc;
use weaver_graph::NodeId;
use weaver_graph::slots::{AskerId, Ticket};

use super::super::engine::{Heard, Said};
use super::super::namespace::{Lender, Loan, SetState};
use super::super::status::RepairSlots;

pub(crate) struct Loans {
    scope: u64,
    mail: mpsc::Sender<Said>,
    // The number the last loan went out under.
    serial: u64,
    out: Option<Out>,
    ticket: Option<Ticket>,
}

// The repair that is out: its set, its number, and what the set said of
// itself as it left, which is what it says until it returns.
struct Out {
    set: NodeId,
    serial: u64,
    state: Option<SetState>,
}

impl Loans {
    pub(crate) fn new(scope: u64, mail: mpsc::Sender<Said>) -> Self {
        Self {
            scope,
            mail,
            serial: 0,
            out: None,
            ticket: None,
        }
    }

    pub(crate) fn out(&self) -> bool {
        self.out.is_some()
    }

    // What the set whose repair is out said of itself as it left.
    pub(crate) fn state(&self, set: NodeId) -> Option<SetState> {
        self.out
            .as_ref()
            .filter(|out| out.set == set)
            .and_then(|out| out.state)
    }

    // The repair numbered `serial` came back to the set: it is no longer
    // out. `false` for anything else, which is not heard.
    pub(crate) fn returned(&mut self, set: NodeId, serial: u64) -> bool {
        let current = self
            .out
            .as_ref()
            .is_some_and(|out| out.set == set && out.serial == serial);
        if current {
            self.out = None;
        }
        current
    }

    // The slot, once nothing is out to use it.
    pub(crate) fn give_back(&mut self) -> Option<Ticket> {
        if self.out.is_some() {
            return None;
        }
        self.ticket.take()
    }

    // The slot, whatever holds it: the job is going away.
    pub(crate) fn forget(&mut self) -> Option<Ticket> {
        self.out = None;
        self.ticket.take()
    }

    // A lender over these loans that asks `slots` for a slot when it holds
    // none, or, without slots, lends only under the one it holds.
    pub(crate) fn lending<'a>(
        &'a mut self,
        slots: Option<(&'a mut RepairSlots, usize)>,
    ) -> Lending<'a> {
        Lending { loans: self, slots }
    }
}

pub(crate) struct Lending<'a> {
    loans: &'a mut Loans,
    slots: Option<(&'a mut RepairSlots, usize)>,
}

impl Lender for Lending<'_> {
    fn admit(&mut self) -> bool {
        let loans = &mut *self.loans;
        if loans.out.is_some() {
            return false;
        }
        if loans.ticket.is_some() {
            return true;
        }
        let Some((slots, limit)) = self.slots.as_mut() else {
            return false;
        };
        loans.ticket = slots.admit(AskerId(loans.scope), *limit);
        loans.ticket.is_some()
    }

    fn lend(&mut self, loan: Loan) {
        let loans = &mut *self.loans;
        loans.serial += 1;
        let (scope, set, serial) = (loans.scope, loan.set, loans.serial);
        loans.out = Some(Out {
            set,
            serial,
            state: loan.state,
        });
        let mail = loans.mail.clone();
        tokio::task::spawn_blocking(move || {
            crate::e2e_failpoint::maybe_delay(crate::e2e_failpoint::REPAIR_TASK_START);
            crate::e2e_failpoint::maybe_trip(crate::e2e_failpoint::REPAIR_TASK_START);
            let returned = loan.run();
            // A closed channel means the pipeline is gone, and no one is
            // left to tell.
            let _ = mail.blocking_send(Said {
                scope,
                node: set,
                generation: serial,
                heard: Heard::Repaired(Box::new(returned)),
            });
        });
    }
}
