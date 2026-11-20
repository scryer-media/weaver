// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the lender of its recovery sets' repairs. A repair holds one of
// the repair slots every graph job shares and runs on a blocking thread, so
// the loop goes on with every other job while it reads and rebuilds. One is
// out at a time per job, and the slot it took is kept for the next repair
// the job lends in the same step, and given back once none is due. What the
// repair came to returns through the run mail, numbered by the loan, and is
// applied by the namespace as the set's ruling.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use weaver_server_disk::operations::disk::reader::TurnStop;
use weaver_server_disk::operations::disk::writer::{Credit, Stand};

use tokio::sync::mpsc;
use weaver_graph::NodeId;
use weaver_graph::slots::{AskerId, Ticket};

use super::super::disk::JobDisk;
use super::super::engine::{Heard, Said};
use super::super::hardware::Hardware;
use super::super::namespace::{Lender, Loan, RepairRead, RepairSlice, Repaired, Replay, SetState};
use super::super::status::RepairSlots;

pub struct Loans {
    scope: u64,
    mail: mpsc::Sender<Said>,
    // The number the last loan went out under.
    serial: u64,
    out: Option<Out>,
    ticket: Option<Ticket>,
    // What a repair is granted as it goes out.
    hardware: Hardware,
}

// The repair that is out: its set, its number, and what the set said of
// itself as it left, which is what it says until it returns.
struct Out {
    set: NodeId,
    serial: u64,
    state: Option<SetState>,
    stop: Option<Arc<TurnStop>>,
    kind: Work,
    // Raised to tell an apply's reader to read no more; raised too as the
    // loan is dropped, its job's with it.
    ends: Arc<AtomicBool>,
}

impl Drop for Out {
    fn drop(&mut self) {
        self.ends.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Work {
    Repair,
    Replay,
    Verification,
    // A repair's return being applied from its staged output.
    Apply,
}

impl Loans {
    pub fn new(scope: u64, mail: mpsc::Sender<Said>, hardware: Hardware) -> Self {
        Self {
            scope,
            mail,
            serial: 0,
            out: None,
            ticket: None,
            hardware,
        }
    }

    pub(crate) fn out(&self) -> bool {
        self.out.is_some()
    }

    // Whether a repair is out, or its return is being applied: the set's
    // repair is not done until its last slice is down.
    pub(crate) fn repairing(&self) -> bool {
        self.is_out(Work::Repair) || self.is_out(Work::Apply)
    }

    // Whether a repair's return is being applied, its staged output read
    // back off the loop.
    pub(crate) fn applying(&self) -> bool {
        self.is_out(Work::Apply)
    }

    // Whether `serial` is the apply out for `set`: a slice of any other is
    // not heard.
    pub(crate) fn applying_under(&self, set: NodeId, serial: u64) -> bool {
        self.out
            .as_ref()
            .is_some_and(|out| out.kind == Work::Apply && out.set == set && out.serial == serial)
    }

    // Tell the reader of the return being applied to read no more: a slice
    // failed, or the job is going. It says what it read and ends.
    pub(crate) fn end_apply(&self) {
        if let Some(out) = self.out.as_ref().filter(|out| out.kind == Work::Apply) {
            out.ends.store(true, Ordering::SeqCst);
        }
    }

    // The set's repair came back, and its return is applied from its
    // staged output: a reader reads it back off the loop a slice at a time,
    // counts each slice on the job's volumes, standing still on its own
    // thread while one is at its limit, and mails it with what it counted
    // (`Heard::RepairSlice`), then says it is done (`Heard::RepairRead`).
    // Its mail is numbered by a loan of its own, under the slot the repair
    // held. A stop does not end the reading: a volume's limit is kept until
    // the stop's end, and a slice not counted by then ends it short.
    pub(crate) fn apply(&mut self, set: NodeId, output: Repaired, disk: JobDisk) {
        self.serial += 1;
        let (scope, serial) = (self.scope, self.serial);
        let ends = Arc::new(AtomicBool::new(false));
        self.out = Some(Out {
            set,
            serial,
            state: None,
            stop: None,
            kind: Work::Apply,
            ends: Arc::clone(&ends),
        });
        let mail = self.mail.clone();
        tokio::task::spawn_blocking(move || {
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let said = |heard| Said {
                scope,
                node: set,
                generation: serial,
                heard,
            };
            let (mut whole, mut gone) = (true, false);
            for (member, offset, bytes) in output.slices() {
                if ends.load(Ordering::SeqCst) {
                    whole = false;
                    break;
                }
                let credit = match &bytes {
                    Ok(bytes) => {
                        let len = bytes.len() as u64;
                        let credit = disk.reserve_ahead(len, Stand::UntilStopsEnd);
                        if !credit.covers(len) {
                            whole = false;
                            break;
                        }
                        credit
                    }
                    Err(_) => Credit::none(),
                };
                let failed = bytes.is_err();
                let slice = RepairSlice {
                    member,
                    offset,
                    bytes,
                    credit,
                };
                // A closed channel means the pipeline is gone, and no one
                // is left to tell.
                if mail
                    .blocking_send(said(Heard::RepairSlice(Box::new(slice))))
                    .is_err()
                {
                    gone = true;
                    break;
                }
                // The loop fails the return on a slice that could not be
                // read: nothing after it is applied.
                if failed {
                    break;
                }
            }
            if gone {
                let _ = output.remove();
                return;
            }
            let read = RepairRead { output, whole };
            if let Err(error) = mail.blocking_send(said(Heard::RepairRead(Box::new(read))))
                && let Heard::RepairRead(read) = error.0.heard
            {
                let _ = read.output.remove();
            }
        });
    }

    // The tail of the set's return found no room on the job's volumes for
    // the next piece it delivers again: a thread counts `bytes` for it on
    // each, standing still on its own thread while one is at its limit, and
    // mails what it counted (`Heard::TailPaid`), on which the tail goes on.
    // Numbered by a loan of its own, as an apply, under the slot the repair
    // held: the set's repair is not done until the tail is. A stop keeps the
    // volumes' limits until its end, which ends the wait short.
    pub(crate) fn pay(&mut self, set: NodeId, bytes: u64, disk: JobDisk) {
        self.serial += 1;
        let (scope, serial) = (self.scope, self.serial);
        self.out = Some(Out {
            set,
            serial,
            state: None,
            stop: None,
            kind: Work::Apply,
            ends: Arc::new(AtomicBool::new(false)),
        });
        let mail = self.mail.clone();
        tokio::task::spawn_blocking(move || {
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let credit = disk.reserve_ahead(bytes, Stand::UntilStopsEnd);
            // A closed channel means the pipeline is gone; the credit goes
            // back as it drops.
            let _ = mail.blocking_send(Said {
                scope,
                node: set,
                generation: serial,
                heard: Heard::TailPaid(Box::new(credit)),
            });
        });
    }

    fn is_out(&self, kind: Work) -> bool {
        self.out.as_ref().is_some_and(|out| out.kind == kind)
    }

    pub(crate) fn request_stop(&self) {
        if let Some(stop) = self.out.as_ref().and_then(|out| out.stop.as_ref()) {
            stop.raise();
        }
    }

    // What the set whose repair is out said of itself as it left.
    pub(crate) fn state(&self, set: NodeId) -> Option<SetState> {
        self.out
            .as_ref()
            .filter(|out| out.set == set)
            .and_then(|out| out.state)
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(super) fn current_repair(&self, set: NodeId, serial: u64) -> bool {
        self.out
            .as_ref()
            .is_some_and(|out| out.kind == Work::Repair && out.set == set && out.serial == serial)
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

    // A lender over these loans that asks `slots` for a slot when it holds
    // none, or, without slots, lends only under the one it holds.
    pub fn lending<'a>(&'a mut self, slots: Option<(&'a mut RepairSlots, usize)>) -> Lending<'a> {
        Lending { loans: self, slots }
    }
}

pub struct Lending<'a> {
    loans: &'a mut Loans,
    slots: Option<(&'a mut RepairSlots, usize)>,
}

impl Lender for Lending<'_> {
    // No repair goes out once the pipeline is shutting down: its set rules
    // again after the restart.
    fn admit(&mut self) -> bool {
        let loans = &mut *self.loans;
        if loans.out.is_some() || loans.hardware.shutdown().started() {
            return false;
        }
        // A retained ticket still belongs to the retiring or paused job,
        // but a zero-slot grant may not start another repair under it.
        if self.slots.as_ref().is_some_and(|(_, limit)| *limit == 0) {
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
            stop: Some(loan.stop()),
            kind: Work::Repair,
            ends: Arc::default(),
        });
        let mail = loans.mail.clone();
        // The repair runs inside the repair pool, on the budget in force as
        // it goes out: a later profile reaches the next repair, not this one.
        let grant = loans.hardware.grant();
        let shutdown = loans.hardware.shutdown().clone();
        tokio::task::spawn_blocking(move || {
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            tracing::info!(
                job_id = scope,
                node = set.0,
                generation = serial,
                "graph repair worker started"
            );
            weaver_server_base::e2e_failpoint::maybe_delay(
                weaver_server_base::e2e_failpoint::REPAIR_TASK_START,
            );
            weaver_server_base::e2e_failpoint::maybe_trip(
                weaver_server_base::e2e_failpoint::REPAIR_TASK_START,
            );
            let returned = grant.install_repair(|| {
                // What the repair says, its timing line among it, names the
                // job.
                let _job = tracing::debug_span!("graph_repair", job_id = scope).entered();
                loan.run(grant.repair.clone(), &shutdown)
            });
            tracing::info!(
                job_id = scope,
                node = set.0,
                generation = serial,
                cancelled = returned.stopped(),
                "graph repair worker ended"
            );
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

impl Lending<'_> {
    pub fn verify(&mut self, check: super::super::namespace::Verification) {
        let loans = &mut *self.loans;
        loans.serial += 1;
        let (scope, node, serial) = (loans.scope, check.node, loans.serial);
        loans.out = Some(Out {
            set: node,
            serial,
            state: None,
            stop: Some(Arc::clone(&check.stop)),
            kind: Work::Verification,
            ends: Arc::default(),
        });
        let mail = loans.mail.clone();
        let shutdown = loans.hardware.shutdown().clone();
        tokio::task::spawn_blocking(move || {
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let checked = check.run(&shutdown);
            let _ = mail.blocking_send(Said {
                scope,
                node,
                generation: serial,
                heard: Heard::Verified(Box::new(checked)),
            });
        });
    }

    // Reconstruction shares admission and the completion acknowledgment
    // with repair work; it never borrows a live object's delivery queue.
    pub(crate) fn replay(&mut self, replay: Replay) {
        let loans = &mut *self.loans;
        loans.serial += 1;
        let (scope, node, serial) = (loans.scope, replay.node, loans.serial);
        loans.out = Some(Out {
            set: node,
            serial,
            state: None,
            stop: Some(Arc::clone(&replay.stop)),
            kind: Work::Replay,
            ends: Arc::default(),
        });
        let mail = loans.mail.clone();
        let shutdown = loans.hardware.shutdown().clone();
        tokio::task::spawn_blocking(move || {
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let replayed = replay.run(&shutdown);
            if let Err(error) = mail.blocking_send(Said {
                scope,
                node,
                generation: serial,
                heard: Heard::Replayed(Box::new(replayed)),
            }) && let Heard::Replayed(replayed) = error.0.heard
            {
                let _ = replayed.remove();
            }
        });
    }
}
