// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the lender of the work its objects hold that is too slow for
// the loop: a keystream run over held ciphertext, a key derived from a
// password, and a restored recovery object's walk over what of its volumes
// arrived. The object is lent out of its node, does its work on the
// post-processing pool, and comes back through the run mail with what it
// said, numbered by the loan; the tree applies that, then hears what waited
// for it. Each object is out at most once, and no slot is taken: the work
// is bounded by what the object held as it left.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc;
use weaver_graph::{Emit, NodeId, Object};

use super::super::decoding::{DecodeClock, Decoding};
use super::super::engine::{Heard, Said};
use super::super::hardware::Hardware;
use super::super::namespace::{RewalkLoan, Rewalked};

pub(crate) struct Works {
    scope: u64,
    mail: mpsc::Sender<Said>,
    // The number the last loan went out under.
    serial: u64,
    // The loans out, by node, under their numbers, each with the word that
    // stops its work once the job has gone.
    out: BTreeMap<NodeId, (u64, Arc<AtomicBool>)>,
    // The rewalks reading, by node: each one's number and the word that lets its reader go on to the next piece once the last
    // one was heard. Its object stays home, so a checkpoint records it as
    // it stands.
    rewalks: BTreeMap<NodeId, Rewalking>,
    // A checkpoint found a loan out and took nothing: until one is
    // recorded, the work is done on the loop, so the next step takes it.
    owed: bool,
    hardware: Hardware,
    // Where a loan keeps the time its work spends executing.
    clock: DecodeClock,
    // A test keeps every loan in hand, to send the job what it would hear
    // while the object is out, and hands each back when it chooses.
    #[cfg(test)]
    held: Option<Vec<HeldLoan>>,
    // How many objects went out for their work.
    #[cfg(test)]
    lent: u64,
}

// A loan a test keeps in hand: its node, its number, the object, and the
// word that stops its work.
#[cfg(test)]
pub(crate) type HeldLoan = (NodeId, u64, Box<dyn Object>, Arc<AtomicBool>);

struct Rewalking {
    serial: u64,
    heard: std::sync::mpsc::SyncSender<()>,
}

// An object back from its work, with what it said.
pub(crate) struct Worked {
    pub(crate) object: Box<dyn Object>,
    pub(crate) said: Vec<Emit>,
}

impl fmt::Debug for Worked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worked")
            .field("kind", &self.object.kind())
            .field("said", &self.said.len())
            .finish()
    }
}

impl Works {
    pub(crate) fn new(scope: u64, mail: mpsc::Sender<Said>, hardware: Hardware) -> Self {
        Self {
            scope,
            mail,
            serial: 0,
            out: BTreeMap::new(),
            rewalks: BTreeMap::new(),
            owed: false,
            hardware,
            clock: DecodeClock::default(),
            #[cfg(test)]
            held: None,
            #[cfg(test)]
            lent: 0,
        }
    }

    // The loans, keeping the time their work executes on `clock`.
    pub(crate) fn timed(mut self, clock: DecodeClock) -> Self {
        self.clock = clock;
        self
    }

    // Work done where it stands, not lent, executes as a loan's does.
    pub(crate) fn decoding(&self) -> Decoding {
        self.clock.decoding()
    }

    #[cfg(test)]
    pub(crate) fn hold_loans(&mut self) {
        self.held.get_or_insert_with(Vec::new);
    }

    // The loans kept in hand, by node, under their numbers.
    #[cfg(test)]
    pub(crate) fn take_held(&mut self) -> Vec<HeldLoan> {
        self.held.as_mut().map(std::mem::take).unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn lent(&self) -> u64 {
        self.lent
    }

    #[cfg(test)]
    pub(crate) fn scope(&self) -> u64 {
        self.scope
    }

    pub(crate) fn out(&self) -> bool {
        !self.out.is_empty() || !self.rewalks.is_empty()
    }

    // Whether a rewalk reads its volumes' files.
    pub(crate) fn rewalks_out(&self) -> bool {
        !self.rewalks.is_empty()
    }

    // Whether an object is out for its work, without the facts a
    // checkpoint would record it by.
    pub(crate) fn working(&self) -> bool {
        !self.out.is_empty()
    }

    // A rewalk reads hundreds of megabytes at most once per restore: none
    // goes out once the pipeline is shutting down, and the next restore
    // asks again. A checkpoint owed does not hold it, since it is lent
    // with its facts.
    pub(crate) fn admit_rewalk(&self) -> bool {
        !self.hardware.shutdown().started()
    }

    // No work goes out once the pipeline is shutting down, so the last
    // checkpoint finds none of it out, nor while a checkpoint is owed.
    pub(crate) fn admit(&self) -> bool {
        !self.owed && !self.hardware.shutdown().started()
    }

    pub(crate) fn owe_checkpoint(&mut self) {
        self.owed = true;
    }

    pub(crate) fn checkpointed(&mut self) {
        self.owed = false;
    }

    // The loan numbered `serial` came back to `node`: it is no longer out.
    // `false` for anything else, which is not heard.
    pub(crate) fn returned(&mut self, node: NodeId, serial: u64) -> bool {
        if self.out.get(&node).is_some_and(|(out, _)| *out == serial) {
            self.out.remove(&node);
            return true;
        }
        false
    }

    // The job is going and will hear none of its loans: each stops its work
    // between one key and the next and comes back with the rest undone. A
    // loan lent after this is not stopped.
    pub(crate) fn request_stop(&self) {
        for (_, stop) in self.out.values() {
            stop.store(true, Ordering::Relaxed);
        }
    }

    // Whether the rewalk numbered `serial` still reads for `node`.
    pub(crate) fn rewalking(&self, node: NodeId, serial: u64) -> bool {
        self.rewalks
            .get(&node)
            .is_some_and(|rewalking| rewalking.serial == serial)
    }

    // A piece the rewalk at `node` read was heard: its reader goes on.
    pub(crate) fn rewalk_heard(&self, node: NodeId) {
        if let Some(rewalking) = self.rewalks.get(&node) {
            let _ = rewalking.heard.try_send(());
        }
    }

    // The rewalk numbered `serial` at `node` ended, read through or not.
    // `false` for anything else. Its reader, if it still reads, stops at
    // its next piece.
    pub(crate) fn rewalk_returned(&mut self, node: NodeId, serial: u64) -> bool {
        self.rewalking(node, serial) && self.rewalks.remove(&node).is_some()
    }

    // Read what a restored recovery object asked again on the pool, one
    // piece in hand at a time: each goes to the loop, and the next is read
    // once the loop heard it.
    pub(crate) fn lend_rewalk(&mut self, loan: RewalkLoan) {
        self.serial += 1;
        let (scope, serial, node) = (self.scope, self.serial, loan.node());
        let (heard, hearing) = std::sync::mpsc::sync_channel(1);
        self.rewalks.insert(node, Rewalking { serial, heard });
        let mail = self.mail.clone();
        let shutdown = self.hardware.shutdown().clone();
        tokio::task::spawn_blocking(move || {
            let _job = crate::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let tell = |piece, whole, scratch| {
                mail.blocking_send(Said {
                    scope,
                    node,
                    generation: serial,
                    heard: Heard::Rewalked(Box::new(Rewalked {
                        piece,
                        whole,
                        scratch,
                    })),
                })
                .is_ok()
            };
            // A closed channel means the pipeline is gone, or the loop let
            // the rewalk go, and no one is left to tell.
            let whole = loan.run(&shutdown, &mut |volume, posted, scratch| {
                tell(Some((volume, posted)), true, scratch) && hearing.recv().is_ok()
            });
            tell(None, whole, Default::default());
        });
    }

    pub(crate) fn lend(&mut self, node: NodeId, mut object: Box<dyn Object>) {
        self.serial += 1;
        let (scope, serial) = (self.scope, self.serial);
        let stop = Arc::new(AtomicBool::new(false));
        self.out.insert(node, (serial, Arc::clone(&stop)));
        #[cfg(test)]
        {
            self.lent += 1;
        }
        #[cfg(test)]
        if let Some(held) = &mut self.held {
            held.push((node, serial, object, stop));
            return;
        }
        let mail = self.mail.clone();
        let grant = self.hardware.grant();
        let clock = self.clock.clone();
        tokio::task::spawn_blocking(move || {
            let _job = crate::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let mut said = Vec::new();
            // Executing once a thread of the pool takes it, until it is done.
            grant.install(|| {
                let _decoding = clock.decoding();
                object.work_until(&stop, &mut said);
            });
            // A closed channel means the pipeline is gone, and no one is
            // left to tell.
            let _ = mail.blocking_send(Said {
                scope,
                node,
                generation: serial,
                heard: Heard::Worked(Box::new(Worked { object, said })),
            });
        });
    }
}
