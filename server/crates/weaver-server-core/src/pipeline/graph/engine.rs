// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Running engines. An engine object reads its volumes as files, so its owner
// runs it: under one of the job's slots, on a blocking thread, over the
// node's envelope files (`volumes`). What the run says comes back through
// the one channel every graph job's runs share, which the pipeline awaits
// beside its other work, and is applied to the tree as if the object had said
// it in `deliver`.
//
// A run has one life: queued for a slot, running, ended. A repair behind what
// it read moves its shelf's generation on, which stops it where it stands;
// its owner then retracts what it built and starts it again, under the slot
// it already holds. Whatever a stopped run still says carries the old
// generation, and is not heard.

mod volumes;

#[cfg(test)]
pub(crate) mod echo;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use weaver_graph::slots::{Admissions, Asked, AskerId, Slots, Ticket};
use weaver_graph::{Emit, Extraction, NodeId};

use self::volumes::RunVolumes;
use super::envelope::Shelf;

// How many things a run may have said before it waits for the pipeline to
// hear them: a bound on the bytes in flight, not on the run.
const MAIL_CAPACITY: usize = 64;

// What a run said, addressed to its job, its node, and the generation it
// runs under.
#[derive(Debug)]
pub(crate) struct Said {
    pub(crate) scope: u64,
    pub(crate) node: NodeId,
    pub(crate) generation: u64,
    pub(crate) heard: Heard,
}

#[derive(Debug)]
pub(crate) enum Heard {
    Emit(Emit),
    // The run returned: nothing more will be said under its generation.
    Ended,
}

// Where every graph job's runs speak.
#[derive(Debug)]
pub(crate) struct RunMail {
    sender: mpsc::Sender<Said>,
    receiver: mpsc::Receiver<Said>,
}

impl Default for RunMail {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(MAIL_CAPACITY);
        Self { sender, receiver }
    }
}

impl RunMail {
    pub(crate) fn sender(&self) -> mpsc::Sender<Said> {
        self.sender.clone()
    }

    pub(crate) async fn recv(&mut self) -> Option<Said> {
        self.receiver.recv().await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Queued,
    Running { generation: u64 },
    Ended { generation: u64 },
}

#[derive(Debug)]
struct Run {
    life: Life,
    ticket: Option<Ticket>,
}

// One job's runs and the slots they take.
#[derive(Debug)]
pub(crate) struct Runs {
    scope: u64,
    mail: mpsc::Sender<Said>,
    slots: Slots,
    runs: BTreeMap<NodeId, Run>,
}

impl Runs {
    pub(crate) fn new(scope: u64, slots: usize, mail: mpsc::Sender<Said>) -> Self {
        Self {
            scope,
            mail,
            slots: Slots::new(slots),
            runs: BTreeMap::new(),
        }
    }

    // A run is waiting for a slot or running.
    pub(crate) fn live(&self) -> bool {
        self.runs
            .values()
            .any(|run| !matches!(run.life, Life::Ended { .. }))
    }

    pub(crate) fn has(&self, node: NodeId) -> bool {
        self.runs.contains_key(&node)
    }

    // The generation the node's run started under, once it started.
    pub(crate) fn generation(&self, node: NodeId) -> Option<u64> {
        match self.runs.get(&node)?.life {
            Life::Queued => None,
            Life::Running { generation } | Life::Ended { generation } => Some(generation),
        }
    }

    // Whether what was said under `generation` is the node's run speaking.
    pub(crate) fn current(&self, node: NodeId, generation: u64) -> bool {
        self.runs
            .get(&node)
            .is_some_and(|run| run.life == Life::Running { generation })
    }

    // The node wants to run, or to run again. True when it may start now:
    // it holds a slot, or one was free.
    pub(crate) fn ask(&mut self, node: NodeId) -> bool {
        let run = self.runs.entry(node).or_insert(Run {
            life: Life::Queued,
            ticket: None,
        });
        if run.ticket.is_some() {
            return true;
        }
        run.life = Life::Queued;
        match self.slots.ask(asker(node)) {
            Asked::Admitted(ticket) => {
                run.ticket = Some(ticket);
                true
            }
            Asked::Queued | Asked::Holding => false,
        }
    }

    // Start the node's run over its shelf, on a blocking thread.
    pub(crate) fn start(
        &mut self,
        node: NodeId,
        extraction: Box<dyn Extraction>,
        shelf: Arc<Shelf>,
    ) {
        let Some(run) = self.runs.get_mut(&node) else {
            return;
        };
        let generation = shelf.begin();
        run.life = Life::Running { generation };
        let (scope, mail) = (self.scope, self.mail.clone());
        let said = move |heard| Said {
            scope,
            node,
            generation,
            heard,
        };
        tokio::task::spawn_blocking(move || {
            let volumes = Arc::new(RunVolumes::new(shelf, generation));
            let reader = Arc::clone(&volumes);
            let mut sink =
                |emit| volumes.current() && mail.blocking_send(said(Heard::Emit(emit))).is_ok();
            extraction.run(reader, &mut sink);
            // A closed channel means the pipeline is gone, and no one is
            // left to tell.
            let _ = mail.blocking_send(said(Heard::Ended));
        });
    }

    // The node's run returned. Its slot goes to whoever waits; returns the
    // nodes that may start now.
    pub(crate) fn ended(&mut self, node: NodeId, generation: u64) -> Vec<NodeId> {
        let Some(run) = self.runs.get_mut(&node) else {
            return Vec::new();
        };
        run.life = Life::Ended { generation };
        let admissions = run
            .ticket
            .take()
            .map(|ticket| self.slots.release(ticket))
            .unwrap_or_default();
        self.admit(admissions)
    }

    // Forget the runs of nodes no longer among `engines`: refused, retracted
    // or unbound. Their slots go to whoever waits; returns the nodes that
    // may start now.
    pub(crate) fn forget_gone(&mut self, engines: &[NodeId]) -> Vec<NodeId> {
        let gone: Vec<NodeId> = self
            .runs
            .keys()
            .filter(|node| !engines.contains(node))
            .copied()
            .collect();
        let mut admissions = Vec::new();
        for node in gone {
            let Some(run) = self.runs.remove(&node) else {
                continue;
            };
            match run.ticket {
                Some(ticket) => admissions.extend(self.slots.release(ticket)),
                None => {
                    self.slots.withdraw(asker(node));
                }
            }
        }
        self.admit(admissions)
    }

    fn admit(&mut self, admissions: Admissions) -> Vec<NodeId> {
        admissions
            .into_iter()
            .filter_map(|(asker, ticket)| {
                let node = NodeId(u32::try_from(asker.0).ok()?);
                self.runs.get_mut(&node)?.ticket = Some(ticket);
                Some(node)
            })
            .collect()
    }
}

fn asker(node: NodeId) -> AskerId {
    AskerId(u64::from(node.0))
}
