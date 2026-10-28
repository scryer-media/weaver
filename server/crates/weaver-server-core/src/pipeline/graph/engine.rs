// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Running engines. An engine object reads its volumes as files, so its owner
// runs it: under one of the job's slots, on a blocking thread, over the
// node's envelope files (`volumes`). What the run says comes back through
// the one channel every graph job's runs share, which the pipeline awaits
// beside its other work, and is applied to the tree as if the object had said
// it in `deliver`.
//
// A run has one life: queued for a slot, running, ended, or stopped where it
// stands when the job goes down. Before it starts it reserves the resident
// bytes it will hold, against the limit every job shares; one that does not
// fit gives its slot back and waits for memory instead. A repair behind what
// it read moves its shelf's generation on, which stops it where it stands;
// its owner then retracts what it built and starts it again, under the slot
// it already holds. Whatever a stopped run still says carries the old
// generation, and is not heard: a run stopped as the job goes down keeps its
// generation, so no run starts under it again.

mod volumes;

#[cfg(test)]
pub(crate) mod echo;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use weaver_graph::holds::{HoldsAccountant, HoldsNamespace, Reserve};
use weaver_graph::slots::{Admissions, Asked, AskerId, Slots, Ticket};
use weaver_graph::{Emit, Extraction, NodeId, Why};

use self::volumes::RunVolumes;
use super::envelope::Shelf;
use super::hardware::Hardware;
use super::namespace::Returned;

// How many things a run may have said before it waits for the pipeline to
// hear them: a bound on the bytes in flight, not on the run.
const MAIL_CAPACITY: usize = 64;

// What a run holds resident beside the window its object declares: its own
// read and output buffers, the decoder's state outside its window, and up to
// MAIL_CAPACITY emits waiting for the pipeline to hear them. A declared
// window is a decoder's dictionary alone, so this is added to every
// reservation, fixed, so the whole is known before the run starts and never
// grows after.
pub(crate) const RUN_OVERHEAD: u64 = 4 << 20;

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
    // A recovery set's repair, lent off the loop, came back: addressed to
    // the set's node, under the loan's number for a generation.
    Repaired(Box<Returned>),
    // The job's checkpoints committed through this stamp, or a write of
    // them failed (`None`), while a member's file waited on them to move.
    // Addressed to the root, under no run's generation.
    Written {
        stamp: Option<u64>,
        control: crate::persistence::checkpoint_write::CheckpointControl,
    },
    CheckpointBlocked {
        control: crate::persistence::checkpoint_write::CheckpointControl,
        revision: u64,
        error: String,
    },
}

// Where every graph job's runs speak, and where the database says a
// checkpoint a move waits on has committed. That word comes from the
// database's writer, which must never wait on the pipeline, so it has a
// channel of its own without a bound: at most one word per checkpoint write.
#[derive(Debug)]
pub(crate) struct RunMail {
    sender: mpsc::Sender<Said>,
    receiver: mpsc::Receiver<Said>,
    written: mpsc::UnboundedSender<Said>,
    heard_written: mpsc::UnboundedReceiver<Said>,
}

impl Default for RunMail {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(MAIL_CAPACITY);
        let (written, heard_written) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver,
            written,
            heard_written,
        }
    }
}

impl RunMail {
    pub(crate) fn sender(&self) -> mpsc::Sender<Said> {
        self.sender.clone()
    }

    pub(crate) fn written(&self) -> mpsc::UnboundedSender<Said> {
        self.written.clone()
    }

    pub(crate) async fn recv(&mut self) -> Option<Said> {
        tokio::select! {
            biased;
            Some(said) = self.heard_written.recv() => Some(said),
            said = self.receiver.recv() => said,
        }
    }

    // What the runs said that waits to be heard, at most what the mail holds
    // at once: a run speaks its output a piece at a time, so the pipeline
    // hears all of it each turn, and a run that keeps speaking cannot keep
    // the turn.
    pub(crate) fn waiting(&mut self) -> Vec<Said> {
        let mut waiting = Vec::new();
        while let Ok(said) = self.heard_written.try_recv() {
            waiting.push(said);
        }
        while waiting.len() < MAIL_CAPACITY {
            let Ok(said) = self.receiver.try_recv() else {
                break;
            };
            waiting.push(said);
        }
        waiting
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Queued,
    // Its reservation did not fit beside what is resident. It holds no
    // slot, and asks for one again once fewer resident bytes are held than
    // when it was turned away.
    Starved { resident: u64 },
    Running { generation: u64 },
    Ended { generation: u64 },
    // Stopped where it stood because the job is going down, under the
    // generation it ran under, if it started. Its thread may still be out
    // and speak under that generation, so the run keeps it: no other run
    // starts on the node, and what the stopped one says is not heard. It did
    // not finish reading, so the job is not quiet while it stands.
    Stopped { generation: Option<u64> },
}

#[derive(Debug)]
struct Run {
    life: Life,
    ticket: Option<Ticket>,
    // The reader the run is out with, through which its owner stops it.
    volumes: Option<Arc<RunVolumes>>,
    // The resident bytes reserved for the run, held from before it starts
    // until it ends or is forgotten; zero while none is taken.
    reserved: u64,
}

// The e2e hook for a job that is extracting, on the run's own thread: with
// every volume the run reads in, it still has members to say or its end to
// report, so a hold here keeps the job from finishing and stops nothing else.
fn extracting_hook(volumes: &RunVolumes) {
    let hook = crate::e2e_failpoint::STATUS_ENTER_EXTRACTING;
    if crate::e2e_failpoint::armed(hook) && volumes.all_in() {
        crate::e2e_failpoint::fire(hook);
    }
}

// Whether a run may start on the memory it declares.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Reservation {
    Granted,
    // It does not fit beside what is resident now: its slot went to the
    // nodes named, which may start now.
    Starved(Vec<NodeId>),
    // It is larger than the resident limit itself, so it can never fit.
    Refused,
}

// A run's last word, said when the run's thread lets it go, however the run
// returned.
struct Ends {
    mail: mpsc::Sender<Said>,
    said: Option<Said>,
}

impl Drop for Ends {
    fn drop(&mut self) {
        if let Some(said) = self.said.take() {
            let _ = self.mail.blocking_send(said);
        }
    }
}

// One job's runs, the slots they take, and the resident bytes they reserve.
#[derive(Debug)]
pub(crate) struct Runs {
    scope: u64,
    mail: mpsc::Sender<Said>,
    slots: Slots,
    accountant: Arc<HoldsAccountant>,
    runs: BTreeMap<NodeId, Run>,
    // What a run is granted as it starts, and the job's repairs too.
    hardware: Hardware,
}

impl Runs {
    pub(crate) fn new(
        scope: u64,
        slots: usize,
        mail: mpsc::Sender<Said>,
        accountant: Arc<HoldsAccountant>,
    ) -> Self {
        Self {
            scope,
            mail,
            slots: Slots::new(slots),
            accountant,
            runs: BTreeMap::new(),
            hardware: Hardware::default(),
        }
    }

    // The runs, granted what `hardware` grants as each starts.
    pub(crate) fn on(mut self, hardware: Hardware) -> Self {
        self.hardware = hardware;
        self
    }

    pub(crate) fn hardware(&self) -> Hardware {
        self.hardware.clone()
    }

    // The job's id and where its runs speak, which its repairs share.
    pub(crate) fn scope(&self) -> u64 {
        self.scope
    }

    pub(crate) fn mail(&self) -> mpsc::Sender<Said> {
        self.mail.clone()
    }

    // A run is waiting for a slot, waiting for memory, running, or stopped
    // before it finished.
    pub(crate) fn live(&self) -> bool {
        self.runs
            .values()
            .any(|run| !matches!(run.life, Life::Ended { .. }))
    }

    pub(crate) fn in_flight(&self) -> bool {
        self.runs
            .values()
            .any(|run| matches!(run.life, Life::Running { .. }))
    }

    pub(crate) fn has(&self, node: NodeId) -> bool {
        self.runs.contains_key(&node)
    }

    // Whether the node's run is waiting for a slot, waiting for memory,
    // running, or stopped before it finished: it may still announce members,
    // or close them.
    pub(crate) fn reading(&self, node: NodeId) -> bool {
        self.runs
            .get(&node)
            .is_some_and(|run| !matches!(run.life, Life::Ended { .. }))
    }

    // The resident bytes reserved for the node's run.
    #[cfg(test)]
    pub(crate) fn reserved(&self, node: NodeId) -> u64 {
        self.runs.get(&node).map_or(0, |run| run.reserved)
    }

    // The nodes holding a slot, in node order.
    #[cfg(test)]
    pub(crate) fn holding(&self) -> Vec<NodeId> {
        self.runs
            .iter()
            .filter(|(_, run)| run.ticket.is_some())
            .map(|(node, _)| *node)
            .collect()
    }

    // The generation the node's run started under, once it started.
    pub(crate) fn generation(&self, node: NodeId) -> Option<u64> {
        match self.runs.get(&node)?.life {
            Life::Queued | Life::Starved { .. } => None,
            Life::Running { generation } | Life::Ended { generation } => Some(generation),
            Life::Stopped { generation } => generation,
        }
    }

    // Whether what was said under `generation` is the node's run speaking.
    pub(crate) fn current(&self, node: NodeId, generation: u64) -> bool {
        self.runs
            .get(&node)
            .is_some_and(|run| run.life == Life::Running { generation })
    }

    // The node wants to run, or to run again, under the slot limit the
    // pipeline reads now. Returns the nodes that may start now: the node
    // itself when it holds a slot or one was free, and any a raised limit
    // or released memory admitted.
    pub(crate) fn ask(&mut self, node: NodeId, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        admitted.extend(self.wake());
        let run = self.runs.entry(node).or_insert(Run {
            life: Life::Queued,
            ticket: None,
            volumes: None,
            reserved: 0,
        });
        if run.ticket.is_some() {
            if !admitted.contains(&node) {
                admitted.push(node);
            }
            return admitted;
        }
        admitted.extend(self.queue(node));
        admitted
    }

    // Reserve what the node's run holds resident before it starts: the
    // window its object declares and the run's fixed overhead. A
    // reservation is taken once and kept across restarts; a window only
    // narrows as the object's first bytes say more, so a restarted run
    // never needs more than the first.
    pub(crate) fn reserve(&mut self, node: NodeId, window: u64) -> Reservation {
        let namespace = self.namespace(node);
        let Some(run) = self.runs.get_mut(&node) else {
            return Reservation::Granted;
        };
        if run.reserved > 0 {
            return Reservation::Granted;
        }
        let len = window.saturating_add(RUN_OVERHEAD);
        if len > self.accountant.budgets().resident_limit {
            return Reservation::Refused;
        }
        if self.accountant.reserve_resident(namespace, len) == Reserve::Granted {
            run.reserved = len;
            return Reservation::Granted;
        }
        run.life = Life::Starved {
            resident: self.accountant.usage().resident,
        };
        let admissions = run
            .ticket
            .take()
            .map(|ticket| self.slots.release(ticket))
            .unwrap_or_default();
        Reservation::Starved(self.admit(admissions))
    }

    // Start the node's run over its shelf, on a blocking thread, with the
    // budget in force now: a later profile reaches the next run, not this.
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
        let volumes = Arc::new(RunVolumes::new(shelf, generation));
        run.volumes = Some(Arc::clone(&volumes));
        let budget = self.hardware.grant().run;
        let (scope, mail) = (self.scope, self.mail.clone());
        let said = move |heard| Said {
            scope,
            node,
            generation,
            heard,
        };
        tokio::task::spawn_blocking(move || {
            // Said as the run returns, and as a run that panics unwinds, so
            // a run always ends: its slot, its memory and a stopped job's
            // graph are waiting on it. A closed channel means the pipeline is
            // gone, and no one is left to tell.
            let _ends = Ends {
                mail: mail.clone(),
                said: Some(said(Heard::Ended)),
            };
            let reader = Arc::clone(&volumes);
            let mut sink = |emit: Emit| {
                if matches!(emit, Emit::Member { .. }) {
                    crate::e2e_failpoint::fire(crate::e2e_failpoint::EXTRACT_MEMBER_START);
                }
                extracting_hook(&volumes);
                volumes.current() && mail.blocking_send(said(Heard::Emit(emit))).is_ok()
            };
            extraction.run(reader, &mut sink, budget);
            // A run that read bytes nothing will bring, and did not read on
            // past them, built on less than its volumes hold and ends silent
            // on the failed read: what it said is refused as damage nothing
            // repairs.
            if volumes.lost() {
                sink(Emit::Refuse(Why::Unrepairable));
            }
            extracting_hook(&volumes);
        });
    }

    // The node's run returned. Its slot and its memory go to whoever waits;
    // returns the nodes that may start now.
    pub(crate) fn ended(&mut self, node: NodeId, generation: u64, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        if let Some(run) = self.runs.get_mut(&node) {
            run.life = Life::Ended { generation };
            admitted.extend(self.free(node));
        }
        admitted.extend(self.wake());
        admitted
    }

    // Forget the runs of nodes no longer among `engines`, which are in node
    // order: refused, retracted or unbound. Their slots and their memory go
    // to whoever waits; returns the nodes that may start now.
    pub(crate) fn forget_gone(&mut self, engines: &[NodeId], limit: usize) -> Vec<NodeId> {
        let gone = self
            .runs
            .keys()
            .filter(|node| engines.binary_search(node).is_err())
            .copied()
            .collect();
        self.forget(gone, limit)
    }

    // Stop every run where it stands: the job is going down. What a run read
    // is on its shelf, for the restored job to read again. Its slot and its
    // memory are given back, but the run stays, stopped under its generation:
    // its thread may still speak under it, and a run started in its place
    // would read under the same generation and hear it as its own.
    pub(crate) fn stop(&mut self, limit: usize) {
        self.request_stop();
        self.follow(limit);
        let all: Vec<NodeId> = self.runs.keys().copied().collect();
        // Out of the queue first, so a slot one of them frees cannot go to
        // another of them.
        for node in &all {
            self.slots.withdraw(asker(*node));
        }
        for node in all {
            self.free(node);
            if let Some(run) = self.runs.get_mut(&node) {
                run.life = match run.life {
                    Life::Queued | Life::Starved { .. } => Life::Stopped { generation: None },
                    Life::Running { generation } => Life::Stopped {
                        generation: Some(generation),
                    },
                    life @ (Life::Ended { .. } | Life::Stopped { .. }) => life,
                };
            }
        }
    }

    pub(crate) fn request_stop(&self) {
        for run in self.runs.values() {
            if let Some(volumes) = &run.volumes {
                volumes.stop();
            }
        }
    }

    // Forget the runs of `gone`, whose threads may still be out: what they
    // say is under a generation no run holds, and is not heard. Their slots
    // and their memory go to whoever waits; returns the nodes that may
    // start now.
    fn forget(&mut self, gone: Vec<NodeId>, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        // Out of the queue first, so a slot one of them frees cannot go to
        // another of them.
        for node in &gone {
            self.slots.withdraw(asker(*node));
        }
        for node in gone {
            admitted.extend(self.free(node));
            self.runs.remove(&node);
        }
        admitted.extend(self.wake());
        admitted
    }

    // The limit as the pipeline reads it now. A rise admits waiters at
    // once; a fall lets every holder finish, and releases admit no one
    // until the count is under it.
    fn follow(&mut self, limit: usize) -> Vec<NodeId> {
        let admissions = self.slots.set_limit(limit);
        self.admit(admissions)
    }

    // Starved runs ask for a slot again once resident bytes were released
    // since they were turned away, in the order they first asked.
    fn wake(&mut self) -> Vec<NodeId> {
        let resident = self.accountant.usage().resident;
        let woken: Vec<NodeId> = self
            .runs
            .iter()
            .filter(
                |(_, run)| matches!(run.life, Life::Starved { resident: then } if resident < then),
            )
            .map(|(node, _)| *node)
            .collect();
        woken
            .into_iter()
            .flat_map(|node| self.queue(node))
            .collect()
    }

    // The node waits for a slot, or takes a free one.
    fn queue(&mut self, node: NodeId) -> Option<NodeId> {
        let run = self.runs.get_mut(&node)?;
        run.life = Life::Queued;
        match self.slots.ask(asker(node)) {
            Asked::Admitted(ticket) => {
                run.ticket = Some(ticket);
                Some(node)
            }
            Asked::Queued | Asked::Holding => None,
        }
    }

    // Give back the node's reservation and its slot.
    fn free(&mut self, node: NodeId) -> Vec<NodeId> {
        let namespace = self.namespace(node);
        let Some(run) = self.runs.get_mut(&node) else {
            return Vec::new();
        };
        let reserved = std::mem::take(&mut run.reserved);
        if reserved > 0 {
            self.accountant.release_resident(namespace, reserved);
        }
        let admissions = run
            .ticket
            .take()
            .map(|ticket| self.slots.release(ticket))
            .unwrap_or_default();
        self.admit(admissions)
    }

    fn namespace(&self, node: NodeId) -> HoldsNamespace {
        HoldsNamespace {
            scope: self.scope,
            node,
        }
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

// A job that goes away gives back what its runs reserved, or every other
// job would count it against the resident limit for good.
impl Drop for Runs {
    fn drop(&mut self) {
        for (node, run) in &self.runs {
            if run.reserved > 0 {
                self.accountant
                    .release_resident(self.namespace(*node), run.reserved);
            }
        }
    }
}

fn asker(node: NodeId) -> AskerId {
    AskerId(u64::from(node.0))
}
