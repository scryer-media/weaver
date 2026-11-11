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
// bytes it will hold, from the decode pool every job shares and the holds do
// not; one that does not fit gives its slot back and waits for memory
// instead. A repair behind what it read moves its shelf's generation on,
// which stops it where it stands; its owner then retracts what it built and
// starts it again, under the slot it already holds. Whatever a stopped run still says carries the old
// generation, and is not heard: a run stopped as the job goes down keeps its
// generation, so no run starts under it again.

mod pool;
mod volumes;

#[cfg(test)]
pub(crate) mod echo;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use weaver_graph::slots::{Admissions, Asked, AskerId, Slots, Ticket};
use weaver_graph::{Emit, Extraction, Marks, NodeId, Why};

pub(crate) use self::pool::DecodePool;
use self::volumes::RunVolumes;
use super::decoding::DecodeClock;
use super::envelope::Shelf;
use super::hardware::Hardware;
use super::namespace::Returned;
use crate::runtime::OverLimit;

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
    Replayed(Box<super::namespace::Replayed>),
    Verified(Box<super::namespace::Verified>),
    // An object lent off the loop for the work it held came back with what
    // it said: addressed to its node, under the loan's number.
    Worked(Box<super::job::Worked>),
    // A restored recovery object walked what of its volumes arrived again,
    // off the loop, and came back with what it said: addressed to its
    // node, under the loan's number.
    Rewalked(Box<super::namespace::Rewalked>),
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
    // The job's steps since its last checkpoint are old enough that the
    // write they join is due: the job's actor takes the checkpoint.
    // Addressed to the root, under no run's generation.
    CheckpointDue {
        control: crate::persistence::checkpoint_write::CheckpointControl,
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
    // Its reservation did not fit beside what the decode pool holds. It
    // holds no slot, and asks for one again once the pool was given room
    // back since it was turned away, or its limit rose.
    Starved { freed: u64, limit: u64 },
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
    // The bytes reserved for the run in the decode pool, held from before
    // it starts until it ends or is forgotten; zero while none is taken.
    reserved: u64,
    // The window the run decodes at past what its object declared: above
    // the grant's ceiling where the policy lets it go above, or within it,
    // the widest its object or its decoder named.
    wide: Option<u64>,
    // The window the run was given when it was reserved.
    window: u64,
    // Whether its reservation is held above the ceiling, apart from the
    // pool.
    above: bool,
    // Where that window is: within the grant, or past it in RAM.
    placed: Placed,
    // Its decoder is handed the job's lender, to borrow every window over
    // its grant as it asks for it: it borrows (`Engine::lends`), the policy
    // lets a window go over the ceiling, and the scratch has not refused it.
    lends: bool,
    // The start its lender was handed for, while it is lent: what the
    // lender heard under any other is not this run's.
    lent: Option<super::window::Start>,
    // The scratch refused it once: from here it is not lent, and its window
    // is placed in RAM.
    in_ram: bool,
    // The run refused a window its policy lets it have: it starts again,
    // wider, once it ends, and what it says before then is not heard.
    rerun: bool,
    // Whether it is counted among the runs waiting above the ceiling.
    whole: bool,
}

impl Run {
    fn queued() -> Self {
        Self {
            life: Life::Queued,
            ticket: None,
            volumes: None,
            reserved: 0,
            wide: None,
            window: 0,
            above: false,
            placed: Placed::Grant,
            lends: false,
            lent: None,
            in_ram: false,
            rerun: false,
            whole: false,
        }
    }
}

// Where a run's decode window is. A window a lending run borrows is on a
// scratch file, asked for as its decoder asks, and is no part of either.
#[derive(Clone, Copy, Debug)]
enum Placed {
    // Within the grant, in RAM.
    Grant,
    // Past what its object declared, in RAM, reserved whole: this many
    // bytes of it.
    Ram(u64),
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
    // It does not fit beside what the decode pool holds now: its slot went
    // to the nodes named, which may start now.
    Starved(Vec<NodeId>),
    // It is larger than the decode pool itself, so it can never fit:
    // `needed` bytes against the `allowed` pool.
    Refused { needed: u64, allowed: u64 },
    // Its window may go above the ceiling, but it needs more RAM than the
    // machine's `memory` leaves a run: `needed` bytes of window.
    Beyond { needed: u64, memory: u64 },
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

// One job's runs, the slots they take, and the bytes they reserve in the
// decode pool.
#[derive(Debug)]
pub(crate) struct Runs {
    scope: u64,
    mail: mpsc::Sender<Said>,
    slots: Slots,
    pool: Arc<DecodePool>,
    runs: BTreeMap<NodeId, Run>,
    // The nodes whose run came, went, or moved on in its life, each under
    // the generation of its latest change: a step reads again only the
    // nodes marked since the one it read at.
    marks: Marks<NodeId>,
    // What a run is granted as it starts, and the job's repairs too.
    hardware: Hardware,
    // Where the runs keep the time their decoders spend executing.
    clock: DecodeClock,
    // The scratch-file windows a run's decoders may borrow, when the job
    // lends any.
    windows: Option<Arc<super::window::Windows>>,
    // The nodes whose decoder's refusal stands only because the policy
    // holds runs to the ceiling: a job lifted of its memory limit would be
    // given the window refused, within what the machine leaves a run. A
    // refused node's run is forgotten, so this outlives it; a node that
    // runs again leaves it.
    held_back: BTreeSet<NodeId>,
}

impl Runs {
    pub(crate) fn new(
        scope: u64,
        slots: usize,
        mail: mpsc::Sender<Said>,
        pool: Arc<DecodePool>,
    ) -> Self {
        Self {
            scope,
            mail,
            slots: Slots::new(slots),
            pool,
            runs: BTreeMap::new(),
            marks: Marks::default(),
            hardware: Hardware::default(),
            clock: DecodeClock::default(),
            windows: None,
            held_back: BTreeSet::new(),
        }
    }

    // The runs, lending their decoders windows from `windows`.
    pub(crate) fn lending(mut self, windows: Arc<super::window::Windows>) -> Self {
        self.windows = Some(windows);
        self
    }

    // The runs, granted what `hardware` grants as each starts.
    pub(crate) fn on(mut self, hardware: Hardware) -> Self {
        self.hardware = hardware;
        self
    }

    // The runs, keeping the time they decode on `clock`.
    pub(crate) fn timed(mut self, clock: DecodeClock) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn clock(&self) -> DecodeClock {
        self.clock.clone()
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

    // The bytes reserved for the node's run.
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

    // The generation of the latest change to any run's life.
    pub(crate) fn marked_at(&self) -> u64 {
        self.marks.at()
    }

    // The nodes whose run's life changed since `at`, one forgotten among
    // them.
    pub(crate) fn changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.marks.since(at)
    }

    // The nodes whose runs are reading, in node order.
    pub(crate) fn running(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.runs
            .iter()
            .filter(|(_, run)| matches!(run.life, Life::Running { .. }))
            .map(|(node, _)| *node)
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
        self.marks.mark(node);
        let run = self.runs.entry(node).or_insert_with(Run::queued);
        if run.ticket.is_some() {
            if !admitted.contains(&node) {
                admitted.push(node);
            }
            return admitted;
        }
        admitted.extend(self.queue(node));
        admitted
    }

    // Reserve what the node's run holds resident before it starts, from the
    // decode pool: the window its object declares, no more than the
    // hardware lets one decoder hold, and the run's fixed overhead. A window
    // wider than that is the decoder's to refuse, with the sizes its headers
    // state. A reservation is taken once and kept across restarts; a window
    // only narrows as the object's first bytes say more, so a restarted run
    // never needs more than the first. It is bookkeeping: nothing is
    // allocated for it.
    #[cfg(test)]
    pub(crate) fn reserve(&mut self, node: NodeId, window: u64) -> Reservation {
        self.reserve_for(node, window, false)
    }

    // As `reserve`, for a run whose decoder borrows a window over its grant
    // from its lender (`Engine::lends`) when `lends` says so.
    //
    // Under every policy such a run is handed the job's lender and borrows
    // every window over its grant as its decoder asks for it: nothing is
    // lent before it starts. It reserves the window its object declares, to
    // the grant's window (`Grant::held`), and its overhead: what its decoder
    // keeps in RAM, for a lent window is scratch, not RAM. On efficient a
    // RAR naming a 4 GiB dictionary reserves 256 MiB and the overhead, not
    // the whole pool. A run the scratch refused (`widen`) is not lent
    // again: it is placed in RAM where the policy lets a run go above the
    // ceiling, and its refusal stands where it does not.
    //
    // Any other run whose window is over the ceiling, as its object
    // declares or as its decoder refused (`widen`), is placed in RAM above
    // the ceiling where the policy lets it go above, reserved whole apart
    // from the pool. It is admitted once it, every other run above the
    // ceiling, and the whole pool fit under what the machine leaves a run.
    // A window its decoder refused within the ceiling is reserved whole from
    // the pool. Under a policy that holds runs to the ceiling, a window a
    // run was widened to above it is dropped: the run is held to the
    // ceiling, and its decoder refuses with the sizes.
    //
    // Where no window can be mapped (`window::MAPS`), no run is lent: one
    // whose window is over the ceiling is placed in RAM from the start where
    // the policy lets it go above, and is refused where it does not.
    pub(crate) fn reserve_for(&mut self, node: NodeId, window: u64, lends: bool) -> Reservation {
        let grant = self.hardware.grant();
        self.held_back.remove(&node);
        let Some(run) = self.runs.get_mut(&node) else {
            return Reservation::Granted;
        };
        if run.reserved > 0 {
            return Reservation::Granted;
        }
        let ceiling = grant.ceiling();
        let above = grant.over_limit == OverLimit::ScratchThenRam;
        run.lends = lends && !run.in_ram && self.windows.is_some() && super::window::MAPS;
        if above && !run.lends && window > ceiling {
            run.wide = run.wide.max(Some(window));
        }
        if !above {
            run.wide = run.wide.filter(|wide| *wide <= grant.held());
        }
        // The window a run had, which a refusal must pass to ask for more.
        // A lending run's decoder lends a window wider than its grant's, so
        // that is the most it had: a refusal over it was never held in RAM.
        let most = if run.lends { grant.held() } else { ceiling };
        run.window = run.wide.unwrap_or(window.min(most));
        let len = match run.wide {
            // What a run inside its grant reserves: the window it was
            // given, to the ceiling, and its overhead. That is the window
            // its object declares, or for a lending run no more than the
            // grant's window, which is all of it the run keeps in RAM.
            None => {
                run.placed = Placed::Grant;
                run.window.min(ceiling).saturating_add(RUN_OVERHEAD)
            }
            Some(wide) => {
                run.placed = Placed::Ram(wide);
                wide.saturating_add(RUN_OVERHEAD)
            }
        };
        let room = self.pool.room();
        let taken = if len <= room.limit {
            self.pool.stop_waiting(&mut run.whole);
            self.pool.take(len)
        } else if let Some(wide) = run.wide {
            let stop = grant.above_ceiling();
            if !self.pool.could_fit_above(len, stop) {
                self.pool.stop_waiting(&mut run.whole);
                run.placed = Placed::Grant;
                return Reservation::Beyond {
                    needed: wide,
                    memory: grant.machine_bytes.unwrap_or(u64::MAX),
                };
            }
            self.pool
                .take_above(len, stop, &mut run.whole)
                .map(|()| run.above = true)
        } else {
            return Reservation::Refused {
                needed: len,
                allowed: room.limit,
            };
        };
        let room = match taken {
            Ok(()) => {
                run.reserved = len;
                return Reservation::Granted;
            }
            Err(room) => room,
        };
        run.life = Life::Starved {
            freed: room.freed,
            limit: room.limit,
        };
        self.marks.mark(node);
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
        self.marks.mark(node);
        let volumes = Arc::new(RunVolumes::new(shelf, generation));
        run.volumes = Some(Arc::clone(&volumes));
        let grant = self.hardware.grant();
        let mut budget = grant.run.clone();
        // Only a decoder that borrows, and that the scratch has not refused,
        // is handed the lender, and borrows every window as it asks for it:
        // every other is held to its grant and refuses a window over it with
        // the sizes.
        let lending = self
            .windows
            .as_ref()
            .filter(|_| run.lends)
            .map(|windows| windows.for_node(node));
        run.lent = lending.as_ref().map(|(_, start)| *start);
        budget.lender = lending.map(|(lender, _)| lender);
        // The decoder may hold the window in RAM: what was reserved, the
        // whole of it, whatever its first bytes declare.
        if let Placed::Ram(wide) = run.placed {
            budget.memory_bytes = budget.memory_bytes.max(Some(wide));
            budget.window_bytes = Some(wide);
            budget.window_reserved = true;
        }
        let (scope, mail, clock) = (self.scope, self.mail.clone(), self.clock.clone());
        let said = move |heard| Said {
            scope,
            node,
            generation,
            heard,
        };
        tokio::task::spawn_blocking(move || {
            let _affinity = crate::runtime::affinity::PostprocessAffinity::enter();
            let _job = crate::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            // Said as the run returns, and as a run that panics unwinds, so
            // a run always ends: its slot, its memory and a stopped job's
            // graph are waiting on it. A closed channel means the pipeline is
            // gone, and no one is left to tell.
            let _ends = Ends {
                mail: mail.clone(),
                said: Some(said(Heard::Ended)),
            };
            // The decoder executes from here until it returns, but while it
            // waits on its volumes or on the job to hear it.
            let decoding = Arc::new(clock.decoding());
            volumes.decoding(Arc::downgrade(&decoding));
            let reader = Arc::clone(&volumes);
            let mut sink = |emit: Emit| {
                if matches!(emit, Emit::Member { .. }) {
                    crate::e2e_failpoint::fire(crate::e2e_failpoint::EXTRACT_MEMBER_START);
                }
                let _waiting = decoding.wait();
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
            drop(decoding);
            extracting_hook(&volumes);
        });
    }

    // The node's run returned. Its slot and its memory go to whoever waits;
    // returns the nodes that may start now.
    pub(crate) fn ended(&mut self, node: NodeId, generation: u64, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        if let Some(run) = self.runs.get_mut(&node)
            && std::mem::take(&mut run.rerun)
        {
            // Widened: it gives back what it reserved at the narrower
            // window and starts again under the slot it holds, reserving
            // the wider one.
            run.life = Life::Queued;
            run.volumes = None;
            run.placed = Placed::Grant;
            give_back(&self.pool, run);
            self.marks.mark(node);
            if run.ticket.is_some() {
                admitted.push(node);
            }
            admitted.extend(self.wake());
            return admitted;
        }
        if let Some(run) = self.runs.get_mut(&node) {
            run.life = Life::Ended { generation };
            self.marks.mark(node);
            admitted.extend(self.free(node));
        }
        admitted.extend(self.wake());
        admitted
    }

    // The node's run refused a window of `needed` bytes, wider than the
    // one it was given, that the grant lets it have: within the window the
    // grant holds one decoder to, or above it where the policy lets a run go
    // above. A decoder that does not say what it needs is given the grant's
    // window first, then the `widest` its format has where the policy lets
    // it go above. The run is stopped
    // where it stands, what it says from here is not heard, and once it
    // ends it starts again at that window. `false` when the grant holds it
    // narrower, or it already had that window: its refusal stands. Each
    // widening is wider than the last, so a run that refuses again with
    // nothing wider to ask for stands refused.
    pub(crate) fn widen(&mut self, node: NodeId, needed: Option<u64>, widest: Option<u64>) -> bool {
        let grant = self.hardware.grant();
        self.held_back.remove(&node);
        let Some(run) = self.runs.get_mut(&node) else {
            return false;
        };
        let held = grant.held();
        let running = matches!(run.life, Life::Running { .. });
        // A lending run whose lender found no folder with room for a window
        // it asked for was refused by the scratch, not by its grant: it
        // starts again, not lent, with the widest of every window it asked
        // for, lent or refused, what its decoder said it needs and the
        // window it had, in RAM under the machine's hard stop. Once only: a
        // run in RAM is lent nothing, so the scratch cannot refuse it again,
        // and what its decoder refuses from there is weighed as any run's
        // refusal is. Only what its lender heard under this start counts.
        // Where the policy lets nothing go above the ceiling, the scratch's
        // refusal stands, with the sizes, as a decoder's refusal over the
        // ceiling does.
        let refused = self
            .windows
            .as_ref()
            .zip(run.lent)
            .filter(|_| run.lends && !run.in_ram)
            .filter(|_| grant.over_limit == OverLimit::ScratchThenRam)
            .and_then(|(windows, start)| windows.refused(node, start));
        if let Some(refused) = refused
            && running
        {
            run.in_ram = true;
            run.wide = Some(run.window.max(refused).max(needed.unwrap_or(0)));
            run.rerun = true;
            if let Some(volumes) = &run.volumes {
                volumes.stop();
            }
            return true;
        }
        let most = match grant.over_limit {
            OverLimit::ScratchThenRam => u64::MAX,
            OverLimit::ScratchThenFail => held,
        };
        // What a run lifted of its memory limit would be given.
        let lifted = needed.or(widest);
        let Some(needed) = needed.or_else(|| {
            widest.map(|widest| match widest.min(held) {
                within if within > run.window => within,
                _ => widest.min(most),
            })
        }) else {
            return false;
        };
        let wider = needed <= most && needed > run.window && running;
        let held_back = !wider
            && grant.over_limit == OverLimit::ScratchThenFail
            && lifted.is_some_and(|lifted| {
                lifted > run.window.max(held)
                    && self
                        .pool
                        .could_fit_above(lifted.saturating_add(RUN_OVERHEAD), grant.above_ceiling())
            });
        if held_back {
            self.held_back.insert(node);
        }
        if wider {
            run.wide = Some(needed);
            run.rerun = true;
            if let Some(volumes) = &run.volumes {
                volumes.stop();
            }
        }
        wider
    }

    // What the node's run was allowed, when it is lent under a policy that
    // holds runs to the ceiling and the scratch refused it a window for want
    // of room: that refusal names no limit, and the run was held to the
    // grant's window. Any other refusal naming no limit is left so.
    pub(crate) fn allowed(&self, node: NodeId) -> Option<u64> {
        let grant = self.hardware.grant();
        let run = self
            .runs
            .get(&node)
            .filter(|run| run.lends && grant.over_limit == OverLimit::ScratchThenFail)?;
        let (windows, start) = self.windows.as_ref().zip(run.lent)?;
        windows.refused(node, start).map(|_| grant.held())
    }

    // Whether a refusal stands in the job only because the policy holds runs
    // to the ceiling, so a reprocess with no memory limit could finish it.
    pub(crate) fn held_to_ceiling(&self) -> bool {
        !self.held_back.is_empty()
    }

    // Whether the node's run was widened and is ending, to start again.
    pub(crate) fn rerunning(&self, node: NodeId) -> bool {
        self.runs.get(&node).is_some_and(|run| run.rerun)
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
                self.marks.mark(node);
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

    // Forget the runs of `gone`, nodes no longer engines: refused,
    // retracted or unbound. Their threads may still be out: what they say
    // is under a generation no run holds, and is not heard. Their slots and
    // their memory go to whoever waits; returns the nodes that may start
    // now, as a raised limit or released memory admits them even when
    // nothing is gone.
    pub(crate) fn forget(&mut self, gone: Vec<NodeId>, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        // Out of the queue first, so a slot one of them frees cannot go to
        // another of them.
        for node in &gone {
            self.slots.withdraw(asker(*node));
        }
        for node in gone {
            admitted.extend(self.free(node));
            if self.runs.remove(&node).is_some() {
                self.marks.mark(node);
            }
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

    // Whether a run turned away for memory may ask again: the decode pool
    // was given bytes back since, by this job or any other, or it grew.
    pub(crate) fn wakeable(&self) -> bool {
        let room = self.pool.room();
        self.runs.values().any(|run| woken(run.life, room))
    }

    // Starved runs ask for a slot again once the decode pool was given
    // bytes back since they were turned away, or it grew, in the order
    // they first asked.
    fn wake(&mut self) -> Vec<NodeId> {
        let room = self.pool.room();
        let woken: Vec<NodeId> = self
            .runs
            .iter()
            .filter(|(_, run)| woken(run.life, room))
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
        self.marks.mark(node);
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
        let Some(run) = self.runs.get_mut(&node) else {
            return Vec::new();
        };
        give_back(&self.pool, run);
        self.pool.stop_waiting(&mut run.whole);
        run.placed = Placed::Grant;
        run.lends = false;
        run.lent = None;
        run.in_ram = false;
        run.rerun = false;
        let admissions = run
            .ticket
            .take()
            .map(|ticket| self.slots.release(ticket))
            .unwrap_or_default();
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

// A job that goes away gives back what its runs reserved, or every other
// job would count it against the decode pool for good.
impl Drop for Runs {
    fn drop(&mut self) {
        for run in self.runs.values_mut() {
            give_back(&self.pool, run);
            self.pool.stop_waiting(&mut run.whole);
        }
    }
}

// Give back what the run reserved, to the pool or above the ceiling where it
// was taken, once.
fn give_back(pool: &DecodePool, run: &mut Run) {
    let len = std::mem::take(&mut run.reserved);
    if std::mem::take(&mut run.above) {
        pool.release_above(len);
    } else {
        pool.release(len);
    }
}

fn woken(life: Life, room: pool::Room) -> bool {
    matches!(life, Life::Starved { freed, limit } if room.freed > freed || room.limit > limit)
}

fn asker(node: NodeId) -> AskerId {
    AskerId(u64::from(node.0))
}
