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

pub mod pool;
pub mod volumes;

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod echo;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use weaver_graph::slots::{Admissions, Asked, AskerId, Slots, Ticket};
use weaver_graph::{Emit, Extraction, Marks, NodeId, Why};

pub use self::pool::DecodePool;
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
pub const RUN_OVERHEAD: u64 = 4 << 20;

// What a run said, addressed to its job, its node, and the generation it
// runs under.
#[derive(Debug)]
pub struct Said {
    pub scope: u64,
    pub node: NodeId,
    pub generation: u64,
    pub heard: Heard,
}

#[derive(Debug)]
pub enum Heard {
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
    // The volumes' writers returned everything the job had submitted when
    // a checkpoint or the job's end began to wait for it. Addressed to the
    // root, under no run's generation.
    DiskReturned,
    // A volume's writer could not complete an operation on one of the
    // job's files. Addressed to the root, under no run's generation.
    DiskFailed(Box<weaver_server_disk::operations::disk::writer::Failure>),
    // The job's give-up has another volume to read back, on a turn of its
    // own. Addressed to the root, under no run's generation.
    GivingUp,
    // A run began to wait on its shelf after its job, waiting on nothing
    // but its runs, asked to hear of it: the job's gate looks again. Sent
    // once per ask, on the root's mail.
    Blocked,
}

// Where every graph job's runs speak, and where the database says a
// checkpoint a move waits on has committed. That word comes from the
// database's writer, which must never wait on the pipeline, so it has a
// channel of its own without a bound: at most one word per checkpoint write.
#[derive(Debug)]
pub struct RunMail {
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
    pub fn sender(&self) -> mpsc::Sender<Said> {
        self.sender.clone()
    }

    pub fn written(&self) -> mpsc::UnboundedSender<Said> {
        self.written.clone()
    }

    pub async fn recv(&mut self) -> Option<Said> {
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

// A job the pipeline does not hold, hearing its runs through the pipeline's
// mail: the step a run's word drives, with the slots the job may use now.
pub trait Hears: Send + Sync {
    fn said(&self, said: Said, slots: usize);
    // Every download paused or resumed: the job's extraction deadline
    // does not run while they are.
    fn hold_clock(&self, paused: bool);
    // The ticket the job took from the disk ledger as it began to wait for
    // room, which orders it among every job waiting, the pipeline's too.
    // A job that never waits for room has none.
    fn disk_ticket(&self) -> Option<u64> {
        None
    }
    // A disk the job waits on has a new reading: whether it has room again,
    // as `GraphJob::wake_disk` says for a job of the pipeline's. A job
    // that cannot be asked now is asked by its own task, and keeps its
    // place until then. A job that never waits for room has it.
    fn wake_disk(&self) -> bool {
        true
    }
    // What the job waits for room for, and the folder it waits on, as
    // `GraphJob::disk_wait` says for a job of the pipeline's: read as its
    // wait begins, so the pipeline says it once, as it says its own. A job
    // that does not wait, or never waits for room, has none.
    fn disk_wait(&self) -> Option<(super::limits::DiskWait, std::path::PathBuf)> {
        None
    }
}

// The jobs outside the pipeline whose runs speak on its mail, by scope,
// and whether every download is paused, which each job's clock follows
// from the moment it joins.
#[derive(Default)]
pub struct Outside(
    std::sync::Mutex<std::collections::HashMap<u64, Arc<dyn Hears>>>,
    std::sync::atomic::AtomicBool,
    DiskWaits,
);

// The jobs outside the pipeline that wait for room on a disk, by scope, and
// those that began to since the pipeline last looked; the pipeline's loop
// is rung as one begins, so it listens for the next reading.
#[derive(Default)]
struct DiskWaits {
    scopes: std::sync::Mutex<(std::collections::BTreeSet<u64>, Vec<u64>)>,
    began: tokio::sync::Notify,
}

impl Outside {
    fn jobs(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<u64, Arc<dyn Hears>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn join(&self, scope: u64, hears: Arc<dyn Hears>) {
        let mut jobs = self.jobs();
        hears.hold_clock(self.1.load(std::sync::atomic::Ordering::Relaxed));
        jobs.insert(scope, hears);
    }

    // Every download paused or resumed: each job's clock follows.
    pub fn hold_clocks(&self, paused: bool) {
        let jobs = self.jobs();
        self.1.store(paused, std::sync::atomic::Ordering::Relaxed);
        for hears in jobs.values() {
            hears.hold_clock(paused);
        }
    }

    pub fn leave(&self, scope: u64) {
        self.jobs().remove(&scope);
        self.disk_unheld(scope);
    }

    fn disk_scopes(
        &self,
    ) -> std::sync::MutexGuard<'_, (std::collections::BTreeSet<u64>, Vec<u64>)> {
        self.2
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // The job `scope` began to wait for room on a disk: the pipeline wakes
    // it with its own, in ticket order.
    pub fn disk_held(&self, scope: u64) {
        let mut scopes = self.disk_scopes();
        if scopes.0.insert(scope) {
            scopes.1.push(scope);
            drop(scopes);
            self.2.began.notify_one();
        }
    }

    // The job `scope` no longer waits for room.
    pub fn disk_unheld(&self, scope: u64) {
        let mut scopes = self.disk_scopes();
        if scopes.0.remove(&scope) {
            scopes.1.retain(|began| *began != scope);
        }
    }

    pub fn waits_for_disk(&self) -> bool {
        !self.disk_scopes().0.is_empty()
    }

    // The jobs that began to wait since the last look, still waiting.
    pub fn disk_waits_began(&self) -> Vec<u64> {
        std::mem::take(&mut self.disk_scopes().1)
    }

    // What the job `scope` waits for room for, and the folder it waits on,
    // if it is here and waits.
    pub fn disk_wait_of(
        &self,
        scope: u64,
    ) -> Option<(super::limits::DiskWait, std::path::PathBuf)> {
        let hears = self.jobs().get(&scope).cloned()?;
        hears.disk_wait()
    }

    // Rung once a job begins to wait while none of the pipeline's loop's
    // listening was for a reading.
    pub(crate) async fn disk_wait_began(&self) {
        self.2.began.notified().await;
    }

    // Every job that waits for room, with its ticket.
    pub fn disk_waiters(&self) -> Vec<(u64, u64, Arc<dyn Hears>)> {
        let scopes: Vec<u64> = self.disk_scopes().0.iter().copied().collect();
        if scopes.is_empty() {
            return Vec::new();
        }
        let jobs = self.jobs();
        scopes
            .into_iter()
            .filter_map(|scope| {
                let hears = jobs.get(&scope)?;
                Some((hears.disk_ticket().unwrap_or(0), scope, Arc::clone(hears)))
            })
            .collect()
    }

    // Hand `said` to the job it is addressed to, if that job is here.
    pub fn deliver(&self, said: Said, slots: usize) {
        let hears = self.jobs().get(&said.scope).cloned();
        if let Some(hears) = hears {
            hears.said(said, slots);
        }
    }
}

impl std::fmt::Debug for Outside {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Outside").field(&self.jobs().len()).finish()
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
    // The scratch refused it and the machine could never hold its window
    // in RAM: the job waits for the scratch's room.
    no_ram: bool,
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
            no_ram: false,
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
    let hook = weaver_server_base::e2e_failpoint::STATUS_ENTER_EXTRACTING;
    if weaver_server_base::e2e_failpoint::armed(hook) && volumes.all_in() {
        weaver_server_base::e2e_failpoint::fire(hook);
    }
}

// Whether a run may start on the memory it declares.
#[derive(Debug, PartialEq, Eq)]
pub enum Reservation {
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

// A run's read turn, given back when the run's thread lets it go: before
// its last word, which may wait for the job.
struct LetGo(Arc<RunVolumes>);

impl Drop for LetGo {
    fn drop(&mut self) {
        self.0.let_go();
    }
}

// One job's runs, the slots they take, and the bytes they reserve in the
// decode pool.
#[derive(Debug)]
pub struct Runs {
    scope: u64,
    mail: mpsc::Sender<Said>,
    slots: Slots,
    pub pool: Arc<DecodePool>,
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
    // How many of `runs` have ended, so whether one is live is a count,
    // not a walk of them.
    ended: usize,
    // The volumes the job's files are written through: a run waits on
    // their budget, on its own thread, before it says more.
    disk: Option<super::disk::JobDisk>,
}

impl Runs {
    pub fn new(scope: u64, slots: usize, mail: mpsc::Sender<Said>, pool: Arc<DecodePool>) -> Self {
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
            ended: 0,
            disk: None,
        }
    }

    // The runs, each waiting on the budget of the volumes of `disk` before
    // it says more, so what a run unpacks is never queued for a slow
    // volume faster than the volume takes it.
    pub(crate) fn writing(mut self, disk: Option<super::disk::JobDisk>) -> Self {
        self.disk = disk;
        self
    }

    // The runs, lending their decoders windows from `windows`.
    pub fn lending(mut self, windows: Arc<super::window::Windows>) -> Self {
        self.windows = Some(windows);
        self
    }

    // The runs, granted what `hardware` grants as each starts.
    pub fn on(mut self, hardware: Hardware) -> Self {
        self.hardware = hardware;
        self
    }

    // The runs, keeping the time they decode on `clock`.
    pub fn timed(mut self, clock: DecodeClock) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn clock(&self) -> DecodeClock {
        self.clock.clone()
    }

    pub fn hardware(&self) -> Hardware {
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
    pub fn live(&self) -> bool {
        debug_assert_eq!(
            self.ended,
            self.runs
                .values()
                .filter(|run| matches!(run.life, Life::Ended { .. }))
                .count(),
        );
        self.runs.len() > self.ended
    }

    pub fn in_flight(&self) -> bool {
        self.runs
            .values()
            .any(|run| matches!(run.life, Life::Running { .. }))
    }

    pub fn has(&self, node: NodeId) -> bool {
        self.runs.contains_key(&node)
    }

    // Whether the node's run is waiting for a slot, waiting for memory,
    // running, or stopped before it finished: it may still announce members,
    // or close them.
    pub fn reading(&self, node: NodeId) -> bool {
        self.runs
            .get(&node)
            .is_some_and(|run| !matches!(run.life, Life::Ended { .. }))
    }

    // The bytes reserved for the node's run.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn reserved(&self, node: NodeId) -> u64 {
        self.runs.get(&node).map_or(0, |run| run.reserved)
    }

    // The nodes holding a slot, in node order.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn holding(&self) -> Vec<NodeId> {
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

    // Whether a run waits for a slot or for memory, or was stopped and has
    // not ended: each is lifted by an event of its own. A run turned away
    // for memory is lifted by a release: one already made, or one another
    // job still holds bytes to make. When every byte the pool holds is this
    // job's, only its own runs can give one back, and a parent starved by
    // children that wait on it waits on them as they wait on it.
    pub fn held_back(&self) -> bool {
        let mut starved = false;
        let (mut pool, mut above) = (0_u64, 0_u64);
        for run in self.runs.values() {
            match run.life {
                Life::Queued | Life::Stopped { .. } => return true,
                Life::Starved { .. } => starved = true,
                Life::Running { .. } | Life::Ended { .. } => {}
            }
            if run.above {
                above = above.saturating_add(run.reserved);
            } else {
                pool = pool.saturating_add(run.reserved);
            }
        }
        if !starved {
            return false;
        }
        let room = self.pool.room();
        room.reserved > pool
            || room.above > above
            || self.runs.values().any(|run| woken(run.life, room))
    }

    // The nodes whose runs are reading, in node order.
    pub(crate) fn running(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.runs
            .iter()
            .filter(|(_, run)| matches!(run.life, Life::Running { .. }))
            .map(|(node, _)| *node)
    }

    // The generation the node's run started under, once it started.
    pub fn generation(&self, node: NodeId) -> Option<u64> {
        match self.runs.get(&node)?.life {
            Life::Queued | Life::Starved { .. } => None,
            Life::Running { generation } | Life::Ended { generation } => Some(generation),
            Life::Stopped { generation } => generation,
        }
    }

    // Whether what was said under `generation` is the node's run speaking.
    pub fn current(&self, node: NodeId, generation: u64) -> bool {
        self.runs
            .get(&node)
            .is_some_and(|run| run.life == Life::Running { generation })
    }

    // The node wants to run, or to run again, under the slot limit the
    // pipeline reads now. Returns the nodes that may start now: the node
    // itself when it holds a slot or one was free, and any a raised limit
    // or released memory admitted.
    pub fn ask(&mut self, node: NodeId, limit: usize) -> Vec<NodeId> {
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
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn reserve(&mut self, node: NodeId, window: u64) -> Reservation {
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
    // Where no window can be mapped (`weaver_server_disk::scratch::MAPS`), no run is lent: one
    // whose window is over the ceiling is placed in RAM from the start where
    // the policy lets it go above, and is refused where it does not.
    pub fn reserve_for(&mut self, node: NodeId, window: u64, lends: bool) -> Reservation {
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
        run.lends =
            lends && !run.in_ram && self.windows.is_some() && weaver_server_disk::scratch::MAPS;
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
        turn(
            &mut self.ended,
            &mut run.life,
            Life::Starved {
                freed: room.freed,
                limit: room.limit,
            },
        );
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
    pub fn start(&mut self, node: NodeId, extraction: Box<dyn Extraction>, shelf: Arc<Shelf>) {
        let Some(run) = self.runs.get_mut(&node) else {
            return;
        };
        let generation = shelf.begin();
        turn(&mut self.ended, &mut run.life, Life::Running { generation });
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
        let disk = self.disk.clone();
        let said = move |heard| Said {
            scope,
            node,
            generation,
            heard,
        };
        tokio::task::spawn_blocking(move || {
            let _affinity = weaver_server_base::runtime::affinity::PostprocessAffinity::enter();
            let _job =
                weaver_server_base::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
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
            // The run's read turn goes back as it ends, a panic's unwinding
            // included: the run's volumes outlive it.
            let _turns = LetGo(Arc::clone(&volumes));
            let reader = Arc::clone(&volumes);
            let mut sink = |emit: Emit| {
                if matches!(emit, Emit::Member { .. }) {
                    weaver_server_base::e2e_failpoint::fire(
                        weaver_server_base::e2e_failpoint::EXTRACT_MEMBER_START,
                    );
                }
                let _waiting = decoding.wait();
                extracting_hook(&volumes);
                // What the run said before is with the volumes' writers
                // or on its way: no more is said while a volume holds
                // its budget of it.
                if let Some(disk) = &disk {
                    disk.wait_below();
                }
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
            volumes.decoded(decoding.spent());
            drop(decoding);
            extracting_hook(&volumes);
        });
    }

    // The node's run returned. Its slot and its memory go to whoever waits;
    // returns the nodes that may start now.
    pub fn ended(&mut self, node: NodeId, generation: u64, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        if let Some(run) = self.runs.get_mut(&node)
            && run.volumes.as_ref().is_some_and(|volumes| volumes.parked())
        {
            // Stopped for the job to wait for room: it gives back what it
            // reserved and its slot, and waits for a slot again, to run
            // again from the start of its volumes. While the job waits its
            // limit is none, so the slot goes to no one of the job's. Its
            // part of the job's decoding clock is given back: it decodes
            // all of it again.
            if let Some(volumes) = &run.volumes {
                self.clock.discard(volumes.decode_time());
            }
            turn(&mut self.ended, &mut run.life, Life::Queued);
            run.volumes = None;
            run.placed = Placed::Grant;
            run.rerun = false;
            give_back(&self.pool, run);
            self.pool.stop_waiting(&mut run.whole);
            let admissions = run
                .ticket
                .take()
                .map(|ticket| self.slots.release(ticket))
                .unwrap_or_default();
            self.marks.mark(node);
            admitted.extend(self.admit(admissions));
            admitted.extend(self.queue(node));
            admitted.extend(self.wake());
            return admitted;
        }
        if let Some(run) = self.runs.get_mut(&node)
            && std::mem::take(&mut run.rerun)
        {
            // Widened: it gives back what it reserved at the narrower
            // window and starts again under the slot it holds, reserving
            // the wider one.
            turn(&mut self.ended, &mut run.life, Life::Queued);
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
            turn(&mut self.ended, &mut run.life, Life::Ended { generation });
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
    pub fn widen(&mut self, node: NodeId, needed: Option<u64>, widest: Option<u64>) -> bool {
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
        // Where the policy lets nothing go above the ceiling, the job waits
        // for the room the window takes (`window_needs`).
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
            // The run starts again in RAM only when the machine could ever
            // hold it there. One it never could is not refused for want of
            // disk room: the job waits for the room the window takes, as
            // under a policy that lets nothing go above the ceiling.
            let wide = run.window.max(refused).max(needed.unwrap_or(0));
            if !self
                .pool
                .could_fit_above(wide.saturating_add(RUN_OVERHEAD), grant.above_ceiling())
            {
                run.no_ram = true;
                return false;
            }
            run.in_ram = true;
            run.wide = Some(wide);
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
    pub fn allowed(&self, node: NodeId) -> Option<u64> {
        let grant = self.hardware.grant();
        let run = self
            .runs
            .get(&node)
            .filter(|run| run.lends && grant.over_limit == OverLimit::ScratchThenFail)?;
        let (windows, start) = self.windows.as_ref().zip(run.lent)?;
        windows.refused(node, start).map(|_| grant.held())
    }

    // What the disk has to show free for the window the scratch refused the
    // node's run for want of room, under a policy that holds runs to the
    // ceiling: the widest window the run asked for and the free space the
    // disk keeps. The job waits for that room; it is not failed.
    pub fn window_needs(&self, node: NodeId) -> Option<u64> {
        let grant = self.hardware.grant();
        let run = self.runs.get(&node).filter(|run| {
            run.lends && (grant.over_limit == OverLimit::ScratchThenFail || run.no_ram)
        })?;
        let (windows, start) = self.windows.as_ref().zip(run.lent)?;
        windows
            .refused(node, start)
            .map(|widest| widest.saturating_add(windows.kept_free()))
    }

    // Whether a quota refused a window's file since this was last asked.
    pub(crate) fn window_quota(&self) -> bool {
        self.windows
            .as_ref()
            .is_some_and(|windows| windows.take_quota())
    }

    // Whether a refusal stands in the job only because the policy holds runs
    // to the ceiling, so a reprocess with no memory limit could finish it.
    pub fn held_to_ceiling(&self) -> bool {
        !self.held_back.is_empty()
    }

    // Whether the node's run was widened and is ending, to start again.
    pub fn rerunning(&self, node: NodeId) -> bool {
        self.runs.get(&node).is_some_and(|run| run.rerun)
    }

    // The node's run wrote while its job waited for room on a disk: it
    // stops where it stands, and once it ended it gives back its memory and
    // its slot, and runs again from the start once the job has room. What
    // it said already landed; nothing it says from here is heard.
    // A run widened, stopped already to start again, is parked as well: it
    // starts again at the wider window once the job has room.
    pub fn park(&mut self, node: NodeId) {
        if let Some(run) = self.runs.get(&node)
            && let Some(volumes) = run.volumes.as_ref()
            && (volumes.current() || run.rerun)
        {
            volumes.park();
        }
    }

    // Whether the run stopped for its job to wait for room on a disk, and is
    // ending to run again from the start.
    pub fn parked(&self, node: NodeId) -> bool {
        self.runs.get(&node).is_some_and(|run| {
            matches!(run.life, Life::Running { .. })
                && run.volumes.as_ref().is_some_and(|volumes| volumes.parked())
        })
    }

    // Stop every run where it stands: the job is going down. What a run read
    // is on its shelf, for the restored job to read again. Its slot and its
    // memory are given back, but the run stays, stopped under its generation:
    // its thread may still speak under it, and a run started in its place
    // would read under the same generation and hear it as its own.
    pub fn stop(&mut self, limit: usize) {
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
                let life = match run.life {
                    Life::Queued | Life::Starved { .. } => Life::Stopped { generation: None },
                    Life::Running { generation } => Life::Stopped {
                        generation: Some(generation),
                    },
                    life @ (Life::Ended { .. } | Life::Stopped { .. }) => life,
                };
                turn(&mut self.ended, &mut run.life, life);
            }
        }
    }

    pub fn request_stop(&self) {
        for run in self.runs.values() {
            if let Some(volumes) = &run.volumes {
                volumes.stop();
            }
        }
    }

    // Forget the runs of `gone`, nodes no longer engines: refused,
    // retracted, unbound or taken away. Each is stopped where it stands, as
    // a stopped job's runs are, so a reader waiting on a shelf nothing feeds
    // again wakes and its thread returns. What a thread says from here is
    // under a generation no run holds, and is not heard. Their slots and
    // their memory go to whoever waits; returns the nodes that may start
    // now, as a raised limit or released memory admits them even when
    // nothing is gone.
    pub fn forget(&mut self, gone: Vec<NodeId>, limit: usize) -> Vec<NodeId> {
        let mut admitted = self.follow(limit);
        // Out of the queue first, so a slot one of them frees cannot go to
        // another of them.
        for node in &gone {
            self.slots.withdraw(asker(*node));
        }
        for node in gone {
            admitted.extend(self.free(node));
            if let Some(run) = self.runs.remove(&node) {
                if matches!(run.life, Life::Ended { .. }) {
                    self.ended -= 1;
                }
                if let Some(volumes) = &run.volumes {
                    volumes.stop();
                }
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
    pub fn wakeable(&self) -> bool {
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
        turn(&mut self.ended, &mut run.life, Life::Queued);
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
        run.no_ram = false;
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

// A run moves on in its life, and the count of runs ended follows it.
fn turn(ended: &mut usize, life: &mut Life, to: Life) {
    let was = matches!(*life, Life::Ended { .. });
    let is = matches!(to, Life::Ended { .. });
    *life = to;
    match (was, is) {
        (false, true) => *ended += 1,
        (true, false) => *ended -= 1,
        _ => {}
    }
}

fn woken(life: Life, room: pool::Room) -> bool {
    matches!(life, Life::Starved { freed, limit } if room.freed > freed || room.limit > limit)
}

fn asker(node: NodeId) -> AskerId {
    AskerId(u64::from(node.0))
}

// What the unit tests compiled apart in tests/pipeline_graph.rs reach in
// this module's private modules.
#[cfg(feature = "pipeline-test-fixtures")]
#[doc(hidden)]
pub mod units {
    pub mod pool {
        pub use super::super::pool::*;
    }
}
