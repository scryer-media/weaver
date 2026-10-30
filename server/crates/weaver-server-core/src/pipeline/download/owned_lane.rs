// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

pub(crate) struct OwnedDownloadLanePool {
    shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    #[cfg(test)]
    reset_calls: AtomicUsize,
}

// Everything the pool and its workers agree on: who is idle, what connection
// each idle worker is holding, and the runs no worker was free to take.
//
// One lock rather than a marker per worker, because the two decisions that
// matter are joint ones. A submit has to pick a worker *and* mark it taken in
// the same breath, or two submits choose the same lane; a worker has to look
// for queued work *and* publish itself idle in the same breath, or a run is
// pushed into the gap between the two and waits for a wake-up that has
// already happened. It is taken at lease boundaries only — never per article.
//
// Worker threads are started on demand, up to `cap`, and leave after a long
// idle spell. A slot keeps its index for the life of the pool, empty or not:
// a socket-recall hook captured by an idle worker addresses its slot by
// index, and a worker leaving must not shift the ones beside it.
struct OwnedLanePoolShared {
    workers: Vec<Option<OwnedLaneWorkerSlot>>,
    // Runs and probes that arrived while every worker was busy. Whichever
    // worker finishes first drains this before it publishes itself idle, so
    // a lease never sits behind one busy worker's private channel.
    queued_runs: VecDeque<QueuedOwnedLaneWork>,
    // The most worker threads that may be alive at once: the configured
    // connection count.
    cap: usize,
    // Tells a worker apart from an earlier one that sat in the same slot.
    next_worker_id: u64,
    intervals: OwnedLaneIntervals,
    #[cfg(test)]
    observer: Option<std_mpsc::Sender<OwnedLaneObservation>>,
}

// How often an idle worker wakes, and how long it stays idle before its
// thread leaves. Fixed in production; tests shorten them.
#[derive(Clone, Copy)]
struct OwnedLaneIntervals {
    liveness: Duration,
    idle_release: Duration,
}

impl Default for OwnedLaneIntervals {
    fn default() -> Self {
        Self {
            liveness: CACHED_LANE_LIVENESS_INTERVAL,
            idle_release: LANE_IDLE_RELEASE,
        }
    }
}

// How long a worker thread may sit idle before it leaves. A machine with
// many configured connections and little to download should not keep a
// thread per connection parked forever; the next lease starts a new one.
const LANE_IDLE_RELEASE: Duration = Duration::from_secs(5 * 60);

// Which worker a thread is: its slot, and which occupant of that slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OwnedLaneSeat {
    index: usize,
    id: u64,
}

// What the pool's workers did, for tests that wait on the pool rather than
// on the clock.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnedLaneObservation {
    Spawned(usize),
    Idle(usize),
    Vacated(usize),
}

// How a new worker thread begins.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OwnedLaneWorkerStart {
    // Claimed by the submit that started it: its first command is the work
    // it was started for, and it must not publish itself idle before that.
    Claimed,
    // Free from the start: it drains the queue and then publishes itself.
    Idle,
}

// One step of an idle worker's release.
enum OwnedLaneRelease {
    // A command was routed here meanwhile; take it instead of leaving.
    Stay,
    // Queued work found on the way out.
    Work(QueuedOwnedLaneWork),
    // Withdrawn from claims; its connection can be let go before it leaves.
    Withdrawn,
    // The slot is vacated (or no longer this worker's) and the thread ends.
    Left,
}

// Work routed to a worker: a lease to run, or a health probe batch to ask.
enum QueuedOwnedLaneWork {
    Run(Box<OwnedLaneRun>),
    Probe(Box<OwnedLaneProbe>),
}

// One health probe batch, seated by the dispatch pass on `seat`.
//
// A probe is answered by a worker between leases, on the connection it
// keeps to `seat` or on one it opens there the way a lease pinned to that
// server would: it never shares a connection with articles in flight. The
// answer goes back through the probes' mail with the batch, whatever it is.
pub(crate) struct OwnedLaneProbe {
    nntp: Arc<weaver_nntp::NntpClient>,
    seat: weaver_nntp::pool::ServerId,
    ids: Vec<String>,
    batch: crate::pipeline::post_health::ProbeBatch,
    mail: mpsc::Sender<crate::pipeline::post_health::ProbeMail>,
}

// What a reset fenced off before any worker could start it: the leases, for
// the scheduler, and the probe batches, to wait for a seat again.
pub(crate) struct StoppedOwnedLaneWork {
    pub(crate) leases: Vec<DownloadBatchLease>,
    pub(crate) probes: Vec<crate::pipeline::post_health::ProbeBatch>,
}

struct OwnedLaneWorkerSlot {
    id: u64,
    sender: std_mpsc::Sender<OwnedLanePoolCommand>,
    // `Some` while the worker sits between runs and no run has been routed to
    // it yet. Taking it is the claim: two concurrent submits cannot choose
    // the same worker, and a worker that is already spoken for is not offered
    // a second lease.
    idle: Option<IdleOwnedLaneWorker>,
}

#[derive(Clone, Default)]
struct IdleOwnedLaneWorker {
    // The connection this worker kept across its park, when it kept one.
    lane: Option<IdleOwnedLane>,
}

// What an idle worker's cached connection can serve, published so a submit
// can route to it without touching the worker.
#[derive(Clone)]
struct IdleOwnedLane {
    // The client the connection belongs to. Weak, so a published marker never
    // keeps a retired client alive: the worker itself holds the strong
    // reference for exactly as long as the connection is cached, and a marker
    // whose client is gone simply matches nothing.
    nntp: std::sync::Weak<weaver_nntp::NntpClient>,
    server: weaver_nntp::pool::ServerId,
}

impl IdleOwnedLane {
    fn from_cached(cached: &CachedOwnedLane) -> Self {
        Self {
            nntp: Arc::downgrade(&cached.nntp),
            server: cached.lane.server_id(),
        }
    }

    // Whether this connection could take `run` without redialling.
    //
    // Deliberately cheap and deliberately advisory: it asks only what can be
    // answered from the published marker — same client and a server the
    // lease does not exclude. The newsgroups the connection was opened for do
    // not matter: the worker re-points its socket at the lease's groups when
    // they differ. Quota and health are the worker's own
    // `CachedOwnedLane::matches` check, which runs against live state a
    // moment later; guessing wrong here costs a park and a dial on a worker
    // that had nothing better to do, never a wrong fetch.
    fn serves(&self, run: &OwnedLaneRun) -> bool {
        self.nntp
            .upgrade()
            .is_some_and(|client| Arc::ptr_eq(&client, &run.nntp))
            && !run
                .initial_lease
                .dial_exclude_servers
                .contains(&self.server.0)
    }

    // Whether this connection is the seat `probe` was given.
    fn seats(&self, probe: &OwnedLaneProbe) -> bool {
        self.server == probe.seat
            && self
                .nntp
                .upgrade()
                .is_some_and(|client| Arc::ptr_eq(&client, &probe.nntp))
    }
}

struct OwnedLaneRun {
    nntp: Arc<weaver_nntp::NntpClient>,
    state: crate::jobs::handle::SharedPipelineState,
    event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    parked_tx: mpsc::Sender<DownloadLaneParked>,
    initial_lease: DownloadBatchLease,
}

enum OwnedLanePoolCommand {
    Run(Box<OwnedLaneRun>),
    Reset,
    RecallSocket(u64),
    // Ask one health probe batch on the seat the dispatch pass gave it.
    Probe(Box<OwnedLaneProbe>),
    // Open the connection a lease for a barred job would ask for, before the
    // lease exists.
    //
    // A job whose first wave is held to a barrier, the recovery index
    // bootstrap being the standing case, leases one batch and leaves every
    // other worker with nothing to dial for. The handshakes are then paid one
    // after the other as the barrier lifts, and the first payload BODY of
    // each lane waits out a TCP, TLS, greeting and authentication exchange
    // that had nothing to wait for. Warming turns those into a connection the
    // lane is already holding when its first lease arrives.
    //
    // A warm lane is an idle lane: it is not counted as an active download
    // connection anywhere, and if no lease ever comes it parks with the rest.
    Warm(Box<OwnedLaneWarm>),
}

struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    lane: weaver_nntp::blocking::BlockingBodyLane,
}

// What a warm dial needs: the article exclusions and size a lease for this
// job would carry, so the dial makes the server choice that lease would, and
// the one server whose unclaimed seat it may fill.
pub(crate) struct OwnedLaneWarm {
    nntp: Arc<weaver_nntp::NntpClient>,
    exclude_servers: Arc<[usize]>,
    byte_estimate: u64,
    seat: weaver_nntp::pool::ServerId,
}

fn lock_pool(
    shared: &std::sync::Mutex<OwnedLanePoolShared>,
) -> std::sync::MutexGuard<'_, OwnedLanePoolShared> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl OwnedLanePoolShared {
    // Route one run to the worker that can start it soonest, and claim that
    // worker so a concurrent submit cannot pick it too.
    //
    // The order is the cost of starting, not fairness. A worker whose cached
    // connection already serves this lease starts on the first round trip; a
    // worker with no connection pays one dial; a worker holding a connection
    // this lease cannot use pays a park and then that dial. Rotating instead
    // of routing is what made a promoted recovery lease land on an arbitrary
    // worker while the connection it wanted sat idle on another — and, since
    // those idle connections hold every server permit, made the dial it fell
    // back to fail for capacity.
    fn claim_worker_for(&mut self, run: &OwnedLaneRun) -> Option<usize> {
        self.claim_worker_where(|lane| lane.serves(run))
    }

    // Route one probe the same way: to the worker idle on its seat, else one
    // with no connection, else any idle worker, which parks what it holds.
    fn claim_worker_for_probe(&mut self, probe: &OwnedLaneProbe) -> Option<usize> {
        self.claim_worker_where(|lane| lane.seats(probe))
    }

    fn claim_worker_where(&mut self, serves: impl Fn(&IdleOwnedLane) -> bool) -> Option<usize> {
        let matching = self.workers.iter().position(|worker| {
            idle_marker(worker)
                .and_then(|idle| idle.lane.as_ref())
                .is_some_and(&serves)
        });
        let index = matching
            .or_else(|| {
                self.workers
                    .iter()
                    .position(|worker| idle_marker(worker).is_some_and(|idle| idle.lane.is_none()))
            })
            .or_else(|| {
                self.workers
                    .iter()
                    .position(|worker| idle_marker(worker).is_some())
            })?;
        if let Some(worker) = self.workers[index].as_mut() {
            worker.idle = None;
        }
        Some(index)
    }

    // The slot `seat` occupies, while it is still that worker's. A slot a
    // resize emptied, or one a later worker has since taken, is not.
    fn seat(&self, seat: OwnedLaneSeat) -> Option<&OwnedLaneWorkerSlot> {
        self.workers
            .get(seat.index)
            .and_then(Option::as_ref)
            .filter(|worker| worker.id == seat.id)
    }

    fn seat_mut(&mut self, seat: OwnedLaneSeat) -> Option<&mut OwnedLaneWorkerSlot> {
        self.workers
            .get_mut(seat.index)
            .and_then(Option::as_mut)
            .filter(|worker| worker.id == seat.id)
    }

    fn live_workers(&self) -> usize {
        self.workers.iter().flatten().count()
    }

    // Take a run this worker should start now, or publish it idle.
    //
    // The two are one decision: a worker that looked for queued work, found
    // none and then published itself idle would leave a window in which a
    // submit sees no idle worker, queues its run, and waits for a worker that
    // is already asleep.
    fn take_queued_run_or_publish_idle(
        &mut self,
        seat: OwnedLaneSeat,
        lane: Option<IdleOwnedLane>,
    ) -> Option<QueuedOwnedLaneWork> {
        if let Some(run) = self.queued_runs.pop_front() {
            return Some(run);
        }
        if let Some(worker) = self.seat_mut(seat) {
            worker.idle = Some(IdleOwnedLaneWorker { lane });
            #[cfg(test)]
            self.observe(OwnedLaneObservation::Idle(seat.index));
        }
        None
    }

    // Update what an already-idle worker is holding. A worker that has been
    // claimed since is left alone: its next run is already on the way.
    fn note_idle_lane(&mut self, seat: OwnedLaneSeat, lane: Option<IdleOwnedLane>) {
        if let Some(worker) = self.seat_mut(seat)
            && let Some(idle) = worker.idle.as_mut()
        {
            idle.lane = lane;
        }
    }

    // Empty the slot `seat` occupies, if it still does. Trailing empty slots
    // are dropped; the indices of the others never move.
    fn vacate(&mut self, seat: OwnedLaneSeat) {
        if self.seat(seat).is_none() {
            return;
        }
        self.workers[seat.index] = None;
        self.trim_vacant_tail();
        #[cfg(test)]
        self.observe(OwnedLaneObservation::Vacated(seat.index));
    }

    fn trim_vacant_tail(&mut self) {
        while self.workers.last().is_some_and(Option::is_none) {
            self.workers.pop();
        }
    }

    // First step of an idle worker leaving: withdraw it from claims, unless
    // a command was routed to it meanwhile or there is queued work to take.
    fn begin_release(&mut self, seat: OwnedLaneSeat) -> OwnedLaneRelease {
        let Some(worker) = self.seat_mut(seat) else {
            return OwnedLaneRelease::Left;
        };
        if worker.idle.is_none() {
            return OwnedLaneRelease::Stay;
        }
        worker.idle = None;
        match self.queued_runs.pop_front() {
            Some(work) => OwnedLaneRelease::Work(work),
            None => OwnedLaneRelease::Withdrawn,
        }
    }

    // Last step of an idle worker leaving. A run queued while it let its
    // connection go is taken instead: with this worker counted against the
    // cap, a submit may have queued it for exactly this worker.
    fn finish_release(&mut self, seat: OwnedLaneSeat) -> OwnedLaneRelease {
        if self.seat(seat).is_some()
            && let Some(work) = self.queued_runs.pop_front()
        {
            return OwnedLaneRelease::Work(work);
        }
        self.vacate(seat);
        OwnedLaneRelease::Left
    }

    // Idle workers with no connection, in pool order: the ones a warm can
    // give a head start without disturbing anything.
    fn idle_workers_without_a_lane(
        &self,
        limit: usize,
    ) -> Vec<std_mpsc::Sender<OwnedLanePoolCommand>> {
        self.workers
            .iter()
            .flatten()
            .filter(|worker| worker.idle.as_ref().is_some_and(|idle| idle.lane.is_none()))
            .take(limit)
            .map(|worker| worker.sender.clone())
            .collect()
    }

    #[cfg(test)]
    fn observe(&self, observation: OwnedLaneObservation) {
        if let Some(observer) = self.observer.as_ref() {
            let _ = observer.send(observation);
        }
    }
}

fn idle_marker(worker: &Option<OwnedLaneWorkerSlot>) -> Option<&IdleOwnedLaneWorker> {
    worker.as_ref().and_then(|worker| worker.idle.as_ref())
}

impl OwnedDownloadLanePool {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self::with_intervals(worker_count, OwnedLaneIntervals::default())
    }

    fn with_intervals(worker_count: usize, intervals: OwnedLaneIntervals) -> Self {
        let mut pool = Self {
            shared: Arc::new(std::sync::Mutex::new(OwnedLanePoolShared {
                workers: Vec::new(),
                queued_runs: VecDeque::new(),
                cap: 0,
                next_worker_id: 0,
                intervals,
                #[cfg(test)]
                observer: None,
            })),
            #[cfg(test)]
            reset_calls: AtomicUsize::new(0),
        };
        pool.resize(worker_count);
        pool
    }

    // A pool whose workers wake and leave on the test's schedule.
    #[cfg(test)]
    pub(crate) fn with_test_intervals(
        worker_count: usize,
        liveness: Duration,
        idle_release: Duration,
    ) -> Self {
        Self::with_intervals(
            worker_count,
            OwnedLaneIntervals {
                liveness,
                idle_release,
            },
        )
    }

    // Report every spawn, idle publication and vacated slot from here on.
    #[cfg(test)]
    pub(crate) fn observe(&self) -> std_mpsc::Receiver<OwnedLaneObservation> {
        let (tx, rx) = std_mpsc::channel();
        lock_pool(&self.shared).observer = Some(tx);
        rx
    }

    // Set how many workers may run at once. Nothing is started here: a
    // worker is started when work finds no idle one, so a machine with many
    // configured connections pays for the threads it uses, not the ones it
    // could. Surplus live workers are asked to leave, highest slot first.
    pub(crate) fn resize(&mut self, worker_count: usize) {
        let cap = worker_count.max(1);
        let mut shared = lock_pool(&self.shared);
        shared.cap = cap;
        let mut live = shared.live_workers();
        let mut index = shared.workers.len();
        while live > cap && index > 0 {
            index -= 1;
            // Dropping the sender is the request: the worker finishes what
            // it is running and leaves when it next reads its channel.
            if shared.workers[index].take().is_some() {
                live -= 1;
            }
        }
        shared.trim_vacant_tail();
        // Work queued while the old cap held every worker busy starts on the
        // room the new cap gives instead of waiting for one to finish.
        let starts = shared.queued_runs.len().min(cap.saturating_sub(live));
        for _ in 0..starts {
            if spawn_owned_lane_worker(&self.shared, &mut shared, OwnedLaneWorkerStart::Idle)
                .is_none()
            {
                break;
            }
        }
    }

    // The servers of the connections idle workers are keeping, one entry
    // per idle worker; `None` for a worker idle without a connection.
    pub(crate) fn idle_lane_servers(&self) -> Vec<Option<usize>> {
        let shared = lock_pool(&self.shared);
        shared
            .workers
            .iter()
            .flatten()
            .filter_map(|worker| {
                let idle = worker.idle.as_ref()?;
                Some(idle.lane.as_ref().map(|lane| lane.server.0))
            })
            .collect()
    }

    // How many workers may run at once: the configured connection count.
    pub(crate) fn worker_count(&self) -> usize {
        lock_pool(&self.shared).cap
    }

    // How many worker threads are alive now.
    #[cfg(test)]
    pub(crate) fn live_worker_count(&self) -> usize {
        lock_pool(&self.shared).live_workers()
    }

    #[cfg(test)]
    pub(crate) fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::Relaxed)
    }

    // Fence queued runs before workers can take another retired-client lease.
    // The actor returns these unstarted leases without charging article retries.
    pub(crate) fn reset(&self) -> StoppedOwnedLaneWork {
        #[cfg(test)]
        self.reset_calls.fetch_add(1, Ordering::Relaxed);
        let mut shared = lock_pool(&self.shared);
        for worker in shared.workers.iter().flatten() {
            let _ = worker.sender.send(OwnedLanePoolCommand::Reset);
        }
        let mut stopped = StoppedOwnedLaneWork {
            leases: Vec::new(),
            probes: Vec::new(),
        };
        for work in shared.queued_runs.drain(..) {
            match work {
                QueuedOwnedLaneWork::Run(run) => stopped.leases.push(run.initial_lease),
                QueuedOwnedLaneWork::Probe(probe) => stopped.probes.push(probe.batch),
            }
        }
        stopped
    }

    // The Err variant hands the lease back to the caller for the async
    // fallback path; it only occurs when the pool is stopped, so the size
    // of the returned lease is irrelevant.
    #[allow(clippy::result_large_err)]
    pub(crate) fn submit(
        &self,
        nntp: Arc<weaver_nntp::NntpClient>,
        state: crate::jobs::handle::SharedPipelineState,
        event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
        refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
        parked_tx: mpsc::Sender<DownloadLaneParked>,
        initial_lease: DownloadBatchLease,
    ) -> Result<(), DownloadBatchLease> {
        let run = Box::new(OwnedLaneRun {
            nntp,
            state,
            event_tx,
            refill_tx,
            parked_tx,
            initial_lease,
        });
        let mut shared = lock_pool(&self.shared);
        if shared.cap == 0 {
            return Err(run.initial_lease);
        }
        let claimed = shared.claim_worker_for(&run);
        route_owned_lane_work(&self.shared, &mut shared, claimed, QueuedOwnedLaneWork::Run(run))
            .map_err(|work| match work {
                QueuedOwnedLaneWork::Run(run) => run.initial_lease,
                QueuedOwnedLaneWork::Probe(_) => {
                    unreachable!("only a run is routed from submit")
                }
            })
    }

    // Ask `ids`, the unfound articles of `batch`, on `seat`: the seat the
    // dispatch pass entered in its ledger for this batch. The batch goes to
    // the worker idle on that seat when there is one, and otherwise waits
    // for whichever worker finishes first, as a lease would. The batch comes
    // back only when the pool has stopped and no worker can take it.
    pub(crate) fn probe(
        &self,
        nntp: Arc<weaver_nntp::NntpClient>,
        seat: weaver_nntp::pool::ServerId,
        ids: Vec<String>,
        batch: crate::pipeline::post_health::ProbeBatch,
        mail: mpsc::Sender<crate::pipeline::post_health::ProbeMail>,
    ) -> Result<(), crate::pipeline::post_health::ProbeBatch> {
        let probe = Box::new(OwnedLaneProbe {
            nntp,
            seat,
            ids,
            batch,
            mail,
        });
        let mut shared = lock_pool(&self.shared);
        if shared.cap == 0 {
            return Err(probe.batch);
        }
        let claimed = shared.claim_worker_for_probe(&probe);
        route_owned_lane_work(
            &self.shared,
            &mut shared,
            claimed,
            QueuedOwnedLaneWork::Probe(probe),
        )
        .map_err(|work| match work {
            QueuedOwnedLaneWork::Probe(probe) => probe.batch,
            QueuedOwnedLaneWork::Run(_) => unreachable!("only a probe is routed from probe"),
        })
    }

    // Ask up to `limit` connectionless idle workers to dial `seat` now,
    // starting workers for the asks no idle one can take while the cap
    // allows.
    //
    // Returns how many were asked. Each dials on its own worker thread, so
    // the handshakes overlap each other and nothing already leased waits on
    // them; a worker whose dial is refused simply stays without a connection.
    pub(crate) fn warm(
        &self,
        nntp: &Arc<weaver_nntp::NntpClient>,
        exclude_servers: Arc<[usize]>,
        byte_estimate: u64,
        seat: weaver_nntp::pool::ServerId,
        limit: usize,
    ) -> usize {
        if limit == 0 {
            return 0;
        }
        let senders = {
            let mut shared = lock_pool(&self.shared);
            let mut senders = shared.idle_workers_without_a_lane(limit);
            while senders.len() < limit && shared.live_workers() < shared.cap {
                let Some(index) =
                    spawn_owned_lane_worker(&self.shared, &mut shared, OwnedLaneWorkerStart::Idle)
                else {
                    break;
                };
                if let Some(worker) = shared.workers[index].as_ref() {
                    senders.push(worker.sender.clone());
                }
            }
            senders
        };
        let mut asked = 0;
        for sender in senders {
            let warm = Box::new(OwnedLaneWarm {
                nntp: Arc::clone(nntp),
                exclude_servers: Arc::clone(&exclude_servers),
                byte_estimate,
                seat,
            });
            if sender.send(OwnedLanePoolCommand::Warm(warm)).is_ok() {
                asked += 1;
            }
        }
        asked
    }
}

// Hand `work` to the worker `claimed` for it; when none was idle, start one
// if the cap allows, else queue it for whichever worker finishes first.
//
// Queueing rather than picking a busy worker matters: a run pushed into a
// busy worker's private channel waits out that worker's whole lease even
// while another goes idle beside it. The work comes back only when the
// worker it was sent to is gone.
fn route_owned_lane_work(
    owner: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    shared: &mut OwnedLanePoolShared,
    claimed: Option<usize>,
    work: QueuedOwnedLaneWork,
) -> Result<(), QueuedOwnedLaneWork> {
    let index = match claimed {
        Some(index) => index,
        None if shared.live_workers() < shared.cap => {
            match spawn_owned_lane_worker(owner, shared, OwnedLaneWorkerStart::Claimed) {
                Some(index) => index,
                None => {
                    shared.queued_runs.push_back(work);
                    return Ok(());
                }
            }
        }
        None => {
            shared.queued_runs.push_back(work);
            return Ok(());
        }
    };
    let command = match work {
        QueuedOwnedLaneWork::Run(run) => OwnedLanePoolCommand::Run(run),
        QueuedOwnedLaneWork::Probe(probe) => OwnedLanePoolCommand::Probe(probe),
    };
    let Some(worker) = shared.workers[index].as_ref() else {
        unreachable!("a claimed or just-started worker holds its slot");
    };
    let seat = OwnedLaneSeat {
        index,
        id: worker.id,
    };
    worker.sender.send(command).map_err(|error| {
        // The receiver is gone, so the worker is too: free its slot.
        shared.vacate(seat);
        match error.0 {
            OwnedLanePoolCommand::Run(run) => QueuedOwnedLaneWork::Run(run),
            OwnedLanePoolCommand::Probe(probe) => QueuedOwnedLaneWork::Probe(probe),
            OwnedLanePoolCommand::Reset
            | OwnedLanePoolCommand::RecallSocket(_)
            | OwnedLanePoolCommand::Warm(_) => {
                unreachable!("only routed work is sent here")
            }
        }
    })
}

// Start a worker thread in the lowest empty slot. Called under the pool lock,
// so two callers cannot take the same slot. A thread that cannot be started
// is logged and reported as `None`; the caller queues its work instead.
fn spawn_owned_lane_worker(
    owner: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    shared: &mut OwnedLanePoolShared,
    start: OwnedLaneWorkerStart,
) -> Option<usize> {
    let index = shared
        .workers
        .iter()
        .position(Option::is_none)
        .unwrap_or(shared.workers.len());
    let id = shared.next_worker_id;
    let seat = OwnedLaneSeat { index, id };
    let intervals = shared.intervals;
    let (tx, rx) = std_mpsc::channel();
    let thread_owner = Arc::clone(owner);
    let spawned = std::thread::Builder::new()
        .name(format!("weaver-nntp-lane-{index}"))
        .spawn(move || {
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();
            run_owned_lane_worker(seat, rx, &thread_owner, start, intervals);
        });
    if let Err(error) = spawned {
        warn!(
            index,
            error = %error,
            "could not start an NNTP lane thread; its work waits for a busy lane"
        );
        return None;
    }
    shared.next_worker_id += 1;
    let slot = Some(OwnedLaneWorkerSlot {
        id,
        sender: tx,
        idle: None,
    });
    if index == shared.workers.len() {
        shared.workers.push(slot);
    } else {
        shared.workers[index] = slot;
    }
    #[cfg(test)]
    shared.observe(OwnedLaneObservation::Spawned(index));
    Some(index)
}

fn run_owned_lane_worker(
    seat: OwnedLaneSeat,
    rx: std_mpsc::Receiver<OwnedLanePoolCommand>,
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    start: OwnedLaneWorkerStart,
    intervals: OwnedLaneIntervals,
) {
    let mut cached_lane: Option<CachedOwnedLane> = None;
    // The answer to the probe this worker just asked, sent once the worker
    // has published what it holds, so whoever hears the answer finds the
    // connection that gave it already offered to the next batch.
    let mut owed: Option<OwedProbeAnswer> = None;
    if start == OwnedLaneWorkerStart::Claimed {
        // Started for one piece of work, which its submit sends right behind
        // the spawn. Publishing idle first would offer this worker to a
        // second submit with the first one's work already on its way.
        loop {
            let Ok(command) = rx.recv() else {
                lock_pool(shared).vacate(seat);
                return;
            };
            if let Some(work) = answer_idle_command(seat, shared, &mut cached_lane, command) {
                owed = run_owned_lane_work(&mut cached_lane, work);
                break;
            }
        }
    }
    'work: loop {
        // Drain the pool's queue before going to sleep, and publish this
        // worker as idle only when there is nothing left to take.
        let queued = lock_pool(shared).take_queued_run_or_publish_idle(
            seat,
            cached_lane.as_ref().map(IdleOwnedLane::from_cached),
        );
        if let Some(answer) = owed.take() {
            answer.send();
        }
        if let Some(work) = queued {
            owed = run_owned_lane_work(&mut cached_lane, work);
            continue;
        }

        // Idle from here until a run is routed to this worker. The other
        // commands are answered in place and leave the worker idle, so the
        // idle marker is only re-published after a run.
        let mut idle_since = Instant::now();
        loop {
            if let Some(cached) = cached_lane.as_ref() {
                let owner = Arc::downgrade(shared);
                cached.lane.mark_idle(Arc::new(move |id| {
                    if let Some(owner) = owner.upgrade()
                        && let Some(worker) = lock_pool(&owner).seat(seat)
                    {
                        let _ = worker.sender.send(OwnedLanePoolCommand::RecallSocket(id));
                    }
                }));
            }
            let command = match rx.recv_timeout(intervals.liveness) {
                Ok(command) => command,
                Err(std_mpsc::RecvTimeoutError::Timeout) => {
                    // A server that times out an idle connection leaves the
                    // socket half-closed on this side, and the worker would
                    // keep the pool permit for it until a lease was routed
                    // here. Give the permit back as soon as that is noticed.
                    if discard_closed_cached_lane(&mut cached_lane) {
                        lock_pool(shared).note_idle_lane(seat, None);
                    }
                    if idle_since.elapsed() >= intervals.idle_release {
                        match release_idle_worker(seat, shared, &mut cached_lane) {
                            OwnedLaneRelease::Stay => {}
                            OwnedLaneRelease::Work(work) => {
                                owed = run_owned_lane_work(&mut cached_lane, work);
                                continue 'work;
                            }
                            OwnedLaneRelease::Withdrawn | OwnedLaneRelease::Left => return,
                        }
                    }
                    continue;
                }
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break 'work,
            };
            // A warmed connection is about to be wanted: its idle spell
            // starts now, not when the worker last finished a run.
            let warmed = matches!(command, OwnedLanePoolCommand::Warm(_));
            if let Some(work) = answer_idle_command(seat, shared, &mut cached_lane, command) {
                owed = run_owned_lane_work(&mut cached_lane, work);
                continue 'work;
            }
            if warmed {
                idle_since = Instant::now();
            }
        }
    }
    lock_pool(shared).vacate(seat);
    park_cached_lane(&mut cached_lane);
}

// Let an idle worker's thread go. Its connection is closed before its slot
// is given up, so the permit is back in the pool by the time anything can
// start a worker in its place. Queued work found on the way is taken instead.
fn release_idle_worker(
    seat: OwnedLaneSeat,
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    cached_lane: &mut Option<CachedOwnedLane>,
) -> OwnedLaneRelease {
    match lock_pool(shared).begin_release(seat) {
        OwnedLaneRelease::Withdrawn => {}
        OwnedLaneRelease::Left => {
            park_cached_lane(cached_lane);
            return OwnedLaneRelease::Left;
        }
        step @ (OwnedLaneRelease::Stay | OwnedLaneRelease::Work(_)) => return step,
    }
    park_cached_lane(cached_lane);
    let step = lock_pool(shared).finish_release(seat);
    if matches!(step, OwnedLaneRelease::Left) {
        debug!(slot = seat.index, "idle NNTP lane thread released");
    }
    step
}

// Start the work routed to this worker. A probe's answer is handed back, to
// be sent once the worker has published itself again.
fn run_owned_lane_work(
    cached_lane: &mut Option<CachedOwnedLane>,
    work: QueuedOwnedLaneWork,
) -> Option<OwedProbeAnswer> {
    match work {
        QueuedOwnedLaneWork::Run(run) => {
            run_owned_blocking_download_lane(cached_lane, *run);
            None
        }
        QueuedOwnedLaneWork::Probe(probe) => Some(ask_owned_lane_probe(cached_lane, *probe)),
    }
}

// A probe batch's answer on its way back through the probes' mail.
struct OwedProbeAnswer {
    mail: mpsc::Sender<crate::pipeline::post_health::ProbeMail>,
    word: crate::pipeline::post_health::ProbeMail,
}

impl OwedProbeAnswer {
    // Hand the batch back unasked: no lane could be had on its seat for a
    // reason that says nothing about the server, so it waits for a seat
    // again, as a lease the scheduler takes back does.
    fn unseated(probe: OwnedLaneProbe) -> Self {
        let OwnedLaneProbe { batch, mail, .. } = probe;
        Self {
            mail,
            word: crate::pipeline::post_health::ProbeMail::Unseated(batch),
        }
    }

    // Answer the batch on its seat without asking it: the seat could not be
    // had, so the batch is unsettled there.
    fn unsettled(probe: OwnedLaneProbe) -> Self {
        let result = weaver_nntp::client::ProbeBatchResult {
            exists: vec![false; probe.ids.len()],
            inconclusive: true,
        };
        Self::with(probe, result)
    }

    fn with(probe: OwnedLaneProbe, result: weaver_nntp::client::ProbeBatchResult) -> Self {
        let OwnedLaneProbe {
            seat, batch, mail, ..
        } = probe;
        Self {
            mail,
            word: crate::pipeline::post_health::ProbeMail::Answered(
                crate::pipeline::post_health::ProbeAnswer {
                    batch,
                    server: seat.0,
                    result,
                },
            ),
        }
    }

    fn send(self) {
        let _ = self.mail.blocking_send(self.word);
    }
}

// Answer one command on an idle worker. A run or a probe is handed back to
// the caller to start; everything else is settled here and leaves the
// worker idle.
fn answer_idle_command(
    seat: OwnedLaneSeat,
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    cached_lane: &mut Option<CachedOwnedLane>,
    command: OwnedLanePoolCommand,
) -> Option<QueuedOwnedLaneWork> {
    match command {
        OwnedLanePoolCommand::RecallSocket(id) => {
            if cached_lane
                .as_ref()
                .is_some_and(|cached| cached.lane.socket_id() == id)
            {
                lock_pool(shared).note_idle_lane(seat, None);
                // Local closure acknowledges this exact recall. No waiting
                // for QUIT, and no refund before socket drop.
                cached_lane.take();
            }
        }
        // The submit that sent this already claimed the worker, so there is
        // no marker to clear.
        OwnedLanePoolCommand::Run(run) => return Some(QueuedOwnedLaneWork::Run(run)),
        OwnedLanePoolCommand::Probe(probe) => return Some(QueuedOwnedLaneWork::Probe(probe)),
        OwnedLanePoolCommand::Reset => {
            lock_pool(shared).note_idle_lane(seat, None);
            park_cached_lane(cached_lane);
        }
        OwnedLanePoolCommand::Warm(warm) => {
            warm_cached_lane(cached_lane, *warm);
            lock_pool(shared)
                .note_idle_lane(seat, cached_lane.as_ref().map(IdleOwnedLane::from_cached));
        }
    }
    None
}

impl CachedOwnedLane {
    // Whether this connection can carry `lease` without redialling.
    //
    // The lease's newsgroups are not a condition: a socket opened for one
    // job's groups serves the next job's after [`Self::adopt_groups`], which
    // on most servers sends nothing at all. Dropping the connection at every
    // job boundary instead is what turned a provider's momentary refusal of
    // new sockets into a stall of every lane.
    fn matches(&self, nntp: &Arc<weaver_nntp::NntpClient>, lease: &DownloadBatchLease) -> bool {
        let server = self.lane.server_id();
        if !Arc::ptr_eq(&self.nntp, nntp) || lease.dial_exclude_servers.contains(&server.0) {
            return false;
        }

        let estimate = Pipeline::bandwidth_reservation_estimate(
            lease
                .works
                .first()
                .expect("owned download lease must contain work")
                .byte_estimate,
        );
        // An egress out of quota keeps refusing on this socket; a fresh dial
        // goes out through whichever egress the route still has.
        if self.lane.egress_quota_rejection(estimate).is_some() {
            return false;
        }
        let cached_rejection = nntp.server_quota_rejection(server, estimate);
        if cached_rejection.is_none() {
            return true;
        }
        // Contention on the shared health state is not a verdict about this
        // connection. Ranking the servers again is only a second opinion on a
        // quota question; when it cannot be taken, the established connection
        // stands rather than being parked and redialled for nothing.
        let Some(selection) = nntp.try_blocking_body_server_selection_with_estimate(
            &lease.dial_exclude_servers,
            estimate,
        ) else {
            return true;
        };
        cached_lane_matches_selection(server, true, &selection)
    }

    // Re-point the cached socket at `groups`. The lane itself knows which
    // group the socket has selected, so this is asked for every lease: on
    // most servers it is one capability lookup, and on one that requires a
    // selected group a socket that already sits on a candidate sends
    // nothing. `Err` means the socket could not be re-pointed and is no
    // longer worth keeping.
    fn adopt_groups(&mut self, groups: &[String]) -> weaver_nntp::Result<()> {
        self.lane.adopt_groups(groups)
    }
}

fn cached_lane_matches_selection(
    server: weaver_nntp::pool::ServerId,
    cached_quota_blocked: bool,
    selection: &weaver_nntp::client::BodyServerSelection,
) -> bool {
    if selection.eligible.contains(&server) {
        return true;
    }
    if selection.eligible.is_empty() {
        return cached_quota_blocked || selection.quota_blocked.is_none();
    }
    false
}

// Open the connection a lease for this job would have asked for, so the lease
// itself does not have to.
//
// The dial is the blocking lane acquisition a lease makes, with the same
// exclusions, held to the one seat the dispatcher found unclaimed. Provider
// policy still decides whether that seat may be used: a backfill server or a
// level past the article's eligible group is refused, and a server held in
// recovery admits no dial that no work is waiting on.
fn warm_cached_lane(cached_lane: &mut Option<CachedOwnedLane>, warm: OwnedLaneWarm) {
    if cached_lane.is_some() {
        return;
    }
    let OwnedLaneWarm {
        nntp,
        exclude_servers,
        byte_estimate,
        seat,
    } = warm;
    match nntp.try_warm_blocking_body_lane(&[], &exclude_servers, byte_estimate, seat) {
        Ok(lane) => {
            *cached_lane = Some(CachedOwnedLane { nntp, lane });
        }
        Err(error) => {
            // A refused warm costs nothing: the worker is simply still
            // without a connection, exactly as it was.
            debug!(error = %error, "owned lane warm dial declined");
        }
    }
}

// Ask a probe batch on its seat; the answer goes back with the batch.
//
// The connection is the one this worker keeps to the seat, or else one it
// opens there with every other server excluded, exactly as a lease pinned
// to that server dials, after parking whatever it held elsewhere, and
// through the same contention retries. A dial that must wait, as a lease's
// would be handed back, hands the batch back unasked. A dial the server
// refuses, or a connection that cannot settle the batch, answers it
// unsettled on that server; the connection that could not settle it is not
// kept.
fn ask_owned_lane_probe(
    cached_lane: &mut Option<CachedOwnedLane>,
    probe: OwnedLaneProbe,
) -> OwedProbeAnswer {
    let seated = cached_lane.as_ref().is_some_and(|cached| {
        Arc::ptr_eq(&cached.nntp, &probe.nntp) && cached.lane.server_id() == probe.seat
    });
    if !seated {
        park_cached_lane(cached_lane);
        let exclude: Vec<usize> = (0..probe.nntp.pool().server_count())
            .filter(|server| *server != probe.seat.0)
            .collect();
        let dialled = acquire_owned_lane_through_contention(
            || true,
            || {
                probe
                    .nntp
                    .try_acquire_blocking_body_lane_with_estimate(&[], &exclude, 0)
            },
        );
        match dialled {
            Ok(lane) => {
                *cached_lane = Some(CachedOwnedLane {
                    nntp: Arc::clone(&probe.nntp),
                    lane,
                });
            }
            // A busy health state or a seat not free yet is the wait a lease
            // the scheduler takes back makes, not an answer from the server.
            Err(error) if error.should_requeue_owned_work() => {
                return OwedProbeAnswer::unseated(probe);
            }
            Err(error) => {
                debug!(
                    server = probe.seat.0,
                    error = %error,
                    "health probe batch could not open its seat"
                );
                return OwedProbeAnswer::unsettled(probe);
            }
        }
    }
    let Some(cached) = cached_lane.as_mut() else {
        return OwedProbeAnswer::unsettled(probe);
    };
    let result = cached.lane.probe_exists(&probe.ids);
    if result.inconclusive {
        cached_lane.take();
    }
    OwedProbeAnswer::with(probe, result)
}

fn park_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) {
    if let Some(cached) = cached_lane.take() {
        cached.lane.park();
    }
}

// How often an idle worker checks that the server still holds its cached
// connection open.
const CACHED_LANE_LIVENESS_INTERVAL: Duration = Duration::from_secs(15);

// Drop a cached connection the server has already closed. There is nothing
// to QUIT; what matters is that its permit goes back to the pool now rather
// than when the next lease finds the socket dead.
fn discard_closed_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) -> bool {
    if !cached_lane
        .as_mut()
        .is_some_and(|cached| cached.lane.peer_closed())
    {
        return false;
    }
    cached_lane.take();
    crate::runtime::perf_probe::record(
        "download.owned_lane.peer_closed",
        std::time::Duration::from_nanos(1),
    );
    true
}

// Everything a downloaded article needs from the lease its work came from.
//
// A lease boundary is no longer a pipeline boundary: the first BODY of the
// next lease goes on the wire while the tail of the current one is still
// being read, so two leases are in flight at once and a response cannot ask
// "what is the current lease" when it lands. The context rides with the work
// instead.
struct LaneLeaseContext {
    body_activity: Arc<std::sync::atomic::AtomicU64>,
    lane_id: u64,
    job_id: JobId,
    runtime_generation: u64,
    completion_critical: bool,
    // The job's retention exclusions, carried from the lease. Job-derived and
    // the same for every article on the lane; each result unions it with the
    // article's own failure ledger.
    retention_excludes: Vec<usize>,
    pressure_clear: bool,
    mode: DownloadLaneMode,
    checkpoint_plan: weaver_yenc::CheckpointPlan,
}

impl LaneLeaseContext {
    fn from_lease(
        lease: &DownloadBatchLease,
        server_idx: usize,
        supports_pipelining: bool,
    ) -> Self {
        Self {
            lane_id: lease.lane_id,
            body_activity: lease.body_activity.clone(),
            job_id: lease.job_id,
            runtime_generation: lease.runtime_generation,
            completion_critical: lease.completion_critical,
            retention_excludes: lease.effective_exclude_servers.clone(),
            pressure_clear: lease.pressure_clear,
            mode: Pipeline::actual_download_lane_mode(
                lease.lane_mode,
                &lease.server_modes,
                server_idx,
                supports_pipelining,
            ),
            checkpoint_plan: lease.checkpoint_plan.clone(),
        }
    }

    fn depth(&self) -> usize {
        self.mode.max_depth().max(1)
    }
}

// Why a lane stopped issuing.
#[derive(Clone, Copy)]
enum LaneStop {
    // The transport is unusable. Everything still on the ring is unanswerable
    // and the connection is discarded.
    ConnectionLost,
    // A quota or unrequested outcome. The socket is exactly where the next
    // BODY would expect it, so the ring still drains, but nothing more may be
    // issued on it.
    PolicyBlocked,
    // The egress this connection leaves through is out of quota. The ring
    // drains as for any policy outcome, but the socket is let go: the route
    // dials its next connection through an egress that can still download.
    EgressQuota,
    RecoveryYield,
    Quarantined,
    // Work reserved earlier is no longer eligible. Drain issued requests,
    // return the unissued tail, and retain the healthy socket for other work.
    ReservationObsolete,
    // The result or refill channel is gone; the orchestrator is shutting down.
    Error,
}

fn lane_stop_park(stop: Option<LaneStop>) -> Option<(LaneParkReason, bool)> {
    Some(match stop? {
        LaneStop::ConnectionLost | LaneStop::Error => (LaneParkReason::Error, false),
        LaneStop::RecoveryYield => (LaneParkReason::ProbeYield, true),
        LaneStop::Quarantined => (LaneParkReason::Capacity, false),
        LaneStop::ReservationObsolete => (LaneParkReason::Capacity, true),
        LaneStop::PolicyBlocked => (
            LaneParkReason::ServerQuota,
            keep_cached_lane_after_park(LaneParkReason::ServerQuota),
        ),
        LaneStop::EgressQuota => (LaneParkReason::ServerQuota, false),
    })
}

// Whether a result is a BODY its connection's egress refused for quota.
fn egress_quota_refused(data: &Result<DownloadPayload, DownloadError>) -> bool {
    matches!(
        data,
        Err(DownloadError::Fetch(failure))
            if failure.quota_rejection.as_ref().is_some_and(|rejection| {
                rejection.scope == weaver_nntp::transfer::TransferScope::Egress
            })
    )
}

// How many times a worker re-asks for a connection when the shared server
// health state was busy. Each attempt already spins on the lock inside the
// client; this only covers the case where the whole spin lost.
const OWNED_LANE_SELECTION_CONTENTION_RETRIES: usize = 3;

// Acquire a connection, treating "the health state was busy" as "ask again"
// rather than as an answer.
//
// The distinction is the whole point of the contended variant: a collapsed
// selection reads as "no server can serve this" and pushes the work off the
// owned lanes onto a path that then queues behind the very permits those
// lanes are holding. `dial` is one attempt; a run's and a probe's lane are
// both had through here, so neither takes contention for an answer. Work
// that is no longer `current` is never dialled for, and is not asked again:
// it ends as contended at once.
fn acquire_owned_lane_through_contention(
    mut current: impl FnMut() -> bool,
    mut dial: impl FnMut() -> std::result::Result<
        weaver_nntp::blocking::BlockingBodyLane,
        weaver_nntp::client::BlockingBodyLaneAcquireError,
    >,
) -> std::result::Result<
    weaver_nntp::blocking::BlockingBodyLane,
    weaver_nntp::client::BlockingBodyLaneAcquireError,
> {
    let mut attempt = 0;
    loop {
        if !current() {
            return Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended);
        }
        match dial() {
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
                if attempt < OWNED_LANE_SELECTION_CONTENTION_RETRIES =>
            {
                attempt += 1;
                std::thread::yield_now();
            }
            outcome => return outcome,
        }
    }
}

// How little unfinished work a lane may still hold before its next lease has
// to be in hand.
//
// A refill costs at least one orchestrator turn to answer, and a lane at
// depth `d` retires about `d` articles per round trip. One pipe of runway
// pays for the answer and a second pays for the ask, plus the article being
// read: 3 articles at depth 1, 5 at depth 2, 9 at depth 4. An ordinary lease
// is exactly one pipe deep, so it trips this the moment it is adopted and the
// ask goes out while the lease's own first article is still on the wire —
// which is the zero-gap handoff sequential mode needs. A runway-sized hot
// lease trips it two round trips before it runs dry.
fn refill_deadline(depth: usize) -> usize {
    2 * depth.max(1) + 1
}

// Hand one finished article to the orchestrator on its own, without waiting
// for an acknowledgement.
//
// Delivery per article rather than per lease is what lets decode start on the
// first article of a lease instead of its last, and what makes a direct-store
// volume settle when its final article lands. The bounded event channel is
// the backpressure; the acknowledgement is kept for the lane's final event,
// where it orders the results ahead of the park message on the other channel.
fn stream_owned_result(
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
    lane: &weaver_nntp::blocking::BlockingBodyLane,
    stats_mark: &mut weaver_nntp::blocking::BlockingLaneStats,
    result: DownloadResult,
) -> Result<(), ()> {
    let now = lane.stats();
    let stats = stats_delta(now, *stats_mark);
    *stats_mark = now;
    send_owned_batch(
        event_tx,
        result.lane_id,
        vec![result],
        Vec::new(),
        stats,
        false,
    )
}

// The newsgroups a connection for this lease has to be pointed at.
//
// A batch is cut around its first work's compatibility and every work that
// joins it is admitted against that, so the first work's groups are the
// lease's groups. They are not decoration: a server that answers a
// message-id fetch only after a GROUP prologue gets none from an empty list,
// and a freshly dialled connection has no candidate for its initial group
// probe.
fn lease_groups(lease: &DownloadBatchLease) -> &[String] {
    match lease.works.first() {
        Some(work) => work.groups.as_ref(),
        None => &[],
    }
}

pub(super) fn job_work_is_current(
    state: &crate::jobs::handle::SharedPipelineState,
    job_id: JobId,
    generation: u64,
) -> bool {
    state.queue.read(|inner| {
        let globals = &inner.globals;
        !globals.paused
            && !globals.scheduled_pause
            && !globals.speed_test_paused
            && !globals.schedule_replay_paused
            && !globals.shutting_down
            && globals.nntp_generation == generation
            && inner.job(job_id).is_some_and(|row| {
                row.report.nzb_admitted
                    && !row.report.retiring
                    && !row.report.restoring
                    && row.admission.is_none()
                    && row.intents.cancelled.is_none()
                    && row.intents.paused != Some(true)
                    && row.report.job.run_state != crate::RunState::Paused
            })
    })
}

// A lease is cut for one server from works that server may serve, and pinned
// to it by `dial_exclude_servers` (see `Pipeline::lease_for_handout`). The
// lease is current while some undialled-out server may still serve every one
// of its works, which for a lease cut by dispatch is its own server.
fn lease_is_current(
    state: &crate::jobs::handle::SharedPipelineState,
    nntp: &weaver_nntp::NntpClient,
    lease: &DownloadBatchLease,
) -> bool {
    if !job_work_is_current(state, lease.job_id, lease.runtime_generation) {
        return false;
    }
    let policy = nntp.pool().provider_policy();
    (0..nntp.pool().server_count()).any(|server| {
        !lease.dial_exclude_servers.contains(&server)
            && lease.works.iter().all(|work| {
                matches!(
                    policy.eligibility(
                        server,
                        &work.exclude_servers,
                        &lease.effective_exclude_servers,
                        Pipeline::bandwidth_reservation_estimate(work.byte_estimate),
                    ),
                    weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
                )
            })
    })
}

// Run one lease to its park on this worker's connection.
#[allow(clippy::too_many_lines)]
fn run_owned_blocking_download_lane(cached_lane: &mut Option<CachedOwnedLane>, run: OwnedLaneRun) {
    let fetch_started = Instant::now();
    let OwnedLaneRun {
        nntp,
        state,
        event_tx,
        refill_tx,
        parked_tx,
        initial_lease,
    } = run;
    let lease = initial_lease;

    if !lease_is_current(&state, &nntp, &lease) {
        let _ = event_tx.blocking_send(OwnedDownloadLaneEvent::LeaseObsolete { lease });
        return;
    }

    if cached_lane
        .as_ref()
        .is_some_and(|cached| !cached.matches(&nntp, &lease))
    {
        park_cached_lane(cached_lane);
    }
    // A cached connection the server closed while it sat idle serves nothing:
    // its first BODY would fail only after a read timeout and burn a retry.
    discard_closed_cached_lane(cached_lane);
    if let Some(cached) = cached_lane.as_ref() {
        cached.lane.mark_active();
    }
    // A connection opened for another job's newsgroups is kept and re-pointed
    // rather than redialled; only a socket that cannot be re-pointed is let
    // go, and the dial below then replaces it.
    if let Some(cached) = cached_lane.as_mut()
        && let Err(error) = cached.adopt_groups(lease_groups(&lease))
    {
        debug!(
            server = cached.lane.server_id().0,
            error = %error,
            "cached owned lane could not adopt the lease's newsgroups; redialling"
        );
        park_cached_lane(cached_lane);
    }

    if cached_lane.is_none() {
        let initial_estimate = Pipeline::bandwidth_reservation_estimate(
            lease
                .works
                .first()
                .expect("owned download lease must contain work")
                .byte_estimate,
        );
        let dialled = acquire_owned_lane_through_contention(
            || lease_is_current(&state, &nntp, &lease),
            || {
                nntp.try_acquire_blocking_body_lane_with_estimate(
                    lease_groups(&lease),
                    &lease.dial_exclude_servers,
                    initial_estimate,
                )
            },
        );
        match dialled {
            Ok(lane) => {
                *cached_lane = Some(CachedOwnedLane {
                    nntp: Arc::clone(&nntp),
                    lane,
                });
            }
            Err(error) => {
                let _ =
                    event_tx.blocking_send(OwnedDownloadLaneEvent::AcquireFailed { lease, error });
                crate::runtime::perf_probe::record(
                    "download.fetch_body.owned",
                    fetch_started.elapsed(),
                );
                return;
            }
        }
    }

    if !lease_is_current(&state, &nntp, &lease) {
        let _ = event_tx.blocking_send(OwnedDownloadLaneEvent::LeaseObsolete { lease });
        return;
    }

    let lane = &mut cached_lane
        .as_mut()
        .expect("owned lane cache populated before run")
        .lane;
    let server_idx = lane.server_id().0;
    let supports_pipelining = lane.supports_pipelining();
    let recovery_probe = lane.is_recovery_probe();

    // The mode the scheduler booked this lane's depth gauge under. The initial
    // dispatch booked the requested mode; every granted refill rebooks the
    // actual one. Reporting the mode the lane is *running* instead would
    // decrement a gauge it was never counted in.
    let mut booked_mode = lease.lane_mode;
    let mut context = Arc::new(LaneLeaseContext::from_lease(
        &lease,
        server_idx,
        supports_pipelining,
    ));
    let mut park_context = Arc::clone(&context);
    let mut pending: VecDeque<(DownloadWork, Arc<LaneLeaseContext>)> = lease
        .works
        .into_iter()
        .map(|work| (work, Arc::clone(&context)))
        .collect();
    if recovery_probe && pending.len() > 1 {
        // Release every other article's actor reservation before issuing the
        // probe. A slow recovery must not hold a normal multi-article lease.
        let returned = pending
            .split_off(1)
            .into_iter()
            .map(|(work, _)| work)
            .collect();
        if send_owned_batch(
            &event_tx,
            context.lane_id,
            Vec::new(),
            returned,
            weaver_nntp::blocking::BlockingLaneStats::default(),
            true,
        )
        .is_err()
        {
            park_cached_lane(cached_lane);
            return;
        }
    }
    let mut inflight: VecDeque<(DownloadWork, Arc<LaneLeaseContext>)> = VecDeque::new();
    let mut pending_refill: Option<oneshot::Receiver<DownloadLaneRefillResponse>> = None;
    let mut refill_denied = false;
    let mut stop: Option<LaneStop> = None;
    let mut stats_mark = lane.stats();

    let (park_reason, keep_cached_lane) = loop {
        // Adopt an answered refill without waiting for it. Taking it here is
        // what puts the next lease's first BODY on the wire before the current
        // lease's last response has been read.
        if stop.is_none()
            && let Some(response_rx) = pending_refill.as_mut()
        {
            match response_rx.try_recv() {
                Ok(response) => {
                    pending_refill = None;
                    match response.lease {
                        Some(next_lease) if !next_lease.works.is_empty() => {
                            booked_mode = Pipeline::actual_download_lane_mode(
                                next_lease.lane_mode,
                                &next_lease.server_modes,
                                server_idx,
                                supports_pipelining,
                            );
                            context = Arc::new(LaneLeaseContext::from_lease(
                                &next_lease,
                                server_idx,
                                supports_pipelining,
                            ));
                            park_context = Arc::clone(&context);
                            pending.extend(
                                next_lease
                                    .works
                                    .into_iter()
                                    .map(|work| (work, Arc::clone(&context))),
                            );
                        }
                        // Denied mid-flight. The decision was taken while this
                        // lane's results were still landing, so it gets one
                        // more chance once the pipe is actually dry; until
                        // then, do not re-ask and spin.
                        _ => refill_denied = true,
                    }
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(oneshot::error::TryRecvError::Closed) => {
                    pending_refill = None;
                    refill_denied = true;
                }
            }
        }

        // Top the ring back up to the depth in force.
        while stop.is_none() && lane.ring_outstanding() < context.depth() {
            if !lane.accepts_new_work() {
                stop = Some(LaneStop::Quarantined);
                break;
            }
            let Some((work, work_context)) = pending.front() else {
                break;
            };
            // A refill can wait behind an in-flight response after the actor
            // granted it. Its grant is not permission to issue after pause,
            // cancellation, a provider edit, or higher-priority recovery.
            if !job_work_is_current(&state, work_context.job_id, work_context.runtime_generation)
                || !matches!(
                    nntp.pool().provider_policy().eligibility(
                        server_idx,
                        &work.exclude_servers,
                        &work_context.retention_excludes,
                        Pipeline::bandwidth_reservation_estimate(work.byte_estimate),
                    ),
                    weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
                )
            {
                stop = Some(LaneStop::ReservationObsolete);
                break;
            }
            let (work, work_context) = pending.pop_front().expect("pending front was inspected");
            let estimate = Pipeline::bandwidth_reservation_estimate(work.byte_estimate);
            let wire_form = work.message_id.wire_form();
            let outcome = lane.ring_issue(&wire_form, estimate, work_context.depth());
            let trace = match outcome {
                weaver_nntp::blocking::RingIssueOutcome::Issued => {
                    inflight.push_back((work, work_context));
                    if recovery_probe {
                        stop = Some(LaneStop::RecoveryYield);
                        break;
                    }
                    continue;
                }
                weaver_nntp::blocking::RingIssueOutcome::Rejected(trace) => {
                    stop = Some(LaneStop::PolicyBlocked);
                    trace
                }
                weaver_nntp::blocking::RingIssueOutcome::Failed(trace) => {
                    stop = Some(LaneStop::ConnectionLost);
                    trace
                }
            };
            let discarded = matches!(stop, Some(LaneStop::ConnectionLost));
            let result = result_from_trace(
                work,
                work_context.runtime_generation,
                work_context.lane_id,
                work_context.job_id,
                *trace,
                DownloadLaneObservation {
                    server_idx: Some(server_idx),
                    mode: work_context.mode,
                    supports_pipelining,
                    latency_sample: None,
                    cold: false,
                    transfer: lane.transfer_ewma(),
                    payload_bytes: 0,
                    policy_elapsed: std::time::Duration::ZERO,
                    pressure_clear: work_context.pressure_clear,
                    // Nothing was measured, so this must not close a depth
                    // trial window.
                    batch_complete: discarded,
                    batch_clean: !discarded,
                    unresolved_count: 0,
                    connection_discarded: discarded,
                },
                &work_context.retention_excludes,
            );
            if matches!(stop, Some(LaneStop::PolicyBlocked)) && egress_quota_refused(&result.data) {
                stop = Some(LaneStop::EgressQuota);
            }
            if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
                stop = Some(LaneStop::Error);
            }
        }

        // Ask for the next lease before the ring can run dry.
        let remaining = pending.len() + inflight.len();
        if stop.is_none()
            && pending_refill.is_none()
            && !refill_denied
            && remaining <= refill_deadline(context.depth())
        {
            let (response_tx, response_rx) = oneshot::channel();
            if refill_tx
                .blocking_send(DownloadLaneRefillRequest {
                    lane_id: context.lane_id,
                    runtime_generation: context.runtime_generation,
                    server_idx,
                    supports_pipelining,
                    current_mode: booked_mode,
                    response_tx,
                })
                .is_ok()
            {
                pending_refill = Some(response_rx);
            } else {
                stop = Some(LaneStop::Error);
            }
        }

        if lane.ring_outstanding() == 0 && (pending.is_empty() || stop.is_some()) {
            if let Some(park) = lane_stop_park(stop) {
                break park;
            }

            // The pipe is dry and nothing is queued behind it. Wait for the
            // answer that is owed, and give a denial one more chance now that
            // every result this lane produced has been delivered.
            let mut answer = match pending_refill.take() {
                Some(response_rx) => match response_rx.blocking_recv() {
                    Ok(response) => Some(response),
                    Err(_) => break (LaneParkReason::ProbeYield, false),
                },
                None => None,
            };
            let answered_with_work = answer.as_ref().is_some_and(|response| {
                response
                    .lease
                    .as_ref()
                    .is_some_and(|lease| !lease.works.is_empty())
            });
            if !answered_with_work {
                let (retry_tx, retry_rx) = oneshot::channel();
                if refill_tx
                    .blocking_send(DownloadLaneRefillRequest {
                        lane_id: context.lane_id,
                        runtime_generation: context.runtime_generation,
                        server_idx,
                        supports_pipelining,
                        current_mode: booked_mode,
                        response_tx: retry_tx,
                    })
                    .is_err()
                {
                    break (LaneParkReason::Error, false);
                }
                match retry_rx.blocking_recv() {
                    Ok(retry) => answer = Some(retry),
                    Err(_) => break (LaneParkReason::ProbeYield, false),
                }
            }
            let response = answer.expect("refill answer present after the drain-point retry");
            match response.lease {
                Some(next_lease) if !next_lease.works.is_empty() => {
                    booked_mode = Pipeline::actual_download_lane_mode(
                        next_lease.lane_mode,
                        &next_lease.server_modes,
                        server_idx,
                        supports_pipelining,
                    );
                    context = Arc::new(LaneLeaseContext::from_lease(
                        &next_lease,
                        server_idx,
                        supports_pipelining,
                    ));
                    park_context = Arc::clone(&context);
                    pending.extend(
                        next_lease
                            .works
                            .into_iter()
                            .map(|work| (work, Arc::clone(&context))),
                    );
                    refill_denied = false;
                    continue;
                }
                _ => {
                    let reason = response.park_reason;
                    break (reason, keep_cached_lane_after_park(reason));
                }
            }
        }

        // The plan is immutable per lease and only read when a response is
        // decoded, so applying the head request's plan here is what keeps two
        // leases' geometries apart on one ring.
        if let Some((_, head_context)) = inflight.front() {
            lane.set_body_activity(head_context.body_activity.clone());
            lane.set_checkpoint_plan(head_context.checkpoint_plan.clone());
        }
        let Some((trace, meta)) = lane.ring_read_next() else {
            break (LaneParkReason::Error, false);
        };
        let Some((work, work_context)) = inflight.pop_front() else {
            break (LaneParkReason::Error, false);
        };
        nntp.record_blocking_attempts(&trace.attempts);
        let (payload_bytes, policy_elapsed) = Pipeline::decoded_trace_throughput_sample(&trace);
        let sample = lane.take_response_sample();
        // A connection's first few articles say nothing about what its
        // address delivers in steady state, and neither does one fetched
        // while the job could not take bytes as fast as the wire offered them.
        if sample.settled && payload_bytes > 0 && work_context.pressure_clear {
            nntp.record_address_delivery(&trace.attempts, payload_bytes, policy_elapsed);
        }
        let result = result_from_trace(
            work,
            work_context.runtime_generation,
            work_context.lane_id,
            work_context.job_id,
            trace,
            DownloadLaneObservation {
                server_idx: Some(server_idx),
                mode: work_context.mode,
                supports_pipelining,
                latency_sample: sample.latency,
                cold: sample.cold,
                transfer: lane.transfer_ewma(),
                payload_bytes,
                policy_elapsed,
                pressure_clear: work_context.pressure_clear,
                batch_complete: meta.batch_complete,
                batch_clean: meta.batch_clean,
                unresolved_count: meta.unresolved_count,
                connection_discarded: meta.connection_discarded,
            },
            &work_context.retention_excludes,
        );
        let keeps_connection = download_outcome_keeps_connection(&result.data);
        let policy_blocked = matches!(
            &result.data,
            Err(DownloadError::Fetch(failure))
                if matches!(
                    failure.kind,
                    DownloadFailureKind::ServerQuota | DownloadFailureKind::Unrequested
                )
        );
        let egress_blocked = egress_quota_refused(&result.data);
        if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
            stop = Some(LaneStop::Error);
        }
        if !keeps_connection || meta.connection_discarded || lane.ring_is_closed() {
            stop.get_or_insert(LaneStop::ConnectionLost);
        } else if egress_blocked {
            stop.get_or_insert(LaneStop::EgressQuota);
        } else if policy_blocked {
            stop.get_or_insert(LaneStop::PolicyBlocked);
        }

        if matches!(stop, Some(LaneStop::ConnectionLost)) {
            let abandoned = lane.ring_abandon();
            let lost = inflight.drain(..).collect::<Vec<_>>();
            let unresolved_count = lost.len().max(abandoned) as u64;
            for (work, work_context) in lost {
                let result = unresolved_result(
                    work,
                    work_context.runtime_generation,
                    work_context.lane_id,
                    work_context.job_id,
                    server_idx,
                    work_context.mode,
                    supports_pipelining,
                    lane.transfer_ewma(),
                    work_context.pressure_clear,
                    unresolved_count,
                    &work_context.retention_excludes,
                    "lane pipeline faulted before this article's response",
                );
                if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
                    stop = Some(LaneStop::Error);
                    break;
                }
            }
        }
    };

    // Return what this lane still holds before waiting on its refill. A
    // refill from a saturated lane is held in the actor until the lane has
    // fetched down below its share, and a lane parking here with a full
    // pending tail never will: its holdings only fall once these articles are
    // back in the queue. Returning them first is what lets the actor answer.
    let unrequested_works = pending
        .into_iter()
        .map(|(work, _)| work)
        .collect::<Vec<_>>();
    let _ = send_owned_batch(
        &event_tx,
        park_context.lane_id,
        Vec::new(),
        unrequested_works,
        weaver_nntp::blocking::BlockingLaneStats::default(),
        false,
    );
    if let Some(granted_context) = drain_pending_refill(
        pending_refill.take(),
        &event_tx,
        server_idx,
        supports_pipelining,
    ) {
        // Granting a refill already rebooks the connection in the actor,
        // even when a transport fault prevents the worker from adopting it.
        booked_mode = granted_context.mode;
        park_context = Arc::new(granted_context);
    }
    // The final event is the ordering barrier: its acknowledgement is what
    // guarantees every streamed result reached the orchestrator before the
    // park message arrives on the other channel and releases the connection.
    let stats = stats_delta(lane.stats(), stats_mark);
    let _ = send_owned_batch(
        &event_tx,
        park_context.lane_id,
        Vec::new(),
        Vec::new(),
        stats,
        true,
    );

    if !keep_cached_lane {
        park_cached_lane(cached_lane);
    }
    // The article buffers this thread allocated are freed by the decode and
    // writer threads, so under a per-thread-heap allocator their pages sit on
    // this lane's heap until it allocates again. A parked lane never does, so
    // the release has to happen here, on the lane thread, once per park —
    // never per article or per refill, where it would cost throughput.
    crate::runtime::thread_release::release_idle_thread_memory();
    let _ = parked_tx.blocking_send(DownloadLaneParked {
        lane_id: park_context.lane_id,
        job_id: park_context.job_id,
        mode: booked_mode,
        completion_critical: park_context.completion_critical,
        reason: park_reason,
        release_connection_slot: true,
    });
    crate::runtime::perf_probe::record("download.fetch_body.owned", fetch_started.elapsed());
}

// Consume a prefetched refill response on a park/error path so its leased
// works are returned to the queue instead of being dropped. The orchestrator
// answers every refill request (or drops the sender on shutdown), so this
// cannot hang — provided the lane has already returned the articles it was
// holding, since a saturated lane's answer waits on exactly that.
fn drain_pending_refill(
    pending_refill: Option<oneshot::Receiver<DownloadLaneRefillResponse>>,
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
    server_idx: usize,
    supports_pipelining: bool,
) -> Option<LaneLeaseContext> {
    let response_rx = pending_refill?;
    if let Ok(response) = response_rx.blocking_recv()
        && let Some(lease) = response.lease
        && !lease.works.is_empty()
    {
        let context = LaneLeaseContext::from_lease(&lease, server_idx, supports_pipelining);
        let _ = send_owned_batch(
            event_tx,
            lease.lane_id,
            Vec::new(),
            lease.works,
            weaver_nntp::blocking::BlockingLaneStats::default(),
            true,
        );
        return Some(context);
    }
    None
}

// Whether a park keeps the socket for the worker's next lease.
//
// Everything but a transport fault does. A yield is the case worth stating:
// it hands the *scheduling* of this connection back so completion-critical
// work can have it, and dropping the socket to do that threw away the one
// thing the critical lease wanted — an authenticated connection it could
// issue on immediately. The routing in `claim_worker_for` is what makes the
// difference visible: the yielded worker is idle holding exactly the
// connection the next critical lease is looking for.
fn keep_cached_lane_after_park(reason: LaneParkReason) -> bool {
    matches!(
        reason,
        LaneParkReason::NoWork
            | LaneParkReason::Pressure
            | LaneParkReason::ProbeYield
            | LaneParkReason::ServerQuota
    )
}

fn stats_delta(
    after: weaver_nntp::blocking::BlockingLaneStats,
    before: weaver_nntp::blocking::BlockingLaneStats,
) -> weaver_nntp::blocking::BlockingLaneStats {
    weaver_nntp::blocking::BlockingLaneStats {
        socket_reads: after.socket_reads.saturating_sub(before.socket_reads),
        socket_writes: after.socket_writes.saturating_sub(before.socket_writes),
        tls_recv_calls: after.tls_recv_calls.saturating_sub(before.tls_recv_calls),
        tls_send_calls: after.tls_send_calls.saturating_sub(before.tls_send_calls),
        body_responses: after.body_responses.saturating_sub(before.body_responses),
        decoded_articles: after
            .decoded_articles
            .saturating_sub(before.decoded_articles),
    }
}

// Hand results and returned works to the orchestrator.
//
// `acknowledge` turns the send into a rendezvous. A streamed article does not
// need one — the bounded event channel already paces the lane, and waiting
// per article would put a thread hop on the hot path — but the lane's last
// event does: it has to be seen before the park message that follows it on a
// different channel.
fn send_owned_batch(
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
    lane_id: u64,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
    stats: weaver_nntp::blocking::BlockingLaneStats,
    acknowledge: bool,
) -> Result<(), ()> {
    if results.is_empty() && unrequested_works.is_empty() && !acknowledge {
        return Ok(());
    }
    let (ack, ack_rx) = if acknowledge {
        let (ack, ack_rx) = std::sync::mpsc::sync_channel(0);
        (Some(ack), Some(ack_rx))
    } else {
        (None, None)
    };
    event_tx
        .blocking_send(OwnedDownloadLaneEvent::BatchComplete {
            lane_id,
            results,
            unrequested_works,
            stats,
            ack,
        })
        .map_err(|_| ())?;
    if let Some(ack_rx) = ack_rx {
        ack_rx.recv().map_err(|_| ())?;
    }
    Ok(())
}

// The exclusions a result reports for one article: the article's own
// failure ledger, plus the job's retention exclusions the lease carried.
//
// The ledger has to come from the work itself. A lane's batch is cut for one
// server but not for one exclusion set, so two articles on the same lane can
// have failed on different servers; reporting the lease's set instead loses
// every server this article has already been refused by, and an article
// missing everywhere then retries forever because its exclusion set never
// grows and exhaustion never trips.
//
// The rotation hint (`avoid_server`) deliberately stays out: it only shapes
// selection, and counting it here would let a transient timeout help declare
// an article missing.
fn result_exclude_servers(work: &DownloadWork, retention_excludes: &[usize]) -> Vec<usize> {
    Pipeline::union_exclude_servers(&work.exclude_servers, retention_excludes)
}

// Credit one fetched article's wire bytes to its job and, beside it, to the
// transfer total the download gauge integrates. Every path that fetches an
// article over the wire calls this, so the jobs' counters sum to the total.
pub(in crate::pipeline) fn credit_transfer(
    counters: &crate::pipeline::queue::JobCounters,
    data: &Result<DownloadPayload, DownloadError>,
) {
    let (wire, decoded) = match data {
        #[cfg(test)]
        Ok(DownloadPayload::Raw(raw)) => (raw.len() as u64, 0),
        Ok(DownloadPayload::Decoded(decoded)) => {
            (decoded.raw_size, decoded.data.len_bytes() as u64)
        }
        Err(DownloadError::Local { raw_size, .. } | DownloadError::Decode { raw_size, .. }) => {
            (*raw_size, 0)
        }
        Err(DownloadError::Fetch(_)) => (0, 0),
    };
    counters.downloaded.fetch_add(wire, Ordering::Relaxed);
    if let Some(totals) = counters.totals.get() {
        totals.bandwidth.record(wire);
        if data.is_ok() {
            totals.articles.fetch_add(1, Ordering::Relaxed);
        }
    }
    counters.record_decoded(decoded);
    if wire > 0 {
        counters
            .last_byte_at_ms
            .store(crate::jobs::epoch_ms_now() as u64, Ordering::Relaxed);
    }
    if data.is_ok() {
        counters.articles_done.fetch_add(1, Ordering::Relaxed);
    }
}

fn result_from_trace(
    work: DownloadWork,
    runtime_generation: u64,
    lane_id: u64,
    job_id: JobId,
    trace: weaver_nntp::client::DecodedBodyTrace,
    mut observation: DownloadLaneObservation,
    retention_excludes: &[usize],
) -> DownloadResult {
    let segment_id = work.segment_id;
    let retry_count = work.retry_count;
    let exclude_servers = result_exclude_servers(&work, retention_excludes);
    // The work's own flags, never the lease's: a lane may be refilled with the
    // other class's work, and the origin books the article that was actually
    // fetched.
    let is_recovery = work.is_recovery;
    let completion_critical = work.completion_critical;
    let (data, attempts, source_server_idx) =
        Pipeline::download_data_from_decoded_trace(segment_id, trace);
    if let Some(counters) = &work.counters {
        credit_transfer(counters, &data);
    }
    // Only outcomes that actually damaged the transport dirty the batch. A
    // 430 and the local quota/unrequested policy outcomes all leave the
    // socket exactly where the next BODY expects it.
    if !download_outcome_keeps_connection(&data) {
        observation.batch_clean = false;
        observation.connection_discarded = true;
    }
    DownloadResult {
        lane_id,
        job_id,
        segment_id,
        runtime_generation,
        data,
        attempts,
        lane_observation: Some(observation),
        source_server_idx,
        origin: DownloadResultOrigin::from_work(is_recovery, completion_critical),
        retry_count,
        exclude_servers,
        release_connection_slot: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn unresolved_result(
    work: DownloadWork,
    runtime_generation: u64,
    lane_id: u64,
    job_id: JobId,
    server_idx: usize,
    mode: DownloadLaneMode,
    supports_pipelining: bool,
    transfer: Option<std::time::Duration>,
    pressure_clear: bool,
    unresolved_count: u64,
    retention_excludes: &[usize],
    message: &'static str,
) -> DownloadResult {
    let is_recovery = work.is_recovery;
    let completion_critical = work.completion_critical;
    let exclude_servers = result_exclude_servers(&work, retention_excludes);
    DownloadResult {
        lane_id,
        job_id,
        segment_id: work.segment_id,
        runtime_generation,
        data: Err(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::EstablishedTransport,
            message,
        ))),
        attempts: Vec::new(),
        lane_observation: Some(DownloadLaneObservation {
            server_idx: Some(server_idx),
            mode,
            supports_pipelining,
            latency_sample: None,
            cold: false,
            transfer,
            payload_bytes: 0,
            policy_elapsed: std::time::Duration::ZERO,
            pressure_clear,
            batch_complete: true,
            batch_clean: false,
            unresolved_count,
            connection_discarded: true,
        }),
        source_server_idx: None,
        origin: DownloadResultOrigin::from_work(is_recovery, completion_critical),
        retry_count: work.retry_count,
        exclude_servers,
        release_connection_slot: false,
    }
}

#[cfg(test)]
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

#[cfg(test)]
fn tail_work(segment_number: u32, retry_count: u32) -> DownloadWork {
    DownloadWork {
        counters: None,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(42),
                file_index: 0,
            },
            segment_number,
        },
        message_id: MessageId::new(&format!("tail-{segment_number}@example.invalid")),
        groups: Arc::from(vec!["alt.binaries.test".to_string()]),
        priority: 3,
        byte_estimate: 1024,
        retry_count,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: Vec::new(),
        avoid_server: None,
    }
}

#[cfg(test)]
fn test_admitted_state(job_id: JobId) -> crate::jobs::handle::SharedPipelineState {
    let state = crate::jobs::handle::SharedPipelineState::new(
        crate::operations::metrics::PipelineMetrics::new(),
        vec![crate::jobs::handle::JobInfo {
            job_id,
            job_hash: None,
            name: "lane-fixture".into(),
            status: crate::JobStatus::Downloading,
            download_state: crate::DownloadState::Downloading,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: crate::PostState::Idle,
            run_state: crate::RunState::Active,
            progress: 0.0,
            total_bytes: 1024,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 1,
            completed_files: 0,
            remaining_par_files: 0,
            file_remaining_bytes: Vec::new(),
            password: None,
            category: None,
            metadata: Vec::new(),
            output_dir: None,
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            server_attribution: Vec::new(),
            created_at_epoch_ms: 0.0,
        }],
    );
    state.queue.write(|inner| {
        inner.jobs[0].report.nzb_admitted = true;
        crate::pipeline::queue::Change::changed(())
    });
    state
}

#[cfg(test)]
fn test_lease(
    job_id: JobId,
    runtime_generation: u64,
    exclude_servers: Vec<usize>,
    works: Vec<DownloadWork>,
) -> DownloadBatchLease {
    DownloadBatchLease {
        body_activity: Arc::default(),
        lane_id: 0,
        job_id,
        runtime_generation,
        lane_mode: DownloadLaneMode::Pipelined { depth: 4 },
        server_modes: vec![(0, DownloadLaneMode::Pipelined { depth: 4 })],
        completion_critical: false,
        effective_exclude_servers: exclude_servers.clone(),
        dial_exclude_servers: exclude_servers,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    }
}

// An idle marker standing for a cached connection on `server`.
//
// The client is a dangling `Weak`, which is what an idle marker looks like
// once its client has been retired: it can serve nothing, so routing skips it.
#[cfg(test)]
fn test_idle_lane(server: usize) -> IdleOwnedLane {
    IdleOwnedLane {
        nntp: std::sync::Weak::new(),
        server: weaver_nntp::pool::ServerId(server),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two leases are on the ring at a lease boundary, so a response has to
    // carry its own lease's identity, not whichever lease the lane happens to
    // be filling from when it lands.
    #[test]
    fn a_result_is_attributed_to_the_lease_its_work_came_from() {
        let old = Arc::new(LaneLeaseContext::from_lease(
            &test_lease(JobId(7), 3, vec![1], vec![tail_work(1, 0)]),
            0,
            true,
        ));
        let new = Arc::new(LaneLeaseContext::from_lease(
            &test_lease(JobId(7), 4, vec![2, 5], vec![tail_work(2, 0)]),
            0,
            true,
        ));

        let observation = DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 4 },
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: std::time::Duration::ZERO,
            pressure_clear: true,
            batch_complete: false,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
        };
        let straggler = result_from_trace(
            tail_work(1, 0),
            old.runtime_generation,
            0,
            JobId(42),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::error::NntpError::ArticleNotFound,
                )),
            },
            observation,
            &old.retention_excludes,
        );

        assert_eq!(
            straggler.runtime_generation, 3,
            "the tail of the old lease keeps the old generation while the new \
             lease is already on the wire"
        );
        assert_eq!(
            straggler.exclude_servers,
            vec![1],
            "the work carried no failure ledger, so the result reports the \
             lease's retention exclusions alone"
        );
        assert_eq!(new.runtime_generation, 4);
        assert_eq!(new.retention_excludes, vec![2, 5]);
        assert_eq!(new.depth(), 4);
    }

    // A lane's batch is cut for one job, not for one exclusion set, so the
    // result has to report the article's own failure ledger. Reporting the
    // lease's set instead loses every server this article has already been
    // refused by, and an article missing on all of them then retries forever
    // because its exclusion set never grows past the server that just
    // answered.
    #[test]
    fn a_result_reports_the_article_s_own_failure_ledger() {
        let context = Arc::new(LaneLeaseContext::from_lease(
            &test_lease(JobId(7), 3, Vec::new(), vec![tail_work(1, 0)]),
            1,
            true,
        ));
        let mut work = tail_work(1, 1);
        work.exclude_servers = vec![0];

        let result = result_from_trace(
            work,
            context.runtime_generation,
            0,
            JobId(7),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::error::NntpError::ArticleNotFound,
                )),
            },
            DownloadLaneObservation {
                server_idx: Some(1),
                mode: DownloadLaneMode::Sequential,
                supports_pipelining: false,
                latency_sample: None,
                cold: false,
                transfer: None,
                payload_bytes: 0,
                policy_elapsed: std::time::Duration::ZERO,
                pressure_clear: true,
                batch_complete: false,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
            },
            &context.retention_excludes,
        );

        assert_eq!(
            result.exclude_servers,
            vec![0],
            "the server this article already failed on must survive the trip \
             back, or the completion path cannot tell that both servers are out"
        );
    }

    // Per-article delivery is the point: decode starts on a lease's first
    // article instead of its last. Only the lane's closing event waits for an
    // acknowledgement, because that is what orders the results ahead of the
    // park message on the other channel.
    #[tokio::test]
    async fn results_stream_one_article_at_a_time_and_only_the_last_waits() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let sender_tx = event_tx.clone();
        let sender = tokio::task::spawn_blocking(move || {
            let stats = weaver_nntp::blocking::BlockingLaneStats::default();
            for segment_number in 0..3u32 {
                let result = unresolved_result(
                    tail_work(segment_number, 0),
                    9,
                    0,
                    JobId(42),
                    0,
                    DownloadLaneMode::Sequential,
                    false,
                    None,
                    true,
                    0,
                    &[],
                    "streamed",
                );
                send_owned_batch(&sender_tx, 0, vec![result], Vec::new(), stats, false)?;
            }
            send_owned_batch(
                &sender_tx,
                0,
                Vec::new(),
                vec![tail_work(9, 0)],
                stats,
                true,
            )
        });

        for segment_number in 0..3u32 {
            let event = event_rx.recv().await.expect("a streamed article");
            let OwnedDownloadLaneEvent::BatchComplete { results, ack, .. } = event else {
                panic!("streamed events are batch completions");
            };
            assert_eq!(results.len(), 1, "one article per event");
            assert_eq!(results[0].segment_id.segment_number, segment_number);
            assert!(
                ack.is_none(),
                "a streamed article must not cost a thread hop on the hot path"
            );
        }

        let event = event_rx.recv().await.expect("the closing event");
        let OwnedDownloadLaneEvent::BatchComplete {
            results,
            unrequested_works,
            ack,
            ..
        } = event
        else {
            panic!("the closing event is a batch completion");
        };
        assert!(results.is_empty());
        assert_eq!(unrequested_works.len(), 1);
        let ack = ack.expect("the closing event is the ordering barrier");
        assert!(
            !sender.is_finished(),
            "the lane is still waiting on the barrier"
        );
        ack.send(()).unwrap();
        sender.await.unwrap().unwrap();
    }

    #[test]
    fn leased_message_ids_are_bracketed_for_the_wire() {
        // Both lanes build their BODY arguments here. `DownloadWork` stores
        // the bare id, and a bare BODY argument is an article-*number*
        // reference that every real provider answers with 430.
        let works = [tail_work(1, 0), tail_work(2, 0)];

        let wire = lease_message_id_wire_forms(&works);

        assert_eq!(
            wire,
            vec![
                "<tail-1@example.invalid>".to_string(),
                "<tail-2@example.invalid>".to_string(),
            ]
        );
        assert!(
            !works[0].message_id.0.starts_with('<'),
            "the stored id is bare, which is exactly why the wire form is needed"
        );
    }

    #[test]
    fn owned_quota_park_keeps_the_cached_lane() {
        // A quota refusal never reaches the wire, so the socket is clean and
        // the lane is worth keeping for the next fill.
        let (reason, keep_cached_lane) =
            lane_stop_park(Some(LaneStop::PolicyBlocked)).expect("a stop always parks");

        assert_eq!(reason, LaneParkReason::ServerQuota);
        assert!(keep_cached_lane);
    }

    #[test]
    fn owned_transport_fault_parks_as_error_and_drops_the_connection() {
        for stop in [LaneStop::ConnectionLost, LaneStop::Error] {
            let (reason, keep_cached_lane) =
                lane_stop_park(Some(stop)).expect("a stop always parks");

            assert_eq!(reason, LaneParkReason::Error);
            assert!(!keep_cached_lane);
        }
        assert!(lane_stop_park(None).is_none());
    }

    // The refill must be asked for while the ring still has runway.
    //
    // An ordinary lease is exactly one pipe deep, so the deadline has to be
    // wider than the lease itself or the ask would land after the pipe is
    // already dry — the round-trip gap this path exists to remove.
    #[test]
    fn refill_deadline_leaves_runway_at_every_rung() {
        assert_eq!(refill_deadline(1), 3);
        assert_eq!(refill_deadline(2), 5);
        assert_eq!(refill_deadline(4), 9);
        assert_eq!(refill_deadline(8), 17);
        // A zero depth is a Sequential lane, not a stalled one.
        assert_eq!(refill_deadline(0), refill_deadline(1));

        for depth in [1usize, 2, 4, 8] {
            assert!(
                refill_deadline(depth) > depth,
                "a one-pipe lease must trip the deadline as soon as it is adopted"
            );
        }
    }

    // Work no longer current is never dialled for and never asked again:
    // contention retries a dial, not a lease that has gone. Work that is
    // current and meets contention is dialled again, a bounded number of
    // times, and the last answer stands.
    #[test]
    fn contention_retries_a_dial_but_never_an_obsolete_lease() {
        let dials = std::cell::Cell::new(0);
        let outcome = acquire_owned_lane_through_contention(
            || false,
            || {
                dials.set(dials.get() + 1);
                Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
            },
        );
        assert!(matches!(
            outcome,
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
        ));
        assert_eq!(dials.get(), 0, "an obsolete lease is never dialled for");

        let outcome = acquire_owned_lane_through_contention(
            || true,
            || {
                dials.set(dials.get() + 1);
                Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
            },
        );
        assert!(matches!(
            outcome,
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
        ));
        assert_eq!(dials.get(), OWNED_LANE_SELECTION_CONTENTION_RETRIES + 1);

        let mut current = [true, false].into_iter();
        dials.set(0);
        let outcome = acquire_owned_lane_through_contention(
            || current.next().unwrap_or(false),
            || {
                dials.set(dials.get() + 1);
                Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
            },
        );
        assert!(outcome.is_err());
        assert_eq!(
            dials.get(),
            1,
            "a lease that goes mid-retry is not dialled again"
        );
    }

    // A 430 must not tear down the owned lane.
    //
    // The article-not-found answer is a complete, bodyless server response:
    // the socket is exactly where the next BODY expects it. Marking it dirty
    // used to QUIT the TLS session, drop the rest of the leased batch as
    // Unrequested, and block the server's pipelining proof — all because one
    // article lives on another provider.
    #[test]
    fn owned_article_not_found_keeps_the_lane_and_the_batch_clean() {
        let result = result_from_trace(
            tail_work(9, 0),
            0,
            0,
            JobId(42),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: vec![weaver_nntp::client::FetchAttemptTrace {
                    route_feedback: None,
                    connection_health: None,
                    server_idx: 0,
                    remote_ip: None,
                    elapsed: Duration::from_millis(3),
                    outcome: weaver_nntp::client::FetchAttemptOutcome::NotFound,
                    error: Some("article not found".to_string()),
                }],
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::NntpError::NoSuchArticle {
                        message_id: "<tail-9@example.invalid>".to_string(),
                    },
                )),
            },
            DownloadLaneObservation {
                server_idx: Some(0),
                mode: DownloadLaneMode::Pipelined { depth: 4 },
                supports_pipelining: true,
                latency_sample: None,
                cold: false,
                transfer: None,
                payload_bytes: 0,
                policy_elapsed: Duration::ZERO,
                pressure_clear: true,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
            },
            &[],
        );

        // The work item itself still fails, so the completion path excludes
        // this server and retries the article elsewhere.
        assert!(matches!(
            result.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::ArticleNotFound,
                ..
            }))
        ));
        // …but the lane keeps its cached connection and keeps refilling.
        let observation = result.lane_observation.unwrap();
        assert!(observation.batch_clean);
        assert!(!observation.connection_discarded);
        assert!(download_outcome_keeps_connection(&result.data));

        // A clean batch is what keeps the rung: only the unclean path drops
        // one, and a 430 must not reach it.
        let mut explorer = crate::pipeline::download::transport::ServerPipelineExplorer::seeded(
            Some(observation.mode.depth()),
            None,
            None,
        );
        assert_eq!(explorer.current_depth(), 4);
        assert_eq!(
            explorer.note_unclean_batch(Instant::now()),
            Some(crate::pipeline::download::transport::RungChange::Dropped { from: 4, to: 2 }),
            "the unclean path is the only one that costs a rung"
        );

        // The rest of the lease is still requested, not handed back: a 430
        // sets no stop, so the ring keeps being topped up from `pending`.
        assert!(lane_stop_park(None).is_none());
    }

    // Transport faults must stay dirty. Only the "fully consumed response"
    // outcomes are allowed to keep the connection.
    #[test]
    fn owned_transport_and_decode_failures_still_discard_the_connection() {
        for data in [
            Err::<DownloadPayload, _>(DownloadError::Fetch(DownloadFailure::new(
                DownloadFailureKind::EstablishedTransport,
                "connection closed mid-body",
            ))),
            Err(DownloadError::Decode {
                raw_size: 128,
                error: "invalid yEnc header".to_string(),
            }),
        ] {
            assert!(
                !download_outcome_keeps_connection(&data),
                "a possibly mid-body socket must not be reused"
            );
        }
    }

    #[test]
    fn owned_quota_result_keeps_connection_observation_clean() {
        let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
        let control = transfers.configure(
            weaver_nntp::transfer::StableServerId(77),
            weaver_nntp::transfer::ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                    limit_bytes: 1,
                    generation: 1,
                    retry_at: Some(Instant::now() + Duration::from_secs(30)),
                }),
            },
        );
        let _reservation = control.try_reserve(1).unwrap();
        let rejection = control.try_reserve(1).err().unwrap();
        let result = result_from_trace(
            tail_work(6, 0),
            0,
            0,
            JobId(42),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::NntpError::quota_blocked(rejection),
                )),
            },
            DownloadLaneObservation {
                server_idx: Some(0),
                mode: DownloadLaneMode::Sequential,
                supports_pipelining: true,
                latency_sample: None,
                cold: false,
                transfer: None,
                payload_bytes: 0,
                policy_elapsed: Duration::ZERO,
                pressure_clear: true,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
            },
            &[],
        );

        assert!(matches!(
            result.data,
            Err(DownloadError::Fetch(DownloadFailure {
                kind: DownloadFailureKind::ServerQuota,
                ..
            }))
        ));
        let observation = result.lane_observation.unwrap();
        assert!(observation.batch_clean);
        assert!(!observation.connection_discarded);
        assert_eq!(observation.unresolved_count, 0);
    }

    #[test]
    fn owned_quota_cache_survives_wait_and_reset_but_reselects_another_fill() {
        let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
        let control = transfers.configure(
            weaver_nntp::transfer::StableServerId(78),
            weaver_nntp::transfer::ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                    limit_bytes: 1,
                    generation: 1,
                    retry_at: Some(Instant::now() + Duration::from_secs(30)),
                }),
            },
        );
        let _reservation = control.try_reserve(1).unwrap();
        let rejection = control.try_reserve(1).err().unwrap();
        let cached_server = weaver_nntp::pool::ServerId(0);

        let all_blocked = weaver_nntp::client::BodyServerSelection {
            eligible: Vec::new(),
            quota_blocked: Some(rejection.clone()),
        };
        assert!(keep_cached_lane_after_park(LaneParkReason::ServerQuota));
        assert!(cached_lane_matches_selection(
            cached_server,
            true,
            &all_blocked
        ));

        let after_reset = weaver_nntp::client::BodyServerSelection {
            eligible: vec![cached_server],
            quota_blocked: None,
        };
        assert!(cached_lane_matches_selection(
            cached_server,
            false,
            &after_reset
        ));

        let alternate_fill = weaver_nntp::client::BodyServerSelection {
            eligible: vec![weaver_nntp::pool::ServerId(1)],
            quota_blocked: Some(rejection),
        };
        assert!(!cached_lane_matches_selection(
            cached_server,
            true,
            &alternate_fill
        ));
    }
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    use crate::jobs::ids::JobId;

    fn test_client() -> Arc<weaver_nntp::NntpClient> {
        Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".to_string(),
                    port: 1,
                    tls: false,
                    ..Default::default()
                },
                4,
            ),
        ))
    }

    #[test]
    fn queued_lease_requires_current_admission_and_runtime() {
        let nntp = test_client();
        let state = test_admitted_state(JobId(42));
        let mut lease = test_lease(JobId(42), 0, Vec::new(), vec![tail_work(1, 0)]);
        assert!(lease_is_current(&state, &nntp, &lease));
        state.queue.write(|inner| {
            inner.jobs[0].report.nzb_admitted = false;
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease));
        state.queue.write(|inner| {
            inner.jobs[0].report.nzb_admitted = true;
            inner.jobs[0].intents.paused = Some(true);
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease));
        state.queue.write(|inner| {
            inner.jobs[0].intents.paused = Some(false);
            inner.globals.nntp_generation = 1;
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease));
        lease.runtime_generation = 1;
        assert!(lease_is_current(&state, &nntp, &lease));
        state.queue.write(|inner| {
            inner.globals.paused = true;
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease));
    }

    // A lease is cut for one server and pinned to it, so two works whose own
    // exclusions differ stay current while that server may serve both.
    #[test]
    fn a_lease_of_works_with_differing_exclusions_stays_current_on_its_server() {
        let server = |port| weaver_nntp::pool::ServerPoolConfig {
            max_connections: 4,
            ..weaver_nntp::pool::ServerPoolConfig::new(weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
                ..Default::default()
            })
        };
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig {
                servers: vec![server(1), server(2)],
                ..weaver_nntp::client::NntpClientConfig::single(
                    weaver_nntp::ServerConfig::default(),
                    1,
                )
            },
        ));
        let state = test_admitted_state(JobId(42));
        let mut not_found_on_zero = tail_work(1, 1);
        not_found_on_zero.exclude_servers = vec![0];
        let lease = test_lease(
            JobId(42),
            0,
            vec![0],
            vec![not_found_on_zero, tail_work(2, 0)],
        );
        assert!(lease_is_current(&state, &nntp, &lease));
    }

    fn test_run(
        nntp: &Arc<weaver_nntp::NntpClient>,
        exclude_servers: Vec<usize>,
    ) -> Box<OwnedLaneRun> {
        let (event_tx, _event_rx) = mpsc::channel(4);
        let (refill_tx, _refill_rx) = mpsc::channel(4);
        let (parked_tx, _parked_rx) = mpsc::channel(4);
        // The receivers are dropped with the run; routing never sends on them.
        let mut lease = test_lease(JobId(7), 1, Vec::new(), vec![tail_work(1, 0)]);
        lease.dial_exclude_servers = exclude_servers;
        Box::new(OwnedLaneRun {
            nntp: Arc::clone(nntp),
            state: crate::jobs::handle::SharedPipelineState::new(
                crate::operations::metrics::PipelineMetrics::new(),
                Vec::new(),
            ),
            event_tx,
            refill_tx,
            parked_tx,
            initial_lease: lease,
        })
    }

    fn idle_lane(nntp: &Arc<weaver_nntp::NntpClient>, server: usize) -> IdleOwnedLane {
        IdleOwnedLane {
            nntp: Arc::downgrade(nntp),
            server: weaver_nntp::pool::ServerId(server),
        }
    }

    fn shared_with(idle: Vec<Option<IdleOwnedLaneWorker>>) -> OwnedLanePoolShared {
        let workers: Vec<_> = idle
            .into_iter()
            .enumerate()
            .map(|(index, idle)| {
                Some(OwnedLaneWorkerSlot {
                    id: index as u64,
                    sender: std_mpsc::channel().0,
                    idle,
                })
            })
            .collect();
        OwnedLanePoolShared {
            cap: workers.len(),
            next_worker_id: workers.len() as u64,
            workers,
            queued_runs: VecDeque::new(),
            intervals: OwnedLaneIntervals::default(),
            observer: None,
        }
    }

    // The worker the test fixtures put in slot `index`.
    fn seat(index: usize) -> OwnedLaneSeat {
        OwnedLaneSeat {
            index,
            id: index as u64,
        }
    }

    fn slot(shared: &OwnedLanePoolShared, index: usize) -> &OwnedLaneWorkerSlot {
        shared.workers[index].as_ref().expect("an occupied slot")
    }

    fn idle_with(lane: Option<IdleOwnedLane>) -> Option<IdleOwnedLaneWorker> {
        Some(IdleOwnedLaneWorker { lane })
    }

    // Routing, not rotation.
    //
    // After a wave parks, every worker sits idle holding a connection — and
    // with it every server permit. Handing the next lease to whichever worker
    // the counter lands on makes a warm connection useless: the chosen worker
    // parks a connection it cannot use and then tries to dial one the idle
    // lanes are holding the permits for. The order here is the cost of
    // starting: no dial, one dial, a park and a dial.
    #[test]
    fn a_lease_goes_to_the_worker_that_can_start_it_soonest() {
        let nntp = test_client();
        let run = test_run(&nntp, vec![3]);
        let mut shared = shared_with(vec![
            // 0: busy on a lease.
            None,
            // 1: idle holding a connection on a server this lease excludes.
            idle_with(Some(idle_lane(&nntp, 3))),
            // 2: idle with no connection at all.
            idle_with(None),
            // 3: idle holding a connection this lease can use.
            idle_with(Some(idle_lane(&nntp, 1))),
        ]);

        assert_eq!(
            shared.claim_worker_for(&run),
            Some(3),
            "the warm, servable connection comes first"
        );
        assert_eq!(
            shared.claim_worker_for(&run),
            Some(2),
            "then the worker that only has to dial"
        );
        assert_eq!(
            shared.claim_worker_for(&run),
            Some(1),
            "then the one that must park a connection before dialling"
        );
        assert_eq!(
            shared.claim_worker_for(&run),
            None,
            "the busy worker is never offered a second lease"
        );
    }

    // A claim is what stops two submits choosing the same idle worker: the
    // marker is taken at selection time, not when the worker starts running.
    #[test]
    fn claiming_a_worker_takes_it_out_of_the_running() {
        let nntp = test_client();
        let run = test_run(&nntp, Vec::new());
        let mut shared = shared_with(vec![idle_with(Some(idle_lane(&nntp, 0)))]);

        assert_eq!(shared.claim_worker_for(&run), Some(0));
        assert_eq!(shared.claim_worker_for(&run), None);
    }

    // An excluded server is not a connection this lease may use, however warm
    // it is.
    #[test]
    fn an_excluded_server_is_not_a_match() {
        let nntp = test_client();
        let run = test_run(&nntp, vec![1]);
        let lane = idle_lane(&nntp, 1);

        assert!(!lane.serves(&run));
        assert!(idle_lane(&nntp, 0).serves(&run));
    }

    // The newsgroups a connection was opened for are no reason to redial:
    // the worker re-points the socket at the next lease's groups, so a job
    // boundary keeps every warm connection in play.
    #[test]
    fn a_connection_opened_for_other_newsgroups_still_serves() {
        let nntp = test_client();
        let run = test_run(&nntp, Vec::new());

        assert!(idle_lane(&nntp, 0).serves(&run));
        let mut shared = shared_with(vec![idle_with(None), idle_with(Some(idle_lane(&nntp, 0)))]);
        assert_eq!(
            shared.claim_worker_for(&run),
            Some(1),
            "the warm connection is preferred over a fresh dial whatever its groups"
        );
    }

    // Whatever connection a lease lands on is pointed at that lease's own
    // newsgroups. An empty list is not the same thing: a server that answers
    // a message-id fetch only after a GROUP prologue is sent none, and a
    // fresh dial has no candidate for its initial group probe.
    #[test]
    fn a_lease_carries_the_newsgroups_its_work_was_posted_to() {
        let lease = test_lease(JobId(7), 1, Vec::new(), vec![tail_work(1, 0)]);
        assert_eq!(
            lease_groups(&lease),
            ["alt.binaries.test".to_string()],
            "the lane is opened for the lease's groups"
        );

        let run = test_run(&test_client(), Vec::new());
        assert_eq!(
            lease_groups(&run.initial_lease),
            ["alt.binaries.test".to_string()],
            "a run carries them through to the worker"
        );
    }

    // A connection belonging to a client that has been retired matches
    // nothing: the marker is weak precisely so it cannot keep one alive.
    #[test]
    fn a_connection_from_another_client_is_not_a_match() {
        let nntp = test_client();
        let other = test_client();
        let run = test_run(&nntp, Vec::new());

        assert!(!idle_lane(&other, 0).serves(&run));
        assert!(!test_idle_lane(0).serves(&run));
    }

    // With every worker mid-lease the run waits for the pool, not for one
    // worker's private channel: whichever finishes first takes it, and it
    // does so before publishing itself idle so the run cannot be missed.
    #[test]
    fn a_run_with_no_idle_worker_waits_for_whichever_finishes_first() {
        let nntp = test_client();
        let run = test_run(&nntp, Vec::new());
        let mut shared = shared_with(vec![None, None]);

        assert!(shared.claim_worker_for(&run).is_none());
        shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));

        // Worker 1 finishes first and takes the queued run instead of going
        // idle; worker 0 finishes into an empty queue and publishes itself.
        assert!(
            shared
                .take_queued_run_or_publish_idle(seat(1), None)
                .is_some_and(|work| matches!(
                    work,
                    QueuedOwnedLaneWork::Run(run) if run.initial_lease.job_id == JobId(7)
                ))
        );
        assert!(slot(&shared, 1).idle.is_none(), "worker 1 is running it");
        assert!(shared.take_queued_run_or_publish_idle(seat(0), None).is_none());
        assert!(slot(&shared, 0).idle.is_some());
    }

    #[test]
    fn reset_returns_queued_leases_before_a_worker_can_take_old_work() {
        let nntp = test_client();
        let (sender, commands) = std_mpsc::channel();
        let mut shared = shared_with(vec![None]);
        shared.workers[0].as_mut().unwrap().sender = sender;
        for segment_number in 0..3 {
            let mut run = test_run(&nntp, Vec::new());
            run.initial_lease.works[0].segment_id.segment_number = segment_number;
            shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));
        }
        let pool = OwnedDownloadLanePool {
            shared: Arc::new(std::sync::Mutex::new(shared)),
            reset_calls: AtomicUsize::new(0),
        };

        let returned = pool.reset().leases;

        assert_eq!(returned.len(), 3);
        for (index, lease) in returned.iter().enumerate() {
            assert_eq!(lease.job_id, JobId(7));
            assert_eq!(lease.works.len(), 1);
            assert_eq!(lease.works[0].segment_id.segment_number, index as u32);
        }
        assert_eq!(
            Arc::strong_count(&nntp),
            1,
            "queued runs release the old client"
        );
        assert!(
            lock_pool(&pool.shared)
                .take_queued_run_or_publish_idle(seat(0), None)
                .is_none(),
            "finishing a lease must reach the pending reset without another old run"
        );
        assert!(matches!(
            commands.try_recv(),
            Ok(OwnedLanePoolCommand::Reset)
        ));
        assert!(
            pool.reset().leases.is_empty(),
            "leases are returned only once"
        );
    }

    // Only workers with nothing to lose are warmed: a worker already holding
    // a connection has nothing to gain, and a busy one is not there to ask.
    #[test]
    fn warming_targets_idle_workers_without_a_connection() {
        let nntp = test_client();
        let shared = shared_with(vec![
            None,
            idle_with(Some(idle_lane(&nntp, 0))),
            idle_with(None),
            idle_with(None),
        ]);

        assert_eq!(shared.idle_workers_without_a_lane(8).len(), 2);
        assert_eq!(
            shared.idle_workers_without_a_lane(1).len(),
            1,
            "never more than the job's share of the free connections"
        );
        assert!(shared.idle_workers_without_a_lane(0).is_empty());
    }

    // A server that greets, answers CAPABILITIES with nothing beyond a
    // reader, and refuses everything else, until its client hangs up.
    fn greeting_server() -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{BufRead, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut writer = socket.try_clone().unwrap();
            writer.write_all(b"200 fixture ready\r\n").unwrap();
            for line in std::io::BufReader::new(socket).lines() {
                let Ok(line) = line else { break };
                let reply: &[u8] = if line == "CAPABILITIES" {
                    b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
                } else {
                    b"500 unsupported\r\n"
                };
                if writer.write_all(reply).is_err() {
                    break;
                }
            }
        });
        (port, server)
    }

    fn warm_for(nntp: &Arc<weaver_nntp::NntpClient>, seat: usize) -> OwnedLaneWarm {
        OwnedLaneWarm {
            nntp: Arc::clone(nntp),
            exclude_servers: Arc::from(Vec::new()),
            byte_estimate: 750_000,
            seat: weaver_nntp::pool::ServerId(seat),
        }
    }

    // A warm lane is an idle lane: it is published exactly like one a park
    // kept, so the next lease for that job is routed straight to it.
    #[test]
    fn a_warm_connection_is_published_as_an_idle_lane() {
        let (port, server) = greeting_server();
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".to_string(),
                    port,
                    tls: false,
                    ..Default::default()
                },
                4,
            ),
        ));
        let shared = Arc::new(std::sync::Mutex::new(shared_with(vec![idle_with(None)])));
        let run = test_run(&nntp, Vec::new());
        let mut cached = None;

        let started = answer_idle_command(
            seat(0),
            &shared,
            &mut cached,
            OwnedLanePoolCommand::Warm(Box::new(warm_for(&nntp, 0))),
        );

        assert!(started.is_none(), "a warm starts no run");
        assert!(
            cached.is_some(),
            "the worker now holds the warmed connection"
        );
        assert!(
            slot(&lock_pool(&shared), 0)
                .idle
                .as_ref()
                .and_then(|idle| idle.lane.as_ref())
                .is_some_and(|lane| lane.server.0 == 0),
            "the warmed connection is published on the worker's idle slot"
        );
        assert_eq!(
            lock_pool(&shared).claim_worker_for(&run),
            Some(0),
            "the warmed connection is the one this lease is routed to"
        );

        drop(cached);
        server.join().unwrap();
    }

    // A warm makes the server choice a lease would make. A fill server that
    // cannot be reached is not a reason to open the backfill tier, so a warm
    // with only a backfill server answering stays without a connection, even
    // when the seat it is handed is on the backfill server.
    #[test]
    fn a_warm_never_dials_a_backfill_server() {
        let backfill = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        backfill.set_nonblocking(true).unwrap();
        let server = |port: u16, backfill: bool| weaver_nntp::pool::ServerPoolConfig {
            max_connections: 4,
            backfill,
            ..weaver_nntp::pool::ServerPoolConfig::new(weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
                ..Default::default()
            })
        };
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig {
                servers: vec![
                    // Nothing listens on port 1: every dial is refused.
                    server(1, false),
                    server(backfill.local_addr().unwrap().port(), true),
                ],
                ..weaver_nntp::client::NntpClientConfig::single(
                    weaver_nntp::ServerConfig::default(),
                    1,
                )
            },
        ));
        let mut cached = None;

        warm_cached_lane(&mut cached, warm_for(&nntp, 0));
        warm_cached_lane(&mut cached, warm_for(&nntp, 1));

        assert!(cached.is_none(), "no fill server answered the warm");
        // A dial completes its TCP handshake into the listener's backlog
        // before it returns, so a dial to the backfill server would be
        // waiting here.
        assert_eq!(
            backfill.accept().map(|_| ()).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "the warm never dialed the backfill server"
        );
    }

    // A server that holds every article whose id ends in `-0` and nothing
    // else, answering STAT and HEAD, until its one client hangs up.
    fn stat_fixture() -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{BufRead, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut writer = socket.try_clone().unwrap();
            writer.write_all(b"200 fixture ready\r\n").unwrap();
            for line in std::io::BufReader::new(socket).lines() {
                let Ok(line) = line else { break };
                let upper = line.to_ascii_uppercase();
                let id = line.get(5..).unwrap_or_default().trim();
                let held = id
                    .trim_matches(['<', '>'])
                    .split('@')
                    .next()
                    .unwrap_or_default()
                    .ends_with("-0");
                let reply = if upper == "CAPABILITIES" {
                    "101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n".to_string()
                } else if upper.starts_with("STAT ") && held {
                    format!("223 0 {id}\r\n")
                } else if upper.starts_with("HEAD ") && held {
                    format!("221 0 {id}\r\nSubject: fixture\r\n.\r\n")
                } else if upper.starts_with("STAT ") || upper.starts_with("HEAD ") {
                    "430 no such article\r\n".to_string()
                } else if upper.starts_with("QUIT") {
                    let _ = writer.write_all(b"205 closing\r\n");
                    break;
                } else {
                    "500 unsupported\r\n".to_string()
                };
                if writer.write_all(reply.as_bytes()).is_err() {
                    break;
                }
            }
        });
        (port, server)
    }

    // A probe is lane work and takes its lane as a lease does. A dial that
    // finds the server health state busy through every retry is no answer
    // from the server: the batch comes back unasked, and the seating after
    // that asks it and gets the server's word.
    #[test]
    fn a_probe_that_meets_a_busy_health_state_is_handed_back_unasked_and_asked_next_time() {
        let (port, server) = stat_fixture();
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".to_string(),
                    port,
                    tls: false,
                    ..Default::default()
                },
                1,
            ),
        ));
        let (mail, _heard) = mpsc::channel(4);
        let probe = |batch: crate::pipeline::post_health::ProbeBatch| OwnedLaneProbe {
            nntp: Arc::clone(&nntp),
            seat: weaver_nntp::pool::ServerId(0),
            ids: batch.unfound(),
            batch,
            mail: mail.clone(),
        };
        let batch = crate::pipeline::post_health::ProbeBatch::new(
            JobId(1),
            1,
            vec![
                "<lantern-0@example.com>".into(),
                "<lantern-1@example.com>".into(),
            ],
        );
        let mut cached = None;

        let health = Arc::clone(nntp.pool().health());
        let busy = health
            .try_lock()
            .expect("nothing else holds the health state");
        let first = ask_owned_lane_probe(&mut cached, probe(batch));
        drop(busy);
        let crate::pipeline::post_health::ProbeMail::Unseated(batch) = first.word else {
            panic!(
                "a busy health state is not the server's answer: {:?}",
                first.word
            );
        };
        assert!(batch.asked().is_empty(), "the batch comes back unasked");
        assert_eq!(batch.unfound().len(), 2, "nothing was folded into it");

        let second = ask_owned_lane_probe(&mut cached, probe(batch));
        let crate::pipeline::post_health::ProbeMail::Answered(answer) = second.word else {
            panic!("the next seating asks the batch: {:?}", second.word);
        };
        assert_eq!(answer.server, 0);
        assert!(!answer.result.inconclusive, "the server settled the batch");
        assert_eq!(answer.result.exists, vec![true, false]);
        assert!(
            cached
                .as_ref()
                .is_some_and(|cached| cached.lane.server_id().0 == 0),
            "the lane that answered stays with the worker"
        );

        park_cached_lane(&mut cached);
        server.join().unwrap();
    }

    // A worker that has already been claimed must not be republished as idle
    // by a command it answers on the way to its next run.
    #[test]
    fn a_claimed_worker_is_not_republished_as_idle() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);
        let run = test_run(&nntp, Vec::new());

        assert_eq!(shared.claim_worker_for(&run), Some(0));
        shared.note_idle_lane(seat(0), Some(idle_lane(&nntp, 0)));

        assert!(
            slot(&shared, 0).idle.is_none(),
            "its next lease is already on the way"
        );
    }

    // Submit a run whose lease is stale: its worker reports it obsolete on
    // `events` and is done. While `events` is full the report waits for
    // room, so the test decides how long the worker stays busy.
    fn submit_obsolete_run(
        pool: &OwnedDownloadLanePool,
        nntp: &Arc<weaver_nntp::NntpClient>,
        events: &mpsc::Sender<OwnedDownloadLaneEvent>,
    ) {
        let (refill_tx, _refill_rx) = mpsc::channel(1);
        let (parked_tx, _parked_rx) = mpsc::channel(1);
        pool.submit(
            Arc::clone(nntp),
            crate::jobs::handle::SharedPipelineState::new(
                crate::operations::metrics::PipelineMetrics::new(),
                Vec::new(),
            ),
            events.clone(),
            refill_tx,
            parked_tx,
            test_lease(JobId(7), 1, Vec::new(), vec![tail_work(1, 0)]),
        )
        .unwrap_or_else(|_| panic!("a pool with room takes the run"));
    }

    fn obsolete_report() -> OwnedDownloadLaneEvent {
        OwnedDownloadLaneEvent::LeaseObsolete {
            lease: test_lease(JobId(9), 1, Vec::new(), vec![tail_work(1, 0)]),
        }
    }

    // Wait for `count` idle publications, whichever workers they come from.
    fn await_idle(observed: &std_mpsc::Receiver<OwnedLaneObservation>, count: usize) {
        let mut seen = 0;
        while seen < count {
            if let OwnedLaneObservation::Idle(_) = observed.recv().unwrap() {
                seen += 1;
            }
        }
    }

    // A pool sized for many connections starts no thread until work comes,
    // and then only as many as the work needs at once.
    #[test]
    fn lane_threads_start_on_demand_up_to_the_cap() {
        let nntp = test_client();
        let pool = OwnedDownloadLanePool::new(48);
        let observed = pool.observe();
        assert_eq!(pool.worker_count(), 48, "the cap is the configured count");
        assert_eq!(pool.live_worker_count(), 0, "no thread before any work");

        let (events, mut reports) = mpsc::channel(1);
        events.try_send(obsolete_report()).unwrap();
        submit_obsolete_run(&pool, &nntp, &events);
        assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Spawned(0));
        assert_eq!(pool.live_worker_count(), 1, "one run, one thread");

        // The first worker cannot finish while `events` is full.
        submit_obsolete_run(&pool, &nntp, &events);
        assert_eq!(
            observed.recv().unwrap(),
            OwnedLaneObservation::Spawned(1),
            "a run that finds every live worker busy starts another"
        );
        assert_eq!(pool.live_worker_count(), 2);

        for _ in 0..3 {
            reports.blocking_recv().unwrap();
        }
        await_idle(&observed, 2);
        submit_obsolete_run(&pool, &nntp, &events);
        assert_eq!(
            pool.live_worker_count(),
            2,
            "an idle worker takes the next run without a new thread"
        );
        reports.blocking_recv().unwrap();
    }

    // A worker idle past the release interval gives up its slot, and the
    // next run starts a thread in that same slot.
    #[test]
    fn an_idle_worker_leaves_and_its_slot_is_reused() {
        let nntp = test_client();
        let pool = OwnedDownloadLanePool::with_test_intervals(
            4,
            Duration::from_millis(1),
            Duration::ZERO,
        );
        let observed = pool.observe();
        let (events, mut reports) = mpsc::channel(4);

        submit_obsolete_run(&pool, &nntp, &events);
        assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Spawned(0));
        reports.blocking_recv().unwrap();
        assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Idle(0));
        assert_eq!(observed.recv().unwrap(), OwnedLaneObservation::Vacated(0));
        assert_eq!(pool.live_worker_count(), 0, "the thread is gone");

        submit_obsolete_run(&pool, &nntp, &events);
        assert_eq!(
            observed.recv().unwrap(),
            OwnedLaneObservation::Spawned(0),
            "the vacated slot is the one reused"
        );
        reports.blocking_recv().unwrap();
    }

    // A leaving worker counts against the cap until its slot is empty, so a
    // run queued while it let its connection go may have been queued for
    // it. It takes that run instead of leaving; with nothing queued it goes.
    #[test]
    fn a_leaving_worker_takes_work_queued_on_its_way_out() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);

        assert!(matches!(
            shared.begin_release(seat(0)),
            OwnedLaneRelease::Withdrawn
        ));
        let run = test_run(&nntp, Vec::new());
        assert!(
            shared.claim_worker_for(&run).is_none(),
            "a withdrawn worker is offered nothing"
        );
        shared.queued_runs.push_back(QueuedOwnedLaneWork::Run(run));
        assert!(matches!(
            shared.finish_release(seat(0)),
            OwnedLaneRelease::Work(QueuedOwnedLaneWork::Run(_))
        ));
        assert!(shared.workers[0].is_some(), "it keeps its slot to run it");

        assert!(shared.take_queued_run_or_publish_idle(seat(0), None).is_none());
        assert!(matches!(
            shared.begin_release(seat(0)),
            OwnedLaneRelease::Withdrawn
        ));
        assert!(matches!(
            shared.finish_release(seat(0)),
            OwnedLaneRelease::Left
        ));
        assert!(shared.workers.is_empty(), "the slot is vacated");
    }

    // Queued work found as a worker starts to leave is taken at once.
    #[test]
    fn a_worker_about_to_leave_takes_queued_work_first() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);
        shared
            .queued_runs
            .push_back(QueuedOwnedLaneWork::Run(test_run(&nntp, Vec::new())));

        assert!(matches!(
            shared.begin_release(seat(0)),
            OwnedLaneRelease::Work(QueuedOwnedLaneWork::Run(_))
        ));
        assert!(slot(&shared, 0).idle.is_none(), "it is busy with the run");
    }

    // A worker claimed for a run does not leave: the run is already in its
    // channel.
    #[test]
    fn a_claimed_worker_stays_for_its_run() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);

        assert_eq!(shared.claim_worker_for(&test_run(&nntp, Vec::new())), Some(0));
        assert!(matches!(
            shared.begin_release(seat(0)),
            OwnedLaneRelease::Stay
        ));
        assert!(shared.workers[0].is_some());
    }

    // A worker a resize sent away must not publish itself into a slot a
    // later worker has taken.
    #[test]
    fn a_departed_worker_never_publishes_into_its_successor_s_slot() {
        let mut shared = shared_with(vec![None]);
        shared.workers[0].as_mut().unwrap().id = 5;

        assert!(shared.take_queued_run_or_publish_idle(seat(0), None).is_none());
        assert!(slot(&shared, 0).idle.is_none(), "the successor stays busy");
        shared.vacate(seat(0));
        assert!(shared.workers[0].is_some(), "nor can it vacate the slot");
    }
}

#[cfg(test)]
mod fault_tests;
