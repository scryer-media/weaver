// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use std::collections::VecDeque;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

pub(crate) struct OwnedDownloadLanePool {
    shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
    // The worker holds the press of a dial a full route refused. A dial
    // routed to it settles that press, so the next run without a connection
    // to reuse goes here first.
    pressing: bool,
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
                .contains(self.server.0)
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
    // The server had proven before this connection was dialled that it
    // refuses message-id fetches without a selected group, so the
    // connection selects one of the groups of every lease that lists any.
    group_required_at_dial: bool,
    // The connection has a group selected.
    group_selected: bool,
}

// The press a dial refused for a full route leaves: the demand of every
// tunnel that refused it, whichever error won. The lane holds it, not the
// work it dialled for, so it lasts across the work's paced requeue and across
// a refusal some other server's answer won: until this lane's next dial
// resolves (a connection drops it, a new refusal replaces it), the lane is
// retired or reset, or the job it dialled for stops.
struct RoutePress {
    state: crate::jobs::handle::SharedPipelineState,
    job_id: JobId,
    generation: u64,
    // The job's counters and this refusal's report among the job's lane
    // refusals (see `JobCounters::drained_through`), when the lease carried
    // the counters.
    report: Option<(Arc<crate::pipeline::queue::JobCounters>, u64)>,
    _demand: Vec<weaver_tunnel::pipe::Demand>,
}

impl RoutePress {
    // Whether the job the refused dial was for still wants downloads, and
    // still has download work to hand out: once a pass after this refusal
    // was heard left it none (its work went to other lanes, or it has moved
    // on past its download), nothing waits for this lane's dial.
    fn stands(&self) -> bool {
        job_work_is_current(&self.state, self.job_id, self.generation)
            && self.report.as_ref().is_none_or(|(counters, report)| {
                counters
                    .drained_through
                    .load(std::sync::atomic::Ordering::Relaxed)
                    < *report
            })
    }
}

// Take the next of the job's lane refusal reports, for the refusal about to
// be sent for `lease`.
fn next_refusal_report(
    lease: &DownloadBatchLease,
) -> Option<(Arc<crate::pipeline::queue::JobCounters>, u64)> {
    let counters = lease.works.iter().find_map(|work| work.counters.as_ref())?;
    let report = counters
        .lane_refusals_sent
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    Some((Arc::clone(counters), report))
}

// The press a lane's refused dial for `lease` leaves, for the pipeline's
// tests: it reports the refusal the way the lane does, and holds no demand.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub(in crate::pipeline) struct TestRoutePress(RoutePress);

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
impl TestRoutePress {
    pub(in crate::pipeline) fn refused(
        state: &crate::jobs::handle::SharedPipelineState,
        lease: &DownloadBatchLease,
    ) -> Self {
        Self(RoutePress {
            state: state.clone(),
            job_id: lease.job_id,
            generation: lease.runtime_generation,
            report: next_refusal_report(lease),
            _demand: Vec::new(),
        })
    }

    pub(in crate::pipeline) fn stands(&self) -> bool {
        self.0.stands()
    }
}

// Settle this worker's press after it did something, and publish the change
// so the next dial without a connection to reuse is routed here. A connection
// the worker now holds settles the press, as does its job stopping.
fn settle_route_press(
    seat: OwnedLaneSeat,
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    cached_lane: &Option<CachedOwnedLane>,
    press: &mut Option<RoutePress>,
    published: &mut bool,
) {
    if cached_lane.is_some() || press.as_ref().is_some_and(|press| !press.stands()) {
        *press = None;
    }
    let pressing = press.is_some();
    if pressing != *published {
        lock_pool(shared).note_press(seat, pressing);
        *published = pressing;
    }
}

// What a warm dial needs: the article exclusions and size a lease for this
// job would carry, so the dial makes the server choice that lease would, and
// the one server whose unclaimed seat it may fill.
pub(crate) struct OwnedLaneWarm {
    nntp: Arc<weaver_nntp::NntpClient>,
    exclude_servers: weaver_nntp::ServerSet,
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
                self.workers.iter().position(|worker| {
                    worker.as_ref().is_some_and(|slot| slot.pressing)
                        && idle_marker(worker).is_some_and(|idle| idle.lane.is_none())
                })
            })
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

    // Publish whether the worker in `seat` holds a refused dial's press.
    fn note_press(&mut self, seat: OwnedLaneSeat, pressing: bool) {
        if let Some(worker) = self.seat_mut(seat) {
            worker.pressing = pressing;
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
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(crate) fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::Relaxed)
    }

    // Fence queued runs before workers can take another retired-client lease.
    // The actor returns these unstarted leases without charging article retries.
    pub(crate) fn reset(&self) -> StoppedOwnedLaneWork {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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

    // The Err variant hands the lease back to the caller; it only occurs
    // when the pool is stopped, so the size of the returned lease is
    // irrelevant.
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
        route_owned_lane_work(
            &self.shared,
            &mut shared,
            claimed,
            QueuedOwnedLaneWork::Run(run),
        )
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
        exclude_servers: weaver_nntp::ServerSet,
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
                exclude_servers,
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
            weaver_server_base::runtime::affinity::pin_current_thread_for_hot_download_path();
            weaver_server_base::runtime::thread_release::start_lane_thread();
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
        pressing: false,
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
    let mut press: Option<RoutePress> = None;
    let mut published_press = false;
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
                owed = run_owned_lane_work(&mut cached_lane, &mut press, work);
                break;
            }
        }
    }
    'work: loop {
        settle_route_press(seat, shared, &cached_lane, &mut press, &mut published_press);
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
            owed = run_owned_lane_work(&mut cached_lane, &mut press, work);
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
                    // A press whose job stopped while nothing was routed here
                    // is let go on this same check.
                    settle_route_press(
                        seat,
                        shared,
                        &cached_lane,
                        &mut press,
                        &mut published_press,
                    );
                    if idle_since.elapsed() >= intervals.idle_release {
                        match release_idle_worker(seat, shared, &mut cached_lane) {
                            OwnedLaneRelease::Stay => {}
                            OwnedLaneRelease::Work(work) => {
                                owed = run_owned_lane_work(&mut cached_lane, &mut press, work);
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
            // A reset retires what the lane holds, its press with it.
            if matches!(command, OwnedLanePoolCommand::Reset) {
                press = None;
            }
            if let Some(work) = answer_idle_command(seat, shared, &mut cached_lane, command) {
                owed = run_owned_lane_work(&mut cached_lane, &mut press, work);
                continue 'work;
            }
            settle_route_press(seat, shared, &cached_lane, &mut press, &mut published_press);
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
    press: &mut Option<RoutePress>,
    work: QueuedOwnedLaneWork,
) -> Option<OwedProbeAnswer> {
    let owed = match work {
        QueuedOwnedLaneWork::Run(run) => {
            run_owned_lane(cached_lane, press, *run);
            None
        }
        QueuedOwnedLaneWork::Probe(probe) => Some(ask_owned_lane_probe(cached_lane, *probe)),
    };
    give_back_unanswered_probe(cached_lane);
    owed
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
            give_back_unanswered_probe(cached_lane);
            lock_pool(shared)
                .note_idle_lane(seat, cached_lane.as_ref().map(IdleOwnedLane::from_cached));
        }
    }
    None
}

impl CachedOwnedLane {
    // A connection just dialled for `groups`. `group_required_at_dial` is
    // read before the dial: the requirement is only ever learned, never
    // forgotten, so a dial that started after it was learned selected a
    // group whenever it was given one.
    fn dialled(
        nntp: Arc<weaver_nntp::NntpClient>,
        lane: weaver_nntp::blocking::BlockingBodyLane,
        group_required_at_dial: bool,
        groups: &[String],
    ) -> Self {
        Self {
            nntp,
            lane,
            group_required_at_dial,
            group_selected: group_required_at_dial && !groups.is_empty(),
        }
    }

    // Whether this connection can carry `lease` without redialling.
    //
    // The lease's newsgroups are not a condition: a socket opened for one
    // job's groups serves the next job's after [`Self::adopt_groups`], which
    // on most servers sends nothing at all. Dropping the connection at every
    // job boundary instead is what turned a provider's momentary refusal of
    // new sockets into a stall of every lane.
    fn matches(&self, nntp: &Arc<weaver_nntp::NntpClient>, lease: &DownloadBatchLease) -> bool {
        let server = self.lane.server_id();
        if !Arc::ptr_eq(&self.nntp, nntp) || lease.dial_exclude_servers.contains(server.0) {
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
        let Some(selection) = nntp
            .try_blocking_body_server_selection_with_estimate(lease.dial_exclude_servers, estimate)
        else {
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
        self.lane.adopt_groups(groups)?;
        if !groups.is_empty() {
            self.group_selected |= self.group_required_at_dial;
        }
        Ok(())
    }
}

// Whether server `idx` has proven it refuses message-id fetches without a
// selected group.
fn server_requires_group(nntp: &weaver_nntp::NntpClient, idx: usize) -> bool {
    nntp.pool().server_configs().get(idx).is_some_and(|config| {
        weaver_nntp::server_caps::requires_group_selection(&config.host, config.port)
    })
}

// The servers that have proven they refuse message-id fetches without a
// selected group.
fn servers_requiring_group(nntp: &weaver_nntp::NntpClient) -> weaver_nntp::ServerSet {
    nntp.pool()
        .server_configs()
        .iter()
        .enumerate()
        .filter(|(_, config)| {
            weaver_nntp::server_caps::requires_group_selection(&config.host, config.port)
        })
        .map(|(idx, _)| idx)
        .collect()
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
    let group_required = server_requires_group(&nntp, seat.0);
    match nntp.try_warm_blocking_body_lane(&[], exclude_servers, byte_estimate, seat) {
        Ok(lane) => {
            *cached_lane = Some(CachedOwnedLane::dialled(nntp, lane, group_required, &[]));
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
        let exclude = weaver_nntp::ServerSet::first(probe.nntp.pool().server_count())
            .difference(weaver_nntp::ServerSet::of(probe.seat.0));
        let group_required = server_requires_group(&probe.nntp, probe.seat.0);
        let dialled = acquire_owned_lane_through_contention(
            || true,
            || {
                probe
                    .nntp
                    .try_acquire_blocking_body_lane_with_estimate(&[], exclude, 0)
            },
        );
        match dialled {
            Ok(lane) => {
                *cached_lane = Some(CachedOwnedLane::dialled(
                    Arc::clone(&probe.nntp),
                    lane,
                    group_required,
                    &[],
                ));
            }
            // A busy health state or a seat not free yet is the wait a lease
            // the scheduler takes back makes, not an answer from the server.
            // A full route is not: nothing holds the server off until the
            // route frees, so putting the batch back would seat and refuse
            // it again at once. It leaves that server inconclusive instead.
            Err(error) if error.should_requeue_owned_work() && !error.is_route_capacity() => {
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

// The cached connection is about to rest: an unanswered recovery probe never
// does (see `BlockingBodyLane::may_rest_idle`), so it is parked here, which
// gives the probe back. Asked wherever the worker's connection goes idle:
// after every piece of work, after a warm, and on a run's way out before it
// tells the pipeline it is done, so the probe is back before the work moves.
fn give_back_unanswered_probe(cached_lane: &mut Option<CachedOwnedLane>) {
    if cached_lane
        .as_ref()
        .is_some_and(|cached| !cached.lane.may_rest_idle())
    {
        park_cached_lane(cached_lane);
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
    weaver_server_base::runtime::perf_probe::record(
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
    retention_excludes: weaver_nntp::ServerSet,
    pressure_clear: bool,
    mode: DownloadLaneMode,
    checkpoint_plan: weaver_yenc::CheckpointPlan,
    // The job's repeated-article cache, when its files share articles.
    repeated: Option<Arc<super::repeated::RepeatedArticles>>,
}

impl LaneLeaseContext {
    // Takes the lease's exclusions and plan rather than copying them: once
    // its context is cut, a lease is only the works it hands the lane.
    fn from_lease(
        lease: &mut DownloadBatchLease,
        server_idx: usize,
        supports_pipelining: bool,
    ) -> Self {
        Self {
            lane_id: lease.lane_id,
            body_activity: lease.body_activity.clone(),
            job_id: lease.job_id,
            runtime_generation: lease.runtime_generation,
            completion_critical: lease.completion_critical,
            retention_excludes: std::mem::take(&mut lease.effective_exclude_servers),
            pressure_clear: lease.pressure_clear,
            mode: Pipeline::actual_download_lane_mode(
                lease.lane_mode,
                &lease.server_modes,
                server_idx,
                supports_pipelining,
            ),
            checkpoint_plan: std::mem::take(&mut lease.checkpoint_plan),
            repeated: lease.repeated.take(),
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
pub(crate) fn acquire_owned_lane_through_contention(
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
        !globals.holds_downloads()
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
// `connected` is the connection the lane already holds, if it holds one.
fn lease_is_current(
    state: &crate::jobs::handle::SharedPipelineState,
    nntp: &weaver_nntp::NntpClient,
    lease: &DownloadBatchLease,
    connected: Option<HeldConnection>,
) -> bool {
    if !job_work_is_current(state, lease.job_id, lease.runtime_generation) {
        return false;
    }
    let policy = match connected {
        Some(connection) => connection.policy(nntp),
        None => nntp.pool().provider_policy(),
    };
    (0..nntp.pool().server_count()).any(|server| {
        !lease.dial_exclude_servers.contains(server)
            && lease.works.iter().all(|work| {
                matches!(
                    policy.eligibility(
                        server,
                        work.exclude_servers,
                        lease.effective_exclude_servers,
                        Pipeline::bandwidth_reservation_estimate(work.byte_estimate),
                    ),
                    weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
                )
            })
    })
}

// The one server a lease may dial, when it may dial only one: a lease cut
// by dispatch is pinned so.
fn sole_dial_server(
    nntp: &weaver_nntp::NntpClient,
    lease: &DownloadBatchLease,
) -> Option<weaver_nntp::ServerId> {
    let mut servers = (0..nntp.pool().server_count())
        .filter(|server| !lease.dial_exclude_servers.contains(*server));
    let server = servers.next()?;
    servers
        .next()
        .is_none()
        .then_some(weaver_nntp::ServerId(server))
}

enum RouteWait {
    // The line let the lane go: it dials again.
    Go,
    // The line closed: the lane takes its refusal.
    Closed,
    // The lease is no longer current: its work goes back.
    Obsolete,
}

// Wait in a route's line until the lane may dial, the line closes, or the
// lease stops being current. Currency is read again only when the queue or a
// server's recovery changes: nothing is polled, and nothing is dialled.
fn wait_for_route(
    state: &crate::jobs::handle::SharedPipelineState,
    nntp: &weaver_nntp::NntpClient,
    lease: &DownloadBatchLease,
    turn: &weaver_nntp::route_line::RouteTurn,
) -> RouteWait {
    let mut queue = state.subscribe_job_changes();
    let mut health = nntp.pool().server_health_changes();
    loop {
        if !lease_is_current(state, nntp, lease, None) {
            return RouteWait::Obsolete;
        }
        let changed = async {
            tokio::select! {
                changed = queue.changed() => changed.is_ok(),
                () = health.changed() => true,
            }
        };
        match turn.wait_blocking(changed) {
            Ok(true) => return RouteWait::Go,
            Ok(false) => return RouteWait::Closed,
            Err(true) => {}
            // The queue is gone with the pipeline.
            Err(false) => return RouteWait::Obsolete,
        }
    }
}

// The connection a lane holds, as every recheck of work bound to it reads
// the provider policy: see `NntpPool::provider_policy_on_connection`, where
// the server's unanswered recovery probe is past its own server's hold.
#[derive(Clone, Copy, Debug)]
struct HeldConnection {
    server: usize,
    holds_probe: bool,
}

impl HeldConnection {
    fn of(lane: &weaver_nntp::blocking::BlockingBodyLane) -> Self {
        Self {
            server: lane.server_id().0,
            holds_probe: lane.is_recovery_probe(),
        }
    }

    fn policy(
        self,
        nntp: &weaver_nntp::NntpClient,
    ) -> weaver_nntp::provider_policy::ProviderPolicy {
        nntp.pool()
            .provider_policy_on_connection(self.server, self.holds_probe)
    }
}

// Whether `work` may still go out on the connection a lane holds, asked just
// before its BODY is written: the job is still running at the generation
// the work was cut for, and the work is still eligible on that connection's
// server, as the connection reads the policy.
fn issue_is_current(
    state: &crate::jobs::handle::SharedPipelineState,
    nntp: &weaver_nntp::NntpClient,
    connection: HeldConnection,
    work: &DownloadWork,
    context: &LaneLeaseContext,
) -> bool {
    job_work_is_current(state, context.job_id, context.runtime_generation)
        && matches!(
            connection.policy(nntp).eligibility(
                connection.server,
                work.exclude_servers,
                context.retention_excludes,
                Pipeline::bandwidth_reservation_estimate(work.byte_estimate),
            ),
            weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
        )
}

// Run one lease to its park on a worker that holds no press.
#[cfg(test)]
fn run_owned_blocking_download_lane(cached_lane: &mut Option<CachedOwnedLane>, run: OwnedLaneRun) {
    run_owned_lane(cached_lane, &mut None, run);
}

// Run one lease to its park on this worker's connection. A dial it makes
// settles the worker's press: a connection drops it, a refusal replaces it.
#[allow(clippy::too_many_lines)]
fn run_owned_lane(
    cached_lane: &mut Option<CachedOwnedLane>,
    press: &mut Option<RoutePress>,
    run: OwnedLaneRun,
) {
    let fetch_started = Instant::now();
    let OwnedLaneRun {
        nntp,
        state,
        event_tx,
        refill_tx,
        parked_tx,
        initial_lease,
    } = run;
    let mut lease = initial_lease;

    if !lease_is_current(&state, &nntp, &lease, None) {
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
        // The dial picks its own server, so the requirement is read for
        // every server before it.
        let group_required = servers_requiring_group(&nntp);
        // Every full route the dial was refused on: this lane presses on
        // each until its next dial resolves.
        let mut route_demand = Vec::new();
        // A lease pinned to one server whose route is up but full waits in
        // that route's line, pressing, and dials again once it frees.
        let sole_server = sole_dial_server(&nntp, &lease);
        let mut turn: Option<weaver_nntp::route_line::RouteTurn> = None;
        let dialled = loop {
            // A turn from the line is given up once the route hands the dial
            // its socket; one refused again comes back to keep its place.
            let (dialled, kept) = weaver_nntp::route_line::held_until_routed(turn.take(), || {
                acquire_owned_lane_through_contention(
                    || lease_is_current(&state, &nntp, &lease, None),
                    || {
                        nntp.try_acquire_blocking_body_lane_pressing(
                            lease_groups(&lease),
                            lease.dial_exclude_servers,
                            initial_estimate,
                            &mut route_demand,
                        )
                    },
                )
            });
            turn = kept;
            let joined = match (&dialled, sole_server) {
                (
                    Err(weaver_nntp::client::BlockingBodyLaneAcquireError::RouteCapacity(error)),
                    Some(server),
                ) => nntp.pool().join_route_line(server, error, turn.is_some()),
                _ => None,
            };
            let Some(joined) = joined else {
                break dialled;
            };
            // Joined before the turn just given is let go, so a lane let go
            // and refused again keeps the head of the line.
            turn = Some(joined);
            match wait_for_route(&state, &nntp, &lease, turn.as_ref().expect("joined")) {
                RouteWait::Go => {}
                RouteWait::Closed => break dialled,
                RouteWait::Obsolete => {
                    drop(turn);
                    *press = None;
                    let _ = event_tx.blocking_send(OwnedDownloadLaneEvent::LeaseObsolete { lease });
                    weaver_server_base::runtime::perf_probe::record(
                        "download.fetch_body.owned",
                        fetch_started.elapsed(),
                    );
                    return;
                }
            }
        };
        // The dial ended: the line asks the route for the next lane.
        drop(turn);
        match dialled {
            Ok(lane) => {
                *press = None;
                let server = lane.server_id().0;
                let required = group_required.contains(server);
                *cached_lane = Some(CachedOwnedLane::dialled(
                    Arc::clone(&nntp),
                    lane,
                    required,
                    lease_groups(&lease),
                ));
            }
            Err(error) => {
                let report = next_refusal_report(&lease);
                *press = (!route_demand.is_empty()).then(|| RoutePress {
                    state: state.clone(),
                    job_id: lease.job_id,
                    generation: lease.runtime_generation,
                    report,
                    _demand: route_demand,
                });
                let _ =
                    event_tx.blocking_send(OwnedDownloadLaneEvent::AcquireFailed { lease, error });
                weaver_server_base::runtime::perf_probe::record(
                    "download.fetch_body.owned",
                    fetch_started.elapsed(),
                );
                return;
            }
        }
    }

    let connected = cached_lane
        .as_ref()
        .map(|cached| HeldConnection::of(&cached.lane));
    if !lease_is_current(&state, &nntp, &lease, connected) {
        // A probe that leaves without its article is given back, never kept
        // idle: kept, it would hold its server's every other connection with
        // nothing on the wire to answer for.
        give_back_unanswered_probe(cached_lane);
        let _ = event_tx.blocking_send(OwnedDownloadLaneEvent::LeaseObsolete { lease });
        return;
    }

    let cached = cached_lane
        .as_mut()
        .expect("owned lane cache populated before run");
    let group_selected = cached.group_selected;
    let lane = &mut cached.lane;
    let server_idx = lane.server_id().0;
    let supports_pipelining = lane.supports_pipelining();
    let recovery_probe = lane.is_recovery_probe();

    // The mode the scheduler booked this lane's depth gauge under. The initial
    // dispatch booked the requested mode; every granted refill rebooks the
    // actual one. Reporting the mode the lane is *running* instead would
    // decrement a gauge it was never counted in.
    let mut booked_mode = lease.lane_mode;
    let mut context = Arc::new(LaneLeaseContext::from_lease(
        &mut lease,
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
    // The claim on the one fetch of a shared article, while its BODY is the
    // only request on the ring. See `RepeatedArticles::lookup`.
    let mut shared_claim: Option<super::repeated::SharedClaim> = None;
    // Whether the ring is being drained so a shared article can go out
    // alone. Neither that drain nor the lone request is a pipe the lane
    // failed to keep fed, so neither is sampled as fed.
    let mut draining_for_shared;

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
                        Some(mut next_lease) if !next_lease.works.is_empty() => {
                            booked_mode = Pipeline::actual_download_lane_mode(
                                next_lease.lane_mode,
                                &next_lease.server_modes,
                                server_idx,
                                supports_pipelining,
                            );
                            context = Arc::new(LaneLeaseContext::from_lease(
                                &mut next_lease,
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
        // Behind a mid-batch 480 the lane drains its ring and logs in again
        // before anything new goes out.
        draining_for_shared = false;
        while stop.is_none()
            && shared_claim.is_none()
            && lane.ring_outstanding() < context.depth()
            && !lane.holds_new_requests()
        {
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
            if !issue_is_current(&state, &nntp, HeldConnection::of(lane), work, work_context) {
                stop = Some(LaneStop::ReservationObsolete);
                break;
            }
            // An article several files share is answered from the cache when
            // it can be, whatever the ring holds: a replay puts nothing on the
            // wire. Its one fetch goes out alone, on an empty ring: the claim
            // is then never held while this lane waits on another's, and its
            // response is the next one read. With requests in flight the lane
            // neither waits on another lane's fetch nor claims one; it drains
            // first.
            let shared = work_context
                .repeated
                .as_ref()
                .filter(|cache| cache.shares(&work.message_id))
                .cloned();
            let mut claim = None;
            if let Some(cache) = shared {
                let lookup = cache.lookup(
                    work,
                    server_idx,
                    result_exclude_servers(work, work_context.retention_excludes),
                    work_context.runtime_generation,
                    lane.ring_outstanding() == 0,
                    // A lookup may wait out another lane's fetch.
                    || {
                        issue_is_current(
                            &state,
                            &nntp,
                            HeldConnection::of(lane),
                            work,
                            work_context,
                        )
                    },
                );
                match lookup {
                    Some(super::repeated::SharedLookup::Replay(reply)) => {
                        let (work, work_context) =
                            pending.pop_front().expect("pending front was inspected");
                        let result = replayed_result(work, &work_context, reply);
                        // Nothing went on the wire for it, so it carries none
                        // of the lane's transfer counts.
                        if send_owned_batch(
                            &event_tx,
                            result.lane_id,
                            vec![result],
                            Vec::new(),
                            weaver_nntp::blocking::BlockingLaneStats::default(),
                            false,
                        )
                        .is_err()
                        {
                            stop = Some(LaneStop::Error);
                        }
                        continue;
                    }
                    Some(super::repeated::SharedLookup::Fetch(fetch)) => claim = Some(fetch),
                    Some(super::repeated::SharedLookup::Obsolete) => {
                        stop = Some(LaneStop::ReservationObsolete);
                        break;
                    }
                    Some(super::repeated::SharedLookup::NotReady) => {
                        draining_for_shared = true;
                        break;
                    }
                    None => {}
                }
            }
            let (work, work_context) = pending.pop_front().expect("pending front was inspected");
            shared_claim = claim;
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
            let mut result = result_from_trace(
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
                    fed: false,
                    // Nothing was measured, so this must not close a depth
                    // trial window.
                    batch_complete: discarded,
                    batch_clean: !discarded,
                    unresolved_count: 0,
                    connection_discarded: discarded,
                    // The requests still on the ring report the fault as
                    // they are read, with the ring's own verdict.
                    answered_pipelined: false,
                },
                work_context.retention_excludes,
                group_selected,
            );
            settle_shared_claim(&mut shared_claim, &mut result);
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
                Some(mut next_lease) if !next_lease.works.is_empty() => {
                    booked_mode = Pipeline::actual_download_lane_mode(
                        next_lease.lane_mode,
                        &next_lease.server_modes,
                        server_idx,
                        supports_pipelining,
                    );
                    context = Arc::new(LaneLeaseContext::from_lease(
                        &mut next_lease,
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
        // What the lane holds as this response is read, the response's own
        // request among it: a full rung means the pipe never waited on the
        // lane's next lease.
        let held = inflight.len() + pending.len();
        let starved_for_shared = draining_for_shared || shared_claim.is_some();
        // The head response is decoded as it is read: a panic decoding it
        // is its job's.
        let read = {
            let _job = inflight.front().map(|(_, head)| {
                weaver_server_base::runtime::panic_trap::JobScope::enter(head.job_id)
            });
            lane.ring_read_next()
        };
        let Some((trace, meta)) = read else {
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
        let mut result = result_from_trace(
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
                fed: !starved_for_shared && held >= usize::from(work_context.mode.depth()),
                batch_complete: meta.batch_complete,
                batch_clean: meta.batch_clean,
                unresolved_count: meta.unresolved_count,
                connection_discarded: meta.connection_discarded,
                answered_pipelined: meta.answered_pipelined,
            },
            work_context.retention_excludes,
            group_selected,
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
        // Settled after the connection's verdict is read: a body the cache
        // cannot keep fails locally, which says nothing of the socket.
        settle_shared_claim(&mut shared_claim, &mut result);
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
                    meta.answered_pipelined,
                    work_context.retention_excludes,
                    "lane pipeline faulted before this article's response",
                );
                if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
                    stop = Some(LaneStop::Error);
                    break;
                }
            }
        }
    };
    // A claim whose response never came is given up before anything below
    // waits: the article's next reference fetches it.
    drop(shared_claim);

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
    give_back_unanswered_probe(cached_lane);
    if frees_read_buffers_after_park(park_reason)
        && let Some(cached) = cached_lane.as_mut()
    {
        cached.lane.release_idle_buffers();
    }
    // The article buffers this thread allocated are freed by the decode and
    // writer threads, so under a per-thread-heap allocator their pages sit on
    // this lane's heap until it allocates again. A parked lane never does, so
    // the release has to happen here, on the lane thread, once per park —
    // never per article or per refill, where it would cost throughput.
    weaver_server_base::runtime::thread_release::release_idle_thread_memory();
    let _ = parked_tx.blocking_send(DownloadLaneParked {
        lane_id: park_context.lane_id,
        job_id: park_context.job_id,
        mode: booked_mode,
        completion_critical: park_context.completion_critical,
        reason: park_reason,
        release_connection_slot: true,
    });
    weaver_server_base::runtime::perf_probe::record(
        "download.fetch_body.owned",
        fetch_started.elapsed(),
    );
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
        && let Some(mut lease) = response.lease
        && !lease.works.is_empty()
    {
        let context = LaneLeaseContext::from_lease(&mut lease, server_idx, supports_pipelining);
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

// Whether a kept socket also gives up its read buffers until its next lease.
//
// Only when the lane has nothing to wait for but new work: no article left
// for this server, or its quota spent. A pressure park or a probe yield
// resumes within the same download, where freeing and reallocating the
// buffers would be paid on the download path.
fn frees_read_buffers_after_park(reason: LaneParkReason) -> bool {
    matches!(reason, LaneParkReason::NoWork | LaneParkReason::ServerQuota)
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
fn result_exclude_servers(
    work: &DownloadWork,
    retention_excludes: weaver_nntp::ServerSet,
) -> weaver_nntp::ServerSet {
    work.exclude_servers.union(retention_excludes)
}

// Credit one fetched article's wire bytes to its job and, beside it, to the
// transfer total the download gauge integrates. Every path that fetches an
// article over the wire calls this, so the jobs' counters sum to the total.
pub(in crate::pipeline) fn credit_transfer(
    counters: &crate::pipeline::queue::JobCounters,
    data: &Result<DownloadPayload, DownloadError>,
) {
    let (wire, decoded) = match data {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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

#[allow(clippy::too_many_arguments)]
fn result_from_trace(
    work: DownloadWork,
    runtime_generation: u64,
    lane_id: u64,
    job_id: JobId,
    trace: weaver_nntp::client::DecodedBodyTrace,
    mut observation: DownloadLaneObservation,
    retention_excludes: weaver_nntp::ServerSet,
    group_selected: bool,
) -> DownloadResult {
    let segment_id = work.segment_id;
    let retry_count = work.retry_count;
    let exclude_servers = result_exclude_servers(&work, retention_excludes);
    // The work's own flags, never the lease's: a lane may be refilled with the
    // other class's work, and the origin books the article that was actually
    // fetched.
    let is_recovery = work.is_recovery;
    let completion_critical = work.completion_critical;
    let (mut data, attempts, source_server_idx) =
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
        // A 412 dirties the batch too, so it is settled here.
        if let Err(DownloadError::Fetch(failure)) = &mut data {
            failure.settle_group_refusal(work.groups.is_empty(), group_selected);
        }
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

// Keep a shared article's fetched answer for its other references.
fn settle_shared_claim(
    claim: &mut Option<super::repeated::SharedClaim>,
    result: &mut DownloadResult,
) {
    if let Some(claim) = claim.take() {
        debug_assert_eq!(claim.segment_id(), result.segment_id);
        claim.settle(
            &mut result.data,
            result.source_server_idx,
            result.attempts.len(),
        );
    }
}

// A reference answered from the repeated-article cache. Nothing went on the
// wire for it, so it carries no attempts, no lane observation and no
// transfer credit.
fn replayed_result(
    work: DownloadWork,
    context: &LaneLeaseContext,
    reply: super::repeated::Reply,
) -> DownloadResult {
    DownloadResult {
        lane_id: context.lane_id,
        job_id: context.job_id,
        segment_id: work.segment_id,
        runtime_generation: context.runtime_generation,
        exclude_servers: result_exclude_servers(&work, context.retention_excludes),
        data: reply.data,
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: reply.source,
        origin: DownloadResultOrigin::from_work(work.is_recovery, work.completion_critical),
        retry_count: work.retry_count,
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
    answered_pipelined: bool,
    retention_excludes: weaver_nntp::ServerSet,
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
            DownloadFailureKind::Unanswered,
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
            fed: false,
            batch_complete: true,
            batch_clean: false,
            unresolved_count,
            connection_discarded: true,
            answered_pipelined,
        }),
        source_server_idx: None,
        origin: DownloadResultOrigin::from_work(is_recovery, completion_critical),
        retry_count: work.retry_count,
        exclude_servers,
        release_connection_slot: false,
    }
}

#[cfg(test)]
use weaver_server_base::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

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
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        avoid_server: None,
    }
}

#[cfg(test)]
fn test_admitted_state(job_id: JobId) -> crate::jobs::handle::SharedPipelineState {
    let state = crate::jobs::handle::SharedPipelineState::new(
        weaver_server_base::operations::metrics::PipelineMetrics::new(),
        vec![crate::jobs::handle::JobInfo {
            protocol: crate::jobs::Protocol::Nzb,
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
            file_parked_bytes: Vec::new(),
            remaining_bytes: 0,
            wanted_bytes: 0,
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
    exclude_servers: weaver_nntp::ServerSet,
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
        effective_exclude_servers: exclude_servers,
        dial_exclude_servers: exclude_servers,
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        repeated: None,
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
            &mut test_lease(
                JobId(7),
                3,
                weaver_nntp::ServerSet::from([1]),
                vec![tail_work(1, 0)],
            ),
            0,
            true,
        ));
        let new = Arc::new(LaneLeaseContext::from_lease(
            &mut test_lease(
                JobId(7),
                4,
                weaver_nntp::ServerSet::from([2, 5]),
                vec![tail_work(2, 0)],
            ),
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
            fed: false,
            batch_complete: false,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
            answered_pipelined: false,
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
            old.retention_excludes,
            false,
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
            &mut test_lease(
                JobId(7),
                3,
                weaver_nntp::ServerSet::EMPTY,
                vec![tail_work(1, 0)],
            ),
            1,
            true,
        ));
        let mut work = tail_work(1, 1);
        work.exclude_servers = weaver_nntp::ServerSet::from([0]);

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
                fed: false,
                batch_complete: false,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
                answered_pipelined: false,
            },
            context.retention_excludes,
            false,
        );

        assert_eq!(
            result.exclude_servers,
            vec![0],
            "the server this article already failed on must survive the trip \
             back, or the completion path cannot tell that both servers are out"
        );
    }

    // The result carries the union of the article's ledger and the lease's
    // retention exclusions.
    #[test]
    fn a_result_carries_the_ledger_and_the_retention_exclusions() {
        let unresolved = |work: DownloadWork, retention: weaver_nntp::ServerSet| {
            unresolved_result(
                work,
                3,
                0,
                JobId(7),
                1,
                DownloadLaneMode::Sequential,
                false,
                None,
                true,
                1,
                false,
                retention,
                "test",
            )
        };

        let mut work = tail_work(1, 1);
        work.exclude_servers = weaver_nntp::ServerSet::from([3, 0]);
        let result = unresolved(work, weaver_nntp::ServerSet::from([0, 2, 3, 5]));
        assert_eq!(result.exclude_servers, [0, 2, 3, 5]);

        let mut work = tail_work(2, 1);
        work.exclude_servers = weaver_nntp::ServerSet::from([4]);
        let result = unresolved(work, weaver_nntp::ServerSet::EMPTY);
        assert_eq!(result.exclude_servers, [4]);

        let result = unresolved(tail_work(3, 0), weaver_nntp::ServerSet::from([1]));
        assert_eq!(result.exclude_servers, [1]);
    }

    // The context takes the lease's retention exclusions and checkpoint plan.
    #[test]
    fn a_lease_context_takes_the_lease_s_exclusions() {
        let mut lease = test_lease(
            JobId(7),
            3,
            weaver_nntp::ServerSet::from([2, 5]),
            vec![tail_work(1, 0)],
        );
        lease.checkpoint_plan =
            weaver_yenc::CheckpointPlan::Single(std::num::NonZeroU64::new(4096).unwrap());
        let context = LaneLeaseContext::from_lease(&mut lease, 0, true);
        assert_eq!(context.retention_excludes, [2, 5]);
        assert_eq!(
            context.checkpoint_plan,
            weaver_yenc::CheckpointPlan::Single(std::num::NonZeroU64::new(4096).unwrap())
        );
        assert_eq!(lease.dial_exclude_servers, [2, 5]);
        assert_eq!(lease.works.len(), 1);
    }

    // Per-article delivery is the point: decode starts on a lease's first
    // article instead of its last. Only the lane's closing event waits for an
    // acknowledgement, because that is what orders the results ahead of the
    // park message on the other channel.
    // The articles abandoned behind a fault were never answered: they keep
    // their retry budget and go back out on another connection.
    #[test]
    fn an_abandoned_article_is_unanswered() {
        let result = unresolved_result(
            tail_work(0, 2),
            1,
            0,
            JobId(7),
            0,
            DownloadLaneMode::Sequential,
            true,
            None,
            true,
            3,
            false,
            weaver_nntp::ServerSet::EMPTY,
            "lane pipeline faulted before this article's response",
        );
        let Err(DownloadError::Fetch(failure)) = &result.data else {
            panic!("an abandoned article is a fetch failure");
        };
        assert_eq!(failure.kind, DownloadFailureKind::Unanswered);
        assert!(failure.kind.preserves_article_retry_budget());
        assert_eq!(result.retry_count, 2);
        // The kind says nothing about the socket; the abandoned result
        // carries the lane's own verdict that the connection was lost.
        let observation = result.lane_observation.unwrap();
        assert!(observation.connection_discarded);
        assert!(!observation.batch_clean);
    }

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
                    false,
                    weaver_nntp::ServerSet::EMPTY,
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
                fed: false,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
                answered_pipelined: false,
            },
            weaver_nntp::ServerSet::EMPTY,
            false,
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
                closed: false,
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
                fed: false,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
                answered_pipelined: false,
            },
            weaver_nntp::ServerSet::EMPTY,
            false,
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

    fn group_refusal_result(work: DownloadWork, group_selected: bool) -> DownloadFailure {
        let result = result_from_trace(
            work,
            0,
            0,
            JobId(42),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::NntpError::NoGroupSelected,
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
                fed: false,
                batch_complete: true,
                batch_clean: true,
                unresolved_count: 0,
                connection_discarded: false,
                answered_pipelined: false,
            },
            weaver_nntp::ServerSet::EMPTY,
            group_selected,
        );
        let observation = result.lane_observation.unwrap();
        assert!(
            observation.connection_discarded,
            "a 412 still drops the socket"
        );
        match result.data {
            Err(DownloadError::Fetch(failure)) => {
                assert_eq!(failure.kind, DownloadFailureKind::ConnectionEstablishment);
                failure
            }
            _ => panic!("a 412 is a fetch failure"),
        }
    }

    // The first 412 a server gives teaches it needs GROUP; the next
    // connection selects one of the work's groups, so it costs the article
    // nothing. A 412 with no group to select, or one given while a group was
    // selected, is answered the same way every time, so it costs a retry.
    #[test]
    fn a_412_is_free_only_while_the_next_connection_can_select_a_group() {
        let learning = group_refusal_result(tail_work(1, 0), false);
        assert_eq!(learning.group_refusal, GroupRefusal::Learning);
        assert!(learning.preserves_article_retry_budget());

        let selected = group_refusal_result(tail_work(1, 0), true);
        assert_eq!(selected.group_refusal, GroupRefusal::Settled);
        assert!(!selected.preserves_article_retry_budget());

        for group_selected in [false, true] {
            let mut work = tail_work(1, 0);
            work.groups = Arc::from(Vec::<String>::new());
            let no_group = group_refusal_result(work, group_selected);
            assert_eq!(no_group.group_refusal, GroupRefusal::Settled);
            assert!(!no_group.preserves_article_retry_budget());
        }
    }

    // Settling looks only at 412s: every other failure keeps its kind's
    // retry-budget verdict whatever the work's groups.
    #[test]
    fn settling_a_group_refusal_leaves_other_failures_alone() {
        for error in [
            weaver_nntp::NntpError::Timeout,
            weaver_nntp::NntpError::ConnectionClosed,
            weaver_nntp::NntpError::ArticleNotFound,
            weaver_nntp::NntpError::AuthenticationFailed,
        ] {
            let mut failure = DownloadFailure::from_nntp(error);
            let before = failure.preserves_article_retry_budget();
            failure.settle_group_refusal(true, true);
            assert_eq!(failure.group_refusal, GroupRefusal::None);
            assert_eq!(failure.preserves_article_retry_budget(), before);
        }
    }

    // The requirement is read per server position, before a dial picks one.
    #[test]
    fn servers_requiring_group_marks_only_the_servers_that_proved_it() {
        let server = |host: &str| weaver_nntp::ServerPoolConfig {
            max_connections: 1,
            ..weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
                host: host.to_string(),
                port: 1,
                tls: false,
                ..Default::default()
            })
        };
        let nntp = weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
            servers: vec![
                server("free-fetch.owned-lane.invalid"),
                server("group-required.owned-lane.invalid"),
            ],
            ..weaver_nntp::client::NntpClientConfig::single(weaver_nntp::ServerConfig::default(), 1)
        });
        assert_eq!(
            servers_requiring_group(&nntp),
            weaver_nntp::ServerSet::EMPTY
        );
        assert!(!server_requires_group(&nntp, 1));

        weaver_nntp::server_caps::note_group_required("group-required.owned-lane.invalid", 1);

        assert_eq!(servers_requiring_group(&nntp), [1]);
        assert!(!server_requires_group(&nntp, 0));
        assert!(server_requires_group(&nntp, 1));
        assert!(
            !server_requires_group(&nntp, 2),
            "past the pool reads as not required"
        );
    }

    #[test]
    fn owned_quota_cache_survives_wait_and_reset_but_reselects_another_fill() {
        let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
        let control = transfers.configure(
            weaver_nntp::transfer::StableServerId(78),
            weaver_nntp::transfer::ServerTransferConfig {
                closed: false,
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

    use weaver_server_base::jobs::ids::JobId;

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
        let mut lease = test_lease(
            JobId(42),
            0,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        );
        assert!(lease_is_current(&state, &nntp, &lease, None));
        state.queue.write(|inner| {
            inner.jobs[0].report.nzb_admitted = false;
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease, None));
        state.queue.write(|inner| {
            inner.jobs[0].report.nzb_admitted = true;
            inner.jobs[0].intents.paused = Some(true);
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease, None));
        state.queue.write(|inner| {
            inner.jobs[0].intents.paused = Some(false);
            inner.globals.nntp_generation = 1;
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease, None));
        lease.runtime_generation = 1;
        assert!(lease_is_current(&state, &nntp, &lease, None));
        state.queue.write(|inner| {
            inner
                .globals
                .holds
                .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
            crate::pipeline::queue::Change::changed(())
        });
        assert!(!lease_is_current(&state, &nntp, &lease, None));
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
        not_found_on_zero.exclude_servers = weaver_nntp::ServerSet::from([0]);
        let lease = test_lease(
            JobId(42),
            0,
            weaver_nntp::ServerSet::from([0]),
            vec![not_found_on_zero, tail_work(2, 0)],
        );
        assert!(lease_is_current(&state, &nntp, &lease, None));
    }

    // A lease cut for a server whose quarantine has lapsed is carried by the
    // dial that becomes the probe. While that probe is out the server takes
    // no new connection, so a lane with no connection finds the lease
    // obsolete; the probe's own connection does not, or it would never issue
    // the article it was admitted for, the probe would never answer, and the
    // server would never come back.
    #[test]
    fn the_probe_connection_still_carries_its_lease_while_the_probe_is_out() {
        let nntp = test_client();
        let state = test_admitted_state(JobId(42));
        let lease = test_lease(
            JobId(42),
            0,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        );
        nntp.pool().quarantine_for_transport_for_test(0);
        nntp.pool().expire_quarantine_for_test(0);
        assert!(
            lease_is_current(&state, &nntp, &lease, None),
            "the probe is due, so the lease has a server to dial"
        );
        let probe = nntp.pool().take_recovery_probe_for_test(0);
        assert!(!lease_is_current(&state, &nntp, &lease, None));
        let on = |holds_probe| {
            Some(HeldConnection {
                server: 0,
                holds_probe,
            })
        };
        assert!(lease_is_current(&state, &nntp, &lease, on(true)));
        assert!(
            !lease_is_current(&state, &nntp, &lease, on(false)),
            "a connection that is not the probe is held with the server"
        );
        drop(probe);
        assert!(lease_is_current(&state, &nntp, &lease, None));
    }

    fn test_run(
        nntp: &Arc<weaver_nntp::NntpClient>,
        exclude_servers: weaver_nntp::ServerSet,
    ) -> Box<OwnedLaneRun> {
        let (event_tx, _event_rx) = mpsc::channel(4);
        let (refill_tx, _refill_rx) = mpsc::channel(4);
        let (parked_tx, _parked_rx) = mpsc::channel(4);
        // The receivers are dropped with the run; routing never sends on them.
        let mut lease = test_lease(
            JobId(7),
            1,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        );
        lease.dial_exclude_servers = exclude_servers;
        Box::new(OwnedLaneRun {
            nntp: Arc::clone(nntp),
            state: crate::jobs::handle::SharedPipelineState::new(
                weaver_server_base::operations::metrics::PipelineMetrics::new(),
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
                    pressing: false,
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::from([3]));
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
        let mut shared = shared_with(vec![idle_with(Some(idle_lane(&nntp, 0)))]);

        assert_eq!(shared.claim_worker_for(&run), Some(0));
        assert_eq!(shared.claim_worker_for(&run), None);
    }

    // An excluded server is not a connection this lease may use, however warm
    // it is.
    #[test]
    fn an_excluded_server_is_not_a_match() {
        let nntp = test_client();
        let run = test_run(&nntp, weaver_nntp::ServerSet::from([1]));
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

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
        let lease = test_lease(
            JobId(7),
            1,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        );
        assert_eq!(
            lease_groups(&lease),
            ["alt.binaries.test".to_string()],
            "the lane is opened for the lease's groups"
        );

        let run = test_run(&test_client(), weaver_nntp::ServerSet::EMPTY);
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

        assert!(!idle_lane(&other, 0).serves(&run));
        assert!(!test_idle_lane(0).serves(&run));
    }

    // With every worker mid-lease the run waits for the pool, not for one
    // worker's private channel: whichever finishes first takes it, and it
    // does so before publishing itself idle so the run cannot be missed.
    #[test]
    fn a_run_with_no_idle_worker_waits_for_whichever_finishes_first() {
        let nntp = test_client();
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
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
        assert!(
            shared
                .take_queued_run_or_publish_idle(seat(0), None)
                .is_none()
        );
        assert!(slot(&shared, 0).idle.is_some());
    }

    #[test]
    fn reset_returns_queued_leases_before_a_worker_can_take_old_work() {
        let nntp = test_client();
        let (sender, commands) = std_mpsc::channel();
        let mut shared = shared_with(vec![None]);
        shared.workers[0].as_mut().unwrap().sender = sender;
        for segment_number in 0..3 {
            let mut run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
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
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
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

    // A route that refuses every dial for room, and the dials it refused.
    struct FullRoute(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait::async_trait]
    impl weaver_tunnel::pipe::Dialer for FullRoute {
        async fn dial(
            &self,
            _: &weaver_tunnel::pipe::Target,
        ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError>
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(weaver_tunnel::pipe::DialError::AtCapacity(
                Default::default(),
            ))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(3600)
        }
        fn describe(&self) -> String {
            "full route fixture".into()
        }
    }

    // A probe batch whose seat sits behind a full route is answered
    // inconclusive on that server, so the health round moves on. Handing it
    // back unasked would seat it on the same server at once and be refused
    // again: nothing holds a server off while its route is full.
    #[test]
    fn a_probe_behind_a_full_route_is_inconclusive_there_and_seated_once() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let dials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dialer = Arc::new(weaver_nntp::route_dialer::RouteDialer {
            inner: Arc::new(FullRoute(Arc::clone(&dials))),
            plan: None,
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
            runtime: runtime.handle().clone(),
            server: 0,
        });
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "nntp-full-route.invalid".to_string(),
                    port: 119,
                    tls: false,
                    dialer,
                    ..Default::default()
                },
                1,
            ),
        ));
        let (mail, _heard) = mpsc::channel(4);
        let batch = crate::pipeline::post_health::ProbeBatch::new(
            JobId(1),
            1,
            vec!["<lantern-0@example.com>".into()],
        );
        let probe = OwnedLaneProbe {
            nntp: Arc::clone(&nntp),
            seat: weaver_nntp::pool::ServerId(0),
            ids: batch.unfound(),
            batch,
            mail,
        };
        let mut cached = None;

        let owed = ask_owned_lane_probe(&mut cached, probe);

        let crate::pipeline::post_health::ProbeMail::Answered(answer) = owed.word else {
            panic!(
                "a full route must not hand the batch back to be seated again: {:?}",
                owed.word
            );
        };
        assert_eq!(answer.server, 0);
        assert!(answer.result.inconclusive, "the server could not be asked");
        assert_eq!(answer.result.exists, vec![false]);
        assert_eq!(
            dials.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the seat is dialled once"
        );
        assert!(cached.is_none());
        let health = nntp.pool().health().blocking_lock();
        assert_eq!(
            health.server(0).failure_count,
            0,
            "a full route is no fault"
        );
    }

    // A route with `room` sockets free, which refuses a dial for room with
    // the slot that rings as one frees, and the dials made on it. A dial it
    // has room for takes the room and fails on the wire.
    struct MeteredRoute {
        room: std::sync::atomic::AtomicUsize,
        freed: Arc<tokio::sync::Notify>,
        dials: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl weaver_tunnel::pipe::Dialer for MeteredRoute {
        async fn dial(
            &self,
            _: &weaver_tunnel::pipe::Target,
        ) -> std::result::Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError>
        {
            use std::sync::atomic::Ordering::SeqCst;
            self.dials.fetch_add(1, SeqCst);
            if self
                .room
                .fetch_update(SeqCst, SeqCst, |room| room.checked_sub(1))
                .is_err()
            {
                return Err(weaver_tunnel::pipe::DialError::AtCapacity(
                    weaver_tunnel::pipe::Capacity {
                        waits: vec![Arc::clone(&self.freed)],
                        demand: Vec::new(),
                    },
                ));
            }
            Err(weaver_tunnel::pipe::DialError::Egress(
                std::io::Error::from(std::io::ErrorKind::ConnectionReset),
            ))
        }
        fn room(
            &self,
            _: &weaver_tunnel::pipe::Purpose,
        ) -> std::result::Result<(), Vec<Arc<tokio::sync::Notify>>> {
            if self.room.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(vec![Arc::clone(&self.freed)])
            } else {
                Ok(())
            }
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(3600)
        }
        fn describe(&self) -> String {
            "metered route fixture".into()
        }
    }

    // Lanes refused by a full route wait in its line and dial nothing until
    // a socket frees; one freed socket lets exactly one of them dial, and the
    // rest wait on. A shutdown sends every one of them back.
    #[test]
    fn lanes_behind_a_full_route_wait_for_a_freed_socket_and_one_dials_per_socket() {
        use std::sync::atomic::Ordering::SeqCst;
        const LANES: usize = 3;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let route = Arc::new(MeteredRoute {
            room: std::sync::atomic::AtomicUsize::new(0),
            freed: Arc::new(tokio::sync::Notify::new()),
            dials: std::sync::atomic::AtomicUsize::new(0),
        });
        let dialer = Arc::new(weaver_nntp::route_dialer::RouteDialer {
            inner: Arc::clone(&route) as Arc<dyn weaver_tunnel::pipe::Dialer>,
            plan: None,
            egress_controls: Arc::new(weaver_nntp::transfer::ServerTransferRegistry::new()),
            runtime: runtime.handle().clone(),
            server: 0,
        });
        let nntp = Arc::new(weaver_nntp::client::NntpClient::new(
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "nntp-metered-route.invalid".to_string(),
                    port: 119,
                    tls: false,
                    dialer,
                    ..Default::default()
                },
                8,
            ),
        ));
        let state = test_admitted_state(JobId(42));
        let (event_tx, mut events) = mpsc::channel(LANES * 2);
        let (refill_tx, _refill_rx) = mpsc::channel(4);
        let (parked_tx, _parked_rx) = mpsc::channel(4);
        let mut waiting = nntp.pool().route_line_waiting(weaver_nntp::ServerId(0));
        let lanes: Vec<_> = (0..LANES)
            .map(|segment| {
                let run = OwnedLaneRun {
                    nntp: Arc::clone(&nntp),
                    state: state.clone(),
                    event_tx: event_tx.clone(),
                    refill_tx: refill_tx.clone(),
                    parked_tx: parked_tx.clone(),
                    initial_lease: test_lease(
                        JobId(42),
                        0,
                        weaver_nntp::ServerSet::EMPTY,
                        vec![tail_work(u32::try_from(segment).unwrap(), 0)],
                    ),
                };
                std::thread::spawn(move || run_owned_blocking_download_lane(&mut None, run))
            })
            .collect();
        drop(event_tx);

        runtime.block_on(async {
            waiting.wait_for(|waiting| *waiting == LANES).await.unwrap();
        });
        assert_eq!(route.dials.load(SeqCst), LANES, "each lane dialled once");

        route.room.store(1, SeqCst);
        route.freed.notify_waiters();
        let first = events.blocking_recv().expect("the lane let go reports");
        assert!(
            matches!(first, OwnedDownloadLaneEvent::AcquireFailed { .. }),
            "the lane let go dialled and met the wire"
        );
        assert_eq!(
            route.dials.load(SeqCst),
            LANES + 1,
            "one freed socket, one dial"
        );
        runtime.block_on(async {
            waiting
                .wait_for(|waiting| *waiting == LANES - 1)
                .await
                .unwrap();
        });
        assert_eq!(
            route.dials.load(SeqCst),
            LANES + 1,
            "the full route is not redialled"
        );

        state.queue.write(|inner| {
            inner.globals.shutting_down = true;
            crate::pipeline::queue::Change::changed(())
        });
        for lane in lanes {
            lane.join().unwrap();
        }
        let mut obsolete = 0;
        while let Some(event) = events.blocking_recv() {
            assert!(
                matches!(event, OwnedDownloadLaneEvent::LeaseObsolete { .. }),
                "a waiting lane gives its lease back"
            );
            obsolete += 1;
        }
        assert_eq!(obsolete, LANES - 1);
        assert_eq!(route.dials.load(SeqCst), LANES + 1);
    }

    // The check just before a BODY is written reads the policy as the lane's
    // connection does: the server's recovery probe still issues the work it
    // was admitted for while its probe is out, and any other connection to
    // the server is held with it.
    #[test]
    fn the_probe_connection_still_issues_its_work_while_the_probe_is_out() {
        let nntp = test_client();
        let state = test_admitted_state(JobId(42));
        let mut lease = test_lease(
            JobId(42),
            0,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        );
        let context = LaneLeaseContext::from_lease(&mut lease, 0, false);
        let work = &lease.works[0];
        let on = |holds_probe| HeldConnection {
            server: 0,
            holds_probe,
        };
        nntp.pool().quarantine_for_transport_for_test(0);
        nntp.pool().expire_quarantine_for_test(0);
        let probe = nntp.pool().take_recovery_probe_for_test(0);
        assert!(issue_is_current(&state, &nntp, on(true), work, &context));
        assert!(!issue_is_current(&state, &nntp, on(false), work, &context));
        drop(probe);
        assert!(issue_is_current(&state, &nntp, on(false), work, &context));
    }

    // A server whose quarantine has lapsed, and the probe batch a worker asks
    // on its seat there: the worker's dial is the server's recovery probe.
    fn probe_due_on_stat_fixture(
        ids: &[&str],
    ) -> (
        Arc<weaver_nntp::NntpClient>,
        OwnedLaneProbe,
        std::thread::JoinHandle<()>,
    ) {
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
        nntp.pool().quarantine_for_transport_for_test(0);
        nntp.pool().expire_quarantine_for_test(0);
        // The answer is handed back unsent, so nothing reads the mail.
        let (mail, _) = mpsc::channel(4);
        let batch = crate::pipeline::post_health::ProbeBatch::new(
            JobId(1),
            1,
            ids.iter().map(|id| (*id).into()).collect(),
        );
        let probe = OwnedLaneProbe {
            nntp: Arc::clone(&nntp),
            seat: weaver_nntp::pool::ServerId(0),
            ids: batch.unfound(),
            batch,
            mail,
        };
        (nntp, probe, server)
    }

    fn server_takes_new_work(nntp: &weaver_nntp::NntpClient) -> bool {
        matches!(
            nntp.pool().provider_policy().eligibility(
                0,
                weaver_nntp::ServerSet::EMPTY,
                weaver_nntp::ServerSet::EMPTY,
                1
            ),
            weaver_nntp::provider_policy::ArticleEligibility::Eligible(_)
        )
    }

    // An existence probe asks no BODY. A STAT the server answers 223 is its
    // answer, so a probe dialled for it recovers the server, and the lane is
    // then kept like any other.
    #[test]
    fn an_existence_probe_answered_found_recovers_its_server() {
        let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-0@example.com>"]);
        let mut cached = None;

        let owed = run_owned_lane_work(
            &mut cached,
            &mut None,
            QueuedOwnedLaneWork::Probe(Box::new(probe)),
        );

        assert!(owed.is_some());
        assert!(
            !nntp.pool().requires_recovery(0),
            "the 223 answered the probe"
        );
        assert!(cached.is_some(), "an answered lane is kept");
        park_cached_lane(&mut cached);
        server.join().unwrap();
    }

    // A probe batch the server answers with nothing found leaves the probe
    // unanswered. The lane is not kept idle holding it: it is parked, the
    // probe is given back, and the server takes new work again.
    #[test]
    fn an_existence_probe_that_finds_nothing_gives_the_probe_back() {
        let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-1@example.com>"]);
        let mut cached = None;

        let owed = run_owned_lane_work(
            &mut cached,
            &mut None,
            QueuedOwnedLaneWork::Probe(Box::new(probe)),
        );

        assert!(owed.is_some());
        assert!(nntp.pool().requires_recovery(0), "a 430 is no answer");
        assert!(cached.is_none(), "the unanswered probe is not kept idle");
        assert!(server_takes_new_work(&nntp), "the probe was given back");
        server.join().unwrap();
    }

    // The one rule for a resting lane: an unanswered probe is parked, which
    // gives the probe back; a lane that is not a probe is kept.
    #[test]
    fn a_resting_lane_gives_back_an_unanswered_probe_and_keeps_anything_else() {
        let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-1@example.com>"]);
        let mut cached = None;
        let owed = ask_owned_lane_probe(&mut cached, probe);
        drop(owed);
        assert!(
            cached.is_some(),
            "the probe's lane came back from its batch"
        );
        assert!(!server_takes_new_work(&nntp), "its probe is out");

        give_back_unanswered_probe(&mut cached);

        assert!(cached.is_none());
        assert!(server_takes_new_work(&nntp));
        server.join().unwrap();

        let (nntp, probe, server) = probe_due_on_stat_fixture(&["<lantern-0@example.com>"]);
        let mut cached = None;
        drop(ask_owned_lane_probe(&mut cached, probe));
        assert!(!nntp.pool().requires_recovery(0));
        give_back_unanswered_probe(&mut cached);
        assert!(cached.is_some(), "a lane that is no probe is kept");
        park_cached_lane(&mut cached);
        server.join().unwrap();
    }

    // A worker that has already been claimed must not be republished as idle
    // by a command it answers on the way to its next run.
    #[test]
    fn a_claimed_worker_is_not_republished_as_idle() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);

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
                weaver_server_base::operations::metrics::PipelineMetrics::new(),
                Vec::new(),
            ),
            events.clone(),
            refill_tx,
            parked_tx,
            test_lease(
                JobId(7),
                1,
                weaver_nntp::ServerSet::EMPTY,
                vec![tail_work(1, 0)],
            ),
        )
        .unwrap_or_else(|_| panic!("a pool with room takes the run"));
    }

    fn obsolete_report() -> OwnedDownloadLaneEvent {
        OwnedDownloadLaneEvent::LeaseObsolete {
            lease: test_lease(
                JobId(9),
                1,
                weaver_nntp::ServerSet::EMPTY,
                vec![tail_work(1, 0)],
            ),
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
        let pool =
            OwnedDownloadLanePool::with_test_intervals(4, Duration::from_millis(1), Duration::ZERO);
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
        let run = test_run(&nntp, weaver_nntp::ServerSet::EMPTY);
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

        assert!(
            shared
                .take_queued_run_or_publish_idle(seat(0), None)
                .is_none()
        );
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
            .push_back(QueuedOwnedLaneWork::Run(test_run(
                &nntp,
                weaver_nntp::ServerSet::EMPTY,
            )));

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

        assert_eq!(
            shared.claim_worker_for(&test_run(&nntp, weaver_nntp::ServerSet::EMPTY)),
            Some(0)
        );
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

        assert!(
            shared
                .take_queued_run_or_publish_idle(seat(0), None)
                .is_none()
        );
        assert!(slot(&shared, 0).idle.is_none(), "the successor stays busy");
        shared.vacate(seat(0));
        assert!(shared.workers[0].is_some(), "nor can it vacate the slot");
    }
}

#[cfg(test)]
mod fault_tests;

#[cfg(test)]
mod route_press_tests;
