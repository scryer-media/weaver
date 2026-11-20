// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use std::collections::VecDeque;
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

pub struct OwnedDownloadLanePool {
    pub shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub reset_calls: AtomicUsize,
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
pub struct OwnedLanePoolShared {
    pub workers: Vec<Option<OwnedLaneWorkerSlot>>,
    // Runs and probes that arrived while every worker was busy. Whichever
    // worker finishes first drains this before it publishes itself idle, so
    // a lease never sits behind one busy worker's private channel.
    pub queued_runs: VecDeque<QueuedOwnedLaneWork>,
    // The most worker threads that may be alive at once: the configured
    // connection count.
    pub cap: usize,
    // Tells a worker apart from an earlier one that sat in the same slot.
    pub next_worker_id: u64,
    pub intervals: OwnedLaneIntervals,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub observer: Option<std_mpsc::Sender<OwnedLaneObservation>>,
}

// How often an idle worker wakes, and how long it stays idle before its
// thread leaves. Fixed in production; tests shorten them.
#[derive(Clone, Copy)]
pub struct OwnedLaneIntervals {
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
pub struct OwnedLaneSeat {
    pub index: usize,
    pub id: u64,
}

// What the pool's workers did, for tests that wait on the pool rather than
// on the clock.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedLaneObservation {
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
pub enum OwnedLaneRelease {
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
pub enum QueuedOwnedLaneWork {
    Run(Box<OwnedLaneRun>),
    Probe(Box<OwnedLaneProbe>),
}

// One health probe batch, seated by the dispatch pass on `seat`.
//
// A probe is answered by a worker between leases, on the connection it
// keeps to `seat` or on one it opens there the way a lease pinned to that
// server would: it never shares a connection with articles in flight. The
// answer goes back through the probes' mail with the batch, whatever it is.
pub struct OwnedLaneProbe {
    pub nntp: Arc<weaver_nntp::NntpClient>,
    pub seat: weaver_nntp::pool::ServerId,
    pub ids: Vec<String>,
    pub batch: crate::pipeline::post_health::ProbeBatch,
    pub mail: mpsc::Sender<crate::pipeline::post_health::ProbeMail>,
}

// What a reset fenced off before any worker could start it: the leases, for
// the scheduler, and the probe batches, to wait for a seat again.
pub struct StoppedOwnedLaneWork {
    pub leases: Vec<DownloadBatchLease>,
    pub(crate) probes: Vec<crate::pipeline::post_health::ProbeBatch>,
}

pub struct OwnedLaneWorkerSlot {
    pub id: u64,
    pub sender: std_mpsc::Sender<OwnedLanePoolCommand>,
    // `Some` while the worker sits between runs and no run has been routed to
    // it yet. Taking it is the claim: two concurrent submits cannot choose
    // the same worker, and a worker that is already spoken for is not offered
    // a second lease.
    pub idle: Option<IdleOwnedLaneWorker>,
    // The worker holds the press of a dial a full route refused. A dial
    // routed to it settles that press, so the next run without a connection
    // to reuse goes here first.
    pub pressing: bool,
}

#[derive(Clone, Default)]
pub struct IdleOwnedLaneWorker {
    // The connection this worker kept across its park, when it kept one.
    pub lane: Option<IdleOwnedLane>,
}

// What an idle worker's cached connection can serve, published so a submit
// can route to it without touching the worker.
#[derive(Clone)]
pub struct IdleOwnedLane {
    // The client the connection belongs to. Weak, so a published marker never
    // keeps a retired client alive: the worker itself holds the strong
    // reference for exactly as long as the connection is cached, and a marker
    // whose client is gone simply matches nothing.
    pub nntp: std::sync::Weak<weaver_nntp::NntpClient>,
    pub server: weaver_nntp::pool::ServerId,
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
    pub fn serves(&self, run: &OwnedLaneRun) -> bool {
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

pub struct OwnedLaneRun {
    pub nntp: Arc<weaver_nntp::NntpClient>,
    pub state: crate::jobs::handle::SharedPipelineState,
    pub event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    pub refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    pub parked_tx: mpsc::Sender<DownloadLaneParked>,
    pub initial_lease: DownloadBatchLease,
}

pub enum OwnedLanePoolCommand {
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

pub struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    pub lane: weaver_nntp::blocking::BlockingBodyLane,
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
pub struct RoutePress {
    pub state: crate::jobs::handle::SharedPipelineState,
    pub job_id: JobId,
    pub generation: u64,
    // The job's counters and this refusal's report among the job's lane
    // refusals (see `JobCounters::drained_through`), when the lease carried
    // the counters.
    pub report: Option<(Arc<crate::pipeline::queue::JobCounters>, u64)>,
    pub _demand: Vec<weaver_tunnel::pipe::Demand>,
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
#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(in crate::pipeline) struct TestRoutePress(RoutePress);

#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
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
pub fn settle_route_press(
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
pub struct OwnedLaneWarm {
    pub nntp: Arc<weaver_nntp::NntpClient>,
    pub exclude_servers: weaver_nntp::ServerSet,
    pub byte_estimate: u64,
    pub seat: weaver_nntp::pool::ServerId,
}

pub fn lock_pool(
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
    pub fn claim_worker_for(&mut self, run: &OwnedLaneRun) -> Option<usize> {
        self.claim_worker_where(|lane| lane.serves(run))
    }

    // Route one probe the same way: to the worker idle on its seat, else one
    // with no connection, else any idle worker, which parks what it holds.
    fn claim_worker_for_probe(&mut self, probe: &OwnedLaneProbe) -> Option<usize> {
        self.claim_worker_where(|lane| lane.seats(probe))
    }

    pub fn claim_worker_where(&mut self, serves: impl Fn(&IdleOwnedLane) -> bool) -> Option<usize> {
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
    pub fn take_queued_run_or_publish_idle(
        &mut self,
        seat: OwnedLaneSeat,
        lane: Option<IdleOwnedLane>,
    ) -> Option<QueuedOwnedLaneWork> {
        if let Some(run) = self.queued_runs.pop_front() {
            return Some(run);
        }
        if let Some(worker) = self.seat_mut(seat) {
            worker.idle = Some(IdleOwnedLaneWorker { lane });
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            self.observe(OwnedLaneObservation::Idle(seat.index));
        }
        None
    }

    // Update what an already-idle worker is holding. A worker that has been
    // claimed since is left alone: its next run is already on the way.
    pub fn note_idle_lane(&mut self, seat: OwnedLaneSeat, lane: Option<IdleOwnedLane>) {
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
    pub fn vacate(&mut self, seat: OwnedLaneSeat) {
        if self.seat(seat).is_none() {
            return;
        }
        self.workers[seat.index] = None;
        self.trim_vacant_tail();
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        self.observe(OwnedLaneObservation::Vacated(seat.index));
    }

    fn trim_vacant_tail(&mut self) {
        while self.workers.last().is_some_and(Option::is_none) {
            self.workers.pop();
        }
    }

    // First step of an idle worker leaving: withdraw it from claims, unless
    // a command was routed to it meanwhile or there is queued work to take.
    pub fn begin_release(&mut self, seat: OwnedLaneSeat) -> OwnedLaneRelease {
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
    pub fn finish_release(&mut self, seat: OwnedLaneSeat) -> OwnedLaneRelease {
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
    pub fn idle_workers_without_a_lane(
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

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
    pub fn new(worker_count: usize) -> Self {
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
                #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                observer: None,
            })),
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            reset_calls: AtomicUsize::new(0),
        };
        pool.resize(worker_count);
        pool
    }

    // A pool whose workers wake and leave on the test's schedule.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn with_test_intervals(
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
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn observe(&self) -> std_mpsc::Receiver<OwnedLaneObservation> {
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
    pub fn worker_count(&self) -> usize {
        lock_pool(&self.shared).cap
    }

    // How many worker threads are alive now.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn live_worker_count(&self) -> usize {
        lock_pool(&self.shared).live_workers()
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::Relaxed)
    }

    // Fence queued runs before workers can take another retired-client lease.
    // The actor returns these unstarted leases without charging article retries.
    pub fn reset(&self) -> StoppedOwnedLaneWork {
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
    pub fn submit(
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
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
pub fn run_owned_lane_work(
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
pub struct OwedProbeAnswer {
    mail: mpsc::Sender<crate::pipeline::post_health::ProbeMail>,
    pub word: crate::pipeline::post_health::ProbeMail,
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
pub fn answer_idle_command(
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
pub fn server_requires_group(nntp: &weaver_nntp::NntpClient, idx: usize) -> bool {
    nntp.pool().server_configs().get(idx).is_some_and(|config| {
        weaver_nntp::server_caps::requires_group_selection(&config.host, config.port)
    })
}

// The servers that have proven they refuse message-id fetches without a
// selected group.
pub fn servers_requiring_group(nntp: &weaver_nntp::NntpClient) -> weaver_nntp::ServerSet {
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

pub fn cached_lane_matches_selection(
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
pub fn warm_cached_lane(cached_lane: &mut Option<CachedOwnedLane>, warm: OwnedLaneWarm) {
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
pub fn ask_owned_lane_probe(
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

pub fn park_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) {
    if let Some(cached) = cached_lane.take() {
        cached.lane.park();
    }
}

// The cached connection is about to rest: an unanswered recovery probe never
// does (see `BlockingBodyLane::may_rest_idle`), so it is parked here, which
// gives the probe back. Asked wherever the worker's connection goes idle:
// after every piece of work, after a warm, and on a run's way out before it
// tells the pipeline it is done, so the probe is back before the work moves.
pub fn give_back_unanswered_probe(cached_lane: &mut Option<CachedOwnedLane>) {
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
pub struct LaneLeaseContext {
    body_activity: Arc<std::sync::atomic::AtomicU64>,
    lane_id: u64,
    job_id: JobId,
    pub runtime_generation: u64,
    completion_critical: bool,
    // The job's retention exclusions, carried from the lease. Job-derived and
    // the same for every article on the lane; each result unions it with the
    // article's own failure ledger.
    pub retention_excludes: weaver_nntp::ServerSet,
    pressure_clear: bool,
    mode: DownloadLaneMode,
    pub checkpoint_plan: weaver_yenc::CheckpointPlan,
    // The job's repeated-article cache, when its files share articles.
    repeated: Option<Arc<super::repeated::RepeatedArticles>>,
}

impl LaneLeaseContext {
    // Takes the lease's exclusions and plan rather than copying them: once
    // its context is cut, a lease is only the works it hands the lane.
    pub fn from_lease(
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

    pub fn depth(&self) -> usize {
        self.mode.max_depth().max(1)
    }
}

// Why a lane stopped issuing.
#[derive(Clone, Copy)]
pub enum LaneStop {
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

pub fn lane_stop_park(stop: Option<LaneStop>) -> Option<(LaneParkReason, bool)> {
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
pub const OWNED_LANE_SELECTION_CONTENTION_RETRIES: usize = 3;

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
pub fn acquire_owned_lane_through_contention(
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
pub fn refill_deadline(depth: usize) -> usize {
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
pub fn lease_groups(lease: &DownloadBatchLease) -> &[String] {
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
pub fn lease_is_current(
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
pub struct HeldConnection {
    pub server: usize,
    pub holds_probe: bool,
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
pub fn issue_is_current(
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

// Run one lease to its park on this worker's connection. A dial it makes
// settles the worker's press: a connection drops it, a refusal replaces it.
#[allow(clippy::too_many_lines)]
pub fn run_owned_lane(
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
pub fn keep_cached_lane_after_park(reason: LaneParkReason) -> bool {
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
#[cfg_attr(
    feature = "pipeline-test-fixtures",
    expect(
        clippy::result_unit_err,
        reason = "exported only for the pipeline_units tests; the unit error is the closed-channel signal"
    )
)]
pub fn send_owned_batch(
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
pub fn result_from_trace(
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
pub fn unresolved_result(
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
