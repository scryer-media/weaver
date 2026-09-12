use super::*;

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

const HOT_SHARE_YIELD_CHECK_ARTICLES: usize = 4;

/// How long a lane-side probe waits for a worker to pick its request up.
///
/// An idle worker picks up immediately. This only bounds the case where the
/// worker took a lease between the idle marker being read and the request
/// arriving: rather than sit behind a whole lease, the probe gives up on that
/// worker and tries the next one, or the async client. It bounds the pickup
/// alone, never the answer: once a lane has taken the batch its STAT and HEAD
/// round trips take as long as the wire takes, and a far provider must not be
/// mistaken for a busy one.
const LANE_PROBE_PICKUP_TIMEOUT: Duration = Duration::from_millis(250);

pub(crate) struct OwnedDownloadLanePool {
    shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    #[cfg(test)]
    reset_calls: AtomicUsize,
}

/// Everything the pool and its workers agree on: who is idle, what connection
/// each idle worker is holding, and the runs no worker was free to take.
///
/// One lock rather than a marker per worker, because the two decisions that
/// matter are joint ones. A submit has to pick a worker *and* mark it taken in
/// the same breath, or two submits choose the same lane; a worker has to look
/// for queued work *and* publish itself idle in the same breath, or a run is
/// pushed into the gap between the two and waits for a wake-up that has
/// already happened. It is taken at lease boundaries only — never per article.
struct OwnedLanePoolShared {
    workers: Vec<OwnedLaneWorkerSlot>,
    /// Runs that arrived while every worker was busy. Whichever worker
    /// finishes first drains this before it publishes itself idle, so a lease
    /// never sits behind one busy worker's private channel.
    queued_runs: VecDeque<Box<OwnedLaneRun>>,
}

struct OwnedLaneWorkerSlot {
    sender: std_mpsc::Sender<OwnedLanePoolCommand>,
    /// `Some` while the worker sits between runs and no run has been routed to
    /// it yet. Taking it is the claim: two concurrent submits cannot choose
    /// the same worker, and a worker that is already spoken for is not offered
    /// a second lease.
    idle: Option<IdleOwnedLaneWorker>,
}

#[derive(Clone, Default)]
struct IdleOwnedLaneWorker {
    /// The connection this worker kept across its park, when it kept one.
    lane: Option<IdleOwnedLane>,
}

/// What an idle worker's cached connection can serve, published so a submit
/// can route to it without touching the worker.
#[derive(Clone)]
struct IdleOwnedLane {
    /// The client the connection belongs to. Weak, so a published marker never
    /// keeps a retired client alive: the worker itself holds the strong
    /// reference for exactly as long as the connection is cached, and a marker
    /// whose client is gone simply matches nothing.
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

    /// Whether this connection could take `run` without redialling.
    ///
    /// Deliberately cheap and deliberately advisory: it asks only what can be
    /// answered from the published marker — same client and a server the
    /// lease does not exclude. The newsgroups the connection was opened for do
    /// not matter: the worker re-points its socket at the lease's groups when
    /// they differ. Quota and health are the worker's own
    /// `CachedOwnedLane::matches` check, which runs against live state a
    /// moment later; guessing wrong here costs a park and a dial on a worker
    /// that had nothing better to do, never a wrong fetch.
    fn serves(&self, run: &OwnedLaneRun) -> bool {
        self.nntp
            .upgrade()
            .is_some_and(|client| Arc::ptr_eq(&client, &run.nntp))
            && !run
                .initial_lease
                .effective_exclude_servers
                .contains(&self.server.0)
    }
}

/// Cloneable view of the owned-lane pool that the health probe uses to ask an
/// idle lane a STAT batch on the connection it is already holding.
///
/// Owned lanes hold their server's connection permits for as long as they are
/// cached, which is the point: a lane that keeps its socket starts its next
/// lease with no dial at all. The cost used to fall on the probe, which had to
/// prise a permit loose and open its own connection — four and a half round
/// trips of TCP, TLS, greeting and authentication before its first STAT. Now
/// it borrows the lane's connection for the length of one batch instead.
#[derive(Clone)]
pub(crate) struct OwnedLaneProbeHandle {
    shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
}

impl OwnedLaneProbeHandle {
    /// Existence for a batch of message-ids, answered on idle owned lanes.
    ///
    /// Returns `None` when no owned lane could answer at all, which is the
    /// caller's signal to fall back to the async client. Otherwise the result
    /// has the same shape as the client's own probe, and `servers_settled`
    /// names the servers whose lanes answered conclusively, so the caller can
    /// tell which servers a miss has already been put to.
    ///
    /// # Every lane at once
    ///
    /// The servers are asked **concurrently**, and the reason is what this
    /// probe is usually waiting for. A missing article is only missing once
    /// every configured server has said so, and each answer is one pipelined
    /// STAT batch — one round trip — plus, for a lane that took a lease
    /// between its idle marker being read and the request arriving, up to
    /// [`LANE_PROBE_PICKUP_TIMEOUT`] of waiting for a pickup that never comes.
    /// Asked one after another those add up: the verdict costs the *sum* over
    /// servers where the wire only requires the *maximum*, and the recovery
    /// that verdict releases waits out the difference.
    ///
    /// The cost of asking at once is that each server is asked about the whole
    /// batch rather than only what the servers before it could not find. That
    /// is bytes in a single write, not round trips, and it buys back an answer
    /// that no longer scales with the number of providers configured.
    async fn probe(&self, message_ids: &[String]) -> Option<LaneProbeOutcome> {
        if message_ids.is_empty() {
            return Some(LaneProbeOutcome {
                result: weaver_nntp::client::ProbeBatchResult {
                    exists: Vec::new(),
                    inconclusive: false,
                },
                servers_settled: Vec::new(),
            });
        }

        // One lane per server: a second lane on a server already being asked
        // can say nothing the first will not.
        let mut candidates: Vec<(usize, std_mpsc::Sender<OwnedLanePoolCommand>)> = Vec::new();
        for (server_idx, sender) in self.idle_workers() {
            let Some(server_idx) = server_idx else {
                continue;
            };
            if candidates.iter().any(|(server, _)| *server == server_idx) {
                continue;
            }
            candidates.push((server_idx, sender));
        }
        if candidates.is_empty() {
            return None;
        }

        let request: Arc<[String]> = Arc::from(message_ids.to_vec());
        // A `JoinSet` rather than detached tasks: dropping it aborts whatever
        // is still outstanding, which is what tells a worker that has not
        // picked the request up to leave it alone — the same contract the
        // dropped pickup receiver carries.
        let mut asking = tokio::task::JoinSet::new();
        for (server, sender) in candidates {
            let batch = Arc::clone(&request);
            asking.spawn(async move { (server, Self::ask_lane(sender, batch).await) });
        }

        let mut exists = vec![false; message_ids.len()];
        let mut servers_settled: Vec<usize> = Vec::new();
        let mut answered = false;
        let mut inconclusive = false;
        while let Some(joined) = asking.join_next().await {
            let Ok((server, Some(answer))) = joined else {
                continue;
            };
            answered = true;
            if answer.inconclusive {
                inconclusive = true;
                continue;
            }
            servers_settled.push(server);
            for (slot, found) in answer.exists.iter().enumerate() {
                if *found && let Some(known) = exists.get_mut(slot) {
                    *known = true;
                }
            }
        }

        answered.then_some(LaneProbeOutcome {
            result: weaver_nntp::client::ProbeBatchResult {
                exists,
                inconclusive,
            },
            servers_settled,
        })
    }

    /// Puts one batch to one lane and waits for its verdict.
    ///
    /// `None` covers every way a lane can decline to answer — a worker that
    /// has gone away, one that did not pick the request up inside
    /// [`LANE_PROBE_PICKUP_TIMEOUT`] because it took a lease, and one that
    /// dropped the request — none of which is a verdict about the articles.
    /// The timeout bounds the pickup alone: once a lane has taken the batch,
    /// its STAT and HEAD round trips take as long as the wire takes.
    async fn ask_lane(
        sender: std_mpsc::Sender<OwnedLanePoolCommand>,
        message_ids: Arc<[String]>,
    ) -> Option<weaver_nntp::client::ProbeBatchResult> {
        let (picked_up_tx, picked_up_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(OwnedLanePoolCommand::Probe {
                message_ids,
                picked_up: picked_up_tx,
                reply: reply_tx,
            })
            .ok()?;
        if !matches!(
            tokio::time::timeout(LANE_PROBE_PICKUP_TIMEOUT, picked_up_rx).await,
            Ok(Ok(()))
        ) {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    /// Every worker that is currently idle, with the server index of the
    /// connection it kept — `None` when it sits idle without one.
    fn idle_workers(&self) -> Vec<(Option<usize>, std_mpsc::Sender<OwnedLanePoolCommand>)> {
        let shared = lock_pool(&self.shared);
        shared
            .workers
            .iter()
            .filter_map(|worker| {
                let idle = worker.idle.as_ref()?;
                Some((
                    idle.lane.as_ref().map(|lane| lane.server.0),
                    worker.sender.clone(),
                ))
            })
            .collect()
    }

    /// Existence for a batch, preferring idle owned lanes and falling back to
    /// the async client for whatever they could not settle.
    ///
    /// The fallback is deliberately narrow: it runs only when no lane answered
    /// at all, when a lane could not settle its batch, or when some usable
    /// server has no idle lane and could still hold an article the settled
    /// servers do not — and then it asks only those servers. The ones whose
    /// lanes have answered are left out, because their connection permits are
    /// exactly what those idle lanes are holding: queueing behind them would
    /// wait out the client's whole acquire deadline for a verdict already in
    /// hand. A miss that every usable server has been asked about is final,
    /// and costs no connection.
    pub(crate) async fn confirm_exists_for_probe(
        &self,
        nntp: &weaver_nntp::NntpClient,
        message_ids: &[String],
    ) -> weaver_nntp::client::ProbeBatchResult {
        let Some(outcome) = self.probe(message_ids).await else {
            let borrowed: Vec<&str> = message_ids.iter().map(String::as_str).collect();
            return nntp.confirm_exists_for_probe(&borrowed).await;
        };

        let LaneProbeOutcome {
            mut result,
            servers_settled,
        } = outcome;
        let unresolved: Vec<usize> = result
            .exists
            .iter()
            .enumerate()
            .filter_map(|(idx, found)| (!found).then_some(idx))
            .collect();
        if unresolved.is_empty() {
            // Everything was found somewhere; a lane that faulted along the
            // way has nothing left to be inconclusive about.
            result.inconclusive = false;
            return result;
        }

        let retry: Vec<&str> = unresolved
            .iter()
            .map(|idx| message_ids[*idx].as_str())
            .collect();
        let Some(fallback) = nntp
            .confirm_exists_for_probe_excluding(&retry, &servers_settled)
            .await
        else {
            // No usable server is left outside the ones that answered, so
            // the lanes' verdict is the whole answer — unless one of them
            // could not settle its part, which leaves the miss unproven.
            return result;
        };
        if fallback.inconclusive {
            result.inconclusive = true;
            return result;
        }
        result.inconclusive = false;
        for (slot, found) in unresolved.iter().zip(fallback.exists) {
            result.exists[*slot] = found;
        }
        result
    }
}

/// What the idle owned lanes could say about one probe batch.
struct LaneProbeOutcome {
    result: weaver_nntp::client::ProbeBatchResult,
    /// Server indexes whose lanes answered conclusively, so the caller knows
    /// which providers a miss has actually been put to and need not be asked
    /// again.
    servers_settled: Vec<usize>,
}

struct OwnedLaneRun {
    nntp: Arc<weaver_nntp::NntpClient>,
    event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    parked_tx: mpsc::Sender<DownloadLaneParked>,
    hot_share_yield_signal: Arc<HotShareYieldSignal>,
    initial_lease: DownloadBatchLease,
}

enum OwnedLanePoolCommand {
    Run(Box<OwnedLaneRun>),
    Reset,
    /// Open a connection now, before there is work for it.
    ///
    /// A job whose first wave is held to a barrier — the PAR2 index bootstrap
    /// is the standing case — leases one batch and leaves every other worker
    /// with nothing to dial for. The handshakes are then paid one after the
    /// other as the barrier lifts, and the first payload BODY of each lane
    /// waits out a TCP, TLS, greeting and authentication exchange that had
    /// nothing to wait for. Warming turns those into a connection the lane is
    /// already holding when its first lease arrives.
    ///
    /// A warm lane is an idle lane: it is not counted as an active download
    /// connection anywhere, and if no lease ever comes it parks with the rest.
    Warm(Box<OwnedLaneWarm>),
    /// Answer an existence probe on this worker's cached connection. The reply
    /// is `None` when the worker has no cached lane to answer with.
    Probe {
        message_ids: Arc<[String]>,
        /// Signalled the moment the worker takes the request up. A caller
        /// that has stopped listening by then has moved on, and the request
        /// is dropped rather than answered into the void.
        picked_up: oneshot::Sender<()>,
        reply: oneshot::Sender<Option<weaver_nntp::client::ProbeBatchResult>>,
    },
}

struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    lane: weaver_nntp::blocking::BlockingBodyLane,
}

/// What a warm dial needs: the connection a lease for this job would have
/// asked for.
pub(crate) struct OwnedLaneWarm {
    nntp: Arc<weaver_nntp::NntpClient>,
    groups: Arc<[String]>,
    exclude_servers: Arc<[usize]>,
    byte_estimate: u64,
}

fn lock_pool(
    shared: &std::sync::Mutex<OwnedLanePoolShared>,
) -> std::sync::MutexGuard<'_, OwnedLanePoolShared> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl OwnedLanePoolShared {
    /// Route one run to the worker that can start it soonest, and claim that
    /// worker so a concurrent submit cannot pick it too.
    ///
    /// The order is the cost of starting, not fairness. A worker whose cached
    /// connection already serves this lease starts on the first round trip; a
    /// worker with no connection pays one dial; a worker holding a connection
    /// this lease cannot use pays a park and then that dial. Rotating instead
    /// of routing is what made a promoted recovery lease land on an arbitrary
    /// worker while the connection it wanted sat idle on another — and, since
    /// those idle connections hold every server permit, made the dial it fell
    /// back to fail for capacity.
    fn claim_worker_for(&mut self, run: &OwnedLaneRun) -> Option<usize> {
        let matching = self.workers.iter().position(|worker| {
            worker
                .idle
                .as_ref()
                .and_then(|idle| idle.lane.as_ref())
                .is_some_and(|lane| lane.serves(run))
        });
        let index = matching
            .or_else(|| {
                self.workers
                    .iter()
                    .position(|worker| worker.idle.as_ref().is_some_and(|idle| idle.lane.is_none()))
            })
            .or_else(|| self.workers.iter().position(|worker| worker.idle.is_some()))?;
        self.workers[index].idle = None;
        Some(index)
    }

    /// Take a run this worker should start now, or publish it idle.
    ///
    /// The two are one decision: a worker that looked for queued work, found
    /// none and then published itself idle would leave a window in which a
    /// submit sees no idle worker, queues its run, and waits for a worker that
    /// is already asleep.
    fn take_queued_run_or_publish_idle(
        &mut self,
        index: usize,
        lane: Option<IdleOwnedLane>,
    ) -> Option<Box<OwnedLaneRun>> {
        if let Some(run) = self.queued_runs.pop_front() {
            return Some(run);
        }
        if let Some(worker) = self.workers.get_mut(index) {
            worker.idle = Some(IdleOwnedLaneWorker { lane });
        }
        None
    }

    /// Update what an already-idle worker is holding. A worker that has been
    /// claimed since is left alone: its next run is already on the way.
    fn note_idle_lane(&mut self, index: usize, lane: Option<IdleOwnedLane>) {
        if let Some(worker) = self.workers.get_mut(index)
            && let Some(idle) = worker.idle.as_mut()
        {
            idle.lane = lane;
        }
    }

    fn mark_busy(&mut self, index: usize) {
        if let Some(worker) = self.workers.get_mut(index) {
            worker.idle = None;
        }
    }

    /// Idle workers with no connection, in pool order — the ones a warm can
    /// give a head start without disturbing anything.
    fn idle_workers_without_a_lane(
        &self,
        limit: usize,
    ) -> Vec<std_mpsc::Sender<OwnedLanePoolCommand>> {
        self.workers
            .iter()
            .filter(|worker| worker.idle.as_ref().is_some_and(|idle| idle.lane.is_none()))
            .take(limit)
            .map(|worker| worker.sender.clone())
            .collect()
    }
}

impl OwnedDownloadLanePool {
    pub(crate) fn new(worker_count: usize) -> Self {
        let mut pool = Self {
            shared: Arc::new(std::sync::Mutex::new(OwnedLanePoolShared {
                workers: Vec::new(),
                queued_runs: VecDeque::new(),
            })),
            #[cfg(test)]
            reset_calls: AtomicUsize::new(0),
        };
        pool.resize(worker_count);
        pool
    }

    pub(crate) fn resize(&mut self, worker_count: usize) {
        let worker_count = worker_count.max(1);
        let mut shared = lock_pool(&self.shared);
        while shared.workers.len() < worker_count {
            let index = shared.workers.len();
            let sender = spawn_owned_lane_worker(index, Arc::clone(&self.shared));
            shared
                .workers
                .push(OwnedLaneWorkerSlot { sender, idle: None });
        }
        shared.workers.truncate(worker_count);
    }

    pub(crate) fn probe_handle(&self) -> OwnedLaneProbeHandle {
        OwnedLaneProbeHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        lock_pool(&self.shared).workers.len()
    }

    #[cfg(test)]
    pub(crate) fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::Relaxed)
    }

    /// Fence queued runs before workers can take another retired-client lease.
    /// The actor returns these unstarted leases without charging article retries.
    pub(crate) fn reset(&self) -> Vec<DownloadBatchLease> {
        #[cfg(test)]
        self.reset_calls.fetch_add(1, Ordering::Relaxed);
        let mut shared = lock_pool(&self.shared);
        for worker in &shared.workers {
            let _ = worker.sender.send(OwnedLanePoolCommand::Reset);
        }
        shared
            .queued_runs
            .drain(..)
            .map(|run| run.initial_lease)
            .collect()
    }

    // The Err variant hands the lease back to the caller for the async
    // fallback path; it only occurs when the pool is stopped, so the size
    // of the returned lease is irrelevant.
    #[allow(clippy::result_large_err)]
    pub(crate) fn submit(
        &self,
        nntp: Arc<weaver_nntp::NntpClient>,
        event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
        refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
        parked_tx: mpsc::Sender<DownloadLaneParked>,
        hot_share_yield_signal: Arc<HotShareYieldSignal>,
        initial_lease: DownloadBatchLease,
    ) -> Result<(), DownloadBatchLease> {
        let run = Box::new(OwnedLaneRun {
            nntp,
            event_tx,
            refill_tx,
            parked_tx,
            hot_share_yield_signal,
            initial_lease,
        });
        let mut shared = lock_pool(&self.shared);
        if shared.workers.is_empty() {
            return Err(run.initial_lease);
        }
        let Some(index) = shared.claim_worker_for(&run) else {
            // Every worker is mid-lease. Queue for whichever finishes first
            // rather than picking one: a run pushed into a busy worker's
            // private channel waits out that worker's whole lease even while
            // another goes idle beside it.
            shared.queued_runs.push_back(run);
            return Ok(());
        };
        shared.workers[index]
            .sender
            .send(OwnedLanePoolCommand::Run(run))
            .map_err(|error| match error.0 {
                OwnedLanePoolCommand::Run(run) => run.initial_lease,
                OwnedLanePoolCommand::Reset
                | OwnedLanePoolCommand::Warm(_)
                | OwnedLanePoolCommand::Probe { .. } => {
                    unreachable!("only a Run command is sent from submit")
                }
            })
    }

    /// Ask up to `limit` connectionless idle workers to dial now.
    ///
    /// Returns how many were asked. Each dials on its own worker thread, so
    /// the handshakes overlap each other and nothing already leased waits on
    /// them; a worker whose dial is refused simply stays without a connection.
    pub(crate) fn warm(
        &self,
        nntp: &Arc<weaver_nntp::NntpClient>,
        groups: Arc<[String]>,
        exclude_servers: Arc<[usize]>,
        byte_estimate: u64,
        limit: usize,
    ) -> usize {
        if limit == 0 {
            return 0;
        }
        let senders = lock_pool(&self.shared).idle_workers_without_a_lane(limit);
        let mut asked = 0;
        for sender in senders {
            let warm = Box::new(OwnedLaneWarm {
                nntp: Arc::clone(nntp),
                groups: Arc::clone(&groups),
                exclude_servers: Arc::clone(&exclude_servers),
                byte_estimate,
            });
            if sender.send(OwnedLanePoolCommand::Warm(warm)).is_ok() {
                asked += 1;
            }
        }
        asked
    }
}

fn spawn_owned_lane_worker(
    index: usize,
    shared: Arc<std::sync::Mutex<OwnedLanePoolShared>>,
) -> std_mpsc::Sender<OwnedLanePoolCommand> {
    let (tx, rx) = std_mpsc::channel();
    std::thread::Builder::new()
        .name(format!("weaver-nntp-lane-{index}"))
        .spawn(move || {
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();
            run_owned_lane_worker(index, rx, &shared);
        })
        .expect("failed to spawn owned blocking NNTP lane");
    tx
}

fn run_owned_lane_worker(
    index: usize,
    rx: std_mpsc::Receiver<OwnedLanePoolCommand>,
    shared: &std::sync::Mutex<OwnedLanePoolShared>,
) {
    let mut cached_lane: Option<CachedOwnedLane> = None;
    'work: loop {
        // Drain the pool's queue before going to sleep, and publish this
        // worker as idle only when there is nothing left to take.
        let queued = lock_pool(shared).take_queued_run_or_publish_idle(
            index,
            cached_lane.as_ref().map(IdleOwnedLane::from_cached),
        );
        if let Some(run) = queued {
            run_owned_blocking_download_lane(&mut cached_lane, *run);
            continue;
        }

        // Idle from here until a run is routed to this worker. The other
        // commands are answered in place and leave the worker idle, so the
        // idle marker is only re-published after a run.
        loop {
            let command = match rx.recv_timeout(CACHED_LANE_LIVENESS_INTERVAL) {
                Ok(command) => command,
                Err(std_mpsc::RecvTimeoutError::Timeout) => {
                    // A server that times out an idle connection leaves the
                    // socket half-closed on this side, and the worker would
                    // keep the pool permit for it until a lease was routed
                    // here. Give the permit back as soon as that is noticed.
                    if discard_closed_cached_lane(&mut cached_lane) {
                        lock_pool(shared).note_idle_lane(index, None);
                    }
                    continue;
                }
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break 'work,
            };
            match command {
                // The submit that sent this already claimed the worker, so
                // there is no marker to clear.
                OwnedLanePoolCommand::Run(run) => {
                    run_owned_blocking_download_lane(&mut cached_lane, *run);
                    continue 'work;
                }
                OwnedLanePoolCommand::Reset => {
                    lock_pool(shared).note_idle_lane(index, None);
                    park_cached_lane(&mut cached_lane);
                }
                OwnedLanePoolCommand::Warm(warm) => {
                    warm_cached_lane(&mut cached_lane, *warm);
                    lock_pool(shared).note_idle_lane(
                        index,
                        cached_lane.as_ref().map(IdleOwnedLane::from_cached),
                    );
                }
                OwnedLanePoolCommand::Probe {
                    message_ids,
                    picked_up,
                    reply,
                } => {
                    if picked_up.send(()).is_err() {
                        // The caller gave up waiting for pickup while this
                        // worker was on a lease; nobody wants the answer now.
                        continue;
                    }
                    // The lane keeps its cached connection and its idle marker
                    // across a probe: a borrow of the socket, not a lease.
                    let answer = cached_lane
                        .as_mut()
                        .map(|cached: &mut CachedOwnedLane| cached.lane.probe_exists(&message_ids));
                    // A probe that faulted the connection must not leave a
                    // dead lane cached for the next lease to inherit.
                    if cached_lane
                        .as_ref()
                        .is_some_and(|cached| !cached.lane.is_healthy())
                    {
                        lock_pool(shared).note_idle_lane(index, None);
                        park_cached_lane(&mut cached_lane);
                    }
                    let _ = reply.send(answer);
                }
            }
        }
    }
    lock_pool(shared).mark_busy(index);
    park_cached_lane(&mut cached_lane);
}

/// Open the connection a lease for this job would have asked for, so the lease
/// itself does not have to.
fn warm_cached_lane(cached_lane: &mut Option<CachedOwnedLane>, warm: OwnedLaneWarm) {
    if cached_lane.is_some() {
        return;
    }
    let OwnedLaneWarm {
        nntp,
        groups,
        exclude_servers,
        byte_estimate,
    } = warm;
    match nntp.try_acquire_blocking_body_lane_with_estimate(
        &groups,
        &exclude_servers,
        byte_estimate,
    ) {
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

impl CachedOwnedLane {
    /// Whether this connection can carry `lease` without redialling.
    ///
    /// The lease's newsgroups are not a condition: a socket opened for one
    /// job's groups serves the next job's after [`Self::adopt_groups`], which
    /// on most servers sends nothing at all. Dropping the connection at every
    /// job boundary instead is what turned a provider's momentary refusal of
    /// new sockets into a stall of every lane.
    fn matches(&self, nntp: &Arc<weaver_nntp::NntpClient>, lease: &DownloadBatchLease) -> bool {
        let server = self.lane.server_id();
        if !Arc::ptr_eq(&self.nntp, nntp) || lease.effective_exclude_servers.contains(&server.0) {
            return false;
        }

        let estimate = Pipeline::bandwidth_reservation_estimate(
            lease
                .works
                .first()
                .expect("owned download lease must contain work")
                .byte_estimate,
        );
        let cached_rejection = nntp.server_quota_rejection(server, estimate);
        if cached_rejection.is_none() {
            return true;
        }
        // Contention on the shared health state is not a verdict about this
        // connection. Ranking the servers again is only a second opinion on a
        // quota question; when it cannot be taken, the established connection
        // stands rather than being parked and redialled for nothing.
        let Some(selection) = nntp.try_blocking_body_server_selection_with_estimate(
            &lease.effective_exclude_servers,
            estimate,
        ) else {
            return true;
        };
        cached_lane_matches_selection(server, true, &selection)
    }

    /// Re-point the cached socket at `groups`. The lane itself knows which
    /// group the socket has selected, so this is asked for every lease: on
    /// most servers it is one capability lookup, and on one that requires a
    /// selected group a socket that already sits on a candidate sends
    /// nothing. `Err` means the socket could not be re-pointed and is no
    /// longer worth keeping.
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

fn park_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) {
    if let Some(cached) = cached_lane.take() {
        cached.lane.park();
    }
}

/// How often an idle worker checks that the server still holds its cached
/// connection open.
const CACHED_LANE_LIVENESS_INTERVAL: Duration = Duration::from_secs(15);

/// Drop a cached connection the server has already closed. There is nothing
/// to QUIT; what matters is that its permit goes back to the pool now rather
/// than when the next lease finds the socket dead.
fn discard_closed_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) -> bool {
    if !cached_lane
        .as_ref()
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

/// Everything a downloaded article needs from the lease its work came from.
///
/// A lease boundary is no longer a pipeline boundary: the first BODY of the
/// next lease goes on the wire while the tail of the current one is still
/// being read, so two leases are in flight at once and a response cannot ask
/// "what is the current lease" when it lands. The context rides with the work
/// instead.
struct LaneLeaseContext {
    job_id: JobId,
    runtime_generation: u64,
    spillover_loan_kind: Option<SpilloverLoanKind>,
    completion_critical: bool,
    exclude_servers: Vec<usize>,
    pressure_clear: bool,
    mode: DownloadLaneMode,
    checkpoint_plan: weaver_yenc::CheckpointPlan,
    compatibility: DownloadBatchCompatibility,
}

impl LaneLeaseContext {
    fn from_lease(
        lease: &DownloadBatchLease,
        server_idx: usize,
        supports_pipelining: bool,
    ) -> Self {
        Self {
            job_id: lease.job_id,
            runtime_generation: lease.runtime_generation,
            spillover_loan_kind: lease.spillover_loan_kind,
            completion_critical: lease.compatibility.completion_critical,
            exclude_servers: lease.compatibility.exclude_servers.clone(),
            pressure_clear: lease.pressure_clear,
            mode: Pipeline::actual_download_lane_mode(
                lease.lane_mode,
                &lease.server_modes,
                server_idx,
                supports_pipelining,
            ),
            checkpoint_plan: lease.checkpoint_plan.clone(),
            compatibility: lease.compatibility.clone(),
        }
    }

    fn depth(&self) -> usize {
        self.mode.max_depth().max(1)
    }
}

/// Why a lane stopped issuing.
#[derive(Clone, Copy)]
enum LaneStop {
    /// The transport is unusable. Everything still on the ring is unanswerable
    /// and the connection is discarded.
    ConnectionLost,
    /// A quota or unrequested outcome. The socket is exactly where the next
    /// BODY would expect it, so the ring still drains, but nothing more may be
    /// issued on it.
    PolicyBlocked,
    /// The hot job asked for this connection back.
    HotShareYield,
    /// The result or refill channel is gone; the orchestrator is shutting down.
    Error,
}

fn lane_stop_park(stop: Option<LaneStop>) -> Option<(LaneParkReason, bool)> {
    Some(match stop? {
        LaneStop::ConnectionLost | LaneStop::Error => (LaneParkReason::Error, false),
        LaneStop::PolicyBlocked => (
            LaneParkReason::ServerQuota,
            keep_cached_lane_after_park(LaneParkReason::ServerQuota),
        ),
        LaneStop::HotShareYield => (
            LaneParkReason::HotShareYield,
            keep_cached_lane_after_park(LaneParkReason::HotShareYield),
        ),
    })
}

/// How many times a worker re-asks for a connection when the shared server
/// health state was busy. Each attempt already spins on the lock inside the
/// client; this only covers the case where the whole spin lost.
const OWNED_LANE_SELECTION_CONTENTION_RETRIES: usize = 3;

/// Acquire a connection, treating "the health state was busy" as "ask again"
/// rather than as an answer.
///
/// The distinction is the whole point of the contended variant: a collapsed
/// selection reads as "no server can serve this" and pushes the lease off the
/// owned lanes onto a path that then queues behind the very permits those
/// lanes are holding.
fn acquire_owned_lane_through_contention(
    nntp: &weaver_nntp::NntpClient,
    groups: &[String],
    exclude_servers: &[usize],
    byte_estimate: u64,
) -> std::result::Result<
    weaver_nntp::blocking::BlockingBodyLane,
    weaver_nntp::client::BlockingBodyLaneAcquireError,
> {
    let mut attempt = 0;
    loop {
        match nntp.try_acquire_blocking_body_lane_with_estimate(
            groups,
            exclude_servers,
            byte_estimate,
        ) {
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

/// How little unfinished work a lane may still hold before its next lease has
/// to be in hand.
///
/// A refill costs at least one orchestrator turn to answer, and a lane at
/// depth `d` retires about `d` articles per round trip. One pipe of runway
/// pays for the answer and a second pays for the ask, plus the article being
/// read: 3 articles at depth 1, 5 at depth 2, 9 at depth 4. An ordinary lease
/// is exactly one pipe deep, so it trips this the moment it is adopted and the
/// ask goes out while the lease's own first article is still on the wire —
/// which is the zero-gap handoff sequential mode needs. A runway-sized hot
/// lease trips it two round trips before it runs dry.
fn refill_deadline(depth: usize) -> usize {
    2 * depth.max(1) + 1
}

/// Hand one finished article to the orchestrator on its own, without waiting
/// for an acknowledgement.
///
/// Delivery per article rather than per lease is what lets decode start on the
/// first article of a lease instead of its last, and what makes a direct-store
/// volume settle when its final article lands. The bounded event channel is
/// the backpressure; the acknowledgement is kept for the lane's final event,
/// where it orders the results ahead of the park message on the other channel.
fn stream_owned_result(
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
    lane: &weaver_nntp::blocking::BlockingBodyLane,
    stats_mark: &mut weaver_nntp::blocking::BlockingLaneStats,
    result: DownloadResult,
) -> Result<(), ()> {
    let now = lane.stats();
    let stats = stats_delta(now, *stats_mark);
    *stats_mark = now;
    send_owned_batch(event_tx, vec![result], Vec::new(), stats, false)
}

#[allow(clippy::too_many_lines)]
fn run_owned_blocking_download_lane(cached_lane: &mut Option<CachedOwnedLane>, run: OwnedLaneRun) {
    let fetch_started = Instant::now();
    let OwnedLaneRun {
        nntp,
        event_tx,
        refill_tx,
        parked_tx,
        hot_share_yield_signal,
        initial_lease,
    } = run;
    let lease = initial_lease;

    if cached_lane
        .as_ref()
        .is_some_and(|cached| !cached.matches(&nntp, &lease))
    {
        park_cached_lane(cached_lane);
    }
    // A cached connection the server closed while it sat idle serves nothing:
    // its first BODY would fail only after a read timeout and burn a retry.
    discard_closed_cached_lane(cached_lane);
    // A connection opened for another job's newsgroups is kept and re-pointed
    // rather than redialled; only a socket that cannot be re-pointed is let
    // go, and the dial below then replaces it.
    if let Some(cached) = cached_lane.as_mut()
        && let Err(error) = cached.adopt_groups(&lease.compatibility.groups)
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
        match acquire_owned_lane_through_contention(
            &nntp,
            &lease.compatibility.groups,
            &lease.effective_exclude_servers,
            initial_estimate,
        ) {
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

    let lane = &mut cached_lane
        .as_mut()
        .expect("owned lane cache populated before run")
        .lane;
    let server_idx = lane.server_id().0;
    let supports_pipelining = lane.supports_pipelining();

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
    let mut inflight: VecDeque<(DownloadWork, Arc<LaneLeaseContext>)> = VecDeque::new();
    let mut pending_refill: Option<oneshot::Receiver<DownloadLaneRefillResponse>> = None;
    let mut refill_denied = false;
    let mut stop: Option<LaneStop> = None;
    let mut stats_mark = lane.stats();
    let mut completed_since_yield_check = 0usize;

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
            let Some((work, work_context)) = pending.pop_front() else {
                break;
            };
            let estimate = Pipeline::bandwidth_reservation_estimate(work.byte_estimate);
            let wire_form = work.message_id.wire_form();
            let outcome = lane.ring_issue(&wire_form, estimate, work_context.depth());
            let trace = match outcome {
                weaver_nntp::blocking::RingIssueOutcome::Issued => {
                    inflight.push_back((work, work_context));
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
                *trace,
                DownloadLaneObservation {
                    server_idx: Some(server_idx),
                    mode: work_context.mode,
                    supports_pipelining,
                    latency: lane.latency_ewma(),
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
                &work_context.exclude_servers,
            );
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
                    job_id: context.job_id,
                    runtime_generation: context.runtime_generation,
                    server_idx,
                    remote_ip: lane.remote_ip(),
                    supports_pipelining,
                    current_mode: booked_mode,
                    spillover_loan_kind: context.spillover_loan_kind,
                    compatibility: context.compatibility.clone(),
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
                        job_id: context.job_id,
                        runtime_generation: context.runtime_generation,
                        server_idx,
                        remote_ip: lane.remote_ip(),
                        supports_pipelining,
                        current_mode: booked_mode,
                        spillover_loan_kind: context.spillover_loan_kind,
                        compatibility: context.compatibility.clone(),
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
        let completion_critical = work_context.completion_critical;
        let result = result_from_trace(
            work,
            work_context.runtime_generation,
            trace,
            DownloadLaneObservation {
                server_idx: Some(server_idx),
                mode: work_context.mode,
                supports_pipelining,
                latency: lane.latency_ewma(),
                transfer: lane.transfer_ewma(),
                payload_bytes,
                policy_elapsed,
                pressure_clear: work_context.pressure_clear,
                batch_complete: meta.batch_complete,
                batch_clean: meta.batch_clean,
                unresolved_count: meta.unresolved_count,
                connection_discarded: meta.connection_discarded,
            },
            &work_context.exclude_servers,
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
        if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
            stop = Some(LaneStop::Error);
        }
        if !keeps_connection || meta.connection_discarded || lane.ring_is_closed() {
            stop.get_or_insert(LaneStop::ConnectionLost);
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
                    server_idx,
                    work_context.mode,
                    supports_pipelining,
                    lane.latency_ewma(),
                    lane.transfer_ewma(),
                    work_context.pressure_clear,
                    unresolved_count,
                    &work_context.exclude_servers,
                    "lane pipeline faulted before this article's response",
                );
                if stream_owned_result(&event_tx, lane, &mut stats_mark, result).is_err() {
                    stop = Some(LaneStop::Error);
                    break;
                }
            }
        }

        completed_since_yield_check = completed_since_yield_check.saturating_add(1);
        if completed_since_yield_check >= HOT_SHARE_YIELD_CHECK_ARTICLES {
            completed_since_yield_check = 0;
            // Completion-critical work never yields here — it is what the
            // signal exists to make room for.
            if !completion_critical && hot_share_yield_signal.is_requested() {
                stop.get_or_insert(LaneStop::HotShareYield);
            }
        }
    };

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
    let unrequested_works = pending
        .into_iter()
        .map(|(work, _)| work)
        .collect::<Vec<_>>();
    // The final event is the ordering barrier: its acknowledgement is what
    // guarantees every streamed result reached the orchestrator before the
    // park message arrives on the other channel and releases the connection.
    let stats = stats_delta(lane.stats(), stats_mark);
    let _ = send_owned_batch(&event_tx, Vec::new(), unrequested_works, stats, true);

    if !keep_cached_lane {
        park_cached_lane(cached_lane);
    }
    let _ = parked_tx.blocking_send(DownloadLaneParked {
        job_id: park_context.job_id,
        mode: booked_mode,
        spillover_loan_kind: park_context.spillover_loan_kind,
        completion_critical: park_context.completion_critical,
        reason: park_reason,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });
    crate::runtime::perf_probe::record("download.fetch_body.owned", fetch_started.elapsed());
}

/// Consume a prefetched refill response on a park/error path so its leased
/// works are returned to the queue instead of being dropped. The orchestrator
/// answers every refill request (or drops the sender on shutdown), so this
/// cannot hang.
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
            Vec::new(),
            lease.works,
            weaver_nntp::blocking::BlockingLaneStats::default(),
            true,
        );
        return Some(context);
    }
    None
}

/// Whether a park keeps the socket for the worker's next lease.
///
/// Everything but a transport fault does. A yield is the case worth stating:
/// it hands the *scheduling* of this connection back so completion-critical
/// work can have it, and dropping the socket to do that threw away the one
/// thing the critical lease wanted — an authenticated connection it could
/// issue on immediately. The routing in `claim_worker_for` is what makes the
/// difference visible: the yielded worker is idle holding exactly the
/// connection the next critical lease is looking for.
fn keep_cached_lane_after_park(reason: LaneParkReason) -> bool {
    matches!(
        reason,
        LaneParkReason::NoWork
            | LaneParkReason::Pressure
            | LaneParkReason::ProbeYield
            | LaneParkReason::HotReclaim
            | LaneParkReason::HotShareYield
            | LaneParkReason::SpilloverWithdraw
            | LaneParkReason::SpilloverSpeedHarm
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

/// Hand results and returned works to the orchestrator.
///
/// `acknowledge` turns the send into a rendezvous. A streamed article does not
/// need one — the bounded event channel already paces the lane, and waiting
/// per article would put a thread hop on the hot path — but the lane's last
/// event does: it has to be seen before the park message that follows it on a
/// different channel.
fn send_owned_batch(
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
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

fn result_from_trace(
    work: DownloadWork,
    runtime_generation: u64,
    trace: weaver_nntp::client::DecodedBodyTrace,
    mut observation: DownloadLaneObservation,
    exclude_servers: &[usize],
) -> DownloadResult {
    let segment_id = work.segment_id;
    let retry_count = work.retry_count;
    // The work's own flags, never the lease's: a lane may be refilled with the
    // other class's work, and the origin books the article that was actually
    // fetched.
    let is_recovery = work.is_recovery;
    let completion_critical = work.completion_critical;
    let (data, attempts, source_server_idx) =
        Pipeline::download_data_from_decoded_trace(segment_id, trace);
    // Only outcomes that actually damaged the transport dirty the batch. A
    // 430 and the local quota/unrequested policy outcomes all leave the
    // socket exactly where the next BODY expects it.
    if !download_outcome_keeps_connection(&data) {
        observation.batch_clean = false;
        observation.connection_discarded = true;
    }
    DownloadResult {
        segment_id,
        runtime_generation,
        data,
        attempts,
        lane_observation: Some(observation),
        source_server_idx,
        origin: DownloadResultOrigin::from_work(is_recovery, completion_critical),
        retry_count,
        exclude_servers: exclude_servers.to_vec(),
        release_connection_slot: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn unresolved_result(
    work: DownloadWork,
    runtime_generation: u64,
    server_idx: usize,
    mode: DownloadLaneMode,
    supports_pipelining: bool,
    latency: Option<std::time::Duration>,
    transfer: Option<std::time::Duration>,
    pressure_clear: bool,
    unresolved_count: u64,
    exclude_servers: &[usize],
    message: &'static str,
) -> DownloadResult {
    let is_recovery = work.is_recovery;
    let completion_critical = work.completion_critical;
    DownloadResult {
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
            latency,
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
        exclude_servers: exclude_servers.to_vec(),
        release_connection_slot: false,
    }
}

#[cfg(test)]
use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

#[cfg(test)]
fn tail_work(segment_number: u32, retry_count: u32) -> DownloadWork {
    DownloadWork {
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
fn test_lease(
    job_id: JobId,
    runtime_generation: u64,
    exclude_servers: Vec<usize>,
    works: Vec<DownloadWork>,
) -> DownloadBatchLease {
    DownloadBatchLease {
        job_id,
        runtime_generation,
        lane_mode: DownloadLaneMode::Pipelined { depth: 4 },
        spillover_loan_kind: None,
        server_modes: vec![(0, DownloadLaneMode::Pipelined { depth: 4 })],
        compatibility: DownloadBatchCompatibility {
            priority: 10,
            is_recovery: false,
            completion_critical: false,
            groups: Arc::from(vec!["alt.binaries.test".to_string()]),
            exclude_servers,
            avoid_server: None,
        },
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    }
}

/// An idle marker standing for a cached connection on `server`.
///
/// The client is a dangling `Weak`, which is what an idle marker looks like
/// once its client has been retired: it can serve nothing, so routing skips it
/// while the probe still recognises it as a connection on that server.
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

    /// Every configured server's lane holds the same batch at the same time.
    ///
    /// A missing article is only missing once every server has said so, so a
    /// probe asked one lane at a time costs the sum of the answers where the
    /// wire only requires the longest of them — and the recovery that verdict
    /// releases waits out the difference. Each fake lane here refuses to answer
    /// conclusively until the other is holding the batch too, which only a
    /// probe that asked them together can satisfy.
    #[tokio::test]
    async fn every_idle_lane_holds_the_batch_at_once() {
        const LANES: usize = 2;
        let holding = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        let mut lanes = Vec::new();
        for server in 0..LANES {
            let (sender, receiver) = std_mpsc::channel();
            workers.push(OwnedLaneWorkerSlot {
                sender,
                idle: Some(IdleOwnedLaneWorker {
                    lane: Some(test_idle_lane(server)),
                }),
            });
            let holding = Arc::clone(&holding);
            lanes.push(std::thread::spawn(move || {
                let Ok(OwnedLanePoolCommand::Probe {
                    message_ids,
                    picked_up,
                    reply,
                }) = receiver.recv()
                else {
                    return;
                };
                let _ = picked_up.send(());
                holding.fetch_add(1, Ordering::SeqCst);
                // Bounded rather than a barrier: a probe that asks in
                // sequence must fail this test, not hang it.
                let deadline = Instant::now() + Duration::from_secs(2);
                let together = loop {
                    if holding.load(Ordering::SeqCst) == LANES {
                        break true;
                    }
                    if Instant::now() >= deadline {
                        break false;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                };
                let _ = reply.send(Some(weaver_nntp::client::ProbeBatchResult {
                    exists: vec![false; message_ids.len()],
                    inconclusive: !together,
                }));
            }));
        }

        let probe = OwnedLaneProbeHandle {
            shared: Arc::new(std::sync::Mutex::new(OwnedLanePoolShared {
                workers,
                queued_runs: VecDeque::new(),
            })),
        };
        let outcome = probe
            .probe(&["<withheld@silver.horizon>".to_string()])
            .await
            .expect("both lanes answered");
        for lane in lanes {
            lane.join().expect("the fake lanes finish");
        }

        assert!(
            !outcome.result.inconclusive,
            "each lane must have been holding the batch while the other was: \
             asked in sequence, the first one times out waiting for the second"
        );
        assert_eq!(
            outcome.servers_settled.len(),
            LANES,
            "and both servers' verdicts count towards the answer"
        );
        assert_eq!(outcome.result.exists, vec![false]);
    }

    /// Two leases are on the ring at a lease boundary, so a response has to
    /// carry its own lease's identity, not whichever lease the lane happens to
    /// be filling from when it lands.
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
            latency: None,
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
            weaver_nntp::client::DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                    weaver_nntp::error::NntpError::ArticleNotFound,
                )),
            },
            observation,
            &old.exclude_servers,
        );

        assert_eq!(
            straggler.runtime_generation, 3,
            "the tail of the old lease keeps the old generation while the new \
             lease is already on the wire"
        );
        assert_eq!(straggler.exclude_servers, vec![1]);
        assert_eq!(new.runtime_generation, 4);
        assert_eq!(new.exclude_servers, vec![2, 5]);
        assert_eq!(new.depth(), 4);
    }

    /// Per-article delivery is the point: decode starts on a lease's first
    /// article instead of its last. Only the lane's closing event waits for an
    /// acknowledgement, because that is what orders the results ahead of the
    /// park message on the other channel.
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
                    DownloadLaneMode::Sequential,
                    false,
                    None,
                    None,
                    true,
                    0,
                    &[],
                    "streamed",
                );
                send_owned_batch(&sender_tx, vec![result], Vec::new(), stats, false)?;
            }
            send_owned_batch(&sender_tx, Vec::new(), vec![tail_work(9, 0)], stats, true)
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
    fn owned_hot_lane_yield_keeps_the_socket_and_its_retry_counts() {
        // A yield hands back the *scheduling* of this connection, not the
        // connection. The socket stays cached and the worker sits idle
        // holding it, which is precisely the state the completion-critical
        // lease that asked for the yield wants to be routed to; dropping it
        // made that lease pay a fresh handshake for a connection it had just
        // been given. The works that were never issued go back to the queue
        // untouched, which is what keeps a yield from spending a retry.
        let (reason, keep_cached_lane) =
            lane_stop_park(Some(LaneStop::HotShareYield)).expect("a stop always parks");

        assert_eq!(reason, LaneParkReason::HotShareYield);
        assert!(keep_cached_lane);

        let pending = VecDeque::from([(tail_work(2, 4), ()), (tail_work(3, 7), ())]);
        let returned = pending
            .into_iter()
            .map(|(work, ())| work)
            .collect::<Vec<_>>();
        assert_eq!(returned[0].segment_id.segment_number, 2);
        assert_eq!(returned[0].retry_count, 4);
        assert_eq!(returned[1].segment_id.segment_number, 3);
        assert_eq!(returned[1].retry_count, 7);
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

    /// The refill must be asked for while the ring still has runway.
    ///
    /// An ordinary lease is exactly one pipe deep, so the deadline has to be
    /// wider than the lease itself or the ask would land after the pipe is
    /// already dry — the round-trip gap this path exists to remove.
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

    /// A 430 must not tear down the owned lane.
    ///
    /// The article-not-found answer is a complete, bodyless server response:
    /// the socket is exactly where the next BODY expects it. Marking it dirty
    /// used to QUIT the TLS session, drop the rest of the leased batch as
    /// Unrequested, and block the server's pipelining proof — all because one
    /// article lives on another provider.
    #[test]
    fn owned_article_not_found_keeps_the_lane_and_the_batch_clean() {
        let result = result_from_trace(
            tail_work(9, 0),
            0,
            weaver_nntp::client::DecodedBodyTrace {
                attempts: vec![weaver_nntp::client::FetchAttemptTrace {
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
                latency: None,
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

    /// Transport faults must stay dirty. Only the "fully consumed response"
    /// outcomes are allowed to keep the connection.
    #[test]
    fn owned_transport_and_decode_failures_still_discard_the_connection() {
        for data in [
            Err::<DownloadPayload, _>(DownloadError::Fetch(DownloadFailure::new(
                DownloadFailureKind::EstablishedTransport,
                "connection closed mid-body",
            ))),
            Err(DownloadError::Decode {
                raw_size: 128,
                error: "crc mismatch".to_string(),
                crc_mismatch: true,
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
                latency: None,
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

    fn test_run(
        nntp: &Arc<weaver_nntp::NntpClient>,
        exclude_servers: Vec<usize>,
    ) -> Box<OwnedLaneRun> {
        let (event_tx, _event_rx) = mpsc::channel(4);
        let (refill_tx, _refill_rx) = mpsc::channel(4);
        let (parked_tx, _parked_rx) = mpsc::channel(4);
        // The receivers are dropped with the run; routing never sends on them.
        let mut lease = test_lease(JobId(7), 1, Vec::new(), vec![tail_work(1, 0)]);
        lease.effective_exclude_servers = exclude_servers;
        Box::new(OwnedLaneRun {
            nntp: Arc::clone(nntp),
            event_tx,
            refill_tx,
            parked_tx,
            hot_share_yield_signal: Arc::new(HotShareYieldSignal::default()),
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
        OwnedLanePoolShared {
            workers: idle
                .into_iter()
                .map(|idle| OwnedLaneWorkerSlot {
                    sender: std_mpsc::channel().0,
                    idle,
                })
                .collect(),
            queued_runs: VecDeque::new(),
        }
    }

    fn idle_with(lane: Option<IdleOwnedLane>) -> Option<IdleOwnedLaneWorker> {
        Some(IdleOwnedLaneWorker { lane })
    }

    /// Routing, not rotation.
    ///
    /// After a wave parks, every worker sits idle holding a connection — and
    /// with it every server permit. Handing the next lease to whichever worker
    /// the counter lands on makes a warm connection useless: the chosen worker
    /// parks a connection it cannot use and then tries to dial one the idle
    /// lanes are holding the permits for. The order here is the cost of
    /// starting: no dial, one dial, a park and a dial.
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

    /// A claim is what stops two submits choosing the same idle worker: the
    /// marker is taken at selection time, not when the worker starts running.
    #[test]
    fn claiming_a_worker_takes_it_out_of_the_running() {
        let nntp = test_client();
        let run = test_run(&nntp, Vec::new());
        let mut shared = shared_with(vec![idle_with(Some(idle_lane(&nntp, 0)))]);

        assert_eq!(shared.claim_worker_for(&run), Some(0));
        assert_eq!(shared.claim_worker_for(&run), None);
    }

    /// An excluded server is not a connection this lease may use, however warm
    /// it is.
    #[test]
    fn an_excluded_server_is_not_a_match() {
        let nntp = test_client();
        let run = test_run(&nntp, vec![1]);
        let lane = idle_lane(&nntp, 1);

        assert!(!lane.serves(&run));
        assert!(idle_lane(&nntp, 0).serves(&run));
    }

    /// The newsgroups a connection was opened for are no reason to redial:
    /// the worker re-points the socket at the next lease's groups, so a job
    /// boundary keeps every warm connection in play.
    #[test]
    fn a_connection_opened_for_other_newsgroups_still_serves() {
        let nntp = test_client();
        let mut run = test_run(&nntp, Vec::new());
        run.initial_lease.compatibility.groups = Arc::from(vec!["alt.binaries.other".to_string()]);

        assert!(idle_lane(&nntp, 0).serves(&run));
        let mut shared = shared_with(vec![idle_with(None), idle_with(Some(idle_lane(&nntp, 0)))]);
        assert_eq!(
            shared.claim_worker_for(&run),
            Some(1),
            "the warm connection is preferred over a fresh dial whatever its groups"
        );
    }

    /// A connection belonging to a client that has been retired matches
    /// nothing: the marker is weak precisely so it cannot keep one alive.
    #[test]
    fn a_connection_from_another_client_is_not_a_match() {
        let nntp = test_client();
        let other = test_client();
        let run = test_run(&nntp, Vec::new());

        assert!(!idle_lane(&other, 0).serves(&run));
        assert!(!test_idle_lane(0).serves(&run));
    }

    /// With every worker mid-lease the run waits for the pool, not for one
    /// worker's private channel: whichever finishes first takes it, and it
    /// does so before publishing itself idle so the run cannot be missed.
    #[test]
    fn a_run_with_no_idle_worker_waits_for_whichever_finishes_first() {
        let nntp = test_client();
        let run = test_run(&nntp, Vec::new());
        let mut shared = shared_with(vec![None, None]);

        assert!(shared.claim_worker_for(&run).is_none());
        shared.queued_runs.push_back(run);

        // Worker 1 finishes first and takes the queued run instead of going
        // idle; worker 0 finishes into an empty queue and publishes itself.
        assert!(
            shared
                .take_queued_run_or_publish_idle(1, None)
                .is_some_and(|run| run.initial_lease.job_id == JobId(7))
        );
        assert!(shared.workers[1].idle.is_none(), "worker 1 is running it");
        assert!(shared.take_queued_run_or_publish_idle(0, None).is_none());
        assert!(shared.workers[0].idle.is_some());
    }

    #[test]
    fn reset_returns_queued_leases_before_a_worker_can_take_old_work() {
        let nntp = test_client();
        let (sender, commands) = std_mpsc::channel();
        let mut shared = shared_with(vec![None]);
        shared.workers[0].sender = sender;
        for segment_number in 0..3 {
            let mut run = test_run(&nntp, Vec::new());
            run.initial_lease.works[0].segment_id.segment_number = segment_number;
            shared.queued_runs.push_back(run);
        }
        let pool = OwnedDownloadLanePool {
            shared: Arc::new(std::sync::Mutex::new(shared)),
            reset_calls: AtomicUsize::new(0),
        };

        let returned = pool.reset();

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
                .take_queued_run_or_publish_idle(0, None)
                .is_none(),
            "finishing a lease must reach the pending reset without another old run"
        );
        assert!(matches!(
            commands.try_recv(),
            Ok(OwnedLanePoolCommand::Reset)
        ));
        assert!(pool.reset().is_empty(), "leases are returned only once");
    }

    /// Only workers with nothing to lose are warmed: a worker already holding
    /// a connection has nothing to gain, and a busy one is not there to ask.
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
            "never more than the free connections the job may use"
        );
        assert!(shared.idle_workers_without_a_lane(0).is_empty());
    }

    /// A warm lane is an idle lane: it is published exactly like one a park
    /// kept, so the next lease for that job is routed straight to it.
    #[test]
    fn a_warm_connection_is_published_as_an_idle_lane() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);
        let run = test_run(&nntp, Vec::new());

        // Nothing to route to yet: the worker has no connection, so it is only
        // the fallback choice.
        assert!(
            shared.workers[0]
                .idle
                .as_ref()
                .is_some_and(|idle| idle.lane.is_none())
        );

        shared.note_idle_lane(0, Some(idle_lane(&nntp, 0)));

        assert_eq!(
            shared.claim_worker_for(&run),
            Some(0),
            "the warmed connection is now the one this lease is routed to"
        );
    }

    /// A worker that has already been claimed must not be republished as idle
    /// by a command it answers on the way to its next run.
    #[test]
    fn a_claimed_worker_is_not_republished_as_idle() {
        let nntp = test_client();
        let mut shared = shared_with(vec![idle_with(None)]);
        let run = test_run(&nntp, Vec::new());

        assert_eq!(shared.claim_worker_for(&run), Some(0));
        shared.note_idle_lane(0, Some(idle_lane(&nntp, 0)));

        assert!(
            shared.workers[0].idle.is_none(),
            "its next lease is already on the way"
        );
    }
}

#[cfg(test)]
mod probe_tests {
    use super::*;

    /// Waits for every worker's first idle publish before taking control of
    /// its idle state. A probe reply is a barrier without changing that state.
    async fn quiesce(pool: &OwnedDownloadLanePool) {
        let senders: Vec<_> = lock_pool(&pool.shared)
            .workers
            .iter()
            .map(|worker| worker.sender.clone())
            .collect();
        for sender in senders {
            let (picked_up, picked_up_rx) = oneshot::channel();
            let (reply, reply_rx) = oneshot::channel();
            assert!(
                sender
                    .send(OwnedLanePoolCommand::Probe {
                        message_ids: Arc::from([]),
                        picked_up,
                        reply,
                    })
                    .is_ok()
            );
            tokio::time::timeout(Duration::from_secs(10), async {
                picked_up_rx.await.expect("worker picked up startup probe");
                assert!(
                    reply_rx
                        .await
                        .expect("worker answered startup probe")
                        .is_none()
                );
            })
            .await
            .expect("worker reached its idle command loop");
        }
        let mut shared = lock_pool(&pool.shared);
        for index in 0..shared.workers.len() {
            shared.mark_busy(index);
        }
    }

    fn set_idle(pool: &OwnedDownloadLanePool, index: usize, lane: Option<IdleOwnedLane>) {
        lock_pool(&pool.shared).workers[index].idle = Some(IdleOwnedLaneWorker { lane });
    }

    /// A worker with no cached lane answers the probe with `None` rather than
    /// with a batch of missing verdicts, so the caller knows to ask elsewhere.
    #[tokio::test]
    async fn a_worker_without_a_cached_lane_declines_the_probe() {
        let pool = OwnedDownloadLanePool::new(1);
        let handle = pool.probe_handle();
        quiesce(&pool).await;
        // Idle, but holding nothing: the state a worker is in before its
        // first lease, and after a park that dropped the socket.
        set_idle(&pool, 0, None);

        let outcome = handle.probe(&["<probe@silver.horizon>".to_string()]).await;

        assert!(
            outcome.is_none(),
            "a worker with no connection cannot settle anything"
        );
    }

    /// A worker that is busy on a lease is not idle, so it is never asked and
    /// the probe reports that no lane could answer.
    #[tokio::test]
    async fn a_busy_worker_is_not_asked() {
        let pool = OwnedDownloadLanePool::new(2);
        let handle = pool.probe_handle();
        quiesce(&pool).await;

        assert!(handle.idle_workers().is_empty());
        assert!(
            handle
                .probe(&["<probe@silver.horizon>".to_string()])
                .await
                .is_none()
        );
    }

    /// An empty batch never touches a lane and is trivially settled.
    #[tokio::test]
    async fn an_empty_batch_needs_no_lane() {
        let pool = OwnedDownloadLanePool::new(1);
        let outcome = pool
            .probe_handle()
            .probe(&[])
            .await
            .expect("an empty batch is always answerable");

        assert!(outcome.result.exists.is_empty());
        assert!(!outcome.result.inconclusive);
        assert!(outcome.servers_settled.is_empty());
    }

    #[tokio::test]
    async fn the_probe_handle_tracks_pool_resizes() {
        let mut pool = OwnedDownloadLanePool::new(1);
        let handle = pool.probe_handle();
        pool.resize(4);
        quiesce(&pool).await;
        set_idle(&pool, 3, Some(test_idle_lane(0)));

        assert_eq!(handle.idle_workers().len(), 1);

        pool.resize(2);
        assert_eq!(lock_pool(&handle.shared).workers.len(), 2);
        quiesce(&pool).await;
        assert!(handle.idle_workers().is_empty());
    }
}

#[cfg(test)]
mod fault_tests;
