use super::*;

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

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

/// How long a probe waits for a *busy* lane to take it up.
///
/// A busy worker looks at its commands once per ring response, and at the
/// dry point it sits on the actor's refill answer for up to the idle hold, so
/// the pickup is bounded by one article's transfer plus that hold. The wait
/// pays off: the alternative is a cold dial for a permit the busy lanes are
/// holding, which never completes while they are.
const LANE_PROBE_BUSY_PICKUP_TIMEOUT: Duration = Duration::from_secs(3);

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
    /// The server a running worker's connection belongs to, published once
    /// its lease has a connection and cleared when it is back at the lease
    /// boundary. It is how a probe reaches a server none of the idle lanes
    /// is holding: the worker answers on its own socket once its ring has
    /// drained, which costs the probe one drain and the lease nothing.
    busy_server: Option<usize>,
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
                .dial_exclude_servers
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
        // can say nothing the first will not. Idle lanes answer on the spot,
        // so they are preferred; a server only busy lanes are holding is
        // asked through one of them, which answers once its ring drains.
        let mut candidates: Vec<(usize, std_mpsc::Sender<OwnedLanePoolCommand>, Duration)> =
            Vec::new();
        for (server_idx, sender) in self.idle_workers() {
            let Some(server_idx) = server_idx else {
                continue;
            };
            if candidates
                .iter()
                .any(|(server, _, _)| *server == server_idx)
            {
                continue;
            }
            candidates.push((server_idx, sender, LANE_PROBE_PICKUP_TIMEOUT));
        }
        for (server_idx, sender) in self.busy_workers() {
            if candidates
                .iter()
                .any(|(server, _, _)| *server == server_idx)
            {
                continue;
            }
            candidates.push((server_idx, sender, LANE_PROBE_BUSY_PICKUP_TIMEOUT));
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
        for (server, sender, pickup) in candidates {
            let batch = Arc::clone(&request);
            asking.spawn(async move { (server, Self::ask_lane(sender, batch, pickup).await) });
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
                debug!(
                    server,
                    batch_len = message_ids.len(),
                    "owned lane probe: server left the batch unverified"
                );
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
    /// `pickup` because it took a lease or is still reading a ring, and one
    /// that dropped the request — none of which is a verdict about the
    /// articles. The timeout bounds the pickup alone: once a lane has taken
    /// the batch, its STAT and HEAD round trips take as long as the wire
    /// takes, and a busy lane's drain comes before them.
    async fn ask_lane(
        sender: std_mpsc::Sender<OwnedLanePoolCommand>,
        message_ids: Arc<[String]>,
        pickup: Duration,
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
        if !matches!(tokio::time::timeout(pickup, picked_up_rx).await, Ok(Ok(()))) {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    /// Every worker that is mid-lease on a connection it has published the
    /// server of. One that is still dialling has nothing to answer on yet.
    fn busy_workers(&self) -> Vec<(usize, std_mpsc::Sender<OwnedLanePoolCommand>)> {
        let shared = lock_pool(&self.shared);
        shared
            .workers
            .iter()
            .filter(|worker| worker.idle.is_none())
            .filter_map(|worker| Some((worker.busy_server?, worker.sender.clone())))
            .collect()
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
    initial_lease: DownloadBatchLease,
}

enum OwnedLanePoolCommand {
    Run(Box<OwnedLaneRun>),
    Reset,
    RecallSocket(u64),
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

/// A probe a worker has taken up mid-lease: the ids to look for, and the
/// channel its verdict is owed on once the lane's ring has drained.
type PendingProbe = (
    Arc<[String]>,
    oneshot::Sender<Option<weaver_nntp::client::ProbeBatchResult>>,
);

struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    lane: weaver_nntp::blocking::BlockingBodyLane,
}

/// What a warm dial needs: the connection a lease for this job would have
/// asked for.
pub(crate) struct OwnedLaneWarm {
    nntp: Arc<weaver_nntp::NntpClient>,
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
        if let Some(worker) = self.workers.get_mut(index) {
            // Back at the boundary: whatever connection the last run used is
            // no longer "the one this worker is busy on".
            worker.busy_server = None;
        }
        if let Some(run) = self.queued_runs.pop_front() {
            return Some(run);
        }
        if let Some(worker) = self.workers.get_mut(index) {
            worker.idle = Some(IdleOwnedLaneWorker { lane });
        }
        None
    }

    /// Publish the server a running worker's connection belongs to.
    fn note_busy_server(&mut self, index: usize, server: Option<usize>) {
        if let Some(worker) = self.workers.get_mut(index) {
            worker.busy_server = server;
        }
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
            shared.workers.push(OwnedLaneWorkerSlot {
                sender,
                idle: None,
                busy_server: None,
            });
        }
        shared.workers.truncate(worker_count);
    }

    pub(crate) fn probe_handle(&self) -> OwnedLaneProbeHandle {
        OwnedLaneProbeHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// The servers of the connections idle workers are keeping, one entry
    /// per idle worker; `None` for a worker idle without a connection.
    pub(crate) fn idle_lane_servers(&self) -> Vec<Option<usize>> {
        let shared = lock_pool(&self.shared);
        shared
            .workers
            .iter()
            .filter_map(|worker| {
                let idle = worker.idle.as_ref()?;
                Some(idle.lane.as_ref().map(|lane| lane.server.0))
            })
            .collect()
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
        initial_lease: DownloadBatchLease,
    ) -> Result<(), DownloadBatchLease> {
        let run = Box::new(OwnedLaneRun {
            nntp,
            event_tx,
            refill_tx,
            parked_tx,
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
                | OwnedLanePoolCommand::RecallSocket(_)
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
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
) {
    let mut cached_lane: Option<CachedOwnedLane> = None;
    // Commands that reached the worker mid-lease and were not a probe. They
    // are answered here, at the boundary, in the order they arrived and ahead
    // of anything newer on the channel.
    let mut backlog: VecDeque<OwnedLanePoolCommand> = VecDeque::new();
    'work: loop {
        // Drain the pool's queue before going to sleep, and publish this
        // worker as idle only when there is nothing left to take.
        let queued = lock_pool(shared).take_queued_run_or_publish_idle(
            index,
            cached_lane.as_ref().map(IdleOwnedLane::from_cached),
        );
        if let Some(run) = queued {
            let link = WorkerLink {
                index,
                shared,
                commands: &rx,
            };
            backlog.extend(run_owned_blocking_download_lane(
                &mut cached_lane,
                *run,
                Some(link),
            ));
            continue;
        }

        // Idle from here until a run is routed to this worker. The other
        // commands are answered in place and leave the worker idle, so the
        // idle marker is only re-published after a run.
        loop {
            if let Some(cached) = cached_lane.as_ref() {
                let owner = Arc::downgrade(shared);
                cached.lane.mark_idle(Arc::new(move |id| {
                    if let Some(owner) = owner.upgrade() {
                        let _ = lock_pool(&owner).workers[index]
                            .sender
                            .send(OwnedLanePoolCommand::RecallSocket(id));
                    }
                }));
            }
            let command = match backlog.pop_front() {
                Some(command) => command,
                None => match rx.recv_timeout(CACHED_LANE_LIVENESS_INTERVAL) {
                    Ok(command) => command,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {
                        // A server that times out an idle connection leaves
                        // the socket half-closed on this side, and the worker
                        // would keep the pool permit for it until a lease was
                        // routed here. Give the permit back as soon as that
                        // is noticed.
                        if discard_closed_cached_lane(&mut cached_lane) {
                            lock_pool(shared).note_idle_lane(index, None);
                        }
                        continue;
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break 'work,
                },
            };
            if let Some(run) = answer_idle_command(index, shared, &mut cached_lane, command) {
                let link = WorkerLink {
                    index,
                    shared,
                    commands: &rx,
                };
                backlog.extend(run_owned_blocking_download_lane(
                    &mut cached_lane,
                    *run,
                    Some(link),
                ));
                continue 'work;
            }
        }
    }
    lock_pool(shared).mark_busy(index);
    park_cached_lane(&mut cached_lane);
}

/// Answer one command on an idle worker. A run is handed back to the caller
/// to start; everything else is settled here and leaves the worker idle.
fn answer_idle_command(
    index: usize,
    shared: &Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    cached_lane: &mut Option<CachedOwnedLane>,
    command: OwnedLanePoolCommand,
) -> Option<Box<OwnedLaneRun>> {
    match command {
        OwnedLanePoolCommand::RecallSocket(id) => {
            if cached_lane
                .as_ref()
                .is_some_and(|cached| cached.lane.socket_id() == id)
            {
                lock_pool(shared).note_idle_lane(index, None);
                // Local closure acknowledges this exact recall. No waiting
                // for QUIT, and no refund before socket drop.
                cached_lane.take();
            }
        }
        // The submit that sent this already claimed the worker, so there is
        // no marker to clear.
        OwnedLanePoolCommand::Run(run) => return Some(run),
        OwnedLanePoolCommand::Reset => {
            lock_pool(shared).note_idle_lane(index, None);
            park_cached_lane(cached_lane);
        }
        OwnedLanePoolCommand::Warm(warm) => {
            warm_cached_lane(cached_lane, *warm);
            lock_pool(shared)
                .note_idle_lane(index, cached_lane.as_ref().map(IdleOwnedLane::from_cached));
        }
        OwnedLanePoolCommand::Probe {
            message_ids,
            picked_up,
            reply,
        } => {
            if picked_up.send(()).is_err() {
                // The caller gave up waiting for pickup while this worker was
                // on a lease; nobody wants the answer now.
                return None;
            }
            // The lane keeps its cached connection and its idle marker across
            // a probe: a borrow of the socket, not a lease.
            discard_closed_cached_lane(cached_lane);
            if let Some(cached) = cached_lane.as_ref() {
                cached.lane.mark_active();
            }
            let answer = cached_lane
                .as_mut()
                .map(|cached: &mut CachedOwnedLane| cached.lane.probe_exists(&message_ids));
            // A probe that faulted the connection must not leave a dead lane
            // cached for the next lease to inherit.
            if cached_lane
                .as_ref()
                .is_some_and(|cached| !cached.lane.is_healthy())
            {
                lock_pool(shared).note_idle_lane(index, None);
                park_cached_lane(cached_lane);
            }
            let _ = reply.send(answer);
        }
    }
    None
}

/// How a run reaches back to the worker it is running on: the slot to
/// publish its connection's server on, and the command channel it polls
/// between ring responses so a probe can reach a busy lane.
struct WorkerLink<'a> {
    index: usize,
    shared: &'a Arc<std::sync::Mutex<OwnedLanePoolShared>>,
    commands: &'a std_mpsc::Receiver<OwnedLanePoolCommand>,
}

/// Open the connection a lease for this job would have asked for, so the lease
/// itself does not have to.
fn warm_cached_lane(cached_lane: &mut Option<CachedOwnedLane>, warm: OwnedLaneWarm) {
    if cached_lane.is_some() {
        return;
    }
    let OwnedLaneWarm {
        nntp,
        mut exclude_servers,
        byte_estimate,
    } = warm;
    exclude_servers = exclude_servers
        .iter()
        .copied()
        .chain((0..nntp.pool().server_count()).filter(|&idx| nntp.pool().requires_recovery(idx)))
        .collect();
    match nntp.try_warm_blocking_body_lane(&[], &exclude_servers, byte_estimate) {
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

/// Everything a downloaded article needs from the lease its work came from.
///
/// A lease boundary is no longer a pipeline boundary: the first BODY of the
/// next lease goes on the wire while the tail of the current one is still
/// being read, so two leases are in flight at once and a response cannot ask
/// "what is the current lease" when it lands. The context rides with the work
/// instead.
struct LaneLeaseContext {
    lane_id: u64,
    job_id: JobId,
    runtime_generation: u64,
    completion_critical: bool,
    /// The job's retention exclusions, carried from the lease. Job-derived and
    /// the same for every article on the lane; each result unions it with the
    /// article's own failure ledger.
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
    RecoveryYield,
    Quarantined,
    /// The result or refill channel is gone; the orchestrator is shutting down.
    Error,
}

fn lane_stop_park(stop: Option<LaneStop>) -> Option<(LaneParkReason, bool)> {
    Some(match stop? {
        LaneStop::ConnectionLost | LaneStop::Error => (LaneParkReason::Error, false),
        LaneStop::RecoveryYield => (LaneParkReason::ProbeYield, true),
        LaneStop::Quarantined => (LaneParkReason::Capacity, false),
        LaneStop::PolicyBlocked => (
            LaneParkReason::ServerQuota,
            keep_cached_lane_after_park(LaneParkReason::ServerQuota),
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
    send_owned_batch(
        event_tx,
        result.lane_id,
        vec![result],
        Vec::new(),
        stats,
        false,
    )
}

/// The newsgroups a connection for this lease has to be pointed at.
///
/// A batch is cut around its first work's compatibility and every work that
/// joins it is admitted against that, so the first work's groups are the
/// lease's groups. They are not decoration: a server that answers a
/// message-id fetch only after a GROUP prologue gets none from an empty list,
/// and a freshly dialled connection has no candidate for its initial group
/// probe.
fn lease_groups(lease: &DownloadBatchLease) -> &[String] {
    match lease.works.first() {
        Some(work) => work.groups.as_ref(),
        None => &[],
    }
}

/// Run one lease to its park on this worker's connection.
///
/// Returns the commands that reached the worker mid-lease and belong to the
/// lease boundary instead: the worker answers them before it goes idle.
#[allow(clippy::too_many_lines)]
fn run_owned_blocking_download_lane(
    cached_lane: &mut Option<CachedOwnedLane>,
    run: OwnedLaneRun,
    link: Option<WorkerLink<'_>>,
) -> Vec<OwnedLanePoolCommand> {
    let fetch_started = Instant::now();
    let mut deferred: Vec<OwnedLanePoolCommand> = Vec::new();
    let OwnedLaneRun {
        nntp,
        event_tx,
        refill_tx,
        parked_tx,
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
        match acquire_owned_lane_through_contention(
            &nntp,
            lease_groups(&lease),
            &lease.dial_exclude_servers,
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
                return deferred;
            }
        }
    }

    let lane = &mut cached_lane
        .as_mut()
        .expect("owned lane cache populated before run")
        .lane;
    let server_idx = lane.server_id().0;
    if let Some(link) = link.as_ref() {
        lock_pool(link.shared).note_busy_server(link.index, Some(server_idx));
    }
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
            return deferred;
        }
    }
    let mut inflight: VecDeque<(DownloadWork, Arc<LaneLeaseContext>)> = VecDeque::new();
    // Probes taken up mid-lease, answered together once the ring is dry.
    let mut probes: VecDeque<PendingProbe> = VecDeque::new();
    let mut pending_refill: Option<oneshot::Receiver<DownloadLaneRefillResponse>> = None;
    let mut refill_denied = false;
    let mut stop: Option<LaneStop> = None;
    let mut stats_mark = lane.stats();

    let (park_reason, keep_cached_lane) = loop {
        // Commands that reached this worker mid-lease. A probe is taken up
        // here and answered below once the ring has drained; everything else
        // belongs to the lease boundary and waits there.
        if let Some(link) = link.as_ref() {
            while let Ok(command) = link.commands.try_recv() {
                match command {
                    OwnedLanePoolCommand::Probe {
                        message_ids,
                        picked_up,
                        reply,
                    } => {
                        if picked_up.send(()).is_ok() {
                            probes.push_back((message_ids, reply));
                        }
                    }
                    other => deferred.push(other),
                }
            }
        }
        // With a probe waiting, nothing new is issued and the ring drains at
        // its own pace: at most one pipe of responses, one round trip plus
        // their transfer. Then the socket is exactly where a STAT expects it,
        // the batch runs on it, and issuing resumes. No dial, no permit.
        if !probes.is_empty() && stop.is_none() && lane.ring_outstanding() == 0 {
            while let Some((message_ids, reply)) = probes.pop_front() {
                let answer = lane.probe_exists(&message_ids);
                let _ = reply.send(Some(answer));
                if !lane.is_healthy() {
                    // Anything still queued for this probe is dropped with
                    // the lane, which tells its caller to ask elsewhere.
                    stop = Some(LaneStop::ConnectionLost);
                    break;
                }
            }
        }

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
        while stop.is_none() && probes.is_empty() && lane.ring_outstanding() < context.depth() {
            if !lane.accepts_new_work() {
                stop = Some(LaneStop::Quarantined);
                break;
            }
            let Some((work, work_context)) = pending.pop_front() else {
                break;
            };
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
                &work_context.retention_excludes,
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
                    lane_id: context.lane_id,
                    runtime_generation: context.runtime_generation,
                    server_idx,
                    remote_ip: lane.remote_ip(),
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
                        remote_ip: lane.remote_ip(),
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
                    work_context.lane_id,
                    work_context.job_id,
                    server_idx,
                    work_context.mode,
                    supports_pipelining,
                    lane.latency_ewma(),
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
    let _ = send_owned_batch(
        &event_tx,
        park_context.lane_id,
        Vec::new(),
        unrequested_works,
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
        release_ip_replacement_burst: false,
    });
    crate::runtime::perf_probe::record("download.fetch_body.owned", fetch_started.elapsed());
    deferred
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

/// The exclusions a result reports for one article: the article's own
/// failure ledger, plus the job's retention exclusions the lease carried.
///
/// The ledger has to come from the work itself. A lane's batch is cut for one
/// server but not for one exclusion set, so two articles on the same lane can
/// have failed on different servers; reporting the lease's set instead loses
/// every server this article has already been refused by, and an article
/// missing everywhere then retries forever because its exclusion set never
/// grows and exhaustion never trips.
///
/// The rotation hint (`avoid_server`) deliberately stays out: it only shapes
/// selection, and counting it here would let a transient timeout help declare
/// an article missing.
fn result_exclude_servers(work: &DownloadWork, retention_excludes: &[usize]) -> Vec<usize> {
    Pipeline::union_exclude_servers(&work.exclude_servers, retention_excludes)
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
    latency: Option<std::time::Duration>,
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
        exclude_servers,
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
                busy_server: None,
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
                // Every lane holds its probe until all of them are held, so
                // a probe that asks in sequence never answers and the runner
                // ends the test.
                while holding.load(Ordering::SeqCst) != LANES {
                    std::thread::yield_now();
                }
                let _ = reply.send(Some(weaver_nntp::client::ProbeBatchResult {
                    exists: vec![false; message_ids.len()],
                    inconclusive: false,
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
             asked in sequence, the first one never answers"
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

    /// A lane's batch is cut for one job, not for one exclusion set, so the
    /// result has to report the article's own failure ledger. Reporting the
    /// lease's set instead loses every server this article has already been
    /// refused by, and an article missing on all of them then retries forever
    /// because its exclusion set never grows past the server that just
    /// answered.
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
                latency: None,
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
                    JobId(42),
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
            0,
            JobId(42),
            weaver_nntp::client::DecodedBodyTrace {
                attempts: vec![weaver_nntp::client::FetchAttemptTrace {
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
        lease.dial_exclude_servers = exclude_servers;
        Box::new(OwnedLaneRun {
            nntp: Arc::clone(nntp),
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
        OwnedLanePoolShared {
            workers: idle
                .into_iter()
                .map(|idle| OwnedLaneWorkerSlot {
                    sender: std_mpsc::channel().0,
                    idle,
                    busy_server: None,
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
        let run = test_run(&nntp, Vec::new());

        assert!(idle_lane(&nntp, 0).serves(&run));
        let mut shared = shared_with(vec![idle_with(None), idle_with(Some(idle_lane(&nntp, 0)))]);
        assert_eq!(
            shared.claim_worker_for(&run),
            Some(1),
            "the warm connection is preferred over a fresh dial whatever its groups"
        );
    }

    /// Whatever connection a lease lands on is pointed at that lease's own
    /// newsgroups. An empty list is not the same thing: a server that answers
    /// a message-id fetch only after a GROUP prologue is sent none, and a
    /// fresh dial has no candidate for its initial group probe.
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
            async {
                picked_up_rx.await.expect("worker picked up startup probe");
                assert!(
                    reply_rx
                        .await
                        .expect("worker answered startup probe")
                        .is_none()
                );
            }
            .await;
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

    /// A worker that is busy but has not published its server yet is still
    /// dialling: there is no connection to answer on, so it is never asked
    /// and the probe reports that no lane could answer.
    #[tokio::test]
    async fn a_busy_worker_without_a_connection_is_not_asked() {
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

    /// A pool of hand-built slots whose command channels the test holds, so
    /// it can see exactly which workers a probe reaches.
    fn handle_over(
        slots: Vec<(Option<IdleOwnedLaneWorker>, Option<usize>)>,
    ) -> (
        OwnedLaneProbeHandle,
        Vec<std_mpsc::Receiver<OwnedLanePoolCommand>>,
    ) {
        let mut receivers = Vec::new();
        let workers = slots
            .into_iter()
            .map(|(idle, busy_server)| {
                let (sender, receiver) = std_mpsc::channel();
                receivers.push(receiver);
                OwnedLaneWorkerSlot {
                    sender,
                    idle,
                    busy_server,
                }
            })
            .collect();
        let shared = Arc::new(std::sync::Mutex::new(OwnedLanePoolShared {
            workers,
            queued_runs: VecDeque::new(),
        }));
        (OwnedLaneProbeHandle { shared }, receivers)
    }

    /// Answer the next probe on `receiver` with `exists`, off the runtime.
    fn answer_probe(
        receiver: std_mpsc::Receiver<OwnedLanePoolCommand>,
        exists: Vec<bool>,
    ) -> tokio::task::JoinHandle<std_mpsc::Receiver<OwnedLanePoolCommand>> {
        tokio::task::spawn_blocking(move || {
            let command = receiver.recv().expect("probe routed to this worker");
            let OwnedLanePoolCommand::Probe {
                picked_up, reply, ..
            } = command
            else {
                panic!("only probes reach a worker here");
            };
            picked_up.send(()).unwrap();
            reply
                .send(Some(weaver_nntp::client::ProbeBatchResult {
                    exists,
                    inconclusive: false,
                }))
                .unwrap();
            receiver
        })
    }

    /// One lane per server, idle before busy.
    ///
    /// Under a saturated pool every worker is mid-lease, and a server only
    /// busy lanes are holding used to be unreachable: the probe went to the
    /// async client, which had no permit to dial with and timed out. A busy
    /// lane answers instead, once its ring drains. An idle lane on the same
    /// server answers on the spot and is asked in its place, and a lane that
    /// is still dialling has nothing to answer on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_server_only_busy_lanes_hold_is_asked_through_one_of_them() {
        let (handle, mut receivers) = handle_over(vec![
            (
                Some(IdleOwnedLaneWorker {
                    lane: Some(test_idle_lane(0)),
                }),
                None,
            ),
            (None, Some(1)),
            (None, Some(0)),
            (None, None),
        ]);
        let busy_on_a_covered_server = receivers.pop().unwrap();
        let still_dialling = receivers.pop().unwrap();
        // The vector was popped back to front.
        let (busy_on_a_covered_server, still_dialling) = (still_dialling, busy_on_a_covered_server);
        let busy_answering = receivers.pop().unwrap();
        let idle_answering = receivers.pop().unwrap();
        let idle = answer_probe(idle_answering, vec![true]);
        let busy = answer_probe(busy_answering, vec![false]);

        let outcome = handle
            .probe(&["<probe@silver.horizon>".to_string()])
            .await
            .expect("two lanes answered");

        let mut settled = outcome.servers_settled.clone();
        settled.sort_unstable();
        assert_eq!(settled, vec![0, 1]);
        assert_eq!(outcome.result.exists, vec![true]);
        assert!(!outcome.result.inconclusive);
        idle.await.unwrap();
        busy.await.unwrap();
        assert!(
            matches!(
                busy_on_a_covered_server.try_recv(),
                Err(std_mpsc::TryRecvError::Empty)
            ),
            "a busy lane on a server an idle lane already covers is left alone"
        );
        assert!(
            matches!(
                still_dialling.try_recv(),
                Err(std_mpsc::TryRecvError::Empty)
            ),
            "a worker without a connection yet is not asked"
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
