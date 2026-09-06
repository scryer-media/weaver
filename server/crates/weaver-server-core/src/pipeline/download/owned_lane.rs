use super::*;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

const HOT_SHARE_YIELD_CHECK_ARTICLES: usize = 4;

pub(crate) struct OwnedDownloadLanePool {
    workers: Vec<OwnedLaneWorkerHandle>,
    release: OwnedLaneReleaseHandle,
    next: AtomicUsize,
    #[cfg(test)]
    reset_calls: AtomicUsize,
}

#[derive(Clone)]
struct OwnedLaneWorkerHandle {
    sender: std_mpsc::Sender<OwnedLanePoolCommand>,
    /// `server_idx + 1` while the worker sits idle holding a cached lane,
    /// and with it one of that server's connection permits; 0 otherwise.
    idle_server: Arc<AtomicUsize>,
}

/// Cloneable view of the owned-lane pool that async lanes use to reclaim
/// exactly one idle owned lane's permit from a specific server, instead of
/// parking the whole fleet on every async lease.
#[derive(Clone)]
pub(crate) struct OwnedLaneReleaseHandle {
    workers: Arc<std::sync::Mutex<Vec<OwnedLaneWorkerHandle>>>,
}

impl OwnedLaneReleaseHandle {
    /// Park one owned lane that is idle on `server_idx` so its permit
    /// returns to the pool. Returns whether such a lane was found; when
    /// none is, every permit is held by a lane that is actually working
    /// and the caller simply waits on the semaphore as before.
    pub(crate) fn release_idle_permit(&self, server_idx: usize) -> bool {
        let marker = server_idx + 1;
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for worker in workers.iter() {
            if worker
                .idle_server
                .compare_exchange(marker, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _ = worker.sender.send(OwnedLanePoolCommand::Reset);
                return true;
            }
        }
        false
    }
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
}

struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    groups: Arc<[String]>,
    lane: weaver_nntp::blocking::BlockingBodyLane,
}

impl OwnedDownloadLanePool {
    pub(crate) fn new(worker_count: usize) -> Self {
        let mut pool = Self {
            workers: Vec::new(),
            release: OwnedLaneReleaseHandle {
                workers: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
            next: AtomicUsize::new(0),
            #[cfg(test)]
            reset_calls: AtomicUsize::new(0),
        };
        pool.resize(worker_count);
        pool
    }

    pub(crate) fn resize(&mut self, worker_count: usize) {
        let worker_count = worker_count.max(1);
        while self.workers.len() < worker_count {
            let index = self.workers.len();
            let idle_server = Arc::new(AtomicUsize::new(0));
            let sender = spawn_owned_lane_worker(index, Arc::clone(&idle_server));
            self.workers.push(OwnedLaneWorkerHandle {
                sender,
                idle_server,
            });
        }
        self.workers.truncate(worker_count);
        *self
            .release
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = self.workers.clone();
    }

    pub(crate) fn release_handle(&self) -> OwnedLaneReleaseHandle {
        self.release.clone()
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    #[cfg(test)]
    pub(crate) fn reset_calls(&self) -> usize {
        self.reset_calls.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        #[cfg(test)]
        self.reset_calls.fetch_add(1, Ordering::Relaxed);
        for worker in &self.workers {
            let _ = worker.sender.send(OwnedLanePoolCommand::Reset);
        }
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
        if self.workers.is_empty() {
            return Err(initial_lease);
        }
        let command = OwnedLanePoolCommand::Run(Box::new(OwnedLaneRun {
            nntp,
            event_tx,
            refill_tx,
            parked_tx,
            hot_share_yield_signal,
            initial_lease,
        }));
        let sender_index = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[sender_index]
            .sender
            .send(command)
            .map_err(|error| match error.0 {
                OwnedLanePoolCommand::Run(run) => run.initial_lease,
                OwnedLanePoolCommand::Reset => {
                    unreachable!("reset command cannot fail from submit")
                }
            })
    }
}

fn spawn_owned_lane_worker(
    index: usize,
    idle_server: Arc<AtomicUsize>,
) -> std_mpsc::Sender<OwnedLanePoolCommand> {
    let (tx, rx) = std_mpsc::channel();
    std::thread::Builder::new()
        .name(format!("weaver-nntp-lane-{index}"))
        .spawn(move || {
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();
            run_owned_lane_worker(rx, idle_server);
        })
        .expect("failed to spawn owned blocking NNTP lane");
    tx
}

fn run_owned_lane_worker(
    rx: std_mpsc::Receiver<OwnedLanePoolCommand>,
    idle_server: Arc<AtomicUsize>,
) {
    let mut cached_lane = None;
    while let Ok(command) = rx.recv() {
        match command {
            OwnedLanePoolCommand::Run(run) => {
                idle_server.store(0, Ordering::Release);
                run_owned_blocking_download_lane(&mut cached_lane, *run);
                let marker = cached_lane
                    .as_ref()
                    .map_or(0, |cached: &CachedOwnedLane| cached.lane.server_id().0 + 1);
                idle_server.store(marker, Ordering::Release);
            }
            OwnedLanePoolCommand::Reset => {
                idle_server.store(0, Ordering::Release);
                park_cached_lane(&mut cached_lane);
            }
        }
    }
    idle_server.store(0, Ordering::Release);
    park_cached_lane(&mut cached_lane);
}

impl CachedOwnedLane {
    fn matches(&self, nntp: &Arc<weaver_nntp::NntpClient>, lease: &DownloadBatchLease) -> bool {
        let server = self.lane.server_id();
        if !Arc::ptr_eq(&self.nntp, nntp)
            || !(Arc::ptr_eq(&self.groups, &lease.compatibility.groups)
                || self.groups == lease.compatibility.groups)
            || lease.effective_exclude_servers.contains(&server.0)
        {
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
        let selection = nntp.blocking_body_server_selection_with_estimate(
            &lease.effective_exclude_servers,
            estimate,
        );
        cached_lane_matches_selection(server, true, &selection)
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
    is_recovery: bool,
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
            is_recovery: lease.compatibility.is_recovery,
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
        LaneStop::HotShareYield => (LaneParkReason::HotShareYield, false),
    })
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

    if cached_lane.is_none() {
        let initial_estimate = Pipeline::bandwidth_reservation_estimate(
            lease
                .works
                .first()
                .expect("owned download lease must contain work")
                .byte_estimate,
        );
        match nntp.try_acquire_blocking_body_lane_with_estimate(
            &lease.compatibility.groups,
            &lease.effective_exclude_servers,
            initial_estimate,
        ) {
            Ok(lane) => {
                *cached_lane = Some(CachedOwnedLane {
                    nntp: Arc::clone(&nntp),
                    groups: lease.compatibility.groups.clone(),
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
                work_context.is_recovery,
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
            work_context.is_recovery,
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
                    work_context.is_recovery,
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

    drain_pending_refill(pending_refill.take(), &event_tx);
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
) {
    let Some(response_rx) = pending_refill else {
        return;
    };
    if let Ok(response) = response_rx.blocking_recv()
        && let Some(lease) = response.lease
        && !lease.works.is_empty()
    {
        let _ = send_owned_batch(
            event_tx,
            Vec::new(),
            lease.works,
            weaver_nntp::blocking::BlockingLaneStats::default(),
            true,
        );
    }
}

fn keep_cached_lane_after_park(reason: LaneParkReason) -> bool {
    matches!(
        reason,
        LaneParkReason::NoWork
            | LaneParkReason::Pressure
            | LaneParkReason::ProbeYield
            | LaneParkReason::HotReclaim
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
    is_recovery: bool,
    exclude_servers: &[usize],
) -> DownloadResult {
    let segment_id = work.segment_id;
    let retry_count = work.retry_count;
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
    is_recovery: bool,
    exclude_servers: &[usize],
    message: &'static str,
) -> DownloadResult {
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
mod tests {
    use super::*;

    use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

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
            old.is_recovery,
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
                    false,
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
    fn owned_hot_lane_yield_discards_the_lane_and_keeps_its_retry_counts() {
        // Yielding hands the connection back, so the lane is not cached; the
        // works that were never issued go back to the queue untouched, which
        // is what keeps a yield from spending a retry.
        let (reason, keep_cached_lane) =
            lane_stop_park(Some(LaneStop::HotShareYield)).expect("a stop always parks");

        assert_eq!(reason, LaneParkReason::HotShareYield);
        assert!(!keep_cached_lane);

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
            false,
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
            false,
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
mod release_tests {
    use super::*;

    #[test]
    fn release_idle_permit_claims_only_a_worker_idle_on_that_server() {
        let pool = OwnedDownloadLanePool::new(3);
        let handle = pool.release_handle();
        // Worker 1 sits idle holding a cached lane (and permit) on server 2.
        pool.workers[1].idle_server.store(3, Ordering::Release);

        assert!(!handle.release_idle_permit(0));
        assert!(!handle.release_idle_permit(1));
        assert!(handle.release_idle_permit(2));
        assert_eq!(pool.workers[1].idle_server.load(Ordering::Acquire), 0);
        // The permit was claimed once; a second reclaim finds nothing idle.
        assert!(!handle.release_idle_permit(2));
    }

    #[test]
    fn release_handle_tracks_pool_resizes() {
        let mut pool = OwnedDownloadLanePool::new(1);
        let handle = pool.release_handle();
        pool.resize(4);
        pool.workers[3].idle_server.store(1, Ordering::Release);

        assert!(handle.release_idle_permit(0));

        pool.resize(2);
        assert_eq!(
            handle
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            2
        );
    }
}
