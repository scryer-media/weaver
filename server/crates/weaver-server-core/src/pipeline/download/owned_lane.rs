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
    let mut lease = initial_lease;

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
    let mut parked_completion_critical: bool;
    let (park_reason, parked_job_id, parked_mode, parked_spillover_loan_kind, keep_cached_lane) = loop {
        let stats_before = lane.stats();
        let DownloadBatchLease {
            job_id,
            runtime_generation,
            lane_mode,
            spillover_loan_kind,
            server_modes,
            compatibility,
            effective_exclude_servers: _,
            checkpoint_plan,
            works,
        } = lease;
        parked_completion_critical = compatibility.completion_critical;
        let server_idx = lane.server_id().0;
        let supports_pipelining = lane.supports_pipelining();
        let actual_mode = Pipeline::actual_download_lane_mode(
            lane_mode,
            &server_modes,
            server_idx,
            supports_pipelining,
        );
        let is_recovery = compatibility.is_recovery;
        let exclude_servers = compatibility.exclude_servers.clone();
        // An owned lane outlives its batch, so the immutable checkpoint plan
        // is re-applied for every batch, including `None` after pooled reuse.
        lane.set_checkpoint_plan(checkpoint_plan);

        // Prefetch the next lease while this batch downloads. The refill
        // response then overlaps the batch instead of serializing at the
        // batch boundary behind the orchestrator's result-ingest loop, which
        // stalled every lane at once when batches completed in lockstep.
        let mut pending_refill: Option<oneshot::Receiver<DownloadLaneRefillResponse>> = None;
        {
            let (response_tx, response_rx) = oneshot::channel();
            if refill_tx
                .blocking_send(DownloadLaneRefillRequest {
                    job_id,
                    runtime_generation,
                    server_idx,
                    remote_ip: lane.remote_ip(),
                    supports_pipelining,
                    current_mode: actual_mode,
                    spillover_loan_kind,
                    compatibility: compatibility.clone(),
                    response_tx,
                })
                .is_ok()
            {
                pending_refill = Some(response_rx);
            }
        }

        let mut batch_clean_for_refill = true;
        let mut policy_blocked_for_refill = false;
        let mut pending_works: VecDeque<DownloadWork> = works.into_iter().collect();
        let mut results = Vec::with_capacity(pending_works.len());
        let mut unrequested_works = Vec::new();
        let mut completed_since_yield_check = 0usize;
        let mut yielded_for_hot_share = false;

        while let Some(first_work) = pending_works.pop_front() {
            let batch_depth = actual_mode.max_depth();
            let mut batch_works = Vec::with_capacity(batch_depth);
            batch_works.push(first_work);
            while batch_works.len() < batch_depth {
                let Some(work) = pending_works.pop_front() else {
                    break;
                };
                batch_works.push(work);
            }

            let message_id_handles = lease_message_id_wire_forms(&batch_works);
            let message_ids = message_id_handles
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>();
            let total = batch_works.len();
            let mut completed = 0usize;
            let mut works_by_index = batch_works.into_iter().map(Some).collect::<Vec<_>>();

            match actual_mode {
                DownloadLaneMode::Sequential => {
                    for (idx, message_id) in message_ids.iter().enumerate() {
                        let estimate = Pipeline::bandwidth_reservation_estimate(
                            works_by_index[idx]
                                .as_ref()
                                .expect("owned download work exists until its BODY result")
                                .byte_estimate,
                        );
                        let trace =
                            lane.fetch_decoded_sequential_with_estimate(message_id, estimate);
                        nntp.record_blocking_attempts(&trace.attempts);
                        completed += 1;
                        let work = works_by_index[idx]
                            .take()
                            .expect("owned lane result emitted once per work item");
                        let result = result_from_trace(
                            work,
                            runtime_generation,
                            trace,
                            DownloadLaneObservation {
                                server_idx: Some(server_idx),
                                mode: DownloadLaneMode::Sequential,
                                supports_pipelining,
                                rtt: lane.rtt_ewma(),
                                batch_complete: true,
                                batch_clean: true,
                                batch_response_count: 1,
                                unresolved_count: 0,
                                connection_discarded: false,
                            },
                            is_recovery,
                            &exclude_servers,
                        );
                        let policy_outcome = matches!(
                            &result.data,
                            Err(DownloadError::Fetch(failure))
                                if matches!(
                                    failure.kind,
                                    DownloadFailureKind::ServerQuota
                                        | DownloadFailureKind::Unrequested
                                )
                        );
                        batch_clean_for_refill &= download_outcome_keeps_connection(&result.data);
                        policy_blocked_for_refill |= policy_outcome;
                        results.push(result);
                    }
                }
                DownloadLaneMode::PipelineDepth2 | DownloadLaneMode::PipelineDepth4 => {
                    let estimated_body_bytes = works_by_index
                        .iter()
                        .map(|work| {
                            Pipeline::bandwidth_reservation_estimate(
                                work.as_ref()
                                    .expect("owned download work exists before BODY issue")
                                    .byte_estimate,
                            )
                        })
                        .collect::<Vec<_>>();
                    for (idx, trace, meta) in lane.fetch_decoded_pipeline_with_estimates(
                        &message_ids,
                        &estimated_body_bytes,
                        actual_mode.max_depth(),
                    ) {
                        nntp.record_blocking_attempts(&trace.attempts);
                        completed += 1;
                        let work = works_by_index[idx]
                            .take()
                            .expect("owned lane result emitted once per work item");
                        let observation = DownloadLaneObservation {
                            server_idx: Some(server_idx),
                            mode: actual_mode,
                            supports_pipelining,
                            rtt: lane.rtt_ewma(),
                            batch_complete: meta.batch_complete,
                            batch_clean: meta.batch_clean,
                            batch_response_count: meta.batch_response_count,
                            unresolved_count: meta.unresolved_count,
                            connection_discarded: meta.connection_discarded,
                        };
                        let result = result_from_trace(
                            work,
                            runtime_generation,
                            trace,
                            observation,
                            is_recovery,
                            &exclude_servers,
                        );
                        let policy_outcome = matches!(
                            &result.data,
                            Err(DownloadError::Fetch(failure))
                                if matches!(
                                    failure.kind,
                                    DownloadFailureKind::ServerQuota
                                        | DownloadFailureKind::Unrequested
                                )
                        );
                        batch_clean_for_refill &=
                            download_outcome_keeps_connection(&result.data) && meta.batch_clean;
                        policy_blocked_for_refill |= policy_outcome;
                        results.push(result);
                    }
                }
            }

            let unresolved_count = total.saturating_sub(completed);
            if unresolved_count > 0 {
                batch_clean_for_refill = false;
            }
            for work in works_by_index.into_iter().flatten() {
                results.push(unresolved_result(
                    work,
                    runtime_generation,
                    server_idx,
                    actual_mode,
                    supports_pipelining,
                    lane.rtt_ewma(),
                    completed as u64,
                    unresolved_count as u64,
                    is_recovery,
                    &exclude_servers,
                    "batch ended without result",
                ));
            }

            if !batch_clean_for_refill || policy_blocked_for_refill {
                break;
            }

            completed_since_yield_check = completed_since_yield_check.saturating_add(completed);
            if completed_since_yield_check >= HOT_SHARE_YIELD_CHECK_ARTICLES {
                completed_since_yield_check = 0;
                // A completion-critical batch never yields here — it is the
                // work the signal exists to make room for.
                if !parked_completion_critical && hot_share_yield_signal.is_requested() {
                    yielded_for_hot_share = true;
                    break;
                }
            }
        }

        unrequested_works.extend(take_unrequested_tail(
            &mut pending_works,
            batch_clean_for_refill,
            yielded_for_hot_share,
            policy_blocked_for_refill,
        ));

        let stats = stats_delta(lane.stats(), stats_before);
        if send_owned_batch(&event_tx, results, unrequested_works, stats).is_err() {
            drain_pending_refill(pending_refill.take(), &event_tx);
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }

        if !batch_clean_for_refill {
            drain_pending_refill(pending_refill.take(), &event_tx);
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }
        if policy_blocked_for_refill {
            drain_pending_refill(pending_refill.take(), &event_tx);
            break (
                LaneParkReason::ServerQuota,
                job_id,
                actual_mode,
                spillover_loan_kind,
                keep_cached_lane_after_park(LaneParkReason::ServerQuota),
            );
        }
        if yielded_for_hot_share {
            drain_pending_refill(pending_refill.take(), &event_tx);
            break (
                LaneParkReason::HotShareYield,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }

        let Some(response_rx) = pending_refill.take() else {
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        };

        match response_rx.blocking_recv() {
            Ok(response) => {
                if let Some(next_lease) = response.lease
                    && !next_lease.works.is_empty()
                {
                    lease = next_lease;
                    continue;
                }
                // The prefetched refill was evaluated mid-batch, where
                // transient decode/write pressure can deny it. Ask once more
                // now that this batch's results are delivered — the
                // pre-prefetch decision point — so a momentary pressure blip
                // does not park a healthy lane.
                let (retry_tx, retry_rx) = oneshot::channel();
                let retry_sent = refill_tx
                    .blocking_send(DownloadLaneRefillRequest {
                        job_id,
                        runtime_generation,
                        server_idx,
                        remote_ip: lane.remote_ip(),
                        supports_pipelining,
                        current_mode: actual_mode,
                        spillover_loan_kind,
                        compatibility: compatibility.clone(),
                        response_tx: retry_tx,
                    })
                    .is_ok();
                if retry_sent && let Ok(retry) = retry_rx.blocking_recv() {
                    if let Some(next_lease) = retry.lease
                        && !next_lease.works.is_empty()
                    {
                        lease = next_lease;
                        continue;
                    }
                    break (
                        retry.park_reason,
                        job_id,
                        actual_mode,
                        spillover_loan_kind,
                        keep_cached_lane_after_park(retry.park_reason),
                    );
                }
                break (
                    response.park_reason,
                    job_id,
                    actual_mode,
                    spillover_loan_kind,
                    keep_cached_lane_after_park(response.park_reason),
                );
            }
            Err(_) => {
                break (
                    LaneParkReason::ProbeYield,
                    job_id,
                    actual_mode,
                    spillover_loan_kind,
                    false,
                );
            }
        }
    };

    if !keep_cached_lane {
        park_cached_lane(cached_lane);
    }
    let _ = parked_tx.blocking_send(DownloadLaneParked {
        job_id: parked_job_id,
        mode: parked_mode,
        spillover_loan_kind: parked_spillover_loan_kind,
        completion_critical: parked_completion_critical,
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
        );
    }
}

fn take_unrequested_tail(
    pending_works: &mut VecDeque<DownloadWork>,
    batch_clean_for_refill: bool,
    yielded_for_hot_share: bool,
    policy_blocked_for_refill: bool,
) -> Vec<DownloadWork> {
    if batch_clean_for_refill && !yielded_for_hot_share && !policy_blocked_for_refill {
        return Vec::new();
    }
    pending_works.drain(..).collect()
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

fn send_owned_batch(
    event_tx: &mpsc::Sender<OwnedDownloadLaneEvent>,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
    stats: weaver_nntp::blocking::BlockingLaneStats,
) -> Result<(), ()> {
    if results.is_empty() && unrequested_works.is_empty() {
        return Ok(());
    }
    let (ack, ack_rx) = std::sync::mpsc::sync_channel(0);
    event_tx
        .blocking_send(OwnedDownloadLaneEvent::BatchComplete {
            results,
            unrequested_works,
            stats,
            ack,
        })
        .map_err(|_| ())?;
    ack_rx.recv().map_err(|_| ())?;
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
    rtt: Option<std::time::Duration>,
    completed: u64,
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
            rtt,
            batch_complete: true,
            batch_clean: false,
            batch_response_count: completed,
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
    fn owned_hot_lane_yield_returns_unrequested_tail_without_retry() {
        let mut pending_works = VecDeque::from([tail_work(2, 4), tail_work(3, 7)]);

        let tail = take_unrequested_tail(&mut pending_works, true, true, false);

        assert!(pending_works.is_empty());
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].segment_id.segment_number, 2);
        assert_eq!(tail[0].retry_count, 4);
        assert_eq!(tail[1].segment_id.segment_number, 3);
        assert_eq!(tail[1].retry_count, 7);
    }

    #[test]
    fn owned_quota_park_returns_unrequested_tail_without_dirtying_batch() {
        let mut pending_works = VecDeque::from([tail_work(4, 2), tail_work(5, 2)]);

        let tail = take_unrequested_tail(&mut pending_works, true, false, true);

        assert!(pending_works.is_empty());
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].segment_id.segment_number, 4);
        assert_eq!(tail[1].segment_id.segment_number, 5);
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
                mode: DownloadLaneMode::PipelineDepth4,
                supports_pipelining: true,
                rtt: None,
                batch_complete: true,
                batch_clean: true,
                batch_response_count: 4,
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

        // A clean batch never blocks the server's pipelining proof.
        let mut proof = crate::pipeline::download::transport::ServerPipelineProof::default();
        let transition = proof.note_pipeline_batch(
            Instant::now(),
            DownloadLaneMode::PipelineDepth4,
            observation.batch_clean,
            observation.batch_response_count,
        );
        assert!(!matches!(
            transition,
            Some(crate::pipeline::download::transport::ServerPipelineState::PipelineBlocked)
        ));

        // The rest of the leased batch is still requested, not handed back.
        let mut pending_works = VecDeque::from([tail_work(10, 0), tail_work(11, 0)]);
        assert!(take_unrequested_tail(&mut pending_works, true, false, false).is_empty());
        assert_eq!(pending_works.len(), 2);
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
                rtt: None,
                batch_complete: true,
                batch_clean: true,
                batch_response_count: 0,
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
