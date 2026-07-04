use super::*;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

const HOT_SHARE_YIELD_CHECK_ARTICLES: usize = 4;

pub(crate) struct OwnedDownloadLanePool {
    senders: Vec<std_mpsc::Sender<OwnedLanePoolCommand>>,
    next: AtomicUsize,
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
    Run(OwnedLaneRun),
    Reset,
}

struct CachedOwnedLane {
    nntp: Arc<weaver_nntp::NntpClient>,
    groups: Vec<String>,
    lane: weaver_nntp::blocking::BlockingBodyLane,
}

impl OwnedDownloadLanePool {
    pub(crate) fn new(worker_count: usize) -> Self {
        let mut pool = Self {
            senders: Vec::new(),
            next: AtomicUsize::new(0),
        };
        pool.resize(worker_count);
        pool
    }

    pub(crate) fn resize(&mut self, worker_count: usize) {
        let worker_count = worker_count.max(1);
        while self.senders.len() < worker_count {
            let index = self.senders.len();
            self.senders.push(spawn_owned_lane_worker(index));
        }
        self.senders.truncate(worker_count);
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.senders.len()
    }

    pub(crate) fn reset(&self) {
        for sender in &self.senders {
            let _ = sender.send(OwnedLanePoolCommand::Reset);
        }
    }

    pub(crate) fn submit(
        &self,
        nntp: Arc<weaver_nntp::NntpClient>,
        event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
        refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
        parked_tx: mpsc::Sender<DownloadLaneParked>,
        hot_share_yield_signal: Arc<HotShareYieldSignal>,
        initial_lease: DownloadBatchLease,
    ) -> Result<(), DownloadBatchLease> {
        if self.senders.is_empty() {
            return Err(initial_lease);
        }
        let command = OwnedLanePoolCommand::Run(OwnedLaneRun {
            nntp,
            event_tx,
            refill_tx,
            parked_tx,
            hot_share_yield_signal,
            initial_lease,
        });
        let sender_index = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        self.senders[sender_index]
            .send(command)
            .map_err(|error| match error.0 {
                OwnedLanePoolCommand::Run(run) => run.initial_lease,
                OwnedLanePoolCommand::Reset => {
                    unreachable!("reset command cannot fail from submit")
                }
            })
    }
}

fn spawn_owned_lane_worker(index: usize) -> std_mpsc::Sender<OwnedLanePoolCommand> {
    let (tx, rx) = std_mpsc::channel();
    std::thread::Builder::new()
        .name(format!("weaver-nntp-lane-{index}"))
        .spawn(move || {
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();
            run_owned_lane_worker(rx);
        })
        .expect("failed to spawn owned blocking NNTP lane");
    tx
}

fn run_owned_lane_worker(rx: std_mpsc::Receiver<OwnedLanePoolCommand>) {
    let mut cached_lane = None;
    while let Ok(command) = rx.recv() {
        match command {
            OwnedLanePoolCommand::Run(run) => {
                run_owned_blocking_s2n_download_lane(&mut cached_lane, run);
            }
            OwnedLanePoolCommand::Reset => {
                park_cached_lane(&mut cached_lane);
            }
        }
    }
    park_cached_lane(&mut cached_lane);
}

impl CachedOwnedLane {
    fn matches(
        &self,
        nntp: &Arc<weaver_nntp::NntpClient>,
        compatibility: &DownloadBatchCompatibility,
    ) -> bool {
        Arc::ptr_eq(&self.nntp, nntp)
            && self.groups == compatibility.groups
            && !compatibility
                .exclude_servers
                .contains(&self.lane.server_id().0)
    }
}

fn park_cached_lane(cached_lane: &mut Option<CachedOwnedLane>) {
    if let Some(cached) = cached_lane.take() {
        cached.lane.park();
    }
}

fn run_owned_blocking_s2n_download_lane(
    cached_lane: &mut Option<CachedOwnedLane>,
    run: OwnedLaneRun,
) {
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
        .is_some_and(|cached| !cached.matches(&nntp, &lease.compatibility))
    {
        park_cached_lane(cached_lane);
    }

    if cached_lane.is_none() {
        match nntp.try_acquire_blocking_body_lane(
            &lease.compatibility.groups,
            &lease.compatibility.exclude_servers,
        ) {
            Ok(lane) => {
                *cached_lane = Some(CachedOwnedLane {
                    nntp: Arc::clone(&nntp),
                    groups: lease.compatibility.groups.clone(),
                    lane,
                });
            }
            Err(error) => {
                let _ = event_tx.blocking_send(OwnedDownloadLaneEvent::AcquireFailed {
                    lease,
                    error: error.to_string(),
                });
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
    let (park_reason, parked_job_id, parked_mode, parked_spillover_loan_kind, keep_cached_lane) = loop {
        let stats_before = lane.stats();
        let DownloadBatchLease {
            job_id,
            lane_mode,
            spillover_loan_kind,
            server_modes,
            compatibility,
            works,
        } = lease;
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
        let mut batch_clean_for_refill = true;
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

            let message_ids = batch_works
                .iter()
                .map(|work| work.message_id.to_string())
                .collect::<Vec<_>>();
            let total = batch_works.len();
            let mut completed = 0usize;
            let mut works_by_index = batch_works.into_iter().map(Some).collect::<Vec<_>>();

            match actual_mode {
                DownloadLaneMode::Sequential => {
                    for (idx, message_id) in message_ids.iter().enumerate() {
                        let trace = lane.fetch_decoded_sequential(message_id);
                        nntp.record_blocking_attempts(&trace.attempts);
                        completed += 1;
                        let work = works_by_index[idx]
                            .take()
                            .expect("owned lane result emitted once per work item");
                        let result = result_from_trace(
                            work,
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
                        batch_clean_for_refill &= result.data.is_ok();
                        results.push(result);
                    }
                }
                DownloadLaneMode::PipelineDepth2 | DownloadLaneMode::PipelineDepth4 => {
                    for (idx, trace, meta) in
                        lane.fetch_decoded_pipeline(&message_ids, actual_mode.max_depth())
                    {
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
                            trace,
                            observation,
                            is_recovery,
                            &exclude_servers,
                        );
                        batch_clean_for_refill &= result.data.is_ok() && meta.batch_clean;
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

            if !batch_clean_for_refill {
                break;
            }

            completed_since_yield_check = completed_since_yield_check.saturating_add(completed);
            if completed_since_yield_check >= HOT_SHARE_YIELD_CHECK_ARTICLES {
                completed_since_yield_check = 0;
                if hot_share_yield_signal.is_requested_for(job_id) {
                    yielded_for_hot_share = true;
                    break;
                }
            }
        }

        unrequested_works.extend(take_unrequested_tail(
            &mut pending_works,
            batch_clean_for_refill,
            yielded_for_hot_share,
        ));

        let stats = stats_delta(lane.stats(), stats_before);
        if send_owned_batch(&event_tx, results, unrequested_works, stats).is_err() {
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }

        if !batch_clean_for_refill {
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }
        if yielded_for_hot_share {
            break (
                LaneParkReason::HotShareYield,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }

        let (response_tx, response_rx) = oneshot::channel();
        if refill_tx
            .blocking_send(DownloadLaneRefillRequest {
                job_id,
                server_idx,
                remote_ip: lane.remote_ip(),
                supports_pipelining,
                current_mode: actual_mode,
                spillover_loan_kind,
                compatibility,
                response_tx,
            })
            .is_err()
        {
            break (
                LaneParkReason::Error,
                job_id,
                actual_mode,
                spillover_loan_kind,
                false,
            );
        }

        match response_rx.blocking_recv() {
            Ok(response) => {
                if let Some(next_lease) = response.lease
                    && !next_lease.works.is_empty()
                {
                    lease = next_lease;
                    continue;
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
        reason: park_reason,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });
    crate::runtime::perf_probe::record("download.fetch_body.owned", fetch_started.elapsed());
}

fn take_unrequested_tail(
    pending_works: &mut VecDeque<DownloadWork>,
    batch_clean_for_refill: bool,
    yielded_for_hot_share: bool,
) -> Vec<DownloadWork> {
    if batch_clean_for_refill && !yielded_for_hot_share {
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
    trace: weaver_nntp::client::DecodedBodyTrace,
    mut observation: DownloadLaneObservation,
    is_recovery: bool,
    exclude_servers: &[usize],
) -> DownloadResult {
    let segment_id = work.segment_id;
    let retry_count = work.retry_count;
    let (data, attempts, source_server_idx) =
        Pipeline::download_data_from_decoded_trace(segment_id, trace);
    observation.batch_clean &= data.is_ok();
    observation.connection_discarded |= data.is_err();
    DownloadResult {
        segment_id,
        data,
        attempts,
        lane_observation: Some(observation),
        source_server_idx,
        origin: DownloadResultOrigin::from_recovery(is_recovery),
        retry_count,
        exclude_servers: exclude_servers.to_vec(),
        release_connection_slot: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn unresolved_result(
    work: DownloadWork,
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
    DownloadResult {
        segment_id: work.segment_id,
        data: Err(DownloadError::Fetch(DownloadFailure {
            kind: DownloadFailureKind::Transient,
            message: message.to_string(),
        })),
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
        origin: DownloadResultOrigin::from_recovery(is_recovery),
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
            groups: vec!["alt.binaries.test".to_string()],
            priority: 3,
            byte_estimate: 1024,
            retry_count,
            is_recovery: false,
            exclude_servers: Vec::new(),
        }
    }

    #[test]
    fn owned_hot_lane_yield_returns_unrequested_tail_without_retry() {
        let mut pending_works = VecDeque::from([tail_work(2, 4), tail_work(3, 7)]);

        let tail = take_unrequested_tail(&mut pending_works, true, true);

        assert!(pending_works.is_empty());
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].segment_id.segment_number, 2);
        assert_eq!(tail[0].retry_count, 4);
        assert_eq!(tail[1].segment_id.segment_number, 3);
        assert_eq!(tail[1].retry_count, 7);
    }
}
