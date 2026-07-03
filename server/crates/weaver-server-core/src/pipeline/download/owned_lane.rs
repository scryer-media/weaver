use super::*;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

pub(super) fn spawn_owned_blocking_s2n_download_batch(
    nntp: Arc<weaver_nntp::NntpClient>,
    event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    parked_tx: mpsc::Sender<DownloadLaneParked>,
    initial_lease: DownloadBatchLease,
) {
    std::thread::Builder::new()
        .name("weaver-owned-nntp".to_string())
        .spawn(move || {
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();
            run_owned_blocking_s2n_download_lane(
                nntp,
                event_tx,
                refill_tx,
                parked_tx,
                initial_lease,
            );
        })
        .expect("failed to spawn owned blocking NNTP lane");
}

fn run_owned_blocking_s2n_download_lane(
    nntp: Arc<weaver_nntp::NntpClient>,
    event_tx: mpsc::Sender<OwnedDownloadLaneEvent>,
    refill_tx: mpsc::Sender<DownloadLaneRefillRequest>,
    parked_tx: mpsc::Sender<DownloadLaneParked>,
    initial_lease: DownloadBatchLease,
) {
    let fetch_started = Instant::now();
    let mut lease = initial_lease;
    let mut lane = match nntp.try_acquire_blocking_body_lane(
        &lease.compatibility.groups,
        &lease.compatibility.exclude_servers,
    ) {
        Ok(lane) => lane,
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
    };

    let (park_reason, parked_job_id, parked_mode) = loop {
        let DownloadBatchLease {
            job_id,
            lane_mode,
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
        }

        if !batch_clean_for_refill {
            unrequested_works.extend(pending_works);
        }

        let stats = lane.stats();
        if send_owned_batch(&event_tx, results, unrequested_works, stats).is_err() {
            break (LaneParkReason::Error, job_id, actual_mode);
        }

        if !batch_clean_for_refill {
            break (LaneParkReason::Error, job_id, actual_mode);
        }

        let (response_tx, response_rx) = oneshot::channel();
        if refill_tx
            .blocking_send(DownloadLaneRefillRequest {
                job_id,
                server_idx,
                remote_ip: lane.remote_ip(),
                supports_pipelining,
                current_mode: actual_mode,
                compatibility,
                response_tx,
            })
            .is_err()
        {
            break (LaneParkReason::Error, job_id, actual_mode);
        }

        match response_rx.blocking_recv() {
            Ok(response) => {
                if let Some(next_lease) = response.lease
                    && !next_lease.works.is_empty()
                {
                    lease = next_lease;
                    continue;
                }
                break (response.park_reason, job_id, actual_mode);
            }
            Err(_) => {
                break (LaneParkReason::ProbeYield, job_id, actual_mode);
            }
        }
    };

    lane.park();
    let _ = parked_tx.blocking_send(DownloadLaneParked {
        job_id: parked_job_id,
        mode: parked_mode,
        reason: park_reason,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });
    crate::runtime::perf_probe::record("download.fetch_body.owned", fetch_started.elapsed());
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
