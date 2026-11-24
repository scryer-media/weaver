// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::download::scheduler::Handout;

pub const SEATED_BATCH_ANSWERS_WITHIN: Duration =
    crate::pipeline::post_health::SEATED_BATCH_ANSWERS_WITHIN;

#[derive(Clone, Copy)]
pub struct ProbeFailure(DownloadFailureKind);

impl ProbeFailure {
    pub const ESTABLISHED_TRANSPORT: Self = Self(DownloadFailureKind::EstablishedTransport);
    pub const CONNECTION_ESTABLISHMENT: Self = Self(DownloadFailureKind::ConnectionEstablishment);
}

pub fn job_position(pipeline: &Pipeline, id: JobId) -> Option<usize> {
    pipeline.job_order.iter().position(|job| *job == id)
}

pub fn admitted(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.nzb_admitted.contains(&id)
}

pub fn articles_in_flight(pipeline: &Pipeline) -> usize {
    pipeline.articles_in_flight
}

// Model a turn busy with an article without installing a transport lane.
pub fn busy_dispatch_at(pipeline: &mut Pipeline, at: Instant) {
    let was_auditing = pipeline.audit_lane_holdings;
    pipeline.audit_lane_holdings = false;
    pipeline.articles_in_flight = 1;
    pipeline.dispatch_downloads_at(at);
    pipeline.articles_in_flight = 0;
    pipeline.audit_lane_holdings = was_auditing;
}

pub fn handoff(pipeline: &mut Pipeline, draining: bool) {
    pipeline.nntp_handoff_draining = draining;
}

pub fn set_rate(pipeline: &mut Pipeline, rate: Option<u64>) {
    pipeline.rate_limiter.set_rate(rate);
}

pub fn consume_rate(pipeline: &mut Pipeline, bytes: u64) {
    pipeline.rate_limiter.consume(bytes);
}

pub fn rate_wait(pipeline: &mut Pipeline) -> bool {
    pipeline.rate_limiter.should_wait()
}

pub fn soft_decode_pressure(pipeline: &mut Pipeline, budget: usize, pending: u64) {
    pipeline.decode_backlog_budget_bytes = budget;
    pipeline
        .metrics
        .decode_pending_bytes
        .store(pending, Ordering::Relaxed);
}

pub fn set_soft_dispatch_after(pipeline: &mut Pipeline, at: Option<Instant>) {
    pipeline.download_pressure_soft_dispatch_after = at;
}

pub fn soft_dispatch_after(pipeline: &Pipeline) -> Option<Instant> {
    pipeline.download_pressure_soft_dispatch_after
}

pub fn retry_deadline(pipeline: &Pipeline) -> Option<tokio::time::Instant> {
    pipeline.infrastructure_retries.next_deadline()
}

pub fn requeue_due_retries(pipeline: &mut Pipeline, now: tokio::time::Instant) -> usize {
    let due = pipeline.infrastructure_retries.take_due(now);
    let count = due.len();
    for retry in due {
        pipeline.receive_retry_work(retry);
    }
    count
}

pub fn probe_handout(pipeline: &mut Pipeline, server: usize) -> Vec<DownloadWork> {
    let pressure = pipeline.refresh_download_pressure();
    match pipeline.next_works(server, 1, None, pressure) {
        Handout::Works(works) => works,
        _ => Vec::new(),
    }
}

pub async fn reject_probe(pipeline: &mut Pipeline, work: DownloadWork, kind: ProbeFailure) {
    let (segment_id, retry_count, exclude_servers) =
        (work.segment_id, work.retry_count, work.exclude_servers);
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(DownloadError::fetch(kind.0, "probe failed")),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(1),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
}

pub fn parked_deadlines(
    pipeline: &Pipeline,
) -> impl Iterator<Item = (Option<tokio::time::Instant>, &DownloadWork)> {
    pipeline
        .infrastructure_retries
        .iter_with_deadlines()
        .map(|(at, retry)| (at, &retry.work))
}

#[derive(Clone, Copy, Debug)]
pub enum RetryWait {
    Ordinary,
    Probe,
}

pub fn park_timed_retry(pipeline: &mut Pipeline, job_id: JobId, wait: RetryWait, delay: Duration) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let work = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.segment_number == 0)
        .pop()
        .expect("segment 0 is queued")
        .dispatched(&state.spec);
    pipeline.note_retry_scheduled(work.segment_id);
    pipeline.schedule_infrastructure_retry(
        Some(delay),
        RetryWork {
            scheduled_pool_generation: pipeline.pool_generation,
            infrastructure_retry: true,
            hold: match wait {
                RetryWait::Ordinary => RetryHold::Ordinary,
                RetryWait::Probe => RetryHold::ProbeHold,
            },
            work,
        },
    );
}

pub fn seat_probe_round(pipeline: &mut Pipeline, id: JobId, batches: usize, now: Instant) {
    pipeline
        .post_probes
        .seat_round_for_test(id, 0, batches, now);
}

pub fn probe_answering(pipeline: &Pipeline, now: Instant) -> bool {
    pipeline.post_probes.answering(now)
}

pub fn set_manual_pause(pipeline: &Pipeline, paused: bool) {
    let all = crate::jobs::HoldOwner::All;
    if paused {
        pipeline.shared_state.hold(crate::jobs::Hold::manual(all));
    } else {
        pipeline
            .shared_state
            .release(crate::jobs::HoldOrigin::Manual, all);
    }
}
