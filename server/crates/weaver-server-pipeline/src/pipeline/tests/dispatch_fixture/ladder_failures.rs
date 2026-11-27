// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub const MAX_SEGMENT_RETRIES: u32 = crate::pipeline::MAX_SEGMENT_RETRIES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentOutcome(SegmentTerminalState);

impl SegmentOutcome {
    pub const DECODE_EXHAUSTED: Self = Self(SegmentTerminalState::DecodeExhausted);
    pub const MISSING: Self = Self(SegmentTerminalState::Missing);
    pub const ABANDONED: Self = Self(SegmentTerminalState::Abandoned);
    pub const UNSERVABLE_NOT_FOUND: Self = Self(SegmentTerminalState::UnservableNotFound);
    pub const RETRIES_EXHAUSTED: Self = Self(SegmentTerminalState::RetriesExhausted);
}

// Answer `work`, handed to `server`, with `error` through the result path a
// lane's BODY reply takes, so the reply's own classification decides.
async fn answer_nntp(
    pipeline: &mut Pipeline,
    server: usize,
    work: DownloadWork,
    error: weaver_nntp::NntpError,
) {
    let (job_id, segment_id) = (work.segment_id.file_id.job_id, work.segment_id);
    let (retry_count, exclude_servers) = (work.retry_count, work.exclude_servers);
    pipeline.hold_test_lane_works(vec![work], false);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: pipeline.pool_generation,
            segment_id,
            data: Err(DownloadError::from_nntp(error)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(server),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count,
            exclude_servers,
            release_connection_slot: true,
        })
        .await;
}

// Puts the retry `server`'s 503 scheduled back on the queue, as the retry
// timer does when it fires.
fn requeue_infrastructure_retry(pipeline: &mut Pipeline) {
    let due = pipeline.infrastructure_retries.next_deadline().unwrap();
    let mut ready = pipeline.infrastructure_retries.take_due(due);
    assert_eq!(ready.len(), 1);
    pipeline.receive_retry_work(ready.pop().unwrap());
}

fn body_reply(code: u16, message: &str) -> weaver_nntp::NntpError {
    weaver_nntp::NntpError::from_status(weaver_nntp::StatusCode::new(code), message)
}

// Answers 503 to the article on `server` until it spends its tries there,
// while other articles keep downloading, so the fault is the article's own.
// Returns how many 503s the server gave.
async fn answer_503_until_spent(pipeline: &mut Pipeline, server: usize) -> u32 {
    let mut answers = 0;
    loop {
        let Some(work) = super::ladder::ask(pipeline, server, 1).pop() else {
            return answers;
        };
        assert!(answers < 3 + MAX_SEGMENT_RETRIES, "tries never spent");
        // Other articles download between this article's faults.
        pipeline
            .metrics
            .segments_downloaded
            .fetch_add(1, Ordering::Relaxed);
        answer_nntp(
            pipeline,
            server,
            work,
            body_reply(503, "service unavailable"),
        )
        .await;
        answers += 1;
        if pipeline.infrastructure_retries.next_deadline().is_some() {
            requeue_infrastructure_retry(pipeline);
        }
    }
}

impl DispatchFixture {
    pub async fn group_acquire_failed(
        &mut self,
        job_id: JobId,
        work: DownloadWork,
    ) -> Vec<Option<usize>> {
        let pipeline = &mut self.0;
        pipeline.hold_test_lane_works(vec![work.clone()], false);
        let lease = DownloadBatchLease {
            body_activity: std::sync::Arc::default(),
            lane_id: 0,
            job_id,
            runtime_generation: pipeline.pool_generation,
            lane_mode: DownloadLaneMode::Sequential,
            server_modes: Vec::new(),
            completion_critical: false,
            effective_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            dial_exclude_servers: weaver_nntp::ServerSet::EMPTY,
            checkpoint_plans: Default::default(),
            pressure_clear: true,
            repeated: None,
            works: vec![work],
        };
        pipeline.active_download_connections = 1;
        pipeline
            .active_download_connections_by_job
            .insert(job_id, 1);
        pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
        let mut pending = VecDeque::new();
        pipeline.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::AcquireFailed {
                lease,
                error: weaver_nntp::client::BlockingBodyLaneAcquireError::GroupsNotCarried {
                    server: weaver_nntp::ServerId(0),
                    error: weaver_nntp::NntpError::NoSuchGroup,
                },
            },
            &mut pending,
        );
        let sources = pending
            .iter()
            .map(|result| result.source_server_idx)
            .collect();
        for result in pending {
            pipeline.process_released_download_done(result).await;
        }
        sources
    }

    pub async fn answer_503_until_spent(&mut self, server: usize) -> u32 {
        answer_503_until_spent(&mut self.0, server).await
    }
    pub fn book_outcome(&mut self, segment: SegmentId, outcome: SegmentOutcome) -> bool {
        self.0.book_terminal_segment(segment, outcome.0)
    }
    pub fn segment_outcomes(&self) -> impl ExactSizeIterator<Item = SegmentOutcome> + '_ {
        self.0
            .segment_terminal_states
            .values()
            .copied()
            .map(SegmentOutcome)
    }
    pub fn ended_segments(&self, id: JobId) -> Vec<(u32, u32, SegmentOutcome)> {
        let mut ended: Vec<_> = self
            .0
            .segment_terminal_states
            .iter()
            .filter(|(segment, _)| segment.file_id.job_id == id)
            .map(|(segment, state)| {
                (
                    segment.file_id.file_index,
                    segment.segment_number,
                    SegmentOutcome(*state),
                )
            })
            .collect();
        ended.sort_by_key(|(file, number, _)| (*file, *number));
        ended
    }
    pub fn segment_outcome(&self, id: SegmentId) -> Option<SegmentOutcome> {
        self.0
            .segment_terminal_states
            .get(&id)
            .copied()
            .map(SegmentOutcome)
    }
    pub fn has_terminal_segments(&self) -> bool {
        !self.0.segment_terminal_states.is_empty()
    }
    pub fn login_refusal_words(&self, id: JobId) -> Option<String> {
        self.0.login_refusal_words(id)
    }
    pub fn note_login_refusals(&mut self, id: JobId, servers: weaver_nntp::ServerSet) {
        self.0.note_login_refusals(id, servers);
    }
    pub fn login_refusal_count(&self, id: JobId) -> usize {
        self.0.login_refusals[&id].len()
    }
    pub fn fail_job_on_health(&mut self, id: JobId, error: String) {
        self.0.fail_job_on_health(id, error);
    }
    pub fn fail_job(&mut self, id: JobId, error: String) {
        self.0.fail_job(id, error);
    }
    pub fn pool_generation(&self) -> u64 {
        self.0.pool_generation
    }
    pub fn activate_nntp(&mut self, client: NntpClient, total: usize, generation: u64) {
        self.0.activate_nntp(client, total, generation);
    }
    pub async fn answer_nntp(
        &mut self,
        server: usize,
        work: DownloadWork,
        error: weaver_nntp::NntpError,
    ) {
        answer_nntp(&mut self.0, server, work, error).await;
    }
    pub fn retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.0.infrastructure_retries.next_deadline()
    }
    pub fn take_due_retry_works(&mut self, at: tokio::time::Instant) -> Vec<DownloadWork> {
        self.0
            .infrastructure_retries
            .take_due(at)
            .into_iter()
            .map(|retry| retry.work)
            .collect()
    }
    pub fn requeue_infrastructure_retry(&mut self) {
        requeue_infrastructure_retry(&mut self.0);
    }
}
