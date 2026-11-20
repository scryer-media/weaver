// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub struct LaneResults(pub(super) VecDeque<DownloadResult>);

impl LaneResults {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// Hand `segments` to lane 0 as a dispatched lease would, so the result a
// test then injects from lane 0 releases an article a lane holds.
pub(in crate::pipeline::tests) fn hold_on_test_lane(
    pipeline: &mut Pipeline,
    segments: &[SegmentId],
) {
    pipeline.hold_test_lane_works(held_works(segments), false);
}

// Ordinary payload works for `segments`, as a lease would carry them.
pub(in crate::pipeline::tests) fn held_works(segments: &[SegmentId]) -> Vec<DownloadWork> {
    segments
        .iter()
        .map(|&segment_id| DownloadWork {
            counters: None,
            segment_id,
            message_id: crate::MessageId::new(&format!(
                "held-{:?}-{}-{}@example.com",
                segment_id.file_id.job_id, segment_id.file_id.file_index, segment_id.segment_number
            )),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 0,
            byte_estimate: 1024,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            avoid_server: None,
        })
        .collect()
}

impl DispatchFixture {
    pub fn hold_segments(&mut self, segments: &[SegmentId]) {
        hold_on_test_lane(&mut self.0, segments);
    }
    pub fn hold_works(&mut self, works: Vec<DownloadWork>) {
        self.0.hold_test_lane_works(works, false);
    }
    pub fn drain_dispatched_queue(&mut self, id: JobId) -> Vec<DownloadWork> {
        let state = self.0.jobs.get_mut(&id).unwrap();
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .drain_all()
            .into_iter()
            .map(|item| item.dispatched(&state.spec))
            .collect()
    }
    pub fn retry_channel_empty(&mut self) -> bool {
        self.0.retry_rx.try_recv().is_err()
    }
    pub fn set_client(&mut self, client: NntpClient) {
        self.0.nntp = Arc::new(client);
    }
    pub fn release_unasked(&mut self, unasked: DownloadWork) -> usize {
        let pipeline = &mut self.0;
        let (event, _ack) = {
            let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
            (
                OwnedDownloadLaneEvent::BatchComplete {
                    lane_id: 0,
                    results: Vec::new(),
                    unrequested_works: vec![unasked],
                    stats: weaver_nntp::blocking::BlockingLaneStats::default(),
                    ack: Some(ack),
                },
                ack_rx,
            )
        };
        let mut pending = VecDeque::new();
        pipeline.handle_owned_download_lane_event(event, &mut pending);
        pending.len()
    }
    pub async fn missing_result(
        &mut self,
        segment_id: SegmentId,
        server: usize,
        generation: u64,
        retry_count: u32,
        exclude_servers: weaver_nntp::ServerSet,
        release: bool,
    ) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: generation,
                segment_id,
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::ArticleNotFound,
                    "430 no such article",
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: Some(server),
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count,
                exclude_servers,
                release_connection_slot: release,
            })
            .await;
    }
    pub fn has_transport_streak(&self, id: SegmentId) -> bool {
        self.0.transport_failure_streaks.contains_key(&id)
    }
    pub async fn deliver_payload(
        &mut self,
        work: DownloadWork,
        server: usize,
        filename: &str,
        payload: &[u8],
    ) {
        let job_id = work.segment_id.file_id.job_id;
        let segment_id = work.segment_id;
        let exclude_servers = work.exclude_servers;
        self.hold_works(vec![work]);
        let pipeline = &mut self.0;
        let raw = encode_article_part(filename, payload, 1, 1, 1, payload.len() as u64);
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: pipeline.pool_generation,
                segment_id,
                data: Ok(DownloadPayload::Raw(raw)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: Some(server),
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers,
                release_connection_slot: true,
            })
            .await;
        drain_decode_results(pipeline, 1).await;
        hear_graph_mail(pipeline, job_id).await;
    }

    pub async fn check_and_pump(&mut self, id: JobId) {
        self.0.check_job_completion(id).await;
        pump_pipeline_runtime_queues(&mut self.0).await;
    }
    pub async fn add_payload_job(&mut self, id: JobId, name: &str, filename: &str, len: u32) {
        let spec = standalone_job_spec(name, &[(filename.to_string(), len)]);
        self.start_pass(id, spec).await;
    }
}
