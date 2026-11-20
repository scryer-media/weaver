// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::lanes::Pressure;
use super::owned_pool::FailureKind;
use super::*;
use crate::MessageId;

pub const PIN_TRIAL_CLEAN_BATCHES: u8 =
    crate::pipeline::download::transport::PIN_TRIAL_CLEAN_BATCHES;
fn declared_index(
    par3: bool,
) -> [(
    weaver_graph::MemberId,
    crate::pipeline::graph::metadata::RecoveryFormat,
); 1] {
    [(
        weaver_graph::MemberId(0),
        if par3 {
            crate::pipeline::graph::metadata::RecoveryFormat::Par3
        } else {
            crate::pipeline::graph::metadata::RecoveryFormat::Par2
        },
    )]
}

// A recovery index of one article and a payload file of seven.
pub fn index_and_payload_spec(par3: bool) -> JobSpec {
    {
        let payload = (0..(7 * 128))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let index = build_test_par2_index_for_files(&[("payload.mkv", &payload)], 64);
        let payload_segments = (0..7)
            .map(|number| {
                segment_spec! {
                    number: number,
                    bytes: 128,
                    message_id: format!("graph-payload-{number}@example.com"),
                }
            })
            .collect::<Vec<_>>();
        JobSpec {
            name: "Graph PAR2 Bootstrap".to_string(),
            password: None,
            total_bytes: (index.len() + payload.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: if par3 {
                        "00-index.par3"
                    } else {
                        "00-index.par2"
                    }
                    .to_string(),
                    role: if par3 {
                        FileRole::Par3 { is_index: true }
                    } else {
                        FileRole::Par2 {
                            is_index: true,
                            recovery_block_count: 0,
                        }
                    },
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: index.len() as u32,
                        message_id: "graph-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: "payload.mkv".to_string(),
                    role: FileRole::Standalone,
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: payload_segments,
                },
            ],
        }
    }
}

// A pipelined BODY batch that ended with its connection torn down.
fn unclean_batch_result(job_id: JobId, segment_number: u32) -> DownloadResult {
    DownloadResult {
        lane_id: 0,
        job_id,
        runtime_generation: 0,
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number,
        },
        data: Err(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::EstablishedTransport,
            "connection closed",
        ))),
        attempts: Vec::new(),
        lane_observation: Some(DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 2 },
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: Duration::ZERO,
            pressure_clear: true,
            fed: false,
            batch_complete: true,
            batch_clean: false,
            unresolved_count: 1,
            connection_discarded: true,
            answered_pipelined: true,
        }),
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

// A complete depth-2 batch from lane 0 that closed clean. `pipelined`: the
// server answered on it with a later request already on the wire.
fn clean_batch_result(job_id: JobId, segment_number: u32, pipelined: bool) -> DownloadResult {
    let mut result = unclean_batch_result(job_id, segment_number);
    result.data = Ok(DownloadPayload::Raw(Bytes::from_static(b"article")));
    let observation = result.lane_observation.as_mut().unwrap();
    observation.payload_bytes = 7;
    observation.policy_elapsed = Duration::from_millis(25);
    observation.answered_pipelined = pipelined;
    observation.batch_clean = true;
    observation.unresolved_count = 0;
    observation.connection_discarded = false;
    result
}

impl DispatchFixture {
    pub async fn fetch_raw_part(&mut self, segment_id: SegmentId, data: Bytes) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Ok(DownloadPayload::Raw(data)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: true,
            })
            .await;
    }

    pub async fn return_failed_result(
        &mut self,
        segment_id: SegmentId,
        kind: FailureKind,
        message: &str,
        retry_count: u32,
        exclude_servers: weaver_nntp::ServerSet,
        release_connection_slot: bool,
    ) {
        self.0
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::fetch(kind.0, message)),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count,
                exclude_servers,
                release_connection_slot,
            })
            .await;
    }

    pub async fn run_pending_completion_checks(&mut self) {
        while let Some(queued_job) = self.0.pending_completion_checks.pop_front() {
            self.0.check_job_completion(queued_job).await;
        }
    }

    pub fn maybe_finish_download_pass(&mut self, id: JobId) {
        self.0.maybe_finish_download_pass(id);
    }

    pub fn seat_carried_article(&mut self, segment_id: SegmentId) -> u64 {
        let job_id = segment_id.file_id.job_id;
        let lane_id = Pipeline::next_download_lane_id();
        self.0.download_lane_owners.insert(
            lane_id,
            DownloadLaneOwner {
                body_activity: Vec::new(),
                job_id,
                mode: DownloadLaneMode::Sequential,
                completion_critical: false,
                server_idx: Some(0),
                connection: true,
                outstanding: HashMap::from([(
                    segment_id,
                    DownloadWork {
                        counters: None,
                        segment_id,
                        message_id: MessageId::new("carried-article@example.com"),
                        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
                        priority: 0,
                        byte_estimate: 128,
                        retry_count: 0,
                        is_recovery: false,
                        completion_critical: false,
                        exclude_servers: weaver_nntp::ServerSet::EMPTY,
                        avoid_server: None,
                    },
                )]),
                outstanding_bytes: 128,
            },
        );
        self.hold_segments(&[segment_id]);
        self.set_active_connections(1);
        self.set_job_active_connections(job_id, 1);
        lane_id
    }

    pub fn release_carried_article(&mut self, lane_id: u64, segment_id: SegmentId) -> bool {
        let job_id = segment_id.file_id.job_id;
        self.0.release_download_result(&DownloadResult {
            lane_id,
            job_id,
            runtime_generation: self.0.pool_generation,
            segment_id,
            data: Ok(DownloadPayload::Raw(Bytes::from_static(b"carried-article"))),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        })
    }

    pub fn egress_quota_park(&self, id: SegmentId) -> Option<u32> {
        self.0.egress_quota_parked.get(&id).copied()
    }

    pub fn wait_for_egress_capacity(
        &self,
        changes: tokio::sync::watch::Receiver<u64>,
        runtime: Arc<crate::proxies::ProxyRuntime>,
        rejection: weaver_nntp::transfer::QuotaRejection,
        excludes: weaver_nntp::ServerSet,
    ) -> impl std::future::Future<Output = ()> + use<> {
        Pipeline::wait_for_egress_quota_capacity(
            changes,
            runtime,
            self.0.nntp.clone(),
            rejection,
            excludes,
        )
    }

    pub fn take_index_off_graph(&mut self, id: JobId, temporarily_detached: bool) {
        let lost = |graph: &mut crate::pipeline::graph::job::GraphJob| {
            graph
                .lost(
                    weaver_graph::VolumeIndex(0),
                    0,
                    &mut crate::pipeline::graph::status::RepairSlots::default(),
                    0,
                )
                .unwrap();
        };
        if temporarily_detached {
            let mut graph = self.0.graph_jobs.remove(&id).unwrap();
            lost(&mut graph);
            self.0.graph_jobs.insert(id, graph);
        } else {
            lost(self.0.graph_jobs.get_mut(&id).unwrap());
        }
    }

    pub fn graph_indexes(&self, id: JobId, par3: bool) -> Option<Vec<weaver_graph::MemberId>> {
        self.0.graph_jobs[&id].indexes_to_come(declared_index(par3))
    }

    pub fn cached_graph_indexes(&mut self, id: JobId, par3: bool) -> Option<Arc<[u32]>> {
        self.0.graph_jobs.indexes_to_come(&id, declared_index(par3))
    }

    pub fn seed_explorer_depth(&mut self, depth: u8) {
        self.0.download_lane_runtime.servers.insert(
            0,
            crate::pipeline::download::transport::ServerPipelineExplorer::seeded(
                Some(depth),
                None,
                None,
            ),
        );
    }

    pub fn explorer_depth_and_pin(&self) -> (u8, bool) {
        let explorer = self.0.download_lane_runtime.servers.get(&0).unwrap();
        (explorer.current_depth(), explorer.pinned_sequential())
    }

    pub fn set_job_intent(&self, id: JobId, paused: Option<bool>, cancelled: bool) {
        self.0.shared_state.queue.write(|inner| {
            let row = inner.jobs.iter_mut().find(|row| row.id == id).unwrap();
            row.intents.paused = paused;
            row.intents.cancelled =
                cancelled.then_some(crate::jobs::handle::CancellationOrigin::User);
            crate::pipeline::queue::Change::changed(())
        });
    }

    pub fn queue_shutting_down(&self, value: bool) {
        self.0.shared_state.queue.write(|inner| {
            inner.globals.shutting_down = value;
            crate::pipeline::queue::Change::changed(())
        });
    }

    pub fn queue_manual_hold(&self, pause: bool) {
        self.0.shared_state.queue.write(|inner| {
            if pause {
                inner
                    .globals
                    .holds
                    .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
            } else {
                inner
                    .globals
                    .holds
                    .release(crate::jobs::HoldOrigin::Manual, crate::jobs::HoldOwner::All);
            }
            crate::pipeline::queue::Change::changed(())
        });
    }

    pub fn unclean_batch_is_server_fault(&self, id: JobId) -> bool {
        self.0.unclean_batch_is_server_fault(id)
    }

    pub fn seat_pin_trial(&mut self) {
        self.0.download_lane_runtime.servers.insert(
            0,
            crate::pipeline::download::transport::ServerPipelineExplorer::on_a_lapsed_pin_trial(
                Instant::now(),
            ),
        );
    }

    pub fn pin_trial_state(&self) -> (bool, Option<u64>, u8, bool) {
        let explorer = self.0.download_lane_runtime.servers.get(&0).unwrap();
        (
            explorer.on_pin_trial(),
            explorer.pin_trial_lane(),
            explorer.current_depth(),
            explorer.pinned_sequential(),
        )
    }

    pub fn trial_pipelined_depth(&mut self, lane_id: u64, pressure: Pressure) -> Option<u8> {
        match self
            .0
            .download_lane_mode_for_lane(0, pressure.0, true, lane_id)
        {
            DownloadLaneMode::Sequential => None,
            DownloadLaneMode::Pipelined { depth } => Some(depth),
        }
    }

    pub fn release_trial_failure(&mut self, id: JobId, ordinal: u32, pipelined: bool) {
        let mut result = unclean_batch_result(id, ordinal);
        result.lane_observation.as_mut().unwrap().answered_pipelined = pipelined;
        self.0.release_download_result(&result);
    }

    pub fn release_trial_success(&mut self, id: JobId, ordinal: u32, pipelined: bool, done: bool) {
        let mut result = clean_batch_result(id, ordinal, pipelined);
        result.release_connection_slot = done;
        self.0.release_download_result(&result);
    }

    pub fn dispatch_hold_reason(&self, id: JobId) -> Option<&'static str> {
        self.0.job_dispatch_hold_reason(id)
    }

    pub fn report_and_read_dispatch_wait(&self, id: JobId) -> Option<String> {
        self.0.job_report(id).unwrap().download_wait_reason
    }

    pub fn report_changed_dispatch_holds(&mut self) {
        self.0.report_changed_dispatch_holds();
    }

    pub fn queue_dispatch_report(&self, id: JobId) -> Option<JobInfo> {
        self.0
            .shared_state
            .queue
            .read(|inner| inner.job(id).map(|row| row.info(&inner.globals)))
    }

    pub fn clear_job_connections(&mut self, id: JobId) {
        self.0.active_download_connections_by_job.remove(&id);
    }
}
