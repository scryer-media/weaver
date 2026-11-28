// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::MessageId;

pub struct LaneGuard {
    pipeline: Pipeline,
    lease: Option<DownloadBatchLease>,
}

pub enum LaneEvent {
    Obsolete {
        job: JobId,
        work_count: usize,
    },
    Refused(String),
    Complete {
        fetched: Vec<u32>,
        unrequested: Vec<u32>,
        ack: Option<std::sync::mpsc::SyncSender<()>>,
    },
}

fn lane_event(event: OwnedDownloadLaneEvent) -> LaneEvent {
    match event {
        OwnedDownloadLaneEvent::LeaseObsolete { lease } => LaneEvent::Obsolete {
            job: lease.job_id,
            work_count: lease.works.len(),
        },
        OwnedDownloadLaneEvent::AcquireFailed { error, .. } => {
            LaneEvent::Refused(format!("{error:?}"))
        }
        OwnedDownloadLaneEvent::BatchComplete {
            results,
            unrequested_works,
            ack,
            ..
        } => LaneEvent::Complete {
            fetched: results
                .iter()
                .map(|result| result.segment_id.segment_number)
                .collect(),
            unrequested: unrequested_works
                .iter()
                .map(|work| work.segment_id.segment_number)
                .collect(),
            ack,
        },
    }
}

pub enum LaneProgress {
    Event(LaneEvent),
    RefillDeclined(bool),
    Parked,
}

impl LaneGuard {
    pub fn spawn(&mut self) {
        self.pipeline
            .spawn_download_batch(self.lease.take().unwrap());
    }

    pub fn stop_downloads(&self) {
        self.pipeline.shared_state.queue.write(|inner| {
            inner
                .globals
                .holds
                .hold(crate::jobs::Hold::manual(crate::jobs::HoldOwner::All));
            crate::pipeline::queue::Change::changed(())
        });
    }

    pub async fn next_event(&mut self) -> LaneEvent {
        lane_event(
            self.pipeline
                .owned_download_lane_event_rx
                .recv()
                .await
                .unwrap(),
        )
    }

    pub async fn decline_refill_or_hear_lane(&mut self) -> LaneProgress {
        tokio::select! {
            Some(event) = self.pipeline.owned_download_lane_event_rx.recv() => LaneProgress::Event(lane_event(event)),
            Some(request) = self.pipeline.download_refill_rx.recv() => {
                LaneProgress::RefillDeclined(request.response_tx.send(DownloadLaneRefillResponse {
                    lease: None,
                    park_reason: LaneParkReason::NoWork,
                }).is_ok())
            }
            Some(_) = self.pipeline.download_lane_parked_rx.recv() => LaneProgress::Parked,
        }
    }

    pub fn no_leases_after_reset(&self) -> bool {
        self.pipeline
            .owned_download_lane_pool
            .reset()
            .leases
            .is_empty()
    }

    pub fn client(&self) -> &weaver_nntp::NntpClient {
        &self.pipeline.nntp
    }
}

// A pipeline on the fixture alone, with one active job of `segments`
// articles, and the lease a dispatch would cut for all of them.
pub async fn lane_guard(temp_dir: &TempDir, port: u16, job_id: JobId, segments: u32) -> LaneGuard {
    let (mut pipeline, _, _) = audited_pipeline(temp_dir).await;
    let mut server = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        ..Default::default()
    });
    server.max_connections = 1;
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server],
            max_idle_age: Duration::from_mins(5),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(3600),
        },
    ));
    pipeline.tuner.set_connection_limit(1);
    pipeline.owned_download_lane_pool.resize(1);
    let spec = segmented_job_spec("lane-guards", "payload.bin", &vec![512; segments as usize]);
    pipeline
        .add_job(
            job_id,
            spec.clone(),
            PathBuf::from("lane-guards.nzb"),
            nzb_zstd_for(&spec),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    activate_job(&mut pipeline, job_id).await;
    let works = (0..segments)
        .map(|segment_number| DownloadWork {
            counters: None,
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            },
            message_id: MessageId::new(&format!("guard-{segment_number}@example.invalid")),
            groups: Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 0,
            byte_estimate: 512,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            avoid_server: None,
        })
        .collect();
    let lease = DownloadBatchLease {
        body_activity: Arc::default(),
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
        works,
    };
    LaneGuard {
        pipeline,
        lease: Some(lease),
    }
}
