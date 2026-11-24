// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::persistence::ArchiveNews;

pub struct BusyTurns {
    pipeline: Pipeline,
    intermediate: PathBuf,
    complete: PathBuf,
}

pub struct ResultStream(mpsc::Sender<DownloadResult>);

impl ResultStream {
    pub async fn fail(&self, segment_id: SegmentId) {
        self.0
            .send(DownloadResult {
                lane_id: 0,
                job_id: segment_id.file_id.job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::fetch(
                    DownloadFailureKind::EstablishedTransport,
                    "busy turn test",
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: weaver_nntp::ServerSet::EMPTY,
                release_connection_slot: false,
            })
            .await
            .unwrap();
    }

    pub fn is_empty(&self) -> bool {
        self.0.capacity() == self.0.max_capacity()
    }
}

pub struct ArchiveReplies(mpsc::UnboundedSender<db_reply::DbReply>);

impl ArchiveReplies {
    pub fn more(&self) {
        self.0
            .send(db_reply::DbReply::Archive(ArchiveNews::More { from: 0 }))
            .unwrap();
    }
}

pub struct StorageRoom(Arc<crate::operations::StorageCapacity>);

impl StorageRoom {
    pub fn waited_paths(&self) -> usize {
        self.0.waited_paths()
    }
}

impl BusyTurns {
    pub async fn new(temp: &TempDir) -> Self {
        let (pipeline, intermediate, complete) = audited_pipeline(temp).await;
        Self {
            pipeline,
            intermediate,
            complete,
        }
    }

    pub fn hold_segments(&mut self, segments: &[SegmentId]) {
        super::results::hold_on_test_lane(&mut self.pipeline, segments);
    }

    pub fn result_stream(&self) -> ResultStream {
        ResultStream(self.pipeline.download_done_tx.clone())
    }

    pub fn archive_replies(&self) -> ArchiveReplies {
        ArchiveReplies(self.pipeline.db_reply_tx.clone())
    }

    pub fn storage_room(&self) -> StorageRoom {
        StorageRoom(self.pipeline.storage_capacity.clone())
    }

    pub fn metrics(&self) -> Arc<PipelineMetrics> {
        self.pipeline.metrics.clone()
    }

    pub fn add_moving_job(&mut self, id: JobId, name: &str) {
        self.pipeline.jobs.insert(
            id,
            minimal_job_state(id, name, self.intermediate.join(name)),
        );
        self.pipeline.job_order.push(id);
        self.pipeline.report_all_for_test();
    }

    pub async fn finish_move(&self, id: JobId, name: &str) {
        self.pipeline
            .move_done_tx
            .send(MoveToCompleteDone {
                job_id: id,
                dest: self.complete.join(name),
                result: Ok(MoveToCompleteResult {
                    moved_entries: 0,
                    renamed_members: Vec::new(),
                }),
            })
            .await
            .unwrap();
    }

    pub async fn fail_decode(&self, segment_id: SegmentId, raw_size: u64) {
        self.pipeline
            .decode_done_tx
            .send(DecodeDone::Failed {
                segment_id,
                raw_size,
                error: "busy turn test".to_owned(),
                servers: PoolServers::stamped(
                    self.pipeline.pool_generation,
                    None,
                    weaver_nntp::ServerSet::EMPTY,
                ),
            })
            .await
            .unwrap();
    }

    pub fn archive_kept(&self, id: JobId) {
        self.pipeline
            .db_reply_tx
            .send(db_reply::DbReply::Archive(ArchiveNews::Kept(id)))
            .unwrap();
    }

    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle::new(
            self.pipeline.event_tx.clone(),
            self.pipeline.shared_state.clone(),
        )
    }

    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.pipeline.run().await })
    }
}
