// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, JOB};
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use crate::jobs::model::{DownloadState, PostState, RunState};
use crate::jobs::queries::JobsQueries;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{pump_pipeline_runtime_queues, submit_decoded_segment_declaring};
use weaver_graph::{ByteRange, MemberId, NodeId, VolumeIndex};
use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};
use weaver_server_disk::operations::disk::volume::LoopSite;

async fn turn(pipeline: &mut Pipeline) {
    pipeline.hear_graph_runs();
    while let Some(job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(job).await;
    }
    pump_pipeline_runtime_queues(pipeline).await;
}

pub async fn article(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    file: u32,
    ordinal: u32,
    parts: usize,
    lost: Option<(u32, u32)>,
) {
    article_in(pipeline, posts, file, ordinal, parts, lost, posts.len() - 1).await;
}

pub async fn article_in(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    file: u32,
    ordinal: u32,
    parts: usize,
    lost: Option<(u32, u32)>,
    data_files: usize,
) {
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    };
    let Some(active) = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|job| job.articles.active_mut())
    else {
        return;
    };
    for queue in [&mut active.download_queue, &mut active.recovery_queue] {
        for work in queue.drain_all() {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
    if lost == Some((file, ordinal)) {
        pipeline.book_failed_segment(id);
    } else {
        let (name, bytes) = &posts[file as usize];
        let data = (file as usize) < data_files;
        let cut = if data {
            bytes.len().div_ceil(parts)
        } else {
            bytes.len()
        };
        let start = (ordinal as usize * cut).min(bytes.len());
        let end = (start + cut).min(bytes.len());
        // The ordinary direct-success fixture states true data lengths;
        // its one-article PAR2 carrier inherits the encoded NZB length.
        let declared = if data {
            bytes.len()
        } else {
            bytes.len() + bytes.len().div_ceil(32) + 2
        };
        submit_decoded_segment_declaring(
            pipeline,
            id.file_id,
            ordinal,
            start as u64,
            &bytes[start..end],
            name,
            None,
            true,
            None,
            declared as u64,
        )
        .await;
    }
    turn(pipeline).await;
}

pub async fn settle(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    parts: usize,
    lost: Option<(u32, u32)>,
) {
    settle_in(pipeline, posts, parts, lost, posts.len() - 1).await;
}

pub async fn settle_in(
    pipeline: &mut Pipeline,
    posts: &[(String, Vec<u8>)],
    parts: usize,
    lost: Option<(u32, u32)>,
    data_files: usize,
) {
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|job| job.articles.active_mut()?.download_queue.pop())
        {
            article_in(
                pipeline,
                posts,
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
                parts,
                lost,
                data_files,
            )
            .await;
        }
        if harness::finished(pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
        if harness::finished(pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline
                .graph_runs
                .recv()
                .await
                .expect("owned work returns");
            pipeline.graph_run_said(said);
        } else if harness::take_retry(pipeline).await {
            continue;
        } else {
            assert!(
                pipeline.jobs.get(&JOB).is_some_and(|job| !job
                    .articles
                    .active()
                    .unwrap()
                    .download_queue
                    .is_empty()),
                "no queued work or worker owns progress"
            );
        }
    }
}

pub fn volume_owner(pipeline: &Pipeline, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
    pipeline.graph_jobs[&JOB]
        .tree()
        .child_of(NodeId::ROOT, member)
}

pub fn node_kind(pipeline: &Pipeline, node: NodeId) -> String {
    pipeline.graph_jobs[&JOB]
        .tree()
        .kind(node)
        .unwrap()
        .to_string()
}

pub fn read_root_member(
    pipeline: &Pipeline,
    member: MemberId,
    extent: ByteRange,
    bytes: &mut Vec<u8>,
) -> bool {
    let graph = &pipeline.graph_jobs[&JOB];
    graph
        .writer()
        .access(graph.tree(), LoopSite::ReadOn)
        .read_at(NodeId::ROOT, member, extent, bytes)
}

#[derive(Debug)]
pub struct SavedCheckpoint(crate::jobs::graph_checkpoint::GraphCheckpoint);

#[derive(Debug)]
pub struct SavedEnvelope {
    pub start: i64,
    pub end: i64,
    pub name: String,
}

impl SavedCheckpoint {
    pub fn owner(&self, node: NodeId) -> Option<(&str, Option<i64>)> {
        let row = self
            .0
            .nodes
            .iter()
            .find(|row| row.node == i64::from(node.0))?;
        Some((row.kind.as_str(), row.parent))
    }

    pub fn envelopes(&self, node: NodeId) -> Vec<SavedEnvelope> {
        self.0
            .envelopes
            .iter()
            .filter(|row| row.node == i64::from(node.0))
            .map(|row| {
                let name = row.file.clone().unwrap_or_else(|| {
                    crate::pipeline::graph::envelope::envelope_name(node, row.volume as u32)
                });
                SavedEnvelope {
                    start: row.start,
                    end: row.end,
                    name,
                }
            })
            .collect()
    }

    pub fn root_member_debug(&self, member: MemberId) -> String {
        format!(
            "{:?}",
            self.0
                .members
                .iter()
                .filter(|row| row.node == 0 && row.member == i64::from(member.0))
                .collect::<Vec<_>>()
        )
    }
}

pub fn saved_checkpoint(pipeline: &Pipeline) -> Option<SavedCheckpoint> {
    pipeline
        .db
        .load_graph_checkpoint(JOB)
        .unwrap()
        .map(SavedCheckpoint)
}

pub async fn restore_from_database(pipeline: &mut Pipeline, spec: crate::JobSpec) {
    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&JOB)
        .unwrap();
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: JOB,
            job_hash: recovered.nzb_hash,
            spec,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            status: crate::jobs::model::job_status_from_persisted_str(
                &recovered.status,
                recovered.error.as_deref(),
            ),
            download_state: recovered
                .download_state
                .as_deref()
                .and_then(DownloadState::parse),
            post_state: recovered.post_state.as_deref().and_then(PostState::parse),
            run_state: recovered.run_state.as_deref().and_then(RunState::parse),
            queued_repair_at_epoch_ms: recovered.queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms: recovered.queued_extract_at_epoch_ms,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
            restart: None,
        })
        .await
        .unwrap();
}
