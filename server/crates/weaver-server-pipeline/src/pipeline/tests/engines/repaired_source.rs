// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, JOB, Scenario};
use crate::pipeline::graph::namespace::Returned;
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_backlog,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::pipeline::{
    Pipeline,
    graph::engine::{Heard, Said},
};
use crate::{JobSpec, JobStatus};
use std::collections::BTreeMap;
use weaver_graph::{ByteRange, MemberId, NodeId, recovery::RepairOutcome};

pub struct SourceSpan {
    pub range: ByteRange,
    pub readable: bool,
    pub bytes: Vec<u8>,
}

pub struct RepairObservation {
    pub current: bool,
    pub node: NodeId,
    pub set: NodeId,
    pub source: Vec<SourceSpan>,
    pub staged: Vec<(MemberId, u64, bytes::Bytes)>,
}

impl RepairObservation {
    fn read(pipeline: &Pipeline, said: &Said, archive_len: usize, returned: &Returned) -> Self {
        let job = &pipeline.graph_jobs[&JOB];
        let member = MemberId(0);
        let source = job
            .namespace()
            .get(member)
            .unwrap()
            .coverage
            .runs()
            .map(|run| {
                let range = ByteRange::new(run.range.start, run.range.end.min(archive_len as u64));
                let mut bytes = Vec::new();
                let readable = job.writer().access(job.tree()).read_at(
                    NodeId::ROOT,
                    member,
                    range,
                    &mut bytes,
                );
                SourceSpan {
                    range,
                    readable,
                    bytes,
                }
            })
            .collect();
        Self {
            current: job.current_repair(said),
            node: said.node,
            set: returned.set,
            source,
            staged: returned
                .repaired
                .slices()
                .map(|(member, offset, bytes)| (member, offset, bytes.unwrap()))
                .collect(),
        }
    }
}

pub async fn drive_periodic_repair(
    scenario: &Scenario,
    spec: JobSpec,
    article: usize,
    archive_len: usize,
    mut inspect: impl FnMut(RepairObservation),
) -> (Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
    let temp = tempfile::tempdir().unwrap();
    let backlog_base_bytes = 16 * 1024 * 1024;
    let (mut pipeline, _, complete) =
        new_direct_pipeline_with_backlog(&temp, backlog_base_bytes, 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, spec).await;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let id = work.segment_id;
            let file = id.file_id.file_index;
            let ordinal = id.segment_number;
            if (file, ordinal) == (0, 0) {
                pipeline.book_failed_segment(id);
                continue;
            }
            let post = &scenario.posts[file as usize];
            let size = if file == 0 {
                article
            } else {
                scenario.posts[1].bytes.len()
            };
            let bytes = post.articles(size).nth(ordinal as usize).unwrap();
            let offset = ordinal as usize * size;
            let mut encoded = Vec::new();
            weaver_yenc::encode_part(
                bytes,
                &mut encoded,
                128,
                &post.name,
                ordinal + 1,
                post.articles(size).count() as u32,
                offset as u64 + 1,
                (offset + bytes.len()) as u64,
                post.bytes.len() as u64,
            )
            .unwrap();
            let mut raw = b"222 article follows\r\n".to_vec();
            for line in encoded.split_inclusive(|byte| *byte == b'\n') {
                if line.first() == Some(&b'.') {
                    raw.push(b'.');
                }
                raw.extend_from_slice(line);
            }
            raw.extend_from_slice(b".\r\n");
            super::wire_recovery::decode_wire_article(&mut pipeline, id, &raw).await;
            harness::hear_probe(&mut pipeline).await;
        }
        if harness::finished(&pipeline) {
            break;
        }
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            continue;
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
                .expect("a live worker speaks");
            if let Heard::Repaired(returned) = &said.heard
                && matches!(returned.outcome, Some(RepairOutcome::Repaired { .. }))
                && returned.namespace == NodeId::ROOT
            {
                inspect(RepairObservation::read(
                    &pipeline,
                    &said,
                    archive_len,
                    returned,
                ));
            }
            pipeline.graph_run_said(said);
            continue;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        assert!(
            harness::finished(&pipeline),
            "no queued or running work may stall the job"
        );
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut delivered = BTreeMap::new();
    harness::collect(&complete, &complete, &mut delivered);
    retire_pipeline_database(pipeline).await;
    (status, delivered)
}
