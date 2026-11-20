// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, JOB, Scenario};
use crate::pipeline::tests::{
    insert_active_job, job_status_for_assert, new_direct_pipeline_with_backlog,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::{JobSpec, JobStatus};
use std::collections::BTreeMap;

pub const FRAME: usize = 3072;
pub const META: usize = 768;

pub async fn drive_staged_short_footer(
    scenario: &Scenario,
    spec: JobSpec,
    lost: usize,
    bad_crc: usize,
) -> (Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
    let temp = tempfile::tempdir().unwrap();
    let backlog_base_bytes = 16 * 1024 * 1024;
    let (mut pipeline, _, complete) =
        new_direct_pipeline_with_backlog(&temp, backlog_base_bytes, 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, spec).await;
    loop {
        if harness::finished(&pipeline) {
            break;
        }
        // Finish the intact first/fourth volumes and index before asking
        // for the missing middle volume and the remaining source/recovery.
        loop {
            let queue = &mut pipeline
                .jobs
                .get_mut(&JOB)
                .unwrap()
                .articles
                .active_mut()
                .unwrap()
                .download_queue;
            let mut queued = Vec::new();
            while let Some(work) = queue.pop() {
                queued.push(work);
            }
            if queued.is_empty() {
                break;
            }
            let first = [0, 3, 4]
                .into_iter()
                .find_map(|file| {
                    queued
                        .iter()
                        .position(|work| work.segment_id.file_id.file_index == file)
                })
                .unwrap_or(0);
            let work = queued.remove(first);
            for work in queued {
                queue.push(work);
            }
            let id = work.segment_id;
            let (file, ordinal) = (id.file_id.file_index, id.segment_number);
            if file == 1 || (file == 2 && ordinal as usize == lost) {
                pipeline.book_failed_segment(id);
                pipeline.maybe_finish_download_pass(JOB);
            } else {
                let post = &scenario.posts[file as usize];
                let size = if file < 4 { FRAME } else { META };
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
                if file == 3 && ordinal as usize == bad_crc {
                    let at = encoded
                        .windows(7)
                        .rposition(|part| part == b"pcrc32=")
                        .unwrap()
                        + 7;
                    let crc = !par2_rs::checksum::crc32(bytes);
                    encoded[at..at + 8].copy_from_slice(format!("{crc:08x}").as_bytes());
                }
                let mut raw = b"222 article follows\r\n".to_vec();
                for line in encoded.split_inclusive(|byte| *byte == b'\n') {
                    if line.first() == Some(&b'.') {
                        raw.push(b'.');
                    }
                    raw.extend_from_slice(line);
                }
                raw.extend_from_slice(b".\r\n");
                super::wire_recovery::decode_wire_article(&mut pipeline, id, &raw).await;
            }
            harness::hear_probe(&mut pipeline).await;
            if harness::finished(&pipeline) {
                break;
            }
        }
        if harness::finished(&pipeline) {
            break;
        }
        pipeline.check_job_completion(JOB).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if harness::finished(&pipeline) {
            break;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let queued = pipeline
            .jobs
            .get(&JOB)
            .is_some_and(|job| !job.articles.active().unwrap().download_queue.is_empty());
        assert!(queued, "the missing source must remain repairable");
    }
    let status = job_status_for_assert(&pipeline, JOB);
    let mut delivered = BTreeMap::new();
    harness::collect(&complete, &complete, &mut delivered);
    retire_pipeline_database(pipeline).await;
    (status, delivered)
}
