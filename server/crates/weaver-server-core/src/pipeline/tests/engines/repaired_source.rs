// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, MemberId, NodeId};
use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};

use super::super::{
    checkpoint_plan, insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use super::harness::{self, JOB, Post, Scenario, par2_volumes_sliced};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::engine::Heard;
use crate::pipeline::{DecodeDone, DownloadPayload, Pipeline, SegmentSource};

#[tokio::test]
async fn periodic_copy7z_repair_preserves_archive_before_extraction() {
    let body: Vec<u8> = (0..6001)
        .map(|index| {
            let index = index as u64;
            13_u64
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(index.wrapping_mul(31))
                .rotate_left((index % 13) as u32) as u8
        })
        .collect();
    assert_eq!(body[..256], body[3328..3584]);
    let mut archive = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    archive.set_encrypt_header(false);
    let mut entry = ArchiveEntry::new_file("payload.bin");
    entry.size = body.len() as u64;
    archive
        .push_archive_entry(entry, Some(Cursor::new(body.clone())))
        .unwrap();
    let archive = archive.finish().unwrap().into_inner();
    assert_eq!(&archive[32..32 + body.len()], body.as_slice());
    let article = archive.len().div_ceil(4);
    let recovery = par2_volumes_sliced(
        "copy",
        &[("copy.7z", &archive)],
        256,
        archive.len().div_ceil(256) as u32,
        1,
    );
    let carrier: Vec<u8> = recovery.into_iter().flat_map(|post| post.bytes).collect();
    let scenario = Scenario::new(vec![
        Post::plain("copy.7z", archive.clone()),
        Post::plain("copy.par2", carrier.clone()),
    ])
    .framed(carrier.len())
    .lost(0, 0);
    // One whole metadata carrier, alongside four source articles.
    let mut spec = scenario.spec();
    spec.files[0].segments = harness::spec("copy", &scenario.posts[..1], article).files[0]
        .segments
        .clone();
    assert_eq!(spec.files[0].segments.len(), 4);
    assert_eq!(spec.files[1].segments.len(), 1);
    let temp = tempfile::tempdir().unwrap();
    let buffers = crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    };
    let (mut pipeline, _, complete) = new_direct_pipeline_with_buffers(&temp, buffers, 1).await;
    scenario.prepare(&mut pipeline);
    insert_active_job(&mut pipeline, JOB, spec).await;
    let mut observed = false;
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
            let size = if file == 0 { article } else { carrier.len() };
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
            let mut decoder = weaver_nntp::fused_yenc::FusedYencArticleDecoder::new();
            decoder.set_checkpoint_plan(checkpoint_plan(&pipeline, JOB));
            let decoded = decoder
                .decode_available(&mut bytes::BytesMut::from(raw.as_slice()))
                .unwrap()
                .unwrap();
            let trace = DecodedBodyTrace {
                attempts: Vec::new(),
                result: Ok(DecodedBody {
                    raw_size: raw.len() as u32,
                    decoded: decoded.chunks,
                    body: decoded.body,
                    cpu: Default::default(),
                    io: Default::default(),
                }),
            };
            let (payload, _, _) = Pipeline::download_data_from_decoded_trace(id, trace);
            let Ok(DownloadPayload::Decoded(result)) = payload else {
                panic!("valid yEnc article must decode");
            };
            pipeline.note_decode_started(id, raw.len() as u64);
            pipeline
                .handle_decode_done(DecodeDone::Success {
                    result,
                    source: SegmentSource {
                        source_server_idx: Some(0),
                        exclude_servers: Vec::new(),
                    },
                })
                .await;
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
                let job = &pipeline.graph_jobs[&JOB];
                assert!(job.current_repair(&said));
                assert_eq!(said.node, returned.set);
                assert!(!observed, "one source repair is expected");
                let member = MemberId(0);
                let mut rebuilt = vec![0; archive.len()];
                let mut covered = vec![false; archive.len()];
                // Only recorded, readable source spans and actual staged writes
                // initialize the reconstruction; expected bytes are an oracle only.
                for run in job.namespace().get(member).unwrap().coverage.runs() {
                    let range =
                        ByteRange::new(run.range.start, run.range.end.min(archive.len() as u64));
                    let mut bytes = Vec::new();
                    assert!(job.writer().access(job.tree()).read_at(
                        NodeId::ROOT,
                        member,
                        range,
                        &mut bytes
                    ));
                    let span = range.start as usize..range.end as usize;
                    rebuilt[span.clone()].copy_from_slice(&bytes);
                    covered[span].fill(true);
                }
                for (written, offset, bytes) in returned.repaired.slices() {
                    assert_eq!(written, member);
                    let bytes = bytes.unwrap();
                    let span = offset as usize..offset as usize + bytes.len();
                    rebuilt[span.clone()].copy_from_slice(&bytes);
                    covered[span].fill(true);
                }
                assert!(covered.into_iter().all(|covered| covered));
                assert_eq!(
                    rebuilt, archive,
                    "repair must reconstruct the original archive before acknowledgment"
                );
                assert_eq!(
                    par2_rs::checksum::crc32(&rebuilt[12..32]),
                    u32::from_le_bytes(rebuilt[8..12].try_into().unwrap())
                );
                observed = true;
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
    assert!(observed, "the matching successful repair must be inspected");
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let mut delivered = BTreeMap::new();
    harness::collect(&complete, &complete, &mut delivered);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered.values().next().unwrap(), &body);
    retire_pipeline_database(pipeline).await;
}
