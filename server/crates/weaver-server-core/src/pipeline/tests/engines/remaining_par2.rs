// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme};
use weaver_nntp::client::{DecodedBody, DecodedBodyTrace};

use super::harness::{self, JOB, Post, Scenario};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::{
    checkpoint_plan, insert_active_job, job_status_for_assert, new_direct_pipeline_with_buffers,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::pipeline::{DecodeDone, DownloadPayload, Pipeline, SegmentSource};

const FRAME: usize = 3072;
const SLICE: usize = 1536;
const META: usize = 768;

fn fixture() -> (Scenario, Vec<u8>, usize, usize) {
    let mut state = 31_u64;
    let body: Vec<_> = (0..300 * SLICE - 256)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let volumes = single_member_store_set("harbour/lantern.mkv", &body, 4);
    let tail = volumes[1].1.len() / SLICE * SLICE;
    assert_eq!(volumes[1].1.len() - tail, 3);
    assert!(
        [0, 2, 3]
            .into_iter()
            .any(|source| { volumes[source].1.get(tail..) == Some(&volumes[1].1[tail..]) }),
        "the absent volume's short end-marker slice has an intact donor"
    );
    let lost = volumes[2].1.len().div_ceil(FRAME) / 2;
    let bad_crc = volumes[3].1.len().div_ceil(FRAME) / 2;
    let scratch = tempfile::tempdir().unwrap();
    let paths = volumes
        .iter()
        .map(|(name, bytes)| {
            let path = scratch.path().join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let mut options = Par2CreatorOptions::with_output(
        scratch.path().join("silver.horizon.par2"),
        Some(scratch.path().to_path_buf()),
        paths,
    );
    options.block_sizing = BlockSizing::Bytes(SLICE as u64);
    options.recovery_amount =
        RecoveryAmount::Count((volumes[1].1.len().div_ceil(SLICE) + 5) as u32);
    options.volume_count = Some(4);
    options.volume_scheme = VolumeScheme::Uniform;
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let mut recovery: Vec<_> = created
        .output_paths
        .iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    recovery.sort_by(|a, b| a.name.cmp(&b.name));
    let mut seen = BTreeSet::new();
    let mut descriptions = BTreeMap::<[u8; 16], Vec<u8>>::new();
    for post in &mut recovery {
        let mut kept = Vec::new();
        let mut at = 0;
        while at < post.bytes.len() {
            assert_eq!(&post.bytes[at..at + 8], b"PAR2\0PKT");
            let len = u64::from_le_bytes(post.bytes[at + 8..at + 16].try_into().unwrap()) as usize;
            let packet = &post.bytes[at..at + len];
            let kind = &packet[48..64];
            if kind == b"PAR 2.0\0FileDesc" || kind == b"PAR 2.0\0IFSC\0\0\0\0" {
                if seen.insert(packet.to_vec()) {
                    descriptions
                        .entry(packet[64..80].try_into().unwrap())
                        .or_default()
                        .extend_from_slice(packet);
                }
            } else {
                kept.extend_from_slice(packet);
            }
            at += len;
        }
        post.bytes = kept;
    }
    assert_eq!(descriptions.len(), 4);
    for (index, packets) in descriptions.into_values().enumerate() {
        recovery[index].bytes.extend(packets);
    }
    let mut posts: Vec<_> = volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    posts.extend(recovery);
    (Scenario::new(posts).framed(META), body, lost, bad_crc)
}

#[tokio::test]
async fn split_metadata_repairs_missing_rar_with_a_staged_short_footer_donor() {
    let (scenario, body, lost, bad_crc) = fixture();
    let mut spec = scenario.spec();
    let data_spec = harness::spec("source", &scenario.posts[..4], FRAME);
    for (file, data) in spec.files.iter_mut().zip(data_spec.files) {
        file.segments = data.segments;
    }
    let temp = tempfile::tempdir().unwrap();
    let buffers = crate::runtime::buffers::BufferPoolConfig {
        small_count: 8,
        medium_count: 4,
        large_count: 2,
    };
    let (mut pipeline, _, complete) = new_direct_pipeline_with_buffers(&temp, buffers, 1).await;
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
                    panic!("valid yEnc");
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
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let mut delivered = BTreeMap::new();
    harness::collect(&complete, &complete, &mut delivered);
    assert!(delivered.values().any(|bytes| bytes == &body));
    retire_pipeline_database(pipeline).await;
}
