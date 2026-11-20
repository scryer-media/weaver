// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, Post, Scenario, par2_volumes_sliced};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use std::io::Cursor;
use weaver_graph::MemberId;
use weaver_server_composition::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::repaired_source::drive_periodic_repair;

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
    let mut observed = false;
    let (status, delivered) =
        drive_periodic_repair(&scenario, spec, article, archive.len(), |repair| {
            assert!(repair.current);
            assert_eq!(repair.node, repair.set);
            assert!(!observed, "one source repair is expected");
            let member = MemberId(0);
            let mut rebuilt = vec![0; archive.len()];
            let mut covered = vec![false; archive.len()];
            // Only recorded, readable source spans and actual staged writes
            // initialize the reconstruction; expected bytes are an oracle only.
            for source in repair.source {
                assert!(source.readable);
                let range = source.range;
                let bytes = source.bytes;
                let span = range.start as usize..range.end as usize;
                rebuilt[span.clone()].copy_from_slice(&bytes);
                covered[span].fill(true);
            }
            for (written, offset, bytes) in repair.staged {
                assert_eq!(written, member);
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
        })
        .await;
    assert!(observed, "the matching successful repair must be inspected");
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered.values().next().unwrap(), &body);
}
