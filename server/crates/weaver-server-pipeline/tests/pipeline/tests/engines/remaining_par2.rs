// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme};

use super::harness::{self, Post, Scenario};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;

use weaver_server_pipeline::pipeline::tests::engines::remaining_par2::{
    FRAME, META, drive_staged_short_footer,
};

const SLICE: usize = 1536;

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
    let (status, delivered) = drive_staged_short_footer(&scenario, spec, lost, bad_crc).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert!(delivered.values().any(|bytes| bytes == &body));
}
