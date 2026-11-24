// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use super::harness::{Post, Scenario};
use super::recovery_audit::explicit_stream;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;

const ARTICLE: usize = 768;
const SLICE: usize = 1048;
const PASSWORD: &str = "lantern-quay";

fn parity(files: &[(String, Vec<u8>)], blocks: u32) -> Vec<Post> {
    use par2_rs::create::{
        BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount, VolumeScheme,
    };
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("protected");
    std::fs::create_dir_all(&base).unwrap();
    let paths = files
        .iter()
        .map(|(name, bytes)| {
            let path = base.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let output = directory.path().join("out");
    std::fs::create_dir_all(&output).unwrap();
    let mut options =
        Par2CreatorOptions::with_output(output.join("silver.horizon.par2"), Some(base), paths);
    options.block_sizing = BlockSizing::Bytes(SLICE as u64);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_scheme = VolumeScheme::Uniform;
    options.volume_count = Some(4);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    created
        .output_paths
        .iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

async fn encrypted_repair(missing: bool) {
    let mut state = 29_u64;
    let payload: Vec<_> = (0..97_792)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let written = encrypted_store_set(
        "harbour/lantern.mkv",
        &payload,
        4,
        PASSWORD,
        Some(PASSWORD),
        false,
    );
    let mut posts: Vec<_> = written
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let affected: Vec<_> = if missing {
        let middle = posts[0].bytes.len().div_ceil(ARTICLE) / 2;
        vec![(0_u32, middle as u32)]
    } else {
        // Four conflicting transport-valid articles in each of three
        // encrypted parts, including their first cipher blocks.
        (0..3)
            .flat_map(|file| (0..4).map(move |article| (file, article)))
            .collect()
    };
    let mut touched = BTreeSet::new();
    for &(file, article) in &affected {
        let begin = article as usize * ARTICLE;
        let end = (begin + ARTICLE).min(posts[file as usize].bytes.len());
        touched.extend((begin / SLICE..end.div_ceil(SLICE)).map(|slice| (file, slice)));
        if missing {
            for byte in &mut posts[file as usize].bytes[begin..end] {
                *byte ^= 0x5a;
            }
        }
    }
    if missing {
        touched.extend((0..posts[2].bytes.len().div_ceil(SLICE)).map(|slice| (2, slice)));
    }
    let blocks = touched.len() as u32 + (touched.len() as u32 / 10).max(2);
    let recovery = parity(&written, blocks);
    let batches: &[usize] = if missing {
        &[0, 0, 1, 2, 3]
    } else {
        &[2, 0, 1, 1, 3]
    };
    let order: Vec<_> = batches
        .iter()
        .flat_map(|&file| {
            (0..posts[file].bytes.len().div_ceil(ARTICLE))
                .map(move |article| (file as u32, article as u32))
        })
        .collect();
    let lost = if missing {
        (0..posts[2].bytes.len().div_ceil(ARTICLE))
            .map(|article| (2, article as u32))
            .collect()
    } else {
        Vec::new()
    };
    posts.extend(recovery);
    let mut scenario = Scenario::new(posts).framed(ARTICLE).password(PASSWORD);
    scenario.lost = lost;
    let conflicting = if missing {
        &[][..]
    } else {
        affected.as_slice()
    };
    let outcome = explicit_stream(&scenario, &order, None, conflicting).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(outcome.files.len(), 1);
    assert!(
        outcome.files.values().any(|bytes| bytes == &payload),
        "byte-exact decrypted payload"
    );
}

#[tokio::test]
async fn an_encrypted_missing_piece_and_silent_damage_are_repaired_together() {
    encrypted_repair(true).await;
}

#[tokio::test]
async fn encrypted_conflicting_copies_do_not_retain_an_earlier_cipher_checksum() {
    encrypted_repair(false).await;
}
