// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, Scenario, par2_volumes_sliced};
use super::recovery_audit::explicit_stream;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::{
    single_member_rar4_store_set_numbered, single_member_store_set,
};

async fn broad_repair(rar4: bool, duplicate: bool) {
    let mut state = 29_u64;
    let body: Vec<_> = (0..97_792)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let written = if rar4 {
        single_member_rar4_store_set_numbered("harbour/lantern.mkv", &body, 4)
    } else {
        single_member_store_set("harbour/lantern.mkv", &body, 4)
    };
    let affected: Vec<_> = [1, 2, 3, 0]
        .into_iter()
        .flat_map(|file| {
            (0..written[file].1.len().div_ceil(768))
                .map(move |article| (file as u32, article as u32))
        })
        .take(100)
        .collect();
    assert_eq!(affected.len(), 100);
    let slices: std::collections::BTreeSet<_> = affected
        .iter()
        .flat_map(|&(file, article)| {
            let begin = article as usize * 768;
            let end = (begin + 768).min(written[file as usize].1.len());
            (begin / 1048..end.div_ceil(1048)).map(move |slice| (file, slice))
        })
        .collect();
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let blocks = slices.len() as u32 + 10;
    let recovery = par2_volumes_sliced("archive", &descriptions, 1048, blocks, 4);
    let mut posts: Vec<_> = written
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    if !duplicate {
        for &(file, article) in &affected {
            let bytes = &mut posts[file as usize].bytes;
            let begin = article as usize * 768;
            let end = (begin + 768).min(bytes.len());
            for byte in &mut bytes[begin..end] {
                *byte ^= 0x5a;
            }
        }
    }
    let order: Vec<_> = posts
        .iter()
        .enumerate()
        .flat_map(|(file, post)| {
            (0..post.bytes.len().div_ceil(768)).map(move |article| (file as u32, article as u32))
        })
        .collect();
    posts.extend(recovery);
    let scenario = Scenario::new(posts).framed(768);
    let duplicates = if duplicate { affected.as_slice() } else { &[] };
    let outcome = explicit_stream(&scenario, &order, None, duplicates).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(outcome.files.len(), 1);
    assert!(
        outcome.files.values().any(|bytes| bytes == &body),
        "byte-exact repaired payload"
    );
}

#[tokio::test]
async fn hundred_conflicting_rar4_articles_repair_without_losing_volume_identity() {
    broad_repair(true, true).await;
}

#[tokio::test]
async fn hundred_conflicting_rar5_articles_repair_without_losing_volume_identity() {
    broad_repair(false, true).await;
}

#[tokio::test]
async fn hundred_silently_corrupt_rar4_articles_repair_as_one_coherent_batch() {
    broad_repair(true, false).await;
}

#[tokio::test]
async fn hundred_silently_corrupt_rar5_articles_repair_as_one_coherent_batch() {
    broad_repair(false, false).await;
}

#[tokio::test]
async fn a_clean_recovery_check_does_not_reopen_an_intrinsically_invalid_archive() {
    let mut written = single_member_store_set("entry.bin", b"payload", 1);
    let payload = written[0]
        .1
        .windows(7)
        .position(|bytes| bytes == b"payload")
        .unwrap();
    written[0].1[payload] ^= 1; // Recovery describes bytes that fail the member's CRC.
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let recovery = par2_volumes_sliced("archive", &descriptions, 1048, 1, 1);
    let mut posts: Vec<_> = written
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    posts.extend(recovery);
    let scenario = Scenario::new(posts).framed(768);
    let order: Vec<_> = (0..scenario.posts[1].bytes.len().div_ceil(768))
        .map(|article| (1, article as u32))
        .chain([(0, 0)])
        .collect();
    let outcome = explicit_stream(&scenario, &order, None, &[]).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        outcome.graph.delivery
    );
    assert!(outcome.files.is_empty());
    assert!(
        outcome.graph.rulings.iter().any(|(_, ruling)| matches!(
            ruling,
            Some(weaver_graph::recovery::RepairOutcome::Clean)
        )),
        "the invalid bytes themselves pass their independent recovery checks"
    );
}
