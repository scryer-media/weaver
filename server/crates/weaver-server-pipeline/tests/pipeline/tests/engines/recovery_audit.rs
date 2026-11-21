// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, Scenario, par2_set, par3_set, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::single_member_store_set;

#[tokio::test]
async fn a_complete_signatureless_numbered_post_keeps_its_joined_bytes() {
    let body = payload(8193, 71);
    let scenario = Scenario::new(vec![Post::plain(
        "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.001",
        body.clone(),
    )]);
    let outcome = run(&scenario).await;
    assert_eq!(outcome.status, Some(JobStatus::Complete));
    assert_eq!(outcome.files.len(), 1);
    assert_eq!(
        outcome.files.keys().next().unwrap(),
        "lantern beacon/5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41"
    );
    assert!(
        outcome.files.values().next() == Some(&body),
        "byte-exact joined posting"
    );
}

async fn silent_damage_is_reextracted(par3: bool, front: bool, backwards: bool) {
    let body = payload(97_792, 13);
    let archive = build_7z_shaped(
        &[Entry::file("lantern.mkv", body.clone())],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let written = split_volumes(&archive, 4);
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let sidecars = if par3 {
        par3_set("archive", &descriptions, 8)
    } else {
        par2_set("archive", &descriptions, 8)
    };
    let mut posts: Vec<_> = written
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    // The transport checksum is valid for this altered posting. Only the
    // archive and recovery metadata know that the member bytes are wrong.
    if front {
        posts[0].bytes[..64]
            .iter_mut()
            .for_each(|byte| *byte ^= 0x80);
    } else {
        posts[1].bytes[767] ^= 0x80;
    }
    posts.extend(sidecars);
    let mut scenario = Scenario::new(posts).framed(768);
    if backwards {
        scenario = scenario.backwards();
    }
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "backwards={backwards}, delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(outcome.files.len(), 1);
    assert!(
        outcome.files.values().any(|bytes| bytes == &body),
        "byte-exact re-extracted member"
    );
}

// The same 7z, posted with a byte already wrong when the recovery set was
// made over it, so the set proves the wrong byte as posted, and with a
// further byte flipped in transit that the set repairs. The set proves
// every piece, and the member is checked again on what it proved: it still
// fails its own checksum, so it stays damaged.
#[tokio::test]
async fn a_sevenz_member_wrong_as_posted_stays_damaged_after_its_set_repairs_the_rest() {
    for par3 in [false, true] {
        for backwards in [false, true] {
            let body = payload(97_792, 13);
            let archive = build_7z_shaped(
                &[Entry::file("lantern.mkv", body.clone())],
                sevenz_turbo::EncoderMethod::COPY,
                None,
                false,
            );
            let mut written = split_volumes(&archive, 4);
            written[2].1[300] ^= 0x80;
            let descriptions: Vec<_> = written
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
                .collect();
            let sidecars = if par3 {
                par3_set("archive", &descriptions, 8)
            } else {
                par2_set("archive", &descriptions, 8)
            };
            let mut posts: Vec<_> = written
                .into_iter()
                .map(|(name, bytes)| Post::plain(&name, bytes))
                .collect();
            posts[1].bytes[767] ^= 0x80;
            posts.extend(sidecars);
            let mut scenario = Scenario::new(posts).framed(768);
            if backwards {
                scenario = scenario.backwards();
            }
            let outcome = run(&scenario).await;
            let context = format!(
                "par3={par3}, backwards={backwards}, delivery={:?}, rulings={:?}",
                outcome.graph.delivery, outcome.graph.rulings
            );
            assert_ne!(outcome.status, Some(JobStatus::Complete), "{context}");
            assert!(
                outcome.graph.delivery.damaged == ["lantern.mkv"],
                "{context}"
            );
            assert!(
                !outcome.files.values().any(|bytes| bytes == &body),
                "{context}"
            );
        }
    }
}

#[tokio::test]
async fn silently_corrupt_fragmented_sevenz_is_reextracted_after_par2() {
    silent_damage_is_reextracted(false, false, true).await;
}

#[tokio::test]
async fn silently_corrupt_fragmented_sevenz_posted_in_order_is_reextracted_after_par2() {
    silent_damage_is_reextracted(false, false, false).await;
}

#[tokio::test]
async fn silently_corrupt_fragmented_sevenz_is_reextracted_after_par3() {
    silent_damage_is_reextracted(true, false, true).await;
}

#[tokio::test]
async fn silently_corrupt_fragmented_sevenz_posted_in_order_is_reextracted_after_par3() {
    silent_damage_is_reextracted(true, false, false).await;
}

#[tokio::test]
async fn silently_corrupt_sevenz_front_is_reidentified_after_par2() {
    silent_damage_is_reextracted(false, true, true).await;
}

#[tokio::test]
async fn silently_corrupt_sevenz_front_is_reidentified_after_par3() {
    silent_damage_is_reextracted(true, true, true).await;
}

// A three-volume RAR5 set posted under numbered names that start above one,
// the interior volume's tail never served, and a PAR2 set that names the
// volumes by what they are. The numbered names claim nothing the volumes'
// own headers deny: the RAR store owns all three by their signatures, the
// PAR2 that covers them by name rebuilds the short one, and the member
// extracts whole, whichever way the articles arrive.
async fn numbered_rar5_set_short_interior_volume(backwards: bool) {
    let bytes = payload(70_001, 29);
    let written = single_member_store_set("nested/feature.mkv", &bytes, 3);
    let named: Vec<_> = written
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("archive.part{}.rar", index + 1), bytes.clone()))
        .collect();
    let frame = named
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(2))
        .max()
        .unwrap();
    let descriptions: Vec<_> = named
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<_> = named
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            Post::plain(
                &format!("51273aad56a8b904e96928935278a627.{}", 101 + index),
                bytes.clone(),
            )
        })
        .collect();
    let slice = (named
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4)
    .max(256);
    let blocks = named
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(slice) as u32)
        .sum();
    posts.extend(super::harness::par2_volumes_sliced(
        "archive.rar",
        &descriptions,
        slice as u64,
        blocks,
        1,
    ));
    let mut scenario = Scenario::new(posts).framed(frame).lost(1, 1);
    if backwards {
        scenario = scenario.backwards();
    }
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}, names={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings,
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        outcome.files.len(),
        1,
        "only the extracted member is delivered: {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert!(
        outcome.files.values().any(|file| file == &bytes),
        "byte-exact extracted member"
    );
}

#[tokio::test]
async fn a_numbered_rar5_set_short_its_interior_tail_is_repaired_by_par2() {
    numbered_rar5_set_short_interior_volume(false).await;
}

#[tokio::test]
async fn a_numbered_rar5_set_short_its_interior_tail_is_repaired_by_par2_backwards() {
    numbered_rar5_set_short_interior_volume(true).await;
}
