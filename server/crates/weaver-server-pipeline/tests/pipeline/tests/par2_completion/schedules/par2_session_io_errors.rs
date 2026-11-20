// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repairs that rebuild whole files, sets over many files, clean sets that
// never fetch their recovery, and archives inside a set's cover.

use sevenz_turbo::EncoderMethod;

use super::{Expect, articles, covered, lose_all, row};
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::engines::harness::{ARTICLE, Post, Scenario, par2_volumes, payload};

// Every article of the payload missing on every server: the set rebuilds it
// whole from its volumes.
#[tokio::test]
async fn an_entirely_missing_payload_is_reconstructed() {
    let body = payload(5 * ARTICLE + 30, 151);
    row(
        &lose_all(Scenario::new(covered(&[("lantern.bin", &body)], 6, 2)), 0),
        Expect::Complete(&[("lantern.bin", &body)]),
        "payload missing",
    )
    .await;
}

// One set over forty files, five of them damaged: every file comes through
// whole however many the set holds open.
#[tokio::test]
async fn a_repair_over_a_large_multifile_set_completes() {
    let bodies: Vec<(String, Vec<u8>)> = (0..40u8)
        .map(|i| {
            (
                format!("file-{i:02}.bin"),
                payload(2 * ARTICLE + usize::from(i), i),
            )
        })
        .collect();
    let files: Vec<(&str, &[u8])> = bodies
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut scenario = Scenario::new(covered(&files, 6, 2));
    for file in [3u32, 11, 19, 27, 35] {
        scenario = scenario.damaged(file, 1);
    }
    row(&scenario, Expect::Complete(&files), "five of forty damaged").await;
}

// A clean set completes from what its articles proved in flight: none of
// its recovery is ever asked for.
#[tokio::test]
async fn a_clean_set_completes_with_its_recovery_left_parked() {
    let body = payload(6 * ARTICLE, 152);
    let posts = covered(&[("lantern.bin", &body)], 3, 2);
    let volumes: Vec<u32> = (2..posts.len() as u32).collect();
    let outcome = row(
        &Scenario::new(posts),
        Expect::Complete(&[("lantern.bin", &body)]),
        "clean",
    )
    .await;
    assert_eq!(outcome.reads, 0, "a clean set reads none of its files back");
    for (file, ordinal) in outcome.requests.keys() {
        assert!(
            !volumes.contains(file) || *ordinal == 0,
            "a clean set fetched recovery article {file}:{ordinal}"
        );
    }
}

// A damaged payload whose set arrives first: the description is in hand
// before any source article, and the restored job reads it back from disk.
#[tokio::test]
async fn a_set_read_before_its_payload_is_restored_from_disk() {
    let body = payload(6 * ARTICLE, 153);
    let posts = covered(&[("lantern.bin", &body)], 2, 1);
    let index: Vec<(u32, u32)> = (0..articles(&posts[1]))
        .map(|ordinal| (1, ordinal))
        .collect();
    row(
        &Scenario::new(posts).damaged(0, 4).first(&index),
        Expect::Complete(&[("lantern.bin", &body)]),
        "index first",
    )
    .await;
}

// A stored RAR set under PAR2 with a damaged volume: the set repairs the
// volume and the member is delivered.
#[tokio::test]
async fn a_damaged_rar_volume_under_par2_is_repaired_and_extracted() {
    let member = payload(9 * ARTICLE + 40, 154);
    let volumes = single_member_store_set("ember.bin", &member, 3);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes("silver.horizon", &described, 3, 1));
    row(
        &Scenario::new(posts).damaged(1, 2),
        Expect::Complete(&[("ember.bin", &member)]),
        "damaged RAR volume",
    )
    .await;
}

// A single stored 7z under PAR2 with a damaged article: the set repairs
// the archive and its member is delivered.
#[tokio::test]
async fn a_corrupt_single_sevenz_is_repaired_before_it_extracts() {
    let member = payload(8 * ARTICLE + 3, 155);
    let archive = build_7z_shaped(
        &[Entry::file("ember.bin", member.clone())],
        EncoderMethod::COPY,
        None,
        false,
    );
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.7z", &archive)], 2, 1));
    row(
        &Scenario::new(posts).damaged(0, 3),
        Expect::Complete(&[("ember.bin", &member)]),
        "damaged 7z",
    )
    .await;
}

// Copy-only repair: a file posted whole under another name is put back
// under its own without a recovery block.
#[tokio::test]
async fn a_rename_only_repair_needs_no_recovery_blocks() {
    let body = payload(5 * ARTICLE, 156);
    let mut posts = covered(&[("lantern.bin", &body)], 1, 1);
    posts[0] = Post::plain("f9e8d7c6b5", body.clone());
    let scenario = lose_all(Scenario::new(posts), 2);
    row(
        &scenario,
        Expect::Complete(&[("lantern.bin", &body)]),
        "renamed, no volume",
    )
    .await;
}
