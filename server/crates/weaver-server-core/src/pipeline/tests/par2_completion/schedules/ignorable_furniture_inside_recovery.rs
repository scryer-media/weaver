// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Furniture inside a set's cover, recovery volumes that arrive in part,
// sets that arrive without their index, and jobs of more than one set.

use super::{Expect, covered, lose_all, row};
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Post, Scenario, one_block_article, par2_volumes, payload,
};

// Damaged furniture and a damaged payload under one set with blocks for
// both: the set repairs the furniture too, and both are delivered whole.
#[tokio::test]
async fn mixed_damage_with_sufficient_blocks_repairs_the_furniture_too() {
    let body = payload(6 * ARTICLE, 131);
    let notes = payload(2 * ARTICLE + 5, 132);
    let files: [(&str, &[u8]); 2] = [("lantern.bin", &body), ("lantern.nfo", &notes)];
    row(
        &Scenario::new(covered(&files, 3, 1))
            .damaged(0, 2)
            .damaged(1, 1),
        Expect::Complete(&files),
        "both damaged",
    )
    .await;
}

// The payload damaged past what the set holds while the furniture beside it
// arrives whole: the furniture is a whole file, so it is delivered and the
// job is partial; the payload is not.
#[tokio::test]
async fn mixed_damage_with_short_blocks_delivers_only_the_whole_furniture() {
    let body = payload(6 * ARTICLE, 133);
    let notes = payload(2 * ARTICLE + 5, 134);
    let files: [(&str, &[u8]); 2] = [("lantern.bin", &body), ("lantern.nfo", &notes)];
    let outcome = row(
        &Scenario::new(covered(&files, 1, 1))
            .damaged(0, 1)
            .damaged(0, 4),
        Expect::Partial(&[("lantern.nfo", &notes)]),
        "payload short",
    )
    .await;
    assert!(
        super::delivered(&outcome.files, "lantern.bin").is_none(),
        "the damaged payload is not delivered"
    );
}

// A recovery volume with one of its articles lost still brings the blocks
// the rest of it holds.
#[tokio::test]
async fn a_partial_recovery_volume_contributes_its_surviving_blocks() {
    let body = payload(6 * ARTICLE, 135);
    let posts = covered(&[("lantern.bin", &body)], 4, 1);
    let lost = one_block_article(&posts[2]);
    row(
        &Scenario::new(posts)
            .damaged(0, 1)
            .damaged(0, 3)
            .lost(2, lost),
        Expect::Complete(&[("lantern.bin", &body)]),
        "one block of four lost",
    )
    .await;
}

// A hole in the only volume too wide to leave the blocks the damage needs:
// the set rules short and the job fails as a repair.
#[tokio::test]
async fn a_hole_too_wide_to_salvage_past_still_fails() {
    let body = payload(6 * ARTICLE, 136);
    let posts = covered(&[("lantern.bin", &body)], 2, 1);
    let lost = one_block_article(&posts[2]);
    row(
        &Scenario::new(posts)
            .damaged(0, 1)
            .damaged(0, 3)
            .lost(2, lost),
        Expect::Failed(HistoryFailureKind::Repair),
        "one block of two lost against two damaged",
    )
    .await;
}

// Every recovery volume missing on every server while the payload is
// damaged: the repair cannot be fed, so the set rules short instead of
// waiting, and the job fails as a repair.
#[tokio::test]
async fn a_repair_waiting_on_recovery_that_cannot_arrive_is_failed() {
    let body = payload(6 * ARTICLE, 137);
    let scenario = lose_all(
        lose_all(
            Scenario::new(covered(&[("lantern.bin", &body)], 4, 2)).damaged(0, 2),
            2,
        ),
        3,
    );
    row(
        &scenario,
        Expect::Failed(HistoryFailureKind::Repair),
        "no volume arrives",
    )
    .await;
}

// A set posted without its index: the volumes carry the description, the
// set binds from them and repairs.
#[tokio::test]
async fn a_set_posted_without_its_index_bootstraps_from_a_volume() {
    let body = payload(6 * ARTICLE, 138);
    let mut posts = covered(&[("lantern.bin", &body)], 3, 2);
    posts.remove(1);
    row(
        &Scenario::new(posts).damaged(0, 3),
        Expect::Complete(&[("lantern.bin", &body)]),
        "no index posted",
    )
    .await;
}

// Two sets in one job, each over its own file and each with damage: each
// set's arithmetic is its own, and both repair.
#[tokio::test]
async fn recovery_arithmetic_is_strictly_isolated_per_set() {
    let ember = payload(5 * ARTICLE, 139);
    let quill = payload(4 * ARTICLE + 7, 140);
    let mut posts = vec![
        Post::plain("ember.bin", ember.clone()),
        Post::plain("quill.bin", quill.clone()),
    ];
    posts.extend(par2_volumes("ember", &[("ember.bin", &ember)], 1, 1));
    posts.extend(par2_volumes("quill", &[("quill.bin", &quill)], 2, 1));
    row(
        &Scenario::new(posts)
            .damaged(0, 1)
            .damaged(1, 0)
            .damaged(1, 3),
        Expect::Complete(&[("ember.bin", &ember), ("quill.bin", &quill)]),
        "two sets",
    )
    .await;
}

// Two sets where one comes up short: the other set's file is whole and
// delivered, the short one's is not, and the job is partial.
#[tokio::test]
async fn another_sets_blocks_never_count_toward_a_short_set() {
    let ember = payload(5 * ARTICLE, 141);
    let quill = payload(4 * ARTICLE + 7, 142);
    let mut posts = vec![
        Post::plain("ember.bin", ember.clone()),
        Post::plain("quill.bin", quill.clone()),
    ];
    posts.extend(par2_volumes("ember", &[("ember.bin", &ember)], 4, 1));
    posts.extend(par2_volumes("quill", &[("quill.bin", &quill)], 1, 1));
    let outcome = row(
        &Scenario::new(posts).damaged(1, 0).damaged(1, 3),
        Expect::Partial(&[("ember.bin", &ember)]),
        "quill short",
    )
    .await;
    // Delivered as it stands, unrepaired: not the posted bytes.
    assert!(super::delivered(&outcome.files, "quill.bin").is_some_and(|bytes| *bytes != quill));
}
