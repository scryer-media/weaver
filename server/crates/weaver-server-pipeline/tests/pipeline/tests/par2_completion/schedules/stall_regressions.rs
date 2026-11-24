// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Jobs that once stopped moving: recovery that runs out in one volume and
// must come from the next, a repair beside an archive that already
// extracted, and names that must be put back as the set repairs. Every row
// must finish; a run that stops moving fails it.

use super::{Expect, covered, row};
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::engines::harness::{
    ARTICLE, Post, Scenario, one_block_article, par2_volumes, payload,
};

fn volumes_of(set: Vec<(String, Vec<u8>)>, rename: Option<&str>) -> Vec<(String, Vec<u8>)> {
    set.into_iter()
        .map(|(name, bytes)| match rename {
            Some(base) => (name.replace("silver.horizon", base), bytes),
            None => (name, bytes),
        })
        .collect()
}

// A RAR set whose damage needs more blocks than its first volume holds,
// with a block of that volume lost: the set reaches into the next volume
// and the member is delivered.
#[tokio::test]
async fn exhausted_rar_recovery_reaches_the_next_volume() {
    let member = payload(9 * ARTICLE + 40, 161);
    let volumes = volumes_of(single_member_store_set("ember.bin", &member, 3), None);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes("silver.horizon", &described, 4, 2));
    let lost = one_block_article(&posts[4]);
    row(
        &Scenario::new(posts)
            .damaged(0, 2)
            .damaged(2, 1)
            .lost(4, lost),
        Expect::Complete(&[("ember.bin", &member)]),
        "first volume short",
    )
    .await;
}

// Two RAR sets in one job, one clean and extracted early, the other damaged
// under its PAR2 set: the repair runs beside the finished extraction and
// both members are delivered.
#[tokio::test]
async fn a_landed_recovery_repairs_beside_a_finished_extraction() {
    let ember = payload(6 * ARTICLE + 7, 162);
    let quill = payload(5 * ARTICLE + 19, 163);
    let clean = volumes_of(
        single_member_store_set("ember.bin", &ember, 2),
        Some("ember.set"),
    );
    let damaged = volumes_of(
        single_member_store_set("quill.bin", &quill, 2),
        Some("quill.set"),
    );
    let described: Vec<(&str, &[u8])> = damaged
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = clean
        .iter()
        .chain(&damaged)
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes("quill.set", &described, 2, 1));
    row(
        &Scenario::new(posts).damaged(3, 1),
        Expect::Complete(&[("ember.bin", &ember), ("quill.bin", &quill)]),
        "repair beside an extraction",
    )
    .await;
}

// Two files posted under each other's names, one of them damaged: the set
// puts the bytes back under their own names and repairs the damaged one in
// the same pass.
#[tokio::test]
async fn swapped_names_with_damage_are_placed_and_repaired() {
    let lantern = payload(4 * ARTICLE + 3, 164);
    let beacon = payload(4 * ARTICLE + 3, 165);
    let files: [(&str, &[u8]); 2] = [("lantern.bin", &lantern), ("beacon.bin", &beacon)];
    let mut posts = covered(&files, 2, 1);
    posts[0] = Post::plain("lantern.bin", beacon.clone());
    posts[1] = Post::plain("beacon.bin", lantern.clone());
    row(
        &Scenario::new(posts).damaged(0, 2),
        Expect::Complete(&files),
        "swapped and damaged",
    )
    .await;
}
