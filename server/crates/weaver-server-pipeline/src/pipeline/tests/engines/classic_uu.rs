// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Stored RAR sets posted uuencoded: no article says where its bytes go or
// carries a checksum of its own, so placement follows post order and only
// the volumes' own checksums and a PAR2 set can see damage.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar::{delivered, posts};
use crate::pipeline::tests::direct_store::single_member_store_set;

const MEMBER: &str = "beacon.mkv";

fn body() -> Vec<u8> {
    payload(9 * ARTICLE + 21, 91)
}

fn assert_member(outcome: &Outcome, body: &[u8]) {
    assert!(
        outcome.completed(),
        "{:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    let names: Vec<String> = outcome
        .files
        .keys()
        .filter(|name| !name.ends_with(".par2"))
        .cloned()
        .collect();
    assert_eq!(names, vec![delivered(MEMBER)], "delivered names");
    assert!(outcome.files[&delivered(MEMBER)] == body, "member bytes");
}

fn with_par2(volumes: Vec<(String, Vec<u8>)>, blocks: u32) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("lantern", &files, blocks);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

#[tokio::test]
async fn a_uuencoded_rar_set_delivers_its_member() {
    let volumes = single_member_store_set(MEMBER, &body(), 3);
    let graph = run(&Scenario::new(posts(volumes)).uuencoded()).await;
    assert_member(&graph, &body());
}

#[tokio::test]
async fn a_uuencoded_rar_set_damaged_beside_par2_is_repaired() {
    let volumes = single_member_store_set(MEMBER, &body(), 3);
    let scenario = Scenario::new(with_par2(volumes, 2))
        .uuencoded()
        .damaged(1, 1);
    assert_member(&run(&scenario).await, &body());
}

// A uuencoded article says nothing of where its bytes go: every part of the
// volume behind the lost one is laid one article early, so the set carries
// a block for every article of that volume.
#[tokio::test]
async fn a_uuencoded_rar_set_missing_an_article_beside_par2_is_repaired() {
    let volumes = single_member_store_set(MEMBER, &body(), 3);
    let blocks = volumes[2].1.len().div_ceil(ARTICLE) as u32 + 1;
    let scenario = Scenario::new(with_par2(volumes, blocks))
        .uuencoded()
        .lost(2, 0);
    assert_member(&run(&scenario).await, &body());
}
