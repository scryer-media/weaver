// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set posted twice under the graph engine: each copy is numbered as the
// volume it repeats, padded differently. A copy is more articles of the
// volume it repeats, so the set is one set and its member one file, and
// an article one copy lost the other brings.

use crate::pipeline::tests::direct_store::{
    single_member_rar4_store_set_numbered, single_member_store_set,
};

use super::harness::{ARTICLE, Claim, Outcome, Post, Scenario, par2_set, payload, run};
use super::restart::{restart_row, run_restarting};
use super::sevenz::{container, split};

const MEMBER: &str = "beacon.bin";

fn body() -> Vec<u8> {
    payload(7 * ARTICLE + 13, 51)
}

// The volumes, then each again under its number padded otherwise.
fn twice(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let copies: Vec<(String, Vec<u8>)> = volumes
        .iter()
        .map(|(name, bytes)| (name.replace(".part0", ".part"), bytes.clone()))
        .collect();
    volumes
        .into_iter()
        .chain(copies)
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

fn assert_one_member(graph: &Outcome, body: &[u8]) {
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let name = format!("lantern beacon/{MEMBER}");
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![name.clone()],
        "delivered names"
    );
    assert!(graph.files[&name] == body, "member bytes");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

#[tokio::test]
async fn a_rar5_set_posted_twice_is_one_set() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_store_set(MEMBER, &body, 2)));
    assert_one_member(&run(&scenario).await, &body);
}

#[tokio::test]
async fn a_rar5_set_posted_twice_served_last_article_first_is_one_set() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_store_set(MEMBER, &body, 2))).backwards();
    assert_one_member(&run(&scenario).await, &body);
}

#[tokio::test]
async fn a_rar4_set_posted_twice_fetched_copy_first_is_one_set() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_rar4_store_set_numbered(
        MEMBER, &body, 2,
    )))
    .together(&[3, 2, 1, 0]);
    assert_one_member(&run(&scenario).await, &body);
}

// No recovery set is posted: the copy's article is the only one that
// brings the bytes the first copy lost.
#[tokio::test]
async fn an_article_one_copy_lost_the_other_brings() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_store_set(MEMBER, &body, 2))).lost(0, 2);
    let graph = run(&scenario).await;
    assert_one_member(&graph, &body);
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete),
        "{:?}",
        graph.graph().claims
    );
}

// An article damaged in the first copy is mended by the copy's whole one,
// with no recovery set: clean bytes replace damaged ones in the store.
#[tokio::test]
async fn an_article_one_copy_damaged_the_other_brings_whole() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_store_set(MEMBER, &body, 2))).damaged(0, 2);
    assert_one_member(&run(&scenario).await, &body);
}

// The same for a stored RAR4 set, damaged in the copy fetched second.
#[tokio::test]
async fn a_rar4_article_one_copy_damaged_the_other_brings_whole() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_rar4_store_set_numbered(
        MEMBER, &body, 2,
    )))
    .damaged(2, 2);
    assert_one_member(&run(&scenario).await, &body);
}

// The same, the copy lost where the first copy is whole, and the first
// copy lost where the copy is whole.
#[tokio::test]
async fn articles_each_copy_lost_the_other_brings() {
    let body = body();
    let scenario = Scenario::new(twice(single_member_store_set(MEMBER, &body, 2)))
        .lost(2, 1)
        .lost(1, 2);
    assert_one_member(&run(&scenario).await, &body);
}

// Every copy under a name of its own that says nothing: the headers place
// each, and a copy placed where a volume is held is that volume.
#[tokio::test]
async fn a_rar5_set_posted_twice_under_names_that_say_nothing_is_one_set() {
    let body = body();
    let posts = twice(single_member_store_set(MEMBER, &body, 2))
        .into_iter()
        .enumerate()
        .map(|(index, post)| {
            Post::plain(
                &format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                post.bytes,
            )
        })
        .collect();
    assert_one_member(&run(&Scenario::new(posts)).await, &body);
}

// The same for a stored RAR4 set, whose fronts state no volume number: each
// copy is placed by the members it continues.
#[tokio::test]
async fn a_rar4_set_posted_twice_under_names_that_say_nothing_is_one_set() {
    let body = body();
    let posts = twice(single_member_rar4_store_set_numbered(MEMBER, &body, 2))
        .into_iter()
        .enumerate()
        .map(|(index, post)| {
            Post::plain(
                &format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                post.bytes,
            )
        })
        .collect();
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        graph.completed(),
        "{:?} {:?} {:#?}",
        graph.status,
        graph.failure,
        graph.graph
    );
    assert_one_member(&graph, &body);
}

// A recovery set over all four postings: an article damaged in both copies
// is repaired, the member still one file.
#[tokio::test]
async fn an_article_damaged_in_both_copies_is_repaired() {
    let body = body();
    let mut posts = twice(single_member_store_set(MEMBER, &body, 2));
    let files: Vec<(String, Vec<u8>)> = posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    posts.extend(par2_set("lantern", &named, 2));
    let scenario = Scenario::new(posts).damaged(0, 2).damaged(2, 2);
    assert_one_member(&run(&scenario).await, &body);
}

// A set posted twice, restarted after every article: the restored tree
// is the one folded, and nothing on disk is fetched again.
#[tokio::test]
async fn a_set_posted_twice_restarted_after_every_article_is_one_set() {
    let body = body();
    restart_row(Scenario::new(twice(single_member_store_set(MEMBER, &body, 2))).lost(0, 2)).await;
}

// The same, each volume fetched beside its copy, a turn of each at a time:
// a volume's own posting ends before a restart and its copy's after it, so
// the restored job ends the own posting again and the member closes with
// its copy.
#[tokio::test]
async fn a_set_posted_twice_fetched_side_by_side_closes_each_member_across_restarts() {
    let body = body();
    let scenario =
        Scenario::new(twice(single_member_store_set(MEMBER, &body, 2))).together(&[0, 2, 1, 3]);
    let whole = run(&scenario).await;
    let mut copies_closed = 0;
    let restarted = run_restarting(&scenario, &mut |rows| {
        for copy in rows.members.iter().filter(|row| row.closed) {
            let Some(into) = copy.copy_of else {
                continue;
            };
            let member = rows
                .members
                .iter()
                .find(|row| row.node == copy.node && row.member == into)
                .expect("a copy's member is checkpointed");
            assert!(
                member.closed,
                "{} ended, {} is open",
                copy.name, member.name
            );
            copies_closed += 1;
        }
    })
    .await;
    assert!(copies_closed > 0, "no checkpoint holds an ended copy");
    assert_eq!(restarted.status, whole.status, "status");
    assert!(restarted.files == whole.files, "delivered files");
    assert_one_member(&whole, &body);
}

// A split 7z posted twice: `.7z.001` and `.7z.0001` are one piece.
#[tokio::test]
async fn a_split_sevenz_posted_twice_is_one_set() {
    let archive = container(
        sevenz_turbo::EncoderConfiguration::new(sevenz_turbo::EncoderMethod::COPY),
        None,
    );
    let pieces = split(&archive, 2);
    let copies: Vec<Post> = pieces
        .iter()
        .map(|post| Post::plain(&post.name.replace(".7z.00", ".7z.000"), post.bytes.clone()))
        .collect();
    let scenario = Scenario::new(pieces.into_iter().chain(copies).collect());
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let names: Vec<String> = graph.files.keys().cloned().collect();
    assert_eq!(
        names,
        vec![
            "lantern beacon/ember.bin".to_string(),
            "lantern beacon/quill.bin".to_string()
        ]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}
