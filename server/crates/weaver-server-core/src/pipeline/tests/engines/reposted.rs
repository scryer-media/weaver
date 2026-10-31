// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set posted twice under the graph engine: each copy is numbered as the
// volume it repeats, padded differently. A copy is more articles of the
// volume it repeats, so the set is one set and its member one file, and
// an article one copy lost the other brings.

use std::collections::BTreeMap;

use crate::pipeline::tests::direct_store::{
    protected_store_set, single_member_rar4_store_set_numbered, single_member_store_set,
};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Claim, Outcome, Post, Scenario, par2_set, payload, run};
use super::restart::{restart_row, run_restarting};
use super::sevenz::{container, members, split};

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

// Each volume carries its own recovery record, and a record is its bytes
// whichever posting brought them. The first posting lost the article that
// ends the first volume's record, the copy brought it, and an article both
// postings served damaged is rebuilt from that record.
#[tokio::test]
async fn a_recovery_record_one_posting_lost_the_copy_brings() {
    let body = body();
    let posts = twice(protected_store_set(MEMBER, &body, 2, 1));
    let last = (posts[0].bytes.len().div_ceil(ARTICLE) - 1) as u32;
    let scenario = Scenario::new(posts)
        .lost(0, last)
        .damaged(0, 2)
        .damaged(2, 2);
    let graph = run(&scenario).await;
    assert_one_member(&graph, &body);
    assert!(
        graph.graph().rulings.contains(&(
            "rar_record".to_string(),
            Some(RepairOutcome::Repaired { slices: 1 })
        )),
        "{:?}",
        graph.graph().rulings
    );
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
            "lantern beacon/ember.mkv".to_string(),
            "lantern beacon/quill.mkv".to_string()
        ]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Every article of posting `file` lost.
fn lose_posting(posts: Vec<Post>, file: u32) -> Scenario {
    let articles = posts[file as usize].articles(ARTICLE).count() as u32;
    (0..articles).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(file, ordinal)
    })
}

// One posting lost whole and no recovery set: the copy numbered alike
// brings the volume and the set extracts whole. The lost posting is no file
// of its own, and the post's health counts the volume once, so the job
// completes.
#[tokio::test]
async fn a_posting_lost_whole_beside_its_copy_completes() {
    let body = body();
    let scenario = lose_posting(twice(single_member_store_set(MEMBER, &body, 2)), 0);
    assert_one_member(&run(&scenario).await, &body);
}

// The same under names that say nothing: nothing ties the lost posting to a
// volume, and beside a set that extracts whole the job completes, whatever
// the feed made of its loss.
#[tokio::test]
async fn a_posting_lost_whole_under_a_name_that_says_nothing_completes() {
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
    assert_one_member(&run(&lose_posting(posts, 0)).await, &body);
}

// Each posting one article, in order, posting `file` lost: it is lost
// before any copy comes.
fn lose_one(posts: Vec<Post>, file: u32) -> Scenario {
    lose_each(posts, &[file])
}

// The same with every posting in `files` lost.
fn lose_each(posts: Vec<Post>, files: &[u32]) -> Scenario {
    let mut scenario = Scenario::new(posts);
    scenario.article = 1 << 20;
    files
        .iter()
        .fold(scenario, |scenario, &file| scenario.lost(file, 0))
}

// A compressed set held by its engine, its second volume's first posting
// lost whole before the copy comes: the engine holds the lost posting by
// its name alone, and the copy's bytes take the place from it, as they
// would had the copy come first.
#[tokio::test]
async fn a_compressed_posting_lost_whole_yields_its_place_to_its_copy() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles"
    );
    let volumes: Vec<(String, Vec<u8>)> = (1..=4)
        .map(|part| {
            let bytes = std::fs::read(format!("{dir}/rar4_lz_four.part0{part}.rar")).unwrap();
            (format!("silver.horizon.part0{part}.rar"), bytes)
        })
        .collect();
    let graph = run(&lose_one(twice(volumes), 1)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/compressible.txt".to_string()]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A 7z coded by `method`, split in two and posted twice, posting `file`
// lost before any copy comes: the set completes with both members.
async fn assert_sevenz_completes_without(method: sevenz_turbo::EncoderMethod, file: u32) {
    let archive = container(sevenz_turbo::EncoderConfiguration::new(method), None);
    let pieces = split(&archive, 2);
    let copies: Vec<Post> = pieces
        .iter()
        .map(|post| Post::plain(&post.name.replace(".7z.00", ".7z.000"), post.bytes.clone()))
        .collect();
    let posts = pieces.into_iter().chain(copies).collect();
    let graph = run(&lose_each(posts, &[file])).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![
            "lantern beacon/ember.mkv".to_string(),
            "lantern beacon/quill.mkv".to_string()
        ]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The same of a coded 7z split in two.
#[tokio::test]
async fn a_coded_sevenz_piece_lost_whole_yields_its_place_to_its_copy() {
    assert_sevenz_completes_without(sevenz_turbo::EncoderMethod::LZMA2, 1).await;
}

// A 7z's first piece lost before anything else comes: the second piece's
// name founds a split, and the first piece's copy, which signs a 7z, founds
// the set and takes the second from it. The lost piece held its place in
// the split by its name alone, so it is a volume of neither, and the split,
// left with nothing, goes: the second piece's copy is the set's.
#[tokio::test]
async fn a_first_sevenz_piece_lost_before_anything_comes_is_no_volume() {
    assert_sevenz_completes_without(sevenz_turbo::EncoderMethod::COPY, 0).await;
}

// A 7z split in two and posted twice under names that say nothing, with a
// recovery set over all four postings and none of its blocks: the last
// piece's first posting is lost whole, and its copy brings every byte. The
// job completes with every member, and every file it publishes is an
// original's bytes: a member as written, or a posted file as posted.
#[tokio::test]
async fn a_piece_lost_beside_its_copy_publishes_only_the_original_bytes() {
    let archive = container(
        sevenz_turbo::EncoderConfiguration::new(sevenz_turbo::EncoderMethod::COPY),
        None,
    );
    let pieces: Vec<Vec<u8>> = split(&archive, 2)
        .into_iter()
        .map(|post| post.bytes)
        .collect();
    let files: Vec<(String, Vec<u8>)> = pieces
        .iter()
        .chain(&pieces)
        .enumerate()
        .map(|(index, bytes)| {
            let name = format!("{:032x}", 0xd1c7_0000_u128 + index as u128);
            (name, bytes.clone())
        })
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set("lantern", &named, 2));
    let articles: Vec<u32> = posts
        .iter()
        .map(|post| post.articles(ARTICLE).count() as u32)
        .collect();
    let lost = [1, posts.len() as u32 - 1];
    let scenario = lost.iter().fold(Scenario::new(posts), |scenario, &file| {
        (0..articles[file as usize])
            .fold(scenario, |scenario, ordinal| scenario.lost(file, ordinal))
    });
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let originals: BTreeMap<String, Vec<u8>> = members()
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .chain(files)
        .map(|(name, bytes)| (format!("lantern beacon/{name}"), bytes))
        .collect();
    for (name, _) in members() {
        assert!(
            graph.files.contains_key(&format!("lantern beacon/{name}")),
            "{name} published"
        );
    }
    for (name, bytes) in &graph.files {
        assert!(
            originals.get(name) == Some(bytes),
            "{name} is an original's bytes"
        );
    }
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}
