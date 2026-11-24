// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// SFV listings in the shapes posts carry them beyond a named listing beside
// plain files: a listing a PAR2 set outranks, a named listing beside a
// nameless one, and a listing naming the volumes of a RAR set the store
// consumes.
//
// A listing is consulted only when no PAR2 set verified the job, and
// listings are looked for by content only when no file is named `*.sfv`.

use super::harness::{ARTICLE, Claim, Post, Scenario, assert_fetched_once, par2_set, payload, run};
use super::rar::{delivered, posts};
use super::sfv::{crc, listing_text};
use crate::pipeline::tests::direct_store::single_member_store_set;
use weaver_server_pipeline::pipeline::tests::engines::loans::stored_zip;

// A PAR2 set that verifies every file outranks a listing that disagrees
// with one of them: the job completes with both files whole.
#[tokio::test]
async fn a_par2_covered_job_never_fails_on_a_disagreeing_sfv() {
    let lantern = payload(3 * ARTICLE, 71);
    let beacon = payload(2 * ARTICLE + 9, 72);
    let mut posts = vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain("beacon.mkv", beacon.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[("lantern.mkv", &lantern), ("beacon.mkv", &beacon)],
        2,
    ));
    posts.push(Post::plain(
        "lantern.sfv",
        listing_text(
            "; generated listing\n",
            &[
                ("lantern.mkv", crc(&lantern)),
                ("beacon.mkv", crc(&beacon) ^ 0x0bad_0bad),
            ],
        ),
    ));
    let graph = run(&Scenario::new(posts)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files["lantern beacon/lantern.mkv"], lantern);
    assert_eq!(graph.files["lantern beacon/beacon.mkv"], beacon);
    assert_eq!(graph.graph().claims["beacon.mkv"], Claim::Complete);
}

// A named listing that agrees with every file is the job's listing: a
// nameless file that reads as a listing and disagrees is not consulted, and
// the job completes.
#[tokio::test]
async fn a_named_sfv_is_used_and_a_nameless_one_beside_it_is_not() {
    let lantern = payload(3 * ARTICLE, 77);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.mkv", lantern.clone()),
        Post::plain(
            "lantern.sfv",
            listing_text("", &[("lantern.mkv", crc(&lantern))]),
        ),
        Post::plain(
            "n0t-a-listing-name",
            listing_text("", &[("lantern.mkv", 0xdead_beef)]),
        ),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.graph().claims["lantern.mkv"], Claim::Complete);
}

// A listing naming the volumes of a stored RAR set, as most posts carry one:
// the volumes agree with it, the store opens them, and the member is
// delivered whole.
#[tokio::test]
async fn an_sfv_naming_the_volumes_of_a_rar_set_verifies_and_the_member_is_delivered() {
    let body = payload(7 * ARTICLE + 13, 78);
    let volumes = single_member_store_set("beacon.mkv", &body, 2);
    let entries: Vec<(String, u32)> = volumes
        .iter()
        .map(|(name, bytes)| (name.clone(), crc(bytes)))
        .collect();
    let named: Vec<(&str, u32)> = entries
        .iter()
        .map(|(name, crc)| (name.as_str(), *crc))
        .collect();
    let mut posts = posts(volumes);
    posts.push(Post::plain(
        "lantern.sfv",
        listing_text("; generated listing\n", &named),
    ));
    let graph = run(&Scenario::new(posts)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files[&delivered("beacon.mkv")], body);
}

// The same listing posted in one article whose header declares a size the
// article cannot reach, so the member learns its length only at its close:
// the listing is still the job's listing, and a job whose every file came
// whole completes.
#[tokio::test]
async fn an_sfv_whose_length_comes_only_at_its_close_is_the_listing() {
    let body = payload(7 * ARTICLE + 13, 79);
    let volumes = single_member_store_set("beacon.mkv", &body, 2);
    let entries: Vec<(String, u32)> = volumes
        .iter()
        .map(|(name, bytes)| (name.clone(), crc(bytes)))
        .collect();
    let named: Vec<(&str, u32)> = entries
        .iter()
        .map(|(name, crc)| (name.as_str(), *crc))
        .collect();
    let mut posts = posts(volumes);
    let listing = listing_text("; generated listing\n", &named);
    let declared = listing.len() as u64 + 64;
    let sfv = posts.len() as u32;
    posts.push(Post::plain("lantern.sfv", listing));
    let graph = run(&Scenario::new(posts).declares(sfv, declared)).await;
    assert!(
        graph.completed(),
        "{:?} {:?} damaged {:?}",
        graph.status,
        graph.failure,
        graph.graph().delivery.damaged
    );
    assert_eq!(graph.files[&delivered("beacon.mkv")], body);
}

// Small files whose one article declares a size it cannot reach, so each
// learns its length only at its close: a stored zip opens on its own
// signature and its member is delivered, and a plain text file that names
// no object is delivered as it came.
#[tokio::test]
async fn small_files_whose_length_comes_only_at_their_close_are_delivered() {
    let member = b"ember, a short member\n".to_vec();
    let zip = stored_zip("ember.txt", &member);
    let notes = b"lantern notes, a short plain text file\n".to_vec();
    let declared = (zip.len() as u64 + 64, notes.len() as u64 + 64);
    let posts = vec![
        Post::plain("lantern.zip", zip),
        Post::plain("lantern.txt", notes.clone()),
    ];
    let scenario = Scenario::new(posts)
        .declares(0, declared.0)
        .declares(1, declared.1);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files.get(&delivered("ember.txt")), Some(&member));
    assert_eq!(graph.files.get(&delivered("lantern.txt")), Some(&notes));
}
