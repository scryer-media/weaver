// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stored RAR4 set below the root whose own bytes are wrong, inside an
// outer RAR a recovery set proved: the outer RAR is whole, so nothing can
// rebuild the nested set. A member whose CRC fails is never delivered as
// whole, at the root or below it.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use weaver_server_pipeline::pipeline::tests::single_member_rar4_old_numbering_set;

const FOLDER: &str = "lantern beacon";
const MOVIE: &str = "quill.mkv";
const NFO: &str = "quill.nfo";
// A RAR4 end record with no body.
const END: usize = 7;

fn movie() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 71)
}

fn nfo() -> Vec<u8> {
    b"quill release notes\r\n".to_vec()
}

// The old-numbering set `quill.rar`, `quill.r00`, ... of `count` volumes
// holding the movie, with the last data byte of volume `volume` flipped.
fn flipped(count: usize, volume: usize) -> Vec<(String, Vec<u8>)> {
    let mut set = single_member_rar4_old_numbering_set("quill", MOVIE, &movie(), count);
    let bytes = &mut set[volume].1;
    let at = bytes.len() - END - 1;
    bytes[at] ^= 0x5a;
    set
}

// `entries` as the members of an outer stored RAR set of two volumes under
// a PAR2 set over those volumes.
fn in_outer_rar(entries: &[(String, Vec<u8>)]) -> Scenario {
    let entries: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer: Vec<(String, Vec<u8>)> = multi_member_store_set(&entries, 2)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("Duck.Tales.part{:02}.rar", index + 1), bytes))
        .collect();
    at_the_root(&outer, "Duck.Tales")
}

// `files` posted at the root under a PAR2 set over them.
fn at_the_root(files: &[(String, Vec<u8>)], base: &str) -> Scenario {
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set(base, &described, 2));
    Scenario::new(posts)
}

fn with_nfo(mut entries: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    entries.push((NFO.to_owned(), nfo()));
    entries
}

fn said(outcome: &Outcome) -> String {
    let delivery = &outcome.graph().delivery;
    format!(
        "{:?} {:?}\nwhole {:?}\ndamaged {:?}\nunchecked {:?}\nwithheld {:?}\nfiles {:?}\nretained {:?}",
        outcome.status,
        outcome.failure,
        delivery.whole,
        delivery.damaged,
        delivery.unchecked,
        delivery.withheld,
        outcome.files.keys().collect::<Vec<_>>(),
        outcome.retained_files.keys().collect::<Vec<_>>(),
    )
}

// The movie is not delivered with bytes that differ from the original, and
// is not reported whole. `named` is what the delivery calls the damaged
// file: the movie below the root, a volume of its set at the root.
fn movie_not_whole(outcome: &Outcome, named: &str) {
    let delivered = outcome
        .files
        .iter()
        .find(|(name, _)| name.ends_with(&format!("/{MOVIE}")) || name.as_str() == MOVIE);
    if let Some((_, bytes)) = delivered {
        assert!(
            *bytes == movie(),
            "a wrong movie was delivered: {}",
            said(outcome)
        );
    }
    movie_said_damaged(outcome, named);
}

// The movie is not reported whole, and `named` is reported damaged.
fn movie_said_damaged(outcome: &Outcome, named: &str) {
    let delivery = &outcome.graph().delivery;
    assert!(
        !delivery.whole.iter().any(|name| name.ends_with(MOVIE)),
        "the movie is reported whole: {}",
        said(outcome)
    );
    assert!(
        delivery.damaged.iter().any(|name| name == named),
        "{named} is not reported damaged: {}",
        said(outcome)
    );
}

#[tokio::test]
async fn a_nested_stored_rar4_with_a_flipped_byte_in_a_middle_volume_is_not_delivered_whole() {
    let outcome = run(&in_outer_rar(&with_nfo(flipped(6, 2)))).await;
    // The whole `.nfo` beside it leaves the job delivered in part, and a
    // job delivered in part hands the damaged movie over as it stands,
    // said damaged.
    movie_said_damaged(&outcome, MOVIE);
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{}",
        said(&outcome)
    );
}

#[tokio::test]
async fn a_nested_stored_rar4_with_a_flipped_byte_in_its_last_volume_is_not_delivered_whole() {
    let outcome = run(&in_outer_rar(&with_nfo(flipped(6, 5)))).await;
    // The whole `.nfo` beside it leaves the job delivered in part, and a
    // job delivered in part hands the damaged movie over as it stands,
    // said damaged.
    movie_said_damaged(&outcome, MOVIE);
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{}",
        said(&outcome)
    );
}

#[tokio::test]
async fn a_nested_single_volume_stored_rar4_with_a_flipped_byte_is_not_delivered_whole() {
    let outcome = run(&in_outer_rar(&with_nfo(flipped(1, 0)))).await;
    // The whole `.nfo` beside it leaves the job delivered in part, and a
    // job delivered in part hands the damaged movie over as it stands,
    // said damaged.
    movie_said_damaged(&outcome, MOVIE);
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{}",
        said(&outcome)
    );
}

// Guard: the same flipped sets at the root, with no recovery set over them,
// are not delivered whole either: their volumes are damaged.
#[tokio::test]
async fn a_root_stored_rar4_with_a_flipped_byte_in_a_middle_volume_is_not_delivered_whole() {
    let posts: Vec<Post> = with_nfo(flipped(6, 2))
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let outcome = run(&Scenario::new(posts)).await;
    movie_not_whole(&outcome, "quill.r01");
}

#[tokio::test]
async fn a_root_stored_rar4_with_a_flipped_byte_in_its_last_volume_is_not_delivered_whole() {
    let posts: Vec<Post> = with_nfo(flipped(6, 5))
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let outcome = run(&Scenario::new(posts)).await;
    movie_not_whole(&outcome, "quill.r04");
}

// Guard: the unflipped nested set delivers the movie whole.
#[tokio::test]
async fn a_nested_stored_rar4_whole_delivers_its_movie() {
    let set = single_member_rar4_old_numbering_set("quill", MOVIE, &movie(), 6);
    let outcome = run(&in_outer_rar(&with_nfo(set))).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert!(
        outcome.files.get(&format!("{FOLDER}/{MOVIE}")) == Some(&movie()),
        "{}",
        said(&outcome)
    );
}

// The first byte of the member's name in the first volume's file header,
// flipped: the first volume names a file the volumes after it do not. The
// store placed the second volume's bytes before it found the two do not sit
// side by side, so it keeps that volume rather than give it back without
// them. The job ends, the nfo beside the set is delivered, and whatever the
// set delivers holds the movie's bytes.
#[tokio::test]
async fn a_nested_stored_rar4_whose_first_volume_names_another_file_ends() {
    // Offset of the name's first byte: the marker (7), the main header
    // (13), and the file header's fixed fields (32).
    const NAME: usize = 52;
    let mut set = single_member_rar4_old_numbering_set("quill", MOVIE, &movie(), 6);
    set[0].1[NAME] ^= 0x5a;
    let outcome = run(&in_outer_rar(&with_nfo(set))).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert!(
        outcome.files.get(&format!("{FOLDER}/{NFO}")) == Some(&nfo()),
        "{}",
        said(&outcome)
    );
    for (name, bytes) in &outcome.files {
        if name.ends_with(".mkv") {
            assert!(
                *bytes == movie(),
                "{name} holds wrong bytes: {}",
                said(&outcome)
            );
        }
    }
}
