// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file cut in pieces posted under hash names, which a recovery set covers
// by those names. Ruling 2026-10-30 3:52 PM MDT: the set founds the split
// store over the pieces for every format beneath, ordered by the pieces
// themselves: the first is the one whose front signs; with one other piece
// that piece is the last whatever its length; with more, the last is the one
// shorter piece and any between are as long as the first (ruling 4A). Each
// case runs in two article orders.

use super::super::harness::{Outcome, Post, Scenario, kinds, par2_set, payload, run};
use super::super::streams::xz;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::single_member_store_set;

const SET: &str = "e81b6d3f0a4c9725";

fn hashed(index: usize) -> String {
    format!("{:032x}", 0x5eed_0000_u128 + index as u128)
}

// Each file under its hash name, and one recovery set over those names.
fn posted(files: &[Vec<u8>]) -> Vec<Post> {
    let names: Vec<String> = (0..files.len()).map(hashed).collect();
    let described: Vec<(&str, &[u8])> = names
        .iter()
        .zip(files)
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = names
        .iter()
        .zip(files)
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_set(SET, &described, 2));
    posts
}

// `bytes` cut after every `cut` bytes.
fn cut(bytes: &[u8], cut: usize) -> Vec<Vec<u8>> {
    bytes.chunks(cut).map(<[u8]>::to_vec).collect()
}

// `bytes` cut in two, the last piece the shorter whatever the length.
fn halved(bytes: &[u8]) -> Vec<Vec<u8>> {
    cut(bytes, bytes.len() / 2 + 1)
}

// The first of the bytes `make` builds from a body of 9000 bytes or more that
// cut evenly in `parts`, cut so: every piece as long as the first.
fn evenly(parts: usize, make: impl Fn(usize) -> Vec<u8>) -> Vec<Vec<u8>> {
    let bytes = (9000..)
        .map(&make)
        .find(|bytes| bytes.len() % parts == 0)
        .expect("a length that cuts evenly");
    let pieces = cut(&bytes, bytes.len() / parts);
    assert_eq!(pieces.len(), parts);
    pieces
}

async fn both_orders(files: &[Vec<u8>]) -> Vec<(bool, Outcome)> {
    let mut outcomes = Vec::new();
    for backwards in [false, true] {
        let mut scenario = Scenario::new(posted(files));
        if backwards {
            scenario = scenario.backwards();
        }
        outcomes.push((backwards, run(&scenario).await));
    }
    outcomes
}

fn sevenz() -> &'static [u8] {
    include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/sevenz_solid.7z"
    ))
}

fn assert_sevenz_members(outcome: &Outcome, backwards: bool) {
    for index in 0..3 {
        let expected: Vec<u8> = (0..8193)
            .map(|n| ((n * 7 + n / 251 + index * 13) % 253) as u8)
            .collect();
        assert!(
            outcome
                .files
                .get(&format!("lantern beacon/part{index}.mkv"))
                == Some(&expected),
            "backwards {backwards}: part{index}.mkv is byte-exact: {:?} {:?}",
            outcome.files.keys().collect::<Vec<_>>(),
            outcome.graph().delivery
        );
    }
}

fn assert_joined(outcome: &Outcome, backwards: bool, pieces: &[Vec<u8>]) {
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "backwards {backwards}: {:?} {:?}",
        outcome.failure,
        outcome.graph().delivery
    );
    assert!(
        kinds(outcome).contains(&"split"),
        "backwards {backwards}: {:?}",
        kinds(outcome)
    );
    assert!(
        pieces
            .iter()
            .all(|piece| !outcome.files.values().any(|file| file == piece)),
        "backwards {backwards}: no piece is delivered as posted: {:?}",
        outcome.files.keys().collect::<Vec<_>>()
    );
}

// A whole 7z and a small plain file beside it, one set over both: the 7z
// reads whole, so nothing is withheld and nothing is grouped.
#[tokio::test]
async fn a_whole_7z_beside_a_plain_file_under_one_set_is_not_grouped() {
    let plain = payload(300, 9);
    let files = [sevenz().to_vec(), plain.clone()];
    for (backwards, outcome) in both_orders(&files).await {
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "backwards {backwards}"
        );
        assert_sevenz_members(&outcome, backwards);
        assert_eq!(
            outcome.files.get(&format!("lantern beacon/{}", hashed(1))),
            Some(&plain),
            "backwards {backwards}: the plain file as posted"
        );
        assert!(!kinds(&outcome).contains(&"split"), "{:?}", kinds(&outcome));
    }
}

// A 7z in three pieces: the first signs, the one between is as long, and
// the last is shorter.
#[tokio::test]
async fn a_7z_in_three_hash_named_pieces_under_one_set_is_joined_in_order() {
    let pieces = cut(sevenz(), 200);
    assert_eq!(pieces.len(), 3);
    for (backwards, outcome) in both_orders(&pieces).await {
        assert_joined(&outcome, backwards, &pieces);
        assert_sevenz_members(&outcome, backwards);
    }
}

// A 7z in two pieces, the first under the archive's own name and the second
// numbered after it, one set over both. The numbered piece founds a split
// store by its name that is never whole, and the first is refused by the 7z
// store it founds; the set then joins the two whichever object gave up its
// piece last, so the second arriving first joins the same.
#[tokio::test]
async fn a_7z_whose_second_piece_alone_is_numbered_under_one_set_is_joined_in_either_order() {
    let pieces = halved(sevenz());
    let names = ["lantern.7z", "lantern.7z.002"];
    for backwards in [false, true] {
        let mut posts: Vec<Post> = names
            .iter()
            .zip(&pieces)
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        let described: Vec<(&str, &[u8])> = names
            .iter()
            .zip(&pieces)
            .map(|(name, bytes)| (*name, bytes.as_slice()))
            .collect();
        posts.extend(par2_set(SET, &described, 2));
        let mut scenario = Scenario::new(posts);
        if backwards {
            scenario = scenario.backwards();
        }
        let outcome = run(&scenario).await;
        assert_joined(&outcome, backwards, &pieces);
        assert_sevenz_members(&outcome, backwards);
    }
}

// A RAR cut in bytes, not in volumes.
#[tokio::test]
async fn a_rar_cut_in_hash_named_pieces_under_one_set_is_joined_and_unpacked() {
    let body = payload(9000, 23);
    let rar = single_member_store_set("beacon.bin", &body, 1).remove(0).1;
    let pieces = halved(&rar);
    for (backwards, outcome) in both_orders(&pieces).await {
        assert_joined(&outcome, backwards, &pieces);
        assert_eq!(
            outcome.files.get("lantern beacon/beacon.bin"),
            Some(&body),
            "backwards {backwards}: {:?}",
            outcome.files.keys().collect::<Vec<_>>()
        );
    }
}

// An xz stream cut in bytes.
#[tokio::test]
async fn an_xz_cut_in_hash_named_pieces_under_one_set_is_joined_and_decoded() {
    let body = payload(9000, 31);
    let stream = xz(&body);
    let pieces = halved(&stream);
    for (backwards, outcome) in both_orders(&pieces).await {
        assert_joined(&outcome, backwards, &pieces);
        assert!(
            outcome.files.values().any(|file| *file == body),
            "backwards {backwards}: {:?}",
            outcome.files.keys().collect::<Vec<_>>()
        );
    }
}

// A plain payload cut in bytes: no piece's front signs, so none is first
// and the pieces go as posted.
#[tokio::test]
async fn a_plain_payload_cut_in_hash_named_pieces_goes_as_posted() {
    let body = payload(900, 37);
    let pieces = cut(&body, 500);
    for (backwards, outcome) in both_orders(&pieces).await {
        for (index, piece) in pieces.iter().enumerate() {
            assert_eq!(
                outcome
                    .files
                    .get(&format!("lantern beacon/{}", hashed(index))),
                Some(piece),
                "backwards {backwards}: {:?}",
                outcome.files.keys().collect::<Vec<_>>()
            );
        }
        assert!(!kinds(&outcome).contains(&"split"), "{:?}", kinds(&outcome));
    }
}

// A RAR cut in two equal pieces: with one piece besides the signing first,
// that piece is the last.
#[tokio::test]
async fn a_rar_cut_in_two_equal_hash_named_pieces_under_one_set_is_joined_and_unpacked() {
    let body = |length: usize| payload(length, 23);
    let pieces = evenly(2, |length| {
        single_member_store_set("beacon.bin", &body(length), 1)
            .remove(0)
            .1
    });
    let length = (9000..)
        .find(|length| {
            single_member_store_set("beacon.bin", &body(*length), 1)
                .remove(0)
                .1
                == pieces.concat()
        })
        .expect("the body the pieces came from");
    for (backwards, outcome) in both_orders(&pieces).await {
        assert_joined(&outcome, backwards, &pieces);
        assert_eq!(
            outcome.files.get("lantern beacon/beacon.bin"),
            Some(&body(length)),
            "backwards {backwards}: {:?}",
            outcome.files.keys().collect::<Vec<_>>()
        );
    }
}

// An xz stream cut in two equal pieces.
#[tokio::test]
async fn an_xz_cut_in_two_equal_hash_named_pieces_under_one_set_is_joined_and_decoded() {
    let body = |length: usize| payload(length, 31);
    let pieces = evenly(2, |length| xz(&body(length)));
    let length = (9000..)
        .find(|length| xz(&body(*length)) == pieces.concat())
        .expect("the body the pieces came from");
    for (backwards, outcome) in both_orders(&pieces).await {
        assert_joined(&outcome, backwards, &pieces);
        assert!(
            outcome.files.values().any(|file| *file == body(length)),
            "backwards {backwards}: {:?}",
            outcome.files.keys().collect::<Vec<_>>()
        );
    }
}

// An xz stream cut in three equal pieces: no piece is shorter, so nothing
// tells the last from the one between, and the set stays as posted. No
// whole file has a known type, so the job fails and its pieces stay where
// they were fetched.
#[tokio::test]
async fn an_xz_cut_in_three_equal_hash_named_pieces_goes_as_posted() {
    let body = |length: usize| payload(length, 41);
    let pieces = evenly(3, |length| xz(&body(length)));
    let length = (9000..)
        .find(|length| xz(&body(*length)) == pieces.concat())
        .expect("the body the pieces came from");
    for (backwards, outcome) in both_orders(&pieces).await {
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "backwards {backwards}: {:?}",
            outcome.status
        );
        assert!(!kinds(&outcome).contains(&"split"), "{:?}", kinds(&outcome));
        for (index, piece) in pieces.iter().enumerate().skip(1) {
            assert_eq!(
                outcome
                    .retained_files
                    .iter()
                    .find(|(path, _)| path.rsplit('/').next() == Some(hashed(index).as_str()))
                    .map(|(_, bytes)| bytes),
                Some(piece),
                "backwards {backwards}: {:?}",
                outcome.retained_files.keys().collect::<Vec<_>>()
            );
        }
        assert!(
            !outcome
                .files
                .values()
                .chain(outcome.retained_files.values())
                .any(|file| *file == body(length)),
            "backwards {backwards}: nothing is joined"
        );
    }
}
