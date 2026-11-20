// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An SFV packed inside an archive, beside the files it lists, under a PAR2
// set that proved the archive's volumes: every file it lists came out of
// that proved archive, so its bytes are what was posted, and the SFV makes
// no claim of damage, whatever CRCs it states. Elsewhere an SFV says what it
// says, as it did.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::sfv::{crc, listing_text};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

const FOLDER: &str = "lantern beacon";
const MOVIE: &str = "quill.mkv";
const SFV: &str = "quill.sfv";

fn movie() -> Vec<u8> {
    payload(9 * ARTICLE + 29, 73)
}

// An SFV over the movie stating a CRC its bytes do not have.
fn wrong_sfv() -> Vec<u8> {
    listing_text("; quill\n", &[(MOVIE, crc(&movie()) ^ 0x00ff_00ff)])
}

// The movie and `sfv` as the members of an outer stored RAR set of two
// volumes, under a PAR2 set over those volumes when `recovery` says so.
fn in_outer_rar(sfv: Vec<u8>, recovery: bool) -> Scenario {
    let entries: Vec<(&str, Vec<u8>)> = vec![(MOVIE, movie()), (SFV, sfv)];
    let outer: Vec<(String, Vec<u8>)> = multi_member_store_set(&entries, 2)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("Duck.Tales.part{:02}.rar", index + 1), bytes))
        .collect();
    let described: Vec<(&str, &[u8])> = outer
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = outer
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    if recovery {
        posts.extend(par2_set("Duck.Tales", &described, 2));
    }
    Scenario::new(posts)
}

fn said(outcome: &Outcome) -> String {
    let delivery = &outcome.graph().delivery;
    format!(
        "{:?} {:?}\nrulings {:?}\nwhole {:?}\ndamaged {:?}\nunrepaired {:?}\nfiles {:?}",
        outcome.status,
        outcome.failure,
        outcome.graph().rulings,
        delivery.whole,
        delivery.damaged,
        delivery.unrepaired,
        outcome
            .files
            .keys()
            .chain(outcome.retained_files.keys())
            .collect::<Vec<_>>()
    )
}

// The SFV inside the proved archive states a CRC the movie does not have:
// it makes no claim, and the job ends Complete with the movie delivered as
// it came out.
#[tokio::test]
async fn an_sfv_over_files_of_a_proved_archive_makes_no_damage_claim() {
    for (case, scenario) in [
        ("in order", in_outer_rar(wrong_sfv(), true)),
        ("backwards", in_outer_rar(wrong_sfv(), true).backwards()),
    ] {
        let outcome = run(&scenario).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{case}: {}",
            said(&outcome)
        );
        assert_eq!(
            outcome.files.get(&format!("{FOLDER}/{MOVIE}")),
            Some(&movie()),
            "{case}: {}",
            said(&outcome)
        );
    }
}

// Guard: the same archive with no recovery set over it. Nothing proved its
// bytes, and the SFV's claim stands as it did: the job is not Complete.
#[tokio::test]
async fn an_sfv_inside_an_archive_nothing_proved_keeps_its_claim() {
    let outcome = run(&in_outer_rar(wrong_sfv(), false)).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}

// Guard: an SFV posted at the root beside the movie, stating the wrong
// CRC, under a PAR2 set over both: the root keeps its rules, under which
// the PAR2 set that verified the movie clean decides, and the job ends
// Complete as it did.
#[tokio::test]
async fn an_sfv_at_the_root_beside_a_clean_par2_set_ends_as_it_did() {
    let sfv = wrong_sfv();
    let mut posts = vec![Post::plain(MOVIE, movie()), Post::plain(SFV, sfv.clone())];
    posts.extend(par2_set(
        "quill",
        &[(MOVIE, movie().as_slice()), (SFV, sfv.as_slice())],
        2,
    ));
    let outcome = run(&Scenario::new(posts)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_eq!(
        outcome.graph().rulings,
        vec![(
            "par2".to_owned(),
            Some(weaver_graph::recovery::RepairOutcome::Clean)
        )]
    );
}

// Guard: the SFV inside the proved archive states the right CRC: Complete,
// as it was.
#[tokio::test]
async fn a_right_sfv_over_files_of_a_proved_archive_completes() {
    let sfv = listing_text("; quill\n", &[(MOVIE, crc(&movie()))]);
    let outcome = run(&in_outer_rar(sfv, true)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
}
