// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Furniture posted at the root, side files such as an `.nfo`: delivered
// whole it is a file of the delivery, so a post of nothing but furniture
// that delivers it ends Complete. Furniture that failed beside other files
// decides nothing. A job is Partial only beside a usable file delivered
// whole, and furniture is none.

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use crate::jobs::model::JobStatus;

const FOLDER: &str = "lantern beacon";

fn nfo(seed: u8) -> Vec<u8> {
    payload(3 * ARTICLE + 11, seed)
}

fn movie() -> Vec<u8> {
    payload(4 * ARTICLE + 17, 91)
}

fn said(outcome: &Outcome) -> String {
    let delivery = &outcome.graph().delivery;
    format!(
        "{:?} {:?}\nwhole {:?}\ndamaged {:?}\nfiles {:?}",
        outcome.status,
        outcome.failure,
        delivery.whole,
        delivery.damaged,
        outcome
            .files
            .keys()
            .chain(outcome.retained_files.keys())
            .collect::<Vec<_>>()
    )
}

// A post of one `.nfo`, or two, every byte in: Complete, the files
// delivered.
#[tokio::test]
async fn a_post_of_furniture_alone_that_delivers_it_ends_complete() {
    for names in [&["lantern.nfo"][..], &["lantern.nfo", "beacon.nfo"]] {
        let posts: Vec<Post> = names
            .iter()
            .zip(1u8..)
            .map(|(name, seed)| Post::plain(name, nfo(seed)))
            .collect();
        let outcome = run(&Scenario::new(posts)).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{names:?}: {}",
            said(&outcome)
        );
        for (name, seed) in names.iter().zip(1u8..) {
            assert_eq!(
                outcome.files.get(&format!("{FOLDER}/{name}")),
                Some(&nfo(seed)),
                "{names:?}: {}",
                said(&outcome)
            );
        }
    }
}

// Guard: a post of one `.nfo` that lost an article delivered nothing
// whole: Failed.
#[tokio::test]
async fn a_post_of_furniture_alone_that_lost_an_article_fails() {
    let outcome = run(&Scenario::new(vec![Post::plain("lantern.nfo", nfo(1))]).lost(0, 1)).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}",
        said(&outcome)
    );
}

// Guard: the `.nfo` beside a whole movie lost an article: it decides
// nothing, and the job ends Complete as it did.
#[tokio::test]
async fn failed_furniture_beside_a_whole_file_decides_nothing() {
    let posts = vec![
        Post::plain("lantern.mkv", movie()),
        Post::plain("lantern.nfo", nfo(1)),
    ];
    let outcome = run(&Scenario::new(posts).lost(1, 1)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_eq!(
        outcome.files.get(&format!("{FOLDER}/lantern.mkv")),
        Some(&movie()),
        "{}",
        said(&outcome)
    );
}

// Guard: a whole movie and a whole `.nfo`: Complete, both delivered, as it
// was.
#[tokio::test]
async fn whole_furniture_beside_a_whole_file_is_delivered() {
    let posts = vec![
        Post::plain("lantern.mkv", movie()),
        Post::plain("lantern.nfo", nfo(1)),
    ];
    let outcome = run(&Scenario::new(posts)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        said(&outcome)
    );
    assert_eq!(
        outcome.files.get(&format!("{FOLDER}/lantern.nfo")),
        Some(&nfo(1)),
        "{}",
        said(&outcome)
    );
}

// Guard: a movie that lost an article, with nothing to rebuild it, beside
// a whole `.nfo`: the `.nfo` is delivered, but it is no usable file, so the
// job is not Partial. It fails.
#[tokio::test]
async fn whole_furniture_beside_a_short_file_does_not_make_a_job_partial() {
    let posts = vec![
        Post::plain("lantern.mkv", movie()),
        Post::plain("lantern.nfo", nfo(1)),
    ];
    let outcome = run(&Scenario::new(posts).lost(0, 1)).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}",
        said(&outcome)
    );
}
