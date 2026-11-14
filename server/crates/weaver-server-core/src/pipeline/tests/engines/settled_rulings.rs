// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery sets over the same members settle: a set's return delivers again
// bytes the namespace already held, under its verdict, cut wherever the read
// back cuts them, and that never takes another set's ruling back. Members
// here are larger than one read back and their slices do not divide it, so
// every return's bytes reach the other sets in pieces no block folds whole.
// An SFV listing beside parity that can rule stands aside, and rules alone
// only where no parity can.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    Outcome, Post, Scenario, par2_volumes_sliced, par3_set_sized, payload, try_run,
};
use super::sfv::{crc, listing};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::engine::Heard;

// Articles larger than a read back's piece of a slice, as a real post's are.
const FRAME: usize = 384_000;
// Three articles to a slice: a read back cuts every few slices mid-slice.
const SLICE: u64 = 3 * FRAME as u64;
// A PAR3 block that neither a read back nor a PAR2 slice divides.
const PAR3_BLOCK: u64 = 256_000;

// Runs the scenario, failing the moment repairs outrun `bound`: a set that
// keeps being made due again repairs without end.
async fn settled(scenario: Scenario, bound: u64) -> (Outcome, u64) {
    let repairs = Arc::new(AtomicU64::new(0));
    let seen = Arc::clone(&repairs);
    let scenario = scenario.framed(FRAME).heard(move |said| {
        if matches!(said.heard, Heard::Repaired(_)) {
            let count = seen.fetch_add(1, Ordering::Relaxed) + 1;
            assert!(
                count <= bound,
                "repair {count} of node {} past {bound}: the sets do not settle",
                said.node.0
            );
        }
    });
    let outcome = try_run(&scenario).await.unwrap_or_else(|stall| {
        panic!("the job stalled at {:?}", stall.status);
    });
    (outcome, repairs.load(Ordering::Relaxed))
}

fn body(articles: usize, seed: u8) -> Vec<u8> {
    payload(articles * FRAME - 1000, seed)
}

fn impossible(ruling: &Option<RepairOutcome>) -> bool {
    matches!(ruling, Some(RepairOutcome::Impossible { .. }))
}

fn failed(outcome: &Outcome) -> bool {
    matches!(outcome.status, Some(JobStatus::Failed { .. }))
}

fn kinds(outcome: &Outcome) -> Vec<&str> {
    outcome
        .graph()
        .rulings
        .iter()
        .map(|(kind, _)| kind.as_str())
        .collect()
}

// The posts with every PAR2 volume after the index renamed by `rename`, given
// its ordinal among the volumes.
fn volumes_renamed(posts: Vec<Post>, rename: impl Fn(usize) -> String) -> Vec<Post> {
    let mut volume = 0;
    posts
        .into_iter()
        .map(|post| {
            if !post.name.contains(".vol") {
                return post;
            }
            let name = rename(volume);
            volume += 1;
            Post {
                bytes: post.bytes,
                ..Post::plain(&name, Vec::new())
            }
        })
        .collect()
}

// A short PAR2 set beside a listing whose volumes are named so that the
// name says nothing of the blocks they hold.
fn short_set_named(rename: impl Fn(usize) -> String, seed: u8) -> Scenario {
    let release = body(12, seed);
    let mut posts = vec![
        Post::plain("fixture.release.mkv", release.clone()),
        listing(
            "fixture.release.sfv",
            &[("fixture.release.mkv", crc(&release))],
        ),
    ];
    let volumes = par2_volumes_sliced(
        "fixture.release",
        &[("fixture.release.mkv", &release)],
        SLICE,
        1,
        1,
    );
    posts.extend(volumes_renamed(volumes, rename));
    Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7)
}

// A listing beside a PAR2 set over the same file, the listing itself one of
// the set's files, and more damage than the set has blocks for.
#[tokio::test]
async fn a_listing_inside_a_short_par2_set_settles_once() {
    let release = body(12, 141);
    let sfv = listing(
        "fixture.release.sfv",
        &[("fixture.release.mkv", crc(&release))],
    );
    let listed = sfv.bytes.clone();
    let mut posts = vec![Post::plain("fixture.release.mkv", release.clone()), sfv];
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &[
            ("fixture.release.mkv", &release),
            ("fixture.release.sfv", &listed),
        ],
        SLICE,
        1,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7);
    let (outcome, repairs) = settled(scenario, 2).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    assert_eq!(kinds(&outcome), ["par2"], "the listing stands aside");
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 1);
}

// The same with enough blocks: the set repairs, and the listing takes no
// part in it.
#[tokio::test]
async fn a_listing_beside_par2_that_repairs_stands_aside() {
    let release = body(12, 142);
    let mut posts = vec![
        Post::plain("fixture.release.mkv", release.clone()),
        listing(
            "fixture.release.sfv",
            &[("fixture.release.mkv", crc(&release))],
        ),
    ];
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &[("fixture.release.mkv", &release)],
        SLICE,
        4,
        2,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7);
    let (outcome, repairs) = settled(scenario, 2).await;
    assert!(outcome.completed(), "{:?}", outcome.status);
    assert_eq!(
        outcome.graph().rulings,
        [(
            "par2".to_string(),
            Some(RepairOutcome::Repaired { slices: 3 })
        )]
    );
    assert!(repairs >= 1);
}

// Volumes named in ranges, `vol00-01`, and volumes whose names carry no
// block count at all settle as `+` names do.
#[tokio::test]
async fn volumes_named_without_a_count_settle() {
    for scenario in [
        short_set_named(
            |volume| format!("fixture.release.vol{volume:02}-{:02}.par2", volume + 1),
            148,
        ),
        short_set_named(|volume| format!("fixture.release.extra{volume}.par2"), 149),
    ] {
        let (outcome, repairs) = settled(scenario, 2).await;
        assert!(failed(&outcome), "{:?}", outcome.status);
        assert_eq!(kinds(&outcome), ["par2"]);
        assert!(
            outcome
                .graph()
                .rulings
                .iter()
                .all(|(_, ruling)| impossible(ruling))
        );
        assert!(repairs >= 1);
    }
}

// Two PAR2 sets over the same file, each short of blocks, read as one PAR2
// object: it rules once over both.
#[tokio::test]
async fn two_short_par2_sets_over_one_file_settle() {
    let release = body(12, 143);
    let mut posts = vec![Post::plain("fixture.release.mkv", release.clone())];
    posts.extend(par2_volumes_sliced(
        "fixture.first",
        &[("fixture.release.mkv", &release)],
        SLICE,
        1,
        1,
    ));
    posts.extend(par2_volumes_sliced(
        "fixture.second",
        &[("fixture.release.mkv", &release)],
        SLICE / 3 * 2,
        1,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7);
    let (outcome, repairs) = settled(scenario, 2).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    assert_eq!(kinds(&outcome), ["par2"]);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 1);
}

// A PAR2 set and a PAR3 set over the same file, each short of blocks: each
// rules once, and neither's return makes the other rule again.
#[tokio::test]
async fn a_par2_and_a_par3_set_over_one_file_settle() {
    let release = body(12, 150);
    let mut posts = vec![Post::plain("fixture.release.mkv", release.clone())];
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &[("fixture.release.mkv", &release)],
        SLICE,
        1,
        1,
    ));
    posts.extend(par3_set_sized(
        "fixture.release.p3",
        &[("fixture.release.mkv", &release)],
        PAR3_BLOCK,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7);
    let (outcome, repairs) = settled(scenario, 4).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    let mut ruled = kinds(&outcome);
    ruled.sort_unstable();
    assert_eq!(ruled, ["par2", "par3"]);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 2);
}

// Three sets over the same file, a PAR2, a PAR3 and a listing: the parity
// sets settle and the listing stands aside.
#[tokio::test]
async fn two_parity_sets_and_a_listing_settle() {
    let release = body(12, 151);
    let mut posts = vec![
        Post::plain("fixture.release.mkv", release.clone()),
        listing(
            "fixture.release.sfv",
            &[("fixture.release.mkv", crc(&release))],
        ),
    ];
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &[("fixture.release.mkv", &release)],
        SLICE,
        1,
        1,
    ));
    posts.extend(par3_set_sized(
        "fixture.release.p3",
        &[("fixture.release.mkv", &release)],
        PAR3_BLOCK,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(0, 7);
    let (outcome, repairs) = settled(scenario, 4).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    let mut ruled = kinds(&outcome);
    ruled.sort_unstable();
    assert_eq!(ruled, ["par2", "par3"]);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 2);
}

// Two PAR2 sets over overlapping files, one a set over the sample alone,
// and a listing over both: they settle, and the listing stands aside.
#[tokio::test]
async fn two_par2_sets_and_a_listing_settle() {
    let release = body(9, 144);
    let extra = body(6, 145);
    let mut posts = vec![
        Post::plain("fixture.release.mkv", release.clone()),
        Post::plain("fixture.release-sample.mkv", extra.clone()),
        listing(
            "fixture.release.sfv",
            &[
                ("fixture.release.mkv", crc(&release)),
                ("fixture.release-sample.mkv", crc(&extra)),
            ],
        ),
    ];
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &[
            ("fixture.release.mkv", &release),
            ("fixture.release-sample.mkv", &extra),
        ],
        SLICE,
        1,
        1,
    ));
    posts.extend(par2_volumes_sliced(
        "fixture.release-sample",
        &[("fixture.release-sample.mkv", &extra)],
        SLICE / 3 * 2,
        1,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 4)
        .damaged(1, 1)
        .damaged(1, 4);
    let (outcome, repairs) = settled(scenario, 2).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    assert_eq!(kinds(&outcome), ["par2"]);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 1);
}

// RAR volumes a store reads, covered by a listing and a short PAR2 set: the
// volumes the store consumes settle under the set as loose files do.
#[tokio::test]
async fn archive_volumes_under_a_short_set_settle() {
    let release = payload(3 * 3 * FRAME - 7, 146);
    let set = crate::pipeline::tests::direct_store::single_member_store_set(
        "fixture.release.mkv",
        &release,
        3,
    );
    let named: Vec<(String, u32)> = set
        .iter()
        .map(|(name, bytes)| (name.clone(), crc(bytes)))
        .collect();
    let entries: Vec<(&str, u32)> = named
        .iter()
        .map(|(name, crc)| (name.as_str(), *crc))
        .collect();
    let covered: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = super::rar::posts(set.clone());
    posts.push(listing("fixture.release.sfv", &entries));
    posts.extend(par2_volumes_sliced(
        "fixture.release",
        &covered,
        SLICE,
        1,
        1,
    ));
    let scenario = Scenario::new(posts)
        .damaged(0, 0)
        .damaged(0, 2)
        .damaged(1, 1)
        .damaged(2, 2);
    let (outcome, repairs) = settled(scenario, 2).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    assert_eq!(kinds(&outcome), ["par2"]);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .all(|(_, ruling)| impossible(ruling))
    );
    assert!(repairs >= 1);
}

// A PAR2 index that never arrives describes nothing: the listing beside it
// is all that can rule, and it does.
#[tokio::test]
async fn a_listing_rules_when_the_par2_index_never_arrives() {
    let release = body(6, 147);
    let mut posts = vec![
        Post::plain("fixture.release.mkv", release.clone()),
        listing(
            "fixture.release.sfv",
            &[("fixture.release.mkv", crc(&release))],
        ),
    ];
    let mut par2 = par2_volumes_sliced(
        "fixture.release",
        &[("fixture.release.mkv", &release)],
        SLICE,
        1,
        1,
    );
    // The index alone: its volume is not posted.
    par2.truncate(1);
    let index_articles = par2[0].bytes.len().div_ceil(FRAME) as u32;
    posts.extend(par2);
    let mut scenario = Scenario::new(posts).damaged(0, 2);
    for article in 0..index_articles {
        scenario = scenario.lost(2, article);
    }
    let (outcome, _) = settled(scenario, 2).await;
    assert!(failed(&outcome), "{:?}", outcome.status);
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .any(|(kind, ruling)| kind == "sfv" && impossible(ruling)),
        "{:?}",
        outcome.graph().rulings
    );
}
