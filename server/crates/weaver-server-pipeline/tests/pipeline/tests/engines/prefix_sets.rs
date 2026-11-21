// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A post with two PAR2 sets read as one object: the main set over a stored
// RAR's volumes, named `.rar`, `.r00` and on, and a sample's set whose stem
// continues the main one's. Their volumes come in doubling sizes, named by
// block range, the main set's three digits wide, and a listing sits beside
// them. Each of three archive volumes loses an article. A recovery block
// spans three articles, so a volume's first article holds none whole and
// the rest of it waits until a shortfall asks for it.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{Post, Scenario, par2_doubling, payload, try_run};
use super::sfv::{crc, listing};
use crate::jobs::model::JobStatus;

const FRAME: usize = 384_000;
const SLICE: u64 = 3 * FRAME as u64;

// The volume's block range from a `+` name, `<stem>.vol<start>+<count>.par2`.
fn range_of(name: &str) -> (u64, u64) {
    let tail = name
        .strip_suffix(".par2")
        .unwrap()
        .rsplit_once(".vol")
        .unwrap()
        .1;
    let (start, count) = tail.split_once('+').unwrap();
    let start: u64 = start.parse().unwrap();
    (start, start + count.parse::<u64>().unwrap())
}

// The set's volumes named `<stem>.vol<start>-<end>.par2`, `width` digits.
fn range_named(posts: Vec<Post>, stem: &str, width: usize) -> Vec<Post> {
    posts
        .into_iter()
        .map(|post| {
            if !post.name.contains(".vol") {
                return post;
            }
            let (start, end) = range_of(&post.name);
            let name = format!("{stem}.vol{start:0width$}-{end:0width$}.par2");
            Post {
                bytes: post.bytes,
                ..Post::plain(&name, Vec::new())
            }
        })
        .collect()
}

// The posts first, the main set's index and its volumes from the fewest
// blocks, then the sample's, the listing and the sample itself, and the
// archive's volumes last. Each of the archive's `lost` articles, numbered
// among its volumes, is gone, and so is each of the main set's `bare`
// articles, numbered among its index and volumes.
fn job(lost: &[(u32, u32)], bare: &[(u32, u32)]) -> Scenario {
    let volumes = 8;
    let release = payload(volumes * 6 * FRAME - 4_321, 161);
    let store: Vec<(String, Vec<u8>)> =
        crate::pipeline::tests::direct_store::single_member_store_set(
            "fixture.release.mkv",
            &release,
            volumes,
        )
        .into_iter()
        .enumerate()
        .map(|(at, (_, bytes))| {
            let name = match at {
                0 => "fixture.release.rar".to_owned(),
                at => format!("fixture.release.r{:02}", at - 1),
            };
            (name, bytes)
        })
        .collect();
    let sample = payload(3 * FRAME - 77, 162);
    let entries: Vec<(String, u32)> = store
        .iter()
        .map(|(name, bytes)| (name.clone(), crc(bytes)))
        .collect();
    let listed: Vec<(&str, u32)> = entries
        .iter()
        .map(|(name, crc)| (name.as_str(), *crc))
        .collect();
    let covered: Vec<(&str, &[u8])> = store
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = range_named(
        par2_doubling("fixture.release", &covered, SLICE, 7),
        "fixture.release",
        3,
    );
    posts.extend(range_named(
        par2_doubling(
            "fixture.release-sample",
            &[("fixture.release-sample.mkv", &sample)],
            FRAME as u64,
            7,
        ),
        "fixture.release-sample",
        2,
    ));
    posts.push(listing("fixture.release.sfv", &listed));
    posts.push(Post::plain("fixture.release-sample.mkv", sample.clone()));
    let first = posts.len() as u32;
    posts.extend(super::rar::posts(store));
    let mut scenario = Scenario::new(posts).framed(FRAME);
    for (file, ordinal) in lost {
        scenario = scenario.lost(first + file, *ordinal);
    }
    for (file, ordinal) in bare {
        scenario = scenario.lost(*file, *ordinal);
    }
    scenario
}

const LOST: [(u32, u32); 3] = [(1, 2), (2, 0), (3, 3)];

// Three slices are lost: the main set asks for its smallest volumes, as
// their names say they carry the three blocks, and repairs.
#[tokio::test]
async fn a_main_set_beside_a_sample_set_reaches_its_range_named_volumes() {
    let scenario = job(&LOST, &[]);
    let outcome = try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("the job stalled at {:?}", stall.status));
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.graph().rulings
    );
    assert_eq!(
        outcome.graph().rulings,
        [(
            "par2".to_owned(),
            Some(RepairOutcome::Repaired { slices: 3 })
        )]
    );
}

// The server has the first article of each of the main set's volumes and
// none past it. The set asks for every one of them, finds no block whole,
// and rules the three slices beyond repair with none reachable. The sample
// arrived whole beside the archive's short volumes: the job is delivered in
// part.
#[tokio::test]
async fn a_main_set_whose_volumes_lost_every_body_rules_with_none_reachable() {
    let probe = job(&LOST, &[]);
    let main: Vec<(u32, u32)> = probe
        .posts
        .iter()
        .enumerate()
        .filter(|(_, post)| post.name.starts_with("fixture.release.vol"))
        .map(|(file, post)| (file as u32, post.bytes.len().div_ceil(FRAME) as u32))
        .collect();
    assert_eq!(main.len(), 3, "the main set's volumes");
    let bare: Vec<(u32, u32)> = main
        .iter()
        .flat_map(|&(file, articles)| (1..articles).map(move |ordinal| (file, ordinal)))
        .collect();
    let scenario = job(&LOST, &bare);
    let outcome = try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("the job stalled at {:?}", stall.status));
    for (file, ordinal) in &bare {
        assert!(
            outcome.order.contains(&(*file, *ordinal)),
            "the set never asked for {file}:{ordinal}: {:?}",
            outcome.order
        );
    }
    assert_eq!(
        outcome.graph().rulings,
        [(
            "par2".to_owned(),
            Some(RepairOutcome::Impossible {
                damaged: 3,
                reachable: 0
            })
        )]
    );
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{:?}",
        outcome.failure
    );
}
