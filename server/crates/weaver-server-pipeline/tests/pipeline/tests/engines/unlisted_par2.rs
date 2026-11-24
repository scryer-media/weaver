// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 volume posted whole whose NZB lacks some of its articles: at its
// start, in its middle or at its end. The bytes the missing articles held
// never come, and every listed article still says where its own bytes go,
// so the hole is lost bytes like any other: the walk steps over it, finds
// the next whole packet after it, and every whole block after the hole is
// one the repair can spend.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Post, Scenario, par2_set, par2_volumes, payload, recovery_blocks, run,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

// One file of eight articles under a set of `blocks` blocks in one volume:
// the file, the index, then the volume.
fn covered(blocks: u32) -> Vec<Post> {
    let data = payload(8 * ARTICLE, 71);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], blocks));
    posts
}

// The blocks of `volume` whose whole packet, header and block alike, lies
// clear of the `unlisted` articles: the ones a repair can read whole, as a
// packet is checked over all of its bytes.
fn whole_blocks(volume: &Post, unlisted: &[u32]) -> u64 {
    const HEADER: usize = 64;
    const RECOVERY: &[u8; 16] = b"PAR 2.0\0RecvSlic";
    let bytes = &volume.bytes;
    let mut whole = 0;
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        let (first, last) = ((at / ARTICLE) as u32, ((at + length - 1) / ARTICLE) as u32);
        if &bytes[at + 48..at + 64] == RECOVERY
            && !(first..=last).any(|article| unlisted.contains(&article))
        {
            whole += 1;
        }
        at += length;
    }
    whole
}

// The volume's articles, as the post cuts it.
fn articles(volume: &Post) -> u32 {
    volume.bytes.len().div_ceil(ARTICLE) as u32
}

// The first article of the volume holding a recovery block's bytes, and
// the one after the last.
fn recovery_span(volume: &Post) -> (u32, u32) {
    let blocks = recovery_blocks(volume);
    let first = blocks.iter().flatten().min().copied().unwrap();
    let last = blocks.iter().flatten().max().copied().unwrap();
    (first, last + 1)
}

// Damage in `slices` of the file's articles, two articles apart, so each
// is a slice of its own.
fn damaged(mut scenario: Scenario, slices: u32) -> Scenario {
    for slice in 0..slices {
        scenario = scenario.damaged(0, 2 * slice + 1);
    }
    scenario
}

// The volume of a set whose first article the NZB does not list: every
// block past it is found and spent, and the job ends complete.
#[tokio::test]
async fn a_recovery_volume_without_its_first_article_still_repairs() {
    let posts = covered(6);
    let reachable = whole_blocks(&posts[2], &[0]);
    assert!(
        reachable >= 3,
        "{reachable} blocks clear of the first article"
    );
    let data = posts[0].bytes.clone();
    let scenario = damaged(Scenario::new(posts).unlisted(2, 0), 3);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(graph.status, Some(JobStatus::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 3 })]
    );
    assert_eq!(graph.graph().blocks_reachable, reachable);
    assert_eq!(graph.files.values().next(), Some(&data));
}

// A hole inside the recovery blocks, in every order the articles can land:
// the blocks the hole cuts are lost, and those after it are found.
#[tokio::test]
async fn a_hole_in_the_middle_of_a_recovery_volume_is_stepped_over() {
    let posts = covered(6);
    let (first, end) = recovery_span(&posts[2]);
    let hole = first + (end - first) / 2;
    let reachable = whole_blocks(&posts[2], &[hole]);
    assert!(reachable >= 3, "{reachable} blocks clear of article {hole}");
    type Arrange = fn(Scenario) -> Scenario;
    let orders: [(&str, Arrange); 4] = [
        ("in order", |scenario| scenario),
        ("backwards", Scenario::backwards),
        ("volume reversed", |scenario| scenario.reversed(&[2])),
        ("two at a time", |scenario| scenario.batched(2)),
    ];
    for (order, arrange) in orders {
        let scenario = damaged(arrange(Scenario::new(covered(6)).unlisted(2, hole)), 3);
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{order}: {:?}", graph.failure);
        assert_eq!(
            graph.graph().outcomes,
            vec![Some(RepairOutcome::Repaired { slices: 3 })],
            "{order}"
        );
        assert_eq!(graph.graph().blocks_reachable, reachable, "{order}");
    }
}

// A volume whose last article the NZB does not list ends where its last
// listed article does: the block the end cut is lost, the rest are spent.
#[tokio::test]
async fn a_recovery_volume_without_its_last_article_still_repairs() {
    let posts = covered(6);
    let last = articles(&posts[2]) - 1;
    let reachable = whole_blocks(&posts[2], &[last]);
    assert!(
        reachable >= 3,
        "{reachable} blocks clear of the last article"
    );
    for backwards in [false, true] {
        let mut scenario = damaged(Scenario::new(covered(6)).unlisted(2, last), 3);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        assert!(
            graph.completed(),
            "backwards={backwards}: {:?}",
            graph.failure
        );
        assert_eq!(
            graph.graph().outcomes,
            vec![Some(RepairOutcome::Repaired { slices: 3 })],
            "backwards={backwards}"
        );
        assert_eq!(
            graph.graph().blocks_reachable,
            reachable,
            "backwards={backwards}"
        );
    }
}

// The same holes across a restart after every new article: the restored
// map tells each hole again, and the job ends as one that never went down.
#[tokio::test]
async fn holes_in_a_recovery_volume_are_told_again_after_a_restart() {
    let posts = covered(6);
    let (first, end) = recovery_span(&posts[2]);
    let hole = first + (end - first) / 2;
    let data = posts[0].bytes.clone();
    for unlisted in [vec![0], vec![hole], vec![0, hole]] {
        let mut scenario = Scenario::new(covered(6));
        for posted in &unlisted {
            scenario = scenario.unlisted(2, *posted);
        }
        let scenario = damaged(scenario, 2);
        let restarted = super::restart::run_restarting(&scenario).await;
        assert_eq!(
            restarted.status,
            Some(JobStatus::Complete),
            "unlisted {unlisted:?}"
        );
        assert!(restarted.restarts > 0, "unlisted {unlisted:?}");
        assert_eq!(
            restarted.files.values().next(),
            Some(&data),
            "unlisted {unlisted:?}"
        );
    }
}

// Holes that leave fewer whole blocks than damaged slices: the job fails
// for its repair as it always has, counting exactly the blocks left.
#[tokio::test]
async fn holes_that_leave_too_few_blocks_still_fail_the_repair() {
    let posts = covered(3);
    let (first, end) = recovery_span(&posts[2]);
    let holes: Vec<u32> = (first..end).step_by(3).collect();
    let reachable = whole_blocks(&posts[2], &holes);
    assert!(reachable < 3, "{reachable} blocks clear of {holes:?}");
    let mut scenario = Scenario::new(posts);
    for hole in &holes {
        scenario = scenario.unlisted(2, *hole);
    }
    let graph = run(&damaged(scenario, 3)).await;
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert_eq!(graph.graph().blocks_reachable, reachable);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 3,
            reachable
        })]
    );
}

// Two volumes, the smaller asked for first, with a hole that costs it a
// block: the blocks it still holds fall short, so the larger one, held
// back until a shortfall wants it, is fetched before anything is ruled,
// and the job ends complete.
#[tokio::test]
async fn a_hole_that_leaves_one_volume_short_fetches_the_next_one() {
    let data = payload(8 * ARTICLE, 72);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 6, 2));
    assert_eq!(posts.len(), 4, "the file, the index and two volumes");
    // The volumes in block order: the first names the fewest.
    let (smaller, larger) = (2u32, 3u32);
    let named = |post: &Post| recovery_blocks(post).len() as u64;
    assert!(named(&posts[2]) <= named(&posts[3]));
    let hole = *recovery_blocks(&posts[smaller as usize])[0].last().unwrap();
    let kept = whole_blocks(&posts[smaller as usize], &[hole]);
    assert!(
        kept < named(&posts[2]),
        "the hole costs the smaller a block"
    );
    let slices = u32::try_from(named(&posts[2])).unwrap();
    assert!(u64::from(slices) <= kept + named(&posts[3]));
    let larger_articles = articles(&posts[larger as usize]);
    let scenario = damaged(Scenario::new(posts).unlisted(smaller, hole), slices);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired {
            slices: u64::from(slices)
        })]
    );
    assert!(
        (0..larger_articles).all(|ordinal| graph.requests.contains_key(&(larger, ordinal))),
        "every article of the larger volume was fetched: {:?}",
        graph.requests
    );
    assert_eq!(graph.files.values().next(), Some(&data));
}
