// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A post short one whole volume, as `par2-heavy-withheld` writes one: five
// header-encrypted stored RAR5 volumes, `fixture.part1.rar` to
// `fixture.part5.rar`, the third listed in the NZB and every article of it
// refused, beside a PAR2 index and the six volumes par2cmdline writes at
// 35% redundancy over the five, `vol00+01` to `vol31+22`, 53 blocks in all.
// The NZB lists the files sorted by name: the index, the five RAR volumes,
// then the recovery volumes, smallest first. The set needs a block per
// slice of the withheld volume, and only the volumes beyond the smallest
// carry that many. Those are asked for once the set first rules, and their
// articles land as many workers' fetches do, out of order: the last first.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{Outcome, Post, Scenario, par2_doubling, payload, run};
use super::rar::delivered;
use crate::history::HistoryFailureKind;
use crate::pipeline::tests::direct_store::scenarios::{HeaderCheck, header_encrypted_store_set};

const MEMBER: &str = "fixture.mkv";
const SECRET: &str = "fixture-password";
// The withheld volume, `fixture.part3.rar`, is the NZB's fourth file.
const WITHHELD: u32 = 3;
// The recovery volumes beyond the smallest, `vol01+02` to `vol31+22`.
const LATER: [u32; 5] = [7, 8, 9, 10, 11];
// The post at half the fixture's scale: 512 KiB slices where it has 1 MiB,
// posted in 384 KiB articles where it has 750 KiB, so the two largest
// recovery volumes run past what a walk holds ahead of its front, as there.
const SLICE: usize = 512 * 1024;
const ARTICLE: usize = 384 * 1024;

// The post, and the member the archive holds. 160 slices of payload over
// five volumes and 53 recovery blocks: the withheld volume touches 33
// slices, more than the 31 the five smallest recovery volumes carry, and
// fewer than the 53.
fn post() -> (Vec<Post>, Vec<u8>) {
    let body = payload(160 * SLICE, 75);
    let volumes: Vec<(String, Vec<u8>)> =
        header_encrypted_store_set(MEMBER, &body, 5, SECRET, HeaderCheck::For(SECRET))
            .into_iter()
            .enumerate()
            .map(|(at, (_, bytes))| (format!("fixture.part{}.rar", at + 1), bytes))
            .collect();
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut recovery = par2_doubling("fixture", &files, SLICE as u64, 53);
    let names: Vec<&str> = recovery.iter().map(|post| post.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "fixture.par2",
            "fixture.vol00+01.par2",
            "fixture.vol01+02.par2",
            "fixture.vol03+04.par2",
            "fixture.vol07+08.par2",
            "fixture.vol15+16.par2",
            "fixture.vol31+22.par2",
        ]
    );
    let mut posts = vec![recovery.remove(0)];
    posts.extend(
        volumes
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes)),
    );
    posts.extend(recovery);
    (posts, body)
}

// The scenario with every article of the withheld volume refused, and of
// each further file in `never`, the recovery volumes beyond the smallest
// arriving last article first.
fn withheld(posts: Vec<Post>, never: &[u32]) -> Scenario {
    let articles: Vec<(u32, u32)> = posts
        .iter()
        .enumerate()
        .filter(|(file, _)| *file as u32 == WITHHELD || never.contains(&(*file as u32)))
        .flat_map(|(file, post)| {
            (0..post.articles(ARTICLE).count() as u32).map(move |ordinal| (file as u32, ordinal))
        })
        .collect();
    let mut scenario = Scenario::new(posts)
        .framed(ARTICLE)
        .reversed(&LATER)
        .password(SECRET);
    for (file, ordinal) in articles {
        scenario = scenario.lost(file, ordinal);
    }
    scenario
}

fn ruled_impossible(outcome: &Outcome) -> bool {
    outcome
        .graph()
        .outcomes
        .iter()
        .any(|outcome| matches!(outcome, Some(RepairOutcome::Impossible { .. })))
}

// The recovery volumes beyond the smallest arrive after the set first
// rules, the largest past what a walk holds ahead of its front: every block
// they carry is counted, the set rebuilds the withheld volume from them, and
// the member is delivered.
#[tokio::test]
async fn a_withheld_volume_is_rebuilt_from_recovery_volumes_that_land_out_of_order() {
    let (posts, body) = post();
    let graph = run(&withheld(posts, &[])).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert!(!ruled_impossible(&graph), "{:?}", graph.graph().outcomes);
    assert!(
        matches!(
            graph.graph().outcomes.as_slice(),
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
    assert_eq!(graph.files.get(&delivered(MEMBER)), Some(&body));
}

// The two largest recovery volumes are withheld too: the 15 blocks the rest
// carry are short of the withheld volume's slices, and the job fails as
// Repair once that is known.
#[tokio::test]
async fn a_withheld_volume_whose_recovery_volumes_never_come_fails_as_repair() {
    let (posts, _) = post();
    let largest = [posts.len() as u32 - 2, posts.len() as u32 - 1];
    let graph = run(&withheld(posts, &largest)).await;
    assert!(!graph.completed(), "{:?}", graph.status);
    assert!(ruled_impossible(&graph), "{:?}", graph.graph().outcomes);
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
}
