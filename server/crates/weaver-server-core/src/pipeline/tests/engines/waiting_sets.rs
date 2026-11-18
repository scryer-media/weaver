// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file under two recovery sets of different kinds, a PAR2 set and a PAR3
// set, one short of blocks and the other with enough, and two of its
// articles lost or damaged. The short set rules impossible; the other asks
// for its recovery volumes, which the job fetches only once a set asks, and
// the job completes on it in either order the articles land. When the set
// with enough blocks cannot get them, the job ends as it did, and does not
// wait for blocks nothing will bring.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes, par3_set, payload, run};
use crate::jobs::model::JobStatus;

// The file, a PAR2 index with up to three volumes sharing `par2_blocks`, and a
// PAR3 set of `par3_blocks`.
fn covered(par2_blocks: u32, par3_blocks: u64) -> Vec<Post> {
    let data = payload(8 * ARTICLE, 91);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes(
        "lantern",
        &[("lantern.bin", &data)],
        par2_blocks,
        par2_blocks.min(3),
    ));
    posts.extend(par3_set(
        "lantern.p3",
        &[("lantern.bin", &data)],
        par3_blocks,
    ));
    posts
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings
    )
}

// Two articles of the file lost, or two damaged, forward or backwards.
fn holes(scenario: Scenario, lost: bool, backwards: bool) -> Scenario {
    let scenario = if lost {
        scenario.lost(0, 2).lost(0, 5)
    } else {
        scenario.damaged(0, 2).damaged(0, 5)
    };
    if backwards {
        scenario.backwards()
    } else {
        scenario
    }
}

#[tokio::test]
async fn a_short_set_does_not_end_a_job_another_kind_repairs() {
    // (PAR2 blocks, PAR3 blocks): each kind the short one in turn.
    for (par2, par3) in [(1, 4), (4, 1)] {
        for lost in [false, true] {
            for backwards in [false, true] {
                let scenario = holes(Scenario::new(covered(par2, par3)), lost, backwards);
                let graph = run(&scenario).await;
                assert!(
                    graph.completed(),
                    "par2 {par2} par3 {par3} lost {lost} backwards {backwards}: {}",
                    summary(&graph)
                );
            }
        }
    }
}

// The PAR2 set has the blocks, but every article of its volumes is lost:
// nothing can bring them, the short PAR3 set rules impossible, and the job
// fails as one file with nothing usable does. The run returning at all is
// the no-wait half of the check.
#[tokio::test]
async fn a_set_whose_blocks_cannot_arrive_does_not_hold_the_job() {
    for backwards in [false, true] {
        let mut scenario = holes(Scenario::new(covered(4, 1)), true, backwards);
        let spec = scenario.spec();
        for (file, entry) in spec.files.iter().enumerate() {
            if entry.filename.contains(".vol") && entry.filename.ends_with(".par2") {
                for ordinal in 0..entry.segments.len() {
                    scenario = scenario.lost(file as u32, ordinal as u32);
                }
            }
        }
        let graph = run(&scenario).await;
        assert!(
            matches!(graph.status, Some(JobStatus::Failed { .. })),
            "backwards {backwards}: {}",
            summary(&graph)
        );
    }
}

// A named PAR2 volume whose first article is lost is still claimed by its
// name, its blocks count, and the job repairs a lost data article.
#[tokio::test]
async fn a_named_volume_with_a_lost_front_still_repairs() {
    for backwards in [false, true] {
        let data = payload(8 * ARTICLE, 92);
        let mut posts = vec![Post::plain("lantern.bin", data.clone())];
        posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 4, 1));
        let mut scenario = Scenario::new(posts).lost(0, 2).lost(2, 0);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        assert!(
            graph.completed(),
            "backwards {backwards}: {}",
            summary(&graph)
        );
    }
}
