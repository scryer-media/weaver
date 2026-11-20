// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 set whose index is lost whole, beside a data file that lost one
// article: the volumes keep their fronts, so they found the set, and their
// blocks repair the article. The volumes' later packets rebuild the set's
// index the same as before, and the repair's verdict stands: a set that
// rebuilt an article ruled Repaired, never Clean.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes_sliced, payload, run};

const REAL_ARTICLE: usize = 716_800;
const REAL_BLOCK: u64 = 768_000;
const ARTICLES: usize = 24;

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

// The data file, then its set: the index, then four blocks in one volume.
fn posts(article: usize, block: u64) -> (Vec<u8>, Vec<Post>) {
    let data = payload(ARTICLES * article, 92);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes_sliced(
        "lantern",
        &[("lantern.bin", data.as_slice())],
        block,
        4,
        1,
    ));
    (data, posts)
}

fn par2_ruling(graph: &Outcome) -> Option<RepairOutcome> {
    graph
        .graph()
        .rulings
        .iter()
        .find(|(kind, _)| kind == "par2")
        .and_then(|(_, ruling)| *ruling)
}

#[tokio::test]
async fn a_lost_index_repairs_the_lost_article_and_rules_repaired() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for lost in [0, ARTICLES as u32 / 2, ARTICLES as u32 - 1] {
            // A lost first article read forwards trips the feed's health
            // probe on two missing samples; backwards keeps it out.
            let (data, posts) = posts(article, block);
            let mut scenario = Scenario::new(posts)
                .framed(article)
                .lost(0, lost)
                .lost(1, 0);
            if lost == 0 {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            let case = format!("article {article} lost {lost}: {}", summary(&graph));
            assert!(graph.completed(), "{case}");
            assert!(
                matches!(par2_ruling(&graph), Some(RepairOutcome::Repaired { .. })),
                "{case}"
            );
            let delivered: Vec<_> = graph.files.iter().collect();
            assert_eq!(delivered.len(), 1, "{case}");
            assert!(delivered[0].0.ends_with("lantern.bin"), "{case}");
            assert!(delivered[0].1 == &data, "{case}");
        }
    }
}

// Five articles lost across five blocks, past the volume's four: the set
// cannot rebuild them, and the job does not complete.
#[tokio::test]
async fn a_lost_index_past_the_sets_blocks_does_not_complete() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        let (_, posts) = posts(article, block);
        let mut scenario = Scenario::new(posts).framed(article).lost(1, 0);
        for lost in [3, 7, 11, 15, 19] {
            scenario = scenario.lost(0, lost);
        }
        let graph = run(&scenario).await;
        let case = format!("article {article}: {}", summary(&graph));
        assert!(!graph.completed(), "{case}");
        assert!(
            !matches!(
                par2_ruling(&graph),
                Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
            ),
            "{case}"
        );
    }
}
