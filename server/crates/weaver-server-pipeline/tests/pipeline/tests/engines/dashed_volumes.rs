// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 set posted as `<stem>.vol-01.par2`, `<stem>.vol-02.par2`, …: a
// dash and one number, not `volNN+MM`, and no plain `<stem>.par2` beside
// them. Every volume carries the set's main, file-description and IFSC
// packets, so the volumes alone are the whole set: they repair a data file
// within their blocks, and past them the job does not complete.

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

// The data file, then its set's three volumes of two blocks each under the
// dashed names. With `index`, the index is posted first as `vol-01` and
// the volumes follow it from `vol-02`, as the job that found this posted
// them; without it, the index is not posted. The data file is posted with
// a byte changed in each of the `changed` articles: they arrive whole and
// pass their checksums, and only the set tells their bytes are wrong.
fn posts(article: usize, block: u64, index: bool, changed: &[usize]) -> (Vec<u8>, Vec<Post>) {
    let data = payload(ARTICLES * article, 92);
    let mut posted = data.clone();
    for at in changed {
        posted[at * article + article / 2] ^= 0x5a;
    }
    let mut posts = vec![Post::plain("lantern.bin", posted)];
    let set = par2_volumes_sliced("lantern", &[("lantern.bin", data.as_slice())], block, 6, 3);
    let skip = usize::from(!index);
    for (at, post) in set.into_iter().enumerate().skip(skip) {
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(
                &format!("lantern.vol-{:02}.par2", at + 1 - skip),
                Vec::new(),
            )
        });
    }
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
async fn dashed_volumes_with_no_index_repair_the_data_file() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for index in [false, true] {
            for backwards in [false, true] {
                let (data, posts) = posts(article, block, index, &[]);
                let mut scenario = Scenario::new(posts)
                    .framed(article)
                    .lost(0, 5)
                    .lost(0, 11)
                    .lost(0, 17);
                if backwards {
                    scenario = scenario.backwards();
                }
                let graph = run(&scenario).await;
                let case = format!(
                    "article {article} index {index} backwards {backwards}: {}",
                    summary(&graph)
                );
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
}

// Seven articles lost across seven blocks, past the volumes' six: the set
// cannot rebuild them, and the job does not complete.
#[tokio::test]
async fn dashed_volumes_past_their_blocks_do_not_complete() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for index in [false, true] {
            let (_, posts) = posts(article, block, index, &[]);
            let mut scenario = Scenario::new(posts).framed(article);
            for lost in [1, 4, 7, 10, 13, 16, 19] {
                scenario = scenario.lost(0, lost);
            }
            let graph = run(&scenario).await;
            let case = format!("article {article} index {index}: {}", summary(&graph));
            assert!(!graph.completed(), "{case}");
            assert!(
                !matches!(
                    par2_ruling(&graph),
                    Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                ),
                "{case}"
            );
            assert!(graph.graph().kinds.contains("par2"), "{case}");
        }
    }
}

// The data file arrives whole, every article passing its checksum, with
// wrong bytes in three of them: only the set can tell, and with no index
// posted its volumes are what tells it. The volumes verify the file, find
// the changed slices and rebuild them.
#[tokio::test]
async fn dashed_volumes_with_no_index_repair_bytes_that_arrived_wrong() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for index in [false, true] {
            let (data, posts) = posts(article, block, index, &[5, 11, 17]);
            let graph = run(&Scenario::new(posts).framed(article)).await;
            let case = format!("article {article} index {index}: {}", summary(&graph));
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

// Wrong bytes in seven articles, past the volumes' six blocks: the set
// cannot rebuild them, and the job does not complete.
#[tokio::test]
async fn dashed_volumes_past_their_blocks_do_not_pass_wrong_bytes() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for index in [false, true] {
            let (_, posts) = posts(article, block, index, &[1, 4, 7, 10, 13, 16, 19]);
            let graph = run(&Scenario::new(posts).framed(article)).await;
            let case = format!("article {article} index {index}: {}", summary(&graph));
            assert!(!graph.completed(), "{case}");
            assert!(
                !matches!(
                    par2_ruling(&graph),
                    Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                ),
                "{case}"
            );
            assert!(graph.graph().kinds.contains("par2"), "{case}");
        }
    }
}
