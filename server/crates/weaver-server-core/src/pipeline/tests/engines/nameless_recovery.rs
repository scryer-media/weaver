// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A data file that lost its first article beside a PAR2 set every file of
// which is posted under a name that says nothing, its fronts all kept. The
// post declares no recovery by name, so it is held to the line of a post
// with none, and two files losing an article arm a health probe. The probe
// asks only articles not already known gone: what it finds is the post's
// real loss, a few articles, and the set repairs the file.

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

// The data file, then its set, the index and one volume of four blocks,
// under names that say nothing.
fn posts(article: usize, block: u64) -> (Vec<u8>, Vec<Post>) {
    let data = payload(ARTICLES * article, 92);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    let set = par2_volumes_sliced("lantern", &[("lantern.bin", data.as_slice())], block, 4, 1);
    for (at, post) in set.into_iter().enumerate() {
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(&format!("q8z{at}w.bin"), Vec::new())
        });
    }
    (data, posts)
}

#[tokio::test]
async fn a_lost_first_article_beside_a_nameless_set_is_repaired_read_forwards() {
    for (article, block) in [(REAL_ARTICLE, REAL_BLOCK), (ARTICLE, ARTICLE as u64)] {
        for index_lost in [false, true] {
            let (data, posts) = posts(article, block);
            let mut scenario = Scenario::new(posts).framed(article).lost(0, 0);
            if index_lost {
                scenario = scenario.lost(1, 0);
            }
            let graph = run(&scenario).await;
            let case = format!(
                "article {article} index lost {index_lost}: {}",
                summary(&graph)
            );
            assert!(graph.completed(), "{case}");
            let delivered: Vec<_> = graph.files.iter().collect();
            assert_eq!(delivered.len(), 1, "{case}");
            assert!(delivered[0].0.ends_with("lantern.bin"), "{case}");
            assert!(delivered[0].1 == &data, "{case}");
        }
    }
}
