// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR5 volume posted under a name that says nothing, whose first article
// is lost: its signature, main header and file header went with it, so
// nothing it still holds says which set it is or where in it.

use super::harness::{ARTICLE, Outcome, Scenario, par2_set, payload, run};
use super::rar::{delivered, posts};
use crate::pipeline::tests::direct_store::{obfuscate_volumes, single_member_store_set};

const MEMBER: &str = "beacon.bin";

fn body() -> Vec<u8> {
    payload(23 * ARTICLE + 13, 51)
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

fn assert_member(graph: &Outcome, body: &[u8], case: &str) {
    assert!(graph.completed(), "{case}: {}", summary(graph));
    assert_eq!(
        graph.files.get(&delivered(MEMBER)),
        Some(&body.to_vec()),
        "{case}: {}",
        summary(graph)
    );
}

// PAR2 over the set's own names, the volumes posted under names that say
// nothing, the second without its first article.
#[tokio::test]
async fn a_nameless_rar5_volume_with_no_front_is_rebuilt_by_par2() {
    let body = body();
    let set = single_member_store_set(MEMBER, &body, 4);
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posted = posts(obfuscate_volumes(&set));
    posted.extend(par2_set("silver.horizon", &files, 2));
    for backwards in [false, true] {
        let mut scenario = Scenario::new(posts_again(&posted)).lost(1, 0);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        assert_member(&graph, &body, &format!("backwards {backwards}"));
    }
}

fn posts_again(posted: &[super::harness::Post]) -> Vec<super::harness::Post> {
    posted
        .iter()
        .map(|post| super::harness::Post {
            bytes: post.bytes.clone(),
            least_article: post.least_article,
            ..super::harness::Post::plain(&post.name, Vec::new())
        })
        .collect()
}
