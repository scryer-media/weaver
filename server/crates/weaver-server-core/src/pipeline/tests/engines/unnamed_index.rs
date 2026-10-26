// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery index posted under a name that says nothing is known by its front:
// the payload waits for it as for one the post declares.

use super::harness::{ARTICLE, Post, Scenario, par2_set, par3_set, payload, run};
use crate::pipeline::graph::Engine;

// The data member and the index are both posted under unrelated names. Once
// the index's first article says what it is, the data member's payload
// waits until the index is in; the index then names the member and it is
// delivered under that name.
#[tokio::test]
async fn an_index_with_an_unrelated_name_holds_the_payload_under_graph() {
    unnamed_index(false).await;
}

#[tokio::test]
async fn a_par3_index_with_an_unrelated_name_holds_the_payload_under_graph() {
    unnamed_index(true).await;
}

async fn unnamed_index(par3: bool) {
    let data = payload(6 * ARTICLE, 45);
    let mut set = if par3 {
        par3_set("lantern", &[("lantern.bin", &data)], 2)
    } else {
        par2_set("lantern", &[("lantern.bin", &data)], 2)
    };
    assert!(!set[0].name.contains(".vol"), "the index first");
    let index = set.remove(0).bytes;
    let posts = vec![
        Post::plain("x3f8a1c0", data.clone()),
        Post::plain("q7k2m9d4", index.clone()),
        set.remove(0),
    ];
    let index_articles = index.len().div_ceil(ARTICLE) as u32;
    assert!(index_articles > 2, "the index spans articles");
    let graph = run(&Scenario::new(posts).leased(), Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/lantern.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/lantern.bin"] == data);
    let at = |article: (u32, u32)| {
        graph
            .order
            .iter()
            .position(|asked| *asked == article)
            .unwrap_or_else(|| panic!("{article:?} never asked: {:?}", graph.order))
    };
    let index_in = at((1, index_articles - 1));
    let index_head = at((1, 0));
    for ordinal in 1..6 {
        let asked = at((0, ordinal));
        assert!(
            asked < index_head || asked > index_in,
            "payload article {ordinal} asked while the index was coming: {:?}",
            graph.order
        );
    }
    assert!(
        (1..6).any(|ordinal| at((0, ordinal)) > index_in),
        "nothing of the payload waited for the index: {:?}",
        graph.order
    );
}
