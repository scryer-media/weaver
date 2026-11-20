// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Child archives one deep that no other row builds: a 7z crossing both
// volume boundaries of a three-volume RAR, and that set repaired at the
// root; an AES 7z offered only a wrong candidate; two levels each opened by
// its own password; stores and streams bound out of a compressed 7z's
// output, its headers encrypted or not; a 7z inside a header-encrypted RAR;
// a 7z of a directory, an empty file and an empty directory; two streams of
// one name in two directories; and an xz cut short inside a store.

use super::archive_scenario_fixtures::pairings::nested_encrypted;
use super::harness::run;

// The node cap, lowered to one: the first node breadth first is kept, and
// the true count and depth stay.
#[tokio::test]
async fn a_capped_summary_keeps_the_first_nodes_and_the_true_count() {
    crate::pipeline::graph::summary::NODES_KEPT_HERE.with(|kept| kept.set(1));
    let outcome = run(&nested_encrypted()).await;
    let summary = super::download_summary::summary(&outcome);
    assert_eq!(summary.nodes.len(), 1, "{summary:?}");
    assert_eq!(summary.nodes[0].kind, "rar_store");
    assert_eq!((summary.node_count, summary.max_depth), (2, 2));
}
