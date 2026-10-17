// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Repair below the root under both engines: a split 7z's volumes repaired
// by a PAR2 set posted beside them inside stored 7z containers one, two and
// three deep; two sets sharing one recovery file; a described file no post
// carries; and a damaged member no set covers, two deep. Classic runs no
// repair inside an archive, so each nested row names how the engines part.

use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, par2_set, payload, run_both,
};
use crate::history::HistoryFailureKind;

// A stored 7z of `entries`, in order.
fn stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in entries {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.to_vec())))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

// The members the innermost archive holds.
fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 11, 61)),
        ("quill.bin", payload(3 * ARTICLE - 3, 62)),
    ]
}

// `inner.7z.001` and `.002`, a byte split of a stored 7z of `members()`,
// with `inner.par2` and its volume over them.
fn inner_set() -> Vec<Post> {
    let entries = members();
    let borrowed: Vec<(&str, &[u8])> = entries.iter().map(|(n, b)| (*n, b.as_slice())).collect();
    let archive = stored(&borrowed);
    let half = archive.len().div_ceil(2);
    let volumes: Vec<Post> = archive
        .chunks(half)
        .zip(1..)
        .map(|(bytes, number)| Post::plain(&format!("inner.7z.{number:03}"), bytes.to_vec()))
        .collect();
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let set = par2_set("inner", &described, 4);
    volumes.into_iter().chain(set).collect()
}

// `posts` wrapped `depth` times in stored 7z containers, the outermost
// named `lantern.7z`.
fn wrapped(mut posts: Vec<Post>, depth: usize) -> (Vec<u8>, Vec<u8>) {
    let first = posts[0].bytes.clone();
    for level in 0..depth {
        let entries: Vec<(&str, &[u8])> = posts
            .iter()
            .map(|post| (post.name.as_str(), post.bytes.as_slice()))
            .collect();
        let name = if level + 1 == depth {
            "lantern.7z".to_string()
        } else {
            format!("level{level}.7z")
        };
        posts = vec![Post::plain(&name, stored(&entries))];
    }
    (posts.remove(0).bytes, first)
}

// An article of `outer` lying wholly inside the bytes of `inner`, past its
// front, so what the member is can still be told from its first bytes.
fn article_inside(outer: &[u8], inner: &[u8]) -> u32 {
    let at = outer
        .windows(inner.len())
        .position(|window| window == inner)
        .expect("a stored container holds its member's bytes as they are");
    let ordinal = at / ARTICLE + 1;
    assert!(
        (ordinal + 1) * ARTICLE <= at + inner.len(),
        "an article fits inside"
    );
    ordinal as u32
}

// The innermost members, delivered as written.
fn members_whole(outcome: &Outcome) -> bool {
    members()
        .iter()
        .all(|(name, bytes)| outcome.files.get(&delivered(name)) == Some(bytes))
}

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The inner set wrapped `depth` deep, with one outer article served
// damaged where the bytes of `inner.7z.001` lie.
fn nested(depth: usize) -> Scenario {
    let (outer, first) = wrapped(inner_set(), depth);
    let ordinal = article_inside(&outer, &first);
    Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, ordinal)
}

// Graph repairs the inner volumes with the set beside them, one slice, and
// delivers the innermost members whole beside the set's own files, asking
// for nothing twice.
fn assert_repaired_below(graph: &Outcome) {
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["ember.bin", "inner.par2", "inner.vol0+4.par2", "quill.bin"].map(delivered),
        "graph: delivered names"
    );
    assert!(members_whole(graph), "graph: the members are rebuilt whole");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(graph.graph().claims["lantern.7z"], Claim::Damaged);
}

// Classic refetches the damaged article, which comes back damaged again, and
// fails the job unpacking the outer archive: its repair does not reach a set
// inside an archive.
fn assert_classic_fails_unpacking(classic: &Outcome) {
    assert_eq!(
        classic.failure.as_deref(),
        Some("archive file lantern.7z is incomplete after downloads and recovery settled")
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
    assert!(!classic.refetched().is_empty(), "classic refetched nothing");
}

#[tokio::test]
async fn a_split_7z_one_deep_is_repaired_by_the_par2_set_beside_it_under_graph_and_fails_under_classic()
 {
    let (classic, graph) = run_both(&nested(1)).await;
    assert_repaired_below(&graph);
    assert_classic_fails_unpacking(&classic);
}

#[tokio::test]
async fn a_split_7z_two_deep_is_repaired_by_the_par2_set_beside_it_under_graph_and_fails_under_classic()
 {
    let (classic, graph) = run_both(&nested(2)).await;
    assert_repaired_below(&graph);
    assert_classic_fails_unpacking(&classic);
}

#[tokio::test]
async fn a_split_7z_three_deep_is_repaired_by_the_par2_set_beside_it_under_graph_and_fails_under_classic()
 {
    let (classic, graph) = run_both(&nested(3)).await;
    assert_repaired_below(&graph);
    assert_classic_fails_unpacking(&classic);
}

// Two sets whose recovery packets share one file, `ab.vol00+5.par2`, made
// by concatenating each set's own volume. One article of each set's file is
// served damaged; each set rebuilds its own.
#[tokio::test]
async fn two_sets_sharing_one_recovery_file_each_repair_their_own_file_under_both_engines() {
    let alpha = payload(4 * ARTICLE, 71);
    let bravo = payload(5 * ARTICLE + 9, 72);
    let [index_a, volume_a]: [Post; 2] = par2_set("a", &[("alpha.bin", alpha.as_slice())], 2)
        .try_into()
        .ok()
        .unwrap();
    let [index_b, volume_b]: [Post; 2] = par2_set("b", &[("bravo.bin", bravo.as_slice())], 3)
        .try_into()
        .ok()
        .unwrap();
    let mut shared = volume_a.bytes;
    shared.extend_from_slice(&volume_b.bytes);
    let scenario = Scenario::new(vec![
        Post::plain("alpha.bin", alpha.clone()),
        Post::plain("bravo.bin", bravo.clone()),
        index_a,
        index_b,
        Post::plain("ab.vol00+5.par2", shared),
    ])
    .damaged(0, 1)
    .damaged(1, 2);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(
            outcome.completed(),
            "{}: {:?}",
            outcome.engine,
            outcome.failure
        );
        assert!(
            outcome.files[&delivered("alpha.bin")] == alpha,
            "{}",
            outcome.engine
        );
        assert!(
            outcome.files[&delivered("bravo.bin")] == bravo,
            "{}",
            outcome.engine
        );
    }
    assert_same(&classic, &graph);
    // One recovery object holds both sets, and rebuilds a slice of each.
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
    assert_eq!(graph.graph().blocks_reachable, 5);
}

// A file the set describes and no post carries is announced, every slice
// missing, and rebuilt whole from the recovery blocks.
#[tokio::test]
async fn a_described_file_no_post_carries_is_rebuilt_whole_under_both_engines() {
    let lantern = payload(4 * ARTICLE, 73);
    let quill = payload(3 * ARTICLE - 3, 74);
    let set = par2_set(
        "lantern",
        &[
            ("lantern.bin", lantern.as_slice()),
            ("quill.bin", quill.as_slice()),
        ],
        4,
    );
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    posts.extend(set);
    let (classic, graph) = run_both(&Scenario::new(posts)).await;
    for outcome in [&classic, &graph] {
        assert!(
            outcome.completed(),
            "{}: {:?}",
            outcome.engine,
            outcome.failure
        );
        assert!(
            outcome.files[&delivered("quill.bin")] == quill,
            "{}",
            outcome.engine
        );
        assert!(
            outcome.files[&delivered("lantern.bin")] == lantern,
            "{}",
            outcome.engine
        );
    }
    assert_same(&classic, &graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 3 })]
    );
}

// A member two deep that no set covers, with one outer article served
// damaged inside it. Graph refuses the archive the damage has no cover in,
// and delivers that archive as it stands; classic fails unpacking the outer
// one after fetching the article again.
#[tokio::test]
async fn an_uncovered_damaged_member_two_deep_is_refused_to_its_archive_under_graph_and_fails_under_classic()
 {
    let entries = members();
    let borrowed: Vec<(&str, &[u8])> = entries.iter().map(|(n, b)| (*n, b.as_slice())).collect();
    let (outer, _) = wrapped(vec![Post::plain("level.7z", stored(&borrowed))], 1);
    let ordinal = article_inside(&outer, &entries[0].1);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, ordinal);
    let (classic, graph) = run_both(&scenario).await;
    assert!(graph.completed(), "graph: {:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("level.7z")]
    );
    assert!(graph.graph().outcomes.is_empty());
    assert_eq!(graph.graph().claims["lantern.7z"], Claim::Damaged);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_classic_fails_unpacking(&classic);
}
