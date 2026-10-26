// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A plain file posted in numbered pieces, `lantern.bin.001` onward, whose
// first piece opens no signature: the pieces are one file, joined in order,
// and that file is what is delivered, never the pieces.

use weaver_graph::recovery::RepairOutcome;

use crate::jobs::model::JobStatus;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run, run_both};
use crate::pipeline::graph::Engine;

const NAME: &str = "lantern.bin";
const PIECE: usize = 6 * ARTICLE;

fn joined() -> Vec<u8> {
    payload(3 * PIECE - 17, 61)
}

fn pieces(bytes: &[u8]) -> Vec<Post> {
    bytes
        .chunks(PIECE)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("{NAME}.{:03}", index + 1), piece.to_vec()))
        .collect()
}

// The joined file alone, every byte as written.
fn assert_joined(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![format!("lantern beacon/{NAME}")],
        "{}: delivered names",
        outcome.engine
    );
    assert!(
        outcome.files[&format!("lantern beacon/{NAME}")] == joined(),
        "{}: joined bytes",
        outcome.engine
    );
}

#[tokio::test]
async fn a_clean_plain_split_is_delivered_joined_under_graph() {
    let (_, graph) = run_both(&Scenario::new(pieces(&joined()))).await;
    assert_joined(&graph);
    assert!(graph.graph().outcomes.is_empty());
}

// The usual shape: the set describes the pieces as posted. It repairs the
// middle piece in place, and the joined file is what is delivered.
#[tokio::test]
async fn a_plain_split_under_par2_over_its_pieces_is_repaired_and_delivered_joined_under_both_engines()
 {
    let original = joined();
    let posted = pieces(&original);
    let described: Vec<(String, Vec<u8>)> = posted
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let described: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = posted;
    posts.extend(par2_set("lantern", &described, 2));
    let (classic, graph) = run_both(&Scenario::new(posts).damaged(1, 2)).await;
    // Classic leaves the joined file it staged behind too; only the joined
    // file is compared there.
    assert!(classic.completed(), "classic: {:?}", classic.failure);
    assert_eq!(
        classic.files.get(&format!("lantern beacon/{NAME}")),
        Some(&original)
    );
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}
// Three slices of the middle piece zeroed as posted: every article arrives
// whole, and only the set's hashes over the joined file say they are wrong.
#[tokio::test]
async fn a_plain_split_under_par2_over_the_joined_file_is_repaired_and_delivered_joined_under_both_engines()
 {
    let original = joined();
    let mut posts = pieces(&original);
    posts[1].bytes[ARTICLE..4 * ARTICLE].fill(0);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 4));
    let (classic, graph) = run_both(&Scenario::new(posts)).await;
    // Classic joins and repairs too, though it may leave the pieces it
    // joined behind in its staging directory; only the joined file is
    // compared there.
    assert!(classic.completed(), "classic: {:?}", classic.failure);
    assert_eq!(
        classic.files.get(&format!("lantern beacon/{NAME}")),
        Some(&original)
    );
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 3 })]
    );
}

// Nothing damaged: the set over the joined file rules it clean where it is
// joined, and rebuilds nothing.
#[tokio::test]
async fn a_clean_plain_split_under_par2_over_the_joined_file_is_ruled_clean_under_both_engines() {
    let original = joined();
    let mut posts = pieces(&original);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 4));
    let (classic, graph) = run_both(&Scenario::new(posts)).await;
    assert!(classic.completed(), "classic: {:?}", classic.failure);
    assert_eq!(
        classic.files.get(&format!("lantern beacon/{NAME}")),
        Some(&original)
    );
    assert_joined(&graph);
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
}

// More slices zeroed than the set has blocks: the set rules where the file
// is joined that it cannot rebuild it, the job fails as a recovery failure,
// and the pieces are never delivered in its place.
#[tokio::test]
async fn a_plain_split_whose_par2_over_the_joined_file_is_short_fails_without_its_pieces_under_both_engines()
 {
    let original = joined();
    let mut posts = pieces(&original);
    posts[1].bytes[ARTICLE..4 * ARTICLE].fill(0);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 2));
    let (classic, graph) = run_both(&Scenario::new(posts)).await;
    for outcome in [&classic, &graph] {
        assert!(
            matches!(
                &outcome.status,
                Some(JobStatus::Failed { error }) if error.starts_with("PAR2 recovery failed")
            ),
            "{}: {:?}",
            outcome.engine,
            outcome.status
        );
        assert!(
            outcome
                .files
                .keys()
                .all(|name| !name.starts_with(&format!("lantern beacon/{NAME}."))),
            "{}: pieces delivered: {:?}",
            outcome.engine,
            outcome.files.keys().collect::<Vec<_>>()
        );
    }
    assert!(graph.files.is_empty(), "graph: {:?}", graph.files.keys());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 3,
            reachable: 2
        })]
    );
}

// One piece's article lost: the set posted over the joined file repairs the
// joined file where the join writes it, and that file is what is handed
// over, whole, with none of the pieces beside it.
#[tokio::test]
async fn a_plain_split_with_a_lost_article_under_par2_over_the_joined_file_is_repaired_under_graph()
{
    let original = joined();
    let mut posts = pieces(&original);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 2));
    let graph = run(&Scenario::new(posts).lost(1, 2), Engine::Graph).await;
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}
