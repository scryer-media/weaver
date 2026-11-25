// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a job's end reads beside its recovery sets: a posted file no set
// describes, the share of the job that is recovery data, and what a Partial
// job says it lacks.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Post, Scenario, par2_set, par2_volumes, payload, run};
use super::sfv::{assert_twin, assert_twin_but_text, crc, listing};
use crate::jobs::model::JobStatus;

// Two tracks under one PAR2 set of `blocks` recovery blocks, and a text
// file posted beside them that no set describes, of three articles: the
// tracks are files 0 and 1, the set's index and volume 2 and 3, and the
// text file 4.
fn tracks_beside(blocks: u32) -> Vec<Post> {
    let first = payload(2 * ARTICLE, 11);
    let second = payload(2 * ARTICLE + 7, 12);
    let mut posts = vec![
        Post::plain("01 Kind Hearted Woman.flac", first.clone()),
        Post::plain("02 Terraplane Blues.flac", second.clone()),
    ];
    posts.extend(par2_set(
        "lantern",
        &[
            ("01 Kind Hearted Woman.flac", first.as_slice()),
            ("02 Terraplane Blues.flac", second.as_slice()),
        ],
        blocks,
    ));
    posts.push(Post::plain("foo_dr.txt", payload(3 * ARTICLE, 13)));
    posts
}

const TEXT: u32 = 4;

// A set that verified its files clean or repaired them
// vouches for the job, and a posted file no set describes is never checked.
// Its damage leaves the job no shorter; the file is delivered as it stands.
#[tokio::test]
async fn damage_to_a_file_no_set_describes_does_not_shorten_a_job_a_set_vouched_for() {
    let graph = run(&Scenario::new(tracks_beside(1)).lost(TEXT, 1)).await;
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Clean)],
        "the set ruled its tracks clean"
    );
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{:?}",
        graph.failure
    );
    assert!(
        graph.files.keys().any(|name| name.ends_with("foo_dr.txt")),
        "the damaged file is delivered as it stands: {:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(
        graph.graph().damaged_files().is_empty(),
        "{:?}",
        graph.graph()
    );
    assert_eq!(graph.graph().unchecked_files(), vec!["foo_dr.txt"]);

    // A repair that rebuilt a track vouches for the job too.
    let repaired = run(&Scenario::new(tracks_beside(1)).lost(0, 1).lost(TEXT, 1)).await;
    assert_eq!(
        repaired.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(
        repaired.status,
        Some(JobStatus::Complete),
        "{:?}",
        repaired.failure
    );
}

// No recovery set at all: nothing vouched for the job, and a plain file
// that lost an article beside whole ones leaves it Partial.
#[tokio::test]
async fn damage_beside_no_recovery_set_still_shortens_the_job() {
    let posts: Vec<Post> = tracks_beside(1)
        .into_iter()
        .filter(|post| !post.name.ends_with(".par2"))
        .collect();
    let graph = run(&Scenario::new(posts).lost(2, 1)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.graph().damaged_files(), vec!["foo_dr.txt"]);
}

// A set that cannot rebuild a file it describes vouches for nothing: the
// track it left short shortens the job, and so does the text file beside.
#[tokio::test]
async fn damage_a_set_describes_and_cannot_repair_still_shortens_the_job() {
    let graph = run(&Scenario::new(tracks_beside(1))
        .lost(0, 0)
        .lost(0, 1)
        .lost(TEXT, 1))
    .await;
    assert!(
        matches!(
            graph.graph().outcomes.as_slice(),
            [Some(RepairOutcome::Impossible { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert!(graph.graph().unchecked_files().is_empty());
}

// The share of a job that is recovery data follows the graph's binding: a
// PAR2 volume posted under a name that says nothing counts as recovery once
// its set holds it, as the same volume under its own name does.
#[tokio::test]
async fn a_recovery_volume_under_a_random_name_counts_as_recovery_bytes() {
    let data = payload(12 * ARTICLE + 17, 41);
    let named: Vec<Post> = std::iter::once(Post::plain("ember.mkv", data.clone()))
        .chain(par2_volumes("ember", &[("ember.mkv", &data)], 6, 3))
        .collect();
    let names = ["a3f9c1e07d", "b71e02c4aa", "5d0e9f3b12", "e62c8a7f40"];
    let renamed: Vec<Post> = std::iter::once(Post::plain("ember.mkv", data.clone()))
        .chain(
            named[1..]
                .iter()
                .zip(names)
                .map(|(post, name)| Post::plain(name, post.bytes.clone())),
        )
        .collect();

    let bytes = |outcome: &super::harness::Outcome| {
        let history = outcome.history.as_ref().expect("a history row");
        history.optional_recovery_bytes
    };
    let control = run(&Scenario::new(named)).await;
    assert_eq!(control.status, Some(JobStatus::Complete));
    let control = bytes(&control);
    assert!(control > 0, "the named volumes are recovery");

    let graph = run(&Scenario::new(renamed)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{:?}",
        graph.failure
    );
    let renamed = bytes(&graph);
    assert!(
        renamed >= control,
        "the volumes the set holds count whatever their names: {renamed} < {control}"
    );
}

// A Partial job says what it lacks, from its delivery's lists: its report,
// the record it leaves when it finishes and its history row each carry the
// same words.
#[tokio::test]
async fn a_partial_job_says_what_it_lacks() {
    let posts: Vec<Post> = tracks_beside(1)
        .into_iter()
        .filter(|post| !post.name.ends_with(".par2"))
        .collect();
    let graph = run(&Scenario::new(posts).lost(2, 1)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let said = "delivered in part; damaged: foo_dr.txt";
    let history = graph.history.as_ref().expect("a history row");
    assert_eq!(history.error_message.as_deref(), Some(said));
    let report = graph.report.as_ref().expect("the job's report");
    assert_eq!(report.status, JobStatus::Partial);
    assert_eq!(report.error.as_deref(), Some(said));

    // A set that could not rebuild a track names it unrepaired.
    let graph = run(&Scenario::new(tracks_beside(1))
        .lost(0, 0)
        .lost(0, 1)
        .lost(TEXT, 1))
    .await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let history = graph.history.as_ref().expect("a history row");
    assert_eq!(
        history.error_message.as_deref(),
        Some("delivered in part; unrepaired: 01 Kind Hearted Woman.flac; damaged: foo_dr.txt"),
    );
}

// The words name twenty files at most and count the rest.
#[tokio::test]
async fn a_partial_job_names_twenty_files_and_counts_the_rest() {
    let mut posts = vec![Post::plain("whole.mkv", payload(2 * ARTICLE, 21))];
    posts.extend((0..23u8).map(|index| {
        Post::plain(
            &format!("note {index:02}.txt"),
            payload(2 * ARTICLE, 30 + index),
        )
    }));
    let scenario = (1..=23).fold(Scenario::new(posts), |scenario, file| {
        scenario.lost(file, 1)
    });
    let graph = run(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    let named: Vec<String> = (0..20)
        .map(|index| format!("note {index:02}.txt"))
        .collect();
    let said = format!(
        "delivered in part; damaged: {}; and 3 more",
        named.join(", ")
    );
    let history = graph.history.as_ref().expect("a history row");
    assert_eq!(history.error_message.as_deref(), Some(said.as_str()));
}

// Two pieces under hex names, the first of one article and lost whole when
// `lose` is set, and an SFV, when `listed`, that lists them under names the
// post does not carry, with CRCs neither has: it names neither. `beside`
// adds a whole `.mkv` the SFV does not list either. The pieces are files 0
// and 1, the `.mkv` file 2 when posted, the SFV last.
const FIRST_PIECE: &str = "e93a07c1d25b4f68a0c3e1f7b9d24c85";
const SECOND_PIECE: &str = "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64";

fn pieces_beside_an_unrelated_sfv(beside: bool) -> Vec<Post> {
    pieces_listed(beside, true)
}

fn pieces_listed(beside: bool, listed: bool) -> Vec<Post> {
    let first = payload(ARTICLE, 51);
    let second = payload(2 * ARTICLE + 3, 52);
    let mut posts = vec![
        Post::plain(FIRST_PIECE, first.clone()),
        Post::plain(SECOND_PIECE, second.clone()),
    ];
    if beside {
        posts.push(Post::plain("feature.mkv", payload(2 * ARTICLE, 53)));
    }
    if listed {
        posts.push(listing(
            "silver.horizon.sfv",
            &[
                ("silver.horizon.7z.001", crc(&first) ^ 1),
                ("silver.horizon.7z.002", crc(&second) ^ 1),
            ],
        ));
    }
    posts
}

// An SFV that names no file vouches for nothing: a piece lost whole beside
// it shortens the job as it does with no SFV at all. Nothing whole is of a
// type people open, so the job fails; a whole `.mkv` beside the pieces
// leaves it Partial.
#[tokio::test]
async fn an_sfv_that_binds_no_file_vouches_for_nothing() {
    let graph = run(&Scenario::new(pieces_beside_an_unrelated_sfv(false)).lost(0, 0)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    assert!(
        graph.graph().unchecked_files().is_empty(),
        "{:?}",
        graph.graph().delivery_debug()
    );
    // The health the failure gives counts the listing's own bytes.
    assert_twin_but_text(
        &graph,
        &run(&Scenario::new(pieces_listed(false, false)).lost(0, 0)).await,
    );

    let beside = run(&Scenario::new(pieces_beside_an_unrelated_sfv(true)).lost(0, 0)).await;
    assert_eq!(
        beside.status,
        Some(JobStatus::Partial),
        "{:?}",
        beside.failure
    );
    assert!(
        beside.graph().unchecked_files().is_empty(),
        "{:?}",
        beside.graph().delivery_debug()
    );
    assert_twin(
        &beside,
        &run(&Scenario::new(pieces_listed(true, false)).lost(0, 0)).await,
    );
}

// The same SFV over a post that lost nothing never fails the job: an entry
// naming no file of the job fails nothing.
#[tokio::test]
async fn an_sfv_that_binds_no_file_leaves_a_whole_post_complete() {
    let graph = run(&Scenario::new(pieces_beside_an_unrelated_sfv(false))).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "{:?}",
        graph.failure
    );
    assert_twin(
        &graph,
        &run(&Scenario::new(pieces_listed(false, false))).await,
    );
}

// An SFV that agrees with a whole file vouches for nothing: a hex-named
// file beside it, lost whole, shortens the job as it does with no SFV, and
// nothing is unchecked.
#[tokio::test]
async fn an_sfv_that_binds_a_whole_file_vouches_for_nothing() {
    let feature = payload(2 * ARTICLE, 54);
    let posts = |listed: bool| {
        let mut posts = vec![
            Post::plain("feature.mkv", feature.clone()),
            Post::plain(FIRST_PIECE, payload(ARTICLE, 55)),
        ];
        if listed {
            posts.push(listing("feature.sfv", &[("feature.mkv", crc(&feature))]));
        }
        Scenario::new(posts).lost(1, 0)
    };
    let graph = run(&posts(true)).await;
    assert!(graph.graph().outcomes.is_empty(), "the SFV rules nothing");
    assert!(
        graph.graph().unchecked_files().is_empty(),
        "{:?}",
        graph.graph().delivery_debug()
    );
    assert_twin(&graph, &run(&posts(false)).await);
}
