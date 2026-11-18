// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a job's end reads beside its recovery sets: a posted file no set
// describes, the share of the job that is recovery data, and what a Partial
// job says it lacks.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Post, Scenario, par2_set, par2_volumes, payload, run};
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

// SABnzbd's rule: a set that verified its files clean or repaired them
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
        graph.graph().delivery.damaged.is_empty(),
        "{:?}",
        graph.graph().delivery
    );
    assert_eq!(graph.graph().delivery.unchecked, vec!["foo_dr.txt"]);

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
    assert_eq!(graph.graph().delivery.damaged, vec!["foo_dr.txt"]);
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
    assert!(graph.graph().delivery.unchecked.is_empty());
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
