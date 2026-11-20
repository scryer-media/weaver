// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery data is told by its bytes, whatever its name. A file under a
// recovery name whose front opens no recovery format is the plain file its
// bytes are: delivered, and payload the job's status and health count. A
// file under a name that says nothing whose front opens a recovery format
// is recovery data, used where an object reads it. The name decides only
// while the front never arrived, and then with what is posted beside it.
// RAR's own `.rev` is left out of the impostor cases: a RAR3 recovery
// volume opens on parity, which signs nothing, so under that name a front
// says nothing either way.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, par3_set, payload, run};
use crate::jobs::model::JobStatus;

// Text a person reads: no recovery format opens on it.
fn notes(len: usize) -> Vec<u8> {
    b"Release notes, line after line of plain text.\n"
        .iter()
        .copied()
        .cycle()
        .take(len)
        .collect()
}

// A PAR1 file's head and some body: its magic and its one file version.
fn par1(len: usize) -> Vec<u8> {
    let mut bytes = b"PAR\0\0\0\0\0\0\0\x01\0".to_vec();
    bytes.extend(payload(len - bytes.len(), 47));
    bytes
}

const IMPOSTORS: &[&str] = &[
    "readme.par2",
    "readme.vol00+01.par2",
    "readme.par3",
    "lantern.p01",
];

fn summary(graph: &Outcome) -> String {
    let delivery = &graph.graph().delivery;
    format!(
        "{:?} {:?} whole {:?} damaged {:?} files {:?} health {:?}",
        graph.status,
        graph.failure,
        delivery.whole,
        delivery.damaged,
        graph.files.keys().collect::<Vec<_>>(),
        graph.report.as_ref().map(|report| report.health)
    )
}

fn delivered<'g>(graph: &'g Outcome, name: &str) -> Option<&'g Vec<u8>> {
    graph
        .files
        .iter()
        .find(|(path, _)| path.ends_with(&format!("/{name}")))
        .map(|(_, bytes)| bytes)
}

// A movie, a text file under a recovery name, and a text file under the
// name of the set's index: no recovery data in the post at all.
fn impostor_posts(name: &str) -> (Vec<u8>, Vec<u8>, Vec<Post>) {
    let movie = payload(4 * ARTICLE + 17, 91);
    let text = notes(3 * ARTICLE + 5);
    let posts = vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain(name, text.clone()),
        Post::plain("lantern.par2", notes(ARTICLE)),
    ];
    (movie, text, posts)
}

#[tokio::test]
async fn a_plain_file_under_a_recovery_name_is_delivered_whole() {
    for name in IMPOSTORS {
        let (movie, text, posts) = impostor_posts(name);
        let graph = run(&Scenario::new(posts)).await;
        let case = format!("{name}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        let delivery = &graph.graph().delivery;
        assert!(delivery.whole.iter().any(|whole| whole == name), "{case}");
        assert!(delivery.damaged.is_empty(), "{case}");
        assert_eq!(delivered(&graph, name), Some(&text), "{case}");
        assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
        assert_eq!(
            delivered(&graph, "lantern.par2"),
            Some(&notes(ARTICLE)),
            "{case}"
        );
        assert_eq!(
            graph.report.as_ref().map(|r| r.health),
            Some(1000),
            "{case}"
        );
    }
}

// Its loss is the payload's: the job is short of it, and its health says so.
#[tokio::test]
async fn a_plain_file_under_a_recovery_name_that_loses_an_article_leaves_the_job_partial() {
    for name in IMPOSTORS {
        let (_, _, posts) = impostor_posts(name);
        let graph = run(&Scenario::new(posts).lost(1, 1)).await;
        let case = format!("{name}: {}", summary(&graph));
        assert_eq!(graph.status, Some(JobStatus::Partial), "{case}");
        let delivery = &graph.graph().delivery;
        assert!(
            delivery.damaged.iter().any(|damaged| damaged == name),
            "{case}"
        );
        assert!(
            delivery.whole.iter().any(|whole| whole == "lantern.mkv"),
            "{case}"
        );
        assert!(
            graph.report.as_ref().is_some_and(|r| r.health < 1000),
            "{case}"
        );
    }
}

// A front that never arrived says nothing: beside other recovery names, the
// name is recovery data, spent, and nothing of the delivery.
#[tokio::test]
async fn a_recovery_named_file_whose_first_article_is_lost_is_spent_by_its_name() {
    for name in IMPOSTORS {
        let (movie, _, posts) = impostor_posts(name);
        let graph = run(&Scenario::new(posts).lost(1, 0)).await;
        let case = format!("{name}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        assert_eq!(delivered(&graph, name), None, "{case}");
        assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
        assert_eq!(
            graph.report.as_ref().map(|r| r.health),
            Some(1000),
            "{case}"
        );
    }
}

// A plain file under an old numbered name beside a real set: the set
// repairs the movie, and the text is delivered whole beside it.
#[tokio::test]
async fn a_plain_file_under_an_old_numbered_name_beside_a_real_set_is_delivered() {
    let movie = payload(4 * ARTICLE + 17, 91);
    let text = notes(3 * ARTICLE + 5);
    let mut posts = vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("lantern.p01", text.clone()),
    ];
    posts.extend(par2_set("lantern", &[("lantern.mkv", &movie)], 2));
    let graph = run(&Scenario::new(posts).damaged(0, 1)).await;
    let case = summary(&graph);
    assert!(graph.completed(), "{case}");
    assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
    assert_eq!(delivered(&graph, "lantern.p01"), Some(&text), "{case}");
}

// Recovery under a name that says nothing, with no extension, is found by
// its bytes and repairs a damaged file: PAR2 and PAR3 alike. (RAR5 recovery
// volumes under such names: `obfuscated_revs`.)
#[tokio::test]
async fn nameless_recovery_of_each_kind_repairs_a_damaged_file() {
    for kind in ["par2", "par3"] {
        let movie = payload(4 * ARTICLE + 17, 93);
        let set = match kind {
            "par2" => par2_set("lantern", &[("lantern.mkv", &movie)], 2),
            _ => par3_set("lantern", &[("lantern.mkv", &movie)], 2),
        };
        let mut posts = vec![Post::plain("lantern.mkv", movie.clone())];
        for (at, post) in set.into_iter().enumerate() {
            posts.push(Post {
                bytes: post.bytes,
                ..Post::plain(&format!("e81b6d3f0a4c97{at:02}"), Vec::new())
            });
        }
        let graph = run(&Scenario::new(posts).damaged(0, 1)).await;
        let case = format!("{kind}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        assert_eq!(graph.files.len(), 1, "{case}");
        assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
    }
}

// No object reads PAR1, but its files are recovery data all the same: under
// a name that says nothing, one is spent, and no file of the delivery.
#[tokio::test]
async fn a_nameless_par1_file_is_recovery_data_and_no_file_of_the_delivery() {
    let movie = payload(4 * ARTICLE + 17, 95);
    let posts = vec![
        Post::plain("lantern.mkv", movie.clone()),
        Post::plain("5f0c2e9b7a314d68", par1(2 * ARTICLE + 9)),
    ];
    let graph = run(&Scenario::new(posts)).await;
    let case = summary(&graph);
    assert!(graph.completed(), "{case}");
    assert_eq!(graph.files.len(), 1, "{case}");
    assert_eq!(delivered(&graph, "lantern.mkv"), Some(&movie), "{case}");
}
