// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The feed's critical line counts the recovery the graph has bound, not
// only the recovery the post names. A PAR2 set posted under names that say
// nothing is bound by its own bytes as its fronts arrive; from then on its
// volumes lower the line as named volumes would. A post whose recovery
// never binds keeps the line its names draw.

use super::harness::{Outcome, Post, Scenario, par2_volumes_sliced, payload, run};
use crate::jobs::model::JobStatus;

// One article, one recovery block: a lost article is a lost block.
const ARTICLE: usize = 16 * 1024;
const FILES: u32 = 10;
const ARTICLES: u32 = 10;

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

fn data(file: u32) -> Vec<u8> {
    payload(ARTICLES as usize * ARTICLE, file as u8)
}

// Ten data files of ten articles each, then a PAR2 set over them of
// `blocks` blocks in five volumes, its index and volumes under names that
// say nothing.
fn posts(blocks: u32) -> Vec<Post> {
    let files: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|file| (format!("lantern{file:02}.mkv"), data(file)))
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let set = par2_volumes_sliced("lantern", &named, ARTICLE as u64, blocks, 5);
    assert_eq!(set.len(), 6, "an index and five volumes");
    for (at, post) in set.into_iter().enumerate() {
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(&format!("q8z{at}w.bin"), Vec::new())
        });
    }
    posts
}

// The bytes of the files from `from` on, as the post cuts them.
fn bytes_from(scenario: &Scenario, from: usize) -> u64 {
    scenario.spec().files[from..]
        .iter()
        .flat_map(|file| &file.segments)
        .map(|segment| u64::from(segment.bytes))
        .sum()
}

// `lost` articles of each data file lost, none of them its first.
fn losing(scenario: Scenario, lost: u32) -> Scenario {
    (0..FILES)
        .flat_map(|file| (1..=lost).map(move |ordinal| (file, ordinal)))
        .fold(scenario, |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        })
}

fn health_milli(total: u64, failed: u64) -> u64 {
    (total - failed) * 1000 / total
}

// The worked case: a hundred data articles, twenty-five recovery blocks
// posted under names that say nothing, twenty data articles lost. By the
// names the post declares no recovery and is held to 85%, which its health
// of 84% is below; by what the graph binds the line is 75%, and the set
// repairs the twenty blocks.
#[tokio::test]
async fn obfuscated_recovery_that_binds_lowers_the_line_and_the_post_repairs() {
    let scenario = losing(Scenario::new(posts(25)).framed(ARTICLE), 2);
    let total = bytes_from(&scenario, 0);
    let recovery = bytes_from(&scenario, FILES as usize);
    let lost = 2 * u64::from(FILES) * ARTICLE as u64;
    let health = health_milli(total, lost);
    let critical = u64::from(super::harness::critical_health_milli(total, recovery));
    assert!(health < 850, "below the line the names draw: {health}");
    assert!(
        health >= critical,
        "above the line the set draws: {health} {critical}"
    );

    let graph = run(&scenario).await;
    let case = summary(&graph);
    assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
    for file in 0..FILES {
        let name = format!("lantern{file:02}.mkv");
        let (_, delivered) = graph
            .files
            .iter()
            .find(|(path, _)| path.ends_with(&name))
            .unwrap_or_else(|| panic!("{name} delivered: {case}"));
        assert!(*delivered == data(file), "{name}: {case}");
    }
}

// The articles asked for, in order, as a row compares them.
fn asked(graph: &Outcome) -> String {
    format!("{:?}", graph.order)
}

// A truly dead release beside the same obfuscated set: every data article
// but each file's first is gone, far past what twenty-five blocks rebuild.
// The post is still given up: no data file but the first is asked past the
// first article it lost. The hold on the feed's verdict waits only for
// first articles, which lead the payload, so it costs at most one article
// per file that might yet bind: the run asks at most today's 59 articles
// and one more per posted file, and when every first article arrives at
// once, as here, exactly today's 59 in today's order.
#[tokio::test]
async fn a_dead_post_beside_obfuscated_recovery_is_still_given_up() {
    const TODAY: usize = 59;
    let posted = FILES as usize + 6;
    for refused in [false, true] {
        let mut scenario = losing(Scenario::new(posts(25)).framed(ARTICLE), ARTICLES - 1);
        // Each recovery file's first article is refused once and asked again
        // later: the feed's verdict may come while they are still out.
        if refused {
            scenario =
                (FILES..FILES + 6).fold(scenario, |scenario, file| scenario.refused_once(file, 0));
        }
        let graph = run(&scenario).await;
        let case = format!("refused {refused}: {} {}", summary(&graph), asked(&graph));
        assert!(
            matches!(graph.status, Some(JobStatus::Failed { .. })),
            "{case}"
        );
        assert!(graph.order.len() <= TODAY + posted, "{case}");
        for file in 1..FILES {
            assert!(
                graph
                    .order
                    .iter()
                    .all(|(index, ordinal)| *index != file || *ordinal <= 1),
                "data file {file} asked past its first loss: {case}"
            );
        }
        if !refused {
            assert_eq!(graph.order.len(), TODAY, "{case}");
            assert_eq!(asked(&graph), DEAD_ORDER, "{case}");
        }
    }
}

const DEAD_ORDER: &str = "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6), (0, 7), (0, 8), (0, 9), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1), (11, 1), (11, 2), (11, 3), (11, 4), (11, 5), (12, 1), (12, 2), (12, 3), (12, 4), (12, 5), (13, 1), (13, 2), (13, 3), (13, 4), (13, 5), (14, 1), (14, 2), (14, 3), (14, 4), (14, 5), (15, 1), (15, 2), (15, 3), (15, 4), (15, 5)]";

// The worked case with every recovery file posted in one article, and each
// of those lost: nothing of the set arrives, nothing binds, and the line
// stays the 85% the names draw. The feed gives the post up on the probe's
// round in today's words, having asked today's articles in today's order.
#[tokio::test]
async fn obfuscated_recovery_whose_fronts_are_lost_keeps_the_line_the_names_draw() {
    let mut posts = posts(25);
    for post in &mut posts[FILES as usize..] {
        post.least_article = post.bytes.len();
    }
    let scenario = (FILES..FILES + 6).fold(
        losing(Scenario::new(posts).framed(ARTICLE), 2),
        |scenario, file| scenario.lost(file, 0),
    );
    for file in &scenario.spec().files[FILES as usize..] {
        assert_eq!(file.segments.len(), 1, "one article per recovery file");
    }
    let graph = run(&scenario).await;
    let case = summary(&graph);
    assert_eq!(
        graph.failure.as_deref(),
        Some("health probe: 11 of 11 samples missing; health 0.0% below critical 85.0%"),
        "{case}"
    );
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{case}"
    );
    assert_eq!(
        asked(&graph),
        "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)]",
        "{case}"
    );
}

// The same post with its set under the names the creator gave it. The
// names count every recovery file, so what the graph binds adds nothing:
// with forty data articles lost, and with ninety, the run asks today's
// articles in today's order and ends in today's words.
#[tokio::test]
async fn a_normally_named_set_is_weighed_as_today() {
    let files: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|file| (format!("lantern{file:02}.mkv"), data(file)))
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par2_volumes_sliced(
        "lantern",
        &named,
        ARTICLE as u64,
        25,
        5,
    ));
    let rows = [
        (4, 80, NAMED_ORDER_FORTY),
        (ARTICLES - 1, 90, NAMED_ORDER_NINETY),
    ];
    for (lost, damaged, order) in rows {
        let posts = posts
            .iter()
            .map(|post| Post {
                bytes: post.bytes.clone(),
                ..Post::plain(&post.name, Vec::new())
            })
            .collect();
        let scenario = losing(Scenario::new(posts).framed(ARTICLE), lost);
        let graph = run(&scenario).await;
        let case = format!("lost {lost}: {}", summary(&graph));
        let failure = format!(
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern00.mkv, lantern01.mkv, \
             lantern02.mkv, lantern03.mkv, lantern04.mkv, lantern05.mkv, lantern06.mkv, \
             lantern07.mkv, lantern08.mkv, lantern09.mkv): not repairable: {damaged} damaged \
             slices, only 25 recovery blocks reachable"
        );
        assert_eq!(graph.failure.as_deref(), Some(failure.as_str()), "{case}");
        assert_eq!(asked(&graph), order, "{case}");
    }
}

const NAMED_ORDER_FORTY: &str = "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (11, 1), (11, 2), (11, 3), (11, 4), (11, 5), (0, 6), (0, 7), (0, 8), (0, 9), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (12, 1), (12, 2), (12, 3), (12, 4), (12, 5), (1, 6), (1, 7), (1, 8), (1, 9), (2, 1), (13, 1), (13, 2), (13, 3), (13, 4), (13, 5), (14, 1), (14, 2), (14, 3), (14, 4), (14, 5), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)]";

const NAMED_ORDER_NINETY: &str = "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6), (0, 7), (0, 8), (0, 9), (11, 1), (11, 2), (11, 3), (11, 4), (11, 5), (12, 1), (12, 2), (12, 3), (12, 4), (12, 5), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (13, 1), (13, 2), (13, 3), (13, 4), (13, 5), (14, 1), (14, 2), (14, 3), (14, 4), (14, 5), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)]";

// Ten data files of one article each and a PAR2 set over them of `blocks`
// blocks in five volumes, the set under names that say nothing, or under
// the names the creator gave it.
fn singles(blocks: u32, named: bool) -> Vec<Post> {
    let files: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|file| {
            (
                format!("lantern{file:02}.mkv"),
                payload(ARTICLE, 40 + file as u8),
            )
        })
        .collect();
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let set = par2_volumes_sliced("lantern", &described, ARTICLE as u64, blocks, 5);
    assert_eq!(set.len(), 6, "an index and five volumes");
    for (at, post) in set.into_iter().enumerate() {
        let name = if named {
            post.name.clone()
        } else {
            format!("q8z{at}w.bin")
        };
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(&name, Vec::new())
        });
    }
    posts
}

// Every data file and the set's index lose their only or first article,
// and so do the first two volumes. The other three volumes' first articles
// are refused once, so they are still to come when the thirteenth loss is
// heard, and each of them then loses every article past its third.
fn fronts_lost(posts: Vec<Post>) -> Scenario {
    let scenario = (0..FILES + 3).fold(Scenario::new(posts).framed(ARTICLE), |scenario, file| {
        scenario.lost(file, 0)
    });
    let lengths: Vec<u32> = scenario.spec().files[FILES as usize + 3..]
        .iter()
        .map(|file| file.segments.len() as u32)
        .collect();
    (FILES + 3..FILES + 6)
        .zip(lengths)
        .fold(scenario, |scenario, (file, length)| {
            (3..length).fold(scenario.refused_once(file, 0), |scenario, ordinal| {
                scenario.lost(file, ordinal)
            })
        })
}

// Ten data files of ten articles and a listing of their checksums.
fn listed() -> Vec<Post> {
    let files: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|file| (format!("lantern{file:02}.mkv"), data(file)))
        .collect();
    let entries: Vec<(&str, u32)> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), super::sfv::crc(bytes)))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.push(super::sfv::listing("lantern.sfv", &entries));
    posts
}

// The gate's worked case. The set is posted under names that say nothing,
// and thirteen of the sixteen posted files lose their first article: every
// data file, the index and two volumes. That is 81% of the files, past the
// gate's 80%, and by the names nothing is recovery, so the 582 KB the lost
// files hold outweigh no recovery at all: the gate gave the post up while
// three volumes' first articles were still to come, and the two volumes
// that lost theirs were never fetched past it. The three left lose all but
// three articles each, so the repair needs the two that were dropped.
//
// Now the verdict waits for those three first articles: were they recovery,
// their 623 KB would outweigh the lost files. They arrive and bind, the
// gate weighs them as recovery, as it weighs the same set under its own
// names, and the post is fetched and repaired.
#[tokio::test]
async fn obfuscated_recovery_still_to_come_holds_the_first_article_gate() {
    let scenario = fronts_lost(singles(60, false));
    let graph = run(&scenario).await;
    let case = format!("{} {}", summary(&graph), asked(&graph));
    assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
    for file in 0..FILES {
        let name = format!("lantern{file:02}.mkv");
        let (_, delivered) = graph
            .files
            .iter()
            .find(|(path, _)| path.ends_with(&name))
            .unwrap_or_else(|| panic!("{name} delivered: {case}"));
        assert!(
            *delivered == payload(ARTICLE, 40 + file as u8),
            "{name}: {case}"
        );
    }
    // The two volumes whose first article was lost are fetched past it.
    for file in [FILES + 1, FILES + 2] {
        assert!(
            graph
                .order
                .iter()
                .any(|(index, ordinal)| *index == file && *ordinal > 0),
            "volume {file} fetched past its lost front: {case}"
        );
    }
}

// The same post under the set's own names: the gate weighs it by the names
// and lets it through, as before.
#[tokio::test]
async fn the_named_twin_of_the_gate_case_is_weighed_as_before() {
    let graph = run(&fronts_lost(singles(60, true))).await;
    let case = summary(&graph);
    assert_eq!(graph.status, Some(JobStatus::Complete), "{case}");
    assert_eq!(asked(&graph), NAMED_GATE_ORDER, "{case}");
}

const NAMED_GATE_ORDER: &str = "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (13, 0), (14, 0), (15, 0), (11, 1), (11, 2), (11, 3), (11, 4), (11, 5), (11, 6), (11, 7), (11, 8), (11, 9), (11, 10), (11, 11), (11, 12), (12, 1), (12, 2), (12, 3), (12, 4), (12, 5), (12, 6), (12, 7), (12, 8), (12, 9), (12, 10), (12, 11), (12, 12), (13, 1), (13, 2), (13, 3), (13, 4), (13, 5), (13, 6), (13, 7), (13, 8), (13, 9), (13, 10), (13, 11), (13, 12), (14, 1), (14, 2), (14, 3), (14, 4), (14, 5), (14, 6), (14, 7), (14, 8), (14, 9), (14, 10), (14, 11), (14, 12), (15, 1), (15, 2), (15, 3), (15, 4), (15, 5), (15, 6), (15, 7), (15, 8), (15, 9), (15, 10), (15, 11), (15, 12)]";

// The gate's case with a set of five blocks: the three volumes still to
// come bring 57 KB, which cannot outweigh the 205 KB lost, so the gate does
// not wait for them. Under names that say nothing and under the set's own,
// the run asks the articles it asked before, in that order, and fails in
// the same words.
#[tokio::test]
async fn a_set_too_small_to_save_the_gate_case_is_given_up_as_before() {
    let rows = [
        (
            false,
            "PAR2 recovery failed for 1 set(s): q8z3w.bin (lantern00.mkv, lantern01.mkv, lantern02.mkv, lantern03.mkv, lantern04.mkv, lantern05.mkv, lantern06.mkv, lantern07.mkv, lantern08.mkv, lantern09.mkv, q8z0w.bin, q8z1w.bin, q8z2w.bin): not repairable: 10 damaged slices, only 3 recovery blocks reachable",
            "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (13, 0), (14, 0), (15, 0), (15, 1), (13, 1), (14, 1)]",
        ),
        (
            true,
            "PAR2 recovery failed for 1 set(s): lantern.vol2+1.par2 (lantern00.mkv, lantern01.mkv, lantern02.mkv, lantern03.mkv, lantern04.mkv, lantern05.mkv, lantern06.mkv, lantern07.mkv, lantern08.mkv, lantern09.mkv, lantern.par2): not repairable: 10 damaged slices, only 3 recovery blocks reachable",
            "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (13, 0), (14, 0), (15, 0), (13, 1), (14, 1), (15, 1)]",
        ),
    ];
    for (named, failure, order) in rows {
        let graph = run(&fronts_lost(singles(5, named))).await;
        let case = format!("named {named}: {}", summary(&graph));
        assert_eq!(graph.failure.as_deref(), Some(failure), "{case}");
        assert_eq!(asked(&graph), order, "{case}");
    }
}

// A listing of checksums binds as a set, but carries nothing to repair
// with: the feed does not count it as recovery. Every data file loses its
// second article; with no recovery the line is the 85% the names draw, and
// the run asks the articles it asked before the graph's bindings were
// counted, and fails in the same words. Counted as recovery, the listing's
// 250 bytes drew the line at 99.9%, and the post was given up at its first
// loss.
#[tokio::test]
async fn a_checksum_listing_is_not_counted_as_recovery() {
    let graph = run(&losing(Scenario::new(listed()).framed(ARTICLE), 1)).await;
    let case = summary(&graph);
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "SFV recovery failed for 1 set(s): lantern.sfv (lantern00.mkv, lantern01.mkv, lantern02.mkv, lantern03.mkv, lantern04.mkv, lantern05.mkv, lantern06.mkv, lantern07.mkv, lantern08.mkv, lantern09.mkv): not repairable: 10 damaged slices, only 0 recovery blocks reachable"
        ),
        "{case}"
    );
    assert_eq!(
        asked(&graph),
        "[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (0, 6), (0, 7), (0, 8), (0, 9), (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (2, 1), (2, 2), (2, 3), (2, 4), (2, 5), (2, 6), (2, 7), (2, 8), (2, 9), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)]",
        "{case}"
    );
}
