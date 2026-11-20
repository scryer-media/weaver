// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A file under two recovery sets of different kinds, a PAR2 set and a PAR3
// set, one short of blocks and the other with enough, and two of its
// articles lost or damaged. The short set rules impossible; the other asks
// for its recovery volumes, which the job fetches only once a set asks, and
// the job completes on it in either order the articles land. When the set
// with enough blocks cannot get them, the job ends as it did, and does not
// wait for blocks nothing will bring.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes, par3_set, payload, run};
use crate::jobs::model::JobStatus;

// The file, a PAR2 index with up to three volumes sharing `par2_blocks`, and a
// PAR3 set of `par3_blocks`.
fn covered(par2_blocks: u32, par3_blocks: u64) -> Vec<Post> {
    let data = payload(8 * ARTICLE, 91);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes(
        "lantern",
        &[("lantern.bin", &data)],
        par2_blocks,
        par2_blocks.min(3),
    ));
    posts.extend(par3_set(
        "lantern.p3",
        &[("lantern.bin", &data)],
        par3_blocks,
    ));
    posts
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings
    )
}

// Two articles of the file lost, or two damaged, forward or backwards.
fn holes(scenario: Scenario, lost: bool, backwards: bool) -> Scenario {
    let scenario = if lost {
        scenario.lost(0, 2).lost(0, 5)
    } else {
        scenario.damaged(0, 2).damaged(0, 5)
    };
    if backwards {
        scenario.backwards()
    } else {
        scenario
    }
}

#[tokio::test]
async fn a_short_set_does_not_end_a_job_another_kind_repairs() {
    // (PAR2 blocks, PAR3 blocks): each kind the short one in turn.
    for (par2, par3) in [(1, 4), (4, 1)] {
        for lost in [false, true] {
            for backwards in [false, true] {
                let scenario = holes(Scenario::new(covered(par2, par3)), lost, backwards);
                let graph = run(&scenario).await;
                assert!(
                    graph.completed(),
                    "par2 {par2} par3 {par3} lost {lost} backwards {backwards}: {}",
                    summary(&graph)
                );
            }
        }
    }
}

// The PAR2 set has the blocks, but every article of its volumes is lost:
// nothing can bring them, the short PAR3 set rules impossible, and the job
// fails as one file with nothing usable does. The run returning at all is
// the no-wait half of the check.
#[tokio::test]
async fn a_set_whose_blocks_cannot_arrive_does_not_hold_the_job() {
    for backwards in [false, true] {
        let mut scenario = holes(Scenario::new(covered(4, 1)), true, backwards);
        let spec = scenario.spec();
        for (file, entry) in spec.files.iter().enumerate() {
            if entry.filename.contains(".vol") && entry.filename.ends_with(".par2") {
                for ordinal in 0..entry.segments.len() {
                    scenario = scenario.lost(file as u32, ordinal as u32);
                }
            }
        }
        let graph = run(&scenario).await;
        assert!(
            matches!(graph.status, Some(JobStatus::Failed { .. })),
            "backwards {backwards}: {}",
            summary(&graph)
        );
    }
}

// A named PAR2 volume whose first article is lost is still claimed by its
// name, its blocks count, and the job repairs a lost data article.
#[tokio::test]
async fn a_named_volume_with_a_lost_front_still_repairs() {
    for backwards in [false, true] {
        let data = payload(8 * ARTICLE, 92);
        let mut posts = vec![Post::plain("lantern.bin", data.clone())];
        posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 4, 1));
        let mut scenario = Scenario::new(posts).lost(0, 2).lost(2, 0);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        assert!(
            graph.completed(),
            "backwards {backwards}: {}",
            summary(&graph)
        );
    }
}

// Two PAR2 sets over the same file, sliced apart so each is its own set:
// `thin` sliced at two articles with one recovery block, and `lantern`
// sliced at one article with four blocks in three volumes. Two articles of
// the file damaged or lost land in two of the thin set's slices and two of
// the other's: the thin set rules impossible, and the other can rebuild
// both once it asks for its volumes.
fn two_par2_sets() -> (Vec<u8>, Vec<Post>) {
    let data = payload(8 * ARTICLE, 93);
    let files = [("lantern.bin", data.as_slice())];
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(super::harness::par2_volumes_sliced(
        "thin",
        &files,
        2 * ARTICLE as u64,
        1,
        1,
    ));
    posts.extend(par2_volumes("lantern", &files, 4, 3));
    (data, posts)
}

// The shape the job ended early in: the thin set rules impossible while the
// other still waits for blocks it can have. The job waits for them, and
// completes with the file rebuilt.
#[tokio::test]
async fn a_thin_par2_set_does_not_end_a_job_a_larger_par2_set_repairs() {
    let mut short = Vec::new();
    for lost in [false, true] {
        for backwards in [false, true] {
            let (data, posts) = two_par2_sets();
            let graph = run(&holes(Scenario::new(posts), lost, backwards)).await;
            if !graph.completed() || !graph.files.values().any(|bytes| *bytes == data) {
                short.push(format!(
                    "lost {lost} backwards {backwards}: {}",
                    summary(&graph)
                ));
            }
        }
    }
    assert!(short.is_empty(), "{short:#?}");
}

// The mirror: the larger set's volumes never arrive, so nothing is left to
// wait for and the thin set's ruling stands. The job fails, as it does
// today, in the thin set's words.
#[tokio::test]
async fn two_par2_sets_with_nothing_left_to_wait_for_fail() {
    // Today's words for each order: the set that holds the file names it.
    for (backwards, words) in [
        (
            false,
            "PAR2 recovery failed for 1 set(s): thin.par2 (lantern.bin): not repairable: 2 damaged slices, only 1 recovery blocks reachable",
        ),
        (
            true,
            "PAR2 recovery failed for 1 set(s): lantern.par2 (lantern.bin): not repairable: 2 damaged slices, only 0 recovery blocks reachable",
        ),
    ] {
        let (_, posts) = two_par2_sets();
        let mut scenario = holes(Scenario::new(posts), true, backwards);
        let spec = scenario.spec();
        for (file, entry) in spec.files.iter().enumerate() {
            if entry.filename.starts_with("lantern.vol") {
                for ordinal in 0..entry.segments.len() {
                    scenario = scenario.lost(file as u32, ordinal as u32);
                }
            }
        }
        let graph = run(&scenario).await;
        assert_eq!(
            graph.status,
            Some(JobStatus::Failed {
                error: words.to_string()
            }),
            "backwards {backwards}: {}",
            summary(&graph)
        );
    }
}

// Two files, each under its own PAR2 set: `ember.mkv` under a set of one
// block that cannot rebuild the two articles it lost or had damaged, and
// `quill.mkv` under a set of four blocks that rebuilds its two. The second
// file comes whole and is a type people open, so the job is partial with it
// repaired; the failure names the set that failed in either order.
#[tokio::test]
async fn a_thin_set_over_one_file_does_not_cost_a_file_another_set_repairs() {
    let mut short = Vec::new();
    for lost in [false, true] {
        for backwards in [false, true] {
            let ember = payload(6 * ARTICLE, 94);
            let quill = payload(6 * ARTICLE, 95);
            let mut posts = vec![
                Post::plain("ember.mkv", ember.clone()),
                Post::plain("quill.mkv", quill.clone()),
            ];
            posts.extend(par2_volumes("ember", &[("ember.mkv", &ember)], 1, 1));
            posts.extend(par2_volumes("quill", &[("quill.mkv", &quill)], 4, 3));
            let mut scenario = Scenario::new(posts);
            for (file, ordinal) in [(0, 1), (0, 4), (1, 1), (1, 4)] {
                scenario = if lost {
                    scenario.lost(file, ordinal)
                } else {
                    scenario.damaged(file, ordinal)
                };
            }
            if backwards {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            if graph.status != Some(JobStatus::Partial)
                || !graph.files.values().any(|bytes| *bytes == quill)
            {
                short.push(format!(
                    "lost {lost} backwards {backwards}: {}",
                    summary(&graph)
                ));
            }
        }
    }
    assert!(short.is_empty(), "{short:#?}");
}

// The mirror: `quill.mkv`'s recovery volumes never arrive, so neither set
// can rebuild its file and nothing usable is whole. The job fails, and its
// words name each set by the file it was posted under, with the file it
// left short, in either order the articles land. Backwards, quill's set is
// ruled on one damaged slice of the two, its ruling standing before the
// other lost article lands, so only ember's numbers are pinned.
#[tokio::test]
async fn two_sets_over_two_files_that_both_fail_name_each_set() {
    let mut wrong = Vec::new();
    for backwards in [false, true] {
        let ember = payload(6 * ARTICLE, 94);
        let quill = payload(6 * ARTICLE, 95);
        let mut posts = vec![
            Post::plain("ember.mkv", ember.clone()),
            Post::plain("quill.mkv", quill.clone()),
        ];
        posts.extend(par2_volumes("ember", &[("ember.mkv", &ember)], 1, 1));
        posts.extend(par2_volumes("quill", &[("quill.mkv", &quill)], 4, 3));
        let mut scenario = Scenario::new(posts);
        for (file, ordinal) in [(0, 1), (0, 4), (1, 1), (1, 4)] {
            scenario = scenario.lost(file, ordinal);
        }
        let spec = scenario.spec();
        for (file, entry) in spec.files.iter().enumerate() {
            if entry.filename.starts_with("quill.vol") {
                for ordinal in 0..entry.segments.len() {
                    scenario = scenario.lost(file as u32, ordinal as u32);
                }
            }
        }
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        let named = match &graph.status {
            Some(JobStatus::Failed { error }) => {
                error.starts_with("PAR2 recovery failed for 2 set(s): ")
                    && error.contains(
                        "ember.par2 (ember.mkv): not repairable: 2 damaged slices, only 1 recovery blocks reachable",
                    )
                    && error.contains("quill.par2 (quill.mkv): not repairable: ")
            }
            _ => false,
        };
        if !named {
            wrong.push(format!("backwards {backwards}: {}", summary(&graph)));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

// The first shape one level down: the file and both PAR2 sets are the
// members of one stored RAR volume posted as `lantern.rar`, its articles in
// posted order and backwards. Backwards, the damage sits further in: an
// early article of the volume damaged and landing before its first leaves
// the volume a leaf with no object below it, with or without a PAR2 set
// inside (a separate defect, outside this shape).
#[tokio::test]
async fn a_thin_par2_set_below_the_root_does_not_end_a_job_a_larger_set_repairs() {
    let mut short = Vec::new();
    for backwards in [false, true] {
        let (data, posts) = two_par2_sets();
        let entries: Vec<(&str, Vec<u8>)> = posts
            .iter()
            .map(|post| (post.name.as_str(), post.bytes.clone()))
            .collect();
        let outer = crate::pipeline::tests::direct_store::multi_member_store_set(&entries, 1)
            .remove(0)
            .1;
        let inside = super::deep::articles_inside(&outer, &data);
        let (first, second) = if backwards { (4, 6) } else { (1, 4) };
        let mut scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)])
            .damaged(0, inside[first])
            .damaged(0, inside[second]);
        if backwards {
            scenario = scenario.backwards();
        }
        let graph = run(&scenario).await;
        if !graph.completed() || !graph.files.values().any(|bytes| *bytes == data) {
            short.push(format!("backwards {backwards}: {}", summary(&graph)));
        }
    }
    assert!(short.is_empty(), "{short:#?}");
}

// A RAR5 volume whose recovery record of one unit cannot rebuild the two
// articles lost inside it, beside a PAR2 set of eight blocks over the
// volume: the record rules short, and the set rebuilds the volume whole.
#[tokio::test]
async fn a_short_recovery_record_does_not_end_a_job_a_par2_set_repairs() {
    let mut short = Vec::new();
    for lost in [false, true] {
        let body = payload(30 * ARTICLE + 7, 96);
        let volumes =
            crate::pipeline::tests::direct_store::protected_store_set("beacon.bin", &body, 1, 1);
        let files: Vec<(&str, &[u8])> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let mut posts = super::rar::posts(volumes.clone());
        posts.extend(par2_volumes("beacon", &files, 8, 3));
        let mut scenario = Scenario::new(posts);
        scenario = if lost {
            scenario.lost(0, 2).lost(0, 20)
        } else {
            scenario.damaged(0, 2).damaged(0, 20)
        };
        let graph = run(&scenario).await;
        if !graph.completed() || !graph.files.values().any(|bytes| *bytes == body) {
            short.push(format!("lost {lost}: {}", summary(&graph)));
        }
    }
    assert!(short.is_empty(), "{short:#?}");
}

// A stored RAR5 set of four volumes, three of them lost or damaged an
// article each, beside one named `.rev` volume that can rebuild one and a
// PAR2 set of eight blocks over the volumes: the `.rev` set rules
// impossible, and the PAR2 set rebuilds the volumes whole, in either order.
#[tokio::test]
async fn a_short_rev_set_does_not_end_a_job_a_par2_set_repairs() {
    let mut short = Vec::new();
    for lost in [false, true] {
        for backwards in [false, true] {
            let body = super::rar_rev::body();
            let volumes = crate::pipeline::tests::direct_store::single_member_store_set(
                "beacon.bin",
                &body,
                4,
            );
            let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
            let files: Vec<(&str, &[u8])> = volumes
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
                .collect();
            let mut posts = super::rar::posts(volumes.clone());
            posts.extend(super::rar_rev::rar5_revs("silver.horizon", &bytes, 1));
            posts.extend(par2_volumes("beacon", &files, 8, 3));
            let mut scenario = (1..4u32).fold(Scenario::new(posts), |scenario, volume| {
                if lost {
                    scenario.lost(volume, 1)
                } else {
                    scenario.damaged(volume, 1)
                }
            });
            if backwards {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            if !graph.completed() || !graph.files.values().any(|bytes| *bytes == body) {
                short.push(format!(
                    "lost {lost} backwards {backwards}: {}",
                    summary(&graph)
                ));
            }
        }
    }
    assert!(short.is_empty(), "{short:#?}");
}
