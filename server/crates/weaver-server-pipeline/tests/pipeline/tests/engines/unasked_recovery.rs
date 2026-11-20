// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An old-style RAR set, `.rar` then `.r00` on, whose follow-on volumes are
// posted under their true names and whose first volume is posted under
// another stem and the `partNN.rar` naming: `Lantern.Beacon.part01.rar`
// beside `lantern_beacon.r00`. Its PAR2 set, over the true names, is
// posted under dashed names only: the index as `Lantern.Beacon.vol-01.par2`
// and its volumes from `vol-02`. Nothing is lost at download, so no
// article asks for recovery: the set as named cannot complete, a store
// with no first volume beside a first volume with no follow-ons, and only
// the PAR2 set, or the first volume's yEnc header name, holds the true
// name that joins them. The job completes, as it does with the true names
// posted.

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use crate::pipeline::tests::direct_store::single_member_rar4_store_set;

const FRAME: usize = 4096;
const STEM: &str = "lantern_beacon";

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} kinds {:?} rulings {:?} delivered {:?}",
        graph.status,
        graph.failure,
        kinds(graph),
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

// The compressed set: four volumes of one LZ member, as `rar` wrote them.
fn compressed() -> Vec<Vec<u8>> {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles"
    );
    (1..=4)
        .map(|part| std::fs::read(format!("{dir}/rar4_lz_four.part0{part}.rar")).unwrap())
        .collect()
}

// The stored set: four volumes of one stored member.
fn stored() -> Vec<Vec<u8>> {
    single_member_rar4_store_set("lantern.bin", &payload(12 * FRAME + 77, 41), 4)
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect()
}

fn true_name(volume: usize) -> String {
    match volume {
        0 => format!("{STEM}.rar"),
        n => format!("{STEM}.r{:02}", n - 1),
    }
}

// The volumes, the first under `first`, then the PAR2 set's volumes over
// the true names under the dashed names, no index. With `changed`, the
// first volume arrives with one byte in its middle not the byte its set
// describes, every article passing its checksum, as the post that found
// this did.
fn posts(volumes: &[Vec<u8>], first: &str, changed: bool) -> Vec<Post> {
    posts_sliced(volumes, first, changed, FRAME as u64, false)
}

// The same, the set sliced at `slice`; with `garbage`, two more files
// named as the set's volumes hold no packet of it, as two of the nine the
// post that found this did.
fn posts_sliced(
    volumes: &[Vec<u8>],
    first: &str,
    changed: bool,
    slice: u64,
    garbage: bool,
) -> Vec<Post> {
    let files: Vec<(String, &[u8])> = volumes
        .iter()
        .enumerate()
        .map(|(at, bytes)| (true_name(at), bytes.as_slice()))
        .collect();
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), *bytes))
        .collect();
    let set = par2_volumes_sliced("Lantern.Beacon", &described, slice, 6, 3);
    let mut posts: Vec<Post> = volumes
        .iter()
        .enumerate()
        .map(|(at, bytes)| {
            let mut bytes = bytes.clone();
            if at == 0 && changed {
                let middle = bytes.len() / 2;
                bytes[middle] ^= 0x5a;
            }
            let name = if at == 0 {
                first.to_string()
            } else {
                true_name(at)
            };
            Post::plain(&name, bytes)
        })
        .collect();
    // The post that found this named its index `vol-01` and its volumes
    // from `vol-02`: the index is posted, under a volume's name.
    for (at, post) in set.into_iter().enumerate() {
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(
                &format!("Lantern.Beacon.vol-{:02}.par2", at + 1),
                Vec::new(),
            )
        });
    }
    if garbage {
        for (at, len) in [(5, 300_000), (6, 900_000)] {
            posts.push(Post::plain(
                &format!("Lantern.Beacon.vol-{at:02}.par2"),
                payload(len, at as u8),
            ));
        }
    }
    posts
}

async fn completes_as_under_true_names(volumes: Vec<Vec<u8>>, format: &str) {
    let truth = run(&Scenario::new(posts(&volumes, &true_name(0), false)).framed(FRAME)).await;
    assert!(
        truth.completed(),
        "{format} true names: {}",
        summary(&truth)
    );
    assert!(!truth.files.is_empty(), "{format}: {}", summary(&truth));
    for changed in [false, true] {
        for backwards in [false, true] {
            let mut scenario =
                Scenario::new(posts(&volumes, "Lantern.Beacon.part01.rar", changed)).framed(FRAME);
            if backwards {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            let case = format!(
                "{format} changed {changed} backwards {backwards}: {}",
                summary(&graph)
            );
            assert!(graph.completed(), "{case}");
            assert_eq!(graph.files, truth.files, "{case}");
        }
    }
}

#[tokio::test]
async fn a_compressed_first_volume_under_another_stem_is_joined_by_unasked_par2() {
    completes_as_under_true_names(compressed(), "compressed").await;
}

#[tokio::test]
async fn a_stored_first_volume_under_another_stem_is_joined_by_unasked_par2() {
    completes_as_under_true_names(stored(), "stored").await;
}

// The posts that found this carried the true name in the first volume's
// yEnc header (`=ybegin ... name=lantern_beacon.rar`) under the dotted
// subject name. The header name joins the set as unrar names it, with no
// PAR2 posted at all, and with the PAR2 set posted it is the set's name too.
fn headed(volumes: &[Vec<u8>], par2: bool) -> Scenario {
    let mut posts = posts(volumes, "Lantern.Beacon.part01.rar", false);
    if !par2 {
        posts.truncate(volumes.len());
    }
    Scenario::new(posts).framed(FRAME).header(0, &true_name(0))
}

async fn header_name_joins_the_set(volumes: Vec<Vec<u8>>, format: &str) {
    let truth = run(&Scenario::new(posts(&volumes, &true_name(0), false)).framed(FRAME)).await;
    assert!(
        truth.completed(),
        "{format} true names: {}",
        summary(&truth)
    );
    for par2 in [false, true] {
        for backwards in [false, true] {
            let mut scenario = headed(&volumes, par2);
            if backwards {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            let case = format!(
                "{format} par2 {par2} backwards {backwards}: {}",
                summary(&graph)
            );
            assert!(graph.completed(), "{case}");
            assert_eq!(graph.files, truth.files, "{case}");
        }
    }
}

#[tokio::test]
async fn a_compressed_first_volume_named_by_its_header_joins_its_set() {
    header_name_joins_the_set(compressed(), "compressed").await;
}

#[tokio::test]
async fn a_stored_first_volume_named_by_its_header_joins_its_set() {
    header_name_joins_the_set(stored(), "stored").await;
}

// As the third post: an article of a follow-on volume lost, the PAR2 set
// posted. The header name joins the set, the set's volumes are fetched
// though nothing named them, and they rebuild the lost article.
#[tokio::test]
async fn a_set_joined_by_its_header_name_repairs_a_lost_article() {
    for (format, volumes) in [("compressed", compressed()), ("stored", stored())] {
        let truth = run(&Scenario::new(posts(&volumes, &true_name(0), false)).framed(FRAME)).await;
        assert!(
            truth.completed(),
            "{format} true names: {}",
            summary(&truth)
        );
        let graph = run(&headed(&volumes, true).lost(2, 1)).await;
        let case = format!("{format}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        assert_eq!(graph.files, truth.files, "{case}");
        assert!(
            graph
                .graph()
                .rulings
                .iter()
                .any(|(kind, ruling)| kind == "par2"
                    && matches!(
                        ruling,
                        Some(weaver_graph::recovery::RepairOutcome::Repaired { .. })
                    )),
            "{case}"
        );
    }
}

// The first ask for each of the PAR2 set's articles is refused, as a
// server answers 430 and a later ask is served: the set's index comes in
// on a retry, after the volumes it names. With nothing lost, nothing asks
// for recovery, and the set as named still cannot complete without it.
#[tokio::test]
async fn a_first_volume_under_another_stem_waits_for_par2_that_comes_in_late() {
    for (format, volumes) in [("compressed", compressed()), ("stored", stored())] {
        let truth = run(&Scenario::new(posts(&volumes, &true_name(0), false)).framed(FRAME)).await;
        assert!(
            truth.completed(),
            "{format} true names: {}",
            summary(&truth)
        );
        let posts = posts(&volumes, "Lantern.Beacon.part01.rar", false);
        let late: Vec<(u32, u32)> = posts
            .iter()
            .enumerate()
            .skip(volumes.len())
            .flat_map(|(file, post)| {
                (0..post.articles(FRAME).count()).map(move |at| (file as u32, at as u32))
            })
            .collect();
        let scenario = late.into_iter().fold(
            Scenario::new(posts).framed(FRAME),
            |scenario, (file, at)| scenario.refused_once(file, at),
        );
        let graph = run(&scenario).await;
        let case = format!("{format}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        assert_eq!(graph.files, truth.files, "{case}");
    }
}

// At the post's own sizes: articles of 768,000 bytes, volumes of four, a
// set sliced at 53,024 bytes, and two of its files no PAR2 at all.
#[tokio::test]
async fn a_stored_first_volume_under_another_stem_at_real_sizes_is_joined_by_unasked_par2() {
    const ARTICLE: usize = 768_000;
    let volumes: Vec<Vec<u8>> =
        single_member_rar4_store_set("lantern.bin", &payload(16 * ARTICLE + 77, 41), 4)
            .into_iter()
            .map(|(_, bytes)| bytes)
            .collect();
    let truth = run(
        &Scenario::new(posts_sliced(&volumes, &true_name(0), false, 53_024, true)).framed(ARTICLE),
    )
    .await;
    assert!(truth.completed(), "true names: {}", summary(&truth));
    for changed in [false, true] {
        let graph = run(&Scenario::new(posts_sliced(
            &volumes,
            "Lantern.Beacon.part01.rar",
            changed,
            53_024,
            true,
        ))
        .framed(ARTICLE))
        .await;
        let case = format!("changed {changed}: {}", summary(&graph));
        assert!(graph.completed(), "{case}");
        assert_eq!(graph.files, truth.files, "{case}");
    }
}
