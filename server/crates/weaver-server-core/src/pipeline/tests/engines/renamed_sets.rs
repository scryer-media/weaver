// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One RAR set posted under names that found more than one store, and a
// PAR2 set over the set's true names that renames every volume by its
// content and repairs a damaged one. After the renames the set is one
// store, whatever the posted names founded and whatever order the posts
// arrive in, and the member is delivered whole.

use weaver_graph::recovery::RepairOutcome;

use super::super::{
    TEST_RAR5_SIG, build_test_rar_end_header, build_test_rar_file_header,
    build_test_rar_main_header,
};
use super::classic_sfv::listing_text;
use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use super::rar::delivered;
use super::sfv::crc;
use crate::pipeline::tests::direct_store::{
    multi_member_store_set, single_member_rar4_store_set, single_member_rar4_store_set_numbered,
    single_member_store_set,
};

const MEMBER: &str = "final.girl.mkv";
// Articles, and PAR2 slices, of 32 KiB: a volume spans several, and its
// first article holds the 16 KiB the PAR2 set renames it by, so a volume
// is renamed as soon as it is bound, as a real post's are.
const FRAME: usize = 32 * 1024;

// Eight volumes of about five articles each.
fn body() -> Vec<u8> {
    payload(40 * FRAME + 13, 87)
}

// The orders the posts arrive in: as given, the PAR2 index and volume
// first, the index last of all, and every post backwards.
#[derive(Clone, Copy, Debug)]
enum Order {
    Posted,
    Par2First,
    IndexLast,
    Backwards,
    // Half the volumes, then the PAR2 set, then the rest.
    IndexMidway,
    // The PAR2 set first, then every follow-on volume, the first volume
    // last of all.
    FirstVolumeLast,
    // The PAR2 set first, then a few follow-on volumes, then the first
    // volume, then the rest.
    FirstVolumeAmid,
    // The PAR2 set first, then the volumes fetched side by side, the
    // follow-on volumes' fronts scattered around the first volume's.
    Scattered,
}

const ORDERS: [Order; 8] = [
    Order::Posted,
    Order::IndexMidway,
    Order::FirstVolumeLast,
    Order::FirstVolumeAmid,
    Order::Par2First,
    Order::IndexLast,
    Order::Backwards,
    Order::Scattered,
];

// The scenario for posts arranged in `order`.
fn scenario(posts: Vec<Post>, order: Order, damaged: u32) -> Scenario {
    let volumes = posts.len() as u32 - 2;
    let scenario = Scenario::new(posts).framed(FRAME).damaged(damaged, 2);
    match order {
        Order::Scattered => {
            // The PAR2 set is posts 0 and 1; volume `v` is post `v + 2`.
            let mut together: Vec<u32> = vec![0, 1];
            let mut rest: Vec<u32> = (1..volumes).map(|v| v + 2).collect();
            // Odd volumes after even ones, the first volume in the middle.
            rest.sort_by_key(|post| (post % 2, *post));
            rest.insert(rest.len() / 2, 2);
            together.extend(rest);
            scenario.together(&together)
        }
        _ => scenario,
    }
}

// The posted volumes, each under its posted name and with the bytes of
// the volume whose true name the PAR2 set gives it, then the set over the
// true names, arranged in `order`. Returns the posts and the index of the
// post holding the volume at `damaged` in the true order.
fn arranged(
    truth: &[(String, Vec<u8>)],
    posted: &[(String, usize)],
    damaged: usize,
    order: Order,
) -> (Vec<Post>, u32) {
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut set = par2_volumes_sliced("Final.Girl.2015.720p", &files, FRAME as u64, 4, 1);
    let mut volumes: Vec<Post> = posted
        .iter()
        .map(|(name, volume)| Post::plain(name, truth[*volume].1.clone()))
        .collect();
    let posts = match order {
        Order::Posted => {
            volumes.extend(set);
            volumes
        }
        Order::IndexMidway => {
            let rest = volumes.split_off(volumes.len() / 2);
            volumes.extend(set);
            volumes.extend(rest);
            volumes
        }
        Order::FirstVolumeLast => {
            let first = volumes.remove(0);
            set.extend(volumes);
            set.push(first);
            set
        }
        Order::FirstVolumeAmid => {
            let first = volumes.remove(0);
            let rest = volumes.split_off(volumes.len().min(3));
            set.extend(volumes);
            set.push(first);
            set.extend(rest);
            set
        }
        Order::Par2First | Order::Scattered => {
            set.extend(volumes);
            set
        }
        Order::IndexLast => {
            let index = set.remove(0);
            volumes.extend(set);
            volumes.push(index);
            volumes
        }
        Order::Backwards => {
            volumes.extend(set);
            volumes.reverse();
            volumes
        }
    };
    let at = posts
        .iter()
        .position(|post| post.bytes == truth[damaged].1)
        .unwrap();
    (posts, at as u32)
}

fn assert_one_store_delivers(graph: &Outcome, name: &str, body: &[u8], case: &str) {
    assert!(
        graph.completed(),
        "{case}: {:?} {:?} kinds {:?} delivery {:?}",
        graph.status,
        graph.failure,
        kinds(graph),
        graph.graph().delivery,
    );
    assert_eq!(
        graph.files.get(&delivered(name)).map(Vec::as_slice),
        Some(body),
        "{case}: member bytes; delivered {:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(
        graph
            .graph()
            .outcomes
            .iter()
            .all(|outcome| matches!(outcome, Some(RepairOutcome::Repaired { .. }))),
        "{case}: {:?}",
        graph.graph().outcomes
    );
    assert!(
        graph.graph().delivery.withheld.is_empty(),
        "{case}: {:?}",
        graph.graph().delivery
    );
}

// An old-style set, `.rar` then `.r00` on: the follow-on volumes posted
// under an obfuscated stem with their true numbers, the first volume
// posted as a `part01.rar` of the release name. The stem founds one store,
// the first volume another, and the PAR2 set renames all of them to the
// release's own old-style names.
async fn old_style_set_under_two_stems(truth: Vec<(String, Vec<u8>)>, format: &str) {
    let body = body();
    let truth: Vec<(String, Vec<u8>)> = truth
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            let name = match index {
                0 => "final.girl.2015.720p-ulshd.rar".to_string(),
                n => format!("final.girl.2015.720p-ulshd.r{:02}", n - 1),
            };
            (name, bytes)
        })
        .collect();
    let posted: Vec<(String, usize)> = (0..truth.len())
        .map(|index| match index {
            0 => ("Final.Girl.2015.720p-ULSHD.part01.rar".to_string(), 0),
            n => (format!("65606-1.r{:02}", n - 1), n),
        })
        .collect();
    for order in ORDERS {
        let (posts, damaged) = arranged(&truth, &posted, 2, order);
        let graph = run(&scenario(posts, order, damaged)).await;
        assert_one_store_delivers(&graph, MEMBER, &body, &format!("{format} {order:?}"));
    }
}

#[tokio::test]
async fn an_old_style_rar5_set_under_two_stems_renamed_by_par2_is_one_store() {
    old_style_set_under_two_stems(single_member_store_set(MEMBER, &body(), 8), "rar5").await;
}

// The first volume holds a member that ends where the volume ends, so no
// member runs from it into the second: its headers do not link it to the
// follow-on volumes the obfuscated stem founded a store on, and only the
// names the PAR2 set gives say it is their set's first.
#[tokio::test]
async fn an_old_style_set_whose_first_volume_links_to_nothing_is_one_store_after_par2() {
    let body = body();
    let nfo = payload(single_chunk(&body, 8), 9);
    let truth = multi_member_store_set(&[("final.girl.nfo.bin", nfo), (MEMBER, body.clone())], 9);
    old_style_set_under_two_stems(truth, "rar5 unlinked first").await;
}

// The size of one of `volumes` equal volumes over a member of `body`'s
// length and one this long before it: the first volume holds that one
// alone, to its last byte.
fn single_chunk(body: &[u8], volumes: usize) -> usize {
    body.len().div_ceil(volumes)
}

#[tokio::test]
async fn an_old_style_rar4_set_under_two_stems_renamed_by_par2_is_one_store() {
    old_style_set_under_two_stems(
        single_member_rar4_store_set_numbered(MEMBER, &body(), 8),
        "rar4 numbered",
    )
    .await;
    old_style_set_under_two_stems(single_member_rar4_store_set(MEMBER, &body(), 8), "rar4").await;
}

// A 48-volume old-style set posted as one release was: the PAR2 index,
// then the follow-on volumes under an obfuscated stem, then the first
// volume as a `part01.rar` of the release name, then an SFV listing the
// true names under the stem, then the PAR2 volumes. The volumes are fetched side by side, so the follow-on volumes bound
// before the first one are not a run: `r00` to `r04` with `r08`, `r16`,
// `r17`, `r24` and `r41` among them, and the first volume comes before
// `r18` and the rest.
async fn a_long_old_style_set_fetched_side_by_side(truth: Vec<(String, Vec<u8>)>, format: &str) {
    let truth: Vec<(String, Vec<u8>)> = truth
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            let name = match index {
                0 => "final.girl.2015.720p-ulshd.rar".to_string(),
                n => format!("final.girl.2015.720p-ulshd.r{:02}", n - 1),
            };
            (name, bytes)
        })
        .collect();
    let entries: Vec<(&str, u32)> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), crc(bytes)))
        .collect();
    let sfv = listing_text("; listing\n", &entries);
    let mut files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    files.push(("final.girl.2015.720p-ulshd.sfv", &sfv));
    let mut set = par2_volumes_sliced("Final.Girl.2015.720p", &files, FRAME as u64, 4, 1);
    let index = set.remove(0);
    let follow_ons = truth.len() as u32 - 1;
    // Post 0 the index, posts 1 on the follow-on volumes `r00` on, then
    // the first volume, the SFV, then the PAR2 volumes.
    let mut posts = vec![index];
    posts.extend(
        truth
            .iter()
            .skip(1)
            .enumerate()
            .map(|(n, (_, bytes))| Post::plain(&format!("65606-1.r{n:02}"), bytes.clone())),
    );
    posts.push(Post::plain(
        "Final.Girl.2015.720p-ULSHD.part01.rar",
        truth[0].1.clone(),
    ));
    posts.push(Post::plain("65606-1.sfv", sfv.clone()));
    posts.extend(set);
    let first = follow_ons + 1;
    let follow_on = |n: u32| n + 1;
    let mut together: Vec<u32> = vec![0];
    together.extend([0, 1, 2, 16, 3, 8, 24, 17, 41, 4].map(follow_on));
    together.extend([first, first + 1]);
    let early = together.clone();
    together.extend(
        (0..follow_ons)
            .map(follow_on)
            .filter(|post| !early.contains(post)),
    );
    together.extend(first + 2..posts.len() as u32);
    let body = long_body();
    let scenario = Scenario::new(posts)
        .framed(FRAME)
        .damaged(follow_on(9), 2)
        .together(&together);
    let graph = run(&scenario).await;
    assert_one_store_delivers(&graph, MEMBER, &body, format);
}

// Forty-eight volumes of three articles each.
fn long_body() -> Vec<u8> {
    payload(48 * 3 * FRAME + 13, 87)
}

#[tokio::test]
async fn a_long_old_style_set_fetched_side_by_side_renamed_by_par2_is_one_store() {
    let body = long_body();
    a_long_old_style_set_fetched_side_by_side(
        single_member_rar4_store_set_numbered(MEMBER, &body, 48),
        "rar4 numbered",
    )
    .await;
    a_long_old_style_set_fetched_side_by_side(
        single_member_rar4_store_set(MEMBER, &body, 48),
        "rar4",
    )
    .await;
    a_long_old_style_set_fetched_side_by_side(single_member_store_set(MEMBER, &body, 48), "rar5")
        .await;
}

// A 24-volume set posted as `part01` to `part09` and then `part010` to
// `part024`, each posted name holding the volume seven places on, so the
// posted numbers say nothing true of the order. The PAR2 set renames them
// to the true `part01` to `part12`.
#[tokio::test]
async fn a_shuffled_mixed_width_set_renamed_by_par2_is_one_store() {
    let first = payload(41 * FRAME + 5, 31);
    let second = payload(19 * FRAME + 41, 32);
    let truth: Vec<(String, Vec<u8>)> = multi_member_store_set(
        &[
            ("BGT.S13E15.mkv", first.clone()),
            ("BGT.S13E15.nfo.bin", second),
        ],
        24,
    )
    .into_iter()
    .enumerate()
    .map(|(index, (_, bytes))| {
        (
            format!("BGT.S13E15.Live.4.720p.part{:02}.rar", index + 1),
            bytes,
        )
    })
    .collect();
    let posted: Vec<(String, usize)> = (0..truth.len())
        .map(|index| {
            let number = index + 1;
            let name = if number < 10 {
                format!("BGT.S13E15.720p.part{number:02}.rar")
            } else {
                format!("BGT.S13E15.720p.part{number:03}.rar")
            };
            (name, (index + 7) % truth.len())
        })
        .collect();
    for order in ORDERS {
        let (posts, damaged) = arranged(&truth, &posted, 3, order);
        let graph = run(&scenario(posts, order, damaged)).await;
        assert_one_store_delivers(&graph, "BGT.S13E15.mkv", &first, &format!("{order:?}"));
    }
}

// A whole stored RAR5 archive holding `name`: no volume of any set, its
// main header says so, and it ends with nothing after it.
fn whole_archive(name: &str, bytes: &[u8]) -> Vec<u8> {
    let mut archive = TEST_RAR5_SIG.to_vec();
    archive.extend_from_slice(&build_test_rar_main_header(0, None));
    archive.extend_from_slice(&build_test_rar_file_header(
        name,
        0,
        bytes.len() as u64,
        bytes.len() as u64,
        Some(crc(bytes)),
    ));
    archive.extend_from_slice(bytes);
    archive.extend_from_slice(&build_test_rar_end_header(false));
    archive
}

// The archives of a pack, each a set of its own, as a pack of clips posts
// them: two sets of several volumes and two whole archives of one, every
// one of them posted as a `partNN` of one obfuscated stem, `part01` to
// `part09` and then `part010` on, so the posted names make one set of
// them all. A PAR2 set over the true names renames each by its content.
// Every set is its own store after the renames, the whole archives among
// them, and every clip is delivered, in every order the posts arrive in.
#[tokio::test]
async fn whole_archives_posted_as_volumes_of_one_stem_are_each_their_own_set() {
    let clips: Vec<(String, Vec<u8>)> = vec![
        ("38_kozjak.mp4".to_string(), payload(13 * FRAME + 7, 41)),
        ("52_kika.mp4".to_string(), payload(7 * FRAME + 19, 42)),
        // Each whole archive is posted in one article, as a small clip's
        // is: it is read to its end before the PAR2 set renames it.
        ("58_zdr.mp4".to_string(), payload(FRAME - 512, 43)),
        ("59_macka.mp4".to_string(), payload(FRAME / 3, 44)),
    ];
    let renamed = |stem: &str, set: Vec<(String, Vec<u8>)>| -> Vec<(String, Vec<u8>)> {
        set.into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| (format!("{stem}.part{}.rar", index + 1), bytes))
            .collect()
    };
    let mut truth = renamed(
        "38_kozjak",
        multi_member_store_set(&[(clips[0].0.as_str(), clips[0].1.clone())], 5),
    );
    truth.extend(renamed(
        "52_kika ",
        multi_member_store_set(&[(clips[1].0.as_str(), clips[1].1.clone())], 3),
    ));
    truth.push((
        "58_zdr.rar".to_string(),
        whole_archive(&clips[2].0, &clips[2].1),
    ));
    truth.push((
        "59_macka.rar".to_string(),
        whole_archive(&clips[3].0, &clips[3].1),
    ));
    // The posted numbering interleaves the sets, a whole archive between
    // volumes of another set, as the pack's was, or a whole archive posted
    // first of all, as `part01`.
    let numberings = [
        [5, 0, 6, 1, 2, 7, 8, 3, 9, 4],
        [8, 0, 5, 1, 9, 2, 6, 3, 7, 4],
    ];
    for (numbering, order) in numberings
        .iter()
        .flat_map(|numbering| ORDERS.map(|order| (numbering, order)))
    {
        let posted: Vec<(String, usize)> = numbering
            .iter()
            .enumerate()
            .map(|(at, volume)| {
                let number = at + 1;
                let name = if number < 10 {
                    format!("Pack.XXX-Amateur-U4A.part{number:02}.rar")
                } else {
                    format!("Pack.XXX-Amateur-U4A.part{number:03}.rar")
                };
                (name, *volume)
            })
            .collect();
        let (posts, _) = arranged(&truth, &posted, 0, order);
        let scenario = match order {
            Order::Scattered => {
                // The PAR2 set first, then the volumes side by side, the
                // whole archives' fronts among the others'.
                let volumes = posted.len() as u32;
                let mut together: Vec<u32> = (0..posts.len() as u32 - volumes).collect();
                let first = together.len() as u32;
                let mut rest: Vec<u32> = (first..first + volumes).collect();
                rest.sort_by_key(|post| (post % 2, *post));
                together.extend(rest);
                Scenario::new(posts).framed(FRAME).together(&together)
            }
            _ => Scenario::new(posts).framed(FRAME),
        };
        let graph = run(&scenario).await;
        let case = format!("{numbering:?} {order:?}");
        assert!(
            graph.completed(),
            "{case}: {:?} {:?} kinds {:?} delivery {:?}",
            graph.status,
            graph.failure,
            kinds(&graph),
            graph.graph().delivery,
        );
        for (name, body) in &clips {
            assert_eq!(
                graph.files.get(&delivered(name)).map(Vec::as_slice),
                Some(body.as_slice()),
                "{case}: {name}; delivered {:?} delivery {:?}",
                graph.files.keys().collect::<Vec<_>>(),
                graph.graph().delivery,
            );
        }
        assert!(
            graph.graph().delivery.withheld.is_empty(),
            "{case}: {:?}",
            graph.graph().delivery
        );
    }
}

// A set of sixty-two volumes posted under another stem, `part01` to
// `part09` and then `part010` on, each posted number its true one, and a
// PAR2 set over the true names `<stem>.partNN.rar`. The PAR2 set comes
// first and the volumes in the order a pack's arrived: from a middle
// volume on, the first volume among the last. The set is one store and
// its member is delivered, with the holds unbounded and with a scratch
// ceiling a third of the set reaches before the first volume arrives.
#[tokio::test]
async fn a_long_mixed_width_set_its_first_volume_late_delivers() {
    const VOLUMES: usize = 62;
    let body = payload(VOLUMES * 2 * FRAME + 101, 51);
    let stem = "12_[sapphicerotica].francesca.dicaprio,.mia.cruise.double.date.21.02.2017";
    let truth: Vec<(String, Vec<u8>)> =
        multi_member_store_set(&[("double.date.mp4", body.clone())], VOLUMES)
            .into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| (format!("{stem}.part{:02}.rar", index + 1), bytes))
            .collect();
    let posted = |number: usize| {
        if number < 10 {
            format!("Mia.Cruise.Pack.XXX-U4A.part{number:02}.rar")
        } else {
            format!("Mia.Cruise.Pack.XXX-U4A.part{number:03}.rar")
        }
    };
    // The order the pack's volumes were bound in, by posted number.
    let arrival = [
        25, 26, 27, 28, 5, 29, 6, 15, 35, 30, 7, 31, 36, 16, 8, 32, 37, 33, 9, 17, 45, 34, 38, 10,
        39, 18, 46, 11, 40, 55, 19, 41, 12, 47, 42, 13, 20, 56, 43, 48, 14, 44, 21, 57, 49, 22, 58,
        50, 23, 59, 51, 1, 24, 60, 52, 2, 61, 3, 53, 54, 62, 4,
    ];
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let posts = || {
        let mut posts = par2_volumes_sliced("Mia.Cruise.Pack.XXX-U4A", &files, FRAME as u64, 4, 1);
        posts.extend(
            arrival
                .iter()
                .map(|number| Post::plain(&posted(*number), truth[number - 1].1.clone())),
        );
        posts
    };
    let total: u64 = truth.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let unbounded = weaver_graph::holds::HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    };
    let spilling = weaver_graph::holds::HoldBudgets {
        resident_limit: 4 * FRAME as u64,
        scratch_total: total / 3,
        scratch_ceiling: total / 3,
        disk_reserve: 0,
    };
    let spills = |budgets: &weaver_graph::holds::HoldBudgets| budgets.scratch_ceiling < total;
    // The PAR2 set's two posts, then the volumes in turn or side by side.
    let side_by_side: Vec<u32> = (0..2 + VOLUMES as u32).collect();
    for (case, budgets, together) in [
        ("unbounded", unbounded, false),
        ("spilling", spilling, false),
        ("spilling side by side", spilling, true),
    ] {
        let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture();
        let scenario = Scenario::new(posts()).framed(FRAME).holds(budgets);
        let scenario = match together {
            true => scenario.together(&side_by_side),
            false => scenario,
        };
        let graph = run(&scenario).await;
        assert_eq!(
            !lines.of("holds spilled").is_empty(),
            spills(&budgets),
            "{case}"
        );
        assert!(
            graph.completed(),
            "{case}: {:?} {:?} kinds {:?} delivery {:?}",
            graph.status,
            graph.failure,
            kinds(&graph),
            graph.graph().delivery,
        );
        assert_eq!(
            graph
                .files
                .get(&delivered("double.date.mp4"))
                .map(Vec::as_slice),
            Some(body.as_slice()),
            "{case}: delivered {:?}",
            graph.files.keys().collect::<Vec<_>>()
        );
        assert!(
            graph.graph().delivery.withheld.is_empty(),
            "{case}: {:?}",
            graph.graph().delivery
        );
    }
}

// A set of sixty-six stored volumes and its PAR2 set posted with no names
// at all: every post is `unknown`, so the ledger names them `unknown`,
// `unknown.duplicate1` and on, the PAR2 index first. The PAR2 set renames
// each volume `<stem>.partNN.rar` by its content. Fetched side by side,
// with or without two volumes' fronts ahead of the index, the set is one
// store and its member is delivered, in RAR5 and in RAR4 with and without
// numbered end records.
#[tokio::test]
async fn a_nameless_long_set_renamed_by_par2_is_one_store() {
    const VOLUMES: usize = 66;
    let body = payload(VOLUMES * 2 * FRAME + 77, 61);
    let stem = "40_#[pornstarspunishment].pesky.piggy.punishment";
    for (format, set) in [
        ("rar5", single_member_store_set("pesky.mp4", &body, VOLUMES)),
        (
            "rar4",
            single_member_rar4_store_set("pesky.mp4", &body, VOLUMES),
        ),
        (
            "rar4 numbered",
            single_member_rar4_store_set_numbered("pesky.mp4", &body, VOLUMES),
        ),
    ] {
        let truth: Vec<(String, Vec<u8>)> = set
            .into_iter()
            .enumerate()
            .map(|(index, (_, bytes))| (format!("{stem}.part{:02}.rar", index + 1), bytes))
            .collect();
        let files: Vec<(&str, &[u8])> = truth
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let recovery = par2_volumes_sliced(stem, &files, FRAME as u64, 8, 8);
        // The index, then every volume, then the recovery volumes, each
        // posted as `unknown`.
        let posts = || -> Vec<Post> {
            let mut posts = vec![Post::plain("unknown", recovery[0].bytes.clone())];
            posts.extend(
                truth
                    .iter()
                    .map(|(_, bytes)| Post::plain("unknown", bytes.clone())),
            );
            posts.extend(
                recovery[1..]
                    .iter()
                    .map(|volume| Post::plain("unknown", volume.bytes.clone())),
            );
            posts
        };
        let total = posts().len() as u32;
        // Side by side from the index on, or with the tenth and thirtieth
        // volumes' fronts ahead of the index, as the pack's arrived.
        for (order, ahead) in [("index first", vec![0]), ("index third", vec![10, 30, 0])] {
            let case = format!("{format} {order}");
            let mut together = ahead.clone();
            together.extend((0..total).filter(|post| !ahead.contains(post)));
            let graph = run(&Scenario::new(posts()).framed(FRAME).together(&together)).await;
            assert!(
                graph.completed(),
                "{case}: {:?} {:?} kinds {:?} delivery {:?}",
                graph.status,
                graph.failure,
                kinds(&graph),
                graph.graph().delivery,
            );
            assert_eq!(
                graph.files.get(&delivered("pesky.mp4")).map(Vec::as_slice),
                Some(body.as_slice()),
                "{case}: delivered {:?}",
                graph.files.keys().collect::<Vec<_>>()
            );
            assert!(
                graph.graph().delivery.withheld.is_empty(),
                "{case}: {:?}",
                graph.graph().delivery
            );
        }
    }
}

// A pack posted as one stem's `partNN` and then `partNNN`, every archive
// of it a set of its own, with no PAR2 set to rename them: sets of twelve,
// eight and two volumes and whole archives of one, as a real pack of clips
// posts them, in the order its posts came in. Every article's yEnc header
// names its file by its own set, `<stem> (N).partM.rar`, and the posted
// numbering places nothing once the header names found the sets again: the
// second volume of a set of two is no store's first lost, and every clip is
// delivered whole, whether the posts are taken in one by one or together.
#[tokio::test]
async fn a_pack_posted_under_one_stem_is_set_apart_by_its_header_names() {
    shared_stem_pack(true).await;
}

// The same pack with no header name to set its archives apart: every
// article's yEnc header carries the shared posted name, so only the
// volumes' own headers say which belong together. A whole archive states
// at its main header that it is no volume, and is a set of its own from its
// first bytes, while it is still held short of its end; a set's volumes
// state their numbers and join the set that numbering continues. Every clip
// is delivered whole, one by one or together.
#[tokio::test]
async fn a_pack_posted_under_one_stem_is_set_apart_by_its_volume_headers() {
    shared_stem_pack(false).await;
}

async fn shared_stem_pack(named: bool) {
    const STEM: &str = "494949c9892d64daac5664541a4cdd5af912";
    // (first posted number, volumes) of each set; a set of one is a whole
    // archive.
    let sets: &[(usize, usize)] = &[
        (1, 1),
        (2, 8),
        (10, 2),
        (12, 1),
        (13, 2),
        (15, 2),
        (17, 1),
        (18, 1),
        (19, 1),
        (20, 1),
        (21, 12),
        (33, 2),
        (35, 1),
        (36, 1),
        (37, 1),
        (38, 1),
        (39, 1),
        (40, 1),
        (41, 1),
        (42, 1),
        (43, 1),
        (44, 1),
        (45, 1),
        (46, 1),
        (47, 1),
        (48, 1),
    ];
    let mut by_number: std::collections::BTreeMap<usize, (String, Vec<u8>)> = Default::default();
    let mut clips = Vec::new();
    for (index, (first, volumes)) in sets.iter().enumerate() {
        let name = format!("clip{index:02}.mp4");
        let body = payload(volumes * 3 * FRAME + 11 * index + 1, index as u8 + 3);
        let set = match *volumes {
            1 => vec![(format!("{STEM} ({index}).rar"), whole_archive(&name, &body))],
            _ => multi_member_store_set(&[(name.as_str(), body.clone())], *volumes)
                .into_iter()
                .enumerate()
                .map(|(at, (_, bytes))| {
                    let part = match *volumes >= 10 {
                        true => format!("{:02}", at + 1),
                        false => format!("{}", at + 1),
                    };
                    (format!("{STEM} ({index}).part{part}.rar"), bytes)
                })
                .collect(),
        };
        for (at, volume) in set.into_iter().enumerate() {
            by_number.insert(first + at, volume);
        }
        clips.push((name, body));
    }
    let posted = |number: usize| match number < 10 {
        true => format!("Karla.Pack.XXX-U4A.part{number:02}.rar"),
        false => format!("Karla.Pack.XXX-U4A.part{number:03}.rar"),
    };
    let arrival = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 41, 21, 42, 43, 44,
        45, 22, 46, 47, 48, 23, 24, 31, 25, 32, 26, 33, 27, 34, 28, 35, 29, 30, 36, 37, 38, 39, 40,
    ];
    for together in [false, true] {
        let posts = arrival
            .iter()
            .map(|number| Post::plain(&posted(*number), by_number[number].1.clone()))
            .collect();
        let mut scenario = Scenario::new(posts).framed(FRAME);
        if named {
            for (file, number) in arrival.iter().enumerate() {
                scenario = scenario.header(file as u32, &by_number[number].0);
            }
        }
        let all: Vec<u32> = (0..arrival.len() as u32).collect();
        let scenario = match together {
            true => scenario.together(&all),
            false => scenario,
        };
        let graph = run(&scenario).await;
        for (name, body) in &clips {
            assert!(
                graph.files.get(&delivered(name)).map(Vec::as_slice) == Some(body.as_slice()),
                "together={together}: {name} delivered whole; delivered {:?}",
                graph.files.keys().collect::<Vec<_>>()
            );
        }
        assert!(
            graph.graph().delivery.withheld.is_empty(),
            "together={together}: {:?}",
            graph.graph().delivery.withheld
        );
        assert!(
            graph.completed(),
            "together={together}: {:?} {:?}",
            graph.status,
            graph.failure
        );
    }
}
