// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One RAR set posted under names that found more than one store, and a
// PAR2 set over the set's true names that renames every volume by its
// content and repairs a damaged one. After the renames the set is one
// store, whatever the posted names founded and whatever order the posts
// arrive in, and the member is delivered whole.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{Outcome, Post, Scenario, kinds, par2_volumes_sliced, payload, run};
use super::rar::delivered;
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
