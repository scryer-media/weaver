// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Holds that run out of room. Bytes that arrive before the headers that
// place them are held in memory, then on scratch; a set whose every byte
// arrives whole delivers its members whatever the holds could keep.

use std::collections::BTreeSet;
use std::path::Path;

use sevenz_turbo::EncoderMethod;
use weaver_graph::holds::HoldBudgets;
use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    Outcome, Post, Scenario, par2_set, par2_volumes_sliced, payload, run, run_then,
};
use super::loans::stored_zip;
use super::rar::{posts as rar_posts, with_par2};
use super::rar_rev::lz_four;
use super::restart::{Down, run_restarting_at};
use super::streams;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::engine::RUN_OVERHEAD;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::single_member_store_set;

const MEMBER: &str = "beacon.bin";

// Room for a few articles in memory and as many on scratch: far less than
// a set whose volumes all open before any of them is placed.
const TIGHT: HoldBudgets = HoldBudgets {
    resident_limit: 4 * FRAME as u64,
    scratch_total: 8 * FRAME as u64,
    scratch_ceiling: 4 * FRAME as u64,
    disk_reserve: 0,
};

// Articles big enough that a volume's first one carries the 16 KiB a PAR2
// set names it by, as a real post's do.
const FRAME: usize = 24 * 1024;

// A stored set posted under names its PAR2 set does not use, the PAR2 index
// first, then every volume fetched side by side in no order, as a crowd of
// workers fetches it: the first article of each, then the second of each.
fn scrambled_store_set(body: &[u8], volumes: usize) -> Scenario {
    let set = single_member_store_set(MEMBER, body, volumes);
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut recovery = par2_set("silver.horizon", &files, 2);
    let mut posts = vec![recovery.remove(0)];
    posts.extend(set.iter().enumerate().map(|(index, (_, bytes))| {
        Post::plain(&format!("lantern.part{:02}.rar", index + 1), bytes.clone())
    }));
    posts.extend(recovery);
    // Index 0 is the PAR2 index; volumes are 1..=volumes.
    let mut order: Vec<u32> = vec![0];
    let mut scrambled: Vec<u32> = (1..=volumes as u32).collect();
    let mut seed = 0x9e37_79b9_u32;
    for at in (1..scrambled.len()).rev() {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        scrambled.swap(at, (seed as usize) % (at + 1));
    }
    order.extend(scrambled);
    order.push(volumes as u32 + 1);
    Scenario::new(posts).framed(FRAME).together(&order)
}

// Every volume of a stored set arrives whole, out of order, under holds too
// small for what waits on the first volume's headers: the member is
// delivered whole all the same.
#[tokio::test]
async fn a_scrambled_stored_set_whose_holds_run_out_delivers_its_member() {
    let body = payload(8 * 3 * FRAME + 13, 77);
    let scenario = scrambled_store_set(&body, 8).holds(TIGHT);
    let graph = run(&scenario).await;
    assert!(
        graph.completed(),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    let delivered = format!("lantern beacon/{MEMBER}");
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered.clone()],
        "delivered names"
    );
    assert!(graph.files[&delivered] == body, "member bytes");
}

// The stores bound over the job's members, by node.
fn store_nodes(lines: &Lines) -> BTreeSet<String> {
    lines
        .of("object bound")
        .into_iter()
        .filter(|line| line.contains("kind=rar_store"))
        .filter_map(|line| {
            line.split_whitespace()
                .find(|field| field.starts_with("node="))
                .map(str::to_owned)
        })
        .collect()
}

// A set its recovery set renames volume by volume as their fronts arrive is
// one store all the same: a volume still under its posted name joins the
// volumes renamed before it, and no store is founded beside the set.
#[tokio::test]
async fn a_set_renamed_as_it_arrives_is_one_store() {
    let (lines, _guard) = Lines::capture();
    let body = payload(8 * 3 * FRAME + 13, 77);
    let graph = run(&scrambled_store_set(&body, 8)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(store_nodes(&lines).len(), 1, "{:?}", lines.of("object bound"));
}

// Room for nothing much: a few articles in memory past what the kind's
// decoding run reserves, as many on scratch.
fn tight(kind: &Kind) -> HoldBudgets {
    let article = kind.article as u64;
    HoldBudgets {
        resident_limit: kind.run + 4 * article,
        scratch_total: 8 * article,
        scratch_ceiling: 4 * article,
        disk_reserve: 0,
    }
}

const UNLIMITED: HoldBudgets = HoldBudgets {
    resident_limit: u64::MAX,
    scratch_total: u64::MAX,
    scratch_ceiling: u64::MAX,
    disk_reserve: 0,
};

const ARTICLE: usize = 4096;

// A kind of post: its posts, which of them are its volumes in volume
// order, the size it is cut into articles at, and what a run decoding it
// reserves in memory before it starts.
struct Kind {
    name: &'static str,
    posts: fn() -> Vec<Post>,
    volumes: &'static [u32],
    article: usize,
    run: u64,
}

fn body() -> Vec<u8> {
    payload(16 * ARTICLE + 13, 41)
}

fn stored_rar() -> Vec<Post> {
    with_par2(single_member_store_set(MEMBER, &body(), 4))
}

fn compressed_rar() -> Vec<Post> {
    rar_posts(lz_four())
}

fn sevenz() -> Vec<Post> {
    let archive = build_7z_shaped(
        &[Entry::file(MEMBER, body())],
        EncoderMethod::COPY,
        None,
        false,
    );
    rar_posts(split_volumes(&archive, 4))
}

fn zip() -> Vec<Post> {
    vec![Post::plain("silver.horizon.zip", stored_zip(MEMBER, &body()))]
}

fn nested() -> Vec<Post> {
    with_par2(single_member_store_set(
        "inner.zip",
        &stored_zip(MEMBER, &body()),
        4,
    ))
}

fn tar() -> Vec<Post> {
    vec![Post::plain(
        "silver.horizon.tar",
        streams::tar(&[(MEMBER, body())]),
    )]
}

fn split() -> Vec<Post> {
    body()
        .chunks(4 * ARTICLE + 3)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("{MEMBER}.{:03}", index + 1), piece.to_vec()))
        .collect()
}

const KINDS: &[Kind] = &[
    Kind {
        name: "stored rar with par2",
        posts: stored_rar,
        volumes: &[0, 1, 2, 3],
        article: ARTICLE,
        run: 0,
    },
    Kind {
        name: "compressed rar",
        posts: compressed_rar,
        volumes: &[0, 1, 2, 3],
        article: 1024,
        // The run's overhead and the set's 4 MiB dictionary.
        run: RUN_OVERHEAD + (4 << 20),
    },
    Kind {
        name: "7z",
        posts: sevenz,
        volumes: &[0, 1, 2, 3],
        article: ARTICLE,
        run: 0,
    },
    Kind {
        name: "zip",
        posts: zip,
        volumes: &[0],
        article: ARTICLE,
        run: 0,
    },
    Kind {
        name: "zip in a stored rar",
        posts: nested,
        volumes: &[0, 1, 2, 3],
        article: ARTICLE,
        run: 0,
    },
    Kind {
        name: "tar",
        posts: tar,
        volumes: &[0],
        article: ARTICLE,
        run: 0,
    },
    Kind {
        name: "split",
        posts: split,
        volumes: &[0, 1, 2, 3, 4],
        article: ARTICLE,
        run: 0,
    },
];

#[derive(Clone, Copy, Debug)]
enum Order {
    InOrder,
    Backwards,
    // The first volume's articles after every other volume's, or a lone
    // volume's front after the rest of it.
    FirstVolumeLast,
}

fn scenario(kind: &Kind, order: Order, budgets: HoldBudgets) -> Scenario {
    let scenario = Scenario::new((kind.posts)())
        .framed(kind.article)
        .holds(budgets);
    match order {
        Order::InOrder => scenario,
        Order::Backwards => scenario.backwards(),
        Order::FirstVolumeLast if kind.volumes.len() == 1 => scenario.reversed(kind.volumes),
        Order::FirstVolumeLast => {
            let mut turns = kind.volumes[1..].to_vec();
            turns.push(kind.volumes[0]);
            scenario.together(&turns)
        }
    }
}

// Every holds scratch or spill file left anywhere under `root`.
fn holds_files_left(root: &Path) -> Vec<String> {
    let mut left = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("graph-holds-") || name.starts_with("graph-spill-"))
            {
                left.push(path.display().to_string());
            }
        }
    }
    left
}

// What a run came to, and the holds files it left behind.
async fn outcome(scenario: &Scenario) -> (Outcome, Vec<String>) {
    let mut left = Vec::new();
    let graph = run_then(scenario, async |_, complete_dir: &Path| {
        left = holds_files_left(complete_dir.parent().expect("the run's temp dir"));
    })
    .await;
    (graph, left)
}

// Every kind, in every order, under holds with no room ends as it ends with
// unlimited holds: the same status, the same files byte for byte, and no
// scratch or spill file left behind.
#[tokio::test]
async fn every_kind_in_every_order_ends_under_tight_holds_as_under_unlimited_ones() {
    for kind in KINDS {
        for order in [Order::InOrder, Order::Backwards, Order::FirstVolumeLast] {
            let cell = format!("{} {order:?}", kind.name);
            let (unlimited, _) = outcome(&scenario(kind, order, UNLIMITED)).await;
            assert!(
                unlimited.completed(),
                "{cell}: {:?} {:?}",
                unlimited.status,
                unlimited.failure
            );
            let (lines, guard) = Lines::capture();
            let (tight, left) = outcome(&scenario(kind, order, tight(kind))).await;
            drop(guard);
            assert_eq!(
                (&tight.status, &tight.failure),
                (&unlimited.status, &unlimited.failure),
                "{cell}"
            );
            assert_eq!(
                tight.files.keys().collect::<Vec<_>>(),
                unlimited.files.keys().collect::<Vec<_>>(),
                "{cell}"
            );
            assert!(tight.files == unlimited.files, "{cell}: delivered bytes");
            assert!(left.is_empty(), "{cell}: {left:?}");
            let held_out: Vec<_> = lines
                .of("object refused")
                .into_iter()
                .filter(|line| line.contains("why=holds_breached"))
                .collect();
            assert!(held_out.is_empty(), "{cell}: {held_out:?}");
            // Each cell spills exactly when its holds run past memory and
            // scratch, as `SPILLS` says it does.
            assert_eq!(
                !lines.of("holds spilled").is_empty(),
                SPILLS.contains(&cell.as_str()),
                "{cell}: holds spilled"
            );
        }
    }
}

// The cells whose holds run past memory and scratch. What arrives before
// the headers that place it is held until they come: every kind but the
// compressed rar, whose run's reservation leaves the holds room for its
// small set, spills backwards, and all of those but the split spill with
// their first volume last too. In order, nothing waits long enough to.
const SPILLS: &[&str] = &[
    "stored rar with par2 Backwards",
    "stored rar with par2 FirstVolumeLast",
    "7z Backwards",
    "7z FirstVolumeLast",
    "zip Backwards",
    "zip FirstVolumeLast",
    "zip in a stored rar Backwards",
    "zip in a stored rar FirstVolumeLast",
    "tar Backwards",
    "tar FirstVolumeLast",
    "split Backwards",
];

// A damaged stored set under tight holds repairs from its recovery set
// reading the bytes the holds spilled, and delivers its member whole.
#[tokio::test]
async fn a_damaged_set_whose_holds_spilled_repairs_from_them() {
    let volumes = single_member_store_set(MEMBER, &body(), 4);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = vec![];
    let mut recovery = par2_volumes_sliced("silver.horizon", &files, ARTICLE as u64, 2, 1);
    posts.push(recovery.remove(0));
    posts.extend(rar_posts(volumes.clone()));
    posts.extend(recovery);
    let scenario = Scenario::new(posts)
        .framed(ARTICLE)
        .holds(tight(&KINDS[0]))
        .together(&[0, 2, 3, 4, 1, 5])
        .damaged(3, 1);
    let (lines, _guard) = Lines::capture();
    let (graph, left) = outcome(&scenario).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(graph.files[&format!("lantern beacon/{MEMBER}")] == body());
    assert!(!lines.of("holds spilled").is_empty());
    assert!(left.is_empty(), "{left:?}");
}

// A stored set whose holds spilled, shut down part way: the restored job
// reads its holds back from scratch and from file, fetches nothing again,
// and delivers its member whole.
#[tokio::test]
async fn a_set_whose_holds_spilled_restarts_and_completes_from_them() {
    let body = payload(8 * 3 * FRAME + 13, 77);
    let scenario = scrambled_store_set(&body, 8).holds(TIGHT);
    let (lines, _guard) = Lines::capture();
    // Whether holds had spilled by the shutdown, and how the checkpoint it
    // wrote recorded the holds: (on scratch, on file).
    let mut at_shutdown = None;
    let restarted = run_restarting_at(
        &scenario,
        &mut |rows| {
            let on_file = rows.holds.iter().filter(|row| row.on_file).count();
            at_shutdown = Some((
                !lines.of("holds spilled").is_empty(),
                rows.holds.len() - on_file,
                on_file,
            ));
        },
        Down::Shutdown,
        &|nth| nth == 14,
    )
    .await;
    assert_eq!(restarted.restarts, 1);
    let (spilled, on_scratch, on_file) = at_shutdown.expect("the shutdown's checkpoint");
    assert!(spilled, "holds spilled before the shutdown");
    assert!(on_file > 0, "rows on file: {on_file}, on scratch: {on_scratch}");
    assert_eq!(restarted.status, Some(JobStatus::Complete));
    assert!(restarted.files[&format!("lantern beacon/{MEMBER}")] == body);
    // Every hold the checkpoint recorded, on file as on scratch, was
    // adopted: a row the restore dropped would have its article fetched
    // again.
    assert!(restarted.from_disk.is_empty(), "{:?}", restarted.from_disk);
}

// A member an older build refused because its holds ran past their budgets
// goes into the checkpoint so. The restore takes the refusal back: it was
// never a ruling on the member's bytes, so the object over it reads them
// again and the member inside comes out whole, as with no refusal.
#[tokio::test]
async fn a_member_refused_for_its_holds_in_a_checkpoint_is_delivered_after_its_restore() {
    for kind in [&KINDS[0], &KINDS[3]] {
        let scenario = Scenario::new((kind.posts)()).framed(kind.article);
        let plain = run(&scenario).await;
        assert!(plain.completed(), "{}: {:?}", kind.name, plain.failure);
        let mut refused = Vec::new();
        let restarted = run_restarting_at(
            &scenario,
            &mut |rows| {
                refused.extend(rows.members.iter().filter_map(|row| row.refused.clone()));
            },
            Down::RefusingForHolds,
            &|nth| nth == 8,
        )
        .await;
        assert_eq!(restarted.restarts, 1, "{}", kind.name);
        // Every volume the object owned was refused with it.
        assert_eq!(
            refused,
            vec!["holds_breached".to_owned(); kind.volumes.len()],
            "{}",
            kind.name
        );
        assert_eq!(restarted.status, Some(JobStatus::Complete), "{}", kind.name);
        assert_eq!(
            restarted.files.keys().collect::<Vec<_>>(),
            plain.files.keys().collect::<Vec<_>>(),
            "{}",
            kind.name
        );
        assert!(restarted.files == plain.files, "{}: delivered bytes", kind.name);
    }
}

// The layout needs of each volume only its front, where its headers are,
// and its tail, where its end record says another volume follows: never
// the data between. A set whose fronts and tails come before the rest, in
// the order a crowd of workers lands them, holds no more than the data
// riding in those articles, one article a volume, whatever the budgets.
#[tokio::test]
async fn a_set_whose_fronts_and_tails_come_first_holds_an_article_a_volume_at_most() {
    let volumes = 24;
    let body = payload(volumes * 3 * FRAME + 13, 77);
    let shape = scrambled_store_set(&body, volumes);
    let order = shape.together.clone();
    let last = |index: u32| shape.posts[index as usize].articles(FRAME).count() as u32 - 1;
    let volume = |index: &&u32| (1..=volumes as u32).contains(*index);
    let mut first = vec![(0, 0)];
    first.extend(order.iter().filter(volume).map(|index| (*index, 0)));
    first.extend(order.iter().filter(volume).map(|index| (*index, last(*index))));
    let shape = shape.first(&first);
    let run = run_restarting_at(&shape, &mut |_| {}, Down::Crash, &|_| false).await;
    assert_eq!(run.restarts, 0);
    assert_eq!(run.status, Some(JobStatus::Complete));
    assert!(run.files[&format!("lantern beacon/{MEMBER}")] == body);
    assert!(
        run.graph.held <= (volumes * FRAME) as u64,
        "held {} of {} volumes",
        run.graph.held,
        volumes
    );
}
