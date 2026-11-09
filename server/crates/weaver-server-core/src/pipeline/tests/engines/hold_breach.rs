// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Holds that run out of room. Bytes that arrive before the headers that
// place them are held in memory, then on scratch; a set whose every byte
// arrives whole delivers its members whatever the holds could keep.

use std::collections::BTreeSet;

use weaver_graph::holds::HoldBudgets;

use super::harness::{Post, Scenario, par2_set, payload, run};
use crate::pipeline::graph::lines::Lines;
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
