// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine timed on one representative job set, in one process, one
// job at a time: a table of each run's time and the counts a run's cost is
// read from. Nothing here asserts a time; a run that stalls is listed as
// stalled.

use std::time::{Duration, Instant};

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, try_run};
use super::nesting::{CHAIN, chained, noise, sevenz_of};
use super::streams::xz;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::rar5_fixture_bytes;

// A plain file of four thousand articles under PAR2, three of them damaged.
fn par2_damaged() -> Scenario {
    let data = payload(4000 * ARTICLE, 31);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 8));
    Scenario::new(posts)
        .damaged(0, 100)
        .damaged(0, 2000)
        .damaged(0, 3500)
}

// One member stored across twenty RAR volumes.
fn rar_store_20() -> Scenario {
    let member = noise(20 * 50 * ARTICLE + 17, 41);
    posted(multi_member_store_set(&[("ember.bin", member)], 20))
}

// `count` small files, each in its own xz, stored in one 7z.
fn sevenz_xz(count: u64) -> Scenario {
    let posted: Vec<(String, Vec<u8>)> = (0..count)
        .map(|index| {
            let bytes = noise(8 + (index as usize * 7) % 41, 1000 + index);
            (format!("m{index:04}.bin.xz"), xz(&bytes))
        })
        .collect();
    let entries: Vec<(&str, Vec<u8>)> = posted
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    Scenario::new(vec![Post::plain("lantern.7z", sevenz_of(&entries))]).framed(4096)
}

// Two hundred members stored across four RAR volumes.
fn rar_200() -> Scenario {
    let files: Vec<(String, Vec<u8>)> = (0..200u64)
        .map(|index| {
            (
                format!("m{index:03}.bin"),
                noise(5 + (index as usize * 13) % 97, 2000 + index),
            )
        })
        .collect();
    let named: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    posted(multi_member_store_set(&named, 4))
}

// A solid compressed RARLAB archive in 1 MiB articles.
fn solid() -> Scenario {
    Scenario::new(vec![Post::plain(
        "solid.rar",
        rar5_fixture_bytes("rar5_solid.rar"),
    )])
    .framed(1 << 20)
}

// Two hundred small plain files.
fn small_200() -> Scenario {
    Scenario::new(
        (0..200u32)
            .map(|index| {
                Post::plain(
                    &format!("f{index:03}.bin"),
                    payload(5 * ARTICLE + index as usize, index as u8),
                )
            })
            .collect(),
    )
}

fn posted(volumes: Vec<(String, Vec<u8>)>) -> Scenario {
    Scenario::new(
        volumes
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect(),
    )
}

// One run: its time less the harness's own looks, and what it came to.
struct Timed {
    took: Duration,
    said: String,
    requests: usize,
    outcome: Option<Outcome>,
}

async fn timed(scenario: &Scenario) -> Timed {
    let start = Instant::now();
    let result = try_run(scenario).await;
    let wall = start.elapsed();
    match result {
        Ok(outcome) => Timed {
            took: wall - outcome.graph.observed,
            said: format!("{:?}/{}", outcome.status, outcome.files.len()),
            requests: outcome.requests.values().sum(),
            outcome: Some(outcome),
        },
        Err(stall) => Timed {
            took: wall,
            said: format!("stalled {:?}", stall.status),
            requests: 0,
            outcome: None,
        },
    }
}

fn ms(took: Duration) -> f64 {
    took.as_secs_f64() * 1000.0
}

async fn row(name: &str, scenario: Scenario) {
    let graph = timed(&scenario).await;
    let tally = graph
        .outcome
        .as_ref()
        .map(|outcome| outcome.graph.checkpoints)
        .unwrap_or_default();
    eprintln!(
        "PERF | {name:<14} | {:>10.1} | {:<28} | {:>6} | {:>6} | {:>9} | {:>7}",
        ms(graph.took),
        graph.said,
        graph.requests,
        tally.checkpoints,
        tally.facts,
        tally.rows,
    );
}

#[tokio::test]
#[ignore = "timings, run on request: no assertion"]
async fn the_graph_engine_timed_on_the_representative_jobs() {
    eprintln!(
        "PERF | scenario       |   graph ms | outcome                      |   reqs | ckpts  | facts B   | rows"
    );
    row("par2_damaged", par2_damaged()).await;
    row("rar_store_20", rar_store_20()).await;
    row("chain_10", chained(&CHAIN)).await;
    row("rar_200", rar_200()).await;
    row("solid_rar", solid()).await;
    row("small_200", small_200()).await;
    row("7z_xz_200", sevenz_xz(200)).await;
    row("7z_xz_1000", sevenz_xz(1000)).await;
}
