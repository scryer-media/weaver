// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Jobs whose archives yield many small members, at two sizes: what their
// checkpoints asked of the database, and the time the run took less the
// harness's looks. The counts follow the clock (a write is due by its age),
// so nothing here asserts them; the bound that holds whatever the clock is
// `checkpoint::tests::steps_take_no_checkpoint_each_while_an_old_change_cannot_be_written`.

use std::io::{Cursor, Write};
use std::time::Instant;

use super::harness::{Post, Scenario, run};
use super::nesting::noise;
use crate::pipeline::tests::direct_store::multi_member_store_set;

fn stored_set(files: &[(String, Vec<u8>)]) -> Scenario {
    let named: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    Scenario::new(
        multi_member_store_set(&named, 4)
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect(),
    )
}

// `count` small members stored across four RAR volumes.
fn stored(count: usize) -> Scenario {
    let files: Vec<(String, Vec<u8>)> = (0..count)
        .map(|index| {
            (
                format!("app/d{:02}/m{index:05}.bin", index % 37),
                noise(5 + (index * 13) % 97, 3000 + index as u64),
            )
        })
        .collect();
    stored_set(&files)
}

// `zips` small stored zips of `each` members, stored across four RAR
// volumes; every zip's members carry the same names.
fn zips(zips: usize, each: usize) -> Scenario {
    let files: Vec<(String, Vec<u8>)> = (0..zips)
        .map(|zip| {
            let mut writer = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
            for member in 0..each {
                writer
                    .start_file(format!("data{member:02}.bin"), super::zip::stored())
                    .unwrap();
                writer
                    .write_all(&noise(5 + member % 31, (zip * each + member) as u64))
                    .unwrap();
            }
            (
                format!("app/d{:02}/x{zip:04}.thu.zip", zip % 7),
                writer.finish().unwrap().into_inner(),
            )
        })
        .collect();
    stored_set(&files)
}

async fn row(name: &str, members: usize, scenario: Scenario) {
    let start = Instant::now();
    let outcome = run(&scenario).await;
    let took = start.elapsed() - outcome.graph.observed;
    assert!(outcome.completed(), "{:?}", outcome.failure);
    assert_eq!(outcome.files.len(), members);
    eprintln!(
        "MANY | {name:<6} | {members:>6} | {:>9.1} ms | {:?}",
        took.as_secs_f64() * 1000.0,
        outcome.graph.checkpoints,
    );
}

#[tokio::test]
#[ignore = "measurements, run on request: no assertion on time or clock-driven counts"]
async fn many_small_members_measured() {
    row("stored", 500, stored(500)).await;
    row("stored", 1500, stored(1500)).await;
    row("zips", 400, zips(20, 20)).await;
    row("zips", 1600, zips(80, 20)).await;
}
