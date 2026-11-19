// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR set posted under names that number nothing, its last volume posted
// first or in any other order, with no recovery set and beside a PAR2 set
// that names its volumes: every volume arrives whole, so the job completes
// with the member as written.
//
// Each case fails without the two rules it reads. A stored three-volume set
// whose headers state no number: its last volume, offered first to the
// store its first volume founds, is held after the first by its links; the
// middle volume then fits only there, and the last makes way for it
// (`claim::makes_way`). The compressed four-volume set, whose end records
// state each volume's number: the store hands its last volume, held after
// the first by its links, to the engine before its end record is read, and
// the engine's holder makes way for the volume whose front states that place
// (`EngineObject::provisional`). Before it, the run read the first and last
// volumes as the first two, found the member's checksum wrong and said both
// damaged; beside PAR2, the renames moved the volumes, the run that read
// again found the second place empty, and the set asked for a fifth volume.

use sha2::Digest;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::rar_rev::lz_four;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::single_member_rar4_old_numbering_set;

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A set: its label, its volumes as RAR named them, its one member, and the
// SHA-256 of that member as written.
struct Set {
    label: &'static str,
    volumes: Vec<(String, Vec<u8>)>,
    member: &'static str,
    sha256: String,
}

// A stored three-volume RAR4 set of one member whose headers state no
// volume number, and the compressed four-volume set.
fn sets() -> Vec<Set> {
    let body = payload(9 * ARTICLE, 83);
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    vec![
        Set {
            label: "stored",
            volumes: single_member_rar4_old_numbering_set("silver.horizon", "beacon.bin", &body, 3),
            member: "beacon.bin",
            sha256: hex::encode(sha2::Sha256::digest(&body)),
        },
        Set {
            label: "lz_four",
            volumes: lz_four(),
            member: "compressible.txt",
            sha256: oracle["archives"]["rar4_lz_four"]["compressible.txt"]["sha256"]
                .as_str()
                .unwrap()
                .to_owned(),
        },
    ]
}

// The set's volumes posted in `order`, each under a name that numbers
// nothing, beside a PAR2 set over the volumes as named when `par2`; each
// batch the queue hands out served last article first when `backwards`.
fn posted(volumes: &[(String, Vec<u8>)], order: &[usize], par2: bool, backwards: bool) -> Scenario {
    let mut posts: Vec<Post> = order
        .iter()
        .map(|volume| {
            Post::plain(
                &format!("{:032x}", 0x0b5c_0000 + volume),
                volumes[*volume].1.clone(),
            )
        })
        .collect();
    if par2 {
        let files: Vec<(&str, &[u8])> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("silver.horizon", &files, 4));
    }
    let mut scenario = Scenario::new(posts);
    scenario.article = 4096;
    match backwards {
        true => scenario.backwards(),
        false => scenario,
    }
}

// The case, when the job did not complete with the member as written.
fn failed(case: String, set: &Set, outcome: &Outcome) -> Option<String> {
    let whole = outcome
        .files
        .get(&delivered(set.member))
        .is_some_and(|bytes| hex::encode(sha2::Sha256::digest(bytes)) == set.sha256);
    (outcome.status != Some(JobStatus::Complete) || !whole).then(|| {
        format!(
            "{case}: {:?} {:?} {:?}",
            outcome.status,
            outcome.files.keys().collect::<Vec<_>>(),
            outcome.graph.claims
        )
    })
}

#[tokio::test]
async fn a_nameless_rar_set_posted_last_volume_first_completes() {
    let mut failures = Vec::new();
    for set in sets() {
        let order: Vec<usize> = (0..set.volumes.len()).rev().collect();
        for par2 in [false, true] {
            for backwards in [false, true] {
                let case = format!("{} par2 {par2} backwards {backwards}", set.label);
                let outcome = run(&posted(&set.volumes, &order, par2, backwards)).await;
                failures.extend(failed(case, &set, &outcome));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// Every order of `0..n`, each once.
fn orders(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for order in orders(n - 1) {
        for at in 0..=order.len() {
            let mut next = order.clone();
            next.insert(at, n - 1);
            out.push(next);
        }
    }
    out
}

// Both sets with no recovery set, posted in every order of their volumes:
// the stored set's 6 and the compressed set's 24, each served forward and
// backwards.
//
// The two middle volumes each only carry the member on, so their links
// place them in the order they come, and the store hands them to an engine
// with fronts that stop short of their end records: [0, 2, 1, 3] is held as
// it came. The engine reads each one's tail, whose end record states its
// number, and a volume its tail puts elsewhere goes back to be claimed
// there, before its run reads the member's checksum.
#[tokio::test]
async fn a_nameless_rar_set_posted_in_any_order_completes() {
    let mut failures = Vec::new();
    for set in sets() {
        for order in orders(set.volumes.len()) {
            for backwards in [false, true] {
                let case = format!("{} order {order:?} backwards {backwards}", set.label);
                let outcome = run(&posted(&set.volumes, &order, false, backwards)).await;
                failures.extend(failed(case, &set, &outcome));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
