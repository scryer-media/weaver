// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph's observability lines over whole jobs: what a set ruled and
// how, what it bound a member as, and each file as it was delivered.

use super::harness::{ARTICLE, Post, Scenario, par2_set, payload, run};
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::lines::Lines;

const JOB: u64 = super::harness::JOB.0;

// Plain files under one PAR2 set named `lantern`.
fn covered(files: &[(&str, &[u8])], posted: &[&str]) -> Vec<Post> {
    let mut posts: Vec<Post> = files
        .iter()
        .zip(posted)
        .map(|((_, bytes), name)| Post::plain(name, bytes.to_vec()))
        .collect();
    posts.extend(par2_set("lantern", files, 2));
    posts
}

// The line's fields, by key, in the order it names them.
fn fields(line: &str) -> Vec<(&str, &str)> {
    line.split(' ')
        .filter_map(|field| field.split_once('='))
        .collect()
}

fn keys<'l>(fields: &[(&'l str, &str)]) -> Vec<&'l str> {
    fields.iter().map(|(key, _)| *key).collect()
}

fn value<'l>(fields: &[(&str, &'l str)], key: &str) -> &'l str {
    fields
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, value)| *value)
        .unwrap_or_else(|| panic!("no {key}"))
}

const RULED: [&str; 11] = [
    "job_id",
    "node",
    "kind",
    "set",
    "recovery_set_id",
    "slice_size",
    "blocks_reachable",
    "damaged_slices",
    "outcome",
    "slices_repaired",
    "verification",
];

async fn ruled(scenario: Scenario) -> (String, u64) {
    let (lines, _guard) = Lines::capture();
    let outcome = run(&scenario, Engine::Graph).await;
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let ruled = lines.of("recovery set ruled");
    assert_eq!(ruled.len(), 1, "{ruled:?}");
    (ruled[0].clone(), outcome.reads)
}

// Every article arrived verified and the grid vouched for every slice: the
// set ruled clean without reading a byte back.
#[tokio::test]
async fn a_set_the_grid_vouches_for_logs_verification_grid() {
    let data = payload(4 * ARTICLE, 41);
    let files: [(&str, &[u8]); 1] = [("lantern.bin", &data)];
    let (line, reads) = ruled(Scenario::new(covered(&files, &["lantern.bin"]))).await;
    assert_eq!(reads, 0);
    let fields = fields(&line);
    assert_eq!(keys(&fields), RULED, "{line}");
    let id = value(&fields, "recovery_set_id");
    assert!(
        id.len() == 32
            && id
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')),
        "{line}"
    );
    let expected = format!(
        "recovery set ruled job_id={JOB} node={} kind=par2 set=lantern.par2 recovery_set_id={id} \
         slice_size={ARTICLE} blocks_reachable=2 damaged_slices=0 outcome=clean slices_repaired=0 \
         verification=grid",
        value(&fields, "node"),
    );
    assert_eq!(line, expected);
}

// Articles that straddle the slices leave the grid short of proof, so the
// set read the member back to rule it clean.
#[tokio::test]
async fn a_set_that_read_bytes_back_logs_verification_read() {
    let data = payload(4 * ARTICLE, 42);
    let files: [(&str, &[u8]); 1] = [("lantern.bin", &data)];
    let scenario = Scenario::new(covered(&files, &["lantern.bin"])).framed(ARTICLE - 16);
    let (line, reads) = ruled(scenario).await;
    assert!(reads > 0, "the repair read nothing back");
    let fields = fields(&line);
    assert_eq!(keys(&fields), RULED, "{line}");
    assert_eq!(value(&fields, "outcome"), "clean", "{line}");
    assert_eq!(value(&fields, "verification"), "read", "{line}");
}

// A member posted under a hex name is bound by its bytes, as the name the
// set describes it by; one posted as described is bound by name.
#[tokio::test]
async fn an_obfuscated_member_logs_recovery_bound_by_content() {
    let lamp = payload(3 * ARTICLE, 43);
    let beacon = payload(2 * ARTICLE + 5, 44);
    let files: [(&str, &[u8]); 2] = [("lantern.bin", &lamp), ("beacon.bin", &beacon)];
    let (lines, _guard) = Lines::capture();
    let outcome = run(
        &Scenario::new(covered(&files, &["3f9a0c71e25b84d6", "beacon.bin"])),
        Engine::Graph,
    )
    .await;
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let mut bound: Vec<String> = lines
        .of("recovery bound")
        .iter()
        .map(|line| {
            let fields = fields(line);
            assert_eq!(
                keys(&fields),
                ["job_id", "node", "kind", "set", "member", "as", "by"],
                "{line}"
            );
            assert_eq!(value(&fields, "job_id"), JOB.to_string());
            assert_eq!(value(&fields, "kind"), "par2");
            assert_eq!(value(&fields, "set"), "lantern.par2");
            format!(
                "{} as={} by={}",
                value(&fields, "member"),
                value(&fields, "as"),
                value(&fields, "by")
            )
        })
        .collect();
    bound.sort();
    assert_eq!(
        bound,
        [
            "3f9a0c71e25b84d6 as=lantern.bin by=content",
            "beacon.bin as=beacon.bin by=name",
        ]
    );
}

// Two posts that want one name: the second is placed beside the first, and
// its line says the name it is delivered under and the one it wanted.
#[tokio::test]
async fn a_delivery_the_ledger_renamed_logs_member_delivered_from_its_wanted_name() {
    let (lines, _guard) = Lines::capture();
    let outcome = run(
        &Scenario::new(vec![
            Post::plain("lantern.bin", payload(2 * ARTICLE, 45)),
            Post::plain("Lantern.bin", payload(ARTICLE + 3, 46)),
        ]),
        Engine::Graph,
    )
    .await;
    assert!(outcome.completed(), "{:?}", outcome.failure);
    assert_eq!(
        lines.of("member delivered"),
        [
            format!(
                "member delivered job_id={JOB} name=lantern.bin from=lantern.bin depth=0 whole=true"
            ),
            format!(
                "member delivered job_id={JOB} name=Lantern (1).bin from=Lantern.bin depth=0 \
                 whole=true"
            ),
        ],
        "{:?}",
        outcome.files.keys()
    );
}
