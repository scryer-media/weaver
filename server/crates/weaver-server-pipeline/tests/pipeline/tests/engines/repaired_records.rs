// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A stored multi-volume RAR whose volumes each carry a recovery record, an
// SFV listing over every volume and a PAR2 set over every volume, as scene
// posts are. Articles of one volume are lost: more units of it than its
// record has parity for, but fewer blocks than the PAR2 set holds. The set
// rebuilds the volume and the member inside it is delivered whole, so the
// job completes and no repair is said to have failed, also when a PAR2
// recovery volume lost an article that nothing rebuilds: the record's
// ruling on a volume the set made whole fails nothing. With no set able to
// rebuild the loss, the job does not complete.

use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes, payload, run};
use super::sfv::{crc, listing};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::protected_store_set;
use weaver_server_base::events::model::PipelineEvent;

const MEMBER: &str = "fixture.release.mkv";

// Three volumes of about sixty articles each, each record of one parity
// unit over four data units; the listing, then a PAR2 set of `blocks`
// one-article blocks over every volume. Articles 2 and 40 of the middle
// volume, in two of its record's units, are lost.
fn scene(blocks: u32, volumes_of_par2: u32) -> (Scenario, Vec<u8>) {
    let body = payload(180 * ARTICLE + 7, 63);
    let volumes = protected_store_set(MEMBER, &body, 3, 1);
    let named: Vec<(String, u32)> = volumes
        .iter()
        .map(|(name, bytes)| (name.clone(), crc(bytes)))
        .collect();
    let entries: Vec<(&str, u32)> = named
        .iter()
        .map(|(name, crc)| (name.as_str(), *crc))
        .collect();
    let covered: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.push(listing("fixture.release.sfv", &entries));
    posts.extend(par2_volumes(
        "fixture.release",
        &covered,
        blocks,
        volumes_of_par2,
    ));
    (Scenario::new(posts).lost(1, 2).lost(1, 40), body)
}

fn repair_failures(outcome: &Outcome) -> Vec<&str> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RepairFailed { error, .. } => Some(error.as_str()),
            _ => None,
        })
        .collect()
}

fn summary(outcome: &Outcome) -> String {
    format!(
        "status {:?}, failure {:?}, rulings {:?}, repair failures {:?}",
        outcome.status,
        outcome.failure,
        outcome.graph().rulings,
        repair_failures(outcome)
    )
}

// The member is delivered whole, the PAR2 set repaired, the job is
// Complete and no repair is said to have failed.
fn assert_complete(outcome: &Outcome, body: &[u8]) {
    let delivered = outcome
        .files
        .iter()
        .find(|(path, _)| path.ends_with(MEMBER))
        .map(|(_, bytes)| bytes.as_slice());
    assert_eq!(delivered, Some(body), "{}", summary(outcome));
    assert!(
        outcome
            .graph()
            .rulings
            .iter()
            .any(|(kind, ruling)| kind == "par2"
                && matches!(ruling, Some(RepairOutcome::Repaired { .. }))),
        "{}",
        summary(outcome)
    );
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        summary(outcome)
    );
    assert!(repair_failures(outcome).is_empty(), "{}", summary(outcome));
}

// The PAR2 set rebuilds the volume its record could not: the member is
// whole, the job completes, and no repair failed.
#[tokio::test]
async fn a_volume_past_its_record_that_par2_rebuilds_completes() {
    let (scenario, body) = scene(8, 1);
    assert_complete(&run(&scenario).await, &body);
}

// The same, the PAR2 set spread over two volumes and an article of the
// second lost too: the set still holds blocks enough and rebuilds the RAR
// volume, so the job completes. Nothing rebuilds a recovery volume, and
// nothing needs to: what it lost is no part of what the job delivers.
#[tokio::test]
async fn a_volume_past_its_record_that_par2_rebuilds_completes_beside_a_short_par2_volume() {
    let (scenario, body) = scene(8, 2);
    // Posts: three RAR volumes, the listing, the index, two PAR2 volumes.
    let scenario = scenario.lost(6, 0);
    assert_complete(&run(&scenario).await, &body);
}

// The same loss with one PAR2 block: nothing rebuilds the volume, and the
// job does not complete.
#[tokio::test]
async fn a_volume_past_its_record_and_past_par2_does_not_complete() {
    let (scenario, _) = scene(1, 1);
    let outcome = run(&scenario).await;
    assert_ne!(
        outcome.status,
        Some(JobStatus::Complete),
        "{}",
        summary(&outcome)
    );
    assert!(
        !outcome.files.keys().any(|path| path.ends_with(MEMBER)),
        "{}",
        summary(&outcome)
    );
}
