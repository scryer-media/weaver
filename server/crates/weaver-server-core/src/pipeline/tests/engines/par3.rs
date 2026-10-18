// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR3 sets posted as their own files, under both engines: beside plain
// files clean, damaged, lost and short of blocks, under obfuscated names
// clean and damaged,
// beside a stored RAR5 set and a stored 7z, beside a PAR2 set describing
// the same file, and over a member it cannot repair beside a clean one.

use std::collections::BTreeMap;
use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, par2_set, par3_set, payload, run,
    run_both, try_run,
};
use super::rar::{assert_classic_stalls, delivered, posts};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::single_member_store_set;

// Plain files followed by one PAR3 set over them, the index then the
// volumes; the files are numbered as listed, the set after.
fn covered(files: &[(&str, Vec<u8>)], blocks: u64) -> Vec<Post> {
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts.extend(par3_set("lantern", &described, blocks));
    posts
}

fn claims_all(claims: &BTreeMap<String, Claim>, expected: Claim) -> bool {
    !claims.is_empty() && claims.values().all(|claim| *claim == expected)
}

// Every named file delivered with its bytes, and nothing else.
fn assert_delivers(outcome: &Outcome, files: &[(&str, &[u8])]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    let expected: BTreeMap<String, &[u8]> = files
        .iter()
        .map(|(name, bytes)| (delivered(name), *bytes))
        .collect();
    let got: BTreeMap<String, &[u8]> = outcome
        .files
        .iter()
        .map(|(name, bytes)| (name.clone(), bytes.as_slice()))
        .collect();
    assert!(got == expected, "{}: {:?}", outcome.engine, got.keys());
}

fn assert_repaired_once(graph: &Outcome, blocks: u64) {
    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(view.blocks_reachable, blocks);
    assert!(graph.reads > 0, "the repair read nothing back");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// PAR3 checks a block by its BLAKE3 fingerprint, which the segment
// checksums cannot vouch for, so the graph reads each bound member back
// once to rule it clean: unlike a clean PAR2 job, the reads are not zero.
// Nothing beyond the volumes' heads is fetched for it, and those heads are
// all that holds a block, so none is reachable.
#[tokio::test]
async fn plain_files_beside_a_clean_par3_set_complete_and_graph_reads_each_member_back_once() {
    let lantern = payload(3 * ARTICLE, 61);
    let beacon = payload(2 * ARTICLE - 9, 62);
    let scenario = Scenario::new(covered(
        &[
            ("lantern.bin", lantern.clone()),
            ("beacon.dat", beacon.clone()),
        ],
        2,
    ));
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert_delivers(
        &graph,
        &[("lantern.bin", &lantern), ("beacon.dat", &beacon)],
    );
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(graph.volume_samples(&classic), vec![(3, 0), (4, 0)]);
    assert_eq!(graph.reads, 2, "one read back per member");
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert_eq!(view.blocks_reachable, 0);
}

#[tokio::test]
async fn one_damaged_article_under_par3_is_repaired_under_both_engines() {
    let data = payload(4 * ARTICLE, 63);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 2)).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert_delivers(&classic, &[("lantern.bin", &data)]);
    assert_delivers(&graph, &[("lantern.bin", &data)]);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_repaired_once(&graph, 2);
}

#[tokio::test]
async fn a_lost_article_under_par3_is_repaired_with_its_blocks_reported() {
    let data = payload(4 * ARTICLE, 64);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 2)).lost(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert_delivers(&classic, &[("lantern.bin", &data)]);
    assert_delivers(&graph, &[("lantern.bin", &data)]);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_repaired_once(&graph, 2);
}

// Divergence: two damaged slices and one recovery block. The graph fails
// the repair with the shortfall named and leaves the member a damaged
// leaf; classic completes the job with the posted bytes. That classic
// outcome is recorded, not explained.
#[tokio::test]
async fn recovery_short_of_blocks_under_par3_fails_as_repair_under_graph_and_completes_under_classic()
 {
    let data = payload(4 * ARTICLE, 65);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 1))
        .damaged(0, 1)
        .damaged(0, 2);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert_delivers(&classic, &[("lantern.bin", &data)]);

    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            "PAR3 recovery failed for 1 set(s): lantern.par3 (lantern.bin): not repairable: \
             2 damaged slices, only 1 recovery blocks reachable"
        )
    );
    let view = graph.graph();
    assert_eq!(view.blocks_reachable, 1);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    );
    assert_eq!(view.claims["lantern.bin"], Claim::Damaged, "a damaged leaf");
}

// Hex names in place of the described ones, then the set under a hex name.
fn obfuscated(files: &[(&str, Vec<u8>)], blocks: u64) -> Vec<Post> {
    const HEX: [&str; 2] = ["3f9a0c71e25b84d6", "a07d5e19c4b26f83"];
    let described: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = files
        .iter()
        .zip(HEX)
        .map(|((_, bytes), hex)| Post::plain(hex, bytes.clone()))
        .collect();
    posts.extend(par3_set("e81b6d3f0a4c9725", &described, blocks));
    posts
}

// Both engines bind the hex-named files by their bytes and rename them from
// the set; the graph reads each back to identify it.
#[tokio::test]
async fn obfuscated_names_are_renamed_by_par3_under_both_engines() {
    let lantern = payload(3 * ARTICLE, 66);
    let beacon = payload(2 * ARTICLE + 7, 67);
    let files = [
        ("lantern.bin", lantern.clone()),
        ("beacon.bin", beacon.clone()),
    ];
    let (classic, graph) = run_both(&Scenario::new(obfuscated(&files, 2))).await;
    assert_same(&classic, &graph);
    let expected = [("lantern.bin", &lantern[..]), ("beacon.bin", &beacon[..])];
    assert_delivers(&classic, &expected);
    assert_delivers(&graph, &expected);
    let view = graph.graph();
    assert_eq!(
        view.claims.keys().collect::<Vec<_>>(),
        ["beacon.bin", "lantern.bin"],
        "the namespace renamed both members"
    );
    assert!(claims_all(&view.claims, Claim::Complete));
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(graph.reads > 0, "identifying a member reads it back");
}

// A damaged article inside a hex-named file: the one interior block that
// fails leaves the rest of it to name it, and the set repairs it.
#[tokio::test]
async fn a_damaged_obfuscated_member_under_par3_is_repaired_and_renamed_under_both_engines() {
    let lantern = payload(4 * ARTICLE, 72);
    let beacon = payload(2 * ARTICLE + 7, 73);
    let files = [
        ("lantern.bin", lantern.clone()),
        ("beacon.bin", beacon.clone()),
    ];
    let scenario = Scenario::new(obfuscated(&files, 2)).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    let expected = [("lantern.bin", &lantern[..]), ("beacon.bin", &beacon[..])];
    assert_delivers(&classic, &expected);
    assert_delivers(&graph, &expected);
    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}

// Divergence, as beside PAR2: classic waits at Downloading on the damaged
// set and never settles; the graph repairs the volume in the stream and
// delivers the member.
#[tokio::test]
async fn a_two_volume_rar5_set_beside_par3_with_a_damaged_article_is_repaired_in_the_stream_under_graph()
 {
    let body = payload(7 * ARTICLE + 13, 68);
    let volumes = single_member_store_set("beacon.bin", &body, 2);
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par3_set("silver.horizon", &described, 2);
    let mut all = posts(volumes.clone());
    all.extend(set);
    let scenario = Scenario::new(all).damaged(0, 2);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_delivers(&graph, &[("beacon.bin", &body)]);
    assert_repaired_once(&graph, 2);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
}

fn stored_7z() -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    let members = vec![
        ("ember.bin", payload(5 * ARTICLE + 11, 41)),
        ("quill.bin", payload(3 * ARTICLE - 3, 42)),
    ];
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    for (name, bytes) in &members {
        let mut entry = ArchiveEntry::new_file(*name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.clone())))
            .unwrap();
    }
    (writer.finish().unwrap().into_inner(), members)
}

// As beside PAR2: classic demotes the damaged container and fetches its
// first article again; the graph repairs the volume in the stream.
#[tokio::test]
async fn a_damaged_stored_7z_beside_par3_is_repaired_under_both_engines_and_only_classic_refetches()
{
    let (archive, members) = stored_7z();
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par3_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is member bytes: the signature header is the
    // first article's, and the map is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    let expected: Vec<(&str, &[u8])> = members
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    assert_delivers(&classic, &expected);
    assert_delivers(&graph, &expected);
    assert_eq!(classic.refetched(), vec![(0, 0)]);
    assert_repaired_once(&graph, 2);
    assert_eq!(graph.graph().claims["lantern.7z"], Claim::Complete);
}

// `ember.bin` is described by a PAR2 set, posted first, and a PAR3 set.
// The PAR2 set repairs the slice; the PAR3 set then reads the member back
// clean and repairs nothing.
#[tokio::test]
async fn a_member_a_par2_and_a_par3_set_describe_is_repaired_once() {
    let ember = payload(3 * ARTICLE, 69);
    let mut posts = vec![Post::plain("ember.bin", ember.clone())];
    posts.extend(par2_set("lantern", &[("ember.bin", &ember)], 2));
    posts.extend(par3_set("beacon", &[("ember.bin", &ember)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert_delivers(&graph, &[("ember.bin", &ember)]);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(
        graph.graph().outcomes,
        vec![
            Some(RepairOutcome::Repaired { slices: 1 }),
            Some(RepairOutcome::Clean)
        ]
    );
}

// `beacon.bin` takes two damaged slices against one recovery block;
// `lantern.bin` beside it arrives whole. The partial-delivery invariant
// (2026-10-18) keeps the whole member delivered and ends the job Partial,
// with `beacon.bin` left a damaged leaf. Classic completes the job with both
// files as posted, recorded, not explained.
#[tokio::test]
async fn a_member_par3_cannot_repair_beside_a_whole_one_fails_the_job_under_graph_and_completes_under_classic()
 {
    let lantern = payload(3 * ARTICLE, 70);
    let beacon = payload(4 * ARTICLE, 71);
    let mut posts = vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par3_set(
        "lantern",
        &[("lantern.bin", &lantern), ("beacon.bin", &beacon)],
        1,
    ));
    let scenario = Scenario::new(posts).damaged(1, 1).damaged(1, 2);
    let (classic, graph) = run_both(&scenario).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
    }
    assert_delivers(
        &classic,
        &[("lantern.bin", &lantern), ("beacon.bin", &beacon)],
    );

    let view = graph.graph();
    assert_eq!(view.claims["lantern.bin"], Claim::Complete);
    assert_eq!(view.claims["beacon.bin"], Claim::Damaged);
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    );
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    // The repair that could not rebuild the member is what left it short.
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(
        graph.files[&delivered("lantern.bin")] == lantern,
        "{:?}",
        graph.files.keys()
    );
}
