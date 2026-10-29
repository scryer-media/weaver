// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR3 sets posted as their own files,: beside plain
// files clean, damaged, lost and short of blocks, under obfuscated names
// clean and damaged,
// beside a stored RAR5 set and a stored 7z, beside a PAR2 set describing
// the same file, and over a member it cannot repair beside a clean one;
// under graph, over a file the post withholds, under FFT in one cohort and
// in two uneven ones, carrying its input as Data packets, and beside a
// second set of the other code.

use std::collections::BTreeMap;
use std::io::Cursor;

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
use weaver_graph::recovery::RepairOutcome;

use par3_rs::creation::{CreationCodec, CreationOptions};

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_fetched_once, par2_set, par3_set,
    par3_set_with, payload, repair_and_terminal_events, run, run_holding,
};
use super::rar::{delivered, posts};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
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
        "{:?} {:?}",
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
    assert!(got == expected, "{:?}", got.keys());
}

// `blocks` is what the set had reached when it ruled, as its ruling says:
// the repair runs in the stream as soon as a block per damaged slice is in
// hand, so a one-slice repair rules on one block whatever the set posted.
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
// A file posted short of the length its set describes, its last article
// ending early and arriving clean. The set finds the last block wrong and
// rebuilds it to the described length, and the member is that length: the
// file is delivered whole, every described byte in it.
#[tokio::test]
async fn a_file_posted_short_of_its_description_is_rebuilt_to_the_described_length() {
    let data = payload(4 * ARTICLE, 63);
    let described = [("lantern.bin", data.as_slice())];
    let mut posts = vec![Post::plain("lantern.bin", data[..data.len() - 24].to_vec())];
    posts.extend(par3_set("lantern", &described, 2));
    let graph = run(&Scenario::new(posts)).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(graph.files.values().next(), Some(&data));
}

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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert_delivers(
        &graph,
        &[("lantern.bin", &lantern), ("beacon.dat", &beacon)],
    );
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_eq!(graph.reads, 2, "one read back per member");
    let view = graph.graph();
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert_eq!(view.blocks_reachable, 0);
}

#[tokio::test]
async fn one_damaged_article_under_par3_is_repaired() {
    let data = payload(4 * ARTICLE, 63);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 2)).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert_delivers(&graph, &[("lantern.bin", &data)]);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_repaired_once(&graph, 1);
}

#[tokio::test]
async fn a_lost_article_under_par3_is_repaired_with_its_blocks_reported() {
    let data = payload(4 * ARTICLE, 64);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 2)).lost(0, 1);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert_delivers(&graph, &[("lantern.bin", &data)]);
    assert!(claims_all(&graph.graph().claims, Claim::Complete));
    assert_repaired_once(&graph, 1);
}

// Two damaged slices and one recovery block. The graph fails the repair
// with the shortfall named and leaves the member a damaged leaf.
#[tokio::test]
async fn recovery_short_of_blocks_under_par3_fails_as_repair() {
    let data = payload(4 * ARTICLE, 65);
    let scenario = Scenario::new(covered(&[("lantern.bin", data.clone())], 1))
        .damaged(0, 1)
        .damaged(0, 2);
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());
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

// `lantern.bin` posted with `zeroed` of its blocks already zeroed, so every
// article arrives with a good checksum and only the set's block
// fingerprints can see the damage, against a set of `recovery` blocks
// described over the bytes before the zeroing.
fn zeroed_under_par3(blocks: usize, zeroed: std::ops::Range<usize>, recovery: u64) -> Scenario {
    let original = payload(blocks * ARTICLE, 0);
    let mut posted = original.clone();
    posted[zeroed.start * ARTICLE..zeroed.end * ARTICLE].fill(0);
    let mut posts = vec![Post::plain("lantern.bin", posted)];
    posts.extend(par3_set("lantern", &[("lantern.bin", &original)], recovery));
    Scenario::new(posts)
}

// The read-back finds the zeroed blocks, the set rules the repair
// impossible with both counts and reports the ranges it proved bad, so the
// member is a damaged leaf the failed set covers: unrepaired, not
// delivered, and the job fails as a repair failure.
fn assert_fails_short_of_blocks(reachable: u64, graph: Outcome) {
    let damaged = 12;
    assert!(graph.refetched().is_empty());
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());

    let view = graph.graph();
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Impossible { damaged, reachable })]
    );
    assert_eq!(view.blocks_reachable, reachable);
    assert_eq!(
        graph.failure.as_deref(),
        Some(
            format!(
                "PAR3 recovery failed for 1 set(s): lantern.par3 (lantern.bin): not repairable: \
                 {damaged} damaged slices, only {reachable} recovery blocks reachable"
            )
            .as_str()
        )
    );
    assert_eq!(view.claims["lantern.bin"], Claim::Damaged, "a damaged leaf");
    assert_eq!(view.delivery.unrepaired, ["lantern.bin"]);
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
}

// Twelve zeroed blocks against one recovery block, every article one block.
#[tokio::test]
async fn zeroed_blocks_in_clean_articles_short_of_par3_recovery_fail_as_repair() {
    const RECOVERY: u64 = 1;
    let scenario = zeroed_under_par3(40, 20..32, RECOVERY);
    assert_eq!(scenario.posts.len(), 3, "an index and one volume");
    assert_fails_short_of_blocks(RECOVERY, run(&scenario).await);
}

// The same shortfall with four recovery blocks spread over three volumes
// (1 + 2 + 1, as the creator lays them out) and articles of eleven and a
// fraction blocks each, so the zeroed run straddles two articles at neither
// edge of either.
#[tokio::test]
async fn zeroed_blocks_short_of_par3_recovery_over_three_volumes_fail_as_repair() {
    const RECOVERY: u64 = 4;
    let scenario = zeroed_under_par3(96, 20..32, RECOVERY).framed(11 * ARTICLE + 46);
    assert_eq!(scenario.posts.len(), 5, "an index and three volumes");
    assert_fails_short_of_blocks(RECOVERY, run(&scenario).await);
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

// The hex-named files are bound by their bytes and renamed from the set;
// the graph reads each back to identify it.
#[tokio::test]
async fn obfuscated_names_are_renamed_by_par3() {
    let lantern = payload(3 * ARTICLE, 66);
    let beacon = payload(2 * ARTICLE + 7, 67);
    let files = [
        ("lantern.bin", lantern.clone()),
        ("beacon.bin", beacon.clone()),
    ];
    let graph = run(&Scenario::new(obfuscated(&files, 2))).await;
    assert_fetched_once(&graph);
    let expected = [("lantern.bin", &lantern[..]), ("beacon.bin", &beacon[..])];
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
async fn a_damaged_obfuscated_member_under_par3_is_repaired_and_renamed() {
    let lantern = payload(4 * ARTICLE, 72);
    let beacon = payload(2 * ARTICLE + 7, 73);
    let files = [
        ("lantern.bin", lantern.clone()),
        ("beacon.bin", beacon.clone()),
    ];
    let scenario = Scenario::new(obfuscated(&files, 2)).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    let expected = [("lantern.bin", &lantern[..]), ("beacon.bin", &beacon[..])];
    assert_delivers(&graph, &expected);
    let view = graph.graph();
    assert_eq!(
        view.outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(claims_all(&view.claims, Claim::Complete));
}

// A hex name that ends in a piece number reads as a piece of a split set
// until the set names the member: then nothing holds it as a piece, and it
// is repaired and delivered under the name the set gives it.
#[tokio::test]
async fn an_obfuscated_member_named_like_a_split_piece_is_repaired_and_renamed() {
    let lantern = payload(4 * ARTICLE, 74);
    let described = [("lantern.mkv", lantern.as_slice())];
    let mut posts = vec![Post::plain(
        "51273aad56a8b904e96928935278a627.201",
        lantern.clone(),
    )];
    posts.extend(par3_set("lantern", &described, 2));
    let graph = run(&Scenario::new(posts).damaged(0, 1)).await;
    assert_delivers(&graph, &[("lantern.mkv", &lantern[..])]);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// As beside PAR2: the volume is repaired in the stream and the member
// delivered.
#[tokio::test]
async fn a_two_volume_rar5_set_beside_par3_with_a_damaged_article_is_repaired_in_the_stream() {
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
    let graph = run(&scenario).await;
    assert_delivers(&graph, &[("beacon.bin", &body)]);
    assert_repaired_once(&graph, 1);
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
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes.clone())))
            .unwrap();
    }
    (writer.finish().unwrap().into_inner(), members)
}

// As beside PAR2: the volume is repaired in the stream.
#[tokio::test]
async fn a_damaged_stored_7z_beside_par3_is_repaired() {
    let (archive, members) = stored_7z();
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par3_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is member bytes: the signature header is the
    // first article's, and the map is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    let expected: Vec<(&str, &[u8])> = members
        .iter()
        .map(|(name, bytes)| (*name, bytes.as_slice()))
        .collect();
    assert_delivers(&graph, &expected);
    assert_repaired_once(&graph, 1);
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
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
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
// with `beacon.bin` left a damaged leaf.
#[tokio::test]
async fn a_member_par3_cannot_repair_beside_a_whole_one_ends_partial() {
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
    let graph = run(&scenario).await;
    assert!(graph.refetched().is_empty());

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

// `ember.bin` under a PAR2 set and `beacon.bin` under a PAR3 set, one
// damaged article each: both sets repair, and the graph announces the
// job's repair once, with the slices of both, before the job completes.
#[tokio::test]
async fn a_par2_and_a_par3_set_repairing_different_members_are_announced_once() {
    let ember = payload(3 * ARTICLE, 72);
    let beacon = payload(3 * ARTICLE, 73);
    let mut posts = vec![
        Post::plain("ember.bin", ember.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par2_set("ember", &[("ember.bin", &ember)], 2));
    posts.extend(par3_set("beacon", &[("beacon.bin", &beacon)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1).damaged(1, 2);
    let graph = run(&scenario).await;
    assert!(
        graph.completed(),
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.graph().outcomes,
        graph.claims,
        repair_and_terminal_events(&graph)
    );
    assert_eq!(
        graph.graph().outcomes,
        vec![
            Some(RepairOutcome::Repaired { slices: 1 }),
            Some(RepairOutcome::Repaired { slices: 1 })
        ]
    );
    assert_eq!(
        repair_and_terminal_events(&graph),
        vec!["RepairComplete 2".to_string(), "JobCompleted".to_string()]
    );
}

// `lantern.bin` under a PAR2 set of one recovery block and a PAR3 set of
// four, two articles damaged: the PAR2 set cannot rebuild them and the PAR3
// set does. The member is whole, so the PAR2 shortfall fails nothing, and
// the graph announces the repair once, with the slices the PAR3 set
// rebuilt, before the job completes.
#[tokio::test]
async fn a_par3_set_repairing_what_a_par2_set_is_short_for_is_announced_once() {
    let lantern = payload(4 * ARTICLE, 74);
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &lantern)], 1));
    posts.extend(par3_set("lantern", &[("lantern.bin", &lantern)], 4));
    let scenario = Scenario::new(posts).damaged(0, 1).damaged(0, 2);
    let graph = run(&scenario).await;
    assert_delivers(&graph, &[("lantern.bin", &lantern)]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(
        graph.graph().outcomes,
        vec![
            Some(RepairOutcome::Impossible {
                damaged: 2,
                reachable: 1
            }),
            Some(RepairOutcome::Repaired { slices: 2 })
        ]
    );
    assert_eq!(
        repair_and_terminal_events(&graph),
        vec!["RepairComplete 2".to_string(), "JobCompleted".to_string()]
    );
}

// A PAR3 set over `files` built with `codec`, `recovery` blocks.
fn par3_coded(
    base: &str,
    files: &[(&str, &[u8])],
    codec: CreationCodec,
    recovery: u64,
) -> Vec<Post> {
    let options = CreationOptions {
        codec,
        recovery_count: recovery,
        ..CreationOptions::default()
    };
    par3_set_with(base, files, options)
}

fn assert_repaired_and_announced(graph: &Outcome, outcomes: &[RepairOutcome], slices: u64) {
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let expected: Vec<Option<RepairOutcome>> = outcomes.iter().copied().map(Some).collect();
    assert_eq!(graph.graph().outcomes, expected);
    assert_eq!(
        repair_and_terminal_events(graph),
        vec![
            format!("RepairComplete {slices}"),
            "JobCompleted".to_string()
        ]
    );
}

// `beacon.bin` is described by the set and never posted: the set announces
// it absent, and the repair writes it whole beside `lantern.bin`.
#[tokio::test]
async fn a_file_a_par3_set_describes_and_the_post_withholds_is_rebuilt() {
    let lantern = payload(3 * ARTICLE, 80);
    let beacon = payload(2 * ARTICLE, 81);
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    posts.extend(par3_set(
        "lantern",
        &[("lantern.bin", &lantern), ("beacon.bin", &beacon)],
        3,
    ));
    let graph = run(&Scenario::new(posts)).await;
    assert_delivers(
        &graph,
        &[("lantern.bin", &lantern), ("beacon.bin", &beacon)],
    );
    assert_repaired_and_announced(&graph, &[RepairOutcome::Repaired { slices: 2 }], 2);
}

// A set written under FFT Reed-Solomon, one cohort, two adjacent articles
// damaged: the FFT recovery blocks are reachable and rebuild them.
#[tokio::test]
async fn two_damaged_articles_under_an_fft_par3_set_are_repaired() {
    let lantern = payload(6 * ARTICLE, 82);
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    let fft = CreationCodec::Fft {
        capacity_log2: 2,
        interleave: 0,
    };
    posts.extend(par3_coded("lantern", &[("lantern.bin", &lantern)], fft, 4));
    let scenario = Scenario::new(posts).damaged(0, 1).damaged(0, 2);
    let graph = run(&scenario).await;
    assert_delivers(&graph, &[("lantern.bin", &lantern)]);
    assert_eq!(graph.graph().blocks_reachable, 4);
    assert_repaired_and_announced(&graph, &[RepairOutcome::Repaired { slices: 2 }], 2);
}

// FFT over two interleaved cohorts of uneven size, nine blocks the last a
// short tail, with damage in both cohorts: each cohort rebuilds its own.
#[tokio::test]
async fn damage_in_both_uneven_cohorts_of_an_interleaved_fft_par3_set_is_repaired() {
    let lantern = payload(8 * ARTICLE + 123, 83);
    let mut posts = vec![Post::plain("lantern.bin", lantern.clone())];
    let fft = CreationCodec::Fft {
        capacity_log2: 1,
        interleave: 1,
    };
    posts.extend(par3_coded("lantern", &[("lantern.bin", &lantern)], fft, 4));
    let scenario = Scenario::new(posts)
        .damaged(0, 1)
        .damaged(0, 4)
        .damaged(0, 8);
    let graph = run(&scenario).await;
    assert_delivers(&graph, &[("lantern.bin", &lantern)]);
    assert_repaired_and_announced(&graph, &[RepairOutcome::Repaired { slices: 3 }], 3);
}

// A set that carries its input as Data packets, posted without the payload
// and without its recovery volumes: the set writes the payload whole from
// the input it carries.
#[tokio::test]
async fn a_par3_set_carrying_its_input_rebuilds_the_unposted_payload() {
    let lantern = payload(3 * ARTICLE, 84);
    let options = CreationOptions {
        store_data: true,
        recovery_count: 1,
        ..CreationOptions::default()
    };
    let posts: Vec<Post> = par3_set_with("lantern", &[("lantern.bin", &lantern)], options)
        .into_iter()
        .filter(|post| !post.name.contains(".vol"))
        .collect();
    assert!(
        posts.len() > 1,
        "{:?}",
        posts.iter().map(|post| &post.name).collect::<Vec<_>>()
    );
    let graph = run(&Scenario::new(posts)).await;
    assert_delivers(&graph, &[("lantern.bin", &lantern)]);
    assert_eq!(graph.graph().blocks_reachable, 0);
    assert_repaired_and_announced(&graph, &[RepairOutcome::Repaired { slices: 3 }], 3);
}

// Two independent sets in one post, one Cauchy and one FFT, each over its
// own damaged member. Every PAR3 file is a volume of the one object, so its
// outcome adds both sets' slices, and the job announces them once.
#[tokio::test]
async fn a_cauchy_and_an_fft_par3_set_each_repair_their_own_member() {
    let lantern = payload(4 * ARTICLE, 85);
    let beacon = payload(5 * ARTICLE + 9, 86);
    let mut posts = vec![
        Post::plain("lantern.bin", lantern.clone()),
        Post::plain("beacon.bin", beacon.clone()),
    ];
    posts.extend(par3_set("lantern", &[("lantern.bin", &lantern)], 2));
    let fft = CreationCodec::Fft {
        capacity_log2: 2,
        interleave: 0,
    };
    posts.extend(par3_coded("beacon", &[("beacon.bin", &beacon)], fft, 4));
    let scenario = Scenario::new(posts)
        .damaged(0, 1)
        .damaged(1, 2)
        .damaged(1, 3);
    let graph = run(&scenario).await;
    assert_delivers(
        &graph,
        &[("lantern.bin", &lantern), ("beacon.bin", &beacon)],
    );
    assert_repaired_and_announced(&graph, &[RepairOutcome::Repaired { slices: 3 }], 3);
}

// The server holds every article of the PAR3 set's one volume and serves
// the rest. The payload lands with a block the set does not vouch for, and
// the set, which took the volume by its posted name, waits on the blocks
// it names rather than ruling short: while the volume is held the job
// reads as fetching repair data, the root still downloading and the post
// lane idle. Once the server lets the volume go the job repairs, no longer
// fetching, and hands the file over whole.
#[tokio::test]
async fn par3_held_recovery_volume_reads_as_fetching_repair_data() {
    let data = payload(8 * ARTICLE, 71);
    let mut damaged = data.clone();
    damaged[2 * ARTICLE..3 * ARTICLE].fill(0);
    let mut posted = vec![Post::plain("lantern.bin", damaged)];
    posted.extend(par3_set("lantern", &[("lantern.bin", &data)], 1));
    assert_eq!(posted.len(), 3, "the file, the index, one volume");
    assert!(posted[2].name.contains(".vol"), "{}", posted[2].name);
    let held = run_holding(&Scenario::new(posted), 2).await;
    assert_eq!(held.outcomes, vec![Some(RepairOutcome::Waiting)]);
    assert_eq!(held.report.status, JobStatus::Downloading);
    assert_eq!(held.report.post_state, crate::PostState::Idle);
    assert!(held.report.fetching_repair_data, "not fetching while held");
    assert!(
        held.after.iter().all(|(_, fetching)| !fetching),
        "still fetching once the volume came: {:?}",
        held.after
    );
    assert_eq!(held.status, Some(JobStatus::Complete));
    assert!(
        held.files[&delivered("lantern.bin")] == data,
        "not repaired"
    );
}
