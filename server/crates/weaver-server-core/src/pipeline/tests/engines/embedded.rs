// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery an archive carries inside itself, and recovery beside an
// enciphered archive: a PAR2 or PAR3 set appended to a
// stored 7z and repaired through it, a trailing PAR3 that loses one of its
// own articles, a RAR5 volume repaired from the recovery record it carries,
// and an AES 7z repaired through what was posted.

use std::io::Cursor;

use sevenz_turbo::encoder_options::AesEncoderOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Claim, Outcome, Post, Scenario, par2_set, par3_set, payload, run};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::protected_store_set;

const SECRET: &str = "quill-ember-secret";

fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 11, 41)),
        ("quill.bin", payload(3 * ARTICLE - 3, 42)),
    ]
}

// One stored container of `members()`, enciphered under `password` when
// there is one.
fn stored(password: Option<&str>) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let mut methods = vec![EncoderConfiguration::new(EncoderMethod::COPY)];
    if let Some(password) = password {
        methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
    }
    writer.set_content_methods(methods);
    for (name, bytes) in members() {
        let mut entry = ArchiveEntry::new_file(name);
        entry.size = bytes.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(bytes)))
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn par3_bytes(archive: &[u8], blocks: u64) -> Vec<Vec<u8>> {
    par3_set("lantern", &[("lantern.7z", archive)], blocks)
        .into_iter()
        .map(|post| post.bytes)
        .collect()
}

fn par2_bytes(archive: &[u8], blocks: u32) -> Vec<Vec<u8>> {
    par2_set("lantern", &[("lantern.7z", archive)], blocks)
        .into_iter()
        .map(|post| post.bytes)
        .collect()
}

// The archive with a recovery set over it appended: posted as one file, the
// set describing the archive under the name it is posted as.
fn trailing(archive: &[u8], set: Vec<Vec<u8>>) -> Post {
    let mut bytes = archive.to_vec();
    for file in set {
        bytes.extend_from_slice(&file);
    }
    Post::plain("lantern.7z", bytes)
}

// Where each recovery block's bytes lie in `bytes`: a PAR3 recovery data
// packet ends in its block, and its length sits 24 bytes into its 48-byte
// header, whose type is the last eight.
fn par3_blocks(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(found) = bytes[at..]
        .windows(8)
        .position(|window| window == b"PAR REC\x00")
    {
        let start = at + found - 40;
        let length = u64::from_le_bytes(bytes[start + 24..start + 32].try_into().unwrap());
        let end = start + length as usize;
        out.push((end - ARTICLE, end));
        at += found + 8;
    }
    out
}

// The article holding the middle of the last recovery block: losing it costs
// that block and no other.
fn last_block_article(post: &Post) -> u32 {
    let blocks = par3_blocks(&post.bytes);
    let (start, _) = *blocks.last().expect("the set carries recovery blocks");
    ((start + ARTICLE / 2) / ARTICLE) as u32
}

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The members of the 7z, every byte as written, and nothing else.
fn assert_members(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    let names: Vec<String> = members().iter().map(|(name, _)| delivered(name)).collect();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        names,
        "delivered names",
    );
    for (name, bytes) in members() {
        assert!(
            outcome.files.get(&delivered(name)) == Some(&bytes),
            "{name} bytes",
        );
    }
}

fn assert_repaired_once(graph: &Outcome, blocks: u64) {
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert_eq!(graph.graph().blocks_reachable, blocks);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The second article is member bytes: the first holds the signature header.
#[tokio::test]
async fn a_stored_7z_carrying_a_trailing_par2_is_repaired() {
    let archive = stored(None);
    let post = trailing(&archive, par2_bytes(&archive, 2));
    let graph = run(&Scenario::new(vec![post]).damaged(0, 1)).await;
    assert_members(&graph);
    assert_repaired_once(&graph, 2);
}

#[tokio::test]
async fn a_stored_7z_carrying_a_trailing_par3_is_repaired() {
    let archive = stored(None);
    let post = trailing(&archive, par3_bytes(&archive, 2));
    let graph = run(&Scenario::new(vec![post]).damaged(0, 1)).await;
    assert_members(&graph);
    assert_repaired_once(&graph, 2);
}

// Three recovery blocks, one lost with its article: two remain for the one
// damaged slice.
#[tokio::test]
async fn a_trailing_par3_losing_one_of_its_blocks_still_repairs() {
    let archive = stored(None);
    let post = trailing(&archive, par3_bytes(&archive, 3));
    let lost = last_block_article(&post);
    let scenario = Scenario::new(vec![post]).damaged(0, 1).lost(0, lost);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_repaired_once(&graph, 2);
}

// One recovery block, lost with its article: nothing is left to repair the
// damaged slice with. The member the damage never reached is delivered
// whole beside the container as it stands, so the job ends Partial, its
// failure the repair's.
#[tokio::test]
async fn a_trailing_par3_losing_its_only_block_ends_partial_for_repair() {
    let archive = stored(None);
    let post = trailing(&archive, par3_bytes(&archive, 1));
    let lost = last_block_article(&post);
    let scenario = Scenario::new(vec![post]).damaged(0, 1).lost(0, lost);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.7z"), delivered("quill.bin")]
    );
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert_eq!(graph.graph().blocks_reachable, 0);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 1,
            reachable: 0
        })]
    );
    // The container is left a damaged leaf of the job.
    assert_eq!(
        graph.graph().claims.get("lantern.7z"),
        Some(&Claim::Damaged)
    );
}

// The record is the volume's own recovery, and one article of the member
// is served damaged. The volume is read back through its record, the
// damaged bytes rebuilt in place and the member delivered whole, asking for
// nothing twice.
#[tokio::test]
async fn a_rar5_volume_with_a_damaged_article_is_repaired_from_its_recovery_record() {
    let body = payload(6 * ARTICLE + 5, 43);
    let posts = protected_store_set("beacon.bin", &body, 1, 1)
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let graph = run(&Scenario::new(posts).damaged(0, 2)).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(graph.files.get(&delivered("beacon.bin")), Some(&body));
    assert_eq!(
        graph.graph().rulings,
        vec![(
            "rar_record".to_string(),
            Some(RepairOutcome::Repaired { slices: 1 })
        )]
    );
    assert!(graph.reads > 0, "the repair read nothing back");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The second article lies inside the enciphered packed stream. The graph
// repair reads the posted cipher back through the store and writes the
// rebuilt bytes as repaired.
#[tokio::test]
async fn an_aes_7z_beside_par2_with_a_damaged_cipher_article_is_repaired() {
    let archive = stored(Some(SECRET));
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    let scenario = Scenario::new(posts).password(SECRET).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_repaired_once(&graph, 2);
    assert!(graph.reads > 0, "the repair read nothing back");
}
