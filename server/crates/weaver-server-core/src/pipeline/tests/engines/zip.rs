// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Zip containers: stored whole, spanned over `.z01`,
// `.z02`, `.zip`, deflated, and enciphered under the job's password.

use std::io::{Cursor, Write};

use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, CompressionMethod, ZipWriter};

use crate::jobs::model::JobStatus;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set, payload, run,
};

const SECRET: &str = "beacon-lantern-secret";

pub(super) fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 13, 51)),
        ("quill.bin", payload(3 * ARTICLE - 5, 52)),
    ]
}

pub(super) fn stored() -> SimpleFileOptions {
    SimpleFileOptions::default().compression_method(CompressionMethod::Stored)
}

pub(super) fn archive(options: SimpleFileOptions) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in members() {
        writer.start_file(name, options).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap())
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

// One archive as a spanned set of `parts` volumes, `.z01` onward and the
// `.zip` last. The `zip` crate writes no spanned sets, so the directory is
// rewritten here: the spanning marker leads the first volume, and every
// entry, and the end record, names its disk and the offset within it.
pub(super) fn spanned(single: &[u8], parts: usize) -> Vec<Post> {
    let part = (SPANNING_MARKER.len() + single.len()).div_ceil(parts);
    let cuts: Vec<usize> = (1..parts).map(|index| index * part).collect();
    spanned_at(single, &cuts)
}

const SPANNING_MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];

// The same, cut at the given offsets of the spanned bytes, the marker
// included, so a test can say where a disk starts.
pub(super) fn spanned_at(single: &[u8], cuts: &[usize]) -> Vec<Post> {
    const MARKER: [u8; 4] = SPANNING_MARKER;
    const CENTRAL: u32 = 0x0201_4b50;
    const END: u32 = 0x0605_4b50;
    const END_LEN: usize = 22;
    // No comment: the end record is the last 22 bytes.
    let tail = single.len() - END_LEN;
    assert_eq!(u32_at(single, tail), END);
    let entries = u16_at(single, tail + 10);
    let size = u32_at(single, tail + 12) as usize;
    let offset = u32_at(single, tail + 16) as usize;
    let mut directory = single[offset..offset + size].to_vec();
    let total = MARKER.len() + single.len();
    let parts = cuts.len() + 1;
    // The disk an offset is on, and where in it.
    let locate = |at: usize| {
        let disk = cuts.iter().filter(|cut| **cut <= at).count();
        let start = disk.checked_sub(1).map_or(0, |before| cuts[before]);
        (disk as u16, (at - start) as u32)
    };
    let mut at = 0;
    for _ in 0..entries {
        assert_eq!(u32_at(&directory, at), CENTRAL);
        let local = u32_at(&directory, at + 42) as usize;
        let (disk, within) = locate(local + MARKER.len());
        directory[at + 34..at + 36].copy_from_slice(&disk.to_le_bytes());
        directory[at + 42..at + 46].copy_from_slice(&within.to_le_bytes());
        at += 46
            + u16_at(&directory, at + 28) as usize
            + u16_at(&directory, at + 30) as usize
            + u16_at(&directory, at + 32) as usize;
    }
    let (directory_disk, directory_at) = locate(offset + MARKER.len());
    let last = (parts - 1) as u16;
    let mut joined = MARKER.to_vec();
    joined.extend_from_slice(&single[..offset]);
    joined.extend_from_slice(&directory);
    joined.extend(END.to_le_bytes());
    joined.extend(last.to_le_bytes());
    joined.extend(directory_disk.to_le_bytes());
    // Every entry's record is on the last disk only when the directory is.
    let here = if directory_disk == last { entries } else { 0 };
    joined.extend(here.to_le_bytes());
    joined.extend(entries.to_le_bytes());
    joined.extend((size as u32).to_le_bytes());
    joined.extend(directory_at.to_le_bytes());
    joined.extend(0u16.to_le_bytes());
    assert_eq!(joined.len(), total);
    let bounds: Vec<usize> = std::iter::once(0)
        .chain(cuts.iter().copied())
        .chain(std::iter::once(total))
        .collect();
    let volumes: Vec<Post> = bounds
        .windows(2)
        .map(|edge| &joined[edge[0]..edge[1]])
        .enumerate()
        .map(|(index, bytes)| {
            let name = if index + 1 == parts {
                "lantern.zip".to_owned()
            } else {
                format!("lantern.z{:02}", index + 1)
            };
            Post::plain(&name, bytes.to_vec())
        })
        .collect();
    assert_eq!(volumes.len(), parts);
    volumes
}

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub(super) fn assert_members(outcome: &Outcome) {
    assert!(outcome.completed(), "{:?}", outcome.failure);
    let expected = members();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>(),
        "delivered names",
    );
    for (name, bytes) in expected {
        assert!(outcome.files[&delivered(name)] == bytes, "{name} bytes",);
    }
}

#[tokio::test]
async fn a_stored_zip_delivers_its_members() {
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive(stored()))]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn a_stored_zip_spanned_over_z01_z02_zip_delivers_its_members() {
    let scenario = Scenario::new(spanned(&archive(stored()), 3));
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

#[tokio::test]
async fn a_deflate_zip_is_extracted() {
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive(deflated))]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_aes_zip_with_the_job_password_is_extracted() {
    let enciphered = stored().with_aes_encryption(AesMode::Aes256, SECRET);
    let scenario =
        Scenario::new(vec![Post::plain("lantern.zip", archive(enciphered))]).password(SECRET);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
}

pub(super) fn deflated() -> Vec<u8> {
    archive(SimpleFileOptions::default().compression_method(CompressionMethod::Deflated))
}

// The deflated zip cut into articles of every size from the whole file down
// to a byte (4 KiB is the whole file again): one outcome and one set of files,
// and nothing is refetched.
#[tokio::test]
async fn a_deflate_zip_delivers_alike_in_every_frame_size() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let scenario = Scenario::new(vec![Post::plain("lantern.zip", deflated())]).framed(size);
        let graph = run(&scenario).await;
        assert_members(&graph);
        assert!(kinds(&graph).contains(&"zip_engine"), "{:?}", kinds(&graph));
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// A wrong candidate and then the right one. The store hands the enciphered
// entries to the engine, whose run asks for a password, fails the wrong one
// and asks again; each candidate is offered once.
#[tokio::test]
async fn an_aes_zip_tries_a_wrong_candidate_once_then_the_right_one() {
    let enciphered = stored().with_aes_encryption(AesMode::Aes256, SECRET);
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive(enciphered))])
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["zip", "zip_engine"]);
    let offered: Vec<_> = graph.graph().offered.values().copied().collect();
    assert_eq!(offered, [2], "one try for each of the two candidates");
    assert_fetched_once(&graph);
}

// The second entry's local header.
fn second_local(bytes: &[u8]) -> usize {
    const LOCAL: [u8; 4] = [0x50, 0x4b, 0x03, 0x04];
    bytes
        .windows(4)
        .enumerate()
        .filter(|(_, window)| *window == LOCAL)
        .nth(1)
        .map(|(at, _)| at)
        .unwrap()
}

// quill.bin's deflated stream damaged with no PAR2, in 16-byte articles: the
// fifth article past its local header's, inside its data. The engine opens
// the archive from its records alone, closes ember.bin whole, then fails on
// quill.bin's lost bytes: ember.bin is delivered as written, quill.bin left
// out, the container as posted put in its place, and the job ends Partial.
#[tokio::test]
async fn a_deflate_zip_with_a_damaged_member_and_no_par2_delivers_the_members_it_closed() {
    let bytes = deflated();
    let damaged = (second_local(&bytes) / 16 + 4) as u32;
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", bytes.clone())])
        .framed(16)
        .damaged(0, damaged);
    let graph = run(&scenario).await;

    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "graph: {:?}",
        graph.failure
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    assert!(kinds(&graph).contains(&"zip_engine"), "{:?}", kinds(&graph));
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered("ember.bin"), delivered("lantern.zip")]
    );
    assert!(
        graph.files[&delivered("ember.bin")] == members()[0].1,
        "ember.bin closed before the failure, delivered whole"
    );
    assert_eq!(
        graph.files[&delivered("lantern.zip")].len(),
        bytes.len(),
        "the container as posted"
    );
    assert_eq!(
        graph.graph().claims.get("lantern.zip"),
        Some(&Claim::Damaged)
    );
}

// The spanned set posted out of order: the `.zip`, which ends the set and
// holds the directory, first. It carries no signature of its own, so it is
// a leaf until `.z01` binds the store, which is then offered it. Its name
// gives no number, so it binds as the symbolic last volume and `.z02` still
// takes its own; the store learns the `.zip`'s disk from the end record in
// its tail and maps.
async fn spanned_out_of_order(scenario: Scenario) -> Outcome {
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    graph
}

#[tokio::test]
async fn a_stored_zip_spanned_and_posted_last_volume_first_delivers_its_members() {
    let mut volumes = spanned(&archive(stored()), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.zip");
    spanned_out_of_order(Scenario::new(volumes)).await;
}

#[tokio::test]
async fn a_stored_zip_spanned_and_posted_backwards_delivers_its_members() {
    let mut volumes = spanned(&archive(stored()), 3);
    volumes.reverse();
    spanned_out_of_order(Scenario::new(volumes)).await;
}

// Deflated entries are the engine's: the store gives the set up with the
// `.zip` bound as the symbolic last volume, and the engine's run walks the
// bound volumes in their order, the `.zip` after `.z02`.
#[tokio::test]
async fn a_deflate_zip_spanned_and_posted_last_volume_first_delivers_its_members() {
    let mut volumes = spanned(&deflated(), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.zip");
    let graph = spanned_out_of_order(Scenario::new(volumes)).await;
    assert!(kinds(&graph).contains(&"zip_engine"), "{:?}", kinds(&graph));
}

#[tokio::test]
async fn an_aes_zip_spanned_and_posted_last_volume_first_delivers_its_members() {
    let enciphered = stored().with_aes_encryption(AesMode::Aes256, SECRET);
    let mut volumes = spanned(&archive(enciphered), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.zip");
    let graph = spanned_out_of_order(Scenario::new(volumes).password(SECRET)).await;
    assert!(kinds(&graph).contains(&"zip_engine"), "{:?}", kinds(&graph));
}

// The last disk starts exactly at an entry's local header, so the `.zip`
// opens like a whole archive and founds the store alone, as disk 0. `.z01`
// binding after it is the conflict: the store gives the set up to the
// engine, which walks `.z01`, `.z02` and the `.zip` in their order.
#[tokio::test]
async fn a_zip_whose_last_disk_starts_at_an_entry_posted_last_volume_first_delivers_through_the_engine()
 {
    let single = archive(stored());
    let boundary = SPANNING_MARKER.len() + second_local(&single);
    let mut volumes = spanned_at(&single, &[boundary / 2, boundary]);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.zip");
    assert_eq!(&volumes[0].bytes[..4], b"PK\x03\x04");
    let graph = spanned_out_of_order(Scenario::new(volumes)).await;
    assert!(kinds(&graph).contains(&"zip_engine"), "{:?}", kinds(&graph));
}

// One article of ember.bin's deflated stream damaged beside PAR2: graph
// repairs it in the stream and the engine reads it whole, with no refetch.
// The engine waits at the damaged article until the PAR2 set has ruled on
// the archive, which it does only once its index is in and it covers it.
#[tokio::test]
async fn a_deflate_zip_damaged_beside_par2_is_repaired_with_no_refetch() {
    let bytes = deflated();
    let mut posts = vec![Post::plain("lantern.zip", bytes.clone())];
    posts.extend(par2_set("lantern", &[("lantern.zip", &bytes)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(kinds(&graph).contains(&"par2"), "{:?}", kinds(&graph));
    assert!(graph.reads > 0, "the repair read its slices");
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// A zip from before names could be flagged UTF-8: its entry named in code
// page 437, where 0x82 is é. The name is written as ASCII and its bytes are
// then swapped in the local header and the directory, so no flag is set.
fn code_page_437(options: SimpleFileOptions) -> Vec<u8> {
    const PLACEHOLDER: &[u8] = b"cafX.bin";
    const NAME: &[u8] = b"caf\x82.bin";
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(std::str::from_utf8(PLACEHOLDER).unwrap(), options)
        .unwrap();
    writer.write_all(&members()[0].1).unwrap();
    let mut bytes = writer.finish().unwrap().into_inner();
    let places: Vec<usize> = bytes
        .windows(PLACEHOLDER.len())
        .enumerate()
        .filter(|(_, window)| *window == PLACEHOLDER)
        .map(|(at, _)| at)
        .collect();
    assert_eq!(
        places.len(),
        2,
        "the local header's name and the directory's"
    );
    for at in places {
        bytes[at..at + NAME.len()].copy_from_slice(NAME);
    }
    bytes
}

#[tokio::test]
async fn a_zip_naming_its_entry_in_code_page_437_delivers_it_decoded() {
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for options in [stored(), deflated] {
        let scenario = Scenario::new(vec![Post::plain("lantern.zip", code_page_437(options))]);
        let graph = run(&scenario).await;
        assert!(graph.completed(), "graph: {:?}", graph.failure);
        assert_eq!(
            graph.files.keys().cloned().collect::<Vec<_>>(),
            [delivered("café.bin")],
            "graph: delivered names"
        );
        assert!(graph.files[&delivered("café.bin")] == members()[0].1);
        assert_fetched_once(&graph);
    }
}

// A zip holding an empty directory beside a member.
fn with_empty_directory(options: SimpleFileOptions) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.add_directory("hollow/", stored()).unwrap();
    writer.start_file("ember.bin", options).unwrap();
    writer.write_all(&members()[0].1).unwrap();
    writer.finish().unwrap().into_inner()
}

// The empty directory is delivered as a directory, stored
// through the store and deflated through the engine.
#[tokio::test]
async fn a_zip_holding_an_empty_directory_delivers_it() {
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    for options in [stored(), deflated] {
        let scenario = Scenario::new(vec![Post::plain(
            "lantern.zip",
            with_empty_directory(options),
        )]);
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_eq!(
            graph.empty.iter().collect::<Vec<_>>(),
            [&delivered("hollow/")],
            "empty directories"
        );
        assert_fetched_once(&graph);
    }
}
