// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Zip containers under both engines: stored whole, spanned over `.z01`,
// `.z02`, `.zip`, deflated, and enciphered under the job's password.

use std::io::{Cursor, Write};

use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, CompressionMethod, ZipWriter};

use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;

use super::harness::{ARTICLE, Outcome, Post, Scenario, assert_same, payload, run_both};

const SECRET: &str = "beacon-lantern-secret";

fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.bin", payload(5 * ARTICLE + 13, 51)),
        ("quill.bin", payload(3 * ARTICLE - 5, 52)),
    ]
}

fn stored() -> SimpleFileOptions {
    SimpleFileOptions::default().compression_method(CompressionMethod::Stored)
}

fn archive(options: SimpleFileOptions) -> Vec<u8> {
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
fn spanned(single: &[u8], parts: usize) -> Vec<Post> {
    const MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];
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
    let part = total.div_ceil(parts);
    let locate = |at: usize| ((at / part) as u16, (at % part) as u32);
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
    let volumes: Vec<Post> = joined
        .chunks(part)
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

fn assert_members(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{}: {:?}",
        outcome.engine,
        outcome.failure
    );
    let expected = members();
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>(),
        "{}: delivered names",
        outcome.engine
    );
    for (name, bytes) in expected {
        assert!(
            outcome.files[&delivered(name)] == bytes,
            "{}: {name} bytes",
            outcome.engine
        );
    }
}

#[tokio::test]
async fn a_stored_zip_delivers_its_members_under_both_engines() {
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive(stored()))]);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_same(&classic, &graph);
}

// Classic opens the first file of the set as the whole zip and reads no
// local header at the spanned offsets; graph walks the set.
#[tokio::test]
async fn a_stored_zip_spanned_over_z01_z02_zip_delivers_its_members_under_graph_and_fails_under_classic()
 {
    let scenario = Scenario::new(spanned(&archive(stored()), 3));
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "classic: {:?}",
        classic.status
    );
    assert_eq!(
        classic.failure_kind,
        Some(HistoryFailureKind::Unpack),
        "classic: failure kind"
    );
    assert!(
        classic.files.is_empty(),
        "classic: {:?}",
        classic.files.keys()
    );
}

#[tokio::test]
async fn a_deflate_zip_is_extracted_under_both_engines() {
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let scenario = Scenario::new(vec![Post::plain("lantern.zip", archive(deflated))]);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn an_aes_zip_with_the_job_password_is_extracted_under_both_engines() {
    let enciphered = stored().with_aes_encryption(AesMode::Aes256, SECRET);
    let scenario =
        Scenario::new(vec![Post::plain("lantern.zip", archive(enciphered))]).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_members(&classic);
    assert_same(&classic, &graph);
}
