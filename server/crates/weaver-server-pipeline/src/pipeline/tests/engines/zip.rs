// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{ARTICLE, Outcome, Post, payload};
use ::zip::write::SimpleFileOptions;
use ::zip::{CompressionMethod, ZipWriter};
use std::io::{Cursor, Write};

pub fn members() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("ember.mkv", payload(5 * ARTICLE + 13, 51)),
        ("quill.mkv", payload(3 * ARTICLE - 5, 52)),
    ]
}

pub fn stored() -> SimpleFileOptions {
    SimpleFileOptions::default().compression_method(CompressionMethod::Stored)
}

pub fn archive(options: SimpleFileOptions) -> Vec<u8> {
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

pub fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

// One archive as a spanned set of `parts` volumes, `.z01` onward and the
// `.zip` last. The `zip` crate writes no spanned sets, so the directory is
// rewritten here: the spanning marker leads the first volume, and every
// entry, and the end record, names its disk and the offset within it.
pub fn spanned(single: &[u8], parts: usize) -> Vec<Post> {
    let part = (SPANNING_MARKER.len() + single.len()).div_ceil(parts);
    let cuts: Vec<usize> = (1..parts).map(|index| index * part).collect();
    spanned_at(single, &cuts)
}

pub const SPANNING_MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];

// The same, cut at the given offsets of the spanned bytes, the marker
// included, so a test can say where a disk starts.
pub fn spanned_at(single: &[u8], cuts: &[usize]) -> Vec<Post> {
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

pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub fn assert_members(outcome: &Outcome) {
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

pub fn deflated() -> Vec<u8> {
    archive(SimpleFileOptions::default().compression_method(CompressionMethod::Deflated))
}
