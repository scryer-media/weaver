// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// ZIP64 archives: every size and offset in the 32-bit fields is the ZIP64
// sentinel and the real values sit in the ZIP64 extra fields, a ZIP64 end
// record and its locator close the archive, and the members are stored or
// deflated, with no data descriptor or with one, signed or not. One 64 KiB
// article per post article, as the archive is posted; served tail first, and
// beside a PAR2 set with its payload changed, its directory changed, an
// interior article lost, and its last article lost.

use std::io::Write;

use super::harness::{Outcome, Post, Scenario, par2_volumes_sliced, run};

const ARTICLE_BYTES: usize = 64 * 1024;
const ARCHIVE: &str = "payload.zip";

type Members = Vec<(&'static str, Vec<u8>)>;

// A forced ZIP64 archive of three members: a small one, a mebibyte of
// noise, and the same noise again, whose local header lies in the middle of
// the archive, away from the directory at its tail.
fn zip64(descriptor: Option<bool>, deflated: bool) -> (Vec<u8>, Members) {
    let mut random = 0x1234_5678u32;
    let mut members: Members = vec![
        ("first.bin", vec![0x31; 8192]),
        (
            "second.bin",
            (0..1024 * 1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    random as u8
                })
                .collect(),
        ),
    ];
    members.push(("third.bin", members[1].1.clone()));
    let mut archive = Vec::new();
    let mut directory = Vec::new();
    let flags: u16 = if descriptor.is_some() { 8 } else { 0 };
    let method = if deflated { 8u16 } else { 0 };
    for (name, bytes) in &members {
        let packed = if deflated {
            let mut encoder =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(bytes).unwrap();
            encoder.finish().unwrap()
        } else {
            bytes.clone()
        };
        let packed_len = packed.len() as u64;
        let offset = archive.len() as u64;
        let len = bytes.len() as u64;
        let crc = par2_rs::checksum::crc32(bytes);
        archive.extend(0x0403_4b50u32.to_le_bytes());
        for value in [45u16, flags, method, 0, 0] {
            archive.extend(value.to_le_bytes());
        }
        for value in [
            if descriptor.is_some() { 0 } else { crc },
            u32::MAX,
            u32::MAX,
        ] {
            archive.extend(value.to_le_bytes());
        }
        archive.extend((name.len() as u16).to_le_bytes());
        archive.extend(20u16.to_le_bytes());
        archive.extend(name.as_bytes());
        archive.extend(1u16.to_le_bytes());
        archive.extend(16u16.to_le_bytes());
        for size in [len, packed_len] {
            archive.extend(if descriptor.is_some() { 0u64 } else { size }.to_le_bytes());
        }
        archive.extend(packed);
        if let Some(signed) = descriptor {
            if signed {
                archive.extend(0x0807_4b50u32.to_le_bytes());
            }
            archive.extend(crc.to_le_bytes());
            archive.extend(packed_len.to_le_bytes());
            archive.extend(len.to_le_bytes());
        }
        directory.extend(0x0201_4b50u32.to_le_bytes());
        for value in [45u16, 45, flags, method, 0, 0] {
            directory.extend(value.to_le_bytes());
        }
        for value in [crc, u32::MAX, u32::MAX] {
            directory.extend(value.to_le_bytes());
        }
        for value in [name.len() as u16, 28, 0, 0, 0] {
            directory.extend(value.to_le_bytes());
        }
        directory.extend(0u32.to_le_bytes());
        directory.extend(u32::MAX.to_le_bytes());
        directory.extend(name.as_bytes());
        directory.extend(1u16.to_le_bytes());
        directory.extend(24u16.to_le_bytes());
        for value in [len, packed_len, offset] {
            directory.extend(value.to_le_bytes());
        }
    }
    let directory_offset = archive.len() as u64;
    let directory_len = directory.len() as u64;
    archive.extend(directory);
    let end_offset = archive.len() as u64;
    archive.extend(0x0606_4b50u32.to_le_bytes());
    archive.extend(44u64.to_le_bytes());
    archive.extend(45u16.to_le_bytes());
    archive.extend(45u16.to_le_bytes());
    archive.extend([0u8; 8]);
    for value in [
        members.len() as u64,
        members.len() as u64,
        directory_len,
        directory_offset,
    ] {
        archive.extend(value.to_le_bytes());
    }
    archive.extend(0x0706_4b50u32.to_le_bytes());
    archive.extend(0u32.to_le_bytes());
    archive.extend(end_offset.to_le_bytes());
    archive.extend(1u32.to_le_bytes());
    archive.extend(0x0605_4b50u32.to_le_bytes());
    archive.extend([0u8; 4]);
    archive.extend([0xffu8; 12]);
    archive.extend(0u16.to_le_bytes());
    (archive, members)
}

fn assert_members(outcome: &Outcome, members: &Members) {
    assert!(
        outcome.completed(),
        "{:?} {:?} {:?}",
        outcome.status,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>()
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(name, _)| format!("lantern beacon/{name}"))
        .collect();
    expected.sort();
    let delivered: Vec<String> = outcome
        .files
        .keys()
        .filter(|name| !name.ends_with(".par2"))
        .cloned()
        .collect();
    assert_eq!(delivered, expected, "delivered names");
    for (name, bytes) in members {
        assert!(
            outcome.files[&format!("lantern beacon/{name}")] == *bytes,
            "{name} bytes"
        );
    }
}

// The archive alone, every batch served last article first.
async fn served_tail_first(descriptor: Option<bool>, deflated: bool) {
    let (archive, members) = zip64(descriptor, deflated);
    let scenario = Scenario::new(vec![Post::plain(ARCHIVE, archive)])
        .framed(ARTICLE_BYTES)
        .backwards();
    assert_members(&run(&scenario).await, &members);
}

#[tokio::test]
async fn a_stored_zip64_served_tail_first_delivers_its_members() {
    served_tail_first(None, false).await;
}

#[tokio::test]
async fn a_deflated_zip64_served_tail_first_delivers_its_members() {
    served_tail_first(None, true).await;
}

#[tokio::test]
async fn a_stored_zip64_with_unsigned_descriptors_served_tail_first_delivers_its_members() {
    served_tail_first(Some(false), false).await;
}

#[tokio::test]
async fn a_deflated_zip64_with_unsigned_descriptors_served_tail_first_delivers_its_members() {
    served_tail_first(Some(false), true).await;
}

#[tokio::test]
async fn a_stored_zip64_with_signed_descriptors_served_tail_first_delivers_its_members() {
    served_tail_first(Some(true), false).await;
}

#[tokio::test]
async fn a_deflated_zip64_with_signed_descriptors_served_tail_first_delivers_its_members() {
    served_tail_first(Some(true), true).await;
}

// The signed-descriptor archive beside a PAR2 set of two 64 KiB blocks made
// from it as it should be, posted as `posted` makes it from the original.
fn beside_par2(posted: impl FnOnce(&mut Vec<u8>)) -> (Scenario, Members, u32) {
    let (original, members) = zip64(Some(true), false);
    let mut bytes = original.clone();
    posted(&mut bytes);
    let mut posts = vec![Post::plain(ARCHIVE, bytes)];
    posts.extend(par2_volumes_sliced(
        "payload",
        &[(ARCHIVE, &original)],
        ARTICLE_BYTES as u64,
        2,
        1,
    ));
    let articles = original.len().div_ceil(ARTICLE_BYTES) as u32;
    (
        Scenario::new(posts).framed(ARTICLE_BYTES),
        members,
        articles,
    )
}

// A member byte changed before posting, every article clean.
#[tokio::test]
async fn a_zip64_whose_payload_changed_before_posting_is_repaired_by_par2() {
    let (scenario, members, _) = beside_par2(|bytes| bytes[4096] ^= 0x7f);
    assert_members(&run(&scenario).await, &members);
}

// A directory byte changed before posting, every article clean.
#[tokio::test]
async fn a_zip64_whose_directory_changed_before_posting_is_repaired_by_par2() {
    let (scenario, members, _) = beside_par2(|bytes| {
        let at = bytes.len() - 55;
        bytes[at] ^= 0x7f;
    });
    assert_members(&run(&scenario).await, &members);
}

#[tokio::test]
async fn a_zip64_missing_an_interior_article_is_repaired_by_par2() {
    let (scenario, members, _) = beside_par2(|_| {});
    assert_members(&run(&scenario.lost(0, 4)).await, &members);
}

#[tokio::test]
async fn a_zip64_missing_its_last_article_is_repaired_by_par2() {
    let (scenario, members, articles) = beside_par2(|_| {});
    assert_members(&run(&scenario.lost(0, articles - 1)).await, &members);
}
