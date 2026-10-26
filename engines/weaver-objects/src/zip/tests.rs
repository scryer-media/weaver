// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;
mod owner;

use std::io::{Cursor, Write};

use ::zip::write::SimpleFileOptions;
use ::zip::{CompressionMethod, ZipWriter};
use weaver_graph::{ByteRange, Emit, Kind, ObjectId, Provenance, Pull, VolumeIndex, Why};

use super::Zip;
use super::records::{self, LOCAL_LEN};
use crate::testing::{payload, zip64_end};
use owner::Owner;

fn files() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("alpha.bin", payload(1, 10_000)),
        ("beta.bin", payload(2, 5_003)),
        ("gamma.bin", payload(3, 300)),
    ]
}

fn stored() -> SimpleFileOptions {
    SimpleFileOptions::default().compression_method(CompressionMethod::Stored)
}

enum Entry<'a> {
    File(&'a str, &'a [u8], SimpleFileOptions),
    Dir(&'a str),
    Link(&'a str, &'a str),
}

fn write(entries: &[Entry<'_>]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for entry in entries {
        match entry {
            Entry::File(name, bytes, options) => {
                writer.start_file(*name, *options).unwrap();
                writer.write_all(bytes).unwrap();
            }
            Entry::Dir(name) => writer.add_directory(*name, stored()).unwrap(),
            Entry::Link(name, target) => writer.add_symlink(*name, *target, stored()).unwrap(),
        }
    }
    writer.finish().unwrap().into_inner()
}

fn archive() -> Vec<u8> {
    let files = files();
    let entries: Vec<_> = files
        .iter()
        .map(|(name, bytes)| Entry::File(name, bytes, stored()))
        .collect();
    write(&entries)
}

fn has(owner: &Owner, wanted: &Emit) -> bool {
    owner.said.contains(wanted)
}

fn no_trouble(owner: &Owner) {
    for emit in &owner.said {
        assert!(
            !matches!(
                emit,
                Emit::Damaged { .. } | Emit::NeedsEngine(_) | Emit::Refuse(_) | Emit::Retract(_)
            ),
            "unexpected {emit:?}"
        );
    }
}

// Every file whole, closed once, with nothing held back.
fn streamed(owner: &Owner) {
    let files = files();
    assert_eq!(owner.members.len(), files.len());
    for (name, bytes) in &files {
        let got = owner.member(name);
        assert_eq!(got.length, Some(bytes.len() as u64));
        assert!(got.closed, "{name} closed");
        assert!(got.coverage.verified(ByteRange::new(0, bytes.len() as u64)));
        assert_eq!(&got.bytes, bytes, "{name}");
    }
    assert_eq!(owner.held(), 0);
}

// A member as it ended: name, stated length, bytes, and whether it closed.
type Ended = (String, Option<u64>, Vec<u8>, bool);

// What one framing of a delivery made of the archive: every member as it
// ended, in id order, and everything else said.
fn transcript(frame: usize) -> (Vec<Ended>, Vec<Emit>) {
    let bytes = archive();
    let mut owner = Owner::single(Zip::new(), bytes.clone());
    owner.whole(frame);
    no_trouble(&owner);
    streamed(&owner);
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
    let members = owner
        .members
        .values()
        .map(|got| (got.name.clone(), got.length, got.bytes.clone(), got.closed))
        .collect();
    (members, owner.said)
}

#[test]
fn a_stored_archive_says_the_same_whole_in_four_kib_frames_and_a_byte_at_a_time() {
    let whole = transcript(usize::MAX >> 1);
    assert_eq!(transcript(4096), whole);
    assert_eq!(transcript(1), whole);
}

#[test]
fn bytes_ahead_of_the_tail_are_held_until_it_arrives() {
    let mut owner = Owner::single(Zip::new(), archive());
    // Nothing moves: the tail arrives in the normal order, last.
    owner.pull = Pull::Moved(0);
    owner.frames(0, 4096);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
}

#[test]
fn a_parent_that_cannot_seek_has_the_local_headers_walked_from_the_front() {
    let mut owner = Owner::single(Zip::new(), archive());
    owner.pull = Pull::Unseekable;
    owner.frames(0, 4096);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
}

// A single archive as a spanned set: a marker ahead of it, cut into
// `parts` volumes, its directory naming each entry's disk and the offset
// within that disk.
fn spanned(single: &[u8], parts: usize) -> Vec<(String, Vec<u8>)> {
    const MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];
    let end = records::find_end(single).unwrap();
    let mut directory = single[end.offset as usize..(end.offset + end.size) as usize].to_vec();
    let entries = records::directory(&directory, end.entries).unwrap();
    let start = end.offset + MARKER.len() as u64;
    let total = start + end.size + 22;
    let part = total.div_ceil(parts as u64);
    let locate = |at: u64| ((at / part) as u16, (at % part) as u32);
    for entry in &entries {
        let (disk, offset) = locate(entry.offset + MARKER.len() as u64);
        let (at, _) = entry.disk_at;
        directory[at..at + 2].copy_from_slice(&disk.to_le_bytes());
        let (at, _) = entry.offset_at;
        directory[at..at + 4].copy_from_slice(&offset.to_le_bytes());
    }
    let (disk, offset) = locate(start);
    let mut joined = MARKER.to_vec();
    joined.extend_from_slice(&single[..end.offset as usize]);
    joined.extend_from_slice(&directory);
    joined.extend(records::END.to_le_bytes());
    joined.extend((parts as u16 - 1).to_le_bytes());
    joined.extend(disk.to_le_bytes());
    joined.extend((end.entries as u16).to_le_bytes());
    joined.extend((end.entries as u16).to_le_bytes());
    joined.extend((end.size as u32).to_le_bytes());
    joined.extend(offset.to_le_bytes());
    joined.extend(0u16.to_le_bytes());
    let volumes: Vec<_> = joined
        .chunks(part as usize)
        .enumerate()
        .map(|(index, chunk)| {
            let name = if index + 1 == parts {
                "alpha.zip".to_owned()
            } else {
                format!("alpha.z{:02}", index + 1)
            };
            (name, chunk.to_vec())
        })
        .collect();
    assert_eq!(volumes.len(), parts);
    volumes
}

#[test]
fn a_spanned_set_streams_with_its_volumes_out_of_order() {
    let volumes = spanned(&archive(), 3);
    let mut owner = Owner::new(Zip::new(), volumes.clone());
    for volume in [2, 0, 1] {
        owner.frames(volume, 4096);
    }
    for volume in [2, 0, 1] {
        owner.close(volume);
    }
    // The `.zip` said it is disk 2: nothing numbered follows it.
    assert_eq!(
        weaver_graph::Object::claim(&mut owner.object, "alpha.z03", b"", None),
        None
    );
    no_trouble(&owner);
    streamed(&owner);
    for (volume, (_, bytes)) in volumes.iter().enumerate() {
        assert_eq!(
            owner.posted(volume).as_ref(),
            Some(bytes),
            "volume {volume}"
        );
    }
}

#[test]
fn the_last_volume_follows_every_numbered_one_and_takes_nothing_of_another_set() {
    let mut zip = Zip::new();
    zip.bound(VolumeIndex(0), "Alpha.Z01", &[], None);
    use weaver_graph::Object;
    assert_eq!(zip.claim("alpha.z02", b"", None), Some(VolumeIndex(1)));
    zip.bound(VolumeIndex(1), "alpha.z02", &[], None);
    assert_eq!(
        zip.claim("alpha.zip", b"", None),
        Some(VolumeIndex(u32::MAX))
    );
    zip.bound(VolumeIndex(u32::MAX), "alpha.zip", &[], None);
    assert_eq!(
        zip.claim("alpha.z03", b"", None),
        Some(VolumeIndex(2)),
        "a numbered volume takes its number until the last says its disk"
    );
    assert_eq!(zip.claim("beta.z03", b"", None), None);
    assert_eq!(
        zip.claim("alpha.z02", b"", None),
        Some(VolumeIndex(1)),
        "a volume posted again is that volume"
    );

    let mut lone = Zip::new();
    lone.bound(VolumeIndex(0), "alpha.zip", &[], None);
    assert_eq!(
        lone.claim("alpha.z01", b"", None),
        None,
        "a lone zip has no siblings"
    );
}

#[test]
fn directories_and_empty_entries_are_members_that_close_at_once_and_links_are_skipped() {
    let alpha = payload(4, 700);
    let bytes = write(&[
        Entry::Dir("deep/"),
        Entry::File("deep/alpha.bin", &alpha, stored()),
        Entry::File("empty.bin", &[], stored()),
        Entry::Link("deep/link", "alpha.bin"),
        Entry::Dir("deep/inner/"),
    ]);
    let mut owner = Owner::single(Zip::new(), bytes.clone());
    owner.whole(4096);
    no_trouble(&owner);
    assert_eq!(owner.members.len(), 4);
    for name in ["deep/", "deep/inner/", "empty.bin"] {
        let got = owner.member(name);
        assert_eq!(got.length, Some(0), "{name}");
        assert!(got.closed, "{name}");
    }
    let got = owner.member("deep/alpha.bin");
    assert!(got.closed);
    assert_eq!(got.bytes, alpha);
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
}

#[test]
fn a_deflated_entry_sends_the_whole_container_to_an_engine_before_any_member() {
    let alpha = payload(5, 2_000);
    let beta = payload(6, 2_000).repeat(4);
    let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
    let bytes = write(&[
        Entry::File("alpha.bin", &alpha, stored()),
        Entry::File("beta.bin", &beta, deflated),
    ]);
    let mut owner = Owner::single(Zip::new(), bytes);
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::Compressed)]);
    assert!(owner.members.is_empty());
}

#[test]
fn an_encrypted_entry_goes_to_an_engine_that_has_the_password() {
    let alpha = payload(7, 2_000);
    let options = stored().with_aes_encryption(::zip::AesMode::Aes256, "a-test-passphrase");
    let bytes = write(&[Entry::File("alpha.bin", &alpha, options)]);
    let mut owner = Owner::single(Zip::new(), bytes);
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::Encrypted)]);
    assert!(owner.members.is_empty());
}

#[test]
fn an_entry_sized_after_its_data_cannot_be_walked() {
    let mut writer = ZipWriter::new_stream(Vec::new());
    for (name, bytes) in files() {
        writer.start_file(name, stored()).unwrap();
        writer.write_all(&bytes).unwrap();
    }
    let bytes = writer.finish().unwrap().into_inner();
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    assert_ne!(
        flags & 1 << 3,
        0,
        "the fixture sizes its entries after their data"
    );

    let mut owner = Owner::single(Zip::new(), bytes.clone());
    owner.pull = Pull::Unseekable;
    owner.frames(0, 4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::UnboundedEntry)]);
    assert!(owner.members.is_empty());

    // The directory says what the local headers cannot.
    let mut owner = Owner::single(Zip::new(), bytes);
    owner.whole(4096);
    no_trouble(&owner);
    streamed(&owner);
}

#[test]
fn an_archive_ended_by_zip64_records_streams_and_one_placing_them_past_its_bytes_goes_to_an_engine()
{
    let mut bytes = archive();
    zip64_end(&mut bytes, 1, |_| {});
    let mut owner = Owner::single(Zip::new(), bytes);
    owner.whole(4096);
    no_trouble(&owner);
    streamed(&owner);

    let mut bytes = archive();
    zip64_end(&mut bytes, 1, |said| said.record = u64::MAX - 8);
    let mut owner = Owner::single(Zip::new(), bytes);
    owner.whole(4096);
    assert_eq!(owner.said, vec![Emit::NeedsEngine(Why::Unsupported)]);
    assert!(owner.members.is_empty());
}

#[test]
fn a_damaged_directory_is_said_and_a_repair_brings_the_members() {
    let real = archive();
    let end = records::find_end(&real).unwrap();
    let directory = ByteRange::at(end.offset, end.size);
    let mut damaged = real.clone();
    // The first entry's signature.
    damaged[directory.start as usize + 1] ^= 0xff;
    let mut owner = Owner::single(Zip::new(), damaged);
    owner.whole(4096);
    assert!(owner.members.is_empty());
    assert!(has(
        &owner,
        &Emit::Damaged {
            volume: VolumeIndex(0),
            range: directory
        }
    ));

    let repair = &real[directory.start as usize..directory.end as usize];
    owner.deliver_as(0, directory, Provenance::Repaired, Some(repair));
    streamed(&owner);
}

#[test]
fn a_local_header_longer_than_its_directory_entry_still_places_the_data() {
    let real = archive();
    let grown = grow_first_local_header(&real, 8);
    let mut owner = Owner::single(Zip::new(), grown.clone());
    owner.whole(4096);
    no_trouble(&owner);
    streamed(&owner);
    assert_eq!(owner.posted(0).as_deref(), Some(grown.as_slice()));
}

// The first entry's local header with `by` more bytes of empty extra
// fields, and every later offset moved along.
fn grow_first_local_header(single: &[u8], by: usize) -> Vec<u8> {
    let end = records::find_end(single).unwrap();
    let start = end.offset as usize;
    let mut directory = single[start..start + end.size as usize].to_vec();
    let entries = records::directory(&directory, end.entries).unwrap();
    for entry in entries.iter().filter(|entry| entry.offset > 0) {
        let (at, _) = entry.offset_at;
        let moved = (entry.offset as u32 + by as u32).to_le_bytes();
        directory[at..at + 4].copy_from_slice(&moved);
    }
    let name = u16::from_le_bytes([single[26], single[27]]) as usize;
    let extra = u16::from_le_bytes([single[28], single[29]]) as usize;
    let mut out = single[..LOCAL_LEN as usize + name + extra].to_vec();
    out[28..30].copy_from_slice(&((extra + by) as u16).to_le_bytes());
    out.extend(std::iter::repeat_n(0u8, by));
    out.extend_from_slice(&single[LOCAL_LEN as usize + name + extra..start]);
    out.extend_from_slice(&directory);
    let mut record = single[start + end.size as usize..].to_vec();
    record[16..20].copy_from_slice(&((start + by) as u32).to_le_bytes());
    out.extend_from_slice(&record);
    out
}

#[test]
fn a_member_whose_checksum_fails_is_damaged_at_its_extent() {
    let real = archive();
    let alpha = &files()[0].1;
    let data = real
        .windows(64)
        .position(|window| window == &alpha[..64])
        .unwrap() as u64;
    let mut damaged = real.clone();
    damaged[data as usize + 100] ^= 0x01;
    let mut owner = Owner::single(Zip::new(), damaged);
    owner.whole(4096);
    assert!(owner.member("alpha.bin").closed);
    assert_eq!(
        owner.said,
        vec![Emit::Damaged {
            volume: VolumeIndex(0),
            range: ByteRange::at(data, alpha.len() as u64)
        }]
    );
}

#[test]
fn the_store_is_built_for_its_kind() {
    let object = crate::build(Kind::Zip, ObjectId(1)).unwrap();
    assert_eq!(object.kind(), Kind::Zip);
    assert!(!object.reads_in_order());
}

// A stored local header for an empty entry named by `name`'s raw bytes, with
// `extra` as its extra field.
fn local_named(name: &[u8], flags: u16, extra: &[u8]) -> Vec<u8> {
    let mut header = records::LOCAL.to_le_bytes().to_vec();
    header.extend(20u16.to_le_bytes());
    header.extend(flags.to_le_bytes());
    header.extend([0u8; 18]);
    header.extend((name.len() as u16).to_le_bytes());
    header.extend((extra.len() as u16).to_le_bytes());
    header.extend(name);
    header.extend(extra);
    header
}

// A Unicode path field naming `path` for a header whose name bytes have
// `checksum`.
fn unicode_path(checksum: u32, path: &str) -> Vec<u8> {
    let mut field = 0x7075u16.to_le_bytes().to_vec();
    field.extend((5 + path.len() as u16).to_le_bytes());
    field.push(1);
    field.extend(checksum.to_le_bytes());
    field.extend(path.as_bytes());
    field
}

#[test]
fn a_name_that_is_not_utf8_reads_as_code_page_437_whatever_its_flag() {
    let name = |bytes: &[u8], flags, extra: &[u8]| {
        records::local(&local_named(bytes, flags, extra))
            .unwrap()
            .name
    };
    assert_eq!(name(b"caf\x82.txt", 0, &[]), "café.txt");
    assert_eq!(name(b"\x80\x9b\xe1\xff", 0, &[]), "Ç¢ß\u{a0}");
    assert_eq!(
        name("café.txt".as_bytes(), 0, &[]),
        "café.txt",
        "valid UTF-8 without the flag stays UTF-8"
    );
    assert_eq!(
        name(b"caf\x82.txt", 1 << 11, &[]),
        "café.txt",
        "the UTF-8 flag does not change how a name reads"
    );
    let checksum = crate::grid::crc32(b"cafe.txt");
    assert_eq!(
        name(b"cafe.txt", 0, &unicode_path(checksum, "café.txt")),
        "café.txt",
        "a Unicode path whose checksum matches the name stands in for it"
    );
    assert_eq!(
        name(b"cafe.txt", 0, &unicode_path(!checksum, "café.txt")),
        "cafe.txt",
        "a Unicode path whose checksum does not match is ignored"
    );
}
