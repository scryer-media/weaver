// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;
mod owner;

use std::io::Cursor;
use std::sync::Arc;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::{ByteRange, Emit, Provenance, Pull, RecoveryKind, VolumeIndex, Why};

use super::SevenZipStore;
use crate::keys::Keys;
use crate::testing::payload;
use owner::Owner;

const PASSWORD: &str = "a-test-passphrase";

fn files() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("alpha.bin", payload(1, 10_000)),
        ("beta.bin", payload(2, 5_003)),
        ("gamma.bin", payload(3, 300)),
    ]
}

fn copy() -> Vec<EncoderConfiguration> {
    vec![EncoderConfiguration::new(EncoderMethod::COPY)]
}

// A cheap key derivation, so a test derives in microseconds.
fn aes() -> Vec<EncoderConfiguration> {
    vec![
        EncoderConfiguration::from(
            AesEncoderOptions::new(Password::new(PASSWORD)).with_num_cycles_power(4),
        ),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ]
}

fn lzma2() -> Vec<EncoderConfiguration> {
    let mut options = Lzma2Options::from_level(1);
    options.set_dictionary_size(64 << 10);
    vec![EncoderConfiguration::from(options)]
}

enum Entry<'a> {
    File(&'a str, &'a [u8]),
    Dir(&'a str),
}

fn write(
    methods: Vec<EncoderConfiguration>,
    encrypt_header: bool,
    entries: &[(Option<Vec<EncoderConfiguration>>, Entry<'_>)],
) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(methods);
    writer.set_encrypt_header(encrypt_header);
    for (methods, entry) in entries {
        if let Some(methods) = methods {
            writer.set_content_methods(methods.clone());
        }
        match entry {
            Entry::File(name, bytes) => writer
                .push_archive_entry(
                    ArchiveEntry::new_file(name),
                    Some(Cursor::new(bytes.to_vec())),
                )
                .unwrap(),
            Entry::Dir(name) => writer
                .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory(name), None)
                .unwrap(),
        };
    }
    writer.finish().unwrap().into_inner()
}

fn archive(methods: Vec<EncoderConfiguration>, encrypt_header: bool) -> Vec<u8> {
    let files = files();
    let entries: Vec<_> = files
        .iter()
        .map(|(name, bytes)| (None, Entry::File(name, bytes)))
        .collect();
    write(methods, encrypt_header, &entries)
}

fn store() -> SevenZipStore {
    SevenZipStore::with_keys(Arc::new(Keys::new(16)))
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

fn copy_in_frames(frame: usize) {
    let bytes = archive(copy(), false);
    let mut owner = Owner::single(store(), bytes.clone());
    owner.whole(frame);
    no_trouble(&owner);
    streamed(&owner);
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
}

#[test]
fn stored_files_stream_from_one_whole_delivery() {
    copy_in_frames(usize::MAX >> 1);
}

#[test]
fn stored_files_stream_in_four_kib_frames() {
    copy_in_frames(4096);
}

#[test]
fn stored_files_stream_a_byte_at_a_time() {
    copy_in_frames(1);
}

#[test]
fn bytes_ahead_of_the_tail_are_held_until_it_arrives() {
    let bytes = archive(copy(), false);
    let mut owner = Owner::single(store(), bytes);
    // Nothing moves: the tail arrives in the normal order, last.
    owner.pull = Pull::Moved(0);
    owner.frames(0, 4096);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
}

fn split(bytes: &[u8], parts: usize) -> Vec<(String, Vec<u8>)> {
    let part = bytes.len().div_ceil(parts);
    bytes
        .chunks(part)
        .enumerate()
        .map(|(index, chunk)| (format!("alpha.7z.{:03}", index + 1), chunk.to_vec()))
        .collect()
}

#[test]
fn a_split_set_streams_with_its_volumes_out_of_order() {
    let bytes = archive(copy(), false);
    let volumes = split(&bytes, 3);
    assert_eq!(volumes.len(), 3);
    let mut owner = Owner::new(store(), volumes);
    for volume in [2, 0, 1] {
        owner.frames(volume, 4096);
    }
    for volume in [2, 0, 1] {
        owner.close(volume);
    }
    no_trouble(&owner);
    streamed(&owner);
    let rebuilt: Vec<u8> = (0..3)
        .flat_map(|volume| owner.posted(volume).unwrap())
        .collect();
    assert_eq!(rebuilt, bytes);
}

#[test]
fn directories_and_empty_files_are_members_that_close_at_once() {
    let alpha = payload(4, 700);
    let bytes = write(
        copy(),
        false,
        &[
            (None, Entry::Dir("deep")),
            (None, Entry::File("deep/alpha.bin", &alpha)),
            (None, Entry::File("empty.bin", &[])),
            (None, Entry::Dir("deep/inner")),
        ],
    );
    let mut owner = Owner::single(store(), bytes);
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
}

#[test]
fn a_compressed_folder_goes_to_an_engine_before_any_member() {
    let bytes = archive(lzma2(), false);
    let mut owner = Owner::single(store(), bytes);
    owner.whole(4096);
    assert!(has(&owner, &Emit::NeedsEngine(Why::Compressed)));
    assert!(owner.members.is_empty());
}

#[test]
fn one_compressed_folder_sends_the_whole_container_to_an_engine() {
    let alpha = payload(5, 2_000);
    let beta = payload(6, 2_000);
    let bytes = write(
        copy(),
        false,
        &[
            (None, Entry::File("alpha.bin", &alpha)),
            (Some(lzma2()), Entry::File("beta.bin", &beta)),
        ],
    );
    let mut owner = Owner::single(store(), bytes);
    owner.whole(4096);
    assert!(has(&owner, &Emit::NeedsEngine(Why::Compressed)));
    assert!(owner.members.is_empty());
}

#[test]
fn a_tail_that_cannot_be_pulled_forward_goes_to_an_engine() {
    let bytes = archive(copy(), false);
    let mut owner = Owner::single(store(), bytes);
    owner.pull = Pull::Unseekable;
    owner.frames(0, 4096);
    assert!(has(&owner, &Emit::NeedsEngine(Why::TailUnreachable)));
    assert!(owner.members.is_empty());
}

// Every frame, last first.
fn reversed(owner: &mut Owner, frame: u64) {
    let length = owner.volumes[0].1.len() as u64;
    let mut starts: Vec<u64> = (0..length).step_by(frame as usize).collect();
    starts.reverse();
    for start in starts {
        owner.deliver(0, ByteRange::new(start, (start + frame).min(length)));
    }
}

#[test]
fn an_encrypted_folder_streams_out_of_order_once_its_password_proves() {
    let bytes = archive(aes(), false);
    let mut owner = Owner::single(store(), bytes.clone());
    reversed(&mut owner, 1000);
    assert!(has(&owner, &Emit::NeedsPassword));
    assert!(
        owner
            .members
            .values()
            .all(|got| got.bytes.is_empty() || got.length == Some(0))
    );
    owner.offer(PASSWORD);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
    // Re-enciphering the members gives back the bytes as posted.
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
}

#[test]
fn a_wrong_password_opens_nothing_and_the_right_one_after_it_does() {
    let bytes = archive(aes(), false);
    let mut owner = Owner::single(store(), bytes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    assert!(
        owner
            .members
            .values()
            .all(|got| got.coverage.bytes_covered() == 0)
    );
    owner.offer(PASSWORD);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
}

#[test]
fn an_encrypted_header_asks_and_opens_by_the_second_candidate() {
    let bytes = archive(aes(), true);
    let mut owner = Owner::single(store(), bytes.clone());
    owner.frames(0, 4096);
    assert!(has(&owner, &Emit::NeedsPassword));
    assert!(owner.members.is_empty());
    owner.offer("not-the-passphrase");
    assert!(owner.members.is_empty());
    owner.offer(PASSWORD);
    owner.close(0);
    no_trouble(&owner);
    streamed(&owner);
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
}

#[test]
fn an_encrypted_header_no_candidate_opens_is_refused_at_close() {
    let bytes = archive(aes(), true);
    let mut owner = Owner::single(store(), bytes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    assert!(!has(&owner, &Emit::Refuse(Why::EncryptedHeader)));
    owner.close(0);
    assert!(has(&owner, &Emit::Refuse(Why::EncryptedHeader)));
    assert!(owner.members.is_empty());
}

// The end header: the last bytes of a container with nothing after it.
fn end_header(bytes: &[u8]) -> ByteRange {
    let offset = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let length = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    ByteRange::at(32 + offset, length)
}

#[test]
fn a_damaged_end_header_is_said_and_a_repair_gives_the_map() {
    let bytes = archive(copy(), false);
    let end = end_header(&bytes);
    let mut bad = bytes.clone();
    bad[end.start as usize + 1] ^= 0x5a;
    let mut owner = Owner::single(store(), bytes.clone());
    // A seek brings nothing new: only the repair does.
    owner.pull = Pull::Moved(0);
    owner.deliver_as(
        0,
        ByteRange::new(0, bytes.len() as u64),
        Provenance::Damaged,
        Some(&bad),
    );
    assert!(has(
        &owner,
        &Emit::Damaged {
            volume: VolumeIndex(0),
            range: end
        }
    ));
    assert!(owner.members.is_empty());
    owner.deliver_as(0, end, Provenance::Repaired, None);
    owner.close(0);
    // The damaged delivery carried every member's bytes, held until the
    // repaired header placed them; their checksums pass.
    let files = files();
    assert_eq!(owner.members.len(), files.len());
    for (name, bytes) in &files {
        let got = owner.member(name);
        assert!(got.closed);
        assert_eq!(&got.bytes, bytes);
    }
}

#[test]
fn a_damaged_member_byte_is_said_at_its_volume_range() {
    let bytes = archive(copy(), false);
    let mut bad = bytes.clone();
    // The first file's bytes start right after the signature header.
    bad[32 + 100] ^= 0x01;
    let mut owner = Owner::single(store(), bytes.clone());
    owner.deliver_as(
        0,
        ByteRange::new(0, bytes.len() as u64),
        Provenance::Fresh,
        Some(&bad),
    );
    owner.close(0);
    assert!(owner.said.iter().any(|emit| matches!(
        emit,
        Emit::Damaged { volume: VolumeIndex(0), range } if range.contains(132)
    )));
}

#[test]
fn recovery_data_after_the_container_is_said() {
    let mut bytes = archive(copy(), false);
    let total = bytes.len() as u64;
    bytes.extend_from_slice(b"PAR3\0PKT");
    bytes.extend_from_slice(&payload(7, 200));
    let length = bytes.len() as u64;
    let mut owner = Owner::single(store(), bytes);
    owner.whole(4096);
    assert!(has(
        &owner,
        &Emit::RecoveryAt {
            volume: VolumeIndex(0),
            range: ByteRange::new(total, length),
            kind: RecoveryKind::Par3,
        }
    ));
    no_trouble(&owner);
    streamed(&owner);
}

#[test]
fn the_key_cache_derives_each_tuple_once_across_objects() {
    let keys = Arc::new(Keys::new(16));
    let bytes = archive(aes(), false);
    let mut first = Owner::single(SevenZipStore::with_keys(keys.clone()), bytes.clone());
    first.frames(0, 4096);
    first.offer("not-the-passphrase");
    first.offer(PASSWORD);
    first.close(0);
    streamed(&first);
    let derived = keys.derivations();
    // One wrong derivation and one right at least; one per tuple.
    assert!(derived >= 2);
    let mut second = Owner::single(SevenZipStore::with_keys(keys.clone()), bytes);
    second.frames(0, 4096);
    second.offer("not-the-passphrase");
    second.offer(PASSWORD);
    second.close(0);
    streamed(&second);
    assert_eq!(keys.derivations(), derived);
}
