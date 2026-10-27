// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;
mod map;
mod owner;

use std::io::Cursor;
use std::sync::Arc;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::{
    ByteRange, Emit, Located, MemberId, Object, Provenance, Pull, RecoveryKind, VolumeIndex, Why,
};

use super::SevenZipStore;
use crate::keys::Keys;
use crate::testing::{Quiet, payload};
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

// Four pieces under names that number none of them, as the store holds
// the first and the last: every piece but the last is as long as the
// first, so either middle piece fits either middle place.
fn unnamed_middles() -> (SevenZipStore, Vec<(&'static str, Vec<u8>)>) {
    let bytes = archive(copy(), false);
    let names = [
        "e93a07c1d25b4f68a0c3e1f7b9d24c85",
        "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64",
        "c40e8b2f6a1d4973b5e0f2c8d16a9b37",
        "7a2c5e9f0b3d4e81a6f9c2b07d4e1f58",
    ];
    let pieces: Vec<_> = names
        .into_iter()
        .zip(split(&bytes, 4))
        .map(|(name, (_, piece))| (name, piece))
        .collect();
    assert_eq!(pieces.len(), 4);
    assert!(pieces[3].1.len() < pieces[0].1.len());
    let mut object = store();
    for volume in [0, 3] {
        let (name, piece) = &pieces[volume];
        let (front, length) = (&piece[..32], Some(piece.len() as u64));
        if volume > 0 {
            assert_eq!(
                object.claim(name, front, length),
                Some(VolumeIndex(volume as u32))
            );
        }
        object.bound(VolumeIndex(volume as u32), name, front, length);
    }
    (object, pieces)
}

// No name or header says which middle piece comes first: the store claims
// neither while the set stands, refuses the set for them once the job is
// quiet, and the refused set takes them past every place it has, so they
// go where the set went.
#[test]
fn two_middle_pieces_nothing_orders_refuse_the_set_at_quiet() {
    let (mut object, pieces) = unnamed_middles();
    let middles: Vec<_> = pieces[1..3]
        .iter()
        .map(|(name, piece)| (*name, &piece[..32], piece.len() as u64))
        .collect();
    for (name, front, length) in &middles {
        assert_eq!(object.claim(name, front, Some(*length)), None);
    }
    let mut out = Vec::new();
    object.quiet(&Quiet(middles.clone()), &mut out);
    assert_eq!(out, vec![Emit::Refuse(Why::OrderUnknown)]);
    let (name, front, length) = middles[0];
    assert_eq!(
        object.claim(name, front, Some(length)),
        Some(VolumeIndex(4))
    );
}

// A recovery set that gives the pieces their real names orders them: once
// the first piece carries its own, each middle piece takes the place its
// number gives, and nothing is left beside the set to refuse it for.
#[test]
fn a_piece_a_recovery_set_names_takes_the_place_its_number_gives() {
    let (mut object, pieces) = unnamed_middles();
    object.renamed(VolumeIndex(0), "alpha.7z.001");
    for (volume, name) in [(2, "alpha.7z.003"), (1, "alpha.7z.002")] {
        let piece = &pieces[volume].1;
        let (front, length) = (&piece[..32], Some(piece.len() as u64));
        assert_eq!(
            object.claim(name, front, length),
            Some(VolumeIndex(volume as u32))
        );
        object.bound(VolumeIndex(volume as u32), name, front, length);
    }
    let mut out = Vec::new();
    object.quiet(&Quiet(Vec::new()), &mut out);
    assert!(!out.contains(&Emit::Refuse(Why::OrderUnknown)), "{out:?}");
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

fn asks(owner: &Owner) -> usize {
    owner
        .said
        .iter()
        .filter(|emit| **emit == Emit::NeedsPassword)
        .count()
}

// An offer answers the store's question: a candidate that opens nothing
// leaves it asking for the next one, folders and header alike.
#[test]
fn a_wrong_candidate_leaves_the_store_asking_for_the_next() {
    for encrypt_header in [false, true] {
        let mut owner = Owner::single(store(), archive(aes(), encrypt_header));
        owner.frames(0, 4096);
        assert_eq!(asks(&owner), 1, "header encrypted: {encrypt_header}");
        owner.offer("not-the-passphrase");
        assert_eq!(asks(&owner), 2, "header encrypted: {encrypt_header}");
        owner.offer("not-the-passphrase");
        assert_eq!(asks(&owner), 3, "the same wrong answer, asked again");
        owner.offer(PASSWORD);
        assert_eq!(asks(&owner), 3, "the right one is not asked past");
        owner.close(0);
        no_trouble(&owner);
        streamed(&owner);
    }
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
fn an_encrypted_header_no_candidate_opens_is_refused_at_quiet() {
    let bytes = archive(aes(), true);
    let mut owner = Owner::single(store(), bytes);
    owner.frames(0, 4096);
    owner.offer("not-the-passphrase");
    owner.close(0);
    // The question is still open: a candidate may yet answer it.
    assert!(!has(&owner, &Emit::Refuse(Why::EncryptedHeader)));
    owner.quiet();
    assert!(has(&owner, &Emit::Refuse(Why::EncryptedHeader)));
    assert!(owner.members.is_empty());
}

// The end header: the last bytes of a container with nothing after it.
fn end_header(bytes: &[u8]) -> ByteRange {
    let offset = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let length = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    ByteRange::at(32 + offset, length)
}

// A signature header alone, declaring an end header right after it.
fn signature_only(end_header_length: u64) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&0u64.to_le_bytes());
    tail.extend_from_slice(&end_header_length.to_le_bytes());
    tail.extend_from_slice(&0u32.to_le_bytes());
    let mut bytes = super::header::MAGIC.to_vec();
    bytes.extend_from_slice(&[0, 4]);
    bytes.extend_from_slice(&crate::grid::crc32(&tail).to_le_bytes());
    bytes.extend_from_slice(&tail);
    bytes
}

#[test]
fn an_end_header_declared_past_any_byte_holds_nothing_until_bytes_arrive() {
    let bytes = signature_only(1 << 62);
    let mut lone = store();
    let mut out = Vec::new();
    lone.deliver(
        weaver_graph::Delivery {
            volume: VolumeIndex(0),
            offset: 0,
            bytes: &bytes,
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut out,
    );
    assert_eq!(lone.kept.from(32), &[] as &[u8]);
    let mut owner = Owner::single(store(), bytes);
    owner.whole(4096);
    assert!(has(&owner, &Emit::NeedsEngine(Why::Unsupported)));
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
            handed: weaver_graph::Handed::default(),
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

// A stretch of cipher posted wrong, its articles said damaged or not
// noticed at all, then its repair: every member comes out whole and marked
// nowhere damaged, and the container reads back as posted, whether the
// password came before the bytes or after them. A repair that changes a
// block whose cipher was let go deciphers it from its plaintext enciphered
// again, and a block that changes re-chains the one after it. The second set
// is the pipeline row's shape: two small members, 64-byte articles that are
// also the PAR2 slices.
#[test]
fn a_repaired_cipher_stretch_gives_the_members_whole() {
    let small = vec![
        ("ember.bin", payload(41, 5 * 64 + 11)),
        ("quill.bin", payload(42, 3 * 64 - 3)),
    ];
    let small_entries: Vec<_> = small
        .iter()
        .map(|(name, bytes)| (None, Entry::File(name, bytes)))
        .collect();
    let (large, smaller) = (archive(aes(), false), write(aes(), false, &small_entries));
    // Each set's articles, and the stretches posted wrong in them.
    let mut cases = Vec::new();
    for article in [384u64, 700, 1000, 4096] {
        for ordinal in [1, 2] {
            let stretch = ByteRange::new(ordinal * article, (ordinal + 1) * article);
            cases.push((&large, files(), article, stretch));
        }
    }
    for stretch in [ByteRange::new(1032, 1732), ByteRange::new(10_022, 10_132)] {
        cases.push((&large, files(), 700, stretch));
    }
    for ordinal in [1, 2] {
        cases.push((
            &smaller,
            small.clone(),
            64,
            ByteRange::new(ordinal * 64, (ordinal + 1) * 64),
        ));
    }
    let mut failed = Vec::new();
    for (bytes, members, article, stretch) in cases {
        let length = bytes.len() as u64;
        let stretch = ByteRange::new(stretch.start, stretch.end.min(length));
        let mut wrong = bytes.clone();
        let ends = [
            stretch.start,
            stretch.end.saturating_sub(8).max(stretch.start),
        ];
        for at in ends
            .into_iter()
            .flat_map(|at| at..(at + 8).min(stretch.end))
        {
            wrong[at as usize] ^= 0x5a;
        }
        let articles: Vec<ByteRange> = (0..length)
            .step_by(article as usize)
            .map(|start| ByteRange::new(start, (start + article).min(length)))
            .collect();
        let hit: Vec<ByteRange> = articles
            .iter()
            .copied()
            .filter(|range| {
                range
                    .intersect(stretch)
                    .is_some_and(|part| !part.is_empty())
            })
            .collect();
        let repair = ByteRange::new(hit[0].start, hit[hit.len() - 1].end);
        for said in [Provenance::Damaged, Provenance::Fresh] {
            for early in [true, false] {
                let mut owner = Owner::single(store(), bytes.clone());
                if early {
                    owner.offer(PASSWORD);
                }
                for range in &articles {
                    let posted = &wrong[range.start as usize..range.end as usize];
                    match hit.contains(range) {
                        true => owner.deliver_as(0, *range, said, Some(posted)),
                        false => owner.deliver(0, *range),
                    }
                }
                if !early {
                    owner.offer(PASSWORD);
                }
                owner.deliver_as(0, repair, Provenance::Repaired, None);
                owner.close(0);
                let whole = owner.members.len() == members.len()
                    && members.iter().all(|(name, bytes)| {
                        let got = owner.member(name);
                        let range = ByteRange::new(0, bytes.len() as u64);
                        got.bytes == *bytes
                            && got.closed
                            && got
                                .coverage
                                .overlapping(range)
                                .all(|run| run.provenance != Provenance::Damaged)
                    });
                if !whole || owner.posted(0).as_deref() != Some(bytes.as_slice()) {
                    failed.push((members.len(), article, stretch, said, early, whole));
                }
            }
        }
    }
    assert!(failed.is_empty(), "{failed:?}");
}

// One file past the largest a password is proved against: the store has
// nothing to prove a candidate by, so the first one left opens on trial.
fn unprovable() -> (Vec<u8>, Vec<u8>) {
    let file = payload(51, (super::open::PROOF_CAP + 1) as usize);
    let bytes = write(aes(), false, &[(None, Entry::File("large.bin", &file))]);
    (bytes, file)
}

fn condemned(owner: &Owner) -> usize {
    owner
        .said
        .iter()
        .filter(|emit| matches!(emit, Emit::Again { .. }))
        .count()
}

// Verified bytes that fail their checksum under a candidate nothing proved
// condemn it: its folder's bytes come again and open under the next one.
#[test]
fn a_wrong_unproven_candidate_on_verified_bytes_gives_way_to_the_next() {
    let (bytes, file) = unprovable();
    let mut owner = Owner::single(store(), bytes.clone());
    owner.offer("not-the-passphrase");
    owner.offer(PASSWORD);
    owner.frames(0, 256 << 10);
    owner.close(0);
    no_trouble(&owner);
    assert_eq!(condemned(&owner), 1);
    let got = owner.member("large.bin");
    assert!(got.closed);
    assert_eq!(got.bytes, file);
    assert!(got.coverage.verified(ByteRange::new(0, file.len() as u64)));
    assert_eq!(owner.posted(0).as_deref(), Some(bytes.as_slice()));
}

// The same failure on bytes not verified is damage: nothing is condemned
// until a repair verifies them, and then the next candidate opens them.
#[test]
fn a_wrong_unproven_candidate_on_unverified_bytes_is_damage_until_repaired() {
    let (bytes, file) = unprovable();
    let length = bytes.len() as u64;
    let mut owner = Owner::single(store(), bytes.clone());
    owner.offer("not-the-passphrase");
    owner.offer(PASSWORD);
    let frame = 256 << 10;
    for start in (0..length).step_by(frame) {
        let range = ByteRange::new(start, (start + frame as u64).min(length));
        owner.deliver_as(0, range, Provenance::Damaged, None);
    }
    assert!(
        owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::Damaged { .. }))
    );
    assert_eq!(condemned(&owner), 0);
    owner.deliver_as(0, ByteRange::new(0, length), Provenance::Repaired, None);
    owner.close(0);
    assert_eq!(condemned(&owner), 1);
    let got = owner.member("large.bin");
    assert!(got.closed);
    assert_eq!(got.bytes, file);
    assert!(got.coverage.verified(ByteRange::new(0, file.len() as u64)));
}

// Every candidate condemned leaves the store asking, and quiet refuses it
// as one that needs a password.
#[test]
fn every_unproven_candidate_wrong_needs_a_password() {
    let (bytes, _) = unprovable();
    let mut owner = Owner::single(store(), bytes);
    owner.offer("not-the-passphrase");
    owner.offer("nor-this-one");
    owner.frames(0, 256 << 10);
    owner.close(0);
    assert_eq!(condemned(&owner), 2);
    assert!(has(&owner, &Emit::NeedsPassword));
    owner.quiet();
    assert!(has(&owner, &Emit::Refuse(Why::EncryptedHeader)));
}

fn member_id(owner: &Owner, name: &str) -> MemberId {
    owner
        .members
        .iter()
        .find(|(_, got)| got.name == name)
        .map(|(id, _)| MemberId(*id))
        .unwrap()
}

// A stored member's bytes are raw runs of the container, so a seek before the
// end header is read is not located yet, the same seek after it lies where
// the map puts it, and only a range the map rules out is never located.
#[test]
fn a_seek_before_the_end_header_is_read_is_located_once_it_is() {
    let bytes = archive(copy(), false);
    let mut owner = Owner::single(store(), bytes.clone());
    let mut extents = Vec::new();
    assert_eq!(
        owner
            .object
            .extent(MemberId(0), ByteRange::new(100, 200), &mut extents),
        Located::NotYet
    );
    assert!(extents.is_empty());
    owner.whole(4096);
    no_trouble(&owner);
    let alpha = member_id(&owner, "alpha.bin");
    assert_eq!(
        owner
            .object
            .extent(alpha, ByteRange::new(100, 200), &mut extents),
        Located::Whole
    );
    let read: Vec<u8> = extents
        .iter()
        .flat_map(|extent| bytes[extent.range.start as usize..extent.range.end as usize].to_vec())
        .collect();
    assert_eq!(read, payload(1, 10_000)[100..200]);
    extents.clear();
    assert_eq!(
        owner
            .object
            .extent(alpha, ByteRange::new(10_000, 10_100), &mut extents),
        Located::Never
    );
    assert!(extents.is_empty());
}

// A compressed folder sends the whole container to an engine, whose members
// are decoded and never lie in the volumes.
#[test]
fn a_seek_under_a_compressed_folder_is_unseekable() {
    let mut owner = Owner::single(store(), archive(lzma2(), false));
    owner.whole(4096);
    assert!(has(&owner, &Emit::NeedsEngine(Why::Compressed)));
    let engine = crate::sevenz_engine::SevenZipEngine::new();
    let mut extents = Vec::new();
    assert_eq!(
        engine.extent(MemberId(0), ByteRange::new(0, 100), &mut extents),
        Located::Never
    );
    assert!(extents.is_empty());
}

// A 7z decode holds to the memory it was granted, and to the decoder's own
// limits only when it was granted none.
#[test]
fn a_decode_holds_to_the_memory_it_was_granted() {
    let granted = super::limits(&weaver_graph::Budget {
        memory_bytes: Some(64 << 20),
        threads: 1,
        ..Default::default()
    });
    assert_eq!(granted.memory_limit_bytes, 64 << 20);
    let own = sevenz_turbo::ArchiveLimits::default();
    let none = super::limits(&weaver_graph::Budget::default());
    assert_eq!(none.memory_limit_bytes, own.memory_limit_bytes);
    assert_eq!(
        granted.max_header_unpacked_bytes,
        own.max_header_unpacked_bytes
    );
}
