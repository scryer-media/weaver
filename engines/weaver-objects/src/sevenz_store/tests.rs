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

// Pieces under names that number none of them, the given ones bound:
// every piece but the last is as long as the first, so a middle piece
// fits every middle place.
fn unnamed(parts: usize, bound: &[usize]) -> (SevenZipStore, Vec<(&'static str, Vec<u8>)>) {
    let bytes = archive(copy(), false);
    let names = [
        "e93a07c1d25b4f68a0c3e1f7b9d24c85",
        "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64",
        "c40e8b2f6a1d4973b5e0f2c8d16a9b37",
        "7a2c5e9f0b3d4e81a6f9c2b07d4e1f58",
        "f2b8d04e6c1a4397a5e3c9d17b0f6e2a",
    ];
    let pieces: Vec<_> = names
        .into_iter()
        .zip(split(&bytes, parts))
        .map(|(name, (_, piece))| (name, piece))
        .collect();
    assert_eq!(pieces.len(), parts);
    assert!(pieces[parts - 1].1.len() < pieces[0].1.len());
    let mut object = store();
    for &volume in bound {
        let (name, piece) = &pieces[volume];
        let (front, length) = (&piece[..32], Some(piece.len() as u64));
        if volume > 0 {
            assert!(
                object
                    .claim(name, front, length)
                    .contains(&VolumeIndex(volume as u32))
            );
        }
        object.bound(VolumeIndex(volume as u32), name, front, length);
    }
    (object, pieces)
}

// Four pieces, the store holding the first and the last.
fn unnamed_middles() -> (SevenZipStore, Vec<(&'static str, Vec<u8>)>) {
    unnamed(4, &[0, 3])
}

// A piece's front and length as the tree hands them, and as quiet sees it
// beside the set.
fn beside<'a>(piece: &'a (&'static str, Vec<u8>)) -> (&'static str, &'a [u8], u64) {
    (piece.0, &piece.1[..32], piece.1.len() as u64)
}

// No name or header says which middle piece comes first: the store offers
// each both middle places, the tree binds neither while two are free, the
// set refuses for them once the job is quiet, and the refused set takes
// them past every place it has, so they go where the set went.
#[test]
fn two_middle_pieces_nothing_orders_refuse_the_set_at_quiet() {
    let (mut object, pieces) = unnamed_middles();
    let middles: Vec<_> = pieces[1..3].iter().map(beside).collect();
    for (name, front, length) in &middles {
        assert_eq!(
            object.claim(name, front, Some(*length)),
            vec![VolumeIndex(1), VolumeIndex(2)]
        );
    }
    let mut out = Vec::new();
    object.quiet(&Quiet(middles.clone()), &mut out);
    assert_eq!(out, vec![Emit::Refuse(Why::OrderUnknown)]);
    let (name, front, length) = middles[0];
    assert_eq!(
        object.claim(name, front, Some(length)),
        vec![VolumeIndex(4)]
    );
}

// An unsigned copy of a held continuation volume is offered the held place,
// before a restore and after: whether it is the piece holding it is the
// tree's to judge, by their fronts, not the store's.
#[test]
fn an_unsigned_copy_of_a_held_continuation_volume_is_offered_the_held_place() {
    let bytes = archive(copy(), false);
    let names = [
        "e93a07c1d25b4f68a0c3e1f7b9d24c85",
        "1b7f3d9e05a24c6f8e1d0b3a7c9f2e64",
    ];
    let pieces = split(&bytes, 3);
    assert_eq!(pieces.len(), 3);
    let mut object = store();
    for volume in [0, 1] {
        let piece = &pieces[volume].1;
        let (front, length) = (&piece[..32], Some(piece.len() as u64));
        object.bound(VolumeIndex(volume as u32), names[volume], front, length);
    }
    let mut restored =
        crate::restore(&object.facts(), weaver_graph::Budget::default()).expect("restores");
    let piece = &pieces[1].1;
    let (front, length) = (&piece[..32], Some(piece.len() as u64));
    let copy = "5d0f9a3c7e1b4826a9c4f0e2b7d13a68";
    assert_eq!(object.claim(copy, front, length), vec![VolumeIndex(1)]);
    assert_eq!(restored.claim(copy, front, length), vec![VolumeIndex(1)]);
}

// A place whose piece's front came is held across a restore: a piece of
// the same length that no name places is offered both middle places, the
// held one for the tree to weigh by fronts, and the set does not refuse
// for it, since one place is left free.
#[test]
fn a_place_whose_front_came_is_held_after_a_restore() {
    let (mut object, pieces) = unnamed_middles();
    let (name, front, length) = beside(&pieces[1]);
    object.bound(VolumeIndex(1), name, front, Some(length));
    let mut restored =
        crate::restore(&object.facts(), weaver_graph::Budget::default()).expect("restores");
    let (name, front, length) = beside(&pieces[2]);
    let objects: [&mut dyn weaver_graph::Object; 2] = [&mut object, restored.as_mut()];
    for object in objects {
        assert_eq!(
            object.claim(name, front, Some(length)),
            vec![VolumeIndex(1), VolumeIndex(2)]
        );
        let mut out = Vec::new();
        object.quiet(&Quiet(vec![beside(&pieces[2])]), &mut out);
        assert!(!out.contains(&Emit::Refuse(Why::OrderUnknown)), "{out:?}");
    }
}

// Five pieces, the first, second and fourth held: a middle piece no name
// places is offered every middle place, and one is free, so the set does
// not refuse for it.
#[test]
fn a_middle_piece_with_one_place_free_does_not_refuse_the_set() {
    let (mut object, pieces) = unnamed(5, &[0, 1, 3]);
    let (name, front, length) = beside(&pieces[2]);
    assert_eq!(
        object.claim(name, front, Some(length)),
        vec![VolumeIndex(1), VolumeIndex(2), VolumeIndex(3)]
    );
    let mut out = Vec::new();
    object.quiet(&Quiet(vec![beside(&pieces[2])]), &mut out);
    assert!(!out.contains(&Emit::Refuse(Why::OrderUnknown)), "{out:?}");
}

// A recovery set that gives the pieces their real names orders them: once
// the first piece carries its own, each middle piece takes the place its
// number gives, and nothing is left beside the set to refuse it for.
#[test]
fn a_piece_a_recovery_set_names_takes_the_place_its_number_gives() {
    let (mut object, pieces) = unnamed_middles();
    object.renamed(VolumeIndex(0), "alpha.7z.001", &mut Vec::new());
    for (volume, name) in [(2, "alpha.7z.003"), (1, "alpha.7z.002")] {
        let piece = &pieces[volume].1;
        let (front, length) = (&piece[..32], Some(piece.len() as u64));
        assert_eq!(
            object.claim(name, front, length),
            vec![VolumeIndex(volume as u32)]
        );
        object.bound(VolumeIndex(volume as u32), name, front, length);
    }
    let mut out = Vec::new();
    object.quiet(&Quiet(Vec::new()), &mut out);
    assert!(!out.contains(&Emit::Refuse(Why::OrderUnknown)), "{out:?}");
}

// A uuencoded piece says no length until it ends, so neither the first
// piece's length nor a later piece's own is known while they arrive: a
// piece numbered from the first piece's name still takes the place its
// number gives, before the first piece has closed.
#[test]
fn a_numbered_piece_of_unknown_length_takes_its_place_before_the_first_closes() {
    let bytes = archive(copy(), false);
    let volumes = split(&bytes, 4);
    let mut object = store();
    let (name, piece) = &volumes[0];
    object.bound(VolumeIndex(0), name, &piece[..32], None);
    for volume in [2, 1, 3] {
        let (name, piece) = &volumes[volume];
        assert_eq!(
            object.claim(name, &piece[..32], None),
            vec![VolumeIndex(volume as u32)]
        );
        object.bound(VolumeIndex(volume as u32), name, &piece[..32], None);
    }
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

// The arrival that completes the proof opens the folder in the same call:
// the proof's bytes reach the member once, through the cipher once, so the
// member comes out whole and enciphers back to the bytes as posted.
#[test]
fn the_arrival_that_proves_the_password_goes_through_the_cipher_once() {
    let lone = payload(4, 40_000);
    let bytes = write(aes(), false, &[(None, Entry::File("alpha.bin", &lone))]);
    let mut owner = Owner::single(store(), bytes.clone());
    // Bytes come in the order sent, never pulled ahead.
    owner.pull = Pull::Moved(0);
    owner.offer(PASSWORD);
    // The end header first, so the proof is chosen before the folder comes;
    // its last piece lies mid-folder, off the cipher's blocks.
    let tail = end_header(&bytes).start;
    let last = ByteRange::new(tail / 2 + 5, tail / 2 + 1005);
    owner.deliver(0, ByteRange::new(0, 32));
    owner.deliver(0, ByteRange::new(tail, bytes.len() as u64));
    owner.deliver(0, ByteRange::new(32, last.start));
    owner.deliver(0, ByteRange::new(last.end, tail));
    owner.deliver(0, last);
    owner.close(0);
    no_trouble(&owner);
    let got = owner.member("alpha.bin");
    assert!(got.closed);
    assert_eq!(got.bytes, lone);
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
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
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

// A 7z of a major version this store cannot read is refused for that, not
// handed to an engine as bytes no format names: it stays the container it
// is.
#[test]
fn a_major_version_the_store_cannot_read_is_refused_unsupported() {
    let mut bytes = archive(copy(), false);
    bytes[6] = 1;
    let mut owner = Owner::single(store(), bytes);
    owner.whole(4096);
    assert!(has(&owner, &Emit::Refuse(Why::Unsupported)));
    assert!(!has(&owner, &Emit::NeedsEngine(Why::Unsupported)));
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

type Files = Vec<(&'static str, Vec<u8>)>;

// The same payload under two directories: the second moves the boundary
// between the first two files.
fn moved_boundary() -> (Vec<u8>, Vec<u8>, Files) {
    let bytes = payload(1, 15_303);
    let layout = |cut: usize| {
        vec![
            ("alpha.bin", bytes[..cut].to_vec()),
            ("beta.bin", bytes[cut..10_000].to_vec()),
            ("gamma.bin", bytes[10_000..].to_vec()),
        ]
    };
    let write_layout = |files: &[(&'static str, Vec<u8>)]| {
        let entries: Vec<_> = files
            .iter()
            .map(|(name, bytes)| (None, Entry::File(name, bytes)))
            .collect();
        write(copy(), false, &entries)
    };
    let (before, after) = (layout(5_000), layout(4_000));
    (write_layout(&before), write_layout(&after), after)
}

// A header repaired into another directory over bytes the store already
// routed: the store moves them to the members the new map gives, from the
// bytes it routed, and asks for nothing twice.
#[test]
fn a_repaired_7z_header_remaps_routed_payload_without_an_engine() {
    let (before, after, files) = moved_boundary();
    assert_eq!(before.len(), after.len());
    assert_eq!(end_header(&before), end_header(&after));
    // The header stream the writer packs, and the end header naming it,
    // follow the payload.
    let payload: u64 = files.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let header = ByteRange::new(32 + payload, before.len() as u64);
    assert!(
        (0..before.len())
            .filter(|at| before[*at] != after[*at])
            .all(|at| at < 32 || header.contains(at as u64)),
        "only the headers differ"
    );
    let old = split(&before, 2);
    let new = split(&after, 2);
    let base = old[0].1.len() as u64;
    assert!(header.start >= base, "the headers lie in the second volume");
    let tail = ByteRange::new(header.start - base, header.end - base);
    let mut owner = Owner::new(store(), old.clone());
    // The first volume, then the old header: alpha whole, beta in part.
    owner.frames(0, 4096);
    owner.deliver(1, tail);
    assert!(owner.member("alpha.bin").closed);
    assert!(owner.replays == 0);
    // The repair writes the new signature header, then the new headers.
    owner.deliver_as(
        0,
        ByteRange::new(0, 32),
        Provenance::Repaired,
        Some(&new[0].1[..32]),
    );
    owner.deliver_as(
        1,
        tail,
        Provenance::Repaired,
        Some(&new[1].1[tail.start as usize..tail.end as usize]),
    );
    assert!(owner.replays > 0, "routed bytes come again");
    assert!(has(&owner, &Emit::Retract(MemberId(0))));
    assert!(has(&owner, &Emit::Retract(MemberId(1))));
    assert!(!has(&owner, &Emit::Retract(MemberId(2))), "gamma stays");
    // The rest of the payload, then both volumes close.
    owner.deliver_as(
        1,
        ByteRange::new(0, tail.start),
        Provenance::Fresh,
        Some(&new[1].1[..tail.start as usize]),
    );
    owner.close(0);
    owner.close(1);
    owner.quiet();
    assert!(
        !owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_) | Emit::Refuse(_))),
        "{:?}",
        owner.said
    );
    assert_eq!(owner.members.len(), files.len());
    for (name, bytes) in &files {
        let got = owner.member(name);
        assert_eq!(got.length, Some(bytes.len() as u64), "{name}");
        assert!(got.closed, "{name} closed");
        assert!(got.coverage.verified(ByteRange::new(0, bytes.len() as u64)));
        assert_eq!(&got.bytes, bytes, "{name}");
    }
    assert_eq!(owner.held(), 0);
    assert!(owner.refetched.is_empty(), "{:?}", owner.refetched);
    let rebuilt: Vec<u8> = (0..2)
        .flat_map(|volume| owner.posted(volume).unwrap())
        .collect();
    assert_eq!(rebuilt, after);
}

#[test]
fn corrected_piece_geometry_preserves_the_old_inverse_map_until_handoff() {
    let bytes = archive(copy(), false);
    let volumes = split(&bytes, 3);
    let mut owner = Owner::new(store(), volumes.clone());
    owner.whole(4096);
    streamed(&owner);
    let before = owner.object.geometry;
    let mut out = Vec::new();
    owner
        .object
        .sized(VolumeIndex(0), volumes[0].1.len() as u64 + 17, &mut out);
    assert_eq!(out, vec![Emit::NeedsEngine(Why::OrderUnknown)]);
    assert_eq!(
        owner.object.geometry, before,
        "old placement coordinates remain readable"
    );
    for (volume, (_, expected)) in volumes.iter().enumerate() {
        assert_eq!(owner.posted(volume).as_ref(), Some(expected));
    }
    out.clear();
    owner
        .object
        .sized(VolumeIndex(0), volumes[0].1.len() as u64 + 17, &mut out);
    assert!(out.is_empty(), "the handoff is requested once");
}

#[test]
fn unchanged_piece_geometry_does_not_restart_the_store() {
    let bytes = archive(copy(), false);
    let mut owner = Owner::single(store(), bytes.clone());
    owner.whole(4096);
    let mut out = Vec::new();
    owner
        .object
        .sized(VolumeIndex(0), bytes.len() as u64, &mut out);
    assert!(out.is_empty());
    // More bytes after a single-volume container do not move any member.
    owner
        .object
        .sized(VolumeIndex(0), bytes.len() as u64 + 17, &mut out);
    assert!(
        !out.iter()
            .any(|emit| matches!(emit, Emit::NeedsEngine(_) | Emit::Refuse(_)))
    );
    assert_eq!(
        owner.object.lengths[&VolumeIndex(0)],
        bytes.len() as u64 + 17
    );
    streamed(&owner);
}
