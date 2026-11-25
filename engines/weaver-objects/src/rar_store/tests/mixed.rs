// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set that is not solid, with stored files around a compressed one: the
// store streams the stored files, keeps the compressed one's packed data on
// the shelf beside the headers, and its run decodes that member from the
// shelf alone, as the RAR engine decodes it from the whole set.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use unrar_rs::{KdfCache, RarVolumeFacts};
use weaver_graph::{Budget, Emit, MemberId, Object, VolumeIndex, Volumes, Why};

use super::super::{claim, counts};
use super::owner::Owner;
use super::writer::{Codec, Item, Options, Version, literal, write};
use super::{PASSWORD, fixture, store, unrar};
use crate::engine::Format;
use crate::keys::{CAPACITY, Keys};
use crate::rar_engine::{Rar, member_window};
use crate::rar_store::RarStore;
use crate::testing::payload;

// A set's volumes whole, as the RAR engine reads them.
struct Whole(Vec<Vec<u8>>);

impl Volumes for Whole {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        let next = after.map_or(0, |after| after.0 + 1);
        Ok(((next as usize) < self.0.len()).then_some(VolumeIndex(next)))
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        self.0.get(volume.0 as usize).map(|v| v.len() as u64)
    }

    fn settled(&self, volume: VolumeIndex) -> bool {
        (volume.0 as usize) < self.0.len()
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let bytes = &self.0[volume.0 as usize];
        let start = (offset as usize).min(bytes.len());
        let end = (start + into.len()).min(bytes.len());
        into[..end - start].copy_from_slice(&bytes[start..end]);
        Ok(end - start)
    }
}

pub(super) fn facts(bytes: &[u8]) -> RarVolumeFacts {
    opened(bytes, None)
}

fn opened(bytes: &[u8], password: Option<&str>) -> RarVolumeFacts {
    claim::headers(bytes, password, Arc::new(KdfCache::new())).expect("a RAR volume")
}

// The bytes of the set's stored files: what the store streams and so never
// shelves. An enciphered one's parts carry its cipher padding past its
// size, which the store keeps on the shelf with the headers.
fn stored_data(volumes: &[Vec<u8>], password: Option<&str>) -> u64 {
    volumes
        .iter()
        .map(|bytes| {
            let facts = opened(bytes, password);
            assert!(facts.services.iter().all(|service| service.data_size == 0));
            facts
                .members
                .iter()
                .filter(|member| member.compression_method == 0 && !member.split_before)
                .map(|member| member.unpacked_size.expect("a stated size"))
                .sum::<u64>()
        })
        .sum()
}

// What a whole-engine run says of each member, by name: the bytes it
// wrote, the CRC32 it stated, whether it closed, and what else it said.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Said {
    pub bytes: Vec<u8>,
    pub crc32: Option<u32>,
    pub closed: bool,
}

pub(super) fn engine_run(
    volumes: &[Vec<u8>],
    password: Option<&str>,
) -> (BTreeMap<String, Said>, Vec<Emit>) {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    let view: Arc<dyn Volumes> = Arc::new(Whole(volumes.to_vec()));
    Rar::run(password.map(str::to_owned)).run(view, &mut sink, Budget::default());
    let mut names = BTreeMap::new();
    let mut said: BTreeMap<String, Said> = BTreeMap::new();
    let mut trouble = Vec::new();
    for emit in emits {
        match emit {
            Emit::Member { member, name, .. } => {
                names.insert(member.0, name.clone());
                said.insert(name, Said::default());
            }
            Emit::Bytes {
                member,
                offset,
                bytes,
                ..
            } => {
                let got = &mut said.get_mut(&names[&member.0]).unwrap().bytes;
                assert_eq!(offset, got.len() as u64);
                got.extend_from_slice(&bytes);
            }
            Emit::Checksum { member, crc32 } => {
                said.get_mut(&names[&member.0]).unwrap().crc32 = Some(crc32);
            }
            Emit::MemberClosed(member) => {
                said.get_mut(&names[&member.0]).unwrap().closed = true;
            }
            other => trouble.push(other),
        }
    }
    (said, trouble)
}

// What the store's run said of one member, as the engine's is read.
pub(super) fn run_said(owner: &Owner, name: &str) -> Said {
    let (id, got) = owner
        .members
        .iter()
        .find(|(_, got)| got.name == name)
        .unwrap_or_else(|| panic!("no member {name}"));
    Said {
        bytes: got.bytes.clone(),
        crc32: owner.said.iter().find_map(|emit| match emit {
            Emit::Checksum { member, crc32 } if member.0 == *id => Some(*crc32),
            _ => None,
        }),
        closed: got.closed,
    }
}

// The trouble a store or its run said.
pub(super) fn trouble(owner: &Owner) -> Vec<&Emit> {
    owner
        .said
        .iter()
        .filter(|emit| {
            matches!(
                emit,
                Emit::Damaged { .. }
                    | Emit::Gap { .. }
                    | Emit::NeedsEngine(_)
                    | Emit::Refuse(_)
                    | Emit::Retract(_)
                    | Emit::NeedsPassword
            )
        })
        .collect()
}

// The set's members in archive order, a part that carries one on joined to
// it: each one's name and whether it is compressed.
fn members(volumes: &[Vec<u8>], password: Option<&str>) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = Vec::new();
    for bytes in volumes {
        let facts = opened(bytes, password);
        let mut parts: Vec<_> = facts.members.iter().collect();
        parts.sort_by_key(|part| part.order);
        for part in parts {
            let compressed = part.compression_method != 0;
            if part.split_before {
                let last = out.last().expect("a part carries a member on");
                assert_eq!(last.1, compressed, "{}", part.name);
                continue;
            }
            out.push((part.name.clone(), compressed));
        }
    }
    out
}

// The compressed file RAR itself wrote in a fixture: its plain bytes,
// packed bytes and how they are coded.
pub(super) fn lifted(name: &str) -> (Vec<u8>, Vec<u8>, Codec) {
    let bytes = fixture(name);
    let first = facts(&bytes).members[0].name.clone();
    super::writer::lifted(&bytes, &first)
}

pub(super) fn packed(version: Version, name: &'static str) -> (Item, Vec<u8>) {
    let source = match version {
        Version::Rar5 => "extraction_profiles/rar5_lz.rar",
        Version::Rar4 => "extraction_profiles/rar4_lz.rar",
    };
    let (plain, packed, codec) = lifted(source);
    (
        Item::Packed {
            name,
            plain: plain.clone(),
            packed,
            codec,
        },
        plain,
    )
}

pub(super) fn mixed_items(version: Version) -> Vec<Item> {
    let (notes, _) = packed(version, "notes.nfo");
    vec![
        Item::File("alpha.mkv", payload(1, 10_000)),
        notes,
        Item::File("beta.mkv", payload(2, 5_003)),
        Item::File("gamma.srt", payload(3, 300)),
    ]
}

// The set delivered whole to the store in frames: nothing handed to an
// engine, every stored member streamed and closed, every compressed one
// announced and left to the run, the run reading only what the shelf
// holds, which is the set less its stored file data. Every member then
// stands as unrar extracts it, and each made member as the RAR engine says
// it, bytes, checksum and close alike.
pub(super) fn check(label: &str, volumes: Vec<Vec<u8>>, password: Option<&str>) -> Owner {
    let listed = members(&volumes, password);
    let extracted = unrar(&volumes, password);
    let total: u64 = volumes.iter().map(|v| v.len() as u64).sum();
    let stored = stored_data(&volumes, password);
    let mut owner = Owner::named(store(), volumes.clone());
    if let Some(password) = password {
        owner.offer(password);
    }
    owner.whole(4096);
    assert!(trouble(&owner).is_empty(), "{label}: {:?}", trouble(&owner));
    for (name, compressed) in &listed {
        let got = owner.member(name);
        assert_eq!(got.closed, !compressed, "{label}: {name}");
        if *compressed {
            assert!(got.bytes.is_empty(), "{label}: {name}");
        }
    }
    let face = owner.object.engine().expect("a mixed set runs");
    for (id, (name, compressed)) in listed.iter().enumerate() {
        assert_eq!(
            face.makes(MemberId(id as u32)),
            *compressed,
            "{label}: {name}"
        );
    }
    assert_eq!(owner.shelf_bytes(), total - stored, "{label}");

    let strays = owner.run();
    assert!(strays.is_empty(), "{label}: {strays:?}");
    assert!(trouble(&owner).is_empty(), "{label}: {:?}", trouble(&owner));
    assert_eq!(owner.members.len(), extracted.len(), "{label}");
    for (name, bytes) in &extracted {
        let got = owner.member(name);
        assert!(got.closed, "{label}: {name} closed");
        assert!(got.bytes == *bytes, "{label}: {name}");
    }
    let (engine, engine_trouble) = engine_run(&volumes, password);
    assert!(engine_trouble.is_empty(), "{label}: {engine_trouble:?}");
    for (name, _) in listed.iter().filter(|(_, compressed)| *compressed) {
        assert_eq!(run_said(&owner, name), engine[name], "{label}: {name}");
    }

    // The shelf has teeth: a run of every member reads a stored file's
    // data, which it never held. A set of compressed files alone holds no
    // such data.
    if stored > 0 {
        let mut sink = |_| true;
        let shelved = owner.shelved();
        let view: Arc<dyn Volumes> = shelved.clone();
        Rar::run(password.map(str::to_owned)).run(view, &mut sink, Budget::default());
        assert!(!shelved.strays.lock().unwrap().is_empty(), "{label}");
    }
    owner
}

#[test]
fn a_mixed_set_streams_its_stored_members_and_runs_its_compressed_ones_from_the_shelf() {
    for version in [Version::Rar5, Version::Rar4] {
        let items = mixed_items(version);
        for (label, split) in [
            ("one volume", None),
            // The packed data runs from the first volume into the second.
            ("packed across a boundary", Some(45_000)),
            // Every file's data spans volumes, stored and packed alike.
            ("many volumes", Some(6_000)),
        ] {
            let mut options = Options::new(version);
            options.split = split;
            let volumes = write(&items, &options);
            match split {
                Some(6_000) => assert!(volumes.len() >= 10, "{}", volumes.len()),
                Some(_) => assert!(volumes.len() > 1),
                None => {}
            }
            check(&format!("{version:?} {label}"), volumes, None);
        }
    }
}

// Sets RAR itself wrote: two stored files, then a compressed one.
#[test]
fn a_real_mixed_set_runs_its_compressed_member_from_the_shelf() {
    for name in [
        "extraction_profiles/rar5_multifile_lz.rar",
        "extraction_profiles/rar4_multifile_lz.rar",
    ] {
        let bytes = fixture(name);
        let listed = members(std::slice::from_ref(&bytes), None);
        assert!(!listed[0].1, "{name}: a stored member first");
        assert!(listed.iter().any(|(_, compressed)| *compressed), "{name}");
        check(name, vec![bytes], None);
    }
}

// The face a mixed set shows is the RAR engine's for the members it makes:
// the window it reserves is the widest of theirs, it lends as the engine
// does, and it makes those members and no other.
#[test]
fn a_mixed_set_faces_as_the_engine_would_for_its_compressed_members() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = Options::new(version);
        options.split = Some(6_000);
        let volumes = write(&mixed_items(version), &options);
        let mut owner = Owner::named(store(), volumes.clone());
        assert!(
            owner.object.engine().is_none(),
            "{version:?}: no run before a header"
        );
        // Before the set's last volume is read: the run starts at the first
        // member it makes, and chases the layout from there.
        for volume in 0..volumes.len() - 1 {
            owner.frames(volume, 4096);
        }
        assert!(
            owner.object.engine().is_some(),
            "{version:?}: a run before the end"
        );
        owner.whole(4096);
        let face = owner.object.engine().expect("a mixed set runs");
        let windows: Vec<u64> = volumes
            .iter()
            .flat_map(|bytes| {
                let facts = facts(bytes);
                facts
                    .members
                    .iter()
                    .filter(|member| !member.split_before)
                    .filter_map(|member| member_window(facts.is_solid, member))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(windows.len(), 1, "{version:?}");
        assert_eq!(
            face.reserve(),
            crate::engine::reserve::<Rar>(windows.clone())
        );
        assert_eq!(face.reserve(), Some(Rar::reserve().max(windows[0])));
        assert!(face.lends());
        assert_eq!(face.lends(), Rar::lends());
        assert_eq!(face.widest(), None);
        for volume in 0..volumes.len() {
            assert!(
                face.placed(VolumeIndex(volume as u32)),
                "{version:?} {volume}"
            );
        }
        // The made member's bytes are the run's, so the store places none.
        let notes = owner
            .members
            .iter()
            .find(|(_, got)| got.name == "notes.nfo")
            .map(|(id, _)| MemberId(*id))
            .unwrap();
        let mut out = Vec::new();
        assert_eq!(
            owner
                .object
                .extent(notes, weaver_graph::ByteRange::new(0, 1), &mut out),
            weaver_graph::Located::Never
        );
    }
}

// What stays with the engine whole: a solid set, whose members a run
// decodes only in order from the first.
#[test]
fn a_solid_set_still_goes_to_the_engine() {
    for version in [Version::Rar5, Version::Rar4] {
        for (label, items) in [
            ("mixed", mixed_items(version)),
            ("compressed first", compressed_first(version)),
        ] {
            let mut options = Options::new(version);
            options.solid = true;
            let volumes = write(&items, &options);
            let mut owner = Owner::named(store(), volumes);
            owner.whole(4096);
            assert!(
                owner.said.contains(&Emit::NeedsEngine(Why::Compressed)),
                "{version:?} {label}: {:?}",
                owner.said
            );
            assert!(owner.object.engine().is_none(), "{version:?} {label}");
        }
    }
}

fn compressed_first(version: Version) -> Vec<Item> {
    let (first, _) = packed(version, "first.nfo");
    vec![first, Item::File("alpha.mkv", payload(1, 10_000))]
}

// A set that is not solid, of compressed files alone: it stays with the
// store, whose run makes every member as the whole engine does, in one
// volume and across many.
#[test]
fn a_set_of_compressed_files_alone_stays_with_the_store() {
    for version in [Version::Rar5, Version::Rar4] {
        let (first, _) = packed(version, "first.nfo");
        let (second, _) = packed(version, "second.nfo");
        let items = vec![first, second];
        for split in [None, Some(6_000)] {
            let mut options = Options::new(version);
            options.split = split;
            let volumes = write(&items, &options);
            let owner = check(&format!("{version:?} {split:?}"), volumes, None);
            assert!(
                !owner
                    .said
                    .iter()
                    .any(|emit| matches!(emit, Emit::NeedsEngine(_)))
            );
        }
    }
}

// A set whose first file is compressed and is followed by a stored one: the
// stored one is streamed and the compressed one made.
#[test]
fn a_set_whose_first_file_is_compressed_streams_the_stored_one_after_it() {
    for version in [Version::Rar5, Version::Rar4] {
        for split in [None, Some(6_000)] {
            let mut options = Options::new(version);
            options.split = split;
            let volumes = write(&compressed_first(version), &options);
            let owner = check(&format!("{version:?} {split:?}"), volumes, None);
            assert!(owner.member("alpha.mkv").closed);
            assert!(
                !owner
                    .said
                    .iter()
                    .any(|emit| matches!(emit, Emit::NeedsEngine(_)))
            );
        }
    }
}

// A store's run is the engine's loop scoped to the members it makes: a
// stored member it is not given is passed, never announced or written.
#[test]
fn a_scoped_run_announces_nothing() {
    let volumes = write(&mixed_items(Version::Rar5), &Options::new(Version::Rar5));
    let mut owner = Owner::named(store(), volumes);
    owner.whole(4096);
    let before = owner.log.len();
    assert!(owner.run().is_empty());
    assert!(
        !owner.log[before..]
            .iter()
            .any(|emit| matches!(emit, Emit::Member { .. })),
        "{:?}",
        &owner.log[before..]
    );
    let written: Vec<MemberId> = owner.log[before..]
        .iter()
        .filter_map(|emit| match emit {
            Emit::Bytes { member, .. } | Emit::MemberClosed(member) => Some(*member),
            _ => None,
        })
        .collect();
    assert!(!written.is_empty());
    assert!(
        written.iter().all(|member| *member == MemberId(1)),
        "{written:?}"
    );
}

// Files enciphered under a password: a compressed one AES keys is made, as
// a compressed one in the clear is, beside stored files in the clear or
// enciphered, and with the headers enciphered too. The run decodes it
// under the password the job offered, as the engine does.
#[test]
fn an_enciphered_compressed_member_is_made_and_decodes_under_the_password() {
    for version in [Version::Rar5, Version::Rar4] {
        for (label, clear, headers) in [
            ("beside stored files in the clear", vec![0, 2, 3], false),
            ("beside enciphered stored files", vec![], false),
            ("headers enciphered", vec![], true),
        ] {
            for split in [None, Some(6_000)] {
                let mut options = Options::new(version);
                options.split = split;
                options.password = Some(PASSWORD);
                options.encrypt_headers = headers;
                options.clear = clear.clone();
                let volumes = write(&mixed_items(version), &options);
                check(
                    &format!("{version:?} {label} {split:?}"),
                    volumes,
                    Some(PASSWORD),
                );
            }
        }
    }
}

// A wrong password the job offered first: the run asks for another, as
// the engine's does, and the right one, offered next, decodes the member.
// The stored files in the clear never wait on it.
#[test]
fn an_enciphered_compressed_member_asks_again_after_a_wrong_password() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut options = Options::new(version);
        options.password = Some(PASSWORD);
        options.clear = vec![0, 2, 3];
        let volumes = write(&mixed_items(version), &options);
        let (_, engine_said) = engine_run(&volumes, Some("not-the-password"));
        assert_eq!(
            engine_said.last(),
            Some(&Emit::NeedsPassword),
            "{version:?}"
        );

        let mut owner = Owner::named(store(), volumes.clone());
        owner.offer("not-the-password");
        owner.whole(4096);
        assert!(
            trouble(&owner).is_empty(),
            "{version:?}: {:?}",
            trouble(&owner)
        );
        for name in ["alpha.mkv", "beta.mkv", "gamma.srt"] {
            assert!(owner.member(name).closed, "{version:?} {name}");
        }
        let before = owner.said.len();
        assert!(owner.run().is_empty());
        // What the engine says of the member, under the store's id.
        assert_eq!(owner.said[before..], engine_said[..], "{version:?}");
        assert!(!owner.member("notes.nfo").closed, "{version:?}");

        owner.said.clear();
        owner.offer(PASSWORD);
        assert!(owner.run().is_empty());
        assert!(
            trouble(&owner).is_empty(),
            "{version:?}: {:?}",
            trouble(&owner)
        );
        let (engine, _) = engine_run(&volumes, Some(PASSWORD));
        assert_eq!(
            run_said(&owner, "notes.nfo"),
            engine["notes.nfo"],
            "{version:?}"
        );
        assert!(owner.member("notes.nfo").closed);
    }
}

// Damage in an enciphered compressed member's packed data, under the right
// password. A RAR5 header with a password check value proves the password
// before a byte is decrypted, so the failure after it is the bytes': the
// whole engine's run and the store's scoped run alike say damage and never
// ask for another password. Without a check value, or in RAR4, which has
// none, the password is unproven and the same failure asks for another, as
// a wrong key's bytes would; but a RAR4 password that unsealed the store's
// headers is proved there, and the store's run, handed it as proved, says
// damage. The whole engine's run has no such proof.
#[test]
fn damage_under_a_proven_password_is_damage_and_under_an_unproven_one_asks_again() {
    for (version, check, proven) in [
        (Version::Rar5, true, true),
        (Version::Rar5, false, false),
        (Version::Rar4, true, false),
    ] {
        for headers in [false, true] {
            let label = format!("{version:?} check {check} headers {headers}");
            let mut options = Options::new(version);
            options.password = Some(PASSWORD);
            options.encrypt_headers = headers;
            options.clear = vec![0, 2, 3];
            options.check = check;
            let mut volumes = write(&mixed_items(version), &options);
            let notes = opened(&volumes[0], Some(PASSWORD))
                .members
                .into_iter()
                .find(|member| member.name == "notes.nfo")
                .expect("the compressed member");
            // Inside the coded stream, past its first block's header, so the
            // decode runs on and its checksum fails.
            volumes[0][(notes.data_offset + notes.data_size / 2) as usize] ^= 0x55;
            let asks = |said: &[&Emit]| said.contains(&&Emit::NeedsPassword);
            let damage =
                |said: &[&Emit]| said.iter().any(|emit| matches!(emit, Emit::Damaged { .. }));

            let (engine, engine_said) = engine_run(&volumes, Some(PASSWORD));
            let engine_said: Vec<&Emit> = engine_said.iter().collect();
            assert!(!engine["notes.nfo"].closed, "{label}");
            assert_eq!(asks(&engine_said), !proven, "{label}: {engine_said:?}");
            assert_eq!(damage(&engine_said), proven, "{label}: {engine_said:?}");

            let mut owner = Owner::named(store(), volumes);
            owner.offer(PASSWORD);
            owner.whole(4096);
            // Enciphered RAR5 headers with no check value go to the engine.
            if headers && !check {
                assert_eq!(
                    trouble(&owner),
                    [&Emit::NeedsEngine(Why::EncryptedHeader)],
                    "{label}"
                );
                continue;
            }
            assert!(trouble(&owner).is_empty(), "{label}: {:?}", trouble(&owner));
            let strays = owner.run();
            assert!(strays.is_empty(), "{label}: {strays:?} {:?}", owner.said);
            let said = trouble(&owner);
            let proven = proven || (headers && version == Version::Rar4);
            assert!(!owner.member("notes.nfo").closed, "{label}");
            assert_eq!(asks(&said), !proven, "{label}: {said:?}");
            assert_eq!(damage(&said), proven, "{label}: {said:?}");
        }
    }
}

// A set whose last volume has no end record, single and in volumes: no
// header says the layout is complete, and the run does not wait on one. It
// starts at the member it makes, before the job is quiet, and makes it.
#[test]
fn a_mixed_set_with_no_end_record_runs_before_one_is_read() {
    for version in [Version::Rar5, Version::Rar4] {
        for split in [None, Some(6_000)] {
            let label = format!("{version:?} {split:?}");
            let mut options = Options::new(version);
            options.split = split;
            options.numbered = true;
            options.ended = false;
            let volumes = write(&mixed_items(version), &options);
            let extracted = unrar(&volumes, None);
            assert_eq!(extracted.len(), 4, "{label}");
            let mut owner = Owner::named(store(), volumes);
            owner.whole(4096);
            assert!(trouble(&owner).is_empty(), "{label}: {:?}", trouble(&owner));
            let face = owner
                .object
                .engine()
                .expect("the set runs with no end record read");
            assert!(face.makes(MemberId(1)), "{label}");
            owner.quiet();
            assert!(owner.run().is_empty(), "{label}");
            assert!(trouble(&owner).is_empty(), "{label}: {:?}", trouble(&owner));
            for (name, bytes) in &extracted {
                let got = owner.member(name);
                assert!(got.closed && got.bytes == *bytes, "{label}: {name}");
            }
        }
    }
}

// A nameless RAR4 set whose end records state no number: its middle
// volumes are held where their links put them, on trust. Once the run can
// start, every volume it reads holds a part of a member, stored or made,
// and a volume the layout placed a part in is never given back or moved,
// so nothing the run read moves under it. A volume held past the set's
// end, which holds none, still goes back, and the run is untouched.
#[test]
fn a_nameless_rar4_volume_the_run_reads_stays_where_it_was_laid() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let volumes = write(&mixed_items(Version::Rar4), &options);
    assert!(volumes.len() >= 10);
    let named: Vec<(String, Vec<u8>)> = volumes
        .iter()
        .enumerate()
        .map(|(index, bytes)| (format!("{:x}3c9e1f", index * 7 + 11), bytes.clone()))
        .collect();
    let mut owner = Owner::new(store(), named);
    owner.whole(4096);
    assert!(trouble(&owner).is_empty(), "{:?}", trouble(&owner));
    assert!(owner.object.engine().is_some());
    let on_trust: Vec<VolumeIndex> = owner
        .object
        .volumes
        .iter()
        .filter(|(_, found)| super::super::layout::on_trust(found))
        .map(|(index, _)| *index)
        .collect();
    assert!(on_trust.len() >= 8, "{on_trust:?}");
    let mut out = Vec::new();
    owner
        .object
        .give_back_held(owner.object.volumes.keys().copied().collect(), &mut out);
    assert!(out.is_empty(), "{out:?}");
    assert!(owner.run().is_empty());
    for (name, bytes) in unrar(&volumes, None) {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{name}");
    }
}

// A RAR4 set, a stored file then a compressed one, whose two neighbouring
// volumes, only the compressed member's packed data in each, are posted
// under each other's names: the names hold each on trust where the other
// belongs, and the layout lays the member across them. A repair then names
// them rightly: each goes back, as the engine's volumes do, the member made
// from them is retracted, and once each is held at its own place the run
// makes it whole.
#[test]
fn a_volume_only_made_pieces_lie_in_named_elsewhere_goes_back_and_is_made_again() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let volumes = write(&stored_then_made(), &options);
    let made = made_only(&volumes);
    let (a, b) = (made[1], made[2]);
    assert_eq!(b, a + 1, "{made:?}");
    let mut posted = volumes.clone();
    posted.swap(a, b);
    let mut owner = Owner::named(store(), posted);
    owner.whole(4096);
    let notes = owner
        .object
        .entries
        .iter()
        .position(|entry| entry.made && entry.name == "notes.nfo")
        .expect("the compressed member is laid");
    let laid_in = |owner: &Owner| -> Vec<u32> {
        owner.object.entries.get(notes).map_or(Vec::new(), |entry| {
            entry.pieces.iter().map(|piece| piece.volume.0).collect()
        })
    };
    for volume in [a, b] {
        assert!(laid_in(&owner).contains(&(volume as u32)), "{volume}");
    }
    owner.said.clear();

    // The repair's names: each volume is misplaced where it is held.
    owner.renamed(a, &format!("alpha.part{}.rar", b + 1));
    owner.renamed(b, &format!("alpha.part{}.rar", a + 1));
    for volume in [a, b] {
        let volume = VolumeIndex(volume as u32);
        assert!(
            owner.said.contains(&Emit::GiveBack(volume)),
            "{:?}",
            owner.said
        );
        assert!(!owner.object.volumes.contains_key(&volume));
    }
    assert!(
        owner.said.contains(&Emit::Retract(MemberId(notes as u32))),
        "{:?}",
        owner.said
    );
    // Laid again only as far as the volumes still held, short of them.
    assert!(
        laid_in(&owner).iter().all(|&volume| (volume as usize) < a),
        "{:?}",
        laid_in(&owner)
    );

    // Each held again at its own place: the member is laid and announced
    // anew, and the run makes it whole.
    owner.exchanged(a, b);
    for volume in [a, b] {
        owner.close(volume);
    }
    assert_eq!(owner.object.volumes.len(), volumes.len());
    assert!(owner.run().is_empty());
    for (name, bytes) in unrar(&volumes, None) {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{name}");
    }
}

// The same swap with a stored file after the compressed member, laid in
// the volumes past the two: once the repair's names send the two back, the
// compressed member alone is retracted, where it stands. The stored file
// after it is still the member the layout lists at its id: it stays closed
// with its bytes, and nothing of it is said or written again, while the
// compressed member is laid and made anew into its own slot once both
// volumes are held at their places.
#[test]
fn a_made_member_retracted_where_it_stands_leaves_the_stored_file_after_it_whole() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let mut items = stored_then_made();
    items.push(Item::File("omega.mkv", payload(4, 9_000)));
    let volumes = write(&items, &options);
    let made = made_only(&volumes);
    let (a, b) = (made[1], made[2]);
    assert_eq!(b, a + 1, "{made:?}");
    let mut posted = volumes.clone();
    posted.swap(a, b);
    let mut owner = Owner::named(store(), posted);
    owner.whole(4096);
    let id_of = |owner: &Owner, name: &str| {
        owner
            .object
            .entries
            .iter()
            .position(|entry| entry.name == name)
            .unwrap_or_else(|| panic!("{name} is laid"))
    };
    let notes = id_of(&owner, "notes.nfo");
    let omega = id_of(&owner, "omega.mkv");
    assert!(owner.object.entries[notes].made);
    assert!(!owner.object.entries[omega].made);
    assert!(omega > notes);
    let laid_in = owner.object.entries[notes]
        .pieces
        .iter()
        .map(|piece| piece.volume.0 as usize)
        .collect::<Vec<_>>();
    assert!(laid_in.contains(&a) && laid_in.contains(&b), "{laid_in:?}");
    let extracted = unrar(&volumes, None);
    let streamed = &extracted
        .iter()
        .find(|(name, _)| name == "omega.mkv")
        .expect("omega.mkv")
        .1;
    let whole = |owner: &Owner| {
        let got = &owner.members[&(omega as u32)];
        got.closed && got.bytes == *streamed
    };
    assert!(whole(&owner));
    owner.said.clear();
    let from = owner.log.len();

    owner.renamed(a, &format!("alpha.part{}.rar", b + 1));
    owner.renamed(b, &format!("alpha.part{}.rar", a + 1));
    let retracted: Vec<&Emit> = owner
        .said
        .iter()
        .filter(|emit| matches!(emit, Emit::Retract(_)))
        .collect();
    assert_eq!(
        retracted,
        [&Emit::Retract(MemberId(notes as u32))],
        "{:?}",
        owner.said
    );
    // Laid again into its own slot, only as far as the volumes still held.
    let entry = &owner.object.entries[notes];
    assert!(
        entry.name == "notes.nfo" && entry.made && !entry.waiting,
        "{}",
        entry.name
    );
    assert!(
        entry
            .pieces
            .iter()
            .all(|piece| (piece.volume.0 as usize) < a),
        "{:?}",
        entry
            .pieces
            .iter()
            .map(|piece| piece.volume)
            .collect::<Vec<_>>()
    );
    assert!(!owner.object.entries[omega].waiting);
    assert!(whole(&owner));

    owner.exchanged(a, b);
    for volume in [a, b] {
        owner.close(volume);
    }
    assert_eq!(owner.object.volumes.len(), volumes.len());
    assert!(!owner.object.entries[notes].waiting);
    assert!(owner.run().is_empty());
    // Nothing of the stored file was said again: no retraction, no new
    // name, no byte and no close.
    let said_of_omega: Vec<&Emit> = owner.log[from..]
        .iter()
        .filter(|emit| match emit {
            Emit::Member { member, .. }
            | Emit::Bytes { member, .. }
            | Emit::Checksum { member, .. }
            | Emit::MemberClosed(member) => member.0 as usize == omega,
            Emit::Retract(member) => member.0 as usize == omega,
            _ => false,
        })
        .collect();
    assert!(said_of_omega.is_empty(), "{said_of_omega:?}");
    assert!(whole(&owner));
    for (name, bytes) in extracted {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{name}");
    }
}

// Its twin: a volume a stored member's bytes were streamed from, named
// elsewhere, stays. Its bytes went to that member's file, and the tree,
// handed it, could only refuse the member.
#[test]
fn a_volume_a_stored_member_streamed_from_named_elsewhere_stays() {
    let mut options = Options::new(Version::Rar4);
    options.split = Some(6_000);
    let volumes = write(&stored_then_made(), &options);
    let made = made_only(&volumes);
    // The stored file's last volume, past the first.
    let streamed = made[0] - 1;
    assert!(streamed > 0, "{made:?}");
    let mut owner = Owner::named(store(), volumes.clone());
    owner.whole(4096);
    owner.said.clear();
    owner.renamed(streamed, &format!("alpha.part{}.rar", streamed + 2));
    let volume = VolumeIndex(streamed as u32);
    // Misplaced by the name, two places on, as the made-only volumes were:
    // only its bytes, gone to the stored member, keep it.
    let found = &owner.object.volumes[&volume];
    assert!(!found.stated().is_some_and(claim::states_position) && found.ordered() != Some(volume));
    assert!(
        !owner.said.contains(&Emit::GiveBack(volume)),
        "{:?}",
        owner.said
    );
    assert!(owner.object.volumes.contains_key(&volume));
    assert!(owner.object.give_back(volume).is_none());
    assert!(owner.run().is_empty());
    for (name, bytes) in unrar(&volumes, None) {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{name}");
    }
}

// A stored file, then a compressed one that ends the set: a retraction of
// the compressed one reaches no stored member after it.
fn stored_then_made() -> Vec<Item> {
    let (notes, _) = packed(Version::Rar4, "notes.nfo");
    vec![Item::File("alpha.mkv", payload(1, 10_000)), notes]
}

// The volumes of a set no stored member's data lies in, in order.
fn made_only(volumes: &[Vec<u8>]) -> Vec<usize> {
    volumes
        .iter()
        .enumerate()
        .filter(|(_, bytes)| {
            facts(bytes)
                .members
                .iter()
                .all(|member| member.compression_method != 0 || member.data_size == 0)
        })
        .map(|(index, _)| index)
        .collect()
}

// A repaired header that names the compressed member otherwise than the
// bytes first posted did: the store renames it where it stands, retracting
// nothing, and the run then makes it under its repaired name. Plain and enciphered headers. The
// store's keys are its own, derived as the bytes come; enciphered RAR5
// headers also under keys already in the cache.
#[test]
fn a_repaired_header_of_a_made_member_renames_it_and_the_run_makes_it_again() {
    for version in [Version::Rar5, Version::Rar4] {
        for headers in [false, true] {
            repaired_header_of_a_made_member(version, headers, false);
        }
    }
    repaired_header_of_a_made_member(Version::Rar5, true, true);
}

// Enciphered RAR4 headers, damaged as first posted, repaired under keys
// already in the cache: the stored member after the made one is given back
// with its record's cipher padding, so its replay deciphers its last block
// and it closes.
#[test]
fn a_repaired_rar4_header_under_cached_keys_delivers_every_member() {
    repaired_header_of_a_made_member(Version::Rar4, true, true);
}

fn repaired_header_of_a_made_member(version: Version, headers: bool, warm: bool) {
    let mut options = Options::new(version);
    if headers {
        options.password = Some(PASSWORD);
        options.encrypt_headers = true;
    }
    let label = format!("{version:?} headers enciphered {headers} warm {warm}");
    let real = write(&mixed_items(version), &options).remove(0);
    let mut other = mixed_items(version);
    let Item::Packed { name, .. } = &mut other[1] else {
        unreachable!()
    };
    *name = "notez.nfo";
    let wrong = write(&other, &options).remove(0);
    assert_eq!(real.len(), wrong.len());
    let length = real.len() as u64;
    let keys = Arc::new(Keys::new(CAPACITY));
    if warm {
        let mut first = Owner::named(RarStore::with_keys(Arc::clone(&keys)), vec![real.clone()]);
        first.offer(PASSWORD);
        first.whole(4096);
    }
    let mut owner = Owner::named(RarStore::with_keys(keys), vec![real.clone()]);
    if headers {
        owner.offer(PASSWORD);
    }
    owner.deliver_as(
        0,
        weaver_graph::ByteRange::new(0, length),
        weaver_graph::Provenance::Damaged,
        Some(&wrong),
    );
    owner.close(0);
    // RAR4 headers enciphered in bytes that came damaged lay no
    // member until they are repaired, so no run starts before.
    let ran = !(headers && version == Version::Rar4);
    assert_eq!(owner.object.engine().is_some(), ran, "{label}");
    if ran {
        assert!(owner.run().is_empty(), "{label}");
        assert!(owner.member("notez.nfo").closed, "{label}");
        let made = owner.object.engine().expect("a mixed set runs");
        assert!(made.makes(MemberId(1)), "{label}");
    }

    // The data the run had of the made member, which stays the run's.
    let made_data: Vec<weaver_graph::ByteRange> = owner
        .object
        .entries
        .get(1)
        .map(|entry| entry.pieces.iter().map(|piece| piece.data).collect())
        .unwrap_or_default();

    // The header comes again repaired, and the rest after it.
    let first = real.iter().zip(&wrong).position(|(a, b)| a != b).unwrap() as u64;
    let header = weaver_graph::ByteRange::new(first.saturating_sub(64), first + 64);
    owner.said.clear();
    owner.deliver_as(0, header, weaver_graph::Provenance::Repaired, None);
    owner.deliver_as(
        0,
        weaver_graph::ByteRange::new(0, length),
        weaver_graph::Provenance::Repaired,
        None,
    );
    owner.close(0);
    assert!(
        !owner
            .said
            .iter()
            .any(|emit| matches!(emit, Emit::Retract(_))),
        "{label}: {:?}",
        owner.said
    );
    assert_eq!(owner.members[&1].name, "notes.nfo", "{label}");
    assert_eq!(
        owner
            .renamed
            .iter()
            .any(|(member, _, name)| *member == 1 && name == "notes.nfo"),
        ran,
        "{label}: {:?}",
        owner.renamed
    );
    // Still made, the member's data is not given back.
    assert!(
        !owner.said.iter().any(|emit| matches!(
            emit,
            Emit::Unkept { range, .. } if made_data.contains(range)
        )),
        "{label}: {:?}",
        owner.said
    );
    assert!(
        owner
            .object
            .engine()
            .expect("still mixed")
            .makes(MemberId(1))
    );
    // The repair replaced header bytes the shelf holds: the run starts
    // again, and makes the member under its repaired name.
    let strays = match ran {
        true => owner.rerun(),
        false => owner.run(),
    };
    assert!(strays.is_empty(), "{label}");
    for (name, bytes) in unrar(std::slice::from_ref(&real), options.password) {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{label}: {name}");
    }
}

// A RAR4 header posted naming a stored member compressed: the store makes
// it, keeping its data on the shelf for the run. Once the header comes
// again repaired, saying the member is stored, the store retracts it,
// gives back the data of each of its pieces, and streams it from those
// bytes: no byte of its data comes again.
#[test]
fn a_repaired_header_turning_a_made_member_stored_gives_back_its_data() {
    turned_stored(false);
}

// The same with a stored file after the turned member: the turned member
// alone is retracted, where it stands, and only its own data is given
// back. The file after it stays closed with its bytes, and nothing of it is
// said again.
#[test]
fn a_member_turned_stored_is_retracted_alone_before_a_stored_file() {
    turned_stored(true);
}

fn turned_stored(after: bool) {
    let version = Version::Rar4;
    let (notes, _) = packed(version, "notes.nfo");
    let mut items = vec![
        Item::File("alpha.mkv", payload(1, 10_000)),
        notes,
        Item::File("beta.mkv", payload(2, 5_003)),
    ];
    if after {
        items.push(Item::File("gamma.srt", payload(3, 3_000)));
    }
    let real = write(&items, &Options::new(version)).remove(0);
    let length = real.len() as u64;
    // beta.mkv's header, its method byte set to a compressed one.
    let mut wrong = real.clone();
    let name = wrong
        .windows(8)
        .rposition(|window| window == b"beta.mkv")
        .expect("beta.mkv's header");
    let at = name - 32;
    assert_eq!(wrong[at + 2], 0x74, "a file header");
    assert_eq!(wrong[at + 25], 0x30, "stored");
    wrong[at + 25] = 0x33;
    let size = u16::from_le_bytes([wrong[at + 5], wrong[at + 6]]) as usize;
    let crc = (crate::grid::crc32(&wrong[at + 2..at + size]) & 0xffff) as u16;
    wrong[at..at + 2].copy_from_slice(&crc.to_le_bytes());
    let header = weaver_graph::ByteRange::new(at as u64, (at + size) as u64);
    let packed = u32::from_le_bytes(wrong[at + 7..at + 11].try_into().unwrap()) as u64;
    let data = weaver_graph::ByteRange::at(header.end, packed);

    let mut owner = Owner::named(store(), vec![real.clone()]);
    owner.deliver(0, weaver_graph::ByteRange::new(0, header.start));
    owner.deliver_as(
        0,
        header,
        weaver_graph::Provenance::Damaged,
        Some(&wrong[header.start as usize..header.end as usize]),
    );
    owner.deliver(0, weaver_graph::ByteRange::new(header.end, length));
    owner.close(0);
    let made = owner.object.engine().expect("a mixed set runs");
    assert!(made.makes(MemberId(2)), "beta.mkv is made");

    let gamma = |owner: &Owner| {
        owner
            .members
            .get(&3)
            .map(|got| (got.closed, got.bytes.clone()))
    };
    let before = gamma(&owner);
    assert_eq!(before.is_some(), after);
    assert!(before.as_ref().is_none_or(|(closed, _)| *closed));
    owner.said.clear();
    let from = owner.log.len();
    owner.deliver_as(0, header, weaver_graph::Provenance::Repaired, None);
    let retracted: Vec<&Emit> = owner
        .said
        .iter()
        .filter(|emit| matches!(emit, Emit::Retract(_)))
        .collect();
    assert_eq!(retracted, [&Emit::Retract(MemberId(2))], "{:?}", owner.said);
    let unkept: Vec<&Emit> = owner
        .said
        .iter()
        .filter(|emit| matches!(emit, Emit::Unkept { .. }))
        .collect();
    assert_eq!(
        unkept,
        [&Emit::Unkept {
            volume: VolumeIndex(0),
            range: data,
            envelope: 0,
            at: data.start,
        }],
        "{:?}",
        owner.said
    );
    assert!(
        !owner
            .object
            .engine()
            .expect("still mixed")
            .makes(MemberId(2))
    );
    assert!(owner.run().is_empty());
    // Nothing of the file after it was said again.
    assert!(
        owner.log[from..].iter().all(|emit| !matches!(
            emit,
            Emit::Member {
                member: MemberId(3),
                ..
            } | Emit::Bytes {
                member: MemberId(3),
                ..
            } | Emit::Checksum {
                member: MemberId(3),
                ..
            } | Emit::MemberClosed(MemberId(3))
                | Emit::Retract(MemberId(3))
        )),
        "{:?}",
        owner.said
    );
    assert_eq!(gamma(&owner), before);
    for (name, bytes) in unrar(std::slice::from_ref(&real), None) {
        let got = owner.member(&name);
        assert!(got.closed && got.bytes == bytes, "{name}");
    }
}

// A part found by where its packed data starts, through the index the walk
// files with the headers, is the one a scan of the headers from the front
// finds, over one volume of many members, stored and made alike, and at
// offsets no part starts at. Each lookup looks at one header where the scan
// looks at every one before it. The run's reserve reads the windows the
// looks filed, and works none out.
#[test]
fn a_part_found_by_where_its_data_starts_is_the_one_a_scan_finds() {
    for version in [Version::Rar5, Version::Rar4] {
        let items: Vec<Item> = (0..400)
            .map(|at| {
                let name: &'static str = Box::leak(format!("m{at:04}.bin").into_boxed_str());
                match at % 2 {
                    0 => Item::File(name, payload(at as u64, 64)),
                    _ => literal(version, name, &payload(at as u64, 64)),
                }
            })
            .collect();
        let volumes = write(&items, &Options::new(version));
        assert_eq!(volumes.len(), 1, "{version:?}");
        let mut owner = Owner::named(store(), volumes);
        owner.whole(4096);
        assert!(owner.object.engine().is_some(), "{version:?}");
        let walk = &owner.object.volumes[&VolumeIndex(0)].walk;
        let members = &walk.facts.as_ref().expect("walked").members;
        assert!(members.len() >= 400, "{version:?}");
        let probes: Vec<u64> = members
            .iter()
            .flat_map(|member| [member.data_offset, member.data_offset + 1])
            .collect();
        let found = |walk: &super::super::walk::Walk| -> Vec<String> {
            probes
                .iter()
                .map(|&at| format!("{:?}", walk.part_at(at)))
                .collect()
        };
        counts::take();
        let indexed = found(walk);
        let indexed_looks = counts::take().parts_compared;
        counts::set_old(true);
        let scanned = found(walk);
        let scanned_looks = counts::take().parts_compared;
        counts::set_old(false);
        assert_eq!(indexed, scanned, "{version:?}");
        assert!(
            indexed.iter().step_by(2).all(|part| part != "None"),
            "{version:?}"
        );
        assert_eq!(indexed_looks, probes.len(), "{version:?}");
        let n = members.len();
        assert!(scanned_looks >= n * n, "{version:?}: {scanned_looks}");
        eprintln!(
            "{version:?}: {n} members, {} lookups: indexed {indexed_looks}, scanned {scanned_looks}",
            probes.len()
        );

        let engine = owner.object.engine().expect("a mixed set runs");
        let reserved = engine.reserve();
        assert_eq!(
            reserved,
            crate::engine::reserve::<Rar>(owner.object.made_windows()),
            "{version:?}"
        );
        counts::take();
        for _ in 0..100 {
            assert_eq!(engine.reserve(), reserved, "{version:?}");
        }
        assert_eq!(counts::take().windows_filed, 0, "{version:?}");
    }
}
