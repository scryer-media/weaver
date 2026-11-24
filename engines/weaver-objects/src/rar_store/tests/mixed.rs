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

use super::super::claim;
use super::owner::Owner;
use super::writer::{Codec, Item, Options, Version, write};
use super::{fixture, store, unrar};
use crate::engine::Format;
use crate::rar_engine::{Rar, member_window};
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
    claim::headers(bytes, None, Arc::new(KdfCache::new())).expect("a RAR volume")
}

// The data bytes of the set's stored file parts: what the store streams
// and so never shelves.
fn stored_data(volumes: &[Vec<u8>]) -> u64 {
    volumes
        .iter()
        .map(|bytes| {
            let facts = facts(bytes);
            assert!(facts.services.iter().all(|service| service.data_size == 0));
            facts
                .members
                .iter()
                .filter(|member| member.compression_method == 0)
                .map(|member| member.data_size)
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
fn members(volumes: &[Vec<u8>]) -> Vec<(String, bool)> {
    let mut out: Vec<(String, bool)> = Vec::new();
    for bytes in volumes {
        let facts = facts(bytes);
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
    let plain = unrar(std::slice::from_ref(&bytes), None);
    assert_eq!(plain.len(), 1);
    let facts = facts(&bytes);
    let member = &facts.members[0];
    assert_ne!(member.compression_method, 0);
    assert!(!facts.is_solid && !member.compression_solid);
    let at = member.data_offset as usize;
    let packed = bytes[at..at + member.data_size as usize].to_vec();
    let codec = Codec {
        method: member.compression_method,
        version: member.compression_version,
        dict: member.dict_size,
    };
    (plain[0].1.clone(), packed, codec)
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
fn check(label: &str, volumes: Vec<Vec<u8>>, password: Option<&str>) -> Owner {
    let listed = members(&volumes);
    let extracted = unrar(&volumes, password);
    let total: u64 = volumes.iter().map(|v| v.len() as u64).sum();
    let stored = stored_data(&volumes);
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
    // data, which it never held.
    let mut sink = |_| true;
    let shelved = owner.shelved();
    let view: Arc<dyn Volumes> = shelved.clone();
    Rar::run(password.map(str::to_owned)).run(view, &mut sink, Budget::default());
    assert!(!shelved.strays.lock().unwrap().is_empty(), "{label}");
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
        let listed = members(std::slice::from_ref(&bytes));
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
        // Not before the set's last volume is read: a header still to come
        // could add a member the run makes.
        for volume in 0..volumes.len() - 1 {
            owner.frames(volume, 4096);
        }
        assert!(
            owner.object.engine().is_none(),
            "{version:?}: no run before the end"
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
        assert_eq!(face.reserve(), crate::engine::reserve::<Rar>(windows.clone()));
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

// What stays with the engine whole: a solid set, a set of compressed files
// alone, and one whose first file is compressed, which nothing up front
// says is followed by a stored one.
#[test]
fn sets_the_store_cannot_stream_part_of_still_go_to_the_engine() {
    for version in [Version::Rar5, Version::Rar4] {
        let (first, _) = packed(version, "first.nfo");
        let (second, _) = packed(version, "second.nfo");
        for (label, items, solid) in [
            ("compressed only", vec![first.clone(), second], false),
            (
                "compressed first",
                vec![first, Item::File("alpha.mkv", payload(1, 10_000))],
                false,
            ),
            ("solid", mixed_items(version), true),
        ] {
            let mut options = Options::new(version);
            options.solid = solid;
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
