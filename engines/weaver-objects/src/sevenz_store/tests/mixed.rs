// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set with stored and coded folders. The store streams the stored members
// and keeps the coded packs in its envelope; its run, over that envelope
// alone, decodes the coded members and never reads a stored byte.

use std::io::{self, Cursor};
use std::sync::{Arc, Mutex};

use sevenz_turbo::encoder_options::DeltaOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, SourceReader};
use weaver_graph::{
    Budget, ByteRange, Emit, Engine, Extent, Located, MemberId, Object, Placement, Provenance,
    VolumeIndex, Volumes,
};

use super::owner::Owner;
use super::{PASSWORD, aes, copy, has, lzma2, no_trouble, store};
use crate::sevenz_engine::SevenZipEngine;
use crate::testing::payload;

type Files = Vec<(&'static str, Vec<u8>)>;

// alpha stored, beta and gamma in one coded folder, delta stored. With
// `header`, the end header is enciphered by those methods.
fn mixed_with(
    coded: Vec<EncoderConfiguration>,
    header: Option<Vec<EncoderConfiguration>>,
) -> (Vec<u8>, Files) {
    mixed_naming("gamma.srt", coded, header)
}

// The same, gamma named `gamma`.
fn mixed_naming(
    gamma: &'static str,
    coded: Vec<EncoderConfiguration>,
    header: Option<Vec<EncoderConfiguration>>,
) -> (Vec<u8>, Files) {
    let files = vec![
        ("alpha.mkv", payload(1, 70_000)),
        ("beta.nfo", payload(2, 3_000)),
        (gamma, payload(3, 9_000)),
        ("delta.mkv", payload(4, 50_000)),
    ];
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(copy());
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[0].0),
            Some(Cursor::new(files[0].1.clone())),
        )
        .unwrap();
    writer.set_content_methods(coded);
    writer
        .push_archive_entries(
            vec![
                ArchiveEntry::new_file(files[1].0),
                ArchiveEntry::new_file(files[2].0),
            ],
            vec![
                SourceReader::new(Cursor::new(files[1].1.clone())),
                SourceReader::new(Cursor::new(files[2].1.clone())),
            ],
        )
        .unwrap();
    writer.set_content_methods(copy());
    writer
        .push_archive_entry(
            ArchiveEntry::new_file(files[3].0),
            Some(Cursor::new(files[3].1.clone())),
        )
        .unwrap();
    if let Some(header) = header {
        writer.set_content_methods(header);
        writer.set_encrypt_header(true);
    }
    (writer.finish().unwrap().into_inner(), files)
}

fn mixed() -> (Vec<u8>, Files) {
    mixed_with(lzma2(), None)
}

// A filter that codes each byte alone, so a byte changed in its pack is one
// byte changed in its output, which its CRC catches.
fn delta() -> Vec<EncoderConfiguration> {
    vec![DeltaOptions::from_distance(1).into()]
}

// The envelope as a run reads it: the container's bytes where the store
// kept them, and an error anywhere else.
struct Envelope {
    bytes: Vec<u8>,
    kept: Vec<ByteRange>,
    strays: Mutex<Vec<ByteRange>>,
}

impl Volumes for Envelope {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        Ok(after.is_none().then_some(VolumeIndex(0)))
    }

    fn len(&self, _volume: VolumeIndex) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }

    fn settled(&self, _volume: VolumeIndex) -> bool {
        true
    }

    fn read_at(&self, _volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let end = (offset + into.len() as u64).min(self.bytes.len() as u64);
        let range = ByteRange::new(offset.min(end), end);
        let mut at = range.start;
        for kept in &self.kept {
            if kept.start <= at && kept.end > at {
                at = kept.end;
            }
        }
        if at < range.end {
            self.strays.lock().unwrap().push(range);
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }
        let len = range.len() as usize;
        into[..len].copy_from_slice(&self.bytes[range.start as usize..range.end as usize]);
        Ok(len)
    }
}

// What the store keeps in its envelope, at its container offsets, from the
// container's bytes as they were delivered.
fn envelope(owner: &Owner, archive: &[u8]) -> Arc<Envelope> {
    let whole = ByteRange::new(0, archive.len() as u64);
    let mut placements = Vec::new();
    owner.object.locate(VolumeIndex(0), whole, &mut placements);
    let kept: Vec<ByteRange> = placements
        .iter()
        .filter_map(|placement| match placement {
            Placement::Envelope { envelope: 0, range } => Some(*range),
            _ => None,
        })
        .collect();
    let mut bytes = vec![0u8; archive.len()];
    for range in &kept {
        bytes[range.start as usize..range.end as usize]
            .copy_from_slice(&archive[range.start as usize..range.end as usize]);
    }
    Arc::new(Envelope {
        bytes,
        kept,
        strays: Mutex::default(),
    })
}

fn run(engine: &mut dyn Engine, volumes: Arc<dyn Volumes>) -> Vec<Emit> {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    engine.start().run(volumes, &mut sink, Budget::default());
    emits
}

// Each member's bytes as a run wrote them, in order, and the members it
// closed.
fn written(emits: &[Emit], members: usize) -> (Vec<Vec<u8>>, Vec<MemberId>) {
    let mut out: Vec<Vec<u8>> = vec![Vec::new(); members];
    let mut closed = Vec::new();
    for emit in emits {
        match emit {
            Emit::Bytes {
                member,
                offset,
                bytes,
                ..
            } => {
                let into = &mut out[member.0 as usize];
                assert_eq!(*offset, into.len() as u64);
                into.extend_from_slice(bytes);
            }
            Emit::MemberClosed(member) => closed.push(*member),
            Emit::Member { .. } | Emit::Checksum { .. } => {}
            other => panic!("the run said {other:?}"),
        }
    }
    (out, closed)
}

#[test]
fn the_store_streams_stored_folders_and_a_run_decodes_the_coded_one_from_the_envelope() {
    let (archive, files) = mixed();
    let mut owner = Owner::single(store(), archive.clone());
    owner.whole(4096);
    no_trouble(&owner);

    // Stored members arrived whole and closed; coded ones are named only.
    for (id, (name, bytes)) in files.iter().enumerate() {
        let got = &owner.members[&(id as u32)];
        assert_eq!(&got.name, name);
        match id {
            0 | 3 => {
                assert!(got.closed, "{name} closed");
                assert_eq!(&got.bytes, bytes, "{name} streamed");
            }
            _ => {
                assert!(!got.closed, "{name} waits for the run");
                assert!(
                    got.bytes.iter().all(|b| *b == 0),
                    "{name} got no bytes from the store"
                );
            }
        }
    }

    let envelope = envelope(&owner, &archive);
    let envelope_bytes: u64 = envelope.kept.iter().map(ByteRange::len).sum();
    let stored_bytes = (files[0].1.len() + files[3].1.len()) as u64;
    assert_eq!(
        envelope_bytes + stored_bytes,
        archive.len() as u64,
        "every byte is a member's or the envelope's"
    );

    let engine = owner.object.engine_mut().expect("a mixed set runs");
    let emits = run(engine, envelope.clone());
    assert!(
        envelope.strays.lock().unwrap().is_empty(),
        "read outside the envelope: {:?}",
        envelope.strays
    );
    // The run names its members as the store did, and only those.
    let named: Vec<_> = emits
        .iter()
        .filter_map(|emit| match emit {
            Emit::Member {
                member,
                name,
                length,
            } => Some((*member, name.clone(), *length)),
            _ => None,
        })
        .collect();
    assert_eq!(
        named,
        vec![
            (MemberId(1), "beta.nfo".to_owned(), Some(3_000)),
            (MemberId(2), "gamma.srt".to_owned(), Some(9_000)),
        ]
    );
    let (out, closed) = written(&emits, files.len());
    assert_eq!(closed, vec![MemberId(1), MemberId(2)]);
    assert_eq!(out[1], files[1].1);
    assert_eq!(out[2], files[2].1);
    assert!(out[0].is_empty() && out[3].is_empty());
}

// The store faces as an engine only once its map codes a folder, makes only
// the coded members, places each at its folder's whole pack, and reserves
// and borrows as the 7z engine the set would otherwise go to.
#[test]
fn a_mixed_set_faces_as_the_engine_the_set_would_go_to() {
    let (archive, _) = mixed();
    let mut owner = Owner::single(store(), archive.clone());
    assert!(owner.object.engine().is_none(), "no run before the map");
    owner.whole(4096);
    let map = owner.object.map.clone().expect("mapped");
    let face = owner.object.engine().expect("a mixed set runs");
    assert_eq!(
        (0..4)
            .map(|id| face.makes(MemberId(id)))
            .collect::<Vec<_>>(),
        vec![false, true, true, false]
    );

    let carried = owner
        .object
        .engine_volume(VolumeIndex(0))
        .expect("the store hands its file on");
    let mut engine = SevenZipEngine::new();
    engine.adopt(
        VolumeIndex(0),
        "alpha.7z",
        &archive[..32],
        Some(archive.len() as u64),
        carried,
    );
    let window = owner.object.window.expect("the coded folder's window");
    assert!(window > 0);
    assert_eq!(face.reserve(), Some(window));
    assert_eq!(face.reserve(), Engine::reserve(&engine));
    assert_eq!(face.lends(), Engine::lends(&engine));
    assert_eq!(face.widest(), Engine::widest(&engine));

    let pack = map.folders[1].pack;
    for id in [1, 2] {
        let mut out = Vec::new();
        let located = owner
            .object
            .extent(MemberId(id), ByteRange::new(0, 1), &mut out);
        assert_eq!(located, Located::Whole);
        assert_eq!(
            out,
            vec![Extent {
                volume: VolumeIndex(0),
                range: pack
            }]
        );
    }

    // A set with nothing coded has no run.
    let mut stored = Owner::single(store(), super::archive(copy(), false));
    stored.whole(4096);
    assert!(stored.object.engine().is_none());
}

// The end header enciphered: the store opens it with the password, and the
// run reads it again under the same one.
#[test]
fn an_encrypted_header_mixed_set_runs_under_the_password_that_opened_it() {
    let (archive, files) = mixed_with(lzma2(), Some(aes()));
    let mut owner = Owner::single(store(), archive.clone());
    owner.frames(0, 4096);
    assert!(has(&owner, &Emit::NeedsPassword));
    assert!(owner.object.engine().is_none());
    owner.offer(PASSWORD);
    owner.close(0);
    no_trouble(&owner);
    for id in [0, 3] {
        assert_eq!(owner.members[&id].bytes, files[id as usize].1);
    }

    let envelope = envelope(&owner, &archive);
    let engine = owner.object.engine_mut().expect("a mixed set runs");
    let emits = run(engine, envelope.clone());
    assert!(envelope.strays.lock().unwrap().is_empty());
    let (out, closed) = written(&emits, files.len());
    assert_eq!(closed, vec![MemberId(1), MemberId(2)]);
    assert_eq!(out[1], files[1].1);
    assert_eq!(out[2], files[2].1);
}

// A coded folder whose bytes fail their CRC: the run says what the 7z
// engine says of the same damage, for the same members.
#[test]
fn a_coded_folder_that_fails_its_crc_says_what_the_engine_says() {
    let (mut archive, files) = mixed_with(delta(), None);
    let (clean, _) = mixed_with(delta(), None);
    let mut owner = Owner::single(store(), clean);
    owner.whole(4096);
    let pack = owner.object.map.clone().expect("mapped").folders[1].pack;
    archive[(pack.start + 100) as usize] ^= 0x5a;

    let envelope = envelope(&owner, &archive);
    let coded = run(
        owner.object.engine_mut().expect("a mixed set runs"),
        envelope.clone(),
    );
    assert!(envelope.strays.lock().unwrap().is_empty());
    assert!(
        matches!(coded.last(), Some(Emit::Refuse(_))),
        "the run refused: {coded:?}"
    );

    // The engine over the whole damaged set, its members numbered alike.
    let mut engine = SevenZipEngine::new();
    let whole = Arc::new(Envelope {
        kept: vec![ByteRange::new(0, archive.len() as u64)],
        bytes: archive,
        strays: Mutex::default(),
    });
    let said = run(&mut engine, whole);
    let coded_member = |member: &MemberId| (1..=2).contains(&member.0);
    let engine_said: Vec<Emit> = said
        .into_iter()
        .filter(|emit| match emit {
            Emit::Member { member, .. }
            | Emit::Checksum { member, .. }
            | Emit::Bytes { member, .. }
            | Emit::Retract(member)
            | Emit::MemberClosed(member) => coded_member(member),
            Emit::Failed(_) | Emit::Refuse(_) => true,
            _ => false,
        })
        .collect();
    assert_eq!(coded, engine_said);
    assert!(coded.contains(&Emit::Retract(MemberId(1))));
    assert_eq!(files.len(), 4);
}

// A header repaired into one that names a coded member otherwise, over a
// set the store mapped and streamed: the store retracts that member alone,
// for its run to make again, and asks for no byte again, since a coded
// member's bytes are its run's and the store routed none of them. Its
// engine face, and the run it starts, are the new map's at once.
#[test]
fn a_repaired_header_that_moves_a_coded_member_retracts_it_alone() {
    let (before, _) = mixed_naming("gamma.srt", lzma2(), None);
    let (after, files) = mixed_naming("gamme.srt", lzma2(), None);
    assert_eq!(before.len(), after.len());
    let first = (32..before.len())
        .find(|at| before[*at] != after[*at])
        .unwrap();
    let header = ByteRange::new(first as u64, before.len() as u64);
    let mut owner = Owner::single(store(), before.clone());
    owner.whole(4096);
    no_trouble(&owner);
    let said = owner.said.len();
    owner.deliver_as(
        0,
        ByteRange::new(0, 32),
        Provenance::Repaired,
        Some(&after[..32]),
    );
    owner.deliver_as(
        0,
        header,
        Provenance::Repaired,
        Some(&after[header.start as usize..]),
    );
    let since = &owner.said[said..];
    let retracted: Vec<&Emit> = since
        .iter()
        .filter(|emit| matches!(emit, Emit::Retract(_)))
        .collect();
    assert_eq!(retracted, vec![&Emit::Retract(MemberId(2))], "{since:?}");
    assert!(
        !since.iter().any(|emit| matches!(emit, Emit::Again { .. })),
        "{since:?}"
    );
    assert_eq!(owner.member("gamme.srt").name, "gamme.srt");
    let kept = envelope(&owner, &after);
    let engine = owner.object.engine_mut().expect("still mixed");
    assert!(engine.makes(MemberId(1)) && engine.makes(MemberId(2)));
    assert!(!engine.makes(MemberId(0)) && !engine.makes(MemberId(3)));
    let emits = run(engine, kept);
    let (out, closed) = written(&emits, files.len());
    assert_eq!(closed, vec![MemberId(1), MemberId(2)]);
    for id in [1, 2] {
        assert_eq!(out[id], files[id].1, "{}", files[id].0);
    }
}
