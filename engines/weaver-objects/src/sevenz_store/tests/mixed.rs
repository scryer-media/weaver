// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Spike: a set with stored and coded folders. The store streams the stored
// members and keeps the coded packs in its envelope; a run over that
// envelope alone decodes the coded members, and never reads a stored byte.

use std::io::{self, Cursor};
use std::sync::{Arc, Mutex};

use sevenz_turbo::{ArchiveEntry, ArchiveWriter, SourceReader};
use weaver_graph::{
    ByteRange, Emit, Extraction, MemberId, Object, Placement, VolumeIndex, Volumes,
};

use super::super::coded::CodedRun;
use super::owner::Owner;
use super::{copy, lzma2, no_trouble, store};
use crate::testing::payload;

fn mixed() -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    let files = vec![
        ("alpha.mkv", payload(1, 70_000)),
        ("beta.nfo", payload(2, 3_000)),
        ("gamma.srt", payload(3, 9_000)),
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
    writer.set_content_methods(lzma2());
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
    (writer.finish().unwrap().into_inner(), files)
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

#[test]
fn the_store_streams_stored_folders_and_a_run_decodes_the_coded_one_from_the_envelope() {
    let (archive, files) = mixed();
    let mut owner = Owner::single(store().mixed(), archive.clone());
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

    // What the store keeps in its envelope, in container coordinates.
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
    let envelope_bytes: u64 = kept.iter().map(ByteRange::len).sum();
    let stored_bytes = (files[0].1.len() + files[3].1.len()) as u64;
    assert_eq!(
        envelope_bytes + stored_bytes,
        archive.len() as u64,
        "every byte is a member's or the envelope's"
    );

    let map = owner.object.map.clone().expect("mapped");
    let envelope = Arc::new(Envelope {
        bytes,
        kept,
        strays: Mutex::default(),
    });
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    let run: Box<dyn Extraction> = Box::new(CodedRun::new(&map, None));
    run.run(envelope.clone(), &mut sink, weaver_graph::Budget::default());
    assert!(
        envelope.strays.lock().unwrap().is_empty(),
        "read outside the envelope: {:?}",
        envelope.strays
    );

    let mut out: Vec<Vec<u8>> = vec![Vec::new(); files.len()];
    let mut closed = Vec::new();
    for emit in &emits {
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
            other => panic!("the run said {other:?}"),
        }
    }
    assert_eq!(closed, vec![MemberId(1), MemberId(2)]);
    assert_eq!(out[1], files[1].1);
    assert_eq!(out[2], files[2].1);
    assert!(out[0].is_empty() && out[3].is_empty());
}
