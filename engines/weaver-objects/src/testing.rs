// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Test support: generated payloads, recovery sets made by the creators, an
// in-memory namespace face that counts its reads, and segment cutting the
// way a namespace cuts them.

pub(crate) mod replay;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use weaver_graph::recovery::MemberAccess;
use weaver_graph::{
    Beside, ByteRange, Delivery, Emit, MemberId, Namespace, Object, ObjectId, Provenance, Segment,
    Standing, VolumeIndex,
};

use crate::grid::crc32;

// A quiet job's record of an object whose members all stand whole and
// placed, beside members no object owns: each by name, front and length.
pub(crate) struct Quiet<'a>(pub(crate) Vec<(&'a str, &'a [u8], u64)>);

impl Standing for Quiet<'_> {
    fn damaged(&self) -> bool {
        false
    }
    fn unplaced(&self) -> bool {
        false
    }
    fn lost(&self) -> bool {
        false
    }
    fn asking(&self) -> bool {
        false
    }
    fn beside(&self) -> Vec<Beside<'_>> {
        self.0
            .iter()
            .map(|(name, front, length)| Beside {
                name,
                front,
                length: Some(*length),
                carried: None,
            })
            .collect()
    }
}

// Deterministic bytes, distinct per seed.
pub(crate) fn payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as u8
        })
        .collect()
}

// What a zip64 end record and its locator say, as `zip64_end` writes them.
pub(crate) struct Zip64 {
    // Where the locator says the zip64 record is, within its disk.
    pub(crate) record: u64,
    pub(crate) size: u64,
    pub(crate) offset: u64,
}

// The last volume of a zip ended by zip64 records: its end record replaced
// by a zip64 record, its locator and a saturated end record, with `edit`
// changing what they say from what the end record said.
pub(crate) fn zip64_end(last: &mut Vec<u8>, disks: u32, edit: impl FnOnce(&mut Zip64)) {
    use crate::zip::records::{END, END64, END64_LEN, LOCATOR, find_end};
    let end = find_end(last).unwrap();
    last.truncate(end.at as usize);
    let mut said = Zip64 {
        record: last.len() as u64,
        size: end.size,
        offset: end.offset,
    };
    edit(&mut said);
    last.extend(END64.to_le_bytes());
    last.extend((END64_LEN - 12).to_le_bytes());
    last.extend([45, 0, 45, 0]);
    last.extend(end.this_disk.to_le_bytes());
    last.extend(end.directory_disk.to_le_bytes());
    last.extend(end.entries.to_le_bytes());
    last.extend(end.entries.to_le_bytes());
    last.extend(said.size.to_le_bytes());
    last.extend(said.offset.to_le_bytes());
    last.extend(LOCATOR.to_le_bytes());
    last.extend(end.this_disk.to_le_bytes());
    last.extend(said.record.to_le_bytes());
    last.extend(disks.to_le_bytes());
    last.extend(END.to_le_bytes());
    last.extend([0xff; 16]);
    last.extend(0u16.to_le_bytes());
}

// A whole zip cloaked: the directory entry named `from` renamed `to`, a
// name of the same length, while its local header keeps the old one.
pub(crate) fn cloak(archive: &mut [u8], from: &str, to: &str) {
    use crate::zip::records::{CENTRAL_LEN, directory, find_end};
    assert_eq!(from.len(), to.len());
    let end = find_end(archive).unwrap();
    let start = end.offset as usize;
    let entries = directory(&archive[start..start + end.size as usize], end.entries).unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry.header.name == from)
        .unwrap();
    let name = start + entry.span.start + CENTRAL_LEN;
    archive[name..name + to.len()].copy_from_slice(to.as_bytes());
}

// A PAR2 set over `files`, made by par2-rs's creator: the index first, then
// the recovery volumes.
pub(crate) fn par2_set(
    stem: &str,
    files: &[(&str, &[u8])],
    block: u64,
    recovery: u32,
    volumes: u32,
) -> Vec<(String, Vec<u8>)> {
    par2_set_from(stem, files, block, 0, recovery, volumes)
}

// The same, its recovery blocks numbered from `first`.
pub(crate) fn par2_set_from(
    stem: &str,
    files: &[(&str, &[u8])],
    block: u64,
    first: u32,
    recovery: u32,
    volumes: u32,
) -> Vec<(String, Vec<u8>)> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

    let dir = tempfile::tempdir().unwrap();
    let inputs = write_inputs(dir.path(), files);
    let mut options = Par2CreatorOptions::with_output(
        dir.path().join(format!("{stem}.par2")),
        Some(dir.path().to_path_buf()),
        inputs,
    );
    options.block_sizing = BlockSizing::Bytes(block);
    options.recovery_amount = RecoveryAmount::Count(recovery);
    options.volume_count = Some(volumes);
    options.first_exponent = first;
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    read_outputs(&created.output_paths)
}

// A PAR3 set over `files`, made by par3-rs's creator.
pub(crate) fn par3_set(
    stem: &str,
    files: &[(&str, &[u8])],
    block: u64,
    recovery: u64,
) -> Vec<(String, Vec<u8>)> {
    use par3_rs::create::{CreateOptions, InputSpec, RecoveryAmount, create};

    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<_> = files
        .iter()
        .map(|(name, _)| std::path::PathBuf::from(name))
        .collect();
    write_inputs(dir.path(), files);
    let report = create(
        &InputSpec::new(dir.path(), &inputs),
        &dir.path().join(format!("{stem}.par3")),
        &CreateOptions::default()
            .with_block_size(block)
            .with_recovery(RecoveryAmount::Blocks(recovery)),
    )
    .unwrap();
    read_outputs(&report.files_written)
}

fn write_inputs(dir: &Path, files: &[(&str, &[u8])]) -> Vec<std::path::PathBuf> {
    files
        .iter()
        .map(|(name, bytes)| {
            let path = dir.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect()
}

fn read_outputs(paths: &[std::path::PathBuf]) -> Vec<(String, Vec<u8>)> {
    let mut outputs: Vec<_> = paths
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    // The index, the one without a `.vol`, first.
    outputs.sort_by_key(|(name, _)| (name.contains(".vol"), name.clone()));
    outputs
}

// Segments of `bytes` at `offset`, cut wherever an article of `article`
// bytes ends and wherever a block of `block` bytes ends: what a namespace
// hands its recovery objects.
pub(crate) fn segments(
    offset: u64,
    bytes: &[u8],
    article: u64,
    block: u64,
    verified: bool,
) -> Vec<Segment> {
    let end = offset + bytes.len() as u64;
    let mut cuts: Vec<u64> = (offset..end)
        .filter(|at| *at > offset && (at % article == 0 || at % block == 0))
        .collect();
    cuts.push(end);
    let mut start = offset;
    cuts.into_iter()
        .map(|cut| {
            let range = ByteRange::new(start, cut);
            let piece = &bytes[(start - offset) as usize..(cut - offset) as usize];
            start = cut;
            Segment {
                range,
                crc32: crc32(piece),
                verified,
            }
        })
        .collect()
}

// A namespace's face toward its recovery objects, over bytes in memory.
// Repaired bytes land at once, so a test reads the result straight back.
#[derive(Default)]
pub(crate) struct Store {
    pub(crate) members: BTreeMap<MemberId, Vec<u8>>,
    pub(crate) volumes: BTreeMap<VolumeIndex, Vec<u8>>,
    // Ranges of members that never arrived, in offset order: a read across
    // one fails, as the namespace's would, and a repair over one fills it.
    pub(crate) holes: BTreeMap<MemberId, Vec<ByteRange>>,
    pub(crate) reads: usize,
    // The bytes of members read back.
    pub(crate) read_bytes: u64,
    pub(crate) volume_reads: usize,
    pub(crate) writes: usize,
    // A directory lent for work on files, as a namespace that keeps them
    // lends one.
    pub(crate) scratch: Option<PathBuf>,
}

impl MemberAccess for Store {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.reads += 1;
        let Some(bytes) = self.members.get(&member) else {
            return false;
        };
        let holes = self.holes.get(&member).map_or(&[][..], Vec::as_slice);
        if holes.iter().any(|hole| hole.intersect(range).is_some()) {
            return false;
        }
        match bytes.get(range.start as usize..range.end as usize) {
            Some(piece) => {
                self.read_bytes += piece.len() as u64;
                into.extend_from_slice(piece);
                true
            }
            None => false,
        }
    }

    fn scratch(&mut self) -> Option<PathBuf> {
        self.scratch.clone()
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.writes += 1;
        let written = ByteRange::at(offset, bytes.len() as u64);
        if let Some(holes) = self.holes.get_mut(&member) {
            holes.retain(|hole| hole.intersect(written).is_none());
        }
        if let Some(found) = self.members.get_mut(&member) {
            let start = offset as usize;
            if found.len() < start + bytes.len() {
                found.resize(start + bytes.len(), 0);
            }
            found[start..start + bytes.len()].copy_from_slice(bytes);
        }
    }

    fn length(&self, member: MemberId) -> Option<u64> {
        self.members.get(&member).map(|bytes| bytes.len() as u64)
    }

    fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        let mut start = range.start;
        for hole in self.holes.get(&member).map_or(&[][..], Vec::as_slice) {
            let Some(cut) = hole.intersect(range) else {
                continue;
            };
            if cut.start > start {
                out.push(ByteRange::new(start, cut.start));
            }
            start = start.max(cut.end);
        }
        if start < range.end {
            out.push(ByteRange::new(start, range.end));
        }
    }

    fn read_volume(&mut self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.volume_reads += 1;
        let Some(bytes) = self.volumes.get(&volume) else {
            return false;
        };
        let end = (range.end.min(bytes.len() as u64)) as usize;
        match bytes.get(range.start as usize..end) {
            Some(piece) => {
                into.extend_from_slice(piece);
                true
            }
            None => false,
        }
    }
}

// A namespace with a recovery object beside its members, the way the tree
// sets one up: the first recovery file builds the object, later ones are
// claimed as its volumes.
pub(crate) struct Job<O> {
    pub(crate) namespace: Namespace,
    pub(crate) store: Store,
    pub(crate) object: O,
    pub(crate) own: ObjectId,
}

impl<O: Object> Job<O> {
    pub(crate) fn new(object: O, own: ObjectId) -> Self {
        Self {
            namespace: Namespace::new(),
            store: Store::default(),
            object,
            own,
        }
    }

    // A member whose every byte arrived fresh.
    pub(crate) fn member(&mut self, name: &str, bytes: &[u8]) -> MemberId {
        let member = self.namespace.announce(name, Some(bytes.len() as u64));
        self.arrive(member, 0, bytes, Provenance::Fresh);
        self.store.members.insert(member, bytes.to_vec());
        member
    }

    pub(crate) fn arrive(
        &mut self,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
    ) {
        if let Some(record) = self.namespace.get_mut(member) {
            record
                .coverage
                .write(ByteRange::at(offset, bytes.len() as u64), provenance);
        }
        self.namespace
            .keep_first_bytes(member, offset, bytes, provenance);
    }

    // A volume named but not yet arrived, claimed and bound as the tree
    // would: the object says where it goes, and `None` on the first makes
    // it the first.
    pub(crate) fn name(&mut self, name: &str, bytes: &[u8]) -> VolumeIndex {
        let (front, length) = (&bytes[..bytes.len().min(64)], Some(bytes.len() as u64));
        let claimed = self.object.claim(name, front, length);
        let volume = match claimed[..] {
            [volume] => volume,
            [] if self.store.volumes.is_empty() => VolumeIndex(0),
            _ => panic!("claimed"),
        };
        self.object.bound(volume, name, front, length);
        self.store.volumes.insert(volume, bytes.to_vec());
        volume
    }

    // A recovery file, named and then delivered.
    // Delivered in pieces of `chunk` bytes, in the order `order` gives.
    pub(crate) fn volume(
        &mut self,
        name: &str,
        bytes: &[u8],
        chunk: usize,
        reverse: bool,
    ) -> VolumeIndex {
        let member = self.namespace.announce(name, Some(bytes.len() as u64));
        self.arrive(member, 0, bytes, Provenance::Fresh);
        self.namespace.bind(member, self.own);
        let volume = self.name(name, bytes);
        let mut pieces: Vec<(usize, &[u8])> = bytes
            .chunks(chunk)
            .enumerate()
            .map(|(at, piece)| (at * chunk, piece))
            .collect();
        if reverse {
            pieces.reverse();
        }
        let mut out = Vec::new();
        let length = bytes.len() as u64;
        for (offset, piece) in pieces {
            self.object.sized(volume, length, &mut out);
            self.deliver(volume, offset as u64, piece, &mut out);
            self.again(volume, bytes, &mut out);
        }
        self.object.sized(volume, length, &mut out);
        self.object.volume_closed(volume, &mut out);
        self.again(volume, bytes, &mut out);
        volume
    }

    fn deliver(&mut self, volume: VolumeIndex, offset: u64, bytes: &[u8], out: &mut Vec<Emit>) {
        self.object.deliver(
            Delivery {
                volume,
                offset,
                bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            out,
        );
    }

    // What the object asked for again of its volume comes again from
    // `bytes`, as the tree reads it back: forgotten first, then delivered.
    fn again(&mut self, volume: VolumeIndex, bytes: &[u8], out: &mut Vec<Emit>) {
        while let Some(at) = out
            .iter()
            .position(|emit| matches!(emit, Emit::Again { .. }))
        {
            let Emit::Again {
                volume: asked,
                range,
            } = out.remove(at)
            else {
                unreachable!();
            };
            if asked != volume {
                continue;
            }
            self.object.unposted(volume, range);
            let piece = &bytes[range.start as usize..range.end as usize];
            self.deliver(volume, range.start, piece, out);
        }
    }
}
