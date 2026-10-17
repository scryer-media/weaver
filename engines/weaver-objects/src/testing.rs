// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Test support: generated payloads, recovery sets made by the creators, an
// in-memory namespace face that counts its reads, and segment cutting the
// way a namespace cuts them.

pub(crate) mod replay;

use std::collections::BTreeMap;
use std::path::Path;

use weaver_graph::recovery::MemberAccess;
use weaver_graph::{
    ByteRange, Delivery, MemberId, Namespace, Object, ObjectId, Provenance, Segment, VolumeIndex,
};

use crate::grid::crc32;

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

// A PAR2 set over `files`, made by par2-rs's creator: the index first, then
// the recovery volumes.
pub(crate) fn par2_set(
    stem: &str,
    files: &[(&str, &[u8])],
    block: u64,
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
    pub(crate) reads: usize,
    pub(crate) volume_reads: usize,
    pub(crate) writes: usize,
}

impl MemberAccess for Store {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.reads += 1;
        let Some(bytes) = self.members.get(&member) else {
            return false;
        };
        match bytes.get(range.start as usize..range.end as usize) {
            Some(piece) => {
                into.extend_from_slice(piece);
                true
            }
            None => false,
        }
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.writes += 1;
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
        self.namespace.keep_first_bytes(member, offset, bytes);
    }

    // A recovery file: volume 0 if it is the first, claimed otherwise.
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
        let first = self.store.volumes.is_empty();
        let volume = if first {
            VolumeIndex(0)
        } else {
            self.object
                .claim(name, &bytes[..bytes.len().min(64)])
                .expect("claimed")
        };
        self.store.volumes.insert(volume, bytes.to_vec());
        let mut pieces: Vec<(usize, &[u8])> = bytes
            .chunks(chunk)
            .enumerate()
            .map(|(at, piece)| (at * chunk, piece))
            .collect();
        if reverse {
            pieces.reverse();
        }
        let mut out = Vec::new();
        for (offset, piece) in pieces {
            self.object.deliver(
                Delivery {
                    volume,
                    offset: offset as u64,
                    bytes: piece,
                    provenance: Provenance::Fresh,
                },
                &mut out,
            );
        }
        self.object.volume_closed(volume, &mut out);
        volume
    }
}
