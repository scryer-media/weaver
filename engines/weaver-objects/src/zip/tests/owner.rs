// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The store's owner, in miniature: it binds volumes, delivers their bytes,
// holds what the store cannot place yet and hands it back once the store
// places it, answers seeks, and writes members the way a namespace does.
// Every volume is bound up front, in name order, as the tree samples them;
// a spanned set's `.zip` binds as the symbolic last volume. Tests name
// volumes by their position in the set.

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Object, Placement, Provenance, Pull, Seek, VolumeIndex,
};

use super::super::Zip;
use crate::zip_engine::naming::LAST;

#[derive(Debug, Default)]
pub(super) struct Got {
    pub name: String,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
    pub coverage: Coverage,
    pub closed: bool,
    // The CRC32 the object said its headers state.
    pub crc32: Option<u32>,
}

pub(super) struct Owner {
    pub object: Zip,
    pub volumes: Vec<(String, Vec<u8>)>,
    pub members: BTreeMap<u32, Got>,
    // Everything said but members and their bytes.
    pub said: Vec<Emit>,
    // What the object wants as of the last round.
    pub seeks: Vec<Seek>,
    // How seeks are answered; a range moved ahead is delivered at once.
    pub pull: Pull,
    // Held pieces, with the bytes they arrived with.
    held: Vec<(VolumeIndex, ByteRange, Provenance, Vec<u8>)>,
    bound: Vec<bool>,
    // The volume each position binds as.
    index: Vec<VolumeIndex>,
    next: u32,
    // What each volume was delivered, by provenance, as its member keeps it.
    delivered: BTreeMap<u32, Coverage>,
    // What came from outside, by volume: delivered, or a seek's answer.
    fetched: BTreeMap<u32, Coverage>,
    // Seeks answered over bytes that had come already.
    pub refetched: Vec<(VolumeIndex, ByteRange)>,
    // Runs read back and delivered again.
    pub replays: usize,
}

// What is held of `range` of `volume`, read the way the owner reads a held
// placement back: every byte of it, or nothing.
fn read_held(
    held: &[(VolumeIndex, ByteRange, Provenance, Vec<u8>)],
    volume: VolumeIndex,
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    let mut pieces: Vec<_> = held
        .iter()
        .filter(|(at, piece, _, _)| *at == volume && piece.intersect(range).is_some())
        .collect();
    pieces.sort_by_key(|(_, piece, _, _)| piece.start);
    let start = into.len();
    let mut cursor = range.start;
    for (_, piece, _, bytes) in pieces {
        if piece.start > cursor {
            break;
        }
        let end = piece.end.min(range.end);
        if end > cursor {
            let from = (cursor - piece.start) as usize;
            into.extend_from_slice(&bytes[from..(end - piece.start) as usize]);
            cursor = end;
        }
    }
    if cursor < range.end {
        into.truncate(start);
        return false;
    }
    true
}

impl Owner {
    pub fn new(object: Zip, volumes: Vec<(String, Vec<u8>)>) -> Self {
        let count = volumes.len();
        let index = volumes
            .iter()
            .enumerate()
            .map(|(position, (name, _))| match position {
                0 => VolumeIndex(0),
                _ if name.to_ascii_lowercase().ends_with(".zip") => VolumeIndex(LAST),
                _ => VolumeIndex(position as u32),
            })
            .collect();
        let mut owner = Self {
            object,
            volumes,
            members: BTreeMap::new(),
            said: Vec::new(),
            seeks: Vec::new(),
            pull: Pull::Moved(1),
            held: Vec::new(),
            bound: vec![false; count],
            index,
            next: 0,
            delivered: BTreeMap::new(),
            fetched: BTreeMap::new(),
            refetched: Vec::new(),
            replays: 0,
        };
        let name = owner.volumes[0].0.clone();
        owner.object.bound(VolumeIndex(0), &name, &[], None);
        owner.bound[0] = true;
        owner.size(0);
        for volume in 1..count {
            owner.bind(volume);
        }
        owner
    }

    pub fn single(object: Zip, bytes: Vec<u8>) -> Self {
        Self::new(object, vec![("alpha.zip".to_owned(), bytes)])
    }

    fn position(&self, volume: VolumeIndex) -> usize {
        self.index
            .iter()
            .position(|at| *at == volume)
            .expect("a volume bound")
    }

    fn size(&mut self, volume: usize) {
        let length = self.volumes[volume].1.len() as u64;
        let mut out = Vec::new();
        self.object.sized(self.index[volume], length, &mut out);
        self.apply(out);
    }

    // A later volume is claimed by its name and first bytes, as the tree
    // binds it when they arrive.
    fn bind(&mut self, volume: usize) {
        if self.bound[volume] {
            return;
        }
        let (name, bytes) = &self.volumes[volume];
        let first = &bytes[..bytes.len().min(32)];
        assert_eq!(
            self.object.claim(name, first, None),
            vec![self.index[volume]]
        );
        let name = name.clone();
        self.object.bound(self.index[volume], &name, &[], None);
        self.bound[volume] = true;
        self.size(volume);
    }

    pub fn deliver(&mut self, volume: usize, range: ByteRange) {
        self.deliver_as(volume, range, Provenance::Fresh, None);
    }

    // Deliver a range, optionally with other bytes than the volume's.
    pub fn deliver_as(
        &mut self,
        volume: usize,
        range: ByteRange,
        provenance: Provenance,
        bytes: Option<&[u8]>,
    ) {
        self.bind(volume);
        self.fetch(self.index[volume], range);
        self.push(self.index[volume], range, provenance, bytes);
        self.settle();
    }

    // Bytes from outside.
    fn fetch(&mut self, volume: VolumeIndex, range: ByteRange) {
        self.fetched
            .entry(volume.0)
            .or_default()
            .write(range, Provenance::Fresh);
    }

    // A seek over a range that had come already asks for it twice.
    fn asked(&mut self, seek: &Seek) {
        if self
            .fetched
            .get(&seek.volume.0)
            .is_some_and(|fetched| fetched.overlapping(seek.range).next().is_some())
        {
            self.refetched.push((seek.volume, seek.range));
        }
    }

    fn push(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        provenance: Provenance,
        bytes: Option<&[u8]>,
    ) {
        if range.is_empty() {
            return;
        }
        let position = self.position(volume);
        let real = &self.volumes[position].1[range.start as usize..range.end as usize];
        let bytes = bytes.unwrap_or(real).to_vec();
        self.delivered
            .entry(volume.0)
            .or_default()
            .write(range, provenance);
        let mut out = Vec::new();
        let held = &self.held;
        self.object.deliver(
            Delivery {
                volume,
                offset: range.start,
                bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
                provenance,
            },
            &mut |placement, into| match placement {
                Placement::Held { range } => read_held(held, volume, *range, into),
                _ => false,
            },
            &mut out,
        );
        self.apply(out);
        let offset = range.start;
        let mut placements = Vec::new();
        self.object.locate(volume, range, &mut placements);
        let mut at = range.start;
        for placement in &placements {
            if let Placement::Held { range } = placement {
                assert_eq!(range.start, at, "held pieces are in volume order");
                let at = (range.start - offset) as usize;
                let piece = bytes[at..at + range.len() as usize].to_vec();
                self.held.push((volume, *range, provenance, piece));
            }
            at += placement.len();
        }
        assert_eq!(at, range.end, "locate accounts for every byte");
    }

    // Answer seeks and hand back held bytes until nothing changes.
    pub fn settle(&mut self) {
        loop {
            let mut changed = false;
            // The object restates its wants every round; only a change in
            // them is news.
            let mut seeks = Vec::new();
            self.object.wants(&mut seeks);
            changed |= seeks != self.seeks;
            self.seeks.clone_from(&seeks);
            for seek in seeks {
                self.asked(&seek);
                let mut out = Vec::new();
                self.object.pulled(seek, self.pull, &mut out);
                self.apply(out);
                if matches!(self.pull, Pull::Moved(moved) if moved > 0) {
                    self.fetch(seek.volume, seek.range);
                    self.push(seek.volume, seek.range, Provenance::Fresh, None);
                }
            }
            for (volume, range, provenance, bytes) in std::mem::take(&mut self.held) {
                let mut placements = Vec::new();
                self.object.locate(volume, range, &mut placements);
                if placements
                    .iter()
                    .all(|placement| matches!(placement, Placement::Held { .. }))
                {
                    self.held.push((volume, range, provenance, bytes));
                } else {
                    changed = true;
                    self.push(volume, range, provenance, Some(&bytes));
                }
            }
            if !changed {
                break;
            }
        }
    }

    pub fn frames(&mut self, volume: usize, frame: usize) {
        let length = self.volumes[volume].1.len() as u64;
        let mut start = 0;
        while start < length {
            let end = (start + frame as u64).min(length);
            self.deliver(volume, ByteRange::new(start, end));
            start = end;
        }
    }

    pub fn close(&mut self, volume: usize) {
        self.bind(volume);
        let mut out = Vec::new();
        self.object.volume_closed(self.index[volume], &mut out);
        self.apply(out);
        self.settle();
    }

    pub fn whole(&mut self, frame: usize) {
        for volume in 0..self.volumes.len() {
            self.frames(volume, frame);
        }
        for volume in 0..self.volumes.len() {
            self.close(volume);
        }
    }

    pub fn held(&self) -> usize {
        self.held.len()
    }

    fn apply(&mut self, out: Vec<Emit>) {
        let mut again = Vec::new();
        for emit in out {
            match emit {
                Emit::Again { volume, range } => {
                    self.said.push(Emit::Again { volume, range });
                    again.push((volume, range));
                }
                Emit::Member {
                    member,
                    name,
                    length,
                } => {
                    // A new member takes the next id; one retracted is
                    // announced again under its own.
                    assert!(member.0 <= self.next, "members are announced in id order");
                    assert!(!self.members.contains_key(&member.0), "announced once");
                    self.next = self.next.max(member.0 + 1);
                    self.members.insert(
                        member.0,
                        Got {
                            name,
                            length,
                            ..Got::default()
                        },
                    );
                }
                Emit::Bytes {
                    member,
                    offset,
                    bytes,
                    provenance,
                } => {
                    let got = self.members.get_mut(&member.0).expect("announced first");
                    let end = offset as usize + bytes.len();
                    if got.bytes.len() < end {
                        got.bytes.resize(end, 0);
                    }
                    got.bytes[offset as usize..end].copy_from_slice(&bytes);
                    got.coverage
                        .write(ByteRange::at(offset, bytes.len() as u64), provenance);
                }
                Emit::MemberClosed(member) => {
                    let got = self.members.get_mut(&member.0).expect("announced first");
                    assert!(!got.closed, "closed once");
                    got.closed = true;
                }
                Emit::Checksum { member, crc32 } => {
                    let got = self.members.get_mut(&member.0).expect("announced first");
                    got.crc32 = Some(crc32);
                }
                Emit::Retract(member) => {
                    self.members.remove(&member.0);
                    self.said.push(Emit::Retract(member));
                }
                other => self.said.push(other),
            }
        }
        if !again.is_empty() {
            self.again(&again);
        }
    }

    // Ranges asked for again, as the tree answers them: what of each was
    // delivered is read back, every range while the store still stands as
    // it said them all, every member they reached is open again, and the
    // bytes come again as they came.
    fn again(&mut self, ranges: &[(VolumeIndex, ByteRange)]) {
        let mut read_back = Vec::new();
        for &(volume, range) in ranges {
            let runs: Vec<(ByteRange, Provenance)> = self
                .delivered
                .get(&volume.0)
                .map(|coverage| {
                    coverage
                        .overlapping(range)
                        .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                        .collect()
                })
                .unwrap_or_default();
            for (piece, provenance) in runs {
                let mut bytes = Vec::new();
                if self.read_posted(volume, piece, &mut bytes) {
                    read_back.push((volume, piece, provenance, bytes));
                }
            }
        }
        for (volume, piece, _, _) in &read_back {
            let mut placements = Vec::new();
            self.object.unposted(*volume, *piece);
            self.object.locate(*volume, *piece, &mut placements);
            for placement in placements {
                if let Placement::Member { member, range } = placement
                    && let Some(got) = self.members.get_mut(&member.0)
                {
                    got.coverage.erase(range);
                    got.closed = false;
                }
            }
        }
        for (volume, piece, provenance, bytes) in read_back {
            self.replays += 1;
            self.push(volume, piece, provenance, Some(&bytes));
        }
    }

    // The bytes a range of a volume was posted with, rebuilt from the
    // members, the store's envelope and what is held.
    fn read_posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let mut read = |placement: &Placement, into: &mut Vec<u8>| -> bool {
            match placement {
                Placement::Member { member, range } => {
                    let Some(got) = self.members.get(&member.0) else {
                        return false;
                    };
                    if !got.coverage.covered(*range) {
                        return false;
                    }
                    into.extend_from_slice(&got.bytes[range.start as usize..range.end as usize]);
                    true
                }
                Placement::Envelope { envelope, range } => {
                    match self.object.envelope(*envelope, *range) {
                        Some(bytes) => {
                            into.extend_from_slice(&bytes);
                            true
                        }
                        None => false,
                    }
                }
                Placement::Held { range } => read_held(&self.held, volume, *range, into),
            }
        };
        self.object.posted(volume, range, &mut read, into)
    }

    // A member by name.
    pub fn member(&self, name: &str) -> &Got {
        self.members
            .values()
            .find(|got| got.name == name)
            .unwrap_or_else(|| panic!("no member {name}"))
    }

    // The bytes a volume was posted with, rebuilt from the members and the
    // store's envelope.
    pub fn posted(&self, volume: usize) -> Option<Vec<u8>> {
        let length = self.volumes[volume].1.len() as u64;
        let mut into = Vec::new();
        self.read_posted(self.index[volume], ByteRange::new(0, length), &mut into)
            .then_some(into)
    }
}
