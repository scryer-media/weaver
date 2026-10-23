// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The store's owner, in miniature: it binds volumes, delivers their bytes,
// holds what the store cannot place yet and hands it back once the store
// places it, keeps envelope bytes as they pass the way a namespace's shelf
// does and holds them again when the store says they are no longer its own,
// answers seeks, and writes members the way a namespace does.

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Object, Placement, Provenance, Pull, Seek, Standing,
    VolumeIndex,
};

use super::super::RarStore;

// How much of a member's front a claim sees here.
const FRONT: usize = 4096;

#[derive(Debug, Default)]
pub(super) struct Got {
    pub name: String,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
    pub coverage: Coverage,
    pub closed: bool,
}

#[derive(Debug, Default)]
struct Shelf {
    bytes: Vec<u8>,
    coverage: Coverage,
}

pub(super) struct Owner {
    pub object: RarStore,
    pub volumes: Vec<(String, Vec<u8>)>,
    pub members: BTreeMap<u32, Got>,
    // Everything said but members and their bytes.
    pub said: Vec<Emit>,
    // What the object wants as of the last round.
    pub seeks: Vec<Seek>,
    // How seeks are answered; a range moved ahead is delivered at once.
    pub pull: Pull,
    held: Vec<(VolumeIndex, ByteRange, Provenance, Vec<u8>)>,
    shelves: BTreeMap<u32, Shelf>,
    bound: Vec<bool>,
    next: u32,
}

impl Owner {
    pub fn new(object: RarStore, volumes: Vec<(String, Vec<u8>)>) -> Self {
        Self::founded(object, volumes, 0)
    }

    // Built for the volume at `first`, the first to arrive. The store is
    // asked where that volume goes, as the tree asks an object it builds.
    pub fn founded(object: RarStore, volumes: Vec<(String, Vec<u8>)>, first: usize) -> Self {
        let count = volumes.len();
        let mut owner = Self {
            object,
            volumes,
            members: BTreeMap::new(),
            said: Vec::new(),
            seeks: Vec::new(),
            // Seeks are answered by the normal order unless a test says.
            pull: Pull::Moved(0),
            held: Vec::new(),
            shelves: BTreeMap::new(),
            bound: vec![false; count],
            next: 0,
        };
        let (name, bytes) = &owner.volumes[first];
        let front = &bytes[..bytes.len().min(FRONT)];
        let volume = owner
            .object
            .claim(name, front, None)
            .unwrap_or(VolumeIndex(0));
        assert_eq!(volume, VolumeIndex(first as u32), "founds on {name}");
        let name = name.clone();
        owner.object.bound(volume, &name, &[], None);
        owner.bound[first] = true;
        owner.size(first);
        owner
    }

    // Volumes named in their set's own numbering.
    pub fn named(object: RarStore, volumes: Vec<Vec<u8>>) -> Self {
        let count = volumes.len();
        let named = volumes
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                let name = match count {
                    1 => "alpha.rar".to_owned(),
                    _ => format!("alpha.part{}.rar", index + 1),
                };
                (name, bytes)
            })
            .collect();
        Self::new(object, named)
    }

    fn size(&mut self, volume: usize) {
        let length = self.volumes[volume].1.len() as u64;
        let mut out = Vec::new();
        self.object
            .sized(VolumeIndex(volume as u32), length, &mut out);
        self.apply(out);
    }

    // A later volume is claimed by its name and first bytes, as the tree
    // binds it when they arrive.
    pub fn bind(&mut self, volume: usize) {
        if self.bound[volume] {
            return;
        }
        let (name, bytes) = &self.volumes[volume];
        let first = &bytes[..bytes.len().min(FRONT)];
        assert_eq!(
            self.object.claim(name, first, None),
            Some(VolumeIndex(volume as u32)),
            "claims {name}"
        );
        let name = name.clone();
        self.object
            .bound(VolumeIndex(volume as u32), &name, &[], None);
        self.bound[volume] = true;
        self.size(volume);
        self.settle();
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
        self.push(VolumeIndex(volume as u32), range, provenance, bytes);
        self.settle();
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
        let real = &self.volumes[volume.0 as usize].1[range.start as usize..range.end as usize];
        let bytes = bytes.unwrap_or(real).to_vec();
        let mut out = Vec::new();
        let members = &self.members;
        let mut read =
            |placement: &Placement, into: &mut Vec<u8>| read_member(members, placement, into);
        self.object.deliver(
            Delivery {
                volume,
                offset: range.start,
                bytes: &bytes,
                provenance,
            },
            &mut read,
            &mut out,
        );
        self.apply(out);
        let mut placements = Vec::new();
        self.object.locate(volume, range, &mut placements);
        let mut at = range.start;
        for placement in &placements {
            let piece =
                &bytes[(at - range.start) as usize..(at - range.start + placement.len()) as usize];
            match placement {
                Placement::Held { range } => {
                    assert_eq!(range.start, at, "held pieces are in volume order");
                    self.held.push((volume, *range, provenance, piece.to_vec()));
                }
                Placement::Envelope { envelope, range } => {
                    let shelf = self.shelves.entry(*envelope).or_default();
                    let end = range.end as usize;
                    if shelf.bytes.len() < end {
                        shelf.bytes.resize(end, 0);
                    }
                    shelf.bytes[range.start as usize..end].copy_from_slice(piece);
                    shelf.coverage.write(*range, provenance);
                }
                Placement::Member { .. } => {}
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
                let mut out = Vec::new();
                self.object.pulled(seek, self.pull, &mut out);
                self.apply(out);
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

    // A volume's frames from its tail to its front.
    pub fn backwards(&mut self, volume: usize, frame: usize) {
        let length = self.volumes[volume].1.len() as u64;
        let mut end = length;
        while end > 0 {
            let start = end.saturating_sub(frame as u64);
            self.deliver(volume, ByteRange::new(start, end));
            end = start;
        }
    }

    pub fn close(&mut self, volume: usize) {
        self.bind(volume);
        let mut out = Vec::new();
        self.object
            .volume_closed(VolumeIndex(volume as u32), &mut out);
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

    // Nothing more will arrive, and every member stands whole and placed.
    pub fn quiet(&mut self) {
        struct Whole;
        impl Standing for Whole {
            fn damaged(&self) -> bool {
                false
            }
            fn unplaced(&self) -> bool {
                false
            }
            fn lost(&self) -> bool {
                false
            }
            fn beside(&self) -> Vec<weaver_graph::Beside<'_>> {
                Vec::new()
            }
        }
        let mut out = Vec::new();
        self.object.quiet(&Whole, &mut out);
        self.apply(out);
    }

    pub fn offer(&mut self, secret: &str) {
        let mut out = Vec::new();
        self.object.offer_password(secret, &mut out);
        self.apply(out);
        self.settle();
    }

    pub fn held(&self) -> usize {
        self.held.len()
    }

    fn apply(&mut self, out: Vec<Emit>) {
        for emit in out {
            match emit {
                Emit::Member {
                    member,
                    name,
                    length,
                } => {
                    assert_eq!(member.0, self.next, "members are announced in id order");
                    self.next += 1;
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
                Emit::Retract(member) => {
                    self.members.remove(&member.0);
                    self.next = self.next.min(member.0);
                    self.said.push(Emit::Retract(member));
                }
                Emit::Unkept {
                    volume,
                    range,
                    envelope,
                    at,
                } => {
                    // Read back from the shelf they were kept on, and held
                    // until the store places them again.
                    let shelf = &self.shelves[&envelope];
                    let kept = ByteRange::at(at, range.len());
                    for run in shelf.coverage.overlapping(kept) {
                        let piece = run.range.intersect(kept).expect("overlapping");
                        let bytes = shelf.bytes[piece.start as usize..piece.end as usize].to_vec();
                        let start = range.start + (piece.start - at);
                        self.held.push((
                            volume,
                            ByteRange::at(start, piece.len()),
                            run.provenance,
                            bytes,
                        ));
                    }
                    self.said.push(emit);
                }
                other => self.said.push(other),
            }
        }
    }

    // A member by name.
    pub fn member(&self, name: &str) -> &Got {
        self.members
            .values()
            .find(|got| got.name == name)
            .unwrap_or_else(|| panic!("no member {name}"))
    }

    // The bytes a volume was posted with, rebuilt from the members and the
    // envelope bytes kept as they passed.
    pub fn posted(&self, volume: usize) -> Option<Vec<u8>> {
        let length = self.volumes[volume].1.len() as u64;
        let mut read = |placement: &Placement, into: &mut Vec<u8>| -> bool {
            match placement {
                Placement::Member { .. } => read_member(&self.members, placement, into),
                Placement::Envelope { envelope, range } => {
                    let Some(shelf) = self.shelves.get(envelope) else {
                        return false;
                    };
                    if !shelf.coverage.covered(*range) {
                        return false;
                    }
                    into.extend_from_slice(&shelf.bytes[range.start as usize..range.end as usize]);
                    true
                }
                Placement::Held { .. } => false,
            }
        };
        let mut into = Vec::new();
        self.object
            .posted(
                VolumeIndex(volume as u32),
                ByteRange::new(0, length),
                &mut read,
                &mut into,
            )
            .then_some(into)
    }
}

// What a member placement holds, read back as a namespace reads it: only the
// bytes that arrived.
fn read_member(members: &BTreeMap<u32, Got>, placement: &Placement, into: &mut Vec<u8>) -> bool {
    let Placement::Member { member, range } = placement else {
        return false;
    };
    let Some(got) = members.get(&member.0) else {
        return false;
    };
    got.coverage.covered(*range) && {
        into.extend_from_slice(&got.bytes[range.start as usize..range.end as usize]);
        true
    }
}
