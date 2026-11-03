// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The store's owner, in miniature: it binds volumes, delivers their bytes,
// holds what the store cannot place yet and hands it back once the store
// places it, answers seeks, and writes members the way a namespace does.

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Object, Placement, Provenance, Pull, Seek, Standing,
    VolumeIndex,
};

use super::super::SevenZipStore;

#[derive(Debug, Default)]
pub(super) struct Got {
    pub name: String,
    pub length: Option<u64>,
    pub bytes: Vec<u8>,
    pub coverage: Coverage,
    pub closed: bool,
}

pub(super) struct Owner {
    pub object: SevenZipStore,
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
    // What each volume was delivered, by provenance, as its member keeps it.
    delivered: BTreeMap<u32, Coverage>,
    bound: Vec<bool>,
    next: u32,
    // What came from outside, by volume: delivered, or a seek's answer.
    fetched: BTreeMap<u32, Coverage>,
    // Seeks answered over bytes that had come already.
    pub refetched: Vec<(VolumeIndex, ByteRange)>,
    // Runs read back and delivered again.
    pub replays: usize,
}

impl Owner {
    pub fn new(object: SevenZipStore, volumes: Vec<(String, Vec<u8>)>) -> Self {
        let count = volumes.len();
        let mut owner = Self {
            object,
            volumes,
            members: BTreeMap::new(),
            said: Vec::new(),
            seeks: Vec::new(),
            pull: Pull::Moved(1),
            held: Vec::new(),
            delivered: BTreeMap::new(),
            bound: vec![false; count],
            next: 0,
            fetched: BTreeMap::new(),
            refetched: Vec::new(),
            replays: 0,
        };
        let (name, bytes) = &owner.volumes[0];
        let (front, length) = (&bytes[..bytes.len().min(32)], Some(bytes.len() as u64));
        owner.object.bound(VolumeIndex(0), name, front, length);
        owner.bound[0] = true;
        owner.size(0);
        owner
    }

    pub fn single(object: SevenZipStore, bytes: Vec<u8>) -> Self {
        Self::new(object, vec![("alpha.7z".to_owned(), bytes)])
    }

    fn size(&mut self, volume: usize) {
        let length = self.volumes[volume].1.len() as u64;
        let mut out = Vec::new();
        self.object
            .sized(VolumeIndex(volume as u32), length, &mut out);
        self.apply(out);
    }

    // A later volume is claimed by its name, first bytes and length, as the
    // tree binds it when they arrive.
    fn bind(&mut self, volume: usize) {
        if self.bound[volume] {
            return;
        }
        let (name, bytes) = &self.volumes[volume];
        let (first, length) = (&bytes[..bytes.len().min(32)], Some(bytes.len() as u64));
        let index = VolumeIndex(volume as u32);
        assert_eq!(self.object.claim(name, first, length), vec![index]);
        self.object.bound(index, name, first, length);
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
        self.fetch(VolumeIndex(volume as u32), range);
        self.push(VolumeIndex(volume as u32), range, provenance, bytes);
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
        let real = &self.volumes[volume.0 as usize].1[range.start as usize..range.end as usize];
        let bytes = bytes.unwrap_or(real).to_vec();
        self.delivered
            .entry(volume.0)
            .or_default()
            .write(range, provenance);
        let mut out = Vec::new();
        let members = &self.members;
        let mut read =
            |placement: &Placement, into: &mut Vec<u8>| read_member(members, placement, into);
        self.object.deliver(
            Delivery {
                volume,
                offset: range.start,
                bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
                provenance,
            },
            &mut read,
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
            fn asking(&self) -> bool {
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

    // A range of a volume a restart could not read back, as the tree
    // uncovers it: the members it reached are not covered there and are
    // open, and the store forgets what it said of it.
    pub fn uncover(&mut self, volume: usize, range: ByteRange) {
        let volume = VolumeIndex(volume as u32);
        let mut placements = Vec::new();
        self.object.locate(volume, range, &mut placements);
        for placement in placements {
            if let Placement::Member { member, range } = placement
                && let Some(got) = self.members.get_mut(&member.0)
            {
                got.coverage.erase(range);
                got.closed = false;
            }
        }
        if let Some(delivered) = self.delivered.get_mut(&volume.0) {
            delivered.erase(range);
        }
        self.object.unposted(volume, range);
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
                let (members, object) = (&self.members, &self.object);
                let mut read = |placement: &Placement, into: &mut Vec<u8>| {
                    read_posted(members, object, placement, into)
                };
                let mut bytes = Vec::new();
                if self.object.posted(volume, piece, &mut read, &mut bytes) {
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
        let mut read = |placement: &Placement, into: &mut Vec<u8>| -> bool {
            read_posted(&self.members, &self.object, placement, into)
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

// What a placement holds: a member's bytes or the store's envelope.
fn read_posted(
    members: &BTreeMap<u32, Got>,
    object: &SevenZipStore,
    placement: &Placement,
    into: &mut Vec<u8>,
) -> bool {
    match placement {
        Placement::Member { .. } => read_member(members, placement, into),
        Placement::Envelope { envelope, range } => match object.envelope(*envelope, *range) {
            Some(bytes) => {
                into.extend_from_slice(&bytes);
                true
            }
            None => false,
        },
        Placement::Held { .. } => false,
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
