// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A checkpoint taken mid-stream, the way a restart takes one: the live
// object goes on, and a copy restored from its facts takes the same steps
// beside it. Each has an owner that holds what it is told to hold and hands
// it back once it is placed; the restored one starts with the live one's
// holds, as a restart fetches them again.

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Facts, MemberId, Object, Placement, Provenance,
    VolumeIndex,
};

pub(crate) enum Step<'a> {
    Bound(u32, &'a str),
    Sized(u32, u64),
    Bytes(u32, u64, &'a [u8]),
    Closed(u32),
    Password(&'a str),
    // Bytes the object placed before the checkpoint, delivered again as a
    // restart replays its partials: what it holds now the owner already
    // holds, so nothing here is held again.
    Replay(u32, u64, &'a [u8]),
}

// The replay of every delivery among `steps`, as a restart makes it.
pub(crate) fn replay<'a>(steps: &[Step<'a>]) -> Vec<Step<'a>> {
    steps
        .iter()
        .filter_map(|step| match *step {
            Step::Bytes(volume, offset, bytes) => Some(Step::Replay(volume, offset, bytes)),
            _ => None,
        })
        .collect()
}

// One volume in frames, in order.
pub(crate) fn frames(volume: u32, bytes: &[u8], frame: usize) -> Vec<Step<'_>> {
    bytes
        .chunks(frame)
        .enumerate()
        .map(|(index, chunk)| Step::Bytes(volume, (index * frame) as u64, chunk))
        .collect()
}

#[derive(Clone, Default)]
struct Owner {
    held: Vec<(VolumeIndex, u64, Vec<u8>)>,
    placed: Vec<(VolumeIndex, ByteRange)>,
    // How far each member is known to reach.
    members: BTreeMap<MemberId, u64>,
    // What each member was written, read back as a namespace reads it.
    written: BTreeMap<MemberId, (Vec<u8>, Coverage)>,
}

impl Owner {
    fn step(&mut self, object: &mut dyn Object, step: &Step<'_>, out: &mut Vec<Emit>) {
        match *step {
            Step::Bound(volume, name) => object.bound(VolumeIndex(volume), name, &[], None),
            Step::Sized(volume, length) => object.sized(VolumeIndex(volume), length, out),
            Step::Bytes(volume, offset, bytes) => {
                self.deliver(object, VolumeIndex(volume), offset, bytes, out);
            }
            Step::Closed(volume) => object.volume_closed(VolumeIndex(volume), out),
            Step::Password(secret) => object.offer_password(secret, out),
            Step::Replay(volume, offset, bytes) => object.deliver(
                Delivery {
                    volume: VolumeIndex(volume),
                    offset,
                    bytes,
                    provenance: Provenance::Fresh,
                },
                &mut |placement, into| read(&self.written, placement, into),
                out,
            ),
        }
        self.release(object, out);
        for emit in out.iter() {
            let (member, end) = match emit {
                Emit::Member { member, length, .. } => (*member, length.unwrap_or(0)),
                Emit::Bytes {
                    member,
                    offset,
                    bytes,
                    ..
                } => (*member, offset + bytes.len() as u64),
                _ => continue,
            };
            let reach = self.members.entry(member).or_default();
            *reach = (*reach).max(end);
            if let Emit::Bytes {
                offset,
                bytes,
                provenance,
                ..
            } = emit
            {
                let (written, coverage) = self.written.entry(member).or_default();
                let (start, end) = (*offset as usize, end as usize);
                written.resize(written.len().max(end), 0);
                written[start..end].copy_from_slice(bytes);
                coverage.write(ByteRange::at(*offset, bytes.len() as u64), *provenance);
            }
        }
    }

    fn deliver(
        &mut self,
        object: &mut dyn Object,
        volume: VolumeIndex,
        offset: u64,
        bytes: &[u8],
        out: &mut Vec<Emit>,
    ) {
        object.deliver(
            Delivery {
                volume,
                offset,
                bytes,
                provenance: Provenance::Fresh,
            },
            &mut |placement, into| read(&self.written, placement, into),
            out,
        );
        let range = ByteRange::at(offset, bytes.len() as u64);
        self.placed.push((volume, range));
        let mut placements = Vec::new();
        object.locate(volume, range, &mut placements);
        for placement in placements {
            if let Placement::Held { range } = placement {
                let from = (range.start - offset) as usize;
                let held = bytes[from..from + range.len() as usize].to_vec();
                self.held.push((volume, range.start, held));
            }
        }
    }

    fn release(&mut self, object: &mut dyn Object, out: &mut Vec<Emit>) {
        loop {
            let placed = self.held.iter().position(|(volume, start, bytes)| {
                let mut placements = Vec::new();
                object.locate(
                    *volume,
                    ByteRange::at(*start, bytes.len() as u64),
                    &mut placements,
                );
                !placements
                    .iter()
                    .any(|placement| matches!(placement, Placement::Held { .. }))
            });
            let Some(index) = placed else {
                return;
            };
            let (volume, start, bytes) = self.held.remove(index);
            self.deliver(object, volume, start, &bytes, out);
        }
    }

    // Both objects place every range the live one was given, and lay every
    // member it said, the same way.
    fn alike(&self, live: &dyn Object, restored: &dyn Object) {
        for (volume, range) in &self.placed {
            let (mut ours, mut theirs) = (Vec::new(), Vec::new());
            live.locate(*volume, *range, &mut ours);
            restored.locate(*volume, *range, &mut theirs);
            assert_eq!(ours, theirs, "{volume} {range}");
        }
        for (member, end) in &self.members {
            let (mut ours, mut theirs): (Vec<Extent>, Vec<Extent>) = (Vec::new(), Vec::new());
            live.extent(*member, ByteRange::new(0, *end), &mut ours);
            restored.extent(*member, ByteRange::new(0, *end), &mut theirs);
            assert_eq!(ours, theirs, "member {}", member.0);
        }
    }
}

// What a member placement holds: only the bytes written to it.
fn read(
    written: &BTreeMap<MemberId, (Vec<u8>, Coverage)>,
    placement: &Placement,
    into: &mut Vec<u8>,
) -> bool {
    let Placement::Member { member, range } = placement else {
        return false;
    };
    let Some((bytes, coverage)) = written.get(member) else {
        return false;
    };
    coverage.covered(*range) && {
        into.extend_from_slice(&bytes[range.start as usize..range.end as usize]);
        true
    }
}

// Run `steps` to `cut` on the live object, restore a copy from its facts
// and give it `again` (a password offered anew), then check that the two
// place alike, and take the rest of the steps with the same emits, ending
// with the same facts.
pub(crate) fn restores_alike(
    mut live: Box<dyn Object>,
    steps: &[Step<'_>],
    cut: usize,
    again: &[Step<'_>],
) -> (Facts, Box<dyn Object>, Box<dyn Object>) {
    let mut owner = Owner::default();
    let mut said = Vec::new();
    for step in &steps[..cut] {
        owner.step(live.as_mut(), step, &mut said);
    }
    let facts = live.facts();
    assert_eq!(
        (facts.kind, facts.version > Facts::UNSUPPORTED),
        (live.kind(), true)
    );
    let mut restored =
        crate::restore(&facts, weaver_graph::Budget::default()).expect("the facts restore");
    assert_eq!(restored.kind(), live.kind());
    let mut theirs = owner.clone();
    for step in again {
        theirs.step(restored.as_mut(), step, &mut Vec::new());
    }
    owner.alike(live.as_ref(), restored.as_ref());
    for step in &steps[cut..] {
        let (mut ours, mut its) = (Vec::new(), Vec::new());
        owner.step(live.as_mut(), step, &mut ours);
        theirs.step(restored.as_mut(), step, &mut its);
        assert_eq!(ours, its);
    }
    owner.alike(live.as_ref(), restored.as_ref());
    assert_eq!(live.facts(), restored.facts());
    (facts, live, restored)
}

// Whether `needle` appears anywhere in `blob`.
pub(crate) fn contains(blob: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && blob.windows(needle.len()).any(|window| window == needle)
}
