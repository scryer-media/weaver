// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a store places a volume and where it holds one misplaced are judged
// on the same facts, so a volume it gives back is never claimed again where
// it would be given back once more. Each post is run in a tree whose owner
// hands every bound volume over as the server does, and the hand-overs are
// counted: a placement that undid itself would hand the same volume over
// without end.

use bytes::Bytes;
use weaver_graph::{Effect, MemberId, NodeId, Posted, Provenance, Tree, VolumeIndex, Why};

use super::writer::{Item, Options, Version, write};
use super::{find, items, split};
use crate::Objects;
use crate::grid::crc32;
use crate::testing::payload;

// More hand-overs than any of these posts makes: a run that reaches it is
// one that never settles.
const ENOUGH: usize = 64;

struct Run {
    tree: Tree,
    effects: Vec<Effect>,
    // Where the owner has handed over to: effects before it are done.
    done: usize,
    handed: usize,
}

impl Run {
    fn new() -> Self {
        Self {
            tree: Tree::new(),
            effects: Vec::new(),
            done: 0,
            handed: 0,
        }
    }

    fn announce(&mut self, name: &str, length: usize) -> MemberId {
        self.tree
            .announce_root(name, Some(length as u64), &mut self.effects)
    }

    fn deliver(&mut self, member: MemberId, offset: usize, bytes: &[u8]) {
        self.tree.deliver(
            NodeId::ROOT,
            member,
            Posted {
                offset: offset as u64,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut Objects::default(),
            &mut self.effects,
        );
        self.hand_over();
    }

    fn close(&mut self, member: MemberId) {
        self.tree.close(
            NodeId::ROOT,
            member,
            &mut Objects::default(),
            &mut self.effects,
        );
        self.hand_over();
    }

    // The owner hands each member bound since over to its object, as the
    // server's namespace does on every `Bound`, until no new one comes.
    fn hand_over(&mut self) {
        while self.handed < ENOUGH {
            let Some(member) = self.effects[self.done..]
                .iter()
                .find_map(|effect| match effect {
                    Effect::Bound {
                        node: NodeId::ROOT,
                        member,
                        ..
                    } => Some(*member),
                    _ => None,
                })
            else {
                self.done = self.effects.len();
                return;
            };
            let at = self.effects[self.done..]
                .iter()
                .position(
                    |effect| matches!(effect, Effect::Bound { member: m, .. } if *m == member),
                )
                .expect("the bound effect");
            self.done += at + 1;
            self.handed += 1;
            self.tree.handed_over(
                NodeId::ROOT,
                member,
                &mut Objects::default(),
                &mut self.effects,
            );
        }
    }

    fn given_back(&self, member: MemberId) -> usize {
        self.effects
            .iter()
            .filter(
                |effect| matches!(effect, Effect::GivenBack { member: at, .. } if *at == member),
            )
            .count()
    }

    fn at(&self, member: MemberId) -> Option<VolumeIndex> {
        self.tree
            .child_of(NodeId::ROOT, member)
            .map(|(_, volume)| volume)
    }
}

fn front(volume: &[u8]) -> &[u8] {
    &volume[..4096.min(volume.len())]
}

// A set's first volume posted under a later volume's name and left open,
// then its second posted under the first volume's name, read to its close.
// The first volume's headers place it first; the second's link it after.
#[test]
fn a_continuation_named_first_beside_a_first_named_third_settles() {
    let set = write(&items(), &split(Version::Rar4));
    assert_eq!(set.len(), 3);
    let mut run = Run::new();
    let first = run.announce("set.part3.rar", set[0].len());
    run.deliver(first, 0, front(&set[0]));
    let second = run.announce("set.part1.rar", set[1].len());
    run.deliver(second, 0, &set[1]);
    run.close(second);
    assert_eq!(run.handed, 2);
    assert_eq!(run.at(first), Some(VolumeIndex(0)));
    assert_eq!(run.at(second), Some(VolumeIndex(1)));
    assert_eq!(run.given_back(second), 0);
}

// Two sets of one kind. The first volume of one is posted first and left
// open; the second volume of the other is posted under the name of the first
// set's third, read to its close. Its header numbers it second, and the
// volume before it does not carry on into it: no place in the first set is
// its own.
#[test]
fn a_volume_whose_header_and_name_disagree_with_its_neighbour_settles() {
    let one = write(&items(), &split(Version::Rar5));
    let other = write(
        &[
            Item::File("delta.bin", payload(7, 10_000)),
            Item::File("epsilon.bin", payload(8, 5_003)),
        ],
        &split(Version::Rar5),
    );
    assert!(one.len() >= 3 && other.len() >= 2);
    let mut run = Run::new();
    let first = run.announce("set.part01.rar", one[0].len());
    run.deliver(first, 0, front(&one[0]));
    let held = run.tree.child_of(NodeId::ROOT, first).expect("a store");
    let stray = run.announce("set.part03.rar", other[1].len());
    run.deliver(stray, 0, &other[1]);
    run.close(stray);
    assert_eq!(run.handed, 2);
    assert_eq!(run.tree.child_of(NodeId::ROOT, first), Some(held));
    // Never the first set's: wherever it went, it is not held beside it.
    assert_ne!(
        run.tree.child_of(NodeId::ROOT, stray).map(|(node, _)| node),
        Some(held.0)
    );
    assert!(run.given_back(stray) <= 1);
}

// An archive that is no volume, posted under the name of a set's second
// volume, whose second file header never arrives: its walk stops short of
// its end, so its headers have not yet said whether it is one of a set.
// Its name places it second, and nothing it has stated says otherwise: it is
// held there, read to its close, and never handed back and forth.
#[test]
fn an_archive_short_of_its_end_under_a_volume_name_is_handed_over_once() {
    let set = write(&items(), &split(Version::Rar4));
    let lone = write(&items(), &Options::new(Version::Rar4));
    assert_eq!(lone.len(), 1);
    let lone = &lone[0];
    let name = find(lone, b"beta.bin");
    // The second file header, name and all.
    let hole = (name - 32, name + 8);
    let mut run = Run::new();
    let first = run.announce("set.part1.rar", set[0].len());
    run.deliver(first, 0, &set[0]);
    run.close(first);
    let second = run.announce("set.part2.rar", lone.len());
    run.deliver(second, 0, &lone[..hole.0]);
    run.deliver(second, hole.1, &lone[hole.1..]);
    run.close(second);
    assert_eq!(run.handed, 2);
    assert_eq!(run.given_back(second), 0);
    assert_eq!(run.at(second), Some(VolumeIndex(1)));
}

// A RAR5 volume's main header with its number set to `number`.
fn renumbered(volume: &[u8], number: u32) -> Vec<u8> {
    const SIGNATURE: usize = 8;
    let size = usize::from(volume[SIGNATURE + 4]);
    assert!(size < 0x80, "a one-byte header size");
    let inner = &volume[SIGNATURE + 5..SIGNATURE + 5 + size];
    // Kind, header flags, archive flags, volume number: one byte each.
    assert_eq!(inner, &[1, 0, inner[2], inner[3]], "a numbered main header");
    assert_eq!(inner[2] & 0x3, 0x3, "a volume with its number");
    let mut vint = Vec::new();
    let mut rest = number;
    loop {
        let low = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest == 0 {
            vint.push(low);
            break;
        }
        vint.push(low | 0x80);
    }
    let mut sized = vec![(3 + vint.len()) as u8, 1, 0, inner[2]];
    sized.extend(vint);
    let header = crc32(&sized);
    let mut out = volume[..SIGNATURE].to_vec();
    out.extend(header.to_le_bytes());
    out.append(&mut sized);
    out.extend_from_slice(&volume[SIGNATURE + 5 + size..]);
    out
}

// Two volumes of a set numbered far past any first, the second its set's
// last, posted under names that number nothing. The work their close does
// for the volumes before them is a few effects, not one for every number.
#[test]
fn two_volumes_numbered_far_out_say_no_gap_for_every_number_before_them() {
    const FAR: u32 = 400_000_000;
    let set = write(&items(), &split(Version::Rar5));
    assert_eq!(set.len(), 3);
    let posted = [renumbered(&set[1], FAR), renumbered(&set[2], FAR + 1)];
    let mut run = Run::new();
    let mut members = Vec::new();
    for (name, bytes) in ["c0ffee", "d00d"].into_iter().zip(&posted) {
        let member = run.announce(name, bytes.len());
        run.deliver(member, 0, bytes);
        members.push(member);
    }
    assert_eq!(run.at(members[0]), Some(VolumeIndex(FAR)));
    assert_eq!(run.at(members[1]), Some(VolumeIndex(FAR + 1)));
    let before = run.effects.len();
    for member in members {
        run.close(member);
    }
    let damaged = run.effects[before..]
        .iter()
        .filter(|effect| matches!(effect, Effect::Damaged { .. }))
        .count();
    assert_eq!(damaged, 0);
    assert_eq!(run.handed, 2);
    assert!(
        run.effects.len() - before < 32,
        "{}",
        run.effects.len() - before
    );
    // The set is refused as no set can be, as one numbered past every set is.
    assert!(run.effects[before..].iter().any(|effect| matches!(
        effect,
        Effect::Refused {
            why: Why::InvalidArchive,
            ..
        }
    )));
}
