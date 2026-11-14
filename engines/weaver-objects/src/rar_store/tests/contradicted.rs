// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR5 volume whose main header marks it its set's first, with no
// number, yet which opens on a member begun before it. No set has such a
// volume: its own headers rule out the one place they state.

use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::{Effect, MemberId, NodeId, Object, Posted, Provenance, Tree, VolumeIndex, Why};

use super::super::claim;
use super::writer::{Version, write};
use super::{items, split, store};
use crate::Objects;
use crate::grid::crc32;

// A later volume of a RAR5 set with its number taken out of its main
// header, so the header states the first position.
fn unnumbered(volume: &[u8]) -> Vec<u8> {
    const SIGNATURE: usize = 8;
    let size = usize::from(volume[SIGNATURE + 4]);
    assert!(size < 0x80, "a one-byte header size");
    let inner = &volume[SIGNATURE + 5..SIGNATURE + 5 + size];
    // Kind, header flags, archive flags, volume number: one byte each.
    assert_eq!(inner, &[1, 0, inner[2], inner[3]], "a numbered main header");
    assert_eq!(inner[2] & 0x3, 0x3, "a volume with its number");
    let mut sized = vec![3, 1, 0, inner[2] & !0x2];
    let header = crc32(&sized);
    let mut out = volume[..SIGNATURE].to_vec();
    out.extend(header.to_le_bytes());
    out.append(&mut sized);
    out.extend_from_slice(&volume[SIGNATURE + 5 + size..]);
    out
}

fn front(volume: &[u8]) -> Vec<u8> {
    volume[..4096.min(volume.len())].to_vec()
}

// Two such volumes, of one set, each opening on a different member.
fn contradicted() -> [Vec<u8>; 2] {
    let set = write(&items(), &split(Version::Rar5));
    assert_eq!(set.len(), 3);
    [unnumbered(&set[1]), unnumbered(&set[2])]
}

// The headers say what the shape is: a volume, no number, opening on a
// member continued from before. The parser accepts them.
#[test]
fn the_parser_reads_an_unnumbered_volume_that_opens_on_a_continued_member() {
    for volume in contradicted() {
        let facts = claim::headers(&front(&volume), None, Arc::new(unrar_rs::KdfCache::new()))
            .expect("headers the parser accepts");
        assert!(facts.is_volume);
        assert_eq!(facts.volume_number, None);
        let first = facts
            .members
            .iter()
            .min_by_key(|member| member.order)
            .expect("a member");
        assert!(first.split_before);
        assert!(claim::misplaced(VolumeIndex(0), &facts));
    }
}

// One such volume never takes the first place from another: the place its
// headers state is one they rule out, held or claimed.
#[test]
fn a_volume_its_headers_misplace_first_never_takes_the_first_place() {
    let [one, two] = contradicted();
    let mut held = store();
    held.bound(VolumeIndex(0), "c0ffee", &front(&one), None);
    assert!(!held.provisional(VolumeIndex(0), "d00d", &front(&two), None));
    assert_eq!(
        held.claim("d00d", &front(&two), None),
        Vec::<VolumeIndex>::new()
    );
}

// The two met in turn, each displaced holder offered again by what was
// parsed of it, as the tree offers it: no turn takes the first place, so
// the turns end at once. Counted, and stopped at a few, so a cycle ends.
#[test]
fn two_volumes_their_headers_misplace_first_take_no_turns_at_the_first_place() {
    let [one, two] = contradicted();
    let mut object = store();
    object.bound(VolumeIndex(0), "c0ffee", &front(&one), None);
    let mut claimant = ("d00d", front(&two), None);
    let mut holder = ("c0ffee", front(&one));
    let mut turns = 0;
    while turns < 4 {
        let (name, bytes, carried) = claimant;
        if !object.provisional(VolumeIndex(0), name, &bytes, carried.as_ref()) {
            break;
        }
        let given = object.give_back(VolumeIndex(0)).expect("a held volume");
        match carried {
            Some(carried) => object.adopt(VolumeIndex(0), name, &bytes, None, carried),
            None => object.bound(VolumeIndex(0), name, &bytes, None),
        }
        let (name, front) = std::mem::replace(&mut holder, (name, bytes));
        claimant = (name, front, Some(given));
        turns += 1;
    }
    assert_eq!(turns, 0);
}

fn post(tree: &mut Tree, name: &str, bytes: &[u8], effects: &mut Vec<Effect>) -> MemberId {
    let member = tree.announce_root(name, Some(bytes.len() as u64), effects);
    deliver(tree, member, 0, &front(bytes), effects);
    tree.handed_over(NodeId::ROOT, member, &mut Objects::default(), effects);
    member
}

fn deliver(
    tree: &mut Tree,
    member: MemberId,
    offset: u64,
    bytes: &[u8],
    effects: &mut Vec<Effect>,
) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        },
        &mut Objects::default(),
        effects,
    );
}

fn displaced(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|effect| matches!(effect, Effect::GivenBack { .. }))
        .count()
}

// Two such volumes in one namespace, each met by its front before its walk
// ends: neither displaces the other from the first place. Read to its
// close, each goes back as its headers misplace it, once, and is a volume
// of no set.
#[test]
fn two_volumes_their_headers_misplace_first_never_displace_each_other() {
    let [one, two] = contradicted();
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let first = post(&mut tree, "c0ffee", &one, &mut effects);
    let held = tree.child_of(NodeId::ROOT, first).expect("a founded store");
    let second = post(&mut tree, "d00d", &two, &mut effects);
    assert_eq!(displaced(&effects), 0);
    assert_eq!(tree.child_of(NodeId::ROOT, first), Some(held));
    for (member, bytes) in [(first, &one), (second, &two)] {
        let rest = 4096.min(bytes.len());
        deliver(&mut tree, member, rest as u64, &bytes[rest..], &mut effects);
        tree.close(NodeId::ROOT, member, &mut Objects::default(), &mut effects);
    }
    assert_eq!(displaced(&effects), 2);
    for member in [first, second] {
        let given = effects
            .iter()
            .filter(
                |effect| matches!(effect, Effect::GivenBack { member: at, .. } if *at == member),
            )
            .count();
        assert_eq!(given, 1, "{member:?}");
        assert_eq!(tree.child_of(NodeId::ROOT, member), None);
        assert_eq!(tree.withholds(NodeId::ROOT, member), Some(Why::Unmapped));
    }
}
