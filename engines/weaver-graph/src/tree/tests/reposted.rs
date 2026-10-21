// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume posted again under a name of its own: the object that holds the
// volume answers that it is that volume, and the posting folds into the
// member it repeats.

use super::*;

const SECOND: u64 = VOLUME_LEN - HEADER;

// A posting at the root, its bytes delivered in pieces at their offsets.
fn post(
    tree: &mut Tree,
    name: &str,
    pieces: &[(u64, &'static [u8], Provenance)],
) -> (MemberId, Vec<Effect>) {
    let member = tree.announce_root(name, None, &mut Vec::new());
    let mut effects = Vec::new();
    for &(offset, bytes, provenance) in pieces {
        tree.deliver(
            NodeId::ROOT,
            member,
            offset,
            Bytes::from_static(bytes),
            provenance,
            &mut Build,
            &mut effects,
        );
    }
    (member, effects)
}

// The store's tree with its second volume posted as `pieces`.
fn held(pieces: &[(u64, &'static [u8], Provenance)]) -> (Tree, NodeId, MemberId) {
    let (mut tree, _, child, _) = bound_tree();
    let (second, _) = post(&mut tree, "show.r00", pieces);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((child, VolumeIndex(1)))
    );
    (tree, child, second)
}

// What reached the store's member, as (offset, bytes, provenance).
fn written(effects: &[Effect], child: NodeId) -> Vec<(u64, Bytes, Provenance)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::WriteLeaf {
                node,
                member: MemberId(0),
                offset,
                bytes,
                provenance,
            } if *node == child => Some((*offset, bytes.clone(), *provenance)),
            _ => None,
        })
        .collect()
}

fn listed(tree: &Tree) -> Vec<MemberId> {
    tree.namespace(NodeId::ROOT)
        .unwrap()
        .members()
        .map(|member| member.id)
        .collect()
}

#[test]
fn a_copy_of_a_held_volume_folds_into_it() {
    let (mut tree, child, second) = held(&[(0, b"world", Provenance::Fresh)]);
    let (copy, effects) = post(&mut tree, "again.r00", &[(0, b"world", Provenance::Fresh)]);
    assert!(effects.contains(&Effect::Folded {
        node: NodeId::ROOT,
        copy,
        into: second,
    }));
    assert_eq!(
        tree.child_of(NodeId::ROOT, copy),
        Some((child, VolumeIndex(1)))
    );
    assert_eq!(
        listed(&tree),
        vec![MemberId(0), second],
        "the copy is no member"
    );
    assert!(
        !effects.iter().any(|effect| matches!(
            effect,
            Effect::WriteLeaf { node: NodeId::ROOT, member, .. } if *member == copy
        )),
        "the copy is no file: {effects:?}"
    );
    assert_eq!(tree.live().len(), 2, "one store");
}

#[test]
fn clean_bytes_of_a_copy_replace_damaged_ones() {
    let (mut tree, child, second) = held(&[
        (0, b"wo", Provenance::Fresh),
        (2, b"Xld", Provenance::Damaged),
    ]);
    let (_, effects) = post(&mut tree, "again.r00", &[(0, b"world", Provenance::Fresh)]);
    assert!(
        written(&effects, child).contains(&(
            SECOND,
            Bytes::from_static(b"world"),
            Provenance::Fresh
        )),
        "{effects:?}"
    );
    let root = tree.namespace(NodeId::ROOT).unwrap();
    assert!(root.get(second).unwrap().coverage.is_clean());
    let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
    assert_eq!(inner.coverage.damaged_ranges().count(), 0);
}

#[test]
fn damaged_bytes_of_a_copy_never_replace_clean_ones() {
    let (mut tree, child, second) = held(&[(0, b"world", Provenance::Fresh)]);
    let (copy, effects) = post(
        &mut tree,
        "again.r00",
        &[
            (0, b"wo", Provenance::Fresh),
            (2, b"Xld", Provenance::Damaged),
        ],
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, copy),
        tree.child_of(NodeId::ROOT, second)
    );
    assert!(
        !written(&effects, child)
            .iter()
            .any(|(_, _, provenance)| *provenance == Provenance::Damaged),
        "{effects:?}"
    );
    let root = tree.namespace(NodeId::ROOT).unwrap();
    assert!(root.get(second).unwrap().coverage.is_clean());
}

#[test]
fn a_member_posted_twice_closes_once_both_postings_have() {
    let (mut tree, _, second) = held(&[(0, b"world", Provenance::Fresh)]);
    let (copy, _) = post(&mut tree, "again.r00", &[(0, b"world", Provenance::Fresh)]);
    let closed = |effects: &[Effect]| {
        effects.contains(&Effect::Closed {
            node: NodeId::ROOT,
            member: second,
        })
    };
    let mut effects = Vec::new();
    tree.close(NodeId::ROOT, second, &mut Build, &mut effects);
    assert!(!closed(&effects), "the copy can still bring bytes");
    tree.close(NodeId::ROOT, copy, &mut Build, &mut effects);
    assert!(closed(&effects), "{effects:?}");
    assert!(
        tree.namespace(NodeId::ROOT)
            .unwrap()
            .get(second)
            .unwrap()
            .closed
    );
}

#[test]
fn a_copy_whose_front_disagrees_is_not_folded() {
    let (mut tree, _, second) = held(&[(0, b"world", Provenance::Fresh)]);
    let (copy, effects) = post(&mut tree, "again.r00", &[(0, b"WORLD", Provenance::Fresh)]);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Folded { .. })),
        "{effects:?}"
    );
    assert_eq!(tree.child_of(NodeId::ROOT, copy), None);
    assert_eq!(listed(&tree), vec![MemberId(0), second, copy]);
}
