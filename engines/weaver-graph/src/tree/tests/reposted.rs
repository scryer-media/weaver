// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume posted again under a name of its own: the object that holds the
// volume answers that it is that volume, and the posting folds into the
// member it repeats.

use super::*;
use crate::deliver::Posted;

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
            Posted {
                offset,
                bytes: Bytes::from_static(bytes),
                provenance,
            },
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

#[test]
fn a_volume_whose_bytes_come_late_folds_into_the_copy_that_took_its_place() {
    let (mut tree, child, second) = held(&[]);
    let (copy, effects) = post(&mut tree, "again.r00", &[(0, b"world", Provenance::Fresh)]);
    assert_eq!(
        tree.child_of(NodeId::ROOT, copy),
        Some((child, VolumeIndex(1))),
        "{effects:?}"
    );
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "a name held it");
    let mut effects = Vec::new();
    tree.deliver(
        NodeId::ROOT,
        second,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(b"world"),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        &mut effects,
    );
    assert!(
        effects.contains(&Effect::Folded {
            node: NodeId::ROOT,
            copy: second,
            into: copy,
        }),
        "{effects:?}"
    );
    assert_eq!(listed(&tree), vec![MemberId(0), copy]);
}

fn forget_front_cache(tree: &mut Tree) {
    let records: Vec<_> = tree
        .namespace(NodeId::ROOT)
        .unwrap()
        .records()
        .cloned()
        .collect();
    let mut namespace = Namespace::new();
    for record in records {
        namespace.restore(record);
    }
    *tree.namespace_mut(NodeId::ROOT).unwrap() = namespace;
}

#[test]
fn retained_byte_authority_prevents_an_empty_cache_from_displacing_a_source() {
    let (mut tree, child, original) = held(&[(0, b"world", Provenance::Fresh)]);
    forget_front_cache(&mut tree);
    let (other, _) = post(&mut tree, "other.r00", &[(0, b"OTHER", Provenance::Fresh)]);
    assert_eq!(
        tree.child_of(NodeId::ROOT, original),
        Some((child, VolumeIndex(1)))
    );
    assert_eq!(tree.child_of(NodeId::ROOT, other), None);
    assert!(!tree.held_by_name(NodeId::ROOT, original));
}

#[test]
fn demotion_copies_authority_without_consuming_the_old_inverse_owner() {
    let (mut tree, original, store, _) = bound_tree();
    forget_front_cache(&mut tree);
    assert!(!tree.held_by_name(NodeId::ROOT, original));
    tree.unbind(store, Why::Compressed, true, &mut Build, &mut Vec::new());
    let (engine, volume) = tree.child_of(NodeId::ROOT, original).unwrap();
    assert_ne!(engine, store);
    assert!(
        tree.nodes.get(&store).unwrap().object.is_some(),
        "the retired store still owns its inverse map"
    );
    assert!(
        !tree
            .nodes
            .get(&engine)
            .unwrap()
            .object
            .as_ref()
            .unwrap()
            .name_only(volume),
        "empty parent caches do not erase handed authority"
    );
}

fn pending_copy(bytes: &'static [u8]) -> (Tree, NodeId, MemberId, MemberId) {
    let (mut tree, child, original) = held(&[(0, b"world", Provenance::Fresh)]);
    forget_front_cache(&mut tree);
    tree.namespace_mut(NodeId::ROOT)
        .unwrap()
        .uncover(original, ByteRange::new(0, 5));
    let (copy, _) = post(&mut tree, "again.r00", &[(0, bytes, Provenance::Fresh)]);
    tree.close(NodeId::ROOT, copy, &mut Build, &mut Vec::new());
    assert!(tree.is_undecided(NodeId::ROOT, copy));
    assert_eq!(
        tree.pending_claims[&(NodeId::ROOT, original)],
        BTreeSet::from([copy])
    );
    (tree, child, original, copy)
}

fn front_returns(tree: &mut Tree, original: MemberId) {
    tree.deliver(
        NodeId::ROOT,
        original,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(b"world"),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        &mut Vec::new(),
    );
}

#[test]
fn an_occupied_slot_waits_for_source_bytes_then_compares_equal_and_unequal_fronts() {
    for bytes in [b"world".as_slice(), b"OTHER".as_slice()] {
        let (mut tree, child, original, copy) = pending_copy(bytes);
        front_returns(&mut tree, original);
        assert!(tree.pending_claims.is_empty());
        assert!(!tree.is_undecided(NodeId::ROOT, copy));
        assert_eq!(
            tree.child_of(NodeId::ROOT, original),
            Some((child, VolumeIndex(1)))
        );
        assert_eq!(
            tree.namespace(NodeId::ROOT).unwrap().copy_of(copy),
            (bytes == b"world").then_some(original)
        );
    }
}

#[test]
fn unrelated_namespace_progress_does_not_retry_pending_claims() {
    let (mut tree, child, original, copy) = pending_copy(b"world");
    tree.emitted(
        child,
        vec![Emit::Member {
            member: MemberId(1),
            name: "neighbor.bin".into(),
            length: Some(5),
        }],
        &mut Build,
        &mut Vec::new(),
    );
    tree.deliver(
        child,
        MemberId(1),
        Posted {
            offset: 0,
            bytes: Bytes::from_static(b"other"),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        &mut Vec::new(),
    );
    assert_eq!(
        tree.retried_claims, 0,
        "only the occupied holder's dependency is visited"
    );
    assert_eq!(
        tree.pending_claims[&(NodeId::ROOT, original)],
        BTreeSet::from([copy])
    );
}

#[test]
fn closing_a_lost_holder_or_retracting_its_owner_releases_only_its_waiters() {
    for retract in [false, true] {
        let (mut tree, child, original, copy) = pending_copy(b"world");
        if retract {
            tree.retract(NodeId::ROOT, original, &mut Build, &mut Vec::new());
        } else {
            tree.volume_lost(
                child,
                VolumeIndex(1),
                ByteRange::new(0, 5),
                &mut Build,
                &mut Vec::new(),
            );
            tree.close(NodeId::ROOT, original, &mut Build, &mut Vec::new());
        }
        assert!(tree.pending_claims.is_empty());
        assert!(!tree.is_undecided(NodeId::ROOT, copy));
        assert_eq!(tree.namespace(NodeId::ROOT).unwrap().copy_of(copy), None);
    }
}

#[test]
fn a_retry_behind_a_lent_owner_uses_the_existing_loan_queue() {
    let (mut tree, _, original, copy) = pending_copy(b"world");
    let set = super::sets::posted(&mut tree, "recovery.par2");
    let object = tree.lend(set).unwrap();
    front_returns(&mut tree, original);
    assert!(tree.is_undecided(NodeId::ROOT, copy));
    tree.take_back(set, object, &mut Build, &mut Vec::new());
    assert_eq!(
        tree.namespace(NodeId::ROOT).unwrap().copy_of(copy),
        Some(original)
    );
}

#[test]
fn a_checkpoint_of_a_deferred_candidate_reconstructs_its_dependency_from_front_replay() {
    struct RestoreSplit;
    impl crate::Restore for RestoreSplit {
        fn restore(&self, facts: &crate::Facts) -> Result<Box<dyn Object>, crate::RestoreError> {
            Ok(Box::new(facts.read::<Split>(Kind::RarStore)?))
        }
    }
    let (tree, _, original, copy) = pending_copy(b"world");
    let mut restored = Tree::rebuild(&tree.records(), &RestoreSplit).unwrap();
    assert!(restored.is_undecided(NodeId::ROOT, copy));
    assert!(
        restored.pending_claims.is_empty(),
        "only recorded graph state is kept"
    );
    restored
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .uncover(copy, ByteRange::new(0, 5));
    restored.deliver(
        NodeId::ROOT,
        copy,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(b"world"),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        &mut Vec::new(),
    );
    assert_eq!(
        restored.pending_claims[&(NodeId::ROOT, original)],
        BTreeSet::from([copy])
    );
    front_returns(&mut restored, original);
    assert!(restored.pending_claims.is_empty());
    assert_eq!(
        restored.namespace(NodeId::ROOT).unwrap().copy_of(copy),
        Some(original)
    );
}
