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
                bytes: Bytes::from_static(bytes).into(),
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
            } if *node == child => Some((*offset, bytes.bytes().clone(), *provenance)),
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
    assert!(folded(&effects, copy, second));
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
    // The first clean copy of `wo` stands: only the damaged rest is taken.
    assert_eq!(
        written(&effects, child),
        vec![(SECOND + 2, Bytes::from_static(b"rld"), Provenance::Fresh)],
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
            bytes: Bytes::from_static(b"world").into(),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        &mut effects,
    );
    assert!(folded(&effects, second, copy), "{effects:?}");
    assert_eq!(listed(&tree), vec![MemberId(0), copy]);
}

// The tree as a restart rebuilds it: every record kept, no front cached.
fn restarted(tree: &Tree, restore: &dyn crate::Restore) -> Tree {
    Tree::rebuild(&tree.records(), restore).unwrap()
}

struct RestoreSplit;
impl crate::Restore for RestoreSplit {
    fn restore(&self, facts: &crate::Facts) -> Result<Box<dyn Object>, crate::RestoreError> {
        Ok(Box::new(facts.read::<Split>(Kind::RarStore)?))
    }
}

#[test]
fn retained_byte_authority_prevents_an_empty_cache_from_displacing_a_source() {
    let (tree, child, original) = held(&[(0, b"world", Provenance::Fresh)]);
    let mut tree = restarted(&tree, &RestoreSplit);
    assert!(
        tree.namespace(NodeId::ROOT)
            .unwrap()
            .first_bytes(original)
            .is_empty()
    );
    let (other, _) = post(&mut tree, "other.r00", &[(0, b"OTHER", Provenance::Fresh)]);
    assert_eq!(
        tree.child_of(NodeId::ROOT, original),
        Some((child, VolumeIndex(1)))
    );
    assert_eq!(tree.child_of(NodeId::ROOT, other), None);
    assert!(!tree.held_by_name(NodeId::ROOT, original));
}

#[test]
fn a_volume_whose_front_never_came_is_held_by_its_name_across_a_restart() {
    let (tree, child, original) = held(&[]);
    assert!(tree.held_by_name(NodeId::ROOT, original));
    let mut tree = restarted(&tree, &RestoreSplit);
    assert!(tree.held_by_name(NodeId::ROOT, original));
    let (copy, _) = post(&mut tree, "again.r00", &[(0, b"world", Provenance::Fresh)]);
    assert_eq!(
        tree.child_of(NodeId::ROOT, copy),
        Some((child, VolumeIndex(1)))
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, original),
        None,
        "a name held it"
    );
}

#[test]
fn demotion_copies_authority_without_consuming_the_old_inverse_owner() {
    let (tree, original, store, _) = bound_tree();
    let mut tree = restarted(&tree, &RestoreSplit);
    assert!(!tree.held_by_name(NodeId::ROOT, original));
    tree.unbind(store, Why::Compressed, true, &mut Build, &mut Vec::new());
    let (engine, _) = tree.child_of(NodeId::ROOT, original).unwrap();
    assert_ne!(engine, store);
    assert!(
        tree.nodes.get(&store).unwrap().object.is_some(),
        "the retired store still owns its inverse map"
    );
    assert!(
        !tree.held_by_name(NodeId::ROOT, original),
        "an empty front cache does not turn a fronted volume into a name"
    );
}

#[test]
fn a_copy_beside_a_holder_whose_front_is_not_kept_is_decided_at_once() {
    for bytes in [b"world".as_slice(), b"OTHER".as_slice()] {
        let (tree, child, original) = held(&[(0, b"world", Provenance::Fresh)]);
        let mut tree = restarted(&tree, &RestoreSplit);
        // Its whole length known, its bytes say all a sniff can.
        let copy = tree.announce_root("again.bin", Some(bytes.len() as u64), &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            copy,
            Posted {
                offset: 0,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        tree.rename(NodeId::ROOT, copy, "again.r00", &mut Build, &mut Vec::new());
        assert!(!tree.is_undecided(NodeId::ROOT, copy), "nothing waits");
        assert_eq!(
            tree.child_of(NodeId::ROOT, original),
            Some((child, VolumeIndex(1)))
        );
        assert_eq!(tree.child_of(NodeId::ROOT, copy), None);
    }
}

// A second volume whose front signs a store, as a copy's does: a copy that
// did not wait would found a store of its own.
const SIGNED: &[u8] = b"Rar!\x1a\x07\x00world";

// The store's second volume as a restart leaves it: its record kept and its
// front not, and, `uncovered`, its bytes not read back either.
fn restored(uncovered: bool) -> (Tree, NodeId, MemberId) {
    let (tree, child, original) = held(&[(0, SIGNED, Provenance::Fresh)]);
    let mut tree = restarted(&tree, &RestoreSplit);
    if uncovered {
        let front = ByteRange::new(0, SIGNED.len() as u64);
        tree.uncover(NodeId::ROOT, VolumeIndex(original.0), front);
    }
    (tree, child, original)
}

// The holder's front, coming again.
fn front_again(tree: &mut Tree, holder: MemberId) -> Vec<Effect> {
    let mut effects = Vec::new();
    let again = Posted {
        offset: 0,
        bytes: Bytes::from_static(SIGNED).into(),
        provenance: Provenance::Fresh,
    };
    tree.deliver(NodeId::ROOT, holder, again, &mut Build, &mut effects);
    effects
}

// The holder's front, lost, and the damage heard as its owner hears it.
fn front_lost(tree: &mut Tree, holder: MemberId) -> Vec<Effect> {
    let mut effects = Vec::new();
    let front = ByteRange::new(0, SIGNED.len() as u64);
    tree.damage(NodeId::ROOT, VolumeIndex(holder.0), front, &mut effects);
    let damaged: Vec<(NodeId, MemberId, ByteRange)> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Damaged {
                node,
                member,
                range,
            } => Some((*node, *member, *range)),
            _ => None,
        })
        .collect();
    for (node, member, range) in damaged {
        tree.damage_reached(node, member, range, &mut Build, &mut effects);
    }
    effects
}

#[test]
fn an_undecided_copy_is_decided_when_the_holders_front_returns() {
    for uncovered in [false, true] {
        for copy_first in [true, false] {
            let case = format!("uncovered {uncovered}, copy first {copy_first}");
            let (mut tree, child, original) = restored(uncovered);
            let mut effects = Vec::new();
            if !copy_first {
                effects.extend(front_again(&mut tree, original));
            }
            let (copy, posted) = post(&mut tree, "again.r00", &[(0, SIGNED, Provenance::Fresh)]);
            effects.extend(posted);
            if copy_first {
                assert!(tree.is_undecided(NodeId::ROOT, copy), "{case}: it waits");
                assert_eq!(tree.child_of(NodeId::ROOT, copy), None, "{case}");
                assert_eq!(tree.live().len(), 2, "{case}: no store of its own");
                effects.extend(front_again(&mut tree, original));
            }
            assert!(folded(&effects, copy, original), "{case}: {effects:?}");
            assert!(!tree.is_undecided(NodeId::ROOT, copy), "{case}");
            assert_eq!(
                tree.child_of(NodeId::ROOT, copy),
                Some((child, VolumeIndex(1))),
                "{case}"
            );
            assert_eq!(listed(&tree), vec![MemberId(0), original], "{case}");
            assert_eq!(tree.live().len(), 2, "{case}: one store");
        }
    }
}

#[test]
fn an_undecided_copy_displaces_a_holder_whose_front_is_lost() {
    for uncovered in [false, true] {
        for copy_first in [true, false] {
            let case = format!("uncovered {uncovered}, copy first {copy_first}");
            let (mut tree, child, original) = restored(uncovered);
            if !copy_first {
                front_lost(&mut tree, original);
            }
            let (copy, _) = post(&mut tree, "again.r00", &[(0, SIGNED, Provenance::Fresh)]);
            if copy_first {
                assert!(tree.is_undecided(NodeId::ROOT, copy), "{case}: it waits");
                front_lost(&mut tree, original);
            }
            assert!(!tree.is_undecided(NodeId::ROOT, copy), "{case}");
            assert_eq!(
                tree.child_of(NodeId::ROOT, copy),
                Some((child, VolumeIndex(1))),
                "{case}"
            );
            assert_eq!(tree.child_of(NodeId::ROOT, original), None, "{case}");
            assert_eq!(tree.live().len(), 2, "{case}: no store of its own");
        }
    }
}

// A store that claims `.r00` as its second volume when the name continues
// the set its first volume's name began, or names no set at all, as an
// obfuscated repost does: `anon.r00` could be any store's.
#[derive(serde::Serialize, serde::Deserialize)]
struct Stem(Option<String>);

impl crate::Durable for Stem {
    const VERSION: u16 = 1;
}

impl Object for Stem {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn facts(&self) -> crate::Facts {
        crate::Facts::of(Kind::RarStore, self)
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        if volume == VolumeIndex(0) {
            self.0 = name.split_once('.').map(|(stem, _)| stem.to_owned());
        }
    }
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Vec<VolumeIndex> {
        name.split_once('.')
            .filter(|(stem, extension)| {
                *extension == "r00" && (*stem == "anon" || self.0.as_deref() == Some(*stem))
            })
            .map(|_| VolumeIndex(1))
            .into_iter()
            .collect()
    }
}

struct Stems;
impl Builder for Stems {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Stem(None)) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}
impl crate::Restore for Stems {
    fn restore(&self, facts: &crate::Facts) -> Result<Box<dyn Object>, crate::RestoreError> {
        Ok(Box::new(facts.read::<Stem>(Kind::RarStore)?))
    }
}

// A posting at the root, delivered whole at offset 0 to the stem stores.
fn posted(tree: &mut Tree, name: &str, bytes: &[u8]) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        },
        &mut Stems,
        &mut Vec::new(),
    );
    member
}

// Two stores beside each other, `holding` founded first or second. The
// holding store holds its second volume by bytes, and after a restart has
// no front of it cached: a candidate it also claims is not its to take.
#[test]
fn a_waiting_holder_in_one_store_does_not_hide_a_candidate_from_another() {
    for holding_first in [true, false] {
        let mut tree = Tree::new();
        let store = |tree: &mut Tree, stem: &str| {
            let mut bytes = SIGNATURE.to_vec();
            bytes.extend_from_slice(stem.as_bytes());
            let first = posted(tree, &format!("{stem}.rar"), &bytes);
            tree.child_of(NodeId::ROOT, first).unwrap().0
        };
        let (holding, other) = if holding_first {
            let holding = store(&mut tree, "held");
            (holding, store(&mut tree, "free"))
        } else {
            let other = store(&mut tree, "free");
            (store(&mut tree, "held"), other)
        };
        let holder = posted(&mut tree, "held.r00", b"world");
        assert_eq!(
            tree.child_of(NodeId::ROOT, holder),
            Some((holding, VolumeIndex(1)))
        );
        let mut tree = restarted(&tree, &Stems);
        // Its name places it with neither store; its bytes come first.
        let candidate = posted(&mut tree, "anon.bin", b"world");
        assert_eq!(tree.child_of(NodeId::ROOT, candidate), None);
        tree.rename(
            NodeId::ROOT,
            candidate,
            "anon.r00",
            &mut Stems,
            &mut Vec::new(),
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, candidate),
            Some((other, VolumeIndex(1))),
            "holding first: {holding_first}"
        );
        assert!(!tree.is_undecided(NodeId::ROOT, candidate));
        assert_eq!(
            tree.child_of(NodeId::ROOT, holder),
            Some((holding, VolumeIndex(1)))
        );
    }
}
