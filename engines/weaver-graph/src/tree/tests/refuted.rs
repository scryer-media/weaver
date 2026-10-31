// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set founded by its numbered names, refuted by a later piece's own
// bytes: every piece goes back and is claimed by its bytes. A piece whose
// bytes sign nothing stays the leaf it was read back as, and its front read
// back or fetched again, before or after a restart, founds no set from its
// name again: the kind refuted is no answer for it.

use super::*;
use crate::deliver::Posted;
use crate::facts::{Facts, Restore, RestoreError};

// A set of pieces founded by name, counted from `.001`, that its own
// bytes refute: a later piece whose front opens with the store's signature.
#[derive(serde::Serialize, serde::Deserialize)]
struct Refuting(Vec<VolumeIndex>);

impl crate::Durable for Refuting {
    const VERSION: u16 = 1;
}

impl Object for Refuting {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Split
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if delivery.volume != VolumeIndex(0)
            && delivery.offset == 0
            && delivery.bytes.starts_with(SIGNATURE)
        {
            out.push(Emit::Refuted);
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Held { range });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::NotYet
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn bound(&mut self, volume: VolumeIndex, _: &str, _: &[u8], _: Option<u64>) {
        self.0.push(volume);
    }
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Claim {
        name.rsplit_once('.')
            .and_then(|(_, digits)| digits.parse::<u32>().ok())
            .and_then(|number| number.checked_sub(1))
            .map(VolumeIndex)
            .into_iter()
            .collect()
    }
    fn taken(&mut self, volume: VolumeIndex) {
        self.0.retain(|held| *held != volume);
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::Split, self)
    }
}

// The store a piece's own bytes sign: it claims no member by its name, so
// a piece that signs nothing is never its volume.
#[derive(serde::Serialize, serde::Deserialize)]
struct Signed;

impl crate::Durable for Signed {
    const VERSION: u16 = 1;
}

impl Object for Signed {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
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
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Held { range });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::NotYet
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn claim(&mut self, _: &str, _: &[u8], _: Option<u64>) -> Claim {
        Vec::new().into()
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::RarStore, self)
    }
}

struct Pieces;

impl Builder for Pieces {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Signed)),
            Kind::Split => Some(Box::new(Refuting(Vec::new()))),
            _ => None,
        }
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

impl Restore for Pieces {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        match facts.kind {
            Kind::RarStore => Ok(Box::new(facts.read::<Signed>(Kind::RarStore)?)),
            _ => Ok(Box::new(facts.read::<Refuting>(Kind::Split)?)),
        }
    }
}

const LENGTH: u64 = 20;

// The first piece's front: bytes that sign nothing.
fn plain() -> Bytes {
    vec![b'x'; LENGTH as usize].into()
}

// The second piece's front: the store's signature, then some of its body.
fn signing() -> Bytes {
    let mut bytes = SIGNATURE.to_vec();
    bytes.resize(LENGTH as usize, b'y');
    bytes.into()
}

fn front(tree: &mut Tree, member: MemberId, bytes: Bytes, effects: &mut Vec<Effect>) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Pieces,
        effects,
    );
}

// The kinds bound at the root, in node order.
fn bound(tree: &Tree) -> Vec<Option<Kind>> {
    tree.bound_below(NodeId::ROOT)
        .into_iter()
        .map(|node| tree.kind(node))
        .collect()
}

// `show.001` signs nothing, `show.002` signs the store: the set its names
// founded is refuted, the second piece is the store's, and the first is
// the leaf it was read back as.
fn refuted() -> (Tree, MemberId) {
    let mut tree = Tree::new();
    let first = tree.announce_root("show.001", Some(LENGTH), &mut Vec::new());
    let second = tree.announce_root("show.002", Some(LENGTH), &mut Vec::new());
    front(&mut tree, first, plain(), &mut Vec::new());
    assert_eq!(bound(&tree), vec![Some(Kind::Split)], "the names found it");
    let mut effects = Vec::new();
    front(&mut tree, second, signing(), &mut effects);
    let disclaimed: Vec<MemberId> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Disclaimed { member, .. } => Some(*member),
            _ => None,
        })
        .collect();
    assert_eq!(disclaimed, vec![first, second], "{effects:?}");
    for member in disclaimed {
        tree.claim_by_bytes(NodeId::ROOT, member, &mut Pieces, &mut Vec::new());
    }
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
    assert_eq!(tree.child_of(NodeId::ROOT, first), None);
    (tree, first)
}

#[test]
fn a_refuted_piece_whose_front_comes_again_founds_no_set_from_its_name() {
    let (mut tree, first) = refuted();
    front(&mut tree, first, plain(), &mut Vec::new());
    assert_eq!(tree.child_of(NodeId::ROOT, first), None);
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
}

// The members the set gave back, each then claimed by its own bytes, as
// the owner claims them once it has read them back.
fn claim_disclaimed(tree: &mut Tree, effects: &[Effect]) -> Vec<MemberId> {
    let disclaimed: Vec<MemberId> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Disclaimed { member, .. } => Some(*member),
            _ => None,
        })
        .collect();
    for member in &disclaimed {
        tree.claim_by_bytes(NodeId::ROOT, *member, &mut Pieces, &mut Vec::new());
    }
    disclaimed
}

// Each root member by name: the kind of the object that owns it, and the
// kind whose reading of it was refuted.
fn ends(tree: &Tree) -> Vec<(String, Option<Kind>, Option<Kind>)> {
    let mut ends: Vec<_> = tree
        .namespace(NodeId::ROOT)
        .expect("a root")
        .members()
        .map(|record| {
            let owner = tree
                .child_of(NodeId::ROOT, record.id)
                .and_then(|(child, _)| tree.kind(child));
            (record.name.clone(), owner, record.refuted)
        })
        .collect();
    ends.sort_by(|a, b| a.0.cmp(&b.0));
    ends
}

// Three numbered pieces, `.003` fetched first: the names found the set
// over all three, `.002` signing the store refutes it, and every piece is
// claimed by its own bytes. `.003`'s bytes sign nothing, so it stays the
// leaf it was read back as.
fn third_first() -> Tree {
    let mut tree = Tree::new();
    let first = tree.announce_root("show.001", Some(LENGTH), &mut Vec::new());
    let second = tree.announce_root("show.002", Some(LENGTH), &mut Vec::new());
    let third = tree.announce_root("show.003", Some(LENGTH), &mut Vec::new());
    front(&mut tree, third, plain(), &mut Vec::new());
    front(&mut tree, first, plain(), &mut Vec::new());
    assert_eq!(bound(&tree), vec![Some(Kind::Split)], "the names found it");
    let mut effects = Vec::new();
    front(&mut tree, second, signing(), &mut effects);
    let mut disclaimed = claim_disclaimed(&mut tree, &effects);
    disclaimed.sort();
    assert_eq!(disclaimed, vec![first, second, third], "{effects:?}");
    tree
}

fn third_first_ends() -> Vec<(String, Option<Kind>, Option<Kind>)> {
    let ends = ends(&third_first());
    assert_eq!(
        ends,
        vec![
            ("show.001".into(), None, Some(Kind::Split)),
            ("show.002".into(), Some(Kind::RarStore), Some(Kind::Split)),
            ("show.003".into(), None, Some(Kind::Split)),
        ]
    );
    ends
}

// `.003` posted only once the set its name numbers is refuted, its bytes
// signing nothing: its name founds no set over the pieces whose own bytes
// claimed them, and it ends as it does fetched first.
#[test]
fn a_piece_posted_after_its_set_was_refuted_ends_as_it_does_fetched_first() {
    let (mut tree, _) = refuted();
    let third = tree.announce_root("show.003", Some(LENGTH), &mut Vec::new());
    front(&mut tree, third, plain(), &mut Vec::new());
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
    assert_eq!(ends(&tree), third_first_ends());
}

// The same, across a restart between the refutation and `.003`'s post.
#[test]
fn a_piece_posted_after_a_restart_past_its_sets_refutation_ends_as_it_does_fetched_first() {
    let (tree, _) = refuted();
    let mut tree = Tree::rebuild(&tree.records(), &Pieces).expect("rebuilt");
    let third = tree.announce_root("show.003", Some(LENGTH), &mut Vec::new());
    front(&mut tree, third, plain(), &mut Vec::new());
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
    assert_eq!(ends(&tree), third_first_ends());
}

// `.003` posted with the others, its front fetched only after `.002`
// refuted the set: it ends as it does fetched first.
#[test]
fn a_piece_fetched_after_its_set_was_refuted_ends_as_it_does_fetched_first() {
    let mut tree = Tree::new();
    let first = tree.announce_root("show.001", Some(LENGTH), &mut Vec::new());
    let second = tree.announce_root("show.002", Some(LENGTH), &mut Vec::new());
    let third = tree.announce_root("show.003", Some(LENGTH), &mut Vec::new());
    front(&mut tree, first, plain(), &mut Vec::new());
    let mut effects = Vec::new();
    front(&mut tree, second, signing(), &mut effects);
    claim_disclaimed(&mut tree, &effects);
    front(&mut tree, third, plain(), &mut Vec::new());
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
    assert_eq!(ends(&tree), third_first_ends());
}

#[test]
fn a_refuted_piece_replayed_after_a_restart_founds_no_set_from_its_name() {
    let (tree, first) = refuted();
    let mut tree = Tree::rebuild(&tree.records(), &Pieces).expect("rebuilt");
    front(&mut tree, first, plain(), &mut Vec::new());
    assert_eq!(tree.child_of(NodeId::ROOT, first), None);
    assert_eq!(bound(&tree), vec![Some(Kind::RarStore)]);
}
