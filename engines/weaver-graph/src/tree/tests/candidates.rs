// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A piece whose name says nothing, offered every place its geometry fits:
// the tree folds it into a holder it is a copy of, waits on a holder whose
// front is away, passes a place another piece holds, and takes the one
// place left free.

use super::*;
use crate::deliver::Posted;

// A store whose volumes are `set.rar`, then `set.002` on, and which places
// a `.bin` piece by geometry alone: every middle place, held or free.
#[derive(serde::Serialize, serde::Deserialize)]
struct Geometry {
    middles: u32,
}

impl crate::Durable for Geometry {
    const VERSION: u16 = 1;
}

impl Object for Geometry {
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
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Claim {
        if name.ends_with(".bin") {
            return (1..=self.middles).map(VolumeIndex).collect();
        }
        name.strip_prefix("set.")
            .and_then(|number| number.parse::<u32>().ok())
            .filter(|number| *number >= 2)
            .map(|number| VolumeIndex(number - 1))
            .into_iter()
            .collect()
    }
}

// Builds and restores the store with `middles` middle places.
struct Pieces(u32);
impl Builder for Pieces {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Geometry { middles: self.0 }) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}
impl crate::Restore for Pieces {
    fn restore(&self, facts: &crate::Facts) -> Result<Box<dyn Object>, crate::RestoreError> {
        Ok(Box::new(facts.read::<Geometry>(Kind::RarStore)?))
    }
}

// Bytes at the front of a member at the root.
fn front(tree: &mut Tree, member: MemberId, bytes: &[u8], pieces: &mut Pieces) -> Vec<Effect> {
    let mut effects = Vec::new();
    let posted = Posted {
        offset: 0,
        bytes: Bytes::copy_from_slice(bytes).into(),
        provenance: Provenance::Fresh,
    };
    tree.deliver(NodeId::ROOT, member, posted, pieces, &mut effects);
    effects
}

// A posting at the root, its front delivered, its length not yet known.
fn posted(
    tree: &mut Tree,
    name: &str,
    bytes: &[u8],
    pieces: &mut Pieces,
) -> (MemberId, Vec<Effect>) {
    let member = tree.announce_root(name, None, &mut Vec::new());
    (member, front(tree, member, bytes, pieces))
}

// The set founded and the given pieces bound by their names, each with its
// own front: the store and each piece's member.
fn set(pieces: &mut Pieces, held: &[(&str, &[u8])]) -> (Tree, NodeId, Vec<MemberId>) {
    let mut tree = Tree::new();
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(b"set");
    let (first, _) = posted(&mut tree, "set.rar", &bytes, pieces);
    let store = tree.child_of(NodeId::ROOT, first).unwrap().0;
    let members = held
        .iter()
        .map(|(name, bytes)| {
            let (member, _) = posted(&mut tree, name, bytes, pieces);
            assert!(tree.child_of(NodeId::ROOT, member).is_some(), "{name}");
            member
        })
        .collect();
    (tree, store, members)
}

// Five places, the first, second and fourth held.
const HELD: [(&str, &[u8]); 2] = [("set.002", b"AAAAA"), ("set.004", b"CCCCC")];

#[test]
fn a_piece_no_name_places_takes_the_one_place_left_free() {
    let pieces = &mut Pieces(3);
    let (mut tree, store, _) = set(pieces, &HELD);
    let (piece, effects) = posted(&mut tree, "x.bin", b"BBBBB", pieces);
    assert_eq!(
        tree.child_of(NodeId::ROOT, piece),
        Some((store, VolumeIndex(2))),
        "{effects:?}"
    );
}

#[test]
fn a_copy_of_a_held_piece_folds_into_its_holder() {
    let pieces = &mut Pieces(3);
    let (mut tree, store, held) = set(pieces, &HELD);
    let (copy, effects) = posted(&mut tree, "x.bin", b"AAAAA", pieces);
    assert!(folded(&effects, copy, held[0]), "{effects:?}");
    assert_eq!(
        tree.child_of(NodeId::ROOT, copy),
        Some((store, VolumeIndex(1)))
    );
}

// After a restart no holder's front is in hand: the piece waits on each
// whose front is coming, and is judged once they have come.
#[test]
fn a_piece_beside_holders_whose_fronts_are_away_waits_for_them() {
    for (bytes, copy) in [(b"BBBBB", false), (b"AAAAA", true)] {
        let pieces = &mut Pieces(3);
        let (tree, store, held) = set(pieces, &HELD);
        let mut tree = Tree::rebuild(&tree.records(), &*pieces).unwrap();
        let (piece, _) = posted(&mut tree, "x.bin", bytes, pieces);
        assert!(
            tree.is_undecided(NodeId::ROOT, piece),
            "copy {copy}: it waits"
        );
        assert_eq!(tree.child_of(NodeId::ROOT, piece), None, "copy {copy}");
        let mut effects = Vec::new();
        for (member, (_, bytes)) in held.iter().zip(HELD) {
            effects.extend(front(&mut tree, *member, bytes, pieces));
        }
        assert!(!tree.is_undecided(NodeId::ROOT, piece), "copy {copy}");
        assert_eq!(folded(&effects, piece, held[0]), copy, "{effects:?}");
        let place = if copy { 1 } else { 2 };
        assert_eq!(
            tree.child_of(NodeId::ROOT, piece),
            Some((store, VolumeIndex(place))),
            "copy {copy}"
        );
    }
}

// Six hundred plain members beside the set, then a restart: a holder's
// front coming again asks only the members still undecided whether they
// waited on it, not every member, and the piece is placed as before.
#[test]
fn a_front_that_comes_asks_only_the_undecided_members() {
    let pieces = &mut Pieces(3);
    let (mut tree, store, held) = set(pieces, &HELD);
    for index in 0..600 {
        let name = format!("plain{index}.dat");
        let member = tree.announce_root(&name, Some(5), &mut Vec::new());
        front(&mut tree, member, b"PLAIN", pieces);
        assert!(!tree.is_undecided(NodeId::ROOT, member), "{name}");
    }
    let mut tree = Tree::rebuild(&tree.records(), &*pieces).unwrap();
    let (piece, _) = posted(&mut tree, "x.bin", b"BBBBB", pieces);
    assert!(tree.is_undecided(NodeId::ROOT, piece), "it waits");
    let undecided = tree.undecided(NodeId::ROOT).count();
    FRONT_CANDIDATES.with(|seen| seen.set(0));
    let mut effects = front(&mut tree, held[0], HELD[0].1, pieces);
    let asked = FRONT_CANDIDATES.with(std::cell::Cell::get);
    assert_eq!(asked, undecided, "the members asked are the undecided ones");
    effects.extend(front(&mut tree, held[1], HELD[1].1, pieces));
    assert!(!tree.is_undecided(NodeId::ROOT, piece));
    assert_eq!(
        tree.child_of(NodeId::ROOT, piece),
        Some((store, VolumeIndex(2))),
        "{effects:?}"
    );
}

// Four places, the first and last held: two middle places are free and
// nothing orders the piece, so neither takes it.
#[test]
fn a_piece_two_free_places_fit_is_left_unclaimed() {
    let pieces = &mut Pieces(2);
    let (mut tree, _, _) = set(pieces, &[("set.004", b"CCCCC")]);
    let (piece, effects) = posted(&mut tree, "x.bin", b"BBBBB", pieces);
    assert_eq!(tree.child_of(NodeId::ROOT, piece), None, "{effects:?}");
}

// A holder's front lost, and the damage heard as its owner hears it.
fn front_lost(tree: &mut Tree, holder: MemberId, pieces: &mut Pieces) {
    let mut effects = Vec::new();
    let front = ByteRange::new(0, 5);
    tree.damage(NodeId::ROOT, VolumeIndex(holder.0), front, &mut effects);
    for effect in effects.clone() {
        if let Effect::Damaged {
            node,
            member,
            range,
        } = effect
        {
            tree.damage_reached(node, member, range, pieces, &mut effects);
        }
    }
}

// Every middle place held, then a restart that kept no front. `lost` of
// the holders lose theirs; the others' come again and differ from the
// piece's. A place whose holder's front was lost is open as a free one is:
// one such takes the piece and displaces its holder, two leave it
// unclaimed. The piece comes before the restart (its front coming again
// before the losses), after it, or after the losses; its length known, so
// that it is decided and no object owns it, or not.
#[test]
fn a_place_whose_holders_front_was_lost_is_open() {
    let held: [(&str, &[u8]); 3] = [
        ("set.002", b"AAAAA"),
        ("set.003", b"DDDDD"),
        ("set.004", b"CCCCC"),
    ];
    let post = |tree: &mut Tree, length: Option<u64>, pieces: &mut Pieces| {
        let piece = tree.announce_root("x.bin", length, &mut Vec::new());
        front(tree, piece, b"BBBBB", pieces);
        piece
    };
    for lost in [1, 2] {
        for when in ["before the restart", "after it", "after the losses"] {
            for length in [None, Some(5)] {
                let case = format!("lost {lost}, piece {when}, length {length:?}");
                let pieces = &mut Pieces(3);
                let (mut tree, store, members) = set(pieces, &held);
                let mut piece = None;
                if when == "before the restart" {
                    piece = Some(post(&mut tree, length, pieces));
                }
                let mut tree = Tree::rebuild(&tree.records(), &*pieces).unwrap();
                if when == "after it" {
                    piece = Some(post(&mut tree, length, pieces));
                }
                for (member, (_, bytes)) in members.iter().zip(held).take(3 - lost) {
                    front(&mut tree, *member, bytes, pieces);
                }
                if let (Some(piece), "before the restart") = (piece, when) {
                    front(&mut tree, piece, b"BBBBB", pieces);
                    let decided = length.is_some();
                    assert_eq!(!tree.is_undecided(NodeId::ROOT, piece), decided, "{case}");
                }
                for member in &members[3 - lost..] {
                    front_lost(&mut tree, *member, pieces);
                }
                let piece = piece.unwrap_or_else(|| post(&mut tree, length, pieces));
                if lost == 1 {
                    assert_eq!(
                        tree.child_of(NodeId::ROOT, piece),
                        Some((store, VolumeIndex(3))),
                        "{case}"
                    );
                    assert_eq!(tree.child_of(NodeId::ROOT, members[2]), None, "{case}");
                } else {
                    assert_eq!(tree.child_of(NodeId::ROOT, piece), None, "{case}");
                    for (index, member) in members.iter().enumerate() {
                        assert_eq!(
                            tree.child_of(NodeId::ROOT, *member),
                            Some((store, VolumeIndex(index as u32 + 1))),
                            "{case}"
                        );
                    }
                }
            }
        }
    }
}
