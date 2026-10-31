// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A numbered name and a signing front name one piece, whichever comes
// first: the piece is the split store's. Posted under its numbered name, it
// founds the split store at once; posted under a name that numbers nothing,
// its bytes found the store they sign, and the numbered name a recovery set
// gives it later ends that hold, the piece read back and claimed again.

use super::*;
use crate::deliver::Posted;

const HASH: &str = "5eed00005eed00005eed00005eed0000";
const NUMBERED: &str = "show.001";

// The piece's front: a store's signature, then some of its body.
fn front() -> Bytes {
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(b"hello");
    bytes.into()
}

// A piece posted at the root under `name`, its front delivered.
fn post(tree: &mut Tree, name: &str, effects: &mut Vec<Effect>) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: front().into(),
            provenance: Provenance::Fresh,
        },
        &mut Build,
        effects,
    );
    member
}

// What the owner does with a member whose reading was refuted: read back,
// then claimed again by the tree.
fn claim_disclaimed(tree: &mut Tree, effects: &[Effect]) {
    for effect in effects {
        if let Effect::Disclaimed { node, member } = effect {
            tree.claim_by_bytes(*node, *member, &mut Build, &mut Vec::new());
        }
    }
}

// Where the piece ends, and which kind holds it.
fn holder(tree: &Tree, member: MemberId) -> (Option<Kind>, VolumeIndex) {
    let (child, volume) = tree.child_of(NodeId::ROOT, member).expect("held");
    (tree.kind(child), volume)
}

#[test]
fn a_numbered_name_before_the_bytes_founds_the_split_store() {
    let mut tree = Tree::new();
    let piece = post(&mut tree, NUMBERED, &mut Vec::new());
    assert_eq!(holder(&tree, piece), (Some(Kind::Split), VolumeIndex(0)));
}

#[test]
fn a_numbered_name_after_the_bytes_moves_the_piece_to_the_split_store() {
    let mut tree = Tree::new();
    let piece = post(&mut tree, HASH, &mut Vec::new());
    let (store, _) = tree.child_of(NodeId::ROOT, piece).expect("founded");
    assert_eq!(
        tree.kind(store),
        Some(Kind::RarStore),
        "its bytes founded it"
    );
    let mut effects = Vec::new();
    tree.rename(NodeId::ROOT, piece, NUMBERED, &mut Build, &mut effects);
    assert!(
        effects.contains(&Effect::Disclaimed {
            node: NodeId::ROOT,
            member: piece,
        }),
        "the store's reading of the piece is refuted: {effects:?}"
    );
    claim_disclaimed(&mut tree, &effects);
    assert_eq!(holder(&tree, piece), (Some(Kind::Split), VolumeIndex(0)));
    assert!(tree.namespace(store).is_none(), "the refuted store goes");
}

// A name that says the kind the bytes founded keeps the hold: nothing is
// read back.
#[test]
fn a_name_that_says_the_owners_kind_keeps_the_piece() {
    let mut tree = Tree::new();
    let piece = post(&mut tree, HASH, &mut Vec::new());
    let (store, _) = tree.child_of(NodeId::ROOT, piece).expect("founded");
    let mut effects = Vec::new();
    tree.rename(NodeId::ROOT, piece, "show.rar", &mut Build, &mut effects);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Disclaimed { .. })),
        "{effects:?}"
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, piece),
        Some((store, VolumeIndex(0)))
    );
}
