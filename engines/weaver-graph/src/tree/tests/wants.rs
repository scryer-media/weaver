// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member no object places, where an object says which bytes of it would:
// the tree asks for them at the member's source, keeps them as they come,
// and offers the member again with them. Bytes that say nothing are heard
// once; a range nothing will bring is asked for no more.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts};

const LENGTH: u64 = 100;
const TAIL: u64 = 4;
const NUMBERED: &[u8] = b"#001";

// A pretend store. `first` (or `first.rar`) is its first volume; any other member it places
// at 1 only when the last four bytes it is offered with say `#001`. Short of
// them it asks for them, unless `mute`, or, when `parked_only`, unless the
// member was given back; with others it places nothing. A `giver` gives
// back a volume past its front. One that `owns` a name places that name at
// 0 and nothing else, carries none of its volumes, and, when `refuses`,
// gives its set up once the job is quiet. Every tail it was offered with is
// kept in `told`. One that is `locked` asks for nothing until it is offered
// a password, which it says nothing of.
#[derive(Default, Serialize, Deserialize)]
struct Tailed {
    giver: bool,
    locked: bool,
    keyed: bool,
    mute: bool,
    parked_only: bool,
    owns: Option<String>,
    refuses: bool,
    held: BTreeMap<VolumeIndex, String>,
    #[serde(skip)]
    told: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Durable for Tailed {
    const VERSION: u16 = 1;
}

impl Tailed {
    fn answer(&self, name: &str, tail: &[u8], length: Option<u64>, parked: bool) -> Claim {
        if let Some(owns) = &self.owns {
            return match name == owns {
                true => vec![VolumeIndex(0)].into(),
                false => Claim::default(),
            };
        }
        if matches!(name, "first" | "first.rar") {
            return vec![VolumeIndex(0)].into();
        }
        if !tail.is_empty() {
            self.told.lock().expect("told").push(tail.to_vec());
        }
        match (tail, length) {
            (NUMBERED, _) => vec![VolumeIndex(1)].into(),
            ([], Some(length))
                if !self.mute
                    && !self.giver
                    && (parked || !self.parked_only)
                    && (!self.locked || self.keyed) =>
            {
                Claim::Wants(ByteRange::new(length - TAIL, length))
            }
            _ => Claim::default(),
        }
    }
}

impl Object for Tailed {
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::RarStore, self)
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if self.giver && delivery.offset > 0 {
            out.push(Emit::GiveBack(delivery.volume));
        }
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
    fn claim(&mut self, name: &str, clean: crate::object::Clean<'_>, length: Option<u64>) -> Claim {
        self.answer(name, clean.tail, length, false)
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        self.held.insert(volume, name.to_owned());
    }
    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        if self.owns.is_some() {
            return None;
        }
        self.held
            .get(&volume)
            .map(|name| name as &(dyn std::any::Any + Send + Sync))
    }
    fn carriable(&self, _: &mut Vec<VolumeIndex>) {}
    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.carried(volume)?;
        self.held
            .remove(&volume)
            .map(|name| Box::new(name) as Carried)
    }
    fn quiet(&mut self, _: &dyn crate::object::Standing, out: &mut Vec<Emit>) {
        if self.refuses {
            out.push(Emit::Refuse(Why::FirstBytesLost));
        }
    }
    fn taken(&mut self, volume: VolumeIndex) {
        self.held.remove(&volume);
    }
    fn offer_password(&mut self, _: &str, _: &mut Vec<Emit>) {
        self.keyed = true;
    }
    fn reclaim_told(
        &self,
        name: &str,
        _: &[u8],
        tail: &[u8],
        length: Option<u64>,
        _: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        self.answer(name, tail, length, true)
    }
}

// Builds the stores in the order they are founded, then a giver.
struct Stores(Vec<Tailed>);

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            let set = match self.0.is_empty() {
                true => Tailed {
                    giver: true,
                    ..Tailed::default()
                },
                false => self.0.remove(0),
            };
            Box::new(set) as Box<dyn Object>
        })
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn bytes(
    tree: &mut Tree,
    member: MemberId,
    (offset, bytes): (u64, &'static [u8]),
    stores: &mut Stores,
) -> Vec<Effect> {
    let mut effects = Vec::new();
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::from_static(bytes).into(),
            provenance: Provenance::Fresh,
        },
        stores,
        &mut effects,
    );
    effects
}

fn post(tree: &mut Tree, name: &str, front: &'static [u8], stores: &mut Stores) -> MemberId {
    let member = tree.announce_root(name, Some(LENGTH), &mut Vec::new());
    bytes(tree, member, (0, front), stores);
    member
}

// The tail's bytes, as the source brings them.
fn tail(
    tree: &mut Tree,
    member: MemberId,
    said: &'static [u8],
    stores: &mut Stores,
) -> Vec<Effect> {
    bytes(tree, member, (LENGTH - TAIL, said), stores)
}

fn wants(tree: &Tree) -> Vec<(NodeId, Seek)> {
    let mut out = Vec::new();
    tree.collect_wants(&mut out);
    out
}

fn the_ask(member: MemberId) -> (NodeId, Seek) {
    (
        NodeId::ROOT,
        Seek::data(
            VolumeIndex(member.0),
            ByteRange::new(LENGTH - TAIL, LENGTH),
            Weight::FIRST,
        ),
    )
}

fn bound_at(effects: &[Effect], member: MemberId, child: NodeId) -> bool {
    effects.iter().any(|effect| {
        matches!(effect, Effect::Bound { node: NodeId::ROOT, member: bound, child: into, .. }
            if *bound == member && *into == child)
    })
}

// A store holding `first`, with what it is told shared.
fn holding(
    tree: &mut Tree,
    holder: Tailed,
    stores: &mut Stores,
) -> (NodeId, Arc<Mutex<Vec<Vec<u8>>>>) {
    let told = Arc::clone(&holder.told);
    stores.0.insert(0, holder);
    let first = post(tree, "first", SIGNATURE, stores);
    let (child, volume) = tree.child_of(NodeId::ROOT, first).expect("founds");
    assert_eq!(volume, VolumeIndex(0));
    (child, told)
}

fn told(told: &Arc<Mutex<Vec<Vec<u8>>>>) -> Vec<Vec<u8>> {
    told.lock().expect("told").clone()
}

// (i) The store answers no place for a leaf and names its last four bytes:
// they are asked for at the member's source, and once they come the member
// is offered again with them, placed, and bound.
#[test]
fn a_member_an_object_asks_the_tail_of_is_placed_once_it_comes() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", b"volume", stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None);
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
    assert_eq!(told(&told_to), vec![NUMBERED.to_vec()]);
    assert!(tree.asked.is_empty(), "owned: the ask is dropped");
    assert!(wants(&tree).is_empty());
}

// A store that asks the tail only of a member given back, so a member it
// is offered fresh founds a store of its own first.
fn asks_parked() -> Tailed {
    Tailed {
        parked_only: true,
        ..Tailed::default()
    }
}

// (ii) A member given back is reclaimed with the tail the same way.
#[test]
fn a_parked_member_is_reclaimed_with_the_tail_it_was_asked_for() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, asks_parked(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    assert!(
        tree.child_of(NodeId::ROOT, second).is_some(),
        "founds a giver"
    );
    bytes(&mut tree, second, (HEADER, b"headers"), stores);
    assert!(tree.given_back(NodeId::ROOT, second), "parked");
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
    assert!(!tree.given_back(NodeId::ROOT, second));
    assert_eq!(told(&told_to), vec![NUMBERED.to_vec()]);
    assert!(tree.asked.is_empty());
}

// (ii) A member given back while its holder, short of a password, asks
// nothing of it is offered to the holder again when the holder is offered
// one, though the holder says nothing of it: the tail is asked for then, and
// the member is reclaimed by it.
#[test]
fn a_parked_member_is_asked_for_its_tail_once_its_holder_is_offered_a_password() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let locked = Tailed {
        locked: true,
        ..asks_parked()
    };
    let (holder, told_to) = holding(&mut tree, locked, stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    bytes(&mut tree, second, (HEADER, b"headers"), stores);
    assert!(tree.given_back(NodeId::ROOT, second), "parked");
    assert!(wants(&tree).is_empty(), "no password, no ask");
    let mut effects = Vec::new();
    tree.offer_password(holder, "secret", stores, &mut effects);
    assert!(tree.given_back(NodeId::ROOT, second), "{effects:?}");
    assert_eq!(wants(&tree), vec![the_ask(second)], "{effects:?}");
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
    assert_eq!(told(&told_to), vec![NUMBERED.to_vec()]);
}

// (ii) A member given back by the very bytes of its tail is asked for the
// tail all the same, inside the take of those bytes: they are kept as the
// ask's, nothing is read back, and the member is reclaimed by them.
#[test]
fn a_tail_that_gives_its_member_back_is_kept_for_the_ask_it_raises() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, asks_parked(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(
        told(&told_to),
        vec![NUMBERED.to_vec()],
        "offered once with it"
    );
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReadOn { .. })),
        "{effects:?}"
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
    assert!(!tree.given_back(NodeId::ROOT, second));
}

// (iii) A tail that says nothing is offered once; the store, told it, asks
// no more, and nothing is sought again.
#[test]
fn a_tail_that_says_nothing_is_offered_once_and_not_sought_again() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (_, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", b"volume", stores);
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    let effects = tail(&mut tree, second, b"none", stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert_eq!(told(&told_to), vec![b"none".to_vec()]);
    assert!(wants(&tree).is_empty(), "no second seek");
    // Offered again later, it is offered with the same tail, and asks
    // nothing.
    let mut effects = Vec::new();
    tree.identify_again(NodeId::ROOT, second, stores, &mut effects);
    assert!(wants(&tree).is_empty());
    assert_eq!(told(&told_to), vec![b"none".to_vec(), b"none".to_vec()]);
}

// (iv) An object that asks for nothing: no entry, no seek.
#[test]
fn a_member_no_object_asks_for_has_no_ask() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let mute = Tailed {
        mute: true,
        ..Tailed::default()
    };
    let (_, told_to) = holding(&mut tree, mute, stores);
    let second = post(&mut tree, "second", b"volume", stores);
    assert!(tree.asked.is_empty());
    assert!(wants(&tree).is_empty());
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert!(told(&told_to).is_empty());
}

// (v) A tail nothing will bring: the ask is kept, lost, so the member is
// not asked again, and the member is identified again as it is when
// nothing asks: a leaf, here, that founds nothing.
#[test]
fn a_lost_tail_is_not_asked_again() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (_, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", b"volume", stores);
    let [(node, seek)] = wants(&tree)[..] else {
        panic!("one ask");
    };
    let mut effects = Vec::new();
    tree.pulled(node, seek, Pull::Lost, stores, &mut effects);
    assert!(tree.asked[&(NodeId::ROOT, second)].lost, "kept, lost");
    assert!(wants(&tree).is_empty());
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert!(!tree.is_undecided(NodeId::ROOT, second), "settled a leaf");
    let mut effects = Vec::new();
    tree.identify_again(NodeId::ROOT, second, stores, &mut effects);
    assert_eq!(tree.asked.len(), 1, "asked once");
    assert!(wants(&tree).is_empty());
    assert!(told(&told_to).is_empty());
}

// An asked member founds nothing while its ask stands, though its front
// would found a store, and the asker places it once the tail comes.
#[test]
fn an_asked_member_founds_nothing_while_its_ask_stands() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "founds nothing");
    assert!(!tree.given_back(NodeId::ROOT, second));
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    // Offered again meanwhile, it still waits.
    let mut effects = Vec::new();
    tree.identify_again(NodeId::ROOT, second, stores, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    let effects = tail(&mut tree, second, NUMBERED, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert_eq!(told(&told_to), vec![NUMBERED.to_vec()]);
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
}

// Lost, an asked member whose front would found a store founds it as it
// does when nothing asks, and is not asked again.
#[test]
fn a_lost_ask_founds_as_today() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "waits");
    let [(node, seek)] = wants(&tree)[..] else {
        panic!("one ask");
    };
    let mut effects = Vec::new();
    tree.pulled(node, seek, Pull::Lost, stores, &mut effects);
    let (founded, volume) = tree
        .child_of(NodeId::ROOT, second)
        .expect("founds a store of its own");
    assert_ne!(founded, holder);
    assert_eq!(volume, VolumeIndex(0));
    assert!(wants(&tree).is_empty(), "not sought again");
    // Bound to its own store, its lost entry leaves with the binding.
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
    assert!(told(&told_to).is_empty());
}

// A volume a refused set kept, read back with its tail in hand, is asked
// for the tail when it is given back; once the tail is read back it is
// given back again with it, and the store beside the set places it.
#[test]
fn a_refused_sets_volume_is_placed_after_its_tail_is_read_back() {
    let refusing = Tailed {
        owns: Some("second".to_owned()),
        refuses: true,
        ..Tailed::default()
    };
    let stores = &mut Stores(vec![refusing, Tailed::default()]);
    let mut tree = Tree::new();
    let second = post(&mut tree, "second", SIGNATURE, stores);
    let (set, volume) = tree.child_of(NodeId::ROOT, second).expect("founds");
    assert_eq!(volume, VolumeIndex(0));
    let first = post(&mut tree, "first", SIGNATURE, stores);
    let (holder, _) = tree.child_of(NodeId::ROOT, first).expect("founds");
    assert_ne!(holder, set);
    tail(&mut tree, second, NUMBERED, stores);
    let mut effects = Vec::new();
    tree.quiet(set, stores, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    let mut effects = Vec::new();
    tree.bury(NodeId::ROOT, second, &mut effects);
    let range = ByteRange::new(LENGTH - TAIL, LENGTH);
    assert!(
        effects.contains(&Effect::ReadOn {
            node: NodeId::ROOT,
            member: second,
            range,
        }),
        "{effects:?}"
    );
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    let mut effects = Vec::new();
    tree.read_on(
        NodeId::ROOT,
        second,
        range.start,
        NUMBERED,
        stores,
        &mut effects,
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(tree.volumes(set).is_none(), "the emptied set goes");
}

// A read-back that empties a refused set removes its node, and says so:
// the owner drops the files of the node's members as after a burial.
#[test]
fn a_read_back_that_removes_a_node_says_it_did() {
    let refusing = Tailed {
        owns: Some("second".to_owned()),
        refuses: true,
        ..Tailed::default()
    };
    let stores = &mut Stores(vec![refusing, Tailed::default()]);
    let mut tree = Tree::new();
    let second = post(&mut tree, "second", SIGNATURE, stores);
    let (set, _) = tree.child_of(NodeId::ROOT, second).expect("founds");
    post(&mut tree, "first", SIGNATURE, stores);
    tail(&mut tree, second, NUMBERED, stores);
    let mut effects = Vec::new();
    tree.quiet(set, stores, &mut effects);
    let mut effects = Vec::new();
    tree.bury(NodeId::ROOT, second, &mut effects);
    let mut effects = Vec::new();
    let removed = tree.read_on(
        NodeId::ROOT,
        second,
        LENGTH - TAIL,
        NUMBERED,
        stores,
        &mut effects,
    );
    assert!(tree.volumes(set).is_none(), "{effects:?}");
    assert!(removed, "{effects:?}");
}

// A read-back that removes no node says so, and the owner does nothing
// more for it: a member it completes founds a store, and none goes.
#[test]
fn a_read_back_that_removes_no_node_says_so() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, _) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    let mut effects = Vec::new();
    let removed = tree.read_on(
        NodeId::ROOT,
        second,
        LENGTH - TAIL,
        b"none",
        stores,
        &mut effects,
    );
    assert!(tree.child_of(NodeId::ROOT, second).is_some(), "{effects:?}");
    assert!(tree.volumes(holder).is_some(), "{effects:?}");
    assert!(!removed, "{effects:?}");
}

// A read-back that returns nothing for bytes asked of a member failed: the
// ask is lost, and the member founds as it does when nothing asks.
#[test]
fn an_empty_read_back_ends_the_ask_and_the_member_founds_as_today() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "waits");
    assert_eq!(wants(&tree), vec![the_ask(second)]);
    let mut effects = Vec::new();
    tree.read_on(
        NodeId::ROOT,
        second,
        LENGTH - TAIL,
        &[],
        stores,
        &mut effects,
    );
    let (founded, volume) = tree
        .child_of(NodeId::ROOT, second)
        .expect("founds a store of its own");
    assert_ne!(founded, holder, "{effects:?}");
    assert_eq!(volume, VolumeIndex(0));
    assert!(wants(&tree).is_empty(), "not sought again");
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
    assert!(told(&told_to).is_empty());
}

// A read-back that makes an ask whole is an arrival: a member no object
// beside it takes with that tail founds through the builder.
#[test]
fn a_member_completed_by_a_read_back_that_none_takes_founds() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    let (holder, told_to) = holding(&mut tree, Tailed::default(), stores);
    let second = post(&mut tree, "second", SIGNATURE, stores);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "waits");
    let mut effects = Vec::new();
    tree.read_on(
        NodeId::ROOT,
        second,
        LENGTH - TAIL,
        b"none",
        stores,
        &mut effects,
    );
    let (founded, volume) = tree
        .child_of(NodeId::ROOT, second)
        .expect("founds a store of its own");
    assert_ne!(founded, holder, "{effects:?}");
    assert_eq!(volume, VolumeIndex(0));
    assert_eq!(told(&told_to), vec![b"none".to_vec()]);
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
}

// A pretend store bound at the root whose volume holds two members,
// `first.rar` and `second.rar`, each opening on a signature: they are
// members of its namespace, not of the root, and open as layers by their
// names.
struct Outer {
    announced: bool,
}

impl Object for Outer {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if std::mem::replace(&mut self.announced, true) {
            return;
        }
        for (member, name) in [(MemberId(0), "first.rar"), (MemberId(1), "second.rar")] {
            out.push(Emit::Member {
                member,
                name: name.into(),
                length: Some(LENGTH),
            });
            out.push(Emit::Bytes {
                member,
                offset: 0,
                bytes: SIGNATURE.to_vec().into(),
                provenance: Provenance::Fresh,
            });
        }
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
}

// Builds an `Outer` first, then the stores.
struct Nest(bool, Stores);

impl Builder for Nest {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        if !std::mem::replace(&mut self.0, true) {
            return (kind == Kind::RarStore)
                .then(|| Box::new(Outer { announced: false }) as Box<dyn Object>);
        }
        self.1.build(kind, id)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A member whose bytes came from below the root is asked nothing, however
// an object answers it: it founds as it does today.
#[test]
fn a_member_not_from_the_root_is_asked_nothing_and_founds_as_today() {
    let holder = Tailed::default();
    let told_to = Arc::clone(&holder.told);
    let nest = &mut Nest(false, Stores(vec![holder]));
    let mut tree = Tree::new();
    let outer = tree.announce_root("outer", Some(LENGTH), &mut Vec::new());
    let mut effects = Vec::new();
    tree.deliver(
        NodeId::ROOT,
        outer,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(SIGNATURE).into(),
            provenance: Provenance::Fresh,
        },
        nest,
        &mut effects,
    );
    let (node, _) = tree.child_of(NodeId::ROOT, outer).expect("outer bound");
    let (holder, volume) = tree.child_of(node, MemberId(0)).expect("first founds");
    assert_eq!(volume, VolumeIndex(0), "{effects:?}");
    let (founded, volume) = tree
        .child_of(node, MemberId(1))
        .expect("second founds a store of its own");
    assert_ne!(founded, holder, "{effects:?}");
    assert_eq!(volume, VolumeIndex(0));
    assert!(tree.asked.is_empty() && tree.sought.is_empty());
    assert!(wants(&tree).iter().all(|(at, _)| *at != NodeId::ROOT));
    assert!(told(&told_to).is_empty());
}

// Where the first effect `matches` sits among `effects`.
fn first_at(effects: &[Effect], matches: impl Fn(&Effect) -> bool) -> Option<usize> {
    effects.iter().position(matches)
}

// A front article of an undecided member raises an ask whose bytes came
// already: the take's own effect for the article goes out before the
// read-back, so the owner holds the article before a read that binds the
// member hands its bytes over.
#[test]
fn a_read_back_an_ask_raises_in_a_take_follows_the_taken_bytes() {
    let stores = &mut Stores(Vec::new());
    let mut tree = Tree::new();
    // The tail comes before any object could ask for it.
    let second = tree.announce_root("second", Some(LENGTH), &mut Vec::new());
    tail(&mut tree, second, NUMBERED, stores);
    let (holder, _) = holding(&mut tree, Tailed::default(), stores);
    assert!(tree.asked.is_empty(), "nothing asked before the front");
    let effects = bytes(&mut tree, second, (0, b"volume"), stores);
    let range = ByteRange::new(LENGTH - TAIL, LENGTH);
    let written = first_at(&effects, |effect| {
        matches!(effect, Effect::WriteLeaf { node: NodeId::ROOT, member, offset: 0, .. }
            if *member == second)
    });
    let read = first_at(&effects, |effect| {
        *effect
            == Effect::ReadOn {
                node: NodeId::ROOT,
                member: second,
                range,
            }
    });
    let (Some(written), Some(read)) = (written, read) else {
        panic!("a write and a read-back: {effects:?}");
    };
    assert!(written < read, "{effects:?}");
    let mut effects = Vec::new();
    tree.read_on(
        NodeId::ROOT,
        second,
        range.start,
        NUMBERED,
        stores,
        &mut effects,
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
}
