// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member given back that no object took is parked beside its namespace's
// stores. A store that answered nothing for it while the headers of its own
// volumes were unread is asked again each time those headers move, and
// takes it where its claim then places it, through the ordinary join.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts};

// A pretend store whose volumes are `<stem>.partNNN.rar`. It places a
// volume at NNN - 1 only once the headers of the volume before that place
// are read: by its first byte past the front. Until then it answers
// nothing for it, by name or by what was parsed of it. A volume past its
// front is walked, and its walk says the headers moved
// (`Emit::HeadersMoved`) unless `silent`. A `giver` gives back a volume
// past its front instead. One that `refuses` gives its set up once the job
// is quiet. Once `late` is set, it reads every volume's headers as read,
// with no word of it.
#[derive(Default, Serialize, Deserialize)]
struct Set {
    stem: String,
    giver: bool,
    refuses: bool,
    silent: bool,
    held: BTreeMap<VolumeIndex, String>,
    walked: BTreeSet<VolumeIndex>,
    #[serde(skip)]
    late: Arc<AtomicBool>,
}

impl Durable for Set {
    const VERSION: u16 = 1;
}

impl Set {
    fn of(stem: &str) -> Self {
        Self {
            stem: stem.to_owned(),
            ..Self::default()
        }
    }

    fn place(&self, name: &str) -> Option<VolumeIndex> {
        let (stem, part) = name.strip_suffix(".rar")?.rsplit_once(".part")?;
        let number = part.parse::<u32>().ok()?.checked_sub(1)?;
        let before = VolumeIndex(number.checked_sub(1)?);
        (stem == self.stem && (self.late.load(Ordering::Relaxed) || self.walked.contains(&before)))
            .then_some(VolumeIndex(number))
    }

    fn places(&self, name: &str) -> Claim {
        self.place(name).into_iter().collect::<Vec<_>>().into()
    }
}

impl Object for Set {
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
        if delivery.offset == 0 {
            return;
        }
        if self.giver {
            out.push(Emit::GiveBack(delivery.volume));
        } else if self.walked.insert(delivery.volume) && !self.silent {
            out.push(Emit::HeadersMoved(delivery.volume));
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
    fn claim(&mut self, name: &str, _: crate::object::Clean<'_>, _: Option<u64>) -> Claim {
        self.places(name)
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        self.held.insert(volume, name.to_owned());
    }
    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.held
            .get(&volume)
            .map(|name| name as &(dyn std::any::Any + Send + Sync))
    }
    fn carriable(&self, _: &mut Vec<VolumeIndex>) {}
    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.held
            .remove(&volume)
            .map(|name| Box::new(name) as Carried)
    }
    fn taken(&mut self, volume: VolumeIndex) {
        self.held.remove(&volume);
    }
    fn reclaim(
        &self,
        _: &str,
        _: &[u8],
        _: Option<u64>,
        carried: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        carried
            .downcast_ref::<String>()
            .map_or_else(Claim::default, |name| self.places(name))
    }
    fn quiet(&mut self, _: &dyn crate::object::Standing, out: &mut Vec<Emit>) {
        if self.refuses {
            out.push(Emit::Refuse(Why::FirstBytesLost));
        }
    }
}

// Builds the stores in the order they are founded, then a fresh store of
// no stem, which places nothing.
struct Stores(Vec<Set>);

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            let set = match self.0.is_empty() {
                true => Set::default(),
                false => self.0.remove(0),
            };
            Box::new(set) as Box<dyn Object>
        })
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn bytes(tree: &mut Tree, member: MemberId, offset: u64, stores: &mut Stores) -> Vec<Effect> {
    let mut effects = Vec::new();
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::from_static(if offset == 0 { SIGNATURE } else { b"headers" }).into(),
            provenance: Provenance::Fresh,
        },
        stores,
        &mut effects,
    );
    effects
}

// A volume posted at the root, its front a signature.
fn post(tree: &mut Tree, name: &str, stores: &mut Stores) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    bytes(tree, member, 0, stores);
    member
}

// The headers of the member's volume are read past its front.
fn walk(tree: &mut Tree, member: MemberId, stores: &mut Stores) -> Vec<Effect> {
    bytes(tree, member, HEADER, stores)
}

// The member, posted when no store places it, founds a store that gives it
// back once its headers are read: it is parked, given back, with no owner.
fn parked(tree: &mut Tree, name: &str, stores: &mut Stores) -> MemberId {
    let member = post(tree, name, stores);
    let (giver, _) = tree.child_of(NodeId::ROOT, member).expect("founds a store");
    walk(tree, member, stores);
    assert!(tree.volumes(giver).is_none(), "the emptied store goes");
    assert_eq!(tree.child_of(NodeId::ROOT, member), None);
    assert!(tree.given_back(NodeId::ROOT, member), "parked");
    member
}

fn bound_at(effects: &[Effect], member: MemberId, child: NodeId) -> bool {
    effects.iter().any(|effect| {
        matches!(effect, Effect::Bound { node: NodeId::ROOT, member: bound, child: into, .. }
            if *bound == member && *into == child)
    })
}

fn giver() -> Set {
    Set {
        giver: true,
        ..Set::default()
    }
}

// (i) A store holds the first volume, its headers unread, and answers
// nothing for the second, which is parked. Once the first volume's headers
// move, the store's claim places the parked member, and it joins there.
#[test]
fn a_parked_member_joins_when_a_holders_headers_move_to_place_it() {
    let stores = &mut Stores(vec![Set::of("s"), giver()]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "s.part001.rar", stores);
    let (holder, volume) = tree.child_of(NodeId::ROOT, first).expect("founds");
    assert_eq!(volume, VolumeIndex(0));
    let second = parked(&mut tree, "s.part002.rar", stores);
    let effects = walk(&mut tree, first, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
    assert!(!tree.given_back(NodeId::ROOT, second));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Refused { .. })),
        "{effects:?}"
    );
}

// (ii) The member is given back while the holder is lent: it queues behind
// the loan and is asked at the take-back, while the holder's headers are
// still unread, and answers nothing. The holder then walks, and the member
// joins it.
#[test]
fn a_member_asked_at_a_take_back_before_the_holder_walks_joins_once_it_walks() {
    let stores = &mut Stores(vec![Set::of("s"), giver()]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "s.part001.rar", stores);
    let (holder, _) = tree.child_of(NodeId::ROOT, first).expect("founds");
    let second = post(&mut tree, "s.part002.rar", stores);
    let (giving, _) = tree.child_of(NodeId::ROOT, second).expect("founds");
    assert_ne!(giving, holder);
    let lent = tree.lend_for_test(holder).expect("lent");
    walk(&mut tree, second, stores);
    assert!(tree.given_back(NodeId::ROOT, second));
    assert!(
        tree.is_undecided(NodeId::ROOT, second),
        "queued behind the loan"
    );
    let mut effects = Vec::new();
    tree.take_back(holder, lent, stores, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert!(tree.given_back(NodeId::ROOT, second), "{effects:?}");
    let effects = walk(&mut tree, first, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, second, holder), "{effects:?}");
}

// (iii) Three volumes held, the third's headers unread; the fourth is
// parked, asked while its left neighbour is unwalked. The neighbour's walk
// places it, and the set holds all four before the namespace stops
// deciding with a place missing.
#[test]
fn a_last_volume_parked_beside_an_unwalked_neighbour_joins_when_it_walks() {
    let stores = &mut Stores(vec![Set::of("s"), giver()]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "s.part001.rar", stores);
    let (holder, _) = tree.child_of(NodeId::ROOT, first).expect("founds");
    walk(&mut tree, first, stores);
    let second = post(&mut tree, "s.part002.rar", stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((holder, VolumeIndex(1)))
    );
    walk(&mut tree, second, stores);
    let third = post(&mut tree, "s.part003.rar", stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, third),
        Some((holder, VolumeIndex(2)))
    );
    let fourth = parked(&mut tree, "s.part004.rar", stores);
    assert_eq!(
        tree.volumes(holder).map(|(_, volumes)| volumes.len()),
        Some(3)
    );
    let effects = walk(&mut tree, third, stores);
    assert_eq!(
        tree.child_of(NodeId::ROOT, fourth),
        Some((holder, VolumeIndex(3))),
        "{effects:?}"
    );
    assert!(bound_at(&effects, fourth, holder), "{effects:?}");
    assert_eq!(
        tree.volumes(holder).map(|(_, volumes)| volumes.len()),
        Some(4),
        "no place missing"
    );
    assert!(!tree.given_back(NodeId::ROOT, fourth));
}

// (iv) A parked member of another set, which the holder never places, stays
// parked through every move of its headers, and the holder still goes
// quiet and refuses its set.
#[test]
fn a_parked_member_no_store_claims_leaves_the_store_to_go_quiet_and_refuse() {
    let mut holder_set = Set::of("s");
    holder_set.refuses = true;
    let stores = &mut Stores(vec![holder_set, giver()]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "s.part001.rar", stores);
    let (holder, _) = tree.child_of(NodeId::ROOT, first).expect("founds");
    let other = parked(&mut tree, "t.part002.rar", stores);
    let effects = walk(&mut tree, first, stores);
    assert!(tree.given_back(NodeId::ROOT, other), "{effects:?}");
    assert_eq!(tree.child_of(NodeId::ROOT, other), None);
    assert!(!bound_at(&effects, other, holder), "{effects:?}");
    let mut effects = Vec::new();
    tree.quiet(holder, stores, &mut effects);
    assert!(
        effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: first,
            why: Why::FirstBytesLost,
        }),
        "{effects:?}"
    );
}

// (iv) With nothing parked, a set missing a volume hears its headers move
// as it would with no word of the move at all: the same effects, and
// nothing given or bound.
#[test]
fn a_header_move_with_nothing_parked_changes_nothing() {
    let run = |silent: bool| {
        let mut set = Set::of("s");
        set.silent = silent;
        let stores = &mut Stores(vec![set]);
        let mut tree = Tree::new();
        let first = post(&mut tree, "s.part001.rar", stores);
        walk(&mut tree, first, stores);
        let third = post(&mut tree, "s.part003.rar", stores);
        let mut effects = walk(&mut tree, third, stores);
        let second = post(&mut tree, "s.part002.rar", stores);
        effects.extend(walk(&mut tree, second, stores));
        let records = tree.records();
        (effects, records.nodes.len(), tree.given.len())
    };
    let heard = run(false);
    assert_eq!(heard, run(true));
    assert_eq!(heard.2, 0, "nothing parked");
}

// (iv) A refused set is not asked again by a move of its headers, though
// its claim would then place the parked member: what it says after its
// refusal is not heard.
#[test]
fn a_refused_set_is_not_asked_by_a_header_move() {
    let mut holder_set = Set::of("s");
    holder_set.refuses = true;
    let late = Arc::clone(&holder_set.late);
    let stores = &mut Stores(vec![holder_set, giver()]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "s.part001.rar", stores);
    let (holder, _) = tree.child_of(NodeId::ROOT, first).expect("founds");
    let second = parked(&mut tree, "s.part002.rar", stores);
    let mut effects = Vec::new();
    tree.quiet(holder, stores, &mut effects);
    assert!(
        effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: first,
            why: Why::FirstBytesLost,
        }),
        "{effects:?}"
    );
    assert!(tree.given_back(NodeId::ROOT, second), "{effects:?}");
    late.store(true, Ordering::Relaxed);
    let mut effects = Vec::new();
    let mut emits = vec![Emit::HeadersMoved(VolumeIndex(0))];
    tree.apply(holder, &mut emits, stores, &mut effects);
    assert!(effects.is_empty(), "{effects:?}");
    assert!(tree.given_back(NodeId::ROOT, second));
    assert_eq!(tree.child_of(NodeId::ROOT, second), None);
}
