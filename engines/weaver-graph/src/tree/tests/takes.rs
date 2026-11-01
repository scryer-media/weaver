// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store its bytes founded, renamed, takes from a store beside it only the
// volumes that store says it can carry: a volume whose bytes route output
// stays where it is, and the store holding it hears nothing. A store that
// refuses its set gives back first the volumes it can carry, which a store
// beside it may take; only what none takes is refused with the set.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts};

// How many volumes the store beside the renamed one holds.
const VOLUMES: u32 = 200;

// A pretend store whose volumes are `<stem>.partNNN.rar`, its stem the
// first volume's. Its `loose` volume is one its headers have not mapped
// yet, and can travel; every other routes output once bound and carries
// nothing. A taker also places any name, or any volume handed to it, at
// its first free place, and one that reclaims places any volume handed to
// it there. One that refuses gives its set up once the job is quiet.
#[derive(Default, Serialize, Deserialize)]
struct Holder {
    taker: bool,
    reclaims: bool,
    refuses: bool,
    loose: Option<VolumeIndex>,
    stem: Option<String>,
    held: BTreeMap<VolumeIndex, String>,
    mapped: BTreeSet<VolumeIndex>,
}

impl Durable for Holder {
    const VERSION: u16 = 1;
}

impl Holder {
    fn free(&self) -> VolumeIndex {
        (1..)
            .map(VolumeIndex)
            .find(|volume| !self.held.contains_key(volume))
            .expect("a free place")
    }

    fn place(&self, name: &str) -> Option<VolumeIndex> {
        let (stem, part) = name.strip_suffix(".rar")?.rsplit_once(".part")?;
        let number = part.parse::<u32>().ok()?.checked_sub(1)?;
        self.stem
            .as_deref()
            .is_none_or(|own| own == stem)
            .then_some(VolumeIndex(number))
    }
}

impl Object for Holder {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::RarStore, self)
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
        match self.place(name) {
            Some(place) => vec![place],
            None if self.taker => vec![self.free()],
            None => Vec::new(),
        }
        .into()
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        if volume == VolumeIndex(0) {
            self.stem = name
                .strip_suffix(".rar")
                .and_then(|name| name.rsplit_once(".part"))
                .map(|(stem, _)| stem.to_owned());
        }
        self.held.insert(volume, name.to_owned());
        if self.loose != Some(volume) {
            self.mapped.insert(volume);
        }
    }
    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        if self.mapped.contains(&volume) {
            return None;
        }
        self.held
            .get(&volume)
            .map(|name| name as &(dyn std::any::Any + Send + Sync))
    }
    fn carriable(&self, out: &mut Vec<VolumeIndex>) {
        out.extend(
            self.held
                .keys()
                .filter(|volume| !self.mapped.contains(volume)),
        );
    }
    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.carried(volume)?;
        self.held
            .remove(&volume)
            .map(|name| Box::new(name) as Carried)
    }
    fn taken(&mut self, volume: VolumeIndex) {
        self.held.remove(&volume);
        self.mapped.remove(&volume);
    }
    fn reclaim(
        &self,
        _: &str,
        _: &[u8],
        _: Option<u64>,
        carried: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        let Some(name) = carried.downcast_ref::<String>() else {
            return Claim::default();
        };
        match self.place(name) {
            Some(place) => vec![place],
            None if self.taker || self.reclaims => vec![self.free()],
            None => Vec::new(),
        }
        .into()
    }
    fn quiet(&mut self, _: &dyn crate::object::Standing, out: &mut Vec<Emit>) {
        if self.refuses {
            out.push(Emit::Refuse(Why::FirstBytesLost));
        }
    }
}

// Builds the stores in the order they are founded.
struct Stores(Vec<Holder>);

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore && !self.0.is_empty())
            .then(|| Box::new(self.0.remove(0)) as Box<dyn Object>)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A volume posted at the root, its front a signature.
fn post(tree: &mut Tree, name: &str, stores: &mut Stores) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(SIGNATURE).into(),
            provenance: Provenance::Fresh,
        },
        stores,
        &mut Vec::new(),
    );
    member
}

fn record(tree: &Tree, node: NodeId) -> crate::NodeRecord {
    tree.records()
        .nodes
        .into_iter()
        .find(|record| record.node == node)
        .expect("recorded")
}

// A store whose every volume routes output, then a second store beside it
// founded by its bytes, whose first volume a recovery set renames.
fn beside_mapped(stores: &mut Stores) -> (Tree, NodeId, Vec<MemberId>, MemberId) {
    let mut tree = Tree::new();
    let mapped: Vec<MemberId> = (1..=VOLUMES)
        .map(|number| post(&mut tree, &format!("big.part{number:03}.rar"), stores))
        .collect();
    let (big, _) = tree.child_of(NodeId::ROOT, mapped[0]).expect("bound");
    for (index, member) in mapped.iter().enumerate() {
        assert_eq!(
            tree.child_of(NodeId::ROOT, *member),
            Some((big, VolumeIndex(index as u32)))
        );
    }
    let renamed = post(&mut tree, "small.part001.rar", stores);
    (tree, big, mapped, renamed)
}

fn stores() -> Stores {
    Stores(vec![
        Holder {
            loose: Some(VolumeIndex(VOLUMES)),
            ..Holder::default()
        },
        Holder {
            taker: true,
            ..Holder::default()
        },
    ])
}

#[test]
fn a_rename_beside_mapped_volumes_moves_nothing_else() {
    let stores = &mut stores();
    let (mut tree, big, mapped, renamed) = beside_mapped(stores);
    let (small, _) = tree.child_of(NodeId::ROOT, renamed).expect("bound");
    assert_ne!(small, big);
    let before = record(&tree, big);
    let mut effects = Vec::new();
    tree.rename(
        NodeId::ROOT,
        renamed,
        "other.part001.rar",
        stores,
        &mut effects,
    );
    for (index, member) in mapped.iter().enumerate() {
        assert_eq!(
            tree.child_of(NodeId::ROOT, *member),
            Some((big, VolumeIndex(index as u32))),
            "volume {index} keeps its slot"
        );
    }
    assert!(
        record(&tree, big) == before,
        "the store beside is unchanged: {effects:?}"
    );
    assert_eq!(
        tree.volumes(small).map(|(_, volumes)| volumes.len()),
        Some(1),
        "the renamed store took nothing"
    );
}

// The same rename with one volume beside it its headers have not mapped:
// that volume, and only it, moves to the renamed store.
#[test]
fn a_rename_takes_only_the_volume_its_sibling_can_carry() {
    let stores = &mut stores();
    let (mut tree, big, mapped, renamed) = beside_mapped(stores);
    let (small, _) = tree.child_of(NodeId::ROOT, renamed).expect("bound");
    let loose = post(
        &mut tree,
        &format!("big.part{:03}.rar", VOLUMES + 1),
        stores,
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, loose),
        Some((big, VolumeIndex(VOLUMES)))
    );
    let mut effects = Vec::new();
    tree.rename(
        NodeId::ROOT,
        renamed,
        "other.part001.rar",
        stores,
        &mut effects,
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, loose),
        Some((small, VolumeIndex(1))),
        "{effects:?}"
    );
    for (index, member) in mapped.iter().enumerate() {
        assert_eq!(
            tree.child_of(NodeId::ROOT, *member),
            Some((big, VolumeIndex(index as u32))),
            "volume {index} keeps its slot"
        );
    }
    let given: Vec<&Effect> = effects
        .iter()
        .filter(|effect| matches!(effect, Effect::GivenBack { .. }))
        .collect();
    assert_eq!(
        given,
        vec![&Effect::GivenBack {
            node: NodeId::ROOT,
            member: loose,
            from: big,
            volume: VolumeIndex(VOLUMES),
        }]
    );
}

// A store founded on `x.part001.rar`, whose headers map it, and beside it
// a store founded on `y.part002.rar` alone, which refuses its set once the
// job is quiet. The second's volume is loose when `loose`, and the first
// takes a volume handed to it when `reclaims`.
fn beside_refusing(loose: bool, reclaims: bool) -> (Tree, Stores, NodeId, NodeId, MemberId) {
    let mut stores = Stores(vec![
        Holder {
            reclaims,
            ..Holder::default()
        },
        Holder {
            refuses: true,
            stem: Some("y".to_owned()),
            loose: loose.then_some(VolumeIndex(1)),
            ..Holder::default()
        },
    ]);
    let mut tree = Tree::new();
    let first = post(&mut tree, "x.part001.rar", &mut stores);
    let (kept, _) = tree.child_of(NodeId::ROOT, first).expect("bound");
    let second = post(&mut tree, "y.part002.rar", &mut stores);
    let (refusing, volume) = tree.child_of(NodeId::ROOT, second).expect("bound");
    assert_ne!(refusing, kept);
    assert_eq!(volume, VolumeIndex(1));
    (tree, stores, kept, refusing, second)
}

// The refused set's loose volume goes to the store beside it that takes it,
// as any volume given back goes, and nothing is refused.
#[test]
fn a_refused_set_gives_back_what_it_can_carry_to_the_store_beside_it() {
    let (mut tree, mut stores, kept, refusing, second) = beside_refusing(true, true);
    let mut effects = Vec::new();
    tree.quiet(refusing, &mut stores, &mut effects);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((kept, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(effects.contains(&Effect::GivenBack {
        node: NodeId::ROOT,
        member: second,
        from: refusing,
        volume: VolumeIndex(1),
    }));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::Refused { .. })),
        "{effects:?}"
    );
    assert!(tree.volumes(refusing).is_none(), "the emptied set goes");
}

// A loose volume no store beside it takes is refused with the set, for the
// set's reason.
#[test]
fn a_volume_given_back_that_nothing_takes_is_refused_with_its_set() {
    let (mut tree, mut stores, _, refusing, second) = beside_refusing(true, false);
    let mut effects = Vec::new();
    tree.quiet(refusing, &mut stores, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None);
    assert!(effects.contains(&Effect::GivenBack {
        node: NodeId::ROOT,
        member: second,
        from: refusing,
        volume: VolumeIndex(1),
    }));
    assert!(
        effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: second,
            why: Why::FirstBytesLost,
        }),
        "{effects:?}"
    );
}

// A store founded on `y.part002.rar`, whose headers map it and which
// refuses its set once the job is quiet, and beside it a store founded on
// `x.part001.rar`, which takes any volume offered when `taker`.
fn refusing_mapped(taker: bool) -> (Tree, Stores, NodeId, NodeId, MemberId) {
    let mut stores = Stores(vec![
        Holder {
            refuses: true,
            stem: Some("y".to_owned()),
            ..Holder::default()
        },
        Holder {
            taker,
            ..Holder::default()
        },
    ]);
    let mut tree = Tree::new();
    let second = post(&mut tree, "y.part002.rar", &mut stores);
    let (refusing, volume) = tree.child_of(NodeId::ROOT, second).expect("bound");
    assert_eq!(volume, VolumeIndex(1));
    let first = post(&mut tree, "x.part001.rar", &mut stores);
    let (beside, _) = tree.child_of(NodeId::ROOT, first).expect("bound");
    assert_ne!(beside, refusing);
    (tree, stores, beside, refusing, second)
}

// A mapped volume of a refused set, once read back into its member's own
// file, is offered to the store beside the set with nothing carried, and
// the set left with no volume goes.
#[test]
fn a_refused_set_gives_back_a_mapped_volume_once_read_back() {
    let (mut tree, mut stores, beside, refusing, second) = refusing_mapped(true);
    let mut effects = Vec::new();
    tree.quiet(refusing, &mut stores, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    tree.bury(NodeId::ROOT, second, &mut effects);
    assert_eq!(
        tree.child_of(NodeId::ROOT, second),
        Some((beside, VolumeIndex(1))),
        "{effects:?}"
    );
    assert!(tree.volumes(refusing).is_none(), "the emptied set goes");
}

// A read-back volume no store beside the set takes stays the refused set's.
#[test]
fn a_read_back_volume_nothing_takes_stays_with_its_refused_set() {
    let (mut tree, mut stores, _, refusing, second) = refusing_mapped(false);
    let mut effects = Vec::new();
    tree.quiet(refusing, &mut stores, &mut effects);
    tree.bury(NodeId::ROOT, second, &mut effects);
    assert_eq!(tree.child_of(NodeId::ROOT, second), None, "{effects:?}");
    assert_eq!(
        tree.volumes(refusing).map(|(_, volumes)| volumes.len()),
        Some(1),
        "the set keeps its volume"
    );
}

// A volume whose bytes route output is not given back: it is refused with
// the set, and the store beside it never hears of it.
#[test]
fn a_refused_set_keeps_the_volumes_it_cannot_carry() {
    let (mut tree, mut stores, _, refusing, second) = beside_refusing(false, true);
    let mut effects = Vec::new();
    tree.quiet(refusing, &mut stores, &mut effects);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::GivenBack { .. })),
        "{effects:?}"
    );
    assert!(
        effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: second,
            why: Why::FirstBytesLost,
        }),
        "{effects:?}"
    );
}
