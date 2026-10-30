// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store its bytes founded, renamed, takes from a store beside it only the
// volumes that store says it can carry: a volume whose bytes route output
// stays where it is, and the store holding it hears nothing.

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
// its first free place.
#[derive(Default, Serialize, Deserialize)]
struct Holder {
    taker: bool,
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
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Vec<VolumeIndex> {
        match self.place(name) {
            Some(place) => vec![place],
            None if self.taker => vec![self.free()],
            None => Vec::new(),
        }
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
    ) -> Vec<VolumeIndex> {
        let Some(name) = carried.downcast_ref::<String>() else {
            return Vec::new();
        };
        match self.place(name) {
            Some(place) => vec![place],
            None if self.taker => vec![self.free()],
            None => Vec::new(),
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
