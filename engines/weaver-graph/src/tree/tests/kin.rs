// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member claimed again by what its bytes say is offered to the objects of
// that kind beside it and to the engine of that kind's family, and to nothing
// else. A RAR set handed to its engine keeps taking its later volumes there,
// a restart having forgotten which store handed it over. An object of an
// unrelated kind holding members by their names is offered none of them,
// and an engine of the family holding another set takes none that is not
// its own.

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts, Restore, RestoreError};

// The set's place for a name of the `stem.partNN.rar` kind: `NN`, counted from
// one, when `stem` is the set's.
fn numbered(stem: Option<&str>, name: &str) -> Claim {
    let place = name
        .strip_suffix(".rar")
        .and_then(|rest| rest.rsplit_once(".part"))
        .filter(|(named, _)| stem.is_some_and(|stem| stem == *named))
        .and_then(|(_, digits)| digits.parse::<u32>().ok())
        .and_then(|number| number.checked_sub(1))
        .filter(|index| *index > 0);
    place
        .map(VolumeIndex)
        .into_iter()
        .collect::<Vec<_>>()
        .into()
}

fn stem_of(name: &str) -> Option<String> {
    name.strip_suffix(".rar")
        .and_then(|rest| rest.rsplit_once(".part"))
        .map(|(stem, _)| stem.to_owned())
}

// A pretend RAR store: a set named `stem.partNN.rar`, whose first volume
// opening on `ENGINE` hands the set to its engine, as a compressed set does.
#[derive(Default, Serialize, Deserialize)]
struct Store {
    stem: Option<String>,
}

impl Durable for Store {
    const VERSION: u16 = 1;
}

// A pretend RAR engine over the volumes of one set, by their names.
#[derive(Default, Serialize, Deserialize)]
struct Engine {
    stem: Option<String>,
    bound: Vec<(u32, String)>,
}

impl Durable for Engine {
    const VERSION: u16 = 1;
}

// A pretend set that holds members by their names alone, of a kind no RAR
// volume is: any RAR name is its next volume.
#[derive(Default, Serialize, Deserialize)]
struct ByName {
    held: u32,
}

impl Durable for ByName {
    const VERSION: u16 = 1;
}

macro_rules! envelope_only {
    () => {
        fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
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
    };
}

impl Object for Store {
    envelope_only!();

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
        let engine = delivery.volume == VolumeIndex(0)
            && delivery.offset == 0
            && delivery.bytes.windows(6).any(|bytes| bytes == b"ENGINE");
        if engine {
            out.push(Emit::NeedsEngine(Why::Compressed));
        }
    }
    fn bound(&mut self, _: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        self.stem = self.stem.take().or_else(|| stem_of(name));
    }
    fn claim(&mut self, name: &str, _: crate::object::Clean<'_>, _: Option<u64>) -> Claim {
        numbered(self.stem.as_deref(), name)
    }
}

impl Object for Engine {
    envelope_only!();

    fn kind(&self) -> Kind {
        Kind::RarEngine
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::RarEngine, self)
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        self.stem = self.stem.take().or_else(|| stem_of(name));
        self.bound.push((volume.0, name.to_owned()));
    }
    fn claim(&mut self, name: &str, _: crate::object::Clean<'_>, _: Option<u64>) -> Claim {
        numbered(self.stem.as_deref(), name)
    }
    fn reclaim(
        &self,
        name: &str,
        _: &[u8],
        _: Option<u64>,
        _: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        numbered(self.stem.as_deref(), name)
    }
    fn reads_in_order(&self) -> bool {
        true
    }
}

impl Object for ByName {
    envelope_only!();

    fn kind(&self) -> Kind {
        Kind::Sfv
    }
    fn facts(&self) -> Facts {
        Facts::of(Kind::Sfv, self)
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn bound(&mut self, _: VolumeIndex, _: &str, _: &[u8], _: Option<u64>) {
        self.held += 1;
    }
    fn claim(&mut self, name: &str, _: crate::object::Clean<'_>, _: Option<u64>) -> Claim {
        name.ends_with(".rar")
            .then_some(VolumeIndex(self.held))
            .into_iter()
            .collect::<Vec<_>>()
            .into()
    }
}

// The doubles above for RAR, the parent module's for the rest.
struct Family;

impl Builder for Family {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Store::default())),
            Kind::RarEngine => Some(Box::new(Engine::default())),
            Kind::Sfv => Some(Box::new(ByName::default())),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

impl Restore for Family {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        Ok(match facts.kind {
            Kind::RarStore => Box::new(facts.read::<Store>(Kind::RarStore)?),
            Kind::RarEngine => Box::new(facts.read::<Engine>(Kind::RarEngine)?),
            Kind::Sfv => Box::new(facts.read::<ByName>(Kind::Sfv)?),
            kind => return Err(RestoreError::Unsupported { kind }),
        })
    }
}

// A RAR volume's front: the signature, then `body`.
fn signed(body: &[u8]) -> Bytes {
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(body);
    bytes.into()
}

// A member posted at the root under `name`, with `bytes` as its front.
fn post(tree: &mut Tree, name: &str, bytes: Bytes) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Family,
        &mut Vec::new(),
    );
    member
}

// A recovery set's name for a member, and the claim made again of it when
// its holder lets it go, as the owner makes it once the bytes are back.
fn renamed(tree: &mut Tree, member: MemberId, name: &str) {
    let mut effects = Vec::new();
    tree.rename(NodeId::ROOT, member, name, &mut Family, &mut effects);
    let disclaimed = effects
        .iter()
        .any(|effect| matches!(effect, Effect::Disclaimed { member: was, .. } if *was == member));
    assert!(disclaimed, "the name moves the member: {effects:?}");
    for effect in effects {
        if let Effect::Disclaimed { node, member, why } = effect {
            tree.claim_again(node, member, why, &mut Family, &mut Vec::new());
        }
    }
}

// The RAR set `stem` handed to its engine over its first volume, and the
// tree as a restart rebuilds it: the engine's link to the store that handed
// it the set is not kept. The tree, the engine and the first volume.
fn handed_and_restarted(stem: &str) -> (Tree, NodeId, MemberId) {
    let mut tree = Tree::new();
    let first = post(&mut tree, &format!("{stem}.part01.rar"), signed(b"ENGINE"));
    let (engine, volume) = tree.child_of(NodeId::ROOT, first).expect("bound");
    assert_eq!(
        (tree.kind(engine), volume),
        (Some(Kind::RarEngine), VolumeIndex(0)),
        "the store handed its set to its engine"
    );
    let mut tree = Tree::rebuild(&tree.records(), &Family).unwrap();
    assert_eq!(tree.kind(engine), Some(Kind::RarEngine));
    assert!(
        tree.nodes
            .get(&engine)
            .is_some_and(|found| found.engaged.is_none()),
        "a restart keeps no link from the engine to the store"
    );
    tree.restored(&mut Family, &mut Vec::new());
    (tree, engine, first)
}

// A later volume posted under a name that numbers it as a split piece, so a
// split store holds it by that name, which a recovery set then corrects.
fn later_volume_as_a_piece(tree: &mut Tree) -> MemberId {
    let later = post(
        tree,
        "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.001",
        signed(b"later"),
    );
    let (piece, _) = tree.child_of(NodeId::ROOT, later).expect("held");
    assert_eq!(tree.kind(piece), Some(Kind::Split), "its name holds it");
    later
}

// Shards 80 and 97 of the extended archive campaign: a solid RAR set posted
// under hex names, its first volume in its engine across a restart, and its
// second named by PAR2 only after. The engine takes the second volume; no
// store is founded over it alone, to give it up headless at the quiet pass
// after the engine's run was told nothing more comes.
#[test]
fn a_later_volume_named_after_a_restart_joins_the_engine_its_set_was_handed_to() {
    let (mut tree, engine, _) = handed_and_restarted("silver.horizon");
    let later = later_volume_as_a_piece(&mut tree);
    renamed(&mut tree, later, "silver.horizon.part02.rar");
    assert_eq!(
        tree.child_of(NodeId::ROOT, later),
        Some((engine, VolumeIndex(1))),
        "the engine places it"
    );
    let stores = tree
        .bound_below(NodeId::ROOT)
        .into_iter()
        .filter(|node| tree.kind(*node) == Some(Kind::RarStore))
        .count();
    assert_eq!(stores, 0, "no store is founded over it");
}

// An engine of the family that holds another set is offered the member and
// declines it: the member founds a store of its own kind.
#[test]
fn an_engine_holding_another_set_declines_a_volume_not_its_own() {
    let (mut tree, engine, _) = handed_and_restarted("other.set");
    let later = later_volume_as_a_piece(&mut tree);
    renamed(&mut tree, later, "silver.horizon.part02.rar");
    let (holder, _) = tree.child_of(NodeId::ROOT, later).expect("held");
    assert_ne!(holder, engine, "the engine declines it");
    assert_eq!(tree.kind(holder), Some(Kind::RarStore));
}

// An object of a kind outside the member's family that holds members by
// their names alone, and would take this one, is not offered it.
#[test]
fn an_object_of_an_unrelated_kind_holding_members_by_name_is_not_offered_the_member() {
    let mut tree = Tree::new();
    let listed = post(&mut tree, "listed.bin", Bytes::from_static(b"plain"));
    let bystander = tree
        .attach(
            NodeId::ROOT,
            Kind::Sfv,
            |_, _| Some(BTreeMap::from([(VolumeIndex(0), listed)])),
            &mut Family,
        )
        .unwrap()
        .expect("attached");
    tree.bound(NodeId::ROOT, listed, Kind::Sfv, bystander, &mut Vec::new());
    let later = later_volume_as_a_piece(&mut tree);
    renamed(&mut tree, later, "silver.horizon.part02.rar");
    let (holder, _) = tree.child_of(NodeId::ROOT, later).expect("held");
    assert_ne!(holder, bystander, "the bystander is not offered it");
    assert_eq!(tree.kind(holder), Some(Kind::RarStore));
}
