// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A repair that rewrites a member reopens the objects whose refusal it
// answers: the one holding the member, by the object's own claim on it, and
// no other, whatever it refused for.

use super::*;
use crate::deliver::Posted;

// A pretend store that holds the volumes of its own base name: `.rar` is
// its first, `.r00` and `.r01` the next.
#[derive(Default)]
struct Based {
    base: Option<String>,
}

impl Object for Based {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

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
            self.base = name.strip_suffix(".rar").map(str::to_owned);
        }
    }
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Claim {
        let Some((base, extension)) = name.rsplit_once('.') else {
            return Claim::default();
        };
        if Some(base) != self.base.as_deref() {
            return Claim::default();
        }
        match extension {
            "r00" => vec![VolumeIndex(1)],
            "r01" => vec![VolumeIndex(2)],
            _ => Vec::new(),
        }
        .into()
    }
}

struct Stores;

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Based::default()) as Box<dyn Object>)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn post(tree: &mut Tree, name: &str, bytes: &'static [u8]) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(bytes).into(),
            provenance: Provenance::Fresh,
        },
        &mut Stores,
        &mut Vec::new(),
    );
    member
}

// Two sets refused, each for a reason that may mean a volume it never had.
// The member the repair rewrote is a volume of the first alone, so only the
// first is opened again; the second keeps its refusal and its volume.
#[test]
fn a_repair_reopens_only_the_refused_set_that_takes_the_rewritten_member() {
    let mut tree = Tree::new();
    let lantern = post(&mut tree, "lantern.rar", SIGNATURE);
    let harbour = post(&mut tree, "harbour.rar", SIGNATURE);
    let (first, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    let (second, _) = tree.child_of(NodeId::ROOT, harbour).unwrap();
    let mut effects = Vec::new();
    tree.emitted(
        first,
        vec![Emit::Refuse(Why::Unmapped)],
        &mut Stores,
        &mut effects,
    );
    tree.emitted(
        second,
        vec![Emit::Refuse(Why::FirstBytesLost)],
        &mut Stores,
        &mut effects,
    );
    // A volume of the first set, heard after it refused, is refused with it.
    let rewritten = post(&mut tree, "lantern.r01", b"w");
    assert_eq!(
        tree.volumes(first)
            .map(|(_, volumes)| volumes.get(&VolumeIndex(2)).copied()),
        Some(Some(rewritten))
    );
    let reopened = tree.reopen(NodeId::ROOT, rewritten, &mut Stores, &mut Vec::new());
    assert_eq!(reopened, vec![rewritten, lantern]);
    assert_eq!(
        tree.kind(second),
        Some(Kind::RarStore),
        "the second set stays"
    );
    assert_eq!(
        tree.volumes(second)
            .map(|(_, volumes)| volumes.get(&VolumeIndex(0)).copied()),
        Some(Some(harbour))
    );
}

// The facts a pretend store hands its engine for each volume, and what the
// engines built adopted, by the store's volume, in order.
type Adopted = std::sync::Arc<std::sync::Mutex<Vec<(VolumeIndex, u32)>>>;

// A pretend store of `.rar` and `.r00` that hands each volume its index,
// plus a hundred, to the engine bound in its place.
#[derive(Default)]
struct Handing(Based);

impl Object for Handing {
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}
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
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.0.locate(volume, range, out);
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn bound(&mut self, volume: VolumeIndex, name: &str, front: &[u8], length: Option<u64>) {
        self.0.bound(volume, name, front, length);
    }
    fn claim(&mut self, name: &str, front: &[u8], length: Option<u64>) -> Claim {
        self.0.claim(name, front, length)
    }
    fn engine_volume(&self, volume: VolumeIndex) -> Option<Carried> {
        Some(Box::new(volume.0 + 100))
    }
}

// A pretend engine: it keeps what each volume carried in, gives it back
// when asked, and records what it adopted.
struct Engine {
    adopted: Adopted,
    carried: BTreeMap<VolumeIndex, u32>,
}

impl Object for Engine {
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarEngine
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
    fn adopt(&mut self, volume: VolumeIndex, _: &str, _: &[u8], _: Option<u64>, carried: Carried) {
        if let Ok(carried) = carried.downcast::<u32>() {
            self.adopted.lock().unwrap().push((volume, *carried));
            self.carried.insert(volume, *carried);
        }
    }
    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.carried
            .remove(&volume)
            .map(|carried| Box::new(carried) as Carried)
    }
}

// Builds stores and engines, and counts each kind it built.
#[derive(Default)]
struct Engines {
    built: std::collections::HashMap<Kind, usize>,
    adopted: Adopted,
}

impl Builder for Engines {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        let object: Box<dyn Object> = match kind {
            Kind::RarStore => Box::new(Handing::default()),
            Kind::RarEngine => Box::new(Engine {
                adopted: std::sync::Arc::clone(&self.adopted),
                carried: BTreeMap::new(),
            }),
            _ => return None,
        };
        *self.built.entry(kind).or_default() += 1;
        Some(object)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn posted(tree: &mut Tree, name: &str, bytes: &'static [u8], build: &mut Engines) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(bytes).into(),
            provenance: Provenance::Fresh,
        },
        build,
        &mut Vec::new(),
    );
    member
}

// What a set came to once the repair reopened it.
#[derive(Debug, PartialEq, Eq)]
struct Reopened {
    // The kind that holds the first volume, and its volumes.
    kind: Option<Kind>,
    volumes: Vec<(VolumeIndex, MemberId)>,
    // What the engine that holds them now adopted.
    adopted: Vec<(VolumeIndex, u32)>,
}

// A two-volume set whose store hands it to an engine `handed` for, whose
// engine then refuses it as damaged, and whose first volume a repair
// rewrites. `again` is what a store bound again over it says, if one is.
fn handed_then_repaired(handed: Why, again: Vec<Emit>) -> (Reopened, Engines, Vec<Effect>) {
    let mut build = Engines::default();
    let mut tree = Tree::new();
    let lantern = posted(&mut tree, "lantern.rar", SIGNATURE, &mut build);
    posted(&mut tree, "lantern.r00", b"w", &mut build);
    let (store, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    tree.emitted(
        store,
        vec![Emit::NeedsEngine(handed)],
        &mut build,
        &mut Vec::new(),
    );
    let (engine, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    assert_eq!(tree.kind(engine), Some(Kind::RarEngine));
    tree.emitted(
        engine,
        vec![Emit::Refuse(Why::Unrepairable)],
        &mut build,
        &mut Vec::new(),
    );
    build.adopted.lock().unwrap().clear();
    let mut effects = Vec::new();
    tree.reopen(NodeId::ROOT, lantern, &mut build, &mut effects);
    let (holder, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    if tree.kind(holder) == Some(Kind::RarStore) && !again.is_empty() {
        tree.emitted(holder, again, &mut build, &mut effects);
    }
    let (holder, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    let reopened = Reopened {
        kind: tree.kind(holder),
        volumes: tree
            .volumes(holder)
            .map(|(_, volumes)| volumes.iter().map(|(v, m)| (*v, *m)).collect())
            .unwrap_or_default(),
        adopted: build.adopted.lock().unwrap().clone(),
    };
    (reopened, build, effects)
}

// A store that handed its set to an engine because the set is compressed
// would hand it over again after any repair: the engine is bound again in
// the refused one's place, holding what the store handed it, and no store
// is built again. What it comes to is what a store bound again and handing
// the set over once more comes to.
#[test]
fn a_repaired_set_its_store_handed_over_as_compressed_binds_its_engine_again() {
    let (direct, build, effects) = handed_then_repaired(Why::Compressed, Vec::new());
    assert_eq!(
        build.built.get(&Kind::RarStore),
        Some(&1),
        "{:?}",
        build.built
    );
    assert_eq!(
        build.built.get(&Kind::RarEngine),
        Some(&2),
        "{:?}",
        build.built
    );
    let bound: Vec<(MemberId, Kind)> = effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Bound { member, kind, .. } => Some((*member, *kind)),
            _ => None,
        })
        .collect();
    assert_eq!(
        bound,
        direct
            .volumes
            .iter()
            .map(|(_, member)| (*member, Kind::RarEngine))
            .collect::<Vec<_>>()
    );
    // The store's way: a set handed over for a reason a repair may change
    // is given to a store again, which hands it over as compressed.
    let (by_store, build, _) =
        handed_then_repaired(Why::HoldsBreached, vec![Emit::NeedsEngine(Why::Compressed)]);
    assert_eq!(
        build.built.get(&Kind::RarStore),
        Some(&2),
        "{:?}",
        build.built
    );
    assert_eq!(direct, by_store);
    assert_eq!(
        direct.adopted,
        vec![(VolumeIndex(0), 100), (VolumeIndex(1), 101)]
    );
}

// An engine bound for a reason a repair may change, and a store that
// refused the set as damaged itself: the repair answers the refusal, so a
// store is bound again over the rewritten volume.
#[test]
fn a_repaired_set_refused_for_what_a_repair_changes_is_given_to_its_store_again() {
    for handed in [Why::HoldsBreached, Why::TailUnreachable, Why::Unsupported] {
        let (reopened, build, _) = handed_then_repaired(handed, Vec::new());
        assert_eq!(reopened.kind, Some(Kind::RarStore), "{handed:?}");
        assert_eq!(build.built.get(&Kind::RarStore), Some(&2), "{handed:?}");
    }
    let mut build = Engines::default();
    let mut tree = Tree::new();
    let lantern = posted(&mut tree, "lantern.rar", SIGNATURE, &mut build);
    let (store, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    tree.emitted(
        store,
        vec![Emit::Refuse(Why::Unrepairable)],
        &mut build,
        &mut Vec::new(),
    );
    tree.reopen(NodeId::ROOT, lantern, &mut build, &mut Vec::new());
    let (holder, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    assert_eq!(tree.kind(holder), Some(Kind::RarStore));
    assert_eq!(
        build.built.get(&Kind::RarStore),
        Some(&2),
        "{:?}",
        build.built
    );
    assert_eq!(build.built.get(&Kind::RarEngine), None, "{:?}", build.built);
}
