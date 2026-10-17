// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The wiring. A tree is the root's namespace plus one node per bound
// member, each node an object with a namespace of its own. The tree does
// four things and knows no format while doing them: it carries bytes down
// into the object that owns a member, it records what each member received,
// it lifts an object's seek up through the forward maps until the root
// answers, and it carries damage down through the inverse maps until a
// namespace with a recovery object that covers it takes over.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::deliver::{Delivery, Emit, Provenance, Why};
use crate::engine::Engine;
use crate::member::{MemberId, Namespace};
use crate::object::{Extent, Kind, Object, ObjectId, Placement};
use crate::recovery::Recovery;
use crate::seek::{Pull, Seek, SeekUp};
use crate::sniff::{Sniff, sniff};
use crate::volume::{ByteRange, VolumeIndex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl NodeId {
    pub const ROOT: NodeId = NodeId(0);
}

// Where a node sits: which members of which namespace are its volumes, by
// volume index. One object may own several members of its parent, one per
// volume: a RAR set's twenty parts are one node with twenty volumes. A map,
// because volumes are claimed as they arrive, and a third part can arrive
// before the second. The root has no parent; its volumes are the source's.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    parent: NodeId,
    volumes: BTreeMap<VolumeIndex, MemberId>,
}

struct Node {
    // `None` for the root, whose namespace is produced by the source, not
    // by an object.
    object: Option<Box<dyn Object>>,
    namespace: Namespace,
    binding: Option<Binding>,
    // Members of this namespace that are volumes of a child node.
    children: BTreeMap<MemberId, (NodeId, VolumeIndex)>,
    // Members whose first bytes have not yet decided an owner. A member not
    // in this set and not in `children` is a leaf.
    undecided: BTreeMap<MemberId, Vec<u8>>,
    // The object gave its volumes up, or one above it did. The node stays
    // only so its owner can read back where its bytes went; it says nothing
    // more, runs nothing and bids for nothing.
    retired: bool,
}

impl Node {
    fn new(object: Option<Box<dyn Object>>, binding: Option<Binding>) -> Self {
        Self {
            object,
            namespace: Namespace::new(),
            binding,
            children: BTreeMap::new(),
            undecided: BTreeMap::new(),
            retired: false,
        }
    }
}

// What the tree tells its owner to do with bytes it cannot place itself:
// write a leaf, start an engine, refuse a member, and so on. The tree never
// touches a file; the owner does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    // Write these bytes of a leaf member of `node`'s namespace.
    WriteLeaf {
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: Bytes,
        provenance: Provenance,
    },
    // A new member of `node`'s namespace was announced.
    Announced {
        node: NodeId,
        member: MemberId,
    },
    // A member of `node`'s namespace is complete.
    Closed {
        node: NodeId,
        member: MemberId,
    },
    // A member was bound to a freshly constructed object of this kind.
    Bound {
        node: NodeId,
        member: MemberId,
        kind: Kind,
        child: NodeId,
    },
    // A member was refused streaming for this reason and is a leaf now. The
    // object that owned it is retired, not gone: the owner materialises the
    // member's bytes through its inverse map, then buries it.
    Refused {
        node: NodeId,
        member: MemberId,
        why: Why,
    },
    // A member is readable only by an engine, and one is bound to it now in
    // place of the retired object; the owner materialises the member's bytes
    // into it through the retired object's inverse map, then buries it.
    NeedsEngine {
        node: NodeId,
        member: MemberId,
        why: Why,
    },
    // A member and everything under it was retracted; the owner drops the
    // files it wrote for that subtree.
    Retracted {
        node: NodeId,
        member: MemberId,
    },
    // The object at `node` wants a password.
    NeedsPassword {
        node: NodeId,
    },
    // The object at `node` reports its own recovery data at this range of
    // its volume; the owner builds a recovery object over it.
    RecoveryAt {
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        kind: crate::deliver::RecoveryKind,
    },
    // Damage reached `node`'s namespace at this member range and nothing
    // there covers it; the owner asks the recovery objects of that
    // namespace, or passes it on.
    Damaged {
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    },
}

// How the tree constructs the object for a kind the sniffer named. Objects
// are built outside the crate's core, so the core never names a format's
// constructor.
pub trait Builder {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>>;
}

pub struct Tree {
    nodes: BTreeMap<NodeId, Node>,
    // Members given up by a retired object, and what they were to it: which
    // node, and which of its volumes. Kept until the owner buries them.
    retired: BTreeMap<(NodeId, MemberId), (NodeId, VolumeIndex)>,
    next_node: u32,
    emits: Vec<Emit>,
    extents: Vec<Extent>,
    placements: Vec<Placement>,
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    pub fn new() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(NodeId::ROOT, Node::new(None, None));
        Self {
            nodes,
            retired: BTreeMap::new(),
            next_node: 1,
            emits: Vec::new(),
            extents: Vec::new(),
            placements: Vec::new(),
        }
    }

    pub fn namespace(&self, node: NodeId) -> Option<&Namespace> {
        self.nodes.get(&node).map(|found| &found.namespace)
    }

    // Run `act` on the recovery object at `node` together with the namespace
    // it lives in, its parent's, where the members it covers are. `None`
    // when the node is the root or its object recovers nothing.
    pub fn with_recovery<R>(
        &mut self,
        node: NodeId,
        act: impl FnOnce(&mut dyn Recovery, &mut Namespace) -> R,
    ) -> Option<R> {
        let found = self.nodes.get_mut(&node)?;
        let parent = found.binding.as_ref()?.parent;
        // The object steps out of its node for the call, so the parent's
        // namespace can be borrowed beside it; it steps back in after.
        let mut object = found.object.take()?;
        let result = match (object.recovery_mut(), self.nodes.get_mut(&parent)) {
            (Some(recovery), Some(parent)) => Some(act(recovery, &mut parent.namespace)),
            _ => None,
        };
        if let Some(found) = self.nodes.get_mut(&node) {
            found.object = Some(object);
        }
        result
    }

    fn object(&self, node: NodeId) -> Option<&dyn Object> {
        self.nodes.get(&node)?.object.as_deref()
    }

    // What the object at `node` is. `None` for the root and for a node
    // whose object is lent out.
    pub fn kind(&self, node: NodeId) -> Option<Kind> {
        self.object(node).map(Object::kind)
    }

    pub fn reads_in_order(&self, node: NodeId) -> bool {
        self.object(node).is_some_and(Object::reads_in_order)
    }

    // The recovery face of the object at `node`, if it has one.
    pub fn recovery(&self, node: NodeId) -> Option<&dyn Recovery> {
        self.object(node)?.recovery()
    }

    pub fn recovers(&self, node: NodeId) -> bool {
        self.recovery(node).is_some()
    }

    // The recovery objects bound in `node`'s namespace, in binding order:
    // the ones that cover its members.
    pub fn recovery_nodes(&self, node: NodeId) -> Vec<NodeId> {
        let mut found = Vec::new();
        if let Some(parent) = self.nodes.get(&node) {
            for (child, _) in parent.children.values() {
                if !found.contains(child) && self.recovers(*child) {
                    found.push(*child);
                }
            }
        }
        found
    }

    // Nothing more will arrive for a member of `node`'s namespace. It takes
    // the length it reached when it declared none, the object it is bound
    // to hears that its volume is closed, and the owner closes its file.
    pub fn close(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        let Some(record) = found.namespace.get_mut(member) else {
            return;
        };
        if record.closed {
            return;
        }
        record.closed = true;
        let reached = record.coverage.runs().last().map_or(0, |run| run.range.end);
        let length = *record.length.get_or_insert(reached);
        if let Some((child, volume)) = found.children.get(&member).copied() {
            let mut emits = std::mem::take(&mut self.emits);
            emits.clear();
            if let Some(object) = self
                .nodes
                .get_mut(&child)
                .and_then(|found| found.object.as_mut())
            {
                object.sized(volume, length, &mut emits);
                object.volume_closed(volume, &mut emits);
            }
            self.apply(child, &mut emits, builder, effects);
            self.emits = emits;
        }
        effects.push(Effect::Closed { node, member });
    }

    // Lend the object at `node` out of the tree, so the owner can run it
    // against the rest of the tree, which stays readable: a recovery object
    // repairing reads the members and volumes around it. While it is out the
    // node answers nothing; `put_object` brings it back.
    pub fn take_object(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        self.nodes.get_mut(&node)?.object.take()
    }

    // Return an object taken with `take_object`. A node retracted meanwhile
    // has nowhere to take it, so the object is dropped with it.
    pub fn put_object(&mut self, node: NodeId, object: Box<dyn Object>) {
        if let Some(found) = self.nodes.get_mut(&node) {
            found.object = Some(object);
        }
    }

    // The namespace `node` was bound in and the members that are its
    // volumes, by volume index. `None` for the root.
    pub fn volumes(&self, node: NodeId) -> Option<(NodeId, &BTreeMap<VolumeIndex, MemberId>)> {
        let binding = self.nodes.get(&node)?.binding.as_ref()?;
        Some((binding.parent, &binding.volumes))
    }

    // The engine face of the object at `node`, if it reads its volumes as
    // files.
    pub fn engine(&self, node: NodeId) -> Option<&dyn Engine> {
        self.object(node)?.engine()
    }

    pub fn engine_mut(&mut self, node: NodeId) -> Option<&mut dyn Engine> {
        self.nodes.get_mut(&node)?.object.as_mut()?.engine_mut()
    }

    // Every live node whose object has an engine face, in node order.
    pub fn engines(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, found)| !found.retired)
            .filter(|(_, found)| found.object.as_ref().is_some_and(|o| o.engine().is_some()))
            .map(|(node, _)| *node)
            .collect()
    }

    // What the object at `node` said outside a delivery: an engine's run
    // says it from the thread it runs on, and its owner hands it here. The
    // emits are applied exactly as if the object had said them in `deliver`.
    pub fn emitted(
        &mut self,
        node: NodeId,
        mut emits: Vec<Emit>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        self.apply(node, &mut emits, builder, effects);
    }

    // A password the job offers the object at `node`. What it says back is
    // applied as if it had said it in `deliver`: another ask, or what the
    // password opened.
    pub fn offer_password(
        &mut self,
        node: NodeId,
        secret: &str,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        if let Some(object) = self
            .nodes
            .get_mut(&node)
            .filter(|found| !found.retired)
            .and_then(|found| found.object.as_mut())
        {
            object.offer_password(secret, &mut emits);
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }

    // What a member of `node`'s namespace was to the object that gave it up:
    // that retired node, and which of its volumes. The node still answers
    // `locate`, and its namespace and children still stand, until `bury`.
    pub fn retired(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.retired.get(&(node, member)).copied()
    }

    // The owner has read the member's bytes back. Once every volume of the
    // retired node is buried, it goes, and everything under it.
    pub fn bury(&mut self, node: NodeId, member: MemberId) {
        let Some((retired, _)) = self.retired.remove(&(node, member)) else {
            return;
        };
        if self.retired.values().all(|(other, _)| *other != retired) {
            self.drop_subtree(retired);
            self.nodes.remove(&retired);
        }
    }

    pub fn namespace_mut(&mut self, node: NodeId) -> Option<&mut Namespace> {
        self.nodes.get_mut(&node).map(|found| &mut found.namespace)
    }

    pub fn child_of(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.nodes.get(&node)?.children.get(&member).copied()
    }

    // Whether the member's first bytes have not yet decided its owner. Its
    // bytes have nowhere final to go, so the owner holds them.
    pub fn is_undecided(&self, node: NodeId, member: MemberId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|found| found.undecided.contains_key(&member))
    }

    // Decide an undecided member as a leaf without waiting for its bytes: the
    // owner could not hold them any longer, or the member ended before its
    // front could say what it is.
    pub fn settle_leaf(&mut self, node: NodeId, member: MemberId) {
        if let Some(found) = self.nodes.get_mut(&node) {
            found.undecided.remove(&member);
        }
    }

    // The inverse map of the object at `node`: where this range of its volume
    // went. Empty for the root, whose volumes are its members.
    pub fn locate(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Placement>,
    ) {
        if let Some(object) = self
            .nodes
            .get(&node)
            .and_then(|found| found.object.as_ref())
        {
            object.locate(volume, range, out);
        }
    }

    // Announce a member of the root namespace: the source's files.
    pub fn announce_root(&mut self, name: &str, length: Option<u64>) -> MemberId {
        self.announce(NodeId::ROOT, name, length)
    }

    fn announce(&mut self, node: NodeId, name: &str, length: Option<u64>) -> MemberId {
        let found = self.nodes.get_mut(&node).expect("announce on a live node");
        let member = found.namespace.announce(name, length);
        found.undecided.insert(member, Vec::new());
        member
    }

    // Bytes for a member of `node`'s namespace. For the root these are the
    // source's deliveries; for any other node they are what its object
    // emitted. The member's owner decides what they are; the tree only
    // routes and records.
    pub fn deliver(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: Bytes,
        provenance: Provenance,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let range = ByteRange::at(offset, bytes.len() as u64);
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        if let Some(record) = found.namespace.get_mut(member) {
            record.coverage.write(range, provenance);
        }
        found.namespace.keep_first_bytes(member, offset, &bytes);
        if found.undecided.contains_key(&member) {
            self.decide(node, member, offset, &bytes, builder, effects);
        }
        let Some(found) = self.nodes.get(&node) else {
            return;
        };
        match found.children.get(&member).copied() {
            Some((child, volume)) => {
                let length = found.namespace.get(member).and_then(|record| record.length);
                let delivery = Delivery {
                    volume,
                    offset,
                    bytes: &bytes,
                    provenance,
                };
                self.deliver_to_object(child, delivery, length, builder, effects);
            }
            None => effects.push(Effect::WriteLeaf {
                node,
                member,
                offset,
                bytes,
                provenance,
            }),
        }
    }

    // The first bytes of an undecided member. First the objects already in
    // this namespace are asked whether the member is one of their volumes;
    // then the sniffer is asked what it is. A member stays undecided until
    // one of them can say, and its bytes are written as a leaf meanwhile,
    // which is right either way: a bound member's bytes are re-delivered to
    // its object from the leaf partial by the owner on `Bound`.
    fn decide(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        let (name, length) = match found.namespace.get(member) {
            Some(record) => (record.name.clone(), record.length),
            None => return,
        };
        let prefix = found
            .undecided
            .get_mut(&member)
            .expect("undecided member has a prefix");
        if offset as usize == prefix.len() {
            prefix.extend_from_slice(bytes);
        } else if offset == 0 {
            prefix.clear();
            prefix.extend_from_slice(bytes);
        } else {
            // Bytes that are not the front yet; the front decides, nothing
            // else does.
            return;
        }
        let prefix = prefix.clone();
        if let Some((child, volume)) = self.claimed_by_sibling(node, &name, &prefix) {
            let parent = self.nodes.get_mut(&node).expect("parent is live");
            parent.undecided.remove(&member);
            parent.children.insert(member, (child, volume));
            let child_node = self.nodes.get_mut(&child).expect("child is live");
            let id = ObjectId(child.0);
            if let Some(binding) = child_node.binding.as_mut() {
                binding.volumes.insert(volume, member);
            }
            let kind = child_node.object.as_mut().map(|object| {
                object.bound(volume, &name);
                object.kind()
            });
            let parent = self.nodes.get_mut(&node).expect("parent is live");
            parent.namespace.bind(member, id);
            if let Some(kind) = kind {
                effects.push(Effect::Bound {
                    node,
                    member,
                    kind,
                    child,
                });
            }
            return;
        }
        let found = self.nodes.get_mut(&node).expect("node is live");
        match sniff(&name, &prefix, length) {
            Sniff::Pending => {}
            Sniff::Leaf => {
                found.undecided.remove(&member);
            }
            Sniff::Object(kind) => {
                found.undecided.remove(&member);
                let volumes = BTreeMap::from([(VolumeIndex(0), member)]);
                match self.attach(node, kind, volumes, builder) {
                    Some(child) => effects.push(Effect::Bound {
                        node,
                        member,
                        kind,
                        child,
                    }),
                    None => effects.push(Effect::Refused {
                        node,
                        member,
                        why: Why::Unsupported,
                    }),
                }
            }
        }
    }

    // Bind members of `node`'s namespace, by the volume each becomes, to a
    // freshly built object of `kind`. `None`, and nothing bound, when no
    // object of that kind can be built.
    fn attach(
        &mut self,
        node: NodeId,
        kind: Kind,
        volumes: BTreeMap<VolumeIndex, MemberId>,
        builder: &mut dyn Builder,
    ) -> Option<NodeId> {
        let child = NodeId(self.next_node);
        // Object ids are node ids: one object per node, for life.
        let id = ObjectId(child.0);
        let mut object = builder.build(kind, id)?;
        self.next_node += 1;
        let parent = self.nodes.get_mut(&node)?;
        for (volume, member) in &volumes {
            if let Some(record) = parent.namespace.get(*member) {
                object.bound(*volume, &record.name);
            }
            parent.namespace.bind(*member, id);
            parent.children.insert(*member, (child, *volume));
        }
        let binding = Binding {
            parent: node,
            volumes,
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        Some(child)
    }

    // Offer a new member to every object already bound in the namespace.
    // The first to claim it wins; objects are asked in the order they were
    // bound, which is arrival order.
    fn claimed_by_sibling(
        &mut self,
        node: NodeId,
        name: &str,
        prefix: &[u8],
    ) -> Option<(NodeId, VolumeIndex)> {
        let siblings: Vec<NodeId> = {
            let found = self.nodes.get(&node)?;
            let mut seen = Vec::new();
            for (child, _) in found.children.values() {
                if !seen.contains(child) {
                    seen.push(*child);
                }
            }
            seen
        };
        for child in siblings {
            if let Some(object) = self
                .nodes
                .get_mut(&child)
                .and_then(|found| found.object.as_mut())
                && let Some(volume) = object.claim(name, prefix)
            {
                return Some((child, volume));
            }
        }
        None
    }

    fn deliver_to_object(
        &mut self,
        node: NodeId,
        delivery: Delivery<'_>,
        length: Option<u64>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        if let Some(object) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.object.as_mut())
        {
            if let Some(length) = length {
                object.sized(delivery.volume, length, &mut emits);
            }
            object.deliver(delivery, &mut emits);
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }

    // What an object said about bytes it received, turned into namespace
    // changes, deliveries to its own members, and effects for the owner.
    fn apply(
        &mut self,
        node: NodeId,
        emits: &mut Vec<Emit>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        // A retired object's word was its last.
        if self.nodes.get(&node).is_none_or(|found| found.retired) {
            emits.clear();
            return;
        }
        for emit in emits.drain(..) {
            match emit {
                Emit::Pending => {}
                Emit::Member {
                    member,
                    name,
                    length,
                } => {
                    let found = self.nodes.get_mut(&node).expect("node is live");
                    // Objects number their own members; the namespace must
                    // agree, so announce until the ids meet.
                    while found.namespace.get(member).is_none() {
                        let announced = found.namespace.announce(&name, length);
                        found.undecided.insert(announced, Vec::new());
                        if announced == member {
                            break;
                        }
                    }
                    // A member announced again after a retraction kept its
                    // record and its name, but not its length.
                    if let Some(record) = found.namespace.get_mut(member) {
                        record.length = record.length.or(length);
                    }
                    effects.push(Effect::Announced { node, member });
                }
                Emit::Bytes {
                    member,
                    offset,
                    bytes,
                    provenance,
                } => {
                    self.deliver(node, member, offset, bytes, provenance, builder, effects);
                }
                Emit::MemberClosed(member) => self.close(node, member, builder, effects),
                Emit::Retract(member) => self.retract(node, member, effects),
                Emit::RecoveryAt {
                    volume,
                    range,
                    kind,
                } => {
                    effects.push(Effect::RecoveryAt {
                        node,
                        volume,
                        range,
                        kind,
                    });
                }
                Emit::Damaged { volume, range } => self.damaged(node, volume, range, effects),
                Emit::NeedsPassword => effects.push(Effect::NeedsPassword { node }),
                Emit::NeedsEngine(why) => self.unbind(node, why, true, builder, effects),
                Emit::Refuse(why) => self.unbind(node, why, false, builder, effects),
            }
            if self.nodes.get(&node).is_none_or(|found| found.retired) {
                // Whatever the object said after giving its volumes up is
                // not heard: the drain drops it.
                return;
            }
        }
    }

    // A member an object produced starts over: whatever node was bound to it
    // is dropped with its subtree, and the member is a fresh, undecided
    // member again.
    fn retract(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        if let Some((child, _)) = self
            .nodes
            .get(&node)
            .and_then(|found| found.children.get(&member).copied())
        {
            self.drop_subtree(child);
            let volumes = self.detach(child);
            if let Some(found) = self.nodes.get_mut(&node) {
                for volume in volumes {
                    found.children.remove(&volume);
                    found.namespace.retract(volume);
                    found.undecided.insert(volume, Vec::new());
                }
            }
        }
        if let Some(found) = self.nodes.get_mut(&node) {
            found.namespace.retract(member);
            found.undecided.insert(member, Vec::new());
        }
        effects.push(Effect::Retracted { node, member });
    }

    // The object at `node` gives its members up: every volume it owned
    // becomes a leaf of the parent namespace, or, when it asked for an
    // engine and its kind has one, the volumes of an engine bound in its
    // place. Either way the object and its subtree retire: they answer where
    // their bytes went until the owner, told once per volume, has read them
    // back and buried them.
    fn unbind(
        &mut self,
        node: NodeId,
        why: Why,
        engine: bool,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(found) = self.nodes.get(&node).filter(|found| !found.retired) else {
            return;
        };
        let Some(binding) = found.binding.clone() else {
            return;
        };
        let kind = found.object.as_ref().map(|object| object.kind());
        self.retire(node);
        let parent = binding.parent;
        if let Some(found) = self.nodes.get_mut(&parent) {
            for (volume, member) in &binding.volumes {
                found.children.remove(member);
                found.namespace.unbind(*member);
                self.retired.insert((parent, *member), (node, *volume));
            }
        }
        let engine = kind
            .and_then(Kind::engine_for)
            .filter(|_| engine)
            .and_then(|kind| self.attach(parent, kind, binding.volumes.clone(), builder));
        for member in binding.volumes.into_values() {
            effects.push(match engine {
                Some(_) => Effect::NeedsEngine {
                    node: parent,
                    member,
                    why,
                },
                None => Effect::Refused {
                    node: parent,
                    member,
                    why,
                },
            });
        }
    }

    fn retire(&mut self, node: NodeId) {
        let mut pending = vec![node];
        while let Some(at) = pending.pop() {
            if let Some(found) = self.nodes.get_mut(&at) {
                found.retired = true;
                pending.extend(found.children.values().map(|(child, _)| *child));
            }
        }
    }

    // Remove a node and return the parent members that were its volumes.
    fn detach(&mut self, node: NodeId) -> Vec<MemberId> {
        self.nodes
            .remove(&node)
            .and_then(|removed| removed.binding)
            .map(|binding| binding.volumes.into_values().collect())
            .unwrap_or_default()
    }

    fn drop_subtree(&mut self, node: NodeId) {
        let mut children: Vec<NodeId> = self
            .nodes
            .get(&node)
            .map(|found| found.children.values().map(|(child, _)| *child).collect())
            .unwrap_or_default();
        children.dedup();
        for child in children {
            self.drop_subtree(child);
            self.nodes.remove(&child);
        }
    }

    // Lift a seek from `node` to the root: the node's volume is a member of
    // its parent's namespace, and the parent object's forward map says
    // which of the parent's volumes hold that range. Repeat until the root,
    // whose volumes are the source's.
    pub fn lift(&mut self, node: NodeId, seek: Seek, root: &mut dyn SeekUp) -> Pull {
        let mut asks = vec![(node, seek)];
        let mut pulled = Pull::Moved(0);
        while let Some((at, ask)) = asks.pop() {
            let Some(found) = self.nodes.get(&at) else {
                continue;
            };
            let Some(binding) = found.binding.as_ref() else {
                pulled = pulled.fold(root.seek(ask));
                continue;
            };
            let parent = binding.parent;
            let Some(member) = binding.volumes.get(&ask.volume).copied() else {
                // A volume the object has not been given yet cannot be
                // asked for by name; it arrives when the parent sees it.
                pulled = pulled.fold(Pull::Unseekable);
                continue;
            };
            let Some(parent_node) = self.nodes.get(&parent) else {
                continue;
            };
            let Some(object) = parent_node.object.as_ref() else {
                // The parent is the root, whose members are the source's
                // volumes in announcement order, so the member is the volume.
                let volume = VolumeIndex(member.0);
                pulled = pulled.fold(root.seek(Seek { volume, ..ask }));
                continue;
            };
            let mut extents = std::mem::take(&mut self.extents);
            extents.clear();
            object.extent(member, ask.range, &mut extents);
            if extents.is_empty() {
                // The parent does not know the member's layout yet, so it
                // cannot say where the bytes lie; the ask waits for the
                // normal order to reach them.
                pulled = pulled.fold(Pull::Unseekable);
            }
            for extent in extents.iter().rev() {
                asks.push((
                    parent,
                    Seek {
                        volume: extent.volume,
                        range: extent.range,
                        ..ask
                    },
                ));
            }
            self.extents = extents;
        }
        pulled
    }

    // Damage at a range of `node`'s volume. The tree asks the node's object
    // where those bytes went and reports damage on each member they landed
    // in; a damaged member bound to a child becomes damage on the child's
    // volume, and so on down. The owner, told of each damaged member, finds
    // the nearest recovery object that covers it.
    pub fn damage(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        effects: &mut Vec<Effect>,
    ) {
        self.descend(vec![(node, volume, range)], effects);
    }

    // An object found its own volume bad: a header checksum failed. The
    // member that volume is in the parent's namespace is marked first, so
    // the recovery beside it repairs those bytes, and the damage then
    // descends as any other.
    fn damaged(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        effects: &mut Vec<Effect>,
    ) {
        let parent = self
            .nodes
            .get(&node)
            .and_then(|found| found.binding.as_ref())
            .and_then(|binding| Some((binding.parent, *binding.volumes.get(&volume)?)));
        match parent {
            Some((parent, member)) => {
                let mut pending = Vec::new();
                self.mark(parent, member, range, effects, &mut pending);
                self.descend(pending, effects);
            }
            None => self.damage(node, volume, range, effects),
        }
    }

    fn descend(
        &mut self,
        mut pending: Vec<(NodeId, VolumeIndex, ByteRange)>,
        effects: &mut Vec<Effect>,
    ) {
        while let Some((at, volume, range)) = pending.pop() {
            let Some(found) = self.nodes.get(&at) else {
                continue;
            };
            let Some(object) = found.object.as_ref() else {
                // The root: the volume is the member.
                let member = MemberId(volume.0);
                self.mark(at, member, range, effects, &mut pending);
                continue;
            };
            let mut placements = std::mem::take(&mut self.placements);
            placements.clear();
            object.locate(volume, range, &mut placements);
            for placement in placements.drain(..) {
                if let Placement::Member { member, range } = placement {
                    self.mark(at, member, range, effects, &mut pending);
                }
            }
            self.placements = placements;
        }
    }

    fn mark(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        effects: &mut Vec<Effect>,
        pending: &mut Vec<(NodeId, VolumeIndex, ByteRange)>,
    ) {
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        if let Some(record) = found.namespace.get_mut(member) {
            record.coverage.write(range, Provenance::Damaged);
        }
        effects.push(Effect::Damaged {
            node,
            member,
            range,
        });
        if let Some((child, volume)) = found.children.get(&member).copied() {
            pending.push((child, volume, range));
        }
    }

    // Every standing seek of every object, lifted. Returns each object's
    // seeks with their answers so the caller can feed `pulled` back.
    pub fn collect_wants(&mut self, out: &mut Vec<(NodeId, Seek)>) {
        let mut seeks = Vec::new();
        for (id, node) in self.nodes.iter_mut().filter(|(_, node)| !node.retired) {
            if let Some(object) = node.object.as_mut() {
                seeks.clear();
                object.wants(&mut seeks);
                out.extend(seeks.iter().map(|seek| (*id, *seek)));
            }
        }
    }

    pub fn pulled(
        &mut self,
        node: NodeId,
        seek: Seek,
        pull: Pull,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        if let Some(object) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.object.as_mut())
        {
            object.pulled(seek, pull, &mut emits);
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deliver::RecoveryKind;

    const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
    const HEADER: u64 = 7;
    const VOLUME_LEN: u64 = 100;

    // A pretend store: a seven-byte header on volume 0, then one member
    // whose bytes run across volumes of a fixed length. It claims `.r00`
    // as its second volume and refuses on a magic word.
    struct Split {
        announced: bool,
        refuse: bool,
    }

    impl Object for Split {
        fn kind(&self) -> Kind {
            Kind::RarStore
        }
        fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
            if delivery.bytes == b"REFUSE" {
                out.push(Emit::Refuse(Why::Compressed));
                return;
            }
            if delivery.bytes == b"ENGINE" {
                out.push(Emit::NeedsEngine(Why::Compressed));
                return;
            }
            if !self.announced {
                self.announced = true;
                out.push(Emit::Member {
                    member: MemberId(0),
                    name: "inner.bin".into(),
                    length: None,
                });
            }
            let base = u64::from(delivery.volume.0) * VOLUME_LEN;
            let start = base + delivery.offset;
            let skip = HEADER
                .saturating_sub(start)
                .min(delivery.bytes.len() as u64);
            if skip == delivery.bytes.len() as u64 {
                return;
            }
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: start + skip - HEADER,
                bytes: delivery.bytes[skip as usize..].to_vec().into(),
                provenance: delivery.provenance,
            });
        }
        fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
        fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
        fn wants(&mut self, out: &mut Vec<Seek>) {
            out.push(Seek::data(
                VolumeIndex(1),
                ByteRange::new(0, 10),
                crate::seek::Weight(1),
            ));
        }
        fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
        fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
            let base = u64::from(volume.0) * VOLUME_LEN;
            let member = ByteRange::new(
                (base + range.start).max(HEADER) - HEADER,
                base + range.end - HEADER,
            );
            out.push(Placement::Member {
                member: MemberId(0),
                range: member,
            });
        }
        fn extent(&self, _: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
            let mut at = range.start + HEADER;
            let end = range.end + HEADER;
            while at < end {
                let volume = VolumeIndex((at / VOLUME_LEN) as u32);
                let stop = end.min((u64::from(volume.0) + 1) * VOLUME_LEN);
                out.push(Extent {
                    volume,
                    range: ByteRange::new(at % VOLUME_LEN, stop - u64::from(volume.0) * VOLUME_LEN),
                });
                at = stop;
            }
        }
        fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
            None
        }
        fn claim(&mut self, name: &str, _: &[u8]) -> Option<VolumeIndex> {
            match name.rsplit_once(".r") {
                Some((_, "00")) => Some(VolumeIndex(1)),
                Some((_, "01")) => Some(VolumeIndex(2)),
                _ => None,
            }
        }
        fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
            if secret == "open" {
                out.push(Emit::Member {
                    member: MemberId(1),
                    name: "opened.bin".into(),
                    length: None,
                });
            } else {
                out.push(Emit::NeedsPassword);
            }
        }
    }

    // A pretend engine: every byte of its volumes is envelope.
    struct Reader(Vec<(VolumeIndex, String)>);

    impl Object for Reader {
        fn kind(&self) -> Kind {
            Kind::RarEngine
        }
        fn deliver(&mut self, _: Delivery<'_>, _: &mut Vec<Emit>) {}
        fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
        fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
        fn wants(&mut self, _: &mut Vec<Seek>) {}
        fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
        fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range,
            });
        }
        fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
        fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
            None
        }
        fn bound(&mut self, volume: VolumeIndex, name: &str) {
            self.0.push((volume, name.to_owned()));
        }
    }

    struct Build;
    impl Builder for Build {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            match kind {
                Kind::RarStore => Some(Box::new(Split {
                    announced: false,
                    refuse: false,
                })),
                Kind::RarEngine => Some(Box::new(Reader(Vec::new()))),
                _ => None,
            }
        }
    }

    struct Root(Vec<Seek>);
    impl SeekUp for Root {
        fn seek(&mut self, seek: Seek) -> Pull {
            self.0.push(seek);
            Pull::Moved(1)
        }
    }

    fn bound_tree() -> (Tree, MemberId, NodeId, Vec<Effect>) {
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let first = tree.announce_root("show.rar", None);
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        tree.deliver(
            NodeId::ROOT,
            first,
            0,
            bytes.into(),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        let child = tree.child_of(NodeId::ROOT, first).expect("bound").0;
        (tree, first, child, effects)
    }

    #[test]
    fn a_member_with_a_signature_is_bound_and_its_payload_flows_to_the_child() {
        let (tree, first, child, effects) = bound_tree();
        assert!(effects.contains(&Effect::Bound {
            node: NodeId::ROOT,
            member: first,
            kind: Kind::RarStore,
            child
        }));
        assert!(effects.contains(&Effect::Announced {
            node: child,
            member: MemberId(0)
        }));
        assert!(effects.contains(&Effect::WriteLeaf {
            node: child,
            member: MemberId(0),
            offset: 0,
            bytes: Bytes::from_static(b"hello"),
            provenance: Provenance::Fresh,
        }));
        assert_eq!(
            tree.namespace(child)
                .unwrap()
                .get(MemberId(0))
                .unwrap()
                .output_name,
            "inner.bin"
        );
    }

    #[test]
    fn a_plain_member_is_a_leaf_of_the_root() {
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let member = tree.announce_root("readme.txt", Some(5));
        tree.deliver(
            NodeId::ROOT,
            member,
            0,
            Bytes::from_static(b"hello"),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![Effect::WriteLeaf {
                node: NodeId::ROOT,
                member,
                offset: 0,
                bytes: Bytes::from_static(b"hello"),
                provenance: Provenance::Fresh
            }]
        );
        assert!(
            tree.namespace(NodeId::ROOT)
                .unwrap()
                .get(member)
                .unwrap()
                .is_complete()
        );
    }

    #[test]
    fn a_sibling_the_object_claims_becomes_its_next_volume() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        let second = tree.announce_root("show.r00", None);
        tree.deliver(
            NodeId::ROOT,
            second,
            0,
            Bytes::from_static(b"world"),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((child, VolumeIndex(1)))
        );
        assert!(effects.contains(&Effect::WriteLeaf {
            node: child,
            member: MemberId(0),
            offset: VOLUME_LEN - HEADER,
            bytes: Bytes::from_static(b"world"),
            provenance: Provenance::Fresh,
        }));
    }

    #[test]
    fn a_seek_is_lifted_through_the_forward_map_to_the_root() {
        let (mut tree, first, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None);
        tree.deliver(
            NodeId::ROOT,
            second,
            0,
            Bytes::from_static(b"w"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        let mut root = Root(Vec::new());
        let seek = Seek::data(
            VolumeIndex(1),
            ByteRange::new(0, 10),
            crate::seek::Weight(1),
        );
        assert_eq!(tree.lift(child, seek, &mut root), Pull::Moved(1));
        assert_eq!(
            root.0,
            vec![Seek {
                volume: VolumeIndex(second.0),
                ..seek
            }]
        );
        let mut wants = Vec::new();
        tree.collect_wants(&mut wants);
        assert_eq!(wants, vec![(child, seek)]);
        let _ = first;
    }

    #[test]
    fn a_seek_for_a_volume_the_object_does_not_have_yet_is_unseekable() {
        let (mut tree, _, child, _) = bound_tree();
        let mut root = Root(Vec::new());
        let seek = Seek::data(
            VolumeIndex(1),
            ByteRange::new(0, 10),
            crate::seek::Weight(1),
        );
        assert_eq!(tree.lift(child, seek, &mut root), Pull::Unseekable);
        assert!(root.0.is_empty());
    }

    #[test]
    fn damage_descends_through_the_inverse_map() {
        let (mut tree, first, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.damage(
            NodeId::ROOT,
            VolumeIndex(first.0),
            ByteRange::new(10, 13),
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![
                Effect::Damaged {
                    node: NodeId::ROOT,
                    member: first,
                    range: ByteRange::new(10, 13)
                },
                Effect::Damaged {
                    node: child,
                    member: MemberId(0),
                    range: ByteRange::new(3, 6)
                },
            ]
        );
        let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
        assert_eq!(
            inner.coverage.damaged_ranges().collect::<Vec<_>>(),
            vec![ByteRange::new(3, 6)]
        );
    }

    #[test]
    fn a_refusal_turns_every_volume_back_into_a_leaf() {
        let (mut tree, first, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None);
        tree.deliver(
            NodeId::ROOT,
            second,
            0,
            Bytes::from_static(b"w"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            first,
            20,
            Bytes::from_static(b"REFUSE"),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![
                Effect::Refused {
                    node: NodeId::ROOT,
                    member: first,
                    why: Why::Compressed
                },
                Effect::Refused {
                    node: NodeId::ROOT,
                    member: second,
                    why: Why::Compressed
                },
            ]
        );
        // Retired, not gone: it still says where its bytes went until the
        // owner has read them back.
        assert_eq!(
            tree.retired(NodeId::ROOT, first),
            Some((child, VolumeIndex(0)))
        );
        assert_eq!(
            tree.retired(NodeId::ROOT, second),
            Some((child, VolumeIndex(1)))
        );
        assert!(tree.namespace(child).is_some());
        assert!(tree.engines().is_empty());
        tree.bury(NodeId::ROOT, first);
        assert!(tree.namespace(child).is_some(), "one volume still unread");
        tree.bury(NodeId::ROOT, second);
        assert!(tree.namespace(child).is_none());
        assert!(
            tree.namespace(NodeId::ROOT)
                .unwrap()
                .get(first)
                .unwrap()
                .is_leaf()
        );
        assert!(
            tree.namespace(NodeId::ROOT)
                .unwrap()
                .get(second)
                .unwrap()
                .is_leaf()
        );
        let _ = RecoveryKind::Par2;
    }

    #[test]
    fn a_volume_claimed_before_the_one_ahead_of_it_leaves_a_gap_until_that_one_comes() {
        let (mut tree, first, child, _) = bound_tree();
        let third = tree.announce_root("show.r01", None);
        let second = tree.announce_root("show.r00", None);
        let deliver = |tree: &mut Tree, member| {
            tree.deliver(
                NodeId::ROOT,
                member,
                0,
                Bytes::from_static(b"w"),
                Provenance::Fresh,
                &mut Build,
                &mut Vec::new(),
            )
        };
        deliver(&mut tree, third);
        let volumes = |tree: &Tree| {
            let (_, volumes) = tree.volumes(child).unwrap();
            volumes.iter().map(|(v, m)| (v.0, *m)).collect::<Vec<_>>()
        };
        assert_eq!(volumes(&tree), vec![(0, first), (2, third)]);
        let seek = Seek::data(
            VolumeIndex(1),
            ByteRange::new(0, 10),
            crate::seek::Weight(1),
        );
        assert_eq!(
            tree.lift(child, seek, &mut Root(Vec::new())),
            Pull::Unseekable
        );
        deliver(&mut tree, second);
        assert_eq!(volumes(&tree), vec![(0, first), (1, second), (2, third)]);
    }

    #[test]
    fn retracts_handed_to_the_tree_start_a_member_over_and_its_length_comes_back() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![Emit::Retract(MemberId(0))],
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![Effect::Retracted {
                node: child,
                member: MemberId(0)
            }]
        );
        let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
        assert_eq!(inner.coverage.bytes_covered(), 0);
        assert!(tree.is_undecided(child, MemberId(0)));

        tree.emitted(
            child,
            vec![Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: Some(5),
            }],
            &mut Build,
            &mut Vec::new(),
        );
        let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
        assert_eq!(
            (inner.length, inner.output_name.as_str()),
            (Some(5), "inner.bin")
        );
        assert_eq!(tree.namespace(child).unwrap().len(), 1);
    }

    fn two_volumes() -> (Tree, MemberId, MemberId, NodeId) {
        let (mut tree, first, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None);
        tree.deliver(
            NodeId::ROOT,
            second,
            0,
            Bytes::from_static(b"w"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        (tree, first, second, child)
    }

    #[test]
    fn a_store_that_needs_an_engine_hands_its_volumes_to_one_engine() {
        let (mut tree, first, second, child) = two_volumes();
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            first,
            20,
            Bytes::from_static(b"ENGINE"),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![
                Effect::NeedsEngine {
                    node: NodeId::ROOT,
                    member: first,
                    why: Why::Compressed
                },
                Effect::NeedsEngine {
                    node: NodeId::ROOT,
                    member: second,
                    why: Why::Compressed
                },
            ]
        );
        let (engine, volume) = tree.child_of(NodeId::ROOT, first).unwrap();
        assert_ne!(engine, child);
        assert_eq!(volume, VolumeIndex(0));
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((engine, VolumeIndex(1)))
        );
        assert_eq!(tree.kind(engine), Some(Kind::RarEngine));
        assert_eq!(
            tree.retired(NodeId::ROOT, first),
            Some((child, VolumeIndex(0)))
        );
        let root = tree.namespace(NodeId::ROOT).unwrap();
        assert_eq!(root.get(first).unwrap().bound_to, Some(ObjectId(engine.0)));
        // The store's own wants went with it; the engine is asked instead.
        let mut wants = Vec::new();
        tree.collect_wants(&mut wants);
        assert!(wants.iter().all(|(node, _)| *node != child));
        tree.bury(NodeId::ROOT, first);
        tree.bury(NodeId::ROOT, second);
        assert!(tree.namespace(child).is_none());
        assert!(tree.namespace(engine).is_some());
    }

    #[test]
    fn an_engine_that_refuses_is_not_rebound() {
        let (mut tree, first, second, _) = two_volumes();
        tree.deliver(
            NodeId::ROOT,
            first,
            20,
            Bytes::from_static(b"ENGINE"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        let (engine, _) = tree.child_of(NodeId::ROOT, first).unwrap();
        tree.bury(NodeId::ROOT, first);
        tree.bury(NodeId::ROOT, second);
        let mut effects = Vec::new();
        tree.emitted(
            engine,
            vec![Emit::NeedsEngine(Why::Unsupported)],
            &mut Build,
            &mut effects,
        );
        assert!(effects.iter().all(|effect| matches!(
            effect,
            Effect::Refused {
                why: Why::Unsupported,
                ..
            }
        )));
        assert_eq!(effects.len(), 2);
        assert!(tree.child_of(NodeId::ROOT, first).is_none());
        assert!(tree.engines().is_empty());
    }

    #[test]
    fn a_retired_object_says_nothing_more() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![
                Emit::Refuse(Why::Compressed),
                Emit::Member {
                    member: MemberId(1),
                    name: "late.bin".into(),
                    length: None,
                },
            ],
            &mut Build,
            &mut effects,
        );
        assert_eq!(effects.len(), 1);
        tree.emitted(
            child,
            vec![Emit::Refuse(Why::Compressed)],
            &mut Build,
            &mut effects,
        );
        assert_eq!(effects.len(), 1, "a second refusal is not heard");
        assert_eq!(tree.namespace(child).unwrap().len(), 1);
    }

    #[test]
    fn a_password_offered_is_answered_as_the_object_says() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.offer_password(child, "wrong", &mut Build, &mut effects);
        assert_eq!(effects, vec![Effect::NeedsPassword { node: child }]);
        effects.clear();
        tree.offer_password(child, "open", &mut Build, &mut effects);
        assert_eq!(
            effects,
            vec![Effect::Announced {
                node: child,
                member: MemberId(1)
            }]
        );
    }
}
