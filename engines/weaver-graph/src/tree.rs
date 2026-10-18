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
use crate::names::{Moved, NameLedger, Position};
use crate::object::{Extent, Kind, Object, ObjectId, Placement};
use crate::recovery::Recovery;
use crate::seek::{Pull, Seek, SeekUp};
use crate::sniff::{Sniff, sniff};
use crate::volume::{ByteRange, VolumeIndex};

mod records;

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
//
// A window is a binding to a range of one member, not to the whole of it:
// its one volume is that range, and the member stays its owner's. Recovery
// data an archive carries inside itself is read this way, by an object of
// its own beside the archive's.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    parent: NodeId,
    volumes: BTreeMap<VolumeIndex, MemberId>,
    window: Option<ByteRange>,
}

struct Node {
    // `None` for the root, whose namespace is produced by the source, not
    // by an object.
    object: Option<Box<dyn Object>>,
    namespace: Namespace,
    binding: Option<Binding>,
    // Members of this namespace that are volumes of a child node.
    children: BTreeMap<MemberId, (NodeId, VolumeIndex)>,
    // Windows on members of this namespace, beside their owners.
    windows: BTreeMap<MemberId, Vec<NodeId>>,
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
            windows: BTreeMap::new(),
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
    // The member's output name changed: a member higher in the tree took
    // its name, or it was renamed. The owner moves the file it wrote, if
    // any; these come in an order that never moves a file onto one still
    // in place.
    Renamed {
        node: NodeId,
        member: MemberId,
        old: String,
        new: String,
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
    // Every namespace writes into one directory, so one ledger names them
    // all.
    names: NameLedger,
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
            names: NameLedger::new(),
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
        let mut found = self.bound_below(node);
        found.retain(|child| self.recovers(*child));
        found
    }

    // Every node bound in `node`'s namespace, once each: the owners of its
    // members, then the windows on them.
    fn bound_below(&self, node: NodeId) -> Vec<NodeId> {
        let mut found = Vec::new();
        if let Some(parent) = self.nodes.get(&node) {
            let owners = parent.children.values().map(|(child, _)| child);
            for child in owners.chain(parent.windows.values().flatten()) {
                if !found.contains(child) {
                    found.push(*child);
                }
            }
        }
        found
    }

    // Where a range of `node`'s volume lies in the member it is bound to:
    // the range itself for a whole member, and for a window the range moved
    // to the window's start and clipped at its end, since nothing past the
    // window is the node's. `None` when the range starts past the window.
    pub fn in_member(&self, node: NodeId, range: ByteRange) -> Option<ByteRange> {
        let binding = self.nodes.get(&node)?.binding.as_ref()?;
        let Some(window) = binding.window else {
            return Some(range);
        };
        let start = window.start.saturating_add(range.start);
        let end = window.start.saturating_add(range.end).min(window.end);
        (start <= end).then(|| ByteRange::new(start, end))
    }

    // The range of its member a window is bound to; `None` for any other
    // node.
    fn window(&self, node: NodeId) -> Option<ByteRange> {
        self.nodes.get(&node)?.binding.as_ref()?.window
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
        for window in self.windows_on(node, member) {
            self.close_window(window, builder, effects);
        }
        effects.push(Effect::Closed { node, member });
    }

    fn windows_on(&self, node: NodeId, member: MemberId) -> Vec<NodeId> {
        self.nodes
            .get(&node)
            .and_then(|found| found.windows.get(&member))
            .cloned()
            .unwrap_or_default()
    }

    // The window's member is closed, so its one volume is, as long as the
    // window.
    fn close_window(
        &mut self,
        window: NodeId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(length) = self.window(window).map(|range| range.len()) else {
            return;
        };
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        if let Some(object) = self
            .nodes
            .get_mut(&window)
            .and_then(|found| found.object.as_mut())
        {
            object.sized(VolumeIndex(0), length, &mut emits);
            object.volume_closed(VolumeIndex(0), &mut emits);
        }
        self.apply(window, &mut emits, builder, effects);
        self.emits = emits;
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

    // Every live node, the root first. A node is numbered after the node
    // it was bound in, so every parent comes before its children.
    pub fn live(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, found)| !found.retired)
            .map(|(node, _)| *node)
            .collect()
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
            self.remove_node(retired);
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

    // The bytes posted for this range of the volume of the object at
    // `node`, rebuilt as the object says from what `read` hands back per
    // placement. `false` for the root, and for anything that cannot be read.
    pub fn posted(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.object(node)
            .is_some_and(|object| object.posted(volume, range, read, into))
    }

    // Announce a member of the root namespace: the source's files. A root
    // member outranks every member below it, so one that wants a name a
    // member below holds takes it, and that member's move is an effect.
    pub fn announce_root(
        &mut self,
        name: &str,
        length: Option<u64>,
        effects: &mut Vec<Effect>,
    ) -> MemberId {
        self.announce(NodeId::ROOT, name, length, effects)
    }

    // Announce a member of a live namespace that no object will produce: a
    // file a recovery object there describes and rebuilds. The owner hears
    // it as it hears any announced member. `None` once the node is gone or
    // retired.
    pub fn announce_rebuilt(
        &mut self,
        node: NodeId,
        name: &str,
        length: u64,
        effects: &mut Vec<Effect>,
    ) -> Option<MemberId> {
        self.nodes.get(&node).filter(|found| !found.retired)?;
        let member = self.announce(node, name, Some(length), effects);
        effects.push(Effect::Announced { node, member });
        Some(member)
    }

    fn announce(
        &mut self,
        node: NodeId,
        name: &str,
        length: Option<u64>,
        effects: &mut Vec<Effect>,
    ) -> MemberId {
        let found = self.nodes.get_mut(&node).expect("announce on a live node");
        let member = found.namespace.announce(name, length);
        found.undecided.insert(member, Vec::new());
        self.place(node, member, effects);
        member
    }

    // A member wants another name: a recovery object recognised it, through
    // its namespace or here. The ledger places it again, and every output
    // name that changed is an effect. `None` for a member that is not there.
    pub fn rename(
        &mut self,
        node: NodeId,
        member: MemberId,
        name: &str,
        effects: &mut Vec<Effect>,
    ) -> Option<String> {
        self.namespace_mut(node)?.rename(member, name)?;
        self.place(node, member, effects);
        self.namespace(node)?
            .get(member)
            .map(|record| record.output_name.clone())
    }

    // The ledger again, from what every namespace holds after a rebuild:
    // each member's wanted name and the output name it had. Taken back in
    // order of position, never of arrival, so any order of restoring gives
    // the names that are on disk.
    pub fn rebuild_names(&mut self) {
        self.names.clear();
        let mut members: Vec<(Position, (NodeId, MemberId), String, String)> = Vec::new();
        for (node, found) in &self.nodes {
            for record in found.namespace.members() {
                members.push((
                    self.position(*node, record.id),
                    (*node, record.id),
                    record.name.clone(),
                    record.output_name.clone(),
                ));
            }
        }
        members.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        for (position, holder, wanted, held) in members {
            self.names.restore(holder, position, &wanted, &held);
            let given = self.names.given(holder).map(str::to_owned);
            if let (Some(given), Some(record)) = (
                given,
                self.namespace_mut(holder.0)
                    .and_then(|namespace| namespace.get_mut(holder.1)),
            ) {
                record.output_name = given;
            }
        }
    }

    // Give the member the output name the ledger allocates for the name it
    // wants, and move whichever members that displaced.
    fn place(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        let Some(wanted) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.name.clone())
        else {
            return;
        };
        let position = self.position(node, member);
        let mut moved = Vec::new();
        let given = self
            .names
            .allocate((node, member), position, &wanted, &mut moved);
        if let Some(record) = self
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
        {
            record.output_name = given;
        }
        for Moved { holder, old, new } in moved {
            let (node, member) = holder;
            if let Some(record) = self
                .namespace_mut(node)
                .and_then(|namespace| namespace.get_mut(member))
            {
                record.output_name = new.clone();
            }
            effects.push(Effect::Renamed {
                node,
                member,
                old,
                new,
            });
        }
    }

    // The member ids from the root down to the member. A node stands at the
    // member that is its first volume.
    fn position(&self, node: NodeId, member: MemberId) -> Position {
        let mut path = vec![member];
        let mut at = node;
        while let Some(binding) = self.nodes.get(&at).and_then(|found| found.binding.as_ref()) {
            let Some(first) = binding.volumes.values().next() else {
                break;
            };
            path.push(*first);
            at = binding.parent;
        }
        path.reverse();
        Position(path)
    }

    // A node leaves the tree, and its members' names with it.
    fn remove_node(&mut self, node: NodeId) -> Option<Node> {
        let removed = self.nodes.remove(&node)?;
        for record in removed.namespace.members() {
            self.names.release((node, record.id));
        }
        Some(removed)
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
        for window in self.windows_on(node, member) {
            self.deliver_window(window, offset, &bytes, provenance, builder, effects);
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
                // The member is the volume its new object places it at, the
                // position its set gives it, whichever volume came first.
                let volumes = |object: &mut dyn Object| {
                    let volume = object.claim(&name, &prefix).unwrap_or(VolumeIndex(0));
                    BTreeMap::from([(volume, member)])
                };
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
    // freshly built object of `kind`, which `volumes` may ask where they go.
    // `None`, and nothing bound, when no object of that kind can be built.
    fn attach(
        &mut self,
        node: NodeId,
        kind: Kind,
        volumes: impl FnOnce(&mut dyn Object) -> BTreeMap<VolumeIndex, MemberId>,
        builder: &mut dyn Builder,
    ) -> Option<NodeId> {
        let child = NodeId(self.next_node);
        // Object ids are node ids: one object per node, for life.
        let id = ObjectId(child.0);
        let mut object = builder.build(kind, id)?;
        self.next_node += 1;
        let volumes = volumes(object.as_mut());
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
            window: None,
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        Some(child)
    }

    // Bind `range` of a member of `node`'s namespace, as volume 0, to a
    // freshly built object of `kind`: a window beside the member's owner,
    // which keeps the member. `passed` is what of the range arrived before,
    // read back by the owner, in the member's offsets; the window takes it
    // now, and the rest as it passes. `None`, and nothing bound, when no
    // object of that kind can be built or the range already has a window.
    pub fn attach_window(
        &mut self,
        node: NodeId,
        (member, range): (MemberId, ByteRange),
        kind: Kind,
        passed: Vec<(u64, Bytes, Provenance)>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) -> Option<NodeId> {
        let found = self.nodes.get(&node)?;
        let record = found.namespace.get(member)?;
        let (name, closed) = (record.name.clone(), record.closed);
        let taken = self.windows_on(node, member).into_iter().any(|window| {
            self.nodes
                .get(&window)
                .and_then(|found| found.binding.as_ref())
                .is_some_and(|binding| binding.window == Some(range))
        });
        if taken {
            return None;
        }
        let child = NodeId(self.next_node);
        let mut object = builder.build(kind, ObjectId(child.0))?;
        self.next_node += 1;
        object.bound(VolumeIndex(0), &name);
        let binding = Binding {
            parent: node,
            volumes: BTreeMap::from([(VolumeIndex(0), member)]),
            window: Some(range),
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        self.nodes
            .get_mut(&node)?
            .windows
            .entry(member)
            .or_default()
            .push(child);
        for (offset, bytes, provenance) in passed {
            self.deliver_window(child, offset, &bytes, provenance, builder, effects);
        }
        if closed {
            self.close_window(child, builder, effects);
        }
        Some(child)
    }

    // The part of a delivery to the window's member that falls in the
    // window, delivered to its object at the window's own offsets.
    fn deliver_window(
        &mut self,
        window: NodeId,
        offset: u64,
        bytes: &Bytes,
        provenance: Provenance,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(range) = self.window(window) else {
            return;
        };
        let Some(piece) = range.intersect(ByteRange::at(offset, bytes.len() as u64)) else {
            return;
        };
        let start = (piece.start - offset) as usize;
        let piece_bytes = bytes.slice(start..start + piece.len() as usize);
        let delivery = Delivery {
            volume: VolumeIndex(0),
            offset: piece.start - range.start,
            bytes: &piece_bytes,
            provenance,
        };
        self.deliver_to_object(window, delivery, Some(range.len()), builder, effects);
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
                    // Objects number their own members; the namespace must
                    // agree, so announce until the ids meet.
                    while self
                        .namespace(node)
                        .is_some_and(|namespace| namespace.get(member).is_none())
                    {
                        if self.announce(node, &name, length, effects) == member {
                            break;
                        }
                    }
                    // A member announced again after a retraction kept its
                    // record and its name, but not its length.
                    if let Some(record) = self
                        .namespace_mut(node)
                        .and_then(|namespace| namespace.get_mut(member))
                    {
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
        // A window was a range of the member's old bytes.
        for window in self.windows_on(node, member) {
            self.drop_subtree(window);
            self.remove_node(window);
        }
        if let Some(found) = self.nodes.get_mut(&node) {
            found.windows.remove(&member);
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
        if binding.window.is_some() {
            // A window owns none of its member: nothing is given up, and
            // nothing reads back through it, so it simply goes.
            self.drop_subtree(node);
            self.remove_node(node);
            if let Some(found) = self.nodes.get_mut(&binding.parent) {
                for windows in found.windows.values_mut() {
                    windows.retain(|window| *window != node);
                }
            }
            return;
        }
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
            .and_then(|kind| {
                let volumes = binding.volumes.clone();
                self.attach(parent, kind, |_| volumes, builder)
            });
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
            pending.extend(self.bound_below(at));
            if let Some(found) = self.nodes.get_mut(&at) {
                found.retired = true;
            }
        }
    }

    // Remove a node and return the parent members that were its volumes.
    fn detach(&mut self, node: NodeId) -> Vec<MemberId> {
        self.remove_node(node)
            .and_then(|removed| removed.binding)
            .map(|binding| binding.volumes.into_values().collect())
            .unwrap_or_default()
    }

    fn drop_subtree(&mut self, node: NodeId) {
        for child in self.bound_below(node) {
            self.drop_subtree(child);
            self.remove_node(child);
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
            // A seek past the end of a window asks for nothing that is the
            // node's.
            let Some(range) = self
                .in_member(at, ask.range)
                .filter(|range| self.window(at).is_none() || !range.is_empty())
            else {
                continue;
            };
            let ask = Seek { range, ..ask };
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
    // descends as any other. A checksum names the whole range it covers,
    // not the bytes that were bad: damage already marked in that range
    // accounts for the failure, at the bytes it reached, and marking the
    // rest would only mark bytes that arrived whole.
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
            Some((parent, member)) if self.damaged_in(parent, member, range) => {}
            Some((parent, member)) => {
                let mut pending = Vec::new();
                self.mark(parent, member, range, effects, &mut pending);
                self.descend(pending, effects);
            }
            None => self.damage(node, volume, range, effects),
        }
    }

    // Some of the range of the member is marked damaged already.
    fn damaged_in(&self, node: NodeId, member: MemberId, range: ByteRange) -> bool {
        self.nodes
            .get(&node)
            .and_then(|found| found.namespace.get(member))
            .is_some_and(|record| {
                record
                    .coverage
                    .overlapping(range)
                    .any(|run| run.provenance == Provenance::Damaged)
            })
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

    // Damage reached a member range, as `Effect::Damaged` said. A window on
    // it reads that range as its own volume, so what of it falls there is
    // lost to the window's object: nothing recovers the recovery data.
    pub fn damaged_under_windows(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        for window in self.windows_on(node, member) {
            let Some((whole, piece)) = self
                .window(window)
                .and_then(|whole| Some((whole, whole.intersect(range)?)))
            else {
                continue;
            };
            let own = piece.shift(-(whole.start as i64));
            let mut emits = std::mem::take(&mut self.emits);
            emits.clear();
            if let Some(object) = self
                .nodes
                .get_mut(&window)
                .and_then(|found| found.object.as_mut())
            {
                object.volume_lost(VolumeIndex(0), own, &mut emits);
            }
            self.apply(window, &mut emits, builder, effects);
            self.emits = emits;
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

    mod windows;

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
        let first = tree.announce_root("show.rar", None, &mut Vec::new());
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
        let member = tree.announce_root("readme.txt", Some(5), &mut Vec::new());
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
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
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
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
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
    fn a_checksum_failure_over_marked_damage_marks_nothing_more() {
        let (mut tree, first, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.damage(
            NodeId::ROOT,
            VolumeIndex(first.0),
            ByteRange::new(10, 13),
            &mut effects,
        );
        effects.clear();
        tree.emitted(
            child,
            vec![Emit::Damaged {
                volume: VolumeIndex(0),
                range: ByteRange::new(7, 12),
            }],
            &mut Build,
            &mut effects,
        );
        assert!(effects.is_empty(), "{effects:?}");
        let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
        assert_eq!(
            inner.coverage.damaged_ranges().collect::<Vec<_>>(),
            vec![ByteRange::new(3, 6)]
        );
    }

    #[test]
    fn a_checksum_failure_over_whole_bytes_marks_its_range_and_descends() {
        let (mut tree, first, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![Emit::Damaged {
                volume: VolumeIndex(0),
                range: ByteRange::new(7, 12),
            }],
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![
                Effect::Damaged {
                    node: NodeId::ROOT,
                    member: first,
                    range: ByteRange::new(7, 12)
                },
                Effect::Damaged {
                    node: child,
                    member: MemberId(0),
                    range: ByteRange::new(0, 5)
                },
            ]
        );
    }

    #[test]
    fn live_nodes_come_parents_first_and_a_retired_one_is_left_out() {
        let (mut tree, _, child, _) = bound_tree();
        assert_eq!(tree.live(), vec![NodeId::ROOT, child]);
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![Emit::Refuse(Why::Compressed)],
            &mut Build,
            &mut effects,
        );
        assert_eq!(tree.live(), vec![NodeId::ROOT]);
    }

    #[test]
    fn a_rebuilt_file_is_announced_at_its_length_on_a_live_node_only() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        let member = tree
            .announce_rebuilt(child, "quill.bin", 9, &mut effects)
            .expect("a live node");
        assert_eq!(
            effects,
            vec![Effect::Announced {
                node: child,
                member
            }]
        );
        let record = tree.namespace(child).unwrap().get(member).unwrap();
        assert_eq!(
            (record.name.as_str(), record.length),
            ("quill.bin", Some(9))
        );

        tree.emitted(
            child,
            vec![Emit::Refuse(Why::Compressed)],
            &mut Build,
            &mut effects,
        );
        effects.clear();
        assert_eq!(
            tree.announce_rebuilt(child, "late.bin", 4, &mut effects),
            None
        );
        assert!(effects.is_empty());
    }

    #[test]
    fn a_refusal_turns_every_volume_back_into_a_leaf() {
        let (mut tree, first, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
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
        let third = tree.announce_root("show.r01", None, &mut Vec::new());
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
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
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
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

    fn output_name(tree: &Tree, node: NodeId, member: MemberId) -> String {
        tree.namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.output_name.clone())
            .expect("member is there")
    }

    fn store_announces_subs(tree: &mut Tree, child: NodeId, effects: &mut Vec<Effect>) {
        let member = Emit::Member {
            member: MemberId(1),
            name: "subs.srt".into(),
            length: None,
        };
        tree.emitted(child, vec![member], &mut Build, effects);
    }

    #[test]
    fn the_root_keeps_a_name_a_store_below_also_wants_in_either_order() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        let top = tree.announce_root("subs.srt", None, &mut effects);
        store_announces_subs(&mut tree, child, &mut effects);
        assert_eq!(output_name(&tree, NodeId::ROOT, top), "subs.srt");
        assert_eq!(output_name(&tree, child, MemberId(1)), "subs (1).srt");
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Renamed { .. }))
        );

        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        store_announces_subs(&mut tree, child, &mut effects);
        assert_eq!(output_name(&tree, child, MemberId(1)), "subs.srt");
        effects.clear();
        let top = tree.announce_root("subs.srt", None, &mut effects);
        assert_eq!(output_name(&tree, NodeId::ROOT, top), "subs.srt");
        assert_eq!(output_name(&tree, child, MemberId(1)), "subs (1).srt");
        assert_eq!(
            effects,
            vec![Effect::Renamed {
                node: child,
                member: MemberId(1),
                old: "subs.srt".into(),
                new: "subs (1).srt".into(),
            }]
        );
    }

    #[test]
    fn rebuilt_names_come_back_from_positions() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        store_announces_subs(&mut tree, child, &mut effects);
        let top = tree.announce_root("subs.srt", None, &mut effects);
        tree.rebuild_names();
        assert_eq!(output_name(&tree, NodeId::ROOT, top), "subs.srt");
        assert_eq!(output_name(&tree, child, MemberId(1)), "subs (1).srt");
        // The ledger holds both again: a third root member that wants the
        // name comes after the first and before the store's.
        let third = tree.announce_root("SUBS.srt", None, &mut effects);
        assert_eq!(output_name(&tree, NodeId::ROOT, third), "SUBS (1).srt");
        assert_eq!(output_name(&tree, child, MemberId(1)), "subs (2).srt");
    }

    #[test]
    fn a_rename_through_the_tree_moves_the_output_name() {
        let (mut tree, _, child, _) = bound_tree();
        let mut effects = Vec::new();
        store_announces_subs(&mut tree, child, &mut effects);
        let top = tree.announce_root("3f9a.bin", None, &mut effects);
        effects.clear();
        let renamed = tree.rename(NodeId::ROOT, top, "subs.srt", &mut effects);
        assert_eq!(renamed.as_deref(), Some("subs.srt"));
        assert_eq!(
            effects,
            vec![
                Effect::Renamed {
                    node: child,
                    member: MemberId(1),
                    old: "subs.srt".into(),
                    new: "subs (1).srt".into(),
                },
                Effect::Renamed {
                    node: NodeId::ROOT,
                    member: top,
                    old: "3f9a.bin".into(),
                    new: "subs.srt".into(),
                },
            ]
        );
    }
}
