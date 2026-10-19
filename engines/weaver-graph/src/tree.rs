// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The wiring. A tree is the root's namespace plus one node per bound
// member, each node an object with a namespace of its own. The tree does
// four things and knows no format while doing them: it carries bytes down
// into the object that owns a member, it records what each member received,
// it lifts an object's seek up through the forward maps until the root
// answers, and it carries damage down through the inverse maps until a
// namespace with a recovery object that covers it takes over.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::deliver::{Delivery, Emit, Provenance, Why};
use crate::engine::Engine;
use crate::member::{Member, MemberId, Namespace};
use crate::names::{Moved, NameLedger, Position};
use crate::object::{Carried, Extent, Kind, Object, ObjectId, Placement};
use crate::recovery::Recovery;
use crate::seek::{Pull, Seek, SeekUp};
use crate::sniff::{Sniff, sniff};
use crate::volume::{ByteRange, VolumeIndex};

mod quiet;
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
    // Volumes bound by a `Bound` whose owner has not yet handed the object
    // what their members received before: their close waits for the
    // hand-over, so the object hears it once, after every byte.
    awaiting: BTreeSet<VolumeIndex>,
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
    undecided: BTreeSet<MemberId>,
    // The object gave its volumes up, or one above it did. The node stays
    // only so its owner can read back where its bytes went; it says nothing
    // more, runs nothing and bids for nothing.
    retired: bool,
    // Why the object refused its volumes, when no engine took them in its
    // place. It stays for as long as its node does, and the later volumes
    // of its set it still claims are refused with it.
    refused: Option<Why>,
}

impl Node {
    fn new(object: Option<Box<dyn Object>>, binding: Option<Binding>) -> Self {
        Self {
            object,
            namespace: Namespace::new(),
            binding,
            children: BTreeMap::new(),
            windows: BTreeMap::new(),
            undecided: BTreeSet::new(),
            retired: false,
            refused: None,
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
    // These bytes of a member of `node`'s namespace reached `child`, the
    // object the member is a volume of, as that volume, or the retired
    // object it was one of until the owner buries it. What the object
    // keeps of them for no member of its own is the owner's to keep,
    // whoever sent them: a source, or an object above that places them
    // only now, read back from what it was handed before.
    Reached {
        node: NodeId,
        member: MemberId,
        child: NodeId,
        volume: VolumeIndex,
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
    // The object at `from` gave this member back, which was its `volume`:
    // its own headers say the member is not its there. The member is a leaf
    // of `node`'s namespace again, offered to the objects beside it. What
    // the object kept of it for no member of its own, its envelope, is the
    // member's own file now, moved and not copied; what it never placed is
    // still held for the member.
    GivenBack {
        node: NodeId,
        member: MemberId,
        from: NodeId,
        volume: VolumeIndex,
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
// constructor. The bytes they place are kept outside it too, so the builder
// is also how an object taking a delivery reads one of its placements back,
// the read `posted` is handed: what this placement of the object at `node`,
// for its `volume`, holds. `false` when the builder keeps no bytes. Required,
// so a builder that keeps none says so rather than inheriting it.
pub trait Builder {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>>;

    fn read(
        &self,
        tree: &Tree,
        node: NodeId,
        volume: VolumeIndex,
        placement: &Placement,
        into: &mut Vec<u8>,
    ) -> bool;
}

pub struct Tree {
    nodes: BTreeMap<NodeId, Node>,
    // Members given up by a retired object, and what they were to it: which
    // node, and which of its volumes, until the owner buries them. The entry
    // outlives the burial, emptied, because a member an object gave up is
    // settled: no object bound later is offered it.
    retired: BTreeMap<(NodeId, MemberId), Option<(NodeId, VolumeIndex)>>,
    // What an object parsed of a member it gave back, parked beside the
    // member until an object of its namespace reclaims it and adopts it.
    given: BTreeMap<(NodeId, MemberId), Carried>,
    // Every namespace writes into one directory, so one ledger names them
    // all.
    names: NameLedger,
    next_node: u32,
    // How deep a namespace may sit: an object that would open one deeper is
    // never built, and its member is refused as the container it is.
    nesting: u32,
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
            given: BTreeMap::new(),
            names: NameLedger::new(),
            next_node: 1,
            nesting: u32::MAX,
            emits: Vec::new(),
            extents: Vec::new(),
            placements: Vec::new(),
        }
    }

    pub fn namespace(&self, node: NodeId) -> Option<&Namespace> {
        self.nodes.get(&node).map(|found| &found.namespace)
    }

    // The deepest a namespace may sit, the root's being 0. The owner sets
    // it, as it sets the hold budgets; a tree it never set nests freely.
    pub fn nest_at_most(&mut self, depth: u32) {
        self.nesting = depth;
    }

    // How deep the namespace of `node` sits: 0 for the root's, one more per
    // object above it.
    pub fn depth(&self, mut node: NodeId) -> u32 {
        let mut depth = 0;
        while let Some(binding) = self
            .nodes
            .get(&node)
            .and_then(|found| found.binding.as_ref())
        {
            depth += 1;
            node = binding.parent;
        }
        depth
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
    // to hears that its volume is closed (at its hand-over, when the owner
    // still owes it the member's earlier bytes), and the owner closes its
    // file.
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
        record.length.get_or_insert(reached);
        if self.awaiting(node, member).is_none() {
            self.volume_closed(node, member, builder, effects);
        }
        for window in self.windows_on(node, member) {
            self.close_window(window, builder, effects);
        }
        effects.push(Effect::Closed { node, member });
    }

    // The owner handed the object bound at a member of `node`'s namespace
    // everything the member received before the bind. A member that closed
    // meanwhile, or before the bind, has its object hear that now: its close
    // waited for this.
    pub fn handed_over(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some((child, volume)) = self.awaiting(node, member) else {
            return;
        };
        if let Some(binding) = self
            .nodes
            .get_mut(&child)
            .and_then(|found| found.binding.as_mut())
        {
            binding.awaiting.remove(&volume);
        }
        self.volume_closed(node, member, builder, effects);
    }

    // The object a member of `node`'s namespace is a volume of, while its
    // owner still owes it the member's earlier bytes.
    fn awaiting(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        let (child, volume) = self.nodes.get(&node)?.children.get(&member).copied()?;
        self.nodes
            .get(&child)?
            .binding
            .as_ref()?
            .awaiting
            .contains(&volume)
            .then_some((child, volume))
    }

    // A member of `node`'s namespace is bound to `child`: the owner is told,
    // and owes the object what the member received before.
    fn bound(
        &mut self,
        node: NodeId,
        member: MemberId,
        kind: Kind,
        child: NodeId,
        effects: &mut Vec<Effect>,
    ) {
        let volume = self
            .nodes
            .get(&node)
            .and_then(|found| found.children.get(&member))
            .map(|(_, volume)| *volume);
        if let (Some(volume), Some(binding)) = (
            volume,
            self.nodes
                .get_mut(&child)
                .and_then(|found| found.binding.as_mut()),
        ) {
            binding.awaiting.insert(volume);
        }
        effects.push(Effect::Bound {
            node,
            member,
            kind,
            child,
        });
    }

    // The object a closed member is a volume of hears it closed, at its
    // length.
    fn volume_closed(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(found) = self.nodes.get(&node) else {
            return;
        };
        let Some(length) = found
            .namespace
            .get(member)
            .filter(|record| record.closed)
            .and_then(|record| record.length)
        else {
            return;
        };
        let Some((child, volume)) = found.children.get(&member).copied() else {
            return;
        };
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
        self.retired.get(&(node, member)).copied().flatten()
    }

    // The object that refused a member of `node`'s namespace: the one it
    // was given up by, or the refused one that claimed it after.
    pub fn refused_by(&self, node: NodeId, member: MemberId) -> Option<NodeId> {
        self.retired(node, member)
            .map(|(object, _)| object)
            .or_else(|| {
                self.refused_below(node).into_iter().find(|object| {
                    self.volumes(*object)
                        .is_some_and(|(_, volumes)| volumes.values().any(|bound| *bound == member))
                })
            })
    }

    // The owner has read the member's bytes back. Once every volume of the
    // retired node is buried, what it closed whole stays, as it was
    // delivered, and the rest of it goes: the posted volumes stand in for
    // that. When an engine took the volumes it reads them again and makes
    // every member over, so nothing of the node stays. A refused node stays
    // whatever it keeps, to claim the rest of its set.
    pub fn bury(&mut self, node: NodeId, member: MemberId) {
        let Some((retired, _)) = self.retired.get_mut(&(node, member)).and_then(Option::take)
        else {
            return;
        };
        if self
            .retired
            .values()
            .flatten()
            .any(|(other, _)| *other == retired)
        {
            return;
        }
        let read_again = self.child_of(node, member).is_some();
        let refused = self
            .nodes
            .get(&retired)
            .is_some_and(|found| found.refused.is_some());
        if read_again || !(self.keep_closed(retired) || refused) {
            self.drop_subtree(retired);
            self.remove_node(retired);
        }
    }

    // What of a buried node stays: each member with a file of its own that
    // closed whole, and each member whose owner keeps something below it,
    // all of them leaves now. A member the object announced and never
    // closed stays too, buried: it is settled as a given-up member is, no
    // object is offered it again, and it is how the tree remembers that the
    // object left one open. It keeps no name and no file. The rest goes with
    // its name, and so does every window, which owns nothing of its member.
    // `false` when nothing stays, and removing the node is the caller's.
    fn keep_closed(&mut self, node: NodeId) -> bool {
        let Some(found) = self.nodes.get_mut(&node) else {
            return false;
        };
        let windows: Vec<NodeId> = std::mem::take(&mut found.windows)
            .into_values()
            .flatten()
            .collect();
        let members: Vec<(MemberId, bool, bool)> = found
            .namespace
            .members()
            .map(|record| (record.id, record.is_whole(), record.closed))
            .collect();
        for window in windows {
            self.drop_subtree(window);
            self.remove_node(window);
        }
        for (member, whole, closed) in members {
            let owner = self.child_of(node, member).map(|(child, _)| child);
            if owner.is_some_and(|child| self.keep_closed(child)) {
                continue;
            }
            // A recovery volume lives in its member's own file, as a leaf does.
            let own_file = owner.is_none_or(|child| self.recovers(child));
            let open = !closed && !owner.is_some_and(|child| self.recovers(child));
            if let Some(child) = owner {
                self.drop_subtree(child);
                self.remove_node(child);
            }
            let Some(found) = self.nodes.get_mut(&node) else {
                return false;
            };
            found.children.remove(&member);
            found.namespace.unbind(member);
            if open {
                found.undecided.remove(&member);
                self.retired.insert((node, member), None);
                self.names.release((node, member));
            } else if !(whole && own_file) {
                found.namespace.remove(member);
                self.retired.remove(&(node, member));
                found.undecided.remove(&member);
                self.names.release((node, member));
            }
        }
        self.nodes
            .get(&node)
            .is_some_and(|found| !found.namespace.is_empty())
    }

    // Whether nothing reads back through the node any more: it is gone, or
    // it and every retired node above it are buried, so what stays of it is
    // members closed whole, which are files and nothing more.
    pub fn buried(&self, node: NodeId) -> bool {
        if self.nodes.get(&node).is_some_and(|found| !found.retired) {
            return false;
        }
        let mut at = Some(node);
        while let Some((id, found)) = at
            .and_then(|id| Some((id, self.nodes.get(&id)?)))
            .filter(|(_, found)| found.retired)
        {
            if self
                .retired
                .values()
                .flatten()
                .any(|(other, _)| *other == id)
            {
                return false;
            }
            at = found.binding.as_ref().map(|binding| binding.parent);
        }
        true
    }

    // A member a buried node's object announced and never closed: settled,
    // so no object is offered it, and never a file. A member given up to a
    // live namespace is settled too, but it is a leaf with its own file.
    pub fn buried_member(&self, node: NodeId, member: MemberId) -> bool {
        self.buried(node)
            && self
                .retired
                .get(&(node, member))
                .is_some_and(Option::is_none)
            && self
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| !record.closed)
    }

    // Whether the object that read `member` of `node`'s namespace, buried
    // since, had announced a member it never closed: its buried node still
    // holds that member.
    pub fn left_open(&self, node: NodeId, member: MemberId) -> bool {
        self.kept(node, member).any(|kept| self.leaves_open(kept))
    }

    // Whether the node's object announced a member it has not closed. A
    // member a recovery object reads is not the object's to close.
    pub fn leaves_open(&self, node: NodeId) -> bool {
        self.namespace(node).is_some_and(|namespace| {
            namespace.members().any(|member| {
                !member.closed
                    && !self
                        .child_of(node, member.id)
                        .is_some_and(|(child, _)| self.recovers(child))
            })
        })
    }

    // The buried nodes that once read `member` of `node`'s namespace and
    // still hold what they closed whole: the member is a leaf again, and
    // those files are delivered beside it.
    pub fn kept(&self, node: NodeId, member: MemberId) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes
            .iter()
            .filter(move |(_, found)| {
                found.binding.as_ref().is_some_and(|binding| {
                    binding.parent == node
                        && binding.window.is_none()
                        && binding.volumes.values().any(|volume| *volume == member)
                })
            })
            .map(|(id, _)| *id)
            .filter(|id| self.buried(*id))
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
            .is_some_and(|found| found.undecided.contains(&member))
    }

    // The members of `node` whose front has not yet decided their owner.
    pub fn undecided(&self, node: NodeId) -> impl Iterator<Item = MemberId> + '_ {
        self.nodes
            .get(&node)
            .into_iter()
            .flat_map(|found| found.undecided.iter().copied())
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
        found.undecided.insert(member);
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
        self.renamed(node, member, effects);
        self.namespace(node)?
            .get(member)
            .map(|record| record.output_name.clone())
    }

    // A name is evidence a claim reads, so a renamed member is a claim to
    // make again. Its owner hears the name and is offered every member
    // beside it that no object owns, which the name may now place after
    // it; a member no object owns is offered to the objects beside it. No
    // owner loses a member here: only one with no owner is offered.
    fn renamed(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        let Some(name) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.name.clone())
        else {
            return;
        };
        let Some((child, volume)) = self.child_of(node, member) else {
            return self.reoffer(node, member, effects);
        };
        if let Some(object) = self
            .nodes
            .get_mut(&child)
            .and_then(|found| found.object.as_mut())
        {
            object.renamed(volume, &name);
        }
        let unowned: Vec<(MemberId, Vec<u8>)> = self
            .namespace(node)
            .map(|namespace| {
                namespace
                    .members()
                    .filter(|record| self.unowned(node, record.id))
                    .map(|record| (record.id, namespace.first_bytes(record.id).to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        for (member, front) in unowned {
            // A take can take others with it: offer only what is left.
            if self.unowned(node, member) {
                self.offer(node, member, &front, &[child], effects);
            }
        }
    }

    // A member with no owner is offered to the objects of its namespace
    // again, as a new member is: its claim was given back, or its name
    // changed.
    fn reoffer(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        if !self.unowned(node, member) {
            return;
        }
        let Some(front) = self
            .namespace(node)
            .map(|namespace| namespace.first_bytes(member).to_vec())
        else {
            return;
        };
        let mut siblings = self.bound_below(node);
        siblings.extend(self.refused_below(node));
        self.offer(node, member, &front, &siblings, effects);
    }

    // The object at `node` gives one of its volumes back: its own headers
    // say the member is not its where it holds it. A claim is a provisional
    // edge, and this takes the edge back: the member is a leaf of its
    // namespace again, with what the object parsed of it parked beside it,
    // and the objects beside it are offered it, the giver among them, which
    // may take it again where its headers place it. Nothing below the
    // member goes with it, since an object gives a volume back before it
    // maps any of it into a member of its own. This is pointer work only:
    // no byte of the member is copied or moved here or by the owner, who
    // makes what the object kept of it the member's file, and the next
    // owner reads that file and the member's holds in place. A member whose
    // parse the object will not carry is refused instead. An object left
    // with no volume goes: it was founded on that member alone.
    fn give_back(&mut self, node: NodeId, volume: VolumeIndex, effects: &mut Vec<Effect>) {
        let Some(found) = self.nodes.get_mut(&node).filter(|found| !found.retired) else {
            return;
        };
        let Some(binding) = found
            .binding
            .as_mut()
            .filter(|binding| binding.window.is_none())
        else {
            return;
        };
        let Some(member) = binding.volumes.remove(&volume) else {
            return;
        };
        binding.awaiting.remove(&volume);
        let (parent, empty) = (binding.parent, binding.volumes.is_empty());
        let carried = found
            .object
            .as_mut()
            .and_then(|object| object.give_back(volume));
        if let Some(found) = self.nodes.get_mut(&parent) {
            found.children.remove(&member);
            found.namespace.unbind(member);
        }
        effects.push(Effect::GivenBack {
            node: parent,
            member,
            from: node,
            volume,
        });
        let carried = match carried {
            Some(carried) => carried,
            None => {
                self.retired.insert((parent, member), None);
                effects.push(Effect::Refused {
                    node: parent,
                    member,
                    why: Why::Unmapped,
                });
                return self.forget_empty(node, empty);
            }
        };
        self.given.insert((parent, member), carried);
        self.forget_empty(node, empty);
        self.reoffer(parent, member, effects);
    }

    fn forget_empty(&mut self, node: NodeId, empty: bool) {
        if empty {
            self.drop_subtree(node);
            self.remove_node(node);
        }
    }

    // The ledger again, from what every namespace holds after a rebuild:
    // each member's wanted name and the output name it had. Taken back in
    // order of position, never of arrival, so any order of restoring gives
    // the names that are on disk.
    pub fn rebuild_names(&mut self) {
        self.names.clear();
        let mut members: Vec<(Position, (NodeId, MemberId), String, String)> = Vec::new();
        for (node, found) in &self.nodes {
            // A buried member holds no name.
            for record in found
                .namespace
                .members()
                .filter(|record| !self.buried_member(*node, record.id))
            {
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
        self.retired.retain(|(at, _), _| *at != node);
        self.given.retain(|(at, _), _| *at != node);
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
        // Bytes that landed in the member's contiguous front, whoever sent
        // them: an undecided member is asked who owns it each time, since
        // a front that stopped at a hole grows again when the hole fills.
        let front =
            usize::try_from(offset).is_ok_and(|at| at < found.namespace.first_bytes(member).len());
        let undecided = found.undecided.contains(&member);
        if front && undecided {
            self.identify_again(node, member, builder, effects);
        }
        for window in self.windows_on(node, member) {
            self.deliver_window(window, offset, &bytes, provenance, builder, effects);
        }
        let Some(found) = self.nodes.get(&node) else {
            return;
        };
        let owner = found
            .children
            .get(&member)
            .copied()
            .or_else(|| self.retired(node, member));
        if let Some((child, volume)) = owner {
            effects.push(Effect::Reached {
                node,
                member,
                child,
                volume,
                offset,
                bytes: bytes.clone(),
                provenance,
            });
        }
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
            None => {
                effects.push(Effect::WriteLeaf {
                    node,
                    member,
                    offset,
                    bytes,
                    provenance,
                });
                // Damage at the front may have hidden what the member is;
                // a repair there lets it say, once its bytes are in its file.
                if front && !undecided && provenance == Provenance::Repaired {
                    self.identify_again(node, member, builder, effects);
                }
            }
        }
    }

    // The front of a member with no owner changed: it is asked again who
    // owns it, from the front it has now. Its bytes are written as a leaf
    // meanwhile, which is right either way: a bound member's bytes are
    // re-delivered to its object by the owner on `Bound`.
    fn identify_again(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if !(self.is_undecided(node, member) || self.unowned(node, member)) {
            return;
        }
        let Some(front) = self
            .namespace(node)
            .map(|namespace| namespace.first_bytes(member).to_vec())
        else {
            return;
        };
        self.identify(node, member, &front, builder, effects);
    }

    // Who owns a member, from its front. First the objects already in its
    // namespace are offered it, as one of their volumes, and so are the
    // objects there that refused theirs; then the sniffer
    // says what it is. An object it builds is offered every member that
    // has no owner, so a volume that came before its set's first is its
    // volume all the same.
    fn identify(
        &mut self,
        node: NodeId,
        member: MemberId,
        prefix: &[u8],
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let mut siblings = self.bound_below(node);
        siblings.extend(self.refused_below(node));
        if self.offer(node, member, prefix, &siblings, effects) {
            return;
        }
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        let Some((name, length)) = found
            .namespace
            .get(member)
            .map(|record| (record.name.clone(), record.length))
        else {
            return;
        };
        match sniff(&name, prefix, length) {
            Sniff::Pending => {}
            Sniff::Leaf => {
                found.undecided.remove(&member);
            }
            Sniff::Object(kind) => {
                found.undecided.remove(&member);
                // The member is the volume its new object places it at, the
                // position its set gives it, whichever volume came first.
                let volumes = |object: &mut dyn Object| {
                    let volume = object.claim(&name, prefix).unwrap_or(VolumeIndex(0));
                    BTreeMap::from([(volume, member)])
                };
                match self.attach(node, kind, volumes, builder) {
                    Ok(child) => {
                        self.bound(node, member, kind, child, effects);
                        self.offer_unowned(node, child, effects);
                    }
                    Err(why) => effects.push(Effect::Refused { node, member, why }),
                }
            }
        }
    }

    // A member no object owns: a leaf whose front has spoken, and which no
    // object is giving up.
    fn unowned(&self, node: NodeId, member: MemberId) -> bool {
        self.nodes.get(&node).is_some_and(|found| {
            found.namespace.get(member).is_some_and(Member::is_leaf)
                && !found.children.contains_key(&member)
                && !found.undecided.contains(&member)
                && !self.retired.contains_key(&(node, member))
        })
    }

    // Offer every member of `node`'s namespace that has no owner to the
    // object bound at `child`, by the one offer a new member gets.
    fn offer_unowned(&mut self, node: NodeId, child: NodeId, effects: &mut Vec<Effect>) {
        let unowned: Vec<(MemberId, Vec<u8>)> = self
            .namespace(node)
            .map(|namespace| {
                namespace
                    .members()
                    .filter(|record| self.unowned(node, record.id))
                    .map(|record| (record.id, namespace.first_bytes(record.id).to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        for (member, front) in unowned {
            // A take can take others with it: offer only what is left.
            if self.unowned(node, member) {
                self.offer(node, member, &front, &[child], effects);
            }
        }
    }

    // The one offer of a member with no owner to objects of its namespace,
    // in the order given; the first to claim it takes it as that volume.
    // Whether one did.
    fn offer(
        &mut self,
        node: NodeId,
        member: MemberId,
        prefix: &[u8],
        objects: &[NodeId],
        effects: &mut Vec<Effect>,
    ) -> bool {
        let Some(name) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.name.clone())
        else {
            return false;
        };
        for child in objects {
            let Some(found) = self.nodes.get_mut(child).filter(|found| {
                // A window owns none of its member, so it takes none.
                (!found.retired || found.refused.is_some())
                    && found
                        .binding
                        .as_ref()
                        .is_some_and(|binding| binding.window.is_none())
            }) else {
                continue;
            };
            let refused = found.refused;
            // A member given back is claimed by what was parsed of it.
            let carried = self.given.get(&(node, member));
            let Some(volume) = found.object.as_mut().and_then(|object| match carried {
                Some(carried) => object.reclaim(&name, carried),
                None => object.claim(&name, prefix),
            }) else {
                continue;
            };
            match refused {
                None => self.join(node, member, (*child, volume), (&name, prefix), effects),
                // A volume of a refused set is refused with it. The object
                // never held any of it, so it is a leaf already, and settled.
                Some(why) => {
                    if let Some(binding) = self
                        .nodes
                        .get_mut(child)
                        .and_then(|found| found.binding.as_mut())
                    {
                        binding.volumes.insert(volume, member);
                    }
                    if let Some(found) = self.nodes.get_mut(&node) {
                        found.undecided.remove(&member);
                    }
                    self.retired.insert((node, member), None);
                    self.given.remove(&(node, member));
                    effects.push(Effect::Refused { node, member, why });
                }
            }
            return true;
        }
        false
    }

    // A member becomes the volume of the object that claimed it.
    fn join(
        &mut self,
        node: NodeId,
        member: MemberId,
        (child, volume): (NodeId, VolumeIndex),
        (name, front): (&str, &[u8]),
        effects: &mut Vec<Effect>,
    ) {
        let carried = self.given.remove(&(node, member));
        let Some(parent) = self.nodes.get_mut(&node) else {
            return;
        };
        parent.undecided.remove(&member);
        parent.children.insert(member, (child, volume));
        parent.namespace.bind(member, ObjectId(child.0));
        let Some(child_node) = self.nodes.get_mut(&child) else {
            return;
        };
        if let Some(binding) = child_node.binding.as_mut() {
            binding.volumes.insert(volume, member);
        }
        // A member given back brings what was parsed of it, which its new
        // owner adopts as it stands: it reads none of the member again.
        let kind = child_node.object.as_mut().map(|object| {
            match carried {
                Some(carried) => object.adopt(volume, name, carried),
                None => object.bound(volume, name, front),
            }
            object.kind()
        });
        if let Some(kind) = kind {
            self.bound(node, member, kind, child, effects);
        }
        self.offer_given(node, child, effects);
    }

    // An object's set grew, which is evidence for the members given back
    // beside it that no object took: each is offered to it again, by what
    // was parsed of it. A take grows the set again, so a chain given back
    // in any order is taken whole. Pointer work, as every offer is.
    fn offer_given(&mut self, node: NodeId, child: NodeId, effects: &mut Vec<Effect>) {
        let given: Vec<MemberId> = self
            .given
            .keys()
            .filter(|(parent, _)| *parent == node)
            .map(|(_, member)| *member)
            .collect();
        for member in given {
            if !self.given.contains_key(&(node, member)) || !self.unowned(node, member) {
                continue;
            }
            let front = self
                .namespace(node)
                .map(|namespace| namespace.first_bytes(member).to_vec())
                .unwrap_or_default();
            self.offer(node, member, &front, &[child], effects);
        }
    }

    // Bind members of `node`'s namespace, by the volume each becomes, to a
    // freshly built object of `kind`, which `volumes` may ask where they go.
    // The one place an object comes to own members, and so where the
    // nesting limit is held: nothing is bound, and why, when the object
    // would open a namespace past it or no object of that kind can be built.
    fn attach(
        &mut self,
        node: NodeId,
        kind: Kind,
        volumes: impl FnOnce(&mut dyn Object) -> BTreeMap<VolumeIndex, MemberId>,
        builder: &mut dyn Builder,
    ) -> Result<NodeId, Why> {
        if self.depth(node) >= self.nesting {
            return Err(Why::TooDeep);
        }
        let child = NodeId(self.next_node);
        // Object ids are node ids: one object per node, for life.
        let id = ObjectId(child.0);
        let mut object = builder.build(kind, id).ok_or(Why::Unsupported)?;
        self.next_node += 1;
        let volumes = volumes(object.as_mut());
        let parent = self.nodes.get_mut(&node).ok_or(Why::Unsupported)?;
        for (volume, member) in &volumes {
            if let Some(record) = parent.namespace.get(*member) {
                object.bound(*volume, &record.name, parent.namespace.first_bytes(*member));
            }
            parent.namespace.bind(*member, id);
            parent.children.insert(*member, (child, *volume));
        }
        let binding = Binding {
            parent: node,
            volumes,
            window: None,
            awaiting: BTreeSet::new(),
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        Ok(child)
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
        // A window's front is the range's, which has not passed yet.
        object.bound(VolumeIndex(0), &name, &[]);
        let binding = Binding {
            parent: node,
            volumes: BTreeMap::from([(VolumeIndex(0), member)]),
            window: Some(range),
            awaiting: BTreeSet::new(),
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
        // The object is out of its node while it takes the delivery, so its
        // reads can walk the tree below it.
        if let Some(mut object) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.object.take())
        {
            if let Some(length) = length {
                object.sized(delivery.volume, length, &mut emits);
            }
            let (tree, reader, volume) = (&*self, &*builder, delivery.volume);
            let mut read = |placement: &Placement, into: &mut Vec<u8>| {
                reader.read(tree, node, volume, placement, into)
            };
            object.deliver(delivery, &mut read, &mut emits);
            if let Some(found) = self.nodes.get_mut(&node) {
                found.object = Some(object);
            }
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
                Emit::GiveBack(volume) => self.give_back(node, volume, effects),
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
                    found.undecided.insert(volume);
                    self.retired.remove(&(node, volume));
                    self.given.remove(&(node, volume));
                }
            }
        }
        // A member that starts over was given up by nothing yet.
        if let Some(found) = self.nodes.get_mut(&node) {
            found.namespace.retract(member);
            found.undecided.insert(member);
            self.retired.remove(&(node, member));
            self.given.remove(&(node, member));
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
                self.retired
                    .insert((parent, *member), Some((node, *volume)));
            }
        }
        let engine = kind
            .and_then(Kind::engine_for)
            .filter(|_| engine)
            .and_then(|kind| {
                let volumes = binding.volumes.clone();
                self.attach(parent, kind, |_| volumes, builder).ok()
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
        match engine {
            Some(engine) => self.offer_unowned(parent, engine, effects),
            None => {
                if let Some(found) = self.nodes.get_mut(&node) {
                    found.refused = Some(why);
                }
            }
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

    // The objects bound in `node`'s namespace that refused their volumes.
    fn refused_below(&self, node: NodeId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, found)| {
                found.refused.is_some()
                    && found
                        .binding
                        .as_ref()
                        .is_some_and(|binding| binding.parent == node)
            })
            .map(|(child, _)| *child)
            .collect()
    }

    // Remove a node and return the parent members that were its volumes.
    fn detach(&mut self, node: NodeId) -> Vec<MemberId> {
        self.remove_node(node)
            .and_then(|removed| removed.binding)
            .map(|binding| binding.volumes.into_values().collect())
            .unwrap_or_default()
    }

    // A node's subtree goes with it, the objects that refused their volumes
    // in its namespace among it.
    fn drop_subtree(&mut self, node: NodeId) {
        let mut below = self.bound_below(node);
        below.extend(self.refused_below(node));
        for child in below {
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
    // as its second volume.
    struct Split {
        announced: bool,
    }

    impl Object for Split {
        fn kind(&self) -> Kind {
            Kind::RarStore
        }
        fn deliver(
            &mut self,
            delivery: Delivery<'_>,
            _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
            out: &mut Vec<Emit>,
        ) {
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
        fn deliver(
            &mut self,
            _: Delivery<'_>,
            _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
            _: &mut Vec<Emit>,
        ) {
        }
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
        fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8]) {
            self.0.push((volume, name.to_owned()));
        }
    }

    struct Build;
    impl Builder for Build {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            match kind {
                Kind::RarStore => Some(Box::new(Split { announced: false })),
                Kind::RarEngine => Some(Box::new(Reader(Vec::new()))),
                _ => None,
            }
        }
        fn read(
            &self,
            _: &Tree,
            _: NodeId,
            _: VolumeIndex,
            _: &Placement,
            _: &mut Vec<u8>,
        ) -> bool {
            false
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
    fn a_member_past_the_nesting_limit_is_refused_as_the_container_it_is() {
        let mut tree = Tree::new();
        tree.nest_at_most(0);
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
        assert!(tree.child_of(NodeId::ROOT, first).is_none());
        assert!(effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: first,
            why: Why::TooDeep,
        }));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Bound { .. }))
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

    // The same volume posted before its set's first: it has no signature,
    // so it is decided a leaf, and the store bound after it claims it by
    // the same offer, as the volume it would have been had it come second.
    #[test]
    fn a_leaf_is_claimed_by_an_object_bound_after_it() {
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let second = tree.announce_root("show.r00", Some(5), &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            second,
            0,
            Bytes::from_static(b"world"),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert_eq!(tree.child_of(NodeId::ROOT, second), None, "a leaf first");
        let first = tree.announce_root("show.rar", None, &mut Vec::new());
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        effects.clear();
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
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((child, VolumeIndex(1)))
        );
        assert!(effects.contains(&Effect::Bound {
            node: NodeId::ROOT,
            member: second,
            kind: Kind::RarStore,
            child
        }));
    }

    // A member whose front stopped at a lost slice, 66 bytes in, cannot yet
    // rule out a tar, and its bytes cannot before its ustar magic is due.
    // The bytes past the hole arrive first and are no front; the repair that
    // fills the hole grows the front to 263 bytes, past them, and the member
    // is decided from it.
    #[test]
    fn an_undecided_member_is_decided_when_a_repair_grows_its_front() {
        let mut tree = Tree::new();
        let member = tree.announce_root("beacon.bin", None, &mut Vec::new());
        let body = vec![b'0'; 263];
        let deliver = |tree: &mut Tree, range: std::ops::Range<usize>, provenance| {
            tree.deliver(
                NodeId::ROOT,
                member,
                range.start as u64,
                Bytes::copy_from_slice(&body[range]),
                provenance,
                &mut Build,
                &mut Vec::new(),
            );
        };
        deliver(&mut tree, 0..66, Provenance::Fresh);
        assert!(
            tree.is_undecided(NodeId::ROOT, member),
            "a tar is not ruled out"
        );
        deliver(&mut tree, 150..263, Provenance::Fresh);
        assert!(
            tree.is_undecided(NodeId::ROOT, member),
            "past the hole is no front"
        );
        deliver(&mut tree, 66..150, Provenance::Repaired);
        let front = tree
            .namespace(NodeId::ROOT)
            .unwrap()
            .first_bytes(member)
            .len();
        assert_eq!(front, 263);
        assert!(!tree.is_undecided(NodeId::ROOT, member));
        assert_eq!(tree.child_of(NodeId::ROOT, member), None, "a leaf");
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
        // The bytes reached the store before it gave its volumes up, so
        // the owner keeps what it placed of them for its refusal to read.
        assert_eq!(
            effects,
            vec![
                Effect::Reached {
                    node: NodeId::ROOT,
                    member: first,
                    child,
                    volume: VolumeIndex(0),
                    offset: 20,
                    bytes: Bytes::from_static(b"REFUSE"),
                    provenance: Provenance::Fresh,
                },
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
        // Nothing of the store stays but a member it left open, buried.
        assert!(tree.namespace(child).is_none_or(|namespace| {
            namespace
                .members()
                .all(|member| tree.buried_member(child, member.id))
        }));
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

    // A volume a refused store gave back is settled: a store bound after the
    // refusal, which would claim it by name, is never offered it.
    #[test]
    fn a_volume_given_up_is_never_offered_to_a_later_object() {
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
        tree.deliver(
            NodeId::ROOT,
            first,
            20,
            Bytes::from_static(b"REFUSE"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        tree.bury(NodeId::ROOT, first);
        tree.bury(NodeId::ROOT, second);
        assert!(
            tree.namespace(child).is_none_or(|namespace| {
                namespace
                    .members()
                    .all(|member| tree.buried_member(child, member.id))
            }),
            "nothing of the store stays but what it left open, buried"
        );
        let later = tree.announce_root("again.rar", None, &mut Vec::new());
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            later,
            0,
            bytes.into(),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        let bound = tree.child_of(NodeId::ROOT, later).expect("bound").0;
        assert_ne!(bound, child);
        assert_eq!(tree.child_of(NodeId::ROOT, second), None);
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Bound { member, .. } if *member == second
        )));
        assert!(
            tree.namespace(NodeId::ROOT)
                .unwrap()
                .get(second)
                .unwrap()
                .is_leaf()
        );
    }

    // A set's later volume joins the object of its set even after that
    // object refused, and is refused with it, at the refusal's reason: it
    // binds no object of its own.
    #[test]
    fn a_later_volume_of_a_refused_set_is_refused_with_it() {
        let (mut tree, first, child, _) = bound_tree();
        tree.deliver(
            NodeId::ROOT,
            first,
            20,
            Bytes::from_static(b"REFUSE"),
            Provenance::Fresh,
            &mut Build,
            &mut Vec::new(),
        );
        tree.bury(NodeId::ROOT, first);
        let later = tree.announce_root("show.r01", None, &mut Vec::new());
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            later,
            0,
            bytes.into(),
            Provenance::Fresh,
            &mut Build,
            &mut effects,
        );
        assert!(effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: later,
            why: Why::Compressed
        }));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Bound { .. })),
            "{effects:?}"
        );
        assert_eq!(tree.live(), vec![NodeId::ROOT]);
        assert_eq!(tree.refused_by(NodeId::ROOT, later), Some(child));
        assert_eq!(tree.refused_by(NodeId::ROOT, first), Some(child));
        assert!(
            tree.namespace(NodeId::ROOT)
                .unwrap()
                .get(later)
                .unwrap()
                .is_leaf()
        );
    }

    // The store closed its first member whole and had a second open when it
    // gave up. Buried, it keeps the first, under its name, and the second
    // buried, which is how the tree says the store left it open; the
    // second's name is free again.
    #[test]
    fn a_buried_refusal_keeps_what_it_closed_whole_and_drops_the_rest() {
        let (mut tree, first, child, _) = bound_tree();
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![
                Emit::MemberClosed(MemberId(0)),
                Emit::Member {
                    member: MemberId(1),
                    name: "open.bin".into(),
                    length: Some(4),
                },
                Emit::Bytes {
                    member: MemberId(1),
                    offset: 0,
                    bytes: Bytes::from_static(b"ab"),
                    provenance: Provenance::Fresh,
                },
                Emit::Refuse(Why::Compressed),
            ],
            &mut Build,
            &mut effects,
        );
        assert!(effects.contains(&Effect::Refused {
            node: NodeId::ROOT,
            member: first,
            why: Why::Compressed
        }));
        assert!(!tree.buried(child), "its volume is still to be read back");
        assert_eq!(tree.kept(NodeId::ROOT, first).count(), 0);
        tree.bury(NodeId::ROOT, first);
        assert!(tree.buried(child));
        assert_eq!(tree.kept(NodeId::ROOT, first).collect::<Vec<_>>(), [child]);
        assert!(tree.live().iter().all(|node| *node != child));
        let kept = tree.namespace(child).expect("what it closed stays");
        assert_eq!(
            kept.members()
                .map(|member| member.output_name.as_str())
                .collect::<Vec<_>>(),
            ["inner.bin", "open.bin"]
        );
        assert!(kept.get(MemberId(0)).unwrap().is_whole());
        assert!(!tree.buried_member(child, MemberId(0)));
        assert!(tree.buried_member(child, MemberId(1)));
        assert!(tree.left_open(NodeId::ROOT, first));
        let root = tree.namespace(NodeId::ROOT).unwrap();
        assert!(root.get(first).unwrap().is_leaf());
        let again = tree.announce_root("open.bin", None, &mut Vec::new());
        let again = tree.namespace(NodeId::ROOT).unwrap().get(again).unwrap();
        assert_eq!(again.output_name, "open.bin");
        // A kept member still holds its name: a root member that wants it
        // moves it aside.
        let mut moved = Vec::new();
        tree.announce_root("inner.bin", None, &mut moved);
        assert!(moved.iter().any(|effect| matches!(
            effect,
            Effect::Renamed { node, member: MemberId(0), .. } if *node == child
        )));
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
        // The bytes reached the store before it gave its volumes up, so
        // the owner keeps what it placed of them for its refusal to read.
        assert_eq!(
            effects,
            vec![
                Effect::Reached {
                    node: NodeId::ROOT,
                    member: first,
                    child,
                    volume: VolumeIndex(0),
                    offset: 20,
                    bytes: Bytes::from_static(b"ENGINE"),
                    provenance: Provenance::Fresh,
                },
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
