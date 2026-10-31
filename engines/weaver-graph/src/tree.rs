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
use std::ops::Bound;

use bytes::Bytes;

use crate::deliver::Posted;
use crate::deliver::{Emit, Handed, Provenance, Why};
use crate::engine::{Engine, Extraction};
use crate::evidence::Evidenced;
use crate::member::{Coverage, FIRST_BYTES, Member, MemberId, Namespace};
use crate::names::{Moved, NameLedger, Position};
use crate::object::{Carried, Claim, Extent, Kind, Located, Object, ObjectId, Placement, Posting};
use crate::recovery::Recovery;
use crate::seek::{Pull, Seek, SeekUp, When};
use crate::sniff::{Sniff, VERDICT_BYTES, signed};
use crate::volume::{ByteRange, VolumeIndex};

mod lent;
mod nodes;
mod pieces;
mod quiet;
mod records;

use lent::{Arrived, Told, Waiting};
pub use nodes::Mark;
use nodes::Nodes;
#[cfg(test)]
pub(crate) use records::tests as record_tests;

// What objects parsed of the members they gave back, by namespace and member.
type Given = BTreeMap<(NodeId, MemberId), Carried>;

// How many members a changed front looked at, for the tests that count it.
#[cfg(test)]
thread_local! {
    static FRONT_CANDIDATES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

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
    // What the window's owner handed its object, last. An object that takes
    // the node again is handed it before it hears anything.
    handed: Handed,
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
    // The namespace a recovery object is bound in when that is not the one
    // its volumes are in: one bound below, where the member it names was
    // founded. Its volumes stay where they were posted.
    covers_in: Option<NodeId>,
    // The object is lent out: what it is, and what waits for its return.
    lent: Option<lent::Lent>,
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
            covers_in: None,
            lent: None,
        }
    }

    fn members_mut(&mut self) -> MembersMut<'_> {
        MembersMut {
            namespace: &mut self.namespace,
            children: &mut self.children,
            undecided: &mut self.undecided,
        }
    }

    fn own(&mut self, member: MemberId, owner: (NodeId, VolumeIndex)) {
        self.members_mut().own(member, owner);
    }

    fn disown(&mut self, member: MemberId) {
        self.members_mut().disown(member);
    }

    fn undecide(&mut self, member: MemberId) {
        self.members_mut().undecide(member);
    }

    fn decide(&mut self, member: MemberId) {
        self.members_mut().decide(member);
    }
}

// A node's members, to change, and nothing of the node's own row: its
// namespace, and what the node keeps of each member beside it.
struct MembersMut<'a> {
    namespace: &'a mut Namespace,
    children: &'a mut BTreeMap<MemberId, (NodeId, VolumeIndex)>,
    undecided: &'a mut BTreeSet<MemberId>,
}

impl MembersMut<'_> {
    // What the node keeps of a member beside its record, its owner and
    // whether its first bytes have yet decided one, is in the member's row:
    // each change marks the member, and these are the only ways to make one.
    fn own(&mut self, member: MemberId, owner: (NodeId, VolumeIndex)) {
        if self.children.insert(member, owner) != Some(owner) {
            self.namespace.mark(member);
        }
    }

    fn disown(&mut self, member: MemberId) {
        if self.children.remove(&member).is_some() {
            self.namespace.mark(member);
        }
    }

    fn undecide(&mut self, member: MemberId) {
        if self.undecided.insert(member) {
            self.namespace.mark(member);
        }
    }

    fn decide(&mut self, member: MemberId) {
        if self.undecided.remove(&member) {
            self.namespace.mark(member);
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
        bytes: Evidenced,
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
        bytes: Evidenced,
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
    // A member of `node`'s namespace is another posting of `into`, a volume
    // an object holds already: its bytes are `into`'s from now on. What the
    // owner held or wrote of it goes down as `into`'s, each run as `held`
    // says it arrived, and it has no file.
    Folded {
        node: NodeId,
        copy: MemberId,
        into: MemberId,
        held: Coverage,
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
    // A member's object read it as what it is not: a volume's own bytes
    // refuted the claim its name made, or a name a recovery set gave it
    // says another kind. Nothing failed. The owner materialises the
    // member's bytes into its own file through the retired object's inverse
    // map, as a refused member's, has the tree claim it again
    // (`Tree::claim_by_bytes`), then buries it.
    Disclaimed {
        node: NodeId,
        member: MemberId,
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
    // The object at `child` placed these bytes of its `volume`, the member
    // of `node`'s namespace, in its envelope at `at`, and its map puts them
    // elsewhere now. The owner reads them back from where it kept them and
    // hands them to the object again, as it hands any bytes it held.
    Unkept {
        node: NodeId,
        member: MemberId,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        envelope: u32,
        at: u64,
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
    // its volume, with what it hands the recovery object over it; the owner
    // builds that object, or hands the one already built what was handed.
    RecoveryAt {
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        kind: crate::deliver::RecoveryKind,
        handed: Handed,
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
    // The object at `node` asked for these ranges of its volumes again, in
    // one say. The owner answers them with `Tree::again` once every effect
    // before it is applied: what the object reads back of its members is
    // then written.
    Again {
        node: NodeId,
        ranges: Vec<(VolumeIndex, ByteRange)>,
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

// Why an object gives its volumes up, which says what holds them next.
#[derive(Clone, Copy)]
enum Unbinding {
    // Nothing here reads them: they are leaves, refused for this reason.
    Refused(Why),
    // Only an engine reads them, bound in the object's place.
    Engine(Why),
    // The object's reading was refuted, by a volume's own bytes against
    // the claim its name made, or by a name that says a volume is another
    // kind: each is claimed again (`Tree::claim_by_bytes`), and nothing
    // failed.
    Refuted,
}

pub struct Tree {
    nodes: Nodes,
    // What an object parsed of a member it gave back, parked beside the
    // member until an object of its namespace reclaims it and adopts it.
    given: Given,
    // Every namespace writes into one directory, so one ledger names them
    // all.
    names: NameLedger,
    // Every live recovery object by the namespace it is bound in, its home,
    // and the home each is listed under: what a namespace's sets are, asked
    // without visiting the members and objects beside them. Kept only by
    // `rehome`, at the moments a set is bound, moves, retires or goes, or its
    // home goes; never recorded, so a rebuild lists every set again.
    sets: BTreeSet<(NodeId, NodeId)>,
    homed: BTreeMap<NodeId, NodeId>,
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
        let mut nodes = Nodes::default();
        nodes.insert(NodeId::ROOT, Node::new(None, None));
        Self {
            nodes,
            given: BTreeMap::new(),
            names: NameLedger::new(),
            sets: BTreeSet::new(),
            homed: BTreeMap::new(),
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
    // it is bound in, where the members it covers are. `None` when the node
    // is the root, its object recovers nothing or is lent. What the object
    // is told may change what it knows, and only it can say whether it did,
    // so its own row is marked; the home is marked only for the members
    // `act` changed. A question that changes nothing asks
    // `consult_recovery`.
    pub fn with_recovery<R>(
        &mut self,
        node: NodeId,
        act: impl FnOnce(&mut dyn Recovery, &mut Namespace) -> R,
    ) -> Option<R> {
        let home = self.home(node)?;
        // The object steps out of its node for the call, so the namespace
        // can be borrowed beside it; it steps back in after.
        let mut object = self.nodes.object_out(node)?;
        let result = object.recovery_mut().and_then(|recovery| {
            self.nodes
                .with_namespace(home, |namespace| act(recovery, namespace))
        });
        if result.is_some() {
            self.nodes.mark_own(node);
        }
        self.nodes.object_in(node, object);
        result
    }

    // Ask the recovery object at `node`, beside the namespace it is bound
    // in, what it knows: a question changes nothing, so nothing is marked.
    // `None` when the node is the root, or its object recovers nothing or
    // is lent.
    pub fn consult_recovery<R>(
        &self,
        node: NodeId,
        ask: impl FnOnce(&dyn Recovery, &Namespace) -> R,
    ) -> Option<R> {
        let recovery = self.recovery(node)?;
        let namespace = self.namespace(self.home(node)?)?;
        Some(ask(recovery, namespace))
    }

    // The namespace the object at `node` is bound in: the one its volumes
    // are in, or the one below it a recovery object moved into, for as long
    // as that one stands. `None` for the root.
    pub fn home(&self, node: NodeId) -> Option<NodeId> {
        let found = self.nodes.get(&node)?;
        let parent = found.binding.as_ref()?.parent;
        Some(
            found
                .covers_in
                .filter(|below| self.nodes.contains_key(below))
                .unwrap_or(parent),
        )
    }

    // A recovery object is bound in the namespace where the member it names
    // was founded: a namespace bound in the one it is bound in now. Its
    // volumes stay where they were posted; what it covers is there.
    pub fn move_recovery(&mut self, node: NodeId, into: NodeId) {
        let below = self
            .volumes(into)
            .is_some_and(|(parent, _)| Some(parent) == self.home(node));
        if below
            && self.recovers(node)
            && let Some(found) = self.nodes.get_mut(&node)
        {
            found.covers_in = Some(into);
            self.rehome(node);
        }
    }

    // List the object at `node` under the namespace it is homed in now, if
    // it is a live recovery object, and under none otherwise: the one place
    // `sets` is kept. Called where a node is bound, moves, retires or goes,
    // which are the only moments its listing can change.
    fn rehome(&mut self, node: NodeId) {
        if let Some(home) = self.homed.remove(&node) {
            self.sets.remove(&(home, node));
        }
        if let Some(home) = self
            .home(node)
            .filter(|_| self.is_live(node) && self.recovers(node))
        {
            self.homed.insert(node, home);
            self.sets.insert((home, node));
        }
    }

    // Where a set stands among the sets of its home: its own before those
    // that moved in from the namespace above, owners by their first member
    // before windows by theirs, as the namespaces list what is bound in them.
    fn standing(&self, set: NodeId) -> (bool, bool, Option<MemberId>, NodeId) {
        let binding = self
            .nodes
            .get(&set)
            .and_then(|found| found.binding.as_ref());
        (
            binding.map(|binding| binding.parent) != self.home(set),
            binding.is_some_and(|binding| binding.window.is_some()),
            binding.and_then(|binding| binding.volumes.values().min().copied()),
            set,
        )
    }

    fn object(&self, node: NodeId) -> Option<&dyn Object> {
        self.nodes.get(&node)?.object.as_deref()
    }

    // What the object at `node` is, lent out or not. `None` for the root.
    pub fn kind(&self, node: NodeId) -> Option<Kind> {
        self.object(node)
            .map(Object::kind)
            .or_else(|| self.lent_kind(node))
    }

    pub fn reads_in_order(&self, node: NodeId) -> bool {
        self.object(node).is_some_and(Object::reads_in_order)
    }

    // The recovery face of the object at `node`, if it has one and is in
    // its node.
    pub fn recovery(&self, node: NodeId) -> Option<&dyn Recovery> {
        self.object(node)?.recovery()
    }

    // Whether the object at `node` is a recovery object: only a recovery
    // object is lent.
    pub fn recovers(&self, node: NodeId) -> bool {
        self.recovery(node).is_some() || self.lent(node)
    }

    // Whether the job spends the member of `node`'s namespace rather than
    // hand it over: it is a volume an object bound in the namespace
    // consumed, whose output is what the object hands over in its stead,
    // unless it is a recovery object's whose volumes are released, as a
    // listing is kept beside what it lists. A member no object holds is
    // spent when its posted name says it is recovery data: one whose first
    // bytes were lost never said by them which set it is.
    pub fn spends(&self, node: NodeId, member: MemberId) -> bool {
        match self.child_of(node, member) {
            Some((child, _)) => self
                .recovery(child)
                .is_none_or(|recovery| !recovery.released()),
            None => self
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| crate::sniff::names_recovery(&record.name)),
        }
    }

    // The recovery objects bound in `node`'s namespace, the ones that cover
    // its members, in the order the namespace lists what is bound in it.
    // Those posted beside its object that moved into it come after its own.
    // Read off `sets`, so it costs what the namespace's sets are, whatever
    // else is bound beside them.
    pub fn recovery_nodes(&self, node: NodeId) -> Vec<NodeId> {
        let mut found: Vec<NodeId> = self
            .sets
            .range((node, NodeId(0))..=(node, NodeId(u32::MAX)))
            .map(|(_, set)| *set)
            .collect();
        found.sort_by_cached_key(|set| self.standing(*set));
        found
    }

    // The first namespace after `after` in node order that a set is homed
    // in, or the first of them when `after` is none: the namespaces that
    // have sets, asked without visiting the nodes beside them.
    pub fn next_recovery_home(&self, after: Option<NodeId>) -> Option<NodeId> {
        let from = after.map_or(Bound::Unbounded, |after| {
            Bound::Excluded((after, NodeId(u32::MAX)))
        });
        self.sets
            .range((from, Bound::Unbounded))
            .next()
            .map(|(home, _)| *home)
    }

    // Every namespace bound in `node`'s whose object is not a recovery
    // object: where a member a recovery object here names may be founded.
    pub fn namespaces_below(&self, node: NodeId) -> Vec<NodeId> {
        let mut found = self.bound_below(node);
        found.retain(|child| {
            !self.recovers(*child) && self.nodes.get(child).is_some_and(|found| !found.retired)
        });
        found
    }

    // Every node bound in `node`'s namespace, once each: the owners of its
    // members, then the windows on them. A namespace's member is identified
    // against everything bound beside it, so this costs what is bound, not
    // its square.
    fn bound_below(&self, node: NodeId) -> Vec<NodeId> {
        let mut found = Vec::new();
        if let Some(parent) = self.nodes.get(&node) {
            let mut seen = BTreeSet::new();
            let owners = parent.children.values().map(|(child, _)| child);
            for child in owners.chain(parent.windows.values().flatten()) {
                if seen.insert(*child) {
                    found.push(*child);
                }
            }
        }
        found
    }

    // Where a range of `node`'s volume lies in the member it is bound to:
    // the range itself for a whole member, and for a window where the
    // window puts it. `None` when the range starts past the window.
    pub fn in_member(&self, node: NodeId, range: ByteRange) -> Option<ByteRange> {
        let binding = self.nodes.get(&node)?.binding.as_ref()?;
        match binding.window {
            Some(window) => range.in_window(window),
            None => Some(range),
        }
    }

    // The range of its member a window is bound to; `None` for any other
    // node.
    pub fn window(&self, node: NodeId) -> Option<ByteRange> {
        self.nodes.get(&node)?.binding.as_ref()?.window
    }

    // Nothing more will arrive for a posting of a member of `node`'s
    // namespace. The member closes once every posting of it has: it takes
    // the length it reached when it declared none or less, the object it is bound
    // to hears that its volume is closed (at its hand-over, when the owner
    // still owes it the member's earlier bytes), and the owner closes its
    // file.
    pub fn close(
        &mut self,
        node: NodeId,
        posting: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(namespace) = self.nodes.namespace_mut(node) else {
            return;
        };
        let member = namespace.resolve(posting);
        if !namespace.end(posting) {
            return;
        }
        let Some(record) = namespace.get_mut(member) else {
            return;
        };
        // A member closes at least as long as what arrived of it: bytes past
        // a length its articles declared are on disk, and its length is
        // theirs until a recovery set proves another.
        let reached = record.coverage.runs().last().map_or(0, |run| run.range.end);
        record.length = Some(record.length.map_or(reached, |length| length.max(reached)));
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
        self.reach(child, Told::Closed { volume, length }, builder, effects);
    }

    // The object a closed member of `node`'s namespace is a volume of hears
    // its close again, at the member's length now: after a restore, or once
    // a recovery set settled that length anew.
    pub fn closed_at(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if self.awaiting(node, member).is_none() {
            self.volume_closed(node, member, builder, effects);
        }
    }

    // A restore tells every object the close of each of its volumes
    // again, as it tells every loss again: an object lent out when the
    // checkpoint was taken comes back as it was when it left, and a close
    // that reached it meanwhile went down with the run.
    pub fn close_again(&mut self, builder: &mut dyn Builder, effects: &mut Vec<Effect>) {
        for node in self.live() {
            let closed: Vec<MemberId> = self.nodes.get(&node).map_or_else(Vec::new, |found| {
                found
                    .namespace
                    .records()
                    .filter(|record| record.closed)
                    .map(|record| record.id)
                    .collect()
            });
            for member in closed {
                self.closed_at(node, member, builder, effects);
                for window in self.windows_on(node, member) {
                    self.close_window(window, builder, effects);
                }
            }
        }
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
        let volume = VolumeIndex(0);
        self.reach(window, Told::Closed { volume, length }, builder, effects);
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

    // A run of the engine at `node`, from its object's present state. A
    // question an earlier run asked is the new run's to ask again.
    pub fn start(&mut self, node: NodeId) -> Option<Box<dyn Extraction>> {
        let extraction = self
            .nodes
            .get_mut(&node)?
            .object
            .as_mut()?
            .engine_mut()?
            .start();
        if let Some(progress) = self.nodes.progress_mut(node) {
            progress.asking = false;
        }
        Some(extraction)
    }

    // How far the engine's run at `node` read: the tree keeps the furthest
    // volume, so the object bids for the volumes after it.
    pub fn run_reached(&mut self, node: NodeId, volume: VolumeIndex) {
        if let Some(progress) = self.nodes.progress_mut(node) {
            progress.reached = progress.reached.max(Some(volume));
        }
    }

    // The engine's run at `node` asked for a password: the object rules on
    // it when the job is quiet, unless an offer or a new run answers first.
    pub fn run_asked(&mut self, node: NodeId) {
        if let Some(progress) = self.nodes.progress_mut(node) {
            progress.asking = true;
        }
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

    // The first live node after `after` in node order, or the first live
    // node when `after` is none.
    pub fn next_live(&self, after: Option<NodeId>) -> Option<NodeId> {
        let from = after.map_or(Bound::Unbounded, Bound::Excluded);
        self.nodes
            .range((from, Bound::Unbounded))
            .find(|(_, found)| !found.retired)
            .map(|(node, _)| *node)
    }

    // Whether `node` is in the tree and not retired.
    pub fn is_live(&self, node: NodeId) -> bool {
        self.nodes.get(&node).is_some_and(|found| !found.retired)
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
    // password opened. Headers it opened are evidence a claim reads, so the
    // object is then offered every member beside it that no object owns.
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
            if let Some(progress) = self.nodes.progress_mut(node) {
                progress.asking = false;
            }
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
        if let Some(binding) = self
            .nodes
            .get(&node)
            .and_then(|found| found.binding.as_ref())
        {
            self.offer_unowned(binding.parent, node, effects);
        }
    }

    // What a member of `node`'s namespace was to the object that gave it up:
    // that retired node, and which of its volumes. The node still answers
    // `locate`, and its namespace and children still stand, until `bury`.
    pub fn retired(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.nodes.retired().get(&(node, member)).copied().flatten()
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
    pub fn bury(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        let Some((retired, true)) = self.unbury(node, member) else {
            return;
        };
        let read_again = self.child_of(node, member).is_some();
        let refused = self
            .nodes
            .get(&retired)
            .is_some_and(|found| found.refused.is_some());
        if read_again || !(self.keep_closed(retired, effects) || refused) {
            self.drop_subtree(retired, effects);
            self.remove_node(retired, effects);
        }
    }

    // The retired node a member was read back through, once the owner has:
    // the member's record keeps that it was given up, and no longer where
    // to read it, and whether no other member reads back through the node.
    fn unbury(&mut self, node: NodeId, member: MemberId) -> Option<(NodeId, bool)> {
        let (retired, _) = self
            .nodes
            .retired_mut(&(node, member))
            .and_then(Option::take)?;
        let last = !self
            .nodes
            .retired()
            .values()
            .flatten()
            .any(|(other, _)| *other == retired);
        Some((retired, last))
    }

    // A member a live object holds was rewritten: its bytes went on to
    // members below, and a refusal of one of those was a verdict on bytes
    // it no longer holds. Every member, at any depth, that the rewritten
    // bytes reached through live objects and that an object refused.
    pub fn refused_through(&self, node: NodeId, member: MemberId) -> Vec<(NodeId, MemberId)> {
        let mut found = Vec::new();
        let mut pending = vec![(node, member)];
        while let Some((node, member)) = pending.pop() {
            let Some((child, volume)) = self.child_of(node, member) else {
                continue;
            };
            let Some(length) = self
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .and_then(|record| record.length)
            else {
                continue;
            };
            let Some(object) = self
                .nodes
                .get(&child)
                .filter(|owner| !owner.retired)
                .and_then(|owner| owner.object.as_ref())
            else {
                continue;
            };
            let mut placements = Vec::new();
            object.locate(volume, ByteRange::new(0, length), &mut placements);
            let mut landed = BTreeSet::new();
            for placement in placements {
                if let Placement::Member { member, .. } = placement {
                    landed.insert(member);
                }
            }
            let refusers = self.refused_below(child);
            for below in landed {
                let refused = refusers.iter().any(|refuser| {
                    self.volumes(*refuser)
                        .is_some_and(|(_, volumes)| volumes.values().any(|held| *held == below))
                });
                if refused {
                    found.push((child, below));
                } else {
                    pending.push((child, below));
                }
            }
        }
        found
    }

    // A recovery object repaired the member, a leaf of `node`'s namespace:
    // a refusal of it was a verdict on bytes it no longer holds, and so was
    // its own front's word that no object owns it. Every object that
    // refused it goes whole, as a buried node does when its volumes are
    // read again: the reader offered them makes every member over. Each of
    // its volumes is a member no object owns, asked again who owns it as a
    // member whose front has just spoken, and so is the member itself.
    // Returns those members; none when an object owns the member.
    pub fn reopen(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) -> Vec<MemberId> {
        if self.child_of(node, member).is_some() {
            return Vec::new();
        }
        // The objects whose refusal the rewrite answers: those holding the
        // member, and those that take the member as a volume of theirs by
        // their own answer to its rewritten front, as `offer` asks it: a set
        // that gave up for want of a volume it never had, which the member
        // is.
        let Some((name, length, front)) = self
            .namespace(node)
            .and_then(|namespace| Some((namespace.get(member)?, namespace.first_bytes(member))))
            .map(|(record, front)| (record.name.clone(), record.length, front.to_vec()))
        else {
            return Vec::new();
        };
        let refusers: Vec<NodeId> = self
            .refused_below(node)
            .into_iter()
            .filter(|object| {
                let holds = self
                    .volumes(*object)
                    .is_some_and(|(_, volumes)| volumes.values().any(|bound| *bound == member));
                holds
                    || self
                        .nodes
                        .get_mut(object)
                        .filter(|refuser| {
                            refuser
                                .binding
                                .as_ref()
                                .is_some_and(|binding| binding.window.is_none())
                        })
                        .and_then(|refuser| refuser.object.as_mut())
                        .is_some_and(|refuser| !refuser.claim(&name, &front, length).is_empty())
            })
            .collect();
        if refusers.is_empty() && !self.unowned(node, member) {
            return Vec::new();
        }
        let mut reopened = vec![member];
        for refuser in refusers {
            if let Some((_, volumes)) = self.volumes(refuser) {
                for volume in volumes.values().copied() {
                    if !reopened.contains(&volume) {
                        reopened.push(volume);
                    }
                }
            }
            self.drop_subtree(refuser, effects);
            self.remove_node(refuser, effects);
        }
        for volume in &reopened {
            self.nodes.unretire(&(node, *volume));
        }
        // The repaired member first: what it binds is offered the rest.
        reopened.sort_by_key(|volume| *volume != member);
        for volume in &reopened {
            self.identify_again(node, *volume, builder, effects);
        }
        reopened
    }

    // What of a buried node stays: each member with a file of its own that
    // closed whole, and each member whose owner keeps something below it,
    // all of them leaves now. A member the object announced and never
    // closed stays too, buried: it is settled as a given-up member is, no
    // object is offered it again, and it is how the tree remembers that the
    // object left one open. It keeps no name and no file. The rest goes with
    // its name, and so does every window, which owns nothing of its member.
    // `false` when nothing stays, and removing the node is the caller's.
    fn keep_closed(&mut self, node: NodeId, effects: &mut Vec<Effect>) -> bool {
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
            self.drop_subtree(window, effects);
            self.remove_node(window, effects);
        }
        for (member, whole, closed) in members {
            let owner = self.child_of(node, member).map(|(child, _)| child);
            if owner.is_some_and(|child| self.keep_closed(child, effects)) {
                continue;
            }
            // A recovery volume lives in its member's own file, as a leaf does.
            let own_file = owner.is_none_or(|child| self.recovers(child));
            let open = !closed && !owner.is_some_and(|child| self.recovers(child));
            if let Some(child) = owner {
                self.drop_subtree(child, effects);
                self.remove_node(child, effects);
            }
            let Some(found) = self.nodes.get_mut(&node) else {
                return false;
            };
            found.disown(member);
            found.namespace.unbind(member);
            if open {
                found.decide(member);
                self.nodes.retire((node, member), None);
                self.release_name((node, member), effects);
            } else if !(whole && own_file) {
                found.namespace.remove(member);
                found.decide(member);
                self.nodes.unretire(&(node, member));
                self.release_name((node, member), effects);
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
                .nodes
                .retired()
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
                .nodes
                .retired()
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
    #[cfg(any(test, feature = "test-support"))]
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
        self.readers(node, member).filter(|id| self.buried(*id))
    }

    // Every node that reads `member` of `node`'s namespace as one of its
    // volumes, live or retired: the object that owns it, an engine bound in
    // a refused object's place, and the refused object itself while it
    // stands.
    pub fn readers(&self, node: NodeId, member: MemberId) -> impl Iterator<Item = NodeId> + '_ {
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
    }

    // The node's namespace, to change: it marks each member it changes, and
    // the node's own row is left unmarked.
    pub fn namespace_mut(&mut self, node: NodeId) -> Option<&mut Namespace> {
        self.nodes.namespace_mut(node)
    }

    // What a member given back carries, until an owner adopts it. Whether
    // the member waits on an owner reads it, so a change marks the member.
    fn give(&mut self, at: (NodeId, MemberId), carried: Carried) {
        self.nodes.mark_member(at.0, at.1);
        self.given.insert(at, carried);
    }

    fn take_given(&mut self, at: &(NodeId, MemberId)) -> Option<Carried> {
        let taken = self.given.remove(at)?;
        self.nodes.mark_member(at.0, at.1);
        Some(taken)
    }

    // The object a member is a volume of, and which volume; a posting of a
    // member is that member's.
    pub fn child_of(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        let found = self.nodes.get(&node)?;
        found
            .children
            .get(&found.namespace.resolve(member))
            .copied()
    }

    // Whether the member's first bytes have not yet decided its owner. Its
    // bytes have nowhere final to go, so the owner holds them.
    pub fn is_undecided(&self, node: NodeId, member: MemberId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|found| found.undecided.contains(&found.namespace.resolve(member)))
    }

    // Whether more of the member's front can still come: it has not ended,
    // the front it has is short of all a member keeps, and no damage stops
    // it, as damage that starts where the bytes in hand from its start run
    // out does. A repair there brings the front back, which asks again who
    // owns it.
    pub fn front_to_come(&self, node: NodeId, member: MemberId) -> bool {
        let Some(namespace) = self.namespace(node) else {
            return false;
        };
        let Some(record) = namespace.get(member) else {
            return false;
        };
        let front = namespace.first_bytes(member).len();
        !record.closed
            && front < FIRST_BYTES
            && !record
                .coverage
                .damaged_ranges()
                .any(|range| usize::try_from(range.start).is_ok_and(|start| start <= front))
    }

    // Whether a member of `node`'s namespace has a front that has not yet
    // decided its owner. It reads the undecided set, not the namespace, so
    // it costs what is undecided, whatever the namespace's size.
    pub fn deciding(&self, node: NodeId) -> bool {
        self.nodes.get(&node).is_some_and(|found| {
            found.undecided.iter().any(|member| {
                found.namespace.get(*member).is_some() && found.namespace.copy_of(*member).is_none()
            })
        })
    }

    // The members of `node` whose front has not yet decided their owner.
    pub fn undecided(&self, node: NodeId) -> impl Iterator<Item = MemberId> + '_ {
        self.nodes
            .get(&node)
            .into_iter()
            .flat_map(|found| found.undecided.iter().copied())
    }

    // Decide an undecided member whose front will say no more: the owner
    // could not hold its bytes any longer, or it ended, or lost its front
    // for good, before the front could say what it is. The objects beside
    // it are offered it with what it has, as a volume of theirs, as a
    // member that has said what it is would be; unclaimed, it is a leaf.
    // One whose front has said, waiting on a holder's front that comes
    // again, waits on: that front decides it, whatever its own did. One
    // beside a lent sibling waits for the loan, as when it was first asked.
    pub fn settle_leaf(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        let Some(found) = self.nodes.get(&node) else {
            return;
        };
        if !found.undecided.contains(&member) {
            return;
        }
        let front = found.namespace.first_bytes(member).to_vec();
        let mut siblings = self.bound_below(node);
        siblings.extend(self.refused_below(node));
        if self.behind_a_loan(node, member, &siblings) {
            return;
        }
        if !self.offer(node, member, &front, &siblings, effects)
            && let Some(mut members) = self.nodes.members_mut(node)
        {
            members.decide(member);
        }
    }

    // A sibling that is lent out cannot be asked whether the member is its
    // volume: the member queues behind the loan, undecided, and is asked
    // again when the sibling is back. Whether it queued.
    fn behind_a_loan(&mut self, node: NodeId, member: MemberId, siblings: &[NodeId]) -> bool {
        let Some(lent) = siblings.iter().copied().find(|sibling| self.lent(*sibling)) else {
            return false;
        };
        self.queue_behind(lent, node, member);
        if let Some(mut members) = self.nodes.members_mut(node) {
            members.undecide(member);
        }
        true
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

    // What `posted` answers for every volume of the object at `node`, as
    // far as each has arrived, owned and apart from the tree. `None` for the
    // root and for a node whose object is out.
    pub fn posting(&self, node: NodeId) -> Option<Box<dyn Posting>> {
        let object = self.object(node)?;
        let (parent, volumes) = self.volumes(node)?;
        let namespace = self.namespace(parent)?;
        let window = self.window(node);
        let ranges: Vec<(VolumeIndex, ByteRange)> = volumes
            .iter()
            .filter_map(|(volume, member)| {
                let length = match window {
                    Some(window) => window.len(),
                    None => {
                        let record = namespace.get(*member)?;
                        let reached = record.coverage.runs().last().map_or(0, |run| run.range.end);
                        record.length.unwrap_or(reached)
                    }
                };
                Some((*volume, ByteRange::new(0, length)))
            })
            .collect();
        Some(object.posting(&ranges))
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
        found.undecide(member);
        self.place(node, member, effects);
        // An object of the namespace may place the member by its name alone,
        // before any of its bytes: it is offered now with no prefix, by the
        // one offer, and asked again when its bytes come if none takes it. A
        // lent object cannot be asked, so the bytes ask it later.
        let siblings = self.bound_below(node);
        if !siblings.is_empty() && !siblings.iter().any(|sibling| self.lent(*sibling)) {
            self.offer(node, member, &[], &siblings, effects);
        }
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
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) -> Option<String> {
        self.namespace_mut(node)?.rename(member, name)?;
        self.place(node, member, effects);
        self.renamed(node, member, builder, effects);
        self.namespace(node)?
            .get(member)
            .map(|record| record.output_name.clone())
    }

    // A name is evidence a claim reads, so a renamed member is a claim to
    // make again. Its owner hears the name and is offered every member
    // beside it that no object owns, which the name may now place after
    // it; a member no object owns is asked again who owns it, as a new
    // member is. An owner its bytes founded then takes what the name now
    // places from a sibling founded by names alone, as it does when it is
    // founded. A sibling with parsed state transfers only what it can
    // carry and this owner accepts.
    fn renamed(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(name) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.name.clone())
        else {
            return;
        };
        let Some((child, volume)) = self.child_of(node, member) else {
            return self.identify_again(node, member, builder, effects);
        };
        self.reach(child, Told::Renamed { volume, name }, builder, effects);
        // The object's answer may already have returned and reassigned it.
        if self.child_of(node, member) != Some((child, volume)) {
            return;
        }
        // A name that says the member is another kind ends the hold, however
        // the object was founded: its reading of the member is refuted and
        // read back, and the member is claimed again as founding claims it.
        if !self.still_holds(node, child, member, volume) {
            return self.unbind(child, Unbinding::Refuted, builder, effects);
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
        if !self.founded_by_name(child) {
            self.take_claimed(node, child, effects);
        }
    }

    // Whether the object at `child` still holds `member` as `volume` by the
    // name the member carries now. It asks what founding asks, whichever
    // came first, the name or the bytes: a kind the name and the front say
    // the member is answers, and only where they say no kind yet does the
    // object's own claim on the name keep it. So a numbered name moves a
    // piece to the split store from an owner its bytes founded, as it would
    // have founded the split store had it come before them.
    fn still_holds(
        &mut self,
        node: NodeId,
        child: NodeId,
        member: MemberId,
        volume: VolumeIndex,
    ) -> bool {
        let Some((name, length, front, said)) = self.namespace(node).and_then(|namespace| {
            let front = namespace.first_bytes(member);
            namespace.get(member).map(|record| {
                (
                    record.name.clone(),
                    record.length,
                    front.to_vec(),
                    record.said(front),
                )
            })
        }) else {
            return true;
        };
        // An engine reads the kind of store it stands in for.
        let owner = self.kind(child);
        match said {
            Sniff::Object(kind) => owner == Some(kind) || owner == kind.engine_for(),
            Sniff::Leaf | Sniff::Pending => self
                .nodes
                .get_mut(&child)
                .and_then(|found| found.object.as_mut())
                .is_none_or(|object| object.claim(&name, &front, length).contains(&volume)),
        }
    }

    // A name was all that held `member` as `volume` of the object at
    // `child`, and its new name places it there no more: the volume leaves
    // the object as one a founded sibling takes does, the member's file its
    // own and nothing copied, and the member is asked again who owns it. An
    // object left with no volume goes.
    fn release_named(
        &mut self,
        node: NodeId,
        child: NodeId,
        (volume, member): (VolumeIndex, MemberId),
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let empty = self.release(node, child, (volume, member), effects);
        self.forget_empty(child, empty, effects);
        self.identify_again(node, member, builder, effects);
    }

    // `member` leaves the object at `child`, where only its name held it as
    // `volume`: the object holds nothing of it from here, and the member's
    // file is its own, nothing copied. Whether the object holds no volume.
    fn release(
        &mut self,
        node: NodeId,
        child: NodeId,
        (volume, member): (VolumeIndex, MemberId),
        effects: &mut Vec<Effect>,
    ) -> bool {
        let mut empty = false;
        if let Some(found) = self.nodes.get_mut(&child) {
            if let Some(binding) = found.binding.as_mut() {
                binding.volumes.remove(&volume);
                binding.awaiting.remove(&volume);
                empty = binding.volumes.is_empty();
            }
            if let Some(object) = found.object.as_mut() {
                object.taken(volume);
            }
        }
        if let Some(found) = self.nodes.get_mut(&node) {
            found.disown(member);
            found.namespace.unbind(member);
        }
        effects.push(Effect::GivenBack {
            node,
            member,
            from: child,
            volume,
        });
        empty
    }

    // The object at `node` gives volumes back, every one it named in one
    // say: its own headers say they are not its where it holds them. A
    // claim is a provisional edge, and this takes the edges back: each
    // member is a leaf of its namespace again, with what the object parsed
    // of it parked beside it. What one say gives back is taken back
    // together, and only then asked again who owns it, as a new member is:
    // the objects beside it are offered it, the giver
    // among them if it still holds a volume, which may take one again where
    // its headers place it; offered while the giver still held the rest, a
    // member would go straight back to it. Nothing below a member goes with
    // it, since an object gives a volume back before it maps any of it into
    // a member of its own. This is pointer work only: no byte of a member
    // is copied or moved here or by the owner, who makes what the object
    // kept of it the member's file, and the next owner reads that file and
    // the member's holds in place. A member whose parse the object will not
    // carry is refused instead. An object left with no volume goes: it was
    // founded on those members alone. A member none of them takes founds
    // its own, as a new member does, where what was parsed of it places it.
    fn give_back(
        &mut self,
        node: NodeId,
        volumes: &[VolumeIndex],
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
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
        let parent = binding.parent;
        let mut taken = Vec::new();
        for volume in volumes {
            if let Some(member) = binding.volumes.remove(volume) {
                binding.awaiting.remove(volume);
                taken.push((*volume, member));
            }
        }
        let empty = binding.volumes.is_empty();
        let mut offered = Vec::new();
        for (volume, member) in taken {
            if self.park(parent, (node, volume), member, effects) {
                offered.push(member);
            }
        }
        self.forget_empty(node, empty, effects);
        for member in offered {
            self.identify_again(parent, member, builder, effects);
        }
    }

    // Whether a member of `node`'s namespace was given back by the object
    // that held it and no object has taken it since: what was parsed of it
    // is parked beside it, and it is a container nothing opened.
    pub fn given_back(&self, node: NodeId, member: MemberId) -> bool {
        self.given.contains_key(&(node, member))
    }

    // What withholds a member given back that no object has taken since: it
    // is a container nothing opened, and that is the tree's say, as it is
    // when the giver carried nothing of it.
    pub fn withholds(&self, node: NodeId, member: MemberId) -> Option<Why> {
        self.given_back(node, member).then_some(Why::Unmapped)
    }

    fn forget_empty(&mut self, node: NodeId, empty: bool, effects: &mut Vec<Effect>) {
        if empty {
            self.drop_subtree(node, effects);
            self.remove_node(node, effects);
        }
    }

    // The ledger again, from what every namespace holds after a rebuild:
    // each member's wanted name and the output name it had. Taken back in
    // order of position, never of arrival, so any order of restoring gives
    // the names that are on disk.
    pub fn rebuild_names(&mut self) {
        self.names.clear();
        let mut members: Vec<(Position, (NodeId, MemberId), String, String)> = Vec::new();
        for (node, found) in self.nodes.iter() {
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

    // A member's file has moved off `name`, so the ledger may give it
    // again, and the member that wanted it while it was left moves to it.
    pub fn vacated(&mut self, name: &str, effects: &mut Vec<Effect>) {
        let mut moved = Vec::new();
        self.names.vacated(name, &mut moved);
        self.report_moves(moved, effects);
    }

    // Restored, every member the ledger kept off the name it wants while a
    // file left it takes the name, now free.
    pub fn settle_names(&mut self, effects: &mut Vec<Effect>) {
        let mut moved = Vec::new();
        self.names.settle(&mut moved);
        self.report_moves(moved, effects);
    }

    // A member leaves the ledger. Each name it was moving off is handed to
    // the member that wanted it while it was left, as that member's move;
    // the member's file goes there once nothing sits at the name on disk.
    fn release_name(&mut self, holder: (NodeId, MemberId), effects: &mut Vec<Effect>) {
        let mut moved = Vec::new();
        self.names.release(holder, &mut moved);
        self.report_moves(moved, effects);
    }

    // A restored member's file still sits at `name`, which it is moving off:
    // the ledger gives it to no other member until it has.
    pub fn leaving(&mut self, node: NodeId, member: MemberId, name: &str) {
        self.names.leaving((node, member), name);
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
        self.report_moves(moved, effects);
    }

    // Members the ledger moved take their new names, and each move is
    // reported.
    fn report_moves(&mut self, moved: Vec<Moved>, effects: &mut Vec<Effect>) {
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

    // A node leaves the tree, and its members' names with it. A set that
    // moved into its namespace is homed where it was posted again.
    fn remove_node(&mut self, node: NodeId, effects: &mut Vec<Effect>) -> Option<Node> {
        let removed = self.nodes.remove(&node)?;
        self.given.retain(|(at, _), _| *at != node);
        for record in removed.namespace.members() {
            self.release_name((node, record.id), effects);
        }
        self.rehome(node);
        for set in self.recovery_nodes(node) {
            self.rehome(set);
        }
        Some(removed)
    }

    // Bytes for a posting of a member of `node`'s namespace. For the root
    // these are the source's deliveries; for any other node they are what
    // its object emitted. The member takes what the namespace says it
    // takes of them: none of a damaged byte another posting brought clean.
    // The member's owner decides what they are; the tree only routes and
    // records.
    pub fn deliver(
        &mut self,
        node: NodeId,
        posting: MemberId,
        arrival: Posted,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Posted {
            offset,
            bytes,
            provenance,
        } = arrival;
        let range = ByteRange::at(offset, bytes.len() as u64);
        let Some(namespace) = self.nodes.namespace_mut(node) else {
            return;
        };
        let (member, taken) = namespace.arrive(posting, range, provenance);
        for (part, landed) in taken {
            let from = (part.start - offset) as usize;
            let piece = bytes.slice(from..from + part.len() as usize);
            let arrived = (part.start, piece, landed);
            self.take(node, member, arrived, builder, effects);
        }
    }

    // What a member took of a delivery: kept at its front, handed to its
    // owner or written as a leaf. A member with no owner is asked again who
    // owns it when bytes reach its front.
    fn take(
        &mut self,
        node: NodeId,
        member: MemberId,
        (offset, bytes, provenance): (u64, Evidenced, Provenance),
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        // An owner of the kind the member's own name says it is, which may
        // hold it by that name alone: it founded the owner, or the owner
        // took it by name before any of its bytes came.
        let named = self.child_of(node, member).filter(|(child, _)| {
            self.namespace(node)
                .and_then(|namespace| namespace.get(member))
                .and_then(|record| crate::sniff::recovery_by_name(&record.name))
                .is_some_and(|kind| self.kind(*child) == Some(kind))
        });
        let Some(namespace) = self.nodes.namespace_mut(node) else {
            return;
        };
        let frontless = namespace.first_bytes(member).is_empty();
        namespace.keep_first_bytes(member, offset, &bytes, provenance);
        let came = frontless && !namespace.first_bytes(member).is_empty();
        // Bytes that landed in the member's contiguous front, whoever sent
        // them: an undecided member is asked who owns it each time, since
        // a front that stopped at a hole grows again when the hole fills.
        let front =
            usize::try_from(offset).is_ok_and(|at| at < namespace.first_bytes(member).len());
        // Bytes outrank names: a front that signs another kind than that
        // owner leaves it, and the member is asked again who owns it, from
        // its bytes.
        if front && let Some((child, volume)) = named {
            let length = namespace.get(member).and_then(|record| record.length);
            let signs = signed(namespace.first_bytes(member), length);
            if matches!(signs, Sniff::Object(kind) if self.kind(child) != Some(kind)) {
                self.release_named(node, child, (volume, member), builder, effects);
            }
        }
        let Some(found) = self.nodes.get(&node) else {
            return;
        };
        let undecided = found.undecided.contains(&member);
        if front && undecided {
            self.identify_again(node, member, builder, effects);
            // A posting the offer folded is its member's from here, these
            // bytes with it; what it held before goes down on `Folded`.
            if self
                .namespace(node)
                .is_some_and(|namespace| namespace.copy_of(member).is_some())
            {
                self.deliver(
                    node,
                    member,
                    Posted {
                        offset,
                        bytes,
                        provenance,
                    },
                    builder,
                    effects,
                );
                return;
            }
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
                let arrived = Arrived {
                    volume,
                    offset,
                    bytes,
                    provenance,
                };
                self.deliver_to_object(child, &arrived, length, builder, effects);
            }
            None => {
                effects.push(Effect::WriteLeaf {
                    node,
                    member,
                    offset,
                    bytes,
                    provenance,
                });
                // A front that grew may say what the member is: one that
                // came late, as one a restart fetches again, or clean bytes
                // over damage that hid it. It says, once they are in its
                // file.
                if front && !undecided {
                    self.identify_again(node, member, builder, effects);
                }
            }
        }
        if came {
            self.front_changed(node, member, false, builder, effects);
        }
    }

    // The member's front changed: it came, or was lost. A member not yet
    // decided whose own front is in hand may have waited on it, as on a
    // holder whose front a restart did not keep: it is asked again who owns
    // it, and folds into the holder, or takes the place of one whose front
    // was lost. A lost front opens that place to a member no object owns as
    // well, which could not wait, so it is asked again too. The undecided
    // set names who may wait; nothing keeps the members no object owns, so
    // only a lost front walks the namespace for them.
    fn front_changed(
        &mut self,
        node: NodeId,
        member: MemberId,
        lost: bool,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let waiting: BTreeSet<MemberId> = self
            .nodes
            .get(&node)
            .map(|found| {
                let unowned: Vec<MemberId> = if lost {
                    found
                        .namespace
                        .members()
                        .map(|record| record.id)
                        .filter(|other| self.unowned(node, *other))
                        .collect()
                } else {
                    Vec::new()
                };
                found
                    .undecided
                    .iter()
                    .copied()
                    .chain(unowned)
                    .filter(|other| {
                        #[cfg(test)]
                        FRONT_CANDIDATES.with(|seen| seen.set(seen.get() + 1));
                        *other != member && !found.namespace.first_bytes(*other).is_empty()
                    })
                    .collect()
            })
            .unwrap_or_default();
        for other in waiting {
            self.identify_again(node, other, builder, effects);
        }
    }

    // The front of a member with no owner changed: it is asked again who
    // owns it, from the front it has now. Its bytes are written as a leaf
    // meanwhile, which is right either way: a bound member's bytes are
    // re-delivered to its object by the owner on `Bound`. A front short of
    // what the sniff reads before its verdict, the whole member's or every
    // signature's, that can grow no more, says no more of what the member
    // is than it did when it stopped: the member settles, and founds
    // nothing from a part of a front.
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
        let Some((front, length)) = self.namespace(node).map(|namespace| {
            let length = namespace.get(member).and_then(|record| record.length);
            (namespace.first_bytes(member).to_vec(), length)
        }) else {
            return;
        };
        let most = VERDICT_BYTES as u64;
        let verdict = length.map_or(most, |length| length.min(most));
        if (front.len() as u64) < verdict && !self.front_to_come(node, member) {
            return self.settle_leaf(node, member, effects);
        }
        self.identify(node, member, &front, builder, effects);
    }

    // A member whose object's reading of it was refuted, once the owner has
    // read its bytes back into its own file: a volume's own bytes refuted
    // the claim its set's names made (`Emit::Refuted`), or the name a
    // recovery set gave it says another kind than the object's (`renamed`).
    // Nothing gave it up: it is a member no object has claimed, and the
    // object goes once the last of its volumes is read back, with
    // everything under it, since nothing of a refuted reading stays.
    //
    // It is claimed as founding claims it, by its name and its front,
    // except that the kind whose reading was refuted is no answer: then its
    // own bytes say what it is. The member keeps that kind (`Member::said`),
    // so a front read back or fetched again later, or after a restart, is
    // asked the same and founds no object of it from the name. Only the
    // objects of the kind it is beside it are
    // offered it, so the set it is takes it where that set places it, and
    // nothing of another kind holding members by their names can take it
    // instead. A member whose bytes have not said yet stays the leaf it was
    // read back as: its file has its bytes, a front fetched again joins them
    // there, and it is asked again once they are in it, as any leaf whose
    // front comes is.
    pub fn claim_by_bytes(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let buried = self.unbury(node, member);
        let refuted_kind = buried.and_then(|(refuted, _)| self.kind(refuted));
        self.nodes.unretire(&(node, member));
        if let Some((refuted, true)) = buried {
            self.drop_subtree(refuted, effects);
            self.remove_node(refuted, effects);
        }
        if let Some(kind) = refuted_kind
            && let Some(record) = self
                .nodes
                .namespace_mut(node)
                .and_then(|namespace| namespace.get_mut(member))
        {
            record.refuted = Some(kind);
        }
        self.claim_as_said(node, member, builder, effects);
    }

    // A member no object owns, claimed by what it says (`Member::said`):
    // offered first to the objects of that kind beside it, else founding
    // one; a member that says no kind stays the leaf it is.
    fn claim_as_said(
        &mut self,
        node: NodeId,
        member: MemberId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(namespace) = self.namespace(node) else {
            return;
        };
        let front = namespace.first_bytes(member).to_vec();
        let Some((name, length, said)) = namespace
            .get(member)
            .map(|record| (record.name.clone(), record.length, record.said(&front)))
        else {
            return;
        };
        let Sniff::Object(kind) = said else {
            return;
        };
        let kin: Vec<NodeId> = self
            .bound_below(node)
            .into_iter()
            .filter(|sibling| self.kind(*sibling) == Some(kind))
            .collect();
        if self.offer(node, member, &front, &kin, effects) {
            return;
        }
        self.found(
            node,
            member,
            kind,
            (&name, &front, length),
            builder,
            effects,
        );
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
        if self.behind_a_loan(node, member, &siblings) {
            return;
        }
        if self.offer(node, member, prefix, &siblings, effects) {
            return;
        }
        let Some(mut members) = self.nodes.members_mut(node) else {
            return;
        };
        let Some((name, length, said)) = members
            .namespace
            .get(member)
            .map(|record| (record.name.clone(), record.length, record.said(prefix)))
        else {
            return;
        };
        match said {
            Sniff::Pending => {}
            Sniff::Leaf => {
                members.decide(member);
            }
            Sniff::Object(kind) => {
                self.found(
                    node,
                    member,
                    kind,
                    (&name, prefix, length),
                    builder,
                    effects,
                );
            }
        }
    }

    // Build the object `member` founds, of `kind`, and bind the member to it.
    // The member is the volume its new object places it at, the position its
    // set gives it, whichever volume came first. A member given back is
    // placed by what was parsed of it, and founds nothing where that places
    // it nowhere: it waits for the set it continues. The new object is
    // offered every member with no owner, and then a recovery volume none
    // took, its bytes not yet come, founds the set its name says it is.
    // A member of a set already refuted founds none: it is refuted with
    // it, and claimed by what its bytes say.
    fn found(
        &mut self,
        node: NodeId,
        member: MemberId,
        kind: Kind,
        (name, prefix, length): (&str, &[u8], Option<u64>),
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if let Some(mut members) = self.nodes.members_mut(node) {
            members.decide(member);
        }
        if self.refuted_with_its_set(node, member, kind, (name, prefix, length), builder) {
            self.claim_as_said(node, member, builder, effects);
            return;
        }
        let volumes = |object: &mut dyn Object, given: &Given| {
            let volume = match given.get(&(node, member)) {
                Some(carried) => only(object.reclaim(name, prefix, length, carried.as_ref()))?,
                None => only(object.claim(name, prefix, length)).unwrap_or(VolumeIndex(0)),
            };
            Some(BTreeMap::from([(volume, member)]))
        };
        match self.attach(node, kind, volumes, builder) {
            Ok(Some(child)) => {
                self.bound(node, member, kind, child, effects);
                self.offer_unowned(node, child, effects);
                if matches!(signed(prefix, length), Sniff::Object(_)) {
                    self.take_claimed(node, child, effects);
                }
                self.found_by_name(node, builder, effects);
            }
            Ok(None) => {}
            Err(why) => effects.push(Effect::Refused { node, member, why }),
        }
    }

    // Whether `member`, not yet refuted and whose own bytes do not sign
    // `kind`, is a member of a set of `kind` already refuted, and is now
    // marked refuted with it: an object of that kind bound to it alone, as
    // founding would bind it, claims a member whose reading as `kind` was
    // refuted. The object says which names are its set; the refutation is a
    // fact of each member, so a member posted or read after it is refuted
    // as the others were.
    fn refuted_with_its_set(
        &mut self,
        node: NodeId,
        member: MemberId,
        kind: Kind,
        (name, prefix, length): (&str, &[u8], Option<u64>),
        builder: &mut dyn Builder,
    ) -> bool {
        if signed(prefix, length) == Sniff::Object(kind) {
            return false;
        }
        let Some(namespace) = self.namespace(node) else {
            return false;
        };
        if namespace
            .get(member)
            .is_none_or(|record| record.refuted.is_some())
        {
            return false;
        }
        let refuted: Vec<(String, Vec<u8>, Option<u64>)> = namespace
            .members()
            .filter(|record| record.id != member && record.refuted == Some(kind))
            .map(|record| {
                let front = namespace.first_bytes(record.id).to_vec();
                (record.name.clone(), front, record.length)
            })
            .collect();
        if refuted.is_empty() {
            return false;
        }
        let Some(mut object) = builder.build(kind, ObjectId(self.next_node)) else {
            return false;
        };
        let volume = only(object.claim(name, prefix, length)).unwrap_or(VolumeIndex(0));
        object.bound(volume, name, prefix, length);
        if refuted
            .iter()
            .all(|(other, front, length)| object.claim(other, front, *length).is_empty())
        {
            return false;
        }
        let Some(record) = self
            .nodes
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
        else {
            return false;
        };
        record.refuted = Some(kind);
        true
    }

    // A recovery volume of the post whose bytes have not come, that no
    // object of its namespace took by its name, founds the set its name
    // says it is: the set counts it as coming from the moment the post is
    // known, as a set already there would. Its hold is its name's: when its
    // own front comes and signs another kind, the member leaves the set and
    // its bytes say what it is; otherwise an object another member's bytes
    // found takes it where its own claim places it, as from any object
    // founded on names.
    fn found_by_name(
        &mut self,
        node: NodeId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let named = self.namespace(node).and_then(|namespace| {
            namespace
                .members()
                .filter(|record| self.undecided_recovery(node, record.id))
                .find_map(|record| {
                    let kind = crate::sniff::recovery_by_name(&record.name)?;
                    Some((record.id, record.name.clone(), record.length, kind))
                })
        });
        if let Some((member, name, length, kind)) = named {
            self.found(node, member, kind, (&name, &[], length), builder, effects);
        }
    }

    // A member no object owns: a leaf whose front has spoken, and which no
    // object is giving up.
    fn unowned(&self, node: NodeId, member: MemberId) -> bool {
        self.nodes.get(&node).is_some_and(|found| {
            found.namespace.copy_of(member).is_none()
                && found.namespace.get(member).is_some_and(Member::is_leaf)
                && !found.children.contains_key(&member)
                && !found.undecided.contains(&member)
                && !self.nodes.retired().contains_key(&(node, member))
        })
    }

    // A member whose front never came: a name is all that holds it as any
    // object's volume, and a member whose front came outranks it.
    fn held_by_name(&self, node: NodeId, member: MemberId) -> bool {
        self.child_of(node, member).is_some()
            && self
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| !record.fronted)
    }

    // An object founded on nothing but names: none of its volumes' fronts
    // signs anything, so its hold on each is its name's. A front that came
    // and is no longer kept placed its volume by more than a name.
    fn founded_by_name(&self, node: NodeId) -> bool {
        let Some(found) = self.nodes.get(&node) else {
            return false;
        };
        let Some(binding) = found.binding.as_ref() else {
            return false;
        };
        let Some(namespace) = self.namespace(binding.parent) else {
            return false;
        };
        binding.volumes.values().all(|member| {
            let Some(record) = namespace.get(*member) else {
                return true;
            };
            let front = namespace.first_bytes(*member);
            !record.fronted
                || (!front.is_empty() && !matches!(signed(front, record.length), Sniff::Object(_)))
        })
    }

    // A member's front, where it lies in its namespace.
    fn front(&self, node: NodeId, member: MemberId) -> &[u8] {
        self.namespace(node)
            .map_or(&[][..], |namespace| namespace.first_bytes(member))
    }

    // The volumes `sibling` can give up to an object its bytes founded,
    // with their members: every one, when names alone hold them; else the
    // ones its object says it can carry, which it keeps as it goes.
    fn transferable(&self, sibling: NodeId, named: bool) -> Vec<(VolumeIndex, MemberId)> {
        let Some(found) = self.nodes.get(&sibling) else {
            return Vec::new();
        };
        let Some(binding) = found.binding.as_ref() else {
            return Vec::new();
        };
        if named {
            return binding.volumes.iter().map(|(v, m)| (*v, *m)).collect();
        }
        let mut carriable = Vec::new();
        if let Some(object) = found.object.as_ref() {
            object.carriable(&mut carriable);
        }
        carriable
            .into_iter()
            .filter_map(|volume| Some((volume, *binding.volumes.get(&volume)?)))
            .collect()
    }

    // An established object takes the volumes its siblings can transfer
    // and its own claims accept. A sibling founded on names alone yields
    // every volume to bytes; any other must consent, and offers only the
    // volumes it says it can carry, so one whose volumes all route output
    // offers none. The receiver judges that parse, the front read where it
    // lies, before any edge changes, and a front is copied only for a
    // volume it accepts. The existing give-back/adopt path keeps its bytes
    // in place.
    fn take_claimed(&mut self, node: NodeId, child: NodeId, effects: &mut Vec<Effect>) {
        let siblings: Vec<NodeId> = self
            .bound_below(node)
            .into_iter()
            .filter(|sibling| *sibling != child && !self.lent(*sibling))
            .collect();
        for sibling in siblings {
            let named = self.founded_by_name(sibling);
            let held = self.transferable(sibling, named);
            for (volume, member) in held {
                let Some((name, length)) = self.namespace(node).and_then(|namespace| {
                    namespace
                        .get(member)
                        .map(|record| (record.name.clone(), record.length))
                }) else {
                    continue;
                };
                let place = if named {
                    let front = self.front(node, member).to_vec();
                    self.nodes
                        .get_mut(&child)
                        .and_then(|found| found.object.as_mut())
                        .and_then(|object| only(object.claim(&name, &front, length)))
                } else {
                    self.nodes
                        .get(&sibling)
                        .and_then(|found| found.object.as_ref())
                        .and_then(|object| object.carried(volume))
                        .and_then(|carried| {
                            self.nodes
                                .get(&child)
                                .and_then(|found| found.object.as_ref())
                                .and_then(|object| {
                                    only(object.reclaim(
                                        &name,
                                        self.front(node, member),
                                        length,
                                        carried,
                                    ))
                                })
                        })
                };
                let Some(place) = place else {
                    continue;
                };
                // A place the object holds already: the member is another
                // posting of the one there, or stays where its name put it.
                // A member whose front never came, at a place the object
                // holds by bytes, is a volume of neither: a name was all
                // that held it, and it is a member no object owns.
                let holder = self
                    .nodes
                    .get(&child)
                    .and_then(|found| found.binding.as_ref())
                    .and_then(|binding| binding.volumes.get(&place))
                    .copied();
                let folds = holder.is_none_or(|into| {
                    self.namespace(node)
                        .is_some_and(|namespace| namespace.folds(member, into))
                });
                let outranked = holder.is_some_and(|into| {
                    self.front(node, member).is_empty() && !self.held_by_name(node, into)
                });
                if !folds && !outranked {
                    continue;
                }
                if !named {
                    let Some(carried) = self
                        .nodes
                        .get_mut(&sibling)
                        .and_then(|found| found.object.as_mut())
                        .and_then(|object| object.give_back(volume))
                    else {
                        continue;
                    };
                    self.give((node, member), carried);
                }
                self.release(node, sibling, (volume, member), effects);
                match holder {
                    Some(into) if folds => {
                        self.fold(node, member, into, effects);
                    }
                    Some(_) => {}
                    None => {
                        let front = self.front(node, member).to_vec();
                        self.join(node, member, (child, place), (&name, &front), effects);
                    }
                }
            }
            let empty = self
                .nodes
                .get(&sibling)
                .and_then(|found| found.binding.as_ref())
                .is_some_and(|binding| binding.volumes.is_empty());
            self.forget_empty(sibling, empty, effects);
        }
    }

    // Offer every member of `node`'s namespace that has no owner to the
    // object bound at `child`, by the one offer a new member gets. A member
    // whose bytes have not said what it is yet, under a name an object may
    // claim, is offered too, by that name, as it would have been had it
    // been announced after the object: a set counts it as coming from the
    // moment the post is known, and its bytes bind it for real when they
    // come.
    fn offer_unowned(&mut self, node: NodeId, child: NodeId, effects: &mut Vec<Effect>) {
        let offered = |tree: &Self, member: MemberId| {
            tree.unowned(node, member)
                || tree.undecided_named(node, member, crate::sniff::names_claimable)
        };
        let unowned: Vec<(MemberId, Vec<u8>)> = self
            .namespace(node)
            .map(|namespace| {
                namespace
                    .members()
                    .filter(|record| offered(self, record.id))
                    .map(|record| (record.id, namespace.first_bytes(record.id).to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        for (member, front) in unowned {
            // A take can take others with it: offer only what is left.
            if offered(self, member) {
                self.offer(node, member, &front, &[child], effects);
            }
        }
    }

    // A member whose bytes have not yet said what it is, no object holds,
    // and whose posted name says it is recovery data.
    fn undecided_recovery(&self, node: NodeId, member: MemberId) -> bool {
        self.undecided_named(node, member, crate::sniff::names_recovery)
    }

    // A member whose bytes have not yet said what it is, no object holds,
    // and whose posted name `says` what it is.
    fn undecided_named(&self, node: NodeId, member: MemberId, says: fn(&str) -> bool) -> bool {
        self.nodes.get(&node).is_some_and(|found| {
            found.namespace.copy_of(member).is_none()
                && found.undecided.contains(&member)
                && !found.children.contains_key(&member)
                && !self.nodes.retired().contains_key(&(node, member))
                && found
                    .namespace
                    .get(member)
                    .is_some_and(|record| says(&record.name))
        })
    }

    // The one offer of a member with no owner to objects of its namespace,
    // in the order given. Each object answers the places the member could
    // take, held or free, and the tree judges them once, whatever the
    // format: a member that is another posting of a holder folds into it; a
    // holder whose front is coming again keeps the member waiting on it; a
    // place another member holds, their fronts differing, is no place for
    // it, and one whose fronts are not both in hand is not judged, so no
    // other place is chosen past it. A place whose holder's front was lost
    // is open, as a free one is, and taking it displaces the holder. Of the
    // places left, one takes it, and several leave it unclaimed, since
    // nothing orders it. Where the object answers one place, the member's
    // bytes also outrank a holder held by its name alone or
    // provisionally. An object that cannot judge the member yet keeps it
    // waiting while more of its front can come, and once none can, answers
    // the places the front it has gives it. Whether one took it, or the
    // member, not yet decided, waits undecided on a holder's front that
    // comes again or on its own front, and is asked again when it comes.
    fn offer(
        &mut self,
        node: NodeId,
        member: MemberId,
        prefix: &[u8],
        objects: &[NodeId],
        effects: &mut Vec<Effect>,
    ) -> bool {
        // How the member takes a place left to it: free, or from the member
        // holding it by its name alone, or holding it as one that yields.
        #[derive(Clone, Copy)]
        enum Taking {
            Free,
            Named(MemberId),
            Yields(MemberId),
        }
        let Some((name, length)) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| (record.name.clone(), record.length))
        else {
            return false;
        };
        let mut waits = false;
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
            let answer = found
                .object
                .as_mut()
                .map(|object| match carried {
                    Some(carried) => object.reclaim(&name, prefix, length, carried.as_ref()),
                    None => object.claim(&name, prefix, length),
                })
                .unwrap_or_default();
            let places = match answer {
                Claim::NotYet(_) if self.front_to_come(node, member) => {
                    waits = true;
                    continue;
                }
                Claim::Places(places) | Claim::NotYet(places) => places,
            };
            let one = places.len() == 1;
            let mut open = Vec::new();
            let mut coming = false;
            let mut judged = true;
            for volume in places {
                // A volume the object holds already: the member is another
                // posting of the one holding it, or none of the object's.
                let holder = self
                    .nodes
                    .get(child)
                    .and_then(|found| found.binding.as_ref())
                    .and_then(|binding| binding.volumes.get(&volume))
                    .copied();
                let Some(into) = holder else {
                    open.push((volume, Taking::Free));
                    continue;
                };
                if into != member && self.fold(node, member, into, effects) {
                    return true;
                }
                // The member it holds there already is asked of the others.
                if into == member || refused.is_some() {
                    continue;
                }
                let fronted = !prefix.is_empty();
                // A holder whose front came and is not in hand: the
                // claimant's front can fold into it only once that front
                // comes again. Lost instead, the claimant's bytes outrank
                // it where they name its place alone; still coming, a
                // claimant not yet decided waits on it.
                let away = fronted
                    .then(|| self.namespace(node)?.front_away(into))
                    .flatten();
                // A holder whose front never came holds its place by its
                // name alone, which the claimant's bytes outrank: it leaves
                // the object as a volume a founded sibling takes does, and
                // is a member no object owns, as it would be had the
                // claimant come first.
                if one && fronted && self.held_by_name(node, into) {
                    open.push((volume, Taking::Named(into)));
                } else if away == Some(true)
                    || one
                        && self
                            .nodes
                            .get(child)
                            .and_then(|found| found.object.as_ref())
                            .is_some_and(|object| {
                                object.provisional(
                                    volume,
                                    &name,
                                    prefix,
                                    self.given.get(&(node, member)),
                                )
                            })
                {
                    open.push((volume, Taking::Yields(into)));
                } else {
                    // Fronts that differ put the place out of its reach;
                    // one not in hand, its own or the holder's, leaves the
                    // place unjudged, and no other place is its while one is.
                    coming |= away == Some(false);
                    judged &= fronted
                        && self
                            .namespace(node)
                            .is_some_and(|namespace| !namespace.first_bytes(into).is_empty());
                }
            }
            if coming {
                waits |= self.is_undecided(node, member);
                continue;
            }
            let ([(volume, taking)], true) = (&open[..], judged) else {
                continue;
            };
            let (volume, taking) = (*volume, *taking);
            match taking {
                Taking::Free => {}
                Taking::Named(into) => {
                    self.release(node, *child, (volume, into), effects);
                    // It has no byte authority yet, so a front still to come
                    // must receive a new ownership decision. One that will
                    // never come says no more: it stays the leaf it is, as a
                    // member whose front was lost is settled a leaf.
                    if self.front_to_come(node, into)
                        && let Some(mut members) = self.nodes.members_mut(node)
                    {
                        members.undecide(into);
                    }
                }
                Taking::Yields(into) => self.displace(node, (*child, volume), into, effects),
            }
            match refused {
                None => self.join(node, member, (*child, volume), (&name, prefix), effects),
                // A volume of a refused set is refused with it. The object
                // never held any of it, so it is a leaf already, and settled.
                // It hears the volume as any object does, so what it claims
                // next is claimed beside it.
                Some(why) => {
                    let carried = self.take_given(&(node, member));
                    if let Some(found) = self.nodes.get_mut(child) {
                        if let Some(binding) = found.binding.as_mut() {
                            binding.volumes.insert(volume, member);
                        }
                        if let Some(object) = found.object.as_mut() {
                            match carried {
                                Some(carried) => object.adopt(volume, &name, carried),
                                None => object.bound(volume, &name, prefix, length),
                            }
                        }
                    }
                    if let Some(mut members) = self.nodes.members_mut(node) {
                        members.decide(member);
                    }
                    self.nodes.retire((node, member), None);
                    effects.push(Effect::Refused { node, member, why });
                }
            }
            return true;
        }
        waits
    }

    // The holder of a place its object holds provisionally makes way for a
    // claim that names the place, as take_claimed moves a volume: the holder
    // leaves the object with what was parsed of it, its holds in place, and
    // is parked as a member given back, so the claimant joins the place
    // first and the holder is offered again beside it.
    fn displace(
        &mut self,
        node: NodeId,
        (child, volume): (NodeId, VolumeIndex),
        holder: MemberId,
        effects: &mut Vec<Effect>,
    ) {
        if let Some(binding) = self
            .nodes
            .get_mut(&child)
            .and_then(|found| found.binding.as_mut())
        {
            binding.volumes.remove(&volume);
            binding.awaiting.remove(&volume);
        }
        self.park(node, (child, volume), holder, effects);
    }

    // A volume out of its object's binding leaves the object with what was
    // parsed of it and is parked beside `parent`'s namespace as a member
    // given back, or is refused when the object will not carry it. Pointer
    // work: the member's holds stay in place. Whether it is parked, for the
    // caller to offer it again.
    fn park(
        &mut self,
        parent: NodeId,
        (child, volume): (NodeId, VolumeIndex),
        member: MemberId,
        effects: &mut Vec<Effect>,
    ) -> bool {
        let carried = self
            .nodes
            .get_mut(&child)
            .and_then(|found| found.object.as_mut())
            .and_then(|object| object.give_back(volume));
        if let Some(found) = self.nodes.get_mut(&parent) {
            found.disown(member);
            found.namespace.unbind(member);
        }
        effects.push(Effect::GivenBack {
            node: parent,
            member,
            from: child,
            volume,
        });
        match carried {
            Some(carried) => {
                // A completed earlier refusal is no longer this member's
                // disposition: its current owner explicitly gave it back.
                if self
                    .nodes
                    .retired()
                    .get(&(parent, member))
                    .is_some_and(Option::is_none)
                {
                    self.nodes.unretire(&(parent, member));
                }
                self.give((parent, member), carried);
                true
            }
            None => {
                self.nodes.retire((parent, member), None);
                effects.push(Effect::Refused {
                    node: parent,
                    member,
                    why: Why::Unmapped,
                });
                false
            }
        }
    }

    // A member an object claims as a volume it holds already is another
    // posting of the member holding it, when the namespace says so: it
    // folds into that member, whose bytes its bytes are from now on, and
    // leaves the ledger, since it is never a file. Whether it folded.
    fn fold(
        &mut self,
        node: NodeId,
        copy: MemberId,
        into: MemberId,
        effects: &mut Vec<Effect>,
    ) -> bool {
        let Some(found) = self.nodes.get_mut(&node) else {
            return false;
        };
        let Some(held) = found.namespace.fold(copy, into) else {
            return false;
        };
        found.decide(copy);
        self.take_given(&(node, copy));
        self.release_name((node, copy), effects);
        effects.push(Effect::Folded {
            node,
            copy,
            into,
            held,
        });
        true
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
        let carried = self.take_given(&(node, member));
        let Some(parent) = self.nodes.get_mut(&node) else {
            return;
        };
        parent.decide(member);
        parent.own(member, (child, volume));
        parent.namespace.bind(member, ObjectId(child.0));
        let length = parent
            .namespace
            .get(member)
            .and_then(|record| record.length);
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
                None => object.bound(volume, name, front, length),
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
    // freshly built object of `kind`, which `volumes` may ask where they go,
    // by what was parsed of those given back. The one place an object comes
    // to own members, and so where the nesting limit is held: nothing is
    // bound, and why, when the object would open a namespace past it or no
    // object of that kind can be built; nothing, and no why, when the object
    // places none of them. A member given back is adopted as it stands.
    fn attach(
        &mut self,
        node: NodeId,
        kind: Kind,
        volumes: impl FnOnce(&mut dyn Object, &Given) -> Option<BTreeMap<VolumeIndex, MemberId>>,
        builder: &mut dyn Builder,
    ) -> Result<Option<NodeId>, Why> {
        if self.depth(node) >= self.nesting {
            return Err(Why::TooDeep);
        }
        let child = NodeId(self.next_node);
        // Object ids are node ids: one object per node, for life.
        let id = ObjectId(child.0);
        let mut object = builder.build(kind, id).ok_or(Why::Unsupported)?;
        let Some(volumes) = volumes(object.as_mut(), &self.given) else {
            return Ok(None);
        };
        self.next_node += 1;
        let parent = self.nodes.get_mut(&node).ok_or(Why::Unsupported)?;
        for (volume, member) in &volumes {
            if let Some(record) = parent.namespace.get(*member) {
                match self.given.remove(&(node, *member)) {
                    Some(carried) => object.adopt(*volume, &record.name, carried),
                    None => {
                        let front = parent.namespace.first_bytes(*member);
                        object.bound(*volume, &record.name, front, record.length)
                    }
                }
            }
            parent.namespace.bind(*member, id);
            parent.own(*member, (child, *volume));
        }
        let binding = Binding {
            parent: node,
            volumes,
            window: None,
            awaiting: BTreeSet::new(),
            handed: Handed::default(),
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        self.rehome(child);
        Ok(Some(child))
    }

    // Bind `range` of a member of `node`'s namespace, as volume 0, to a
    // freshly built object of `kind`, handed what the member's owner
    // handed it: a window beside the member's owner, which keeps the
    // member. `passed` is what of the range arrived before, read back by
    // the owner, in the member's offsets; the window takes it now, and the
    // rest as it passes. `None`, and nothing bound, when no object of that
    // kind can be built or the range already has a window, which takes
    // what was handed in place of what it held, now or when its object is
    // taken back.
    pub fn attach_window(
        &mut self,
        node: NodeId,
        (member, range): (MemberId, ByteRange),
        (kind, handed): (Kind, &Handed),
        passed: Vec<(u64, Bytes, Provenance)>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) -> Option<NodeId> {
        let record = self.nodes.get(&node)?.namespace.get(member)?;
        let (name, closed, length) = (record.name.clone(), record.closed, record.length);
        let taken = self.windows_on(node, member).into_iter().find(|window| {
            self.nodes
                .get(window)
                .and_then(|found| found.binding.as_ref())
                .is_some_and(|binding| binding.window == Some(range))
        });
        if let Some(window) = taken {
            self.hand(window, handed);
            return None;
        }
        let child = NodeId(self.next_node);
        let mut object = builder.build(kind, ObjectId(child.0))?;
        self.next_node += 1;
        object.bound(VolumeIndex(0), &name, &[], length);
        let binding = Binding {
            parent: node,
            volumes: BTreeMap::from([(VolumeIndex(0), member)]),
            window: Some(range),
            awaiting: BTreeSet::new(),
            handed: Handed::default(),
        };
        self.nodes
            .insert(child, Node::new(Some(object), Some(binding)));
        self.hand(child, handed);
        self.nodes
            .get_mut(&node)?
            .windows
            .entry(member)
            .or_default()
            .push(child);
        self.rehome(child);
        for (offset, bytes, provenance) in passed {
            let bytes = Evidenced::new(bytes);
            self.deliver_window(child, offset, &bytes, provenance, builder, effects);
        }
        if closed {
            self.close_window(child, builder, effects);
        }
        Some(child)
    }

    // The window's owner hands its object what it reads the range by. The
    // node keeps it, so an object lent out meanwhile, or rebuilt from its
    // facts in its place, is handed it when it is taken back.
    fn hand(&mut self, window: NodeId, handed: &Handed) {
        let Some(found) = self.nodes.get_mut(&window) else {
            return;
        };
        if let Some(binding) = found.binding.as_mut() {
            binding.handed = handed.clone();
        }
        if let Some(object) = found.object.as_mut() {
            object.handed(handed);
        }
    }

    // The part of a delivery to the window's member that falls in the
    // window, delivered to its object at the window's own offsets.
    fn deliver_window(
        &mut self,
        window: NodeId,
        offset: u64,
        bytes: &Evidenced,
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
        let arrived = Arrived {
            volume: VolumeIndex(0),
            offset: piece.start - range.start,
            bytes: bytes.slice(start..start + piece.len() as usize),
            provenance,
        };
        self.deliver_to_object(window, &arrived, Some(range.len()), builder, effects);
    }

    fn deliver_to_object(
        &mut self,
        node: NodeId,
        arrived: &Arrived,
        length: Option<u64>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if let Some(lent) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.lent.as_mut())
        {
            let arrived = Arrived {
                bytes: arrived.bytes.clone(),
                ..*arrived
            };
            return lent.wait(Waiting::Delivered { arrived, length });
        }
        let delivery = arrived.delivery();
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        // The object is out of its node while it takes the delivery, so its
        // reads can walk the tree below it. A retired object hears nothing:
        // its word was its last.
        if let Some(mut object) = self
            .nodes
            .get_mut(&node)
            .filter(|found| !found.retired)
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
        // What one say gives back is taken back together, after the rest,
        // and what it asks for again is asked together: each range reads
        // back as the object stood before any of them comes again.
        let mut given = Vec::new();
        let mut again = Vec::new();
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
                    // record, but not its length; the name it is announced
                    // under now is the one its producer read from the bytes
                    // it starts over on, so a different one renames it. A
                    // length said again is the producer's word on it now: a
                    // store whose geometry moved says its member anew.
                    let mut renamed = false;
                    if let Some(record) = self
                        .namespace_mut(node)
                        .and_then(|namespace| namespace.get_mut(member))
                    {
                        record.length = length.or(record.length);
                        renamed = record.name != name;
                    }
                    if renamed {
                        self.rename(node, member, &name, builder, effects);
                    }
                    effects.push(Effect::Announced { node, member });
                }
                Emit::Bytes {
                    member,
                    offset,
                    bytes,
                    provenance,
                } => {
                    self.deliver(
                        node,
                        member,
                        Posted {
                            offset,
                            bytes,
                            provenance,
                        },
                        builder,
                        effects,
                    );
                }
                Emit::Checksum { member, crc32 } => {
                    if let Some(record) = self
                        .namespace_mut(node)
                        .and_then(|namespace| namespace.get_mut(member))
                    {
                        record.crc32 = Some(crc32);
                    }
                }
                Emit::MemberClosed(member) => self.close(node, member, builder, effects),
                Emit::Retract(member) => self.retract(node, member, effects),
                Emit::RecoveryAt {
                    volume,
                    range,
                    kind,
                    handed,
                } => {
                    effects.push(Effect::RecoveryAt {
                        node,
                        volume,
                        range,
                        kind,
                        handed,
                    });
                }
                Emit::Unkept {
                    volume,
                    range,
                    envelope,
                    at,
                } => self.unkept(node, (volume, range), (envelope, at), effects),
                Emit::Again { volume, range } => again.push((volume, range)),
                Emit::Damaged { volume, range } => self.damaged(node, volume, range, effects),
                Emit::NeedsPassword => effects.push(Effect::NeedsPassword { node }),
                Emit::NeedsEngine(why) => {
                    self.unbind(node, Unbinding::Engine(why), builder, effects);
                }
                Emit::Refuse(why) => self.unbind(node, Unbinding::Refused(why), builder, effects),
                Emit::Refuted => self.unbind(node, Unbinding::Refuted, builder, effects),
                Emit::GiveBack(volume) => given.push(volume),
            }
            if self.nodes.get(&node).is_none_or(|found| found.retired) {
                // Whatever the object said after giving its volumes up is
                // not heard: the drain drops it.
                return;
            }
        }
        if !again.is_empty() {
            effects.push(Effect::Again {
                node,
                ranges: again,
            });
        }
        if !given.is_empty() {
            self.give_back(node, &given, builder, effects);
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
            self.drop_subtree(child, effects);
            let volumes = self.detach(child, effects);
            if let Some(found) = self.nodes.get_mut(&node) {
                for volume in &volumes {
                    found.disown(*volume);
                    found.namespace.retract(*volume);
                    found.undecide(*volume);
                    self.given.remove(&(node, *volume));
                }
                for volume in volumes {
                    self.nodes.unretire(&(node, volume));
                }
            }
        }
        // A member that starts over was given up by nothing yet.
        if let Some(found) = self.nodes.get_mut(&node) {
            found.namespace.retract(member);
            found.undecide(member);
            self.nodes.unretire(&(node, member));
            self.take_given(&(node, member));
        }
        // A window was a range of the member's old bytes.
        for window in self.windows_on(node, member) {
            self.drop_subtree(window, effects);
            self.remove_node(window, effects);
        }
        if let Some(found) = self.nodes.get_mut(&node) {
            found.windows.remove(&member);
        }
        effects.push(Effect::Retracted { node, member });
    }

    // The object at `node` gives its members up: every volume it owned
    // becomes a leaf of the parent namespace, or, when it asked for an
    // engine and its kind has one, the volumes of an engine bound in its
    // place, or, when a volume's bytes refuted its claim, a member the owner
    // has claimed by its own bytes once it has its bytes back. Every way the
    // object and its subtree retire: they answer where their bytes went
    // until the owner, told once per volume, has read them back and buried
    // them.
    fn unbind(
        &mut self,
        node: NodeId,
        unbinding: Unbinding,
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
            self.drop_subtree(node, effects);
            self.remove_node(node, effects);
            if let Some(found) = self.nodes.get_mut(&binding.parent) {
                for windows in found.windows.values_mut() {
                    windows.retain(|window| *window != node);
                }
            }
            return;
        }
        let parent = binding.parent;
        if let Some(found) = self.nodes.get_mut(&parent) {
            for member in binding.volumes.values() {
                found.disown(*member);
                found.namespace.unbind(*member);
            }
        }
        for (volume, member) in &binding.volumes {
            self.nodes.retire((parent, *member), Some((node, *volume)));
        }
        let (why, engine) = match unbinding {
            Unbinding::Refused(why) => (why, false),
            Unbinding::Engine(why) => (why, true),
            // Nothing holds the set's volumes now: each is claimed by its
            // own bytes once the owner has them back, and nothing failed.
            Unbinding::Refuted => {
                for member in binding.volumes.into_values() {
                    effects.push(Effect::Disclaimed {
                        node: parent,
                        member,
                    });
                }
                return;
            }
        };
        let engine = kind
            .and_then(Kind::engine_for)
            .filter(|_| engine)
            .and_then(|kind| {
                let handed: Vec<_> = self
                    .nodes
                    .get(&node)
                    .and_then(|found| found.object.as_ref())
                    .into_iter()
                    .flat_map(|object| {
                        binding.volumes.iter().filter_map(|(volume, member)| {
                            Some((*member, object.engine_volume(*volume)?))
                        })
                    })
                    .collect();
                for (member, metadata) in handed {
                    self.give((parent, member), metadata);
                }
                let volumes = binding.volumes.clone();
                let attached = self
                    .attach(parent, kind, |_, _| Some(volumes), builder)
                    .ok()
                    .flatten();
                if attached.is_none() {
                    for member in binding.volumes.values() {
                        self.take_given(&(parent, *member));
                    }
                }
                attached
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
        // What no object owns beside the set is offered to what holds it
        // now: the engine, or the refused set, which claims the rest of
        // itself so the rest goes where the set went.
        let holder = match engine {
            Some(engine) => engine,
            None => {
                if let Some(found) = self.nodes.refuse(&node, why)
                    && let Some(object) = found.object.as_mut()
                {
                    object.refused();
                }
                node
            }
        };
        self.offer_unowned(parent, holder, effects);
    }

    fn retire(&mut self, node: NodeId) {
        let mut pending = vec![node];
        while let Some(at) = pending.pop() {
            pending.extend(self.bound_below(at));
            if let Some(found) = self.nodes.get_mut(&at) {
                found.retired = true;
            }
            self.rehome(at);
        }
    }

    // The objects bound in `node`'s namespace that refused their volumes.
    fn refused_below(&self, node: NodeId) -> Vec<NodeId> {
        self.nodes.refused_below(node).collect()
    }

    // Remove a node and return the parent members that were its volumes.
    fn detach(&mut self, node: NodeId, effects: &mut Vec<Effect>) -> Vec<MemberId> {
        self.remove_node(node, effects)
            .and_then(|removed| removed.binding)
            .map(|binding| binding.volumes.into_values().collect())
            .unwrap_or_default()
    }

    // A node's subtree goes with it, the objects that refused their volumes
    // in its namespace among it.
    fn drop_subtree(&mut self, node: NodeId, effects: &mut Vec<Effect>) {
        let mut below = self.bound_below(node);
        below.extend(self.refused_below(node));
        for child in below {
            self.drop_subtree(child, effects);
            self.remove_node(child, effects);
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
                pulled = pulled.fold(ask.unseekable());
                continue;
            };
            // A window's set that covers the member it sits in rules on that
            // member only once all of it is in, its own recovery bytes with
            // it: deferring them would hold back the ruling that asks for
            // them, so the deferral parks nothing.
            if ask.when == When::Deferred
                && self.window(at).is_some()
                && self
                    .recovery(at)
                    .is_some_and(|recovery| recovery.covers(member))
            {
                continue;
            }
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
            match object.extent(member, ask.range, &mut extents) {
                Located::Whole => {}
                // The parent has not read the header that places the rest
                // of the bytes; the ask is lifted again next round, when it
                // may have.
                Located::NotYet => pulled = pulled.fold(ask.unlocated()),
                // Nothing above can bring the bytes forward; the ask waits
                // for the normal order to reach them.
                Located::Never => pulled = pulled.fold(ask.unseekable()),
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
        self.descend(vec![(node, volume, range)], Provenance::Damaged, effects);
    }

    // The member's bytes, every one of them in, fail the checksum its
    // object's headers state for them. Which bytes are wrong is not known,
    // so the whole member is damaged, and the damage descends as any other.
    pub fn checksum_failed(&mut self, node: NodeId, member: MemberId, effects: &mut Vec<Effect>) {
        let Some(found) = self.nodes.members_mut(node) else {
            return;
        };
        let member = found.namespace.resolve(member);
        let Some(record) = found.namespace.get_mut(member) else {
            return;
        };
        let Some(length) = record.length else {
            return;
        };
        let range = ByteRange::new(0, length);
        // This checks the assembled member itself, not one posting's
        // report of damage. A parent's repair proves its input bytes;
        // it cannot overrule the derived member's own failed checksum.
        record.coverage.write(range, Provenance::Refuted);
        effects.push(Effect::Damaged {
            node,
            member,
            range,
        });
        let pending = found
            .children
            .get(&member)
            .map(|&(child, volume)| (child, volume, range))
            .into_iter()
            .collect();
        self.descend(pending, Provenance::Refuted, effects);
    }

    // Capture the complete input dependencies of checksummed, damaged
    // outputs before a repair's partial deliveries erase that evidence.
    // This is a repair transition, not a payload-delivery query. Engine
    // objects restart through their run generation; stores replay through
    // the same inverse map used when they ask for bytes again.
    pub fn rejected_inputs(
        &self,
        parent: NodeId,
        rewritten: impl IntoIterator<Item = MemberId>,
    ) -> Vec<(NodeId, Vec<(VolumeIndex, ByteRange)>)> {
        let mut pending: Vec<_> = rewritten
            .into_iter()
            .filter_map(|member| self.child_of(parent, member).map(|(node, _)| node))
            .collect();
        let mut seen = BTreeSet::new();
        let mut rejected = Vec::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let Some(found) = self.nodes.get(&node).filter(|found| !found.retired) else {
                continue;
            };
            let Some(object) = found.object.as_ref() else {
                continue;
            };
            if object.engine().is_some() {
                continue;
            }
            pending.extend(self.bound_below(node));
            let mut extents = Vec::new();
            for member in found.namespace.members() {
                let Some(length) = member.length.filter(|_| member.crc32.is_some()) else {
                    continue;
                };
                if member
                    .coverage
                    .runs()
                    .any(|run| run.provenance.is_damaged())
                {
                    object.extent(member.id, ByteRange::new(0, length), &mut extents);
                }
            }
            extents.sort_by_key(|extent| (extent.volume, extent.range.start));
            let mut ranges: Vec<(VolumeIndex, ByteRange)> = Vec::new();
            for extent in extents {
                if let Some((volume, range)) = ranges.last_mut()
                    && *volume == extent.volume
                    && extent.range.start <= range.end
                {
                    range.end = range.end.max(extent.range.end);
                } else {
                    ranges.push((extent.volume, extent.range));
                }
            }
            if !ranges.is_empty() {
                rejected.push((node, ranges));
            }
        }
        rejected.sort_by_key(|(node, _)| *node);
        rejected
    }

    // The damage marked in the namespaces below `node` on members an object
    // further down reads, as ranges of the members of `node` their bytes
    // came from: each object's forward map carries a damaged range of its
    // member to where it lies in the volumes it is bound to, and on up. Such
    // a mark is the reading object's verdict on bytes it was handed, and a
    // set that proves those ranges here proves the bytes it was given on. A
    // mark on a member nothing reads further is its producer's verdict on
    // what it made, which proof of the bytes it was made from does not
    // answer: it stands until its producer judges the member again.
    pub fn damaged_below(&self, node: NodeId) -> Vec<(MemberId, ByteRange)> {
        let mut damaged = Vec::new();
        for child in self.bound_below(node) {
            let Some(found) = self.nodes.get(&child).filter(|found| !found.retired) else {
                continue;
            };
            let (Some(object), Some(binding)) = (found.object.as_ref(), found.binding.as_ref())
            else {
                continue;
            };
            let mut extents = Vec::new();
            let read_below = |member: &MemberId| {
                let member = found.namespace.resolve(*member);
                found.children.contains_key(&member) || found.windows.contains_key(&member)
            };
            for (member, range) in found
                .namespace
                .damaged_ranges()
                .filter(|(member, _)| read_below(member))
                .chain(self.damaged_below(child))
            {
                object.extent(member, range, &mut extents);
            }
            for extent in extents {
                if let (Some(member), Some(range)) = (
                    binding.volumes.get(&extent.volume),
                    self.in_member(child, extent.range),
                ) {
                    damaged.push((*member, range));
                }
            }
        }
        damaged
    }

    // An object found its own volume bad: a header checksum failed. The
    // member that volume is in the parent's namespace is marked refuted
    // first, so the recovery beside it repairs those bytes and nothing but
    // proof lifts the verdict, and it then descends as any other. A
    // checksum names the whole range it covers, not the bytes that were
    // bad: damage already marked in that range accounts for the failure,
    // at the bytes it reached, and marking the rest would only mark bytes
    // that arrived whole. A volume no member is has nothing in hand to
    // judge: it is lost, as any range that never arrives.
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
                let verdict = (range, Provenance::Refuted);
                self.mark(parent, member, verdict, effects, &mut pending);
                self.descend(pending, Provenance::Refuted, effects);
            }
            None => self.damage(node, volume, range, effects),
        }
    }

    // An object's map puts bytes it kept in its envelope elsewhere now: the
    // owner hears which member of the parent's namespace they are of.
    fn unkept(
        &self,
        child: NodeId,
        (volume, range): (VolumeIndex, ByteRange),
        (envelope, at): (u32, u64),
        effects: &mut Vec<Effect>,
    ) {
        let Some((node, member)) = self
            .nodes
            .get(&child)
            .and_then(|found| found.binding.as_ref())
            .and_then(|binding| Some((binding.parent, *binding.volumes.get(&volume)?)))
        else {
            return;
        };
        effects.push(Effect::Unkept {
            node,
            member,
            child,
            volume,
            range,
            envelope,
            at,
        });
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
                    .any(|run| run.provenance.is_damaged())
            })
    }

    // Damage descends with the provenance it was marked with: a loss as
    // `Damaged`, a checksum's verdict as `Refuted`.
    fn descend(
        &mut self,
        pending: Vec<(NodeId, VolumeIndex, ByteRange)>,
        provenance: Provenance,
        effects: &mut Vec<Effect>,
    ) {
        self.walk_down(pending, |tree, node, member, range, pending| {
            tree.mark(node, member, (range, provenance), effects, pending);
        });
    }

    // A range of `node`'s volume a restart could not read back, which comes
    // again: at every depth it reaches it is not covered, its members are
    // open, and each object bound below forgets what it said of it, so the
    // bytes are said again as they first were.
    pub fn uncover(&mut self, node: NodeId, volume: VolumeIndex, range: ByteRange) {
        self.forget_input(node, volume, range, false);
    }

    // Input staged through the existing map is being checked again, not
    // lost. A repair below that map still answers old upstream damage.
    pub fn recheck(&mut self, node: NodeId, volume: VolumeIndex, range: ByteRange) {
        self.forget_input(node, volume, range, true);
    }

    fn forget_input(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        keep_repairs: bool,
    ) {
        if let Some(object) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.object.as_mut())
        {
            object.unposted(volume, range);
        }
        self.walk_down(
            vec![(node, volume, range)],
            |tree, node, member, range, pending| {
                if !keep_repairs {
                    tree.unsay(node, member, range, pending);
                    return;
                }
                let mut forgotten = crate::member::Coverage::new();
                forgotten.write(range, Provenance::Fresh);
                if let Some(record) = tree.namespace(node).and_then(|ns| ns.get(member)) {
                    for run in record.coverage.overlapping(range) {
                        if run.provenance == Provenance::Repaired {
                            forgotten.erase(run.range);
                        }
                    }
                }
                for run in forgotten.runs() {
                    tree.unsay(node, member, run.range, pending);
                }
            },
        );
    }

    // An object asked for ranges of its volumes again. What of each was
    // posted is read back as the object rebuilds it, every range while the
    // object still stands as it said them all, then uncovered at every depth
    // as a restart's lost range is, and delivered to the object again, each
    // run with the provenance the parent's member keeps for it. A run that
    // was never posted, or does not read back, is not said again: it comes
    // when it arrives. The owner calls it for `Effect::Again`, after
    // applying the effects before it.
    pub fn again(
        &mut self,
        node: NodeId,
        ranges: &[(VolumeIndex, ByteRange)],
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        for (volume, posted) in self.again_read(node, ranges, builder) {
            self.replay_piece(node, volume, posted, builder, effects);
        }
    }

    // What `again` delivers, read back and uncovered, each run to go to the
    // object by `replay_piece` as any delivery does: what it places nowhere
    // yet, a file it cannot key, is the owner's to hold. The owner applies
    // each run's effects before the next run goes, so a run the object
    // could not place when it came never looks placed by a later one.
    pub fn again_read(
        &mut self,
        node: NodeId,
        ranges: &[(VolumeIndex, ByteRange)],
        builder: &dyn Builder,
    ) -> Vec<(VolumeIndex, Posted)> {
        let mut again = Vec::new();
        for &(volume, range) in ranges {
            let Some((parent, member)) = self
                .nodes
                .get(&node)
                .and_then(|found| found.binding.as_ref())
                .and_then(|binding| Some((binding.parent, *binding.volumes.get(&volume)?)))
            else {
                continue;
            };
            let Some(within) = self.in_member(node, range) else {
                continue;
            };
            let shift = within.start as i64 - range.start as i64;
            let record = self
                .namespace(parent)
                .and_then(|namespace| namespace.get(member));
            let runs: Vec<(ByteRange, Provenance)> = record
                .map(|record| {
                    record
                        .coverage
                        .overlapping(within)
                        .filter_map(|run| {
                            let piece = run.range.intersect(within)?;
                            (!piece.is_empty()).then(|| (piece.shift(-shift), run.provenance))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let Some(object) = self
                .nodes
                .get(&node)
                .and_then(|found| found.object.as_ref())
            else {
                return Vec::new();
            };
            let (tree, reader) = (&*self, builder);
            let mut read = |placement: &Placement, into: &mut Vec<u8>| {
                reader.read(tree, node, volume, placement, into)
            };
            for (piece, provenance) in runs {
                let mut bytes = Vec::new();
                if object.posted(volume, piece, &mut read, &mut bytes) {
                    again.push((
                        volume,
                        Posted {
                            offset: piece.start,
                            bytes: Evidenced::from(bytes),
                            provenance,
                        },
                    ));
                }
            }
        }
        for (volume, posted) in &again {
            let piece = ByteRange::at(posted.offset, posted.bytes.len() as u64);
            self.uncover(node, *volume, piece);
        }
        again
    }

    // One piece whose posted bytes the owner staged before uncovering the
    // replay's ranges. The owner drains its effects before the next piece,
    // so neither replay input nor derived output accumulates in memory.
    pub fn replay_piece(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        posted: Posted,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some((parent, member)) = self
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return;
        };
        let length = self
            .namespace(parent)
            .and_then(|namespace| namespace.get(member))
            .and_then(|member| member.length);
        effects.push(Effect::Reached {
            node: parent,
            member,
            child: node,
            volume,
            offset: posted.offset,
            bytes: posted.bytes.clone(),
            provenance: posted.provenance,
        });
        let arrived = Arrived {
            volume,
            offset: posted.offset,
            bytes: posted.bytes,
            provenance: posted.provenance,
        };
        self.deliver_to_object(node, &arrived, length, builder, effects);
    }

    // Each member the ranges of the nodes' volumes landed in, by the node's
    // object, handed to `each`, which pushes what goes on down.
    fn walk_down(
        &mut self,
        mut pending: Vec<(NodeId, VolumeIndex, ByteRange)>,
        mut each: impl FnMut(
            &mut Self,
            NodeId,
            MemberId,
            ByteRange,
            &mut Vec<(NodeId, VolumeIndex, ByteRange)>,
        ),
    ) {
        while let Some((at, volume, range)) = pending.pop() {
            let Some(found) = self.nodes.get(&at) else {
                continue;
            };
            let Some(object) = found.object.as_ref() else {
                // The root: the volume is the member.
                each(self, at, MemberId(volume.0), range, &mut pending);
                continue;
            };
            let mut placements = std::mem::take(&mut self.placements);
            placements.clear();
            object.locate(volume, range, &mut placements);
            for placement in placements.drain(..) {
                if let Placement::Member { member, range } = placement {
                    each(self, at, member, range, &mut pending);
                }
            }
            self.placements = placements;
        }
    }

    fn unsay(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        pending: &mut Vec<(NodeId, VolumeIndex, ByteRange)>,
    ) {
        if range.is_empty() {
            return;
        }
        let Some(found) = self.nodes.members_mut(node) else {
            return;
        };
        let member = found.namespace.uncover(member, range);
        let Some((child, volume)) = found.children.get(&member).copied() else {
            return;
        };
        if let Some(object) = self
            .nodes
            .get_mut(&child)
            .and_then(|found| found.object.as_mut())
        {
            object.unposted(volume, range);
        }
        pending.push((child, volume, range));
    }

    fn mark(
        &mut self,
        node: NodeId,
        member: MemberId,
        (range, provenance): (ByteRange, Provenance),
        effects: &mut Vec<Effect>,
        pending: &mut Vec<(NodeId, VolumeIndex, ByteRange)>,
    ) {
        let Some(found) = self.nodes.members_mut(node) else {
            return;
        };
        // A posting's loss is the member's only where no other posting
        // brought those bytes clean.
        let (member, lost) = found.namespace.arrive(member, range, provenance);
        for (range, _) in lost {
            effects.push(Effect::Damaged {
                node,
                member,
                range,
            });
            if let Some((child, volume)) = found.children.get(&member).copied() {
                pending.push((child, volume, range));
            }
        }
    }

    // Damage reached a member range, as `Effect::Damaged` said. A window on
    // it reads that range as its own volume, so what of it falls there is
    // lost to the window's object: nothing recovers the recovery data.
    // Damage at its first byte lost its front, as `front_changed` hears.
    pub fn damage_reached(
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
            self.volume_lost(window, VolumeIndex(0), own, builder, effects);
        }
        if range.start == 0 {
            self.front_changed(node, member, true, builder, effects);
        }
    }

    // Every standing seek of every object, lifted, an engine's bids for
    // what its run reads next among them. Returns each object's seeks with
    // their answers so the caller can feed `pulled` back.
    pub fn collect_wants(&self, out: &mut Vec<(NodeId, Seek)>) {
        let mut seeks = Vec::new();
        for (id, node) in self.nodes.iter().filter(|(_, node)| !node.retired) {
            if let Some(object) = node.object.as_ref() {
                seeks.clear();
                object.wants(&mut seeks);
                if let (Some(engine), Some(reached)) =
                    (object.engine(), self.nodes.progress(*id).reached)
                {
                    engine.bid(reached, &mut seeks);
                }
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
        self.reach(node, Told::Pulled { seek, pull }, builder, effects);
    }
}

// The one place an object's answer names, where a step moves a member
// without weighing it against the places held: several name none.
fn only(places: impl AsRef<[VolumeIndex]>) -> Option<VolumeIndex> {
    match places.as_ref()[..] {
        [place] => Some(place),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deliver::{Delivery, RecoveryKind};

    mod again;
    mod candidates;
    mod handed;
    mod lift;
    mod numbered;
    mod refuted;
    mod reopen;
    mod reposted;
    mod sets;
    mod takes;
    mod windows;

    const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
    const HEADER: u64 = 7;
    const VOLUME_LEN: u64 = 100;

    // The root's `copy` folded into `into`, whatever it brought before.
    fn folded(effects: &[Effect], copy: MemberId, into: MemberId) -> bool {
        effects.iter().any(|effect| {
            matches!(effect, Effect::Folded { node, copy: was, into: is, .. }
                if *node == NodeId::ROOT && *was == copy && *is == into)
        })
    }

    // A pretend store: a seven-byte header on volume 0, then one member
    // whose bytes run across volumes of a fixed length. It claims `.r00`
    // as its second volume.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Split {
        announced: bool,
    }

    impl crate::Durable for Split {
        const VERSION: u16 = 1;
    }

    impl Object for Split {
        // It keeps no record of what it said of its posted bytes.
        fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

        fn kind(&self) -> Kind {
            Kind::RarStore
        }
        fn facts(&self) -> crate::Facts {
            crate::Facts::of(Kind::RarStore, self)
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
        fn wants(&self, out: &mut Vec<Seek>) {
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
        fn extent(&self, _: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
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
            Located::Whole
        }
        fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
            None
        }
        fn claim(&mut self, name: &str, _: &[u8], _length: Option<u64>) -> Claim {
            match name.rsplit_once('.') {
                Some((_, "r00" | "002")) => vec![VolumeIndex(1)],
                Some((_, "r01" | "003")) => vec![VolumeIndex(2)],
                _ => Vec::new(),
            }
            .into()
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
        // It keeps no record of what it said of its posted bytes.
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
        fn bound(&mut self, volume: VolumeIndex, name: &str, _front: &[u8], _length: Option<u64>) {
            self.0.push((volume, name.to_owned()));
        }
    }

    // A pretend set of pieces, founded by name alone: any `.NNN` piece is
    // its volume, counted from `.001`.
    struct Named(Vec<VolumeIndex>);

    impl Object for Named {
        // It keeps no record of what it said of its posted bytes.
        fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

        fn kind(&self) -> Kind {
            Kind::Split
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
        fn bound(&mut self, volume: VolumeIndex, _: &str, _: &[u8], _: Option<u64>) {
            self.0.push(volume);
        }
        fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Claim {
            name.rsplit_once('.')
                .and_then(|(_, digits)| digits.parse::<u32>().ok())
                .and_then(|number| number.checked_sub(1))
                .map(VolumeIndex)
                .into_iter()
                .collect()
        }
        fn taken(&mut self, volume: VolumeIndex) {
            self.0.retain(|held| *held != volume);
        }
    }

    struct Build;
    impl Builder for Build {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            match kind {
                Kind::RarStore => Some(Box::new(Split { announced: false })),
                Kind::RarEngine => Some(Box::new(Reader(Vec::new()))),
                Kind::Split => Some(Box::new(Named(Vec::new()))),
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
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
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
            bytes: Bytes::from_static(b"hello").into(),
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
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"hello").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut effects,
        );
        assert_eq!(
            effects,
            vec![Effect::WriteLeaf {
                node: NodeId::ROOT,
                member,
                offset: 0,
                bytes: Bytes::from_static(b"hello").into(),
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

    // A volume announced beside a bound object is offered to it at once, by
    // its name, before any of its bytes arrive.
    #[test]
    fn an_announced_volume_is_offered_to_the_object_before_its_bytes() {
        let (mut tree, _, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((child, VolumeIndex(1)))
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"world").into(),
                provenance: Provenance::Fresh,
            },
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
            bytes: Bytes::from_static(b"world").into(),
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"world").into(),
                provenance: Provenance::Fresh,
            },
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
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
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

    // A piece that comes before its first founds a set by its name alone;
    // the first, signed, founds the store, which takes the piece from the
    // set where its own claim places it, and the set, left empty, goes.
    #[test]
    fn a_piece_founded_by_name_goes_to_the_store_its_first_founds() {
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let piece = tree.announce_root("show.002", Some(5), &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            piece,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"world").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut effects,
        );
        let (named, place) = tree.child_of(NodeId::ROOT, piece).expect("founded by name");
        assert_eq!(place, VolumeIndex(1));
        assert!(effects.contains(&Effect::Bound {
            node: NodeId::ROOT,
            member: piece,
            kind: Kind::Split,
            child: named
        }));
        let first = tree.announce_root("show.rar", None, &mut Vec::new());
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        effects.clear();
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut effects,
        );
        let store = tree.child_of(NodeId::ROOT, first).expect("bound").0;
        assert_eq!(
            tree.child_of(NodeId::ROOT, piece),
            Some((store, VolumeIndex(1)))
        );
        assert!(effects.contains(&Effect::GivenBack {
            node: NodeId::ROOT,
            member: piece,
            from: named,
            volume: VolumeIndex(1),
        }));
        assert!(effects.contains(&Effect::Bound {
            node: NodeId::ROOT,
            member: piece,
            kind: Kind::RarStore,
            child: store
        }));
        assert!(tree.namespace(named).is_none(), "an empty set goes");
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
                Posted {
                    offset: range.start as u64,
                    bytes: Bytes::copy_from_slice(&body[range]).into(),
                    provenance,
                },
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"w").into(),
                provenance: Provenance::Fresh,
            },
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

    // A deferral walks up as any seek does and reaches the root's volume
    // still deferred; where nothing can be translated it parks nothing and
    // is no problem for the asker.
    #[test]
    fn a_deferral_is_lifted_unchanged_and_is_never_unseekable() {
        let (mut tree, _, child, _) = bound_tree();
        let mut root = Root(Vec::new());
        let early = Seek::deferred(VolumeIndex(1), ByteRange::new(0, 10));
        assert_eq!(tree.lift(child, early, &mut root), Pull::Moved(0));
        assert!(root.0.is_empty());
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            second,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"w").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        let mut root = Root(Vec::new());
        assert_eq!(tree.lift(child, early, &mut root), Pull::Moved(1));
        assert_eq!(
            root.0,
            vec![Seek {
                volume: VolumeIndex(second.0),
                ..early
            }]
        );
        assert_eq!(root.0[0].when, crate::seek::When::Deferred);
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
    fn rejected_inputs_follow_bindings_and_deduplicate_rewritten_volumes() {
        let (mut tree, first, child, _) = bound_tree();
        let unrelated = tree.announce_root("unrelated.bin", Some(6), &mut Vec::new());
        let record = tree
            .nodes
            .get_mut(&child)
            .unwrap()
            .namespace
            .get_mut(MemberId(0))
            .unwrap();
        record.length = Some(6);
        record.crc32 = Some(0);
        record
            .coverage
            .write(ByteRange::new(0, 6), Provenance::Damaged);
        assert!(tree.rejected_inputs(NodeId::ROOT, [unrelated]).is_empty());
        assert_eq!(
            tree.rejected_inputs(NodeId::ROOT, [first, first]),
            vec![(
                child,
                vec![(VolumeIndex(0), ByteRange::new(HEADER, HEADER + 6))]
            )]
        );
        tree.nodes
            .get_mut(&child)
            .unwrap()
            .namespace
            .get_mut(MemberId(0))
            .unwrap()
            .coverage
            .write(ByteRange::new(0, 6), Provenance::Repaired);
        assert!(tree.rejected_inputs(NodeId::ROOT, [first]).is_empty());
    }

    #[test]
    fn replay_keeps_repairs_but_missing_input_does_not() {
        let (mut tree, _, child, _) = bound_tree();
        let record = tree
            .nodes
            .get_mut(&child)
            .unwrap()
            .namespace
            .get_mut(MemberId(0))
            .unwrap();
        record
            .coverage
            .write(ByteRange::new(0, 6), Provenance::Fresh);
        let repaired = ByteRange::new(2, 4);
        record.coverage.write(repaired, Provenance::Repaired);
        let input = ByteRange::new(HEADER, HEADER + 6);
        tree.recheck(child, VolumeIndex(0), input);
        let coverage = &tree
            .namespace(child)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .coverage;
        assert_eq!(coverage.bytes_covered(), repaired.len());
        assert!(coverage.verified(repaired));
        let mut effects = Vec::new();
        tree.replay_piece(
            child,
            VolumeIndex(0),
            Posted {
                offset: HEADER,
                bytes: Bytes::from_static(b"abcdef").into(),
                provenance: Provenance::Damaged,
            },
            &mut Build,
            &mut effects,
        );
        let coverage = &tree
            .namespace(child)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .coverage;
        assert!(
            coverage.verified(repaired),
            "old damage cannot replace the repair"
        );
        assert!(coverage.damaged(ByteRange::new(0, 2)));
        assert!(coverage.damaged(ByteRange::new(4, 6)));
        tree.uncover(child, VolumeIndex(0), input);
        assert_eq!(
            tree.namespace(child)
                .unwrap()
                .get(MemberId(0))
                .unwrap()
                .coverage
                .bytes_covered(),
            0
        );
    }

    #[test]
    fn a_members_own_checksum_can_reject_repaired_bytes() {
        let (mut tree, first, child, _) = bound_tree();
        let range = ByteRange::new(0, 6);
        let record = tree
            .nodes
            .get_mut(&child)
            .unwrap()
            .namespace
            .get_mut(MemberId(0))
            .unwrap();
        record.length = Some(range.end);
        record.coverage.write(range, Provenance::Repaired);
        let mut effects = Vec::new();
        // An upstream damage report cannot take back a repair.
        tree.damage(child, VolumeIndex(0), ByteRange::new(7, 13), &mut effects);
        assert!(
            tree.namespace(child)
                .unwrap()
                .get(MemberId(0))
                .unwrap()
                .coverage
                .verified(range)
        );
        effects.clear();
        tree.checksum_failed(child, MemberId(0), &mut effects);
        assert_eq!(
            effects,
            vec![Effect::Damaged {
                node: child,
                member: MemberId(0),
                range
            }]
        );
        assert!(
            tree.namespace(child)
                .unwrap()
                .get(MemberId(0))
                .unwrap()
                .coverage
                .damaged(range)
        );
        assert!(
            !tree
                .namespace(NodeId::ROOT)
                .unwrap()
                .get(first)
                .unwrap()
                .coverage
                .damaged(ByteRange::new(7, 13))
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"w").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 20,
                bytes: Bytes::from_static(b"REFUSE").into(),
                provenance: Provenance::Fresh,
            },
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
                    bytes: Bytes::from_static(b"REFUSE").into(),
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
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
        assert!(tree.namespace(child).is_some(), "one volume still unread");
        tree.bury(NodeId::ROOT, second, &mut Vec::new());
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
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"w").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 20,
                bytes: Bytes::from_static(b"REFUSE").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
        tree.bury(NodeId::ROOT, second, &mut Vec::new());
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
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
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
            Posted {
                offset: 20,
                bytes: Bytes::from_static(b"REFUSE").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
        let later = tree.announce_root("show.r01", None, &mut Vec::new());
        let mut bytes = SIGNATURE.to_vec();
        bytes.extend_from_slice(b"hello");
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            later,
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
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
                    bytes: Bytes::from_static(b"ab").into(),
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
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
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

    // A member moved off a name leaves the ledger before its file has left
    // the name: the member given another name while it was left takes it
    // now, as its move, so it is written under the name it wanted once the
    // file is gone, never under the suffixed one for good.
    #[test]
    fn a_name_a_released_member_was_leaving_goes_to_the_member_that_wanted_it() {
        let (mut tree, first, child, _) = bound_tree();
        tree.emitted(
            child,
            vec![
                Emit::Retract(MemberId(0)),
                Emit::Member {
                    member: MemberId(0),
                    name: "other.bin".into(),
                    length: Some(5),
                },
            ],
            &mut Build,
            &mut Vec::new(),
        );
        let wanting = tree.announce_root("inner.bin", None, &mut Vec::new());
        assert_eq!(output_name(&tree, NodeId::ROOT, wanting), "inner (1).bin");
        tree.emitted(
            child,
            vec![Emit::Refuse(Why::Compressed)],
            &mut Build,
            &mut Vec::new(),
        );
        let mut effects = Vec::new();
        tree.bury(NodeId::ROOT, first, &mut effects);
        assert!(
            effects.contains(&Effect::Renamed {
                node: NodeId::ROOT,
                member: wanting,
                old: "inner (1).bin".into(),
                new: "inner.bin".into(),
            }),
            "{effects:?}"
        );
        assert_eq!(output_name(&tree, NodeId::ROOT, wanting), "inner.bin");
    }

    // Volumes named out of order are each held at announce, by name, before
    // any of their bytes: the one named ahead leaves no gap, and a seek into
    // it goes up to the source for that member's bytes.
    #[test]
    fn a_volume_named_ahead_is_held_before_its_bytes_come() {
        let (mut tree, first, child, _) = bound_tree();
        let third = tree.announce_root("show.r01", None, &mut Vec::new());
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
        let (_, volumes) = tree.volumes(child).unwrap();
        let volumes: Vec<_> = volumes.iter().map(|(v, m)| (v.0, *m)).collect();
        assert_eq!(volumes, vec![(0, first), (1, second), (2, third)]);
        let seek = Seek::data(
            VolumeIndex(1),
            ByteRange::new(0, 10),
            crate::seek::Weight(1),
        );
        let mut root = Root(Vec::new());
        assert_eq!(tree.lift(child, seek, &mut root), Pull::Moved(1));
        assert_eq!(
            root.0.iter().map(|seek| seek.volume).collect::<Vec<_>>(),
            vec![VolumeIndex(second.0)]
        );
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

    // A member its producer retracted and announces again under another
    // name, read from the bytes it starts over on, takes that name.
    #[test]
    fn a_member_announced_again_under_another_name_is_renamed() {
        let (mut tree, _, child, _) = bound_tree();
        tree.emitted(
            child,
            vec![Emit::Retract(MemberId(0))],
            &mut Build,
            &mut Vec::new(),
        );
        let mut effects = Vec::new();
        tree.emitted(
            child,
            vec![Emit::Member {
                member: MemberId(0),
                name: "other.bin".into(),
                length: Some(5),
            }],
            &mut Build,
            &mut effects,
        );
        let inner = tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
        assert_eq!(
            (inner.name.as_str(), inner.output_name.as_str()),
            ("other.bin", "other.bin")
        );
        assert!(
            effects.contains(&Effect::Renamed {
                node: child,
                member: MemberId(0),
                old: "inner.bin".into(),
                new: "other.bin".into(),
            }),
            "{effects:?}"
        );
    }

    fn two_volumes() -> (Tree, MemberId, MemberId, NodeId) {
        let (mut tree, first, child, _) = bound_tree();
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            second,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"w").into(),
                provenance: Provenance::Fresh,
            },
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
            Posted {
                offset: 20,
                bytes: Bytes::from_static(b"ENGINE").into(),
                provenance: Provenance::Fresh,
            },
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
                    bytes: Bytes::from_static(b"ENGINE").into(),
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
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
        tree.bury(NodeId::ROOT, second, &mut Vec::new());
        assert!(tree.namespace(child).is_none());
        assert!(tree.namespace(engine).is_some());
    }

    #[test]
    fn an_engine_that_refuses_is_not_rebound() {
        let (mut tree, first, second, _) = two_volumes();
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 20,
                bytes: Bytes::from_static(b"ENGINE").into(),
                provenance: Provenance::Fresh,
            },
            &mut Build,
            &mut Vec::new(),
        );
        let (engine, _) = tree.child_of(NodeId::ROOT, first).unwrap();
        tree.bury(NodeId::ROOT, first, &mut Vec::new());
        tree.bury(NodeId::ROOT, second, &mut Vec::new());
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
        let renamed = tree.rename(NodeId::ROOT, top, "subs.srt", &mut Build, &mut effects);
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

    // A pretend store whose first volume is `show.rar`, which holds
    // `linked.bin` where its links put it, provisionally: the first free
    // place after the first, and `stated.r00` at the place it states, 1.
    // A full one takes nothing back from what it parsed of a volume it gave
    // up, so the volume waits, given back, to be offered again.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Yielder {
        held: BTreeMap<VolumeIndex, String>,
        full: bool,
    }

    impl crate::facts::Durable for Yielder {
        const VERSION: u16 = 1;
    }

    impl Yielder {
        fn free(&self) -> Option<VolumeIndex> {
            (1..)
                .map(VolumeIndex)
                .find(|volume| !self.held.contains_key(volume))
        }
    }

    impl Object for Yielder {
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
        fn claim(&mut self, name: &str, _: &[u8], _length: Option<u64>) -> Claim {
            match name {
                "show.rar" => Some(VolumeIndex(0)),
                "stated.r00" => Some(VolumeIndex(1)),
                "linked.bin" => self.free(),
                _ => None,
            }
            .into_iter()
            .collect()
        }
        fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _length: Option<u64>) {
            self.held.insert(volume, name.to_owned());
        }
        fn provisional(
            &self,
            volume: VolumeIndex,
            _: &str,
            front: &[u8],
            _: Option<&Carried>,
        ) -> bool {
            front == b"stat"
                && self
                    .held
                    .get(&volume)
                    .is_some_and(|name| name == "linked.bin")
        }
        fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
            self.held
                .remove(&volume)
                .map(|name| Box::new(name) as Carried)
        }
        fn reclaim(
            &self,
            _: &str,
            _: &[u8],
            _: Option<u64>,
            carried: &(dyn std::any::Any + Send + Sync),
        ) -> Claim {
            (!self.full
                && carried
                    .downcast_ref::<String>()
                    .is_some_and(|name| name == "linked.bin"))
            .then(|| self.free())
            .flatten()
            .into_iter()
            .collect()
        }
        fn facts(&self) -> crate::facts::Facts {
            crate::facts::Facts::of(Kind::RarStore, self)
        }
    }

    struct YielderBuild;
    impl Builder for YielderBuild {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            (kind == Kind::RarStore).then(|| Box::new(Yielder::default()) as Box<dyn Object>)
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

    // A member whose claim names a place its object holds only
    // provisionally takes the place; the holder is given back, its parse
    // with it, and joins the object again at the place it then links to.
    #[test]
    fn a_claim_naming_a_provisionally_held_place_takes_it_and_the_holder_moves() {
        let mut build = YielderBuild;
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let first = tree.announce_root("show.rar", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(SIGNATURE).into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        let store = tree.child_of(NodeId::ROOT, first).expect("bound").0;
        let linked = tree.announce_root("linked.bin", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            linked,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"held").into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, linked),
            Some((store, VolumeIndex(1)))
        );
        effects.clear();
        let stated = tree.announce_root("stated.r00", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            stated,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"stat").into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, stated),
            Some((store, VolumeIndex(1)))
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, linked),
            Some((store, VolumeIndex(2)))
        );
        assert!(effects.contains(&Effect::GivenBack {
            node: NodeId::ROOT,
            member: linked,
            from: store,
            volume: VolumeIndex(1),
        }));
        assert!(!tree.given_back(NodeId::ROOT, linked));
    }

    // A store that takes back nothing it gave up, built as found or as its
    // facts restore it.
    struct FullBuild;
    impl Builder for FullBuild {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            (kind == Kind::RarStore).then(|| {
                Box::new(Yielder {
                    full: true,
                    ..Yielder::default()
                }) as Box<dyn Object>
            })
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

    impl crate::facts::Restore for FullBuild {
        fn restore(
            &self,
            facts: &crate::facts::Facts,
        ) -> Result<Box<dyn Object>, crate::facts::RestoreError> {
            Ok(Box::new(facts.read::<Yielder>(Kind::RarStore)?))
        }
    }

    // A member given back with its parse waits to be offered again, and
    // the parse is not in the records: a tree rebuilt from them holds it
    // undecided, so its front, coming again, offers it again and it joins
    // the store as it would have live.
    #[test]
    fn a_member_given_back_comes_back_undecided_and_is_offered_again() {
        let mut build = FullBuild;
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let deliver = |tree: &mut Tree, member, bytes: &'static [u8], build: &mut FullBuild| {
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
        };
        let first = tree.announce_root("show.rar", None, &mut effects);
        deliver(&mut tree, first, SIGNATURE, &mut build);
        let store = tree.child_of(NodeId::ROOT, first).expect("bound").0;
        let linked = tree.announce_root("linked.bin", None, &mut effects);
        deliver(&mut tree, linked, b"held", &mut build);
        let stated = tree.announce_root("stated.r00", None, &mut effects);
        deliver(&mut tree, stated, b"stat", &mut build);
        assert_eq!(tree.child_of(NodeId::ROOT, linked), None);
        assert!(tree.given_back(NodeId::ROOT, linked));

        let record = tree.records();
        let parked = record.nodes[0]
            .members
            .iter()
            .find(|member| member.member == linked)
            .expect("recorded");
        assert!(parked.undecided);

        let mut rebuilt = Tree::rebuild(&record, &FullBuild).expect("rebuilds");
        deliver(&mut rebuilt, linked, b"held", &mut build);
        assert_eq!(
            rebuilt.child_of(NodeId::ROOT, linked),
            Some((store, VolumeIndex(2)))
        );
    }

    // A pretend store that holds every byte as envelope and, on `GIVE`,
    // gives back every volume it holds in one say. Asked to take one back,
    // it would, at the volume it held it at: what it is never asked is
    // what the tree must not offer it.
    struct Giver {
        held: Vec<VolumeIndex>,
        asked: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Object for Giver {
        // It keeps no record of what it said of its posted bytes.
        fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

        fn kind(&self) -> Kind {
            Kind::RarStore
        }
        fn deliver(
            &mut self,
            delivery: Delivery<'_>,
            _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
            out: &mut Vec<Emit>,
        ) {
            if delivery.bytes == b"GIVE" {
                out.extend(self.held.iter().map(|volume| Emit::GiveBack(*volume)));
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
        fn claim(&mut self, name: &str, _: &[u8], _length: Option<u64>) -> Claim {
            name.ends_with(".r00")
                .then_some(VolumeIndex(1))
                .into_iter()
                .collect()
        }
        fn bound(&mut self, volume: VolumeIndex, _: &str, _: &[u8], _length: Option<u64>) {
            self.held.push(volume);
        }
        fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
            self.held.retain(|held| *held != volume);
            Some(Box::new(volume))
        }
        fn reclaim(
            &self,
            _: &str,
            _: &[u8],
            _: Option<u64>,
            carried: &(dyn std::any::Any + Send + Sync),
        ) -> Claim {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            carried
                .downcast_ref::<VolumeIndex>()
                .copied()
                .into_iter()
                .collect()
        }
    }

    struct GiverBuild(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Builder for GiverBuild {
        fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
            (kind == Kind::RarStore).then(|| {
                Box::new(Giver {
                    held: Vec::new(),
                    asked: std::sync::Arc::clone(&self.0),
                }) as Box<dyn Object>
            })
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

    // What one say gives back is taken back whole before any of it is
    // offered: the giver, left with no volume, is gone by then, so it is
    // asked to take none of them again. Each member then founds or joins a
    // fresh object where its parse places it, and the parse is spent.
    #[test]
    fn volumes_given_back_in_one_say_are_all_taken_before_any_is_offered() {
        let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut build = GiverBuild(std::sync::Arc::clone(&asked));
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let first = tree.announce_root("show.rar", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            first,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(SIGNATURE).into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        let giver = tree.child_of(NodeId::ROOT, first).expect("bound").0;
        let second = tree.announce_root("show.r00", None, &mut Vec::new());
        tree.deliver(
            NodeId::ROOT,
            second,
            Posted {
                offset: 0,
                bytes: Bytes::from_static(b"held").into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((giver, VolumeIndex(1)))
        );
        effects.clear();
        tree.deliver(
            NodeId::ROOT,
            second,
            Posted {
                offset: 4,
                bytes: Bytes::from_static(b"GIVE").into(),
                provenance: Provenance::Fresh,
            },
            &mut build,
            &mut effects,
        );
        for (member, volume) in [(first, VolumeIndex(0)), (second, VolumeIndex(1))] {
            assert!(effects.contains(&Effect::GivenBack {
                node: NodeId::ROOT,
                member,
                from: giver,
                volume,
            }));
            assert!(!tree.given_back(NodeId::ROOT, member), "the parse is spent");
        }
        assert!(!tree.live().contains(&giver), "the giver is gone");
        let (fresh, volume) = tree.child_of(NodeId::ROOT, first).expect("founded again");
        assert_ne!(fresh, giver);
        assert_eq!(volume, VolumeIndex(0));
        assert_eq!(
            tree.child_of(NodeId::ROOT, second),
            Some((fresh, VolumeIndex(1)))
        );
        assert_eq!(
            asked.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "each parse is reclaimed once, by the fresh object alone"
        );
    }
}
