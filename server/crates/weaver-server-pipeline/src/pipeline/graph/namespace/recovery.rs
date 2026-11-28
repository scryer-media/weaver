// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace's recovery sets. A set lives in the namespace of the members
// it describes, wherever it was found: posted beside them, posted for a file
// they join into, or carried inside an archive past its end record. The
// namespace owns everything about a set but its bytes: which of its members
// each set covers, the evidence of every byte that passes, the payload it
// holds while an index that will describe it is still to come, when each set
// rules, and the repair, run through `MemberAccess` and written back down the
// one write path. A set parses, verifies and rewrites bytes; when, and over
// what, is decided here, by the same rules at every depth.

use std::cmp::Reverse;
use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::collections::{BTreeSet, HashSet};
use weaver_server_disk::operations::disk::volume::LoopSite;

use bytes::Bytes;
use weaver_graph::recovery::{Recovery, RepairOutcome, SetRuling, ShortSets, Unbound};
use weaver_graph::{
    Algorithm, Builder, ByteRange, Effect, Handed, Kind, MemberId, Namespace, NodeId, Provenance,
    RecoveryKind, Restore, Segment, Tree, VolumeIndex,
};

use super::{
    Acknowledging, Apply, Applying, Arrival, HeldWant, LeafError, Loan, NamespaceWriter, READ_BACK,
    ReturnTail, Returned, Segments, TailStep, Tailed, Write, WriteError, absent_from_posts,
    in_own_file, index,
};

// How many times a job waiting for room asked again what its due sets
// would rebuild.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
thread_local! {
    pub static HELD_RECHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// Who a due set's repair is lent to, to run off the loop. One goes out at a
// time, under a repair slot; its return comes back to `returned`.
pub trait Lender {
    // Whether a repair may go out now.
    fn admit(&mut self) -> bool;
    fn lend(&mut self, loan: Loan);
}

// How many member names a bind kept to log what it newly covered, for the
// tests that count them.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
thread_local! {
    pub static NAMES_KEPT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// For the tests that count the binds a recovery volume's bytes make: past
// its front, of a volume not yet whole, how many articles there were, how
// many of them left their object a bind to do, and how many bound. Set,
// `VOLUME_BINDS_OPEN` binds after every such article, as each did before
// its object could say whether a bind was due.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
thread_local! {
    pub static VOLUME_ARTICLES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub static VOLUME_BINDS_DUE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub static VOLUME_BINDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub static VOLUME_BINDS_OPEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// What one set says of the members it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetState {
    // The tree node the set's object is. One PAR2 object holds every PAR2
    // set of its namespace, so this is per object, not per set id.
    pub(crate) node: NodeId,
    // The namespace it lives in, where the members it covers are.
    pub(crate) namespace: NodeId,
    pub kind: Kind,
    // What its last repair came to; `None` before it ran one.
    pub outcome: Option<RepairOutcome>,
}

// What the root still fetches unasked: whether a payload volume has an
// article to come, and the sets one of whose own volumes has. A set rules
// on what it has read, so one whose own volume is still arriving has its
// ruling still to come, whatever it said of the part that came first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coming {
    pub payload: bool,
    pub sets: BTreeSet<NodeId>,
}

// The set a root member is an own volume of, if it is one.
fn own_set(tree: &Tree, member: MemberId) -> Option<NodeId> {
    tree.child_of(NodeId::ROOT, member)
        .map(|(set, _)| set)
        .filter(|set| tree.recovers(*set))
}

impl NamespaceWriter {
    // What is still to come, from the root members with an article the
    // queue fetches unasked: a member that is a set's own volume names its
    // set, and any other is payload.
    pub fn coming(&self, tree: &Tree, members: impl Iterator<Item = MemberId>) -> Coming {
        let mut coming = Coming::default();
        let Some(root) = tree.namespace(NodeId::ROOT) else {
            return coming;
        };
        for member in members.filter(|member| root.get(*member).is_some()) {
            match own_set(tree, member) {
                Some(set) => {
                    coming.sets.insert(set);
                }
                None => coming.payload = true,
            }
        }
        coming
    }

    // Whether `coming` would say payload is still to come, stopping at the
    // first payload member.
    pub(crate) fn payload_coming(
        &self,
        tree: &Tree,
        mut members: impl Iterator<Item = MemberId>,
    ) -> bool {
        let Some(root) = tree.namespace(NodeId::ROOT) else {
            return false;
        };
        members.any(|member| root.get(member).is_some() && own_set(tree, member).is_none())
    }

    // Every set in the tree with the namespace it lives in, parents first.
    pub fn sets(&self, tree: &Tree) -> Vec<(NodeId, NodeId)> {
        tree.live()
            .into_iter()
            .flat_map(|namespace| {
                let sets = tree.recovery_nodes(namespace);
                let standing: Vec<NodeId> = sets
                    .iter()
                    .copied()
                    .filter(|set| !aside(tree, *set))
                    .collect();
                standing.into_iter().map(move |set| (namespace, set))
            })
            .collect()
    }

    // Of `sets`, a namespace's sets as the tree listed them, the ones that
    // cover the member. The namespace's sets are asked once and handed to
    // the work over each of its members.
    pub fn covering<'t>(
        &'t self,
        tree: &'t Tree,
        sets: &'t [NodeId],
        member: MemberId,
    ) -> impl Iterator<Item = NodeId> + 't {
        sets.iter()
            .copied()
            .filter(move |set| self.covers(tree, *set, member) && !aside(tree, *set))
    }

    // Whether the set covers the member: the one place a member is matched
    // to a set that describes it. A set out on loan answers as it left.
    pub(crate) fn covers(&self, tree: &Tree, set: NodeId, member: MemberId) -> bool {
        tree.recovery(set).map_or_else(
            || tree.lent_covers(set, member),
            |recovery| recovery.covers(member),
        )
    }

    // Whether the set covers any member at all. A set out on loan answers
    // as it left.
    pub(crate) fn covers_some(&self, tree: &Tree, set: NodeId) -> bool {
        tree.recovery(set).map_or_else(
            || tree.lent_covers_any(set),
            |recovery| recovery.covers_some(),
        )
    }

    // The member is a set's own volume: recovery data, in its own file.
    pub fn carries_recovery(&self, tree: &Tree, node: NodeId, member: MemberId) -> bool {
        tree.child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child))
    }

    // The member is a volume of a set that rebuilds what it covers: data
    // to repair with, in its own file. The set says whether it rebuilds; a
    // set that only checks carries none.
    pub(crate) fn carries_parity(&self, tree: &Tree, node: NodeId, member: MemberId) -> bool {
        tree.child_of(node, member)
            .is_some_and(|(child, _)| tree.rebuilds(child))
    }

    // Every set of the namespace has ruled on something other than waiting:
    // nothing a set of it will rebuild is still to come. A set whose repair
    // is out has its ruling still to come, and so has one whose own volume
    // still has an article coming. A set taken back with its return still
    // being applied, or with that return's tail still standing, has not
    // ruled either: what it rebuilds is not down yet, and a run told its
    // volumes are settled would read the bytes still to land as lost.
    pub fn ruled(&self, tree: &Tree, node: NodeId, coming: &Coming) -> bool {
        let sets = tree.recovery_nodes(node);
        sets.iter().copied().all(|set| {
            !self.returning(set)
                && (aside(tree, set)
                    || (!tree.lent(set)
                        && !coming.sets.contains(&set)
                        && tree.recovery(set).is_none_or(|recovery| {
                            recovery
                                .outcome()
                                .is_some_and(|outcome| outcome != RepairOutcome::Waiting)
                        })))
        })
    }

    // Whether the set's return is still being applied, or its tail stands.
    fn returning(&self, set: NodeId) -> bool {
        self.applying
            .as_ref()
            .is_some_and(|applying| applying.apply.set == set)
            || self.tail.as_ref().is_some_and(|tail| tail.set == set)
    }

    // Nothing more will come for the namespace's members, not even a
    // repair: the payload is in, and the namespace's sets have ruled. One
    // that has not may cover a member yet, since it binds what it covers as
    // its own volumes arrive, and those can come after the member closed,
    // or rebuild a member that never came at all.
    pub fn ended(&self, tree: &Tree, node: NodeId, coming: &Coming) -> bool {
        !coming.payload && self.ruled(tree, node, coming)
    }

    // Some member of a namespace holds damage a set of that namespace
    // covers: a repair of it has something to rebuild.
    pub(crate) fn damage_covered(&self, tree: &Tree) -> bool {
        tree.live().into_iter().any(|node| {
            let sets = tree.recovery_nodes(node);
            tree.namespace(node).is_some_and(|namespace| {
                namespace.members().any(|member| {
                    member
                        .coverage
                        .runs()
                        .any(|run| run.provenance.is_damaged())
                        && self.covering(tree, &sets, member.id).next().is_some()
                })
            })
        })
    }

    // The member is closed whole and the sets proved it: one covers it, and
    // every one that does ruled it clean or repaired it and is not out on
    // loan. No set reads the source's checksums of its bytes again. `sets`
    // are the namespace's sets as the tree lists them, asked once by the
    // caller for every member it asks about.
    pub(crate) fn proven_whole(
        &self,
        tree: &Tree,
        (node, sets): (NodeId, &[NodeId]),
        member: MemberId,
    ) -> bool {
        let whole = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.is_whole());
        if !whole {
            return false;
        }
        let mut covering = self.covering(tree, sets, member).peekable();
        covering.peek().is_some() && covering.all(|set| self.stands_proved(tree, set))
    }

    // The set is home and last ruled clean or repaired: it proves what it
    // covers.
    pub(crate) fn stands_proved(&self, tree: &Tree, set: NodeId) -> bool {
        !tree.lent(set)
            && tree
                .recovery(set)
                .and_then(|recovery| recovery.outcome())
                .is_some_and(|outcome| {
                    matches!(
                        outcome,
                        RepairOutcome::Clean | RepairOutcome::Repaired { .. }
                    )
                })
    }

    // Every set of every namespace has ruled on something other than
    // waiting.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn all_ruled(&self, tree: &Tree, coming: &Coming) -> bool {
        tree.live()
            .into_iter()
            .all(|node| self.ruled(tree, node, coming))
    }

    // Every set of every namespace has ruled, as `all_ruled` asks, or waits
    // only on the job's quiet: what the job waits for before its data
    // objects hear it. `reading` says which namespaces' sources may still
    // announce members.
    pub(crate) fn ruled_but_for_quiet(
        &self,
        tree: &Tree,
        coming: &Coming,
        reading: &dyn Fn(NodeId) -> bool,
    ) -> bool {
        tree.live().into_iter().all(|node| {
            self.ruled(tree, node, coming) || self.waits_on_quiet(tree, node, coming, reading(node))
        })
    }

    // Below the root, a set that has yet to rule may wait on nothing but
    // the quiet. Its namespace has every member it will get, nothing above
    // can bring back what those members lost, and every set above has
    // ruled; it is not due, as a member it covers is open short of bytes.
    // Its object holds the rest of that member until it hears the losses of
    // its volumes, which it hears only once the job is quiet, and the member
    // closes only then. Waiting for such a set before the quiet waits for
    // ever: once its object has heard, the member closes and the set rules.
    fn waits_on_quiet(&self, tree: &Tree, node: NodeId, coming: &Coming, reading: bool) -> bool {
        if node == NodeId::ROOT
            || !members_known(tree, node, reading)
            || !self.lost_for_good(tree, node, coming)
        {
            return false;
        }
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        let sets = tree.recovery_nodes(node);
        let due = self.due(tree, node, reading);
        sets.iter().copied().all(|set| {
            if aside(tree, set) {
                return true;
            }
            if tree.lent(set) || coming.sets.contains(&set) || due.contains(&set) {
                return false;
            }
            let Some(recovery) = tree.recovery(set) else {
                return true;
            };
            match recovery.outcome() {
                Some(outcome) => outcome != RepairOutcome::Waiting,
                None => namespace.members().any(|member| {
                    recovery.covers(member.id) && !member.closed && !member.is_complete()
                }),
            }
        })
    }

    // What each set last ruled, parents first; `None` for one that has not.
    pub fn outcomes(&self, tree: &Tree) -> Vec<Option<RepairOutcome>> {
        self.sets(tree)
            .into_iter()
            .map(|(_, set)| tree.recovery(set).and_then(|recovery| recovery.outcome()))
            .collect()
    }

    // Each set of every namespace, parents first, with what it last ruled.
    // A set whose repair is out is one whose ruling is pending; what it said
    // when it left is its lender's to tell. A set that read nothing ruled on
    // nothing, so it is not among them.
    pub(crate) fn states(&self, tree: &Tree) -> Vec<SetState> {
        self.sets(tree)
            .into_iter()
            .filter_map(|(namespace, node)| self.state(tree, namespace, node))
            .collect()
    }

    pub(crate) fn state(&self, tree: &Tree, namespace: NodeId, node: NodeId) -> Option<SetState> {
        let kind = tree.kind(node)?;
        let Some(recovery) = tree.recovery(node) else {
            return tree.lent(node).then_some(SetState {
                node,
                namespace,
                kind,
                outcome: None,
            });
        };
        let outcome = recovery.outcome();
        (outcome != Some(RepairOutcome::Unread)).then_some(SetState {
            node,
            namespace,
            kind,
            outcome,
        })
    }

    // Each set the object at `set` holds, as its last repair left it.
    pub fn rulings(&self, tree: &Tree, set: NodeId, out: &mut Vec<SetRuling>) {
        if let Some(recovery) = tree.recovery(set) {
            recovery.sets(out);
        }
    }

    // What the object at `set` says of its sets that cannot rebuild what
    // they bind, set by set, when it can say it. A set out on loan says
    // nothing until it returns.
    pub(crate) fn short_sets(&self, tree: &Tree, set: NodeId) -> Option<ShortSets> {
        tree.recovery(set)?.short_sets()
    }

    // Each standing set of every namespace that bound no member, with its
    // object's node and kind and why, as its object says. Asked once, when
    // the job is done.
    pub(crate) fn unbound(&self, tree: &Tree) -> Vec<(NodeId, Kind, Unbound)> {
        let mut found = Vec::new();
        let mut said = Vec::new();
        for (_, set) in self.sets(tree) {
            let (Some(kind), Some(recovery)) = (tree.kind(set), tree.recovery(set)) else {
                continue;
            };
            said.clear();
            recovery.unbound(&mut said);
            found.extend(said.drain(..).map(|unbound| (set, kind, unbound)));
        }
        found
    }

    // Every member a set disputes, as its object says once the job is
    // done: the set, and the member's namespace. Sets that stand aside
    // are asked too; a set's rulings say what it disputes otherwise.
    pub(crate) fn disputed(&self, tree: &Tree) -> Vec<(NodeId, NodeId, MemberId)> {
        let mut found = Vec::new();
        let mut said = Vec::new();
        for node in tree.live() {
            let Some(namespace) = tree.namespace(node) else {
                continue;
            };
            for set in tree.recovery_nodes(node) {
                let Some(recovery) = tree.recovery(set) else {
                    continue;
                };
                said.clear();
                recovery.disputed(namespace, &mut said);
                found.extend(said.drain(..).map(|member| (set, node, member)));
            }
        }
        found
    }

    // What a set is called: what the file that carries it is.
    pub fn set_name<'t>(&self, tree: &'t Tree, set: NodeId) -> Option<&'t str> {
        tree.volumes(set)
            .and_then(|(parent, volumes)| tree.namespace(parent)?.get(*volumes.values().next()?))
            .map(|record| record.output_name.as_str())
    }

    // The block sizes the namespace's sets fold the member's evidence at.
    // Bytes its source cuts at their boundaries prove a slice as they
    // pass, rather than be read back.
    pub(crate) fn block_sizes(&self, tree: &Tree, node: NodeId, member: MemberId) -> Vec<u64> {
        let mut sizes = Vec::new();
        for set in tree.recovery_nodes(node) {
            if let Some(recovery) = tree.recovery(set) {
                recovery.block_sizes(member, &mut sizes);
            }
        }
        sizes
    }

    // Rule the namespace's sets that are due. A file a set describes and
    // no member carries is announced first, once the namespace has every
    // member it will get, so the repair has a member to write it into.
    // `reading` says which namespaces' sources may still announce members.
    // A due set's repair goes to `lender`, as far as it admits one.
    pub fn rule(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, reading): (NodeId, &dyn Fn(NodeId) -> bool),
        lender: &mut dyn Lender,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.announce_absent(tree, builder, segments, (node, reading), out)?;
        let due = self.due(tree, node, reading(node));
        for &set in &due {
            self.feed_moved(tree, segments, node, set);
        }
        self.repair(tree, node, &due, lender)
    }

    // Ranges of `node`'s volumes that will never arrive. The tree marks them
    // damage on every member they land in, at every depth, and whatever set
    // covers them repairs them. A lost range of a set's own volume is gone
    // for good, since nothing recovers the recovery: the set hears it now,
    // or it waits on the gap, and a walk that never passes it finds none of
    // the blocks beyond. A data object hears its losses only once the job is
    // quiet, as a repair may still bring its bytes back. Every set where a
    // range lands is told it is refused, as far as its parent will never
    // bring it.
    pub fn lost(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, ranges): (NodeId, Vec<(VolumeIndex, ByteRange)>),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        for (volume, range) in ranges {
            let from = effects.len();
            tree.damage(node, volume, range, &mut effects);
            let marked: Vec<(NodeId, MemberId, ByteRange)> = effects[from..]
                .iter()
                .filter_map(|effect| match effect {
                    Effect::Damaged {
                        node,
                        member,
                        range,
                    } => Some((*node, *member, *range)),
                    _ => None,
                })
                .collect();
            for marked in marked {
                self.marked_lost(tree, builder, node, marked, &mut effects);
            }
        }
        if effects.is_empty() {
            return Ok(());
        }
        self.apply_all(tree, builder, segments, effects, out)
    }

    // A member range a loss told at `origin` marked: a recovery set bound
    // to it hears its volume lost, and every set of its namespace hears it
    // refused as far as `origin` will never bring it.
    pub(crate) fn marked_lost(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        origin: NodeId,
        marked: (NodeId, MemberId, ByteRange),
        effects: &mut Vec<Effect>,
    ) {
        self.set_volume_lost(tree, builder, marked, effects);
        self.refuse_range(tree, origin, marked);
        self.lost_marked.insert((marked.0, marked.1));
    }

    // The parent's other answer to a seek: the range of the member will
    // never come. Every set of the member's namespace hears it, bound to the
    // member or not, through its recovery face and nothing else, once
    // nothing above can bring the range back.
    fn refuse_range(
        &self,
        tree: &mut Tree,
        origin: NodeId,
        (node, member, range): (NodeId, MemberId, ByteRange),
    ) {
        if !self.refused_above(tree, origin, node) {
            return;
        }
        for set in tree.recovery_nodes(node) {
            tree.refuse(set, member, range);
        }
    }

    // Whether what was lost at `origin` is lost for good at `node`, a
    // namespace at or below it: at `origin` it is. Below, the run above
    // refuses what its damaged volumes cannot produce, which is everything
    // they lost unless a set beside them that has not ruled impossible
    // covers one: that set may yet rebuild what the run reads. A volume
    // that holds every byte, each one good, lost nothing for a set to
    // rebuild, whatever covers it.
    fn refused_above(&self, tree: &Tree, origin: NodeId, node: NodeId) -> bool {
        let mut at = node;
        while at != origin {
            let Some((parent, volumes)) = tree.volumes(at) else {
                return false;
            };
            let sets = tree.recovery_nodes(parent);
            let namespace = tree.namespace(parent);
            let rebuilt = volumes.values().any(|member| {
                let good = namespace
                    .and_then(|namespace| namespace.get(*member))
                    .is_some_and(|record| record.is_complete() && record.coverage.is_clean());
                !good
                    && self.covering(tree, &sets, *member).any(|set| {
                        tree.recovery(set).is_none_or(|recovery| {
                            !matches!(recovery.outcome(), Some(RepairOutcome::Impossible { .. }))
                        })
                    })
            });
            if rebuilt {
                return false;
            }
            at = parent;
        }
        true
    }

    fn set_volume_lost(
        &self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        (node, member, range): (NodeId, MemberId, ByteRange),
        effects: &mut Vec<Effect>,
    ) {
        let Some((set, volume)) = tree
            .child_of(node, member)
            .filter(|(child, _)| tree.recovers(*child))
        else {
            return;
        };
        tree.volume_lost(set, volume, range, &mut self.placing(builder), effects);
    }

    // The namespace's sets due to rule, each judged alone. A set that has
    // not ruled rules once every member it covers is closed, or is open
    // only for bytes refused above it, and the
    // namespace has announced every member it will get, since a volume of
    // its own may still be on its way behind them. A ruling stands until
    // the object takes it back: a wait on recovery bytes ends in the step
    // the object holds what it waited for, or knows it never will, and the
    // set rules again then. It learns what it covers from its own bytes, so
    // one that covers nothing yet rules only once a volume of its own is
    // in: a verdict on nothing would read as settled to every member beside
    // it, and a member it binds after would never be repaired before an
    // engine reads it. One that has yet to learn what it describes rules
    // once every other member is in too: nothing else will ask for the
    // volumes that would say, and its ruling waits on them.
    //
    // A set over members a narrower set covers too rules after it: while
    // a set over a strict subset of its covered members has yet to rule or
    // is out repairing, the wider one waits, so it rules on what the
    // narrower one left. A record inside one volume repairs that volume
    // before the PAR2 set over every volume reads it.
    fn due(&self, tree: &Tree, node: NodeId, reading: bool) -> Vec<NodeId> {
        let Some(namespace) = tree.namespace(node) else {
            return Vec::new();
        };
        let known = members_known(tree, node, reading);
        let sets = tree.recovery_nodes(node);
        // What each set that has yet to rule, or is out, covers: asked once,
        // and only once a set is otherwise due, for its own ruling and for
        // every wider set beside it.
        let mut pending: Option<Vec<(NodeId, BTreeSet<MemberId>)>> = None;
        sets.iter()
            .copied()
            .filter(|&set| {
                let Some(recovery) = tree
                    .recovery(set)
                    .filter(|recovery| recovery.outcome().is_none())
                else {
                    return false;
                };
                if aside(tree, set) {
                    return false;
                }
                if !known
                    || !namespace.members().all(|member| {
                        !recovery.covers(member.id)
                            || member.closed
                            || self.open_only_for_refused(tree, node, reading, member)
                    })
                {
                    return false;
                }
                let pending = pending.get_or_insert_with(|| pending_covers(tree, namespace, &sets));
                let Some((_, covers)) = pending.iter().find(|(pending, _)| *pending == set) else {
                    return false;
                };
                (!covers.is_empty()
                    || closed_volumes(tree, set) > 0
                    || (recovery.undescribed() && self.members_in(tree, node)))
                    && !pending.iter().any(|(other, narrower)| {
                        *other != set
                            && !narrower.is_empty()
                            && narrower.len() < covers.len()
                            && narrower.is_subset(covers)
                    })
            })
            .collect()
    }

    // Below the root, the object that reads a member closes it only once
    // the job is quiet, and the job is not quiet while a set over the
    // member waits to rule. A member there that holds every byte, with no
    // run left to read more into it and its damage lost for good above,
    // holds every byte it will: its sets rule on it as on a closed one. A
    // root member is closed by the root once nothing it waits on can come.
    fn open_only_for_refused(
        &self,
        tree: &Tree,
        node: NodeId,
        reading: bool,
        member: &weaver_graph::Member,
    ) -> bool {
        node != NodeId::ROOT
            && !reading
            && member.is_complete()
            && self.refused_above(tree, NodeId::ROOT, node)
    }

    // Below the root, nothing above can bring back what the namespace's
    // members lost: once nothing more comes to them and nothing above them
    // reads, a member holds every byte it will, closed or not: its losses
    // held as damage, and any bytes an object above still holds staying
    // held until the job is quiet. Its object closes it only once the job
    // is quiet, and a run reading it keeps the job from being quiet, so the
    // run is told it holds all it will now.
    //
    // Every set of every namespace above has ruled first: one yet to learn
    // what it covers, to bind a volume by its content, or to read a volume
    // of its own still arriving covers nothing yet, and may still rebuild
    // what the member lost once it does.
    pub(crate) fn lost_for_good(&self, tree: &Tree, node: NodeId, coming: &Coming) -> bool {
        if node == NodeId::ROOT {
            return false;
        }
        let mut at = node;
        while at != NodeId::ROOT {
            let Some((parent, _)) = tree.volumes(at) else {
                return false;
            };
            if !self.ruled(tree, parent, coming) {
                return false;
            }
            at = parent;
        }
        self.refused_above(tree, NodeId::ROOT, node)
    }

    // Every member of the namespace holds every byte it will: closed,
    // whole, or a set's own volume, whose object reads what of it comes. A
    // range marked lost is held as damage, so a member open only for bytes
    // lost above it is in: nothing left it unplaced.
    fn members_in(&self, tree: &Tree, node: NodeId) -> bool {
        tree.namespace(node).is_some_and(|namespace| {
            namespace.members().all(|member| {
                member.closed
                    || member.is_complete()
                    || self.carries_recovery(tree, node, member.id)
            })
        })
    }

    // A file a set of the namespace describes and no member carries is
    // announced once the namespace has every member it will get and every
    // byte of them, and closed at once, since nothing will arrive for it.
    // Its set binds it by name, finds every slice of it missing, and the
    // repair writes it whole. A set that covers none of the namespace's
    // members describes files that may yet be founded in a namespace bound
    // in it, where it would move to them: its files wait until every one of
    // those has announced every member it will get too, so a namespace
    // above never claims the name of a member founded below it.
    fn announce_absent(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, reading): (NodeId, &dyn Fn(NodeId) -> bool),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // Only a set describes files: a namespace with none announces
        // nothing, whatever its members.
        let sets = tree.recovery_nodes(node);
        let known = |namespace: NodeId| members_known(tree, namespace, reading(namespace));
        if sets.is_empty() || !known(node) || !self.members_in(tree, node) {
            return Ok(());
        }
        let below_known = tree.namespaces_below(node).into_iter().all(known);
        let mut absent = Vec::new();
        for set in sets {
            let Some(recovery) = tree.recovery(set) else {
                continue;
            };
            if below_known || covers_any(tree, node, recovery) {
                recovery.absent(&mut absent);
            }
        }
        // A name some posting already has is its member's, bound or not.
        if let Some(namespace) = tree.namespace(node) {
            unposted(
                &mut absent,
                namespace.records().map(|member| member.name.as_str()),
            );
        }
        for (name, length) in absent {
            let mut effects = Vec::new();
            let Some(member) = tree.announce_rebuilt(node, &name, length, &mut effects) else {
                continue;
            };
            if !effects.is_empty() {
                self.apply_all(tree, builder, segments, effects, out)?;
            }
            self.close(tree, builder, segments, node, member, out)?;
        }
        Ok(())
    }

    // Lend the sets that are due to rule, and only those: one ruled before
    // it is due rules on members still arriving and volumes it has not read,
    // and its verdict stands. Each goes out of the tree with what its reads
    // reach, so it repairs off the loop while the tree goes on; a due check
    // meanwhile finds it out and does not take it again. A PAR3 set over
    // the posts goes without the files that never arrived a disk of its
    // size could never hold, and waits, with the job, until the disks have
    // room for the rest. While the job waits for room no repair goes out,
    // and what the job waits for on the sets' behalf follows what they
    // would write now: a file that arrived since is no longer theirs to
    // rebuild, and a set no longer due wants nothing. That is asked again
    // only once something it rests on changed, not on every walk.
    pub fn repair(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        due: &[NodeId],
        lender: &mut dyn Lender,
    ) -> Result<(), WriteError> {
        if node == NodeId::ROOT {
            if !self.caps.disk_held() {
                self.held_want = None;
            } else {
                if !self.held_want_stands(tree, due) {
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    HELD_RECHECKS.with(|rechecks| rechecks.set(rechecks.get() + 1));
                    self.caps.rewant();
                    let mut absent = BTreeSet::new();
                    for &set in due {
                        let _ = self.withheld(tree, set, Some(&mut absent));
                    }
                    self.held_want = Some(HeldWant {
                        due: due.to_vec(),
                        tickets: self.caps.disk_tickets(),
                        sizes: self.disk_sizes(),
                        at: tree
                            .namespace(NodeId::ROOT)
                            .map_or(0, Namespace::generation),
                        absent,
                    });
                }
                return Ok(());
            }
        }
        for &set in due {
            // A repair writes what it rebuilds: none goes out while the job
            // waits for room.
            if self.caps.disk_held() || !lender.admit() {
                break;
            }
            let withheld = match node == NodeId::ROOT {
                true => self.withheld(tree, set, None),
                false => Some(BTreeSet::new()),
            };
            let Some(withheld) = withheld else {
                continue;
            };
            if let Some(mut loan) = self.lend(tree, node, set)? {
                loan.withhold(withheld);
                lender.lend(loan);
            }
        }
        Ok(())
    }

    // Whether what the job waiting for room last found its due sets would
    // rebuild still holds: the same sets, the same wait, the same disks,
    // and no file of the posts marked since has come to arrive, been lost,
    // or come to be wanted. Only the files marked since are looked at.
    fn held_want_stands(&mut self, tree: &Tree, due: &[NodeId]) -> bool {
        let tickets = self.caps.disk_tickets();
        let sizes = self.disk_sizes();
        let Some(held) = self.held_want.as_mut() else {
            return false;
        };
        if held.due != due || held.tickets != tickets || held.sizes != sizes {
            return false;
        }
        let Some(posts) = tree.namespace(NodeId::ROOT) else {
            return false;
        };
        let at = posts.generation();
        if at == held.at {
            return true;
        }
        let stands = posts.changed_since(held.at).all(|member| {
            let absent = posts.get(member).is_some_and(|record| {
                absent_from_posts(record)
                    && due.iter().any(|&set| {
                        tree.recovery(set).is_some_and(|recovery| {
                            recovery.outgrows_recovery() && recovery.covers(member)
                        })
                    })
            });
            absent == held.absent.contains(&member)
        });
        if stands {
            held.at = at;
        }
        stands
    }

    // A lent repair came back. The object takes its node again and hears
    // what reached it while it was out. When the loop moved something the
    // repair read, its ruling was made on bytes that are not there now: the
    // object is rebuilt as it left, from the facts its node kept and its
    // volumes' files, and the set is due again. Otherwise what it came to
    // is applied: the ranges it repaired come back through the one write
    // path as repaired bytes, read from the repair's output a slice at a
    // time, so a leaf is overwritten and a bound member's object takes them
    // as any delivery. A member whose bytes it rewrote or
    // proved is offered again once applied, so whatever refused it on the
    // bytes it held before reads it as it is now. Stale work that cannot be
    // reconstructed fails explicitly and publishes none of its staged bytes.
    // The slices a repair applied rebuilt, when it rebuilt any: that return
    // is the one moment the repair is complete, said once.
    pub fn returned(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        restore: &dyn Restore,
        mut returned: Returned,
        out: &mut Vec<Effect>,
    ) -> Result<Tailed, WriteError> {
        // A repair whose output refused a write failed with it, and nothing
        // it came to is applied. The refusal is the error, whatever removing
        // the output then says.
        self.gave_back(&returned.repaired);
        if let Some(error) = returned.repaired.failed() {
            let _ = returned.repaired.remove();
            return Err(error.into());
        }
        // A set retracted while it was out has nowhere to rule.
        if !tree.lent(returned.set) {
            returned.repaired.remove()?;
            return Ok(Tailed::Done(None));
        }
        // A repair the shutdown stopped is not repaired: what it read was
        // cut short. It is rebuilt as it left, as one the loop moved under
        // is, and its set is due again, which the restored job rules.
        let stopped = returned.stopped();
        let stale = self.moved_since(tree, &returned);
        let rebuilt = if stopped || stale {
            tree.lent_facts(returned.set)
                .and_then(|facts| restore.restore(facts).ok())
        } else {
            None
        };
        if stale && !stopped && rebuilt.is_none() {
            returned.repaired.remove()?;
            return Err(WriteError::StaleRecovery { set: returned.set });
        }
        let Returned {
            set,
            namespace: node,
            object,
            outcome,
            reads,
            repaired,
            found,
            diagnostics,
            ..
        } = returned;
        let mut effects = Vec::new();
        if let Some(rebuilt) = rebuilt {
            tree.take_back(set, rebuilt, &mut self.placing(builder), &mut effects);
            self.apply_all(tree, builder, segments, effects, out)?;
            // Facts keep no bytes of the volumes: the object hears them
            // again from their files, as a restored one hears them again
            // from the articles.
            let volumes: Vec<(NodeId, MemberId)> = tree
                .volumes(set)
                .map(|(parent, volumes)| volumes.values().map(|member| (parent, *member)).collect())
                .unwrap_or_default();
            // What arrived of the volumes, which `replay_file` reads again.
            let bytes: u64 = volumes
                .iter()
                .filter_map(|&(parent, member)| tree.namespace(parent)?.get(member))
                .flat_map(|record| record.coverage.runs())
                .filter(|run| run.provenance.arrived())
                .map(|run| run.range.len())
                .sum();
            tracing::info!(
                job_id = self.scope,
                node = set.0,
                volumes = volumes.len(),
                bytes,
                "repair return stale, volumes replayed"
            );
            for (parent, member) in volumes {
                self.replay_file(tree, builder, segments, parent, member, None, out)?;
            }
            let mut effects = Vec::new();
            tree.heard_again(set, &mut effects);
            self.apply_all(tree, builder, segments, effects, out)?;
            repaired.remove()?;
            return Ok(Tailed::Done(None));
        }
        tree.take_back(set, object, &mut self.placing(builder), &mut effects);
        self.apply_all(tree, builder, segments, effects, out)?;
        // A stopped repair whose object keeps no facts is taken back as it
        // came, and nothing it came to is applied.
        if stopped {
            repaired.remove()?;
            return Ok(Tailed::Done(None));
        }
        for diagnostic in diagnostics {
            tracing::warn!(
                job_id = self.scope,
                node = set.0,
                namespace = node.0,
                operation = %diagnostic.operation,
                context = %diagnostic.context,
                error = %diagnostic.error,
                causes = ?diagnostic.causes,
                "recovery worker operation failed"
            );
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            self.diagnostics.push(diagnostic);
        }
        self.read_backs += reads;
        // What a set that can outgrow its recovery wrote into a post none of
        // whose bytes arrived is held to the disk reserve before any of it
        // lands; what a repair wrote into an object's output is held to the
        // job's limits as it lands, as that output always is.
        if node == NodeId::ROOT
            && tree
                .recovery(set)
                .is_some_and(|recovery| recovery.outgrows_recovery())
            && let Err(error) = self.admit_repaired(tree, repaired.sizes())
        {
            let _ = repaired.remove();
            return Err(error);
        }
        // The set binds what it ruled before its bytes land: a member it
        // rebuilds takes the length it describes, so the slices that reach
        // that length close the file there. A repair reads members back, so
        // it may have identified one by its content; this bind gives it the
        // name it is.
        self.acknowledging = Some(Acknowledging {
            set,
            closed: BTreeSet::new(),
            rereading: false,
            placing: true,
        });
        let mut apply = Apply {
            set,
            node,
            found,
            outcome,
            rejected: tree.rejected_inputs(node, repaired.members()),
            rewritten: BTreeSet::new(),
            offered: false,
            landed: false,
            contested: false,
        };
        let bound = self.bind_recoveries(tree, builder, segments, node, out);
        // Whether every slice was turned away. A slice lands only where no
        // repair wrote before it, unless the set outranks every other set
        // beside it: a set that disagrees with another's repair loses there
        // to an equal or a higher one, so no two rewrite each other in turn.
        apply.contested = !tree.outranks(node, set);
        // With volumes to write to, the slices are read back off the loop
        // and counted there on the volumes they may land on, a mail each
        // (`RepairSlice`): the loop places each as it comes and never waits
        // for a disk. The job hands the output to its reader
        // (`apply_output`). A return that staged nothing writes nothing, so
        // it is applied in this step, as a ruling with no slices always is:
        // the set takes back what it ruled with its ruling, and what arrives
        // after is heard by the set as it then stands.
        if self.disk().is_some() && repaired.sizes().next().is_some() {
            self.placing_return(false);
            self.applying = Some(Applying {
                apply,
                error: bound.err(),
                output: Some(repaired),
                said: std::mem::take(out),
                slices: 0,
            });
            return Ok(Tailed::Done(None));
        }
        // Here the job has no volumes to wait on, or the return staged no
        // slices.
        let placed = bound.and_then(|()| {
            for (member, offset, bytes) in repaired.slices() {
                self.place_slice(
                    tree,
                    builder,
                    segments,
                    &mut apply,
                    (member, offset, bytes?),
                    out,
                )?;
            }
            Ok(())
        });
        self.applied(tree, builder, segments, apply, (repaired, placed), out)
    }

    // Place one slice of a repair's return through the one write path, as
    // repaired bytes. Called with the return's batch open.
    fn place_slice(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        apply: &mut Apply,
        (member, offset, bytes): (MemberId, u64, Bytes),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        apply.offered = true;
        let arrival = Arrival {
            node: apply.node,
            member,
            offset,
            // A repair's output: nothing proved it on the way.
            bytes: bytes.into(),
            provenance: Provenance::Repaired,
        };
        let write = Write::new(&arrival, LoopSite::Repair);
        if self.place_as(
            tree,
            builder,
            segments,
            (arrival, apply.contested),
            write,
            out,
        )? > 0
        {
            apply.landed = true;
            apply.rewritten.insert(member);
        }
        Ok(())
    }

    // The tail of a repair's return, whether its slices were placed in one
    // step or a mail at a time: called with the return's batch open, once
    // every slice is down or `placed` failed. What it delivers again is
    // paid piece by piece without the loop standing still for a volume
    // (`ReturnTail`): it may stand for room and go on when a thread counted it
    // (`tail_paid`). Done, with the slices a repair applied rebuilt when it
    // rebuilt any, or standing.
    fn applied(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        apply: Apply,
        (repaired, placed): (super::Repaired, Result<(), WriteError>),
        out: &mut Vec<Effect>,
    ) -> Result<Tailed, WriteError> {
        let Apply {
            set,
            node,
            found,
            outcome,
            rejected,
            rewritten,
            offered,
            landed,
            ..
        } = apply;
        let begun = (|| {
            // The staged output goes whether or not its slices landed: one
            // a member had no room for is not left on the disk while the
            // job waits. The repair runs again, so the job waits for all it
            // writes, staged and placed.
            let writes = repaired.writes();
            let removed = repaired.remove();
            if let Err(error) = placed {
                if let WriteError::Leaf(super::LeafError::Io { source, .. }) = &error
                    && crate::operations::is_out_of_space(source)
                    && writes > 0
                {
                    self.gave_back = Some(self.gave_back.unwrap_or(0).max(writes));
                    self.left_to_write = None;
                }
                return Err(error);
            }
            removed?;
            Ok::<_, WriteError>(())
        })();
        if let Err(error) = begun {
            self.acknowledging = None;
            return Err(error);
        }
        self.tail = Some(ReturnTail {
            set,
            node,
            outcome,
            rejected,
            rewritten,
            turned_away: offered && !landed,
            step: TailStep::Found,
            into: node,
            ranges: found
                .into_iter()
                .map(|(member, range)| (member, range, Provenance::Refuted))
                .collect(),
            member: MemberId(0),
            provenance: Provenance::Refuted,
            parts: Default::default(),
            paid: 0,
            stood: 0,
            said: Vec::new(),
            redelivered: (0, 0),
        });
        self.tail_on(tree, builder, segments, out)
    }

    // A thread counted on the job's volumes the piece the standing tail
    // waits for, and the step hearing it was prepaid with that: the tail
    // goes on from that piece. A wait the end of a stop cut short leaves the
    // return unapplied, as the loop's own wait at that end did
    // (`WriteError::LeftAtStop`).
    pub(crate) fn tail_paid(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<Tailed, WriteError> {
        let Some(tail) = self.tail.as_mut() else {
            return Ok(Tailed::Done(None));
        };
        if self.disk.as_ref().is_some_and(|disk| disk.left_at_stop()) {
            self.drop_tail();
            return Err(WriteError::LeftAtStop);
        }
        tail.paid = tail.stood;
        self.placing_return(true);
        self.tail_on(tree, builder, segments, out)
    }

    // The job drops a standing tail, as it fails or retires: what it
    // delivered is not ruled on. Whether one stood.
    pub(crate) fn drop_tail(&mut self) -> bool {
        let stood = self.tail.take().is_some();
        if stood {
            self.acknowledging = None;
        }
        stood
    }

    // The tail goes on until it is done, fails, or stands for room for its
    // next piece. Its batch closes as it ends either way.
    fn tail_on(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<Tailed, WriteError> {
        let Some(mut tail) = self.tail.take() else {
            return Ok(Tailed::Done(None));
        };
        let stood = self.tail_steps(tree, builder, segments, &mut tail, out);
        match stood {
            Ok(Some(bytes)) => {
                // Bytes that arrive while it stands are fed as any others.
                self.placing_return(false);
                tail.stood = bytes;
                tail.said.append(out);
                self.tail = Some(tail);
                Ok(Tailed::Standing(bytes))
            }
            Ok(None) => {
                let batch = self
                    .acknowledging
                    .take()
                    .expect("the batch is open while a return is applied");
                let mut said = std::mem::take(&mut tail.said);
                said.append(out);
                *out = said;
                self.acknowledged(tree, builder, segments, batch, out)?;
                // A repair none of whose slices landed rewrote nothing, and
                // is not said to have.
                Ok(Tailed::Done(match tail.outcome {
                    Some(RepairOutcome::Repaired { slices }) if !tail.turned_away => Some(slices),
                    _ => None,
                }))
            }
            Err(error) => {
                self.acknowledging = None;
                Err(error)
            }
        }
    }

    // The tail's steps from where it stands: what the set found delivered
    // again, what was held behind it released, what the set vouches for
    // delivered again, and the rest of the ruling, each released hold paid
    // as a piece is. `Some` with the bytes of the next piece or span when
    // the job's volumes have no room for it now; the tail goes on at the
    // same step.
    fn tail_steps(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        tail: &mut ReturnTail,
        out: &mut Vec<Effect>,
    ) -> Result<Option<u64>, WriteError> {
        loop {
            match tail.step {
                TailStep::Found | TailStep::Proved => {
                    if let Some(bytes) = self.redeliver_paid(tree, builder, segments, tail, out)? {
                        tail.into = tail.node;
                        return Ok(Some(bytes));
                    }
                    tail.step = match tail.step {
                        TailStep::Found => TailStep::Held,
                        _ => TailStep::Close,
                    };
                }
                // A rebuilt volume can let its object place bytes held
                // behind it; they go down before the set's proof is read
                // back through the map.
                TailStep::Held => {
                    let paid = &mut tail.paid;
                    if let Some((bytes, into)) =
                        self.release_held_paid(tree, builder, segments, paid, out)?
                    {
                        tail.into = into;
                        return Ok(Some(bytes));
                    }
                    // The ruling as the set stands now: one given before the
                    // bind changed what it covers is void, and vouches for
                    // nothing.
                    if self.stands_proved(tree, tail.set) {
                        let marked = self.vouched(tree, (tail.node, tail.set));
                        tail.rewritten
                            .extend(marked.iter().map(|(member, _)| *member));
                        tail.ranges.extend(
                            marked
                                .into_iter()
                                .map(|(member, range)| (member, range, Provenance::Repaired)),
                        );
                    }
                    tail.step = TailStep::Proved;
                }
                TailStep::Close => {
                    let (ranges, bytes) = tail.redelivered;
                    tracing::info!(
                        job_id = self.scope,
                        node = tail.node.0,
                        ranges,
                        bytes,
                        "repair tail redelivered"
                    );
                    self.tail_close(tree, builder, segments, tail, out)?;
                    tail.step = TailStep::Last;
                }
                TailStep::Last => {
                    let paid = &mut tail.paid;
                    if let Some((bytes, into)) =
                        self.release_held_paid(tree, builder, segments, paid, out)?
                    {
                        tail.into = into;
                        return Ok(Some(bytes));
                    }
                    return Ok(None);
                }
            }
        }
    }

    // The members the set rewrote or proved are reopened and shut, and the
    // damage their changed inputs make auditable is queued to be checked.
    fn tail_close(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        tail: &mut ReturnTail,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let node = tail.node;
        let rewritten = std::mem::take(&mut tail.rewritten);
        let mut rejected = std::mem::take(&mut tail.rejected);
        // Repair or proof delivery can complete a member's checksum
        // metadata or inverse map. Capture any newly auditable damage
        // through those changed inputs, alongside the earlier captures.
        rejected.extend(tree.rejected_inputs(node, rewritten.iter().copied()));
        for &member in &rewritten {
            self.reopen(tree, builder, segments, (node, member), out)?;
        }
        // A member the set rebuilt or proved whole has its length and every
        // byte proven: that proof closes it, through the close its
        // producer's would take, and the batch judges it with every member
        // it closed.
        for member in rewritten {
            if proven_open(tree, node, member) {
                self.shut(tree, builder, segments, node, member, out)?;
            }
        }
        self.queue_rechecks(rejected);
        Ok(())
    }

    // How many slices of the return being applied were placed so far.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn slices_applied(&self) -> u64 {
        self.applying.as_ref().map_or(0, |applying| applying.slices)
    }

    // The staged output of the return whose apply just opened, for the job
    // to hand its reader, with the set it is of.
    pub(crate) fn apply_output(&mut self) -> Option<(NodeId, super::Repaired)> {
        let applying = self.applying.as_mut()?;
        Some((applying.apply.set, applying.output.take()?))
    }

    // One slice of the return being applied came back from its reader: it
    // is placed under the set's ruling, as the slices of a return applied
    // in one step are, and nothing after a failure is. Whether the reader
    // may read on.
    pub(crate) fn repair_slice(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        slice: super::RepairSlice,
    ) -> bool {
        let Some(mut applying) = self.applying.take() else {
            return false;
        };
        if applying.error.is_none() {
            self.placing_return(true);
            let placed = slice.bytes.map_err(WriteError::from).and_then(|bytes| {
                self.place_slice(
                    tree,
                    builder,
                    segments,
                    &mut applying.apply,
                    (slice.member, slice.offset, bytes),
                    &mut applying.said,
                )
            });
            self.placing_return(false);
            applying.slices += 1;
            applying.error = placed.err();
        }
        let reads_on = applying.error.is_none();
        self.applying = Some(applying);
        reads_on
    }

    // The reader of the return being applied is done: the return's tail
    // runs, as an apply in one step ends. A reader cut short before the
    // last slice, at the end of a stop, leaves the return unapplied: the
    // job stands on bytes that are not written and is left at its last
    // checkpoint (`WriteError::LeftAtStop`).
    pub(crate) fn repair_read(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        read: super::RepairRead,
        out: &mut Vec<Effect>,
    ) -> Result<Tailed, WriteError> {
        let Some(applying) = self.applying.take() else {
            read.output.remove()?;
            return Ok(Tailed::Done(None));
        };
        let Applying {
            apply, error, said, ..
        } = applying;
        out.extend(said);
        self.placing_return(true);
        let placed = match error {
            Some(error) => Err(error),
            None if !read.whole => Err(WriteError::LeftAtStop),
            None => Ok(()),
        };
        self.applied(tree, builder, segments, apply, (read.output, placed), out)
    }

    // Whether the bytes being delivered are the return's own (`placing`).
    fn placing_return(&mut self, placing: bool) {
        if let Some(batch) = self.acknowledging.as_mut() {
            batch.placing = placing;
        }
    }

    // The job drops the return being applied, as it fails or retires: what
    // its slices placed is not ruled on, and its staged output goes. Whether
    // one was being applied.
    pub(crate) fn drop_apply(&mut self, output: super::Repaired) -> Result<bool, LeafError> {
        let applying = self.applying.take().is_some();
        if applying {
            self.acknowledging = None;
        }
        output.remove()?;
        Ok(applying)
    }

    // The batch is down: each member it closed that is still closed in its
    // own file gets its verdict once, on the bytes it ends with. One the
    // batch reopened or handed to an object is judged when it closes again.
    pub fn acknowledged(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        batch: Acknowledging,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        for (node, member) in batch.closed {
            let closed = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| record.closed);
            if closed && in_own_file(tree, node, member) {
                self.judge(tree, builder, segments, (node, member), out)?;
            }
        }
        Ok(())
    }

    // A set that found what it covers whole, or made it so, has proven
    // every byte its checks reach, the ones that arrived marked damaged and
    // were good after all among them: it rewrites only the slices that were
    // bad. Every byte still marked damaged there, or marked damaged below on
    // a member an object further down reads, over bytes that came from
    // there, is delivered again as repaired, read back from wherever it
    // went, so those marks say what the set proved; damage past what it
    // proves stays marked, and so does a producer's verdict on a member
    // nothing reads further, which its producer judges again. Returns the
    // ranges to deliver again, each member's in order, none overlapping.
    fn vouched(&self, tree: &Tree, (node, set): (NodeId, NodeId)) -> Vec<(MemberId, ByteRange)> {
        let sets = tree.recovery_nodes(node);
        let mut marked: Vec<(MemberId, ByteRange)> = tree
            .namespace(node)
            .map(|namespace| {
                namespace
                    .damaged_ranges()
                    .chain(tree.damaged_below(node))
                    .filter(|(member, _)| {
                        self.covering(tree, &sets, *member)
                            .any(|covering| covering == set)
                    })
                    .filter_map(|(member, range)| {
                        Some((
                            member,
                            namespace.proven(member, range, tree.recovery(set)?)?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        // A range marked here and below goes again once.
        marked.sort_by_key(|(member, range)| (*member, range.start));
        marked.dedup_by(|(member, range), (into, merged)| {
            let overlaps = member == into && range.start <= merged.end;
            if overlaps {
                merged.end = merged.end.max(range.end);
            }
            overlaps
        });
        marked
    }

    // The member's bytes were rewritten or proved: any refusal of it was
    // given on bytes it no longer holds. The tree takes it back and asks
    // who owns it again; what the refusing object kept goes with it, made
    // over by whatever reads the volumes again, and a member no object
    // takes is a leaf for no reason.
    pub(super) fn reopen(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // Held by a live object, the member's bytes went on below it: a
        // refusal there is answered at its own depth.
        for below in tree.refused_through(node, member) {
            self.reopen(tree, builder, segments, below, out)?;
        }
        let mut effects = Vec::new();
        let reopened = tree.reopen(node, member, &mut self.placing(builder), &mut effects);
        if reopened.is_empty() {
            return Ok(());
        }
        let files = self.files_mut(node);
        for volume in &reopened {
            files.unrefuse(*volume);
        }
        self.prune(tree)?;
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::Reopen),
            out,
        )
    }

    // Deliver a range of a member again, read back from wherever it went,
    // under what a set proved of it: the one write path then marks it here
    // and in every namespace below. A set read those bytes to rule on them,
    // and reads take held custody first, so bytes that do not read back now
    // are gone: an error, never a proof dropped. Bytes the member's record
    // says were lost may never have come: where none read back the range
    // stays the loss it is, which a repair mends or the set's verdict
    // answers.
    //
    // The tail's ranges go a read-back piece at a time, each counted on the
    // job's volumes before it is read: by the loop when every volume has
    // room for it now, or else by the thread the job sends to stand for it
    // (`Some` with its bytes), the piece left where it was until that
    // credit comes. `None` once every range is down.
    fn redeliver_paid(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        tail: &mut ReturnTail,
        out: &mut Vec<Effect>,
    ) -> Result<Option<u64>, WriteError> {
        let node = tail.node;
        loop {
            let Some(&(piece, arrived)) = tail.parts.front() else {
                let Some((member, range, provenance)) = tail.ranges.pop_front() else {
                    return Ok(None);
                };
                tail.member = member;
                tail.provenance = provenance;
                tail.redelivered.0 += 1;
                tail.redelivered.1 += range.len();
                tail.parts = tree
                    .namespace(node)
                    .and_then(|namespace| namespace.get(member))
                    .map_or_else(
                        || vec![(range, true)],
                        |record| record.coverage.by_arrival(range),
                    )
                    .into_iter()
                    .filter(|(piece, _)| piece.start < piece.end)
                    .collect();
                continue;
            };
            let chunk = ByteRange::new(
                piece.start,
                piece.end.min(piece.start.saturating_add(READ_BACK)),
            );
            let credit = if std::mem::take(&mut tail.paid) > 0 {
                None
            } else {
                match self.disk.as_ref() {
                    Some(disk) => match disk.try_reserve(chunk.len()) {
                        Some(credit) => Some(credit),
                        None => return Ok(Some(chunk.len())),
                    },
                    None => None,
                }
            };
            if chunk.end < piece.end {
                tail.parts[0].0 = ByteRange::new(chunk.end, piece.end);
            } else {
                tail.parts.pop_front();
            }
            let at = (node, tail.member, chunk, tail.provenance);
            self.redeliver(tree, builder, segments, at, arrived, credit, out)?;
        }
    }

    // One piece delivered again, paid by `credit` when the loop counted it
    // itself, as a step that hears a producer's bytes is paid
    // (`GraphJob::run_said`), or by what the step was prepaid with.
    #[allow(clippy::too_many_arguments)]
    fn redeliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member, chunk, provenance): (NodeId, MemberId, ByteRange, Provenance),
        arrived: bool,
        credit: Option<weaver_server_disk::operations::disk::writer::Credit>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut bytes = Vec::new();
        if !self
            .access(tree, LoopSite::Tail)
            .read_at(node, member, chunk, &mut bytes)
        {
            if !arrived {
                return Ok(());
            }
            return Err(WriteError::ProofUnreadable {
                node,
                member,
                range: chunk,
            });
        }
        self.with_credit(tree, node, credit, |writer, tree| {
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                // Read back: no evidence comes with it.
                bytes: Bytes::from(bytes).into(),
                provenance,
            };
            let rereading = writer.rereading(true);
            let delivered = writer.arrive(
                tree,
                builder,
                segments,
                Write::new(&arrival, LoopSite::Tail),
                arrival,
                out,
            );
            writer.rereading(rereading);
            delivered
        })
    }

    // Mark the bytes being delivered as ones the namespace held already, or
    // not; returns what was marked before.
    fn rereading(&mut self, rereading: bool) -> bool {
        self.acknowledging
            .as_mut()
            .is_some_and(|batch| std::mem::replace(&mut batch.rereading, rereading))
    }

    // The evidence the grid folds: the segment checksums of the range, to
    // every set that covers the member. Fresh bytes carry the source's
    // checksums as they came. Any other bytes are not the source's, and nor
    // are bytes the source has no checksums for, as in a namespace below the
    // root: they carry checksums of what is in hand, cut where the covering
    // sets fold their blocks, each from the evidence the bytes came with, and
    // are evidence unless they are damage. `posting` is the volume the
    // source keeps its checksums of the bytes under.
    pub(super) fn feed(
        &mut self,
        tree: &mut Tree,
        segments: &dyn Segments,
        arrival: &Arrival,
        posting: MemberId,
    ) {
        let member = arrival.member;
        let sets = tree.recovery_nodes(arrival.node);
        let covering: Vec<NodeId> = self.covering(tree, &sets, member).collect();
        if covering.is_empty() {
            return;
        }
        let range = arrival.range();
        self.segments.clear();
        segments.segments_for(
            arrival.node,
            VolumeIndex(posting.0),
            range,
            &mut self.segments,
        );
        let crc32 = |piece: ByteRange| {
            let (start, end) = (
                index(piece.start - range.start),
                index(piece.end - range.start),
            );
            arrival
                .bytes
                .slice(start..end)
                .digest(Algorithm::Crc32)
                .value as u32
        };
        if self.segments.is_empty() {
            let verified = arrival.provenance != Provenance::Damaged;
            let mut sizes = Vec::new();
            for set in &covering {
                if let Some(recovery) = tree.recovery(*set) {
                    recovery.block_sizes(member, &mut sizes);
                }
            }
            let cuts = block_cuts(range, &mut sizes);
            let mut rest = arrival.bytes.clone();
            let mut at = range.start;
            for cut in cuts.into_iter().chain([range.end]) {
                let (piece, after) = rest.split_at(index(cut - at));
                self.segments.push(Segment {
                    range: ByteRange::new(at, cut),
                    crc32: piece.digest(Algorithm::Crc32).value as u32,
                    verified,
                });
                (rest, at) = (after, cut);
            }
        } else if arrival.provenance != Provenance::Fresh
            || self
                .segments
                .iter()
                .any(|segment| !range.covers(segment.range))
        {
            self.segments.retain_mut(|segment| {
                let Some(piece) = segment.range.intersect(range) else {
                    return false;
                };
                let verified = if arrival.provenance == Provenance::Fresh {
                    segment.verified
                } else {
                    !arrival.provenance.is_damaged()
                };
                *segment = Segment {
                    range: piece,
                    crc32: crc32(piece),
                    verified,
                };
                true
            });
        }
        // Bytes said again keep every ruling over them: they are the bytes
        // each set read, cut wherever the read back cut them, which a set's
        // fold cannot tell from a change. Only bytes the namespace did not
        // hold, a repair's among them, take a ruling back.
        for &set in &covering {
            if self
                .acknowledging
                .as_ref()
                .is_some_and(|batch| (batch.set == set && batch.placing) || batch.rereading)
            {
                tree.feed_proven(set, member, &self.segments);
            } else {
                tree.feed(set, member, &self.segments);
            }
        }
        for set in covering {
            self.feed_moved(tree, segments, arrival.node, set);
        }
    }

    // A member a set came to describe after some of its bytes arrived is
    // said again for that set, as far as the source still keeps its
    // checksums. A set learns what it describes as its own volumes arrive,
    // so the namespace asks after a bind, after it feeds the set and before
    // it rules it: the set reads back only what the source no longer
    // vouches for.
    // A set with nothing to ask for is only read, so its row is not marked.
    fn feed_moved(&mut self, tree: &mut Tree, segments: &dyn Segments, node: NodeId, set: NodeId) {
        if !tree.recovery(set).is_some_and(|recovery| recovery.moved()) {
            return;
        }
        let mut unfed = Vec::new();
        tree.with_recovery(set, |recovery, _| recovery.unfed(&mut unfed));
        for member in unfed {
            self.early_evidence(tree, segments, node, member);
            if !self.segments.is_empty() {
                tree.feed(set, member, &self.segments);
            }
        }
    }

    // What arrived for the member before any set covered it, as the
    // source's checksums of each run the coverage holds. Fresh runs are
    // evidence; a damaged run's checksums are of the bytes the source
    // delivered, which are the bytes written, so they mark its slices. A
    // repaired run's bytes are not the source's, so it gives nothing here
    // and its slices are read back if a repair needs them; so does every
    // run of a namespace the source has no checksums for.
    fn early_evidence(
        &mut self,
        tree: &Tree,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
    ) {
        self.segments.clear();
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return;
        };
        let mut found = Vec::new();
        for run in record.coverage.runs() {
            let verified = match run.provenance {
                Provenance::Fresh => true,
                Provenance::Damaged | Provenance::Refuted | Provenance::Lost => false,
                Provenance::Repaired => continue,
            };
            found.clear();
            segments.segments_for(node, VolumeIndex(member.0), run.range, &mut found);
            self.segments.extend(
                found
                    .iter()
                    .filter(|segment| run.range.covers(segment.range))
                    .map(|segment| Segment {
                        verified,
                        ..*segment
                    }),
            );
        }
    }

    // The source's checksums of each member the set asks for, as it asks:
    // whatever is kept of them, once. Nothing is read back for it.
    fn answer_evidence(
        &mut self,
        tree: &mut Tree,
        segments: &dyn Segments,
        node: NodeId,
        sets: &[NodeId],
        set: NodeId,
    ) {
        let mut wanted = Vec::new();
        if let (Some(recovery), Some(namespace)) = (tree.recovery(set), tree.namespace(node)) {
            let covered = |member| self.covering(tree, sets, member).next().is_some();
            recovery.evidence(namespace, &covered, &mut wanted);
        }
        for member in wanted {
            self.early_evidence(tree, segments, node, member);
            let evidence = &self.segments;
            tree.with_recovery(set, |recovery, namespace| {
                recovery.evidenced(namespace, member, evidence);
            });
        }
    }

    // Let every set of the namespace match its members again, after the
    // members, their lengths or their first bytes changed. A set that names
    // a member just founded in a namespace bound here, or beside here, moves
    // there first and binds there. A member a set
    // covers for the first time is fed everything that arrived for it
    // before, so early segments are not lost. One recognised by content was
    // renamed in the namespace, and its file follows the new name.
    pub fn bind_recoveries(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let sets = tree.recovery_nodes(node);
        let mut moved = follow_founded(tree, node, sets);
        if let Some((parent, _)) = tree.volumes(node) {
            let sets = tree.recovery_nodes(parent);
            moved.extend(follow_founded(tree, parent, sets));
        }
        for into in moved.into_iter().filter(|into| *into != node) {
            self.bind_recoveries(tree, builder, segments, into, out)?;
        }
        let sets = tree.recovery_nodes(node);
        self.bind_sets(tree, builder, segments, node, &sets, out)
    }

    // Let `sets`, bound in the namespace of `node`, match its members.
    fn bind_sets(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        sets: &[NodeId],
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if sets.is_empty() {
            return Ok(());
        }
        let members: Vec<MemberId> = tree
            .namespace(node)
            .map(|namespace| namespace.members().map(|member| member.id).collect())
            .unwrap_or_default();
        let mut renamed = std::mem::take(&mut self.renamed);
        renamed.clear();
        for &set in sets {
            self.answer_evidence(tree, segments, node, sets, set);
            let newly = tree
                .with_recovery(set, |recovery, namespace| {
                    // A member covered before the bind is not newly
                    // covered by it, so only the others' names before the
                    // bind are kept.
                    let uncovered: Vec<(MemberId, String)> = members
                        .iter()
                        .filter(|member| !recovery.covers(**member))
                        .map(|member| {
                            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                            NAMES_KEPT.with(|kept| kept.set(kept.get() + 1));
                            let name = namespace
                                .get(*member)
                                .map(|record| record.name.clone())
                                .unwrap_or_default();
                            (*member, name)
                        })
                        .collect();
                    let earlier = renamed.len();
                    recovery.bind(namespace, &mut renamed);
                    let by_content = &renamed[earlier..];
                    uncovered
                        .into_iter()
                        .filter(|(member, _)| recovery.covers(*member))
                        .map(|(member, name)| {
                            let described = namespace
                                .get(member)
                                .map(|record| record.name.clone())
                                .unwrap_or_default();
                            let by = if by_content.contains(&member) && described != name {
                                "content"
                            } else {
                                "name"
                            };
                            (member, name, described, by)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let kind = tree
                .kind(set)
                .map(|kind| kind.to_string())
                .unwrap_or_default();
            let name = self.set_name(tree, set).unwrap_or_default().to_owned();
            for (member, was, described, by) in newly {
                tracing::info!(
                    job_id = self.scope,
                    node = set.0,
                    kind = %kind,
                    set = %name,
                    member = %was,
                    r#as = %described,
                    by = %by,
                    "recovery bound"
                );
                self.early_evidence(tree, segments, node, member);
                let evidence = &self.segments;
                if !evidence.is_empty() {
                    tree.with_recovery(set, |recovery, _| recovery.feed(member, evidence));
                }
            }
            self.feed_moved(tree, segments, node, set);
        }
        // A member a set renamed in its namespace wants that name now; the
        // tree's ledger places it, and the claims the name makes again,
        // through the tree, bind as any claim does.
        let mut effects = Vec::new();
        for member in &renamed {
            let wanted = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(*member))
                .map(|record| record.name.clone());
            if let Some(wanted) = wanted {
                tree.rename(
                    node,
                    *member,
                    &wanted,
                    &mut self.placing(builder),
                    &mut effects,
                );
            }
        }
        let (moves, mut claims): (Vec<Effect>, Vec<Effect>) = effects
            .into_iter()
            .partition(|effect| matches!(effect, Effect::Renamed { .. }));
        self.moved(tree, &moves)?;
        // A member a set recognised or settled at another length is the
        // description's length: the object it is a volume of hears its
        // close again at it, and its file, if it has one, takes it.
        for member in &renamed {
            tree.closed_at(node, *member, &mut self.placing(builder), &mut claims);
        }
        for member in &renamed {
            let length = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(*member))
                .and_then(|record| record.length.filter(|_| record.is_complete()));
            let has_file = self
                .files
                .get(&node)
                .is_some_and(|files| files.leaves.contains_key(member));
            if let Some(length) = length.filter(|_| has_file) {
                self.admit_file(tree, node, *member, length, 0)?;
                let keeps = self.keeps_changes(node, *member);
                if let Some(leaf) = self
                    .files
                    .get_mut(&node)
                    .and_then(|files| files.leaves.get_mut(member))
                {
                    leaf.close(LoopSite::BindSets, length, keeps)?;
                }
            }
        }
        self.renamed = renamed;
        self.apply_each(
            tree,
            builder,
            segments,
            claims,
            Write::Again(LoopSite::BindSets),
            out,
        )
    }

    // An object found its own recovery data at a range of its volume. The
    // set for it is built over that range of the member the volume is, as a
    // window beside the object, so it lives in the namespace of the members
    // it covers, and takes what the object handed it. What of the range
    // has passed is read back through the inverse map and handed to it;
    // the rest reaches it as it passes.
    pub(super) fn recovery_at(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, volume, range): (NodeId, VolumeIndex, ByteRange),
        (kind, handed): (RecoveryKind, &Handed),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Some((parent, member)) = tree
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return Ok(());
        };
        let object = kind.object();
        let runs: Vec<(ByteRange, Provenance)> = tree
            .namespace(parent)
            .and_then(|namespace| namespace.get(member))
            .map(|record| {
                record
                    .coverage
                    .overlapping(range)
                    .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                    .collect()
            })
            .unwrap_or_default();
        let access = self.access(tree, LoopSite::RecoveryAt);
        let mut passed = Vec::new();
        for (piece, provenance) in runs {
            let mut bytes = Vec::new();
            if access.read_at(parent, member, piece, &mut bytes) {
                passed.push((piece.start, Bytes::from(bytes), provenance));
            }
        }
        let mut effects = Vec::new();
        let window = tree.attach_window(
            parent,
            (member, range),
            (object, handed),
            passed,
            &mut self.placing(builder),
            &mut effects,
        );
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::RecoveryAt),
            out,
        )?;
        if let Some(window) = window {
            // The window is the one set of its namespace not yet bound: the
            // others bound when its members last changed, and a window
            // changes none of them. It binds alone, where it covers, so each
            // window attached costs one bind however many came before it.
            follow_founded(tree, parent, vec![window]);
            let home = tree.home(window).unwrap_or(parent);
            self.bind_sets(tree, builder, segments, home, &[window], out)?;
        }
        Ok(())
    }
}

// What each of `sets` that has yet to rule, or is out, covers of the
// namespace's members.
fn pending_covers(
    tree: &Tree,
    namespace: &weaver_graph::Namespace,
    sets: &[NodeId],
) -> Vec<(NodeId, BTreeSet<MemberId>)> {
    sets.iter()
        .filter(|&&set| !aside(tree, set))
        .filter_map(|&set| {
            let covers: BTreeSet<MemberId> = match tree.recovery(set) {
                Some(recovery) if recovery.outcome().is_none() => namespace
                    .members()
                    .map(|member| member.id)
                    .filter(|member| recovery.covers(*member))
                    .collect(),
                None if tree.lent(set) => namespace
                    .members()
                    .map(|member| member.id)
                    .filter(|member| tree.lent_covers(set, *member))
                    .collect(),
                _ => return None,
            };
            Some((set, covers))
        })
        .collect()
}

// A set that only checks, as an SFV listing does, always stands aside: it
// binds, and may name what it lists, but it is never fed, never repairs,
// never rules, vouches or proves, and never waits a job. Each set says for
// itself whether it rebuilds.
fn aside(tree: &Tree, set: NodeId) -> bool {
    !tree.rebuilds(set)
}

// How many of the set's own volumes are closed.
fn closed_volumes(tree: &Tree, set: NodeId) -> usize {
    tree.volumes(set).map_or(0, |(parent, volumes)| {
        volumes
            .values()
            .filter(|member| closed_in(tree, parent, **member))
            .count()
    })
}

// The namespace has announced every member it will get: the members that
// are its object's volumes are closed and its source is not still reading.
// The root's members are the post's, known from the start.
fn members_known(tree: &Tree, node: NodeId, reading: bool) -> bool {
    let volumes_in = tree.volumes(node).is_none_or(|(parent, volumes)| {
        volumes
            .values()
            .all(|member| closed_in(tree, parent, *member))
    });
    volumes_in && !reading
}

fn closed_in(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    tree.namespace(node)
        .and_then(|namespace| namespace.get(member))
        .is_some_and(|record| record.closed)
}

// Still open, though every byte of its stated length is in and none is
// damaged: whole by what a set proved, not yet by its close.
fn proven_open(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    tree.namespace(node)
        .and_then(|namespace| namespace.get(member))
        .is_some_and(|record| {
            !record.closed
                && record.is_complete()
                && record
                    .length
                    .is_some_and(|length| !record.coverage.damaged(ByteRange::new(0, length)))
        })
}

// Whether the recovery object covers some member of the namespace.
fn covers_any(tree: &Tree, node: NodeId, recovery: &dyn Recovery) -> bool {
    tree.namespace(node)
        .is_some_and(|namespace| namespace.members().any(|member| recovery.covers(member.id)))
}

// A recovery object is bound in the namespace of the member it names. One
// that covers nothing where it is bound and names a member founded in a
// namespace bound there moves into that namespace, where it binds, rules
// and repairs; a set made over the posted files stays with them. Of `sets`,
// bound in `node`'s namespace, the namespaces they moved into.
fn follow_founded(tree: &mut Tree, node: NodeId, sets: Vec<NodeId>) -> Vec<NodeId> {
    let mut moved = Vec::new();
    for set in sets {
        let Some(recovery) = tree.recovery(set) else {
            continue;
        };
        if covers_any(tree, node, recovery) {
            continue;
        }
        let mut named = Vec::new();
        recovery.absent(&mut named);
        if named.is_empty() {
            continue;
        }
        let named = folded(&named);
        let mut name = String::new();
        let founded = tree.namespaces_below(node).into_iter().find(|below| {
            tree.namespace(*below).is_some_and(|namespace| {
                namespace
                    .members()
                    .any(|member| names(&named, &member.name, &mut name))
            })
        });
        if let Some(into) = founded {
            tree.move_recovery(set, into);
            moved.push(into);
        }
    }
    moved
}

// Leave out of `absent` every name some posting has.
pub fn unposted<'a>(absent: &mut Vec<(String, u64)>, posted: impl Iterator<Item = &'a str>) {
    if absent.is_empty() {
        return;
    }
    let posted: HashSet<&str> = posted.collect();
    absent.retain(|(name, _)| !posted.contains(name.as_str()));
}

// The names, each in ASCII lower case, so a name is looked up regardless
// of ASCII case.
pub fn folded(named: &[(String, u64)]) -> HashSet<String> {
    named
        .iter()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect()
}

// Whether `name` is one of the folded names regardless of ASCII case,
// folded into `buffer`.
pub fn names(folded: &HashSet<String>, name: &str, buffer: &mut String) -> bool {
    buffer.clear();
    buffer.push_str(name);
    buffer.make_ascii_lowercase();
    folded.contains(buffer.as_str())
}

// Where, inside `range`, a block of any of `sizes` begins: the cuts a
// piece of evidence has to be split at for a set that folds blocks of that
// size to take it whole. Each cut once, in order. The smallest size that
// cuts the range leads; a size it divides cuts nowhere new, and every other
// size merges in by its next cut, so nothing is kept but the cuts made.
pub fn block_cuts(range: ByteRange, sizes: &mut [u64]) -> Vec<u64> {
    let first = |size: u64| {
        (range.start / size + 1)
            .checked_mul(size)
            .filter(|at| *at < range.end)
    };
    let next = |at: u64, size: u64| at.checked_add(size).filter(|at| *at < range.end);
    sizes.sort_unstable();
    let mut sizes = sizes.iter().copied().filter(|size| *size > 0);
    let Some((lead, at)) = sizes.by_ref().find_map(|size| Some((size, first(size)?))) else {
        return Vec::new();
    };
    let mut rest = BinaryHeap::new();
    let mut last = lead;
    for size in sizes {
        if size == last || size % lead == 0 {
            continue;
        }
        last = size;
        if let Some(at) = first(size) {
            rest.push(Reverse((at, size)));
        }
    }
    let mut cuts = Vec::with_capacity(index((range.end - 1) / lead - range.start / lead));
    let mut ahead = Some(at);
    while let Some(cut) = ahead
        .into_iter()
        .chain(rest.peek().map(|Reverse((at, _))| *at))
        .min()
    {
        cuts.push(cut);
        if ahead == Some(cut) {
            ahead = next(cut, lead);
        }
        while let Some(mut head) = rest.peek_mut()
            && head.0.0 == cut
        {
            match next(cut, head.0.1) {
                Some(at) => head.0.0 = at,
                None => {
                    PeekMut::pop(head);
                }
            }
        }
    }
    cuts
}
