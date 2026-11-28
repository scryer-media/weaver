// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree's nodes, and the members each gave up, under one generation:
// every way to change a node marks it, so a snapshot asks again only what
// was marked since the generation it took last. Nothing outside reaches a
// node to change it but through here. A mark is as fine as the rows a
// snapshot keeps: a node's own row (its object, binding and state) is marked
// apart from its members, and of its members only which namespace moved is
// marked here; the namespace says which of its members did, and a member's
// owner, whether it waits on one, and what it was given up as are marked
// there too.
// Beside them, unmarked, how each node's run goes: a run's progress is the
// tree's, read live and kept in no record, so a snapshot never asks for it
// and a rebuilt tree hears it again from the runs it starts.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{Arrived, MembersMut, Node, NodeId, Waiting};
use crate::deliver::Why;
use crate::marks::Marks;
use crate::member::{MemberId, Namespace};
use crate::object::Object;
use crate::volume::VolumeIndex;

// What a change touched of a node: its own row, or its members, which its
// namespace names one by one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Mark {
    Node(NodeId),
    Members(NodeId),
}

// What a member given up was to the retired object that gave it up: its
// node, and which of its volumes; `None` once the owner buried it.
pub(super) type GivenUp = Option<(NodeId, VolumeIndex)>;

// One tree among those a process builds: a cursor taken on one says
// nothing of another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeId(u64);

impl Default for TreeId {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

// Where an owner last read what may have become prunable. The default
// names no tree, so the first read is of everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneCursor {
    tree: Option<TreeId>,
    nodes: u64,
    members: u64,
}

// What may have become prunable since a cursor: every node at or below
// one marked, and each member marked. `all` when the cursor is another
// tree's, and everything is to be asked.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Prunable {
    pub all: bool,
    pub nodes: BTreeSet<NodeId>,
    pub members: BTreeSet<(NodeId, MemberId)>,
}

// How far a node's run has gone: the furthest volume it reached, whether
// it asked for a password that no offer, and no run started since, has
// answered, and whether it said bytes of its volumes were damaged that a
// recovery object above had proved whole.
#[derive(Clone, Copy, Default)]
pub(super) struct Progress {
    pub(super) reached: Option<VolumeIndex>,
    pub(super) asking: bool,
    pub(super) refuted: bool,
}

#[derive(Default)]
pub(super) struct Nodes {
    nodes: BTreeMap<NodeId, Node>,
    marks: Marks<Mark>,
    // Members given up by a retired object, and what they were to it, until
    // the owner buries them. The entry outlives the burial, emptied,
    // because a member an object gave up is settled: no object bound later
    // is offered it.
    retired: BTreeMap<(NodeId, MemberId), GivenUp>,
    // The refused nodes, each under the node it is bound in. Filed where a
    // node comes, goes, or is refused, the only paths that change it.
    refused: BTreeSet<(NodeId, NodeId)>,
    // Each node's run progress, from its first report until the node goes.
    progress: BTreeMap<NodeId, Progress>,
    // How many nodes have gone, so a caller tells in one read whether a
    // step removed any. Never recorded: it is compared within one step.
    removed: u64,
    // The nodes whose progress was handed out to change since the wants
    // last read them: the volume a run reached weighs its engine's bids.
    // Taken each round, so it holds at most one entry per node.
    progressed: BTreeSet<NodeId>,
    // How many members given up still name each retired node as the one
    // they were given up by: whether something reads back through a node,
    // in one lookup. Kept by every path that changes `retired`.
    given_up_by: BTreeMap<NodeId, usize>,
    // Each bound node under the node it is bound in, filed where a node
    // comes or goes: a binding's parent never changes. A node's entries
    // outlive it until each node below it goes, so what stood below a node
    // that went is still found.
    below: BTreeSet<(NodeId, NodeId)>,
    // What an owner that forgets what the tree dropped asks again: the
    // nodes below which whether a node is buried may have changed (one that
    // went, retired, or is named by a member given up or no longer), and
    // the members that may have gone or reopened, or whose giving up
    // changed. Each under the generation of its latest change.
    prunable: Marks<NodeId>,
    prunable_members: Marks<(NodeId, MemberId)>,
    // Which tree this is, for an owner's cursor into the marks above.
    id: TreeId,
    // For the check of the kept wants: the generation each object stepped out at,
    // and the nodes whose object stepped back in with no mark of its row
    // since.
    #[cfg(any(test, feature = "test-support"))]
    out_at: BTreeMap<NodeId, u64>,
    #[cfg(any(test, feature = "test-support"))]
    unmarked_moves: BTreeSet<NodeId>,
}

impl Deref for Nodes {
    type Target = BTreeMap<NodeId, Node>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl Nodes {
    // The whole node, to change: its own row is marked, and its namespace
    // is asked again for what it marked.
    pub(super) fn get_mut(&mut self, node: &NodeId) -> Option<&mut Node> {
        let found = self.nodes.get_mut(node)?;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        Some(found)
    }
    // Only the node's namespace, to change: its own row is not marked, and
    // the namespace marks each member it changes.
    pub(super) fn namespace_mut(&mut self, node: NodeId) -> Option<&mut Namespace> {
        let found = self.nodes.get_mut(&node)?;
        self.marks.mark(Mark::Members(node));
        Some(&mut found.namespace)
    }

    // Only the node's members, to change: its namespace and what it keeps
    // of each member beside it. Its own row is not marked, and each member
    // changed marks itself.
    pub(super) fn members_mut(&mut self, node: NodeId) -> Option<MembersMut<'_>> {
        let found = self.nodes.get_mut(&node)?;
        self.marks.mark(Mark::Members(node));
        Some(found.members_mut())
    }

    // Only the node's namespace, to change: each member changed marks
    // itself there, and the node is marked for its members only when one
    // did.
    pub(super) fn with_namespace<R>(
        &mut self,
        node: NodeId,
        change: impl FnOnce(&mut Namespace) -> R,
    ) -> Option<R> {
        let found = self.nodes.get_mut(&node)?;
        let at = found.namespace.generation();
        let result = change(&mut found.namespace);
        if found.namespace.generation() != at {
            self.marks.mark(Mark::Members(node));
        }
        Some(result)
    }

    // The node's object steps out to run beside the rest of the tree, and
    // back in: the move changes nothing of the node, so it marks nothing.
    pub(super) fn object_out(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        #[cfg(any(test, feature = "test-support"))]
        self.out_at.insert(node, self.marks.at());
        self.nodes.get_mut(&node)?.object.take()
    }

    pub(super) fn object_in(&mut self, node: NodeId, object: Box<dyn Object>) {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(at) = self.out_at.remove(&node)
            && !self.marks.since(at).any(|mark| mark == Mark::Node(node))
        {
            self.unmarked_moves.insert(node);
        }
        if let Some(found) = self.nodes.get_mut(&node) {
            found.object = Some(object);
        }
    }

    // The nodes whose object stepped out and back with no mark of its row,
    // since the last time they were taken.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn take_unmarked_moves(&mut self) -> BTreeSet<NodeId> {
        std::mem::take(&mut self.unmarked_moves)
    }

    // A delivery waits on a lent object without changing the facts it left.
    pub(super) fn queue_lent_delivery(
        &mut self,
        node: NodeId,
        arrived: &Arrived,
        length: Option<u64>,
    ) -> bool {
        let Some(lent) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.lent.as_mut())
        else {
            return false;
        };
        let arrived = Arrived {
            bytes: arrived.bytes.clone(),
            ..*arrived
        };
        lent.wait(Waiting::Delivered { arrived, length });
        true
    }

    // The node's own row changed: what its object knows.
    pub(super) fn mark_own(&mut self, node: NodeId) {
        if self.nodes.contains_key(&node) {
            self.marks.mark(Mark::Node(node));
        }
    }

    // A member's rows changed by what the tree keeps of it beside its
    // namespace.
    pub(super) fn mark_member(&mut self, node: NodeId, member: MemberId) {
        if let Some(namespace) = self.namespace_mut(node) {
            namespace.mark(member);
        }
    }

    pub(super) fn progress(&self, node: NodeId) -> Progress {
        self.progress.get(&node).copied().unwrap_or_default()
    }

    // The progress of a node's run, to change: it marks nothing, and only
    // rings for the node's wants, which the volume it reached weighs.
    pub(super) fn progress_mut(&mut self, node: NodeId) -> Option<&mut Progress> {
        if !self.nodes.contains_key(&node) {
            return None;
        }
        self.progressed.insert(node);
        Some(self.progress.entry(node).or_default())
    }

    // The nodes whose progress was handed out to change since the last time
    // they were taken.
    pub(super) fn take_progressed(&mut self) -> BTreeSet<NodeId> {
        std::mem::take(&mut self.progressed)
    }

    pub(super) fn insert(&mut self, node: NodeId, found: Node) {
        self.progress.remove(&node);
        if let (Some(_), Some(binding)) = (found.refused, &found.binding) {
            self.refused.insert((binding.parent, node));
        }
        if let Some(binding) = &found.binding {
            self.below.insert((binding.parent, node));
        }
        self.marks.mark(Mark::Node(node));
        self.marks.mark(Mark::Members(node));
        self.nodes.insert(node, found);
    }

    // The object at `node` refused its volumes, for `why`.
    pub(super) fn refuse(&mut self, node: &NodeId, why: Why) -> Option<&mut Node> {
        let found = self.nodes.get_mut(node)?;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        found.refused = Some(why);
        if let Some(binding) = &found.binding {
            self.refused.insert((binding.parent, *node));
        }
        Some(found)
    }

    // The refused nodes bound in `parent`'s namespace, in node order.
    pub(super) fn refused_below(&self, parent: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.refused
            .range((parent, NodeId(0))..=(parent, NodeId(u32::MAX)))
            .map(|(_, node)| *node)
    }

    // A node goes, and what it gave up with it.
    pub(super) fn remove(&mut self, node: &NodeId) -> Option<Node> {
        let removed = self.nodes.remove(node)?;
        self.removed += 1;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        self.progress.remove(node);
        self.progressed.remove(node);
        let given: Vec<(NodeId, MemberId)> = self
            .retired
            .range((*node, MemberId(0))..=(*node, MemberId(u32::MAX)))
            .map(|(at, _)| *at)
            .collect();
        for at in given {
            if let Some(was) = self.retired.remove(&at) {
                self.given_up_changed(was, None);
            }
        }
        if let Some(binding) = &removed.binding {
            self.refused.remove(&(binding.parent, *node));
            self.below.remove(&(binding.parent, *node));
        }
        self.prunable.mark(*node);
        Some(removed)
    }

    pub(super) fn retired(&self) -> &BTreeMap<(NodeId, MemberId), GivenUp> {
        &self.retired
    }

    // How many members given up still name `node` as the one they were
    // given up by.
    pub(super) fn given_up_by(&self, node: NodeId) -> usize {
        let counted = self.given_up_by.get(&node).copied().unwrap_or(0);
        #[cfg(any(test, feature = "test-support"))]
        assert_eq!(
            counted,
            self.retired
                .values()
                .flatten()
                .filter(|(other, _)| *other == node)
                .count(),
            "members given up by {node:?}, counted and scanned"
        );
        counted
    }

    // A member given up named `was` and now `now`: the count of each moves,
    // and whether a node below either is buried may have changed.
    fn given_up_changed(&mut self, was: GivenUp, now: GivenUp) {
        if was.map(|(node, _)| node) == now.map(|(node, _)| node) {
            return;
        }
        if let Some((node, _)) = was {
            if let Some(count) = self.given_up_by.get_mut(&node) {
                *count -= 1;
                if *count == 0 {
                    self.given_up_by.remove(&node);
                }
            }
            self.prunable.mark(node);
        }
        if let Some((node, _)) = now {
            *self.given_up_by.entry(node).or_default() += 1;
            self.prunable.mark(node);
        }
    }

    pub(super) fn retire(&mut self, at: (NodeId, MemberId), given: GivenUp) {
        self.mark_member(at.0, at.1);
        self.prunable_members.mark(at);
        let was = self.retired.insert(at, given).flatten();
        self.given_up_changed(was, given);
        self.given_up_kept(at, true);
    }

    // The node keeps beside it which of its members are given up, for the
    // members no object owns it keeps (`Node::loose`).
    fn given_up_kept(&mut self, (node, member): (NodeId, MemberId), given: bool) {
        if let Some(found) = self.nodes.get_mut(&node) {
            if given {
                found.given_up.insert(member);
            } else {
                found.given_up.remove(&member);
            }
            found.refront(member);
        }
    }

    // The member is given up by nothing. Asked again whether it stands,
    // whether or not it was given up: each caller has just changed it.
    pub(super) fn unretire(&mut self, at: &(NodeId, MemberId)) {
        self.prunable_members.mark(*at);
        if let Some(was) = self.retired.remove(at) {
            self.mark_member(at.0, at.1);
            self.given_up_changed(was, None);
            self.given_up_kept(*at, false);
        }
    }

    // The owner buried the member given up: its entry stays, emptied, and
    // what it named is returned.
    pub(super) fn bury_given(&mut self, at: &(NodeId, MemberId)) -> Option<(NodeId, VolumeIndex)> {
        let was = self.retired.get_mut(at)?.take();
        self.mark_member(at.0, at.1);
        self.prunable_members.mark(*at);
        self.given_up_changed(was, None);
        was
    }

    // The node's object gave its volumes up, or one above it did.
    pub(super) fn set_retired(&mut self, node: NodeId) {
        if let Some(found) = self.get_mut(&node) {
            found.retired = true;
            self.prunable.mark(node);
        }
    }

    // A member that may have gone or reopened by a change its namespace
    // made, asked again by an owner that forgets what the tree dropped.
    pub(super) fn mark_prunable_member(&mut self, node: NodeId, member: MemberId) {
        self.prunable_members.mark((node, member));
    }

    pub(super) fn prune_cursor(&self) -> PruneCursor {
        PruneCursor {
            tree: Some(self.id),
            nodes: self.prunable.at(),
            members: self.prunable_members.at(),
        }
    }

    // What may have become prunable since `cursor`: each node marked and
    // every node bound below one, and each member marked.
    pub(super) fn prunable_since(&self, cursor: &PruneCursor) -> Prunable {
        if cursor.tree != Some(self.id) {
            return Prunable {
                all: true,
                ..Prunable::default()
            };
        }
        let mut nodes = BTreeSet::new();
        let mut pending: Vec<NodeId> = self.prunable.since(cursor.nodes).collect();
        while let Some(node) = pending.pop() {
            if nodes.insert(node) {
                pending.extend(
                    self.below
                        .range((node, NodeId(0))..=(node, NodeId(u32::MAX)))
                        .map(|(_, child)| *child),
                );
            }
        }
        Prunable {
            all: false,
            nodes,
            members: self.prunable_members.since(cursor.members).collect(),
        }
    }

    pub(super) fn removed(&self) -> u64 {
        self.removed
    }

    pub(super) fn generation(&self) -> u64 {
        self.marks.at()
    }

    pub(super) fn changed_since(&self, at: u64) -> impl Iterator<Item = Mark> + '_ {
        self.marks.since(at)
    }
}
