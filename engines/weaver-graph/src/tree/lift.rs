// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume lifted out of its namespace. A set whose volumes are each packed
// alone in a container of their own, as a scene release puts each RAR
// volume in a zip, has no volume beside another: each set below gives its
// volume up as scattered (`Why::Scattered`), and once the volume is in its
// own file, closed, it is announced again one namespace up, in the one
// that holds the container it came out of. There the scattered volumes are
// members side by side, and the objects that group them, read them and
// unpack them run unchanged. The lifted member is carried as a member given
// back is: its bytes in its own file, offered again, adopted and read in
// place. It keeps the position it had below for its name, so the names
// rank as they did before it moved; and the namespace it sits in is still
// as deep as the one it came out of, for the nesting limit.
//
// The member it was below stays where it was, a member its container may
// still say again: it is buried there, holds no name and is no file, and
// what reaches it goes to the member it is now.

use super::{Builder, Effect, NodeId, Tree};
use crate::deliver::{Provenance, Why};
use crate::member::MemberId;
use crate::names::Position;
use crate::object::Clean;

// Where a lifted member came from, and the position its name ranks at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lift {
    pub from: (NodeId, MemberId),
    pub position: Position,
}

impl Tree {
    // A refused member, read back into its own file: a volume of a set
    // scattered across containers is lifted (`lift_scattered`); any other
    // may be the first piece of a file, and is joined with its pieces
    // (`refused_whole`).
    pub fn refused_written(
        &mut self,
        node: NodeId,
        member: MemberId,
        why: Why,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let set = self.refused_by(node, member);
        match set
            .and_then(|set| self.nodes.get(&set))
            .and_then(|found| found.refused)
        {
            Some(Why::Scattered) => self.lift_scattered(node, member, set, builder, effects),
            // A member refused as not its object's to open is the posted
            // file, whole, and never a piece of another.
            _ if why.disclaims() => {}
            _ => self.refused_whole(node, member, builder, effects),
        }
    }

    // The member a set below gave up as scattered, read back into its own
    // file and closed, is lifted into the namespace that holds what its
    // namespace came out of, and claimed there as its name and front say.
    // A member of the root's namespace has no namespace above it and stays
    // as it is, refused with its set, as a set short of volumes is.
    fn lift_scattered(
        &mut self,
        node: NodeId,
        member: MemberId,
        set: Option<NodeId>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if self.member_home((node, member)) != (node, member)
            || self.child_of(node, member).is_some()
        {
            return;
        }
        let Some(set) = set else {
            return;
        };
        let Some(parent) = self
            .nodes
            .get(&node)
            .and_then(|found| found.binding.as_ref())
            .map(|binding| binding.parent)
        else {
            return;
        };
        let Some(record) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .filter(|record| record.closed)
            .cloned()
        else {
            return;
        };
        let front = self.front(node, member).to_vec();
        let position = self.position(node, member);
        let Some(namespace) = self.nodes.namespace_mut(parent) else {
            return;
        };
        let lifted = namespace.announce(&record.name, record.length);
        namespace.keep_first_bytes(lifted, 0, &front, Provenance::Fresh);
        if let Some(into) = namespace.get_mut(lifted) {
            into.coverage = record.coverage.clone();
            into.closed = true;
            into.crc32 = record.crc32;
            into.fronted = record.fronted;
        }
        self.lifted.insert(
            (parent, lifted),
            Lift {
                from: (node, member),
                position: position.clone(),
            },
        );
        self.lifted_away.insert((node, member), (parent, lifted));
        self.nodes.mark_member(parent, lifted);
        // The name moves with the member: it ranks where the member below
        // ranked, ahead of it, and the member below then lets it go.
        let mut moved = Vec::new();
        let given = self
            .names
            .allocate((parent, lifted), position, &record.name, &mut moved);
        moved.retain(|moved| moved.holder != (node, member));
        self.names.release((node, member), &mut moved);
        if let Some(into) = self
            .namespace_mut(parent)
            .and_then(|namespace| namespace.get_mut(lifted))
        {
            into.output_name = given;
        }
        self.report_moves(moved, effects);
        effects.push(Effect::Lifted {
            node,
            member,
            to: parent,
            lifted,
        });
        // The set below holds the volume no more; with none left, it goes.
        if let Some(binding) = self
            .nodes
            .get_mut(&set)
            .and_then(|found| found.binding.as_mut())
        {
            let volume = binding
                .volumes
                .iter()
                .find(|(_, held)| **held == member)
                .map(|(volume, _)| *volume);
            if let Some(volume) = volume {
                binding.volumes.remove(&volume);
                binding.awaiting.remove(&volume);
            }
            let empty = binding.volumes.is_empty();
            self.forget_empty(set, empty, effects);
        }
        // Offered first to every object there, as a member arriving is:
        // its set may already be read by an object of another kind, as a
        // RAR set whose member is compressed is by its engine.
        let siblings = self.bound_below(parent);
        if self.offer(
            parent,
            lifted,
            Clean::start(&front),
            &siblings,
            None,
            effects,
        ) {
            return;
        }
        self.claim_as_said(parent, lifted, builder, effects);
    }

    // How many members were lifted into the namespace they sit in.
    pub fn lifts(&self) -> usize {
        self.lifted.len()
    }

    // Where the member lives now: the member it became, through every lift
    // it took, or the member itself when it never moved. The one answer to
    // where a member's bytes are, asked once where they enter: an arrival,
    // a read, a loan. The member it was stays in the namespace below, since
    // the object there still places its bytes under that id and may say it
    // again; it holds no name and is no file (`holds_no_file`).
    pub fn member_home(&self, mut at: (NodeId, MemberId)) -> (NodeId, MemberId) {
        while let Some(to) = self.lifted_away.get(&at) {
            at = *to;
        }
        at
    }

    // Where the member's bytes were posted: the member it was below,
    // through every lift it took, or the member itself. A seek for it goes
    // up from there.
    pub(super) fn came_from(&self, mut at: (NodeId, MemberId)) -> (NodeId, MemberId) {
        while let Some(lift) = self.lifted.get(&at) {
            at = lift.from;
        }
        at
    }

    // What a member lifted into `node`'s namespace came from, and the
    // position its name ranks at.
    pub fn lift_of(&self, node: NodeId, member: MemberId) -> Option<&Lift> {
        self.lifted.get(&(node, member))
    }

    // How deeply nested the member's bytes are: the depth of the namespace
    // they were posted in, which for a lifted member is the one it first
    // came out of. The one notion of depth; a node's depth is its first
    // volume's plus one (`Tree::depth`).
    pub fn member_depth(&self, node: NodeId, member: MemberId) -> u32 {
        match self.lifted.get(&(node, member)) {
            Some(lift) => (lift.position.0.len() as u32).saturating_sub(1),
            None => self.depth(node),
        }
    }

    // A restored lifted member: where it came from is buried there, and
    // reaches it here.
    pub(super) fn restore_lift(&mut self, at: (NodeId, MemberId), lift: Lift) {
        if self
            .namespace(lift.from.0)
            .is_some_and(|namespace| namespace.get(lift.from.1).is_some())
        {
            self.lifted_away.insert(lift.from, at);
        }
        self.lifted.insert(at, lift);
    }

    // A node leaves: the lifts into its namespace go, and so does the way
    // to them from below; a member lifted out of it is lifted still.
    pub(super) fn forget_lifts(&mut self, node: NodeId) {
        let range = (node, MemberId(0))..=(node, MemberId(u32::MAX));
        let into: Vec<(NodeId, MemberId)> = self
            .lifted
            .range(range.clone())
            .map(|(at, _)| *at)
            .collect();
        for at in into {
            if let Some(lift) = self.lifted.remove(&at) {
                self.lifted_away.remove(&lift.from);
            }
        }
        let away: Vec<(NodeId, MemberId)> =
            self.lifted_away.range(range).map(|(at, _)| *at).collect();
        for at in away {
            self.lifted_away.remove(&at);
        }
    }
}
