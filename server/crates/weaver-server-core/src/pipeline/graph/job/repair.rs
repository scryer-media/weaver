// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's walk over its namespaces. Damage the source reports is handed to
// the root namespace, which marks it down the tree to every member it lands
// in. One walk over the namespaces, parents before children, asks each to
// rule its recovery sets, so a repair above is in place before anything
// below rules on bytes it rewrote; what a set covers, when it rules and what
// its repair writes are the namespace's. Once nothing more can change, every
// data object, deepest first, hears the lost ranges nothing vouched for and
// that the job is quiet, and an object that cannot finish refuses itself for
// its own reason. The server tells; it chooses no reason.

use std::collections::BTreeSet;

use weaver_graph::slots::Ticket;
use weaver_graph::{MemberId, NodeId, VolumeIndex};

use super::{GraphError, GraphJob};
use crate::pipeline::graph::namespace::{Lender, WriteError};
use crate::pipeline::graph::status::RepairSlots;

impl GraphJob {
    // Walk the namespaces without asking for a slot: a job just restored
    // holds none, and lends what is due once the pipeline has it.
    pub(super) fn repair(&mut self) -> Result<(), GraphError> {
        self.walk(None)
    }

    // Walk the namespaces and lend what is due, asking `slots` for a slot
    // when the job holds none. The slot goes back once nothing is out. A
    // step lends after its runs are stepped, so `reading` counts the runs
    // the step started: a namespace one of them is about to read may still
    // announce members, and no set over it is due yet.
    pub(crate) fn lend(&mut self, slots: &mut RepairSlots, limit: usize) -> Result<(), GraphError> {
        let walked = self.walk(Some((&mut *slots, limit)));
        if let Some(ticket) = self.loans.give_back() {
            slots.release(ticket);
        }
        walked
    }

    // A retiring job releases an idle slot. An outstanding repair keeps
    // its loan and ticket until its matching completion acknowledgment.
    pub(crate) fn forget_slot(&mut self) -> Option<Ticket> {
        self.loans.give_back()
    }

    // Walk the namespaces that have sets in node order, which puts every
    // parent before its children, and ask each to rule them: a namespace
    // with none has nothing to rule or announce. A set the walk's own
    // repairs bring into being is walked too. A replay or a checksum check
    // waiting goes out once no repair took the job's one loan: what either
    // changes, applied, takes back the rulings it touches (an uncovered
    // input), and the set rules again through the same walk.
    fn walk(&mut self, mut slots: Option<(&mut RepairSlots, usize)>) -> Result<(), GraphError> {
        let mut walked = None;
        while let Some(node) = self.tree.next_recovery_home(walked) {
            walked = Some(node);
            if !self.tree.is_live(node) {
                continue;
            }
            #[cfg(test)]
            tests::RULED.with_borrow_mut(|ruled| ruled.push(node));
            let runs = &self.runs;
            let mut lending = self
                .loans
                .lending(slots.as_mut().map(|(slots, limit)| (&mut **slots, *limit)));
            let mut out = Vec::new();
            self.writer.rule(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                (node, &|namespace| runs.reading(namespace)),
                &mut lending,
                &mut out,
            )?;
            self.heard(out)?;
        }
        self.seal_proven();
        // An ended namespace's obligations nothing can check end here.
        self.writer
            .drop_unmet_verifications(&self.tree, &self.coming());
        if !self.writer.rechecks_waiting() && !self.writer.verifications_waiting() {
            return Ok(());
        }
        let mut lending = self
            .loans
            .lending(slots.as_mut().map(|(slots, limit)| (&mut **slots, *limit)));
        if !lending.admit() {
            return Ok(());
        }
        if let Some(replay) = self
            .writer
            .take_replay(&self.tree)
            .map_err(WriteError::from)?
        {
            lending.replay(replay);
        } else if let Some(check) = self.writer.take_verification(&self.tree) {
            lending.verify(check);
        }
        Ok(())
    }

    // Every root member the walk's rulings proved whole has its decoder
    // checksums dropped: no set reads them again. A member is proved by its
    // own record being whole and by every root set over it standing clean
    // or repaired, so a pass asks only what could have become proved since
    // the last: the members the root's namespace marked, and the members a
    // set covers when it came to stand clean or a volume of its own came
    // while it stood. A set that left the root may have been the one thing
    // keeping any member unproved, so every member is asked. A job's first
    // pass asks every member.
    fn seal_proven(&mut self) {
        let Some(namespace) = self.tree.namespace(NodeId::ROOT) else {
            return;
        };
        let tree = &self.tree;
        let writer = &self.writer;
        let sets = tree.recovery_nodes(NodeId::ROOT);
        let standing: BTreeSet<NodeId> = sets
            .iter()
            .copied()
            .filter(|set| writer.stands_proved(tree, *set))
            .collect();
        let mut asked: BTreeSet<MemberId> = BTreeSet::new();
        let every = || namespace.members().map(|member| member.id);
        let covered = |set: NodeId| every().filter(move |member| writer.covers(tree, set, *member));
        match &self.sealed_at {
            None => asked.extend(every()),
            Some(sealed) if sealed.sets.iter().any(|set| !sets.contains(set)) => {
                asked.extend(every());
            }
            Some(sealed) => {
                let mut grew: BTreeSet<NodeId> =
                    standing.difference(&sealed.standing).copied().collect();
                for member in namespace.changed_since(sealed.members) {
                    asked.insert(member);
                    if let Some((child, _)) = tree.child_of(NodeId::ROOT, member)
                        && standing.contains(&child)
                    {
                        grew.insert(child);
                    }
                }
                for set in grew {
                    asked.extend(covered(set));
                }
            }
        }
        let proven: Vec<MemberId> = asked
            .into_iter()
            .filter(|member| {
                namespace.get(*member).is_some() && namespace.copy_of(*member).is_none()
            })
            .filter(|member| {
                #[cfg(test)]
                tests::SEAL_ASKED.with_borrow_mut(|asked| asked.push(*member));
                writer.proven_whole(tree, (NodeId::ROOT, &sets), *member)
            })
            .collect();
        self.sealed_at = Some(Sealed {
            members: namespace.generation(),
            sets,
            standing,
        });
        for member in proven {
            self.root.seal(VolumeIndex(member.0));
        }
    }

    // Once nothing more can change, every data object hears the lost
    // ranges of its volumes that nothing vouched for, then that the job is
    // quiet, and rules for itself: the tree tells it and reads it what the
    // tree records of it, and the server applies what it says. Children
    // come first, so the deepest object the damage reached rules before any
    // object above it, and a parent never speaks for bytes lost below it.
    // What the losses set going is settled before the object hears quiet.
    // Asked only once every run is started, since a repair's bytes reach a
    // namespace below by its run reading them again. Returns whether
    // anything changed.
    pub(super) fn tell_quiet(&mut self) -> Result<bool, GraphError> {
        if !self.quiet() {
            return Ok(false);
        }
        let mut changed = false;
        for node in self.tree.live().into_iter().rev() {
            let mut effects = Vec::new();
            self.tree.tell_lost(
                node,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            changed |= !effects.is_empty();
            self.settle(effects)?;
            let mut effects = Vec::new();
            self.tree.quiet(
                node,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            changed |= !effects.is_empty();
            self.settle(effects)?;
        }
        Ok(changed)
    }

    // The payload is in, no run is reading, no repair is out, and every
    // namespace's sets have ruled on something other than waiting, none
    // with a volume of its own still arriving.
    fn quiet(&self) -> bool {
        let coming = self.coming();
        !coming.payload
            && !self.runs.live()
            && !self.loans.out()
            && !self.writer.rechecks_waiting()
            && !self.verifications_due()
            && self.writer.all_ruled(&self.tree, &coming)
    }

    // Ranges of lost articles go to the root namespace as soon as they can
    // be told, which marks them on every member they land in at every
    // depth; whatever covers them repairs them.
    pub(super) fn mark_damage(&mut self) -> Result<(), GraphError> {
        let lost = self.root.damage();
        if lost.is_empty() {
            return Ok(());
        }
        let mut out = Vec::new();
        self.writer.lost(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            (NodeId::ROOT, lost),
            &mut out,
        )?;
        self.heard(out)
    }
}

// Where the last seal pass left the root: its namespace's generation, its
// sets, and those of them that stood clean or repaired.
pub(super) struct Sealed {
    members: u64,
    sets: Vec<NodeId>,
    standing: BTreeSet<NodeId>,
}

#[cfg(test)]
#[path = "repair_tests.rs"]
pub(super) mod tests;
