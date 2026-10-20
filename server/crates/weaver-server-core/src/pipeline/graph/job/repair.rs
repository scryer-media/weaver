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

use weaver_graph::NodeId;
use weaver_graph::slots::Ticket;

use super::{GraphError, GraphJob};
use crate::pipeline::graph::root::Root;
use crate::pipeline::graph::status::RepairSlots;

impl<R: Root> GraphJob<R> {
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

    // The repair slot the job holds, for a job going away.
    pub(crate) fn forget_slot(&mut self) -> Option<Ticket> {
        self.loans.forget()
    }

    // Walk the namespaces in node order, which puts every parent before its
    // children, and ask each to rule its sets. A namespace the walk's own
    // repairs bring into being is walked too.
    fn walk(&mut self, mut slots: Option<(&mut RepairSlots, usize)>) -> Result<(), GraphError> {
        let mut walked = None;
        while let Some(node) = self
            .tree
            .live()
            .into_iter()
            .find(|node| walked.is_none_or(|last| *node > last))
        {
            walked = Some(node);
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
        Ok(())
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
    // namespace's sets have ruled on something other than waiting.
    fn quiet(&self) -> bool {
        !self.payload_waiting()
            && !self.runs.live()
            && !self.loans.out()
            && self.writer.all_ruled(&self.tree)
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
