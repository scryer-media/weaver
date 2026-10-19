// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the owner of its engines' runs. After every step it brings each
// engine node's shelf up to what the tree knows, retracts and restarts a run
// whose bytes a repair replaced, starts one whose object has bytes and no run,
// and tells each object how far its run has read, so it bids for what comes
// next. What a run says is applied here, at the node it speaks for.

use std::collections::BTreeSet;

use weaver_graph::{Emit, Engine, MemberId, NodeId, Why};

use super::{GraphError, GraphJob};
use crate::pipeline::graph::engine::{Heard, Reservation, Said};
use crate::pipeline::graph::namespace::WriteError;

impl GraphJob {
    // What a run said. A stopped run's words carry an old generation and
    // are not heard.
    pub(crate) fn run_said(&mut self, said: Said, limit: usize) -> Result<(), GraphError> {
        let node = said.node;
        if !self.runs.current(node, said.generation) {
            return Ok(());
        }
        let admitted = match said.heard {
            Heard::Emit(emit) => {
                // The object is told its run is asking, as it is told how
                // far the run read: it rules on it when the job is quiet.
                if emit == Emit::NeedsPassword
                    && let Some(engine) = self.tree.engine_mut(node)
                {
                    engine.asked();
                }
                let mut out = Vec::new();
                self.writer.said(
                    &mut self.tree,
                    &mut *self.builder,
                    &self.root,
                    node,
                    emit,
                    &mut out,
                )?;
                self.heard(out)?;
                Vec::new()
            }
            Heard::Ended => {
                // A member is closed by its engine alone, once it verified
                // every byte of it: one a run left open stays open, and
                // nothing more is announced, so the namespace is in.
                self.runs.ended(node, said.generation, limit)
            }
        };
        // What the run said may have closed what a recovery object covers.
        self.repair()?;
        self.step_runs(admitted, limit)
    }

    // Bring every engine's run up to the tree, under the slot limit the
    // pipeline reads now, and start every run that may: those `admitted`
    // already, and those each step admits in turn. A start can free a slot
    // (a run refused, or turned away for memory), and whoever that admits
    // starts in the same step, so nothing waits beside a free slot.
    pub(super) fn step_runs(
        &mut self,
        mut admitted: Vec<NodeId>,
        limit: usize,
    ) -> Result<(), GraphError> {
        loop {
            let engines = self.tree.engines();
            admitted.extend(self.runs.forget_gone(&engines, limit));
            for node in engines {
                let shelf = self.writer.shelf(node);
                self.feed(node)?;
                let asks = match self.runs.generation(node) {
                    Some(generation) if generation != shelf.generation() => {
                        self.retract_run(node)?;
                        true
                    }
                    None => {
                        !self.runs.has(node)
                            && shelf.has_bytes()
                            && self.tree.engine(node).and_then(Engine::reserve).is_some()
                    }
                    Some(_) => false,
                };
                if asks {
                    admitted.extend(self.runs.ask(node, limit));
                }
                if let (Some((volume, offset)), Some(engine)) =
                    (shelf.reached(), self.tree.engine_mut(node))
                {
                    engine.reached(volume, offset);
                }
            }
            // With every run started, a damaged namespace nothing will
            // repair is refused, and what that frees is stepped again.
            if admitted.is_empty() && !self.tell_quiet()? {
                return Ok(());
            }
            for node in std::mem::take(&mut admitted) {
                admitted.extend(self.start_run(node)?);
            }
        }
    }

    // Start the node's run once its memory is reserved. A run whose memory
    // can never fit is refused to the tree as a run's own refusal is; one
    // that does not fit now hands back the nodes its slot admitted.
    fn start_run(&mut self, node: NodeId) -> Result<Vec<NodeId>, GraphError> {
        let Some(window) = self.tree.engine(node).and_then(Engine::reserve) else {
            return Ok(Vec::new());
        };
        match self.runs.reserve(node, window) {
            Reservation::Granted => {}
            Reservation::Starved(admitted) => return Ok(admitted),
            Reservation::Refused => {
                let mut effects = Vec::new();
                self.tree.emitted(
                    node,
                    vec![Emit::Refuse(Why::Unsupported)],
                    &mut self.writer.placing(&mut *self.builder),
                    &mut effects,
                );
                self.settle(effects)?;
                return Ok(Vec::new());
            }
        }
        if let Some(extraction) = self.tree.engine(node).map(Engine::start) {
            let shelf = self.writer.shelf(node);
            self.runs.start(node, extraction, shelf);
        }
        Ok(Vec::new())
    }

    // Everything the run built was built on bytes a repair replaced. The
    // members start over; the envelope keeps the repaired bytes in place.
    fn retract_run(&mut self, node: NodeId) -> Result<(), GraphError> {
        let retracts = self.members(node).into_iter().map(Emit::Retract).collect();
        let mut effects = Vec::new();
        self.tree.emitted(
            node,
            retracts,
            &mut self.writer.placing(&mut *self.builder),
            &mut effects,
        );
        self.settle(effects)
    }

    fn members(&self, node: NodeId) -> Vec<MemberId> {
        self.tree
            .namespace(node)
            .map(|namespace| namespace.members().map(|member| member.id).collect())
            .unwrap_or_default()
    }

    // Bring the node's shelf up to what the tree knows of its volumes: how
    // many there are, which are closed and how long, which will receive
    // nothing more, and whether any more can join.
    fn feed(&mut self, node: NodeId) -> Result<(), GraphError> {
        let Some((parent, volumes)) = self.tree.volumes(node) else {
            return Ok(());
        };
        let Some(namespace) = self.tree.namespace(parent) else {
            return Ok(());
        };
        let bound: BTreeSet<u32> = volumes.keys().map(|volume| volume.0).collect();
        // A volume can still join while a member's front has yet to decide
        // its owner, and while anything more can come: a recovery object yet
        // to rule may rebuild a volume never posted.
        let ended = self.ended(parent);
        let deciding = namespace
            .members()
            .any(|member| self.tree.is_undecided(parent, member.id));
        let complete = ended && !deciding;
        let ends: Vec<(u32, Option<u64>, bool)> = volumes
            .iter()
            .filter_map(|(volume, member)| {
                let record = namespace.get(*member)?;
                let closed = record.closed.then(|| record.length.unwrap_or(0));
                Some((volume.0, closed, record.closed && ended))
            })
            .collect();
        let shelf = self.writer.shelf(node);
        shelf
            .update(|state| {
                state.volumes = bound;
                state.complete = complete;
                state.deciding = deciding;
                for (volume, closed, settled) in ends {
                    let kept = shelf.kept(state, volume)?;
                    kept.closed = closed;
                    kept.settled = settled;
                }
                Ok(())
            })
            .map_err(|error| GraphError::Write(WriteError::Leaf(error)))
    }

    // Nothing more will come for the namespace's members, not even a
    // repair: the payload is in, and the namespace's sets have ruled. One
    // that has not may cover a member yet, since it binds what it covers as
    // its own volumes arrive, and those can come after the member closed,
    // or rebuild a member that never came at all.
    fn ended(&self, parent: NodeId) -> bool {
        !self.payload_waiting() && self.writer.ruled(&self.tree, parent)
    }
}
