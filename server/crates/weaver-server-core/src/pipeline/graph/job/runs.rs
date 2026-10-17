// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the owner of its engines' runs. After every step it brings each
// engine node's shelf up to what the tree knows, retracts and restarts a run
// whose bytes a repair replaced, starts one whose object has bytes and no run,
// and tells each object how far its run has read, so it bids for what comes
// next. What a run says is applied here, at the node it speaks for.

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{Emit, Engine, MemberId, NodeId};

use super::{GraphError, GraphJob};
use crate::pipeline::graph::engine::{Heard, Said};
use crate::pipeline::graph::namespace::WriteError;

impl GraphJob {
    // What a run said. A stopped run's words carry an old generation and
    // are not heard.
    pub(crate) fn run_said(&mut self, said: Said) -> Result<(), GraphError> {
        let node = said.node;
        if !self.runs.current(node, said.generation) {
            return Ok(());
        }
        match said.heard {
            Heard::Emit(emit) => {
                let mut effects = Vec::new();
                self.tree
                    .emitted(node, vec![emit], &mut *self.builder, &mut effects);
                self.settle(effects)?;
            }
            Heard::Ended => {
                // The run was the only producer of its members, so nothing
                // more will arrive for any of them.
                let open: Vec<MemberId> = self
                    .members(node)
                    .into_iter()
                    .filter(|member| !self.closed_in(node, *member))
                    .collect();
                for member in open {
                    let mut out = Vec::new();
                    self.writer.close(
                        &mut self.tree,
                        &mut *self.builder,
                        &self.root,
                        node,
                        member,
                        &mut out,
                    )?;
                    self.heard(out)?;
                }
                for admitted in self.runs.ended(node, said.generation) {
                    self.start_run(admitted);
                }
            }
        }
        self.step_runs()
    }

    pub(super) fn step_runs(&mut self) -> Result<(), GraphError> {
        let engines = self.tree.engines();
        for admitted in self.runs.forget_gone(&engines) {
            self.start_run(admitted);
        }
        for node in engines {
            let shelf = self.writer.shelf(node);
            self.feed(node)?;
            match self.runs.generation(node) {
                Some(generation) if generation != shelf.generation() => {
                    self.retract_run(node)?;
                    if self.runs.ask(node) {
                        self.start_run(node);
                    }
                }
                None if !self.runs.has(node) && shelf.has_bytes() => {
                    if self.runs.ask(node) {
                        self.start_run(node);
                    }
                }
                _ => {}
            }
            if let (Some((volume, offset)), Some(engine)) =
                (shelf.reached(), self.tree.engine_mut(node))
            {
                engine.reached(volume, offset);
            }
        }
        Ok(())
    }

    fn start_run(&mut self, node: NodeId) {
        let Some(extraction) = self.tree.engine(node).map(Engine::start) else {
            return;
        };
        let shelf = self.writer.shelf(node);
        self.runs.start(node, extraction, shelf);
    }

    // Everything the run built was built on bytes a repair replaced. The
    // members start over; the envelope keeps the repaired bytes in place.
    fn retract_run(&mut self, node: NodeId) -> Result<(), GraphError> {
        let retracts = self.members(node).into_iter().map(Emit::Retract).collect();
        let mut effects = Vec::new();
        self.tree
            .emitted(node, retracts, &mut *self.builder, &mut effects);
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
        let count = volumes.keys().next_back().map_or(0, |volume| volume.0 + 1);
        let complete = namespace
            .members()
            .all(|member| !self.tree.is_undecided(parent, member.id));
        let ends: Vec<(u32, Option<u64>, bool)> = volumes
            .iter()
            .filter_map(|(volume, member)| {
                let record = namespace.get(*member)?;
                let closed = record.closed.then(|| record.length.unwrap_or(0));
                Some((
                    volume.0,
                    closed,
                    record.closed && self.settled(parent, *member),
                ))
            })
            .collect();
        let shelf = self.writer.shelf(node);
        shelf
            .update(|state| {
                state.volumes = count;
                state.complete = complete;
                for (volume, closed, settled) in ends {
                    let kept = shelf.kept(state, volume)?;
                    kept.closed = closed;
                    kept.settled = settled;
                }
                Ok(())
            })
            .map_err(|error| GraphError::Write(WriteError::Leaf(error)))
    }

    // Nothing more will come for the member, not even a repair: the payload
    // is in, and every recovery object that covers it has ruled.
    fn settled(&self, parent: NodeId, member: MemberId) -> bool {
        !self.payload_waiting()
            && self.tree.recovery_nodes(parent).into_iter().all(|node| {
                self.tree.recovery(node).is_none_or(|recovery| {
                    !recovery.covers(member)
                        || recovery
                            .outcome()
                            .is_some_and(|outcome| outcome != RepairOutcome::Waiting)
                })
            })
    }
}
