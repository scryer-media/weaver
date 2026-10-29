// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the owner of its engines' runs. After every step it brings each
// engine node's shelf up to what the tree knows, retracts and restarts a run
// whose bytes a repair replaced, starts one whose object has bytes and no run,
// and tells the tree how far each run has read, so its object bids for what
// comes next. What a run says is applied here, at the node it speaks for.

use weaver_graph::{Emit, Engine, MemberId, NodeId, Why};

use super::{GraphError, GraphJob};
use crate::pipeline::graph::engine::{Heard, Reservation, Said};
use crate::pipeline::graph::namespace::{Returned, WriteError};
use crate::pipeline::graph::status::RepairSlots;

impl GraphJob {
    // A terminal failure keeps worker ownership until the matching end
    // acknowledgments arrive. Late output and checkpoint acknowledgments
    // must not publish more files or start another step of the failed job.
    pub(crate) fn drain_failed_run(&mut self, said: Said) -> Result<(), GraphError> {
        match said.heard {
            Heard::Ended if self.runs.current(said.node, said.generation) => {
                self.runs.ended(said.node, said.generation, 0);
            }
            Heard::Repaired(returned) => {
                self.loans.returned(said.node, said.generation);
                returned.repaired.remove().map_err(WriteError::from)?;
            }
            Heard::Replayed(replayed) => {
                self.loans.returned(said.node, said.generation);
                replayed.remove().map_err(WriteError::from)?;
            }
            Heard::Verified(_) => {
                self.loans.returned(said.node, said.generation);
            }
            Heard::Written { stamp, control } => {
                if self.pending.control().same(&control) {
                    self.acknowledge_checkpoint(stamp.unwrap_or(said.generation));
                }
            }
            Heard::Emit(_) | Heard::Ended | Heard::CheckpointBlocked { .. } => {}
        }
        Ok(())
    }

    // What a run said, or a repair lent off the loop came back with. A
    // stopped run's words carry an old generation and are not heard.
    pub(crate) fn run_said(
        &mut self,
        said: Said,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        let node = said.node;
        let admitted = match said.heard {
            Heard::Repaired(returned) => {
                return self.repaired(node, said.generation, *returned, slots, limit);
            }
            Heard::Replayed(replayed) => {
                if !self.loans.returned(node, said.generation) {
                    return Ok(replayed.remove().map_err(WriteError::from)?);
                }
                let mut out = Vec::new();
                self.writer.replayed(
                    &mut self.tree,
                    &mut *self.builder,
                    &self.root,
                    *replayed,
                    &mut out,
                )?;
                self.heard(out)?;
                self.step_runs(Vec::new(), limit)?;
                return self.lend(slots, limit);
            }
            Heard::Verified(verified) => {
                if !self.loans.returned(node, said.generation) {
                    return Ok(());
                }
                let mut out = Vec::new();
                self.writer.verified(
                    &mut self.tree,
                    &mut *self.builder,
                    &self.root,
                    *verified,
                    &mut out,
                )?;
                self.heard(out)?;
                self.step_runs(Vec::new(), limit)?;
                return self.lend(slots, limit);
            }
            Heard::Written { stamp, control } => {
                return if self.pending.control().same(&control) {
                    self.written(stamp, said.generation)
                } else {
                    Ok(())
                };
            }
            Heard::CheckpointBlocked { .. } => return Ok(()),
            _ if !self.runs.current(node, said.generation) => return Ok(()),
            Heard::Emit(emit) => {
                // The tree is told the run is asking, as it is told how far
                // the run read: the object rules on it when the job is quiet.
                if emit == Emit::NeedsPassword {
                    self.tree.run_asked(node);
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
        // What the run said may have closed what a recovery object covers:
        // the walk rules on it once the runs the step starts are reading.
        self.step_runs(admitted, limit)?;
        self.lend(slots, limit)
    }

    // The set's repair came back. Its ruling is applied, what the ruling
    // closed is stepped, and the next repair due goes out under the slot
    // the job holds or one `slots` gives it. A set the loop moved under is
    // rebuilt from its facts as `weaver_objects` restores them.
    fn repaired(
        &mut self,
        set: NodeId,
        serial: u64,
        returned: Returned,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        if !self.loans.returned(set, serial) {
            return Ok(returned.repaired.remove().map_err(WriteError::from)?);
        }
        let mut out = Vec::new();
        let objects = self.runs.hardware();
        self.writer.returned(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            &objects,
            returned,
            &mut out,
        )?;
        self.heard(out)?;
        self.step_runs(Vec::new(), limit)?;
        self.lend(slots, limit)
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
                self.feed(node)?;
                let shelf = self.writer.shelf(node);
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
                if let Some((volume, _)) = shelf.reached() {
                    self.tree.run_reached(node, volume);
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
        if let Some(extraction) = self.tree.start(node) {
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
    // nothing more, and whether any more can join. A shelf already there is
    // left as it is, and its run is not woken to find nothing new.
    fn feed(&mut self, node: NodeId) -> Result<(), GraphError> {
        let Some((parent, volumes)) = self.tree.volumes(node) else {
            return Ok(());
        };
        let Some(namespace) = self.tree.namespace(parent) else {
            return Ok(());
        };
        // A volume can still join while a member's front has yet to decide
        // its owner, and while anything more can come: a recovery object yet
        // to rule may rebuild a volume never posted.
        let ended = self.ended(parent);
        let deciding = !ended || self.tree.deciding(parent);
        let complete = ended && !deciding;
        let bound = volumes.keys().map(|volume| volume.0);
        self.writer
            .update_shelf(node, |shelf, state| {
                let mut changed = state.complete != complete || state.deciding != deciding;
                if !state.volumes.iter().copied().eq(bound.clone()) {
                    state.volumes = bound.collect();
                    changed = true;
                }
                state.complete = complete;
                state.deciding = deciding;
                for (volume, member) in volumes {
                    let Some(record) = namespace.get(*member) else {
                        continue;
                    };
                    let closed = record.closed.then(|| record.length.unwrap_or(0));
                    let settled = record.closed && ended;
                    changed |= !state.kept.contains_key(&volume.0);
                    let kept = shelf.kept(state, volume.0)?;
                    changed |= kept.closed != closed || kept.settled != settled;
                    // A volume that closed again at another length moves
                    // every byte after it: a run that read it, or read on
                    // past it, built on the wrong offsets and starts over,
                    // as it does when repaired bytes land behind it. One
                    // that reopens, as a restore reopens a volume to
                    // refetch it, has moved nothing yet.
                    let moved = kept.closed.zip(closed).is_some_and(|(was, now)| was != now);
                    kept.closed = closed;
                    kept.settled = settled;
                    if moved
                        && state
                            .kept
                            .range(volume.0..)
                            .any(|(_, kept)| kept.reached > 0)
                    {
                        state.generation += 1;
                    }
                }
                Ok(changed)
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
