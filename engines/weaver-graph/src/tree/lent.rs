// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object lent out of its node, so its owner can run its work away from
// the tree: a recovery object's repair, or the slow work any object says it
// holds. The node keeps what the object is and, for a repair, what its facts
// were when it left; everything that reaches the object meanwhile waits in arrival
// order, and a member of its namespace that the object might claim queues
// behind it. Taken back, the object hears what waited as if it had been
// there, and the queued members are asked again who owns them.

use super::{Builder, Down, Effect, NodeId, Tree};
use crate::deliver::{Delivery, Emit, Provenance};
use crate::evidence::Evidenced;
use crate::facts::Facts;
use crate::member::MemberId;
use crate::object::{Kind, Object};
use crate::recovery::Segment;
use crate::seek::{Pull, Seek};
use crate::volume::{ByteRange, VolumeIndex};

pub(super) struct Lent {
    kind: Kind,
    // What a repair's object was as it left. An object out for its work
    // leaves none: no checkpoint is taken while it is out, and it always
    // comes back, so nothing reads them, and taking them on every loan
    // would encode what it holds each time.
    facts: Option<Facts>,
    // The members of its namespace it covered when it left.
    covers: Vec<MemberId>,
    waiting: Vec<Waiting>,
    // Members that could not be asked whether they are its volumes, in the
    // order they asked.
    queued: Vec<(NodeId, MemberId)>,
    // Lent for the work it held, not for a repair.
    work: bool,
}

// Bytes for one of an object's volumes, owned, so a delivery can wait.
pub(super) struct Arrived {
    pub(super) volume: VolumeIndex,
    pub(super) offset: u64,
    pub(super) bytes: Evidenced,
    pub(super) provenance: Provenance,
}

impl Arrived {
    pub(super) fn delivery(&self) -> Delivery<'_> {
        Delivery {
            volume: self.volume,
            offset: self.offset,
            bytes: &self.bytes,
            provenance: self.provenance,
        }
    }
}

// What reached an object, in the form it waits in: bytes, which it reads
// placements beside, or something it is told.
pub(super) enum Waiting {
    Delivered {
        arrived: Arrived,
        length: Option<u64>,
    },
    Told(Told),
    // A range of one of its volumes that went down the tree and reached it
    // while it was out, to go on down from it.
    Descended {
        volume: VolumeIndex,
        range: ByteRange,
        down: Down,
    },
}

pub(super) enum Told {
    Closed {
        volume: VolumeIndex,
        length: u64,
        reached: u64,
    },
    Lost {
        volume: VolumeIndex,
        range: ByteRange,
    },
    Renamed {
        volume: VolumeIndex,
        name: String,
    },
    Pulled {
        seek: Seek,
        pull: Pull,
    },
    Fed {
        member: MemberId,
        evidence: Vec<Segment>,
        proven: bool,
    },
    Refused {
        member: MemberId,
        range: ByteRange,
    },
}

impl Lent {
    pub(super) fn wait(&mut self, waiting: Waiting) {
        self.waiting.push(waiting);
    }
}

impl Tree {
    // Lend the recovery object at `node` out of the tree. `None` when the
    // node holds no recovery object.
    pub fn lend(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        self.recovery(node)?;
        let covers = self
            .consult_recovery(node, |recovery, namespace| {
                namespace
                    .members()
                    .map(|member| member.id)
                    .filter(|member| recovery.covers(*member))
                    .collect()
            })
            .unwrap_or_default();
        let found = self.nodes.get_mut(&node)?;
        let object = found.object.take()?;
        found.lent = Some(Lent {
            kind: object.kind(),
            facts: Some(object.facts()),
            covers,
            waiting: Vec::new(),
            queued: Vec::new(),
            work: false,
        });
        Some(object)
    }

    // The nodes whose object holds work to be lent out for, in node order.
    pub fn working(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, found)| !found.retired && found.lent.is_none())
            .filter(|(_, found)| {
                found
                    .object
                    .as_ref()
                    .is_some_and(|object| object.work_due())
            })
            .map(|(node, _)| *node)
            .collect()
    }

    // Lend the object at `node` out to do the work it holds. `None` when it
    // holds none, is retired, or is out already.
    pub fn lend_work(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        let found = self.nodes.get_mut(&node)?;
        if found.retired
            || found.lent.is_some()
            || !found
                .object
                .as_ref()
                .is_some_and(|object| object.work_due())
        {
            return None;
        }
        self.lend_out(node)
    }

    // Out of its node as if for its work, whatever it holds.
    fn lend_out(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        let found = self.nodes.get_mut(&node)?;
        let object = found.object.take()?;
        found.lent = Some(Lent {
            kind: object.kind(),
            facts: None,
            covers: Vec::new(),
            waiting: Vec::new(),
            queued: Vec::new(),
            work: true,
        });
        Some(object)
    }

    #[cfg(test)]
    pub(super) fn lend_for_test(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        self.lend_out(node)
    }

    pub fn lent(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|found| found.lent.is_some())
    }

    // Whether the object at `node` is out for a repair.
    pub(super) fn lent_for_repair(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .and_then(|found| found.lent.as_ref())
            .is_some_and(|lent| !lent.work)
    }

    // Whether the object lent from `node` covered the member when it left.
    pub fn lent_covers(&self, node: NodeId, member: MemberId) -> bool {
        self.nodes
            .get(&node)
            .and_then(|found| found.lent.as_ref())
            .is_some_and(|lent| lent.covers.contains(&member))
    }

    pub(super) fn lent_kind(&self, node: NodeId) -> Option<Kind> {
        Some(self.nodes.get(&node)?.lent.as_ref()?.kind)
    }

    // The facts of the object at `node` as they were when it was lent for a
    // repair: what it is checkpointed as while it is out, and what rebuilds
    // it as it was. An object out for its work has none.
    pub fn lent_facts(&self, node: NodeId) -> Option<&Facts> {
        self.nodes.get(&node)?.lent.as_ref()?.facts.as_ref()
    }

    // A lent object comes back, the one lent or one rebuilt from its facts.
    // It is handed what its node's owner handed last, then hears everything
    // that waited for it, in the order it came, and every member that queued
    // behind it is asked again who owns it. A node retracted meanwhile has
    // nowhere to take it, so the object is dropped with it.
    pub fn take_back(
        &mut self,
        node: NodeId,
        object: Box<dyn Object>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        self.take_back_worked(node, object, Vec::new(), builder, effects);
    }

    // A lent object comes back with what its work said, which is applied
    // first, as if it said it in its node, before what waited is heard.
    pub fn take_back_worked(
        &mut self,
        node: NodeId,
        mut object: Box<dyn Object>,
        mut said: Vec<Emit>,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        let Some(found) = self.nodes.get_mut(&node) else {
            return;
        };
        let (waiting, queued) = found
            .lent
            .take()
            .map_or_else(Default::default, |lent| (lent.waiting, lent.queued));
        if let Some(binding) = &found.binding {
            object.handed(&binding.handed);
        }
        found.object = Some(object);
        if !said.is_empty() {
            self.apply(node, &mut said, builder, effects);
        }
        for waiting in waiting {
            self.hear(node, waiting, builder, effects);
        }
        for (parent, member) in queued {
            self.identify_again(parent, member, builder, effects);
        }
    }

    // A member of `parent` waits for the object lent from `node` to be back
    // before it is asked who owns it.
    pub(super) fn queue_behind(&mut self, node: NodeId, parent: NodeId, member: MemberId) {
        if let Some(lent) = self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.lent.as_mut())
            && !lent.queued.contains(&(parent, member))
        {
            lent.queued.push((parent, member));
        }
    }

    // Segment evidence for a member the recovery object at `set` covers.
    pub fn feed(&mut self, set: NodeId, member: MemberId, evidence: &[Segment]) {
        self.feed_input(set, member, evidence, false);
    }

    pub fn feed_proven(&mut self, set: NodeId, member: MemberId, evidence: &[Segment]) {
        self.feed_input(set, member, evidence, true);
    }

    fn feed_input(&mut self, set: NodeId, member: MemberId, evidence: &[Segment], proven: bool) {
        let waiting = Waiting::Told(Told::Fed {
            member,
            evidence: evidence.to_vec(),
            proven,
        });
        if self.wait(set, waiting).is_some() {
            self.with_recovery(set, |recovery, _| {
                if proven {
                    recovery.feed_proven(member, evidence);
                } else {
                    recovery.feed(member, evidence);
                }
            });
        }
    }

    // A range of a member the recovery object at `set` covers will never
    // come. The object hears it through its recovery face, never as bytes.
    pub fn refuse(&mut self, set: NodeId, member: MemberId, range: ByteRange) {
        let waiting = Waiting::Told(Told::Refused { member, range });
        if self.wait(set, waiting).is_some() {
            self.with_recovery(set, |recovery, _| recovery.refused(member, range));
        }
    }

    // A range of the volume of the object at `node` will never arrive. What
    // it says is applied as if it said it in `deliver`.
    pub fn volume_lost(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        self.reach(node, Told::Lost { volume, range }, builder, effects);
    }

    // Keep what reached a lent object for its return; hand it back when the
    // object is in its node.
    pub(super) fn wait(&mut self, node: NodeId, waiting: Waiting) -> Option<Waiting> {
        match self
            .nodes
            .get_mut(&node)
            .and_then(|found| found.lent.as_mut())
        {
            Some(lent) => {
                lent.wait(waiting);
                None
            }
            None => Some(waiting),
        }
    }

    pub(super) fn reach(
        &mut self,
        node: NodeId,
        told: Told,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if let Some(waiting) = self.wait(node, Waiting::Told(told)) {
            self.hear(node, waiting, builder, effects);
        }
    }

    fn hear(
        &mut self,
        node: NodeId,
        waiting: Waiting,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        match waiting {
            Waiting::Delivered { arrived, length } => {
                self.deliver_to_object(node, &arrived, length, builder, effects);
            }
            Waiting::Told(told) => self.tell(node, told, builder, effects),
            Waiting::Descended {
                volume,
                range,
                down,
            } => match down {
                Down::Lost { origin } => effects.push(Effect::Lost {
                    origin,
                    node,
                    volume,
                    range,
                }),
                Down::Refuted => self.descend(vec![(node, volume, range)], down, effects),
                Down::Forget { keep_repairs } => {
                    self.forget_input(node, volume, range, keep_repairs);
                }
            },
        }
    }

    // The object at `node` is told; what it says is applied as if it said
    // it in `deliver`. A retired object hears nothing: its word was its last.
    fn tell(
        &mut self,
        node: NodeId,
        told: Told,
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
            match told {
                Told::Closed {
                    volume,
                    length,
                    reached,
                } => {
                    object.sized(volume, length, &mut emits);
                    object.volume_reached(volume, reached, &mut emits);
                    object.volume_closed(volume, &mut emits);
                }
                Told::Lost { volume, range } => object.volume_lost(volume, range, &mut emits),
                Told::Renamed { volume, name } => object.renamed(volume, &name, &mut emits),
                Told::Pulled { seek, pull } => object.pulled(seek, pull, &mut emits),
                Told::Fed {
                    member,
                    evidence,
                    proven,
                } => {
                    if let Some(recovery) = object.recovery_mut() {
                        if proven {
                            recovery.feed_proven(member, &evidence);
                        } else {
                            recovery.feed(member, &evidence);
                        }
                    }
                }
                Told::Refused { member, range } => {
                    if let Some(recovery) = object.recovery_mut() {
                        recovery.refused(member, range);
                    }
                }
            }
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }
}
