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
    // Its `Recovery::precedence` when it left.
    precedence: u8,
    // Its `Recovery::rebuilds` and `Recovery::carried_inside` when it left.
    rebuilds: bool,
    carried_inside: bool,
    waiting: Vec<Waiting>,
    // Members that could not be asked whether they are its volumes, in the
    // order they asked.
    queued: Vec<(NodeId, MemberId)>,
    // Lent for the work it held, not for a repair.
    work: bool,
    // Its `Object::passes_on` when it left: what the node answers while it
    // is out.
    passes_on: bool,
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
    // A password the job offered while the object was out.
    Password(String),
}

impl Lent {
    pub(super) fn wait(&mut self, waiting: Waiting) {
        self.waiting.push(waiting);
    }

    // What waited for it is counted as it goes: taken back, or dropped with
    // its node.
    pub(super) fn count(&self, waits: &mut LentWaits) {
        waits.count(&self.waiting);
    }
}

// How many bytes ever waited behind lent objects in the tree: the loans that
// ended with bytes queued, the most one loan's queue held as it ended, and
// the sum over all of them. Kept in memory for the tree's life; never
// recorded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LentWaits {
    pub loans_that_queued_bytes: u64,
    pub peak_queued_bytes: u64,
    pub total_queued_bytes: u64,
}

impl LentWaits {
    fn count(&mut self, waiting: &[Waiting]) {
        let mut queued = 0;
        let mut delivered = false;
        for waiting in waiting {
            if let Waiting::Delivered { arrived, .. } = waiting {
                delivered = true;
                queued += arrived.bytes.bytes().len() as u64;
            }
        }
        if delivered {
            self.loans_that_queued_bytes += 1;
            self.peak_queued_bytes = self.peak_queued_bytes.max(queued);
            self.total_queued_bytes += queued;
        }
    }
}

impl Tree {
    // Lend the recovery object at `node` out of the tree. `None` when the
    // node holds no recovery object.
    pub fn lend(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        let recovery = self.recovery(node)?;
        let (precedence, rebuilds, carried_inside) = (
            recovery.precedence(),
            recovery.rebuilds(),
            recovery.carried_inside(),
        );
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
            precedence,
            rebuilds,
            carried_inside,
            waiting: Vec::new(),
            queued: Vec::new(),
            work: false,
            passes_on: object.passes_on(),
        });
        Some(object)
    }

    // The nodes whose object holds work to be lent out for, in node order.
    pub fn working(&self) -> Vec<NodeId> {
        self.due_work().collect()
    }

    // Whether any object holds work to be lent out for.
    pub fn work_due(&self) -> bool {
        self.due_work().next().is_some()
    }

    fn due_work(&self) -> impl Iterator<Item = NodeId> + '_ {
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
        let (precedence, rebuilds, carried_inside) =
            self.recovery(node).map_or((0, false, false), |recovery| {
                (
                    recovery.precedence(),
                    recovery.rebuilds(),
                    recovery.carried_inside(),
                )
            });
        let found = self.nodes.get_mut(&node)?;
        let object = found.object.take()?;
        found.lent = Some(Lent {
            kind: object.kind(),
            facts: None,
            covers: Vec::new(),
            precedence,
            rebuilds,
            carried_inside,
            waiting: Vec::new(),
            queued: Vec::new(),
            work: true,
            passes_on: object.passes_on(),
        });
        Some(object)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn lend_for_test(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        self.lend_out(node)
    }

    // How many bytes waited behind lent objects so far.
    pub fn lent_waits(&self) -> LentWaits {
        self.lent_waits
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

    // Whether the object lent from `node` covered any member when it left.
    pub fn lent_covers_any(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .and_then(|found| found.lent.as_ref())
            .is_some_and(|lent| !lent.covers.is_empty())
    }

    // Whether the object at `node` is a recovery object that rebuilds what
    // it covers (`Recovery::rebuilds`), as it left when it is out for a
    // repair. Wherever `recovers` answers no, so does this.
    pub fn rebuilds(&self, node: NodeId) -> bool {
        match self.recovery(node) {
            Some(recovery) => recovery.rebuilds(),
            None => self
                .nodes
                .get(&node)
                .and_then(|found| found.lent.as_ref())
                .is_some_and(|lent| !lent.work && lent.rebuilds),
        }
    }

    // Whether the object at `node` is a recovery object carried inside the
    // one member it covers (`Recovery::carried_inside`), as it left when it
    // is out for a repair. Wherever `recovers` answers no, so does this.
    pub fn carried_inside(&self, node: NodeId) -> bool {
        match self.recovery(node) {
            Some(recovery) => recovery.carried_inside(),
            None => self
                .nodes
                .get(&node)
                .and_then(|found| found.lent.as_ref())
                .is_some_and(|lent| !lent.work && lent.carried_inside),
        }
    }

    // The set's `Recovery::precedence`, as it left when it is out.
    fn precedence(&self, set: NodeId) -> u8 {
        match self.recovery(set) {
            Some(recovery) => recovery.precedence(),
            None => self
                .nodes
                .get(&set)
                .and_then(|found| found.lent.as_ref())
                .map_or(0, |lent| lent.precedence),
        }
    }

    // Whether the set ranks above every other set homed in `node`: its
    // returned repair may rewrite bytes an earlier repair wrote. A set of
    // the lowest rank outranks nothing, and asks no other set.
    pub fn outranks(&self, node: NodeId, set: NodeId) -> bool {
        let rank = self.precedence(set);
        rank > 0
            && self
                .sets
                .range((node, NodeId(0))..=(node, NodeId(u32::MAX)))
                .all(|&(_, other)| other == set || self.precedence(other) < rank)
    }

    pub(super) fn lent_kind(&self, node: NodeId) -> Option<Kind> {
        Some(self.nodes.get(&node)?.lent.as_ref()?.kind)
    }

    // What the object at `node` said of `Object::passes_on` as it left.
    pub(super) fn lent_passes_on(&self, node: NodeId) -> Option<bool> {
        Some(self.nodes.get(&node)?.lent.as_ref()?.passes_on)
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
        self.lent_waits.count(&waiting);
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
                // Marked now, in its turn: bytes that waited behind it land
                // after it, as they came.
                Down::Lost { origin } => self.descend_lost(node, (volume, range), origin, effects),
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
        // An offer is heard as the job makes it, with what it opened.
        let told = match told {
            Told::Password(secret) => {
                return self.offer_password(node, &secret, builder, effects);
            }
            told => told,
        };
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
                // Heard as an offer, above.
                Told::Password(_) => {}
            }
        }
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }
}
