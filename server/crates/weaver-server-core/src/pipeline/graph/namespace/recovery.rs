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

use std::collections::BTreeSet;

use bytes::Bytes;
use weaver_graph::recovery::{Recovery, RepairOutcome, SetRuling};
use weaver_graph::{
    Algorithm, Builder, ByteRange, Effect, Handed, Kind, MemberId, NodeId, Provenance,
    RecoveryKind, Restore, Segment, Tree, VolumeIndex,
};

use super::{
    Acknowledging, Arrival, Loan, NamespaceWriter, READ_BACK, Returned, Segments, Write,
    WriteError, in_own_file, index,
};

// Who a due set's repair is lent to, to run off the loop. One goes out at a
// time, under a repair slot; its return comes back to `returned`.
pub(crate) trait Lender {
    // Whether a repair may go out now.
    fn admit(&mut self) -> bool;
    fn lend(&mut self, loan: Loan);
}

// How many member names a bind kept to log what it newly covered, for the
// tests that count them.
#[cfg(test)]
thread_local! {
    pub(super) static NAMES_KEPT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// What one set says of the members it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SetState {
    // The tree node the set's object is. One PAR2 object holds every PAR2
    // set of its namespace, so this is per object, not per set id.
    pub(crate) node: NodeId,
    // The namespace it lives in, where the members it covers are.
    pub(crate) namespace: NodeId,
    pub(crate) kind: Kind,
    // What its last repair came to; `None` before it ran one.
    pub(crate) outcome: Option<RepairOutcome>,
}

impl NamespaceWriter {
    // Every set in the tree with the namespace it lives in, parents first.
    pub(crate) fn sets(&self, tree: &Tree) -> Vec<(NodeId, NodeId)> {
        tree.live()
            .into_iter()
            .flat_map(|namespace| {
                tree.recovery_nodes(namespace)
                    .into_iter()
                    .map(move |set| (namespace, set))
            })
            .collect()
    }

    // Of `sets`, a namespace's sets as the tree listed them, the ones that
    // cover the member. The namespace's sets are asked once and handed to
    // the work over each of its members.
    pub(crate) fn covering<'t>(
        &'t self,
        tree: &'t Tree,
        sets: &'t [NodeId],
        member: MemberId,
    ) -> impl Iterator<Item = NodeId> + 't {
        sets.iter()
            .copied()
            .filter(move |set| self.covers(tree, *set, member))
    }

    // Whether the set covers the member: the one place a member is matched
    // to a set that describes it. A set out on loan answers as it left.
    pub(crate) fn covers(&self, tree: &Tree, set: NodeId, member: MemberId) -> bool {
        tree.recovery(set).map_or_else(
            || tree.lent_covers(set, member),
            |recovery| recovery.covers(member),
        )
    }

    // The member is a set's own volume: recovery data, in its own file.
    pub(crate) fn carries_recovery(&self, tree: &Tree, node: NodeId, member: MemberId) -> bool {
        tree.child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child))
    }

    // Every set of the namespace has ruled on something other than waiting:
    // nothing a set of it will rebuild is still to come. A set whose repair
    // is out has its ruling still to come.
    pub(crate) fn ruled(&self, tree: &Tree, node: NodeId) -> bool {
        tree.recovery_nodes(node).into_iter().all(|set| {
            !tree.lent(set)
                && tree.recovery(set).is_none_or(|recovery| {
                    recovery
                        .outcome()
                        .is_some_and(|outcome| outcome != RepairOutcome::Waiting)
                })
        })
    }

    // Nothing more will come for the namespace's members, not even a
    // repair: the payload is in, and the namespace's sets have ruled. One
    // that has not may cover a member yet, since it binds what it covers as
    // its own volumes arrive, and those can come after the member closed,
    // or rebuild a member that never came at all.
    pub(crate) fn ended(&self, tree: &Tree, node: NodeId, payload_waiting: bool) -> bool {
        !payload_waiting && self.ruled(tree, node)
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
    pub(crate) fn all_ruled(&self, tree: &Tree) -> bool {
        tree.live().into_iter().all(|node| self.ruled(tree, node))
    }

    // What each set last ruled, parents first; `None` for one that has not.
    pub(crate) fn outcomes(&self, tree: &Tree) -> Vec<Option<RepairOutcome>> {
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
    pub(crate) fn rulings(&self, tree: &Tree, set: NodeId, out: &mut Vec<SetRuling>) {
        if let Some(recovery) = tree.recovery(set) {
            recovery.sets(out);
        }
    }

    // What a set is called: what the file that carries it is.
    pub(crate) fn set_name<'t>(&self, tree: &'t Tree, set: NodeId) -> Option<&'t str> {
        tree.volumes(set)
            .and_then(|(parent, volumes)| tree.namespace(parent)?.get(*volumes.values().next()?))
            .map(|record| record.output_name.as_str())
    }

    // The block sizes the namespace's sets fold their evidence at. Bytes its
    // source cuts at their boundaries prove a slice as they pass, rather
    // than be read back.
    pub(crate) fn block_sizes(&self, tree: &Tree, node: NodeId) -> Vec<u64> {
        let mut sizes = Vec::new();
        for set in tree.recovery_nodes(node) {
            if let Some(recovery) = tree.recovery(set) {
                recovery.block_sizes(&mut sizes);
            }
        }
        sizes
    }

    // Rule the namespace's sets that are due. A file a set describes and
    // no member carries is announced first, once the namespace has every
    // member it will get, so the repair has a member to write it into.
    // `reading` says which namespaces' sources may still announce members.
    // A due set's repair goes to `lender`, as far as it admits one.
    pub(crate) fn rule(
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
    pub(crate) fn lost(
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
                self.set_volume_lost(tree, builder, marked, &mut effects);
                self.refuse_range(tree, node, marked);
            }
        }
        if effects.is_empty() {
            return Ok(());
        }
        self.apply_all(tree, builder, segments, effects, out)
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
    // covers one: that set may yet rebuild what the run reads.
    fn refused_above(&self, tree: &Tree, origin: NodeId, node: NodeId) -> bool {
        let mut at = node;
        while at != origin {
            let Some((parent, volumes)) = tree.volumes(at) else {
                return false;
            };
            let sets = tree.recovery_nodes(parent);
            let rebuilt = volumes.values().any(|member| {
                self.covering(tree, &sets, *member).any(|set| {
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
            absent.retain(|(name, _)| namespace.records().all(|member| member.name != *name));
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
    // meanwhile finds it out and does not take it again.
    pub(super) fn repair(
        &self,
        tree: &mut Tree,
        node: NodeId,
        due: &[NodeId],
        lender: &mut dyn Lender,
    ) -> Result<(), WriteError> {
        for &set in due {
            if !lender.admit() {
                break;
            }
            if let Some(loan) = self.lend(tree, node, set)? {
                lender.lend(loan);
            }
        }
        Ok(())
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
    pub(crate) fn returned(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        restore: &dyn Restore,
        mut returned: Returned,
        out: &mut Vec<Effect>,
    ) -> Result<Option<u64>, WriteError> {
        // A repair whose output refused a write failed with it, and nothing
        // it came to is applied. The refusal is the error, whatever removing
        // the output then says.
        if let Some(error) = returned.repaired.failed() {
            let _ = returned.repaired.remove();
            return Err(error.into());
        }
        // A set retracted while it was out has nowhere to rule.
        if !tree.lent(returned.set) {
            returned.repaired.remove()?;
            return Ok(None);
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
            for (parent, member) in volumes {
                self.replay_file(tree, builder, segments, parent, member, None, out)?;
            }
            repaired.remove()?;
            return Ok(None);
        }
        tree.take_back(set, object, &mut self.placing(builder), &mut effects);
        self.apply_all(tree, builder, segments, effects, out)?;
        // A stopped repair whose object keeps no facts is taken back as it
        // came, and nothing it came to is applied.
        if stopped {
            repaired.remove()?;
            return Ok(None);
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
            #[cfg(test)]
            self.diagnostics.push(diagnostic);
        }
        self.read_backs += reads;
        // The set binds what it ruled before its bytes land: a member it
        // rebuilds takes the length it describes, so the slices that reach
        // that length close the file there. A repair reads members back, so
        // it may have identified one by its content; this bind gives it the
        // name it is.
        self.acknowledging = Some(Acknowledging {
            set,
            closed: BTreeSet::new(),
        });
        let applied = (|| {
            let mut rejected = tree.rejected_inputs(node, repaired.members());
            self.bind_recoveries(tree, builder, segments, node, out)?;
            let mut rewritten = std::collections::BTreeSet::new();
            for (member, offset, bytes) in repaired.slices() {
                let bytes = bytes?;
                rewritten.insert(member);
                let arrival = Arrival {
                    node,
                    member,
                    offset,
                    // A repair's output: nothing proved it on the way.
                    bytes: bytes.into(),
                    provenance: Provenance::Repaired,
                };
                self.arrive(tree, builder, segments, Write::new(&arrival), arrival, out)?;
            }
            repaired.remove()?;
            for (member, range) in found {
                let marked = (node, member, range, Provenance::Refuted);
                self.redeliver(tree, builder, segments, marked, out)?;
            }
            // A rebuilt volume can let its object place bytes held behind it;
            // they go down before the set's proof is read back through the map.
            self.release_held(tree, builder, segments, out)?;
            // The ruling as the set stands now: one given before the bind
            // above changed what it covers is void, and vouches for nothing.
            if self.stands_proved(tree, set) {
                rewritten.extend(self.vouched(tree, builder, segments, (node, set), out)?);
            }
            // Repair or proof delivery can complete a member's checksum
            // metadata or inverse map. Capture any newly auditable damage
            // through those changed inputs, alongside the earlier captures.
            rejected.extend(tree.rejected_inputs(node, rewritten.iter().copied()));
            for &member in &rewritten {
                self.reopen(tree, builder, segments, (node, member), out)?;
            }
            // A member the set rebuilt or proved whole has its length and
            // every byte proven: that proof closes it, through the close
            // its producer's would take, and the batch judges it with every
            // member it closed.
            for member in rewritten {
                if proven_open(tree, node, member) {
                    self.shut(tree, builder, segments, node, member, out)?;
                }
            }
            self.queue_rechecks(rejected);
            self.release_held(tree, builder, segments, out)
        })();
        let batch = self.acknowledging.take().expect("the batch opened above");
        applied?;
        self.acknowledged(tree, builder, segments, batch, out)?;
        Ok(match outcome {
            Some(RepairOutcome::Repaired { slices }) => Some(slices),
            _ => None,
        })
    }

    // The batch is down: each member it closed that is still closed in its
    // own file gets its verdict once, on the bytes it ends with. One the
    // batch reopened or handed to an object is judged when it closes again.
    pub(super) fn acknowledged(
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
    // members it delivered again.
    fn vouched(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, set): (NodeId, NodeId),
        out: &mut Vec<Effect>,
    ) -> Result<Vec<MemberId>, WriteError> {
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
        let members: Vec<_> = marked.iter().map(|(member, _)| *member).collect();
        for (member, range) in marked {
            let proven = (node, member, range, Provenance::Repaired);
            self.redeliver(tree, builder, segments, proven, out)?;
        }
        Ok(members)
    }

    // The member's bytes were rewritten or proved: any refusal of it was
    // given on bytes it no longer holds. The tree takes it back and asks
    // who owns it again; what the refusing object kept goes with it, made
    // over by whatever reads the volumes again, and a member no object
    // takes is a leaf for no reason.
    fn reopen(
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
        self.apply_each(tree, builder, segments, effects, Write::Again, out)
    }

    // Deliver a range of a member again, read back from wherever it went,
    // under what a set proved of it: the one write path then marks it here
    // and in every namespace below. A set read those bytes to rule on them,
    // and reads take held custody first, so bytes that do not read back now
    // are gone: an error, never a proof dropped.
    fn redeliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member, range, provenance): (NodeId, MemberId, ByteRange, Provenance),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut at = range.start;
        while at < range.end {
            let chunk = ByteRange::new(at, range.end.min(at.saturating_add(READ_BACK)));
            at = chunk.end;
            let mut bytes = Vec::new();
            if !self.access(tree).read_at(node, member, chunk, &mut bytes) {
                return Err(WriteError::ProofUnreadable {
                    node,
                    member,
                    range: chunk,
                });
            }
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                // Read back: no evidence comes with it.
                bytes: Bytes::from(bytes).into(),
                provenance,
            };
            self.arrive(tree, builder, segments, Write::new(&arrival), arrival, out)?;
        }
        Ok(())
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
                    recovery.block_sizes(&mut sizes);
                }
            }
            let cuts = block_cuts(range, &sizes);
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
        for set in covering {
            if self
                .acknowledging
                .as_ref()
                .is_some_and(|batch| batch.set == set)
            {
                tree.feed_proven(set, member, &self.segments);
            } else {
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
                Provenance::Damaged | Provenance::Refuted => false,
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

    // Let every set of the namespace match its members again, after the
    // members, their lengths or their first bytes changed. A set that names
    // a member just founded in a namespace bound here, or beside here, moves
    // there first and binds there. A member a set
    // covers for the first time is fed everything that arrived for it
    // before, so early segments are not lost. One recognised by content was
    // renamed in the namespace, and its file follows the new name.
    pub(super) fn bind_recoveries(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut moved = follow_founded(tree, node);
        if let Some((parent, _)) = tree.volumes(node) {
            moved.extend(follow_founded(tree, parent));
        }
        for into in moved.into_iter().filter(|into| *into != node) {
            self.bind_recoveries(tree, builder, segments, into, out)?;
        }
        let sets = tree.recovery_nodes(node);
        if sets.is_empty() {
            return Ok(());
        }
        let members: Vec<MemberId> = tree
            .namespace(node)
            .map(|namespace| namespace.members().map(|member| member.id).collect())
            .unwrap_or_default();
        let mut renamed = std::mem::take(&mut self.renamed);
        renamed.clear();
        for set in sets {
            let newly = tree
                .with_recovery(set, |recovery, namespace| {
                    // A member covered before the bind is not newly
                    // covered by it, so only the others' names before the
                    // bind are kept.
                    let uncovered: Vec<(MemberId, String)> = members
                        .iter()
                        .filter(|member| !recovery.covers(**member))
                        .map(|member| {
                            #[cfg(test)]
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
                self.admit_file(tree, node, *member, length)?;
                if let Some(leaf) = self
                    .files
                    .get_mut(&node)
                    .and_then(|files| files.leaves.get_mut(member))
                {
                    leaf.close(length)?;
                }
            }
        }
        self.renamed = renamed;
        self.apply_each(tree, builder, segments, claims, Write::Again, out)
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
        let access = self.access(tree);
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
        self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        if window.is_some() {
            self.bind_recoveries(tree, builder, segments, parent, out)?;
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
// and repairs; a set made over the posted files stays with them. The
// namespaces it moved into.
fn follow_founded(tree: &mut Tree, node: NodeId) -> Vec<NodeId> {
    let mut moved = Vec::new();
    for set in tree.recovery_nodes(node) {
        let Some(recovery) = tree.recovery(set) else {
            continue;
        };
        if covers_any(tree, node, recovery) {
            continue;
        }
        let mut named = Vec::new();
        recovery.absent(&mut named);
        let founded = tree.namespaces_below(node).into_iter().find(|below| {
            tree.namespace(*below).is_some_and(|namespace| {
                namespace.members().any(|member| {
                    named
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case(&member.name))
                })
            })
        });
        if let Some(into) = founded {
            tree.move_recovery(set, into);
            moved.push(into);
        }
    }
    moved
}

// Where, inside `range`, a block of any of `sizes` begins: the cuts a
// piece of evidence has to be split at for a set that folds blocks of that
// size to take it whole.
fn block_cuts(range: ByteRange, sizes: &[u64]) -> Vec<u64> {
    let mut cuts: Vec<u64> = Vec::new();
    for size in sizes.iter().copied().filter(|size| *size > 0) {
        let mut cut = (range.start / size + 1).checked_mul(size);
        while let Some(at) = cut.filter(|at| *at < range.end) {
            cuts.push(at);
            cut = at.checked_add(size);
        }
    }
    cuts.sort_unstable();
    cuts.dedup();
    cuts
}
