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

use bytes::Bytes;
use weaver_graph::recovery::{Recovery, RepairOutcome, SetRuling, SliceVerdict};
use weaver_graph::{
    Builder, ByteRange, Effect, Kind, MemberId, NodeId, Provenance, RecoveryKind, Segment, Tree,
    VolumeIndex,
};

use super::{Arrival, NamespaceWriter, READ_BACK, Segments, WriteError, crc32, index};

// What one set says of the members it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SetState {
    // The tree node the set's object is. One PAR2 object holds every PAR2
    // set of its namespace, so this is per object, not per set id.
    pub(crate) node: NodeId,
    // The namespace it lives in, where the members it covers are.
    pub(crate) namespace: NodeId,
    pub(crate) kind: Kind,
    // Recovery blocks in hand or still on their way.
    pub(crate) blocks_reachable: u64,
    // Slices the evidence in hand says are damaged, over every member the
    // set covers.
    pub(crate) damaged_slices: u64,
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

    // The sets of the namespace that cover the member: the one place a
    // member is matched to the sets that describe it.
    pub(crate) fn covering<'t>(
        &self,
        tree: &'t Tree,
        node: NodeId,
        member: MemberId,
    ) -> impl Iterator<Item = NodeId> + 't {
        tree.recovery_nodes(node).into_iter().filter(move |set| {
            tree.recovery(*set)
                .is_some_and(|recovery| recovery.covers(member))
        })
    }

    // The member is a set's own volume: recovery data, in its own file.
    pub(crate) fn carries_recovery(&self, tree: &Tree, node: NodeId, member: MemberId) -> bool {
        tree.child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child))
    }

    // Every set of the namespace has ruled on something other than waiting:
    // nothing a set of it will rebuild is still to come.
    pub(crate) fn ruled(&self, tree: &Tree, node: NodeId) -> bool {
        tree.recovery_nodes(node).into_iter().all(|set| {
            tree.recovery(set).is_none_or(|recovery| {
                recovery
                    .outcome()
                    .is_some_and(|outcome| outcome != RepairOutcome::Waiting)
            })
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

    // Each set of every namespace, parents first, with the blocks it can
    // reach and the slices it knows are damaged.
    pub(crate) fn states(&self, tree: &Tree) -> Vec<SetState> {
        let mut verdicts = Vec::new();
        self.sets(tree)
            .into_iter()
            .filter_map(|(namespace, node)| {
                let (kind, recovery) = (tree.kind(node)?, tree.recovery(node)?);
                verdicts.clear();
                for member in tree.namespace(namespace)?.members() {
                    if recovery.covers(member.id) {
                        recovery.verdicts(member.id, &mut verdicts);
                    }
                }
                let damaged_slices = verdicts
                    .iter()
                    .filter(|verdict| **verdict == SliceVerdict::Damaged)
                    .count() as u64;
                Some(SetState {
                    node,
                    namespace,
                    kind,
                    blocks_reachable: recovery.blocks_reachable(),
                    damaged_slices,
                    outcome: recovery.outcome(),
                })
            })
            .collect()
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

    // The members the namespace holds its payload for: each index its
    // source declared that still has bytes to come. Evidence cut before an
    // index gives its block sizes is cut at none, so its slices are read
    // back rather than proven as they pass.
    pub(crate) fn holding_for(
        &self,
        declared: impl IntoIterator<Item = MemberId>,
        to_come: impl Fn(MemberId) -> bool,
    ) -> Vec<MemberId> {
        declared
            .into_iter()
            .filter(|member| to_come(*member))
            .collect()
    }

    // Rule the namespace's sets that are due. A file a set describes and
    // no member carries is announced first, once the namespace has every
    // member it will get, so the repair has a member to write it into.
    // `reading` says which namespaces' sources may still announce members.
    pub(crate) fn rule(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, reading): (NodeId, &dyn Fn(NodeId) -> bool),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.announce_absent(tree, builder, segments, (node, reading), out)?;
        let due = self.due(tree, node, reading(node));
        if due.is_empty() {
            return Ok(());
        }
        self.repair(tree, builder, segments, node, &due, out)?;
        Ok(())
    }

    // Ranges of `node`'s volumes that will never arrive. The tree marks them
    // damage on every member they land in, at every depth, and whatever set
    // covers them repairs them. A lost range of a set's own volume is gone
    // for good, since nothing recovers the recovery: the set hears it now,
    // or it waits on the gap, and a walk that never passes it finds none of
    // the blocks beyond. A data object hears its losses only once the job is
    // quiet, as a repair may still bring its bytes back.
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
            }
        }
        if effects.is_empty() {
            return Ok(());
        }
        self.apply_all(tree, builder, segments, effects, out)
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
        let Some(mut object) = tree.take_object(set) else {
            return;
        };
        let mut emits = Vec::new();
        object.volume_lost(volume, range, &mut emits);
        tree.put_object(set, object);
        tree.emitted(set, emits, &mut self.placing(builder), effects);
    }

    // The namespace's sets due to rule, each judged alone. A set that has
    // not ruled rules once every member it covers is closed and the
    // namespace has announced every member it will get, since a volume of
    // its own may still be on its way behind them. A ruling stands until
    // the object takes it back: a wait on recovery bytes ends in the step
    // the object holds what it waited for, or knows it never will, and the
    // set rules again then. It learns what it covers from its own bytes, so
    // one that covers nothing yet rules only once a volume of its own is
    // in: a verdict on nothing would read as settled to every member beside
    // it, and a member it binds after would never be repaired before an
    // engine reads it.
    fn due(&self, tree: &Tree, node: NodeId, reading: bool) -> Vec<NodeId> {
        let Some(namespace) = tree.namespace(node) else {
            return Vec::new();
        };
        let known = members_known(tree, node, reading);
        let mut due = tree.recovery_nodes(node);
        due.retain(|&set| {
            let Some(recovery) = tree.recovery(set) else {
                return false;
            };
            let covers = namespace.members().any(|member| recovery.covers(member.id));
            recovery.outcome().is_none()
                && known
                && (covers || closed_volumes(tree, set) > 0)
                && namespace
                    .members()
                    .all(|member| !recovery.covers(member.id) || member.closed)
        });
        due
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
        let known = |namespace: NodeId| members_known(tree, namespace, reading(namespace));
        if !known(node) || !self.members_in(tree, node) {
            return Ok(());
        }
        let below_known = tree.namespaces_below(node).into_iter().all(known);
        let mut absent = Vec::new();
        for set in tree.recovery_nodes(node) {
            let Some(recovery) = tree.recovery(set) else {
                continue;
            };
            if below_known || covers_any(tree, node, recovery) {
                recovery.absent(&mut absent);
            }
        }
        // A name some member already has is that member's, bound or not.
        if let Some(namespace) = tree.namespace(node) {
            absent.retain(|(name, _)| namespace.members().all(|member| member.name != *name));
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

    // Run the sets that are due to rule, and only those: one ruled before
    // it is due rules on members still arriving and volumes it has not read,
    // and its verdict stands. Each is lent out of the tree for the call, so
    // it reads the members and its own volumes through the tree it lives
    // in. The ranges it repaired come back through the one write path as
    // repaired bytes, so a leaf is overwritten and a bound member's object
    // takes them as any delivery. A member whose bytes a set rewrote or
    // proved is offered again once the sets are done, so whatever refused
    // it on the bytes it held before reads it as it is now. Returns what
    // each set that ruled came to.
    pub(crate) fn repair(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        due: &[NodeId],
        out: &mut Vec<Effect>,
    ) -> Result<Vec<(NodeId, RepairOutcome)>, WriteError> {
        let mut ruled = Vec::new();
        let mut rewritten = std::collections::BTreeSet::new();
        for &set in due {
            let Some(mut object) = tree.take_object(set) else {
                continue;
            };
            let mut access = self.access(tree, node);
            access.recovery = Some(set);
            let outcome = object
                .recovery_mut()
                .map(|recovery| recovery.repair(&mut access));
            let (reads, repaired, found) = (access.reads, access.repaired, access.found);
            if let Some(dir) = access.scratch {
                super::remove_scratch(&dir);
            }
            tree.put_object(set, object);
            self.read_backs += reads;
            ruled.extend(outcome.map(|outcome| (set, outcome)));
            for (member, offset, bytes) in repaired {
                rewritten.insert(member);
                let arrival = Arrival {
                    node,
                    member,
                    offset,
                    bytes,
                    provenance: Provenance::Repaired,
                };
                self.arrive(tree, builder, segments, arrival, out)?;
            }
            for (member, range) in found {
                let marked = (node, member, range, Provenance::Damaged);
                self.redeliver(tree, builder, segments, marked, out)?;
            }
            if matches!(
                outcome,
                Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
            ) {
                rewritten.extend(self.vouched(tree, builder, segments, (node, set), out)?);
            }
        }
        for member in rewritten {
            self.reopen(tree, builder, segments, (node, member), out)?;
        }
        // A repair reads members back, so it may have identified one by its
        // content; the bind that follows gives it the name it is.
        self.bind_recoveries(tree, builder, segments, node, out)?;
        self.release_held(tree, builder, segments, out)?;
        Ok(ruled)
    }

    // A set that found what it covers whole, or made it so, has proven
    // every byte of it, the ones that arrived marked damaged and were good
    // after all among them: it rewrites only the slices that were bad. Every
    // byte still marked damaged is delivered again as repaired, read back
    // from wherever it went, so the marks here and in every namespace below
    // it say what the set proved. Returns the members it delivered again.
    fn vouched(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, set): (NodeId, NodeId),
        out: &mut Vec<Effect>,
    ) -> Result<Vec<MemberId>, WriteError> {
        let marked: Vec<(MemberId, ByteRange)> = tree
            .namespace(node)
            .map(|namespace| {
                namespace
                    .damaged_ranges()
                    .filter(|(member, _)| {
                        self.covering(tree, node, *member)
                            .any(|covering| covering == set)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let members = marked.iter().map(|(member, _)| *member).collect();
        for (member, range) in marked {
            let proven = (node, member, range, Provenance::Repaired);
            self.redeliver(tree, builder, segments, proven, out)?;
        }
        Ok(members)
    }

    // The member's bytes were rewritten or proved: any refusal of it was
    // given on bytes it no longer holds. The tree takes it back and asks
    // who owns it again; what the refusing object kept goes with it, and a
    // member no object takes is a leaf for no reason.
    fn reopen(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        let reopened = tree.reopen(node, member, &mut self.placing(builder), &mut effects);
        if reopened.is_empty() {
            return Ok(());
        }
        let files = self.files_mut(node);
        for volume in &reopened {
            files.refused.remove(volume);
        }
        self.prune(tree)?;
        self.apply_each(tree, builder, segments, effects, out)
    }

    // Deliver a range of a member again, read back from wherever it went,
    // under what a set proved of it: the one write path then marks it here
    // and in every namespace below.
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
            if !self
                .access(tree, node)
                .read_at(node, member, chunk, &mut bytes)
            {
                continue;
            }
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                bytes: Bytes::from(bytes),
                provenance,
            };
            self.arrive(tree, builder, segments, arrival, out)?;
        }
        Ok(())
    }

    // The evidence the grid folds: the segment checksums of the range, to
    // every set that covers the member. Fresh bytes carry the source's
    // checksums as they came. Any other bytes are not the source's, and nor
    // are bytes the source has no checksums for, as in a namespace below the
    // root: they carry checksums of what is in hand, and are evidence unless
    // they are damage.
    pub(super) fn feed(&mut self, tree: &mut Tree, segments: &dyn Segments, arrival: &Arrival) {
        let member = arrival.member;
        let covering: Vec<NodeId> = self.covering(tree, arrival.node, member).collect();
        if covering.is_empty() {
            return;
        }
        let range = arrival.range();
        self.segments.clear();
        segments.segments_for(
            arrival.node,
            VolumeIndex(member.0),
            range,
            &mut self.segments,
        );
        let sourced = !self.segments.is_empty();
        if !sourced {
            // One checksum of the whole range, filled in below.
            self.segments.push(Segment {
                range,
                crc32: 0,
                verified: false,
            });
        }
        if arrival.provenance != Provenance::Fresh || !sourced {
            let verified = arrival.provenance != Provenance::Damaged;
            self.segments.retain_mut(|segment| {
                let Some(piece) = segment.range.intersect(range) else {
                    return false;
                };
                let start = index(piece.start - range.start);
                let end = index(piece.end - range.start);
                *segment = Segment {
                    range: piece,
                    crc32: crc32(&arrival.bytes[start..end]),
                    verified,
                };
                true
            });
        }
        let evidence = &self.segments;
        for set in covering {
            tree.with_recovery(set, |recovery, _| recovery.feed(member, evidence));
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
                Provenance::Damaged => false,
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
                    let before: Vec<(bool, String)> = members
                        .iter()
                        .map(|member| {
                            let name = namespace
                                .get(*member)
                                .map(|record| record.name.clone())
                                .unwrap_or_default();
                            (recovery.covers(*member), name)
                        })
                        .collect();
                    let earlier = renamed.len();
                    recovery.bind(namespace, &mut renamed);
                    let by_content = &renamed[earlier..];
                    members
                        .iter()
                        .zip(before)
                        .filter(|(member, (was, _))| !was && recovery.covers(**member))
                        .map(|(member, (_, name))| {
                            let described = namespace
                                .get(*member)
                                .map(|record| record.name.clone())
                                .unwrap_or_default();
                            let by = if by_content.contains(member) {
                                "content"
                            } else {
                                "name"
                            };
                            (*member, name, described, by)
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
                tree.rename(node, *member, &wanted, &mut effects);
            }
        }
        self.renamed = renamed;
        let (moves, claims): (Vec<Effect>, Vec<Effect>) = effects
            .into_iter()
            .partition(|effect| matches!(effect, Effect::Renamed { .. }));
        self.moved(&moves)?;
        self.apply_each(tree, builder, segments, claims, out)
    }

    // An object found its own recovery data at a range of its volume. The
    // set for it is built over that range of the member the volume is, as a
    // window beside the object, so it lives in the namespace of the members
    // it covers. What of the range has passed is read back through the
    // inverse map and handed to it; the rest reaches it as it passes. Data
    // no object here reads is only recorded.
    pub(super) fn recovery_at(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, volume, range): (NodeId, VolumeIndex, ByteRange),
        kind: RecoveryKind,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Some((parent, member)) = tree
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return Ok(());
        };
        let Some(object) = kind.object() else {
            if !self.unusable.contains(&(parent, member)) {
                self.unusable.push((parent, member));
            }
            return Ok(());
        };
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
        let access = self.access(tree, parent);
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
            object,
            passed,
            &mut self.placing(builder),
            &mut effects,
        );
        self.apply_each(tree, builder, segments, effects, out)?;
        if window.is_some() {
            self.bind_recoveries(tree, builder, segments, parent, out)?;
        }
        Ok(())
    }
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
