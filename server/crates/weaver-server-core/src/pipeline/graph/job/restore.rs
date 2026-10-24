// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job back from its checkpoint. The promise is no refetch: what the
// checkpoint says is on disk is read back and delivered down the tree again,
// and only what is not there is fetched. In the plan's order:
//
//   2. every node is rebuilt from its facts, and each node that was offered
//      a candidate is offered it again, so its keys are derived anew
//   3. every article the root had in hand is read back through the inverse
//      map and checked against the decoder's checksums; one that reads but
//      does not match is damaged, one that does not read is fetched again
//   4. each article read is delivered down the tree before the next is
//      read, as it first came, so every object is where it was and a stream
//      replays from its start
//   5. the engines' runs are asked again for their slots; holds were in
//      memory and are fetched again
//   6. what the root still waits on is what the queue fetches
//
// Step 1, refusing a graph job with no checkpoint, is the caller's.

use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::{ByteRange, MemberId, NodeId, Provenance, RestoreError, Tree, VolumeIndex};

use super::loans::Loans;
use super::passwords::Passwords;
use super::{GraphError, GraphJob, RootFile, Storage};
use crate::pipeline::graph::checkpoint::{Pending, Snapshot};
use crate::pipeline::graph::engine::Runs;
use crate::pipeline::graph::namespace::{Arrival, NamespaceWriter, WriteError};
use crate::pipeline::graph::root::ArticleRoot;
use crate::pipeline::graph::root::facts::RootFactsError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RestoreFailure {
    #[error(transparent)]
    Tree(#[from] RestoreError),
    #[error(transparent)]
    Root(#[from] RootFactsError),
    #[error("root member {0} is not one of the post's files")]
    Post(u32),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    Step(#[from] GraphError),
}

pub(crate) struct Restored {
    pub(crate) job: GraphJob,
    // Articles this run does not fetch: read back, or lost for good.
    pub(crate) settled: Vec<(VolumeIndex, u32)>,
    // Articles the checkpoint had that were not on disk.
    pub(crate) refetched: Vec<(VolumeIndex, u32)>,
}

// An article read back, and how each of its pieces is delivered again.
struct ReadBack {
    volume: VolumeIndex,
    pieces: Vec<(ByteRange, Bytes, Provenance)>,
}

impl GraphJob {
    pub(crate) fn restore(
        snapshot: &Snapshot,
        files: &[RootFile],
        scope: u64,
        storage: Storage,
        limit: usize,
    ) -> Result<Restored, RestoreFailure> {
        let mut tree = Tree::rebuild(&snapshot.tree, &weaver_objects::Objects)?;
        tree.nest_at_most(storage.nesting);
        let root = ArticleRoot::restore(files, &snapshot.root)?;
        same_post(&tree, files)?;
        let runs = Runs::new(
            scope,
            storage.slots,
            storage.mail,
            Arc::clone(&storage.accountant),
        );
        let mut writer = NamespaceWriter::new(
            scope,
            storage.working_dir,
            storage.accountant,
            storage.free_disk,
        )
        .capped(storage.caps);
        writer.restore_members(&tree);
        for ((node, member), state) in &snapshot.files {
            if tree
                .namespace(*node)
                .and_then(|found| found.get(*member))
                .is_none()
            {
                continue;
            }
            writer.restore_file(&tree, *node, *member, *state)?;
        }
        writer.restore_read_backs(snapshot.root.read_backs);
        for kept in &snapshot.kept {
            writer.restore_kept(kept)?;
        }
        let mut job = Self {
            tree,
            root,
            builder: Box::new(weaver_objects::Objects),
            writer,
            loans: Loans::new(runs.scope(), runs.mail()),
            runs,
            passwords: Passwords::new(storage.passwords),
            reported: snapshot.root.reported,
            pending: Pending::default(),
        };
        for (node, index) in &snapshot.passwords {
            job.offer_again(*node, *index)?;
        }
        let refetched = job.read_back()?;
        job.mark_damage()?;
        for volume in 0..job.root.map.volume_count() {
            job.after_member(VolumeIndex(volume as u32))?;
        }
        job.repair()?;
        job.step_runs(Vec::new(), limit)?;
        Ok(Restored {
            settled: job.root.map.settled(),
            refetched,
            job,
        })
    }

    // The node was offered this candidate last: it is offered again, so the
    // object derives its keys from it as it did before.
    fn offer_again(&mut self, node: NodeId, index: usize) -> Result<(), GraphError> {
        let Some(secret) = self.passwords.resume_at(node, index).map(str::to_owned) else {
            return Ok(());
        };
        let mut effects = Vec::new();
        self.tree.offer_password(
            node,
            &secret,
            &mut self.writer.placing(&mut *self.builder),
            &mut effects,
        );
        self.settle(effects)
    }

    // Every article the root had in hand, read back and delivered before the
    // next is read: an object's inverse map for a later article can lean on
    // what the earlier ones rebuilt, as a cipher's chain does. What does not
    // read is the root's to fetch again: the article waits, and the bytes
    // it covered are no longer covered, or said at any depth they reached.
    // A restore starts with no holds, so any it reads through are bytes an
    // earlier article's delivery moved there from disk, as a member bound
    // by its bytes moves its file.
    fn read_back(&mut self) -> Result<Vec<(VolumeIndex, u32)>, GraphError> {
        let mut missing = Vec::new();
        let mut bytes = Vec::new();
        for index in 0..self.root.map.volume_count() {
            let volume = VolumeIndex(index as u32);
            let member = MemberId(volume.0);
            for (ordinal, range) in self.root.map.arrived_ranges(volume) {
                bytes.clear();
                if !self
                    .writer
                    .access(&self.tree)
                    .read_at(NodeId::ROOT, member, range, &mut bytes)
                {
                    missing.push((volume, ordinal, Some(range)));
                    continue;
                }
                let read = Bytes::copy_from_slice(&bytes);
                let pieces = self.pieces(volume, range, &read);
                self.deliver_again(ReadBack { volume, pieces })?;
            }
        }
        missing.extend(
            self.root
                .map
                .unseen()
                .into_iter()
                .map(|(volume, ordinal)| (volume, ordinal, None)),
        );
        let mut refetched = Vec::with_capacity(missing.len());
        for (volume, ordinal, range) in missing {
            self.root.map.unarrive(volume, ordinal);
            if let Some(range) = range {
                self.tree.uncover(NodeId::ROOT, volume, range);
            }
            if let Some(found) = self
                .tree
                .namespace_mut(NodeId::ROOT)
                .and_then(|namespace| namespace.get_mut(MemberId(volume.0)))
            {
                found.closed = false;
            }
            refetched.push((volume, ordinal));
        }
        Ok(refetched)
    }

    // The article's bytes cut at its coverage runs, each with the
    // provenance it arrived with. A fresh piece that no longer matches the
    // decoder's checksums is damaged: it is repaired, not fetched.
    fn pieces(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        bytes: &Bytes,
    ) -> Vec<(ByteRange, Bytes, Provenance)> {
        let Some(member) = self
            .tree
            .namespace(NodeId::ROOT)
            .and_then(|namespace| namespace.get(MemberId(volume.0)))
        else {
            return Vec::new();
        };
        member
            .coverage
            .overlapping(range)
            .filter_map(|run| {
                let piece = run.range.intersect(range)?;
                let from = (piece.start - range.start) as usize;
                let slice = bytes.slice(from..from + piece.len() as usize);
                let provenance = match run.provenance {
                    Provenance::Fresh if !self.root.matches(volume, piece, &slice) => {
                        Provenance::Damaged
                    }
                    provenance => provenance,
                };
                Some((piece, slice, provenance))
            })
            .collect()
    }

    // The articles a restart now would fetch again: in hand but not on disk,
    // or heard of before the root saw them.
    #[cfg(test)]
    pub(crate) fn in_memory(&self) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        let mut bytes = Vec::new();
        for index in 0..self.root.map.volume_count() {
            let volume = VolumeIndex(index as u32);
            for (ordinal, range) in self.root.map.arrived_ranges(volume) {
                bytes.clear();
                if !self
                    .writer
                    .read_posted(&self.tree, MemberId(volume.0), range, &mut bytes)
                {
                    out.push((volume.0, ordinal));
                }
            }
        }
        out.extend(
            self.root
                .map
                .unseen()
                .into_iter()
                .map(|(volume, ordinal)| (volume.0, ordinal)),
        );
        out
    }

    fn deliver_again(&mut self, read: ReadBack) -> Result<(), GraphError> {
        for (range, bytes, provenance) in read.pieces {
            let mut out = Vec::new();
            self.writer.deliver(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                Arrival {
                    node: NodeId::ROOT,
                    member: MemberId(read.volume.0),
                    offset: range.start,
                    bytes,
                    provenance,
                },
                &mut out,
            )?;
            self.heard(out)?;
        }
        Ok(())
    }
}

// A root member the post carries is the post's file of its number: its
// identity is that index, never its name, which a recovery set's rename
// changes. That the post is the one checkpointed, by its names, is the
// root's facts' check. The members past the post's files are files a
// recovery set describes, which the root announced itself after them; a
// further posting is the post's, so none of them is one.
fn same_post(tree: &Tree, files: &[RootFile]) -> Result<(), RestoreFailure> {
    let Some(namespace) = tree.namespace(NodeId::ROOT) else {
        return Err(RestoreFailure::Post(0));
    };
    match namespace.records().find(|member| {
        member.id.0 as usize >= files.len() && namespace.resolve(member.id) != member.id
    }) {
        Some(member) => Err(RestoreFailure::Post(member.id.0)),
        None => Ok(()),
    }
}
