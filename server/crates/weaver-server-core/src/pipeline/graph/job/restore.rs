// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job back from its checkpoint. The promise is no refetch and no
// second pass over the data: what the checkpoint says is on disk is taken
// as there, and only what is not there is fetched. In the plan's order:
//
//   2. every node is rebuilt from its facts, and each node that was offered
//      a candidate is offered it again, so its keys are derived anew; a
//      member's file folds its check from the decoder's checksums
//   3. every article the root had in hand is judged on the records and the
//      files' lengths, never read: one whose bytes are not where they say
//      is fetched again
//   4. each member's front, which no record keeps, is read again alone, as
//      far as it came; an object whose facts leave out what it read of its
//      volumes asks for those bytes again itself, and an engine's run reads
//      its volumes from its files as it decodes them
//   5. the engines' runs are asked again for their slots; holds the
//      checkpoint recorded on scratch were adopted before step 3, and holds
//      that were in memory are fetched again
//   6. what the root still waits on is what the queue fetches
//
// Step 1, refusing a graph job with no checkpoint, is the caller's.

use weaver_graph::{MemberId, NodeId, RestoreError, Tree, VolumeIndex};

use super::loans::Loans;
use super::passwords::Passwords;
use super::{GraphError, GraphJob, RootFile, Storage};
use crate::pipeline::graph::checkpoint::{Pending, Snapshot};
use crate::pipeline::graph::engine::Runs;
use crate::pipeline::graph::namespace::{NamespaceWriter, WriteError};
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

impl GraphJob {
    pub(crate) fn restore(
        snapshot: &Snapshot,
        files: &[RootFile],
        scope: u64,
        storage: Storage,
        limit: usize,
    ) -> Result<Restored, RestoreFailure> {
        let mut tree = Tree::rebuild(&snapshot.tree, &storage.hardware)?;
        tree.nest_at_most(storage.nesting);
        let root = ArticleRoot::restore(files, &snapshot.root)?;
        same_post(&tree, files)?;
        let windows = storage.windows(scope);
        let runs = Runs::new(scope, storage.slots, storage.mail, storage.decode)
            .on(storage.hardware)
            .lending(windows);
        let mut writer = NamespaceWriter::new(
            scope,
            storage.working_dir,
            storage.accountant,
            storage.free_disk,
        )
        .delivering(storage.output)
        .capped(storage.caps)
        .metered(storage.metrics);
        writer.restore_verifications(&snapshot.root.pending_verifications);
        writer.restore_members(&tree);
        for ((node, member), state) in &snapshot.files {
            if tree
                .namespace(*node)
                .and_then(|found| found.get(*member))
                .is_none()
            {
                continue;
            }
            writer.restore_file(&tree, *node, *member, state, &root)?;
        }
        // A file that still sits at a name it is moving off keeps that name
        // from every other member until it has moved.
        for (node, member, name) in writer.waiting() {
            tree.leaving(node, member, &name);
        }
        let mut settled = Vec::new();
        tree.settle_names(&mut settled);
        writer.moved(&mut tree, &settled)?;
        writer.restore_read_backs(snapshot.root.read_backs);
        for kept in &snapshot.kept {
            writer.restore_kept(kept)?;
        }
        writer.restore_holds(&tree, &snapshot.holds)?;
        // The deadline counts the time the runs and the loans decode. What
        // a run before spent is not in the checkpoint: it counts again.
        let runs = runs.timed(writer.decode_clock());
        let mut job = Self {
            tree,
            root,
            builder: Box::new(runs.hardware()),
            writer,
            loans: Loans::new(runs.scope(), runs.mail(), runs.hardware()),
            works: super::works::Works::new(runs.scope(), runs.mail(), runs.hardware())
                .timed(runs.clock()),
            runs,
            passwords: Passwords::new(storage.passwords),
            reported: snapshot.root.reported,
            pending: Pending::default(),
            mail_owed: None,
            commit_held: None,
            notes: Vec::new(),
            fed: super::runs::Fed::default(),
            sealed_at: None,
        };
        for (node, index) in &snapshot.passwords {
            job.offer_again(*node, *index)?;
        }
        let rejected = job.tree.rejected_inputs(
            NodeId::ROOT,
            job.namespace().members().map(|member| member.id),
        );
        let refetched = job.reach()?;
        job.fronts_again()?;
        let mut effects = Vec::new();
        job.tree
            .restored(&mut job.writer.placing(&mut *job.builder), &mut effects);
        job.settle(effects)?;
        job.lift_hold_refusals()?;
        job.writer.queue_rechecks(rejected);
        job.mark_damage()?;
        let mut effects = Vec::new();
        job.tree
            .close_again(&mut job.writer.placing(&mut *job.builder), &mut effects);
        job.settle(effects)?;
        for volume in 0..job.root.map.volume_count() {
            job.after_member(VolumeIndex(volume as u32))?;
        }
        job.repair()?;
        job.step_runs(Vec::new(), limit)?;
        // What the restore closed again landed before the restart, and was
        // said then.
        job.notes.clear();
        #[cfg(test)]
        job.pending.restored(&job);
        Ok(Restored {
            settled: job.root.map.settled(),
            refetched,
            job,
        })
    }

    // Refusals an older build gave when holds ran past their budgets are
    // taken back, and their sets read their volumes again from their files.
    fn lift_hold_refusals(&mut self) -> Result<(), GraphError> {
        let mut out = Vec::new();
        self.writer
            .lift_hold_refusals(&mut self.tree, &mut *self.builder, &self.root, &mut out)?;
        self.heard(out)
    }

    // What an older build left when a member's holds ran past their
    // budgets: the object bound over the first root member that has one
    // refuses it for its holds, as that build's object said.
    #[cfg(test)]
    pub(crate) fn refuse_for_holds(&mut self) -> Result<NodeId, GraphError> {
        let node = self
            .tree
            .namespace(NodeId::ROOT)
            .and_then(|root| {
                root.records()
                    .find_map(|record| self.tree.child_of(NodeId::ROOT, record.id))
            })
            .map(|(node, _)| node)
            .expect("an object bound over a root member");
        let mut out = Vec::new();
        self.writer.said(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            node,
            weaver_graph::Emit::Refuse(weaver_graph::Why::HoldsBreached),
            &mut out,
        )?;
        self.heard(out)?;
        Ok(node)
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

    // Every article the root had in hand is where the checkpoint says, as
    // far as the records and the files' lengths tell: nothing is read, and
    // nothing is delivered again. What does not reach is the root's to fetch
    // again: the article waits, and the bytes it covered are no longer
    // covered, or said at any depth they reached. A file cut shorter than
    // its record says loses what it no longer holds this way. Holds the
    // checkpoint recorded on scratch reach as files do; holds that were in
    // memory are on no disk, and their articles are fetched again.
    fn reach(&mut self) -> Result<Vec<(VolumeIndex, u32)>, GraphError> {
        let mut missing = Vec::new();
        let mut scratch = Vec::new();
        let access = self.writer.access(&self.tree);
        for index in 0..self.root.map.volume_count() {
            let volume = VolumeIndex(index as u32);
            let member = MemberId(volume.0);
            for (ordinal, range) in self.root.map.arrived_ranges(volume) {
                if !access.reaches(NodeId::ROOT, member, range, &mut scratch) {
                    missing.push((volume, ordinal));
                }
            }
        }
        missing.extend(self.root.map.unseen());
        let mut refetched = Vec::with_capacity(missing.len());
        // Nothing is lent inside a restore: the job lends once installed.
        for (volume, ordinal) in missing {
            self.unarrive(volume, ordinal, 0)?;
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

    // Each member's front that came and is not in hand, read alone from
    // where its bytes went, the runs that arrived inside its first
    // FIRST_BYTES, and handed to the tree, which judges it as a front that
    // came. A run that does not read is left away, as its bytes are fetched
    // again.
    fn fronts_again(&mut self) -> Result<(), GraphError> {
        for (node, member, runs) in self.tree.fronts_away() {
            let access = self.writer.access(&self.tree);
            let pieces: Vec<(u64, Vec<u8>)> = runs
                .into_iter()
                .filter_map(|run| {
                    let mut bytes = Vec::new();
                    access
                        .read_at(node, member, run, &mut bytes)
                        .then_some((run.start, bytes))
                })
                .collect();
            if pieces.is_empty() {
                continue;
            }
            let mut effects = Vec::new();
            self.tree.front_again(
                node,
                member,
                &pieces,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            self.writer.admit_front(&self.tree, node, member)?;
            self.settle(effects)?;
        }
        Ok(())
    }

    // The articles a crash now would cost, so a restore fetches them again:
    // in hand but not on disk, on disk but not in the checkpoint the
    // database will hold once the writes already queued for it run, or
    // heard of before the root saw them.
    #[cfg(test)]
    pub(crate) fn in_memory(&self) -> Vec<(u32, u32)> {
        let named = self.pending.named();
        let mut out = Vec::new();
        self.arrived(|article, on_disk| {
            if !on_disk || !named.contains(&article) {
                out.push(article);
            }
        });
        out.extend(
            self.root
                .map
                .unseen()
                .into_iter()
                .map(|(volume, ordinal)| (volume.0, ordinal)),
        );
        out
    }

    // The articles the root has in hand whose bytes are on disk: what a
    // checkpoint taken now names.
    #[cfg(test)]
    pub(crate) fn on_disk(&self) -> std::collections::BTreeSet<(u32, u32)> {
        let mut out = std::collections::BTreeSet::new();
        self.arrived(|article, on_disk| {
            if on_disk {
                out.insert(article);
            }
        });
        out
    }

    // Each article the root has in hand, and whether its bytes read back
    // from disk, in the root's order. Every checkpoint asks this of every
    // article, so articles that lie end to end are read back
    // as one run, and only a run that does not read back is halved until
    // each article in it is answered alone.
    #[cfg(test)]
    fn arrived(&self, mut each: impl FnMut((u32, u32), bool)) {
        let mut bytes = Vec::new();
        for index in 0..self.root.map.volume_count() {
            let volume = VolumeIndex(index as u32);
            let articles = self.root.map.arrived_ranges(volume);
            for run in articles.chunk_by(|(_, a), (_, b)| a.end == b.start) {
                self.reads_back(volume, run, &mut bytes, &mut each);
            }
        }
    }

    // Whether each article of `run`, which lie end to end, reads back.
    #[cfg(test)]
    fn reads_back(
        &self,
        volume: VolumeIndex,
        run: &[(u32, weaver_graph::ByteRange)],
        bytes: &mut Vec<u8>,
        each: &mut impl FnMut((u32, u32), bool),
    ) {
        let whole = weaver_graph::ByteRange::new(run[0].1.start, run[run.len() - 1].1.end);
        bytes.clear();
        let read = self
            .writer
            .read_posted(&self.tree, MemberId(volume.0), whole, bytes);
        if read || run.len() == 1 {
            for (ordinal, _) in run {
                each((volume.0, *ordinal), read);
            }
            return;
        }
        let (front, back) = run.split_at(run.len() / 2);
        self.reads_back(volume, front, bytes, each);
        self.reads_back(volume, back, bytes, each);
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
