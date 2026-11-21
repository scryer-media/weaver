// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member an object claimed after it had been decided a leaf: a volume that
// came before the one that built its object, or one whose front only said
// what it is once a repair rewrote it. Its bytes went to its own file, so
// they go down to its object again from there, each run with the
// provenance it arrived with, by the one write path, exactly as the bytes
// held for an undecided member go down once it is bound. The file is no
// delivery of its own after that, unless the object is a recovery object,
// whose volumes live in their files.

use std::sync::Arc;
use weaver_server_disk::operations::disk::volume::LoopSite;

use bytes::Bytes;
use weaver_graph::{ByteRange, Coverage, Effect, MemberId, NodeId, Tree};

use super::refusal::{kept_as_volume, outside};
use super::{Arrival, NamespaceWriter, READ_BACK, Segments, WriteError};
use crate::pipeline::graph::envelope::{envelope_name, free_name};

impl NamespaceWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn replay_file(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn weaver_graph::Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        said: Option<&Coverage>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // A member with no file has nothing in one: what it received is in
        // its holds. A member with a file has every byte that arrived for it
        // in the file, so a run the file does not give back ends the write
        // with the file's error, a file gone with it included. Each chunk
        // goes by the coverage and the file as they are then: routing a
        // run can name the member and settle its length, which moves and
        // trims the file and the coverage behind it. A loss records no bytes,
        // even when a sparse file extends across it: only runs that arrived
        // go down. Damage past the file's end is not read. The file goes to
        // the owner the member has now: if a route changes that owner, the
        // hand-off carries the whole file to whatever holds the member next,
        // so this replay is done. A folded posting's runs
        // are `said`, its record from before it folded, which no route
        // moves.
        if !self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member))
        {
            return self.release_owned_elsewhere(tree, node, member);
        }
        // A member can have both: a file a refusal read it back into, and
        // holds for what came after an object took it again. What the holds
        // carry never reached the file, so it goes on from the holds, and
        // the file is read only around them, and around what the delivery
        // that bound the member gave its object, which no file holds either.
        self.share_file(tree, node, member, said);
        let held = self.not_in_file(tree, node, member);
        let owner = tree.child_of(node, member);
        let mut at = 0;
        while tree.child_of(node, member) == owner
            && let Some(run) = said
                .or_else(|| {
                    tree.namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .map(|record| &record.coverage)
                })
                .and_then(|coverage| {
                    coverage
                        .overlapping(ByteRange::new(at, u64::MAX))
                        .next()
                        .copied()
                })
        {
            if !run.provenance.arrived() {
                at = run.range.end;
                continue;
            }
            let leaf = self
                .files
                .get(&node)
                .and_then(|files| files.leaves.get(&member))
                .ok_or(WriteError::UnknownMember { node, member })?;
            let end = if run.provenance.is_damaged() {
                run.range.end.min(leaf.len(LoopSite::ReplayFile)?)
            } else {
                run.range.end
            };
            let start = run.range.start.max(at);
            if start >= end {
                at = run.range.end;
                continue;
            }
            if let Some(hold) = held.iter().find(|hold| hold.contains(start)) {
                at = hold.end;
                continue;
            }
            let end = held
                .iter()
                .map(|hold| hold.start)
                .filter(|from| *from > start)
                .fold(end, u64::min);
            let chunk = ByteRange::new(start, end.min(start.saturating_add(READ_BACK)));
            at = chunk.end;
            let mut bytes = Vec::new();
            leaf.read(
                LoopSite::ReplayFile,
                chunk.start,
                super::index(chunk.len()),
                &mut bytes,
            )?;
            // The file the chunk came from, asked at each read: an envelope
            // whose file is that very file takes the chunk where it was
            // read without writing it over itself.
            let read_from = leaf
                .file_id(LoopSite::ReplayFile)
                .map(|file| super::FromFile {
                    node,
                    member,
                    range: chunk,
                    file,
                });
            let arrival = Arrival {
                node,
                member,
                offset: chunk.start,
                bytes: Bytes::from(bytes).into(),
                provenance: run.provenance,
            };
            let outer = std::mem::replace(&mut self.from_file, read_from);
            let routed = self.route_as_brought(tree, builder, segments, arrival, out);
            self.from_file = outer;
            routed?;
        }
        self.release_owned_elsewhere(tree, node, member)
    }

    // An object that keeps the member's volume whole, as one envelope at the
    // volume's own offsets, keeps exactly what the member's file holds, so
    // the envelope starts in the file itself, under a second name, so the
    // volume's bytes are on disk once. What goes down to it from the file
    // after lands where it was read, and is kept there without a write
    // while the envelope's file is that very file; the object hears every
    // run of it all the same. Bytes from anywhere else, the holds or a
    // repair, are written as always. The member's own name goes once the
    // checkpoint that stops naming it commits: a restore before then finds
    // the file under that name, and the second name, which no checkpoint
    // names yet, is swept. An object that places the bytes any other way, a
    // recovery object, whose volumes stay in their files, an envelope begun
    // already, or a file system that gives a file no second name, leaves the
    // file as it is, and what goes down is written afresh.
    pub(super) fn share_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        said: Option<&Coverage>,
    ) {
        let Some((child, volume)) = tree.child_of(node, member) else {
            return;
        };
        let Some((files, coverage)) = self.files.get(&node).zip(said.or_else(|| {
            tree.namespace(node)
                .and_then(|namespace| namespace.get(member))
                .map(|record| &record.coverage)
        })) else {
            return;
        };
        let Some(leaf) = files.leaves.get(&member) else {
            return;
        };
        let held = self.not_in_file(tree, node, member);
        let ranges: Vec<ByteRange> = coverage
            .runs()
            .flat_map(|run| outside(run.range, &held))
            .collect();
        let Some(envelope) = kept_as_volume(tree, (child, volume), &ranges).filter(|envelope| {
            !tree.recovers(child) && self.envelopes.kept_file(child, *envelope).is_none()
        }) else {
            return;
        };
        let Some(disk) = &self.disk else {
            let name = free_name(&self.working_dir, &envelope_name(child, envelope));
            let link = self.working_dir.join(&name);
            let linked = second_name(leaf.path(), &link);
            if linked && !self.envelopes.start_in(child, envelope, &name, true) {
                let _ = std::fs::remove_file(&link);
            }
            return;
        };
        // The second name is given by the writer of the working directory's
        // volume, behind everything submitted for the file, so the loop
        // never waits for the file to lie as said. Only a file that writer
        // writes can be given it: one on another volume has no second name
        // here, and its bytes are written afresh.
        let to = Arc::clone(disk.working());
        if !leaf.on_disk() || !Arc::ptr_eq(&to, disk.under(leaf.root())) {
            return;
        }
        let name = self.envelopes.given().free(
            Some(&to),
            &self.working_dir,
            &envelope_name(child, envelope),
        );
        let link = self.working_dir.join(&name);
        let (from, at) = (leaf.path().to_path_buf(), link.clone());
        // Where the file system gives a file no second name, an empty file
        // is made at the name and nothing fails: the bytes that go down to
        // the envelope after, which are all of them, are written afresh as
        // any envelope's are. The envelope's leaf is told its file is there,
        // and it is either way, so a move, a length read or a removal that
        // comes before the first of those bytes finds a file.
        let made = to.run(None, link.clone(), move || {
            second_name_or_empty(&from, &at);
            Ok(())
        });
        if !self
            .envelopes
            .start_after(child, envelope, &name, true, made)
        {
            to.run(None, link.clone(), move || {
                match std::fs::remove_file(&link) {
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
                    _ => Ok(()),
                }
            });
        }
    }

    // The file is no delivery of its own once a non-recovery object owns the
    // member.
    fn release_owned_elsewhere(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        let owned_elsewhere = tree
            .child_of(node, member)
            .is_some_and(|(child, _)| !tree.recovers(child));
        if owned_elsewhere {
            let files = self.files_mut(node);
            files.unrefuse(member);
            if let Some(leaf) = files.leaves.remove(&member) {
                self.discard(leaf)?;
            }
        }
        Ok(())
    }
}

// Give the file at `from` the second name `at`. Whether it has it: a file
// system that gives a file no second name leaves nothing at `at`.
fn second_name(from: &std::path::Path, at: &std::path::Path) -> bool {
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    if refused::SECOND_NAMES.get() {
        return false;
    }
    at.parent()
        .map_or(Ok(()), std::fs::create_dir_all)
        .and_then(|()| std::fs::hard_link(from, at))
        .is_ok()
}

// As `second_name`, for a name that must be a file after: where the file
// system gives no second name, an empty file is made there. A disk that
// cannot make even that says so on the first write to the file.
pub fn second_name_or_empty(from: &std::path::Path, at: &std::path::Path) {
    if second_name(from, at) {
        return;
    }
    let _ = at
        .parent()
        .map_or(Ok(()), std::fs::create_dir_all)
        .and_then(|()| {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(at)
        });
}

// A test's file system that gives no file a second name: on the test's own
// thread only, so a test that sets it never reaches another running beside it.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub mod refused {
    thread_local! {
        pub static SECOND_NAMES: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }
}
