// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Replay first stages posted bytes through the unchanged inverse map on a
// worker. Only a current return may uncover and deliver them. The snapshot
// is evidence to validate, not an immutable copy of the backing files.

use std::sync::atomic::{AtomicBool, Ordering};

use weaver_graph::{Builder, Effect, Posted, Provenance};

use super::super::{Segments, WriteError};
use super::*;

pub(crate) struct Replay {
    pub(crate) node: NodeId,
    pub(crate) stop: Arc<AtomicBool>,
    generation: u64,
    records: BTreeMap<NodeId, weaver_graph::records::NodeRecord>,
    ranges: Vec<(VolumeIndex, ByteRange)>,
    access: Lent,
}

pub(crate) struct Replayed {
    pub(crate) node: NodeId,
    generation: u64,
    ranges: Vec<(VolumeIndex, ByteRange)>,
    records: BTreeMap<NodeId, weaver_graph::records::NodeRecord>,
    backing: Vec<Backing>,
    read: Read,
    bytes: Repaired,
    provenance: Vec<Provenance>,
}

struct Backing {
    node: NodeId,
    member: MemberId,
    bound: Option<(NodeId, VolumeIndex)>,
    leaf: Option<(u64, u64)>,
    scratch: Option<(u64, u64)>,
    coverage: Coverage,
    length: Option<u64>,
}

impl std::fmt::Debug for Replayed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replayed")
            .field("node", &self.node)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl Replay {
    pub(crate) fn run(self, shutdown: &Shutdown) -> Replayed {
        self.run_with(shutdown, || {})
    }

    pub(crate) fn run_with(
        mut self,
        shutdown: &Shutdown,
        mut chunk_read: impl FnMut(),
    ) -> Replayed {
        self.access.shutdown = shutdown.clone();
        let mut provenance = Vec::new();
        let staged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for &(volume, range) in &self.ranges {
                let Some(&(_, member)) = self.access.volumes.get(&volume) else {
                    continue;
                };
                let runs: Vec<_> = self
                    .access
                    .members
                    .get(&(self.access.namespace, member))
                    .map(|taken| {
                        taken
                            .coverage
                            .overlapping(range)
                            .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                            .collect()
                    })
                    .unwrap_or_default();
                for (range, source) in runs {
                    let mut at = range.start;
                    while at < range.end {
                        if self.stop.load(Ordering::Relaxed) || self.access.stopped() {
                            break;
                        }
                        let piece = ByteRange::new(at, range.end.min(at.saturating_add(READ_BACK)));
                        at = piece.end;
                        let mut bytes = Vec::new();
                        if self.access.read(member, piece, &mut bytes)
                            && bytes.len() as u64 == piece.len()
                        {
                            self.access
                                .repaired
                                .write(MemberId(volume.0), piece.start, &bytes);
                            provenance.push(source);
                            chunk_read();
                        }
                    }
                }
            }
        }));
        if staged.is_err() {
            self.access.repaired.failed = Some(LeafError::Io {
                path: self.access.repaired.output.path().to_path_buf(),
                source: std::io::Error::other("posted-input reconstruction worker panicked"),
            });
        }
        let stopped = self.stop.load(Ordering::Relaxed) || self.access.stopped();
        let access = self.access;
        Replayed {
            node: self.node,
            generation: self.generation,
            ranges: self.ranges,
            records: self.records,
            backing: access
                .members
                .iter()
                .map(|((node, member), taken)| Backing {
                    node: *node,
                    member: *member,
                    bound: taken.bound,
                    leaf: taken.revision,
                    scratch: taken.scratch_revision,
                    coverage: taken.coverage.clone(),
                    length: taken.length,
                })
                .collect(),
            read: Read {
                shelves: access.shelves.into_values().collect(),
                leaves: access
                    .members
                    .iter()
                    .filter_map(|((node, member), taken)| {
                        Some((*node, *member, taken.path.clone()?))
                    })
                    .collect(),
                stopped,
            },
            bytes: access.repaired,
            provenance,
        }
    }
}

impl Replayed {
    #[cfg(test)]
    pub(crate) fn chunk_lengths(&self) -> Vec<u64> {
        self.bytes
            .runs
            .iter()
            .map(|(_, _, range)| range.len())
            .collect()
    }

    pub(crate) fn remove(self) -> Result<(), LeafError> {
        self.bytes.remove()
    }
}

impl NamespaceWriter {
    pub(crate) fn rechecks_waiting(&self) -> bool {
        !self.rechecks.is_empty()
    }

    pub(crate) fn queue_rechecks(
        &mut self,
        rechecks: Vec<(NodeId, Vec<(VolumeIndex, ByteRange)>)>,
    ) {
        for (node, ranges) in rechecks {
            let pending = self.rechecks.entry(node).or_default();
            pending.extend(ranges);
            pending.sort_by_key(|(volume, range)| (*volume, range.start));
            let mut merged: Vec<(VolumeIndex, ByteRange)> = Vec::new();
            for (volume, range) in pending.drain(..) {
                if let Some((last_volume, last)) = merged.last_mut()
                    && *last_volume == volume
                    && range.start <= last.end
                {
                    last.end = last.end.max(range.end);
                } else {
                    merged.push((volume, range));
                }
            }
            *pending = merged;
        }
    }

    pub(crate) fn take_replay(&mut self, tree: &Tree) -> Result<Option<Replay>, LeafError> {
        while let Some((node, ranges)) = self.rechecks.pop_first() {
            if !tree.live().contains(&node) {
                continue;
            }
            let Some((namespace, volumes)) = tree.volumes(node) else {
                continue;
            };
            let mut access = Lent {
                namespace,
                // No recovery object is part of this snapshot.
                set: node,
                recovery: None,
                defer_holds: true,
                internal: self.working_dir.join(weaver_graph::names::INTERNAL_DIR),
                members: BTreeMap::new(),
                postings: BTreeMap::new(),
                shelves: BTreeMap::new(),
                volumes: volumes
                    .iter()
                    .map(|(volume, member)| (*volume, (namespace, *member)))
                    .collect(),
                window: None,
                open: RefCell::new(None),
                reads: 0,
                repaired: Repaired::new(&self.working_dir, namespace, node)?,
                found: Vec::new(),
                scratch: None,
                shutdown: Shutdown::default(),
            };
            let mut seen = BTreeSet::new();
            for member in volumes.values() {
                self.take_member(tree, namespace, *member, &mut access, &mut seen);
            }
            return Ok(Some(Replay {
                node,
                stop: Arc::new(AtomicBool::new(false)),
                generation: tree.generation(),
                records: access
                    .postings
                    .keys()
                    .copied()
                    .chain(std::iter::once(node))
                    .filter_map(|node| tree.record(node).map(|record| (node, record)))
                    .collect(),
                ranges,
                access,
            }));
        }
        Ok(None)
    }

    pub(crate) fn replayed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        mut replayed: Replayed,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if let Some(error) = replayed.bytes.failed() {
            let _ = replayed.remove();
            return Err(error.into());
        }
        let changed = tree.changed_since(replayed.generation).any(|node| {
            replayed
                .records
                .get(&node)
                .is_some_and(|before| tree.record(node).as_ref() != Some(before))
        }) || replayed.backing.iter().any(|backing| {
            let files = self.files.get(&backing.node);
            let leaf = files
                .and_then(|files| files.leaves.get(&backing.member))
                .map(Leaf::revision);
            let scratch = files
                .and_then(|files| files.scratch.as_ref())
                .map(Leaf::revision);
            let bound = tree
                .child_of(backing.node, backing.member)
                .filter(|(child, _)| !tree.recovers(*child));
            let verdict_changed = tree
                .namespace(backing.node)
                .and_then(|namespace| namespace.get(backing.member))
                .is_none_or(|record| {
                    record.length != backing.length || record.coverage != backing.coverage
                });
            leaf != backing.leaf
                || scratch != backing.scratch
                || bound != backing.bound
                || verdict_changed
        });
        let moved = replayed
            .read
            .shelves
            .iter()
            .any(|(shelf, generation)| shelf.generation() != *generation)
            || replayed.read.leaves.iter().any(|(node, member, path)| {
                self.files
                    .get(node)
                    .and_then(|files| files.leaves.get(member))
                    .is_none_or(|leaf| leaf.path() != path)
            });
        if replayed.read.stopped || changed || moved {
            if !replayed.read.stopped {
                self.queue_rechecks(vec![(replayed.node, replayed.ranges.clone())]);
            }
            return Ok(replayed.remove()?);
        }
        // All input was captured before the first unposted callback can
        // invalidate a cipher seed, placement or dependent member.
        for ((volume, offset, range), provenance) in
            replayed.bytes.runs.iter().zip(&replayed.provenance)
        {
            let (node, volume, range) = (
                replayed.node,
                VolumeIndex(volume.0),
                ByteRange::at(*offset, range.len()),
            );
            if *provenance == Provenance::Damaged {
                tree.recheck(node, volume, range);
            } else {
                // Verified replacement bytes must face the member's own
                // checksum again, even where an older repair stood.
                tree.uncover(node, volume, range);
            }
        }
        for ((volume, offset, bytes), provenance) in
            replayed.bytes.slices().zip(&replayed.provenance)
        {
            let mut effects = Vec::new();
            tree.replay_piece(
                replayed.node,
                VolumeIndex(volume.0),
                Posted {
                    offset,
                    bytes: bytes?,
                    provenance: *provenance,
                },
                &mut self.placing(builder),
                &mut effects,
            );
            let mut applied = Vec::new();
            self.apply_each(tree, builder, segments, effects, &mut applied)?;
            // The job answers password requests; payload effects have
            // already reached disk and must not accumulate across chunks.
            out.extend(
                applied
                    .into_iter()
                    .filter(|effect| matches!(effect, Effect::NeedsPassword { .. })),
            );
        }
        Ok(replayed.remove()?)
    }
}
