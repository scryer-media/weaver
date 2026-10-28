// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a checkpoint says of the kept envelopes, and the shelves a restore
// puts back from it. The bytes stay in their files; only which ranges of
// them are there is written down, so a restore reopens the files as they
// are instead of starting them over.

use weaver_graph::{Coverage, NodeId, Run};

use super::{Envelopes, Kept, envelope_name};
use crate::pipeline::graph::leaf::{Leaf, LeafError};

// One kept envelope: its node, its number, the runs of it that are on disk,
// its length once its member closed, and whether nothing more will come.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeptRecord {
    pub(crate) node: NodeId,
    pub(crate) envelope: u32,
    pub(crate) runs: Vec<Run>,
    pub(crate) closed: Option<u64>,
    pub(crate) settled: bool,
    // The file it sits in, when not the one its node and number name.
    pub(crate) file: Option<String>,
}

impl Envelopes {
    // The node's kept envelopes, in envelope order.
    pub(crate) fn records(&self, node: NodeId) -> Vec<KeptRecord> {
        let Some(shelf) = self.shelves.get(&node) else {
            return Vec::new();
        };
        let state = shelf.lock();
        state
            .kept
            .iter()
            .map(|(envelope, kept)| KeptRecord {
                node,
                envelope: *envelope,
                runs: kept.coverage.runs().copied().collect(),
                closed: kept.closed,
                settled: kept.settled,
                file: Some(kept.leaf.name())
                    .filter(|name| *name != envelope_name(node, *envelope))
                    .map(str::to_owned),
            })
            .collect()
    }

    // The generation of the kept envelopes: a change to what a checkpoint
    // says of a node's marks it under a later one.
    pub(crate) fn generation(&self) -> u64 {
        self.marks.at()
    }

    // The nodes whose kept envelopes changed since generation `at`.
    pub(crate) fn changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.marks.since(at)
    }

    // An envelope as a checkpoint described it, over the file it left.
    pub(crate) fn restore(&mut self, record: &KeptRecord) -> Result<(), LeafError> {
        let shelf = self.shelf(record.node);
        self.marks.mark(record.node);
        let name = record
            .file
            .clone()
            .unwrap_or_else(|| envelope_name(record.node, record.envelope));
        let leaf = Leaf::at(&shelf.dir, &name);
        let mut coverage = Coverage::new();
        for run in &record.runs {
            coverage.write(run.range, run.provenance);
        }
        shelf.update(|state| {
            state.kept.insert(
                record.envelope,
                Kept {
                    leaf,
                    coverage,
                    closed: record.closed,
                    settled: record.settled,
                    reached: 0,
                },
            );
        });
        Ok(())
    }
}
