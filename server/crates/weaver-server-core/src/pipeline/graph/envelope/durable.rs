// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a checkpoint says of the kept envelopes, and the shelves a restore
// puts back from it. The bytes stay in their files; only which ranges of
// them are there is written down, so a restore reopens the files as they
// are instead of starting them over.

use weaver_graph::{Coverage, NodeId, Run};

use super::{Envelopes, Kept};
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
}

impl Envelopes {
    pub(crate) fn records(&self) -> Vec<KeptRecord> {
        let mut out = Vec::new();
        for (node, shelf) in &self.shelves {
            let state = shelf.lock();
            for (envelope, kept) in &state.kept {
                out.push(KeptRecord {
                    node: *node,
                    envelope: *envelope,
                    runs: kept.coverage.runs().copied().collect(),
                    closed: kept.closed,
                    settled: kept.settled,
                });
            }
        }
        out
    }

    // An envelope as a checkpoint described it, over the file it left.
    pub(crate) fn restore(&mut self, record: &KeptRecord) -> Result<(), LeafError> {
        let shelf = self.shelf(record.node);
        let name = format!(
            ".weaver-chunks/graph-envelope-{}-{}",
            record.node.0, record.envelope
        );
        let leaf = Leaf::at(&shelf.dir, &name)?;
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
