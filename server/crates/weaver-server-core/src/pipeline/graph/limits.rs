// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's limits, from the settings that already exist. The
// engine adds no settings of its own: an operator who tuned the direct
// store's holds, the extraction concurrency or the extraction limits gets
// the same limits here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::warn;
use weaver_graph::holds::HoldBudgets;
use weaver_graph::{MemberId, NodeId, Tree};

use crate::RuntimeTuner;
use crate::operations::disk::{Capacity, CapacityDebits};
use crate::pipeline::completion::finalize::MAX_GRAPH_NESTING_DEPTH;
use crate::pipeline::direct_store::DirectStoreSettings;
pub(crate) use crate::pipeline::extraction::ExtractionLimits;
use crate::pipeline::extraction::{ExtractionFailure, ExtractionRejectionReason};
use crate::pipeline::graph::job::RootFile;
use crate::pipeline::graph::leaf::Leaf;
use crate::post_processing::model::{PostProcessingSettings, unwanted_extension_reason};

// The holds budgets and the number of slots.
//
//   resident_limit   holds_resident_limit_bytes, process-wide RAM, as before
//   scratch_total    holds_scratch_total_bytes, process-wide scratch, as before
//   scratch_ceiling  holds_scratch_ceiling_bytes, which bounded one set's
//                    scratch and now bounds one namespace's
//   disk_reserve     holds_disk_reserve_bytes, zero still disables it
//   slots            the extraction concurrency the tuner allows, which
//                    already bounds streaming member extractions
pub(crate) fn budgets(holds: &DirectStoreSettings, tuner: &RuntimeTuner) -> (HoldBudgets, usize) {
    let budgets = HoldBudgets {
        resident_limit: holds.holds_resident_limit_bytes,
        scratch_total: holds.holds_scratch_total_bytes,
        scratch_ceiling: holds.holds_scratch_ceiling_bytes,
        disk_reserve: holds.holds_disk_reserve_bytes,
    };
    (budgets, tuner.max_concurrent_extractions())
}

// How deep a graph job's namespaces may nest, handed to its tree beside the
// hold budgets. An object that would open one deeper is never built: its
// member is refused as the container it is, and the job goes on.
pub(crate) const NESTING: u32 = MAX_GRAPH_NESTING_DEPTH;

// The job's extraction budget: the limits classic's extraction enforces, one
// budget per job, read where the namespace writes. Every byte an object
// produces counts, at any depth; the posts' own bytes are the job's input,
// so they count only as what the ratio is taken over. The first breach
// stands: every later check answers with it, so nothing more of the job is
// written, and what is already written stays.
//
//   job bytes, ratio   bytes objects produced, against the job byte cap or
//                      the ratio over the bytes posted for the objects
//   member bytes       one member's extent
//   entries            members announced below the root
//   deadline           the job's wall clock since an object first produced
//                      output: extraction's own time, however long the
//                      posts or pieces took to arrive
//   disk reserve       the free space a write would leave, against the
//                      reading the holds already take
//   content policy     the operator's unacceptable extensions, on every
//                      name a namespace places
pub(crate) struct Caps {
    limits: Arc<ExtractionLimits>,
    policy: Result<Arc<PostProcessingSettings>, String>,
    // The bytes posted for each root member, by member id.
    declared: Vec<u64>,
    // When an object first produced output.
    started: Option<Instant>,
    debits: CapacityDebits,
    // How far each member an object produced is written.
    extents: BTreeMap<(NodeId, MemberId), u64>,
    written: u64,
    refusal: Option<ExtractionFailure>,
}

impl Caps {
    pub(crate) fn new(
        limits: Arc<ExtractionLimits>,
        policy: Result<Arc<PostProcessingSettings>, String>,
        declared: Vec<u64>,
    ) -> Self {
        Self {
            limits,
            policy,
            declared,
            started: None,
            debits: CapacityDebits::default(),
            extents: BTreeMap::new(),
            written: 0,
            refusal: None,
        }
    }

    // No limit and no policy: a writer standing alone, outside a job.
    pub(crate) fn unbounded() -> Self {
        let limits = ExtractionLimits::unbounded();
        let policy = PostProcessingSettings {
            unacceptable_extensions: Vec::new(),
            ..PostProcessingSettings::default()
        };
        Self::new(Arc::new(limits), Ok(Arc::new(policy)), Vec::new())
    }

    // The breach the job stopped on, if it did.
    pub(crate) fn refusal(&self) -> Option<&ExtractionFailure> {
        self.refusal.as_ref()
    }

    // The breach, if one stands, or the deadline's once it has passed.
    pub(crate) fn check(&mut self) -> Result<(), ExtractionFailure> {
        if let Some(refusal) = &self.refusal {
            return Err(refusal.clone());
        }
        if self
            .started
            .is_some_and(|started| started.elapsed() > Duration::from_secs(self.limits.max_seconds))
        {
            let detail = format!(
                "extraction exceeded the {} second deadline",
                self.limits.max_seconds
            );
            return Err(self.breach(ExtractionRejectionReason::Deadline, detail));
        }
        Ok(())
    }

    // An object produced output: extraction's clock runs from the first time.
    fn start(&mut self) {
        self.started.get_or_insert_with(Instant::now);
    }

    // A namespace places `name`. A directory carries no extension.
    pub(crate) fn admit_name(&mut self, name: &str) -> Result<(), ExtractionFailure> {
        self.check()?;
        if Leaf::is_directory(name) {
            return Ok(());
        }
        let rejected = match &self.policy {
            Ok(policy) => policy
                .unacceptable_extension_match(name)
                .map(|_| unwanted_extension_reason(name)),
            Err(error) => Some(error.clone()),
        };
        match rejected {
            Some(detail) => Err(self.breach(ExtractionRejectionReason::ContentPolicy, detail)),
            None => Ok(()),
        }
    }

    // A namespace announced a member. Every member an object's namespace
    // holds is an entry; the posts are not.
    pub(crate) fn admit_entry(
        &mut self,
        tree: &Tree,
        node: NodeId,
    ) -> Result<(), ExtractionFailure> {
        self.check()?;
        if !produced(tree, node) {
            return Ok(());
        }
        self.start();
        let entries: u64 = tree
            .live()
            .into_iter()
            .filter(|node| produced(tree, *node))
            .filter_map(|node| tree.namespace(node))
            .map(|namespace| namespace.len() as u64)
            .sum();
        if entries > self.limits.max_entries {
            let detail = format!(
                "archive contains {entries} entries, limit is {}",
                self.limits.max_entries
            );
            return Err(self.breach(ExtractionRejectionReason::Entries, detail));
        }
        Ok(())
    }

    // Bytes up to `end` of a member of `node`, against the disk reading the
    // holds take. Only what an object produced counts.
    pub(crate) fn admit_write(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        end: u64,
        capacity: Capacity,
    ) -> Result<(), ExtractionFailure> {
        self.check()?;
        if !produced(tree, node) {
            return Ok(());
        }
        self.start();
        let extent = self.extents.get(&(node, member)).copied().unwrap_or(0);
        let Some(grown) = end.checked_sub(extent).filter(|grown| *grown > 0) else {
            return Ok(());
        };
        if end > self.limits.max_member_bytes {
            let detail = format!(
                "member output would reach {end} bytes, limit is {}",
                self.limits.max_member_bytes
            );
            return Err(self.breach(ExtractionRejectionReason::MemberBytes, detail));
        }
        let written = self.written.saturating_add(grown);
        // The limit only rises with what was posted, so the posts are read
        // once the output passes the least it can be.
        if written > self.limits.job_output_limit(0).0 {
            let (limit, reason) = self.limits.job_output_limit(self.declared(tree));
            if written > limit {
                let detail = format!(
                    "archive output would reach {written} bytes, effective job limit is {limit}"
                );
                return Err(self.breach(reason, detail));
            }
        }
        if let Some(reading) = self.debits.apply(capacity).reading() {
            if let Some(remaining) = self.limits.short_of_reserve(&reading, grown) {
                let detail = format!(
                    "writing {grown} bytes would leave {remaining} bytes free, reserve is {}",
                    self.limits.min_free_bytes
                );
                return Err(self.breach(ExtractionRejectionReason::DiskReserve, detail));
            }
            self.debits.debit(grown);
        }
        self.extents.insert((node, member), end);
        self.written = written;
        Ok(())
    }

    // The member's file was emptied or removed: its bytes no longer count.
    pub(crate) fn forget(&mut self, node: NodeId, member: MemberId) {
        if let Some(extent) = self.extents.remove(&(node, member)) {
            self.written = self.written.saturating_sub(extent);
        }
    }

    // A member's file as a checkpoint left it, written as far as its
    // namespace's coverage reaches.
    pub(crate) fn restore(&mut self, tree: &Tree, node: NodeId, member: MemberId) {
        if !produced(tree, node) {
            return;
        }
        let extent = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .and_then(|record| record.coverage.runs().map(|run| run.range.end).max())
            .unwrap_or(0);
        self.forget(node, member);
        if extent > 0 {
            self.start();
            self.extents.insert((node, member), extent);
            self.written = self.written.saturating_add(extent);
        }
    }

    // The bytes posted for the root members an object other than a
    // recovery object owns, or gave up: the archives the output came from.
    fn declared(&self, tree: &Tree) -> u64 {
        self.declared
            .iter()
            .zip(0u32..)
            .filter(|(_, index)| {
                let member = MemberId(*index);
                tree.child_of(NodeId::ROOT, member)
                    .or_else(|| tree.retired(NodeId::ROOT, member))
                    .is_some_and(|(object, _)| !tree.recovers(object))
            })
            .map(|(bytes, _)| *bytes)
            .fold(0u64, u64::saturating_add)
    }

    fn breach(&mut self, reason: ExtractionRejectionReason, detail: String) -> ExtractionFailure {
        self.refusal
            .get_or_insert_with(|| {
                warn!(reason = reason.as_str(), detail = %detail, "graph job's extraction budget refused it");
                ExtractionFailure::new(reason, detail)
            })
            .clone()
    }
}

// Whether the namespace's members are an object's output: every namespace
// but the root's, whose members are the posts.
fn produced(tree: &Tree, node: NodeId) -> bool {
    tree.volumes(node).is_some()
}

impl crate::pipeline::Pipeline {
    // A graph job's budget: the pipeline's extraction limits, the operator's
    // policy as the database holds it now, and what the posts declared.
    pub(in crate::pipeline::graph) fn graph_caps(&self, files: &[RootFile]) -> Caps {
        let policy = self
            .db
            .post_processing_settings()
            .map(Arc::new)
            .map_err(|error| format!("could not load unacceptable extension policy: {error}"));
        let declared = files
            .iter()
            .map(|file| {
                file.articles
                    .iter()
                    .map(|(_, bytes)| u64::from(*bytes))
                    .sum()
            })
            .collect();
        Caps::new(Arc::clone(&self.extraction_limits), policy, declared)
    }

    // What a job outside the pipeline builds its graph with: the pipeline's
    // holds accountant, slots, extraction limits and the complete root's
    // free-space reader.
    pub(crate) fn graph_budget(&self) -> GraphBudget {
        GraphBudget {
            accountant: Arc::clone(&self.graph_holds),
            slots: self.graph_slot_limit(),
            limits: Arc::clone(&self.extraction_limits),
            free_disk: self
                .storage_capacity()
                .reader(crate::operations::disk::StorageRoot::Complete),
            mail: self.graph_runs.sender(),
            outside: Arc::clone(&self.graph_outside),
        }
    }
}

// The pipeline's budget, for a job it does not run.
#[derive(Clone)]
pub(crate) struct GraphBudget {
    pub(crate) accountant: Arc<weaver_graph::holds::HoldsAccountant>,
    pub(crate) slots: usize,
    pub(crate) limits: Arc<ExtractionLimits>,
    pub(crate) free_disk: crate::operations::disk::CapacityReader,
    // Where the job's runs speak, and where it joins to hear them.
    pub(crate) mail: tokio::sync::mpsc::Sender<super::engine::Said>,
    pub(crate) outside: Arc<super::engine::Outside>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // The deadline counts from extraction's start, not from the job's: a
    // job whose input took past the deadline to arrive is not refused, and
    // the clock, once running, keeps its first start.
    #[test]
    fn the_deadline_runs_from_the_first_output_not_the_job() {
        let mut limits = ExtractionLimits::unbounded();
        limits.max_seconds = 0;
        let mut caps = Caps::new(
            Arc::new(limits),
            Ok(Arc::new(PostProcessingSettings::default())),
            Vec::new(),
        );
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("a second before now");

        // The input arriving, however long it takes, starts nothing.
        caps.admit_entry(&Tree::new(), NodeId::ROOT).unwrap();
        assert_eq!(caps.started, None);
        caps.check().unwrap();

        caps.start();
        let first = caps.started.expect("started");
        caps.start();
        assert_eq!(caps.started, Some(first));

        caps.started = Some(long_ago);
        let refused = caps.check().unwrap_err();
        assert_eq!(refused.reason(), ExtractionRejectionReason::Deadline);
    }
}
