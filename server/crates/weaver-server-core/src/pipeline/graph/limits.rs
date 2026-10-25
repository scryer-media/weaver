// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's limits, from the settings that already exist. The
// engine adds no settings of its own: an operator who tuned the direct
// store's holds, the extraction concurrency or the extraction limits gets
// the same limits here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tracing::warn;
use weaver_graph::holds::HoldBudgets;
use weaver_graph::{MemberId, NodeId, Tree};

use crate::RuntimeTuner;
use crate::operations::disk::{Capacity, CapacityDebits};
use crate::pipeline::completion::finalize::MAX_GRAPH_NESTING_DEPTH;
use crate::pipeline::direct_store::DirectStoreSettings;
use crate::pipeline::extraction::{ExtractionFailure, ExtractionLimits, ExtractionRejectionReason};
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
//   entries            members of the live namespaces objects produced
//   resident           what those namespaces keep in memory for their
//                      members, against the memory limit
//   deadline           the wall clock since the first member an object
//                      produced reached its namespace, as classic's runs
//                      from the job's first extraction; the posts' own
//                      writes are not under it
//   disk reserve       the free space a write would leave, against the
//                      reading the holds already take
//   content policy     the operator's unacceptable extensions, on every
//                      name a namespace places
pub(crate) struct Caps {
    limits: Arc<ExtractionLimits>,
    policy: Result<Arc<PostProcessingSettings>, String>,
    // The bytes posted for each root member, by member id.
    declared: Vec<u64>,
    // When extraction began: the first time an object's namespace was
    // charged. A job queued or paused before then has spent none of it.
    started: Option<Instant>,
    debits: CapacityDebits,
    // How far each member an object produced is written.
    extents: BTreeMap<(NodeId, MemberId), u64>,
    written: u64,
    // Each live namespace an object produced, as last charged; with the
    // members they hold and the bytes they keep resident, all together.
    accounts: BTreeMap<NodeId, Account>,
    entries: u64,
    resident: u64,
    refusal: Option<ExtractionFailure>,
}

// One namespace's charge: how many members it held, and what it kept
// resident for each record it was charged for.
#[derive(Default)]
struct Account {
    members: u64,
    resident: BTreeMap<MemberId, u64>,
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
            accounts: BTreeMap::new(),
            entries: 0,
            resident: 0,
            refusal: None,
        }
    }

    // No limit and no policy: a writer standing alone, outside a job.
    pub(crate) fn unbounded() -> Self {
        let limits = ExtractionLimits {
            max_job_bytes: u64::MAX,
            max_member_bytes: u64::MAX,
            max_entries: u64::MAX,
            max_ratio: u64::MAX,
            ratio_floor_bytes: u64::MAX,
            max_seconds: u64::MAX,
            min_free_bytes: 0,
            max_memory_bytes: u64::MAX,
        };
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

    // The breach, if one stands.
    pub(crate) fn check(&self) -> Result<(), ExtractionFailure> {
        match &self.refusal {
            Some(refusal) => Err(refusal.clone()),
            None => Ok(()),
        }
    }

    // A member of `node` changed. What an object produced is extraction:
    // the first such change starts the clock, and every one after it is
    // refused once the deadline has passed. The posts are not under it.
    fn extracting(&mut self, tree: &Tree, node: NodeId) -> Result<(), ExtractionFailure> {
        if !produced(tree, node) {
            return Ok(());
        }
        let started = *self.started.get_or_insert_with(Instant::now);
        if started.elapsed() > Duration::from_secs(self.limits.max_seconds) {
            let detail = format!(
                "extraction exceeded the {} second deadline",
                self.limits.max_seconds
            );
            return Err(self.breach(ExtractionRejectionReason::Deadline, detail));
        }
        Ok(())
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

    // A namespace announced a member, or bytes of one reached it: the
    // member is charged again as it stands. Every member an object's
    // namespace holds is an entry, and what the namespace keeps of it is
    // resident; the posts are neither.
    pub(crate) fn admit_member(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), ExtractionFailure> {
        self.check()?;
        self.extracting(tree, node)?;
        self.charge(tree, node, member);
        if self.entries > self.limits.max_entries {
            let detail = format!(
                "archive contains {} entries, limit is {}",
                self.entries, self.limits.max_entries
            );
            return Err(self.breach(ExtractionRejectionReason::Entries, detail));
        }
        if self.resident > self.limits.max_memory_bytes {
            let detail = format!(
                "members hold {} bytes resident, limit is {}",
                self.resident, self.limits.max_memory_bytes
            );
            return Err(self.breach(ExtractionRejectionReason::Memory, detail));
        }
        Ok(())
    }

    // The member's namespace as it stands now: its count of members and
    // what it keeps for the member replace what they were charged at. A
    // namespace that is no longer live is charged nothing.
    fn charge(&mut self, tree: &Tree, node: NodeId, member: MemberId) {
        if !produced(tree, node) {
            return;
        }
        let Some(namespace) = tree.namespace(node).filter(|_| tree.is_live(node)) else {
            self.release(node);
            return;
        };
        let account = self.accounts.entry(node).or_default();
        let members = namespace.len() as u64;
        self.entries = self.entries + members - account.members;
        account.members = members;
        let resident = namespace.resident(member);
        let was = match resident {
            0 => account.resident.remove(&member),
            _ => account.resident.insert(member, resident),
        };
        self.resident = self.resident + resident - was.unwrap_or(0);
    }

    fn release(&mut self, node: NodeId) {
        if let Some(account) = self.accounts.remove(&node) {
            self.entries -= account.members;
            self.resident -= account.resident.values().sum::<u64>();
        }
    }

    // The tree retired or dropped namespaces: they hold no entries and keep
    // nothing resident any more.
    pub(crate) fn release_gone(&mut self, tree: &Tree) {
        let gone: Vec<NodeId> = self
            .accounts
            .keys()
            .copied()
            .filter(|node| !tree.is_live(*node))
            .collect();
        for node in gone {
            self.release(node);
        }
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
        self.extracting(tree, node)?;
        if !produced(tree, node) {
            return Ok(());
        }
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
            let remaining = reading.available_bytes.saturating_sub(grown);
            if remaining < self.limits.min_free_bytes && !reading.stale {
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

    // The member's file was emptied or removed, and its record started
    // over, folded or went: its bytes no longer count, and it is charged
    // as it stands.
    pub(crate) fn forget(&mut self, tree: &Tree, node: NodeId, member: MemberId) {
        self.unwrite(node, member);
        self.charge(tree, node, member);
    }

    fn unwrite(&mut self, node: NodeId, member: MemberId) {
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
        self.unwrite(node, member);
        if extent > 0 {
            self.extents.insert((node, member), extent);
            self.written = self.written.saturating_add(extent);
        }
    }

    // The restored tree's members, each charged as the tree holds it: what
    // a checkpoint brings back is counted from the tree, never stored.
    pub(crate) fn restore_members(&mut self, tree: &Tree) {
        for node in tree.live() {
            let records: Vec<MemberId> = tree
                .namespace(node)
                .map(|namespace| namespace.records().map(|record| record.id).collect())
                .unwrap_or_default();
            for member in records {
                self.charge(tree, node, member);
            }
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
}
