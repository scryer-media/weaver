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
use crate::pipeline::extraction::{ExtractionFailure, ExtractionLimits, ExtractionRejectionReason};
use crate::pipeline::graph::job::RootFile;
use crate::pipeline::graph::leaf::Leaf;
use crate::post_processing::model::{PostProcessingSettings, unwanted_extension_reason};

// The hold settings, resolved once at pipeline construction. Each value is
// read from the environment, then from the `[direct_store]` settings table,
// then from a default the host informs.
//
//   resident_limit   process-wide RAM the holds may keep resident
//   scratch_total    process-wide scratch the holds may page to
//   scratch_ceiling  scratch one namespace's holds may page to
//   disk_reserve     free space the working filesystem keeps under scratch;
//                    zero disables it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HoldSettings {
    pub(crate) resident_limit_bytes: u64,
    pub(crate) scratch_total_bytes: u64,
    pub(crate) scratch_ceiling_bytes: u64,
    pub(crate) disk_reserve_bytes: u64,
}

// Environment overrides for the hold settings, in bytes.
const SCRATCH_CEILING_ENV: &str = "WEAVER_RAR_DIRECT_STORE_SCRATCH_CEILING_BYTES";
const RESIDENT_LIMIT_ENV: &str = "WEAVER_RAR_DIRECT_STORE_HOLDS_RESIDENT_LIMIT_BYTES";
const SCRATCH_TOTAL_ENV: &str = "WEAVER_RAR_DIRECT_STORE_HOLDS_SCRATCH_TOTAL_BYTES";
const DISK_RESERVE_ENV: &str = "WEAVER_RAR_DIRECT_STORE_HOLDS_DISK_RESERVE_BYTES";

// One namespace's scratch ceiling when nothing configures it.
const SCRATCH_CEILING_DEFAULT_BYTES: u64 = 1024 * 1024 * 1024;
// The smallest resident limit a host derives, and a sixteenth of the most.
const RESIDENT_LIMIT_FLOOR_BYTES: u64 = 64 * 1024 * 1024;
// How many namespaces' worth of scratch the process-wide total allows when
// nothing configures it.
const SCRATCH_TOTAL_NAMESPACES: u64 = 4;

// What the environment says about the hold settings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HoldEnv {
    pub(crate) scratch_ceiling: Option<u64>,
    pub(crate) resident_limit: Option<u64>,
    pub(crate) scratch_total: Option<u64>,
    pub(crate) disk_reserve: Option<u64>,
}

impl HoldEnv {
    // The process environment's overrides.
    pub(crate) fn read() -> Self {
        let bytes = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
        };
        Self {
            scratch_ceiling: bytes(SCRATCH_CEILING_ENV),
            resident_limit: bytes(RESIDENT_LIMIT_ENV),
            scratch_total: bytes(SCRATCH_TOTAL_ENV),
            disk_reserve: bytes(DISK_RESERVE_ENV),
        }
    }
}

// What the host can tell the defaults: the memory the process may use, the
// size of the filesystem under the working directory, and how much of that
// memory the hardware profile lets resident holds claim.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HostFacts {
    pub(crate) total_memory_bytes: Option<u64>,
    pub(crate) working_fs_total_bytes: Option<u64>,
    // The hardware profile's cap on the derived resident limit. A configured
    // limit is the operator's own number and is never capped.
    pub(crate) resident_default_cap_bytes: Option<u64>,
}

impl HostFacts {
    // Probes the real host. `working_dir` may not exist yet at startup, so
    // the nearest existing ancestor answers for its filesystem.
    pub(crate) fn probe(
        working_dir: &std::path::Path,
        resident_default_cap_bytes: Option<u64>,
    ) -> Self {
        let working_fs_total_bytes = match crate::operations::probe_nearest_disk_space(working_dir)
        {
            Ok(space) => Some(space.total_bytes),
            Err(error) => {
                warn!(
                    path = %working_dir.display(),
                    error = %error,
                    "working directory capacity is unknown; hold limits fall back to their defaults"
                );
                None
            }
        };
        Self {
            total_memory_bytes: crate::runtime::system_probe::detect_total_memory_bytes(),
            working_fs_total_bytes,
            resident_default_cap_bytes,
        }
    }
}

impl HoldSettings {
    // Resolves the settings against a loaded config on a probed host.
    pub(crate) fn resolve(config: &crate::settings::Config, host: HostFacts) -> Self {
        Self::resolve_parts(config.direct_store.as_ref(), HoldEnv::read(), host)
    }

    // The precedence rule itself, with the environment and the host passed in.
    pub(crate) fn resolve_parts(
        config: Option<&crate::settings::DirectStoreOverrides>,
        env: HoldEnv,
        host: HostFacts,
    ) -> Self {
        let pick = |from_env: Option<u64>, from_config: Option<u64>, default: u64| {
            from_env.or(from_config).unwrap_or(default)
        };
        let scratch_ceiling_bytes = pick(
            env.scratch_ceiling,
            config.and_then(|cfg| cfg.holds_scratch_ceiling_bytes),
            SCRATCH_CEILING_DEFAULT_BYTES,
        );
        let derived_resident_limit = host
            .total_memory_bytes
            .map_or(4 * RESIDENT_LIMIT_FLOOR_BYTES, |total| {
                (total / 16).clamp(RESIDENT_LIMIT_FLOOR_BYTES, 16 * RESIDENT_LIMIT_FLOOR_BYTES)
            });
        let default_resident_limit = host
            .resident_default_cap_bytes
            .map_or(derived_resident_limit, |cap| {
                derived_resident_limit.min(cap)
            });
        Self {
            resident_limit_bytes: pick(
                env.resident_limit,
                config.and_then(|cfg| cfg.holds_resident_limit_bytes),
                default_resident_limit,
            ),
            scratch_total_bytes: pick(
                env.scratch_total,
                config.and_then(|cfg| cfg.holds_scratch_total_bytes),
                scratch_ceiling_bytes.saturating_mul(SCRATCH_TOTAL_NAMESPACES),
            ),
            scratch_ceiling_bytes,
            disk_reserve_bytes: pick(
                env.disk_reserve,
                config.and_then(|cfg| cfg.holds_disk_reserve_bytes),
                crate::pipeline::extraction::safety::default_disk_reserve_bytes(
                    host.working_fs_total_bytes,
                ),
            ),
        }
    }

    // The budgets the holds accountant enforces.
    pub(crate) fn budgets(&self) -> HoldBudgets {
        HoldBudgets {
            resident_limit: self.resident_limit_bytes,
            scratch_total: self.scratch_total_bytes,
            scratch_ceiling: self.scratch_ceiling_bytes,
            disk_reserve: self.disk_reserve_bytes,
        }
    }
}

// How many namespaces may extract at once: the extraction concurrency the
// tuner allows, which already bounds streaming member extractions.
pub(crate) fn slots(tuner: &RuntimeTuner) -> usize {
    tuner.max_concurrent_extractions()
}

// How deep a graph job's namespaces may nest, handed to its tree beside the
// hold budgets. An object that would open one deeper is never built: its
// member is refused as the container it is, and the job goes on.
pub(crate) const NESTING: u32 = MAX_GRAPH_NESTING_DEPTH;

// The job's extraction budget: the operator's extraction limits, one budget
// per job, read where the namespace writes. Every byte an object
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
//                      produced reached its namespace; the posts' own
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
        caps_for(&self.db, &self.extraction_limits, files)
    }
}

// A graph job's budget from the pipeline's extraction limits and the
// database, for a caller off the pipeline thread.
pub(in crate::pipeline::graph) fn caps_for(
    db: &crate::Database,
    extraction_limits: &Arc<ExtractionLimits>,
    files: &[RootFile],
) -> Caps {
    let policy = db
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
    Caps::new(Arc::clone(extraction_limits), policy, declared)
}
