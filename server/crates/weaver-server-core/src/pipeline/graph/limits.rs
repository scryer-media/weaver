// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's limits, from the settings that already exist. The
// engine adds no settings of its own: an operator who tuned the direct
// store's holds, the extraction concurrency or the extraction limits gets
// the same limits here.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;
use weaver_graph::holds::HoldBudgets;
use weaver_graph::{MemberId, NodeId, Tree};

use crate::RuntimeTuner;
use crate::operations::disk::{Capacity, CapacityDebits};
use crate::pipeline::completion::finalize::MAX_GRAPH_NESTING_DEPTH;
use crate::pipeline::extraction::{ExtractionFailure, ExtractionLimits, ExtractionRejectionReason};
use crate::pipeline::graph::decoding::DecodeClock;
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
// Output counts once. A member another object reads is the same output as
// what that object makes of it, one level on, so the two are never added:
// the files the job delivers, at any depth, are one total, and what waits
// for an object at each depth is a total of its own. Each total is held to
// the job byte cap and the ratio alone.
//
//   job bytes, ratio   each total, against the job byte cap or the ratio
//                      over the bytes posted for the objects
//   member bytes       one delivered file's extent; what waits for an
//                      object is not under it
//   entries            members of the live namespaces objects produced
//   resident           what those namespaces keep in memory for their
//                      members, against the memory limit
//   deadline           the time the job's decoders spent executing, less
//                      what they spent while the job was paused; waiting
//                      for articles, a slot, memory or a thread is not
//                      under it, nor are the posts' own writes
//   disk reserve       the free space a write would leave, against the
//                      reading of the device it lands on
//   content policy     the operator's unacceptable extensions, on every
//                      name a namespace places; a name placed while the
//                      database could not be read waits, and is judged when
//                      a read succeeds, before the job delivers; a stored
//                      setting that does not parse is the default
pub(crate) struct Caps {
    limits: Arc<ExtractionLimits>,
    read_policy: PolicyRead,
    policy: Policy,
    // Names placed and not judged yet: placed while the policy was unread,
    // or brought back by a restore.
    waiting: Vec<String>,
    // The bytes posted for each root member, by member id.
    declared: Vec<u64>,
    // The time the job's decoders spent executing, which its runs and
    // loans keep.
    decoding: DecodeClock,
    // What of it was spent while the job was paused, and what it stood at
    // when the job paused, while it is paused.
    excused: Duration,
    paused: Option<Duration>,
    // What was admitted against each device's latest reading, by `Device`.
    debits: [CapacityDebits; 2],
    // How far each member an object produced is written, and the level it
    // counts at.
    extents: BTreeMap<(NodeId, MemberId), (u64, usize)>,
    // What each level holds, by `level`: the files the job delivers, then
    // what waits for an object at each depth.
    written: [u64; LEVELS],
    // Each live namespace an object produced, as last charged; with the
    // members they hold and the bytes they keep resident, all together.
    accounts: BTreeMap<NodeId, Account>,
    entries: u64,
    resident: u64,
    refusal: Option<ExtractionFailure>,
}

// A read of the operator's settings, answering with them or with why the
// database could not be read. A failed read is the host's, never the
// post's: it is tried again when the policy is next needed. A stored value
// that does not parse is no failed read: it answers with the default.
pub(crate) type PolicyRead =
    Arc<dyn Fn() -> Result<Arc<PostProcessingSettings>, String> + Send + Sync>;

// What a failed read of the policy is said as: the setting, never the post.
const POLICY_UNREAD: &str = "The Unacceptable extensions setting could not be read";

// A read that always answers with these settings.
pub(crate) fn known_policy(settings: PostProcessingSettings) -> PolicyRead {
    let settings = Arc::new(settings);
    Arc::new(move || Ok(Arc::clone(&settings)))
}

// The unacceptable extensions as the job knows them: read, or not yet,
// with why the last read failed.
enum Policy {
    Known(Arc<PostProcessingSettings>),
    Unknown(String),
}

// The device a write lands on, each judged against its own reading: the
// output directory's, where the files the job delivers lie, or the working
// directory's, where what an object produced waits for the object that
// reads it. One device when the two directories are one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Device {
    Output = 0,
    Working = 1,
}

// What a member's bytes are to the job: a file it delivers, or output that
// waits for an object to read it, in that object's envelope or held until
// its owner is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    File,
    Kept,
}

// The levels output counts at: the files, then one for each depth a
// namespace may sit at below the root.
const LEVELS: usize = NESTING as usize + 1;

// The level a member of `node` counts at as `output`. A member another
// object reads counts with the rest of its namespace's depth.
fn level(tree: &Tree, node: NodeId, output: Output) -> usize {
    match output {
        Output::File => 0,
        Output::Kept => (tree.depth(node) as usize).clamp(1, LEVELS - 1),
    }
}

// What a member of `node` is to the job as the tree holds it now: read by
// an object that is not recovery, or held while its owner is undecided, it
// waits; otherwise it is a file.
fn output_of(tree: &Tree, node: NodeId, member: MemberId) -> Output {
    let read = tree
        .child_of(node, member)
        .is_some_and(|(child, _)| !tree.recovers(child));
    if read || tree.is_undecided(node, member) {
        Output::Kept
    } else {
        Output::File
    }
}

// One namespace's charge: how many members it held, and what it kept
// resident for each record it was charged for.
#[derive(Default)]
struct Account {
    members: u64,
    resident: BTreeMap<MemberId, u64>,
}

impl Caps {
    // The policy is read once here; a read that fails is tried again.
    pub(crate) fn new(
        limits: Arc<ExtractionLimits>,
        read_policy: PolicyRead,
        declared: Vec<u64>,
    ) -> Self {
        let policy = match read_policy() {
            Ok(settings) => Policy::Known(settings),
            Err(error) => {
                warn!(%error, "{POLICY_UNREAD}; names wait to be judged until a read succeeds");
                Policy::Unknown(error)
            }
        };
        Self {
            limits,
            read_policy,
            policy,
            waiting: Vec::new(),
            declared,
            decoding: DecodeClock::default(),
            excused: Duration::ZERO,
            paused: None,
            debits: [CapacityDebits::default(); 2],
            extents: BTreeMap::new(),
            written: [0; LEVELS],
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
        Self::new(Arc::new(limits), known_policy(policy), Vec::new())
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
    // every such change is refused once the job's decoders have spent the
    // deadline. The posts are not under it.
    fn extracting(&mut self, tree: &Tree, node: NodeId) -> Result<(), ExtractionFailure> {
        if !produced(tree, node) {
            return Ok(());
        }
        let spent = self
            .paused
            .unwrap_or_else(|| self.decoding.spent())
            .saturating_sub(self.excused);
        if spent > Duration::from_secs(self.limits.max_seconds) {
            let detail = format!(
                "extraction exceeded the {} second deadline",
                self.limits.max_seconds
            );
            return Err(self.breach(ExtractionRejectionReason::Deadline, detail));
        }
        Ok(())
    }

    // The job paused or resumed. What its decoders spend while it is
    // paused is not counted: on resume it is excused.
    pub(crate) fn hold_clock(&mut self, paused: bool) {
        match (paused, self.paused) {
            (true, None) => self.paused = Some(self.decoding.spent()),
            (false, Some(at)) => {
                self.paused = None;
                self.excused += self.decoding.spent().saturating_sub(at);
            }
            _ => {}
        }
    }

    // Where the job's runs and loans keep the time they spend decoding.
    pub(crate) fn decode_clock(&self) -> DecodeClock {
        self.decoding.clone()
    }

    #[cfg(test)]
    pub(crate) fn clock_held(&self) -> bool {
        self.paused.is_some()
    }

    // A namespace places `name`. A directory carries no extension.
    // While the policy is unread the name waits to be judged.
    pub(crate) fn admit_name(&mut self, name: &str) -> Result<(), ExtractionFailure> {
        self.check()?;
        if Leaf::is_directory(name) {
            return Ok(());
        }
        let policy = match &mut self.policy {
            Policy::Known(policy) => Arc::clone(policy),
            Policy::Unknown(_) => {
                self.waiting.push(name.to_owned());
                return Ok(());
            }
        };
        match policy.unacceptable_extension_match(name) {
            Some(_) => Err(self.breach(
                ExtractionRejectionReason::ContentPolicy,
                unwanted_extension_reason(name),
            )),
            None => Ok(()),
        }
    }

    // Read the policy again if it is still unread, and judge every name
    // that waited for it. The error, worded for the operator, while the
    // read still fails; a name the policy refuses stands as the breach.
    pub(crate) fn learn_policy(&mut self) -> Result<(), String> {
        if let Policy::Unknown(error) = &mut self.policy {
            match (self.read_policy)() {
                Ok(settings) => self.policy = Policy::Known(settings),
                Err(failed) => {
                    *error = failed;
                    return Err(format!("{POLICY_UNREAD}: {error}"));
                }
            }
        }
        for name in std::mem::take(&mut self.waiting) {
            if self.admit_name(&name).is_err() {
                break;
            }
        }
        Ok(())
    }

    // A namespace announced a member, or bytes of one reached it: the
    // member is charged again as it stands. Every record an object's
    // namespace holds is an entry, a further posting folded into another
    // member among them, and what the namespace keeps of it is resident;
    // the posts are neither.
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
        let members = namespace.records_len() as u64;
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

    // Bytes up to `end` of a member of `node`, which are `output` to the
    // job, against the reading of the device under `folder`, which `device`
    // names. Only what an object produced counts, wherever it waits: a
    // member's file, the envelope of the object that reads the member, or
    // the holds. A member that becomes a file, or starts to wait, takes what
    // it holds to its new level.
    pub(crate) fn admit_write(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        end: u64,
        output: Output,
        (folder, capacity, device): (&Path, Capacity, Device),
    ) -> Result<(), ExtractionFailure> {
        self.check()?;
        self.extracting(tree, node)?;
        if !produced(tree, node) {
            return Ok(());
        }
        let (extent, was) = self.extents.get(&(node, member)).copied().unwrap_or((0, 0));
        let level = match (output, was) {
            (Output::Kept, 1..) => was,
            _ => level(tree, node, output),
        };
        let moved = if level == was { 0 } else { extent };
        let grown = end.saturating_sub(extent);
        if grown == 0 && moved == 0 {
            return Ok(());
        }
        // Only a file is held to the member limit, at all it holds: one
        // that was waiting brings what it already holds. What waits for an
        // object is held to its level's limit alone.
        let reach = extent.max(end);
        if level == 0 && reach > self.limits.max_member_bytes {
            let detail = format!(
                "member output would reach {reach} bytes, limit is {}",
                self.limits.max_member_bytes
            );
            return Err(self.breach(ExtractionRejectionReason::MemberBytes, detail));
        }
        let written = self.written[level]
            .saturating_add(moved)
            .saturating_add(grown);
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
        self.reserve(grown, (folder, capacity, device))?;
        self.written[was] = self.written[was].saturating_sub(moved);
        self.written[level] = written;
        self.extents
            .insert((node, member), (extent.max(end), level));
        Ok(())
    }

    // Bytes a repair writes into the posts. The posts are under none of the
    // job's limits, but what a repair writes into them takes room on their
    // disk as any output does, so it is held to the reserve.
    pub(crate) fn admit_repaired(
        &mut self,
        bytes: u64,
        (folder, capacity, device): (&Path, Capacity, Device),
    ) -> Result<(), ExtractionFailure> {
        if bytes == 0 {
            return Ok(());
        }
        self.check()?;
        self.reserve(bytes, (folder, capacity, device))
    }

    // How many bytes the device `device` names can take before less than
    // the reserve is left free, against its latest reading less what was
    // admitted since. `None` when no reading holds it to the reserve: none
    // was taken, or the last is stale, and a write is then never refused.
    pub(crate) fn room(&mut self, (capacity, device): (Capacity, Device)) -> Option<u64> {
        let reading = self.debits[device as usize].apply(capacity).reading()?;
        (!reading.stale).then(|| {
            reading
                .available_bytes
                .saturating_sub(self.limits.min_free_bytes)
        })
    }

    // `grown` more bytes on the device under `folder`, which `device`
    // names, against its latest reading less what was admitted since: a
    // write that would leave less than the reserve free is refused.
    fn reserve(
        &mut self,
        grown: u64,
        (folder, capacity, device): (&Path, Capacity, Device),
    ) -> Result<(), ExtractionFailure> {
        let device = device as usize;
        if grown > 0
            && let Some(reading) = self.debits[device].apply(capacity).reading()
        {
            let remaining = reading.available_bytes.saturating_sub(grown);
            if remaining < self.limits.min_free_bytes && !reading.stale {
                let detail = disk_full(
                    folder,
                    reading.available_bytes,
                    grown,
                    self.limits.min_free_bytes,
                );
                return Err(self.breach(ExtractionRejectionReason::DiskReserve, detail));
            }
            self.debits[device].debit(grown);
        }
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
        if let Some((extent, level)) = self.extents.remove(&(node, member)) {
            self.written[level] = self.written[level].saturating_sub(extent);
        }
    }

    // A member's file as a checkpoint left it, written as far as its
    // namespace's coverage reaches, at the level the tree now says it
    // counts at.
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
            let level = level(tree, node, output_of(tree, node, member));
            self.extents.insert((node, member), (extent, level));
            self.written[level] = self.written[level].saturating_add(extent);
        }
    }

    // The restored tree's members, each charged as the tree holds it: what
    // a checkpoint brings back is counted from the tree, never stored, and a
    // member another object reads counts as far as it arrived, as a file
    // does. No checkpoint says which names a run before judged, so each
    // waits to be judged again before the job delivers: a refusal here would
    // stop the restore itself, and the job would never be failed for it.
    pub(crate) fn restore_members(&mut self, tree: &Tree) {
        for node in tree.live() {
            let records: Vec<(MemberId, String)> = tree
                .namespace(node)
                .map(|namespace| {
                    namespace
                        .records()
                        .map(|record| (record.id, record.output_name.clone()))
                        .collect()
                })
                .unwrap_or_default();
            for (member, name) in records {
                if !Leaf::is_directory(&name) {
                    self.waiting.push(name);
                }
                self.charge(tree, node, member);
                self.restore(tree, node, member);
            }
        }
    }

    // The bytes posted for every root member but recovery data. A member
    // no object holds yet counts: an archive's member takes its whole
    // length at its first write, before its later volumes are bound.
    fn declared(&self, tree: &Tree) -> u64 {
        self.declared
            .iter()
            .zip(0u32..)
            .filter(|(_, index)| !recovery_post(tree, MemberId(*index)))
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

// A disk reserve breach as the operator reads it: the disk is out of space,
// where, and by how much.
fn disk_full(folder: &Path, free: u64, writing: u64, reserve: u64) -> String {
    use crate::support::human_bytes;
    format!(
        "out of disk space: the disk holding {} has {} free, writing {} more would leave {}, and {} must stay free",
        folder.display(),
        human_bytes(free),
        human_bytes(writing),
        human_bytes(free.saturating_sub(writing)),
        human_bytes(reserve),
    )
}

// Whether a post is recovery data: a recovery object holds it or gave it
// up, or, held by none, its posted name says so.
fn recovery_post(tree: &Tree, member: MemberId) -> bool {
    match tree
        .child_of(NodeId::ROOT, member)
        .or_else(|| tree.retired(NodeId::ROOT, member))
    {
        Some((object, _)) => tree.recovers(object),
        None => tree
            .namespace(NodeId::ROOT)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| weaver_graph::names_recovery(&record.name)),
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
    let db = db.clone();
    // A stored value that does not parse reads as the default policy; only
    // a failed read leaves the policy unknown.
    let policy: PolicyRead = Arc::new(move || {
        db.post_processing_settings()
            .map(Arc::new)
            .map_err(|error| error.to_string())
    });
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
