// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's limits, from the settings that already exist. The
// engine adds no settings of its own: an operator who tuned the direct
// store's holds, the extraction concurrency or the extraction limits gets
// the same limits here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;
use weaver_graph::holds::HoldBudgets;
use weaver_graph::{MemberId, NodeId, Tree};

use crate::RuntimeTuner;
use crate::operations::disk::{CapacityReader, DiskLedger};
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
//                      members, and the job's name ledger for the parts
//                      of its paths, against the memory limit
//   deadline           the time the job's decoders spent executing, less
//                      what they spent while the job was paused, and less
//                      the part of it each run stopped for room took
//                      before it stopped, which runs again; waiting
//                      for articles, a slot, memory or a thread is not
//                      under it, nor are the posts' own writes
//   disk reserve       the free space a write would leave, against the
//                      reading of the device it lands on less what every
//                      writer promised it; a write that would leave less
//                      lands, and the job then waits, its writing work
//                      held, until a reading shows room for all it owes,
//                      or it owes nothing. The
//                      reserve never fails the job
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
    // What the job owes each device, by `Device`, and whether it waits
    // for room on either.
    spend: [Spend; 2],
    // Set while a run stopped for room takes back what it built: each
    // member that goes, its own or one buried under it, is written again
    // when the run runs again.
    rewriting: bool,
    // How far each member an object produced is written, the level it
    // counts at, and what of it is owed to its device.
    extents: BTreeMap<(NodeId, MemberId), Extent>,
    // What each level holds, by `level`: the files the job delivers, then
    // what waits for an object at each depth.
    written: [u64; LEVELS],
    // Each live namespace an object produced, as last charged; with the
    // members they hold and the bytes they keep resident, all together.
    accounts: BTreeMap<NodeId, Account>,
    entries: u64,
    resident: u64,
    // What the job's name ledger keeps for the parts of its paths, as last
    // charged: one ledger names every namespace, so it is charged once, in
    // `resident` with the records.
    names: u64,
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

// How far a member is written, the level it counts at, and what of it is
// owed to the device it lands on.
#[derive(Debug, Clone, Copy)]
struct Extent {
    end: u64,
    level: usize,
    owed: u64,
    device: Device,
}

// How much is promised from a device beyond what a write needs when the
// ledger is asked, so that most writes touch nothing shared, and how much
// that was promised may land before the ledger hears of it.
const SLAB_BYTES: u64 = 64 * 1024 * 1024;

// The room above `reserve` a good reading of `reader` leaves on its
// ledger: every byte with no reserve, no reading, or a stale one, which
// checks nothing.
fn checked_room(ledger: &DiskLedger, reader: &CapacityReader, reserve: u64) -> u64 {
    match ledger.free_with(|| reader.current()) {
        Some(free) if reserve > 0 && !free.stale => free.bytes.saturating_sub(reserve),
        _ => u64::MAX,
    }
}

// What the job owes one device: the bytes its members took and have not
// written yet, and how much of that the device's ledger holds promised for
// it. While the job waits for room on the device, nothing it owes there is
// promised, so a job that waits holds no room another could use.
#[derive(Debug, Default)]
struct Spend {
    ledger: Option<Arc<DiskLedger>>,
    // Taken for members, not landed yet.
    owed: u64,
    // Of `owed`, what is not promised: all of it while the job waits.
    unbooked: u64,
    // What a repair the job holds back until there is room would write:
    // promised in hand when the job wakes, for the repair to spend.
    wanted: u64,
    // What members removed while the job waits held: the runs that built
    // them write them again once it wakes, so it is promised then, to spend
    // as they write.
    again: u64,
    // Promised beyond what is owed, spent before the ledger is asked again.
    slab: u64,
    // Of `slab`, what was promised for a held repair when the job woke and
    // no repair has taken yet.
    in_hand: u64,
    // What landed since the ledger last heard: of what was promised, and of
    // what was not.
    landed: u64,
    landed_unbooked: u64,
    // The job's place among those waiting for room on the device.
    waiting: Option<u64>,
}

impl Spend {
    // The ledger of the device `reader` reads, kept from the first use.
    fn bind(&mut self, reader: &CapacityReader) -> Arc<DiskLedger> {
        Arc::clone(
            self.ledger
                .get_or_insert_with(|| Arc::clone(reader.ledger())),
        )
    }

    // The ledger hears what landed.
    fn settle(&mut self) {
        if let Some(ledger) = &self.ledger {
            ledger.landed(self.landed, self.landed_unbooked);
        }
        self.landed = 0;
        self.landed_unbooked = 0;
    }

    // `bytes` of what is owed landed: what was not promised first. The
    // ledger hears of it once a slab's worth has landed, or sooner when it
    // is next asked.
    fn land(&mut self, bytes: u64) {
        let unbooked = bytes.min(self.unbooked);
        self.unbooked -= unbooked;
        self.owed = self.owed.saturating_sub(bytes);
        self.landed += bytes - unbooked;
        self.landed_unbooked += unbooked;
        if self.landed >= SLAB_BYTES {
            self.settle();
        }
    }

    // `bytes` that were owed will not land.
    fn forgo(&mut self, bytes: u64) {
        let unbooked = bytes.min(self.unbooked);
        self.unbooked -= unbooked;
        self.owed = self.owed.saturating_sub(bytes);
        if let Some(ledger) = &self.ledger {
            ledger.release(bytes - unbooked);
        }
    }

    // `bytes` more are owed to the device `reader` reads: promised from
    // what is in hand, or from the ledger while the device has room for
    // them above `reserve`, with up to a slab more. When it has not, the
    // job begins to wait for room and gives back all it promised here;
    // what it owes is promised again when it wakes. A device with no
    // reading, or only the last good one held across a failed probe, or a
    // job with no reserve, is not checked: only a good reading begins a
    // wait.
    fn book(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) {
        if bytes == 0 {
            return;
        }
        self.owed += bytes;
        if self.waiting.is_some() {
            self.unbooked += bytes;
            return;
        }
        if bytes <= self.slab {
            self.slab -= bytes;
            self.in_hand = self.in_hand.min(self.slab);
            return;
        }
        self.in_hand = 0;
        let ledger = self.bind(reader);
        self.settle();
        let need = bytes - self.slab;
        let room = checked_room(&ledger, reader, reserve);
        if need <= room {
            let extra = SLAB_BYTES.min(room - need);
            ledger.promise(need + extra);
            self.slab = extra;
            return;
        }
        self.hold(&ledger, self.owed - bytes);
    }

    // The job begins to wait: all it promised here is given back, and all
    // it owes is unbooked until it wakes. `promised` is what of `owed` was
    // promised.
    fn hold(&mut self, ledger: &DiskLedger, promised: u64) {
        ledger.release(self.slab + promised.saturating_sub(self.unbooked));
        self.slab = 0;
        self.in_hand = 0;
        self.unbooked = self.owed;
        self.waiting = Some(ledger.wait());
    }

    // A repair would write `bytes` on the device `reader` reads before it
    // can start. Whether it may: the bytes are then promised in hand for
    // it. When the device has no room for them above `reserve`, the job
    // begins to wait, as it does for a write, and the repair waits with it;
    // what it would write is promised in hand when the job wakes. Each
    // repair that goes out is promised its own bytes: from what was put in
    // hand for a held one, else from the ledger, so two cannot go out on
    // one promise.
    fn want(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) -> bool {
        if self.waiting.is_some() {
            self.wanted = self.wanted.max(bytes);
            return false;
        }
        if bytes == 0 {
            return true;
        }
        if bytes <= self.in_hand {
            self.in_hand -= bytes;
            return true;
        }
        let ledger = self.bind(reader);
        self.settle();
        let need = bytes - self.in_hand;
        let room = checked_room(&ledger, reader, reserve);
        if need <= room {
            ledger.promise(need);
            self.slab = self.slab.saturating_add(need);
            self.in_hand = 0;
            return true;
        }
        self.hold(&ledger, self.owed);
        self.wanted = bytes;
        false
    }

    // What `want` may take from what this holds: given back with
    // `give_back` should the repair not go out after all.
    fn taking(&self) -> (u64, u64) {
        (self.slab, self.in_hand)
    }

    // A `want` since `taken` is undone: what it promised is given back.
    fn give_back(&mut self, (slab, in_hand): (u64, u64)) {
        let promised = self.slab.saturating_sub(slab);
        if promised > 0
            && let Some(ledger) = &self.ledger
        {
            ledger.release(promised);
        }
        self.slab = slab;
        self.in_hand = in_hand;
    }

    // A repair that would write `bytes` here waits with the job: if the job
    // waits here already, for those bytes too; else it begins to, for them.
    fn wait_for(&mut self, bytes: u64, reader: &CapacityReader) {
        if self.waiting.is_some() {
            self.wanted = self.wanted.max(bytes);
            return;
        }
        if bytes == 0 {
            return;
        }
        let ledger = self.bind(reader);
        self.settle();
        self.hold(&ledger, self.owed);
        self.wanted = bytes;
    }

    // What the device must have room for before the job wakes.
    fn needed(&self) -> u64 {
        self.unbooked
            .saturating_add(self.wanted)
            .saturating_add(self.again)
    }

    // The job has room again: all it owes is promised, what a held repair
    // would write is promised in hand for it, and it no longer waits.
    fn rebook(&mut self) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        if self.waiting.take().is_some() {
            ledger.promise(self.needed());
            self.slab = self
                .slab
                .saturating_add(self.wanted)
                .saturating_add(self.again);
            self.in_hand = self.wanted;
            self.unbooked = 0;
            self.wanted = 0;
            self.again = 0;
            ledger.unwait();
        }
    }

    // All that was promised is given back, and the job no longer waits.
    fn release_all(&mut self) {
        self.settle();
        if let Some(ledger) = &self.ledger {
            ledger.release(self.slab + (self.owed - self.unbooked));
            if self.waiting.take().is_some() {
                ledger.unwait();
            }
        }
        self.owed = 0;
        self.unbooked = 0;
        self.wanted = 0;
        self.again = 0;
        self.slab = 0;
        self.in_hand = 0;
    }
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
            spend: [Spend::default(), Spend::default()],
            rewriting: false,
            extents: BTreeMap::new(),
            written: [0; LEVELS],
            accounts: BTreeMap::new(),
            entries: 0,
            resident: 0,
            names: 0,
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
        if self.deadline_spent() > Duration::from_secs(self.limits.max_seconds) {
            let detail = format!(
                "extraction exceeded the {} second deadline",
                self.limits.max_seconds
            );
            return Err(self.breach(ExtractionRejectionReason::Deadline, detail));
        }
        Ok(())
    }

    // The time counted against the deadline: what the job's decoders
    // spent, less what they spent while it was paused and what runs thrown
    // away took.
    pub(crate) fn deadline_spent(&self) -> Duration {
        self.paused
            .unwrap_or_else(|| self.decoding.spent())
            .saturating_sub(self.excused)
            .saturating_sub(self.decoding.discarded())
    }

    // The job paused or resumed. What its decoders spend while it is
    // paused is not counted: on resume it is excused.
    pub(crate) fn hold_clock(&mut self, paused: bool) {
        self.decoding.hold(paused);
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
    // what it keeps for the member replace what they were charged at, as
    // what the name ledger keeps replaces its charge. A namespace that is
    // no longer live is charged nothing.
    fn charge(&mut self, tree: &Tree, node: NodeId, member: MemberId) {
        if !produced(tree, node) {
            return;
        }
        let names = tree.names_resident();
        self.resident = self.resident + names - self.names;
        self.names = names;
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
    // job, `landing` of them written now, against the device `reader`
    // reads, which `device` names. Only what an object produced counts,
    // wherever it waits: a member's file, the envelope of the object that
    // reads the member, or the holds. A member that becomes a file, or
    // starts to wait, takes what it holds to its new level.
    //
    // What a member grows by is owed to its device until it lands: a file
    // takes its whole length at its first write and fills as its bytes
    // land; what waits for an object lands as it is admitted. A write the
    // device has no room for above the reserve lands all the same, and the
    // job waits for room: see `Spend::book`.
    pub(crate) fn admit_write(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        end: u64,
        (output, landing): (Output, u64),
        (reader, device): (&CapacityReader, Device),
    ) -> Result<(), ExtractionFailure> {
        self.check()?;
        self.extracting(tree, node)?;
        if !produced(tree, node) {
            return Ok(());
        }
        let key = (node, member);
        let at = level(tree, node, output);
        let fixed = |output: Output, was: usize| match (output, was) {
            (Output::Kept, 1..) => was,
            _ => at,
        };
        // A write within what the member already took: what it lands is no
        // longer owed.
        if let Some(entry) = self.extents.get_mut(&key)
            && fixed(output, entry.level) == entry.level
            && end <= entry.end
        {
            let landed = landing.min(entry.owed);
            if landed > 0 {
                entry.owed -= landed;
                self.spend[entry.device as usize].land(landed);
            }
            return Ok(());
        }
        let entry = self.extents.get(&key).copied();
        let (extent, was) = entry.map_or((0, 0), |entry| (entry.end, entry.level));
        let level = fixed(output, was);
        let moved = if level == was { 0 } else { extent };
        let grown = end.saturating_sub(extent);
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
        // What the member still owes another device is counted as landed
        // there: it is owed here from now on.
        let mut owed = 0;
        if let Some(entry) = entry {
            match entry.device == device {
                true => owed = entry.owed,
                false => owed = self.carry(entry.device, entry.owed, (reader, device)),
            }
        }
        self.book(device, grown, reader);
        owed += grown;
        let landed = match output {
            Output::Kept => grown,
            Output::File => landing,
        }
        .min(owed);
        owed -= landed;
        self.spend[device as usize].land(landed);
        self.written[was] = self.written[was].saturating_sub(moved);
        self.written[level] = written;
        self.extents.insert(
            key,
            Extent {
                end: reach,
                level,
                owed,
                device,
            },
        );
        Ok(())
    }

    // Bytes a repair writes into the posts. The posts are under none of the
    // job's limits, but what a repair writes into them takes room on their
    // disk as any output does, so it is owed to the reserve as any output
    // is, and counted as landed once admitted.
    pub(crate) fn admit_repaired(
        &mut self,
        bytes: u64,
        (reader, device): (&CapacityReader, Device),
    ) -> Result<(), ExtractionFailure> {
        if bytes == 0 {
            return Ok(());
        }
        self.check()?;
        self.book(device, bytes, reader);
        self.spend[device as usize].land(bytes);
        Ok(())
    }

    // A member that owed `owed` bytes to `from` moves to the device `reader`
    // reads: they are counted as landed on `from`, and owed to the new one,
    // which is what this returns.
    fn carry(&mut self, from: Device, owed: u64, (reader, to): (&CapacityReader, Device)) -> u64 {
        self.spend[from as usize].land(owed);
        self.book(to, owed, reader);
        owed
    }

    // `bytes` more owed to `device`. Should the job begin to wait for room
    // on it, its writing work is held.
    fn book(&mut self, device: Device, bytes: u64, reader: &CapacityReader) {
        self.spend[device as usize].book(bytes, reader, self.limits.min_free_bytes);
    }

    // A repair would write `output` bytes on the output device and
    // `working` on the working one before it can start, each read through
    // its reader: whether it may, on both at once. When either has no room,
    // nothing stays promised for the repair on the other, and the job waits
    // for room for its part on each, so one wake judges the two together.
    pub(crate) fn want_both(
        &mut self,
        (output, output_reader): (u64, &CapacityReader),
        (working, working_reader): (u64, &CapacityReader),
    ) -> bool {
        let reserve = self.limits.min_free_bytes;
        let [first, second] = &mut self.spend;
        let taken = first.taking();
        let first_may = first.want(output, output_reader, reserve);
        if first_may && second.want(working, working_reader, reserve) {
            return true;
        }
        if first_may {
            first.give_back(taken);
        }
        first.wait_for(output, output_reader);
        second.wait_for(working, working_reader);
        false
    }

    // What the job waits for on its repairs' behalf is asked again: each
    // repair that would still write wants its bytes anew.
    pub(crate) fn rewant(&mut self) {
        for spend in &mut self.spend {
            spend.wanted = 0;
        }
    }

    #[cfg(test)]
    pub(crate) fn wanted(&self) -> u64 {
        self.spend.iter().map(|spend| spend.wanted).sum()
    }

    // The free space every disk the job writes keeps.
    pub(crate) fn reserve(&self) -> u64 {
        self.limits.min_free_bytes
    }

    // Whether the job waits for room on a device: its runs decode nothing
    // until it has room again.
    pub(crate) fn disk_held(&self) -> bool {
        self.spend.iter().any(|spend| spend.waiting.is_some())
    }

    // The job's place among those waiting for room: the first device it
    // began to wait on.
    pub(crate) fn disk_ticket(&self) -> Option<u64> {
        self.spend.iter().filter_map(|spend| spend.waiting).min()
    }

    // The job's place on each device it waits on: a wait begun anew takes
    // a new one.
    pub(crate) fn disk_tickets(&self) -> [Option<u64>; 2] {
        [self.spend[0].waiting, self.spend[1].waiting]
    }

    // A reading came: whether every device the job waits on, `output` read
    // through `output` and the working one through `working`, has room above
    // the reserve for all the job owes it. If so, all of it is promised
    // again and the job no longer waits. One filesystem under both is
    // judged once, for both together. Only a good reading shows room: a
    // stale one is the last good one held across a failed probe, and the
    // job goes on waiting; with no reading at all nothing is checked.
    pub(crate) fn wake_disk(&mut self, output: &CapacityReader, working: &CapacityReader) -> bool {
        if !self.disk_held() {
            return true;
        }
        let reserve = self.limits.min_free_bytes;
        let readers = [output, working];
        let mut rooms = [None::<(Arc<DiskLedger>, Option<u64>)>, None];
        let mut good = true;
        for (index, spend) in self.spend.iter_mut().enumerate() {
            if spend.waiting.is_none() {
                continue;
            }
            let ledger = spend.bind(readers[index]);
            spend.settle();
            let room = match ledger.free_with(|| readers[index].current()) {
                None => None,
                Some(free) if free.stale => {
                    good = false;
                    Some(0)
                }
                Some(free) => Some(free.bytes.saturating_sub(reserve)),
            };
            // A disk at or under its reserve wakes no one, though the job
            // owes it nothing: what it writes next would not fit.
            good &= room != Some(0);
            rooms[index] = Some((ledger, room));
        }
        let need = |index: usize| self.spend[index].needed();
        let fits = match (&rooms[0], &rooms[1]) {
            (Some((one, room)), Some((other, _))) if one.same_disk(other) => {
                room.is_none_or(|room| need(0).saturating_add(need(1)) <= room)
            }
            _ => rooms.iter().enumerate().all(|(index, room)| {
                room.as_ref()
                    .is_none_or(|(_, room)| room.is_none_or(|room| need(index) <= room))
            }),
        };
        if !good || !fits {
            return false;
        }
        for spend in &mut self.spend {
            spend.rebook();
        }
        true
    }

    // Whether the job owes nothing and has nothing to write again.
    pub(crate) fn owes_nothing(&self) -> bool {
        self.spend.iter().all(|spend| spend.needed() == 0)
    }

    // The job waits for room though it owes nothing, has nothing to write
    // again, and nothing of it is left that would write: it no longer
    // waits, whatever the disk reads. Whether it waited.
    pub(crate) fn wake_idle(&mut self) -> bool {
        let held = self.disk_held();
        if held && self.owes_nothing() {
            for spend in &mut self.spend {
                spend.rebook();
            }
        }
        held && !self.disk_held()
    }

    // The member's file was closed: what it still owes is counted as
    // landed. A file closed short keeps holes that never fill, which the
    // next reading shows the disk without.
    pub(crate) fn closed(&mut self, node: NodeId, member: MemberId) {
        if let Some(entry) = self.extents.get_mut(&(node, member))
            && entry.owed > 0
        {
            self.spend[entry.device as usize].land(entry.owed);
            entry.owed = 0;
        }
    }

    // The member's file was emptied or removed, and its record started
    // over, folded or went: its bytes no longer count, and it is charged
    // as it stands.
    pub(crate) fn forget(&mut self, tree: &Tree, node: NodeId, member: MemberId) {
        self.unwrite(node, member);
        self.charge(tree, node, member);
    }

    // A run stopped for room takes back what it built, or has done: each
    // member that goes meanwhile, the run's own or one buried under it, is
    // written again when the run runs again. The job wakes only when its
    // device has room for those bytes too, or it would fill the disk, stop
    // and free them over and over.
    pub(crate) fn rewriting(&mut self, rewriting: bool) {
        self.rewriting = rewriting;
    }

    fn unwrite(&mut self, node: NodeId, member: MemberId) {
        if let Some(entry) = self.extents.remove(&(node, member)) {
            self.written[entry.level] = self.written[entry.level].saturating_sub(entry.end);
            let spend = &mut self.spend[entry.device as usize];
            spend.forgo(entry.owed);
            if self.rewriting && spend.waiting.is_some() {
                spend.again = spend.again.saturating_add(entry.end);
            }
        }
    }

    // A member's file as a checkpoint left it, written as far as its
    // namespace's coverage reaches, at the level the tree now says it
    // counts at, on the device it lies on: the output one for a file, and
    // `kept` for a member that waits. What it has yet to write past that
    // is owed when it next grows.
    pub(crate) fn restore(&mut self, tree: &Tree, node: NodeId, member: MemberId, kept: Device) {
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
            let output = output_of(tree, node, member);
            let level = level(tree, node, output);
            let device = match output {
                Output::File => Device::Output,
                Output::Kept => kept,
            };
            self.extents.insert(
                (node, member),
                Extent {
                    end: extent,
                    level,
                    owed: 0,
                    device,
                },
            );
            self.written[level] = self.written[level].saturating_add(extent);
        }
    }

    // The restored tree's members, each charged as the tree holds it: what
    // a checkpoint brings back is counted from the tree, never stored, and a
    // member another object reads counts as far as it arrived, as a file
    // does. No checkpoint says which names a run before judged, so each
    // waits to be judged again before the job delivers: a refusal here would
    // stop the restore itself, and the job would never be failed for it.
    pub(crate) fn restore_members(&mut self, tree: &Tree, kept: Device) {
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
                self.restore(tree, node, member, kept);
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

// A job that ends, is cancelled or is dropped gives back all it promised,
// and no longer waits for room.
impl Drop for Caps {
    fn drop(&mut self) {
        for spend in &mut self.spend {
            spend.release_all();
        }
    }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use super::*;
    use crate::operations::disk::{Capacity, CapacityReading};

    const MIB: u64 = 1024 * 1024;

    fn free(available_bytes: u64) -> CapacityReader {
        CapacityReader::from_fn(move || {
            Capacity::Known(CapacityReading {
                available_bytes,
                total_bytes: u64::MAX,
                sampled_at: Instant::now(),
                stale: false,
            })
        })
    }

    // Two repairs due in one walk are each promised their own bytes: the
    // second does not go out on what the first was promised.
    #[test]
    fn each_repair_that_goes_out_is_promised_its_own_bytes() {
        let reader = free(10 * 1024 * MIB);
        let mut spend = Spend::default();
        assert!(spend.want(100 * MIB, &reader, MIB));
        assert!(spend.want(50 * MIB, &reader, MIB));
        assert_eq!(reader.ledger().promised(), 150 * MIB);
    }

    // A repair held while the job waited is promised in hand when it wakes,
    // and takes that promise when it goes out; another is promised afresh.
    #[test]
    fn a_held_repair_takes_what_was_put_in_hand_for_it_and_no_more() {
        let short = Arc::new(std::sync::atomic::AtomicU64::new(10 * MIB));
        let reader = {
            let short = Arc::clone(&short);
            CapacityReader::from_fn(move || {
                Capacity::Known(CapacityReading {
                    available_bytes: short.load(Ordering::SeqCst),
                    total_bytes: u64::MAX,
                    sampled_at: Instant::now(),
                    stale: false,
                })
            })
        };
        let mut spend = Spend::default();
        assert!(
            !spend.want(100 * MIB, &reader, MIB),
            "no room: the job waits"
        );
        assert_eq!(reader.ledger().promised(), 0);
        short.store(10 * 1024 * MIB, Ordering::SeqCst);
        spend.rebook();
        assert_eq!(reader.ledger().promised(), 100 * MIB, "promised in hand");
        assert!(spend.want(100 * MIB, &reader, MIB));
        assert_eq!(reader.ledger().promised(), 100 * MIB, "taken from hand");
        assert!(spend.want(30 * MIB, &reader, MIB));
        assert_eq!(reader.ledger().promised(), 130 * MIB, "the next is its own");
    }

    // A member that moves to another device brings what it still owes: it
    // is promised on the new device, where it lands from now on.
    #[test]
    fn a_member_that_moves_device_is_owed_on_the_new_one() {
        let (output, working) = (free(10 * 1024 * MIB), free(10 * 1024 * MIB));
        let mut caps = Caps::unbounded();
        caps.book(Device::Output, 10 * MIB, &output);
        let owed = caps.carry(Device::Output, 10 * MIB, (&working, Device::Working));
        assert_eq!(owed, 10 * MIB);
        assert_eq!(caps.spend[Device::Working as usize].owed, 10 * MIB);
        assert_eq!(caps.spend[Device::Output as usize].owed, 0);
        assert!(working.ledger().promised() >= 10 * MIB);
    }

    // A member taken back while the job waits, by a run stopped for room,
    // is written again once the run goes on: the job wakes only when the
    // disk has room for it as well as for what it owes, and what it is
    // promised then covers both, so the run that writes it again does not
    // fill the disk and stop once more.
    #[test]
    fn a_member_retracted_while_the_job_waits_is_room_the_job_needs_to_wake() {
        let available = Arc::new(std::sync::atomic::AtomicU64::new(10 * MIB));
        let working = {
            let available = Arc::clone(&available);
            CapacityReader::from_fn(move || {
                Capacity::Known(CapacityReading {
                    available_bytes: available.load(Ordering::SeqCst),
                    total_bytes: u64::MAX,
                    sampled_at: Instant::now(),
                    stale: false,
                })
            })
        };
        let output = free(10 * 1024 * MIB);
        let mut caps = Caps::unbounded();
        Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
        let member = (NodeId(1), MemberId(0));
        caps.extents.insert(
            member,
            Extent {
                end: 40 * MIB,
                level: 0,
                owed: 0,
                device: Device::Working,
            },
        );
        caps.written[0] = 40 * MIB;
        caps.book(Device::Working, 100 * MIB, &working);
        assert!(caps.disk_held(), "no room for what it owes: the job waits");

        caps.rewriting(true);
        caps.forget(&Tree::new(), member.0, member.1);
        caps.rewriting(false);
        available.store(MIB + 120 * MIB, Ordering::SeqCst);
        assert!(
            !caps.wake_disk(&output, &working),
            "room for what it owes, not for the member it writes again"
        );
        available.store(MIB + 140 * MIB, Ordering::SeqCst);
        assert!(caps.wake_disk(&output, &working));
        assert_eq!(working.ledger().promised(), 140 * MIB);
        caps.book(Device::Working, 40 * MIB, &working);
        assert!(!caps.disk_held());
        assert_eq!(
            working.ledger().promised(),
            140 * MIB,
            "the member written again spends what was promised for it"
        );
    }

    fn reading(available: &Arc<AtomicU64>, stale: bool) -> CapacityReader {
        let available = Arc::clone(available);
        CapacityReader::from_fn(move || {
            Capacity::Known(CapacityReading {
                available_bytes: available.load(Ordering::SeqCst),
                total_bytes: u64::MAX,
                sampled_at: Instant::now(),
                stale,
            })
        })
    }

    fn member(caps: &mut Caps, member: (NodeId, MemberId), end: u64, device: Device) {
        caps.extents.insert(
            member,
            Extent {
                end,
                level: 0,
                owed: 0,
                device,
            },
        );
        caps.written[0] += end;
    }

    // Every member that goes while a run stopped for room takes back what
    // it built is written again, the run's own and those buried under them
    // alike; one that goes for any other reason while the job waits is not,
    // and asks for no room.
    #[test]
    fn only_what_a_stopped_run_takes_back_is_room_the_job_needs_again() {
        let available = Arc::new(AtomicU64::new(10 * MIB));
        let working = reading(&available, false);
        let output = free(10 * 1024 * MIB);
        let mut caps = Caps::unbounded();
        Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
        let (own, buried, refused) = (
            (NodeId(1), MemberId(0)),
            (NodeId(2), MemberId(0)),
            (NodeId(3), MemberId(0)),
        );
        member(&mut caps, own, 30 * MIB, Device::Working);
        member(&mut caps, buried, 20 * MIB, Device::Working);
        member(&mut caps, refused, 70 * MIB, Device::Working);
        caps.book(Device::Working, 100 * MIB, &working);
        assert!(caps.disk_held());

        caps.forget(&Tree::new(), refused.0, refused.1);
        caps.rewriting(true);
        caps.forget(&Tree::new(), own.0, own.1);
        caps.forget(&Tree::new(), buried.0, buried.1);
        caps.rewriting(false);
        available.store(MIB + 149 * MIB, Ordering::SeqCst);
        assert!(
            !caps.wake_disk(&output, &working),
            "a byte short of what it owes and both members it writes again"
        );
        available.store(MIB + 150 * MIB, Ordering::SeqCst);
        assert!(caps.wake_disk(&output, &working));
        assert_eq!(working.ledger().promised(), 150 * MIB);
    }

    // A job that waits, owes nothing and has nothing to write again wakes
    // idle whatever the reading: holding it would hold back nothing that
    // takes room. A reading under the reserve still wakes no one.
    #[test]
    fn a_job_that_owes_nothing_wakes_idle_whatever_the_reading() {
        let available = Arc::new(AtomicU64::new(0));
        let working = reading(&available, false);
        let output = free(10 * 1024 * MIB);
        let mut caps = Caps::unbounded();
        Arc::make_mut(&mut caps.limits).min_free_bytes = 5 * 1024 * MIB;
        let file = (NodeId(1), MemberId(0));
        member(&mut caps, file, 0, Device::Working);
        caps.extents.get_mut(&file).unwrap().owed = 40 * MIB;
        caps.book(Device::Working, 40 * MIB, &working);
        assert!(caps.disk_held(), "the disk is under its reserve");
        assert!(
            !caps.wake_disk(&output, &working),
            "it still owes what it took"
        );
        assert!(!caps.wake_idle(), "nor does it wake idle");
        caps.closed(file.0, file.1);
        assert!(caps.owes_nothing());
        assert!(
            !caps.wake_disk(&output, &working),
            "the disk is still under its reserve"
        );
        assert!(caps.wake_idle(), "it owes nothing now");
        assert!(!caps.disk_held());
        assert_eq!(working.ledger().promised(), 0);
    }

    // A stale reading is the last good one held across a failed probe: it
    // begins no wait, as no reading begins none.
    #[test]
    fn a_stale_reading_begins_no_wait() {
        let available = Arc::new(AtomicU64::new(0));
        let stale = reading(&available, true);
        let mut caps = Caps::unbounded();
        Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
        caps.book(Device::Output, 100 * MIB, &stale);
        assert!(!caps.disk_held());
        assert!(caps.want_both((10 * MIB, &stale), (10 * MIB, &stale)));
        assert!(!caps.disk_held());
    }

    // A repair that writes on both devices goes out on both or on neither:
    // when the second has no room, nothing stays promised on the first, and
    // the job waits for its part on each.
    #[test]
    fn a_repair_held_on_one_device_holds_no_room_on_the_other() {
        let roomy = free(10 * 1024 * MIB);
        let available = Arc::new(AtomicU64::new(10 * MIB));
        let short = reading(&available, false);
        let mut caps = Caps::unbounded();
        Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
        assert!(!caps.want_both((100 * MIB, &roomy), (100 * MIB, &short)));
        assert_eq!(roomy.ledger().promised(), 0, "nothing kept on the first");
        assert_eq!(short.ledger().promised(), 0);
        assert!(caps.disk_held());
        available.store(MIB + 100 * MIB, Ordering::SeqCst);
        assert!(caps.wake_disk(&roomy, &short));
        assert_eq!(roomy.ledger().promised(), 100 * MIB);
        assert_eq!(short.ledger().promised(), 100 * MIB);
        assert!(caps.want_both((100 * MIB, &roomy), (100 * MIB, &short)));
        assert_eq!(
            roomy.ledger().promised(),
            100 * MIB,
            "the repair takes what was put in hand on the first"
        );
        assert_eq!(short.ledger().promised(), 100 * MIB, "and on the second");
    }
}
