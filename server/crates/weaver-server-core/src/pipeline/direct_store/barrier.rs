//! The per-set coverage barrier.
//!
//! Between barriers nothing durable happens: successfully written bytes
//! accumulate as coalesced source ranges in memory. A barrier converts that
//! into one replaced checkpoint row, in a fixed order:
//!
//! 1. **Drain** — stop new writes for the set and drain the mapped batch
//!    already in flight. Overshoot is bounded to one decoded write batch.
//! 2. **Sync** — sync every destination and envelope file touched *since the
//!    previous successful barrier*, not only the files in the final batch. The
//!    published floors cover the whole interval, so a file touched early and not
//!    again would otherwise be claimed but unsynced.
//! 3. **Persist** — write and commit the single snapshot row. The commit is the
//!    checkpoint's sync.
//! 4. **Publish** — only now are the floors visible in memory, and only now is
//!    the transient state cleared.
//!
//! A failure at any step leaves the previous checkpoint authoritative and the
//! touched-file set uncleared, so the next successful barrier still syncs
//! everything the interval touched. It also starts a cooldown: the dirty bytes
//! that provoked the barrier are still dirty, so the age trigger would
//! otherwise be due again immediately and retry a wedged sync or a down
//! database as fast as the caller polls. The cooldown damps **only** that
//! trigger — the byte threshold and every explicit demand still fire, because a
//! shutdown must always get its attempt.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use super::ByteRanges;
use super::snapshot::{
    CoverageSnapshot, DestinationClaim, DestinationExtent, SnapshotError, VolumeFloor,
};
use crate::e2e_failpoint;
use crate::jobs::ids::JobId;

/// Aggregate unique dirty bytes across the set that force a barrier.
pub(crate) const BARRIER_DIRTY_BYTES: u64 = 256 * 1024 * 1024;

/// How long dirty data may exist before a barrier is forced, so an idle set
/// still checkpoints.
pub(crate) const BARRIER_DIRTY_AGE: Duration = Duration::from_secs(5);

/// How long the [`BarrierTrigger::DirtyAge`] trigger is suppressed after a
/// failed barrier. A failing barrier keeps its dirty bytes, so without this the
/// age trigger is due again immediately and the set retries a failing sync or a
/// failing transaction as fast as the loop can call it.
pub(crate) const BARRIER_FAILURE_BACKOFF: Duration = Duration::from_secs(5);

/// The ceiling the backoff doubles up to. A wedged disk or a down database
/// should cost one attempt every few minutes, not one per loop iteration.
pub(crate) const BARRIER_FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// The cooldown after `consecutive_failures` failed barriers: 5 s doubling per
/// failure, capped at 5 min.
fn failure_backoff(consecutive_failures: u32) -> Duration {
    let doublings = consecutive_failures.saturating_sub(1).min(16);
    BARRIER_FAILURE_BACKOFF
        .saturating_mul(1u32 << doublings)
        .min(BARRIER_FAILURE_BACKOFF_MAX)
}

/// Why the caller is demanding a barrier. The caller decides when these happen;
/// the controller only records which one it served.
// `Demotion` still has no caller: demotion by reconstruction *deletes* the row
// rather than checkpointing it, so the demand exists in the vocabulary the seam
// is specified in without a path that raises it. `Pause` and `PhaseChange` are
// wired at the command seam and the set's last volume respectively.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarrierDemand {
    Pause,
    Shutdown,
    PhaseChange,
    Demotion,
    Finalization,
    /// A PAR2 repair rewrote bytes the deleted row used to claim.
    /// Demanded immediately after the repaired spans are routed, so the window
    /// where the set has no durable coverage at all is as short as the code can
    /// make it rather than one 5 s timer.
    RepairRecreate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarrierTrigger {
    /// Aggregate unique dirty bytes reached [`BARRIER_DIRTY_BYTES`].
    DirtyBytes,
    /// Dirty data has existed for [`BARRIER_DIRTY_AGE`].
    DirtyAge,
    /// The set's plan digest has moved since the committed row was written, so
    /// that row would be refused at restart even though it describes this very
    /// set (see [`CoverageBarrier::set_plan_digest`]).
    ///
    /// Fires with no dirty bytes at all, which no other automatic trigger does:
    /// the work it exists to save is already durable, and what is stale is the
    /// label the restart reader validates it against. It is damped by the same
    /// failure cooldown as the age trigger, because a set whose persist is
    /// failing must not re-stamp on every turn of the pipeline loop.
    PlanDigestChanged,
    Demand(BarrierDemand),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarrierStep {
    Drain,
    Sync,
    Persist,
    Publish,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BarrierError {
    Drain(String),
    Sync {
        destination: String,
        error: String,
    },
    /// Encoding failed, so nothing was written. Classified as a persist-step
    /// failure because it happens inside step 3.
    Encode(SnapshotError),
    Persist(String),
}

impl BarrierError {
    #[allow(dead_code)]
    pub(crate) fn step(&self) -> BarrierStep {
        match self {
            Self::Drain(_) => BarrierStep::Drain,
            Self::Sync { .. } => BarrierStep::Sync,
            Self::Encode(_) | Self::Persist(_) => BarrierStep::Persist,
        }
    }
}

impl std::fmt::Display for BarrierError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drain(error) => write!(formatter, "coverage barrier drain failed: {error}"),
            Self::Sync { destination, error } => write!(
                formatter,
                "coverage barrier failed to sync {destination}: {error}"
            ),
            Self::Encode(error) => write!(formatter, "coverage barrier encode failed: {error}"),
            Self::Persist(error) => write!(formatter, "coverage barrier persist failed: {error}"),
        }
    }
}

/// Step 1. The caller owns quiescing: it knows the write pool, the reorder
/// buffer and the lease state, none of which belong in this module.
pub(crate) trait BarrierDrain {
    fn drain(&mut self) -> Result<(), String>;
}

/// Step 2. One call per destination file touched during the interval.
pub(crate) trait DestinationSync {
    fn sync(&mut self, relative_path: &str) -> Result<(), String>;
}

/// Step 3. Exactly one replaced row per archive set — no history, no append,
/// and no per-volume statements.
pub(crate) trait CoveragePersist {
    /// Replaces the set's checkpoint row.
    ///
    /// **Single writer per (job, set).** The row is replaced wholesale and the
    /// generation counter lives in the blob, not in a compare-and-set: nothing
    /// here serializes two writers, so a stale one would silently clobber a
    /// newer generation with older floors. The invariant that makes that
    /// impossible is structural — one pipeline owns a job, and one
    /// [`CoverageBarrier`] owns each of its archive sets — and it is the
    /// caller's to keep. If a second writer ever becomes possible (a
    /// concurrently repairing job, a second process on the same database), this
    /// needs a real generation guard in the statement, not a comment.
    fn write(&mut self, job_id: JobId, set_name: &str, blob: &[u8]) -> Result<(), String>;

    /// Retires the set's checkpoint. Repair over checkpoint-covered output
    /// deletes the row and lets the next barrier recreate coverage from
    /// scratch.
    fn delete(&mut self, job_id: JobId, set_name: &str) -> Result<(), String>;
}

/// The production [`CoveragePersist`]: a thin adapter over the two
/// `active_direct_coverage` statements, so the barrier's persist step is the
/// real database write rather than only a test double.
///
/// Both calls block on the SQL runtime, which is what step 3 wants — the commit
/// *is* the checkpoint's sync, and the barrier may not publish floors until it
/// returns.
#[derive(Clone)]
pub(crate) struct DatabaseCoveragePersist {
    database: crate::Database,
}

impl std::fmt::Debug for DatabaseCoveragePersist {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DatabaseCoveragePersist")
    }
}

impl DatabaseCoveragePersist {
    pub(crate) fn new(database: crate::Database) -> Self {
        Self { database }
    }
}

impl CoveragePersist for DatabaseCoveragePersist {
    fn write(&mut self, job_id: JobId, set_name: &str, blob: &[u8]) -> Result<(), String> {
        self.database
            .save_direct_coverage(job_id, set_name, blob)
            .map_err(|error| error.to_string())
    }

    fn delete(&mut self, job_id: JobId, set_name: &str) -> Result<(), String> {
        self.database
            .delete_direct_coverage(job_id, set_name)
            .map_err(|error| error.to_string())
    }
}

/// One routed source span that reached its destination successfully.
///
/// Recorded only after the write returned: partial failure leaves orphan bytes,
/// and the coverage map is the truth, not the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedWrite {
    pub(crate) volume_index: u32,
    /// Offset within the source volume.
    pub(crate) source_offset: u64,
    pub(crate) len: u64,
    pub(crate) member_index: u32,
    /// Offset within the destination file.
    pub(crate) destination_offset: u64,
}

/// Why a routed write was not recorded.
///
/// Every variant is a caller bug: the write named an identity the controller
/// was never told about, and the coverage map cannot invent it. The write is
/// refused **entirely** — no floor advances, nothing becomes dirty and no
/// destination is marked for sync — because a checkpoint that claims bytes it
/// cannot attribute to a synced file is worse than no checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteRefused {
    /// No destination is registered for this member index, so the write's
    /// bytes would never be claimed, synced or attributed.
    UnregisteredMember { member_index: u32 },
    /// No source volume is registered for this volume index, so its floor
    /// would be published against a defaulted NZB file index — refetch would
    /// then target the wrong file.
    UnregisteredVolume { volume_index: u32 },
}

impl std::fmt::Display for WriteRefused {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnregisteredMember { member_index } => write!(
                formatter,
                "routed write names member {member_index}, which has no registered destination"
            ),
            Self::UnregisteredVolume { volume_index } => write!(
                formatter,
                "routed write names volume {volume_index}, which is not registered"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BarrierReport {
    pub(crate) trigger: BarrierTrigger,
    /// The generation now committed.
    pub(crate) generation: u64,
    /// The order the four steps actually ran in.
    pub(crate) steps: Vec<BarrierStep>,
    pub(crate) synced_destinations: usize,
    pub(crate) snapshot_bytes: usize,
    /// Volume index to published contiguous floor.
    pub(crate) published_floors: BTreeMap<u32, u64>,
}

#[derive(Debug, Clone, Default)]
struct VolumeCoverage {
    file_index: u32,
    /// The volume's decoded length, once every article of it has arrived. `None`
    /// while it is still downloading.
    ///
    /// Deliberately the *length* rather than a `complete` flag: the snapshot's
    /// bit means "all bytes durable", which is the conjunction of this and the
    /// published floor, and a flag latched here would publish that claim for a
    /// volume whose bytes are still held. See
    /// [`super::snapshot::VolumeFloor::complete`].
    decoded_len: Option<u64>,
    /// Last published contiguous floor. Bytes below it have been trimmed out of
    /// `ranges`, so the floor itself is what continuity is measured from.
    floor: u64,
    ranges: ByteRanges,
}

#[derive(Debug, Clone)]
struct DestinationCoverage {
    relative_path: String,
    ranges: ByteRanges,
}

/// Per-archive-set coverage tracker and barrier driver.
#[derive(Debug)]
pub(crate) struct CoverageBarrier {
    job_id: JobId,
    set_name: String,
    /// The digest the **next** snapshot stamps. Pushed by the set on every
    /// registration, because it is a function of facts that grow as volumes
    /// arrive; see [`Self::set_plan_digest`].
    plan_digest: [u8; 32],
    /// The digest the **committed** row carries. Only
    /// [`BarrierTrigger::PlanDigestChanged`] reads it, and only against
    /// `plan_digest`.
    committed_digest: [u8; 32],
    committed_generation: u64,
    /// Transient coalesced source ranges, per source volume. In memory only —
    /// nothing per-segment is ever persisted.
    volumes: BTreeMap<u32, VolumeCoverage>,
    /// Accumulated destination claims for the whole set, across barriers.
    destinations: BTreeMap<u32, DestinationCoverage>,
    /// Destination member indices touched since the last **successful**
    /// barrier. Cleared only on success.
    touched: BTreeSet<u32>,
    /// Aggregate unique dirty bytes across the set. Out-of-order writes count
    /// even when a volume's contiguous floor is stalled.
    dirty_bytes: u64,
    dirty_since: Option<Instant>,
    published_floors: BTreeMap<u32, u64>,
    /// Barriers that have failed in a row. Reset by any success.
    consecutive_failures: u32,
    /// While set, the age trigger is suppressed. Byte-threshold and demanded
    /// barriers ignore it.
    cooldown_until: Option<Instant>,
    /// The crypt rows the next checkpoint carries, by member index. Empty for
    /// every set with no encrypted member, which is every unencrypted set.
    member_crypt: BTreeMap<u32, super::router::crypt::MemberCryptSnapshot>,
}

impl CoverageBarrier {
    pub(crate) fn new(job_id: JobId, set_name: impl Into<String>, plan_digest: [u8; 32]) -> Self {
        Self {
            job_id,
            set_name: set_name.into(),
            plan_digest,
            committed_digest: plan_digest,
            committed_generation: 0,
            volumes: BTreeMap::new(),
            destinations: BTreeMap::new(),
            touched: BTreeSet::new(),
            dirty_bytes: 0,
            dirty_since: None,
            published_floors: BTreeMap::new(),
            consecutive_failures: 0,
            cooldown_until: None,
            member_crypt: BTreeMap::new(),
        }
    }

    /// Hands the barrier the crypt rows its next checkpoint must carry.
    ///
    /// Pushed rather than pulled because the barrier owns no router: the set
    /// reads them off the router immediately before every run, so a checkpoint
    /// can never carry padding or checkpoints older than the coverage beside
    /// them.
    pub(crate) fn set_member_crypt(
        &mut self,
        rows: BTreeMap<u32, super::router::crypt::MemberCryptSnapshot>,
    ) {
        self.member_crypt = rows;
    }

    /// Points the next snapshot at the plan the set is **currently** routing
    /// against.
    ///
    /// # Why this is pushed at all
    ///
    /// The digest binds the member names and their declared unpacked sizes, and
    /// a set learns those as its volumes arrive: a two-member archive whose
    /// second member's header lives in volume 3 has a one-member digest until
    /// volume 3 lands, and a member that migrates into the envelope leaves the
    /// digest again. Stamping once — at the first adopted member, which is where
    /// the barrier is built — froze a digest that was true for one instant, and
    /// every row written afterwards was refused at restart with
    /// `PlanDigestMismatch` on a set that was in perfect health. That is a silent
    /// whole-set redownload, and it is the *normal* case for a multi-member set.
    ///
    /// # What a digest change does to already-committed coverage: nothing
    ///
    /// Deliberately nothing, and this is the load-bearing half. Discovering a
    /// member does not move one byte of an existing member's destination: the
    /// layout's logical offsets are prefix sums a later volume only extends, and
    /// the library guarantees no offset moves while a member still routes. So
    /// every floor and every claim already accumulated still describes exactly
    /// the bytes it did before, and they are carried into the next snapshot
    /// unchanged — the digest is a *label* for validating the row against a
    /// rebuilt plan, not a checksum of the coverage. Clearing coverage here
    /// would throw away durable work to record a fact about the plan.
    ///
    /// What *is* stale is the committed row's label, which is why the change
    /// makes the barrier due: see [`BarrierTrigger::PlanDigestChanged`].
    pub(crate) fn set_plan_digest(&mut self, plan_digest: [u8; 32]) {
        self.plan_digest = plan_digest;
    }

    /// Drops a destination the set no longer owns, so the next snapshot carries
    /// no claim for it.
    ///
    /// The reachable caller is the split-BLAKE2sp migration, which moves a
    /// member's routed bytes into the envelope and **unlinks its partial**. A
    /// claim on a file that no longer exists is refused at restart as a missing
    /// destination, taking the whole row down to a redownload. Keeping it in the
    /// sync set is worse than useless besides: the write pool opens for sync with
    /// `create(true)`, so the barrier either fsyncs an unlinked inode or puts an
    /// empty `.direct.partial` back beside the member it just migrated.
    ///
    /// The entry is **removed**, not emptied. The rule is that a claim over
    /// zero bytes is not a claim and is omitted from the blob, and a
    /// destination that is gone must land in the same place rather than
    /// becoming a zero-byte claim on a path restart would then probe.
    ///
    /// The path is checked, not just the key: member ids are in-run counters,
    /// and retiring a live destination because an id was reused would drop
    /// claims on bytes that really are on disk. A mismatch keeps the claim,
    /// which is the safe direction — the row is refused and the set redownloads,
    /// exactly as it did before this existed.
    pub(crate) fn retire_destination(&mut self, member_index: u32, relative_path: &str) -> bool {
        let matches = self
            .destinations
            .get(&member_index)
            .is_some_and(|destination| destination.relative_path == relative_path);
        if !matches {
            return false;
        }
        self.destinations.remove(&member_index);
        // Or the barrier would fsync a path nothing owns any more, and a failure
        // there fails the whole barrier.
        self.touched.remove(&member_index);
        true
    }

    /// Rebuilds a controller from a validated checkpoint after restart.
    ///
    /// The floors and destination claims come back; the transient per-segment
    /// ranges deliberately do not, because they were never persisted. Coverage
    /// above a floor is redownloaded.
    pub(crate) fn resume(
        job_id: JobId,
        set_name: impl Into<String>,
        snapshot: &CoverageSnapshot,
    ) -> Self {
        let mut barrier = Self::new(job_id, set_name, snapshot.plan_digest);
        barrier.committed_generation = snapshot.generation;
        for entry in &snapshot.floors {
            let volume = barrier.volumes.entry(entry.volume_index).or_default();
            volume.file_index = entry.file_index;
            volume.floor = entry.floor;
            // A published `complete` always means the floor *is* the volume's
            // decoded length, so the floor is the length coming back. Rebuilding
            // it this way keeps the invariant self-checking: the next barrier
            // re-derives the bit from `floor >= decoded_len` exactly as the run
            // that wrote it did, rather than carrying a claim forward unexamined.
            volume.decoded_len = entry.complete.then_some(entry.floor);
            barrier
                .published_floors
                .insert(entry.volume_index, entry.floor);
        }
        for claim in &snapshot.destinations {
            let mut ranges = ByteRanges::new();
            for extent in &claim.extents {
                ranges.insert(extent.start, extent.len());
            }
            barrier.destinations.insert(
                claim.member_index,
                DestinationCoverage {
                    relative_path: claim.relative_path.clone(),
                    ranges,
                },
            );
        }
        barrier
    }

    /// Reporting accessors the barrier publishes for a health surface the
    /// wiring does not wire; exercised by this module's own tests.
    #[allow(dead_code)]
    pub(crate) fn generation(&self) -> u64 {
        self.committed_generation
    }

    #[allow(dead_code)]
    pub(crate) fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    #[allow(dead_code)]
    pub(crate) fn published_floors(&self) -> &BTreeMap<u32, u64> {
        &self.published_floors
    }

    /// Barriers that have failed in a row, for the caller's health reporting.
    /// Zero after any success.
    #[allow(dead_code)]
    pub(crate) fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// When the age trigger becomes eligible again after a failure. `None` when
    /// no barrier is in backoff. A caller scheduling its own wake-ups can use
    /// this instead of polling.
    #[allow(dead_code)]
    pub(crate) fn cooldown_until(&self) -> Option<Instant> {
        self.cooldown_until
    }

    /// Everything the controller knows is durably on disk for one source
    /// volume: the published floor, plus the coalesced ranges above it that
    /// have been written but not yet checkpointed.
    ///
    /// This is the truth demotion-by-reconstruction reads. Deliberately *this*
    /// rather than the router's own routed map: the controller is only told
    /// about writes whose every destination returned, so a span whose write
    /// failed is absent here and will be refetched rather than read back from a
    /// file that never received it.
    pub(crate) fn volume_coverage(&self, volume_index: u32) -> Option<ByteRanges> {
        let volume = self.volumes.get(&volume_index)?;
        let mut coverage = ByteRanges::new();
        if volume.floor > 0 {
            coverage.insert(0, volume.floor);
        }
        for &(start, end) in volume.ranges.ranges() {
            coverage.insert(start, end - start);
        }
        Some(coverage)
    }

    /// Everything durably placed in **one destination's own space**.
    ///
    /// Unlike [`Self::volume_coverage`] these ranges are never trimmed against
    /// a floor, because a destination's claim is what restart re-probes and
    /// what the hybrid provider reads back: a floor describes a *source
    /// volume*'s refetch point and says nothing about which file holds a byte.
    /// For a volume's envelope the destination space **is** the physical space,
    /// so this is the exact answer to "did the envelope ever receive this
    /// offset".
    pub(crate) fn destination_coverage(&self, member_index: u32) -> Option<&ByteRanges> {
        self.destinations
            .get(&member_index)
            .map(|destination| &destination.ranges)
    }

    /// Destination keys and their relative paths, for everything touched since
    /// the last successful barrier, in member order.
    ///
    /// The key rides along because a relative path alone no longer says which
    /// root it hangs off: member payload resolves under the job's staging root
    /// and envelopes under its working directory, and the key band is what
    /// separates them (see
    /// [`super::plan::DirectSetPlan::barrier_destination_path`]).
    pub(crate) fn touched_destinations(&self) -> Vec<(u32, &str)> {
        self.touched
            .iter()
            .filter_map(|member_index| {
                self.destinations
                    .get(member_index)
                    .map(|destination| (*member_index, destination.relative_path.as_str()))
            })
            .collect()
    }

    /// Binds a source volume to its NZB file index and a member to its
    /// destination path. Idempotent.
    pub(crate) fn register_volume(&mut self, volume_index: u32, file_index: u32) {
        self.volumes.entry(volume_index).or_default().file_index = file_index;
    }

    /// Records that every article of a source volume has arrived, and how long
    /// the volume decoded to.
    ///
    /// The length is what makes this a fact rather than a latch: the snapshot's
    /// `complete` bit is re-derived at every barrier from this length against the
    /// floor being published, so a volume that finished downloading into *held*
    /// bytes publishes `complete: false` until those bytes are actually durable.
    pub(crate) fn note_volume_complete(&mut self, volume_index: u32, decoded_len: u64) {
        self.volumes.entry(volume_index).or_default().decoded_len = Some(decoded_len);
    }

    pub(crate) fn register_destination(
        &mut self,
        member_index: u32,
        relative_path: impl Into<String>,
    ) {
        self.destinations
            .entry(member_index)
            .or_insert_with(|| DestinationCoverage {
                relative_path: relative_path.into(),
                ranges: ByteRanges::new(),
            });
    }

    /// Records one successfully written routed span and returns the number of
    /// bytes it newly made dirty.
    ///
    /// A destination the write touches becomes part of this interval's sync
    /// set, whether or not it is touched again before the barrier.
    ///
    /// A write the controller cannot attribute is **refused whole**: see
    /// [`WriteRefused`]. Refusing in debug builds is loud, because every
    /// refusal is a routing bug in the caller, not a runtime condition.
    pub(crate) fn record_write(
        &mut self,
        write: &RoutedWrite,
        now: Instant,
    ) -> Result<u64, WriteRefused> {
        let result = self.try_record_write(write, now);
        debug_assert!(
            result.is_ok(),
            "direct-store coverage refused a routed write: {:?} ({:?})",
            result.as_ref().err(),
            write
        );
        result
    }

    /// [`Self::record_write`] without the debug assertion, so this module's own
    /// tests can exercise the refusal path that the original wiring must never
    /// reach.
    pub(super) fn try_record_write(
        &mut self,
        write: &RoutedWrite,
        now: Instant,
    ) -> Result<u64, WriteRefused> {
        // Both checks run *before* any mutation. Half-recording a write would
        // advance the source floor for bytes whose destination is never
        // touched, synced or claimed, so restart would skip segments nothing
        // ever wrote — exactly the loss the checkpoint exists to bound.
        //
        // Neither identity can be auto-created from a write: a destination
        // needs its relative path and a volume needs its NZB file index, and
        // the write carries neither. Registration is the caller's job.
        if !self.destinations.contains_key(&write.member_index) {
            return Err(WriteRefused::UnregisteredMember {
                member_index: write.member_index,
            });
        }
        let Some(volume) = self.volumes.get_mut(&write.volume_index) else {
            return Err(WriteRefused::UnregisteredVolume {
                volume_index: write.volume_index,
            });
        };
        let fresh = volume.ranges.insert(write.source_offset, write.len);

        let destination = self
            .destinations
            .get_mut(&write.member_index)
            .expect("destination registration was checked before anything was recorded");
        destination
            .ranges
            .insert(write.destination_offset, write.len);
        self.touched.insert(write.member_index);

        if fresh > 0 {
            self.dirty_bytes = self.dirty_bytes.saturating_add(fresh);
            self.dirty_since.get_or_insert(now);
        }
        Ok(fresh)
    }

    /// The automatic triggers. Explicit demands come through
    /// [`BarrierTrigger::Demand`] and are always honoured — a shutdown must
    /// still attempt a barrier, however many have just failed.
    pub(crate) fn due(&self, now: Instant) -> Option<BarrierTrigger> {
        // Deliberately ahead of the cooldown: the byte threshold is not damped.
        // 256 MiB of dirty bytes is enough work that retrying a failing barrier
        // is worth the attempt, and a set that keeps filling up while its
        // checkpoint fails should keep saying so.
        if self.dirty_bytes >= BARRIER_DIRTY_BYTES {
            return Some(BarrierTrigger::DirtyBytes);
        }
        // The damped arms. The age trigger is the one that busy-loops: a failed
        // barrier keeps its dirty bytes, so `dirty_since` stays old and it would
        // be due again on the very next poll. The re-stamp below is damped for
        // the same reason — its condition is not cleared by a failed attempt
        // either.
        if self.cooldown_until.is_some_and(|until| now < until) {
            return None;
        }
        // Dirty bytes are beside the point here: what is stale is the committed
        // row's plan digest, and it stays stale — and refused at restart —
        // however quiet the set goes. Only a row that exists can be stale.
        if self.committed_generation > 0 && self.committed_digest != self.plan_digest {
            return Some(BarrierTrigger::PlanDigestChanged);
        }
        if self
            .dirty_since
            .is_some_and(|since| now.duration_since(since) >= BARRIER_DIRTY_AGE)
        {
            return Some(BarrierTrigger::DirtyAge);
        }
        None
    }

    /// Candidate per-volume contiguous floors, computed after the drain.
    fn candidate_floors(&self) -> BTreeMap<u32, u64> {
        self.volumes
            .iter()
            .map(|(volume_index, volume)| {
                (*volume_index, volume.ranges.contiguous_from(volume.floor))
            })
            .collect()
    }

    fn build_snapshot(&self, generation: u64, floors: &BTreeMap<u32, u64>) -> CoverageSnapshot {
        CoverageSnapshot {
            generation,
            plan_digest: self.plan_digest,
            // Destinations with no claimed byte are **omitted**. Envelope
            // destinations are registered for every volume of the set up front
            // (`DirectSet::ensure_registered`), so a set that has touched three
            // of its two thousand volumes would otherwise write 1 997 claims
            // naming files that do not exist yet — and restart's destination
            // probe, which refuses a missing destination, would refuse the whole
            // row on the strength of a claim that claims nothing. A claim over
            // zero bytes is not a claim.
            destinations: self
                .destinations
                .iter()
                .filter(|(_, destination)| !destination.ranges.is_empty())
                .map(|(member_index, destination)| DestinationClaim {
                    member_index: *member_index,
                    relative_path: destination.relative_path.clone(),
                    extents: destination
                        .ranges
                        .ranges()
                        .iter()
                        .map(|&(start, end)| DestinationExtent { start, end })
                        .collect(),
                    crypt: self.member_crypt.get(member_index).cloned(),
                })
                .collect(),
            floors: floors
                .iter()
                .map(|(volume_index, floor)| VolumeFloor {
                    volume_index: *volume_index,
                    file_index: self
                        .volumes
                        .get(volume_index)
                        .map(|volume| volume.file_index)
                        .unwrap_or_default(),
                    floor: *floor,
                    // Re-evaluated here, against *this* barrier's floor, and
                    // never latched independently of it: the bit restart reads
                    // as "skip every segment of this file" is only true when the
                    // download finished **and** the floor about to be published
                    // covers the whole decoded volume. A volume whose bytes are
                    // still held publishes `false` and is refetched — the safe
                    // direction — until a later barrier's floor catches up.
                    complete: self
                        .volumes
                        .get(volume_index)
                        .and_then(|volume| volume.decoded_len)
                        .is_some_and(|decoded_len| *floor >= decoded_len),
                })
                .collect(),
        }
    }

    /// Runs the four-step barrier. On error nothing is published, the touched
    /// set is not cleared, and the previously committed checkpoint stays
    /// authoritative.
    ///
    /// `now` is the caller's clock, and only failure handling uses it: a failed
    /// barrier starts a cooldown that suppresses the age trigger (see
    /// [`failure_backoff`]), and a successful one clears it.
    pub(crate) fn barrier<D, S, P>(
        &mut self,
        trigger: BarrierTrigger,
        now: Instant,
        drain: &mut D,
        sync: &mut S,
        persist: &mut P,
    ) -> Result<BarrierReport, BarrierError>
    where
        D: BarrierDrain + ?Sized,
        S: DestinationSync + ?Sized,
        P: CoveragePersist + ?Sized,
    {
        match self.run_steps(trigger, drain, sync, persist) {
            Ok(report) => {
                self.consecutive_failures = 0;
                self.cooldown_until = None;
                Ok(report)
            }
            Err(error) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                self.cooldown_until = Some(now + failure_backoff(self.consecutive_failures));
                Err(error)
            }
        }
    }

    fn run_steps<D, S, P>(
        &mut self,
        trigger: BarrierTrigger,
        drain: &mut D,
        sync: &mut S,
        persist: &mut P,
    ) -> Result<BarrierReport, BarrierError>
    where
        D: BarrierDrain + ?Sized,
        S: DestinationSync + ?Sized,
        P: CoveragePersist + ?Sized,
    {
        let mut steps = Vec::with_capacity(4);

        e2e_failpoint::maybe_trip(e2e_failpoint::DIRECT_STORE_BARRIER_DRAIN);
        drain.drain().map_err(BarrierError::Drain)?;
        steps.push(BarrierStep::Drain);

        // Computed after the drain so the drained batch counts toward the
        // floors this barrier publishes.
        let floors = self.candidate_floors();

        let mut synced = 0usize;
        for member_index in &self.touched {
            let Some(destination) = self.destinations.get(member_index) else {
                continue;
            };
            sync.sync(&destination.relative_path)
                .map_err(|error| BarrierError::Sync {
                    destination: destination.relative_path.clone(),
                    error,
                })?;
            synced += 1;
            // Inside the loop and *after* the call, because the hook aborts on
            // its first trip: placed before it, a one-trip budget would abort
            // with nothing synced, which is only the drain crash again. Here it
            // lands mid-sync — file A synced, file B not — which is the state
            // step 2 exists to survive. Whatever restart then reads, it is the
            // previous checkpoint, never floors covering the unsynced file.
            e2e_failpoint::maybe_trip(e2e_failpoint::DIRECT_STORE_BARRIER_SYNC);
        }
        steps.push(BarrierStep::Sync);

        let generation = self.committed_generation.saturating_add(1);
        let snapshot = self.build_snapshot(generation, &floors);
        let blob = super::snapshot::encode(&snapshot).map_err(BarrierError::Encode)?;
        e2e_failpoint::maybe_trip(e2e_failpoint::DIRECT_STORE_BARRIER_PERSIST);
        persist
            .write(self.job_id, &self.set_name, &blob)
            .map_err(BarrierError::Persist)?;
        steps.push(BarrierStep::Persist);

        e2e_failpoint::maybe_trip(e2e_failpoint::DIRECT_STORE_BARRIER_PUBLISH);
        self.committed_generation = generation;
        // Read off the snapshot that was persisted, not off `self`: what
        // satisfies the re-stamp trigger is the digest the *row* carries.
        self.committed_digest = snapshot.plan_digest;
        for (volume_index, floor) in &floors {
            if let Some(volume) = self.volumes.get_mut(volume_index) {
                volume.floor = *floor;
                volume.ranges.trim_below(*floor);
            }
        }
        self.published_floors = floors.clone();
        self.touched.clear();
        self.dirty_bytes = 0;
        self.dirty_since = None;
        steps.push(BarrierStep::Publish);

        Ok(BarrierReport {
            trigger,
            generation,
            steps,
            synced_destinations: synced,
            snapshot_bytes: blob.len(),
            published_floors: floors,
        })
    }

    /// Deletes the durable row and **keeps** the in-memory controller.
    ///
    /// The difference from [`Self::retire`] is the whole of the distinction
    /// between its two transitions. A demotion is leaving: the destinations are
    /// about to be deleted, so every extent and floor is meaningless and
    /// keeping one would let the next barrier write a checkpoint strictly worse
    /// than none. A repair is *staying*: the destinations keep existing, at the
    /// same offsets, holding better bytes than before — so the controller's
    /// account of what is on disk is still exactly right, and it is also what
    /// the hybrid provider reads to answer the re-verify.
    ///
    /// What must not survive is the **row**, because a crash mid-repair would
    /// otherwise leave floors claiming bytes that are half-rewritten. Clearing
    /// the committed generation is what makes the next barrier write a fresh row
    /// from scratch rather than an increment of a row that is gone.
    pub(crate) fn delete_committed_row<P: CoveragePersist + ?Sized>(
        &mut self,
        persist: &mut P,
    ) -> Result<(), BarrierError> {
        persist
            .delete(self.job_id, &self.set_name)
            .map_err(BarrierError::Persist)?;
        self.committed_generation = 0;
        Ok(())
    }

    /// Deletes the set's checkpoint row **and retires the controller with it**.
    /// Demotion's half of the pair; [`Self::delete_committed_row`] is the
    /// repair's.
    ///
    /// The controller is reset to a fresh one, not merely un-generationed.
    /// Demotion deletes the destinations outright, so every extent, floor and
    /// touched entry retire with the row. Anything kept would let the next
    /// barrier write a checkpoint claiming extents in files that no longer
    /// contain them — a checkpoint strictly worse than none, because restart
    /// trusts it.
    ///
    /// A retired controller is therefore unregistered: the caller re-registers
    /// its volumes and destinations before routing resumes, and until it does,
    /// [`Self::record_write`] refuses every write.
    pub(crate) fn retire<P: CoveragePersist + ?Sized>(
        &mut self,
        persist: &mut P,
    ) -> Result<(), BarrierError> {
        persist
            .delete(self.job_id, &self.set_name)
            .map_err(BarrierError::Persist)?;
        self.committed_generation = 0;
        self.published_floors.clear();
        self.volumes.clear();
        self.destinations.clear();
        self.touched.clear();
        self.dirty_bytes = 0;
        self.dirty_since = None;
        self.consecutive_failures = 0;
        self.cooldown_until = None;
        Ok(())
    }
}
