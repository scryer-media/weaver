//! One live direct set: its router, its coverage barrier, and the bookkeeping
//! that keeps the two agreeing (plan 135, D3/D6).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Instant;

use super::ByteRanges;
use super::barrier::{
    BarrierError, BarrierReport, BarrierTrigger, CoverageBarrier, CoveragePersist, RoutedWrite,
};
use super::plan::DirectSetPlan;
use super::provider::{HybridVolumeProvider, VirtualVolume};
use super::restart::ExpectedSet;
use super::router::{CrcRuns, DemotionReason, DirectDestination, DirectSetRouter, RoutedSpan};
use super::snapshot::CoverageSnapshot;
use crate::jobs::ids::JobId;

/// Destination key for volume `volume_index`'s envelope file.
///
/// Envelope v2 gives every source volume its own sparse envelope file, so the
/// barrier now tracks *n+1* destination identities per set rather than two. The
/// encoding is **`u32::MAX - volume_index`**: destination keys are member ids,
/// which the router hands out from zero upwards, so counting volumes down from
/// the top keeps envelopes inside the same registration, sync and claim
/// machinery as members with no parallel bookkeeping and no ambiguity.
///
/// The two bands can only meet if one set had `u32::MAX` distinct destinations,
/// which is bounded by (members + volumes) of a single archive;
/// [`DirectSet::ensure_registered`] asserts the gap anyway. The *durable*
/// identity in the checkpoint blob is the destination's relative path
/// (`<set>.vol00007.envelope`), not this key, so restart stays coherent even if
/// the encoding is ever changed.
pub(crate) const fn envelope_destination_key(volume_index: u32) -> u32 {
    u32::MAX - volume_index
}

/// Whether a set is still routing, and if not, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectSetStatus {
    Routing,
    /// Every member passed its gate and its bytes are at their destination.
    Finalized,
    /// The set left direct mode; the caller refetches its volumes normally.
    Demoted(DemotionReason),
}

pub(crate) struct DirectSet {
    job_id: JobId,
    pub(crate) router: DirectSetRouter,
    /// Created once the first member is known, because the plan digest binds the
    /// member destinations and there is nothing to claim before then.
    barrier: Option<CoverageBarrier>,
    registered_members: HashSet<u32>,
    registered_volumes: bool,
    /// The router's [`DirectSetRouter::member_facts_revision`] the barrier's plan
    /// digest was computed at, so a set that adopts nothing new re-hashes
    /// nothing. `None` until the first push, which is how a freshly built or
    /// retired barrier is made to take one.
    digest_revision: Option<u64>,
    /// Source volumes whose NZB file has completed, and the **decoded** length
    /// each one turned out to be.
    ///
    /// The length rides along because the checkpoint's per-volume `complete` bit
    /// is the conjunction of "download finished" and "the floor covers all of it"
    /// — see [`super::snapshot::VolumeFloor::complete`] — and the barrier can only
    /// evaluate the second half against a length. Replaying a completion into a
    /// freshly built barrier (see [`Self::ensure_registered`]) needs it too.
    complete_volumes: BTreeMap<u32, u64>,
    /// Per-volume yEnc part-CRC32 composition over *source* space (M4).
    ///
    /// A physical volume is checked against its `=yend crc32` trailer at
    /// file-complete time; a direct volume has no file to re-read, but the
    /// per-article part CRCs compose into exactly the same value, so the gate
    /// survives without a byte of extra I/O.
    volume_crcs: BTreeMap<u32, CrcRuns>,
    /// Per volume, the **decoded** extent of every article that has been routed
    /// into it: `segment number -> (offset, length)`.
    ///
    /// The NZB's `<segment bytes>` is the yEnc-*encoded* size, ~3% larger, so
    /// nothing derived from the spec can say which source bytes an article
    /// actually covers. Demotion-by-reconstruction needs exactly that: which
    /// articles are wholly on disk in the volume it just materialized, and so
    /// must not be fetched again. Recording it here is the only place the true
    /// decoded geometry is known.
    segment_extents: BTreeMap<u32, BTreeMap<u32, (u64, u64)>>,
    /// Post-write accounting, kept alongside the barrier's and by the same call:
    /// per source volume, the physical ranges every destination write returned
    /// for, and the subset of those the **envelope** received.
    ///
    /// The barrier is still the durable truth, and where it exists it is what
    /// gets read. This exists because it also has to be right *before* the
    /// barrier does — a set can demote before its first member registers one —
    /// and the router's own routed map is not an answer to that question: it
    /// records what routing handed over, including spans whose write later
    /// failed. Claiming those would have the demotion sweep read bytes back out
    /// of a file that never received them (B1).
    placed: BTreeMap<u32, ByteRanges>,
    placed_envelope: BTreeMap<u32, ByteRanges>,
    /// Coverage restored from a checkpoint, applied when the barrier is built.
    resumed: Option<CoverageSnapshot>,
    /// Volumes whose logical length must be read off the coverage map rather
    /// than off the assembly's `received_bytes` — see
    /// [`Self::virtual_volume_len`].
    restart_seeded_volumes: BTreeSet<u32>,
    /// The demotion's one-time cleanup (delete output, retire the row, refetch)
    /// has already run. The *status* alone cannot say so: the router demotes
    /// the set from inside `route`, so by the time the wiring seam is told, the
    /// set already reads as demoted.
    demotion_cleaned_up: bool,
    /// A repair-while-direct has already been carried out for this set, so a
    /// second damage verdict demotes instead of repairing again (phase 6
    /// review).
    ///
    /// The bound is a **once-latch**, the same shape the completion gate's
    /// `normalization_retried` uses, and it is load-bearing rather than
    /// defensive: nothing else terminates the loop. A repair that leaves the
    /// set damaged — a rewrite the layout placed differently than the verifier
    /// read it, recovery that was sufficient on paper and not in practice —
    /// produces the very same verdict on the next completion check, which would
    /// materialize, repair, re-route and re-verify again, forever. One attempt,
    /// then the whole-set demotion that is always correct.
    repair_attempted: bool,
    /// Latched reporting bits: never cleared, so a set that started fast and
    /// later demoted reads as "partly on disk" — that is what happened (D1).
    pub(crate) latched_direct: bool,
    pub(crate) latched_materialized: bool,
    pub(crate) status: DirectSetStatus,
    /// The set's virtual volume image, captured at finalization and kept alive
    /// past it so a **neighbour's** PAR2 repair can still read this set's source
    /// volumes (phase 6 review, F2 follow-up).
    ///
    /// Captured rather than re-derived, for two reasons that are both fatal
    /// otherwise: finalization calls [`Self::retire`], which resets the coverage
    /// controller, so `volume_coverage` would answer from `placed` alone — empty
    /// for every range a *restart* seeded rather than this run writing — and the
    /// member paths inside it point at the committed destinations, which only
    /// this capture knows to substitute for the `.direct.partial`s the renames
    /// took away.
    ///
    /// `None` for every set that is not finalized, and for a finalized set whose
    /// envelopes were deleted the moment it committed — which is every set of a
    /// job with no live neighbour, i.e. the overwhelming majority.
    retained: Option<Vec<VirtualVolume>>,
}

impl std::fmt::Debug for DirectSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectSet")
            .field("set_name", &self.router.plan().set_name)
            .field("status", &self.status)
            .field("complete_volumes", &self.complete_volumes.len())
            .finish()
    }
}

impl DirectSet {
    pub(crate) fn new(job_id: JobId, plan: DirectSetPlan) -> Self {
        Self {
            job_id,
            router: DirectSetRouter::new(plan),
            barrier: None,
            registered_members: HashSet::new(),
            registered_volumes: false,
            digest_revision: None,
            complete_volumes: BTreeMap::new(),
            volume_crcs: BTreeMap::new(),
            segment_extents: BTreeMap::new(),
            placed: BTreeMap::new(),
            placed_envelope: BTreeMap::new(),
            resumed: None,
            restart_seeded_volumes: BTreeSet::new(),
            demotion_cleaned_up: false,
            repair_attempted: false,
            latched_direct: false,
            latched_materialized: false,
            status: DirectSetStatus::Routing,
            retained: None,
        }
    }

    /// Rebuilds the set's layout from its cached volume facts (D6).
    ///
    /// Runs **before** the checkpoint is validated, because validating it needs
    /// the plan digest and the digest binds the member destinations, which only
    /// exist once the layout has named them. A set whose facts no longer form a
    /// routable archive demotes here and redownloads — the same outcome a refused
    /// checkpoint produces, reached one step earlier.
    pub(crate) fn restore_layout(
        &mut self,
        facts: &BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
    ) -> Result<(), DemotionReason> {
        match self.router.restore_layout(facts) {
            Ok(()) => Ok(()),
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    /// Seeds the set with an accepted checkpoint: the barrier's floors and
    /// claims, the router's coverage, and the volumes whose download is done.
    ///
    /// # Re-keying
    ///
    /// The blob's destination keys are the **previous run's** member ids, which
    /// are in-run counters assigned as volumes arrived. This run rebuilt its
    /// layout from the complete fact set in volume order, so it may well have
    /// numbered the same members differently. Every claim is therefore re-keyed
    /// by its relative path — the durable identity, derived from the header name
    /// — and a claim naming a path this layout does not produce is **dropped**:
    /// its bytes go unclaimed and are refetched, which is the safe direction.
    /// Keeping it would leave the barrier with two destinations for one file and
    /// the next snapshot claiming the same bytes twice.
    ///
    /// `complete_volumes` maps each volume the checkpoint calls complete to its
    /// decoded length. That length is the row's own floor: a published `complete`
    /// means the floor covers the whole decoded volume, so the two are the same
    /// number by construction (see [`super::snapshot::VolumeFloor::complete`]).
    pub(crate) fn apply_restored_snapshot(
        &mut self,
        snapshot: &CoverageSnapshot,
        complete_volumes: &BTreeMap<u32, u64>,
    ) {
        let mut keys: HashMap<String, u32> = self
            .router
            .member_partials()
            .into_iter()
            .map(|(member_id, _, partial)| (partial.to_string(), member_id))
            .collect();
        for volume_index in self.router.plan().volumes.keys() {
            keys.insert(
                self.router.plan().envelope_relative_path(*volume_index),
                envelope_destination_key(*volume_index),
            );
        }

        let mut rekeyed = snapshot.clone();
        rekeyed
            .destinations
            .retain_mut(|claim| match keys.get(&claim.relative_path).copied() {
                Some(member_index) => {
                    claim.member_index = member_index;
                    true
                }
                None => false,
            });
        rekeyed.destinations.sort_by_key(|claim| claim.member_index);

        // Plan 136, E-D4. The crypt rows go in **before** any coverage does, so
        // a row that disagrees with the rebuilt headers demotes a set that has
        // seeded nothing rather than one half-seeded. Refusing here costs a
        // materialization from bytes already on disk; trusting a mismatched row
        // rebuilds a key against the wrong IV, and coverage gates cannot see
        // that — they say where bytes are, never what they decrypt to.
        for claim in &rekeyed.destinations {
            if let Err(reason) = self
                .router
                .restore_member_crypt(&claim.relative_path, claim.crypt.as_ref())
            {
                self.demote(reason);
                return;
            }
        }

        for claim in &rekeyed.destinations {
            let extents: Vec<(u64, u64)> = claim
                .extents
                .iter()
                .map(|extent| (extent.start, extent.end))
                .collect();
            self.router
                .restore_member_coverage(&claim.relative_path, &extents);
        }

        self.resumed = Some(rekeyed);
        self.ensure_registered();

        for volume_index in self
            .router
            .plan()
            .volumes
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            let covered = self.volume_coverage(volume_index);
            let decoded_len = complete_volumes.get(&volume_index).copied();
            let complete = decoded_len.is_some();
            if let Some(decoded_len) = decoded_len {
                self.complete_volumes.insert(volume_index, decoded_len);
            }
            if covered.is_empty() && !complete {
                continue;
            }
            self.latched_direct = true;
            self.restart_seeded_volumes.insert(volume_index);
            for &(start, end) in covered.ranges() {
                self.placed
                    .entry(volume_index)
                    .or_default()
                    .insert(start, end - start);
            }
            self.router
                .restore_volume_coverage(volume_index, &covered, decoded_len);
        }
    }

    /// The logical length to present one source volume at, given whatever the
    /// download layer says it has received.
    ///
    /// For a volume this run downloaded, `received_bytes` is the sum of the
    /// **decoded** sizes the decoder reported, which is the volume's true length
    /// — and it is preferred, because it is right even before every byte has been
    /// routed.
    ///
    /// For a volume restored from a checkpoint it is **wrong and too large**.
    /// Restore commits the skipped segments into the assembly with the spec's
    /// `<segment bytes>`, which is the yEnc-*encoded* size, about 3% larger than
    /// the payload. Presenting a virtual volume at that length hands PAR2 a file
    /// 3% longer than the one its descriptions cover, and the verifier reports
    /// damage on a set that is byte-perfect — which is a demotion, a full
    /// materialization and a redownload, for arithmetic. The coverage map is in
    /// decoded space throughout, so for those volumes it is the only honest
    /// answer: exact once the volume is complete, a lower bound while it is not,
    /// and a mid-download set is neither verified against nor demoted for its
    /// holes anyway (H3).
    pub(crate) fn virtual_volume_len(&self, volume_index: u32, received_bytes: u64) -> u64 {
        let covered_end = self.volume_coverage(volume_index).end();
        if self.restart_seeded_volumes.contains(&volume_index) {
            return covered_end;
        }
        received_bytes.max(covered_end)
    }

    /// Whether the set is carrying restart-seeded coverage no gate has verified.
    pub(crate) fn has_restart_seeded_coverage(&self) -> bool {
        self.router.has_restart_seeded_coverage()
    }

    /// Whether any of the set's coverage came back from a checkpoint rather than
    /// from articles this run decoded. Latched: it stays true after the gate
    /// re-arm has verified those bytes, because the fact it states is about where
    /// they came from, not whether they are trusted yet.
    pub(crate) fn was_restored(&self) -> bool {
        !self.restart_seeded_volumes.is_empty()
    }

    pub(crate) fn plan(&self) -> &DirectSetPlan {
        self.router.plan()
    }

    pub(crate) fn set_name(&self) -> &str {
        &self.router.plan().set_name
    }

    /// The plan facts the checkpoint reader validates a row against.
    pub(crate) fn expected_set(&self) -> ExpectedSet {
        ExpectedSet {
            plan_digest: self.plan_digest(),
            volume_files: self.router.plan().expected_volume_files(),
            fact_volumes: self.router.fact_volumes(),
        }
    }

    /// The digest the checkpoint is written under. Stable across volume growth;
    /// see [`DirectSetPlan::digest`].
    pub(crate) fn plan_digest(&self) -> [u8; 32] {
        // The real declared sizes, not a literal zero (nit). The digest's own
        // reason for excluding the per-part extents is that "any change to the
        // facts a claimed extent depends on shows up as a different member name
        // or unpacked size" — which only holds if the size is actually in it.
        self.router
            .plan()
            .digest(&self.router.member_digest_entries())
    }

    pub(crate) fn is_demoted(&self) -> bool {
        matches!(self.status, DirectSetStatus::Demoted(_))
    }

    pub(crate) fn is_finalized(&self) -> bool {
        matches!(self.status, DirectSetStatus::Finalized)
    }

    /// Leaves direct mode. Refuses once the set is terminal in either
    /// direction: a demotion is idempotent, and a **finalized** set has already
    /// renamed its members to their destinations and been marked extracted, so
    /// demoting it would delete completed output and refetch volumes nobody is
    /// waiting for. Defence in depth — the callers check too (D1).
    pub(crate) fn demote(&mut self, reason: DemotionReason) {
        if self.is_demoted() || self.is_finalized() {
            return;
        }
        self.router.demote(reason);
        self.latched_materialized = true;
        self.status = DirectSetStatus::Demoted(reason);
    }

    /// Claims the demotion's one-time cleanup.
    ///
    /// `true` exactly once per set, and never for a finalized one. Separate
    /// from [`Self::demote`] because the router demotes from inside `route`, so
    /// the status is already `Demoted` by the time the wiring seam — which owns
    /// deleting the output, retiring the row and refetching — is asked.
    pub(crate) fn claim_demotion(&mut self, reason: DemotionReason) -> bool {
        if self.is_finalized() {
            return false;
        }
        self.demote(reason);
        if self.demotion_cleaned_up {
            return false;
        }
        self.demotion_cleaned_up = true;
        true
    }

    /// Feeds one article's yEnc part CRC32 into its volume's composition (M4).
    /// Overlapping runs are ignored by [`CrcRuns`], so a duplicate article
    /// never advances the composition twice.
    pub(crate) fn note_volume_part_crc(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        len: u64,
        part_crc: u32,
    ) {
        self.volume_crcs
            .entry(volume_index)
            .or_default()
            .insert(source_offset, len, part_crc);
    }

    /// The composed whole-volume CRC32, when the parts cover `[0, len)` end to
    /// end.
    pub(crate) fn volume_crc(&self, volume_index: u32, len: u64) -> Option<u32> {
        self.volume_crc_run(volume_index, 0, len)
    }

    /// The composed CRC32 of one exact source run of a volume, when the yEnc
    /// part composition happens to have coalesced into precisely that run.
    ///
    /// Deliberately exact rather than "the value covering this range": a run the
    /// composition can only bound is no reference value at all, and D8 asks for
    /// verification *where available*.
    pub(crate) fn volume_crc_run(&self, volume_index: u32, start: u64, len: u64) -> Option<u32> {
        self.volume_crcs
            .get(&volume_index)
            .and_then(|runs| runs.compose(start, len))
    }

    /// The whole yEnc part composition for one volume, for a caller that has to
    /// ask about several sub-ranges of it (D8's reconstruction sweep).
    pub(crate) fn volume_crc_runs(&self, volume_index: u32) -> CrcRuns {
        self.volume_crcs
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    /// Rewrites one volume's yEnc composition over a span a PAR2 repair
    /// changed (plan 135, D4).
    ///
    /// `insert` would be wrong here for the same reason it is wrong for a
    /// member: the bytes on disk moved, so a composition that kept the old value
    /// would describe a volume that no longer exists — and the next
    /// reconstruction would compare rebuilt bytes against it and refuse a volume
    /// that is now correct.
    ///
    /// Unlike the member-space twin in
    /// [`super::router::DirectSetRouter::note_member_bytes`], the gaps
    /// [`CrcRuns::overwrite`] reports here must always be **empty**, and the
    /// caller discards them rather than re-reading them. That is not an
    /// oversight, it is the whole point of
    /// [`super::repair::widen_to_articles`]: a rewrite span is widened to whole
    /// articles wherever the decoded geometry is known, so it lands run for run
    /// on the article-shaped volume composition, and a span in a region no
    /// article ever covered has no run to half-cover. A gap here would mean the
    /// widening stopped covering the composition it exists to keep whole, and
    /// the next reconstruction sweep would refuse the volume with
    /// `UnverifiableRun` — so it is asserted, mirroring
    /// [`super::router::DirectSetRouter::note_restored_member_crc`].
    pub(crate) fn note_repaired_volume_crcs(
        &mut self,
        volume_index: u32,
        spans: &[super::repair::RepairedSpan],
    ) {
        let runs = self.volume_crcs.entry(volume_index).or_default();
        let owned: Vec<&super::repair::RepairedSpan> = spans
            .iter()
            .filter(|span| span.volume_index == volume_index)
            .collect();
        for span in owned {
            let gaps = runs.overwrite(span.source_offset, span.len, span.crc32);
            debug_assert!(
                gaps.is_empty(),
                "the article-widened rewrite of volume {volume_index} at {} left the \
                 volume composition with gaps at {gaps:?}",
                span.source_offset
            );
        }
    }

    /// Whether a repair-while-direct has already run for this set. See
    /// [`Self::repair_attempted`].
    pub(crate) fn repair_attempted(&self) -> bool {
        self.repair_attempted
    }

    /// Burns the repair once-latch. Called at the first irreversible step of a
    /// repair — the checkpoint delete — so a refusal that costs the set nothing
    /// does not spend the one attempt it gets.
    pub(crate) fn note_repair_attempted(&mut self) {
        self.repair_attempted = true;
    }

    /// The RAM ceiling this set's holds are bounded by (D2), which is also what
    /// a repair's rewrite is sized against before it is planned: every repaired
    /// byte re-enters the router as a hold.
    pub(crate) fn holds_budget(&self) -> u64 {
        self.router.holds_budget()
    }

    /// Routes one repaired span back through the router with D3's replacement
    /// semantics. A refusal demotes the set, exactly as [`Self::route`] does.
    pub(crate) fn route_repaired(
        &mut self,
        volume_index: u32,
        spans: &[super::router::RepairedChunk],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        match self.router.route_repaired(volume_index, spans) {
            Ok(spans) => {
                if !spans.is_empty() {
                    self.latched_direct = true;
                }
                Ok(spans)
            }
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    pub(crate) fn mark_finalized(&mut self) {
        if !self.is_demoted() {
            self.status = DirectSetStatus::Finalized;
        }
    }

    /// Routes one decoded source span. A demotion is returned rather than
    /// panicking: the caller abandons direct output for the whole set.
    pub(crate) fn route(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        data: &[u8],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        let spans = self.router.route(volume_index, source_offset, data);
        match spans {
            Ok(spans) => {
                if !spans.is_empty() {
                    self.latched_direct = true;
                }
                Ok(spans)
            }
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    /// Every volume the set plans has completed and every member has passed the
    /// whole-member gate.
    pub(crate) fn ready_to_finalize(&self) -> bool {
        !self.is_demoted()
            && !self.is_finalized()
            && self.all_volumes_complete()
            && self.router.all_members_verified()
    }

    /// Every source volume the set plans has finished downloading.
    ///
    /// The payload half of [`Self::ready_to_finalize`], on its own: a PAR2
    /// verdict over a set that is still receiving articles reads its not-yet
    /// downloaded ranges as holes, and a hole is indistinguishable from damage
    /// at that layer. Callers that must not confuse "not here yet" with
    /// "corrupt" ask this first (H3).
    pub(crate) fn all_volumes_complete(&self) -> bool {
        self.complete_volumes.len() == self.router.plan().volumes.len()
    }

    /// Marks a source volume complete and returns whatever the confirming parse
    /// just made routable. The caller must write those spans before recording
    /// them, exactly as it does for [`Self::route`]'s.
    /// `decoded_len` is the volume's decoded length — the assembly's
    /// `received_bytes`, not the spec's yEnc-encoded segment sizes — and is what
    /// lets the checkpoint distinguish "the download finished" from "every byte
    /// of it is durable".
    ///
    /// For a volume **restored** from a checkpoint it is an over-estimate rather
    /// than the exact length: restore commits the skipped segments into the
    /// assembly at the spec's encoded sizes, ~3% large (see
    /// [`Self::virtual_volume_len`]). That errs in the safe direction — the
    /// checkpoint's `complete` bit stays `false`, so a *second* restart refetches
    /// the volume's last article instead of skipping it, which is the same
    /// bounded cost the contiguous-floor model already pays for every partially
    /// covered volume. An under-estimate would be the unsafe direction, and no
    /// path produces one.
    pub(crate) fn note_volume_complete(
        &mut self,
        volume_index: u32,
        decoded_len: u64,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        self.complete_volumes.insert(volume_index, decoded_len);
        if let Some(barrier) = self.barrier.as_mut() {
            barrier.note_volume_complete(volume_index, decoded_len);
        }
        match self.router.note_volume_complete(volume_index) {
            Ok(spans) => {
                if !spans.is_empty() {
                    self.latched_direct = true;
                }
                Ok(spans)
            }
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    /// Registers the set's volumes and every destination the router has learned,
    /// retires the ones it has lost, and keeps the barrier's plan digest level
    /// with the facts it is routing against.
    ///
    /// Idempotent, and the only place a barrier comes into existence.
    pub(crate) fn ensure_registered(&mut self) {
        // Drained first, and ahead of the `members.is_empty()` return below: a
        // migration deletes a partial the barrier is claiming, and any snapshot
        // built between the unlink and this retirement claims a file that is not
        // there. A set with a migration always has a routable member left — the
        // migration's own budget check demotes otherwise — so the early return
        // is not reachable with one parked, and the ordering says so anyway.
        let retired = self.router.take_retired_destinations();
        for (member_id, relative_partial) in &retired {
            self.registered_members.remove(member_id);
            if let Some(barrier) = self.barrier.as_mut() {
                barrier.retire_destination(*member_id, relative_partial);
            }
        }

        let members = self
            .router
            .member_partials()
            .into_iter()
            .map(|(index, _, partial)| (index, partial.to_string()))
            .collect::<Vec<_>>();
        if members.is_empty() {
            return;
        }
        if self.barrier.is_none() {
            let digest = self.plan_digest();
            let barrier = match self.resumed.take() {
                Some(snapshot) if snapshot.plan_digest == digest => {
                    CoverageBarrier::resume(self.job_id, self.set_name().to_string(), &snapshot)
                }
                _ => CoverageBarrier::new(self.job_id, self.set_name().to_string(), digest),
            };
            self.barrier = Some(barrier);
            self.registered_volumes = false;
            self.registered_members.clear();
            self.digest_revision = Some(self.router.member_facts_revision());
        }
        // The digest binds the member names and sizes, which a set learns as its
        // volumes arrive — a member whose header lives in volume 3, a size a
        // later header fills in, a member a migration takes away. Stamped once at
        // the first member, the digest described a plan that stopped being true
        // minutes later, and every row written after that was refused at restart
        // for a set in perfect health. Re-pushed here, where every registration
        // already passes, and only when the router says the facts moved.
        let revision = self.router.member_facts_revision();
        if self.digest_revision != Some(revision) {
            let digest = self.plan_digest();
            if let Some(barrier) = self.barrier.as_mut() {
                barrier.set_plan_digest(digest);
            }
            self.digest_revision = Some(revision);
        }
        let Some(barrier) = self.barrier.as_mut() else {
            return;
        };
        if !self.registered_volumes {
            for (volume_index, file_index) in &self.router.plan().volumes {
                barrier.register_volume(*volume_index, *file_index);
            }
            // A volume can complete before the first member registers a barrier
            // — a set whose first volume is pure payload, say — and after a
            // retire the controller is rebuilt from nothing, so the completion
            // has to be replayed rather than only recorded as it happens.
            for (volume_index, decoded_len) in &self.complete_volumes {
                barrier.note_volume_complete(*volume_index, *decoded_len);
            }
            self.registered_volumes = true;
        }
        for (member_id, partial) in members {
            if self.registered_members.insert(member_id) {
                barrier.register_destination(member_id, partial);
            }
        }
        // Envelope v2: one destination per source volume, keyed down from the
        // top of the space. Registered up front rather than on first envelope
        // byte, so `record_write` can never meet an unregistered envelope.
        let volumes: Vec<u32> = self.router.plan().volumes.keys().copied().collect();
        debug_assert!(
            self.router
                .member_partials()
                .iter()
                .all(|(member_id, _, _)| volumes
                    .iter()
                    .all(|volume| *member_id < envelope_destination_key(*volume))),
            "a member id reached into the envelope destination band of {}",
            self.set_name()
        );
        for volume_index in volumes {
            let key = envelope_destination_key(volume_index);
            if self.registered_members.insert(key) {
                let envelope = self.router.plan().envelope_relative_path(volume_index);
                barrier.register_destination(key, envelope);
            }
        }
    }

    /// Records the decoded extent of one article of a source volume.
    pub(crate) fn note_segment_extent(
        &mut self,
        volume_index: u32,
        segment_number: u32,
        source_offset: u64,
        len: u64,
    ) {
        self.segment_extents
            .entry(volume_index)
            .or_default()
            .insert(segment_number, (source_offset, len));
    }

    /// The decoded extents recorded for one volume's articles.
    pub(crate) fn segment_extents(&self, volume_index: u32) -> BTreeMap<u32, (u64, u64)> {
        self.segment_extents
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    /// Records spans whose writes have **all** returned. A refusal here is a
    /// wiring bug, not a runtime condition, and the barrier says so loudly.
    pub(crate) fn record_writes(&mut self, spans: &[RoutedSpan], now: Instant) {
        self.ensure_registered();
        // Ordering assumption, asserted rather than assumed: the barrier comes
        // into existence with the *first member*, and every volume's envelope is
        // registered in the same call. A span can therefore only be recorded
        // once its destination is registered — including envelope spans, which
        // the router emits only after a parse that also named a member. A set
        // that ever emitted envelope bytes with no member would have written
        // bytes the coverage map cannot claim, so they would be refetched
        // rather than trusted; that is safe, but it is not the design, and it
        // would mean the parse produced envelope runs from a member-less
        // layout.
        debug_assert!(
            spans.is_empty()
                || self.barrier.is_some()
                || spans
                    .iter()
                    .all(|span| !matches!(span.destination, DirectDestination::Envelope { .. })),
            "direct-store emitted envelope spans for {} before any member registered a barrier",
            self.set_name()
        );
        // Recorded before the barrier is consulted, and whether or not one
        // exists: this is the account [`Self::volume_coverage`] falls back on,
        // and it must describe writes that *returned*, exactly like the
        // barrier's.
        for span in spans {
            self.placed
                .entry(span.volume_index)
                .or_default()
                .insert(span.source_offset, span.len());
            if let DirectDestination::Envelope { volume_index } = span.destination {
                self.placed_envelope
                    .entry(volume_index)
                    .or_default()
                    .insert(span.destination_offset, span.len());
            }
        }
        let Some(barrier) = self.barrier.as_mut() else {
            return;
        };
        for span in spans {
            let member_index = match span.destination {
                DirectDestination::Member { member_id } => member_id,
                DirectDestination::Envelope { volume_index } => {
                    envelope_destination_key(volume_index)
                }
            };
            let _ = barrier.record_write(
                &RoutedWrite {
                    volume_index: span.volume_index,
                    source_offset: span.source_offset,
                    len: span.len(),
                    member_index,
                    destination_offset: span.destination_offset,
                },
                now,
            );
        }
    }

    pub(crate) fn due(&self, now: Instant) -> Option<BarrierTrigger> {
        self.barrier.as_ref().and_then(|barrier| barrier.due(now))
    }

    /// Aggregate unique dirty bytes the set is carrying, i.e. what the barrier
    /// is about to make durable. Read before a barrier runs, because running it
    /// resets the count.
    pub(crate) fn dirty_bytes(&self) -> u64 {
        self.barrier
            .as_ref()
            .map(|barrier| barrier.dirty_bytes())
            .unwrap_or(0)
    }

    /// Destination paths touched since the last successful barrier, resolved to
    /// absolute paths for the sync step.
    pub(crate) fn touched_paths(&self) -> Vec<std::path::PathBuf> {
        let working_dir = &self.router.plan().working_dir;
        self.barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .touched_destinations()
                    .into_iter()
                    .map(|relative| working_dir.join(relative))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn run_barrier<D, S, P>(
        &mut self,
        trigger: BarrierTrigger,
        now: Instant,
        drain: &mut D,
        sync: &mut S,
        persist: &mut P,
    ) -> Option<Result<BarrierReport, BarrierError>>
    where
        D: super::barrier::BarrierDrain + ?Sized,
        S: super::barrier::DestinationSync + ?Sized,
        P: CoveragePersist + ?Sized,
    {
        // Level the barrier with the router before it builds a snapshot: the
        // plan digest it stamps and the destinations it claims must both be the
        // ones the set is routing against *now*, not the ones it was built with.
        // Guarded on an existing barrier so this stays a refresh — a demanded
        // barrier for a set that has never recorded a write still writes no row,
        // exactly as before.
        if self.barrier.is_some() {
            self.ensure_registered();
        }
        // Read off the router immediately before the run (plan 136, E-D4), so a
        // checkpoint's crypt rows are never older than the coverage beside them:
        // the retained tail padding and the cipher checkpoints are both produced
        // by the same routing call that produced the bytes being claimed.
        let crypt = self.router.member_crypt_snapshots();
        let barrier = self.barrier.as_mut()?;
        barrier.set_member_crypt(crypt);
        Some(barrier.barrier(trigger, now, drain, sync, persist))
    }

    /// Deletes the set's checkpoint row and keeps everything else (D8's
    /// repair-while-direct), so the coverage the hybrid provider reads survives
    /// a repair that only rewrote bytes in place.
    ///
    /// [`Self::retire`] is the demotion form and it is not interchangeable: it
    /// resets the controller, which is right when the destinations are about to
    /// be deleted and catastrophic when they are not — a repaired set whose
    /// coverage was reset reports every volume it did not touch as *missing* to
    /// the re-verify, and the whole set demotes for damage that is an empty map.
    pub(crate) fn delete_checkpoint_row<P: CoveragePersist + ?Sized>(
        &mut self,
        persist: &mut P,
    ) -> Result<(), BarrierError> {
        match self.barrier.as_mut() {
            Some(barrier) => barrier.delete_committed_row(persist),
            None => persist
                .delete(self.job_id, &self.router.plan().set_name)
                .map_err(BarrierError::Persist),
        }
    }

    /// Deletes the set's checkpoint row **and** retires the controller (D8).
    /// Used on demotion, where the destinations it describes are about to go.
    ///
    /// The delete runs even with no barrier built. A set can be resumed from a
    /// checkpoint written before a restart and then demote before its layout
    /// names a member again (`FormatMismatch`, `UnparsableVolume`), which is
    /// exactly the case where the row exists and the in-memory controller does
    /// not; skipping the delete there would leave a checkpoint claiming
    /// destinations that are about to be deleted.
    pub(crate) fn retire<P: CoveragePersist + ?Sized>(
        &mut self,
        persist: &mut P,
    ) -> Result<(), BarrierError> {
        if let Some(barrier) = self.barrier.as_mut() {
            barrier.retire(persist)?;
        } else {
            persist
                .delete(self.job_id, &self.router.plan().set_name)
                .map_err(BarrierError::Persist)?;
        }
        self.registered_members.clear();
        self.registered_volumes = false;
        // A retired controller is unregistered in every sense, the digest
        // included: the next registration re-derives it rather than trusting a
        // revision recorded for a controller that no longer holds anything.
        self.digest_revision = None;
        Ok(())
    }

    /// Everything durably placed for one source volume, in physical space.
    ///
    /// The barrier is authoritative: it only learns about writes whose every
    /// destination returned. Before the first member registers there is no
    /// barrier at all — a set that demotes that early has written envelope bytes
    /// and nothing else — and the fallback is [`Self::placed`], which is fed by
    /// the same call and under the same rule. Deliberately **not** the router's
    /// routed map: that records what routing emitted, including spans whose
    /// write failed, and claiming one of those would send the demotion sweep to
    /// read a byte back out of a file that never received it (B1).
    pub(crate) fn volume_coverage(&self, volume_index: u32) -> ByteRanges {
        self.barrier
            .as_ref()
            .and_then(|barrier| barrier.volume_coverage(volume_index))
            .unwrap_or_else(|| self.placed.get(&volume_index).cloned().unwrap_or_default())
    }

    /// The physical ranges one volume's **envelope file** received.
    ///
    /// The provider needs this separately from [`Self::volume_coverage`]: an
    /// envelope is sparse, so a read at an offset it never received answers with
    /// zeros rather than failing, and "the volume placed this byte somewhere" is
    /// not evidence that the envelope is where it went.
    pub(crate) fn envelope_coverage(&self, volume_index: u32) -> ByteRanges {
        self.barrier
            .as_ref()
            .and_then(|barrier| {
                barrier.destination_coverage(envelope_destination_key(volume_index))
            })
            .cloned()
            .unwrap_or_else(|| {
                self.placed_envelope
                    .get(&volume_index)
                    .cloned()
                    .unwrap_or_default()
            })
    }

    /// A [`HybridVolumeProvider`] over this set's partials and envelopes.
    ///
    /// `volume_lengths` gives each volume its logical length — the provider
    /// cannot know it, because a direct volume's length is the decoded total the
    /// download layer tracks, not anything a partial or an envelope states.
    /// Volumes absent from the map are omitted, since a reader with no length
    /// could not answer `SeekFrom::End` or stop at the right place.
    pub(crate) fn virtual_provider(
        &self,
        volume_lengths: &BTreeMap<u32, u64>,
    ) -> HybridVolumeProvider {
        HybridVolumeProvider::new(self.virtual_volumes(volume_lengths))
    }

    /// The same volumes as [`Self::virtual_provider`], unassembled.
    ///
    /// A job can hold several direct sets, and every set numbers its volumes
    /// from zero — so a caller that has to put *all* of them behind one provider
    /// (the PAR2 `FileAccess` adapter, which sees one job's whole recovery set)
    /// needs to re-key them first. That caller gets the parts; everything else
    /// wants the assembled provider.
    pub(crate) fn virtual_volumes(
        &self,
        volume_lengths: &BTreeMap<u32, u64>,
    ) -> Vec<VirtualVolume> {
        // Built once and shared, never cloned per volume: every volume of a set
        // resolves member ids against the *same* partial paths, and a set with
        // `v` volumes and `m` members would otherwise pay `v * m` path clones
        // every time a provider is assembled — which is once per authoritative
        // PAR2 pass, per demotion sweep and per tolerated extraction (nit).
        let working_dir = &self.router.plan().working_dir;
        let partials: std::sync::Arc<std::collections::HashMap<u32, std::path::PathBuf>> =
            std::sync::Arc::new(
                self.router
                    .member_partials()
                    .into_iter()
                    .map(|(member_id, _, partial)| (member_id, working_dir.join(partial)))
                    .collect(),
            );
        volume_lengths
            .iter()
            .map(|(volume_index, len)| VirtualVolume {
                volume_index: *volume_index,
                envelope: self.router.plan().envelope_path(*volume_index),
                extents: self.router.volume_member_extents(*volume_index),
                partials: std::sync::Arc::clone(&partials),
                covered: self.volume_coverage(*volume_index),
                envelope_covered: self.envelope_coverage(*volume_index),
                len: *len,
            })
            .collect()
    }

    /// [`Self::virtual_volumes`]' member map, pointed at the **committed
    /// destinations** instead of the `.direct.partial`s finalization renamed
    /// away. Byte-for-byte the same file — a commit is a rename — so the extents
    /// resolve unchanged.
    ///
    /// `None` when a member has no resolvable destination, which is the one
    /// shape a retained image must never be built over: a missing entry reads as
    /// a hole, and a hole inside a member extent is a volume the verifier calls
    /// damaged. `sync_members` already demotes a set whose members cannot all be
    /// resolved *and* refuses two that collide onto one destination, so this can
    /// only fire if those two ever drift — but the whole point of serving a
    /// committed member is that the path is the member's own, so it is checked
    /// here rather than assumed.
    fn committed_member_paths(
        &self,
    ) -> Option<std::sync::Arc<std::collections::HashMap<u32, std::path::PathBuf>>> {
        let mut paths: std::collections::HashMap<u32, std::path::PathBuf> =
            std::collections::HashMap::new();
        for (member_id, name, _) in self.router.member_partials() {
            paths.insert(member_id, self.router.plan().member_output_path(name).ok()?);
        }
        Some(std::sync::Arc::new(paths))
    }

    /// Captures the set's virtual volume image so it survives finalization, and
    /// reports whether it is worth keeping (phase 6 review, F2 follow-up).
    ///
    /// Must be called **before** [`Self::retire`] and **after** the members have
    /// been renamed to their destinations: the first because retiring resets the
    /// coverage controller this reads, the second because nothing but the rename
    /// makes the substituted paths real.
    ///
    /// `false` — and nothing retained — unless every planned volume reads as one
    /// unbroken run from zero to its length. A retained image exists to answer a
    /// *neighbour's* repair, and a repair reads its surviving inputs whole: an
    /// image with a hole in it would have the pass call this set damaged, plan a
    /// repair of volumes nobody can write, and refuse the neighbour's along with
    /// it. Refusing to retain leaves the job on the pre-existing path, where
    /// `forgive_finalized_direct_volumes` excuses the absent volumes instead.
    pub(crate) fn retain_finalized_volumes(&mut self, volume_lengths: &BTreeMap<u32, u64>) -> bool {
        self.retained = None;
        if volume_lengths.len() != self.router.plan().volumes.len() {
            return false;
        }
        let Some(partials) = self.committed_member_paths() else {
            return false;
        };
        let volumes: Vec<VirtualVolume> = volume_lengths
            .iter()
            .map(|(volume_index, len)| VirtualVolume {
                volume_index: *volume_index,
                envelope: self.router.plan().envelope_path(*volume_index),
                extents: self.router.volume_member_extents(*volume_index),
                partials: std::sync::Arc::clone(&partials),
                covered: self.volume_coverage(*volume_index),
                envelope_covered: self.envelope_coverage(*volume_index),
                len: *len,
            })
            .collect();
        if !volumes
            .iter()
            .all(|volume| volume.readable_prefix() == Some(volume.len) && volume.len > 0)
        {
            return false;
        }
        self.retained = Some(volumes);
        true
    }

    /// The retained image, or `None` for a set that never kept one or has since
    /// released it.
    pub(crate) fn retained_volumes(&self) -> Option<&[VirtualVolume]> {
        self.retained.as_deref()
    }

    /// Drops the retained image. The caller deletes the envelope files it named
    /// in the same breath — they are what the image reads through, and keeping
    /// either without the other is a lie in one direction or dead bytes in the
    /// other.
    pub(crate) fn release_retained_volumes(&mut self) {
        self.retained = None;
    }

    /// The two checkpoint systems must never both own a member (D6 risk list).
    /// A direct set is marked extracted at finalization without ever entering
    /// the incremental extractor, so an extraction checkpoint naming one of its
    /// members means routing and extraction both claimed it.
    pub(crate) fn assert_not_extraction_owned(&self, extraction_members: &HashSet<String>) {
        debug_assert!(
            self.router
                .member_partials()
                .iter()
                .all(|(_, name, _)| !extraction_members.contains(*name)),
            "direct-store and the incremental extractor both claim a member of {}",
            self.set_name()
        );
    }
}
