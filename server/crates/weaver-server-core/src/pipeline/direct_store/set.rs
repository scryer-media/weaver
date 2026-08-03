//! One live direct set: its router, its coverage barrier, and the bookkeeping
//! that keeps the two agreeing (plan 135, D3/D6).

use std::collections::{BTreeMap, BTreeSet, HashSet};
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
    /// Source volumes whose NZB file has completed.
    complete_volumes: BTreeSet<u32>,
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
    /// The demotion's one-time cleanup (delete output, retire the row, refetch)
    /// has already run. The *status* alone cannot say so: the router demotes
    /// the set from inside `route`, so by the time the wiring seam is told, the
    /// set already reads as demoted.
    demotion_cleaned_up: bool,
    /// Latched reporting bits: never cleared, so a set that started fast and
    /// later demoted reads as "partly on disk" — that is what happened (D1).
    pub(crate) latched_direct: bool,
    pub(crate) latched_materialized: bool,
    pub(crate) status: DirectSetStatus,
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
            complete_volumes: BTreeSet::new(),
            volume_crcs: BTreeMap::new(),
            segment_extents: BTreeMap::new(),
            placed: BTreeMap::new(),
            placed_envelope: BTreeMap::new(),
            resumed: None,
            demotion_cleaned_up: false,
            latched_direct: false,
            latched_materialized: false,
            status: DirectSetStatus::Routing,
        }
    }

    /// Seeds the set with an accepted checkpoint. The barrier is rebuilt from it
    /// as soon as the layout names a member again. Reached only from the restart
    /// reader, which phase 4 left unwired (see `restart`'s module docs).
    #[allow(dead_code)]
    pub(crate) fn resume_from(&mut self, snapshot: CoverageSnapshot) {
        self.resumed = Some(snapshot);
    }

    pub(crate) fn plan(&self) -> &DirectSetPlan {
        self.router.plan()
    }

    pub(crate) fn set_name(&self) -> &str {
        &self.router.plan().set_name
    }

    /// Same: restart-only, and unwired in phase 4.
    #[allow(dead_code)]
    pub(crate) fn expected_set(&self) -> ExpectedSet {
        ExpectedSet {
            plan_digest: self.plan_digest(),
            volume_files: self.router.plan().expected_volume_files(),
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
            && self.complete_volumes.len() == self.router.plan().volumes.len()
            && self.router.all_members_verified()
    }

    /// Marks a source volume complete and returns whatever the confirming parse
    /// just made routable. The caller must write those spans before recording
    /// them, exactly as it does for [`Self::route`]'s.
    pub(crate) fn note_volume_complete(
        &mut self,
        volume_index: u32,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        self.complete_volumes.insert(volume_index);
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

    /// Registers the set's volumes and every destination the router has learned.
    /// Idempotent, and the only place a barrier comes into existence.
    pub(crate) fn ensure_registered(&mut self) {
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
        }
        let Some(barrier) = self.barrier.as_mut() else {
            return;
        };
        if !self.registered_volumes {
            for (volume_index, file_index) in &self.router.plan().volumes {
                barrier.register_volume(*volume_index, *file_index);
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
        let barrier = self.barrier.as_mut()?;
        Some(barrier.barrier(trigger, now, drain, sync, persist))
    }

    /// Deletes the set's checkpoint row (D8). Used on demotion and before
    /// repairing over checkpoint-covered output.
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
        let working_dir = &self.router.plan().working_dir;
        let partials: std::collections::HashMap<u32, std::path::PathBuf> = self
            .router
            .member_partials()
            .into_iter()
            .map(|(member_id, _, partial)| (member_id, working_dir.join(partial)))
            .collect();
        volume_lengths
            .iter()
            .map(|(volume_index, len)| VirtualVolume {
                volume_index: *volume_index,
                envelope: self.router.plan().envelope_path(*volume_index),
                extents: self.router.volume_member_extents(*volume_index),
                partials: partials.clone(),
                covered: self.volume_coverage(*volume_index),
                envelope_covered: self.envelope_coverage(*volume_index),
                len: *len,
            })
            .collect()
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
