//! One live direct set: its router, its coverage barrier, and the bookkeeping
//! that keeps the two agreeing (plan 135, D3/D6).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Instant;

use super::barrier::{
    BarrierError, BarrierReport, BarrierTrigger, CoverageBarrier, CoveragePersist, RoutedWrite,
};
use super::plan::DirectSetPlan;
use super::restart::ExpectedSet;
use super::router::{CrcRuns, DemotionReason, DirectDestination, DirectSetRouter, RoutedSpan};
use super::snapshot::CoverageSnapshot;
use crate::jobs::ids::JobId;

/// Member index the envelope file is registered under.
///
/// Real member indices are the layout's positions, which start at zero and stay
/// small; the top of the space is free and keeps the envelope inside the same
/// registration, sync and claim machinery as a member rather than growing a
/// parallel one.
pub(crate) const ENVELOPE_MEMBER_INDEX: u32 = u32::MAX;

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
    /// Coverage restored from a checkpoint, applied when the barrier is built.
    resumed: Option<CoverageSnapshot>,
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
            resumed: None,
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
        let members: Vec<(u32, String, u64)> = self
            .router
            .member_partials()
            .into_iter()
            .map(|(index, name, _)| (index, name.to_string(), 0))
            .collect();
        self.router.plan().digest(&members)
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
        self.volume_crcs
            .get(&volume_index)
            .and_then(|runs| runs.exact(0, len))
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

    pub(crate) fn note_volume_complete(&mut self, volume_index: u32) -> Result<(), DemotionReason> {
        self.complete_volumes.insert(volume_index);
        match self.router.note_volume_complete(volume_index) {
            Ok(()) => Ok(()),
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
        for (index, partial) in members {
            if self.registered_members.insert(index) {
                barrier.register_destination(index, partial);
            }
        }
        if self.registered_members.insert(ENVELOPE_MEMBER_INDEX) {
            let envelope = self.router.plan().envelope_relative_path();
            barrier.register_destination(ENVELOPE_MEMBER_INDEX, envelope);
        }
    }

    /// Records spans whose writes have **all** returned. A refusal here is a
    /// wiring bug, not a runtime condition, and the barrier says so loudly.
    pub(crate) fn record_writes(&mut self, spans: &[RoutedSpan], now: Instant) {
        self.ensure_registered();
        // Ordering assumption, asserted rather than assumed: the barrier comes
        // into existence with the *first member*, and the envelope is
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
                    .all(|span| span.destination != DirectDestination::Envelope),
            "direct-store emitted envelope spans for {} before any member registered a barrier",
            self.set_name()
        );
        let Some(barrier) = self.barrier.as_mut() else {
            return;
        };
        for span in spans {
            let member_index = match span.destination {
                DirectDestination::Member { member_index } => member_index,
                DirectDestination::Envelope => ENVELOPE_MEMBER_INDEX,
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
