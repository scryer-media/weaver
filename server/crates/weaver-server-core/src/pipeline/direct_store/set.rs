// One live direct set: its router, its coverage barrier, and the bookkeeping
// that keeps the two agreeing.

use std::collections::{BTreeMap, HashMap, HashSet};
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

type NestedReplayMember = (u32, std::path::PathBuf, Vec<(u64, u64)>);

// Destination key for volume `volume_index`'s envelope file.
//
// Envelope v2 gives every source volume its own sparse envelope file, so the
// barrier now tracks *n+1* destination identities per set rather than two. The
// encoding is **`u32::MAX - volume_index`**: destination keys are member ids,
// which the router hands out from zero upwards, so counting volumes down from
// the top keeps envelopes inside the same registration, sync and claim
// machinery as members with no parallel bookkeeping and no ambiguity.
//
// The two bands can only meet if one set had `u32::MAX` distinct destinations,
// which is bounded by (members + volumes) of a single archive;
// [`DirectSet::ensure_registered`] asserts the gap anyway. The *durable*
// identity in the checkpoint blob is the destination's relative path
// (`<set>.vol00007.envelope`), not this key, so restart stays coherent even if
// the encoding is ever changed.
pub(crate) const fn envelope_destination_key(volume_index: u32) -> u32 {
    u32::MAX - volume_index
}

// The inverse of [`envelope_destination_key`]: the volume a destination key
// would name *if* it is an envelope key.
//
// Meaningless on its own — every `u32` maps to some volume index — so the
// answer is only a classification when the caller checks the result against the
// volumes it actually plans
// ([`super::plan::DirectSetPlan::is_envelope_destination`]). A member id `m`
// would have to satisfy `u32::MAX - m < volumes` to be mistaken for one, which
// is the same gap [`DirectSet::ensure_registered`] already asserts.
//
// It exists because the two bands now resolve against **different roots**:
// envelopes are working data and members are payload, and the destination key
// is the one discriminator that is stable across runs (a volume index is a
// layout coordinate; the relative path text is not something to sniff).
pub(crate) const fn envelope_volume_for_key(destination_key: u32) -> u32 {
    u32::MAX - destination_key
}

// Whether a set is still routing, and if not, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectSetStatus {
    Routing,
    // Every member passed its gate and its bytes are at their destination.
    Finalized,
    // The set left direct mode; the caller refetches its volumes normally.
    Demoted(DemotionReason),
}

pub(crate) struct LiveChildDescription {
    pub(crate) node: u32,
    pub(crate) parent: u32,
    pub(crate) format: super::plan::SetFormat,
    pub(crate) set_name: String,
    pub(crate) input_names: BTreeMap<u32, String>,
    pub(crate) parts: BTreeMap<u32, crate::pipeline::extraction::input::LivePart>,
    pub(crate) roster_closed: bool,
}

pub(crate) struct DirectSet {
    job_id: JobId,
    pub(crate) router: DirectSetRouter,
    // Created once the first member is known, because the plan digest binds the
    // member destinations and there is nothing to claim before then.
    barrier: Option<CoverageBarrier>,
    registered_members: HashSet<u32>,
    // How many of the plan's volumes have been registered on the barrier. A
    // count rather than a latch because an identity-admitted plan's volume
    // map grows after the barrier exists, and each newly bound volume still
    // has to be registered before a write can be recorded against its
    // envelope. Registration is idempotent, so re-running the loop when the
    // counts disagree is safe.
    registered_volumes: usize,
    // The router's [`DirectSetRouter::member_facts_revision`] the barrier's plan
    // digest was computed at, so a set that adopts nothing new re-hashes
    // nothing. `None` until the first push, which is how a freshly built or
    // retired barrier is made to take one.
    digest_revision: Option<u64>,
    // Source volumes whose NZB file has completed, and the **decoded** length
    // each one turned out to be.
    //
    // The length rides along because the checkpoint's per-volume `complete` bit
    // is the conjunction of "download finished" and "the floor covers all of it"
    // — see [`super::snapshot::VolumeFloor::complete`] — and the barrier can only
    // evaluate the second half against a length. Replaying a completion into a
    // freshly built barrier (see [`Self::ensure_registered`]) needs it too.
    complete_volumes: BTreeMap<u32, u64>,
    // Per-volume yEnc part-CRC32 composition over *source* space.
    //
    // A physical volume is checked against its `=yend crc32` trailer at
    // file-complete time; a direct volume has no file to re-read, but the
    // per-article part CRCs compose into exactly the same value, so the gate
    // survives without a byte of extra I/O.
    volume_crcs: BTreeMap<u32, CrcRuns>,
    // Per volume, the **decoded** extent of every article that has been routed
    // into it: `segment number -> (offset, length)`.
    //
    // The NZB's `<segment bytes>` is the yEnc-*encoded* size, ~3% larger, so
    // nothing derived from the spec can say which source bytes an article
    // actually covers. Demotion-by-reconstruction needs exactly that: which
    // articles are wholly on disk in the volume it just materialized, and so
    // must not be fetched again. Recording it here is the only place the true
    // decoded geometry is known.
    segment_extents: BTreeMap<u32, BTreeMap<u32, (u64, u64)>>,
    // Post-write accounting, kept alongside the barrier's and by the same call:
    // per source volume, the physical ranges every destination write returned
    // for, and the subset of those the **envelope** received.
    //
    // The barrier is still the durable truth, and where it exists it is what
    // gets read. This exists because it also has to be right *before* the
    // barrier does — a set can demote before its first member registers one —
    // and the router's own routed map is not an answer to that question: it
    // records what routing handed over, including spans whose write later
    // failed. Claiming those would have the demotion sweep read bytes back out
    // of a file that never received them.
    placed: BTreeMap<u32, ByteRanges>,
    placed_envelope: BTreeMap<u32, ByteRanges>,
    // Coverage restored from a checkpoint, applied when the barrier is built.
    resumed: Option<CoverageSnapshot>,
    // Volumes whose logical length must be read off the coverage map rather
    // than off the assembly's `received_bytes` — see
    // [`Self::virtual_volume_len`] — with the physical coverage a checkpoint
    // seeded into each. Those bytes were placed by a process that is gone, so
    // no article record of this one accounts for them; see
    // [`Self::restored_volume_coverage`].
    restart_seeded_volumes: BTreeMap<u32, ByteRanges>,
    // The demotion's one-time cleanup (delete output, retire the row, refetch)
    // has already run. The *status* alone cannot say so: the router demotes
    // the set from inside `route`, so by the time the wiring seam is told, the
    // set already reads as demoted.
    demotion_cleaned_up: bool,
    // A repair-while-direct has already been carried out for this set, so a
    // second damage verdict demotes instead of repairing again.
    //
    // The bound is a **once-latch**, the same shape the completion gate's
    // `normalization_retried` uses, and it is load-bearing rather than
    // defensive: nothing else terminates the loop. A repair that leaves the
    // set damaged — a rewrite the layout placed differently than the verifier
    // read it, recovery that was sufficient on paper and not in practice —
    // produces the very same verdict on the next completion check, which would
    // materialize, repair, re-route and re-verify again, forever. One attempt,
    // then the whole-set demotion that is always correct.
    repair_attempted: bool,
    // Latched reporting bits: never cleared, so a set that started fast and
    // later demoted reads as "partly on disk" — that is what happened.
    pub(crate) latched_direct: bool,
    pub(crate) latched_materialized: bool,
    pub(crate) status: DirectSetStatus,
    // The set's virtual volume image, captured at finalization and kept alive
    // past it so a **neighbour's** PAR2 repair can still read this set's
    // source volumes.
    //
    // Captured rather than re-derived, for two reasons that are both fatal
    // otherwise: finalization calls [`Self::retire`], which resets the coverage
    // controller, so `volume_coverage` would answer from `placed` alone — empty
    // for every range a *restart* seeded rather than this run writing — and the
    // member paths inside it point at the committed destinations, which only
    // this capture knows to substitute for the `.direct.partial`s the renames
    // took away.
    //
    // `None` for every set that is not finalized, and for a finalized set whose
    // envelopes were deleted the moment it committed — which is every set of a
    // job with no live neighbour, i.e. the overwhelming majority.
    retained: Option<Vec<VirtualVolume>>,
    // The recovery-set fingerprints this set's checkpoints carry, by NZB file
    // index. Taken once per file of a standalone set, and given back by a
    // restored checkpoint.
    proven_fingerprints: BTreeMap<u32, ([u8; 16], u64)>,
    repost_digests: BTreeMap<u32, super::snapshot::RepostDigest>,
    nested: Option<super::nested::NestedGraph>,
    nested_authority: bool,
    delegated: bool,
    qualified_placements: BTreeMap<QualifiedKey, QualifiedPlacement>,
    graph_dirty_bytes: u64,
    graph_dirty_since: Option<Instant>,
    graph_metadata: Option<blake3::Hash>,
    live_metadata: Option<blake3::Hash>,
    live_placement_revision: u64,
    installed: Option<super::snapshot::InstalledSet>,
}

type QualifiedKey = (u32, u64, u8, u32, Option<(u32, u32, u64)>);

pub(crate) struct QualifiedPlacement {
    pub(crate) owner: super::router::SpanOwner,
    pub(crate) destination: DirectDestination,
    pub(crate) path: std::path::PathBuf,
    pub(crate) ranges: ByteRanges,
}

pub(crate) struct GraphRearmPlan {
    pub(crate) node: u32,
    pub(crate) generation: u64,
    pub(crate) runs: Vec<super::router::RestartReadRun>,
    pub(crate) physical: std::sync::Arc<HashMap<u32, std::path::PathBuf>>,
    pub(crate) nested: super::provider::NestedMembers,
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
            registered_volumes: 0,
            digest_revision: None,
            complete_volumes: BTreeMap::new(),
            volume_crcs: BTreeMap::new(),
            segment_extents: BTreeMap::new(),
            placed: BTreeMap::new(),
            placed_envelope: BTreeMap::new(),
            resumed: None,
            restart_seeded_volumes: BTreeMap::new(),
            demotion_cleaned_up: false,
            repair_attempted: false,
            latched_direct: false,
            latched_materialized: false,
            status: DirectSetStatus::Routing,
            retained: None,
            proven_fingerprints: BTreeMap::new(),
            repost_digests: BTreeMap::new(),
            nested: None,
            nested_authority: false,
            delegated: false,
            qualified_placements: BTreeMap::new(),
            graph_dirty_bytes: 0,
            graph_dirty_since: None,
            graph_metadata: None,
            live_metadata: None,
            live_placement_revision: 0,
            installed: None,
        }
    }

    pub(crate) fn install_restored_nested(&mut self, graph: super::nested::NestedGraph) {
        self.nested = Some(graph);
    }

    pub(crate) fn nested_graph(&self) -> Option<&super::nested::NestedGraph> {
        self.nested.as_ref()
    }

    pub(crate) fn take_live_refresh(&mut self) -> bool {
        let mut hash = blake3::Hasher::new();
        hash.update(self.graph_metadata_fingerprint().as_bytes());
        hash.update(&self.live_placement_revision.to_le_bytes());
        let next = hash.finalize();
        if self.live_metadata == Some(next) {
            return false;
        }
        self.live_metadata = Some(next);
        true
    }

    pub(crate) fn direct_producer_states(&self) -> Vec<(u32, Option<u32>, u64, bool)> {
        if !self.nested_authority {
            return Vec::new();
        }
        let mut result = vec![(
            0,
            None,
            0,
            self.all_volumes_complete()
                && self.router.all_members_verified()
                && !self.router.awaits_par3_verdict(),
        )];
        result.extend(
            self.nested
                .as_ref()
                .expect("active graph")
                .direct_producer_states(),
        );
        result
    }

    pub(crate) fn live_child_descriptions(&self) -> Vec<LiveChildDescription> {
        if !self.nested_authority {
            return Vec::new();
        }
        self.nested
            .as_ref()
            .expect("active graph")
            .live_child_descriptions(&self.router)
    }

    pub(crate) fn graph_rearm_plans(&self) -> Vec<GraphRearmPlan> {
        if !self.nested_authority {
            return Vec::new();
        }
        let graph = self.nested.as_ref().expect("active graph");
        (0..=graph.nodes.len() as u32)
            .filter_map(|node| {
                let router = graph.router(&self.router, node);
                let runs = router.restart_read_plan();
                if runs.is_empty() {
                    return None;
                }
                let (physical, nested) = graph.member_backings(&self.router, node);
                Some(GraphRearmPlan {
                    node,
                    generation: if node == 0 {
                        0
                    } else {
                        graph.node(node).generation
                    },
                    runs,
                    physical,
                    nested,
                })
            })
            .collect()
    }

    pub(crate) fn apply_graph_rearm(
        &mut self,
        node: u32,
        generation: u64,
        runs: &[super::router::RestartReadRun],
        checksums: Vec<u32>,
    ) -> Result<(), DemotionReason> {
        let router = if node == 0 {
            &mut self.router
        } else {
            let graph = self
                .nested
                .as_mut()
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            if graph.node(node).generation != generation {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
            &mut graph.node_mut(node).router
        };
        if router.restart_read_plan() != runs || runs.len() != checksums.len() {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        for (run, checksum) in runs.iter().zip(checksums) {
            router.note_restored_member_crc(
                run.member_id,
                run.logical_offset,
                run.len,
                checksum,
            )?;
        }
        Ok(())
    }

    pub(crate) fn installed_snapshot(&self) -> Option<&super::snapshot::InstalledSet> {
        self.installed.as_ref()
    }

    pub(crate) fn set_installed_snapshot(&mut self, installed: super::snapshot::InstalledSet) {
        self.installed = Some(installed);
    }

    pub(crate) fn has_nested_checkpoint_authority(&self) -> bool {
        self.nested_authority
    }

    // Called only after the job graph's first CAS has migrated every root.
    pub(crate) fn enable_nested_checkpoint_authority(&mut self) {
        self.router.retain_checkpoint_backings();
        self.nested.get_or_insert_with(Default::default);
        self.nested_authority = true;
    }

    pub(crate) fn nested_replay_members(&self) -> Vec<NestedReplayMember> {
        let candidates: std::collections::HashSet<_> =
            super::nested::catalog::archive_candidates(&self.router)
                .into_iter()
                .map(|candidate| candidate.0)
                .collect();
        let partials: HashMap<_, _> = self
            .router
            .member_partials()
            .into_iter()
            .map(|(member, _, relative)| (member, self.plan().destination_path(relative)))
            .collect();
        let mut coverage: BTreeMap<u32, ByteRanges> = BTreeMap::new();
        for claim in self.legacy_physical_claims() {
            for extent in claim.extents {
                coverage
                    .entry(claim.member_index)
                    .or_default()
                    .insert(extent.start, extent.end - extent.start);
            }
        }
        for placement in self.qualified_placements() {
            if placement.owner.node == 0
                && placement.owner.raw_child.is_none()
                && let DirectDestination::Member { member_id } = placement.destination
            {
                for &(start, end) in placement.ranges.ranges() {
                    coverage
                        .entry(member_id)
                        .or_default()
                        .insert(start, end - start);
                }
            }
        }
        coverage
            .into_iter()
            .filter_map(|(member, ranges)| {
                if !candidates.contains(&member) {
                    return None;
                }
                let path = partials.get(&member)?.clone();
                let delivered = self.nested.as_ref().and_then(|graph| {
                    graph
                        .binding(0, member)
                        .map(|(child, volume)| &graph.node(child).inputs[&volume].delivered)
                });
                let ranges: Vec<_> = ranges
                    .ranges()
                    .iter()
                    .flat_map(|&(start, end)| {
                        delivered.map_or_else(
                            || vec![(start, end)],
                            |delivered| delivered.missing(start, end - start),
                        )
                    })
                    .collect();
                (!ranges.is_empty()).then_some((member, path, ranges))
            })
            .collect()
    }

    pub(crate) fn forward_existing_member(
        &mut self,
        member: u32,
        offset: u64,
        bytes: bytes::Bytes,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        use crate::pipeline::extraction::catalog::{
            InputProjection, MemberProjection, MemberRange,
        };
        let graph = self
            .nested
            .as_mut()
            .ok_or(DemotionReason::ConflictingVolumeFacts)?;
        graph.forward(&self.router, super::nested::ROOT_NODE, Vec::new())?;
        if graph.binding(super::nested::ROOT_NODE, member).is_none() {
            return Ok(Vec::new());
        }
        let InputProjection::Mapped(sources) = self
            .router
            .project_member(
                member,
                MemberRange {
                    offset,
                    len: bytes.len() as u64,
                    write_offset: 0,
                },
            )
            .map_err(|_| DemotionReason::ConflictingVolumeFacts)?
        else {
            return Err(DemotionReason::ConflictingVolumeFacts);
        };
        let spans = sources
            .into_iter()
            .map(|source| {
                let start = usize::try_from(source.write_offset)
                    .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
                let length = usize::try_from(source.len)
                    .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
                let end = start
                    .checked_add(length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or(DemotionReason::ConflictingVolumeFacts)?;
                Ok(RoutedSpan {
                    owner: Default::default(),
                    destination: DirectDestination::Member { member_id: member },
                    destination_offset: offset + source.write_offset,
                    volume_index: source.volume,
                    source_offset: source.offset,
                    bytes: vec![bytes.slice(start..end)],
                })
            })
            .collect::<Result<_, _>>()?;
        let spans = graph.forward(&self.router, super::nested::ROOT_NODE, spans)?;
        let mut forwarded = Vec::new();
        for span in spans {
            if span.owner == super::nested::ROOT_NODE {
                if span.raw_child.is_some() {
                    // This replay came from an already receipt-backed root
                    // partial. A refused child keeps that physical evidence.
                    self.record_placements(std::iter::once(span.placement()), Instant::now());
                }
            } else {
                forwarded.push(span.into_span());
            }
        }
        Ok(forwarded)
    }

    pub(crate) fn qualified_placements(&self) -> impl Iterator<Item = &QualifiedPlacement> {
        self.qualified_placements.values().filter(|placement| {
            self.placement_path(placement.owner, placement.destination)
                .is_some_and(|path| path == placement.path)
        })
    }

    pub(crate) fn seed_qualified_placement(
        &mut self,
        owner: super::router::SpanOwner,
        destination: DirectDestination,
        path: std::path::PathBuf,
        extents: Vec<(u64, u64)>,
    ) {
        let (kind, destination_key) = match destination {
            DirectDestination::Member { member_id } => (0, member_id),
            DirectDestination::Envelope { volume_index } => (1, volume_index),
        };
        let raw = owner
            .raw_child
            .map(|child| (child.node, child.volume, child.generation));
        let key = (owner.node, owner.generation, kind, destination_key, raw);
        let claim = self
            .qualified_placements
            .entry(key)
            .or_insert_with(|| QualifiedPlacement {
                owner,
                destination,
                path,
                ranges: ByteRanges::new(),
            });
        for (start, end) in extents {
            claim.ranges.insert(start, end - start);
        }
    }

    pub(crate) fn seed_root_source_coverage(&mut self, volume: u32, ranges: Vec<(u64, u64)>) {
        let coverage = self.placed.entry(volume).or_default();
        for (start, end) in ranges {
            coverage.insert(start, end - start);
        }
    }

    pub(crate) fn mark_graph_checkpoint_committed(&mut self, _now: Instant) {
        self.graph_dirty_bytes = 0;
        self.graph_dirty_since = None;
        self.graph_metadata = Some(self.graph_metadata_fingerprint());
    }

    fn graph_metadata_fingerprint(&self) -> blake3::Hash {
        let mut hash = blake3::Hasher::new();
        hash.update(&self.router.member_facts_revision().to_le_bytes());
        hash.update(&[
            u8::from(self.router.all_members_verified()),
            u8::from(self.router.awaits_par3_verdict()),
        ]);
        for (&volume, &length) in &self.complete_volumes {
            hash.update(&volume.to_le_bytes());
            hash.update(&length.to_le_bytes());
        }
        if let Some(graph) = &self.nested {
            for node in &graph.nodes {
                hash.update(&node.parent.to_le_bytes());
                hash.update(&node.generation.to_le_bytes());
                hash.update(&node.router.member_facts_revision().to_le_bytes());
                hash.update(&[
                    u8::from(node.router.all_members_verified()),
                    u8::from(node.router.awaits_par3_verdict()),
                ]);
                hash.update(&[
                    u8::from(node.roster_closed),
                    match node.mode {
                        super::nested::NodeMode::Direct => 0,
                        super::nested::NodeMode::MaterializationPending(_) => 1,
                        super::nested::NodeMode::Materialized => 2,
                    },
                ]);
                for (&volume, input) in &node.inputs {
                    hash.update(&volume.to_le_bytes());
                    hash.update(&input.length.unwrap_or(u64::MAX).to_le_bytes());
                    hash.update(&[u8::from(input.confirmed)]);
                }
            }
        }
        hash.finalize()
    }

    pub(crate) fn legacy_physical_claims(&self) -> Vec<super::snapshot::DestinationClaim> {
        self.barrier
            .as_ref()
            .map(|barrier| barrier.checkpoint_snapshot(1).destinations)
            .or_else(|| {
                self.resumed
                    .as_ref()
                    .map(|snapshot| snapshot.destinations.clone())
            })
            .unwrap_or_default()
    }

    pub(crate) fn checkpoint_metadata(&self, generation: u64) -> CoverageSnapshot {
        let crypt = self.router.member_crypt_snapshots();
        let mut destinations = self.legacy_physical_claims();
        for (member_index, _, relative) in self.router.member_partials() {
            let claim = super::snapshot::DestinationClaim {
                member_index,
                relative_path: relative.to_owned(),
                extents: Vec::new(),
                crypt: crypt.get(&member_index).cloned(),
            };
            match destinations
                .iter_mut()
                .find(|entry| entry.member_index == member_index)
            {
                Some(existing) => existing.crypt = claim.crypt,
                None => destinations.push(claim),
            }
        }
        destinations.sort_by_key(|entry| entry.member_index);
        CoverageSnapshot {
            generation,
            plan_digest: self.plan_digest(),
            destinations,
            floors: self
                .plan()
                .volumes
                .iter()
                .map(|(&volume_index, &file_index)| {
                    let floor = self.volume_coverage(volume_index).contiguous_from(0);
                    let complete = self
                        .complete_volumes
                        .get(&volume_index)
                        .is_some_and(|length| floor >= *length);
                    super::snapshot::VolumeFloor {
                        volume_index,
                        file_index,
                        floor,
                        complete,
                    }
                })
                .collect(),
            identity: self.plan().identity_binding(),
            fingerprints: self.fingerprint_rows(),
            repost_digests: self.repost_digests.values().copied().collect(),
        }
    }

    pub(crate) fn checkpoint_volume_lengths(&self) -> BTreeMap<u32, u64> {
        let facts = self.router.checkpoint_facts();
        self.plan()
            .volumes
            .keys()
            .map(|&volume| {
                let known = self
                    .complete_volumes
                    .get(&volume)
                    .map(|length| self.virtual_volume_len(volume, *length))
                    .or_else(|| match facts.get(&volume) {
                        Some(super::restart::DirectVolumeFacts::SevenZip(facts)) => {
                            Some(facts.declared_len)
                        }
                        _ => None,
                    });
                let length = known.unwrap_or_else(|| {
                    self.segment_extents
                        .get(&volume)
                        .into_iter()
                        .flat_map(|segments| segments.values())
                        .map(|(offset, length)| offset.saturating_add(*length))
                        .chain(
                            self.volume_coverage(volume)
                                .ranges()
                                .iter()
                                .map(|(_, end)| *end),
                        )
                        .chain(
                            self.router
                                .held_ranges(volume)
                                .into_iter()
                                .map(|(_, end)| end),
                        )
                        .max()
                        .unwrap_or(0)
                });
                (volume, length)
            })
            .collect()
    }

    pub(crate) fn owner_router(&self, owner: u32) -> Option<&DirectSetRouter> {
        if owner == super::nested::ROOT_NODE {
            return Some(&self.router);
        }
        let graph = self.nested.as_ref()?;
        graph.nodes.get(owner as usize - 1).map(|node| &node.router)
    }

    pub(crate) fn finalization_members(
        &self,
    ) -> Vec<(String, u64, std::path::PathBuf, std::path::PathBuf)> {
        fn append(
            set: &DirectSet,
            owner: u32,
            visited: &mut HashSet<u32>,
            output: &mut Vec<(String, u64, std::path::PathBuf, std::path::PathBuf)>,
        ) {
            let Some(router) = set.owner_router(owner) else {
                return;
            };
            let sizes: HashMap<_, _> = router.member_digest_entries().into_iter().collect();
            for (member, name, partial) in router.member_partials() {
                if set.nested_authority
                    && let Some(graph) = set.nested.as_ref()
                    && let Some((child, _)) = graph.binding(owner, member)
                    && graph.node(child).mode != super::nested::NodeMode::Materialized
                {
                    if visited.insert(child) {
                        append(set, child, visited, output);
                    }
                    continue;
                }
                if let Ok(destination) = set.plan().member_output_path(name) {
                    output.push((
                        name.to_owned(),
                        sizes.get(name).copied().unwrap_or(0),
                        router.plan().destination_path(partial),
                        destination,
                    ));
                }
            }
        }
        let mut members = Vec::new();
        append(
            self,
            super::nested::ROOT_NODE,
            &mut HashSet::new(),
            &mut members,
        );
        members
    }

    pub(crate) fn all_envelope_paths(&self) -> Vec<std::path::PathBuf> {
        let mut paths = self.plan().envelope_paths();
        if let Some(graph) = self.nested.as_ref() {
            for node in &graph.nodes {
                paths.extend(node.router.plan().envelope_paths());
            }
        }
        paths
    }

    pub(crate) fn prepare_nested_materialization(
        &self,
    ) -> Option<super::nested::materialize::Materialization> {
        if !self.nested_authority {
            return None;
        }
        let graph = self.nested.as_ref()?;
        let node = graph.nodes.iter().position(|node| {
            matches!(
                node.mode,
                super::nested::NodeMode::MaterializationPending(_)
            )
        })?;
        graph.prepare_materialization(&self.router, node as u32 + 1)
    }

    pub(crate) fn apply_nested_materialization(
        &mut self,
        materialized: super::nested::materialize::Materialized,
    ) -> Result<(), DemotionReason> {
        let receipts = self
            .nested
            .as_mut()
            .ok_or(DemotionReason::ConflictingVolumeFacts)?
            .apply_materialization(&self.router, materialized)?;
        for (receipt, sources) in receipts {
            let destination = DirectDestination::Member {
                member_id: receipt.member,
            };
            let path = self
                .placement_path(receipt.owner, destination)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            let raw = receipt
                .owner
                .raw_child
                .map(|child| (child.node, child.volume, child.generation));
            let key = (
                receipt.owner.node,
                receipt.owner.generation,
                0,
                receipt.member,
                raw,
            );
            self.qualified_placements
                .entry(key)
                .or_insert_with(|| QualifiedPlacement {
                    owner: receipt.owner,
                    destination,
                    path,
                    ranges: ByteRanges::new(),
                })
                .ranges
                .insert(receipt.offset, receipt.len);
            for source in sources {
                self.placed
                    .entry(source.volume)
                    .or_default()
                    .insert(source.offset, source.len);
            }
        }
        Ok(())
    }

    pub(crate) fn placement_path(
        &self,
        owner: super::router::SpanOwner,
        destination: DirectDestination,
    ) -> Option<std::path::PathBuf> {
        let router = self.owner_router(owner.node)?;
        if owner.node != super::nested::ROOT_NODE
            && self.nested.as_ref()?.node(owner.node).generation != owner.generation
        {
            return None;
        }
        match destination {
            DirectDestination::Envelope { volume_index } => {
                Some(router.plan().envelope_path(volume_index))
            }
            DirectDestination::Member { member_id } => router
                .member_partials()
                .into_iter()
                .find(|(member, _, _)| *member == member_id)
                .map(|(_, _, relative)| router.plan().destination_path(relative)),
        }
    }

    pub(crate) fn record_repost_digest(&mut self, digest: super::snapshot::RepostDigest) {
        if self
            .plan()
            .identity
            .is_some_and(|identity| identity.kind == super::plan::IdentityKind::HeaderVolumeSet)
            && self.plan().files.contains_key(&digest.file_index)
        {
            self.repost_digests
                .entry(digest.file_index)
                .or_insert(digest);
        }
    }

    pub(crate) fn repost_digest(&self, file_index: u32) -> Option<&super::snapshot::RepostDigest> {
        self.repost_digests.get(&file_index)
    }

    // The recovery-set fingerprints this set holds, by NZB file index.
    pub(crate) fn proven_fingerprints(&self) -> &BTreeMap<u32, ([u8; 16], u64)> {
        &self.proven_fingerprints
    }

    // Records the fingerprint of one of this set's files. The first one
    // recorded for a file stands.
    pub(crate) fn record_proven_fingerprint(
        &mut self,
        file_index: u32,
        hash_16k: [u8; 16],
        length: u64,
    ) {
        self.proven_fingerprints
            .entry(file_index)
            .or_insert((hash_16k, length));
    }

    fn fingerprint_rows(&self) -> Vec<super::snapshot::ProvenFingerprint> {
        self.proven_fingerprints
            .iter()
            .map(
                |(&file_index, &(hash_16k, length))| super::snapshot::ProvenFingerprint {
                    file_index,
                    hash_16k,
                    length,
                },
            )
            .collect()
    }

    // Rebuilds the set's layout from its cached volume facts.
    //
    // Runs **before** the checkpoint is validated, because validating it needs
    // the plan digest and the digest binds the member destinations, which only
    // exist once the layout has named them. A set whose facts no longer form a
    // routable archive demotes here and redownloads — the same outcome a refused
    // checkpoint produces, reached one step earlier.
    pub(crate) fn restore_layout(
        &mut self,
        facts: &BTreeMap<u32, super::restart::DirectVolumeFacts>,
    ) -> Result<(), DemotionReason> {
        match self.router.restore_layout(facts) {
            Ok(()) => Ok(()),
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    // Seeds the set with an accepted checkpoint: the barrier's floors and
    // claims, the router's coverage, and the volumes whose download is done.
    //
    // # Re-keying
    //
    // The blob's destination keys are the **previous run's** member ids, which
    // are in-run counters assigned as volumes arrived. This run rebuilt its
    // layout from the complete fact set in volume order, so it may well have
    // numbered the same members differently. Every claim is therefore re-keyed
    // by its relative path — the durable identity, derived from the header name
    // — and a claim naming a path this layout does not produce is **dropped**:
    // its bytes go unclaimed and are refetched, which is the safe direction.
    // Keeping it would leave the barrier with two destinations for one file and
    // the next snapshot claiming the same bytes twice.
    //
    // `complete_volumes` maps each volume the checkpoint calls complete to its
    // decoded length. That length is the row's own floor: a published `complete`
    // means the floor covers the whole decoded volume, so the two are the same
    // number by construction (see [`super::snapshot::VolumeFloor::complete`]).
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

        // The crypt rows go in **before** any coverage does, so
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

        // Only for files this plan binds: a fingerprint naming another file
        // would bind that file to a description it was never measured against.
        self.proven_fingerprints = rekeyed
            .fingerprints
            .iter()
            .filter(|fingerprint| {
                self.router
                    .plan()
                    .files
                    .contains_key(&fingerprint.file_index)
            })
            .map(|fingerprint| {
                (
                    fingerprint.file_index,
                    (fingerprint.hash_16k, fingerprint.length),
                )
            })
            .collect();
        self.repost_digests = rekeyed
            .repost_digests
            .iter()
            .filter(|digest| self.plan().files.contains_key(&digest.file_index))
            .map(|digest| (digest.file_index, *digest))
            .collect();
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
            let seeded = self.restart_seeded_volumes.entry(volume_index).or_default();
            for &(start, end) in covered.ranges() {
                seeded.insert(start, end - start);
                self.placed
                    .entry(volume_index)
                    .or_default()
                    .insert(start, end - start);
            }
            self.router
                .restore_volume_coverage(volume_index, &covered, decoded_len);
        }
    }

    // The logical length to present one source volume at, given whatever the
    // download layer says it has received.
    //
    // For a volume this run downloaded, `received_bytes` is the sum of the
    // **decoded** sizes the decoder reported, which is the volume's true length
    // — and it is preferred, because it is right even before every byte has been
    // routed.
    //
    // For a volume restored from a checkpoint it is **wrong and too large**.
    // Restore commits the skipped segments into the assembly with the spec's
    // `<segment bytes>`, which is the yEnc-*encoded* size, about 3% larger
    // than the payload. Presenting a virtual volume at that length hands PAR2
    // a file 3% longer than the one its descriptions cover, and the verifier
    // reports damage on a set that is byte-perfect — which is a demotion, a
    // full materialization and a redownload, for arithmetic. The coverage map
    // is in decoded space throughout, so for those volumes it is the only
    // honest answer: exact once the volume is complete, a lower bound while it
    // is not, and a mid-download set is neither verified against nor demoted
    // for its holes anyway.
    //
    // Either way the floor is the posted extent the virtual volume can
    // actually serve, holds included: `received_bytes` falls short of the
    // true length by exactly the articles that never came, and a hold waiting
    // past such a hole — the payload of a volume whose header was lost — is a
    // posted byte the provider answers for ([`Self::volume_coverage_with_holds`]).
    // Presenting the volume shorter than its last hold would put those bytes
    // past the end of the file PAR2 is handed, and the pass would rebuild
    // them from parity it did not need to spend.
    pub(crate) fn virtual_volume_len(&self, volume_index: u32, received_bytes: u64) -> u64 {
        let covered_end = self.volume_coverage_with_holds(volume_index).end();
        if self.restart_seeded_volumes.contains_key(&volume_index) {
            return covered_end;
        }
        received_bytes.max(covered_end)
    }

    // Whether the volume's download finished and its bytes are all placed.
    pub(crate) fn volume_is_complete(&self, volume_index: u32) -> bool {
        self.complete_volumes.contains_key(&volume_index)
    }

    // Whether the set is carrying restart-seeded coverage no gate has verified.
    pub(crate) fn has_restart_seeded_coverage(&self) -> bool {
        self.router.has_restart_seeded_coverage()
    }

    // Whether any of the set's coverage came back from a checkpoint rather than
    // from articles this run decoded. Latched: it stays true after the gate
    // re-arm has verified those bytes, because the fact it states is about where
    // they came from, not whether they are trusted yet.
    pub(crate) fn was_restored(&self) -> bool {
        !self.restart_seeded_volumes.is_empty()
    }

    // The physical ranges of one volume a checkpoint seeded, in the space
    // [`Self::volume_coverage`] answers in. Latched for the same reason as
    // [`Self::was_restored`]: it says where the bytes came from, and a later
    // re-read of a member does not change that this process holds no article
    // record for them.
    pub(crate) fn restored_volume_coverage(&self, volume_index: u32) -> ByteRanges {
        self.restart_seeded_volumes
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn plan(&self) -> &DirectSetPlan {
        self.router.plan()
    }

    // Records one identity binding on the plan. See
    // [`DirectSetPlan::bind_identity_volume`] for the refusal semantics.
    pub(crate) fn bind_identity_volume(&mut self, volume_index: u32, file_index: u32) -> bool {
        self.router.bind_identity_volume(volume_index, file_index)
    }

    // Marks a volume no posted file carries. See
    // [`DirectSetPlan::note_unposted_volume`].
    pub(crate) fn note_unposted_volume(&mut self, volume_index: u32) -> bool {
        self.router.note_unposted_volume(volume_index)
    }

    // Gives an unposted volume its reserved file. See
    // [`DirectSetPlan::bind_unposted_volume`].
    pub(crate) fn bind_unposted_volume(&mut self, volume_index: u32, file_index: u32) -> bool {
        self.router.bind_unposted_volume(volume_index, file_index)
    }

    pub(crate) fn set_name(&self) -> &str {
        &self.router.plan().set_name
    }

    // The plan facts the checkpoint reader validates a row against.
    pub(crate) fn expected_set(&self) -> ExpectedSet {
        ExpectedSet {
            plan_digest: self.plan_digest(),
            volume_files: self.router.plan().expected_volume_files(),
            fact_volumes: self.router.fact_volumes(),
        }
    }

    // The digest the checkpoint is written under. Stable across volume growth;
    // see [`DirectSetPlan::digest`].
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

    pub(crate) fn delegate_effective_extraction(&mut self) {
        self.delegated = true;
    }

    pub(crate) fn is_delegated(&self) -> bool {
        self.delegated
    }

    // The reason the set left direct mode under, which is the router's own
    // when it demoted from inside `route` before the wiring seam was asked.
    #[cfg(test)]
    pub(crate) fn demotion_reason(&self) -> Option<DemotionReason> {
        match self.status {
            DirectSetStatus::Demoted(reason) => Some(reason),
            _ => None,
        }
    }

    // Leaves direct mode. Refuses once the set is terminal in either
    // direction: a demotion is idempotent, and a **finalized** set has already
    // renamed its members to their destinations and been marked extracted, so
    // demoting it would delete completed output and refetch volumes nobody is
    // waiting for. Defence in depth — the callers check too.
    pub(crate) fn demote(&mut self, reason: DemotionReason) {
        if self.is_demoted() || self.is_finalized() {
            return;
        }
        self.router.demote(reason);
        self.latched_materialized = true;
        self.status = DirectSetStatus::Demoted(reason);
    }

    // Claims the demotion's one-time cleanup.
    //
    // `true` exactly once per set, and never for a finalized one. Separate
    // from [`Self::demote`] because the router demotes from inside `route`, so
    // the status is already `Demoted` by the time the wiring seam — which owns
    // deleting the output, retiring the row and refetching — is asked.
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

    // Feeds one article's yEnc part CRC32 into its volume's composition.
    // Overlapping runs are ignored by [`CrcRuns`], so a duplicate article
    // never advances the composition twice.
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

    // The composed whole-volume CRC32, when the parts cover `[0, len)` end to
    // end.
    pub(crate) fn volume_crc(&self, volume_index: u32, len: u64) -> Option<u32> {
        self.volume_crc_run(volume_index, 0, len)
    }

    // The composed CRC32 of one exact source run of a volume, when the yEnc
    // part composition happens to have coalesced into precisely that run.
    //
    // Deliberately exact rather than "the value covering this range": a run
    // the composition can only bound is no reference value at all, and
    // reconstruction asks for verification *where available*.
    pub(crate) fn volume_crc_run(&self, volume_index: u32, start: u64, len: u64) -> Option<u32> {
        self.volume_crcs
            .get(&volume_index)
            .and_then(|runs| runs.compose(start, len))
    }

    // The whole yEnc part composition for one volume, for a caller that has to
    // ask about several sub-ranges of it (the reconstruction sweep).
    pub(crate) fn volume_crc_runs(&self, volume_index: u32) -> CrcRuns {
        self.volume_crcs
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    // Rewrites one volume's yEnc composition over a span a PAR2 repair
    // changed.
    //
    // `insert` would be wrong here for the same reason it is wrong for a
    // member: the bytes on disk moved, so a composition that kept the old value
    // would describe a volume that no longer exists — and the next
    // reconstruction would compare rebuilt bytes against it and refuse a volume
    // that is now correct.
    //
    // Unlike the member-space twin in
    // [`super::router::DirectSetRouter::note_member_bytes`], the gaps
    // [`CrcRuns::overwrite`] reports here must always be **empty**, and the
    // caller discards them rather than re-reading them. That is not an
    // oversight, it is the whole point of
    // [`super::repair::widen_to_articles`]: a rewrite span is widened to whole
    // articles wherever the decoded geometry is known, so it lands run for run
    // on the article-shaped volume composition, and a span in a region no
    // article ever covered has no run to half-cover. A gap here would mean the
    // widening stopped covering the composition it exists to keep whole, and
    // the next reconstruction sweep would refuse the volume with
    // `UnverifiableRun` — so it is asserted, mirroring
    // [`super::router::DirectSetRouter::note_restored_member_crc`].
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

    // Whether a repair-while-direct has already run for this set. See
    // [`Self::repair_attempted`].
    pub(crate) fn repair_attempted(&self) -> bool {
        self.repair_attempted
    }

    // Burns the repair once-latch. Called at the first irreversible step of a
    // repair — the checkpoint delete — so a refusal that costs the set nothing
    // does not spend the one attempt it gets.
    pub(crate) fn note_repair_attempted(&mut self) {
        self.repair_attempted = true;
    }

    // The RAM ceiling this set's holds are bounded by, which is also what a
    // repair's rewrite is sized against before it is planned: every repaired
    // byte re-enters the router as a hold.
    pub(crate) fn holds_budget(&self) -> u64 {
        self.router.holds_budget()
    }

    // Routes one volume's complete rewrite inside an open repair transaction.
    //
    // Unlike [`Self::route_repaired`] this does not settle: a repaired byte
    // that needs another volume's rewrite — an encrypted block straddling the
    // seam, a trailing region awaiting confirmation — stays staged until
    // [`Self::finish_repair_transaction`] answers for it.
    pub(crate) fn route_repaired_volume(
        &mut self,
        volume_index: u32,
        spans: &[super::router::RepairedChunk],
        lead_in: &[(u32, u64, bytes::Bytes)],
        whole_volume: bool,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        match self
            .router
            .route_repaired_batch(volume_index, spans, lead_in, whole_volume, true)
        {
            Ok(spans) => {
                self.latched_direct |= !spans.is_empty();
                Ok(spans)
            }
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    pub(crate) fn begin_repair_transaction(
        &mut self,
        volumes: Vec<u32>,
    ) -> Result<(), DemotionReason> {
        self.router
            .begin_repair_transaction(volumes)
            .inspect_err(|reason| {
                self.demote(*reason);
            })
    }

    pub(crate) fn finish_repair_transaction(&mut self) -> Result<(), DemotionReason> {
        self.router
            .finish_repair_transaction()
            .inspect_err(|reason| {
                self.demote(*reason);
            })
    }

    // Apply one bounded replacement batch; the caller must place its spans
    // before advancing and retire the job on any placement failure.
    pub(crate) fn route_repaired_batch(
        &mut self,
        volume: u32,
        chunks: &[super::router::RepairedChunk],
        lead_in: &[(u32, u64, bytes::Bytes)],
        finish: bool,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        match self
            .router
            .route_repaired_batch(volume, chunks, lead_in, finish, finish)
        {
            Ok(spans) => {
                self.latched_direct |= !spans.is_empty();
                Ok(spans)
            }
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    // Replace article composition only after a complete verified image has
    // been routed and placed. Stripe boundaries are not article boundaries.
    pub(crate) fn note_repaired_whole_volume_crc(&mut self, volume: u32, len: u64, crc: u32) {
        let runs = self.volume_crcs.entry(volume).or_default();
        *runs = CrcRuns::default();
        runs.insert(0, len, crc);
    }

    pub(crate) fn mark_finalized(&mut self) {
        if !self.is_demoted() {
            self.status = DirectSetStatus::Finalized;
        }
    }

    // Routes one decoded source span. A demotion is returned rather than
    // panicking: the caller abandons direct output for the whole set.
    // [`DirectSetRouter::release_article_views`].
    pub(crate) fn release_article_views(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        len: u64,
        pool_scarce: bool,
    ) -> u64 {
        self.router
            .release_article_views(volume_index, source_offset, len, pool_scarce)
    }

    // Records the length one volume's yEnc headers declare. See
    // [`super::router::DirectSetRouter::note_declared_volume_size`].
    pub(crate) fn note_declared_volume_size(
        &mut self,
        volume_index: u32,
        declared_len: u64,
    ) -> Result<(), DemotionReason> {
        match self
            .router
            .note_declared_volume_size(volume_index, declared_len)
        {
            Ok(()) => Ok(()),
            Err(reason) => {
                self.demote(reason);
                Err(reason)
            }
        }
    }

    // What the set needs off the wire next to resolve its layout.
    pub(crate) fn header_probe(&self) -> super::router::HeaderProbe {
        self.router.header_probe()
    }

    pub(crate) fn route(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        pieces: &[bytes::Bytes],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        let spans = self
            .router
            .route(volume_index, source_offset, pieces)
            .and_then(|spans| self.forward_root_spans(spans));
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

    // Every volume has completed, every member has passed its archive gate,
    // and any deferred PAR3 verdict has been applied to the router.
    pub(crate) fn ready_to_finalize(&self) -> bool {
        !self.delegated
            && !self.is_demoted()
            && !self.is_finalized()
            && self.all_volumes_complete()
            && !self.router.awaits_par3_verdict()
            && self.router.all_members_verified()
            && (!self.nested_authority
                || self.nested.as_ref().is_some_and(|graph| {
                    graph.nodes.iter().all(|node| {
                        node.mode == super::nested::NodeMode::Materialized
                            || (node.mode == super::nested::NodeMode::Direct
                                && node.roster_closed
                                && node.inputs.values().all(|input| input.confirmed)
                                && node.router.all_members_verified()
                                && !node.router.awaits_par3_verdict())
                    })
                }))
    }

    fn forward_root_spans(
        &mut self,
        spans: Vec<RoutedSpan>,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if !self.nested_authority {
            return Ok(spans);
        }
        self.nested
            .as_mut()
            .expect("active graph")
            .forward(&self.router, super::nested::ROOT_NODE, spans)
            .map(|spans| {
                spans
                    .into_iter()
                    .map(super::nested::NodeSpan::into_span)
                    .collect()
            })
    }

    // Every source volume the set plans has finished downloading.
    //
    // The payload half of [`Self::ready_to_finalize`], on its own: a PAR2
    // verdict over a set that is still receiving articles reads its not-yet
    // downloaded ranges as holes, and a hole is indistinguishable from damage
    // at that layer. Callers that must not confuse "not here yet" with
    // "corrupt" ask this first.
    pub(crate) fn all_volumes_complete(&self) -> bool {
        // The expected count, not the mapped count: an identity-admitted set's
        // mapping trails its full set, and comparing against `volumes.len()`
        // would finalize an archive most of whose volumes have not even been
        // matched to files yet. An open set — its count not yet learnable —
        // is never complete.
        self.router
            .plan()
            .expected_volume_count()
            .is_some_and(|expected| self.complete_volumes.len() == expected)
    }

    // Marks a source volume complete and returns whatever the confirming parse
    // just made routable. The caller must write those spans before recording
    // them, exactly as it does for [`Self::route`]'s.
    // `decoded_len` is the volume's decoded length — the assembly's
    // `received_bytes`, not the spec's yEnc-encoded segment sizes — and is what
    // lets the checkpoint distinguish "the download finished" from "every byte
    // of it is durable".
    //
    // For a volume **restored** from a checkpoint it is an over-estimate rather
    // than the exact length: restore commits the skipped segments into the
    // assembly at the spec's encoded sizes, ~3% large (see
    // [`Self::virtual_volume_len`]). That errs in the safe direction — the
    // checkpoint's `complete` bit stays `false`, so a *second* restart refetches
    // the volume's last article instead of skipping it, which is the same
    // bounded cost the contiguous-floor model already pays for every partially
    // covered volume. An under-estimate would be the unsafe direction, and no
    // path produces one.
    pub(crate) fn note_volume_complete(
        &mut self,
        volume_index: u32,
        decoded_len: u64,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        self.complete_volumes.insert(volume_index, decoded_len);
        if let Some(barrier) = self.barrier.as_mut() {
            barrier.note_volume_complete(volume_index, decoded_len);
        }
        // The router holds a container volume's length against the geometry,
        // and the over-estimate a restored volume carries is the wrong number
        // to hold there: 3% over the part size reads as a part-size hint that
        // was wrong, and demotes a set whose bytes are exactly where the map
        // put them. The coverage end is the decoded length such a volume
        // really has, the same answer PAR2 is given for it. The checkpoint
        // above keeps the over-estimate on purpose.
        let routed_len = self.virtual_volume_len(volume_index, decoded_len);
        let spans = self
            .router
            .note_volume_complete(volume_index, routed_len)
            .and_then(|spans| self.forward_root_spans(spans))
            .and_then(|mut spans| {
                if self.nested_authority && self.all_volumes_complete() {
                    spans.extend(
                        self.nested
                            .as_mut()
                            .expect("active graph")
                            .close_parent_roster(&self.router, super::nested::ROOT_NODE)?
                            .into_iter()
                            .map(super::nested::NodeSpan::into_span),
                    );
                }
                Ok(spans)
            });
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

    // Registers the set's volumes and every destination the router has learned,
    // retires the ones it has lost, and keeps the barrier's plan digest level
    // with the facts it is routing against.
    //
    // Idempotent, and the only place a barrier comes into existence.
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
            self.registered_volumes = 0;
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
        //
        // Or when the volume map grew. The digest binds the mapping too, and an
        // identity-admitted set's mapping grows file by file as later volumes
        // bind — without a single member fact moving. A digest stamped at the
        // first binding would label every later row with a one-volume plan the
        // set stopped being, and the restart that rebuilds the plan from the
        // row's own binding would refuse it. A name-admitted set's map is whole
        // from admission, so for it this only recomputes the same value.
        let revision = self.router.member_facts_revision();
        if self.digest_revision != Some(revision)
            || self.registered_volumes != self.router.plan().volumes.len()
        {
            let digest = self.plan_digest();
            if let Some(barrier) = self.barrier.as_mut() {
                barrier.set_plan_digest(digest);
            }
            self.digest_revision = Some(revision);
        }
        let Some(barrier) = self.barrier.as_mut() else {
            return;
        };
        if self.registered_volumes != self.router.plan().volumes.len() {
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
            self.registered_volumes = self.router.plan().volumes.len();
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

    // Records the decoded extent of one article of a source volume.
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

    // The decoded extents recorded for one volume's articles.
    pub(crate) fn segment_extents(&self, volume_index: u32) -> BTreeMap<u32, (u64, u64)> {
        self.segment_extents
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    // Records spans whose writes have **all** returned. A refusal here is a
    // wiring bug, not a runtime condition, and the barrier says so loudly.
    pub(crate) fn record_writes(&mut self, spans: &[RoutedSpan], now: Instant) {
        self.record_placements(spans.iter().map(RoutedSpan::placement), now);
    }

    pub(crate) fn record_placements(
        &mut self,
        spans: impl Iterator<Item = super::router::PlacementReceipt> + Clone,
        now: Instant,
    ) {
        if self.nested_authority {
            for receipt in spans {
                let Some(path) = self.placement_path(receipt.owner, receipt.destination) else {
                    self.demote(DemotionReason::ConflictingVolumeFacts);
                    return;
                };
                let sources = match self
                    .nested
                    .as_mut()
                    .expect("active graph")
                    .record_placement(&self.router, receipt)
                {
                    Ok(sources) => sources,
                    Err(reason) => {
                        self.demote(reason);
                        return;
                    }
                };
                let (kind, destination) = match receipt.destination {
                    DirectDestination::Member { member_id } => (0, member_id),
                    DirectDestination::Envelope { volume_index } => (1, volume_index),
                };
                let raw = receipt
                    .owner
                    .raw_child
                    .map(|child| (child.node, child.volume, child.generation));
                let key = (
                    receipt.owner.node,
                    receipt.owner.generation,
                    kind,
                    destination,
                    raw,
                );
                let fresh = self
                    .qualified_placements
                    .entry(key)
                    .or_insert_with(|| QualifiedPlacement {
                        owner: receipt.owner,
                        destination: receipt.destination,
                        path,
                        ranges: ByteRanges::new(),
                    })
                    .ranges
                    .insert(receipt.destination_offset, receipt.len);
                self.graph_dirty_bytes = self.graph_dirty_bytes.saturating_add(fresh);
                if fresh > 0 {
                    self.live_placement_revision = self.live_placement_revision.saturating_add(1);
                    self.graph_dirty_since.get_or_insert(now);
                }
                for source in sources {
                    self.placed
                        .entry(source.volume)
                        .or_default()
                        .insert(source.offset, source.len);
                }
                if receipt.owner.node == super::nested::ROOT_NODE
                    && let DirectDestination::Envelope { volume_index } = receipt.destination
                {
                    self.placed_envelope
                        .entry(volume_index)
                        .or_default()
                        .insert(receipt.destination_offset, receipt.len);
                }
            }
            return;
        }
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
            spans.clone().next().is_none()
                || self.barrier.is_some()
                || spans
                    .clone()
                    .all(|span| !matches!(span.destination, DirectDestination::Envelope { .. })),
            "direct-store emitted envelope spans for {} before any member registered a barrier",
            self.set_name()
        );
        // Recorded before the barrier is consulted, and whether or not one
        // exists: this is the account [`Self::volume_coverage`] falls back on,
        // and it must describe writes that *returned*, exactly like the
        // barrier's.
        for span in spans.clone() {
            self.placed
                .entry(span.volume_index)
                .or_default()
                .insert(span.source_offset, span.len);
            if let DirectDestination::Envelope { volume_index } = span.destination {
                self.placed_envelope
                    .entry(volume_index)
                    .or_default()
                    .insert(span.destination_offset, span.len);
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
                    len: span.len,
                    member_index,
                    destination_offset: span.destination_offset,
                },
                now,
            );
        }
    }

    pub(crate) fn due(&self, now: Instant) -> Option<BarrierTrigger> {
        if self.nested_authority {
            if self.graph_metadata != Some(self.graph_metadata_fingerprint()) {
                return Some(BarrierTrigger::PlanDigestChanged);
            }
            if self.graph_dirty_bytes >= super::barrier::BARRIER_DIRTY_BYTES {
                return Some(BarrierTrigger::DirtyBytes);
            }
            return self
                .graph_dirty_since
                .filter(|since| {
                    now.saturating_duration_since(*since) >= super::barrier::BARRIER_DIRTY_AGE
                })
                .map(|_| BarrierTrigger::DirtyAge);
        }
        self.barrier.as_ref().and_then(|barrier| barrier.due(now))
    }

    // Aggregate unique dirty bytes the set is carrying, i.e. what the barrier
    // is about to make durable. Read before a barrier runs, because running it
    // resets the count.
    pub(crate) fn dirty_bytes(&self) -> u64 {
        if self.nested_authority {
            return self.graph_dirty_bytes;
        }
        self.barrier
            .as_ref()
            .map(|barrier| barrier.dirty_bytes())
            .unwrap_or(0)
    }

    // Destination paths touched since the last successful barrier, each paired
    // with the relative path the barrier knows it by.
    //
    // Both halves are returned because the caller needs both and cannot derive
    // one from the other any more: the sync step is keyed by the relative path
    // the barrier will ask for, while the fsync itself needs the absolute path
    // — and the two roots mean stripping a single prefix off the absolute path
    // no longer recovers the relative one.
    pub(crate) fn touched_paths(&self) -> Vec<(String, std::path::PathBuf)> {
        let plan = self.router.plan();
        self.barrier
            .as_ref()
            .map(|barrier| {
                barrier
                    .touched_destinations()
                    .into_iter()
                    .map(|(destination_key, relative)| {
                        (
                            relative.to_string(),
                            plan.barrier_destination_path(destination_key, relative),
                        )
                    })
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
        // A repair deleted the old checkpoint before changing destinations.
        // Do not recreate it from a mixture of old and replacement bytes.
        if self.router.repair_batch_in_progress() {
            return None;
        }
        // Finalization retired the checkpoint and left the set's installation
        // marker in its row. A snapshot now would claim nothing — every
        // member is committed and the barrier is empty — and overwrite the
        // marker with it, so a restart would refetch the whole set.
        if self.is_finalized() {
            return None;
        }
        // Level the barrier with the router before it builds a snapshot: the
        // plan digest it stamps and the destinations it claims must both be the
        // ones the set is routing against *now*, not the ones it was built with.
        // Guarded on an existing barrier so this stays a refresh — a demanded
        // barrier for a set that has never recorded a write still writes no row,
        // exactly as before.
        if self.barrier.is_some() {
            self.ensure_registered();
        }
        // Read off the router immediately before the run, so a
        // checkpoint's crypt rows are never older than the coverage beside them:
        // the retained tail padding and the cipher checkpoints are both produced
        // by the same routing call that produced the bytes being claimed.
        let crypt = self.router.member_crypt_snapshots();
        let identity = self.router.plan().identity_binding();
        let fingerprints = self.fingerprint_rows();
        let repost_digests = self.repost_digests.values().copied().collect();
        let barrier = self.barrier.as_mut()?;
        barrier.set_member_crypt(crypt);
        barrier.set_identity_binding(identity);
        barrier.set_proven_fingerprints(fingerprints);
        barrier.set_repost_digests(repost_digests);
        Some(barrier.barrier(trigger, now, drain, sync, persist))
    }

    // The first half of [`Self::run_barrier`]: the same guards and the same
    // leveling, then a [`super::barrier::PreparedBarrier`] whose sync the
    // caller runs wherever it likes before [`Self::commit_barrier`].
    pub(crate) fn prepare_barrier<D>(
        &mut self,
        trigger: BarrierTrigger,
        now: Instant,
        drain: &mut D,
    ) -> Option<Result<super::barrier::PreparedBarrier, BarrierError>>
    where
        D: super::barrier::BarrierDrain + ?Sized,
    {
        if self.router.repair_batch_in_progress() || self.is_finalized() {
            return None;
        }
        if self.barrier.is_some() {
            self.ensure_registered();
        }
        let crypt = self.router.member_crypt_snapshots();
        let identity = self.router.plan().identity_binding();
        let fingerprints = self.fingerprint_rows();
        let repost_digests = self.repost_digests.values().copied().collect();
        let barrier = self.barrier.as_mut()?;
        barrier.set_member_crypt(crypt);
        barrier.set_identity_binding(identity);
        barrier.set_proven_fingerprints(fingerprints);
        barrier.set_repost_digests(repost_digests);
        Some(barrier.prepare(trigger, now, drain))
    }

    // The second half of [`Self::run_barrier`] for a prepared barrier.
    //
    // `None` means the barrier was abandoned rather than run: the set was
    // demoted, finalized, or entered a repair while the sync ran, so the
    // prepared snapshot no longer describes bytes the row may claim. The
    // interval it captured goes back to the controller for the next barrier.
    pub(crate) fn commit_barrier<S, P>(
        &mut self,
        prepared: super::barrier::PreparedBarrier,
        now: Instant,
        sync: &mut S,
        persist: &mut P,
    ) -> Option<Result<BarrierReport, BarrierError>>
    where
        S: super::barrier::DestinationSync + ?Sized,
        P: CoveragePersist + ?Sized,
    {
        let stale =
            self.router.repair_batch_in_progress() || self.is_finalized() || self.is_demoted();
        let barrier = self.barrier.as_mut()?;
        if stale {
            barrier.abandon(prepared);
            return None;
        }
        Some(barrier.commit(prepared, now, sync, persist))
    }

    // Deletes the set's checkpoint row and keeps everything else (repair while
    // still direct), so the coverage the hybrid provider reads survives a
    // repair that only rewrote bytes in place.
    //
    // [`Self::retire`] is the demotion form and it is not interchangeable: it
    // resets the controller, which is right when the destinations are about to
    // be deleted and catastrophic when they are not — a repaired set whose
    // coverage was reset reports every volume it did not touch as *missing* to
    // the re-verify, and the whole set demotes for damage that is an empty map.
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

    // Deletes the set's checkpoint row **and** retires the controller. Used on
    // demotion, where the destinations it describes are about to go.
    //
    // The delete runs even with no barrier built. A set can be resumed from a
    // checkpoint written before a restart and then demote before its layout
    // names a member again (`FormatMismatch`, `UnparsableVolume`), which is
    // exactly the case where the row exists and the in-memory controller does
    // not; skipping the delete there would leave a checkpoint claiming
    // destinations that are about to be deleted.
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
        self.registered_volumes = 0;
        // A retired controller is unregistered in every sense, the digest
        // included: the next registration re-derives it rather than trusting a
        // revision recorded for a controller that no longer holds anything.
        self.digest_revision = None;
        Ok(())
    }

    // Everything durably placed for one source volume, in physical space.
    //
    // The barrier is authoritative: it only learns about writes whose every
    // destination returned. Before the first member registers there is no
    // barrier at all — a set that demotes that early has written envelope
    // bytes and nothing else — and the fallback is [`Self::placed`], which is
    // fed by the same call and under the same rule. Deliberately **not** the
    // router's routed map: that records what routing emitted, including spans
    // whose write failed, and claiming one of those would send the demotion
    // sweep to read a byte back out of a file that never received it.
    pub(crate) fn volume_coverage(&self, volume_index: u32) -> ByteRanges {
        let mut coverage = self
            .barrier
            .as_ref()
            .and_then(|barrier| barrier.volume_coverage(volume_index))
            .unwrap_or_else(|| self.placed.get(&volume_index).cloned().unwrap_or_default());
        if self.nested_authority
            && let Some(placed) = self.placed.get(&volume_index)
        {
            for &(start, end) in placed.ranges() {
                coverage.insert(start, end - start);
            }
        }
        coverage
    }

    // [`Self::volume_coverage`] plus the volume's holds: everything a
    // reconstruction sweep can read back in posted space.
    //
    // A hold is a posted byte too — staged, verified by its article's yEnc
    // CRC32, and simply not yet routed. The virtual volume serves holds from
    // staging ([`super::router::DirectSetRouter::held_runs`]), so a repair
    // sweep that only claimed the placed bytes would leave a hole exactly
    // where an encrypted member's edge block waits for a lost article, and
    // refuse a volume whose every posted byte is in hand. The demotion sweep
    // reads it only for a set demoted for room, with any handed-off article's
    // range cut out first: that article belongs to the conventional path.
    pub(crate) fn volume_coverage_with_holds(&self, volume_index: u32) -> ByteRanges {
        let mut coverage = self.volume_coverage(volume_index);
        for (start, end) in self.router.held_ranges(volume_index) {
            coverage.insert(start, end.saturating_sub(start));
        }
        coverage
    }

    // The physical ranges one volume's **envelope file** received.
    //
    // The provider needs this separately from [`Self::volume_coverage`]: an
    // envelope is sparse, so a read at an offset it never received answers with
    // zeros rather than failing, and "the volume placed this byte somewhere" is
    // not evidence that the envelope is where it went.
    pub(crate) fn envelope_coverage(&self, volume_index: u32) -> ByteRanges {
        let mut coverage = self
            .barrier
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
            });
        if self.nested_authority
            && let Some(placed) = self.placed_envelope.get(&volume_index)
        {
            for &(start, end) in placed.ranges() {
                coverage.insert(start, end - start);
            }
        }
        coverage
    }

    // A [`HybridVolumeProvider`] over this set's partials and envelopes.
    //
    // `volume_lengths` gives each volume its logical length — the provider
    // cannot know it, because a direct volume's length is the decoded total the
    // download layer tracks, not anything a partial or an envelope states.
    // Volumes absent from the map are omitted, since a reader with no length
    // could not answer `SeekFrom::End` or stop at the right place.
    pub(crate) fn virtual_provider(
        &self,
        volume_lengths: &BTreeMap<u32, u64>,
    ) -> HybridVolumeProvider {
        HybridVolumeProvider::new(self.virtual_volumes(volume_lengths))
    }

    // The same volumes as [`Self::virtual_provider`], unassembled.
    //
    // A job can hold several direct sets, and every set numbers its volumes
    // from zero — so a caller that has to put *all* of them behind one provider
    // (the PAR2 `FileAccess` adapter, which sees one job's whole recovery set)
    // needs to re-key them first. That caller gets the parts; everything else
    // wants the assembled provider.
    pub(crate) fn virtual_volumes(
        &self,
        volume_lengths: &BTreeMap<u32, u64>,
    ) -> Vec<VirtualVolume> {
        // Built once and shared, never cloned per volume: every volume of a set
        // resolves member ids against the *same* partial paths, and a set with
        // `v` volumes and `m` members would otherwise pay `v * m` path clones
        // every time a provider is assembled — which is once per authoritative
        // PAR2 pass, per demotion sweep and per tolerated extraction (nit).
        let plan = self.router.plan();
        let partials: std::sync::Arc<std::collections::HashMap<u32, std::path::PathBuf>> =
            std::sync::Arc::new(
                self.router
                    .member_partials()
                    .into_iter()
                    .map(|(member_id, _, partial)| (member_id, plan.destination_path(partial)))
                    .collect(),
            );
        // Shared for the same reason the paths are, and empty for every set
        // with no encrypted member — which is what leaves the provider's read
        // path exactly as it is for an unencrypted set. The router hands back
        // its own snapshot rather than a fresh copy of it, which is what keeps
        // a per-read provider off the checkpoint map.
        let ciphers = self.router.member_ciphers();
        let (partials, nested) = if self.nested_authority {
            self.nested
                .as_ref()
                .expect("active graph")
                .member_backings(&self.router, super::nested::ROOT_NODE)
        } else {
            (partials, Default::default())
        };
        volume_lengths
            .iter()
            .map(|(volume_index, len)| VirtualVolume {
                backing_leases: Default::default(),
                volume_index: *volume_index,
                envelope: self.router.plan().envelope_path(*volume_index),
                extents: self.router.volume_member_extents(*volume_index),
                partials: std::sync::Arc::clone(&partials),
                covered: self.volume_coverage(*volume_index),
                envelope_covered: self.envelope_coverage(*volume_index),
                held: std::sync::Arc::new(self.router.held_runs(*volume_index)),
                len: *len,
                ciphers: std::sync::Arc::clone(&ciphers),
                nested: nested.clone(),
            })
            .collect()
    }

    // [`Self::virtual_volumes`]' member map, pointed at the **committed
    // destinations** instead of the `.direct.partial`s finalization renamed
    // away. Byte-for-byte the same file — a commit is a rename — so the extents
    // resolve unchanged.
    //
    // `None` when a member has no resolvable destination, which is the one
    // shape a retained image must never be built over: a missing entry reads as
    // a hole, and a hole inside a member extent is a volume the verifier calls
    // damaged. `sync_members` already demotes a set whose members cannot all be
    // resolved *and* refuses two that collide onto one destination, so this can
    // only fire if those two ever drift — but the whole point of serving a
    // committed member is that the path is the member's own, so it is checked
    // here rather than assumed.
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

    // Captures the set's virtual volume image so it survives finalization, and
    // reports whether it is worth keeping.
    //
    // Must be called **before** [`Self::retire`] and **after** the members have
    // been renamed to their destinations: the first because retiring resets the
    // coverage controller this reads, the second because nothing but the rename
    // makes the substituted paths real.
    //
    // `false` — and nothing retained — unless every planned volume reads as one
    // unbroken run from zero to its length. A retained image exists to answer a
    // *neighbour's* repair, and a repair reads its surviving inputs whole: an
    // image with a hole in it would have the pass call this set damaged, plan a
    // repair of volumes nobody can write, and refuse the neighbour's along with
    // it. Refusing to retain leaves the job on the pre-existing path, where
    // `forgive_finalized_direct_volumes` excuses the absent volumes instead.
    pub(crate) fn retain_finalized_volumes(&mut self, volume_lengths: &BTreeMap<u32, u64>) -> bool {
        self.retained = None;
        if volume_lengths.len() != self.router.plan().volumes.len() {
            return false;
        }
        if self.nested_authority {
            let volumes = self.virtual_volumes(volume_lengths);
            if !volumes
                .iter()
                .all(|volume| volume.len > 0 && volume.readable_prefix() == Some(volume.len))
            {
                return false;
            }
            self.retained = Some(volumes);
            return true;
        }
        let Some(partials) = self.committed_member_paths() else {
            return false;
        };
        // The committed member file is byte-for-byte the partial — a commit is a
        // rename — so an encrypted set's retained image re-encrypts out of it
        // exactly as the live one did.
        let ciphers = self.router.member_ciphers();
        let volumes: Vec<VirtualVolume> = volume_lengths
            .iter()
            .map(|(volume_index, len)| VirtualVolume {
                backing_leases: Default::default(),
                volume_index: *volume_index,
                envelope: self.router.plan().envelope_path(*volume_index),
                extents: self.router.volume_member_extents(*volume_index),
                partials: std::sync::Arc::clone(&partials),
                covered: self.volume_coverage(*volume_index),
                envelope_covered: self.envelope_coverage(*volume_index),
                // A finalized set has routed everything; nothing is held.
                held: std::sync::Arc::new(Vec::new()),
                len: *len,
                ciphers: std::sync::Arc::clone(&ciphers),
                nested: Default::default(),
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

    // The retained image, or `None` for a set that never kept one or has since
    // released it.
    pub(crate) fn retained_volumes(&self) -> Option<&[VirtualVolume]> {
        self.retained.as_deref()
    }

    pub(crate) fn retain_restored_finalized(&mut self) -> bool {
        let lengths = self.checkpoint_volume_lengths();
        let volumes = self.virtual_volumes(&lengths);
        if volumes.is_empty()
            || !volumes
                .iter()
                .all(|volume| volume.len > 0 && volume.readable_prefix() == Some(volume.len))
        {
            return false;
        }
        self.retained = Some(volumes);
        true
    }

    // Drops the retained image. The caller deletes the envelope files it named
    // in the same breath — they are what the image reads through, and keeping
    // either without the other is a lie in one direction or dead bytes in the
    // other.
    pub(crate) fn release_retained_volumes(&mut self) {
        self.retained = None;
    }

    // The two checkpoint systems must never both own a member (the risk list).
    // A direct set is marked extracted at finalization without ever entering
    // the incremental extractor, so an extraction checkpoint naming one of its
    // members means routing and extraction both claimed it.
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
